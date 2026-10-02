//! Streams multipart parts into the provider's own upload on S3 backends and completes it there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::{build_backend_path, intent_key};
use crate::hash::Hasher;
use crate::part_chain::{PartAttempt, PartChain};
use crate::s3::{NativeMultipart, multipart_etag};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle as _;
use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, ResolvedBackend, WriteOwner,
};
use aruna_core::structs::storage::multipart::{BackendUpload, MultipartPart};
use bytes::Bytes;
use byteview::ByteView;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::SystemTime;
use tokio::time::timeout;
use ulid::Ulid;

/// Bytes seen while a part streams: its own hashes and, when it is next in order, the upload's.
struct PartTee {
    part: Hasher,
    chain: Option<Hasher>,
    size: u64,
    client_failed: Option<String>,
}

impl BlobHandler {
    fn chains(&self) -> std::sync::MutexGuard<'_, HashMap<String, PartChain>> {
        self.part_chains
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn native_upload(&self, location: &BackendLocation) -> Result<NativeMultipart, BlobError> {
        self.native_for(location)?.ok_or_else(|| {
            BlobError::WriteError("the backend has no provider multipart upload".to_string())
        })
    }

    /// Replaces the target's cleanup row: it is kept while the record names it, else discarded.
    async fn write_target_row(
        &self,
        location: &BackendLocation,
        record_id: Ulid,
    ) -> Result<(), BlobError> {
        let work = BlobCleanupWork::ReconcileWrite {
            location: location.clone(),
            owner: WriteOwner::Upload {
                upload_id: record_id,
            },
        };
        let value = ByteView::from(work.to_bytes().map_err(BlobError::ConversionError)?);
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key: intent_key(location),
                value,
                txn_id: None,
            }))
            .await;
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            event => Err(BlobError::WriteError(format!(
                "failed to write the upload target row: {event:?}"
            ))),
        }
    }

    /// Removes an in-place target, every provider upload under its path and its reservation.
    pub(super) async fn discard_target(&self, location: &BackendLocation) -> Result<(), BlobError> {
        let path = location.get_storage_path()?;
        let operator = self.operator_from_location(location)?;
        self.delete_path(&operator, &path).await?;
        self.abort_uploads(location, &path).await?;
        self.release_reservation(location).await
    }

    pub(super) async fn open_upload(
        &self,
        record_id: Ulid,
        bucket: &str,
        key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
    ) -> BlobEvent {
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(error) => return BlobEvent::Error(error),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(bucket, key, ulid) {
            Ok(path) => path,
            Err(error) => return BlobEvent::Error(BlobError::ConversionError(error)),
        };
        let template = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class,
            root,
            storage_bucket: String::new(),
            backend_path,
            ulid,
            compressed: false,
            encrypted: false,
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        // Only S3 backends have a provider upload; the others keep one blob per part.
        match self.native_for(&template) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return BlobEvent::UploadOpened {
                    backend_upload: None,
                };
            }
            Err(error) => return BlobEvent::Error(error),
        }
        let location = match self.reserve_bucket(&resolved.backend, &template).await {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        // Written before the provider upload exists, so a lost answer or a stop leaves a row.
        if let Err(error) = self.write_target_row(&location, record_id).await {
            _ = self.release_reservation(&location).await;
            return BlobEvent::Error(error);
        }
        let opened = async {
            let native = self.native_upload(&location)?;
            let path = location.get_storage_path()?;
            timeout(self.io_timeout(), native.create(&path))
                .await
                .map_err(|_| BlobError::WriteError("timed out opening the upload".to_string()))?
        }
        .await;
        match opened {
            Ok(upload_id) => BlobEvent::UploadOpened {
                backend_upload: Some(BackendUpload {
                    location,
                    upload_id,
                    record_id,
                }),
            },
            Err(error) => {
                // A create whose answer was lost may still have opened an upload at this path.
                _ = self.discard_target(&location).await;
                BlobEvent::Error(error)
            }
        }
    }

    /// Streams one part into the provider upload, hashing it as it passes.
    pub(super) async fn write_upload_part(
        &self,
        upload: BackendUpload,
        part_number: u16,
        size: Option<u64>,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let Some(size) = size else {
            return BlobEvent::Error(BlobError::WriteError(
                "an in-place part needs its Content-Length".to_string(),
            ));
        };
        let attempt = Ulid::generate();
        let chain = self
            .chains()
            .entry(upload.upload_id.clone())
            .or_default()
            .begin(part_number, attempt);
        let tee = Arc::new(StdMutex::new(PartTee {
            part: Hasher::new(),
            chain,
            size: 0,
            client_failed: None,
        }));
        let seen = Arc::clone(&tee);
        let body = blob.0.map(move |chunk| {
            let mut tee = seen.lock().unwrap_or_else(PoisonError::into_inner);
            match &chunk {
                Ok(bytes) => {
                    tee.part.update(bytes);
                    if let Some(chain) = tee.chain.as_mut() {
                        chain.update(bytes);
                    }
                    tee.size += bytes.len() as u64;
                }
                Err(error) => tee.client_failed = Some(error.to_string()),
            }
            chunk
        });
        let sent = async {
            let native = self.native_upload(&upload.location)?;
            let path = upload.location.get_storage_path()?;
            native
                .upload_part(
                    &path,
                    &upload.upload_id,
                    part_number,
                    size,
                    BackendStream(Box::pin(body)),
                )
                .await
        }
        .await;
        let tee = std::mem::replace(
            &mut *tee.lock().unwrap_or_else(PoisonError::into_inner),
            PartTee {
                part: Hasher::new(),
                chain: None,
                size: 0,
                client_failed: None,
            },
        );
        let backend_etag = match (sent, tee.client_failed) {
            (Ok(etag), None) if tee.size == size => etag,
            (sent, failed) => {
                self.with_chain(&upload.upload_id, |chain| {
                    chain.abandon(part_number, attempt);
                });
                return BlobEvent::Error(match (sent, failed) {
                    (_, Some(message)) => BlobError::StreamFailed(message),
                    (Err(error), None) => error,
                    (Ok(_), None) => BlobError::StreamFailed(
                        "body size did not match Content-Length".to_string(),
                    ),
                });
            }
        };
        if let Some(state) = tee.chain {
            let part = PartAttempt {
                part_number,
                attempt,
                size,
            };
            self.with_chain(&upload.upload_id, |chain| chain.finish(part, state));
        }
        BlobEvent::PartWritten {
            location: BackendLocation {
                ulid: attempt,
                created_by,
                created_at: SystemTime::now(),
                partial: true,
                blob_size: size,
                hashes: tee.part.to_map(),
                ..upload.location
            },
            backend_etag,
        }
    }

    fn with_chain(&self, upload_id: &str, change: impl FnOnce(&mut PartChain)) {
        if let Some(chain) = self.chains().get_mut(upload_id) {
            change(chain);
        }
    }

    /// Assembles the listed parts at the provider, then hashes only what the saved states miss.
    pub(super) async fn complete_upload(
        &self,
        upload: BackendUpload,
        parts: Vec<MultipartPart>,
    ) -> BlobEvent {
        let Some(mut reservation) = self.hold_reservation(upload.location.ulid) else {
            return BlobEvent::Error(BlobError::WriteError(
                "too many active blob reservations".to_string(),
            ));
        };
        match self.finish_upload(&upload, &parts).await {
            Ok(location) => {
                self.chains().remove(&upload.upload_id);
                reservation.retain();
                // The object is the only copy of the parts: its row keeps it for a retry.
                match self.write_target_row(&location, upload.record_id).await {
                    Ok(()) => BlobEvent::WriteFinished { location },
                    Err(error) => BlobEvent::Error(error),
                }
            }
            Err(error) => BlobEvent::Error(error),
        }
    }

    async fn finish_upload(
        &self,
        upload: &BackendUpload,
        parts: &[MultipartPart],
    ) -> Result<BackendLocation, BlobError> {
        let mut listed = Vec::with_capacity(parts.len());
        let mut etags = Vec::with_capacity(parts.len());
        for part in parts {
            let Some(etag) = part.backend_etag.clone().filter(|_| part.location.partial) else {
                return Err(BlobError::WriteError(format!(
                    "part {} was not written into the provider upload",
                    part.part_number
                )));
            };
            etags.push((part.part_number, etag));
            listed.push(PartAttempt {
                part_number: part.part_number,
                attempt: part.location.ulid,
                size: part.location.blob_size,
            });
        }
        let total: u64 = listed.iter().map(|part| part.size).sum();
        let native = self.native_upload(&upload.location)?;
        let path = upload.location.get_storage_path()?;
        let operator = self.operator_from_location(&upload.location)?;
        if let Err(error) = native.complete(&path, &upload.upload_id, &etags).await {
            // A completion whose answer was lost leaves the object. Only the ETag of exactly
            // these parts proves it is this selection, not an earlier one of the same size.
            let expected = multipart_etag(&etags);
            match timeout(self.io_timeout(), operator.stat(&path)).await {
                Ok(Ok(metadata))
                    if metadata.content_length() == total
                        && expected.is_some()
                        && metadata.etag().map(|etag| etag.trim_matches('"'))
                            == expected.as_deref() => {}
                _ => return Err(error),
            }
        }

        let (mut state, offset, _) = self
            .chains()
            .get(&upload.upload_id)
            .map_or_else(|| (Hasher::new(), 0, 0), |chain| chain.resume(&listed));
        let mut hashed = offset;
        if offset < total {
            let reader = timeout(self.io_timeout(), operator.reader(&path))
                .await
                .map_err(|_| BlobError::ReadError("timed out opening the object".to_string()))?
                .map_err(|error| BlobError::ReadError(error.to_string()))?;
            let mut stream = timeout(self.io_timeout(), reader.into_bytes_stream(offset..))
                .await
                .map_err(|_| BlobError::ReadError("timed out reading the object".to_string()))?
                .map_err(|error| BlobError::ReadError(error.to_string()))?;
            loop {
                let next = timeout(self.transfer_idle_timeout(), stream.next())
                    .await
                    .map_err(|_| BlobError::ReadError("object read idle timeout".to_string()))?;
                let Some(chunk) = next else {
                    break;
                };
                let bytes = chunk.map_err(|error| BlobError::ReadError(error.to_string()))?;
                state.update(&bytes);
                hashed += bytes.len() as u64;
            }
        }
        if hashed != total {
            return Err(BlobError::IntegrityCheckFailed(format!(
                "the assembled object holds {hashed} bytes, the parts {total}"
            )));
        }
        Ok(BackendLocation {
            blob_size: total,
            hashes: state.to_map(),
            ..upload.location.clone()
        })
    }

    /// Frees the provider's parts and any object a lost completion left at the target.
    pub(super) async fn abort_upload(&self, upload: BackendUpload) -> BlobEvent {
        let aborted = async {
            let native = self.native_upload(&upload.location)?;
            let path = upload.location.get_storage_path()?;
            timeout(self.io_timeout(), native.abort(&path, &upload.upload_id))
                .await
                .map_err(|_| BlobError::DeleteError("timed out aborting the upload".into()))??;
            let operator = self.operator_from_location(&upload.location)?;
            self.delete_path(&operator, &path).await?;
            self.release_reservation(&upload.location).await
        }
        .await;
        match aborted {
            Ok(()) => {
                self.chains().remove(&upload.upload_id);
                BlobEvent::UploadAborted
            }
            Err(error) => BlobEvent::Error(error),
        }
    }
}
