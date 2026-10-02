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
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, ResolvedBackend, WriteOwner,
};
use aruna_core::structs::storage::multipart::{BackendUpload, MultipartPart, MultipartPartKey};
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

/// What this node knows about one provider upload: its saved hash states and part writes.
#[derive(Debug, Default)]
pub(super) struct UploadState {
    chain: PartChain,
    writes: usize,
    /// The attempt allowed to write each provider part until its operation settles.
    claims: HashMap<u16, Ulid>,
    /// Set by an abort; a part write that settles afterwards must abort again.
    aborted: bool,
}

impl BlobHandler {
    pub(super) fn chains(&self) -> std::sync::MutexGuard<'_, HashMap<String, UploadState>> {
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
        self.write_upload_row(intent_key(location), location, record_id)
            .await
    }

    /// Queues one more discard of the target under its own key, which no reservation release
    /// removes: the provider may still take late parts after an abort.
    async fn queue_discard(&self, upload: &BackendUpload) -> Result<(), BlobError> {
        let key = ByteView::from(Ulid::generate().to_bytes().to_vec());
        self.write_upload_row(key, &upload.location, upload.record_id)
            .await
    }

    async fn write_upload_row(
        &self,
        key: ByteView,
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
                key,
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
        Box::pin(self.abort_uploads(location, &path)).await?;
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
                _ = Box::pin(self.discard_target(&location)).await;
                BlobEvent::Error(error)
            }
        }
    }

    /// Streams one part into the provider upload, hashing it as it passes.
    /// Claims the provider part for `attempt`, with the hash state to feed. `None` means
    /// another attempt holds it: only one attempt at a time may write a provider part.
    pub(super) fn claim_part(
        &self,
        upload_id: &str,
        part_number: u16,
        attempt: Ulid,
    ) -> Result<Option<Option<Hasher>>, BlobError> {
        let mut uploads = self.chains();
        let state = uploads.entry(upload_id.to_string()).or_default();
        if state.aborted {
            return Err(aborted_upload());
        }
        if state.claims.contains_key(&part_number) {
            return Ok(None);
        }
        state.claims.insert(part_number, attempt);
        state.writes += 1;
        Ok(Some(state.chain.begin(part_number, attempt)))
    }

    /// Frees the provider part an attempt claimed. A committed attempt keeps its claim, so a
    /// request admitted before that commit cannot overwrite the acknowledged part later.
    pub(super) fn release_claim(&self, attempt: Ulid) {
        for state in self.chains().values_mut() {
            state.claims.retain(|_, held| *held != attempt);
        }
    }

    /// Writes a part of an in-place upload. While another attempt holds the provider part, this
    /// one waits in a blob of its own, which completion copies in if its record is accepted.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_part(
        &self,
        upload: BackendUpload,
        part: MultipartPartKey,
        resolved: ResolvedBackend,
        created_by: UserId,
        compressed: bool,
        encrypted: bool,
        size: Option<u64>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let attempt = Ulid::generate();
        match self.claim_part(&upload.upload_id, part.part_number, attempt) {
            Ok(Some(chain)) => {
                Box::pin(self.stream_part(
                    upload,
                    part.part_number,
                    attempt,
                    chain,
                    size,
                    created_by,
                    blob,
                ))
                .await
            }
            Ok(None) => {
                Box::pin(
                    self.write_blob_part(part, resolved, created_by, compressed, encrypted, blob),
                )
                .await
            }
            Err(error) => BlobEvent::Error(error),
        }
    }

    #[cfg(test)]
    pub(super) async fn write_upload_part(
        &self,
        upload: BackendUpload,
        part_number: u16,
        size: Option<u64>,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let attempt = Ulid::generate();
        match self.claim_part(&upload.upload_id, part_number, attempt) {
            Ok(Some(chain)) => {
                Box::pin(self.stream_part(
                    upload,
                    part_number,
                    attempt,
                    chain,
                    size,
                    created_by,
                    blob,
                ))
                .await
            }
            Ok(None) => BlobEvent::Error(BlobError::WriteError(
                "another write holds this provider part".to_string(),
            )),
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// Streams one claimed part into the provider upload, hashing it as it passes.
    #[allow(clippy::too_many_arguments)]
    async fn stream_part(
        &self,
        upload: BackendUpload,
        part_number: u16,
        attempt: Ulid,
        chain: Option<Hasher>,
        size: Option<u64>,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let write = PartWrite {
            handler: self.clone(),
            upload: upload.clone(),
            attempt,
            settled: false,
        };
        let Some(size) = size else {
            write.settle(false).await;
            return BlobEvent::Error(BlobError::WriteError(
                "an in-place part needs its Content-Length".to_string(),
            ));
        };
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
        let accepted = sent.is_ok() && tee.client_failed.is_none() && tee.size == size;
        if write.settle(accepted).await {
            return BlobEvent::Error(aborted_upload());
        }
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

    /// Aborts the provider upload again for a part that settled after the abort. A failure
    /// leaves durable work, so the cleanup drain retries it.
    async fn abort_again(&self, upload: &BackendUpload) {
        let aborted = async {
            let native = self.native_upload(&upload.location)?;
            let path = upload.location.get_storage_path()?;
            timeout(self.io_timeout(), native.abort(&path, &upload.upload_id))
                .await
                .map_err(|_| BlobError::DeleteError("timed out aborting the upload".into()))?
        }
        .await;
        if let Err(error) = aborted {
            tracing::warn!(%error, "Aborting a late part failed; queuing it for the cleanup drain");
            if let Err(error) = self.queue_discard(upload).await {
                tracing::error!(%error, "Failed to queue the abort of a late part");
            }
        }
    }

    /// Ends one part write; reports whether the upload was aborted meanwhile.
    fn settle_write(&self, upload_id: &str) -> bool {
        let mut uploads = self.chains();
        let Some(state) = uploads.get_mut(upload_id) else {
            return false;
        };
        state.writes = state.writes.saturating_sub(1);
        let aborted = state.aborted;
        if aborted && state.writes == 0 {
            uploads.remove(upload_id);
        }
        aborted
    }

    fn with_chain(&self, upload_id: &str, change: impl FnOnce(&mut PartChain)) {
        if let Some(state) = self.chains().get_mut(upload_id) {
            change(&mut state.chain);
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
        match Box::pin(self.finish_upload(&upload, &parts)).await {
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
            // A staged replacement gets the ETag of its bytes once completion copies it in.
            let etag = match (&part.backend_etag, part.location.partial) {
                (Some(etag), true) => etag.clone(),
                (_, false) => match part.location.hashes.get(HASH_MD5) {
                    Some(md5) => format!("\"{}\"", hex::encode(md5)),
                    None => {
                        return Err(BlobError::WriteError(format!(
                            "staged part {} has no MD5",
                            part.part_number
                        )));
                    }
                },
                (None, true) => {
                    return Err(BlobError::WriteError(format!(
                        "part {} has no provider ETag",
                        part.part_number
                    )));
                }
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
        if let Err(error) = self
            .assemble(&native, &path, upload, parts, etags.clone())
            .await
        {
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

        let (mut state, offset, _) = self.chains().get(&upload.upload_id).map_or_else(
            || (Hasher::new(), 0, 0),
            |state| state.chain.resume(&listed),
        );
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

    /// Copies staged replacements into the provider upload, then completes it.
    async fn assemble(
        &self,
        native: &NativeMultipart,
        path: &str,
        upload: &BackendUpload,
        parts: &[MultipartPart],
        mut etags: Vec<(u16, String)>,
    ) -> Result<(), BlobError> {
        for (slot, part) in etags.iter_mut().zip(parts) {
            if part.location.partial {
                continue;
            }
            let source = self.native_upload(&part.location)?;
            let source_path = part.location.get_storage_path()?;
            slot.1 = native
                .copy_part(
                    path,
                    &upload.upload_id,
                    part.part_number,
                    &source,
                    &source_path,
                )
                .await?;
        }
        native.complete(path, &upload.upload_id, &etags).await
    }

    /// Frees the provider's parts and any object a lost completion left at the target. Parts
    /// still streaming abort again when they settle, and the target row aborts once more later.
    pub(super) async fn abort_upload(&self, upload: BackendUpload) -> BlobEvent {
        self.chains()
            .entry(upload.upload_id.clone())
            .or_default()
            .aborted = true;
        let aborted = async {
            let native = self.native_upload(&upload.location)?;
            let path = upload.location.get_storage_path()?;
            timeout(self.io_timeout(), native.abort(&path, &upload.upload_id))
                .await
                .map_err(|_| BlobError::DeleteError("timed out aborting the upload".into()))??;
            let operator = self.operator_from_location(&upload.location)?;
            self.delete_path(&operator, &path).await?;
            self.release_reservation(&upload.location).await?;
            // After the release, which removes the reservation row, so this work stays queued.
            self.queue_discard(&upload).await
        }
        .await;
        let mut uploads = self.chains();
        match aborted {
            Ok(()) => {
                if uploads
                    .get(&upload.upload_id)
                    .is_some_and(|state| state.writes == 0)
                {
                    uploads.remove(&upload.upload_id);
                }
                BlobEvent::UploadAborted
            }
            Err(error) => {
                // The upload stays open for a retry, so its parts may stream again.
                if let Some(state) = uploads.get_mut(&upload.upload_id) {
                    state.aborted = false;
                }
                BlobEvent::Error(error)
            }
        }
    }
}

/// One part write in flight. Dropped unfinished, as when its request is cancelled, it still
/// settles, and after an abort it aborts the provider upload again.
struct PartWrite {
    handler: BlobHandler,
    upload: BackendUpload,
    attempt: Ulid,
    settled: bool,
}

impl PartWrite {
    /// Reports whether the upload was aborted while the part streamed. An accepted part keeps
    /// its claim, also after its commit, until its part is deleted; a failed one frees it now.
    async fn settle(mut self, accepted: bool) -> bool {
        self.settled = true;
        if !accepted {
            self.handler.release_claim(self.attempt);
        }
        let aborted = self.handler.settle_write(&self.upload.upload_id);
        if aborted {
            self.handler.abort_again(&self.upload).await;
        }
        aborted
    }
}

impl Drop for PartWrite {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        self.handler.release_claim(self.attempt);
        if !self.handler.settle_write(&self.upload.upload_id) {
            return;
        }
        let (handler, upload) = (self.handler.clone(), self.upload.clone());
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { handler.abort_again(&upload).await });
            }
            Err(_) => {
                tracing::error!("Cannot abort after a cancelled part write without a runtime")
            }
        }
    }
}

fn aborted_upload() -> BlobError {
    BlobError::WriteError("the multipart upload was aborted".to_string())
}
