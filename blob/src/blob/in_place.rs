//! Streams multipart parts into the provider's own upload on S3 backends and completes it there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::{build_backend_path, intent_key};
use super::framed_parts::{Finished, part_level, streams_now};
use super::io::{backend_version, reader_at};
use crate::codec::Guard;
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
use aruna_core::structs::storage::backend_import::PlainPath;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, ResolvedBackend, WriteOwner,
};
use aruna_core::structs::storage::format::{Compression, PartLayout, StoredFormat, StoredLayout};
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

    pub(super) fn native_upload(
        &self,
        location: &BackendLocation,
    ) -> Result<NativeMultipart, BlobError> {
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
    /// A tenant path may hold other files and uploads, so only its row goes.
    pub(super) async fn discard_target(&self, location: &BackendLocation) -> Result<(), BlobError> {
        if location.is_external() {
            return self.release_reservation(location).await;
        }
        let path = self.object_path(location)?;
        let operator = self.operator_from_location(location)?;
        self.delete_path(&operator, &path).await?;
        Box::pin(self.abort_uploads(location, &path)).await?;
        self.release_reservation(location).await
    }

    /// Aborts the provider uploads under an in-place target and keeps any object there.
    pub(super) async fn abort_target(&self, location: &BackendLocation) -> Result<(), BlobError> {
        if location.is_external() {
            return Ok(());
        }
        let path = self.object_path(location)?;
        Box::pin(self.abort_uploads(location, &path)).await
    }

    pub(super) async fn open_upload(
        &self,
        record_id: Ulid,
        bucket: &str,
        key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
        plain: Option<PlainPath>,
    ) -> BlobEvent {
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(error) => return BlobEvent::Error(error),
        };
        // An upload under an import rule ends at its tenant path, which takes no reservation.
        let (ulid, backend_path) = match &plain {
            Some(plain) => (plain.ulid, plain.path.clone()),
            None => {
                let ulid = Ulid::generate();
                match build_backend_path(bucket, key, ulid) {
                    Ok(path) => (ulid, path),
                    Err(error) => return BlobEvent::Error(BlobError::ConversionError(error)),
                }
            }
        };
        let mut template = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class,
            root,
            storage_bucket: String::new(),
            backend_path,
            ulid,
            format: StoredFormat::default(),
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        // Only S3 backends have a provider upload; the others keep one blob per part. The parts
        // of a compressed upload record their frames in this layout; a tenant path keeps raw bytes.
        match (resolved.compression, &plain) {
            // A tenant path copy keeps its own location row, as a single plain write does.
            (_, Some(plain)) => template.format.layout = StoredLayout::Plain(plain.ulid),
            (Compression::Zstd { level }, None) => {
                template.format.layout = StoredLayout::FramedPart(Box::new(PartLayout {
                    level,
                    frames: Vec::new(),
                    padding: 0,
                }))
            }
            (Compression::Off, None) => {}
        }
        match self.native_for(&template) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return BlobEvent::UploadOpened {
                    backend_upload: None,
                };
            }
            Err(error) => return BlobEvent::Error(error),
        }
        let reserved = match plain {
            Some(_) => Ok(template),
            None => self.reserve_bucket(&resolved.backend, &template).await,
        };
        let location = match reserved {
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
            let path = self.object_path(&location)?;
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
        size: Option<u64>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        if part_level(&upload).is_some() && !streams_now(part.part_number, size) {
            return Box::pin(self.write_blob_part(part, resolved, created_by, blob)).await;
        }
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
            Ok(None) => Box::pin(self.write_blob_part(part, resolved, created_by, blob)).await,
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
        let body = BackendStream(Box::pin(body));
        let sent = match part_level(&upload) {
            Some(level) => {
                let framed = self.upload_framed(&upload, part_number, size, level, body);
                framed.await.map(|(etag, layout)| (etag, Some(layout)))
            }
            None => {
                async {
                    let native = self.native_upload(&upload.location)?;
                    let path = self.object_path(&upload.location)?;
                    native
                        .upload_part(&path, &upload.upload_id, part_number, size, body)
                        .await
                        .map(|etag| (etag, None))
                }
                .await
            }
        };
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
        let (backend_etag, layout) = match (sent, tee.client_failed) {
            (Ok(sent), None) if tee.size == size => sent,
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
        let mut location = BackendLocation {
            ulid: attempt,
            created_by,
            created_at: SystemTime::now(),
            partial: true,
            blob_size: size,
            hashes: tee.part.to_map(),
            ..upload.location
        };
        if let Some(layout) = layout {
            location.format.layout = StoredLayout::FramedPart(Box::new(layout));
        }
        BlobEvent::PartWritten {
            location,
            backend_etag,
        }
    }

    /// Aborts the provider upload again for a part that settled after the abort. A failure
    /// leaves durable work, so the cleanup drain retries it.
    async fn abort_again(&self, upload: &BackendUpload) {
        let aborted = async {
            let native = self.native_upload(&upload.location)?;
            let path = self.object_path(&upload.location)?;
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
        streamed: &[u16],
        compression: Compression,
        if_not_exists: bool,
    ) -> BlobEvent {
        let Some(mut reservation) = self.hold_reservation(upload.location.ulid) else {
            return BlobEvent::Error(BlobError::WriteError(
                "too many active blob reservations".to_string(),
            ));
        };
        let finished = self.finish_upload(&upload, &parts, streamed, compression, if_not_exists);
        match Box::pin(finished).await {
            Ok((location, version, etag)) => {
                self.chains().remove(&upload.upload_id);
                reservation.retain();
                // A composed copy keeps its own reservation; the target row discards the target.
                if !location.same_object(&upload.location) {
                    return BlobEvent::WriteFinished { location };
                }
                // The object is the only copy of the parts: its row keeps it for a retry.
                match self.write_target_row(&location, upload.record_id).await {
                    Ok(()) if location.is_external() => BlobEvent::PlainWritten {
                        location,
                        version,
                        etag,
                    },
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
        streamed: &[u16],
        compression: Compression,
        if_not_exists: bool,
    ) -> Result<(BackendLocation, Option<String>, Option<String>), BlobError> {
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
        let path = self.object_path(&upload.location)?;
        let operator = self.operator_from_location(&upload.location)?;
        let mut location = BackendLocation {
            blob_size: total,
            ..upload.location.clone()
        };
        // The completion's reservation also covers the hash read of the framed object.
        let mut share = None;
        let (mut version, mut etag) = (None, None);
        if let Some(level) = part_level(upload) {
            let finished = self.finish_framed(upload, parts, level, streamed, compression);
            match Box::pin(finished).await? {
                Finished::Framed(layout, held) => {
                    location.format.layout = StoredLayout::Frames(Box::new(layout));
                    share = Some(held);
                }
                Finished::Composed(composed) => return Ok((*composed, None, None)),
            }
        } else {
            // A tenant path is reserved first, as a storage may ignore the completion's
            // condition; the completion then replaces only that reservation. A taken path may
            // hold this upload's own completed object, which the recovery below proves.
            let reserved_for = upload.location.ulid.to_string();
            let reserved = match if_not_exists && upload.location.is_external() {
                true => native.reserve(&path, &reserved_for).await.map(|()| true),
                false => Ok(false),
            };
            let condition = match reserved {
                Ok(true) => Ok((false, Some(reserved_for.as_str()))),
                // A storage without conditional writes completes as it did before.
                Ok(false) | Err(BlobError::ConditionalUnsupported) => Ok((if_not_exists, None)),
                Err(error) => Err(error),
            };
            let assembled = match condition {
                Ok(condition) => {
                    let etags = etags.clone();
                    self.assemble(&native, &path, upload, parts, etags, condition)
                        .await
                }
                Err(error) => Err(error),
            };
            match assembled {
                Ok((id, tag)) => {
                    let versioned = operator.info().capability().read_with_version;
                    version = id.filter(|id| versioned && id != "null");
                    etag = tag;
                }
                Err(error) => {
                    // A completion whose answer was lost leaves the object. Only the ETag of
                    // exactly these parts proves it is this selection, not an earlier one.
                    let expected = multipart_etag(&etags);
                    match timeout(self.io_timeout(), operator.stat(&path)).await {
                        Ok(Ok(metadata))
                            if metadata.content_length() == total
                                && expected.is_some()
                                && metadata.etag().map(|etag| etag.trim_matches('"'))
                                    == expected.as_deref() =>
                        {
                            version = backend_version(&operator, &metadata);
                            etag = metadata.etag().map(str::to_string);
                        }
                        // A later write at a tenant path may have replaced it as newest bytes.
                        _ if upload.location.is_external() => {
                            let tag = expected.as_deref().unwrap_or_default();
                            let found = self.completed_version(&operator, &path, tag, total);
                            version = Some(found.await.ok_or(error)?);
                            etag = expected;
                        }
                        _ => return Err(error),
                    }
                }
            }
        }

        let (mut state, offset, _) = self.chains().get(&upload.upload_id).map_or_else(
            || (Hasher::new(), 0, 0),
            |state| state.chain.resume(&listed),
        );
        let mut hashed = offset;
        if offset < total {
            let mut stream = match &location.format.layout {
                StoredLayout::Frames(layout) => {
                    let guard = share.clone().map(|share| -> Guard { share });
                    self.frames_stream(&location, layout, offset..total, guard)
                        .await?
                }
                _ => {
                    let late = |what: &str| BlobError::ReadError(format!("timed out {what}"));
                    // The bytes of the completed version, not of a later write at a tenant path.
                    let opened = reader_at(&operator, &path, version.as_deref());
                    let reader = timeout(self.io_timeout(), opened)
                        .await
                        .map_err(|_| late("opening the object"))?
                        .map_err(|error| BlobError::ReadError(error.to_string()))?;
                    let stream = timeout(self.io_timeout(), reader.into_bytes_stream(offset..))
                        .await
                        .map_err(|_| late("reading the object"))?
                        .map_err(|error| BlobError::ReadError(error.to_string()))?;
                    BackendStream::new(stream)
                }
            };
            loop {
                let next = timeout(self.transfer_idle_timeout(), stream.0.next())
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
        location.hashes = state.to_map();
        drop(share);
        Ok((location, version, etag))
    }

    /// The provider version of `path` assembled from the parts with `etag`, found among all
    /// versions. `None` where the backend lists no versions.
    async fn completed_version(
        &self,
        operator: &opendal::Operator,
        path: &str,
        etag: &str,
        size: u64,
    ) -> Option<String> {
        let capability = operator.info().capability();
        if etag.is_empty() || !capability.list_with_versions || !capability.read_with_version {
            return None;
        }
        let listed = operator.list_with(path).versions(true);
        let entries = timeout(self.io_timeout(), listed).await.ok()?.ok()?;
        entries.into_iter().find_map(|entry| {
            let metadata = entry.metadata();
            let found = entry.path() == path
                && metadata.content_length() == size
                && metadata.etag().map(|tag| tag.trim_matches('"')) == Some(etag);
            found
                .then(|| metadata.version().map(str::to_string))
                .flatten()
        })
    }

    /// Copies staged replacements into the provider upload, then completes it while the path
    /// still holds the reservation of `held`, if one is held.
    async fn assemble(
        &self,
        native: &NativeMultipart,
        path: &str,
        upload: &BackendUpload,
        parts: &[MultipartPart],
        mut etags: Vec<(u16, String)>,
        (if_not_exists, held): (bool, Option<&str>),
    ) -> Result<(Option<String>, Option<String>), BlobError> {
        for (slot, part) in etags.iter_mut().zip(parts) {
            if part.location.partial {
                continue;
            }
            let source = self.native_upload(&part.location)?;
            let source_path = self.object_path(&part.location)?;
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
        if let Some(version) = held
            && !native.holds(path, version).await?
        {
            return Err(BlobError::PathTaken);
        }
        native
            .complete(path, &upload.upload_id, &etags, if_not_exists)
            .await
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
            let path = self.object_path(&upload.location)?;
            timeout(self.io_timeout(), native.abort(&path, &upload.upload_id))
                .await
                .map_err(|_| BlobError::DeleteError("timed out aborting the upload".into()))??;
            // A tenant path keeps what it holds but this upload's own reservation where the storage
            // keeps versions; only Aruna's own target is deleted.
            match upload.location.is_external() {
                true => (native.unreserve(&path, &upload.location.ulid.to_string())).await?,
                false => {
                    let operator = self.operator_from_location(&upload.location)?;
                    self.delete_path(&operator, &path).await?;
                }
            }
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
