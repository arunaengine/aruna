//! Writes objects to fixed paths of a tenant's folders for backend imports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::{key_fits, split_bucket};
use super::io::{backend_version, compose_chunk};
use crate::s3::NativeMultipart;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle as _;
use aruna_core::keyspaces::{BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::backend_import::{
    COPY_PART, PlainPath, copy_provable, tagged_path, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, ResolvedBackend,
};
use aruna_core::structs::storage::format::{Compression, StoredFormat, StoredLayout};
use bytes::Bytes;
use futures::StreamExt;
use opendal::{ErrorKind, Operator};
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::time::timeout;

/// Without conditional writes a stat proves nothing, so a write that must find its path
/// free goes to its tagged path, chosen before any byte is read; an exact one is refused.
pub(super) fn conditional(
    operator: &Operator,
    location: &mut BackendLocation,
    plain: &PlainPath,
) -> Result<bool, BlobError> {
    if operator.info().capability().write_with_if_not_exists || !plain.if_not_exists {
        return Ok(plain.if_not_exists);
    }
    unconditional(location, plain).map(|()| false)
}

/// Sends a write that must find its path free, on a storage that cannot check that, to its
/// tagged path; an exact one is refused.
fn unconditional(location: &mut BackendLocation, plain: &PlainPath) -> Result<(), BlobError> {
    if plain.exact {
        return Err(BlobError::ConditionalUnsupported);
    }
    if !plain.path.ends_with(&format!("~{}", plain.ulid)) {
        location.backend_path = tagged_path(&plain.path, plain.ulid);
    }
    Ok(())
}

/// Bytes one provider copy request takes at most.
const COPY_WHOLE: u64 = 5 << 30;

/// The reservation a write to a tenant path holds: the client that made it and the version it
/// names.
pub(super) type Held = (NativeMultipart, String);

/// The source of a provider copy: its client, its pinned version, the ETag it must still have
/// without one, and the ETag recorded for it.
type Source<'a> = (
    &'a NativeMultipart,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

impl BlobHandler {
    /// The location of `path` on `backend`; the empty storage bucket marks a tenant path.
    fn plain_location(
        &self,
        backend: BackendRef,
        storage_class: Option<String>,
        plain: &PlainPath,
        created_by: UserId,
    ) -> Result<BackendLocation, BlobError> {
        let root = self.registry.config_for(&backend)?.root.clone();
        Ok(BackendLocation {
            backend,
            storage_class,
            root,
            storage_bucket: String::new(),
            backend_path: plain.path.clone(),
            ulid: plain.ulid,
            format: StoredFormat {
                layout: StoredLayout::Plain(plain.ulid),
                ..StoredFormat::default()
            },
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        })
    }

    /// No cleanup marker is written: nothing ever deletes bytes at a tenant path.
    pub(super) async fn write_plain(
        &self,
        resolved: ResolvedBackend,
        plain: PlainPath,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let class = resolved.storage_class.clone();
        let mut location = match self.plain_location(resolved.backend, class, &plain, created_by) {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        let operator = match self.operator_from_location(&location) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        if plain.path.ends_with('/') {
            return self.write_folder(location, operator, blob).await;
        }
        let (conditional, reserved) =
            match self.reserve_path(&operator, &mut location, &plain).await {
                Ok(reserved) => reserved,
                Err(error) => return BlobEvent::Error(error),
            };
        let target = location.clone();
        let condition = (conditional, reserved.clone());
        let event = Box::pin(self.write_tenant(location, operator, blob, condition)).await;
        if matches!(event, BlobEvent::Error(_)) {
            self.unreserve(reserved.as_ref(), &target).await;
        }
        event
    }

    /// Holds the free path of a create-only write before any byte reaches it: on S3 with an
    /// empty reservation for its version, as a storage may ignore the condition of a completion
    /// or a copy. Answers whether the write must still be create-only, and the reservation.
    async fn reserve_path(
        &self,
        operator: &Operator,
        location: &mut BackendLocation,
        plain: &PlainPath,
    ) -> Result<(bool, Option<Held>), BlobError> {
        if !conditional(operator, location, plain)? {
            return Ok((false, None));
        }
        let Some(native) = self.native_for(location)? else {
            return Ok((true, None));
        };
        let (path, version) = (self.object_path(location)?, plain.version_id.to_string());
        match native.reserve(&path, &version).await {
            Ok(()) => Ok((false, Some((native, version)))),
            Err(BlobError::ConditionalUnsupported) => {
                unconditional(location, plain).map(|()| (false, None))
            }
            Err(error) => Err(error),
        }
    }

    /// Removes a failed write's own empty reservation where the storage keeps versions. One left
    /// behind keeps the path in progress through the write's intent, and a retry reuses it.
    async fn unreserve(&self, held: Option<&Held>, location: &BackendLocation) {
        let (Some((native, version)), Ok(path)) = (held, self.object_path(location)) else {
            return;
        };
        if let Err(error) = native.unreserve(&path, version).await {
            tracing::warn!(%error, "The reservation of a failed write stays");
        }
    }

    /// Copies `source` to `plain.path`: inside the provider where both lie on one S3 storage
    /// and the source has a recorded version or `etag`, or fits one part with a known MD5,
    /// else through the node.
    pub(super) async fn copy_plain(
        &self,
        resolved: ResolvedBackend,
        plain: PlainPath,
        source: BackendLocation,
        etag: Option<String>,
    ) -> BlobEvent {
        // Without a version, ETag or MD5 the node streams the bytes; the caller checks their hash.
        let exact = match self.read_version(&source).await {
            Ok(version) => version,
            Err(error) => return BlobEvent::Error(error),
        };
        let inside = resolved.backend == source.backend && !is_folder(&source);
        let proved = exact.is_some() || etag.is_some() || copy_provable(&source);
        let native = match inside && proved {
            true => self.native_for(&source).ok().flatten(),
            false => None,
        };
        if let Some(native) = native {
            // Without a version the provider copies the source only while it keeps its ETag.
            let if_match = etag.as_deref().filter(|_| exact.is_none());
            let from = (&native, exact.as_deref(), if_match, etag.as_deref());
            let copied = self.copy_inside(resolved.clone(), &plain, &source, from);
            match Box::pin(copied).await {
                Ok(Some(event)) => return event,
                Ok(None) => {}
                Err(error) => return BlobEvent::Error(error),
            }
        }
        match self.read_blob(source.clone()).await {
            BlobEvent::ReadFinished { blob, .. } => {
                let created_by = source.created_by;
                Box::pin(self.write_plain(resolved, plain, created_by, blob)).await
            }
            other => other,
        }
    }

    /// The provider copies the source version, or its current bytes, onto the reserved path: in
    /// one request up to `COPY_WHOLE` bytes, else in parts. `None` asks for a streamed copy; a
    /// failed copy drops its own empty reservation.
    async fn copy_inside(
        &self,
        resolved: ResolvedBackend,
        plain: &PlainPath,
        source: &BackendLocation,
        (native, version, if_match, recorded): Source<'_>,
    ) -> Result<Option<BlobEvent>, BlobError> {
        let class = resolved.storage_class.clone();
        let mut location =
            self.plain_location(resolved.backend, class, plain, source.created_by)?;
        let operator = self.operator_from_location(&location)?;
        // The upload an interrupted try of this copy recorded goes first, and only that one.
        self.settle_upload(plain.ulid).await?;
        let (_, reserved) = self.reserve_path(&operator, &mut location, plain).await?;
        let target = location.clone();
        // Only a source pinned by its version or ETag is copied in one request; any other is
        // proved by its part's ETag before its copy replaces the reservation.
        let pinned = version.is_some() || if_match.is_some();
        let copied = match pinned && source.blob_size <= COPY_WHOLE {
            true => {
                let from = (native, version, if_match, recorded);
                Box::pin(self.copy_whole((location, &operator), source, from)).await
            }
            false => {
                let from = (native, version, if_match);
                let held = reserved.as_ref();
                let copied = self.copy_parts((location, &operator), (plain, held), source, from);
                Box::pin(copied).await
            }
        };
        if copied.is_err() {
            self.unreserve(reserved.as_ref(), &target).await;
        }
        copied
    }

    /// Copies `source` onto the reserved path in one request. An ETag equal to the source MD5
    /// proves the bytes; a pinned source's kept multipart or recorded ETag leaves the size to
    /// check, and any other ETag sends the bytes through the node onto the same path.
    async fn copy_whole(
        &self,
        (mut location, operator): (BackendLocation, &Operator),
        source: &BackendLocation,
        (native, version, if_match, recorded): Source<'_>,
    ) -> Result<Option<BlobEvent>, BlobError> {
        let target = self.native_upload(&location)?;
        let (path, source_path) = (self.object_path(&location)?, self.object_path(source)?);
        let from = (native, source_path.as_str(), version, if_match);
        // Accepted window: a foreign write after the reservation is replaced by this copy.
        let (written, etag) = match target.copy_object(&path, from).await {
            Ok(copied) => copied,
            Err(BlobError::CopyUnsupported) => return Ok(None),
            Err(error) => return Err(error),
        };
        let unquoted = |etag: &str| etag.trim_matches('"').to_string();
        let tag = etag.as_deref().map(unquoted);
        let md5 = (source.hashes.get(aruna_core::structs::checksum::HASH_MD5)).map(hex::encode);
        let proved = md5.is_some() && tag == md5;
        // Only a source pinned by its version or ETag makes a kept ETag name these bytes.
        let pinned = version.is_some() || if_match.is_some();
        let kept = pinned
            && (tag.as_deref()).is_some_and(|tag| {
                tag.contains('-') || recorded.map(unquoted).as_deref() == Some(tag)
            });
        if md5.is_some() && !proved && !kept {
            tracing::warn!("A copy's ETag is not the source MD5; the bytes go through the node");
            // Accepted window: a foreign write while the bytes stream is replaced by this write.
            return match self.read_blob(source.clone()).await {
                BlobEvent::ReadFinished { blob, .. } => {
                    let written =
                        self.write_tenant(location, operator.clone(), blob, (false, None));
                    Ok(Some(Box::pin(written).await))
                }
                other => Ok(Some(other)),
            };
        }
        let versioned = operator.info().capability().read_with_version;
        let written = written.filter(|version| versioned && version != "null");
        if !proved {
            let size = source.blob_size;
            self.check_size(operator, &path, written.as_deref(), size)
                .await?;
        }
        location.blob_size = source.blob_size;
        location.hashes = source.hashes.clone();
        Ok(Some(BlobEvent::PlainWritten {
            location,
            version: written,
            etag: tag,
        }))
    }

    /// The provider copies the source in parts of at most `COPY_PART` bytes and completes while
    /// the path still holds the copy's reservation, and the size is checked. `None` asks for a
    /// streamed copy: the storage cannot copy, or a one-part ETag is not the MD5.
    async fn copy_parts(
        &self,
        (mut location, operator): (BackendLocation, &Operator),
        (plain, held): (&PlainPath, Option<&Held>),
        source: &BackendLocation,
        (native, version, if_match): (&NativeMultipart, Option<&str>, Option<&str>),
    ) -> Result<Option<BlobEvent>, BlobError> {
        let target = self.native_upload(&location)?;
        let (path, source_path) = (self.object_path(&location)?, self.object_path(source)?);
        let size = source.blob_size;
        let ranges = copy_ranges(size);
        let upload_id = target.create(&path).await?;
        // The upload is on disk before any part moves, so a crash leaves it reconcilable.
        if let Err(error) = self
            .record_upload(plain.ulid, Some((&location, &upload_id)))
            .await
        {
            let _ = target.abort(&path, &upload_id).await;
            return Err(error);
        }
        let mut parts = Vec::with_capacity(ranges.len());
        for (index, range) in ranges.iter().enumerate() {
            let Ok(number) = u16::try_from(index + 1) else {
                return self.settle(target, (&path, &upload_id), plain.ulid).await;
            };
            let from = (native, source_path.as_str(), version, if_match);
            match target
                .copy_range(&path, &upload_id, (number, *range), from)
                .await
            {
                Ok(etag) => {
                    parts.push((number, etag));
                    // Each copied part renews the record, so a copy that progresses is never
                    // taken for a leftover.
                    let renewed = self.record_upload(plain.ulid, Some((&location, &upload_id)));
                    if let Err(error) = renewed.await {
                        let _ = target.abort(&path, &upload_id).await;
                        return Err(error);
                    }
                }
                // Only a storage that cannot copy at all gets the bytes through the node.
                Err(BlobError::CopyUnsupported) => {
                    tracing::warn!("The storage cannot copy inside itself; the bytes are streamed");
                    return self.settle(target, (&path, &upload_id), plain.ulid).await;
                }
                // Any other refusal, like a changed source or a busy storage, is retried later.
                Err(error) => {
                    self.settle(target, (&path, &upload_id), plain.ulid).await?;
                    return Err(error);
                }
            }
        }
        let md5 = source.hashes.get(aruna_core::structs::checksum::HASH_MD5);
        if let ([(_, etag)], Some(md5)) = (parts.as_slice(), md5)
            && etag.trim_matches('"') != hex::encode(md5)
        {
            return self.settle(target, (&path, &upload_id), plain.ulid).await;
        }
        // One part whose ETag is the source MD5 proves the bytes, and so their size.
        let proved = parts.len() == 1 && md5.is_some();
        // A create-only copy holds its path by its reservation, which must still be its own.
        // Accepted window: a foreign write after this check is replaced by the completion.
        if let Some((native, version)) = held {
            let holds = native.holds(&path, version).await;
            if !matches!(holds, Ok(true)) {
                self.settle(target, (&path, &upload_id), plain.ulid).await?;
                return Err(holds.err().unwrap_or(BlobError::PathTaken));
            }
        }
        let completed = target.complete(&path, &upload_id, &parts, false).await;
        let (written, etag) = match completed {
            Ok(completed) => completed,
            Err(error) => {
                self.settle(target, (&path, &upload_id), plain.ulid).await?;
                return Err(error);
            }
        };
        self.record_upload(plain.ulid, None).await?;
        let versioned = operator.info().capability().read_with_version;
        let written = written.filter(|version| versioned && version != "null");
        location.blob_size = size;
        location.hashes = source.hashes.clone();
        let event = BlobEvent::PlainWritten {
            location,
            version: written.clone(),
            etag: etag.map(|etag| etag.trim_matches('"').to_string()),
        };
        if !proved {
            self.check_size(operator, &path, written.as_deref(), size)
                .await?;
        }
        Ok(Some(event))
    }

    /// Fails unless the copy at `path`, at `version` where the storage reads versions, holds
    /// `size` bytes.
    async fn check_size(
        &self,
        operator: &Operator,
        path: &str,
        version: Option<&str>,
        size: u64,
    ) -> Result<(), BlobError> {
        let stat = operator.stat_with(path);
        let stat = match version {
            Some(version) if operator.info().capability().stat_with_version => {
                stat.version(version)
            }
            _ => stat,
        };
        let found = timeout(self.io_timeout(), stat.into_future())
            .await
            .map_err(|_| BlobError::ReadError("timed out reading the copy".to_string()))?
            .map_err(|error| BlobError::ReadError(error.to_string()))?;
        if found.content_length() != size {
            return Err(BlobError::IntegrityCheckFailed(format!(
                "the copy holds {} bytes, the source {size}",
                found.content_length()
            )));
        }
        Ok(())
    }

    /// Aborts this copy's own provider upload and forgets it, then asks for a streamed copy. A
    /// failed abort keeps the record, so a later try aborts it before anything else.
    async fn settle(
        &self,
        target: crate::s3::NativeMultipart,
        (path, upload_id): (&str, &str),
        attempt: ulid::Ulid,
    ) -> Result<Option<BlobEvent>, BlobError> {
        target.abort(path, upload_id).await?;
        self.record_upload(attempt, None).await?;
        Ok(None)
    }

    /// Aborts the provider upload copy `attempt` recorded, if any, and forgets it.
    pub(super) async fn settle_upload(&self, attempt: ulid::Ulid) -> Result<(), BlobError> {
        let Some(upload) = self.copy_upload(attempt).await? else {
            return Ok(());
        };
        self.settle_exact(attempt, upload).await
    }

    /// Aborts exactly `upload` and forgets the record of `attempt` only while it still names
    /// it, in one transaction, so a newer try of the attempt keeps its own record.
    async fn settle_exact(
        &self,
        attempt: ulid::Ulid,
        (location, upload_id): (BackendLocation, String),
    ) -> Result<(), BlobError> {
        let native = self.native_upload(&location)?;
        native
            .abort(&self.object_path(&location)?, &upload_id)
            .await?;
        let storage = &self.storage;
        let failed = |event: Event| BlobError::WriteError(format!("copy upload record: {event:?}"));
        let started = StorageEffect::StartTransaction { read: false };
        let txn_id = match storage.send_effect(Effect::Storage(started)).await {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            other => return Err(failed(other)),
        };
        let key: aruna_core::types::Key = attempt.to_bytes().to_vec().into();
        let read = StorageEffect::Read {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        };
        let current = match storage.send_effect(Effect::Storage(read)).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => return Err(failed(other)),
        };
        let same = current
            .as_deref()
            .map(BlobCleanupWork::from_bytes)
            .is_some_and(|work| {
                matches!(work, Ok(BlobCleanupWork::SettleUpload { upload_id: current, .. })
                if current == upload_id)
            });
        let effect = match same {
            true => StorageEffect::Delete {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            },
            false => StorageEffect::AbortTransaction { txn_id },
        };
        let answer = storage.send_effect(Effect::Storage(effect)).await;
        if !same {
            return Ok(());
        }
        if !matches!(answer, Event::Storage(StorageEvent::DeleteResult { .. })) {
            return Err(failed(answer));
        }
        let commit = StorageEffect::CommitTransaction { txn_id };
        match storage.send_effect(Effect::Storage(commit)).await {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            other => Err(failed(other)),
        }
    }

    /// Answers `SettleCopy`.
    pub(super) async fn settle_copy(
        &self,
        attempt: ulid::Ulid,
        upload: Option<(BackendLocation, String)>,
    ) -> BlobEvent {
        let settled = match upload {
            Some(upload) => self.settle_exact(attempt, upload).await,
            None => self.settle_upload(attempt).await,
        };
        match settled {
            Ok(()) => BlobEvent::UploadAborted,
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// The provider upload a copy attempt recorded: its target and upload id.
    pub(super) async fn copy_upload(
        &self,
        attempt: ulid::Ulid,
    ) -> Result<Option<(BackendLocation, String)>, BlobError> {
        let read = StorageEffect::Read {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: attempt.to_bytes().to_vec().into(),
            txn_id: None,
        };
        let value = match self.storage.send_effect(Effect::Storage(read)).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => {
                return Err(BlobError::ReadError(format!(
                    "failed to read the copy upload: {other:?}"
                )));
            }
        };
        let work = value
            .as_deref()
            .map(BlobCleanupWork::from_bytes)
            .transpose();
        match work.map_err(BlobError::ConversionError)? {
            Some(BlobCleanupWork::SettleUpload {
                location,
                upload_id,
                ..
            }) => Ok(Some((location, upload_id))),
            _ => Ok(None),
        }
    }

    /// Records a copy attempt's provider upload durably as cleanup work, which aborts it if
    /// the copy never settles, or forgets it with `None`.
    pub(super) async fn record_upload(
        &self,
        attempt: ulid::Ulid,
        upload: Option<(&BackendLocation, &str)>,
    ) -> Result<(), BlobError> {
        let key = attempt.to_bytes().to_vec().into();
        let space = BLOB_CLEANUP_KEYSPACE.to_string();
        let effect = match upload {
            Some((location, upload_id)) => {
                let since_ms = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as u64);
                let work = BlobCleanupWork::SettleUpload {
                    attempt,
                    location: location.clone(),
                    upload_id: upload_id.to_string(),
                    since_ms,
                };
                let value = work.to_bytes().map_err(BlobError::ConversionError)?;
                StorageEffect::Write {
                    key_space: space,
                    key,
                    value: value.into(),
                    txn_id: None,
                }
            }
            None => StorageEffect::Delete {
                key_space: space,
                key,
                txn_id: None,
            },
        };
        let stored = self.storage.send_effect(Effect::Storage(effect)).await;
        if !matches!(
            stored,
            Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. })
        ) {
            return Err(BlobError::WriteError(format!(
                "failed to record the copy upload: {stored:?}"
            )));
        }
        if upload.is_none() {
            return Ok(());
        }
        let synced = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::SyncAll))
            .await;
        match synced {
            Event::Storage(StorageEvent::SyncAllFinished) => Ok(()),
            other => Err(BlobError::WriteError(format!(
                "failed to sync the copy upload: {other:?}"
            ))),
        }
    }

    /// A key that ends in `/` is an empty folder object at that exact key.
    async fn write_folder(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        while let Some(chunk) = blob.next().await {
            match chunk {
                Ok(bytes) if bytes.is_empty() => {}
                Ok(_) => {
                    let message = "A key that ends in `/` cannot hold bytes in a provider bucket.";
                    return BlobEvent::Error(BlobError::WriteError(message.to_string()));
                }
                Err(error) => return BlobEvent::Error(BlobError::WriteError(error.to_string())),
            }
        }
        let path = match self.object_path(&location) {
            Ok(path) => path,
            Err(error) => return BlobEvent::Error(error),
        };
        let created = timeout(self.io_timeout(), operator.create_dir(&path)).await;
        match created {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return BlobEvent::Error(BlobError::WriteError(error.to_string())),
            Err(_) => {
                let message = "timed out writing the folder object".to_string();
                return BlobEvent::Error(BlobError::WriteError(message));
            }
        }
        location.blob_size = 0;
        location.hashes = crate::hash::Hasher::new().to_map();
        match location.is_external() {
            true => BlobEvent::PlainWritten {
                location,
                version: None,
                etag: None,
            },
            false => BlobEvent::WriteFinished { location },
        }
    }

    pub(super) async fn compose_plain(
        &self,
        resolved: ResolvedBackend,
        plain: PlainPath,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let class = resolved.storage_class.clone();
        let mut location = match self.plain_location(resolved.backend, class, &plain, created_by) {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        let (operator, backend_type) = match self
            .operator_from_location(&location)
            .and_then(|operator| Ok((operator, self.registry.config_for(&location.backend)?)))
        {
            Ok((operator, config)) => (operator, config.backend_type),
            Err(error) => return BlobEvent::Error(error),
        };
        let total = parts.iter().map(|part| part.blob_size).sum();
        let chunk = compose_chunk(&backend_type, total, false);
        let (conditional, reserved) =
            match self.reserve_path(&operator, &mut location, &plain).await {
                Ok(reserved) => reserved,
                Err(error) => return BlobEvent::Error(error),
            };
        let target = location.clone();
        let composed = self.compose_parts(
            location,
            operator,
            parts,
            chunk,
            Compression::Off,
            (conditional, reserved.as_ref()),
        );
        let event = Box::pin(composed).await;
        if matches!(event, BlobEvent::Error(_)) {
            self.unreserve(reserved.as_ref(), &target).await;
        }
        event
    }

    pub(super) async fn stat_plain(&self, backend: BackendRef, path: String) -> BlobEvent {
        let (bucket, path) = match self.registry.splits(&backend) {
            true => match split_bucket(&path) {
                Ok(parts) => parts,
                Err(error) => return BlobEvent::Error(error),
            },
            false => (String::new(), path),
        };
        // A key the provider would refuse is answered before any request.
        let fits = self
            .registry
            .config_for(&backend)
            .and_then(|config| key_fits(&config.root, &path));
        if let Err(error) = fits {
            return BlobEvent::Error(error);
        }
        let operator = match self
            .registry
            .bucket_operator(&backend, &bucket, &self.egress)
        {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        match timeout(self.io_timeout(), operator.stat(&path)).await {
            Ok(Ok(metadata)) => BlobEvent::PlainStat {
                exists: true,
                version: backend_version(&operator, &metadata),
            },
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => BlobEvent::PlainStat {
                exists: false,
                version: None,
            },
            Ok(Err(error)) => BlobEvent::Error(BlobError::ReadError(error.to_string())),
            Err(_) => BlobEvent::Error(BlobError::ReadError("timed out reading the path".into())),
        }
    }

    /// Answers `DropReservation`; a storage without provider calls holds no reservations.
    pub(super) async fn drop_reservation(
        &self,
        backend: BackendRef,
        path: String,
        version_id: ulid::Ulid,
    ) -> BlobEvent {
        let plain = PlainPath {
            path,
            ulid: version_id,
            version_id,
            if_not_exists: false,
            exact: false,
        };
        let dropped = async {
            let location = self.plain_location(backend, None, &plain, UserId::default())?;
            let Some(native) = self.native_for(&location)? else {
                return Ok(());
            };
            let path = self.object_path(&location)?;
            native.unreserve(&path, &version_id.to_string()).await
        };
        match dropped.await {
            Ok(()) => BlobEvent::DeleteFinished,
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// Whether the backend still has the object of `location`, at the backend version reads use.
    pub(super) async fn stat_copy(&self, location: BackendLocation) -> BlobEvent {
        let (operator, path) = match self
            .operator_from_location(&location)
            .and_then(|operator| Ok((operator, self.object_path(&location)?)))
        {
            Ok(found) => found,
            Err(error) => return BlobEvent::Error(error),
        };
        let version = match self.read_version(&location).await {
            Ok(version) => version,
            Err(error) => return BlobEvent::Error(error),
        };
        let stat = operator.stat_with(&path);
        let stat = match version.filter(|_| operator.info().capability().stat_with_version) {
            Some(version) => stat.version(&version),
            None => stat,
        };
        match timeout(self.io_timeout(), stat.into_future()).await {
            Ok(Ok(_)) => BlobEvent::CopyStat { exists: true },
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                BlobEvent::CopyStat { exists: false }
            }
            Ok(Err(error)) => BlobEvent::Error(BlobError::ReadError(error.to_string())),
            Err(_) => BlobEvent::Error(BlobError::ReadError("timed out reading the path".into())),
        }
    }

    /// Only an explicit rename deletes a tenant file, after its version names the new one, and
    /// only the exact backend version it read; a later write at the path stays. An empty
    /// version deletes a file of an unversioned storage whose size and ETag still match.
    pub(super) async fn delete_plain(
        &self,
        location: BackendLocation,
        version: String,
        etag: Option<String>,
    ) -> BlobEvent {
        if !location.storage_bucket.is_empty() {
            return BlobEvent::Error(BlobError::DeleteError("not a plain path".to_string()));
        }
        // "null" names whatever the path holds now, so it never selects the bytes to delete.
        if version == "null" {
            let reason = "an unversioned path names no exact bytes".to_string();
            return BlobEvent::Error(BlobError::DeleteError(reason));
        }
        let (operator, path) = match self
            .operator_from_location(&location)
            .and_then(|operator| Ok((operator, self.object_path(&location)?)))
        {
            Ok(found) => found,
            Err(error) => return BlobEvent::Error(error),
        };
        if version.is_empty() {
            return (self.delete_unversioned(&operator, &path, &location, etag)).await;
        }
        if !operator.info().capability().delete_with_version {
            return BlobEvent::Error(BlobError::CleanupUnsupported);
        }
        let delete = operator.delete_with(&path).version(&version);
        match timeout(self.io_timeout(), delete.into_future()).await {
            Ok(Ok(())) => BlobEvent::DeleteFinished,
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => BlobEvent::DeleteFinished,
            Ok(Err(error)) => BlobEvent::Error(BlobError::DeleteError(error.to_string())),
            Err(_) => BlobEvent::Error(BlobError::DeleteError("timed out deleting".into())),
        }
    }

    /// Deletes the file at `path` only while its size and full ETag equal the recorded ones;
    /// without a recorded ETag the file stays.
    async fn delete_unversioned(
        &self,
        operator: &Operator,
        path: &str,
        location: &BackendLocation,
        etag: Option<String>,
    ) -> BlobEvent {
        let Some(recorded) = etag else {
            return BlobEvent::Error(BlobError::CleanupUnsupported);
        };
        let metadata = match timeout(self.io_timeout(), operator.stat(path)).await {
            Ok(Ok(metadata)) => metadata,
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                return BlobEvent::DeleteFinished;
            }
            Ok(Err(error)) => return BlobEvent::Error(BlobError::DeleteError(error.to_string())),
            Err(_) => return BlobEvent::Error(BlobError::DeleteError("timed out reading".into())),
        };
        let unquoted = |etag: &str| etag.trim_matches('"').to_string();
        let same = metadata.etag().map(unquoted) == Some(unquoted(&recorded));
        if metadata.content_length() != location.blob_size || !same {
            return BlobEvent::Error(BlobError::CleanupUnsupported);
        }
        // The delete itself names the ETag where the storage can; elsewhere a write between
        // the check and the delete is the accepted remaining risk.
        let guarded = operator.info().capability().delete_with_if_match;
        let unsupported = || opendal::Error::new(ErrorKind::Unsupported, "no conditional delete");
        let delete = match guarded {
            true => {
                let delete = operator.delete_with(path).if_match(&recorded);
                timeout(self.io_timeout(), delete.into_future()).await
            }
            false => Ok(Err(unsupported())),
        };
        let delete = match delete {
            Ok(Err(error)) if conditional_unsupported(&error) => {
                timeout(self.io_timeout(), operator.delete(path)).await
            }
            delete => delete,
        };
        match delete {
            Ok(Ok(())) => BlobEvent::DeleteFinished,
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => BlobEvent::DeleteFinished,
            // A failed condition means other bytes, or none, at the path now.
            Ok(Err(error)) if error.kind() == ErrorKind::ConditionNotMatch => {
                match timeout(self.io_timeout(), operator.stat(path)).await {
                    Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                        BlobEvent::DeleteFinished
                    }
                    _ => BlobEvent::Error(BlobError::CleanupUnsupported),
                }
            }
            Ok(Err(error)) => BlobEvent::Error(BlobError::DeleteError(error.to_string())),
            Err(_) => BlobEvent::Error(BlobError::DeleteError("timed out deleting".into())),
        }
    }

    /// The backend version a write recorded for a tenant path; `None`, also for an unversioned
    /// "null" one, reads the newest bytes.
    pub(super) async fn read_version(
        &self,
        location: &BackendLocation,
    ) -> Result<Option<String>, BlobError> {
        if !location.is_external() {
            return Ok(None);
        }
        let read = StorageEffect::Read {
            key_space: BACKEND_VERSION_KEYSPACE.to_string(),
            key: version_key(location.ulid),
            txn_id: None,
        };
        match self.storage.send_effect(Effect::Storage(read)).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .map(|value| String::from_utf8(value.to_vec()))
                .transpose()
                .map(|version| version.filter(|version| version != "null"))
                .map_err(|error| BlobError::ReadError(error.to_string())),
            other => Err(BlobError::ReadError(format!(
                "failed to read the backend version: {other:?}"
            ))),
        }
    }
}

/// Whether a storage refused a conditional delete as not implemented, which OpenDAL may also
/// report as an unexpected 501 answer; a failed condition is never such a refusal.
fn conditional_unsupported(error: &opendal::Error) -> bool {
    match error.kind() {
        ErrorKind::Unsupported => true,
        ErrorKind::Unexpected => error
            .message()
            .starts_with("S3Error { code: \"NotImplemented\""),
        _ => false,
    }
}

/// The inclusive byte ranges of a copy; `None` copies an object of one part whole.
fn copy_ranges(size: u64) -> Vec<Option<(u64, u64)>> {
    if size <= COPY_PART {
        return vec![None];
    }
    (0..size.div_ceil(COPY_PART))
        .map(|part| {
            let first = part * COPY_PART;
            Some((first, (first + COPY_PART).min(size) - 1))
        })
        .collect()
}

/// A folder object: its key ends in `/` and it holds no bytes.
pub(super) fn is_folder(location: &BackendLocation) -> bool {
    location.blob_size == 0 && location.backend_path.ends_with('/')
}

/// The empty read of a folder object.
pub(super) fn folder_read(location: &BackendLocation) -> Option<BlobEvent> {
    is_folder(location).then(|| BlobEvent::ReadFinished {
        blob: BackendStream::new(futures::stream::empty::<Result<Bytes, BlobError>>()),
        stream_size: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spots_unsupported_conditions() {
        let refused = |kind, message| conditional_unsupported(&opendal::Error::new(kind, message));
        assert!(refused(ErrorKind::Unsupported, "no conditional delete"));
        assert!(refused(
            ErrorKind::Unexpected,
            "S3Error { code: \"NotImplemented\" }"
        ));
        assert!(!refused(ErrorKind::ConditionNotMatch, "PreconditionFailed"));
        assert!(!refused(
            ErrorKind::Unexpected,
            "S3Error { code: \"AccessDenied\" }"
        ));
        let uri = opendal::Error::new(
            ErrorKind::Unexpected,
            "S3Error { code: \"InternalError\", message: \"\" }",
        )
        .with_context("uri", "https://s3/b/01M5G_NotImplementedError.py");
        assert!(!conditional_unsupported(&uri));
    }

    #[test]
    fn proves_unversioned_copies() {
        // Without a provider version or ETag only one part with a known MD5 proves its copy.
        let mut source = BackendLocation {
            backend: BackendRef::Group(ulid::Ulid::nil()),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "raw/a.faa.gz".to_string(),
            ulid: ulid::Ulid::nil(),
            format: StoredFormat::default(),
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: COPY_PART,
            hashes: HashMap::new(),
        };
        assert!(!copy_provable(&source));
        let md5 = aruna_core::structs::checksum::HASH_MD5.to_string();
        source.hashes.insert(md5, vec![0; 16]);
        assert!(copy_provable(&source));
        source.blob_size = COPY_PART + 1;
        assert!(!copy_provable(&source));
    }

    #[test]
    fn splits_large_copies() {
        assert_eq!(copy_ranges(0), vec![None]);
        assert_eq!(copy_ranges(COPY_PART), vec![None]);
        let ranges = copy_ranges(2 * COPY_PART + 5);
        assert_eq!(
            ranges,
            vec![
                Some((0, COPY_PART - 1)),
                Some((COPY_PART, 2 * COPY_PART - 1)),
                Some((2 * COPY_PART, 2 * COPY_PART + 4)),
            ]
        );
    }
}
