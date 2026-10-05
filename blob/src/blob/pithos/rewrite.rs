//! Rewrites a stored copy into another format inside the adapter. The plaintext of a converted
//! copy goes from the old copy straight into the new writer, never to an operation or reader.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Opening, read_within, working_set};
use crate::blob::BlobHandler;
use crate::blob::backend::build_backend_path;
use crate::blob::frames::read_range;
use crate::blob::unlock::LeaseGuard;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::encryption::{BucketKeyError, BucketKeyRef, ReadLease};
use aruna_core::structs::storage::format::{PithosLayout, StoredFormat, StoredLayout};
use bytes::Bytes;
use futures::Stream;
use opendal::Operator;
use pithos_lib::archive::{AccessKeys, AsyncArchive, GrantReplacement, OpenOptions};
use pithos_lib::crypto::{PrivateKey, PublicKey};
use pithos_lib::error::PithosError;
use pithos_lib::source::{AsyncArchiveSource, SourceError};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::timeout;
use ulid::Ulid;
use zeroize::Zeroizing;

/// Bytes of the old archive copied per backend read during a grant replacement.
const COPY_CHUNK: u64 = 8 << 20;

/// Makes a sendable stream shareable: it is only polled through `&mut self`.
struct Exclusive<S>(Mutex<Pin<Box<S>>>);

impl<S: Stream> Stream for Exclusive<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<S::Item>> {
        match self.get_mut().0.get_mut() {
            Ok(stream) => stream.as_mut().poll_next(context),
            Err(_) => Poll::Ready(None),
        }
    }
}

/// The stored bytes of one archive, served to the Pithos reader by range.
pub(super) struct StoredBytes {
    pub(super) operator: Operator,
    pub(super) path: String,
    pub(super) idle: Duration,
}

impl AsyncArchiveSource for StoredBytes {
    async fn len(&self) -> Result<u64, SourceError> {
        let metadata = self.operator.stat(&self.path).await;
        metadata
            .map(|metadata| metadata.content_length())
            .map_err(|error| SourceError::Remote {
                offset: 0,
                message: error.to_string(),
            })
    }

    async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, SourceError> {
        let end = offset.saturating_add(len);
        let bytes = read_range(&self.operator, &self.path, offset..end, self.idle).await;
        bytes
            .map(|bytes| bytes.to_vec())
            .map_err(|error| SourceError::Remote {
                offset,
                message: error.to_string(),
            })
    }
}

impl BlobHandler {
    /// Writes `source` again in the format of `target` and answers the new copy, whose
    /// reservation stays held. `grants_only` keeps the sealed blocks and grants them to the
    /// target key; otherwise the copy is decoded and encoded again.
    pub(in crate::blob) async fn rewrite_copy(
        &self,
        bucket: &str,
        key: &str,
        source: BackendLocation,
        lease: Option<ReadLease>,
        target: ResolvedBackend,
        grants_only: bool,
    ) -> BlobEvent {
        let result = match grants_only {
            true => {
                Box::pin(self.replace_grants(bucket, key, &source, lease.as_ref(), target)).await
            }
            false => Box::pin(self.reencode(bucket, key, source, lease.as_ref(), target)).await,
        };
        // The lease keeps the source key and archive until the new copy is written.
        drop(lease);
        result.unwrap_or_else(BlobEvent::Error)
    }

    async fn reencode(
        &self,
        bucket: &str,
        key: &str,
        source: BackendLocation,
        lease: Option<&ReadLease>,
        target: ResolvedBackend,
    ) -> Result<BlobEvent, BlobError> {
        // One reservation covers the read and the write, so two rewrites never wait half-held.
        // Work above the node budget is refused, never clipped to fit.
        let read_share = match &source.format.layout {
            StoredLayout::Pithos(_) => working_set(source.blob_size),
            _ => 0,
        };
        let write_share = match target.encryption {
            Some(_) => working_set(source.blob_size),
            None => 0,
        };
        let mut permit = match read_share + write_share {
            0 => None,
            share => Some(self.reserve_pithos(share).await?),
        };
        let plain = match &source.format.layout {
            StoredLayout::Pithos(layout) => {
                let keys = self.lease_keys(&source, lease)?;
                let operator = self.operator_from_location(&source)?;
                let path = source.get_storage_path()?;
                let range = 0..source.blob_size;
                let idle = self.transfer_idle_timeout();
                let opening = Opening {
                    original: source.blob_size,
                    share: permit.clone(),
                };
                let stream =
                    read_within(operator, path, layout, keys, range, idle, opening).await?;
                BackendStream::new(Exclusive(Mutex::new(Box::pin(stream))))
            }
            _ => match Box::pin(self.read_blob(source.clone())).await {
                BlobEvent::ReadFinished { blob, .. } => blob,
                BlobEvent::Error(error) => return Err(error),
                _ => return Err(BlobError::ReadError("unexpected read event".to_string())),
            },
        };
        // A sealed target takes the share into its encoder; otherwise it is held until the end.
        let reserved = target.encryption.and_then(|_| permit.take());
        let size = Some(source.blob_size);
        let created_by = source.created_by;
        let written =
            self.write_reserved_blob((bucket, key), target, created_by, plain, size, reserved);
        let event = Box::pin(written).await;
        drop(permit);
        Ok(match event {
            BlobEvent::WriteFinished { location } => BlobEvent::CopyRewritten { location },
            other => other,
        })
    }

    /// The key of a sealed source, only through a lease admitted for exactly this archive.
    pub(in crate::blob) fn lease_keys(
        &self,
        source: &BackendLocation,
        lease: Option<&ReadLease>,
    ) -> Result<AccessKeys, BlobError> {
        let key = (source.format.bucket_key()).ok_or_else(super::needs_bucket_key)?;
        // The lease keeps its own key, so a lock after admission does not stop this rewrite.
        let secret = lease
            .filter(|lease| lease.key == key && lease.archive == ArchiveKey::of(source))
            .and_then(LeaseGuard::secret)
            .ok_or(BucketKeyError::Locked(key.bucket_id))?;
        let bytes = secret.bytes().expose();
        if bytes.len() != 32 {
            return Err(BucketKeyError::WrongKey.into());
        }
        let mut raw = Zeroizing::new([0u8; 32]);
        raw.copy_from_slice(bytes);
        Ok(AccessKeys::new().with_key(PrivateKey::from_raw(raw)))
    }

    /// The unlocked key of `key`, for the node vault copy of a bucket leaving `vault_locked`.
    pub(in crate::blob) fn read_unlocked(&self, key: BucketKeyRef) -> BlobEvent {
        let unlocked = match self.unlocks.lock() {
            Ok(mut registry) => registry.unlocked_key(key, Instant::now()),
            Err(_) => Err(BucketKeyError::Locked(key.bucket_id)),
        };
        match unlocked {
            Ok((private_key, _)) => BlobEvent::UnlockedKeyRead { key, private_key },
            Err(error) => BlobEvent::Error(error.into()),
        }
    }

    /// Opens `source` with its key, grants its piece keys only to the target key and writes the
    /// result to a new path: the old header is replaced, the blocks copied, the directory new.
    async fn replace_grants(
        &self,
        bucket: &str,
        key: &str,
        source: &BackendLocation,
        lease: Option<&ReadLease>,
        target: ResolvedBackend,
    ) -> Result<BlobEvent, BlobError> {
        let (StoredLayout::Pithos(layout), Some(plan)) = (&source.format.layout, target.encryption)
        else {
            let message = "grant replacement needs a sealed source and target";
            return Err(BlobError::WriteError(message.to_string()));
        };
        let keys = self.lease_keys(source, lease)?;
        // Covers the decoded view, the raw directory and the replacement held at once.
        let _budget = self.reserve_pithos(working_set(source.blob_size)).await?;
        let operator = self.operator_from_location(source)?;
        let path = source.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        let stored = StoredBytes {
            operator: operator.clone(),
            path: path.clone(),
            idle,
        };
        let options = OpenOptions::default()
            .with_limits(super::open_limits(source.blob_size))
            .with_access_keys(keys)
            .with_expected_metadata_digest(layout.metadata_digest);
        let archive = AsyncArchive::open(stored, options, Some(layout.stored_size)).await;
        let archive = archive.map_err(integrity)?;
        let view = archive.view();
        let directory = read_range(&operator, &path, view.directory_range(), idle).await?;
        let recipient = PublicKey::from_raw(plan.public_key).map_err(|error| {
            BlobError::WriteError(format!("invalid bucket public key: {error}"))
        })?;
        let replacement = view.replace_grants(&directory, vec![recipient]);
        let replacement = replacement.map_err(integrity)?;
        let new_layout = PithosLayout {
            stored_size: replacement.archive_len(),
            metadata_digest: replacement.metadata_digest(),
            storage_generation: plan.storage_generation,
        };
        let format = StoredFormat::pithos(new_layout, plan.key);
        let copy = (&operator, path.as_str(), &replacement);
        Box::pin(self.write_replacement(bucket, key, source, target, format, copy)).await
    }

    /// Reserves and writes the replacement like any new copy, so an unclear outcome is
    /// reconciled from its reservation.
    async fn write_replacement(
        &self,
        bucket: &str,
        key: &str,
        source: &BackendLocation,
        target: ResolvedBackend,
        format: StoredFormat,
        copy: (&Operator, &str, &GrantReplacement),
    ) -> Result<BlobEvent, BlobError> {
        let root = self.registry.config_for(&target.backend)?.root.clone();
        let ulid = Ulid::generate();
        let backend_path = build_backend_path(bucket, key, ulid)?;
        let template = BackendLocation {
            backend: target.backend.clone(),
            storage_class: target.storage_class.clone(),
            root,
            storage_bucket: String::new(),
            backend_path,
            ulid,
            format,
            created_by: source.created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: source.blob_size,
            hashes: source.hashes.clone(),
        };
        let Some(mut reservation) = self.hold_reservation(ulid) else {
            let message = "too many active blob reservations";
            return Err(BlobError::WriteError(message.to_string()));
        };
        let location = self.reserve_bucket(&target.backend, &template).await?;
        let written = self.copy_archive(&location, copy).await;
        if let Err(error) = written {
            if let Ok(operator) = self.operator_from_location(&location)
                && let Ok(path) = location.get_storage_path()
            {
                _ = self.delete_path(&operator, &path).await;
            }
            _ = self.release_reservation(&location).await;
            return Err(error);
        }
        reservation.retain();
        Ok(match self.finalize_reservation(&location).await {
            Ok(()) => BlobEvent::CopyRewritten { location },
            Err(error) => BlobEvent::Error(BlobError::WriteCleanup {
                location,
                message: error.to_string(),
            }),
        })
    }

    async fn copy_archive(
        &self,
        location: &BackendLocation,
        (old, old_path, replacement): (&Operator, &str, &GrantReplacement),
    ) -> Result<(), BlobError> {
        let operator = self.registry.bucket_operator(
            &location.backend,
            &location.storage_bucket,
            &self.egress,
        )?;
        let path = location.get_storage_path()?;
        let (io, idle) = (self.io_timeout(), self.transfer_idle_timeout());
        let mut writer = match timeout(io, operator.writer(&path)).await {
            Ok(Ok(writer)) => writer,
            Ok(Err(error)) => return Err(BlobError::OperatorCreationFailed(error.to_string())),
            Err(_) => return Err(expired()),
        };
        let header = Bytes::copy_from_slice(&replacement.header());
        within(idle, writer.write(header)).await?;
        let range = replacement.copy_range();
        let mut offset = range.start;
        while offset < range.end {
            let end = range.end.min(offset + COPY_CHUNK);
            let bytes = read_range(old, old_path, offset..end, idle).await?;
            within(idle, writer.write(bytes)).await?;
            offset = end;
        }
        let directory = Bytes::copy_from_slice(replacement.directory());
        within(idle, writer.write(directory)).await?;
        within(io, writer.close()).await.map(|_| ())
    }
}

async fn within<T>(
    limit: Duration,
    call: impl Future<Output = opendal::Result<T>>,
) -> Result<T, BlobError> {
    match timeout(limit, call).await {
        Ok(result) => result.map_err(|error| BlobError::WriteError(error.to_string())),
        Err(_) => Err(expired()),
    }
}

fn expired() -> BlobError {
    BlobError::WriteError("archive copy deadline expired".to_string())
}

fn integrity(error: PithosError) -> BlobError {
    BlobError::IntegrityCheckFailed(error.to_string())
}
