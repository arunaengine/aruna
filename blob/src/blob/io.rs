//! Writes, reads, lists and deletes blob objects on a backend, including hidden staged blobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::{
    build_backend_path, build_hidden_path, build_part_path, intent_key, intent_value,
};
use super::group::GROUP_WRITE_CHUNK;
use super::pithos::{ArchiveEncoder, working_set};
use crate::codec::FrameEncoder;
use crate::hash::Hasher;
use crate::opendal::{UnsupportedAbort, abort_partial_writer, abort_writer};
use crate::s3::NativeMultipart;
use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle as _;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, COPY_OWNER_KEYSPACE, PENDING_LOCATION_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{
    ArchiveKey, Backend, BackendLocation, BackendRef, BlobLocationKey, CopyOwner,
    HIDDEN_BLOB_PREFIX, HiddenBlobEntry, HiddenBlobKey, ResolvedBackend,
};
use aruna_core::structs::storage::encryption::{BucketKeyRef, SealPlan};
use aruna_core::structs::storage::format::{Compression, FrameLayout, StoredFormat, StoredLayout};
use aruna_core::structs::storage::group_backend::GroupBackendKind;
use aruna_core::structs::storage::multipart::MultipartPartKey;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::{EntryMode, ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::ops::{Bound, Range, RangeBounds};
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, timeout, timeout_at};
use ulid::Ulid;

/// Size and time limits of a hidden spool; other writes have none.
#[derive(Default)]
struct WriteLimits {
    max_bytes: Option<u64>,
    deadline: Option<StdInstant>,
    /// Writer chunk that keeps a large object within its provider's part limit.
    chunk: Option<usize>,
}

/// Turns original bytes into stored bytes: zstd frames, or one Pithos archive of a bucket key.
enum Encoder {
    Frames(FrameEncoder),
    Pithos(Box<ArchiveEncoder>, BucketKeyRef),
}

impl Encoder {
    async fn push(&mut self, bytes: &[u8]) -> Result<Vec<Bytes>, BlobError> {
        match self {
            Self::Frames(encoder) => encoder.push(bytes).await,
            Self::Pithos(encoder, _) => encoder.push(bytes).await,
        }
    }

    /// The closing bytes and the stored format; a Pithos content hash must equal `blake3`.
    async fn finish(self, blake3: &[u8]) -> Result<(Vec<Bytes>, StoredFormat), BlobError> {
        match self {
            Self::Frames(encoder) => {
                let (pieces, layout) = encoder.finish().await?;
                let format = StoredFormat {
                    layout: StoredLayout::Frames(Box::new(layout)),
                    ..StoredFormat::default()
                };
                Ok((pieces, format))
            }
            Self::Pithos(encoder, key) => {
                let (pieces, layout, content_hash) = encoder.finish().await?;
                if content_hash != blake3 {
                    let message = "the Pithos content hash differs from the original bytes";
                    return Err(BlobError::IntegrityCheckFailed(message.to_string()));
                }
                Ok((pieces, StoredFormat::pithos(layout, key)))
            }
        }
    }
}

const HIDDEN_LIST_PAGE: usize = 128;
const HIDDEN_BACKEND_LIMIT: usize = 256;
const HIDDEN_SOURCE_HOPS: usize = 32;

#[derive(Debug, Deserialize, Serialize)]
enum HiddenCursor {
    Objects {
        backend: BackendRef,
        bucket: Option<String>,
        start_after: Option<String>,
    },
    Reservations {
        start_after: Option<Vec<u8>>,
    },
}

/// Resolves the requested bounds against the stored size so a range read can
/// report how many bytes its stream yields.
fn range_length(range: &impl RangeBounds<u64>, blob_size: u64) -> u64 {
    let range = clamped_range(range, blob_size);
    range.end - range.start
}

/// The requested bounds as a range inside `0..blob_size`.
fn clamped_range(range: &impl RangeBounds<u64>, blob_size: u64) -> Range<u64> {
    let start = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(end) => end.saturating_add(1),
        Bound::Excluded(end) => *end,
        Bound::Unbounded => blob_size,
    };
    let end = end.min(blob_size);
    start.min(end)..end
}

/// Tenant writers open with an explicit chunk so a small-chunk stream cannot
/// exhaust a provider's per-object block ceiling.
async fn open_writer(
    operator: &Operator,
    path: &str,
    backend: &BackendRef,
    chunk: Option<usize>,
) -> Result<opendal::Writer, opendal::Error> {
    match (backend, chunk) {
        (_, Some(chunk)) => operator.writer_with(path).chunk(chunk).await,
        (BackendRef::Group(_), None) => operator.writer_with(path).chunk(GROUP_WRITE_CHUNK).await,
        (BackendRef::Node(_), None) => operator.writer(path).await,
    }
}

/// Writer chunk for a composition. `None` keeps each input part as one S3 part; other kinds
/// stream in chunks only as large as their provider's part limit requires. Frames do not
/// follow the input parts, so on S3 they stream in chunks sized for its 10,000 part limit.
pub(super) fn compose_chunk(backend: &Backend, total: u64, framed: bool) -> Option<usize> {
    const MIB: u64 = 1024 * 1024;
    let limit: u64 = match backend {
        Backend::S3 | Backend::Group(GroupBackendKind::S3) if !framed => return None,
        Backend::S3 | Backend::Group(GroupBackendKind::S3) => 10_000,
        Backend::Group(GroupBackendKind::Azblob | GroupBackendKind::Azdls) => 50_000,
        Backend::FileSystem | Backend::Group(GroupBackendKind::Gcs | GroupBackendKind::B2) => {
            10_000
        }
    };
    // Raw frames and the frame entries can store a little more than the input.
    let total = if framed {
        total.saturating_add(total / 1024).saturating_add(MIB)
    } else {
        total
    };
    let needed = total.div_ceil(limit).div_ceil(MIB) * MIB;
    Some(
        usize::try_from(needed)
            .unwrap_or(usize::MAX)
            .max(GROUP_WRITE_CHUNK),
    )
}

async fn with_deadline<F, T>(deadline: Option<StdInstant>, future: F) -> Result<T, ()>
where
    F: Future<Output = T>,
{
    match deadline {
        Some(deadline) => timeout_at(Instant::from_std(deadline), future)
            .await
            .map_err(|_| ()),
        None => Ok(future.await),
    }
}

async fn next_chunk(
    stream: &mut BackendStream<Result<Bytes, StreamError>>,
    idle_timeout: Duration,
) -> Result<Option<Result<Bytes, StreamError>>, BlobError> {
    timeout(idle_timeout, stream.next())
        .await
        .map_err(|_| BlobError::ReadError("blob read idle timeout".to_string()))
}

struct HiddenReservation {
    handler: BlobHandler,
    key: Arc<StdMutex<Option<HiddenBlobKey>>>,
    location: Option<BackendLocation>,
    operator: Option<Operator>,
    storage_path: Option<String>,
    writer: Option<opendal::Writer>,
    uncertain: bool,
    /// A writer future dropped mid-poll by a timeout or cancellation leaves
    /// opendal's retry layer in a bad state, so the writer must never be polled again.
    abandoned: bool,
}

impl HiddenReservation {
    fn new(handler: BlobHandler) -> Self {
        Self {
            handler,
            key: Arc::new(StdMutex::new(None)),
            location: None,
            operator: None,
            storage_path: None,
            writer: None,
            uncertain: false,
            abandoned: false,
        }
    }

    fn set_operator(&mut self, operator: Operator, storage_path: String) {
        self.operator = Some(operator);
        self.storage_path = Some(storage_path);
    }

    fn set_location(&mut self, location: BackendLocation) {
        self.location = Some(location);
    }

    fn set_writer(&mut self, writer: opendal::Writer) {
        self.writer = Some(writer);
    }

    fn writer_mut(&mut self) -> Option<&mut opendal::Writer> {
        self.writer.as_mut()
    }

    fn commit(&mut self) {
        self.writer = None;
        if let Ok(mut key) = self.key.lock() {
            *key = None;
        }
    }

    fn finish(&mut self) {
        self.writer = None;
    }

    fn mark_uncertain(&mut self) {
        self.uncertain = true;
    }

    fn mark_abandoned(&mut self) {
        self.abandoned = true;
    }

    fn mark_settled(&mut self) {
        self.abandoned = false;
    }

    /// Boxed, so each failure exit of a write keeps only a pointer in the caller's stack frame.
    fn fail(&mut self, error: BlobError) -> BoxFuture<'_, BlobEvent> {
        Box::pin(async move {
            match self.abort().await {
                Ok(()) => BlobEvent::Error(error),
                Err(cleanup) => {
                    let plain = self.key.lock().map_or(true, |key| key.is_none());
                    match (plain, self.location.clone()) {
                        (true, Some(location)) => BlobEvent::Error(BlobError::WriteCleanup {
                            location,
                            message: cleanup.to_string(),
                        }),
                        _ => BlobEvent::Error(cleanup),
                    }
                }
            }
        })
    }

    fn fail_close(&mut self, error: BlobError) -> BoxFuture<'_, BlobEvent> {
        Box::pin(async move {
            self.mark_uncertain();
            let cleanup = self.abort().await;
            let Some(location) = self.location.clone() else {
                return match cleanup {
                    Ok(()) => BlobEvent::Error(error),
                    Err(cleanup) => BlobEvent::Error(cleanup),
                };
            };
            BlobEvent::Error(BlobError::WriteCleanup {
                location,
                message: match cleanup {
                    Ok(()) => error.to_string(),
                    Err(cleanup) => format!("{error}; {cleanup}"),
                },
            })
        })
    }

    async fn abort(&mut self) -> Result<(), BlobError> {
        let handler = self.handler.clone();
        let operator = self.operator.clone();
        let storage_path = self.storage_path.clone();
        let abandoned = self.abandoned;
        let cleanup = if self.writer.is_some() {
            let cleanup = handler
                .clean_partial(
                    self.writer.as_mut(),
                    abandoned,
                    operator.as_ref(),
                    storage_path.as_deref(),
                    self.location.as_ref(),
                )
                .await;
            if cleanup.is_ok() {
                self.writer = None;
            }
            cleanup
        } else if !self.uncertain {
            handler
                .clean_partial(
                    None,
                    abandoned,
                    operator.as_ref(),
                    storage_path.as_deref(),
                    self.location.as_ref(),
                )
                .await
        } else {
            Ok(())
        };
        // Capacity is released even if cleanup fails; the reclaim sweep
        // collects the leftover object.
        let released = match self.key.lock().ok().and_then(|key| key.clone()) {
            Some(key) if !self.uncertain => {
                let released = handler.release_hidden(&key).await;
                if released.is_ok()
                    && let Ok(mut current) = self.key.lock()
                {
                    *current = None;
                }
                released
            }
            _ => Ok(()),
        };
        cleanup.and(released)
    }

    fn key_slot(&self) -> Arc<StdMutex<Option<HiddenBlobKey>>> {
        Arc::clone(&self.key)
    }
}

impl Drop for HiddenReservation {
    fn drop(&mut self) {
        if self.key.lock().map_or(true, |key| key.is_none()) && self.writer.is_none() {
            return;
        }
        if self.writer.is_none() {
            tracing::warn!("hidden blob cleanup deferred to the orphan sweep");
            return;
        }
        let handler = self.handler.clone();
        let Ok(permit) = handler.spool_slots.clone().try_acquire_owned() else {
            tracing::warn!("hidden blob cleanup deferred to the orphan sweep");
            return;
        };
        let Ok(runtime) = Handle::try_current() else {
            tracing::error!("cannot schedule hidden blob cleanup without a runtime");
            return;
        };
        let key = self.key.lock().ok().and_then(|key| key.clone());
        let mut writer = self.writer.take();
        let operator = self.operator.clone();
        let storage_path = self.storage_path.clone();
        let location = self.location.clone();
        let abandoned = self.abandoned;
        let uncertain = self.uncertain;
        runtime.spawn(async move {
            // A failed cleanup must not strand the reservation, so the release
            // below runs either way and the reclaim sweep collects the object.
            if let Err(error) = handler
                .clean_partial(
                    writer.as_mut(),
                    abandoned,
                    operator.as_ref(),
                    storage_path.as_deref(),
                    location.as_ref(),
                )
                .await
            {
                tracing::error!(%error, "failed to clean cancelled hidden blob");
            }
            if !uncertain
                && let Some(key) = key
                && let Err(error) = handler.release_hidden(&key).await
            {
                tracing::error!(%error, "failed to release cancelled hidden blob");
            }
            drop(permit);
        });
    }
}

impl BlobHandler {
    pub(super) async fn write_stream(
        &self,
        location: BackendLocation,
        operator: Operator,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let limits = WriteLimits::default();
        Box::pin(self.write_stream_limit(location, operator, blob, limits, None, None)).await
    }

    /// Writes the original bytes as frames compressed with `compression`, or as one Pithos
    /// archive when `seal` names a bucket key.
    pub(super) async fn write_encoded(
        &self,
        location: BackendLocation,
        operator: Operator,
        blob: BackendStream<Result<Bytes, StreamError>>,
        compression: Compression,
        (seal, reserved): (Option<SealPlan>, Option<OwnedSemaphorePermit>),
        size: Option<u64>,
    ) -> BlobEvent {
        let mut limits = WriteLimits::default();
        // A sealed archive of a known size picks a chunk that its provider's part limit admits.
        if let (Some(_), Some(size)) = (seal, size) {
            let backend = match self.registry.config_for(&location.backend) {
                Ok(config) => config.backend_type,
                Err(error) => return BlobEvent::Error(error),
            };
            limits.chunk = compose_chunk(&backend, size, true);
        }
        let encoder = match (seal, compression) {
            (Some(plan), _) => {
                let covered = size.unwrap_or(super::pithos::GROWTH);
                // A caller that already holds the share of this write passes it in.
                let permit = match reserved {
                    Some(permit) => permit,
                    None => match self.reserve_pithos(working_set(covered)).await {
                        Ok(permit) => permit,
                        Err(error) => return BlobEvent::Error(error),
                    },
                };
                let budget = Arc::clone(&self.pithos_budget);
                match ArchiveEncoder::new(&plan, compression) {
                    Ok(encoder) => {
                        let encoder = encoder.with_budget(budget, permit, covered);
                        Some(Encoder::Pithos(Box::new(encoder), plan.key))
                    }
                    Err(error) => return BlobEvent::Error(error),
                }
            }
            (None, Compression::Off) => None,
            (None, Compression::Zstd { level }) => Some(Encoder::Frames(FrameEncoder::new(level))),
        };
        Box::pin(self.write_stream_limit(location, operator, blob, limits, encoder, None)).await
    }

    /// Appends one piece to the open writer, failing the reservation on error.
    async fn write_stored(
        &self,
        reservation: &mut HiddenReservation,
        deadline: Option<StdInstant>,
        bytes: Bytes,
    ) -> Result<(), BlobEvent> {
        // Stays set if the caller drops this future before the write returns.
        reservation.mark_abandoned();
        let write = match reservation.writer_mut() {
            Some(writer) => match deadline {
                Some(deadline) => with_deadline(Some(deadline), writer.write(bytes)).await,
                None => timeout(self.transfer_idle_timeout(), writer.write(bytes))
                    .await
                    .map_err(|_| ()),
            },
            None => {
                let error = BlobError::WriteError("hidden writer is missing".to_string());
                return Err(reservation.fail(error).await);
            }
        };
        if write.is_ok() {
            reservation.mark_settled();
        }
        match write {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(reservation
                .fail(BlobError::WriteError(err.to_string()))
                .await),
            Err(()) => {
                reservation.mark_abandoned();
                let error = BlobError::WriteError("blob write deadline expired".to_string());
                Err(reservation.fail(error).await)
            }
        }
    }

    async fn write_stream_limit(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
        limits: WriteLimits,
        mut encoder: Option<Encoder>,
        reservation: Option<&mut HiddenReservation>,
    ) -> BlobEvent {
        let WriteLimits {
            max_bytes,
            deadline,
            chunk,
        } = limits;
        let mut plain = HiddenReservation::new(self.clone());
        let reservation = reservation.unwrap_or(&mut plain);
        reservation.set_location(location.clone());
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return reservation.fail(e).await,
        };
        reservation.set_operator(operator.clone(), storage_path.clone());
        let open = match deadline {
            Some(deadline) => {
                with_deadline(
                    Some(deadline),
                    open_writer(&operator, &storage_path, &location.backend, chunk),
                )
                .await
            }
            None => timeout(
                self.io_timeout(),
                open_writer(&operator, &storage_path, &location.backend, chunk),
            )
            .await
            .map_err(|_| ()),
        };
        match open {
            Ok(Ok(writer)) => reservation.set_writer(writer),
            Ok(Err(_)) => {
                return reservation
                    .fail(BlobError::OperatorCreationFailed(
                        "Failed to create writer from operator".to_string(),
                    ))
                    .await;
            }
            Err(()) => {
                return reservation
                    .fail(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ))
                    .await;
            }
        }

        if let Some(Encoder::Pithos(archive, _)) = &encoder
            && let Err(event) = self
                .write_stored(reservation, deadline, archive.header())
                .await
        {
            return event;
        }
        let mut hasher = Hasher::new();
        let mut bytes_written = 0u64;
        loop {
            let chunk = match deadline {
                Some(deadline) => with_deadline(Some(deadline), blob.next()).await,
                None => timeout(self.transfer_idle_timeout(), blob.next())
                    .await
                    .map_err(|_| ()),
            };
            let chunk = match chunk {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(()) => {
                    return reservation
                        .fail(BlobError::WriteError(
                            "blob write deadline expired".to_string(),
                        ))
                        .await;
                }
            };
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => {
                    return reservation
                        .fail(BlobError::StreamFailed(err.to_string()))
                        .await;
                }
            };
            let Some(next_size) = bytes_written.checked_add(bytes.len() as u64) else {
                return reservation
                    .fail(BlobError::SizeLimitExceeded {
                        limit: max_bytes.unwrap_or(u64::MAX),
                    })
                    .await;
            };
            if let Some(limit) = max_bytes
                && next_size > limit
            {
                return reservation
                    .fail(BlobError::SizeLimitExceeded { limit })
                    .await;
            }
            hasher.update(&bytes);
            let pieces = match encoder.as_mut() {
                Some(encoder) => match encoder.push(&bytes).await {
                    Ok(pieces) => pieces,
                    Err(error) => return reservation.fail(error).await,
                },
                None => vec![bytes],
            };
            for piece in pieces {
                if let Err(event) = self.write_stored(reservation, deadline, piece).await {
                    return event;
                }
            }
            bytes_written = next_size;
        }
        let hashes = hasher.to_map();
        if let Some(encoder) = encoder {
            let blake3 = hashes.get(HASH_BLAKE3).map_or(&[][..], Vec::as_slice);
            let (pieces, format) = match encoder.finish(blake3).await {
                Ok(finished) => finished,
                Err(error) => return reservation.fail(error).await,
            };
            for piece in pieces {
                if let Err(event) = self.write_stored(reservation, deadline, piece).await {
                    return event;
                }
            }
            location.format = format;
            // An uncertain close reports the stored format, so cleanup knows a Pithos archive.
            reservation.set_location(location.clone());
        }

        reservation.mark_abandoned();
        let close = match reservation.writer_mut() {
            Some(writer) => match deadline {
                Some(deadline) => with_deadline(Some(deadline), writer.close()).await,
                None => timeout(self.io_timeout(), writer.close())
                    .await
                    .map_err(|_| ()),
            },
            None => {
                return reservation
                    .fail(BlobError::WriteError(
                        "hidden writer is missing".to_string(),
                    ))
                    .await;
            }
        };
        if close.is_ok() {
            reservation.mark_settled();
        }
        match close {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                return reservation
                    .fail_close(BlobError::WriteError(err.to_string()))
                    .await;
            }
            Err(()) => {
                reservation.mark_abandoned();
                return reservation
                    .fail_close(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ))
                    .await;
            }
        }
        reservation.finish();
        location.blob_size = bytes_written;
        location.hashes = hashes;
        BlobEvent::WriteFinished { location }
    }

    pub async fn spool_hidden_blob(
        &self,
        namespace: Ulid,
        name: &str,
        created_by: UserId,
        max_bytes: Option<u64>,
        deadline: Option<StdInstant>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        // Hidden blobs are job spool, never routed: they always use the default.
        let resolved = self.registry.default_resolved();
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_hidden_path(namespace, name, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let mut reservation = HiddenReservation::new(self.clone());
        let key = match with_deadline(
            deadline,
            self.reserve_hidden_key(
                &resolved.backend,
                &root,
                &backend_path,
                reservation.key_slot(),
            ),
        )
        .await
        {
            Ok(Ok(key)) => key,
            Ok(Err(err)) => return reservation.fail(err).await,
            Err(()) => {
                return reservation
                    .fail(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ))
                    .await;
            }
        };
        let backend_bucket = key.storage_bucket.clone();
        let location = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class.clone(),
            root,
            storage_bucket: backend_bucket.clone(),
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
        let operator =
            match self
                .registry
                .bucket_operator(&resolved.backend, &backend_bucket, &self.egress)
            {
                Ok(operator) => operator,
                Err(err) => {
                    return reservation.fail(err).await;
                }
            };
        let location = match self
            .write_stream_limit(
                location,
                operator,
                blob,
                WriteLimits {
                    max_bytes,
                    deadline,
                    chunk: None,
                },
                None,
                Some(&mut reservation),
            )
            .await
        {
            BlobEvent::WriteFinished { location } => location,
            other => return other,
        };
        let Some(hash) = location.get_blake3() else {
            let error = BlobError::IntegrityCheckFailed("hidden blob hash is missing".to_string());
            return reservation.fail(error).await;
        };
        let Ok(blake3) = hash.try_into() else {
            let error = BlobError::IntegrityCheckFailed(
                "hidden blob hash has an invalid length".to_string(),
            );
            return reservation.fail(error).await;
        };
        reservation.commit();
        BlobEvent::HiddenSpooled {
            size: location.blob_size,
            location,
            blake3,
        }
    }

    /// Removes the partial object a failed or cancelled write left behind.
    /// An abandoned writer is never polled again; backends without writer
    /// abort keep the partial object, so deleting its path is equivalent.
    pub(super) async fn clean_partial(
        &self,
        writer: Option<&mut opendal::Writer>,
        abandoned: bool,
        operator: Option<&Operator>,
        storage_path: Option<&str>,
        location: Option<&BackendLocation>,
    ) -> Result<(), BlobError> {
        if let Some(writer) = writer
            && !abandoned
        {
            match abort_writer(writer, self.io_timeout(), UnsupportedAbort::DeletePath).await {
                Ok(()) => return Ok(()),
                // Other abort failures stay uncertain and must not delete.
                Err(BlobError::CleanupUnsupported) => {}
                Err(error) => return Err(error),
            }
        }
        match (operator, storage_path) {
            (Some(operator), Some(path)) => {
                self.delete_path(operator, path).await?;
                match location {
                    Some(location) if abandoned => {
                        Box::pin(self.abort_uploads(location, path)).await
                    }
                    _ => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }

    /// The native multipart client for a location on an S3 backend; `None` for other kinds.
    pub(super) fn native_for(
        &self,
        location: &BackendLocation,
    ) -> Result<Option<NativeMultipart>, BlobError> {
        let entry = self.registry.backend(&location.backend)?;
        let config = &entry.config.service_config;
        let (bucket, guard) = match entry.config.backend_type {
            Backend::S3 => (location.storage_bucket.as_str(), None),
            Backend::Group(GroupBackendKind::S3) => {
                let bucket = config.get("bucket").ok_or_else(|| {
                    BlobError::OperatorCreationFailed("group backend has no bucket".to_string())
                })?;
                (bucket.as_str(), Some(&self.egress))
            }
            _ => return Ok(None),
        };
        NativeMultipart::from_config(config, bucket, &location.root, guard).map(Some)
    }

    /// An abandoned writer took its provider upload id along, so deleting the path leaves the
    /// uploaded parts behind. The path is unique to one blob, so its uploads are aborted.
    pub(super) async fn abort_uploads(
        &self,
        location: &BackendLocation,
        storage_path: &str,
    ) -> Result<(), BlobError> {
        let Some(native) = self.native_for(location)? else {
            return Ok(());
        };
        match timeout(self.io_timeout(), native.abort_path(storage_path)).await {
            Ok(result) => result.map(|_| ()),
            Err(_) => Err(BlobError::DeleteError(
                "timed out aborting unfinished multipart uploads".to_string(),
            )),
        }
    }

    pub(super) async fn delete_path(
        &self,
        operator: &Operator,
        storage_path: &str,
    ) -> Result<(), BlobError> {
        match timeout(self.io_timeout(), operator.delete(storage_path)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => Ok(()),
            Ok(Err(error)) => Err(BlobError::DeleteError(error.to_string())),
            Err(_) => Err(BlobError::DeleteError(
                "timed out deleting partial blob output".to_string(),
            )),
        }
    }

    async fn release_hidden(&self, key: &HiddenBlobKey) -> Result<(), BlobError> {
        match timeout(self.io_timeout(), self.release_hidden_key(key)).await {
            Ok(result) => result,
            Err(_) => Err(BlobError::DeleteError(
                "timed out releasing hidden blob reservation".to_string(),
            )),
        }
    }

    pub(super) async fn finalize_reservation(
        &self,
        location: &BackendLocation,
    ) -> Result<(), BlobError> {
        let value = intent_value(location)?;
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE.to_string(),
                key: intent_key(location),
                value,
                txn_id: None,
            }))
            .await;
        if matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            Ok(())
        } else {
            Err(BlobError::WriteError(format!(
                "failed to finalize bucket reservation: {event:?}"
            )))
        }
    }

    pub async fn reconcile_reservation(
        &self,
        location: BackendLocation,
    ) -> Result<bool, BlobError> {
        if !self.marker_present(&location).await? {
            self.clear_active(location.ulid);
            return Ok(true);
        }
        let active = self.reservation_active(location.ulid);
        // A pending archive has no hash, so its own records prove the commit.
        if let StoredLayout::Pithos(_) = location.format.layout
            && self.archive_owned(&location).await?
        {
            self.clear_marker(&location).await?;
            return Ok(true);
        }
        let hash = match location.get_blake3() {
            Some(hash) => hash
                .try_into()
                .map_err(|_| BlobError::ReadError("invalid reservation hash".to_string()))?,
            None if active => return Ok(false),
            None => {
                // Metadata is admitted only after the finalized marker is durable.
                let operator = self.operator_from_location(&location)?;
                let storage_path = location.get_storage_path()?;
                let _claim = self.claim_archive(&location)?;
                self.delete_path(&operator, &storage_path).await?;
                self.release_reservation(&location).await?;
                return Ok(true);
            }
        };
        let operator = self.operator_from_location(&location)?;
        let storage_path = location.get_storage_path()?;
        match timeout(self.io_timeout(), operator.stat(&storage_path)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                self.release_reservation(&location).await?;
                return Ok(true);
            }
            Ok(Err(error)) => return Err(BlobError::ReadError(error.to_string())),
            Err(_) => {
                return Err(BlobError::ReadError(
                    "timed out checking bucket reservation object".to_string(),
                ));
            }
        }

        let key = BlobLocationKey::new(hash, location.format.encoding(), location.backend.clone());
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: key.to_bytes().into(),
                txn_id: None,
            }))
            .await;
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(BlobError::ReadError(format!(
                    "failed to read bucket reservation owner: {error}"
                )));
            }
            _ => {
                return Err(BlobError::ReadError(
                    "unexpected bucket reservation owner event".to_string(),
                ));
            }
        };
        if let Some(value) = value {
            let owner = BackendLocation::from_bytes(&value).map_err(BlobError::ConversionError)?;
            if owner.same_object(&location) {
                self.clear_marker(&location).await?;
                return Ok(true);
            }
        }
        if active {
            return Ok(false);
        }
        let _claim = self.claim_archive(&location)?;
        self.delete_path(&operator, &storage_path).await?;
        self.release_reservation(&location).await?;
        Ok(true)
    }

    /// Claims a Pithos archive for deletion, so no lease starts while its backend copy goes.
    /// A pinned archive fails the claim and stays for a later pass.
    fn claim_archive(
        &self,
        location: &BackendLocation,
    ) -> Result<Option<super::unlock::DeleteClaim>, BlobError> {
        match location.format.layout {
            StoredLayout::Pithos(_) => self.claim_delete(&ArchiveKey::of(location)).map(Some),
            _ => Ok(None),
        }
    }

    /// Whether committed records keep a Pithos archive: its pending location names this exact
    /// object, or a version still owns it.
    async fn archive_owned(&self, location: &BackendLocation) -> Result<bool, BlobError> {
        let archive = ArchiveKey::of(location);
        let reads = [
            StorageEffect::Read {
                key_space: PENDING_LOCATION_KEYSPACE.to_string(),
                key: archive.to_bytes().into(),
                txn_id: None,
            },
            StorageEffect::Iter {
                key_space: COPY_OWNER_KEYSPACE.to_string(),
                prefix: Some(CopyOwner::prefix(&archive).into()),
                start: None,
                limit: 1,
                txn_id: None,
            },
        ];
        for read in reads {
            match self.storage.send_effect(Effect::Storage(read)).await {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => {
                    let pending = BackendLocation::from_bytes(&value)?;
                    if pending.same_object(location) {
                        return Ok(true);
                    }
                }
                Event::Storage(StorageEvent::IterResult { values, .. }) if !values.is_empty() => {
                    return Ok(true);
                }
                Event::Storage(
                    StorageEvent::ReadResult { .. } | StorageEvent::IterResult { .. },
                ) => {}
                other => {
                    return Err(BlobError::ReadError(format!(
                        "failed to read archive owners: {other:?}"
                    )));
                }
            }
        }
        Ok(false)
    }

    pub async fn write_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let written = self.write_sized_blob(
            request_bucket,
            request_key,
            resolved,
            created_by,
            blob,
            None,
        );
        Box::pin(written).await
    }

    /// Like `write_blob`; a declared `size` sizes the provider chunks of a sealed archive.
    pub async fn write_sized_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
        size: Option<u64>,
    ) -> BlobEvent {
        let written = self.write_reserved_blob(
            (request_bucket, request_key),
            resolved,
            created_by,
            blob,
            size,
            None,
        );
        Box::pin(written).await
    }

    /// Like `write_sized_blob`, with the working-set share of a sealed write already reserved.
    pub(super) async fn write_reserved_blob(
        &self,
        (request_bucket, request_key): (&str, &str),
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
        size: Option<u64>,
        reserved: Option<OwnedSemaphorePermit>,
    ) -> BlobEvent {
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let template = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class.clone(),
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
        let Some(mut reservation) = self.hold_reservation(template.ulid) else {
            return BlobEvent::Error(BlobError::WriteError(
                "too many active blob reservations".to_string(),
            ));
        };
        let location = match self.reserve_bucket(&resolved.backend, &template).await {
            Ok(location) => location,
            Err(err) => return BlobEvent::Error(err),
        };
        let operator = match self.registry.bucket_operator(
            &resolved.backend,
            &location.storage_bucket,
            &self.egress,
        ) {
            Ok(op) => op,
            Err(err) => {
                _ = self.release_reservation(&location).await;
                return BlobEvent::Error(err);
            }
        };
        match Box::pin(self.write_encoded(
            location.clone(),
            operator,
            blob,
            resolved.compression,
            (resolved.encryption, reserved),
            size,
        ))
        .await
        {
            BlobEvent::WriteFinished { location } => {
                reservation.retain();
                match self.finalize_reservation(&location).await {
                    Ok(()) => BlobEvent::WriteFinished { location },
                    Err(error) => BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: error.to_string(),
                    }),
                }
            }
            other => {
                if !matches!(&other, BlobEvent::Error(BlobError::WriteCleanup { .. })) {
                    _ = self.release_reservation(&location).await;
                } else {
                    reservation.retain();
                }
                other
            }
        }
    }

    pub async fn write_blob_part(
        &self,
        part: MultipartPartKey,
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(err) => return BlobEvent::Error(err),
        };
        let multipart_bucket = match self.multipart_bucket(&resolved.backend) {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let location = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class.clone(),
            root,
            storage_bucket: multipart_bucket.clone(),
            backend_path: build_part_path(part.upload_id, part.part_number, ulid),
            ulid,
            format: StoredFormat::default(),
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        let Some(mut reservation) = self.hold_reservation(location.ulid) else {
            return BlobEvent::Error(BlobError::WriteError(
                "too many active blob reservations".to_string(),
            ));
        };
        if let Err(error) = self.finalize_reservation(&location).await {
            return BlobEvent::Error(error);
        }
        let operator =
            match self
                .registry
                .bucket_operator(&resolved.backend, &multipart_bucket, &self.egress)
            {
                Ok(op) => op,
                Err(err) => {
                    _ = self.release_reservation(&location).await;
                    return BlobEvent::Error(err);
                }
            };
        let result = Box::pin(self.write_stream(location.clone(), operator, blob)).await;
        match result {
            BlobEvent::WriteFinished { location } => {
                reservation.retain();
                BlobEvent::WriteFinished { location }
            }
            BlobEvent::Error(BlobError::WriteCleanup { location, message }) => {
                reservation.retain();
                BlobEvent::Error(BlobError::WriteCleanup { location, message })
            }
            other => match self.release_reservation(&location).await {
                Ok(()) => other,
                Err(cleanup) => {
                    reservation.retain();
                    BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: format!("{other:?}; {cleanup}"),
                    })
                }
            },
        }
    }

    pub async fn compose_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let template = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class.clone(),
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
        let Some(mut reservation) = self.hold_reservation(template.ulid) else {
            return BlobEvent::Error(BlobError::WriteError(
                "too many active blob reservations".to_string(),
            ));
        };
        let location = match self.reserve_bucket(&resolved.backend, &template).await {
            Ok(location) => location,
            Err(err) => return BlobEvent::Error(err),
        };
        let operator = match self.registry.bucket_operator(
            &resolved.backend,
            &location.storage_bucket,
            &self.egress,
        ) {
            Ok(op) => op,
            Err(err) => {
                _ = self.release_reservation(&location).await;
                return BlobEvent::Error(err);
            }
        };
        let backend_type = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.backend_type,
            Err(err) => {
                _ = self.release_reservation(&location).await;
                return BlobEvent::Error(err);
            }
        };
        let total = parts.iter().map(|part| part.blob_size).sum();
        let framed = resolved.compression != Compression::Off;
        let chunk = compose_chunk(&backend_type, total, framed);
        let composed = self.compose_parts(
            location.clone(),
            operator,
            parts,
            chunk,
            resolved.compression,
        );
        match Box::pin(composed).await {
            BlobEvent::WriteFinished { location } => {
                reservation.retain();
                match self.finalize_reservation(&location).await {
                    Ok(()) => BlobEvent::WriteFinished { location },
                    Err(error) => BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: error.to_string(),
                    }),
                }
            }
            other => {
                if !matches!(&other, BlobEvent::Error(BlobError::WriteCleanup { .. })) {
                    _ = self.release_reservation(&location).await;
                } else {
                    reservation.retain();
                }
                other
            }
        }
    }

    pub(super) async fn compose_parts(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        parts: Vec<BackendLocation>,
        chunk: Option<usize>,
        compression: Compression,
    ) -> BlobEvent {
        let mut encoder = match compression {
            Compression::Off => None,
            Compression::Zstd { level } => Some(FrameEncoder::new(level)),
        };
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        // Without a fixed chunk, each write of at least the backend minimum is one backend part.
        let opened = match chunk {
            Some(chunk) => {
                timeout(
                    self.io_timeout(),
                    operator
                        .writer_with(&storage_path)
                        .chunk(chunk)
                        .into_future(),
                )
                .await
            }
            None => timeout(self.io_timeout(), operator.writer(&storage_path)).await,
        };
        let mut writer = match opened {
            Ok(Ok(writer)) => writer,
            Ok(Err(error)) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(error.to_string()));
            }
            Err(_) => {
                return BlobEvent::Error(BlobError::WriteCleanup {
                    location,
                    message: "timed out opening compose writer".to_string(),
                });
            }
        };

        let mut hasher = Hasher::new();
        let mut ambiguous = false;
        // A timeout drops the writer future mid-poll, which leaves the writer unusable.
        let mut abandoned = false;
        let compose_result: Result<(u64, Option<FrameLayout>), BlobError> = async {
            let mut bytes_written = 0u64;
            for part in parts {
                let part_operator = self.operator_from_location(&part)?;
                let part_storage_path = part.get_storage_path()?;
                let reader = timeout(self.io_timeout(), part_operator.reader(&part_storage_path))
                    .await
                    .map_err(|_| {
                        BlobError::ReadError("timed out opening compose reader".to_string())
                    })?
                    .map_err(|err| BlobError::ReadError(err.to_string()))?;
                let reader = timeout(self.io_timeout(), reader.into_bytes_stream(..))
                    .await
                    .map_err(|_| {
                        BlobError::ReadError("timed out starting compose reader".to_string())
                    })?
                    .map_err(|err| BlobError::ReadError(err.to_string()))?;

                let mut reader = BackendStream::new(reader);
                // Without a chunk the whole part is written at once so it keeps its own boundary.
                let mut part_chunks = Vec::new();
                let mut part_size = 0u64;
                loop {
                    let next = timeout(self.transfer_idle_timeout(), reader.next())
                        .await
                        .map_err(|_| {
                            BlobError::ReadError("compose reader idle timeout".to_string())
                        })?;
                    let Some(next) = next else {
                        break;
                    };
                    let bytes = next.map_err(|err| BlobError::ReadError(err.to_string()))?;
                    hasher.update(&bytes);
                    part_size += bytes.len() as u64;
                    if let Some(encoder) = encoder.as_mut() {
                        let pieces = encoder.push(&bytes).await?;
                        self.compose_write(&mut writer, pieces, &mut abandoned)
                            .await?;
                        continue;
                    }
                    part_chunks.push(bytes);
                    if chunk.is_some() {
                        let buffered = std::mem::take(&mut part_chunks);
                        self.compose_write(&mut writer, buffered, &mut abandoned)
                            .await?;
                    }
                }
                if !part_chunks.is_empty() {
                    self.compose_write(&mut writer, part_chunks, &mut abandoned)
                        .await?;
                }
                bytes_written += part_size;
            }
            let mut layout = None;
            if let Some(encoder) = encoder.take() {
                let (pieces, framed) = encoder.finish().await?;
                self.compose_write(&mut writer, pieces, &mut abandoned)
                    .await?;
                layout = Some(framed);
            }
            timeout(self.transfer_idle_timeout(), writer.close())
                .await
                .map_err(|_| {
                    ambiguous = true;
                    abandoned = true;
                    BlobError::WriteError("compose close idle timeout".to_string())
                })?
                .map_err(|err| {
                    ambiguous = true;
                    BlobError::WriteError(err.to_string())
                })?;
            Ok((bytes_written, layout))
        }
        .await;
        ambiguous |= abandoned;

        let (bytes_written, layout) = match compose_result {
            Ok(composed) => composed,
            Err(err) => {
                let cleanup = if abandoned {
                    match self.delete_path(&operator, &storage_path).await {
                        Ok(()) => Box::pin(self.abort_uploads(&location, &storage_path)).await,
                        Err(error) => Err(error),
                    }
                } else {
                    abort_partial_writer(&mut writer, self.io_timeout()).await
                };
                if ambiguous {
                    return BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: match cleanup {
                            Ok(()) => err.to_string(),
                            Err(cleanup) => format!("{err}; {cleanup}"),
                        },
                    });
                }
                return match cleanup {
                    Ok(()) => BlobEvent::Error(err),
                    Err(cleanup) => BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: format!("{err}; {cleanup}"),
                    }),
                };
            }
        };

        location.blob_size = bytes_written;
        location.hashes = hasher.to_map();
        if let Some(layout) = layout {
            location.format.layout = StoredLayout::Frames(Box::new(layout));
        }
        BlobEvent::WriteFinished { location }
    }

    /// A timeout drops the write mid-poll, so the writer is marked abandoned.
    async fn compose_write(
        &self,
        writer: &mut opendal::Writer,
        buffer: Vec<Bytes>,
        abandoned: &mut bool,
    ) -> Result<(), BlobError> {
        timeout(self.transfer_idle_timeout(), writer.write(buffer))
            .await
            .map_err(|_| {
                *abandoned = true;
                BlobError::WriteError("compose writer idle timeout".to_string())
            })?
            .map_err(|err| BlobError::WriteError(err.to_string()))
    }

    pub async fn read_blob(&self, location: BackendLocation) -> BlobEvent {
        if let StoredLayout::Pithos(_) = &location.format.layout {
            return BlobEvent::Error(super::pithos::needs_bucket_key());
        }
        if let StoredLayout::Frames(layout) = &location.format.layout {
            let range = 0..location.blob_size;
            return Box::pin(self.read_frames(&location, layout, range)).await;
        }
        let expected_blake3: [u8; 32] = match location.get_blake3() {
            Some(hash) => match hash.try_into() {
                Ok(hash) => hash,
                Err(_) => {
                    return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                        "invalid stored blake3 hash".to_string(),
                    ));
                }
            },
            None => {
                return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                    "missing stored blake3 hash".to_string(),
                ));
            }
        };

        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let reader = match timeout(self.io_timeout(), operator.reader(&storage_path)).await {
            Ok(Ok(reader)) => {
                match timeout(self.io_timeout(), reader.into_bytes_stream(..)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        return BlobEvent::Error(BlobError::ReadError(error.to_string()));
                    }
                    Err(_) => {
                        return BlobEvent::Error(BlobError::ReadError(
                            "timed out starting blob reader".to_string(),
                        ));
                    }
                }
            }
            Ok(Err(error)) => return BlobEvent::Error(BlobError::ReadError(error.to_string())),
            Err(_) => {
                return BlobEvent::Error(BlobError::ReadError(
                    "timed out opening blob reader".to_string(),
                ));
            }
        };

        let expected_size = location.blob_size;
        let idle_timeout = self.transfer_idle_timeout();
        let blob = BackendStream::new(stream::try_unfold(
            (BackendStream::new(reader), blake3::Hasher::new(), 0u64),
            move |(mut stream, mut hasher, bytes_read)| async move {
                match next_chunk(&mut stream, idle_timeout).await? {
                    Some(Ok(bytes)) => {
                        hasher.update(&bytes);
                        let next_bytes_read = bytes_read + bytes.len() as u64;
                        Ok(Some((bytes, (stream, hasher, next_bytes_read))))
                    }
                    Some(Err(err)) => Err(BlobError::ReadError(err.to_string())),
                    None => {
                        if bytes_read != expected_size {
                            return Err(BlobError::IntegrityCheckFailed(format!(
                                "expected {} bytes but streamed {} bytes",
                                expected_size, bytes_read
                            )));
                        }

                        if hasher.finalize().as_bytes() != &expected_blake3 {
                            return Err(BlobError::IntegrityCheckFailed(
                                "blake3 hash mismatch".to_string(),
                            ));
                        }

                        Ok(None)
                    }
                }
            },
        ));

        BlobEvent::ReadFinished {
            blob,
            stream_size: expected_size,
        }
    }

    pub async fn read_blob_range(
        &self,
        location: BackendLocation,
        range: impl RangeBounds<u64>,
    ) -> BlobEvent {
        if let StoredLayout::Pithos(_) = &location.format.layout {
            return BlobEvent::Error(super::pithos::needs_bucket_key());
        }
        if let StoredLayout::Frames(layout) = &location.format.layout {
            let range = clamped_range(&range, location.blob_size);
            return Box::pin(self.read_frames(&location, layout, range)).await;
        }
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let stream_size = range_length(&range, location.blob_size);
        let reader = match timeout(self.io_timeout(), operator.reader(&storage_path)).await {
            Ok(Ok(reader)) => {
                match timeout(self.io_timeout(), reader.into_bytes_stream(range)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        return BlobEvent::Error(BlobError::ReadError(error.to_string()));
                    }
                    Err(_) => {
                        return BlobEvent::Error(BlobError::ReadError(
                            "timed out starting blob reader".to_string(),
                        ));
                    }
                }
            }
            Ok(Err(error)) => return BlobEvent::Error(BlobError::ReadError(error.to_string())),
            Err(_) => {
                return BlobEvent::Error(BlobError::ReadError(
                    "timed out opening blob reader".to_string(),
                ));
            }
        };

        let idle_timeout = self.transfer_idle_timeout();
        BlobEvent::ReadFinished {
            blob: BackendStream::new(stream::try_unfold(
                (BackendStream::new(reader), idle_timeout),
                move |(mut stream, idle_timeout)| async move {
                    match next_chunk(&mut stream, idle_timeout).await? {
                        Some(Ok(bytes)) => Ok(Some((bytes, (stream, idle_timeout)))),
                        Some(Err(error)) => Err(BlobError::ReadError(error.to_string())),
                        None => Ok(None),
                    }
                },
            )),
            stream_size,
        }
    }

    pub async fn read_hidden_range(
        &self,
        location: BackendLocation,
        range: std::ops::Range<u64>,
    ) -> BlobEvent {
        if let Err(error) = HiddenBlobKey::try_from(&location) {
            return BlobEvent::Error(BlobError::ConversionError(error));
        }
        if range.start > range.end || range.end > location.blob_size {
            return BlobEvent::Error(BlobError::ReadError(
                "hidden blob range is outside the stored size".to_string(),
            ));
        }
        let stream_size = range.end - range.start;
        match self.read_blob_range(location, range).await {
            BlobEvent::ReadFinished { blob, .. } => BlobEvent::HiddenRead { blob, stream_size },
            other => other,
        }
    }

    pub async fn delete_hidden_blob(&self, key: HiddenBlobKey) -> BlobEvent {
        let operator = match self.operator_from_hidden(&key) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        let storage_path = match key.get_storage_path() {
            Ok(path) => path,
            Err(error) => return BlobEvent::Error(BlobError::ConversionError(error)),
        };
        if let Err(error) = self.delete_path(&operator, &storage_path).await {
            return BlobEvent::Error(BlobError::DeleteError(error.to_string()));
        }
        if let Err(error) = self.release_hidden(&key).await {
            return BlobEvent::Error(error);
        }
        BlobEvent::HiddenDeleted
    }

    /// Returns one bounded page and an opaque cursor for the next source.
    pub async fn list_hidden_blobs(
        &self,
        namespace: Option<Ulid>,
        cursor: Option<Vec<u8>>,
    ) -> BlobEvent {
        let cursor = match cursor {
            Some(cursor) => match postcard::from_bytes::<HiddenCursor>(&cursor) {
                Ok(cursor) => cursor,
                Err(error) => return BlobEvent::Error(BlobError::ConversionError(error.into())),
            },
            None => match self.hidden_backends() {
                Ok(backends) => match backends.first() {
                    Some(backend) => HiddenCursor::Objects {
                        backend: backend.clone(),
                        bucket: None,
                        start_after: None,
                    },
                    None => match self.list_reservation_page(namespace, None).await {
                        Ok((entries, next_cursor)) => {
                            return BlobEvent::HiddenListed {
                                entries,
                                next_cursor,
                            };
                        }
                        Err(error) => return BlobEvent::Error(error),
                    },
                },
                Err(error) => return BlobEvent::Error(error),
            },
        };
        let result = match cursor {
            HiddenCursor::Objects {
                backend,
                bucket,
                start_after,
            } => {
                self.list_object_page(namespace, backend, bucket, start_after)
                    .await
            }
            HiddenCursor::Reservations { start_after } => {
                self.list_reservation_page(namespace, start_after).await
            }
        };
        match result {
            Ok((entries, next_cursor)) => BlobEvent::HiddenListed {
                entries,
                next_cursor,
            },
            Err(error) => BlobEvent::Error(error),
        }
    }

    async fn list_object_page(
        &self,
        namespace: Option<Ulid>,
        mut backend: BackendRef,
        mut bucket: Option<String>,
        mut start_after: Option<String>,
    ) -> Result<(Vec<HiddenBlobEntry>, Option<Vec<u8>>), BlobError> {
        let backends = self.hidden_backends()?;
        if backends.is_empty() {
            return self.list_reservation_page(namespace, None).await;
        }
        if !backends.contains(&backend) {
            let Some(next) =
                next_backend(&backends, &backend).or_else(|| backends.first().cloned())
            else {
                return self.list_reservation_page(namespace, None).await;
            };
            backend = next;
            bucket = None;
            start_after = None;
        }
        for _ in 0..HIDDEN_SOURCE_HOPS {
            let bucket_name = match bucket.take() {
                Some(bucket) => bucket,
                None => match self.hidden_bucket_after(&backend, None).await {
                    Ok(Some(bucket)) => bucket,
                    Ok(None) => {
                        if let Some(next) = next_backend(&backends, &backend) {
                            backend = next;
                            start_after = None;
                            continue;
                        }
                        return self.list_reservation_page(namespace, None).await;
                    }
                    Err(error) => {
                        tracing::warn!(%backend, %error, "hidden backend bucket listing failed");
                        return self.skip_backend(namespace, &backends, &backend).await;
                    }
                },
            };
            let page = match self
                .list_bucket_page(&backend, &bucket_name, namespace, start_after.as_deref())
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(%backend, bucket = %bucket_name, %error, "hidden backend page failed");
                    return self.skip_backend(namespace, &backends, &backend).await;
                }
            };
            let last_path = page.last().map(|entry| entry.path().to_string());
            let mut entries = Vec::with_capacity(page.len());
            let root = self.registry.config_for(&backend)?.root.clone();
            let prefix = hidden_prefix(namespace);
            for entry in &page {
                if entry.metadata().mode() != EntryMode::FILE {
                    continue;
                }
                let listed_path = entry.path();
                let backend_path = Path::new(listed_path)
                    .strip_prefix(Path::new(&bucket_name))
                    .map_err(|_| {
                        BlobError::ListError("hidden blob path is outside bucket".to_string())
                    })?
                    .to_str()
                    .ok_or_else(|| {
                        BlobError::ListError("hidden blob path is not valid utf-8".to_string())
                    })?
                    .to_string();
                if !backend_path.starts_with(&prefix) {
                    continue;
                }
                let key = HiddenBlobKey::new(
                    backend.clone(),
                    root.clone(),
                    bucket_name.clone(),
                    backend_path,
                )
                .map_err(BlobError::ConversionError)?;
                let modified_at = entry
                    .metadata()
                    .last_modified()
                    .map(Into::into)
                    .or_else(|| hidden_timestamp(&key.backend_path));
                entries.push(HiddenBlobEntry { key, modified_at });
            }
            let next = if page.len() >= HIDDEN_LIST_PAGE {
                HiddenCursor::Objects {
                    backend,
                    bucket: Some(bucket_name),
                    start_after: last_path,
                }
            } else if let Some(next_bucket) = match self
                .hidden_bucket_after(&backend, Some(&bucket_name))
                .await
            {
                Ok(next_bucket) => next_bucket,
                Err(error) => {
                    tracing::warn!(%backend, bucket = %bucket_name, %error, "hidden bucket continuation failed");
                    return self.skip_backend(namespace, &backends, &backend).await;
                }
            } {
                HiddenCursor::Objects {
                    backend,
                    bucket: Some(next_bucket),
                    start_after: None,
                }
            } else if let Some(next_backend) = next_backend(&backends, &backend) {
                HiddenCursor::Objects {
                    backend: next_backend,
                    bucket: None,
                    start_after: None,
                }
            } else {
                HiddenCursor::Reservations { start_after: None }
            };
            if !entries.is_empty() {
                return Ok((entries, Some(encode_cursor(next)?)));
            }
            match next {
                HiddenCursor::Objects {
                    backend: next_backend,
                    bucket: next_bucket,
                    start_after: next_start_after,
                } => {
                    backend = next_backend;
                    bucket = next_bucket;
                    start_after = next_start_after;
                }
                HiddenCursor::Reservations { start_after } => {
                    return self.list_reservation_page(namespace, start_after).await;
                }
            }
        }
        Ok((
            Vec::new(),
            Some(encode_cursor(HiddenCursor::Objects {
                backend,
                bucket,
                start_after,
            })?),
        ))
    }

    async fn skip_backend(
        &self,
        namespace: Option<Ulid>,
        backends: &[BackendRef],
        backend: &BackendRef,
    ) -> Result<(Vec<HiddenBlobEntry>, Option<Vec<u8>>), BlobError> {
        if let Some(next) = next_backend(backends, backend) {
            return Ok((
                Vec::new(),
                Some(encode_cursor(HiddenCursor::Objects {
                    backend: next,
                    bucket: None,
                    start_after: None,
                })?),
            ));
        }
        self.list_reservation_page(namespace, None).await
    }

    async fn list_bucket_page(
        &self,
        backend: &BackendRef,
        bucket: &str,
        namespace: Option<Ulid>,
        start_after: Option<&str>,
    ) -> Result<Vec<opendal::Entry>, BlobError> {
        let operator = self
            .registry
            .bucket_operator(backend, bucket, &self.egress)?;
        let storage_prefix = format!("{bucket}/{}", hidden_prefix(namespace));
        let mut request = operator
            .lister_with(&storage_prefix)
            .recursive(true)
            .limit(HIDDEN_LIST_PAGE);
        if let Some(start_after) = start_after {
            request = request.start_after(start_after);
        }
        tokio::time::timeout(self.io_timeout(), async move {
            let lister = request
                .await
                .map_err(|error| BlobError::ListError(error.to_string()))?;
            take_page(lister).await
        })
        .await
        .map_err(|_| BlobError::ListError("timed out listing hidden blobs".to_string()))?
    }

    async fn list_reservation_page(
        &self,
        namespace: Option<Ulid>,
        start_after: Option<Vec<u8>>,
    ) -> Result<(Vec<HiddenBlobEntry>, Option<Vec<u8>>), BlobError> {
        let event = tokio::time::timeout(
            self.io_timeout(),
            self.storage
                .send_effect(Effect::Storage(StorageEffect::Iter {
                    key_space: aruna_core::keyspaces::HIDDEN_RESERVATION_KEYSPACE.to_string(),
                    prefix: None,
                    start: start_after.map(|key| IterStart::After(key.into())),
                    limit: HIDDEN_LIST_PAGE,
                    txn_id: None,
                })),
        )
        .await
        .map_err(|_| BlobError::ListError("timed out listing hidden reservations".to_string()))?;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return Err(BlobError::ListError(
                "unexpected hidden reservation iteration event".to_string(),
            ));
        };
        let mut entries = Vec::with_capacity(values.len());
        for (key, _) in values {
            let key: HiddenBlobKey = postcard::from_bytes(key.as_ref())
                .map_err(|error| BlobError::ConversionError(error.into()))?;
            if namespace.is_some_and(|namespace| key.namespace().ok() != Some(namespace)) {
                continue;
            }
            entries.push(HiddenBlobEntry {
                modified_at: hidden_timestamp(&key.backend_path),
                key,
            });
        }
        let next = next_start_after
            .map(|key| {
                encode_cursor(HiddenCursor::Reservations {
                    start_after: Some(key.to_vec()),
                })
            })
            .transpose()?;
        Ok((entries, next))
    }

    fn hidden_backends(&self) -> Result<Vec<BackendRef>, BlobError> {
        let mut backends = self
            .registry
            .entries()
            .map(|(name, _)| BackendRef::Node(name.clone()))
            .collect::<Vec<_>>();
        if backends.len() > HIDDEN_BACKEND_LIMIT {
            return Err(BlobError::ListError(
                "hidden blob backend count exceeds limit".to_string(),
            ));
        }
        backends.sort();
        Ok(backends)
    }

    pub async fn delete_blob(&self, location: BackendLocation) -> BlobEvent {
        // An admitted read or keyless work still uses the archive; the cleanup row retries.
        // The claim keeps new reads out until the backend delete ends.
        let _claim = match location.format.layout {
            StoredLayout::Pithos(_) => match self.claim_delete(&ArchiveKey::of(&location)) {
                Ok(claim) => Some(claim),
                Err(error) => return BlobEvent::Error(error),
            },
            _ => None,
        };
        self.clear_active(location.ulid);
        // An in-place part is no object: its provider upload holds the bytes until it settles.
        // Deleting it ends its claim: it was rolled back, or a staged record replaced it.
        if location.partial {
            self.release_claim(location.ulid);
            return BlobEvent::DeleteFinished;
        }
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        // A retried cleanup must not decrement the load a second time.
        match timeout(self.io_timeout(), operator.stat(&storage_path)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                if let Err(error) = self.release_reservation(&location).await {
                    return BlobEvent::Error(error);
                }
                return BlobEvent::DeleteFinished;
            }
            Ok(Err(error)) => {
                return BlobEvent::Error(BlobError::DeleteError(error.to_string()));
            }
            Err(_) => {
                return BlobEvent::Error(BlobError::DeleteError(
                    "timed out checking blob before deletion".to_string(),
                ));
            }
        }
        match timeout(self.io_timeout(), operator.delete(&storage_path)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {}
            Ok(Err(error)) => return BlobEvent::Error(BlobError::DeleteError(error.to_string())),
            Err(_) => {
                return BlobEvent::Error(BlobError::DeleteError(
                    "timed out deleting blob".to_string(),
                ));
            }
        }
        if let Err(err) = self.release_reservation(&location).await {
            return BlobEvent::Error(err);
        }
        BlobEvent::DeleteFinished
    }
}

/// OpenDAL's list limit is only a per-request hint, so one page is bounded
/// here instead of buffering every entry the bucket holds.
async fn take_page(lister: opendal::Lister) -> Result<Vec<opendal::Entry>, BlobError> {
    lister
        .take(HIDDEN_LIST_PAGE)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| BlobError::ListError(error.to_string()))
}

fn hidden_prefix(namespace: Option<Ulid>) -> String {
    match namespace {
        Some(namespace) => format!("{HIDDEN_BLOB_PREFIX}/{namespace}/"),
        None => format!("{HIDDEN_BLOB_PREFIX}/"),
    }
}

fn encode_cursor(cursor: HiddenCursor) -> Result<Vec<u8>, BlobError> {
    postcard::to_allocvec(&cursor).map_err(|error| BlobError::ConversionError(error.into()))
}

fn next_backend(backends: &[BackendRef], current: &BackendRef) -> Option<BackendRef> {
    backends.iter().find(|backend| *backend > current).cloned()
}

fn hidden_timestamp(path: &str) -> Option<SystemTime> {
    let suffix = Path::new(path).file_name()?.to_str()?.rsplit_once('_')?.1;
    let ulid = Ulid::from_string(suffix).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_millis(ulid.timestamp_ms()))
}

#[cfg(test)]
mod tests {
    use super::{HIDDEN_LIST_PAGE, next_chunk, take_page};
    use aruna_core::errors::BlobError;
    use aruna_core::stream::BackendStream;
    use bytes::Bytes;
    use futures::stream;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn times_out_read() {
        let mut stream = BackendStream::new(stream::pending::<Result<Bytes, std::io::Error>>());
        let task =
            tokio::spawn(async move { next_chunk(&mut stream, Duration::from_secs(1)).await });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        assert!(matches!(
            task.await.unwrap(),
            Err(BlobError::ReadError(message)) if message == "blob read idle timeout"
        ));
    }

    #[tokio::test]
    async fn page_stays_bounded() {
        // A bucket holding more than one page must not be buffered whole: the
        // opendal list limit is only a per-request hint.
        let dir = tempfile::tempdir().unwrap();
        let operator =
            opendal::Operator::from_iter::<opendal::services::Fs>(std::collections::HashMap::from(
                [("root".to_string(), dir.path().to_str().unwrap().to_string())],
            ))
            .unwrap()
            .finish();
        for index in 0..HIDDEN_LIST_PAGE + 5 {
            operator
                .write(&format!("hidden/blob-{index:04}"), Bytes::from_static(b"x"))
                .await
                .unwrap();
        }

        let listed = operator
            .list_with("hidden/")
            .recursive(true)
            .limit(HIDDEN_LIST_PAGE)
            .await
            .unwrap();
        assert!(listed.len() > HIDDEN_LIST_PAGE);

        let lister = operator
            .lister_with("hidden/")
            .recursive(true)
            .limit(HIDDEN_LIST_PAGE)
            .await
            .unwrap();

        assert_eq!(take_page(lister).await.unwrap().len(), HIDDEN_LIST_PAGE);
    }
}
