use super::BlobHandler;
use super::backend::{
    build_backend_path, build_hidden_path, build_multipart_part_path, intent_key, intent_value,
};
use super::group::GROUP_WRITE_CHUNK;
use crate::hash::Hasher;
use crate::opendal::abort_partial_writer;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::BLOB_LOCATIONS_KEYSPACE;
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobLocationKey, HIDDEN_BLOB_PREFIX, HiddenBlobEntry,
    HiddenBlobKey, MultipartUploadPartKey, ResolvedBackend,
};
use aruna_core::types::UserId;
use bytes::Bytes;
use futures::{StreamExt, stream};
use opendal::{EntryMode, ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::time::{Instant, timeout, timeout_at};
use ulid::Ulid;

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

/// Tenant writers open with an explicit chunk so a small-chunk stream cannot
/// exhaust a provider's per-object block ceiling.
async fn open_writer(
    operator: &Operator,
    path: &str,
    backend: &BackendRef,
) -> Result<opendal::Writer, opendal::Error> {
    match backend {
        BackendRef::Group(_) => operator.writer_with(path).chunk(GROUP_WRITE_CHUNK).await,
        BackendRef::Node(_) => operator.writer(path).await,
    }
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

struct HiddenReservation {
    handler: BlobHandler,
    key: Arc<StdMutex<Option<HiddenBlobKey>>>,
    location: Option<BackendLocation>,
    operator: Option<Operator>,
    storage_path: Option<String>,
    writer: Option<opendal::Writer>,
    uncertain: bool,
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

    async fn fail(&mut self, error: BlobError) -> BlobEvent {
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
    }

    async fn fail_close(&mut self, error: BlobError) -> BlobEvent {
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
    }

    async fn abort(&mut self) -> Result<(), BlobError> {
        let cleanup = if self.writer.is_some() {
            let cleanup = {
                let Some(writer) = self.writer.as_mut() else {
                    return Err(BlobError::DeleteError(
                        "hidden writer is missing".to_string(),
                    ));
                };
                self.handler.abort_writer(writer).await
            };
            if cleanup.is_ok() {
                self.writer = None;
            }
            cleanup
        } else if !self.uncertain
            && let (Some(operator), Some(storage_path)) =
                (self.operator.as_ref(), self.storage_path.as_deref())
        {
            self.handler.delete_path(operator, storage_path).await
        } else {
            Ok(())
        };
        cleanup?;
        let Some(key) = self.key.lock().ok().and_then(|key| key.clone()) else {
            return Ok(());
        };
        if self.uncertain {
            return Ok(());
        }
        self.handler.release_hidden(&key).await?;
        if let Ok(mut current) = self.key.lock() {
            *current = None;
        }
        Ok(())
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
        let writer = self.writer.take();
        let uncertain = self.uncertain;
        runtime.spawn(async move {
            let cleanup = match writer {
                Some(mut writer) => handler.abort_writer(&mut writer).await,
                None => Ok(()),
            };
            if let Err(error) = cleanup {
                tracing::error!(%error, "failed to clean cancelled hidden blob");
                return;
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
    pub(super) async fn write_stream_to_location(
        &self,
        location: BackendLocation,
        operator: Operator,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        Box::pin(self.write_stream_limit(location, operator, blob, None, None, None)).await
    }

    async fn write_stream_limit(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
        max_bytes: Option<u64>,
        deadline: Option<StdInstant>,
        reservation: Option<&mut HiddenReservation>,
    ) -> BlobEvent {
        let mut plain = HiddenReservation::new(self.clone());
        let reservation = reservation.unwrap_or(&mut plain);
        reservation.set_location(location.clone());
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return reservation.fail(e).await,
        };
        reservation.set_operator(operator.clone(), storage_path.clone());
        match with_deadline(
            deadline,
            open_writer(&operator, &storage_path, &location.backend),
        )
        .await
        {
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

        let mut hasher = Hasher::new();
        let mut bytes_written = 0u64;
        loop {
            let chunk = match with_deadline(deadline, blob.next()).await {
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
            let write = match reservation.writer_mut() {
                Some(writer) => with_deadline(deadline, writer.write(bytes.to_vec())).await,
                None => {
                    return reservation
                        .fail(BlobError::WriteError(
                            "hidden writer is missing".to_string(),
                        ))
                        .await;
                }
            };
            match write {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    return reservation
                        .fail(BlobError::WriteError(err.to_string()))
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
            bytes_written = next_size;
        }

        let close = match reservation.writer_mut() {
            Some(writer) => with_deadline(deadline, writer.close()).await,
            None => {
                return reservation
                    .fail(BlobError::WriteError(
                        "hidden writer is missing".to_string(),
                    ))
                    .await;
            }
        };
        match close {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                return reservation
                    .fail_close(BlobError::WriteError(err.to_string()))
                    .await;
            }
            Err(()) => {
                return reservation
                    .fail_close(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ))
                    .await;
            }
        }
        reservation.finish();
        location.blob_size = bytes_written;
        location.hashes = hasher.to_map();
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
            compressed: false,
            encrypted: false,
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
                max_bytes,
                deadline,
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

    async fn abort_writer(
        &self,
        writer: &mut opendal::Writer,
    ) -> Result<(), BlobError> {
        match timeout(self.control_plane_io_timeout(), writer.abort()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(BlobError::DeleteError(format!(
                "partial blob cleanup is uncertain: {error}"
            ))),
            Err(_) => Err(BlobError::DeleteError(
                "partial blob cleanup is uncertain: timed out aborting partial blob writer"
                    .to_string(),
            )),
        }
    }

    pub(super) async fn delete_path(
        &self,
        operator: &Operator,
        storage_path: &str,
    ) -> Result<(), BlobError> {
        match timeout(
            self.control_plane_io_timeout(),
            operator.delete(storage_path),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => Ok(()),
            Ok(Err(error)) => Err(BlobError::DeleteError(error.to_string())),
            Err(_) => Err(BlobError::DeleteError(
                "timed out deleting partial blob output".to_string(),
            )),
        }
    }

    async fn release_hidden(&self, key: &HiddenBlobKey) -> Result<(), BlobError> {
        match timeout(
            self.control_plane_io_timeout(),
            self.release_hidden_key(key),
        )
        .await
        {
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
        let hash = match location.get_blake3() {
            Some(hash) => hash
                .try_into()
                .map_err(|_| BlobError::ReadError("invalid reservation hash".to_string()))?,
            None if active => return Ok(false),
            None => {
                // Metadata is admitted only after the finalized marker is durable.
                let operator = self.operator_from_location(&location)?;
                let storage_path = location.get_storage_path()?;
                self.delete_path(&operator, &storage_path).await?;
                self.release_reservation(&location).await?;
                return Ok(true);
            }
        };
        let operator = self.operator_from_location(&location)?;
        let storage_path = location.get_storage_path()?;
        match operator.stat(&storage_path).await {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                self.release_reservation(&location).await?;
                return Ok(true);
            }
            Err(error) => return Err(BlobError::ReadError(error.to_string())),
        }

        let key = BlobLocationKey::new(hash, location.backend.clone());
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
        self.delete_path(&operator, &storage_path).await?;
        self.release_reservation(&location).await?;
        Ok(true)
    }

    pub async fn write_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
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
            compressed: false,
            encrypted: false,
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
        match self
            .write_stream_to_location(location.clone(), operator, blob)
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
        part: MultipartUploadPartKey,
        resolved: ResolvedBackend,
        created_by: UserId,
        compressed: bool,
        encrypted: bool,
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
            backend_path: build_multipart_part_path(part.upload_id, part.part_number, ulid),
            ulid,
            compressed,
            encrypted,
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
                .bucket_operator(&resolved.backend, &multipart_bucket, &self.egress)
            {
                Ok(op) => op,
                Err(err) => return BlobEvent::Error(err),
            };
        Box::pin(self.write_stream_to_location(location, operator, blob)).await
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
            compressed: false,
            encrypted: false,
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
        match self
            .compose_parts_to_location(location.clone(), operator, parts)
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

    pub(super) async fn compose_parts_to_location(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let mut writer = match timeout(
            self.control_plane_io_timeout(),
            open_writer(&operator, &storage_path, &location.backend),
        )
        .await
        {
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
        let compose_result: Result<u64, BlobError> = async {
            let mut bytes_written = 0u64;
            for part in parts {
                let part_operator = self.operator_from_location(&part)?;
                let part_storage_path = part.get_storage_path()?;
                let reader = timeout(
                    self.control_plane_io_timeout(),
                    part_operator.reader(&part_storage_path),
                )
                .await
                .map_err(|_| {
                    BlobError::ReadError("timed out opening compose reader".to_string())
                })?
                .map_err(|err| BlobError::ReadError(err.to_string()))?;
                let reader = timeout(
                    self.control_plane_io_timeout(),
                    reader.into_bytes_stream(..),
                )
                .await
                .map_err(|_| {
                    BlobError::ReadError("timed out starting compose reader".to_string())
                })?
                .map_err(|err| BlobError::ReadError(err.to_string()))?;

                let mut reader = BackendStream::new(reader);
                loop {
                    let chunk = timeout(self.transfer_idle_timeout(), reader.next())
                        .await
                        .map_err(|_| {
                            BlobError::ReadError("compose reader idle timeout".to_string())
                        })?;
                    let Some(chunk) = chunk else {
                        break;
                    };
                    let bytes = chunk.map_err(|err| BlobError::ReadError(err.to_string()))?;
                    hasher.update(&bytes);
                    timeout(self.transfer_idle_timeout(), writer.write(bytes.to_vec()))
                        .await
                        .map_err(|_| {
                            ambiguous = true;
                            BlobError::WriteError("compose writer idle timeout".to_string())
                        })?
                        .map_err(|err| BlobError::WriteError(err.to_string()))?;
                    bytes_written += bytes.len() as u64;
                }
            }
            timeout(self.transfer_idle_timeout(), writer.close())
                .await
                .map_err(|_| {
                    ambiguous = true;
                    BlobError::WriteError("compose close idle timeout".to_string())
                })?
                .map_err(|err| {
                    ambiguous = true;
                    BlobError::WriteError(err.to_string())
                })?;
            Ok(bytes_written)
        }
        .await;

        let bytes_written = match compose_result {
            Ok(bytes_written) => bytes_written,
            Err(err) => {
                let cleanup = abort_partial_writer(
                    &mut writer,
                    self.control_plane_io_timeout(),
                )
                .await;
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
        BlobEvent::WriteFinished { location }
    }

    pub async fn read_blob(&self, location: BackendLocation) -> BlobEvent {
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
        let reader = match operator.reader(&storage_path).await {
            Ok(r) => match r.into_bytes_stream(..).await {
                Ok(stream) => stream,
                Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
            },
            Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
        };

        let expected_size = location.blob_size;
        let blob = BackendStream::new(stream::try_unfold(
            (BackendStream::new(reader), Hasher::new(), 0u64),
            move |(mut stream, mut hasher, bytes_read)| async move {
                match stream.next().await {
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

                        if hasher.finalize().blake3.as_bytes() != &expected_blake3 {
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
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let reader = match operator.reader(&storage_path).await {
            Ok(r) => match r.into_bytes_stream(range).await {
                Ok(stream) => stream,
                Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
            },
            Err(e) => return BlobEvent::Error(BlobError::ReadError(e.to_string())),
        };

        BlobEvent::ReadFinished {
            blob: BackendStream::new(reader),
            stream_size: 0,
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
                    .ok_or_else(|| {
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
            .list_with(&storage_prefix)
            .recursive(true)
            .limit(HIDDEN_LIST_PAGE);
        if let Some(start_after) = start_after {
            request = request.start_after(start_after);
        }
        tokio::time::timeout(self.control_plane_io_timeout(), request)
            .await
            .map_err(|_| BlobError::ListError("timed out listing hidden blobs".to_string()))?
            .map_err(|error| BlobError::ListError(error.to_string()))
    }

    async fn list_reservation_page(
        &self,
        namespace: Option<Ulid>,
        start_after: Option<Vec<u8>>,
    ) -> Result<(Vec<HiddenBlobEntry>, Option<Vec<u8>>), BlobError> {
        let event = tokio::time::timeout(
            self.control_plane_io_timeout(),
            self.storage
                .send_effect(Effect::Storage(StorageEffect::Iter {
                    key_space: aruna_core::keyspaces::BLOB_HIDDEN_RESERVATION_KEYSPACE.to_string(),
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
        self.clear_active(location.ulid);
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        // A retried cleanup must not decrement the load a second time.
        match timeout(self.control_plane_io_timeout(), operator.stat(&storage_path)).await {
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
        match timeout(self.control_plane_io_timeout(), operator.delete(&storage_path)).await {
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
