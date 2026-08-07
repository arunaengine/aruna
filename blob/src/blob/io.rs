use super::BlobHandler;
use super::backend::{build_backend_path, build_hidden_path, build_multipart_part_path};
use super::group::GROUP_WRITE_CHUNK;
use crate::hash::Hasher;
use crate::opendal::abort_partial_writer;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::{
    BackendLocation, BackendRef, HIDDEN_BLOB_PREFIX, HiddenBlobEntry, HiddenBlobKey,
    MultipartUploadPartKey, ResolvedBackend,
};
use aruna_core::types::UserId;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::{EntryMode, ErrorKind, Operator};
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, timeout, timeout_at};
use ulid::Ulid;

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

impl BlobHandler {
    pub(super) async fn write_stream_to_location(
        &self,
        location: BackendLocation,
        operator: Operator,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        Box::pin(self.write_stream_limit(location, operator, blob, None, None)).await
    }

    async fn write_stream_limit(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
        max_bytes: Option<u64>,
        deadline: Option<StdInstant>,
    ) -> BlobEvent {
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let mut writer = match with_deadline(
            deadline,
            open_writer(&operator, &storage_path, &location.backend),
        )
        .await
        {
            Ok(Ok(writer)) => writer,
            Ok(Err(_)) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(
                    "Failed to create writer from operator".to_string(),
                ));
            }
            Err(()) => {
                self.delete_path(&operator, &storage_path).await;
                return BlobEvent::Error(BlobError::WriteError(
                    "blob write deadline expired".to_string(),
                ));
            }
        };

        let mut hasher = Hasher::new();
        let mut bytes_written = 0u64;
        loop {
            let chunk = match with_deadline(deadline, blob.next()).await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(()) => {
                    self.abort_writer(&mut writer, &operator, &storage_path)
                        .await;
                    return BlobEvent::Error(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ));
                }
            };
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => {
                    self.abort_writer(&mut writer, &operator, &storage_path)
                        .await;
                    return BlobEvent::Error(BlobError::StreamFailed(err.to_string()));
                }
            };
            let Some(next_size) = bytes_written.checked_add(bytes.len() as u64) else {
                self.abort_writer(&mut writer, &operator, &storage_path)
                    .await;
                return BlobEvent::Error(BlobError::SizeLimitExceeded {
                    limit: max_bytes.unwrap_or(u64::MAX),
                });
            };
            if let Some(limit) = max_bytes
                && next_size > limit
            {
                self.abort_writer(&mut writer, &operator, &storage_path)
                    .await;
                return BlobEvent::Error(BlobError::SizeLimitExceeded { limit });
            }
            hasher.update(&bytes);
            match with_deadline(deadline, writer.write(bytes.to_vec())).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    self.abort_writer(&mut writer, &operator, &storage_path)
                        .await;
                    return BlobEvent::Error(BlobError::WriteError(err.to_string()));
                }
                Err(()) => {
                    self.abort_writer(&mut writer, &operator, &storage_path)
                        .await;
                    return BlobEvent::Error(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ));
                }
            }
            bytes_written = next_size;
        }

        match with_deadline(deadline, writer.close()).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                self.abort_writer(&mut writer, &operator, &storage_path)
                    .await;
                return BlobEvent::Error(BlobError::WriteError(err.to_string()));
            }
            Err(()) => {
                self.abort_writer(&mut writer, &operator, &storage_path)
                    .await;
                return BlobEvent::Error(BlobError::WriteError(
                    "blob write deadline expired".to_string(),
                ));
            }
        }
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
        let backend_bucket =
            match with_deadline(deadline, self.eval_backend_bucket(&resolved.backend)).await {
                Ok(Ok(bucket)) => bucket,
                Ok(Err(err)) => return BlobEvent::Error(err),
                Err(()) => {
                    return BlobEvent::Error(BlobError::WriteError(
                        "blob write deadline expired".to_string(),
                    ));
                }
            };
        let ulid = Ulid::generate();
        let backend_path = match build_hidden_path(namespace, name, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
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
                Err(err) => return BlobEvent::Error(err),
            };
        let location = match self
            .write_stream_limit(location, operator, blob, max_bytes, deadline)
            .await
        {
            BlobEvent::WriteFinished { location } => location,
            other => return other,
        };
        let Some(hash) = location.get_blake3() else {
            let _ = self.discard_hidden(&location).await;
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "hidden blob hash is missing".to_string(),
            ));
        };
        let Ok(blake3) = hash.try_into() else {
            let _ = self.discard_hidden(&location).await;
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "hidden blob hash has an invalid length".to_string(),
            ));
        };
        match with_deadline(
            deadline,
            self.increment_bucket_load(&location.backend, &location.storage_bucket),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let cleanup = timeout(
                    self.control_plane_io_timeout(),
                    self.discard_hidden(&location),
                )
                .await;
                if !matches!(&cleanup, Ok(Ok(())))
                    && let (Ok(operator), Ok(storage_path)) = (
                        self.operator_from_location(&location),
                        location.get_storage_path(),
                    )
                {
                    self.delete_path(&operator, &storage_path).await;
                }
                if let Ok(Err(cleanup)) = cleanup {
                    return BlobEvent::Error(cleanup);
                }
                return BlobEvent::Error(err);
            }
            Err(()) => {
                if let (Ok(operator), Ok(storage_path)) = (
                    self.operator_from_location(&location),
                    location.get_storage_path(),
                ) {
                    self.delete_path(&operator, &storage_path).await;
                }
                return BlobEvent::Error(BlobError::WriteError(
                    "blob write deadline expired".to_string(),
                ));
            }
        }
        BlobEvent::HiddenSpooled {
            size: location.blob_size,
            location,
            blake3,
        }
    }

    async fn abort_writer(
        &self,
        writer: &mut opendal::Writer,
        operator: &Operator,
        storage_path: &str,
    ) {
        if timeout(
            self.control_plane_io_timeout(),
            abort_partial_writer(writer, operator, storage_path),
        )
        .await
        .is_err()
        {
            self.delete_path(operator, storage_path).await;
        }
    }

    async fn delete_path(&self, operator: &Operator, storage_path: &str) {
        match timeout(
            self.control_plane_io_timeout(),
            operator.delete(storage_path),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, storage_path, "failed to delete partial blob output");
            }
            Err(_) => {
                tracing::warn!(storage_path, "timed out deleting partial blob output");
            }
        }
    }

    async fn discard_hidden(&self, location: &BackendLocation) -> Result<(), BlobError> {
        let operator = self.operator_from_location(location)?;
        let storage_path = location.get_storage_path()?;
        operator
            .delete(&storage_path)
            .await
            .map_err(|error| BlobError::DeleteError(error.to_string()))
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
        // Reserved before the write so a stats failure never orphans bytes.
        let backend_bucket = match Box::pin(self.reserve_bucket(&resolved.backend)).await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => {
                self.release_bucket(&resolved.backend, &backend_bucket)
                    .await;
                return BlobEvent::Error(BlobError::ConversionError(err));
            }
        };
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
                Ok(op) => op,
                Err(err) => {
                    self.release_bucket(&resolved.backend, &backend_bucket)
                        .await;
                    return BlobEvent::Error(err);
                }
            };
        match self
            .write_stream_to_location(location, operator, blob)
            .await
        {
            BlobEvent::WriteFinished { location } => BlobEvent::WriteFinished { location },
            other => {
                self.release_bucket(&resolved.backend, &backend_bucket)
                    .await;
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
        // Reserved before the compose so a stats failure never orphans bytes.
        let backend_bucket = match Box::pin(self.reserve_bucket(&resolved.backend)).await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => {
                self.release_bucket(&resolved.backend, &backend_bucket)
                    .await;
                return BlobEvent::Error(BlobError::ConversionError(err));
            }
        };
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
                Ok(op) => op,
                Err(err) => {
                    self.release_bucket(&resolved.backend, &backend_bucket)
                        .await;
                    return BlobEvent::Error(err);
                }
            };
        match self
            .compose_parts_to_location(location, operator, parts)
            .await
        {
            BlobEvent::WriteFinished { location } => BlobEvent::WriteFinished { location },
            other => {
                self.release_bucket(&resolved.backend, &backend_bucket)
                    .await;
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
        let Ok(mut writer) = open_writer(&operator, &storage_path, &location.backend).await else {
            return BlobEvent::Error(BlobError::OperatorCreationFailed(
                "Failed to create writer from operator".to_string(),
            ));
        };

        let mut hasher = Hasher::new();
        let compose_result: Result<u64, BlobError> = async {
            let mut bytes_written = 0u64;
            for part in parts {
                let part_operator = self.operator_from_location(&part)?;
                let part_storage_path = part.get_storage_path()?;
                let reader = part_operator
                    .reader(&part_storage_path)
                    .await
                    .map_err(|err| BlobError::ReadError(err.to_string()))?
                    .into_bytes_stream(..)
                    .await
                    .map_err(|err| BlobError::ReadError(err.to_string()))?;

                let mut reader = BackendStream::new(reader);
                while let Some(chunk) = reader.next().await {
                    let bytes = chunk.map_err(|err| BlobError::ReadError(err.to_string()))?;
                    hasher.update(&bytes);
                    writer
                        .write(bytes.to_vec())
                        .await
                        .map_err(|err| BlobError::WriteError(err.to_string()))?;
                    bytes_written += bytes.len() as u64;
                }
            }
            writer
                .close()
                .await
                .map_err(|err| BlobError::WriteError(err.to_string()))?;
            Ok(bytes_written)
        }
        .await;

        let bytes_written = match compose_result {
            Ok(bytes_written) => bytes_written,
            Err(err) => {
                abort_partial_writer(&mut writer, &operator, &storage_path).await;
                return BlobEvent::Error(err);
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
        match operator.stat(&storage_path).await {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return BlobEvent::HiddenDeleted;
            }
            Err(error) => return BlobEvent::Error(BlobError::DeleteError(error.to_string())),
        }
        if let Err(error) = operator.delete(&storage_path).await {
            return BlobEvent::Error(BlobError::DeleteError(error.to_string()));
        }
        if let Err(error) = self
            .decrement_bucket_load(&key.backend, &key.storage_bucket)
            .await
        {
            return BlobEvent::Error(error);
        }
        BlobEvent::HiddenDeleted
    }

    /// Sweeps every registered node backend: a demoted default keeps serving its
    /// stamped objects, so its crash leftovers must stay reachable too.
    pub async fn list_hidden_blobs(&self, namespace: Option<Ulid>) -> BlobEvent {
        let prefix = hidden_prefix(namespace);
        let backends: Vec<BackendRef> = self
            .registry
            .entries()
            .map(|(name, _)| BackendRef::Node(name.clone()))
            .collect();
        let mut entries = Vec::new();
        for backend in backends {
            if let Err(error) = self.collect_hidden(&backend, &prefix, &mut entries).await {
                return BlobEvent::Error(error);
            }
        }
        entries.sort_by(|left, right| {
            (
                &left.key.backend,
                &left.key.storage_bucket,
                &left.key.backend_path,
            )
                .cmp(&(
                    &right.key.backend,
                    &right.key.storage_bucket,
                    &right.key.backend_path,
                ))
        });
        BlobEvent::HiddenListed { entries }
    }

    async fn collect_hidden(
        &self,
        backend: &BackendRef,
        prefix: &str,
        entries: &mut Vec<HiddenBlobEntry>,
    ) -> Result<(), BlobError> {
        let root = self.registry.config_for(backend)?.root.clone();
        for bucket in self.hidden_buckets(backend).await? {
            let operator = self
                .registry
                .bucket_operator(backend, &bucket, &self.egress)?;
            let storage_prefix = PathBuf::from(&bucket).join(prefix);
            let Some(storage_prefix) = storage_prefix.to_str() else {
                return Err(BlobError::ListError(
                    "hidden blob prefix is not valid utf-8".to_string(),
                ));
            };
            let mut lister = operator
                .lister_with(storage_prefix)
                .recursive(true)
                .await
                .map_err(|error| BlobError::ListError(error.to_string()))?;
            loop {
                let entry = match lister.try_next().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(error) => return Err(BlobError::ListError(error.to_string())),
                };
                if entry.metadata().mode() != EntryMode::FILE {
                    continue;
                }
                let listed_path = PathBuf::from(entry.path());
                let backend_path = listed_path
                    .strip_prefix(&bucket)
                    .map_err(|error| BlobError::ListError(error.to_string()))?
                    .to_str()
                    .ok_or_else(|| {
                        BlobError::ListError("hidden blob path is not valid utf-8".to_string())
                    })?
                    .to_string();
                let key =
                    HiddenBlobKey::new(backend.clone(), root.clone(), bucket.clone(), backend_path)
                        .map_err(BlobError::ConversionError)?;
                let modified_at = entry
                    .metadata()
                    .last_modified()
                    .map(Into::into)
                    .or_else(|| hidden_timestamp(&key.backend_path));
                entries.push(HiddenBlobEntry { key, modified_at });
            }
        }
        Ok(())
    }

    pub async fn delete_blob(&self, location: BackendLocation) -> BlobEvent {
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        // A retried cleanup must not decrement the load a second time.
        match operator.stat(&storage_path).await {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return BlobEvent::DeleteFinished;
            }
            Err(error) => return BlobEvent::Error(BlobError::DeleteError(error.to_string())),
        }
        if let Err(e) = operator.delete(&storage_path).await {
            return BlobEvent::Error(BlobError::DeleteError(e.to_string()));
        }
        if let Err(err) = self
            .decrement_bucket_load(&location.backend, &location.storage_bucket)
            .await
        {
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

fn hidden_timestamp(path: &str) -> Option<SystemTime> {
    let suffix = Path::new(path).file_name()?.to_str()?.rsplit_once('_')?.1;
    let ulid = Ulid::from_string(suffix).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_millis(ulid.timestamp_ms()))
}
