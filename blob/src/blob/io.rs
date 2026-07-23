use super::BlobHandler;
use super::backend::{build_backend_path, build_hidden_path, build_multipart_part_path};
use crate::hash::Hasher;
use crate::opendal::{abort_partial_writer, init_backend_operator};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::{BackendLocation, HIDDEN_BLOB_PREFIX, HiddenBlobEntry, HiddenBlobKey};
use aruna_core::types::UserId;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::{EntryMode, ErrorKind, Operator};
use std::collections::HashMap;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;

impl BlobHandler {
    pub(super) async fn write_stream_to_location(
        &self,
        location: BackendLocation,
        operator: Operator,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        self.write_stream_limit(location, operator, blob, None)
            .await
    }

    async fn write_stream_limit(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
        max_bytes: Option<u64>,
    ) -> BlobEvent {
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let Ok(mut writer) = operator.writer(&storage_path).await else {
            return BlobEvent::Error(BlobError::OperatorCreationFailed(
                "Failed to create writer from operator".to_string(),
            ));
        };

        let mut hasher = Hasher::new();
        let mut bytes_written = 0u64;
        while let Some(chunk) = blob.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => {
                    abort_partial_writer(&mut writer, &operator, &storage_path).await;
                    return BlobEvent::Error(BlobError::StreamFailed(err.to_string()));
                }
            };
            let Some(next_size) = bytes_written.checked_add(bytes.len() as u64) else {
                abort_partial_writer(&mut writer, &operator, &storage_path).await;
                return BlobEvent::Error(BlobError::SizeLimitExceeded {
                    limit: max_bytes.unwrap_or(u64::MAX),
                });
            };
            if let Some(limit) = max_bytes
                && next_size > limit
            {
                abort_partial_writer(&mut writer, &operator, &storage_path).await;
                return BlobEvent::Error(BlobError::SizeLimitExceeded { limit });
            }
            hasher.update(&bytes);
            if let Err(err) = writer.write(bytes.to_vec()).await {
                abort_partial_writer(&mut writer, &operator, &storage_path).await;
                return BlobEvent::Error(BlobError::WriteError(err.to_string()));
            }
            bytes_written = next_size;
        }

        if let Err(err) = writer.close().await {
            abort_partial_writer(&mut writer, &operator, &storage_path).await;
            return BlobEvent::Error(BlobError::WriteError(err.to_string()));
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
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_hidden_path(namespace, name, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
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
        let operator = match init_backend_operator(self.backend_config.clone(), backend_bucket) {
            Ok(operator) => operator,
            Err(err) => return BlobEvent::Error(err),
        };
        let location = match self
            .write_stream_limit(location, operator, blob, max_bytes)
            .await
        {
            BlobEvent::WriteFinished { location } => location,
            other => return other,
        };
        if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
            return BlobEvent::Error(err);
        }
        let Some(hash) = location.get_blake3() else {
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "hidden blob hash is missing".to_string(),
            ));
        };
        let Ok(blake3) = hash.try_into() else {
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "hidden blob hash has an invalid length".to_string(),
            ));
        };
        BlobEvent::HiddenSpooled {
            size: location.blob_size,
            location,
            blake3,
        }
    }

    pub async fn write_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
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

        let operator = match init_backend_operator(self.backend_config.clone(), backend_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        match self
            .write_stream_to_location(location, operator, blob)
            .await
        {
            BlobEvent::WriteFinished { location } => {
                if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
                    BlobEvent::Error(err)
                } else {
                    BlobEvent::WriteFinished { location }
                }
            }
            other => other,
        }
    }

    pub async fn write_blob_part(
        &self,
        upload_id: Ulid,
        part_number: u16,
        created_by: UserId,
        compressed: bool,
        encrypted: bool,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let multipart_bucket = match self.multipart_bucket() {
            Ok(bucket) => bucket.to_string(),
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
            storage_bucket: multipart_bucket.clone(),
            backend_path: build_multipart_part_path(upload_id, part_number, ulid),
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
        let operator = match init_backend_operator(self.backend_config.clone(), multipart_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        self.write_stream_to_location(location, operator, blob)
            .await
    }

    pub async fn compose_blob(
        &self,
        request_bucket: &str,
        request_key: &str,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        let backend_path = match build_backend_path(request_bucket, request_key, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        let location = BackendLocation {
            root: self.backend_config.root.clone(),
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
        let operator = match init_backend_operator(self.backend_config.clone(), backend_bucket) {
            Ok(op) => op,
            Err(err) => return BlobEvent::Error(err),
        };
        match self
            .compose_parts_to_location(location, operator, parts)
            .await
        {
            BlobEvent::WriteFinished { location } => {
                if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
                    BlobEvent::Error(err)
                } else {
                    BlobEvent::WriteFinished { location }
                }
            }
            other => other,
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
        let Ok(mut writer) = operator.writer(&storage_path).await else {
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
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
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
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
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
        if let Err(error) = self.decrement_bucket_load(&key.storage_bucket).await {
            return BlobEvent::Error(error);
        }
        BlobEvent::HiddenDeleted
    }

    pub async fn list_hidden_blobs(&self, namespace: Option<Ulid>) -> BlobEvent {
        let buckets = match self.hidden_buckets().await {
            Ok(buckets) => buckets,
            Err(error) => return BlobEvent::Error(error),
        };
        let prefix = hidden_prefix(namespace);
        let mut entries = Vec::new();
        for bucket in buckets {
            let operator = match init_backend_operator(self.backend_config.clone(), bucket.clone())
            {
                Ok(operator) => operator,
                Err(error) => return BlobEvent::Error(error),
            };
            let storage_prefix = PathBuf::from(&bucket).join(&prefix);
            let Some(storage_prefix) = storage_prefix.to_str() else {
                return BlobEvent::Error(BlobError::ListError(
                    "hidden blob prefix is not valid utf-8".to_string(),
                ));
            };
            let mut lister = match operator.lister_with(storage_prefix).recursive(true).await {
                Ok(lister) => lister,
                Err(error) => return BlobEvent::Error(BlobError::ListError(error.to_string())),
            };
            loop {
                let entry = match lister.try_next().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(error) => {
                        return BlobEvent::Error(BlobError::ListError(error.to_string()));
                    }
                };
                if entry.metadata().mode() != EntryMode::FILE {
                    continue;
                }
                let listed_path = PathBuf::from(entry.path());
                let backend_path = match listed_path.strip_prefix(&bucket) {
                    Ok(path) => match path.to_str() {
                        Some(path) => path.to_string(),
                        None => {
                            return BlobEvent::Error(BlobError::ListError(
                                "hidden blob path is not valid utf-8".to_string(),
                            ));
                        }
                    },
                    Err(error) => {
                        return BlobEvent::Error(BlobError::ListError(error.to_string()));
                    }
                };
                let key = match HiddenBlobKey::new(
                    self.backend_config.root.clone(),
                    bucket.clone(),
                    backend_path,
                ) {
                    Ok(key) => key,
                    Err(error) => return BlobEvent::Error(BlobError::ConversionError(error)),
                };
                let modified_at = entry
                    .metadata()
                    .last_modified()
                    .map(Into::into)
                    .or_else(|| hidden_timestamp(&key.backend_path));
                entries.push(HiddenBlobEntry { key, modified_at });
            }
        }
        entries.sort_by(|left, right| {
            (&left.key.storage_bucket, &left.key.backend_path)
                .cmp(&(&right.key.storage_bucket, &right.key.backend_path))
        });
        BlobEvent::HiddenListed { entries }
    }

    pub async fn delete_blob(&self, location: BackendLocation) -> BlobEvent {
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        if let Err(e) = operator.delete(&storage_path).await {
            return BlobEvent::Error(BlobError::DeleteError(e.to_string()));
        }
        if let Err(err) = self.decrement_bucket_load(&location.storage_bucket).await {
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
