use super::BlobHandler;
use super::backend::{build_backend_path, build_multipart_part_path};
use crate::hash::Hasher;
use crate::opendal::{abort_partial_writer, init_backend_operator};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::BackendLocation;
use aruna_core::types::UserId;
use bytes::Bytes;
use futures::{StreamExt, stream};
use opendal::Operator;
use std::collections::HashMap;
use std::ops::RangeBounds;
use std::time::SystemTime;
use ulid::Ulid;

impl BlobHandler {
    pub(super) async fn write_stream_to_location(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
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
                    abort_partial_writer(&mut writer).await;
                    return BlobEvent::Error(BlobError::WriteError(err.to_string()));
                }
            };
            hasher.update(&bytes);
            if let Err(err) = writer.write(bytes.to_vec()).await {
                abort_partial_writer(&mut writer).await;
                return BlobEvent::Error(BlobError::WriteError(err.to_string()));
            }
            bytes_written += bytes.len() as u64;
        }

        if let Err(err) = writer.close().await {
            abort_partial_writer(&mut writer).await;
            return BlobEvent::Error(BlobError::WriteError(err.to_string()));
        }
        location.blob_size = bytes_written;
        location.hashes = hasher.to_map();
        BlobEvent::WriteFinished { location }
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
        let ulid = Ulid::new();
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
        let ulid = Ulid::new();
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
        let ulid = Ulid::new();
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
                abort_partial_writer(&mut writer).await;
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
