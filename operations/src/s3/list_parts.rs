use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    MultipartUpload, MultipartUploadPart, MultipartUploadPartKey, MultipartUploadStatus,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const PART_SCAN_LIMIT: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListPartsState {
    Init,
    StartTransaction,
    ReadUpload,
    ReadParts,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListPartsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListPartsState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified upload does not exist.")]
    NoSuchUpload,
    #[error("The specified multipart upload does not match the target object.")]
    UploadTargetMismatch,
    #[error("The multipart upload is no longer open.")]
    UploadNotOpen,
    #[error("ListParts failed")]
    ListPartsFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListPartsInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
    pub part_number_marker: Option<u16>,
    pub max_parts: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListPartsResult {
    pub upload: MultipartUpload,
    pub parts: Vec<MultipartUploadPart>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u16>,
}

#[derive(Debug, PartialEq)]
pub struct ListPartsOperation {
    input: ListPartsInput,
    state: ListPartsState,
    txn_id: Option<Ulid>,
    upload: Option<MultipartUpload>,
    output: Option<Result<ListPartsResult, ListPartsError>>,
}

impl ListPartsOperation {
    pub const DEFAULT_MAX_PARTS: usize = 1_000;

    pub fn new(input: ListPartsInput) -> Self {
        Self {
            input,
            state: ListPartsState::Init,
            txn_id: None,
            upload: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListPartsError) -> Effects {
        self.state = ListPartsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListPartsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListPartsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = ListPartsState::ReadUpload;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn validate_upload_record(&self, record: &MultipartUpload) -> Result<(), ListPartsError> {
        if record.bucket != self.input.bucket || record.key != self.input.key {
            return Err(ListPartsError::UploadTargetMismatch);
        }
        if record.status != MultipartUploadStatus::Open {
            return Err(ListPartsError::UploadNotOpen);
        }
        Ok(())
    }

    fn handle_upload_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(ListPartsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.emit_error(ListPartsError::NoSuchUpload);
        };
        let record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.emit_error(err.into()),
        };
        if let Err(err) = self.validate_upload_record(&record) {
            return self.emit_error(err);
        }
        self.upload = Some(record);

        let prefix = match MultipartUploadPartKey::prefix(self.input.upload_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = ListPartsState::ReadParts;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: PART_SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(ListPartsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let mut parts = Vec::with_capacity(values.len());
        for (_key, value) in values {
            let part = match MultipartUploadPart::from_bytes(value.as_ref()) {
                Ok(part) => part,
                Err(err) => return self.emit_error(err.into()),
            };
            parts.push(part);
        }
        // Part keys are varint-encoded, so iteration is not numeric order past
        // the 127/255 boundaries; sort in memory to restore ascending order.
        parts.sort_by_key(|part| part.part_number);
        if let Some(marker) = self.input.part_number_marker {
            parts.retain(|part| part.part_number > marker);
        }

        let is_truncated = parts.len() > self.input.max_parts;
        parts.truncate(self.input.max_parts);
        // With max_parts=0 the truncation empties `parts`, so fall back to the
        // marker preceding the first unreturned part (the request marker, or 0).
        let next_part_number_marker = is_truncated.then(|| {
            parts
                .last()
                .map(|part| part.part_number)
                .unwrap_or(self.input.part_number_marker.unwrap_or(0))
        });

        let Some(upload) = self.upload.take() else {
            return self.emit_error(ListPartsError::ListPartsFailed);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListPartsError::NoTransactionFound);
        };

        self.state = ListPartsState::CommitTransaction;
        self.output = Some(Ok(ListPartsResult {
            upload,
            parts,
            is_truncated,
            next_part_number_marker,
        }));
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListPartsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListPartsState::Finish;
        smallvec![]
    }
}

impl Operation for ListPartsOperation {
    type Output = Option<Result<ListPartsResult, ListPartsError>>;
    type Error = ListPartsError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListPartsError::StorageError(error));
        }

        match self.state {
            ListPartsState::Init => self.handle_init(),
            ListPartsState::StartTransaction => self.handle_transaction_started(event),
            ListPartsState::ReadUpload => self.handle_upload_read(event),
            ListPartsState::ReadParts => self.handle_parts_read(event),
            ListPartsState::CommitTransaction => self.handle_transaction_committed(event),
            ListPartsState::Finish | ListPartsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListPartsState::Finish | ListPartsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListPartsState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListPartsError::ListPartsFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::{BackendLocation, RealmId};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn driver_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn part_location() -> BackendLocation {
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "parts".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::r#gen(),
            compressed: false,
            encrypted: false,
            created_by: UserId::local(Ulid::r#gen(), RealmId::from_bytes([1u8; 32])),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 5,
            hashes: HashMap::new(),
        }
    }

    fn upload_record(upload_id: Ulid, bucket: &str, key: &str) -> MultipartUpload {
        MultipartUpload {
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id: Ulid::r#gen(),
            created_by: UserId::local(Ulid::r#gen(), RealmId::from_bytes([1u8; 32])),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
        }
    }

    async fn seed_upload(storage_handle: &storage::StorageHandle, record: &MultipartUpload) {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
                key: record.upload_id.to_bytes().to_vec().into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    async fn seed_part(storage_handle: &storage::StorageHandle, upload_id: Ulid, part_number: u16) {
        let record = MultipartUploadPart {
            part_number,
            location: part_location(),
            created_at: SystemTime::UNIX_EPOCH,
        };
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
                key: MultipartUploadPartKey::new(upload_id, part_number)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn list_parts_sorts_numerically_across_varint_boundary() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let upload_id = Ulid::r#gen();
        seed_upload(
            &storage_handle,
            &upload_record(upload_id, "bucket", "object"),
        )
        .await;
        // Insert scrambled and across the varint width boundary (128 spans two
        // bytes, and byte order would place 256 before 130).
        for part_number in [300u16, 1, 256, 130, 128, 100, 255, 127] {
            seed_part(&storage_handle, upload_id, part_number).await;
        }

        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                upload_id,
                part_number_marker: None,
                max_parts: ListPartsOperation::DEFAULT_MAX_PARTS,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let numbers: Vec<u16> = result.parts.iter().map(|part| part.part_number).collect();
        assert_eq!(numbers, vec![1, 100, 127, 128, 130, 255, 256, 300]);
        assert!(!result.is_truncated);
        assert_eq!(result.next_part_number_marker, None);
    }

    #[tokio::test]
    async fn list_parts_paginates_with_marker() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let upload_id = Ulid::r#gen();
        seed_upload(
            &storage_handle,
            &upload_record(upload_id, "bucket", "object"),
        )
        .await;
        for part_number in [1u16, 100, 128, 130, 256, 300] {
            seed_part(&storage_handle, upload_id, part_number).await;
        }

        let mut marker = None;
        let mut collected = Vec::new();
        loop {
            let result = drive(
                ListPartsOperation::new(ListPartsInput {
                    bucket: "bucket".to_string(),
                    key: "object".to_string(),
                    upload_id,
                    part_number_marker: marker,
                    max_parts: 2,
                }),
                &driver_ctx,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();

            collected.extend(result.parts.iter().map(|part| part.part_number));
            if result.is_truncated {
                assert!(result.next_part_number_marker.is_some());
                marker = result.next_part_number_marker;
            } else {
                assert_eq!(result.next_part_number_marker, None);
                break;
            }
        }

        assert_eq!(collected, vec![1, 100, 128, 130, 256, 300]);
    }

    #[tokio::test]
    async fn list_parts_zero_max_returns_resume_marker() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let upload_id = Ulid::r#gen();
        seed_upload(
            &storage_handle,
            &upload_record(upload_id, "bucket", "object"),
        )
        .await;
        for part_number in [1u16, 2, 3] {
            seed_part(&storage_handle, upload_id, part_number).await;
        }

        // No marker: resume marker is 0 so the client can restart from the top.
        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                upload_id,
                part_number_marker: None,
                max_parts: 0,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(result.parts.is_empty());
        assert!(result.is_truncated);
        assert_eq!(result.next_part_number_marker, Some(0));

        // With a marker the resume marker is preserved.
        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                upload_id,
                part_number_marker: Some(2),
                max_parts: 0,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(result.is_truncated);
        assert_eq!(result.next_part_number_marker, Some(2));
    }

    #[tokio::test]
    async fn list_parts_missing_upload_returns_no_such_upload() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle);

        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                upload_id: Ulid::r#gen(),
                part_number_marker: None,
                max_parts: ListPartsOperation::DEFAULT_MAX_PARTS,
            }),
            &driver_ctx,
        )
        .await;

        assert!(matches!(result, Err(ListPartsError::NoSuchUpload)));
    }

    #[tokio::test]
    async fn list_parts_target_mismatch_returns_error() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let upload_id = Ulid::r#gen();
        seed_upload(
            &storage_handle,
            &upload_record(upload_id, "bucket", "object"),
        )
        .await;

        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "other".to_string(),
                upload_id,
                part_number_marker: None,
                max_parts: ListPartsOperation::DEFAULT_MAX_PARTS,
            }),
            &driver_ctx,
        )
        .await;

        assert!(matches!(result, Err(ListPartsError::UploadTargetMismatch)));
    }

    #[tokio::test]
    async fn list_parts_non_open_upload_returns_error() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let upload_id = Ulid::r#gen();
        let mut record = upload_record(upload_id, "bucket", "object");
        record.status = MultipartUploadStatus::Completing;
        seed_upload(&storage_handle, &record).await;

        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                upload_id,
                part_number_marker: None,
                max_parts: ListPartsOperation::DEFAULT_MAX_PARTS,
            }),
            &driver_ctx,
        )
        .await;

        assert!(matches!(result, Err(ListPartsError::UploadNotOpen)));
    }
}
