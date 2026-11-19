//! Lists the parts already stored for one multipart upload, one page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::multipart::{
    MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

const PART_SCAN_LIMIT: usize = 10_000;

#[derive(Debug, Error, PartialEq)]
pub enum ListPartsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified upload does not exist.")]
    NoSuchUpload,
    #[error("The specified multipart upload does not match the target object.")]
    UploadTargetMismatch,
    #[error("The multipart upload is no longer open.")]
    UploadNotOpen,
    #[error("ListParts failed: {0}")]
    Io(IoError),
}

impl From<IoError> for ListPartsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
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
    pub parts: Vec<MultipartPart>,
    pub is_truncated: bool,
    pub next_part_marker: Option<u16>,
}

type Step = Result<Next<ListPartsResult>, ListPartsError>;

/// Reads the upload record and its parts in one read snapshot.
#[derive(Debug, PartialEq)]
pub struct ListPartsOperation {
    input: ListPartsInput,
    /// The upload record once read; the part page follows.
    upload: Option<MultipartUpload>,
}

impl ListPartsOperation {
    pub const DEFAULT_MAX_PARTS: usize = 1_000;

    pub fn new(input: ListPartsInput) -> Self {
        Self {
            input,
            upload: None,
        }
    }

    fn validate_upload_record(&self, record: &MultipartUpload) -> Result<(), ListPartsError> {
        if record.bucket != self.input.bucket || record.key != self.input.key {
            return Err(ListPartsError::UploadTargetMismatch);
        }
        // A running completion still owns its parts: a retry lists them and joins it.
        if record.status == MultipartUploadStatus::Aborting {
            return Err(ListPartsError::UploadNotOpen);
        }
        Ok(())
    }

    fn upload_read(&mut self, value: Option<Value>) -> Step {
        let value = value.ok_or(ListPartsError::NoSuchUpload)?;
        let record = MultipartUpload::from_bytes(value.as_ref())?;
        self.validate_upload_record(&record)?;
        self.upload = Some(record);
        let prefix = MultipartPartKey::prefix(self.input.upload_id)?;
        Ok(Next::Call(IoCall::Page {
            space: UPLOAD_PART_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: PART_SCAN_LIMIT,
        }))
    }

    fn parts_read(&self, upload: MultipartUpload, values: Vec<(Key, Value)>) -> Step {
        let mut parts = (values.into_iter())
            .map(|(_, value)| MultipartPart::from_bytes(value.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
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
        let next_part_marker = is_truncated.then(|| {
            parts
                .last()
                .map(|part| part.part_number)
                .unwrap_or(self.input.part_number_marker.unwrap_or(0))
        });
        Ok(Next::Done(ListPartsResult {
            upload,
            parts,
            is_truncated,
            next_part_marker,
        }))
    }
}

impl Operation for ListPartsOperation {
    type Output = ListPartsResult;
    type Error = ListPartsError;

    fn first(&mut self) -> Step {
        let key = self.input.upload_id.to_bytes().to_vec();
        Ok(Next::Call(read_row(UPLOAD_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.upload.take() {
            Some(upload) => self.parts_read(upload, result.page()?.0),
            None => self.upload_read(result.value()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::drive;
    use crate::testing::test_context;
    use crate::tests::s3::seed_upload;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn part_location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "parts".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::generate(),
            format: StoredFormat::default(),
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 5,
            hashes: HashMap::new(),
        }
    }

    fn upload_record(upload_id: Ulid, bucket: &str, key: &str) -> MultipartUpload {
        MultipartUpload {
            backend: BackendRef::node_default(),
            storage_class: None,
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id: Ulid::generate(),
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: None,
            encryption: None,
        }
    }

    async fn seed_part(storage_handle: &storage::StorageHandle, upload_id: Ulid, part_number: u16) {
        let record = MultipartPart {
            part_number,
            location: part_location(),
            created_at: SystemTime::UNIX_EPOCH,
            backend_etag: None,
            piece: None,
        };
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: UPLOAD_PART_KEYSPACE.to_string(),
                key: MultipartPartKey::new(upload_id, part_number)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn varint_parts_sorted() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle.clone());

        let upload_id = Ulid::generate();
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
        .unwrap();

        let numbers: Vec<u16> = result.parts.iter().map(|part| part.part_number).collect();
        assert_eq!(numbers, vec![1, 100, 127, 128, 130, 255, 256, 300]);
        assert!(!result.is_truncated);
        assert_eq!(result.next_part_marker, None);
    }

    #[tokio::test]
    async fn marker_paginates() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle.clone());

        let upload_id = Ulid::generate();
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
            .unwrap();

            collected.extend(result.parts.iter().map(|part| part.part_number));
            if result.is_truncated {
                assert!(result.next_part_marker.is_some());
                marker = result.next_part_marker;
            } else {
                assert_eq!(result.next_part_marker, None);
                break;
            }
        }

        assert_eq!(collected, vec![1, 100, 128, 130, 256, 300]);
    }

    #[tokio::test]
    async fn zero_limit_resumes() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle.clone());

        let upload_id = Ulid::generate();
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
        .unwrap();
        assert!(result.parts.is_empty());
        assert!(result.is_truncated);
        assert_eq!(result.next_part_marker, Some(0));

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
        .unwrap();
        assert!(result.is_truncated);
        assert_eq!(result.next_part_marker, Some(2));
    }

    #[tokio::test]
    async fn missing_upload_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle);

        let result = drive(
            ListPartsOperation::new(ListPartsInput {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                upload_id: Ulid::generate(),
                part_number_marker: None,
                max_parts: ListPartsOperation::DEFAULT_MAX_PARTS,
            }),
            &driver_ctx,
        )
        .await;

        assert!(matches!(result, Err(ListPartsError::NoSuchUpload)));
    }

    #[tokio::test]
    async fn target_mismatch_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle.clone());

        let upload_id = Ulid::generate();
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
    async fn completing_upload_listed() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle.clone());

        let upload_id = Ulid::generate();
        let mut record = upload_record(upload_id, "bucket", "object");
        record.status = MultipartUploadStatus::Completing;
        record.completing_since_ms = Some(1);
        seed_upload(&storage_handle, &record).await;
        seed_part(&storage_handle, upload_id, 1).await;
        seed_part(&storage_handle, upload_id, 2).await;

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
        .unwrap();

        let numbers: Vec<u16> = result.parts.iter().map(|part| part.part_number).collect();
        assert_eq!(numbers, vec![1, 2]);
    }

    #[tokio::test]
    async fn closed_upload_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = test_context(storage_handle.clone());

        let upload_id = Ulid::generate();
        let mut record = upload_record(upload_id, "bucket", "object");
        record.status = MultipartUploadStatus::Aborting;
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
