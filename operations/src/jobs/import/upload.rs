//! Stages an uploaded RO-Crate archive as a hidden blob and tracks its claim and cleanup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::keyspaces::{ROCRATE_UPLOAD_KEYSPACE, UPLOAD_CLEANUP_KEYSPACE};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{
    JobId, RoCrateMediaType, RoCrateUploadCleanup, RoCrateUploadRecord, upload_key,
};
use aruna_core::structs::storage::blob::HiddenBlobKey;
use bytes::Bytes;
use byteview::ByteView;
use std::time::{Duration, Instant};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub struct CreateRoCrateConfig {
    pub upload_id: Ulid,
    pub owner: UserId,
    pub media_type: RoCrateMediaType,
    pub expires_at_ms: u64,
    pub max_bytes: u64,
    pub deadline: Option<Instant>,
    pub blob: BackendStream<Result<Bytes, StreamError>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateRoCrateError {
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("upload record is invalid: {0}")]
    Invalid(String),
    #[error("RO-Crate upload creation was aborted")]
    Aborted,
    #[error("RO-Crate upload creation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: &'static str,
        expected: &'static str,
        got: &'static str,
    },
}

}

#[derive(Debug, PartialEq)]
pub struct CreateRoCrateOperation {
    upload_id: Ulid,
    owner: UserId,
    media_type: RoCrateMediaType,
    expires_at_ms: u64,
    max_bytes: u64,
    deadline: Option<Instant>,
    blob: Option<BackendStream<Result<Bytes, StreamError>>>,
    record: Option<RoCrateUploadRecord>,
    hidden_key: Option<HiddenBlobKey>,
}

impl CreateRoCrateOperation {
    pub fn new(config: CreateRoCrateConfig) -> Self {
        Self {
            upload_id: config.upload_id,
            owner: config.owner,
            media_type: config.media_type,
            expires_at_ms: config.expires_at_ms,
            max_bytes: config.max_bytes,
            deadline: config.deadline,
            blob: Some(config.blob),
            record: None,
            hidden_key: None,
        }
    }

            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }
        }
    }

        }
                key: TaskKey::SweepHiddenBlobs,
                after: Duration::ZERO,
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UploadClaimError {
    #[error("upload not found")]
    NotFound,
    #[error("upload is owned by another user")]
    WrongOwner,
    #[error("upload expired")]
    Expired,
    #[error("upload is already claimed")]
    AlreadyClaimed,
    #[error("upload record is invalid: {0}")]
    Invalid(String),
    #[error("upload storage failed: {0}")]
    Storage(String),
}

/// Claims `record` for job `job_id` of `owner`. A claimed upload stays with its job, also after it
/// expires; an unclaimed one is claimable until it expires. Returns whether the record changed.
pub(crate) fn claim_upload(
    record: &mut RoCrateUploadRecord,
    owner: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<bool, UploadClaimError> {
    if record.owner != owner {
        return Err(UploadClaimError::WrongOwner);
    }
    match record.claimed_by {
        Some(existing) if existing != job_id => Err(UploadClaimError::AlreadyClaimed),
        Some(_) => Ok(false),
        None if record.expires_at_ms <= now_ms => Err(UploadClaimError::Expired),
        None => {
            record.claimed_by = Some(job_id);
            Ok(true)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::jobs::import::upload_store::{
        claim_rocrate_upload, read_rocrate_upload, write_rocrate_upload,
    };
    use aruna_core::structs::execution::job::RoCrateMediaType;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_storage::FjallStorage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn upload_operation() -> (CreateRoCrateOperation, BackendLocation) {
        upload_operation_with(None)
    }

    fn upload_operation_with(
        deadline: Option<Instant>,
    ) -> (CreateRoCrateOperation, BackendLocation) {
        let owner = UserId::nil(RealmId::from_bytes([1u8; 32]));
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: format!("_jobs/{upload_id}/input"),
            ulid: Ulid::from_bytes([3u8; 16]),
            format: StoredFormat::default(),
            created_by: owner,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 7,
            hashes: HashMap::new(),
        };
        let operation = CreateRoCrateOperation::new(CreateRoCrateConfig {
            upload_id,
            owner,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms: 20,
            max_bytes: 10,
            deadline,
            blob: BackendStream::new(
                futures_util::stream::empty::<Result<Bytes, std::io::Error>>(),
            ),
        });
        (operation, location)
    }

    #[test]
    fn carries_deadline() {
        let deadline = Instant::now();
        let (mut operation, _) = upload_operation_with(Some(deadline));
        else {
            panic!("expected hidden spool effect")
        };
    }

    #[test]
    fn creates_upload() {
        let (mut operation, location) = upload_operation();
            panic!("expected upload record write")
        };
    }

    #[test]
        );
    }

    #[test]
        let (mut operation, location) = upload_operation();
        assert!(matches!(
        ));
    }

    #[tokio::test]
    async fn reclaims_expired_upload() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner = UserId::nil(realm_id);
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let job_id = JobId::from_bytes([3u8; 16]);
        let record = RoCrateUploadRecord {
            upload_id,
            owner,
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: "_jobs/input".to_string(),
                ulid: Ulid::from_bytes([4u8; 16]),
                format: StoredFormat::default(),
                created_by: owner,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: HashMap::new(),
            },
            blake3: [5u8; 32],
            size: 1,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms: 10,
            claimed_by: None,
        };
        write_rocrate_upload(&context, &record).await.unwrap();
        claim_rocrate_upload(&context, upload_id, owner, job_id, 9)
            .await
            .unwrap();

        let reclaimed = claim_rocrate_upload(&context, upload_id, owner, job_id, 11)
            .await
            .unwrap();

        assert_eq!(reclaimed.claimed_by, Some(job_id));
        assert!(matches!(
            claim_rocrate_upload(&context, upload_id, owner, JobId::from_bytes([6u8; 16]), 11)
                .await,
            Err(UploadClaimError::AlreadyClaimed)
        ));
    }

    pub(crate) fn upload_record(
        owner: UserId,
        upload_id: Ulid,
        expires_at_ms: u64,
    ) -> RoCrateUploadRecord {
        RoCrateUploadRecord {
            upload_id,
            owner,
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: "_jobs/input".to_string(),
                ulid: Ulid::from_bytes([4u8; 16]),
                format: StoredFormat::default(),
                created_by: owner,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: HashMap::new(),
            },
            blake3: [5u8; 32],
            size: 1,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms,
            claimed_by: None,
        }
    }

    #[tokio::test]
    async fn reads_missing_upload() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let upload_id = Ulid::from_bytes([2u8; 16]);
        assert!(
            read_rocrate_upload(&context, upload_id)
                .await
                .unwrap()
                .is_none()
        );
        let owner = UserId::nil(RealmId::from_bytes([1u8; 32]));
        write_rocrate_upload(&context, &upload_record(owner, upload_id, 10))
            .await
            .unwrap();
        assert_eq!(
            read_rocrate_upload(&context, upload_id)
                .await
                .unwrap()
                .unwrap()
                .owner,
            owner
        );
    }

    #[tokio::test]
    async fn claim_rejects_owner() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner = UserId::nil(realm_id);
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let job_id = JobId::from_bytes([3u8; 16]);
        assert!(matches!(
            claim_rocrate_upload(&context, upload_id, owner, job_id, 5).await,
            Err(UploadClaimError::NotFound)
        ));
        write_rocrate_upload(&context, &upload_record(owner, upload_id, 100))
            .await
            .unwrap();
        let intruder = UserId::new(Ulid::from_bytes([7u8; 16]), realm_id);
        assert!(matches!(
            claim_rocrate_upload(&context, upload_id, intruder, job_id, 5).await,
            Err(UploadClaimError::WrongOwner)
        ));
    }

    #[tokio::test]
    async fn claim_rejects_expired() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let owner = UserId::nil(RealmId::from_bytes([1u8; 32]));
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let job_id = JobId::from_bytes([3u8; 16]);
        write_rocrate_upload(&context, &upload_record(owner, upload_id, 10))
            .await
            .unwrap();
        assert!(matches!(
            claim_rocrate_upload(&context, upload_id, owner, job_id, 10).await,
            Err(UploadClaimError::Expired)
        ));
    }

    #[tokio::test]
    async fn deletes_claimed_upload() {
        // Cleanup deletes only when the claiming job matches; an absent upload is a no-op.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let owner = UserId::nil(RealmId::from_bytes([1u8; 32]));
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let job_id = JobId::from_bytes([3u8; 16]);
        delete_rocrate_upload(&context, upload_id, job_id)
            .await
            .unwrap();
        write_rocrate_upload(&context, &upload_record(owner, upload_id, 100))
            .await
            .unwrap();
        claim_rocrate_upload(&context, upload_id, owner, job_id, 5)
            .await
            .unwrap();
        let intruder = JobId::from_bytes([8u8; 16]);
        assert!(
            delete_rocrate_upload(&context, upload_id, intruder)
                .await
                .is_err()
        );
        delete_rocrate_upload(&context, upload_id, job_id)
            .await
            .unwrap();
        assert!(
            read_rocrate_upload(&context, upload_id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
