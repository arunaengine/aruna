//! Stages an uploaded RO-Crate archive as a hidden blob and tracks its claim and cleanup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{ROCRATE_UPLOAD_KEYSPACE, UPLOAD_CLEANUP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{
    JobId, RoCrateMediaType, RoCrateUploadCleanup, RoCrateUploadRecord, upload_key,
};
use aruna_core::structs::storage::blob::HiddenBlobKey;
use aruna_core::task::{TaskEffect, TaskKey};
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

impl From<IoError> for CreateRoCrateError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "call",
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::Aborted,
            IoError::Codec(error) => Self::Invalid(error),
        }
    }
}

type Step = Result<Next<RoCrateUploadRecord>, CreateRoCrateError>;

/// Spools the upload into a hidden blob and records it. A run that fails after the spool
/// deletes the record and the blob, and leaves a cleanup row the hidden-blob sweep finishes.
#[derive(Debug, PartialEq)]
pub struct CreateRoCrateOperation {
    upload_id: Ulid,
    owner: UserId,
    media_type: RoCrateMediaType,
    expires_at_ms: u64,
    max_bytes: u64,
    deadline: Option<Instant>,
    blob: Option<BackendStream<Result<Bytes, StreamError>>>,
    /// The record being written for the spooled blob.
    record: Option<RoCrateUploadRecord>,
    /// The spooled blob until the record holding it is written.
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

    /// Writes the record of the spooled blob.
    fn spooled(&mut self, event: Event) -> Step {
        let location = match event {
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }) => (location, blake3, size),
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            other => {
                return Err(CreateRoCrateError::UnexpectedEvent {
                    state: "spool",
                    expected: "Event::Blob(BlobEvent::HiddenSpooled | BlobEvent::Error)",
                    got: other.label(),
                });
            }
        };
        let (location, blake3, size) = location;
        self.hidden_key = Some(HiddenBlobKey::try_from(&location).map_err(BlobError::from)?);
        let record = RoCrateUploadRecord {
            upload_id: self.upload_id,
            owner: self.owner,
            location,
            blake3,
            size,
            media_type: self.media_type,
            expires_at_ms: self.expires_at_ms,
            claimed_by: None,
        };
        let value = postcard::to_allocvec(&record)
            .map_err(|error| CreateRoCrateError::Invalid(error.to_string()))?;
        let row = (
            ROCRATE_UPLOAD_KEYSPACE.to_string(),
            upload_key(self.upload_id),
            ByteView::from(value),
        );
        self.record = Some(record);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for CreateRoCrateOperation {
    type Output = RoCrateUploadRecord;
    type Error = CreateRoCrateError;

    fn first(&mut self) -> Step {
        let blob = self.blob.take().ok_or(CreateRoCrateError::NotFinished)?;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(
            BlobEffect::SpoolHidden {
                namespace: self.upload_id,
                name: "input".to_string(),
                created_by: self.owner,
                max_bytes: Some(self.max_bytes),
                deadline: self.deadline,
                blob,
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match (self.record.take(), result) {
            (None, IoResult::Event(event)) => self.spooled(*event),
            (Some(record), IoResult::Done) => {
                self.hidden_key = None;
                Ok(Next::Done(record))
            }
            (_, other) => Err(CreateRoCrateError::UnexpectedEvent {
                state: "call",
                expected: "the spool event or the record write",
                got: other.label(),
            }),
        }
    }

    /// Removes what a failed run left: the record, then the blob, with a cleanup row first so
    /// the sweep finishes whatever of it does not land.
    fn abort(&mut self) -> Vec<IoCall> {
        let Some(hidden_key) = self.hidden_key.take() else {
            return Vec::new();
        };
        let mut calls = vec![IoCall::Delete(vec![(
            ROCRATE_UPLOAD_KEYSPACE.to_string(),
            upload_key(self.upload_id),
        )])];
        let obligation = RoCrateUploadCleanup {
            upload_id: self.upload_id,
            hidden_key: hidden_key.clone(),
        };
        if let Ok(value) = obligation.to_bytes() {
            calls.push(IoCall::Write(vec![(
                UPLOAD_CLEANUP_KEYSPACE.to_string(),
                upload_key(self.upload_id),
                ByteView::from(value),
            )]));
        }
        calls.push(IoCall::Effect(Box::new(Effect::Blob(
            BlobEffect::DeleteHidden { key: hidden_key },
        ))));
        calls.push(IoCall::Effect(Box::new(Effect::Task(
            TaskEffect::ShortenTimer {
                key: TaskKey::SweepHiddenBlobs,
                after: Duration::ZERO,
            },
        ))));
        calls
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

    fn spooled(location: BackendLocation) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3: [4u8; 32],
            size: 7,
        })))
    }

    #[test]
    fn carries_deadline() {
        let deadline = Instant::now();
        let (mut operation, _) = upload_operation_with(Some(deadline));
        let Ok(Next::Call(IoCall::Effect(effect))) = operation.first() else {
            panic!("expected hidden spool call")
        };
        let Effect::Blob(BlobEffect::SpoolHidden {
            deadline: actual, ..
        }) = *effect
        else {
            panic!("expected hidden spool effect")
        };
        assert_eq!(actual, Some(deadline));
    }

    #[test]
    fn creates_upload() {
        let (mut operation, location) = upload_operation();
        operation.first().expect("spool call");
        let Ok(Next::Call(IoCall::Write(rows))) = operation.next(spooled(location)) else {
            panic!("expected upload record write")
        };
        let record: RoCrateUploadRecord = postcard::from_bytes(rows[0].2.as_ref()).unwrap();
        assert_eq!(rows[0].0, ROCRATE_UPLOAD_KEYSPACE);
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(record)));
        // A written record keeps its blob, so nothing is cleaned up afterwards.
        assert!(operation.abort().is_empty());
    }

    // A run that fails before the spool finished has no blob to clean up.
    #[test]
    fn spool_failure_clean() {
        let (mut operation, _) = upload_operation();
        operation.first().expect("spool call");
        let failed = IoResult::Event(Box::new(Event::Blob(BlobEvent::Error(
            BlobError::ChannelClosed,
        ))));
        assert_eq!(
            operation.next(failed),
            Err(CreateRoCrateError::Blob(BlobError::ChannelClosed))
        );
        assert!(operation.abort().is_empty());
    }

    // A run that fails after the spool removes the record and the blob, and leaves a cleanup
    // row and a sweep for whatever of that does not land.
    #[test]
    fn abort_cleans_spool() {
        let (mut operation, location) = upload_operation();
        operation.first().expect("spool call");
        operation.next(spooled(location)).expect("record write");
        let calls = operation.abort();
        assert!(matches!(
            &calls[..],
            [
                IoCall::Delete(records),
                IoCall::Write(cleanups),
                IoCall::Effect(blob),
                IoCall::Effect(timer),
            ] if records[0].0 == ROCRATE_UPLOAD_KEYSPACE
                && cleanups[0].0 == UPLOAD_CLEANUP_KEYSPACE
                && matches!(**blob, Effect::Blob(BlobEffect::DeleteHidden { .. }))
                && matches!(
                    **timer,
                    Effect::Task(TaskEffect::ShortenTimer {
                        key: TaskKey::SweepHiddenBlobs,
                        ..
                    })
                )
        ));
        assert!(operation.abort().is_empty());
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
