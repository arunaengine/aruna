//! Tests the retirement of a processed copy job by state and event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::IoError;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::storage::replication_queue::{ReplicateScopeInput, ReplicationMode};
use aruna_core::structs::{SyncMode, SyncStatusSnapshot};
use ulid::Ulid;

use super::*;

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn node(seed: u8) -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn user() -> UserId {
    UserId::local(Ulid::from_parts(1, 1), realm())
}

fn job() -> BlobJobRecord {
    let input = ReplicateScopeInput {
        bucket: "bucket".to_string(),
        target: ReplicateScopeTarget::Version {
            key: "key".to_string(),
            version_id: Ulid::from_parts(4, 4),
        },
        target_node_id: node(2),
        auth_context: AuthContext {
            user_id: user(),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        },
        replicate_delete_markers: true,
        mode: ReplicationMode::Live,
    };
    let mut record = BlobJobRecord::new_relationship(input, Some(false), Ulid::from_parts(7, 7), 1);
    record.job_id = Ulid::from_parts(6, 6);
    record
}

fn relationship(state: SyncState) -> Vec<u8> {
    SyncRelationship {
        id: Ulid::from_parts(7, 7),
        source: ArunaArn::s3_bucket(realm(), node(1), "bucket").unwrap(),
        target: ArunaArn::s3_bucket(realm(), node(2), "bucket").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: user(),
        created_at: SystemTime::UNIX_EPOCH,
        state,
        status: SyncStatusSnapshot::default(),
    }
    .to_bytes()
    .unwrap()
}

fn bucket() -> Vec<u8> {
    BucketInfo {
        group_id: Ulid::from_parts(3, 3),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: None,
    }
    .to_bytes()
    .unwrap()
}

fn read_result(value: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![value.map(Into::into)])
}

fn read(key_space: &str, key: Vec<u8>) -> Step {
    Ok(Next::Call(read_row(key_space, key)))
}

#[test]
fn completed_job_journals() {
    // The job's own row goes, and the completed sync job journals one event in the same commit.
    let mut operation =
        RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Completed, 5);
    assert_eq!(
        operation.transaction(),
        aruna_core::operation::TransactionMode::Write
    );
    let delete = IoCall::Delete(vec![(
        REPLICATION_JOB_KEYSPACE.to_string(),
        b"row".to_vec().into(),
    )]);
    let relationship_key = sync_relationship_key("bucket", Ulid::from_parts(7, 7));
    assert_eq!(
        operation.first(),
        read(REPLICATION_JOB_KEYSPACE, b"row".to_vec())
    );
    assert_eq!(
        operation.next(read_result(Some(job().to_bytes().unwrap()))),
        read(RELATIONSHIP_OUT_KEYSPACE, relationship_key)
    );
    assert_eq!(
        operation.next(read_result(Some(relationship(SyncState::Enabled)))),
        read(S3_BUCKET_KEYSPACE, b"bucket".to_vec())
    );
    assert_eq!(
        operation.next(read_result(Some(bucket()))),
        Ok(Next::Call(delete))
    );
    let journal = operation.next(IoResult::Done);
    assert!(matches!(
        &journal,
        Ok(Next::Call(IoCall::Write(writes)))
            if writes.iter().all(|(space, _, _)| space == EVENT_JOURNAL_KEYSPACE)
    ));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn rewritten_row_stays() {
    // A row an enqueue wrote again names another job, so the run ends without a write.
    let mut operation =
        RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
    let mut other = job();
    other.job_id = Ulid::from_parts(9, 9);
    let _ = operation.first();
    let stored = read_result(Some(other.to_bytes().unwrap()));
    assert_eq!(operation.next(stored), Ok(Next::Done(())));
}

#[test]
fn stopped_failure_silent() {
    // A failed job of a paused relationship reports through the relationship status instead.
    let mut operation = RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Failed, 5);
    let _ = operation.first();
    let _ = operation.next(read_result(None));
    let _ = operation.next(read_result(Some(relationship(SyncState::Paused))));
    assert_eq!(
        operation.next(read_result(Some(bucket()))),
        Ok(Next::Done(()))
    );
}

#[test]
fn errors_fail() {
    let error = BlobQueueError::from(IoError::Storage(StorageError::Timeout));
    assert_eq!(error, BlobQueueError::Storage(StorageError::Timeout));
    let mut reading = RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
    let _ = reading.first();
    assert!(matches!(
        reading.next(IoResult::Done),
        Err(BlobQueueError::Io(IoError::Unexpected { .. }))
    ));
}
