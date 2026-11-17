//! Tests the retirement of a processed copy job by state and event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::errors::StorageError;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
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

fn txn() -> TxnId {
    Ulid::from_parts(8, 8)
}

fn read_result(value: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: value.map(Into::into),
    })
}

fn read(key_space: &str, key: Vec<u8>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: Some(txn()),
    })
}

fn started() -> Event {
    Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
}

fn commit() -> Effect {
    Effect::Storage(StorageEffect::CommitTransaction { txn_id: txn() })
}

fn committed() -> Event {
    Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
}

fn begin() -> [Effect; 1] {
    [Effect::Storage(StorageEffect::StartTransaction {
        read: false,
    })]
}

#[test]
fn completed_job_journals() {
    // The job's own row goes, and the completed sync job journals one event in the same commit.
    let mut operation =
        RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Completed, 5);
    let delete = Effect::Storage(StorageEffect::BatchDelete {
        deletes: vec![(REPLICATION_JOB_KEYSPACE.to_string(), b"row".to_vec().into())],
        txn_id: Some(txn()),
    });
    let relationship_key = sync_relationship_key("bucket", Ulid::from_parts(7, 7));
    let steps = vec![
        (
            started(),
            vec![read(REPLICATION_JOB_KEYSPACE, b"row".to_vec())],
        ),
        (
            read_result(Some(job().to_bytes().unwrap())),
            vec![read(RELATIONSHIP_OUT_KEYSPACE, relationship_key)],
        ),
        (
            read_result(Some(relationship(SyncState::Enabled))),
            vec![read(S3_BUCKET_KEYSPACE, b"bucket".to_vec())],
        ),
        (read_result(Some(bucket())), vec![delete]),
    ];
    check_trace(&mut operation, &begin(), steps);
    let deleted = Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    });
    let journal = operation.step(deleted);
    assert!(matches!(
        journal.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { writes, .. })]
            if writes.iter().all(|(space, _, _)| space == EVENT_JOURNAL_KEYSPACE)
    ));
    let written = Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    });
    assert_eq!(operation.step(written).as_slice(), [commit()]);
    assert!(operation.step(committed()).is_empty());
    assert_eq!(operation.finalize(), Ok(()));
}

#[test]
fn rewritten_row_stays() {
    // A row an enqueue wrote again names another job, so only the commit follows.
    let mut operation =
        RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
    let mut other = job();
    other.job_id = Ulid::from_parts(9, 9);
    let steps = vec![
        (
            started(),
            vec![read(REPLICATION_JOB_KEYSPACE, b"row".to_vec())],
        ),
        (read_result(Some(other.to_bytes().unwrap())), vec![commit()]),
        (committed(), vec![]),
    ];
    check_trace(&mut operation, &begin(), steps);
    assert_eq!(operation.finalize(), Ok(()));
}

#[test]
fn stopped_failure_silent() {
    // A failed job of a paused relationship reports through the relationship status instead.
    let mut operation = RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Failed, 5);
    operation.start();
    operation.step(started());
    operation.step(read_result(None));
    operation.step(read_result(Some(relationship(SyncState::Paused))));
    assert_eq!(
        operation.step(read_result(Some(bucket()))).as_slice(),
        [commit()]
    );
}

#[test]
fn errors_abort_transaction() {
    let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
    let timeout = || {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    };
    let mut reading = RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
    reading.start();
    reading.step(started());
    assert_eq!(reading.step(timeout()).as_slice(), [abort]);
    assert_eq!(
        reading.finalize(),
        Err(BlobQueueError::Storage(StorageError::Timeout))
    );
    let mut committing =
        RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
    committing.start();
    committing.step(started());
    committing.step(read_result(None));
    assert!(committing.step(timeout()).is_empty());
    assert!(committing.finalize().is_err());
}

#[test]
fn retire_contract() {
    let reading = || {
        let mut operation =
            RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
        operation.start();
        operation.step(started());
        operation
    };
    let refused = rejects_events(reading, sample_events, |event| {
        matches!(event, Event::Storage(StorageEvent::ReadResult { .. }))
    });
    let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
    assert!(
        refused
            .iter()
            .all(|(_, effects, _)| effects.as_slice() == [abort.clone()])
    );
    let mut open = reading();
    check_abort(&mut open, &[abort]);
    rejects_finalize(open);
    let mut fresh = RetireJobOperation::new(b"row".to_vec(), job(), &BlobJobOutcome::Retired, 5);
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
}
