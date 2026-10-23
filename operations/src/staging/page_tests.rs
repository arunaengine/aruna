//! Tests the page import transitions: guards, replay, newer writes, usage and the checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::purge_fence::PurgeFenceError;
use crate::staging::descriptor::build_source_binding;
use crate::staging::page_rows::{GUARD_READS, MAX_PAGE_ENTRIES};
use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
    STAGING_STATE_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    JobClaim, JobId, JobPayload, JobRecord, StagingJobCheckpoint, StagingJobPhase,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::{
    SourceConnector, SourceConnectorKind, SourceConnectorSecret,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, CurrentVersionPointer};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use ulid::Ulid;

const JOB_CREATED_MS: u64 = 1_000_000;

fn token() -> Ulid {
    Ulid::from_bytes([7; 16])
}

fn txn() -> TxnId {
    Ulid::from_bytes([9; 16])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[4; 32]).public()
}

fn connector() -> SourceConnector {
    SourceConnector::new(
        Ulid::from_bytes([2; 16]),
        Ulid::from_bytes([1; 16]),
        "public-data".to_string(),
        SourceConnectorKind::S3,
        HashMap::from([("bucket".to_string(), "public".to_string())]),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        Default::default(),
    )
}

fn bucket() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([1; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    }
}

fn observation(etag: &str) -> SourceMetadata {
    SourceMetadata {
        content_length: 3,
        content_type: None,
        etag: Some(format!("\"{etag}\"")),
        last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(60)),
        source_version: None,
        digests: Default::default(),
    }
}

fn entry(path: &str) -> PageEntry {
    PageEntry {
        source_path: format!("data/{path}"),
        target_key: path.to_string(),
        observation: observation(path),
    }
}

fn checkpoint() -> StagingJobCheckpoint {
    StagingJobCheckpoint {
        phase: StagingJobPhase::Registering,
        flat: Some(true),
        items_done: 0,
        prefixes_done: 1,
        walk: None,
        queue_head: 0,
        queue_tail: 0,
        pending_items: Vec::new(),
        pending_directories: Vec::new(),
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: None,
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        unknown_sizes: 0,
        current_path: None,
        errors: Vec::new(),
    }
}

fn input(entries: Vec<PageEntry>) -> PageImportInput {
    PageImportInput {
        job_id: JobId::from_bytes([5; 16]),
        claim_token: token(),
        realm_id: RealmId::from_bytes([3; 32]),
        group_id: bucket().group_id,
        user_id: UserId::default(),
        node_id: node(),
        bucket: "imports".to_string(),
        expected_bucket: bucket(),
        connector: connector(),
        secret_fingerprint: None,
        entries,
        checkpoint: checkpoint(),
        writes: Vec::new(),
        deletes: Vec::new(),
        now: SystemTime::UNIX_EPOCH + Duration::from_secs(2_000),
    }
}

fn record(claim_token: Ulid) -> JobRecord {
    let mut record = JobRecord::new(
        JobId::from_bytes([5; 16]),
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        UserId::default(),
        node(),
        JOB_CREATED_MS,
        JOB_CREATED_MS,
        None,
    );
    record.claim = Some(JobClaim {
        holder_node_id: node(),
        claim_token,
        lease_expires_ms: u64::MAX,
    });
    record
}

fn value(bytes: Vec<u8>) -> Option<Value> {
    Some(ByteView::from(bytes))
}

fn guards(fence: Option<Value>, record: &JobRecord, heads: Vec<Option<Value>>) -> Event {
    let mut values = vec![
        (Key::from(b"fence".to_vec()), fence),
        (
            Key::from(b"bucket".to_vec()),
            value(bucket().to_bytes().unwrap()),
        ),
        (
            Key::from(b"connector".to_vec()),
            value(connector().to_bytes().unwrap()),
        ),
        (
            Key::from(b"job".to_vec()),
            value(record.to_bytes().unwrap()),
        ),
    ];
    values.extend(
        heads
            .into_iter()
            .map(|head| (Key::from(b"head".to_vec()), head)),
    );
    Event::Storage(StorageEvent::BatchReadResult { values })
}

fn no_secret() -> Event {
    Event::Storage(StorageEvent::VaultResult {
        entry: SourceConnectorSecret::vault_entry(connector().connector_id),
        secret: None,
    })
}

fn written() -> Event {
    Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    })
}

fn head(version_id: Ulid) -> Option<Value> {
    value(CurrentVersionPointer::new(version_id).to_bytes().unwrap())
}

/// A head version bound to `path` of the test connector, or of another source.
fn reference(path: &str, observation: SourceMetadata, same_source: bool) -> Option<Value> {
    let mut binding = build_source_binding(
        StagingStrategy::Reference,
        &connector(),
        &observation,
        path.to_string(),
        Some(node()),
        Some(connector().connector_id),
    );
    if !same_source {
        binding.connector_id = Some(Ulid::from_bytes([8; 16]));
    }
    let now = SystemTime::UNIX_EPOCH;
    let version = BlobVersion::reference(binding, observation, now, UserId::default(), now);
    value(version.to_bytes().unwrap())
}

/// Drives the operation up to its row writes and returns them.
fn run_to_writes(
    operation: &mut PageImportOperation,
    heads: Vec<Option<Value>>,
    versions: Option<Vec<Option<Value>>>,
) -> Vec<(KeySpace, Key, Value)> {
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));
    let reads = heads.len() + GUARD_READS;
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads: batch, .. })] if batch.len() == reads
    ));
    operation.step(guards(None, &record(token()), heads));
    let mut effects = operation.step(no_secret());
    if let Some(versions) = versions {
        let values = versions
            .into_iter()
            .map(|version| (Key::from(b"version".to_vec()), version))
            .collect();
        effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    }
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the page rows, got {effects:?}");
    };
    writes.clone()
}

fn rows_in(writes: &[(KeySpace, Key, Value)], key_space: &str) -> usize {
    writes
        .iter()
        .filter(|(space, ..)| space == key_space)
        .count()
}

#[test]
fn page_registers_atomically() {
    let mut operation = PageImportOperation::new(input(vec![entry("a"), entry("b")]));

    let writes = run_to_writes(&mut operation, vec![None, None], None);

    assert_eq!(rows_in(&writes, BLOB_VERSIONS_KEYSPACE), 2);
    assert_eq!(rows_in(&writes, BLOB_HEAD_KEYSPACE), 2);
    assert_eq!(rows_in(&writes, CONNECTOR_REFERENCE_KEYSPACE), 2);
    assert_eq!(rows_in(&writes, STAGING_STATE_KEYSPACE), 1);
    let effects = operation.step(written());
    let [Effect::Storage(StorageEffect::AddUsage { deltas, .. })] = effects.as_slice() else {
        panic!("expected one usage update, got {effects:?}");
    };
    assert!(
        deltas
            .iter()
            .all(|(_, delta)| delta.objects == 2 && delta.referenced_bytes == 6)
    );
    operation.step(written());
    assert!(matches!(
        operation.step(written()).as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    assert!(matches!(
        operation
            .step(Event::Storage(StorageEvent::TransactionCommitted {
                txn_id: txn()
            }))
            .as_slice(),
        [Effect::Task(_)]
    ));
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::PublishUsageSnapshots,
        after: Duration::from_secs(1),
    }));

    let result = operation.finalize().unwrap();
    assert_eq!(
        (result.registered, result.unchanged, result.kept),
        (2, 0, 0)
    );
    assert_eq!(result.checkpoint.items_current, 2);
    assert_eq!(result.checkpoint.items_succeeded, 2);
}

#[test]
fn replay_counts_once() {
    let first = entry("a");
    let mut operation = PageImportOperation::new(input(vec![first.clone()]));
    let held = reference(&first.source_path, first.observation.clone(), true);

    let writes = run_to_writes(
        &mut operation,
        vec![head(Ulid::from_parts(JOB_CREATED_MS + 5, 1))],
        Some(vec![held]),
    );

    assert_eq!(rows_in(&writes, BLOB_VERSIONS_KEYSPACE), 0);
    assert_eq!(rows_in(&writes, STAGING_STATE_KEYSPACE), 1);
    // Nothing new is registered, so no usage moves and no publication is scheduled.
    assert!(matches!(
        operation.step(written()).as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    assert!(
        operation
            .step(Event::Storage(StorageEvent::TransactionCommitted {
                txn_id: txn()
            }))
            .is_empty()
    );
    let result = operation.finalize().unwrap();
    assert_eq!((result.registered, result.unchanged), (0, 1));
}

#[test]
fn newer_write_kept() {
    let (newer, older) = (entry("a"), entry("b"));
    let mut operation = PageImportOperation::new(input(vec![newer.clone(), older.clone()]));
    let other = |entry: &PageEntry| reference(&entry.source_path, observation("x"), false);

    let writes = run_to_writes(
        &mut operation,
        vec![
            head(Ulid::from_parts(JOB_CREATED_MS + 1, 1)),
            head(Ulid::from_parts(JOB_CREATED_MS - 1, 1)),
        ],
        Some(vec![other(&newer), other(&older)]),
    );

    // Only the head written before the job started is replaced.
    assert_eq!(rows_in(&writes, BLOB_VERSIONS_KEYSPACE), 1);
    let effects = operation.step(written());
    let [Effect::Storage(StorageEffect::AddUsage { deltas, .. })] = effects.as_slice() else {
        panic!("expected one usage update, got {effects:?}");
    };
    assert!(
        deltas
            .iter()
            .all(|(_, delta)| delta.objects == 0 && delta.referenced_bytes == 3)
    );
    operation.step(written());
    operation.step(written());
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: txn(),
    }));
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::PublishUsageSnapshots,
        after: Duration::from_secs(1),
    }));
    let result = operation.finalize().unwrap();
    assert_eq!((result.registered, result.kept), (1, 1));
    assert_eq!(result.checkpoint.items_failed, 1);
    assert_eq!(result.checkpoint.errors[0].target_key, newer.target_key);
}

#[test]
fn guards_abort_page() {
    let fence = StoragePurgeFence {
        job_id: JobId::from_bytes([6; 16]),
        scope: StoragePurgeScope::Bucket {
            bucket: "imports".to_string(),
        },
    };
    let cases = [
        (
            guards(
                value(fence.to_bytes().unwrap()),
                &record(token()),
                vec![None],
            ),
            PageImportError::Purge(PurgeFenceError::Suspended),
        ),
        (
            guards(None, &record(Ulid::from_bytes([1; 16])), vec![None]),
            PageImportError::ClaimLost,
        ),
    ];
    for (event, expected) in cases {
        let mut operation = PageImportOperation::new(input(vec![entry("a")]));
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: txn(),
        }));

        assert!(matches!(
            operation.step(event).as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: txn(),
        }));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Err(expected));
    }
}

#[test]
fn changed_secret_aborts() {
    let mut operation = PageImportOperation::new(PageImportInput {
        secret_fingerprint: Some([1; 16]),
        ..input(vec![entry("a")])
    });
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));
    operation.step(guards(None, &record(token()), vec![None]));

    assert!(matches!(
        operation.step(no_secret()).as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: txn(),
    }));
    assert_eq!(operation.finalize(), Err(PageImportError::ConnectorChanged));
}

#[test]
fn page_rejects_event() {
    let mut operation = PageImportOperation::new(input(vec![entry("a")]));
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));

    assert!(matches!(
        operation.step(no_secret()).as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: txn(),
    }));
    assert!(matches!(
        operation.finalize(),
        Err(PageImportError::UnexpectedEvent { .. })
    ));
}

#[test]
fn page_bounds_entries() {
    let entries = (0..=MAX_PAGE_ENTRIES)
        .map(|index| entry(&index.to_string()))
        .collect();
    let mut operation = PageImportOperation::new(input(entries));

    assert!(operation.start().is_empty());
    assert!(operation.is_complete());
    assert_eq!(
        operation.finalize(),
        Err(PageImportError::TooManyEntries(MAX_PAGE_ENTRIES + 1))
    );
}
