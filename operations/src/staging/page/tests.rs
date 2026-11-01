//! Tests the page import transitions: guards, replay, newer writes, usage and the checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::purge_fence::PurgeFenceError;
use crate::staging::descriptor::build_source_binding;
use crate::staging::page::rows::{GUARD_READS, MAX_PAGE_ENTRIES};
use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
    REPLICATION_OBLIGATION_KEYSPACE, STAGING_STATE_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    JobClaim, JobId, JobPayload, JobRecord, StagingJobCheckpoint, StagingJobPhase,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::{
    SourceConnector, SourceConnectorKind, SourceConnectorSecret,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, CurrentVersionPointer};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
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
        distribution: None,
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
        metadata: Default::default(),
        mirrors: Vec::new(),
    }
}

fn checkpoint() -> StagingJobCheckpoint {
    StagingJobCheckpoint {
        phase: StagingJobPhase::Registering,
        flat: Some(true),
        items_done: 0,
        prefixes_done: 1,
        walk: None,
        manifest: None,
        table: None,
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: None,
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
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
        auth_context: AuthContext {
            user_id: UserId::default(),
            realm_id: RealmId::from_bytes([3; 32]),
            path_restrictions: None,
            session: None,
        },
        node_id: node(),
        bucket: "imports".to_string(),
        expected_bucket: bucket(),
        connector: connector(),
        secret_fingerprint: None,
        entries,
        checkpoint: checkpoint(),
        checkpoint_key: ByteView::from(JobId::from_bytes([5; 16]).to_bytes().to_vec()),
        writes: Vec::new(),
        deletes: Vec::new(),
        now: SystemTime::UNIX_EPOCH + Duration::from_secs(2_000),
        ranges: None,
        mirror_connectors: Vec::new(),
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

fn claim(record: &JobRecord) -> Option<Value> {
    let guard = crate::jobs::store::ClaimGuard::of(record)?;
    value(guard.to_bytes().unwrap())
}

fn value(bytes: Vec<u8>) -> Option<Value> {
    Some(ByteView::from(bytes))
}

fn guards(fence: Option<Value>, record: &JobRecord) -> Event {
    let values = vec![
        (Key::from(b"fence".to_vec()), fence),
        (
            Key::from(b"bucket".to_vec()),
            value(bucket().to_bytes().unwrap()),
        ),
        (
            Key::from(b"connector".to_vec()),
            value(connector().to_bytes().unwrap()),
        ),
        (Key::from(b"job".to_vec()), claim(record)),
    ];
    Event::Storage(StorageEvent::BatchReadResult { values })
}

/// Answers the head interval read as holding too many other keys, then the head reads with
/// `heads` in entry order.
fn answer_heads(operation: &mut PageImportOperation, heads: Vec<Option<Value>>) -> Effects {
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: Some(Key::from(b"other".to_vec())),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == heads.len()
    ));
    let values = (heads.into_iter())
        .map(|head| (Key::from(b"head".to_vec()), head))
        .collect();
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
}

/// The answer to the relationship read: `count` relationships replicate the bucket.
fn relationships(count: usize) -> Event {
    let row = || (Key::from(b"rel".to_vec()), ByteView::from(Vec::new()));
    Event::Storage(StorageEvent::IterResult {
        values: (0..count).map(|_| row()).collect(),
        next_start_after: None,
    })
}

fn no_secret() -> Event {
    Event::Storage(StorageEvent::VaultResult {
        entry: SourceConnectorSecret::vault_entry(connector().connector_id),
        secret: None,
    })
}

fn scheduled(key: TaskKey) -> Event {
    Event::Task(TaskEvent::TimerScheduled {
        key,
        after: Duration::ZERO,
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
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == GUARD_READS
    ));
    operation.step(guards(None, &record(token())));
    answer_heads(operation, heads);
    operation.step(relationships(1));
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
    assert_eq!(rows_in(&writes, REPLICATION_OBLIGATION_KEYSPACE), 2);
    let effects = operation.step(written());
    let [
        Effect::Storage(StorageEffect::AddUsage {
            deltas, buckets, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected one usage update, got {effects:?}");
    };
    assert!(
        deltas
            .iter()
            .all(|(_, delta)| delta.objects == 2 && delta.referenced_bytes == 6)
    );
    // The bucket row counts the page like the group does, with one version row per entry.
    let [(_, bucket)] = buckets.as_slice() else {
        panic!("expected the bucket row, got {buckets:?}");
    };
    assert_eq!(
        (bucket.objects, bucket.versions, bucket.referenced_bytes),
        (2, 2, 6)
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
    assert!(matches!(
        operation
            .step(scheduled(TaskKey::PublishUsageSnapshots))
            .as_slice(),
        [Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainReplicationQueue,
            ..
        })]
    ));
    operation.step(scheduled(TaskKey::DrainReplicationQueue));

    let result = operation.finalize().unwrap();
    assert_eq!(
        (result.registered, result.unchanged, result.kept),
        (2, 0, 0)
    );
    assert_eq!(result.checkpoint.items_current, 2);
    assert_eq!(result.checkpoint.items_succeeded, 2);
}

// An imported version records its external source and its import job as its origin.
#[test]
fn origin_marks_source() {
    use aruna_core::structs::storage::blob::BlobVersion;
    use aruna_core::structs::storage::origin::VersionOrigin;
    let input = input(vec![entry("a")]);
    let job_id = Some(input.job_id.as_ulid());
    let mut operation = PageImportOperation::new(input);

    let writes = run_to_writes(&mut operation, vec![None], None);

    let (_, _, value) = (writes.iter())
        .find(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
        .unwrap();
    let version = BlobVersion::from_bytes(value).unwrap();
    assert_eq!(version.origin, Some(VersionOrigin::Source { job_id }));
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
    operation.step(scheduled(TaskKey::PublishUsageSnapshots));
    operation.step(scheduled(TaskKey::DrainReplicationQueue));
    let result = operation.finalize().unwrap();
    assert_eq!((result.registered, result.kept), (1, 1));
    assert_eq!(result.checkpoint.items_failed, 1);
    assert_eq!(result.checkpoint.errors[0].target_key, newer.target_key);
}

// A drift successor of the same file written after the job started is newer than the listing.
#[test]
fn newer_same_kept() {
    let first = entry("a");
    let mut operation = PageImportOperation::new(input(vec![first.clone()]));
    let held = reference(&first.source_path, observation("drifted"), true);

    let writes = run_to_writes(
        &mut operation,
        vec![head(Ulid::from_parts(JOB_CREATED_MS + 1, 1))],
        Some(vec![held]),
    );

    assert_eq!(rows_in(&writes, BLOB_VERSIONS_KEYSPACE), 0);
    operation.step(written());
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: txn(),
    }));
    let result = operation.finalize().unwrap();
    assert_eq!((result.registered, result.kept), (0, 1));
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
            guards(value(fence.to_bytes().unwrap()), &record(token())),
            PageImportError::Purge(PurgeFenceError::Suspended),
        ),
        (
            guards(None, &record(Ulid::from_bytes([1; 16]))),
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
    operation.step(guards(None, &record(token())));
    answer_heads(&mut operation, vec![None]);
    operation.step(relationships(1));

    assert!(matches!(
        operation.step(no_secret()).as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: txn(),
    }));
    assert_eq!(operation.finalize(), Err(PageImportError::ConnectorChanged));
}

// The heads of a page are read as one interval of its keys; an empty interval proves every
// key new without a read per key.
#[test]
fn interval_proves_absence() {
    use aruna_core::effects::{IterEnd, IterStart};
    use aruna_core::structs::storage::blob::BlobHeadKey;
    let mut operation = PageImportOperation::new(input(vec![entry("b"), entry("a")]));
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));

    let effects = operation.step(guards(None, &record(token())));

    let key = |name| Key::from(BlobHeadKey::new("imports", name).to_bytes().unwrap());
    let [Effect::Storage(StorageEffect::IterRange { start, end, .. })] = effects.as_slice() else {
        panic!("expected one head interval read, got {effects:?}");
    };
    assert_eq!(
        (start, end),
        (&IterStart::At(key("a")), &IterEnd::Through(key("b")))
    );
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    operation.step(relationships(1));
    let effects = operation.step(no_secret());
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the page rows, got {effects:?}");
    };
    assert_eq!(rows_in(writes, BLOB_VERSIONS_KEYSPACE), 2);
}

// A bucket nothing replicates gets no replication obligation per imported file.
#[test]
fn obligations_need_downstream() {
    let mut operation = PageImportOperation::new(input(vec![entry("a"), entry("b")]));
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));
    operation.step(guards(None, &record(token())));
    let effects = answer_heads(&mut operation, vec![None, None]);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter {
            limit: 1,
            txn_id: Some(_),
            ..
        })]
    ));
    operation.step(relationships(0));

    let effects = operation.step(no_secret());

    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the page rows, got {effects:?}");
    };
    assert_eq!(rows_in(writes, BLOB_VERSIONS_KEYSPACE), 2);
    assert_eq!(rows_in(writes, REPLICATION_OBLIGATION_KEYSPACE), 0);
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

// A progress flush and a lease renewal that commit while the page is open must not conflict.
#[tokio::test]
async fn flush_keeps_page() {
    use crate::jobs::store::{flush_progress, renew_lease};
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};

    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let storage = &context.storage_handle;
    let group_id = bucket().group_id;
    let connector = create_http_connector(context, group_id, "http://127.0.0.1:9").await;
    let expected_bucket = create_test_bucket(context, group_id, UserId::default(), "imports").await;
    let job = record(token());
    crate::jobs::store::insert_job(storage, &job).await.unwrap();
    let mut operation = PageImportOperation::new(PageImportInput {
        connector,
        expected_bucket,
        ..input(vec![entry("a")])
    });

    let mut pending = std::collections::VecDeque::from(operation.start().into_vec());
    while let Some(effect) = pending.pop_front() {
        let Effect::Storage(effect) = effect else {
            break;
        };
        let commit = matches!(effect, StorageEffect::CommitTransaction { .. });
        if commit {
            let progress = job.progress.clone();
            flush_progress(storage, job.job_id, token(), progress, 2)
                .await
                .unwrap();
            renew_lease(storage, job.job_id, token(), 3, None)
                .await
                .unwrap();
        }
        let event = storage.send_storage_effect(effect).await;
        if commit {
            assert!(
                matches!(
                    event,
                    Event::Storage(StorageEvent::TransactionCommitted { .. })
                ),
                "the page must commit, got {event:?}"
            );
            return;
        }
        pending.extend(operation.step(event));
    }
    panic!("the page never reached its commit");
}

// An observation with nothing to compare but its size, as a table row gives it, imported
// again unchanged writes nothing; a different size or a moved source is no longer unchanged.
#[tokio::test]
async fn bare_size_unchanged() {
    use crate::connectors::repository::source_connector_key;
    use crate::driver::drive;
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE;

    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let group_id = bucket().group_id;
    let connector = create_http_connector(context, group_id, "http://127.0.0.1:9").await;
    let expected_bucket = create_test_bucket(context, group_id, UserId::default(), "imports").await;
    crate::jobs::store::insert_job(&context.storage_handle, &record(token()))
        .await
        .unwrap();
    let bare = |content_length| PageEntry {
        observation: SourceMetadata {
            content_length,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        },
        ..entry("a")
    };
    let page = |content_length, checkpoint| PageImportInput {
        connector: connector.clone(),
        expected_bucket: expected_bucket.clone(),
        checkpoint,
        ..input(vec![bare(content_length)])
    };
    let run = |page| drive(PageImportOperation::new(page), context);

    let first = run(page(4, checkpoint())).await.unwrap();
    let again = run(page(4, first.checkpoint.clone())).await.unwrap();
    assert_eq!(
        (first.registered, again.registered, again.unchanged),
        (1, 0, 1)
    );
    let resized = run(page(5, again.checkpoint.clone())).await.unwrap();
    assert_eq!(resized.unchanged, 0);

    // A later job sees the same bare row as unchanged until the connector's endpoint moves;
    // then it binds the file to the new source.
    let started = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let started = started.as_millis() as u64 + 1;
    let job_id = JobId::from_bytes([6; 16]);
    let mut later = record(token());
    later.job_id = job_id;
    (later.created_at_ms, later.updated_at_ms) = (started, started);
    crate::jobs::store::insert_job(&context.storage_handle, &later)
        .await
        .unwrap();
    let mut moved = connector.clone();
    let endpoint = "http://127.0.0.1:10".to_string();
    moved.public_config.insert("endpoint".to_string(), endpoint);
    let later_page = |connector: &SourceConnector| PageImportInput {
        job_id,
        checkpoint_key: ByteView::from(job_id.to_bytes().to_vec()),
        connector: connector.clone(),
        ..page(4, checkpoint())
    };
    let kept = run(later_page(&connector)).await.unwrap();
    assert_eq!((kept.registered, kept.unchanged), (0, 1));
    let write = StorageEffect::Write {
        key_space: SOURCE_INDEX_KEYSPACE.to_string(),
        key: source_connector_key(group_id, moved.connector_id),
        value: moved.to_bytes().unwrap().into(),
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(write).await;
    let rebound = run(later_page(&moved)).await.unwrap();
    assert_eq!((rebound.registered, rebound.unchanged), (1, 0));
}

// Runs the page twice against real storage: the commit is atomic and the replay finds the
// registered head, so usage and connector rows are counted once.
#[tokio::test]
async fn replay_charges_once() {
    use crate::driver::drive;
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use aruna_core::effects::IterStart;
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::storage::usage::{UsageCounters, usage_group_key};

    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let group_id = bucket().group_id;
    let connector = create_http_connector(context, group_id, "http://127.0.0.1:9").await;
    let expected_bucket = create_test_bucket(context, group_id, UserId::default(), "imports").await;
    crate::jobs::store::insert_job(&context.storage_handle, &record(token()))
        .await
        .unwrap();
    let page = PageImportInput {
        connector: connector.clone(),
        expected_bucket,
        ..input(vec![entry("a")])
    };

    let first = drive(PageImportOperation::new(page.clone()), context)
        .await
        .unwrap();
    let second = drive(
        PageImportOperation::new(PageImportInput {
            checkpoint: first.checkpoint.clone(),
            ..page
        }),
        context,
    )
    .await
    .unwrap();

    assert_eq!((first.registered, second.unchanged), (1, 1));
    assert_eq!(second.checkpoint.items_succeeded, 2);
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(usage), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: usage_group_key(group_id).into(),
            txn_id: None,
        })
        .await
    else {
        panic!("group usage must exist");
    };
    let usage = UsageCounters::from_bytes(&usage).unwrap();
    assert_eq!((usage.objects, usage.referenced_bytes), (1, 3));
    let Event::Storage(StorageEvent::IterResult { values, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(connector.connector_id.to_bytes().to_vec())),
            start: None::<IterStart>,
            limit: 10,
            txn_id: None,
        })
        .await
    else {
        panic!("reference rows must be readable");
    };
    assert_eq!(values.len(), 1);
}

#[test]
fn guards_carry_hint() {
    use aruna_core::structs::storage::distribution::{DistributionBinding, SetHint};

    let mut bound = bucket();
    bound.distribution = Some(DistributionBinding {
        set_id: Ulid::from_bytes([5u8; 16]),
        member_id: Ulid::from_bytes([6u8; 16]),
        incarnation: 1,
        revision: Default::default(),
    });
    let values = vec![
        (Key::from(b"fence".to_vec()), None),
        (
            Key::from(b"bucket".to_vec()),
            value(bound.to_bytes().unwrap()),
        ),
        (
            Key::from(b"connector".to_vec()),
            value(connector().to_bytes().unwrap()),
        ),
        (Key::from(b"job".to_vec()), claim(&record(token()))),
    ];
    let input = input(vec![entry("a")]);
    let guards = crate::staging::page::rows::check_guards(&input, values).unwrap();
    assert_eq!(
        guards.set_hint,
        Some(SetHint::of_set(Ulid::from_bytes([5u8; 16])))
    );
}

// A page into a distributed bucket writes inventory rows and range counters in its own
// transaction; its replay changes neither.
#[tokio::test]
async fn page_maintains_inventory() {
    use crate::distribution::test_store::{counters, has_row, install, range};
    use crate::driver::drive;
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::structs::storage::distribution::accounting::generation_key;
    use aruna_core::structs::storage::distribution::inventory::head_position;

    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let handle = &context.storage_handle;
    let group_id = bucket().group_id;
    let connector = create_http_connector(context, group_id, "http://127.0.0.1:9").await;
    let mut bound = create_test_bucket(context, group_id, UserId::default(), "shared").await;
    // Member 1 of the test set coordinates the keys "a" and "b".
    let (snapshot, record_bytes) = crate::distribution::test_record::record(true);
    let set_id = snapshot.set_id();
    crate::distribution::test_record::store_record(handle, set_id, record_bytes).await;
    bound.distribution = Some(crate::distribution::test_record::binding(&snapshot, 1, 5));
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: b"shared".to_vec().into(),
        value: bound.to_bytes().unwrap().into(),
        txn_id: None,
    };
    assert!(matches!(
        handle.send_storage_effect(write).await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    install(handle, set_id, &[range(1, "", None)]).await;
    crate::jobs::store::insert_job(handle, &record(token()))
        .await
        .unwrap();
    let page = PageImportInput {
        connector,
        bucket: "shared".to_string(),
        expected_bucket: bound,
        ..input(vec![entry("a"), entry("b")])
    };

    let first = drive(PageImportOperation::new(page.clone()), context)
        .await
        .unwrap();
    let replay = PageImportInput {
        checkpoint: first.checkpoint.clone(),
        ..page
    };
    let second = drive(PageImportOperation::new(replay), context)
        .await
        .unwrap();

    assert_eq!((first.registered, second.unchanged), (2, 2));
    let total = counters(handle, generation_key(set_id, Ulid::from_bytes([1; 16]), 1)).await;
    assert_eq!(
        (total.objects, total.versions, total.referenced_bytes),
        (2, 2, 6)
    );
    assert!(has_row(handle, head_position(set_id, "a")).await);
    assert!(has_row(handle, head_position(set_id, "b")).await);
    // The set keeps no search index, so the page feeds none.
    let feed = crate::distribution::test_store::get(
        handle,
        aruna_core::keyspaces::SEARCH_FEED_KEYSPACE,
        head_position(set_id, "a"),
    );
    assert!(feed.await.is_none());
}

// A page whose keys reach a range another member coordinates writes nothing.
#[tokio::test]
async fn page_needs_coordinator() {
    use crate::distribution::guard::GuardError;
    use crate::distribution::test_record::{binding, record as set_record, store_record};
    use crate::distribution::test_store::{has_row, install, range};
    use crate::driver::drive;
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::structs::storage::distribution::inventory::head_position;

    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let handle = &context.storage_handle;
    let group_id = bucket().group_id;
    let connector = create_http_connector(context, group_id, "http://127.0.0.1:9").await;
    let mut bound = create_test_bucket(context, group_id, UserId::default(), "shared").await;
    // Member 1 coordinates "a"; member 2 coordinates "x".
    let (snapshot, record_bytes) = set_record(true);
    let set_id = snapshot.set_id();
    store_record(handle, set_id, record_bytes).await;
    bound.distribution = Some(binding(&snapshot, 1, 5));
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: b"shared".to_vec().into(),
        value: bound.to_bytes().unwrap().into(),
        txn_id: None,
    };
    assert!(matches!(
        handle.send_storage_effect(write).await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    install(handle, set_id, &[range(1, "", None)]).await;
    crate::jobs::store::insert_job(handle, &record(token()))
        .await
        .unwrap();
    let page = PageImportInput {
        connector,
        bucket: "shared".to_string(),
        expected_bucket: bound,
        ..input(vec![entry("a"), entry("x")])
    };
    let refused = drive(PageImportOperation::new(page), context).await;
    assert!(matches!(
        refused,
        Err(PageImportError::Distribution(
            GuardError::NotCoordinator { .. }
        ))
    ));
    assert!(!has_row(handle, head_position(set_id, "a")).await);
}

/// Bytes of keys and values per keyspace in `handle`.
async fn keyspace_bytes(handle: &aruna_storage::StorageHandle) -> HashMap<&'static str, usize> {
    let mut sizes = HashMap::new();
    for key_space in aruna_core::keyspaces::KEYSPACE_CATALOG {
        let iter = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 1_000_000,
            txn_id: None,
        };
        if let Event::Storage(StorageEvent::IterResult { values, .. }) =
            handle.send_storage_effect(iter).await
        {
            let bytes = values.iter().map(|(key, value)| key.len() + value.len());
            sizes.insert(*key_space, bytes.sum());
        }
    }
    sizes
}

/// A public bakrep-like S3 connector stored for the test group.
async fn bakrep_connector(context: &crate::driver::DriverContext) -> SourceConnector {
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    let input = SourceConnectorInput {
        group_id: bucket().group_id,
        created_by: UserId::default(),
        name: "bakrep".to_string(),
        kind: SourceConnectorKind::S3,
        public_config: HashMap::from([
            ("bucket".to_string(), "bakrep-v2-data-000".to_string()),
            (
                "endpoint".to_string(),
                "https://s3.bwsfs.uni-freiburg.de".to_string(),
            ),
            ("region".to_string(), "us-east-1".to_string()),
            ("skip_signature".to_string(), "true".to_string()),
        ]),
        secret_config: HashMap::new(),
    };
    let operation = SourceConnectorOperation::new(input);
    crate::driver::drive(operation, context)
        .await
        .unwrap()
        .connector
}

fn bakrep_entry(index: usize) -> PageEntry {
    let sample = format!("SAMD{:08}", 344 + index);
    let source_path = format!("{sample}/{sample}.bakta.gff3.gz");
    PageEntry {
        target_key: format!("bakrep-v2-data-000/{source_path}"),
        source_path,
        observation: SourceMetadata {
            content_length: 1_234_567,
            content_type: None,
            etag: Some("\"9b2cf535f27731c974343645a3985328\"".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            source_version: None,
            digests: Default::default(),
        },
        metadata: Default::default(),
        mirrors: Vec::new(),
    }
}

/// Prints the bytes each row kind gained per entry between `before` and `after`.
fn print_sizes(
    label: &str,
    before: &HashMap<&'static str, usize>,
    after: &HashMap<&'static str, usize>,
    entries: usize,
) {
    let mut rows: Vec<_> = (after.iter())
        .map(|(space, bytes)| (*space, bytes - before.get(space).copied().unwrap_or(0)))
        .filter(|(_, bytes)| *bytes > 0)
        .collect();
    rows.sort();
    let total: usize = rows.iter().map(|(_, bytes)| bytes / entries).sum();
    for (space, bytes) in rows {
        println!("{label} {space}: {} bytes per entry", bytes / entries);
    }
    println!("{label} total: {total} bytes per entry");
}

// The same measurement for a distributed bucket, inventory rows included.
#[tokio::test]
async fn distributed_sizes_measured() {
    use crate::distribution::test_store::{install, range};
    use crate::driver::drive;
    use crate::tests::staging::{create_test_bucket, setup_driver_context};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;

    const ENTRIES: usize = 100;
    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let handle = &context.storage_handle;
    let connector = bakrep_connector(context).await;
    let group_id = bucket().group_id;
    let mut bound = create_test_bucket(context, group_id, UserId::default(), "shared").await;
    let (snapshot, record_bytes) = crate::distribution::test_record::record(true);
    let set_id = snapshot.set_id();
    crate::distribution::test_record::store_record(handle, set_id, record_bytes).await;
    bound.distribution = Some(crate::distribution::test_record::binding(&snapshot, 1, 5));
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: b"shared".to_vec().into(),
        value: bound.to_bytes().unwrap().into(),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
    install(handle, set_id, &[range(1, "", None)]).await;
    crate::jobs::store::insert_job(handle, &record(token()))
        .await
        .unwrap();
    let before = keyspace_bytes(handle).await;
    let page = PageImportInput {
        connector,
        bucket: "shared".to_string(),
        expected_bucket: bound,
        ..input((0..ENTRIES).map(bakrep_entry).collect())
    };
    drive(PageImportOperation::new(page), context)
        .await
        .unwrap();

    let after = keyspace_bytes(handle).await;
    print_sizes("distributed", &before, &after, ENTRIES);
}

// Prints the stored bytes per entry and row kind of a realistic bakrep page; a measurement
// for the entry size work, not a size contract. Run with `--nocapture` to read it.
#[tokio::test]
async fn entry_sizes_measured() {
    use crate::driver::drive;
    use crate::tests::staging::{create_test_bucket, setup_driver_context};

    const ENTRIES: usize = 100;
    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let handle = &context.storage_handle;
    let connector = bakrep_connector(context).await;
    let group_id = bucket().group_id;
    let expected_bucket = create_test_bucket(context, group_id, UserId::default(), "imports").await;
    crate::jobs::store::insert_job(handle, &record(token()))
        .await
        .unwrap();
    let before = keyspace_bytes(handle).await;
    let page = PageImportInput {
        connector,
        expected_bucket,
        ..input((0..ENTRIES).map(bakrep_entry).collect())
    };
    drive(PageImportOperation::new(page), context)
        .await
        .unwrap();

    let after = keyspace_bytes(handle).await;
    print_sizes("plain", &before, &after, ENTRIES);
}

/// Drives `operation` against `handle` and counts its storage request/response exchanges.
async fn count_exchanges(
    mut operation: PageImportOperation,
    handle: &aruna_storage::StorageHandle,
) -> (usize, Result<PageImportResult, PageImportError>) {
    let mut exchanges = 0;
    let mut effects: std::collections::VecDeque<Effect> = operation.start().into_iter().collect();
    while let Some(effect) = effects.pop_front() {
        let event = match effect {
            Effect::Storage(effect) => {
                exchanges += 1;
                handle.send_storage_effect(effect).await
            }
            Effect::Task(
                TaskEffect::ShortenTimer { key, after } | TaskEffect::ResetTimer { key, after },
            ) => Event::Task(TaskEvent::TimerScheduled { key, after }),
            other => panic!("unexpected effect: {other:?}"),
        };
        effects.extend(operation.step(event));
    }
    (exchanges, operation.finalize())
}

// Prints the storage exchanges of four 500-key pages into one distributed range, the last ones
// with a full key sample, then of two gathered 5,000-key pages; a measurement, not a contract.
// Run with `--nocapture` to read it.
#[tokio::test]
async fn page_exchanges_measured() {
    use crate::distribution::test_store::{install, range};
    use crate::tests::staging::{create_test_bucket, setup_driver_context};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;

    const ENTRIES: usize = 500;
    let test = setup_driver_context().await;
    let context = &test.driver_context;
    let handle = &context.storage_handle;
    let connector = bakrep_connector(context).await;
    let group_id = bucket().group_id;
    let mut bound = create_test_bucket(context, group_id, UserId::default(), "shared").await;
    let (snapshot, record_bytes) = crate::distribution::test_record::record(true);
    let set_id = snapshot.set_id();
    crate::distribution::test_record::store_record(handle, set_id, record_bytes).await;
    bound.distribution = Some(crate::distribution::test_record::binding(&snapshot, 1, 5));
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: b"shared".to_vec().into(),
        value: bound.to_bytes().unwrap().into(),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
    install(handle, set_id, &[range(1, "", None)]).await;
    crate::jobs::store::insert_job(handle, &record(token()))
        .await
        .unwrap();
    for page in 0..4 {
        let entries = (page * ENTRIES..(page + 1) * ENTRIES).map(bakrep_entry);
        let input = PageImportInput {
            connector: connector.clone(),
            bucket: "shared".to_string(),
            expected_bucket: bound.clone(),
            ..input(entries.collect())
        };
        let (exchanges, result) = count_exchanges(PageImportOperation::new(input), handle).await;
        assert_eq!(result.unwrap().registered, ENTRIES as u64);
        println!("distributed page {page}: {exchanges} storage exchanges for {ENTRIES} keys");
    }
    const GATHERED: usize = 5_000;
    for page in 0..2 {
        let first = 4 * ENTRIES + page * GATHERED;
        let entries = (first..first + GATHERED).map(bakrep_entry);
        let input = PageImportInput {
            connector: connector.clone(),
            bucket: "shared".to_string(),
            expected_bucket: bound.clone(),
            ..input(entries.collect())
        };
        let started = std::time::Instant::now();
        let (exchanges, result) = count_exchanges(PageImportOperation::new(input), handle).await;
        assert_eq!(result.unwrap().registered, GATHERED as u64);
        let elapsed = started.elapsed().as_millis();
        println!(
            "gathered page {page}: {exchanges} storage exchanges, {elapsed} ms for {GATHERED} keys"
        );
    }
}

// A further source's connector is read again with the page's guards: replaced or removed since
// the page resolved it, the page stops.
#[test]
fn mirror_connector_rechecked() {
    let mirror = SourceConnector::new(
        Ulid::from_bytes([3; 16]),
        Ulid::from_bytes([1; 16]),
        "mirror".to_string(),
        SourceConnectorKind::S3,
        HashMap::from([("bucket".to_string(), "mirror".to_string())]),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        Default::default(),
    );
    let input = PageImportInput {
        mirror_connectors: vec![mirror.clone()],
        ..input(vec![entry("a")])
    };
    let reads = crate::staging::page::rows::guard_reads(&input).unwrap();
    assert_eq!(reads.len(), GUARD_READS + 1);
    let values = |found: Option<&SourceConnector>| {
        let Event::Storage(StorageEvent::BatchReadResult { mut values }) =
            guards(None, &record(token()))
        else {
            unreachable!()
        };
        let found = found.map(|connector| connector.to_bytes().unwrap().into());
        values.push((Key::from(b"mirror".to_vec()), found));
        values
    };
    let check = crate::staging::page::rows::check_guards;
    assert!(check(&input, values(Some(&mirror))).is_ok());
    let mut replaced = mirror.clone();
    replaced
        .public_config
        .insert("bucket".to_string(), "other".to_string());
    for found in [None, Some(&replaced)] {
        let result = check(&input, values(found)).map(|_| ());
        assert_eq!(result, Err(PageImportError::ConnectorChanged));
    }
}
