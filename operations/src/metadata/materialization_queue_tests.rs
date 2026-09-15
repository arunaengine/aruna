use super::*;
use aruna_core::NodeId;
use aruna_core::keyspaces::{IRI_INDEX_KEYSPACE, RAW_REVISION_KEYSPACE};
use aruna_core::storage_entries::{create_event_entry, raw_revision_key};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::placement_record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_storage::{FjallStorage, StorageHandle};
use std::collections::BTreeSet;
use std::thread;
use tempfile::tempdir;

use crate::tests::metadata::{storage_key_exists, write_entries};

#[tokio::test]
async fn delete_waits_fence() {
    let graph_iri = "urn:test:graph-fence";
    let held = metadata_graph_fence(graph_iri)
        .acquire()
        .await
        .expect("graph fence remains open");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = ready_tx.send(());
        let _permit = metadata_graph_fence(graph_iri)
            .acquire()
            .await
            .expect("graph fence remains open");
        let _ = done_tx.send(());
    });

    ready_rx.await.expect("delete task started");
    assert!(matches!(
        done_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    drop(held);
    done_rx.await.expect("delete task completed");
    task.await.expect("delete task joined");
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn create_event(document_id: Ulid, event_id: Ulid, name: &str) -> MetadataEventRecord {
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let group_id = Ulid::from_parts(7, 1);
    let document_path = format!("datasets/{name}");
    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: document_path.clone(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            &document_path,
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node(1)],
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: event_id,
        last_event_id: event_id,
    };
    MetadataEventRecord {
        event_id,
        record,
        user_id: aruna_core::UserId::local(Ulid::from_parts(7, 2), realm_id),
        node_id: node(1),
        payload: MetadataEventPayload::Scaffold {
            name: name.to_string(),
            description: "Materialization test".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
        },
        occurred_at_ms: 1,
    }
}

fn with_payload(
    mut event: MetadataEventRecord,
    payload: MetadataEventPayload,
) -> MetadataEventRecord {
    event.payload = payload;
    event
}

// Loads the document's group first, exactly as the drain does before it
// checks a job's predecessors.
async fn older_exists(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
    advanced: &BTreeSet<Ulid>,
) -> Result<bool, MetadataMaterializationError> {
    let group = load_group_jobs(storage, document_id).await?;
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 0,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    older_job_exists(storage, &group, &job, advanced).await
}

async fn index_job_keys(storage: &StorageHandle) -> Vec<(u64, Ulid, Ulid)> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 4096,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .filter_map(|(key, _)| job_key_parts(&key))
            .collect(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn corrupt_job_only() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let corrupt_key = vec![0];
    write_entries(
        &storage,
        vec![(
            MATERIALIZATION_JOB_KEYSPACE.to_string(),
            ByteView::from(corrupt_key.clone()),
            ByteView::from(vec![1, 2, 3]),
        )],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let result = process_materialization_batch(&context)
        .await
        .expect("corrupt-only drain succeeds");

    assert_eq!(result.processed, 0);
    assert!(!result.has_more_due);
    assert!(!storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, corrupt_key).await);
}

#[tokio::test]
async fn jobs_exist_deletes() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let corrupt_key = vec![0];
    let event = create_event(
        Ulid::from_bytes([12u8; 16]),
        Ulid::from_parts(12, 1),
        "valid",
    );
    let valid_job = new_materialization_job(&event, 1);
    write_entries(
        &storage,
        vec![
            (
                MATERIALIZATION_JOB_KEYSPACE.to_string(),
                ByteView::from(corrupt_key.clone()),
                ByteView::from(vec![1, 2, 3]),
            ),
            create_event_entry(&event).unwrap(),
            materialization_job_entry(&valid_job).expect("job entry"),
            document_job_entry(&valid_job).expect("sidecar"),
        ],
    )
    .await;

    assert!(materialization_jobs_exist(&storage).await.unwrap());
    assert!(!storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, corrupt_key).await);
}

#[tokio::test]
async fn corrupt_global_job() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([14u8; 16]);
    let old_event_id = Ulid::from_parts(14, 1);
    let newer_event_id = Ulid::from_parts(14, 2);
    let old_job = MetadataMaterializationRecord {
        document_id,
        event_id: old_event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let (_, global_key, _) = materialization_job_entry(&old_job).unwrap();
    let document_key = document_job_key(document_id, old_event_id);
    write_entries(
        &storage,
        vec![
            (
                MATERIALIZATION_JOB_KEYSPACE.to_string(),
                global_key.clone(),
                ByteView::from(vec![1, 2, 3]),
            ),
            document_job_entry(&old_job).unwrap(),
        ],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let result = process_materialization_batch(&context)
        .await
        .expect("corrupt global job drain succeeds");

    assert_eq!(result.processed, 0);
    assert!(!storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, global_key.to_vec()).await);
    assert!(!storage_key_exists(&storage, DOCUMENT_JOB_KEYSPACE, document_key.to_vec()).await);
    assert!(
        !older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn orphan_malformed_sidecar() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([16u8; 16]);
    let old_event_id = Ulid::from_parts(16, 1);
    let newer_event_id = Ulid::from_parts(16, 2);
    let document_key = document_job_key(document_id, old_event_id);
    write_entries(
        &storage,
        vec![(
            DOCUMENT_JOB_KEYSPACE.to_string(),
            document_key.clone(),
            ByteView::from(vec![1, 2, 3]),
        )],
    )
    .await;

    assert!(
        !older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
            .await
            .unwrap()
    );
    assert!(!storage_key_exists(&storage, DOCUMENT_JOB_KEYSPACE, document_key.to_vec()).await);
}

#[tokio::test]
async fn orphan_valid_sidecar() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([17u8; 16]);
    let old_event_id = Ulid::from_parts(17, 1);
    let newer_event_id = Ulid::from_parts(17, 2);
    let old_job = MetadataMaterializationRecord {
        document_id,
        event_id: old_event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let document_key = document_job_key(document_id, old_event_id);
    write_entries(&storage, vec![document_job_entry(&old_job).unwrap()]).await;

    assert!(
        !older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
            .await
            .unwrap()
    );
    assert!(!storage_key_exists(&storage, DOCUMENT_JOB_KEYSPACE, document_key.to_vec()).await);
}

#[tokio::test]
async fn orphan_global_job() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([22u8; 16]);
    let event_id = Ulid::from_parts(22, 1);
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let global_key = materialization_job_key(&job);
    let document_key = document_job_key(document_id, event_id);
    write_entries(
        &storage,
        vec![
            materialization_job_entry(&job).unwrap(),
            document_job_entry(&job).unwrap(),
        ],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let result = process_materialization_batch(&context)
        .await
        .expect("orphan global job drain succeeds");

    assert_eq!(result.processed, 0);
    assert!(!storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, global_key.to_vec()).await);
    assert!(!storage_key_exists(&storage, DOCUMENT_JOB_KEYSPACE, document_key.to_vec()).await);
}

#[tokio::test]
async fn corrupt_job_deleted() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([13u8; 16]);
    let event_id = Ulid::from_bytes([255u8; 16]);
    let mut corrupt_key = document_id.to_bytes().to_vec();
    corrupt_key.push(0);
    write_entries(
        &storage,
        vec![(
            DOCUMENT_JOB_KEYSPACE.to_string(),
            ByteView::from(corrupt_key.clone()),
            ByteView::from(vec![1, 2, 3]),
        )],
    )
    .await;

    assert!(
        !older_exists(&storage, document_id, event_id, &BTreeSet::new())
            .await
            .unwrap()
    );
    assert!(!storage_key_exists(&storage, DOCUMENT_JOB_KEYSPACE, corrupt_key).await);
}

#[tokio::test]
async fn scan_stops_early() {
    // Due jobs sort before future jobs, so the scan returns at the first
    // future key without paging the whole keyspace.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let now_ms = unix_timestamp_millis();
    let document_id = Ulid::from_bytes([40u8; 16]);
    let due_event = Ulid::from_parts(1, 1);
    let event = create_event(document_id, due_event, "due");
    let due_job = MetadataMaterializationRecord {
        document_id,
        event_id: due_event,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let mut writes = vec![
        create_event_entry(&event).unwrap(),
        materialization_job_entry(&due_job).unwrap(),
        document_job_entry(&due_job).unwrap(),
    ];
    for index in 0..600u64 {
        let future_job = MetadataMaterializationRecord {
            document_id: Ulid::from_bytes([41u8; 16]),
            event_id: Ulid::from_parts(2, u128::from(index)),
            due_at_ms: now_ms.saturating_add(60_000).saturating_add(index),
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        writes.push(materialization_job_entry(&future_job).unwrap());
    }
    write_entries(&storage, writes).await;

    let before = storage.snapshot_metrics().requests_total;
    let (jobs, has_more_due, next_due_ms) =
        scan_due_jobs(&storage, now_ms, MATERIALIZATION_BATCH_SIZE)
            .await
            .unwrap();
    let delta = storage.snapshot_metrics().requests_total - before;

    assert_eq!(
        jobs,
        vec![(materialization_job_key(&due_job).to_vec(), due_job)]
    );
    assert!(!has_more_due);
    assert!(next_due_ms.is_some());
    assert!(delta <= 10, "scan issued {delta} storage requests");
}

#[tokio::test]
async fn scan_batches_reads() {
    // A page of due jobs resolves in a handful of batch reads instead of a
    // sidecar, event and status read per row.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let now_ms = unix_timestamp_millis();
    let mut writes = Vec::new();
    for index in 0..64u8 {
        let document_id = Ulid::from_bytes([index; 16]);
        let event_id = Ulid::from_parts(1, u128::from(index));
        let event = create_event(document_id, event_id, "batched");
        let job = MetadataMaterializationRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        writes.push(create_event_entry(&event).unwrap());
        writes.push(materialization_job_entry(&job).unwrap());
        writes.push(document_job_entry(&job).unwrap());
    }
    write_entries(&storage, writes).await;

    let before = storage.snapshot_metrics().requests_total;
    let (jobs, _, _) = scan_due_jobs(&storage, now_ms, MATERIALIZATION_BATCH_SIZE)
        .await
        .unwrap();
    let delta = storage.snapshot_metrics().requests_total - before;

    assert_eq!(jobs.len(), 64);
    assert!(delta <= 8, "scan issued {delta} storage requests");
}

#[tokio::test]
async fn probe_reads_one() {
    // The timer probe asks for a single due job, so it must not resolve the
    // whole page of index rows behind it.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut writes = Vec::new();
    for index in 0..64u8 {
        let document_id = Ulid::from_bytes([100 + index; 16]);
        let event_id = Ulid::from_parts(1, u128::from(index));
        let event = create_event(document_id, event_id, "probe");
        let job = MetadataMaterializationRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        writes.push(create_event_entry(&event).unwrap());
        writes.push(materialization_job_entry(&job).unwrap());
        writes.push(document_job_entry(&job).unwrap());
    }
    write_entries(&storage, writes).await;

    let before = storage.snapshot_metrics().requests_total;
    let after = next_timer_after(&storage).await.unwrap();
    let delta = storage.snapshot_metrics().requests_total - before;

    assert_eq!(after, Some(Duration::ZERO));
    assert!(delta <= 6, "probe issued {delta} storage requests");
}

#[tokio::test]
async fn stale_index_pruned() {
    // An index row with no sidecar, and one whose sidecar due time differs,
    // are both deleted and yield no job.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let now_ms = unix_timestamp_millis();
    let orphan = MetadataMaterializationRecord {
        document_id: Ulid::from_bytes([42u8; 16]),
        event_id: Ulid::from_parts(1, 1),
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let mismatched = MetadataMaterializationRecord {
        document_id: Ulid::from_bytes([43u8; 16]),
        event_id: Ulid::from_parts(1, 2),
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let mismatched_sidecar = MetadataMaterializationRecord {
        due_at_ms: 999,
        ..mismatched.clone()
    };
    let orphan_key = materialization_job_key(&orphan);
    let mismatched_key = materialization_job_key(&mismatched);
    write_entries(
        &storage,
        vec![
            materialization_job_entry(&orphan).unwrap(),
            materialization_job_entry(&mismatched).unwrap(),
            document_job_entry(&mismatched_sidecar).unwrap(),
        ],
    )
    .await;

    let (jobs, has_more_due, _next) = scan_due_jobs(&storage, now_ms, MATERIALIZATION_BATCH_SIZE)
        .await
        .unwrap();

    assert!(jobs.is_empty());
    assert!(!has_more_due);
    assert!(!storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, orphan_key.to_vec()).await);
    assert!(
        !storage_key_exists(
            &storage,
            MATERIALIZATION_JOB_KEYSPACE,
            mismatched_key.to_vec()
        )
        .await
    );
}

#[tokio::test]
async fn older_check_bounded() {
    // The predecessor check for one document must not scan the sidecar rows
    // of unrelated documents.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([44u8; 16]);
    let first = Ulid::from_parts(1, 1);
    let middle = Ulid::from_parts(2, 1);
    let last = Ulid::from_parts(3, 1);
    let first_event = create_event(document_id, first, "first");
    let job_for = |event_id: Ulid| MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let mut writes = vec![
        create_event_entry(&first_event).unwrap(),
        document_job_entry(&job_for(first)).unwrap(),
        document_job_entry(&job_for(middle)).unwrap(),
        document_job_entry(&job_for(last)).unwrap(),
    ];
    for index in 0..500u64 {
        let other = MetadataMaterializationRecord {
            document_id: Ulid::from_parts(9, u128::from(index)),
            event_id: Ulid::from_parts(9, u128::from(index)),
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        writes.push(document_job_entry(&other).unwrap());
    }
    write_entries(&storage, writes).await;

    let before = storage.snapshot_metrics().requests_total;
    let older = older_exists(&storage, document_id, middle, &BTreeSet::new())
        .await
        .unwrap();
    let delta = storage.snapshot_metrics().requests_total - before;

    assert!(older);
    assert!(delta <= 10, "older check issued {delta} storage requests");
}

fn application_failure() -> MetadataMaterializationError {
    MetadataMaterializationError::Metadata(MetadataError::Backend("boom".to_string()))
}

#[tokio::test]
async fn failure_cap_parks() {
    // The apply that spends the last of the failure budget parks as a dead
    // letter: both job rows go, the record that can requeue them stays.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([45u8; 16]);
    let event_id = Ulid::from_parts(1, 1);
    let event = create_event(document_id, event_id, "capped");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES - 1,
        failures: MATERIALIZATION_MAX_FAILURES - 1,
        parks: 0,
    };
    let index_key = materialization_job_key(&job);
    let sidecar_key = document_job_key(document_id, event_id);
    write_entries(
        &storage,
        vec![
            create_event_entry(&event).unwrap(),
            materialization_job_entry(&job).unwrap(),
            document_job_entry(&job).unwrap(),
        ],
    )
    .await;

    let parked =
        defer_materialization_job(index_key.as_ref(), &job, &event, &application_failure());
    assert!(matches!(parked, FinishedMaterializationJob::Parked { .. }));
    finish_completed_jobs(&storage, vec![parked]).await.unwrap();

    assert!(!storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, index_key.to_vec()).await);
    assert!(!storage_key_exists(&storage, DOCUMENT_JOB_KEYSPACE, sidecar_key.to_vec()).await);
    let status = read_materialization_status(&storage, document_id, None)
        .await
        .unwrap()
        .expect("failed status is written");
    assert_eq!(status.state, MaterializationState::Failed);
    assert_eq!(status.failures, MATERIALIZATION_MAX_FAILURES);
    assert!(!materialization_jobs_exist(&storage).await.unwrap());
    let dead_letter = read_dead_letter(&storage, document_id, event_id)
        .await
        .unwrap()
        .expect("dead letter is written");
    assert_eq!(dead_letter.parks, 1);
    assert!(dead_letter.requeue_at_ms > unix_timestamp_millis());
}

#[test]
fn cap_boundary_reschedules() {
    // Below the cap reschedules with failures advanced; at the cap parks.
    let document_id = Ulid::from_bytes([46u8; 16]);
    let event_id = Ulid::from_parts(2, 1);
    let event = create_event(document_id, event_id, "boundary");
    let reschedule = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES - 2,
        failures: MATERIALIZATION_MAX_FAILURES - 2,
        parks: 0,
    };
    let key = materialization_job_key(&reschedule);
    match defer_materialization_job(key.as_ref(), &reschedule, &event, &application_failure()) {
        FinishedMaterializationJob::Rescheduled { status, .. } => {
            assert_eq!(status.failures, MATERIALIZATION_MAX_FAILURES - 1);
        }
        other => panic!("expected reschedule, got {other:?}"),
    }
    let park = MetadataMaterializationRecord {
        failures: MATERIALIZATION_MAX_FAILURES - 1,
        parks: 0,
        ..reschedule
    };
    assert!(matches!(
        defer_materialization_job(key.as_ref(), &park, &event, &application_failure()),
        FinishedMaterializationJob::Parked { .. }
    ));
}

#[test]
fn transient_skips_cap() {
    // A storage timeout is infrastructure: it advances the retry backoff but
    // must never spend the failure budget that parks a job.
    let document_id = Ulid::from_bytes([47u8; 16]);
    let event_id = Ulid::from_parts(3, 1);
    let event = create_event(document_id, event_id, "transient");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES + 5,
        failures: MATERIALIZATION_MAX_FAILURES - 1,
        parks: 0,
    };
    let key = materialization_job_key(&job);
    let errors = [
        MetadataMaterializationError::Storage(StorageError::Timeout),
        MetadataMaterializationError::Storage(StorageError::TransactionConflict),
        // A failed durability flush runs after every apply; charging it would
        // park documents whenever the disk is the bottleneck.
        MetadataMaterializationError::Metadata(MetadataError::Persist("journal".to_string())),
    ];
    for error in errors {
        match defer_materialization_job(key.as_ref(), &job, &event, &error) {
            FinishedMaterializationJob::Rescheduled { status, .. } => {
                assert_eq!(status.failures, job.failures);
                assert_eq!(status.attempts, job.attempts + 1);
            }
            other => panic!("expected reschedule, got {other:?}"),
        }
    }
}

#[test]
fn iri_failure_transient() {
    // A bulk-lane QueueFull surfaced through the IRI index keeps its storage
    // cause, so it defers without spending the failure budget.
    let document_id = Ulid::from_bytes([51u8; 16]);
    let event_id = Ulid::from_parts(5, 1);
    let event = create_event(document_id, event_id, "indexed");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: MATERIALIZATION_MAX_FAILURES - 1,
        parks: 0,
    };
    let key = materialization_job_key(&job);
    let error =
        MetadataMaterializationError::from(MetadataIriError::Storage(StorageError::QueueFull));
    match defer_materialization_job(key.as_ref(), &job, &event, &error) {
        FinishedMaterializationJob::Rescheduled { status, .. } => {
            assert_eq!(status.failures, job.failures);
        }
        other => panic!("expected reschedule, got {other:?}"),
    }
}

#[test]
fn handle_failure_transient() {
    // Handle infrastructure failures do not spend the document failure budget.
    let document_id = Ulid::from_bytes([53u8; 16]);
    let event_id = Ulid::from_parts(7, 1);
    let event = create_event(document_id, event_id, "handle");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: MATERIALIZATION_MAX_FAILURES - 1,
        parks: 0,
    };
    let key = materialization_job_key(&job);
    let errors = [
        MetadataMaterializationError::Metadata(MetadataError::Storage(StorageError::QueueFull)),
        MetadataMaterializationError::Metadata(MetadataError::Storage(StorageError::Timeout)),
    ];
    for error in errors {
        match defer_materialization_job(key.as_ref(), &job, &event, &error) {
            FinishedMaterializationJob::Rescheduled { status, .. } => {
                assert_eq!(status.failures, job.failures);
            }
            other => panic!("expected reschedule, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn deadletters_requeue_due() {
    // A due dead letter returns to the queue with one failure of budget left
    // and a pending status; one that is not due yet stays parked.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([48u8; 16]);
    let event_id = Ulid::from_parts(4, 1);
    let event = create_event(document_id, event_id, "requeue");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES,
        failures: MATERIALIZATION_MAX_FAILURES,
        parks: 0,
    };
    let pending = DeadLetterRecord {
        job: job.clone(),
        last_error: "boom".to_string(),
        parked_at_ms: 1,
        parks: 1,
        requeue_at_ms: unix_timestamp_millis().saturating_add(600_000),
    };
    write_entries(
        &storage,
        vec![
            create_event_entry(&event).unwrap(),
            dead_letter_entry(&pending).unwrap(),
        ],
    )
    .await;
    assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 0);

    let due = DeadLetterRecord {
        requeue_at_ms: 1,
        ..pending
    };
    write_entries(&storage, vec![dead_letter_entry(&due).unwrap()]).await;
    assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 1);

    assert!(
        read_dead_letter(&storage, document_id, event_id)
            .await
            .unwrap()
            .is_none()
    );
    let requeued = read_document_job(&storage, document_id, event_id)
        .await
        .unwrap()
        .expect("job is requeued");
    assert_eq!(requeued.failures, MATERIALIZATION_MAX_FAILURES - 1);
    assert_eq!(requeued.attempts, 0);
    let status = read_materialization_status(&storage, document_id, None)
        .await
        .unwrap()
        .expect("status is reset");
    assert_eq!(status.state, MaterializationState::Pending);
    assert!(materialization_jobs_exist(&storage).await.unwrap());
}

#[tokio::test]
async fn requeued_parks_fast() {
    // A requeued dead letter carries one failure of budget, so a document
    // that is still poisonous re-parks after a single apply.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([53u8; 16]);
    let event_id = Ulid::from_parts(6, 1);
    let event = create_event(document_id, event_id, "poison");
    let parked = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES,
        failures: MATERIALIZATION_MAX_FAILURES,
        parks: 0,
    };
    let due = DeadLetterRecord {
        job: parked,
        last_error: "boom".to_string(),
        parked_at_ms: 1,
        parks: 1,
        requeue_at_ms: 1,
    };
    write_entries(
        &storage,
        vec![
            create_event_entry(&event).unwrap(),
            dead_letter_entry(&due).unwrap(),
        ],
    )
    .await;
    assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 1);

    let requeued = read_document_job(&storage, document_id, event_id)
        .await
        .unwrap()
        .expect("job is requeued");
    let key = materialization_job_key(&requeued);
    let FinishedMaterializationJob::Parked { job, status, .. } =
        defer_materialization_job(key.as_ref(), &requeued, &event, &application_failure())
    else {
        panic!("expected park");
    };

    // The requeue deleted the dead letter that held the park count, so the
    // job carries it: the second park must wait longer than the first.
    let reparked = parked_dead_letter(&job, &status, None);
    assert_eq!(reparked.parks, due.parks + 1);
    assert!(
        reparked.requeue_at_ms.saturating_sub(reparked.parked_at_ms) > requeue_after_ms(due.parks)
    );
}

// A status that already materialized `event_id`, as a newer event would.
fn materialized_status(
    document_id: Ulid,
    event: &MetadataEventRecord,
) -> MaterializationStatusRecord {
    let job = MetadataMaterializationRecord {
        document_id,
        event_id: event.event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    materialization_success_status(&job, event, None)
}

#[tokio::test]
async fn deadletter_drops_superseded() {
    // Requeueing a dead letter whose document has since materialized a newer
    // event would regress the projection, so the dead letter is dropped.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([54u8; 16]);
    let old_event_id = Ulid::from_parts(1, 1);
    let newer_event_id = Ulid::from_parts(2, 1);
    let old_event = create_event(document_id, old_event_id, "old");
    let newer_event = create_event(document_id, newer_event_id, "newer");
    let newer_status = materialized_status(document_id, &newer_event);
    let due = DeadLetterRecord {
        job: MetadataMaterializationRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES,
            failures: MATERIALIZATION_MAX_FAILURES,
            parks: 0,
        },
        last_error: "boom".to_string(),
        parked_at_ms: 1,
        parks: 1,
        requeue_at_ms: 1,
    };
    write_entries(
        &storage,
        vec![
            create_event_entry(&old_event).unwrap(),
            create_event_entry(&newer_event).unwrap(),
            materialization_status_entry(&newer_status).unwrap(),
            dead_letter_entry(&due).unwrap(),
        ],
    )
    .await;

    assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 0);

    assert!(
        read_dead_letter(&storage, document_id, old_event_id)
            .await
            .unwrap()
            .is_none()
    );
    let status = read_materialization_status(&storage, document_id, None)
        .await
        .unwrap()
        .expect("status survives");
    assert_eq!(status.event_id, newer_event_id);
    assert_eq!(status.state, MaterializationState::Materialized);
    assert!(
        read_document_job(&storage, document_id, old_event_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn requeue_aborts_raced() {
    // A newer finish must prevent this requeue from restoring an obsolete event.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([56u8; 16]);
    let old_event_id = Ulid::from_parts(1, 1);
    let newer_event_id = Ulid::from_parts(2, 1);
    let old_event = create_event(document_id, old_event_id, "old");
    let newer_event = create_event(document_id, newer_event_id, "newer");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id: old_event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES,
        failures: MATERIALIZATION_MAX_FAILURES,
        parks: 1,
    };
    let status = MaterializationStatusRecord {
        failures: job.failures,
        ..new_pending_status(&old_event, 1)
    };
    write_entries(&storage, vec![create_event_entry(&old_event).unwrap()]).await;

    let txn_id = start_write_transaction(&storage).await.unwrap();
    assert!(
        requeue_in_txn(&storage, txn_id, &job, &status)
            .await
            .unwrap()
    );

    let racing = start_write_transaction(&storage).await.unwrap();
    transactional_batch_write(
        &storage,
        racing,
        vec![
            materialization_status_entry(&materialized_status(document_id, &newer_event)).unwrap(),
        ],
    )
    .await
    .unwrap();
    commit_storage_transaction(&storage, racing).await.unwrap();

    assert!(commit_storage_transaction(&storage, txn_id).await.is_err());
    let current = read_materialization_status(&storage, document_id, None)
        .await
        .unwrap()
        .expect("racing status survives");
    assert_eq!(current.event_id, newer_event_id);
    assert!(
        read_document_job(&storage, document_id, old_event_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn park_skips_superseded() {
    // Parking a job the document has already moved past must not leave a
    // dead letter that a later sweep could resurrect.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([55u8; 16]);
    let old_event_id = Ulid::from_parts(1, 1);
    let newer_event_id = Ulid::from_parts(2, 1);
    let old_event = create_event(document_id, old_event_id, "old");
    let newer_event = create_event(document_id, newer_event_id, "newer");
    let newer_status = materialized_status(document_id, &newer_event);
    let job = MetadataMaterializationRecord {
        document_id,
        event_id: old_event_id,
        due_at_ms: 1,
        attempts: MATERIALIZATION_MAX_FAILURES - 1,
        failures: MATERIALIZATION_MAX_FAILURES - 1,
        parks: 0,
    };
    let job_key = materialization_job_key(&job);
    write_entries(
        &storage,
        vec![
            materialization_status_entry(&newer_status).unwrap(),
            materialization_job_entry(&job).unwrap(),
            document_job_entry(&job).unwrap(),
        ],
    )
    .await;

    let parked =
        defer_materialization_job(job_key.as_ref(), &job, &old_event, &application_failure());
    assert!(matches!(parked, FinishedMaterializationJob::Parked { .. }));
    let plan = plan_finish_chunk(&storage, vec![parked]).await.unwrap();

    assert!(
        !plan
            .writes
            .iter()
            .any(|(key_space, _, _)| key_space == DEAD_LETTER_KEYSPACE)
    );
    assert!(
        plan.deletes
            .contains(&(MATERIALIZATION_JOB_KEYSPACE.to_string(), job_key.clone()))
    );
    assert!(plan.deletes.contains(&(
        DOCUMENT_JOB_KEYSPACE.to_string(),
        document_job_key(document_id, old_event_id)
    )));
}

#[test]
fn effect_uses_event() {
    let document_id = Ulid::from_bytes([1u8; 16]);
    let event_id = Ulid::from_parts(1, 1);
    let event = create_event(document_id, event_id, "deterministic");
    let deterministic_actor = Some(deterministic_materialization_actor(event_id));

    match graph_materialization_effect(&event, None, false) {
        Effect::Metadata(MetadataEffect::CreateCrate { request }) => {
            assert_eq!(
                request.durability,
                MetadataRequestDurability::WalAlreadyDurable
            );
            assert_eq!(request.deterministic_actor, deterministic_actor);
        }
        other => panic!("unexpected materialization effect: {other:?}"),
    }

    let rocrate = with_payload(
        event.clone(),
        MetadataEventPayload::RoCrate {
            jsonld: "{}".to_string(),
        },
    );
    match graph_materialization_effect(&rocrate, None, false) {
        Effect::Metadata(MetadataEffect::ApplyRoCrate { request }) => {
            assert_eq!(
                request.durability,
                MetadataRequestDurability::WalAlreadyDurable
            );
            assert_eq!(request.deterministic_actor, deterministic_actor);
        }
        other => panic!("unexpected materialization effect: {other:?}"),
    }

    let data = with_payload(
        event.clone(),
        MetadataEventPayload::UpsertDataEntity {
            jsonld: r#"{"@id":"./file.txt","@type":"File","name":"file"}"#.to_string(),
        },
    );
    match graph_materialization_effect(&data, None, false) {
        Effect::Metadata(MetadataEffect::UpsertDataEntity { request }) => {
            assert_eq!(
                request.durability,
                MetadataRequestDurability::WalAlreadyDurable
            );
            assert_eq!(request.deterministic_actor, deterministic_actor);
        }
        other => panic!("unexpected materialization effect: {other:?}"),
    }

    let contextual = with_payload(
        event,
        MetadataEventPayload::UpsertContextualEntity {
            jsonld: r##"{"@id":"#lab","@type":"Organization","name":"lab"}"##.to_string(),
        },
    );
    match graph_materialization_effect(&contextual, None, false) {
        Effect::Metadata(MetadataEffect::UpsertContextualEntity { request }) => {
            assert_eq!(
                request.durability,
                MetadataRequestDurability::WalAlreadyDurable
            );
            assert_eq!(request.deterministic_actor, deterministic_actor);
        }
        other => panic!("unexpected materialization effect: {other:?}"),
    }

    let raw_revision = MetadataRawRevision {
        jsonld: r#"{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[]}"#.to_string(),
        winning_event_id: event_id,
        context_digest: [1; 32],
        dataset_digest: Some([2; 32]),
        merged: None,
    };
    match graph_materialization_effect(&contextual, Some(&raw_revision), true) {
        Effect::Metadata(MetadataEffect::ApplyRoCrate { request }) => {
            assert_eq!(request.jsonld, raw_revision.jsonld);
            assert_eq!(
                request.durability,
                MetadataRequestDurability::WalAlreadyDurable
            );
            assert_eq!(request.deterministic_actor, deterministic_actor);
        }
        other => panic!("unexpected materialization effect: {other:?}"),
    }
    assert!(matches!(
        graph_materialization_effect(&contextual, Some(&raw_revision), false),
        Effect::Metadata(MetadataEffect::UpsertContextualEntity { .. })
    ));
}

#[test]
fn replaying_same_event() {
    let document_id = Ulid::from_bytes([2u8; 16]);
    let event_id = Ulid::from_parts(2, 1);
    let event = create_event(document_id, event_id, "replay");
    let data = with_payload(
        event.clone(),
        MetadataEventPayload::UpsertDataEntity {
            jsonld: r#"{"@id":"./file.txt","@type":"File","name":"file"}"#.to_string(),
        },
    );

    for event in [event, data] {
        assert_eq!(
            graph_materialization_effect(&event, None, false),
            graph_materialization_effect(&event, None, false)
        );
    }
}

#[test]
fn newer_pending_status() {
    let document_id = Ulid::from_bytes([8u8; 16]);
    let older_event_id = Ulid::from_parts(8, 1);
    let newer_event_id = Ulid::from_parts(8, 2);
    let older_job = MetadataMaterializationRecord {
        document_id,
        event_id: older_event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let newer_pending = MaterializationStatusRecord {
        document_id,
        event_id: newer_event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 0,
        failures: 0,
        last_error: None,
        updated_at_ms: 1,
    };
    let newer_final = MaterializationStatusRecord {
        state: MaterializationState::Materialized,
        ..newer_pending.clone()
    };

    assert!(!status_obsoletes_job(&newer_pending, &older_job));
    assert!(status_obsoletes_job(&newer_final, &older_job));
}

#[test]
fn older_retry_status() {
    let document_id = Ulid::from_bytes([9u8; 16]);
    let older_event_id = Ulid::from_parts(9, 1);
    let newer_event_id = Ulid::from_parts(9, 2);
    let older_retry = MaterializationStatusRecord {
        document_id,
        event_id: older_event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 1,
        failures: 0,
        last_error: Some("transient".to_string()),
        updated_at_ms: 1,
    };
    let newer_pending = MaterializationStatusRecord {
        document_id,
        event_id: newer_event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 0,
        failures: 0,
        last_error: None,
        updated_at_ms: 2,
    };

    assert!(!should_write_retry(Some(&newer_pending), &older_retry));
    assert!(should_write_retry(None, &older_retry));
}

#[test]
fn stale_final_status() {
    let document_id = Ulid::from_bytes([29u8; 16]);
    let event_id = Ulid::from_parts(29, 1);
    let retry_status = MaterializationStatusRecord {
        document_id,
        event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 1,
        failures: 0,
        last_error: Some("transient".to_string()),
        updated_at_ms: 1,
    };
    let stale_final = MaterializationStatusRecord {
        state: MaterializationState::Materialized,
        last_error: None,
        updated_at_ms: 2,
        ..retry_status.clone()
    };
    let fresh_final = MaterializationStatusRecord {
        attempts: 2,
        ..stale_final.clone()
    };

    assert!(!should_write_final(Some(&retry_status), &stale_final));
    assert!(should_write_final(Some(&retry_status), &fresh_final));
}

// Rescheduled jobs for `count` distinct documents, rows already persisted.
async fn reschedule_batch(
    storage: &StorageHandle,
    count: usize,
) -> Vec<FinishedMaterializationJob> {
    let mut finished = Vec::new();
    for seed in 0..count {
        let document_id = Ulid::from_parts(900, seed as u128);
        let event_id = Ulid::from_parts(1, seed as u128);
        let event = create_event(document_id, event_id, "chunk");
        let job = MetadataMaterializationRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        write_entries(
            storage,
            vec![
                materialization_job_entry(&job).unwrap(),
                document_job_entry(&job).unwrap(),
            ],
        )
        .await;
        let key = materialization_job_key(&job);
        finished.push(defer_materialization_job(
            key.as_ref(),
            &job,
            &event,
            &application_failure(),
        ));
    }
    finished
}

#[tokio::test]
async fn finish_commits_chunks() {
    // More jobs than one chunk holds still resolve completely, and they do
    // so in several transactions so a late failure cannot undo early work.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let count = MATERIALIZATION_FINISH_CHUNK + 3;
    let finished = reschedule_batch(&storage, count).await;

    let before = storage.snapshot_metrics().requests_total;
    finish_completed_jobs(&storage, finished).await.unwrap();
    let delta = storage.snapshot_metrics().requests_total - before;

    // Two chunks, each a handful of requests: far below one transaction per
    // job, and each chunk commits on its own.
    assert!(delta <= 16, "finish issued {delta} storage requests");
    assert_eq!(index_job_keys(&storage).await.len(), count);
    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
}

#[tokio::test]
async fn finish_avoids_conflicts() {
    // A status write landing between the guard snapshot and the commit must
    // not conflict: the finish transaction only writes.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let finished = reschedule_batch(&storage, 4).await;
    let plan = plan_finish_chunk(&storage, finished).await.unwrap();

    let document_id = Ulid::from_bytes([120u8; 16]);
    let racing = MaterializationStatusRecord {
        document_id,
        event_id: Ulid::from_parts(9, 9),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 0,
        failures: 0,
        last_error: None,
        updated_at_ms: 9,
    };
    write_entries(
        &storage,
        vec![materialization_status_entry(&racing).unwrap()],
    )
    .await;

    let txn_id = start_write_transaction(&storage).await.unwrap();
    transactional_batch_write(&storage, txn_id, plan.writes)
        .await
        .unwrap();
    transactional_batch_delete(&storage, txn_id, plan.deletes)
        .await
        .unwrap();
    commit_storage_transaction(&storage, txn_id).await.unwrap();
    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
}

#[tokio::test]
async fn finish_keeps_newer() {
    // A newer job remains due if an older completion overwrites its status.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([77u8; 16]);
    let old_event_id = Ulid::from_parts(1, 1);
    let newer_event_id = Ulid::from_parts(2, 1);
    let old_event = create_event(document_id, old_event_id, "old");
    let newer_event = create_event(document_id, newer_event_id, "newer");
    let old_job = MetadataMaterializationRecord {
        document_id,
        event_id: old_event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let (_, old_job_key, _) = materialization_job_entry(&old_job).unwrap();
    write_entries(
        &storage,
        vec![
            create_event_entry(&old_event).unwrap(),
            materialization_job_entry(&old_job).unwrap(),
            document_job_entry(&old_job).unwrap(),
        ],
    )
    .await;
    let finished = vec![FinishedMaterializationJob::Completed(
        CompletedMaterializationJob {
            job_key: old_job_key.to_vec(),
            document_job_key: Some(document_job_key(document_id, old_event_id).to_vec()),
            status: Some(materialization_success_status(&old_job, &old_event, None)),
            iri_index_writes: Vec::new(),
            raw_state_write: None,
            validation_write: None,
            sync: None,
        },
    )];
    let plan = plan_finish_chunk(&storage, finished).await.unwrap();

    // The newer event lands after the snapshot was taken.
    let newer_job = MetadataMaterializationRecord {
        document_id,
        event_id: newer_event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    write_entries(
        &storage,
        vec![
            create_event_entry(&newer_event).unwrap(),
            materialization_status_entry(&new_pending_status(&newer_event, 2)).unwrap(),
            materialization_job_entry(&newer_job).unwrap(),
            document_job_entry(&newer_job).unwrap(),
        ],
    )
    .await;

    let txn_id = start_write_transaction(&storage).await.unwrap();
    transactional_batch_write(&storage, txn_id, plan.writes)
        .await
        .unwrap();
    transactional_batch_delete(&storage, txn_id, plan.deletes)
        .await
        .unwrap();
    commit_storage_transaction(&storage, txn_id).await.unwrap();

    let (jobs, _, _) = scan_due_jobs(
        &storage,
        unix_timestamp_millis(),
        MATERIALIZATION_BATCH_SIZE,
    )
    .await
    .unwrap();
    assert_eq!(
        jobs.iter().map(|(_, job)| job.event_id).collect::<Vec<_>>(),
        vec![newer_event_id],
        "the newer job survives the older completion"
    );
}

#[tokio::test]
async fn finish_not_regress() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([3u8; 16]);
    let old_event_id = Ulid::from_parts(3, 1);
    let newer_event_id = Ulid::from_parts(4, 1);
    let old_event = create_event(document_id, old_event_id, "old");
    let old_job = MetadataMaterializationRecord {
        document_id,
        event_id: old_event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let newer_status = MaterializationStatusRecord {
        document_id,
        event_id: newer_event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 7,
        failures: 0,
        last_error: Some("newer pending".to_string()),
        updated_at_ms: 7,
    };
    let (_, old_job_key, _) = materialization_job_entry(&old_job).unwrap();
    let stale_index_key = vec![9u8; 16];
    let raw_state_key = raw_revision_key(document_id);
    write_entries(
        &storage,
        vec![
            materialization_status_entry(&newer_status).unwrap(),
            materialization_job_entry(&old_job).unwrap(),
            document_job_entry(&old_job).unwrap(),
        ],
    )
    .await;

    finish_completed_jobs(
        &storage,
        vec![FinishedMaterializationJob::Completed(
            CompletedMaterializationJob {
                job_key: old_job_key.to_vec(),
                document_job_key: Some(
                    document_job_key(old_job.document_id, old_job.event_id).to_vec(),
                ),
                status: Some(materialization_success_status(&old_job, &old_event, None)),
                iri_index_writes: vec![(
                    IRI_INDEX_KEYSPACE.to_string(),
                    ByteView::from(stale_index_key.clone()),
                    ByteView::from(vec![1]),
                )],
                raw_state_write: Some((
                    RAW_REVISION_KEYSPACE.to_string(),
                    raw_state_key.clone(),
                    ByteView::from(vec![1]),
                )),
                validation_write: None,
                sync: None,
            },
        )],
    )
    .await
    .unwrap();

    assert_eq!(
        read_materialization_status(&storage, document_id, None)
            .await
            .unwrap(),
        Some(newer_status)
    );
    assert!(!storage_key_exists(&storage, IRI_INDEX_KEYSPACE, stale_index_key,).await);
    assert!(!storage_key_exists(&storage, RAW_REVISION_KEYSPACE, raw_state_key.to_vec(),).await);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
            key: old_job_key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: DOCUMENT_JOB_KEYSPACE.to_string(),
            key: document_job_key(old_job.document_id, old_job.event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn supersedes_prior_rows() {
    // A second revision must leave only its own cursor's IRI index rows.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([5u8; 16]);

    let build = |event_id: Ulid| {
        let event = create_event(document_id, event_id, "rev");
        let job = MetadataMaterializationRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        CompletedMaterializationJob {
            job_key: materialization_job_entry(&job).unwrap().1.to_vec(),
            document_job_key: Some(document_job_key(document_id, event_id).to_vec()),
            status: Some(materialization_success_status(&job, &event, None)),
            iri_index_writes: vec![(
                IRI_INDEX_KEYSPACE.to_string(),
                aruna_core::storage_entries::iri_reference_key("p", "o", document_id, event_id),
                ByteView::from(vec![1u8]),
            )],
            raw_state_write: Some((
                RAW_REVISION_KEYSPACE.to_string(),
                raw_revision_key(document_id),
                ByteView::from(event_id.to_bytes().to_vec()),
            )),
            validation_write: None,
            sync: None,
        }
    };

    let first = Ulid::from_parts(1, 1);
    let second = Ulid::from_parts(2, 1);
    finish_completed_jobs(
        &storage,
        vec![FinishedMaterializationJob::Completed(build(first))],
    )
    .await
    .unwrap();
    finish_completed_jobs(
        &storage,
        vec![FinishedMaterializationJob::Completed(build(second))],
    )
    .await
    .unwrap();

    let key_of = |cursor: Ulid| {
        aruna_core::storage_entries::iri_reference_key("p", "o", document_id, cursor)
            .as_ref()
            .to_vec()
    };
    assert!(
        !storage_key_exists(&storage, IRI_INDEX_KEYSPACE, key_of(first)).await,
        "prior cursor rows must be removed"
    );
    assert!(
        storage_key_exists(&storage, IRI_INDEX_KEYSPACE, key_of(second)).await,
        "current cursor rows must remain"
    );
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: RAW_REVISION_KEYSPACE.to_string(),
            key: raw_revision_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => assert_eq!(value.as_ref(), second.to_bytes()),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn prune_resumes_parked() {
    // Park the cursor when deleted jobs leave no other way to retry a failed prune.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([57u8; 16]);
    let stale = Ulid::from_parts(1, 1);
    let current = Ulid::from_parts(2, 1);
    let stale_key = aruna_core::storage_entries::iri_reference_key("p", "o", document_id, stale);
    write_entries(
        &storage,
        vec![
            (
                IRI_INDEX_KEYSPACE.to_string(),
                stale_key.clone(),
                ByteView::from(vec![1u8]),
            ),
            materialization_prune_entry(document_id, current).unwrap(),
        ],
    )
    .await;

    prune_superseded_rows(&storage, HashMap::new())
        .await
        .unwrap();

    assert!(!storage_key_exists(&storage, IRI_INDEX_KEYSPACE, stale_key.to_vec()).await);
    assert!(
        !storage_key_exists(
            &storage,
            MATERIALIZATION_PRUNE_KEYSPACE,
            materialization_prune_key(document_id).to_vec()
        )
        .await
    );
}

#[tokio::test]
async fn older_check_local() {
    // Predecessor lookups read the status then one document-local sidecar
    // scan; they never fall back to a full global keyspace scan.
    let (storage, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    let document_id = Ulid::from_bytes([11u8; 16]);
    let event_id = Ulid::from_parts(11, 2);
    let scripted = thread::spawn(move || {
        let (effect, response_tx, _span, _enqueued_at, _in_flight) =
            receiver.recv().expect("status read request");
        let status_key = match effect {
            StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            } => {
                assert_eq!(key_space, MATERIALIZATION_STATUS_KEYSPACE);
                assert_eq!(key, materialization_status_key(document_id));
                key
            }
            other => panic!("unexpected storage effect: {other:?}"),
        };
        response_tx.send(StorageEvent::ReadResult {
            key: status_key,
            value: None,
        });

        let (effect, response_tx, _span, _enqueued_at, _in_flight) =
            receiver.recv().expect("document-local predecessor scan");
        match effect {
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id: None,
            } => {
                assert_eq!(key_space, DOCUMENT_JOB_KEYSPACE);
                assert_eq!(prefix, Some(document_job_prefix(document_id)));
                assert_eq!(start, None);
                assert_eq!(limit, MATERIALIZATION_PAGE_SIZE);
            }
            other => panic!("unexpected storage effect: {other:?}"),
        }
        response_tx.send(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        });

        assert!(
            receiver.recv().is_err(),
            "no global predecessor scan is issued"
        );
    });

    assert!(
        !older_exists(&storage, document_id, event_id, &BTreeSet::new())
            .await
            .unwrap()
    );
    drop(storage);
    scripted.join().expect("scripted storage actor finished");
}

#[tokio::test]
async fn older_queued_job() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([10u8; 16]);
    let older_event_id = Ulid::from_parts(10, 1);
    let newer_event_id = Ulid::from_parts(10, 2);
    let older_job = MetadataMaterializationRecord {
        document_id,
        event_id: older_event_id,
        due_at_ms: 30_000,
        attempts: 1,
        failures: 0,
        parks: 0,
    };
    let newer_pending = MaterializationStatusRecord {
        document_id,
        event_id: newer_event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        context_digest: None,
        dataset_digest: None,
        state: MaterializationState::Pending,
        attempts: 0,
        failures: 0,
        last_error: None,
        updated_at_ms: 1,
    };
    write_entries(
        &storage,
        vec![
            create_event_entry(&create_event(document_id, older_event_id, "older-queued")).unwrap(),
            materialization_status_entry(&newer_pending).unwrap(),
            materialization_job_entry(&older_job).unwrap(),
            document_job_entry(&older_job).unwrap(),
        ],
    )
    .await;

    assert!(
        older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
            .await
            .unwrap()
    );

    let mut advanced = BTreeSet::new();
    advanced.insert(older_event_id);
    assert!(
        !older_exists(&storage, document_id, newer_event_id, &advanced)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn reschedules_batched() {
    // Three failing jobs across distinct documents resolve in one finish
    // transaction: the request delta stays at single-transaction scale.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let jobs: Vec<_> = (0..3u8)
        .map(|seed| {
            let document_id = Ulid::from_bytes([70 + seed; 16]);
            let event_id = Ulid::from_parts(1, u128::from(seed));
            let event = create_event(document_id, event_id, "retry");
            let job = MetadataMaterializationRecord {
                document_id,
                event_id,
                due_at_ms: 1,
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            let index_key = materialization_job_key(&job);
            (job, event, index_key)
        })
        .collect();
    for (job, _, _) in &jobs {
        write_entries(
            &storage,
            vec![
                materialization_job_entry(job).unwrap(),
                document_job_entry(job).unwrap(),
            ],
        )
        .await;
    }
    let finished: Vec<_> = jobs
        .iter()
        .map(|(job, event, index_key)| {
            defer_materialization_job(index_key.as_ref(), job, event, &application_failure())
        })
        .collect();
    assert!(
        finished
            .iter()
            .all(|finished| matches!(finished, FinishedMaterializationJob::Rescheduled { .. }))
    );

    let before = storage.snapshot_metrics().requests_total;
    finish_completed_jobs(&storage, finished).await.unwrap();
    let delta = storage.snapshot_metrics().requests_total - before;

    // A single finish transaction issues far fewer requests than one
    // transaction per job would.
    assert!(delta <= 10, "finish issued {delta} storage requests");
    for (job, _, _) in &jobs {
        let requeued = read_document_job(&storage, job.document_id, job.event_id)
            .await
            .unwrap()
            .expect("job requeued");
        assert_eq!(requeued.attempts, 1);
    }

    // Exactly the rescheduled index rows survive: the old due_at=1 keys are
    // gone and each document keeps one new-due row.
    let index_keys = index_job_keys(&storage).await;
    assert_eq!(index_keys.len(), jobs.len());
    for (job, _, old_key) in &jobs {
        assert!(
            !storage_key_exists(&storage, MATERIALIZATION_JOB_KEYSPACE, old_key.to_vec()).await
        );
        let row = index_keys
            .iter()
            .find(|(_, doc, event)| *doc == job.document_id && *event == job.event_id)
            .expect("rescheduled index row present");
        assert_ne!(row.0, job.due_at_ms);
    }
}

#[tokio::test]
async fn failing_apply_reschedules() {
    // A non-terminal apply failure (missing metadata handle) reschedules the
    // job with attempts advanced and both rows kept at a new due time.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let document_id = Ulid::from_bytes([90u8; 16]);
    let event_id = Ulid::from_parts(3, 1);
    let event = create_event(document_id, event_id, "reschedule");
    let job = MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms: 1,
        attempts: 0,
        failures: 0,
        parks: 0,
    };
    let old_index_key = materialization_job_key(&job);
    write_entries(
        &storage,
        vec![
            create_event_entry(&event).unwrap(),
            materialization_job_entry(&job).unwrap(),
            document_job_entry(&job).unwrap(),
        ],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let result = process_materialization_batch(&context)
        .await
        .expect("batch drains");
    assert_eq!(result.processed, 1);

    let requeued = read_document_job(&storage, document_id, event_id)
        .await
        .unwrap()
        .expect("job rescheduled");
    assert_eq!(requeued.attempts, 1);
    assert!(
        !storage_key_exists(
            &storage,
            MATERIALIZATION_JOB_KEYSPACE,
            old_index_key.to_vec()
        )
        .await
    );
    let index_keys = index_job_keys(&storage).await;
    assert_eq!(index_keys.len(), 1);
    assert_eq!(index_keys[0].1, document_id);
    assert_eq!(index_keys[0].2, event_id);
    assert_ne!(index_keys[0].0, job.due_at_ms);
}

#[test]
fn sync_dedupes_graphs() {
    let graph_iri = MetadataRegistryRecord::graph_iri_for(Ulid::from_bytes([50u8; 16]));
    let completed = |peers: Vec<NodeId>| {
        FinishedMaterializationJob::Completed(CompletedMaterializationJob {
            job_key: vec![1],
            document_job_key: None,
            status: None,
            iri_index_writes: Vec::new(),
            raw_state_write: None,
            validation_write: None,
            sync: Some(CompletedMaterializationSync {
                graph_iri: graph_iri.clone(),
                peers,
            }),
        })
    };
    let finished = vec![completed(vec![node(1)]), completed(vec![node(2)])];

    let syncs = dedupe_graph_syncs(&finished);

    assert_eq!(syncs.len(), 1);
    assert_eq!(syncs[0].graph_iri, graph_iri);
    assert_eq!(syncs[0].peers, vec![node(2)]);
}
