//! Projects events, drains markers and restores the projection timer on stored rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::testing::write_entries;
use aruna_core::UserId;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::handle::Handle;
use aruna_core::metadata::{MetadataEventPayload, MetadataLifecycleRecord};
use aruna_core::placement::registry_placement;
use aruna_core::storage_entries::{create_event_entry, pending_projection_key};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::{PlacementRef, PlacementStrategy};
use aruna_core::task::TaskKey;
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::mpsc;

async fn project_create_events(
    context: &DriverContext,
    events: Vec<MetadataEventRecord>,
    local_node_id: Option<NodeId>,
) -> Result<usize, MetadataProjectionError> {
    let project = ProjectEventsOperation::new(events, local_node_id, max_clock_skew());
    drive(project, context).await
}

async fn drain_projection_queue(
    context: &DriverContext,
) -> Result<PendingProjectionResult, MetadataProjectionError> {
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    drive(
        DrainProjectionOperation::new(local_node, max_clock_skew()),
        context,
    )
    .await
}

async fn transactional_projection_write(
    context: &DriverContext,
    writes: Vec<(String, ByteView, ByteView)>,
    graph_iris: BTreeSet<String>,
    fence: &WriteFence,
) -> Result<(), MetadataProjectionError> {
    let commit = ProjectionCommitOperation {
        writes,
        graph_iris,
        fence: fence.clone(),
    };
    drive(commit, context).await
}

async fn project_create_event(
    context: &DriverContext,
    event: MetadataEventRecord,
    local_node_id: Option<NodeId>,
) -> Result<(), MetadataProjectionError> {
    project_create_events(context, vec![event], local_node_id)
        .await
        .map(|_| ())
}

struct RecordingTaskHandler {
    seen: mpsc::Sender<TaskKey>,
}

#[async_trait]
impl InboundTaskHandler for RecordingTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
        let _ = self.seen.send(key).await;
    }
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn create_event() -> MetadataEventRecord {
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let group_id = Ulid::generate();
    let document_id = Ulid::generate();
    let event_id = Ulid::generate();
    let document_path = "datasets/outbox-lifecycle";
    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: document_path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            document_path,
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node(2)],
        created_at_ms: 1_000,
        updated_at_ms: 1_000,
        establishing_event_id: event_id,
        last_event_id: event_id,
    };
    MetadataEventRecord {
        event_id,
        record,
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        node_id: node(1),
        payload: MetadataEventPayload::Scaffold {
            name: "Lifecycle Outbox".to_string(),
            description: "Projector outbox envelope".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
        },
        occurred_at_ms: 1_000,
    }
}

// Stands in for the create operation: stamps the bucket its origin would
// have chosen from the buckets it holds.
fn stamped(mut event: MetadataEventRecord, config: &RealmConfigDocument) -> MetadataEventRecord {
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: event.record.document_id,
    };
    let context = aruna_core::placement::PlacementResolutionContext {
        group_id: Some(event.record.group_id),
        metadata_path: Some(event.record.document_path.as_str()),
    };
    let (strategy, _) = aruna_core::placement::strategy_for_target(config, &target, context)
        .expect("strategy resolves");
    event.record.placement = aruna_core::placement::choose_origin_bucket(
        config,
        strategy,
        event.node_id,
        &aruna_core::placement::subject_bytes(&target),
    )
    .expect("origin holds a bucket");
    event
}

fn realm_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(*node, RealmNodeKind::Server);
    }
    config
}

async fn store_realm_config(storage: &StorageHandle, config: &RealmConfigDocument) {
    let target = DocumentTarget::RealmConfig {
        realm_id: config.realm_id,
    };
    let bytes = postcard::to_allocvec(config).expect("realm config serializes");
    match storage
        .send_effect(crate::document_repository::write_effect(
            &target, bytes, None,
        ))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// A realm whose buckets are activated at generation one, so a projection
/// resolves a generation and takes the bucket's fence.
fn activated_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = realm_config(realm_id, nodes);
    config.snapshot_candidate_map();
    config
}

async fn outbox_rows(storage: &StorageHandle) -> Vec<DocumentOutboxRecord> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 64,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| postcard::from_bytes(&value).expect("outbox row decodes"))
            .collect(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn projection_takes_fence() {
    // An admitted projection stamps the generation it resolved at onto
    // every outbox row it commits.
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let mut event = create_event();
    event.node_id = node(1);
    let config = activated_config(event.record.realm_id, &[node(1), node(2), node(3)]);
    let event = stamped(event, &config);
    store_realm_config(&storage, &config).await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    project_create_events(&context, vec![event.clone()], Some(event.node_id))
        .await
        .expect("projection succeeds");
    let rows = outbox_rows(&storage).await;
    assert!(!rows.is_empty(), "an admitted projection publishes");
    for row in rows {
        assert_eq!(row.generation, 1, "row for {:?}", row.placement);
    }
}

#[tokio::test]
async fn fence_rejects_create() {
    // The departing holder closed generation one: the projection must not
    // commit a row the drained bucket can no longer publish.
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let mut event = create_event();
    event.node_id = node(1);
    let config = activated_config(event.record.realm_id, &[node(1), node(2), node(3)]);
    let event = stamped(event, &config);
    store_realm_config(&storage, &config).await;
    crate::driver::drive(
        crate::placement::process_transitions::CloseFenceOperation::new(
            event.record.realm_id,
            event.record.placement,
            1,
        ),
        &DriverContext::storage_only(storage.clone()),
    )
    .await
    .expect("the departing holder closes the fence");
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let error = project_create_events(&context, vec![event.clone()], Some(event.node_id))
        .await
        .expect_err("the fenced projection is rejected");
    assert!(
        matches!(error, MetadataProjectionError::PlacementFenced),
        "unexpected error: {error:?}"
    );
    assert!(
        outbox_rows(&storage).await.is_empty(),
        "a fenced projection commits nothing"
    );
}

#[tokio::test]
async fn deferred_mint_retries() {
    // A locally authored create whose shard has no live holder must keep its
    // pending marker so a later placement change still mints the genesis.
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let mut event = create_event();
    event.node_id = node(1);
    let live = realm_config(event.record.realm_id, &[node(1), node(2), node(3)]);
    let event = stamped(event, &live);
    let mut holderless = live.clone();
    holderless.nodes.clear();
    store_realm_config(&storage, &holderless).await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let marker_key = pending_projection_key(event.record.document_id, event.event_id);

    let projected = project_create_events(&context, vec![event.clone()], Some(event.node_id))
        .await
        .expect("projection succeeds");
    assert_eq!(projected, 1);
    assert!(pending_marker_exists(&storage, marker_key.to_vec()).await);

    store_realm_config(&storage, &live).await;
    let projected = project_create_events(&context, vec![event.clone()], Some(event.node_id))
        .await
        .expect("retry projection succeeds");

    assert_eq!(projected, 1);
    assert!(!pending_marker_exists(&storage, marker_key.to_vec()).await);
}

async fn pending_marker_exists(storage: &StorageHandle, key: Vec<u8>) -> bool {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: PENDING_PROJECTION_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn tombstone_blocks_projection() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let event = create_event();
    let tombstone = GraphLifecycleRecord::deleted(
        event.record.graph_iri.clone(),
        event.record.realm_id,
        event.record.group_id,
        event.record.document_id,
        2,
    );
    write_entries(
        &storage,
        vec![
            aruna_core::storage_entries::graph_lifecycle_entry(&tombstone)
                .expect("tombstone serializes"),
        ],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = transactional_projection_write(
        &context,
        aruna_core::storage_entries::registry_write_entries(&event.record)
            .expect("registry writes serialize"),
        BTreeSet::from([event.record.graph_iri.clone()]),
        &Default::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(MetadataProjectionError::Storage(
            StorageError::TransactionConflict
        ))
    ));
    assert!(matches!(
        storage
            .send_storage_effect(StorageEffect::Read {
                key_space: aruna_core::keyspaces::METADATA_INDEX_KEYSPACE.to_string(),
                key: aruna_core::storage_entries::metadata_registry_key(
                    event.record.group_id,
                    event.record.document_id,
                ),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[tokio::test]
async fn malformed_marker_deleted() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let corrupt_key = b"short".to_vec();
    write_entries(
        &storage,
        vec![(
            PENDING_PROJECTION_KEYSPACE.to_string(),
            ByteView::from(corrupt_key.clone()),
            ByteView::from(Vec::new()),
        )],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = drain_projection_queue(&context)
        .await
        .expect("projection drain succeeds");

    assert_eq!(result.markers_examined, 0);
    assert_eq!(result.projected, 0);
    assert!(!result.has_more);
    assert!(!pending_marker_exists(&storage, corrupt_key).await);
}

#[tokio::test]
async fn malformed_before_valid() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let corrupt_key = b"short".to_vec();
    let event = create_event();
    let valid_key = pending_projection_key(event.record.document_id, event.event_id);
    write_entries(
        &storage,
        vec![
            (
                PENDING_PROJECTION_KEYSPACE.to_string(),
                ByteView::from(corrupt_key.clone()),
                ByteView::from(Vec::new()),
            ),
            create_event_entry(&event).expect("event log entry"),
            (
                PENDING_PROJECTION_KEYSPACE.to_string(),
                valid_key.clone(),
                ByteView::from(Vec::new()),
            ),
        ],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = drain_projection_queue(&context)
        .await
        .expect("projection drain succeeds");

    assert_eq!(result.markers_examined, 1);
    assert_eq!(result.projected, 1);
    assert!(!pending_marker_exists(&storage, corrupt_key).await);
    assert!(!pending_marker_exists(&storage, valid_key.to_vec()).await);
}

#[tokio::test]
async fn orphan_pending_marker() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let document_id = Ulid::from_bytes([21u8; 16]);
    let event_id = Ulid::from_parts(21, 1);
    let marker_key = pending_projection_key(document_id, event_id);
    write_entries(
        &storage,
        vec![(
            PENDING_PROJECTION_KEYSPACE.to_string(),
            marker_key.clone(),
            ByteView::from(Vec::new()),
        )],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = drain_projection_queue(&context)
        .await
        .expect("projection drain succeeds");

    assert_eq!(result.markers_examined, 0);
    assert_eq!(result.projected, 0);
    assert!(!pending_marker_exists(&storage, marker_key.to_vec()).await);
}

#[tokio::test]
async fn timer_for_marker() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let event = create_event();
    write_entries(
        &storage,
        vec![
            create_event_entry(&event).expect("event log entry"),
            (
                PENDING_PROJECTION_KEYSPACE.to_string(),
                pending_projection_key(event.record.document_id, event.event_id),
                ByteView::from(Vec::new()),
            ),
        ],
    )
    .await;
    let task_handle = TaskHandle::new();
    let (seen_tx, mut seen_rx) = mpsc::channel(1);
    task_handle
        .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
        .await;

    let mut context = crate::testing::test_context(storage.clone());
    context.task_handle = Some(task_handle.clone());
    drive(ProjectionTimerOperation::new(), &context)
        .await
        .expect("timer restores");

    let restored_key = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
        .await
        .expect("restored projection timer should fire")
        .expect("recording handler should receive timer key");
    assert_eq!(restored_key, TaskKey::DrainProjectionQueue);
}

#[test]
fn origin_holders_frozen() {
    let mut event = create_event();
    event.node_id = node(1);
    event.record.holder_node_ids = vec![node(1)];
    let config = realm_config(event.record.realm_id, &[node(1), node(2), node(3), node(4)]);
    let event = stamped(event, &config);

    let expanded = expand_event_holders(event.clone());

    assert_eq!(expanded.record.holder_node_ids, vec![node(1)]);
    assert!(expanded.record.holder_node_ids.contains(&event.node_id));
    assert_eq!(expanded.record.last_event_id, event.event_id);
}

// The origin's outbox record rides the bucket stored on the record, and its
// peers are that bucket's live holders.
#[tokio::test]
async fn outbox_rides_stored() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let event = create_event();
    let config = realm_config(event.record.realm_id, &[event.node_id, node(2), node(3)]);
    let event = stamped(event, &config);
    write_entries(
        &storage,
        vec![(
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*event.record.realm_id.as_bytes()),
            ByteView::from(postcard::to_allocvec(&config).expect("config serializes")),
        )],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    project_create_event(&context, event.clone(), Some(event.node_id))
        .await
        .expect("projection succeeds");

    let expected_holders = resolve_shard_holders(&config, &event.record.placement);

    // Two publishes, on two topics: the lifecycle event onto the document's own capped
    // bucket, the registry row onto the everywhere-bound registry class.
    let records = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 4,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .iter()
            .map(|(_, value)| {
                postcard::from_bytes::<DocumentOutboxRecord>(value.as_ref())
                    .expect("outbox record decodes")
            })
            .collect::<Vec<_>>(),
        other => panic!("expected outbox iter result, got {other:?}"),
    };
    assert_eq!(records.len(), 2);

    let outbox = records
        .iter()
        .find(|record| {
            matches!(
                record.target,
                DocumentTarget::MetadataDocumentLifecycle { .. }
            )
        })
        .expect("origin projection writes a lifecycle outbox record");
    assert_eq!(outbox.placement, event.record.placement);
    assert_eq!(outbox.peers, expected_holders);
    let DocumentOutboxEvent::Upsert { change, .. } = &outbox.event else {
        panic!("expected lifecycle upsert outbox event");
    };
    assert_eq!(change.placement, event.record.placement);

    let registry = records
        .iter()
        .find(|record| matches!(record.target, DocumentTarget::MetadataRegistry { .. }))
        .expect("origin projection writes a registry outbox record");
    let registry_ref = registry_placement(&config, &event.record);
    assert_eq!(registry.placement, registry_ref);
    assert_ne!(registry_ref, event.record.placement);
}

/// Rows that arrived before the create event: none, its registry row, or the registry row
/// and the materialized status of a newer update.
#[derive(Clone, Copy, PartialEq)]
enum Before {
    Nothing,
    Registry,
    Update,
}

fn arrived_before(
    event: &MetadataEventRecord,
    before: Before,
) -> Vec<(String, ByteView, ByteView)> {
    let mut record = event.record.clone();
    if before == Before::Update {
        record.last_event_id = Ulid::from_parts(event.event_id.timestamp_ms() + 1, 1);
        record.updated_at_ms += 1_000;
    }
    let mut rows = aruna_core::storage_entries::registry_write_entries(&record).unwrap();
    if before == Before::Update {
        let status = MaterializationStatusRecord {
            document_id: record.document_id,
            event_id: record.last_event_id,
            graph_iri: record.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state: aruna_core::metadata::MaterializationState::Materialized,
            attempts: 1,
            failures: 0,
            last_error: None,
            updated_at_ms: record.updated_at_ms,
        };
        let entry = aruna_core::storage_entries::materialization_status_entry(&status);
        rows.push(entry.unwrap());
    }
    match before {
        Before::Nothing => Vec::new(),
        Before::Registry | Before::Update => rows,
    }
}

// A create is journaled once on an infrastructure node with the origin's anchor, also when
// its registry row or a newer update arrived first; devices journal nothing.
#[tokio::test]
async fn journals_first_projection() {
    let journaled = async |local: NodeId, kind: RealmNodeKind, before: Before| {
        journaled_aged(local, kind, before, 0, None).await
    };
    let holder = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let (_, late) = journaled(holder, RealmNodeKind::Server, Before::Registry).await;
    assert_eq!(late.len(), 1);
    let (_, updated) = journaled(holder, RealmNodeKind::Server, Before::Update).await;
    assert_eq!(updated.len(), 1);
    let (event, rows) = journaled(holder, RealmNodeKind::Server, Before::Nothing).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].source,
        format!("dataset_created/{}", event.event_id)
    );
    assert_eq!(rows[0].anchor_ms, event.event_id.timestamp_ms());
    assert_eq!(rows[0].resource, event.record.document_id.to_string());
    let owner = UserId::local(Ulid::from_parts(4, 4), event.record.realm_id);
    let (_, none) = journaled(holder, RealmNodeKind::User { owner }, Before::Nothing).await;
    assert!(none.is_empty());
}

// A change is journaled once as an update, also as a merge older than the materialized
// status; a checkpoint journals nothing.
#[tokio::test]
async fn journals_projected_update() {
    let holder = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let batch = MetadataEventPayload::ApplyBatch {
        batch: aruna_core::metadata::MetadataBatch {
            graph_iri: "urn:graph".to_string(),
            actor: [7u8; 32],
            counter: 1,
            base_clock: craqle::VectorClock::default(),
            ops: Vec::new(),
            timestamp_millis: 1,
        },
        authored: aruna_core::metadata::MetadataBatchSource::UpsertDataEntity {
            jsonld: "{}".to_string(),
        },
    };
    for before in [Before::Nothing, Before::Update] {
        let change = Some(batch.clone());
        let (event, rows) = journaled_aged(holder, RealmNodeKind::Server, before, 0, change).await;
        let [row] = rows.as_slice() else {
            panic!("expected one journal row, got {rows:?}");
        };
        let kind = aruna_core::plugin_runs::SourceKind::DatasetUpdated;
        assert_eq!(row.kind, kind);
        assert_eq!(row.source, format!("dataset_updated/{}", event.event_id));
    }
    let checkpoint = Some(MetadataEventPayload::Checkpoint { snapshot: None });
    let (_, none) = journaled_aged(
        holder,
        RealmNodeKind::Server,
        Before::Nothing,
        0,
        checkpoint,
    )
    .await;
    assert!(none.is_empty());
}

// A holder that returns after eight days still journals a create its receivers could run.
#[tokio::test]
async fn journals_delayed_create() {
    let holder = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let age = 8 * 24 * 3600 * 1000;
    let (event, rows) =
        journaled_aged(holder, RealmNodeKind::Server, Before::Nothing, age, None).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].anchor_ms, event.event_id.timestamp_ms());
    let gone = DEDUP_RETENTION_MS + 1;
    let (_, none) =
        journaled_aged(holder, RealmNodeKind::Server, Before::Nothing, gone, None).await;
    assert!(none.is_empty());
}

/// Projects a create, or the given change, minted `age_ms` ago twice on `local` and returns
/// its journal rows.
async fn journaled_aged(
    local: NodeId,
    kind: RealmNodeKind,
    before: Before,
    age_ms: u64,
    change: Option<MetadataEventPayload>,
) -> (MetadataEventRecord, Vec<SourceEvent>) {
    let dir = tempdir().expect("temp dir");
    let storage = FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage");
    let mut event = create_event();
    if let Some(payload) = change {
        event.payload = payload;
    }
    let minted = aruna_core::time::unix_timestamp_millis() - age_ms;
    event.event_id = Ulid::from_parts(minted, event.event_id.random());
    event.record.establishing_event_id = event.event_id;
    event.record.last_event_id = event.event_id;
    let mut config = realm_config(event.record.realm_id, &[]);
    config.ensure_node(local, kind);
    let config_row = (
        aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
        ByteView::from(*event.record.realm_id.as_bytes()),
        ByteView::from(postcard::to_allocvec(&config).expect("config")),
    );
    write_entries(&storage, vec![config_row]).await;
    let earlier = arrived_before(&event, before);
    if !earlier.is_empty() {
        write_entries(&storage, earlier).await;
    }
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    for _ in 0..2 {
        project_create_event(&context, event.clone(), Some(local))
            .await
            .expect("projection succeeds");
    }
    let rows = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(vec![aruna_core::plugin_runs::JOURNAL_ROW])),
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::IterResult { values, .. }) = rows else {
        panic!("journal rows are readable");
    };
    let rows: Vec<SourceEvent> = (values.iter())
        .map(|(_, value)| SourceEvent::from_bytes(value).unwrap())
        .collect();
    (event, rows)
}

// Another holder names the author's commit message in its audit row, from the replicated
// lifecycle record of the same event; a record of another event names none.
#[tokio::test]
async fn projected_audit_message() {
    let audit_details = async |message: Option<&str>, lifecycle_event: Option<Ulid>| {
        let dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage");
        let event = create_event();
        let mut stored = event.clone();
        stored.event_id = lifecycle_event.unwrap_or(event.event_id);
        let lifecycle = MetadataLifecycleRecord::for_event(stored, message.map(str::to_owned));
        let (key_space, key, value) = document_lifecycle_entry(&lifecycle).unwrap();
        write_entries(&storage, vec![(key_space, key, value)]).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let other = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        project_create_event(&context, event, Some(other))
            .await
            .expect("projection succeeds");
        let rows = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 10,
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = rows else {
            panic!("audit rows are readable");
        };
        let (_, value) = values.first().expect("an audit row is written");
        let audit: MetadataAuditRecord = postcard::from_bytes(value).unwrap();
        audit.details.unwrap_or_default()
    };
    let named = audit_details(Some("plugin a install b run c"), None).await;
    assert!(named.ends_with("; plugin a install b run c"), "{named}");
    let other = audit_details(Some("not this one"), Some(Ulid::from_parts(1, 1))).await;
    assert!(!other.contains(';'), "{other}");
    assert!(!audit_details(None, None).await.contains(';'));
}

// A locally authored event with no eligible holders must not enqueue an
// outbox publish; replay re-plans it once eligible holders exist.
#[tokio::test]
async fn holderless_projection_defers() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let event = create_event();
    let config = realm_config(event.record.realm_id, &[]);
    write_entries(
        &storage,
        vec![(
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*event.record.realm_id.as_bytes()),
            ByteView::from(postcard::to_allocvec(&config).expect("config serializes")),
        )],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    project_create_event(&context, event.clone(), Some(event.node_id))
        .await
        .expect("projection succeeds");

    let outboxes = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        outboxes,
        Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty()
    ));

    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: event.record.document_id,
    };
    let lifecycle = storage
        .send_effect(crate::document_repository::read_effect(&target, None))
        .await;
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = lifecycle
    else {
        panic!("origin must retain lifecycle transfer source, got {lifecycle:?}");
    };
    let lifecycle: MetadataLifecycleRecord =
        postcard::from_bytes(&value).expect("lifecycle decodes");
    let expected_event = event;
    assert_eq!(
        lifecycle,
        MetadataLifecycleRecord::Upsert {
            event: Box::new(expected_event)
        }
    );
}

#[test]
fn strategy_holders_frozen() {
    let mut event = create_event();
    event.node_id = node(1);
    event.record.holder_node_ids = vec![node(1), node(4)];
    let mut config = RealmConfigDocument::new(event.record.realm_id, Vec::new(), 4);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([9; 16]),
        name: "metadata-two".to_string(),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy];
    for candidate in [node(1), node(2), node(3), node(4)] {
        config.ensure_node(candidate, RealmNodeKind::Server);
    }

    let event = stamped(event, &config);
    let expanded = expand_event_holders(event.clone());
    let expected = vec![node(1), node(4)];

    assert_eq!(config.metadata_replication.default_replication_factor, 4);
    assert_eq!(expanded.record.holder_node_ids, expected);
}

#[test]
fn user_origin_frozen() {
    let mut event = create_event();
    event.node_id = node(1);
    event.record.holder_node_ids = vec![node(1)];
    let mut config = RealmConfigDocument::new(event.record.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    let owner = UserId::nil(config.realm_id);
    config.ensure_node(node(1), RealmNodeKind::User { owner });
    config.ensure_node(node(2), RealmNodeKind::Server);
    config.ensure_node(node(3), RealmNodeKind::Server);
    config.ensure_node(node(4), RealmNodeKind::Server);
    // A user node holds no bucket, so its create fell back to the hashed
    // one (stage 1); it still never becomes a holder of it.
    event.record.placement = aruna_core::placement::target_placement_ref(
        &config,
        &DocumentTarget::MetadataDocumentLifecycle {
            document_id: event.record.document_id,
        },
        Default::default(),
    );

    let expanded = expand_event_holders(event);

    assert_eq!(expanded.record.holder_node_ids, vec![node(1)]);
}

#[test]
fn recipient_preserves_authoritative() {
    let mut event = create_event();
    event.node_id = node(1);
    event.record.holder_node_ids = vec![node(3), node(1), node(3)];
    let expanded = expand_event_holders(event);

    assert_eq!(expanded.record.holder_node_ids, vec![node(1), node(3)]);
}

#[test]
fn recipient_not_synthesize() {
    let mut event = create_event();
    event.node_id = node(1);
    event.record.holder_node_ids.clear();
    let expanded = expand_event_holders(event);

    assert!(expanded.record.holder_node_ids.is_empty());
}

#[test]
fn outbox_uses_lifecycle() {
    let event = create_event();
    let outbox = create_outbox_record(&event, None, None, true);

    assert!(outbox.allow_genesis);
    assert_eq!(outbox.outbox_id, event.event_id);
    // Without a realm config the holders are unresolvable here; the outbox
    // drain resolves them from the bucket before publishing.
    assert!(outbox.peers.is_empty());
    assert_eq!(
        outbox.target,
        DocumentTarget::MetadataDocumentLifecycle {
            document_id: event.record.document_id
        }
    );
    let DocumentOutboxEvent::Upsert { bytes, change } = outbox.event else {
        panic!("expected lifecycle upsert outbox event");
    };
    let lifecycle: MetadataLifecycleRecord =
        postcard::from_bytes(&bytes).expect("lifecycle payload decodes");
    assert_eq!(
        lifecycle,
        MetadataLifecycleRecord::Upsert {
            event: Box::new(event.clone())
        }
    );
    assert_eq!(
        change.kind,
        aruna_core::document::DocumentChangeKind::Upsert
    );
    assert_eq!(change.current.event_id, event.event_id);
}

#[test]
fn create_update_stamp() {
    let create = create_event();
    let config = realm_config(create.record.realm_id, &[node(1), node(2)]);
    let create = stamped(create, &config);

    // An update of the same document reuses its id and path; only the event
    // identity/timestamps advance.
    let mut update = create.clone();
    update.event_id = Ulid::generate();
    update.record.last_event_id = update.event_id;
    update.record.updated_at_ms = create.record.updated_at_ms + 1;
    update.occurred_at_ms = create.occurred_at_ms + 1;

    let placement_of = |event: &MetadataEventRecord| {
        let DocumentOutboxEvent::Upsert { change, .. } =
            create_outbox_record(event, None, Some(&config), true).event
        else {
            panic!("expected upsert outbox event");
        };
        change.placement
    };

    let create_ref = placement_of(&create);
    assert_ne!(
        create_ref,
        aruna_core::structs::placement::record::PlacementRef::NIL
    );
    assert_eq!(create_ref, placement_of(&update));
}

fn skew_event(updated_at_ms: u64, occurred_at_ms: u64) -> MetadataEventRecord {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let document_id = Ulid::generate();
    let event_id = Ulid::generate();
    MetadataEventRecord {
        event_id,
        record: MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::generate(),
            document_id,
            document_path: "datasets/skew".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: updated_at_ms,
            updated_at_ms,
            establishing_event_id: event_id,
            last_event_id: event_id,
        },
        user_id: UserId::nil(realm_id),
        node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
        payload: MetadataEventPayload::RoCrate {
            jsonld: String::new(),
        },
        occurred_at_ms,
    }
}

#[tokio::test]
async fn skewed_direct_writes() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let event = skew_event(now_ms + max_clock_skew() + 60_000, now_ms);
    let marker_key = pending_projection_key(event.record.document_id, event.event_id);
    write_entries(
        &storage,
        vec![create_event_entry(&event).expect("event log entry")],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let error = project_create_events(&context, vec![event], None)
        .await
        .expect_err("skewed event should be deferred");

    assert!(matches!(
        error,
        MetadataProjectionError::ClockSkewDeferred { deferred: 1 }
    ));
    assert!(pending_marker_exists(&storage, marker_key.to_vec()).await);
}

#[tokio::test]
async fn skewed_queue_keeps() {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let event = skew_event(now_ms + max_clock_skew() + 60_000, now_ms);
    let marker_key = pending_projection_key(event.record.document_id, event.event_id);
    write_entries(
        &storage,
        vec![
            create_event_entry(&event).expect("event log entry"),
            (
                PENDING_PROJECTION_KEYSPACE.to_string(),
                marker_key.clone(),
                ByteView::from(Vec::new()),
            ),
        ],
    )
    .await;
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let error = drain_projection_queue(&context)
        .await
        .expect_err("skewed queued event should be deferred");

    assert!(matches!(
        error,
        MetadataProjectionError::ClockSkewDeferred { deferred: 1 }
    ));
    assert!(pending_marker_exists(&storage, marker_key.to_vec()).await);
}

#[test]
fn skew_guard_accepts() {
    let now_ms = 1_000_000;
    let max_skew_ms = 300_000;
    assert!(!exceeds_clock_skew(
        &skew_event(now_ms + max_skew_ms, now_ms),
        now_ms,
        max_skew_ms
    ));
    assert!(!exceeds_clock_skew(
        &skew_event(now_ms, now_ms + max_skew_ms),
        now_ms,
        max_skew_ms
    ));
    assert!(!exceeds_clock_skew(&skew_event(0, 0), now_ms, max_skew_ms));
}

#[test]
fn skew_guard_rejects() {
    let now_ms = 1_000_000;
    let max_skew_ms = 300_000;
    assert!(exceeds_clock_skew(
        &skew_event(now_ms + max_skew_ms + 1, now_ms),
        now_ms,
        max_skew_ms
    ));
    assert!(exceeds_clock_skew(
        &skew_event(now_ms, now_ms + max_skew_ms + 1),
        now_ms,
        max_skew_ms
    ));
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
}

fn answer(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn no_cache() -> IoResult {
    answer(Event::Metadata(MetadataEvent::Error {
        graph_iri: None,
        error: MetadataError::HandleMissing,
    }))
}

#[test]
fn empty_projects_nothing() {
    let mut project = ProjectEventsOperation::new(Vec::new(), None, max_clock_skew());
    assert_eq!(project.first(), Ok(Next::Done(0)));
}

#[test]
fn received_create_projects() {
    // A create received from another node is projected, committed, cached and its marker
    // removed; it wakes the materialization drain.
    let event = create_event();
    let mut project = ProjectEventsOperation::new(vec![event.clone()], None, max_clock_skew());
    project.stamp(event.occurred_at_ms);
    assert!(matches!(project.first(), Ok(Next::Call(IoCall::Effect(_)))));
    assert!(matches!(
        project.next(no_cache()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    // The graph lives, the registry holds no row yet.
    assert!(matches!(
        project.next(child(Ok::<_, GraphDeletedError>(false))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let message = author_message_read(&event);
    let unregistered = Ok::<Option<MetadataRegistryRecord>, StorageReadError>(None);
    assert_eq!(project.next(child(unregistered)), Ok(Next::Call(message)));
    let lifecycle = read_row(
        DOCUMENT_LIFECYCLE_KEYSPACE,
        document_lifecycle_key(event.record.document_id),
    );
    assert_eq!(
        project.next(IoResult::Values(vec![None])),
        Ok(Next::Call(lifecycle))
    );
    let Ok(Next::Call(IoCall::Child(_))) = project.next(IoResult::Values(vec![None])) else {
        panic!("expected the fenced commit");
    };
    let Ok(Next::Call(IoCall::Effect(cache))) =
        project.next(child(Ok::<_, MetadataProjectionError>(())))
    else {
        panic!("expected the cache update");
    };
    assert!(matches!(
        *cache,
        Effect::Metadata(MetadataEffect::CacheRecords {
            generation: None,
            ..
        })
    ));
    assert_eq!(
        project.next(no_cache()),
        Ok(Next::Call(effect_call(schedule_materialization())))
    );
    let woken = answer(Event::Task(aruna_core::task::TaskEvent::TimerScheduled {
        after: Duration::ZERO,
        key: TaskKey::DrainMaterializationQueue,
    }));
    let Ok(Next::Call(IoCall::Delete(markers))) = project.next(woken) else {
        panic!("expected the marker delete");
    };
    assert_eq!(
        markers,
        vec![delete_projection_entry(
            event.record.document_id,
            event.event_id
        )]
    );
    assert_eq!(project.next(IoResult::Done), Ok(Next::Done(1)));
}

#[test]
fn skewed_event_deferred() {
    let event = create_event();
    let mut project = ProjectEventsOperation::new(vec![event.clone()], None, 0);
    project.stamp(event.occurred_at_ms.saturating_sub(1));
    project.first().unwrap();
    let Ok(Next::Call(IoCall::Write(markers))) = project.next(no_cache()) else {
        panic!("expected the retry marker");
    };
    assert_eq!(
        markers[0].1,
        pending_projection_key(event.record.document_id, event.event_id)
    );
    assert_eq!(
        project.next(IoResult::Done),
        Err(MetadataProjectionError::ClockSkewDeferred { deferred: 1 })
    );
}

#[test]
fn deleted_graph_conflicts() {
    let event = create_event();
    let mut commit = ProjectionCommitOperation {
        writes: aruna_core::storage_entries::registry_write_entries(&event.record).unwrap(),
        graph_iris: BTreeSet::from([event.record.graph_iri.clone()]),
        fence: WriteFence::default(),
    };
    assert_eq!(commit.transaction(), TransactionMode::Write);
    commit.first().unwrap();
    let tombstone = GraphLifecycleRecord::deleted(
        event.record.graph_iri.clone(),
        event.record.realm_id,
        event.record.group_id,
        event.record.document_id,
        2,
    );
    let row = Some(postcard::to_allocvec(&tombstone).unwrap().into());
    assert_eq!(
        commit.next(IoResult::Values(vec![row])),
        Err(StorageError::TransactionConflict.into())
    );
}

#[test]
fn missing_event_fails() {
    let event = create_event();
    let target = (event.record.document_id, event.event_id);
    let mut direct = ProjectLoggedOperation::new([target], false, None, max_clock_skew());
    direct.first().unwrap();
    assert_eq!(
        direct.next(IoResult::Values(vec![None])),
        Err(MetadataProjectionError::CreateEventMissing {
            document_id: target.0,
            event_id: target.1,
        })
    );
    let mut drain = ProjectLoggedOperation::new([target], true, None, max_clock_skew());
    drain.first().unwrap();
    assert_eq!(
        drain.next(IoResult::Values(vec![None])),
        Ok(Next::Call(IoCall::Delete(vec![delete_projection_entry(
            target.0, target.1
        )])))
    );
    assert!(matches!(
        drain.next(IoResult::Done),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}

#[test]
fn malformed_marker_call() {
    let mut drain = DrainProjectionOperation::new(None, max_clock_skew());
    drain.first().unwrap();
    let rows = vec![(Key::from(vec![1, 2, 3]), Value::from(Vec::new()))];
    let Ok(Next::Call(IoCall::Delete(deleted))) = drain.next(IoResult::Page { rows, next: None })
    else {
        panic!("expected the malformed marker delete");
    };
    assert_eq!(deleted[0].0, PENDING_PROJECTION_KEYSPACE);
    assert!(matches!(
        drain.next(IoResult::Done),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let logged = LoggedProjection {
        projected: 0,
        existing: 0,
    };
    assert_eq!(
        drain.next(child(Ok::<_, MetadataProjectionError>(logged))),
        Ok(Next::Done(PendingProjectionResult {
            markers_examined: 0,
            projected: 0,
            has_more: false,
        }))
    );
}

#[test]
fn timer_needs_marker() {
    let mut empty = ProjectionTimerOperation::new();
    empty.first().unwrap();
    assert_eq!(
        empty.next(IoResult::Page {
            rows: Vec::new(),
            next: None
        }),
        Ok(Next::Done(()))
    );
    let mut pending = ProjectionTimerOperation::new();
    pending.first().unwrap();
    let rows = vec![(Key::from(vec![1]), Value::from(Vec::new()))];
    assert_eq!(
        pending.next(IoResult::Page { rows, next: None }),
        Ok(Next::Call(effect_call(schedule_projection(Duration::ZERO))))
    );
}
