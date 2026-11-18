//! Runs the node info flows against real storage through their driver.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::compute::quota::ResourceTotals;
use aruna_core::events::Event;
use aruna_core::heartbeat::RunningJobs;
use aruna_core::node_state::ReadInfoOperation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::node_info::NodeUrls;
use aruna_core::types::GroupId;
use aruna_tasks::TaskHandle;

use crate::driver::{DriverContext, drive, node_facts};
use crate::jobs::demand::{DemandSnapshotOperation, merge_view};
use crate::jobs::lifecycle::held::HeldReservations;
use crate::node::info::flows::DepartureStateOperation;
use crate::node::info::flows::{
    GroupDemandOperation, ObservePlacementOperation, OperatorDrainOperation, RefreshInfoOperation,
    SeedInfoOperation,
};
use crate::node::info::inputs::{HeldDocumentsOperation, advertises_executors, held_placement_set};
use crate::node::info::write::PUBLISH_OWED_KEY;
use crate::placement::policy::{SubjectScanError, SubjectScanResult};
use aruna_core::compute::quota::ComputeDemandSnapshot;
use aruna_core::compute::quota::ComputeReservationSnapshot;
use aruna_core::compute::quota::{
    DemandFamily, DemandGroup, JobReservationRecord, MAX_DEMAND_FAMILIES,
};
use aruna_core::document::DocumentTarget;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::heartbeat::NodeHeartbeat;
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::keyspaces::METADATA_INDEX_KEYSPACE;
use aruna_core::keyspaces::NODE_SUBJECT_KEYSPACE;
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::keyspaces::{
    FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE,
};
use aruna_core::node_state::DrainStateOperation;
use aruna_core::node_state::SetDrainOperation;
use aruna_core::placement::{build_view, held_buckets};
use aruna_core::storage_entries::metadata_registry_key;
use aruna_core::structs::execution::job::record_keys::kind_prefix;
use aruna_core::structs::execution::job::record_rows::ProjectionCache;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind, LogicalJobState,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::node_subject::NODE_SUBJECT_KEY;
use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
use aruna_core::structs::placement::record::{NodePlacementEntry, PlacementRef, PlacementStrategy};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::node_info::AdvertisementEpoch;
use aruna_core::structs::storage::node_info::KIND_LABEL_KEY;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_core::structs::storage::node_info::NodeUtilization;
use aruna_core::types::Key;
use aruna_core::types::Value;
use aruna_storage::FjallStorage;
use tempfile::tempdir;
use ulid::Ulid;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

async fn node_info_row(
    ctx: &DriverContext,
    node_id: NodeId,
) -> Result<Option<NodeInfoDocument>, String> {
    let rows = drive(ReadInfoOperation::new(vec![node_id]), ctx).await;
    (rows.map(|mut rows| rows.remove(&node_id))).map_err(|error| error.to_string())
}

async fn seed_info_document(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    urls: NodeUrls,
) -> Result<(), String> {
    let seed = SeedInfoOperation::new(realm_id, node_id, urls, node_facts(ctx), false);
    (drive(seed, ctx).await).map_err(|error| error.to_string())
}

async fn publish_node_info(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    urls: NodeUrls,
) -> Result<(), String> {
    let seed = SeedInfoOperation::new(realm_id, node_id, urls, node_facts(ctx), true);
    (drive(seed, ctx).await).map_err(|error| error.to_string())
}

async fn refresh_info_heartbeat(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    sequence: u64,
    jobs: Option<RunningJobs>,
) -> Result<(), String> {
    let facts = node_facts(ctx);
    let refresh = RefreshInfoOperation::new(realm_id, node_id, sequence, jobs, facts);
    (drive(refresh, ctx).await).map_err(|error| error.to_string())
}

async fn observe_placement(
    ctx: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    now_ms: u64,
) -> Result<Option<SubjectScanResult>, SubjectScanError> {
    let facts = node_facts(ctx);
    drive(
        ObservePlacementOperation::new(realm_id, node_id, now_ms, facts),
        ctx,
    )
    .await
}

async fn set_operator_drain(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    draining: bool,
) -> Result<bool, String> {
    let facts = node_facts(ctx);
    let drain = OperatorDrainOperation::new(realm_id, node_id, draining, facts);
    (drive(drain, ctx).await).map_err(|error| error.to_string())
}

async fn group_demand(
    ctx: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    group_id: &GroupId,
) -> Result<(ResourceTotals, bool), String> {
    let operation = GroupDemandOperation::new(realm_id, node_id, *group_id, ctx.fresh_heartbeats());
    (drive(operation, ctx).await).map_err(|error| error.to_string())
}

async fn set_departure_state(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    departing: bool,
) -> Result<bool, String> {
    let facts = node_facts(ctx);
    let departure = DepartureStateOperation::new(realm_id, node_id, departing, facts);
    (drive(departure, ctx).await).map_err(|error| error.to_string())
}

async fn read_reservations(ctx: &DriverContext) -> Vec<JobReservationRecord> {
    drive(HeldReservations::all(), ctx).await.unwrap()
}

async fn demand_snapshot(ctx: &DriverContext) -> ComputeDemandSnapshot {
    let epoch = AdvertisementEpoch::default();
    drive(DemandSnapshotOperation::new(epoch), ctx)
        .await
        .unwrap()
}

fn test_ctx(root: &str) -> DriverContext {
    DriverContext {
        storage_handle: FjallStorage::open(root).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        // Node info replication is strict, so its drain scheduling must succeed.
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
        outbound_handle: None,
    }
}

fn realm_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

async fn write_realm_config(ctx: &DriverContext, config: &RealmConfigDocument) {
    // A removal fixture writes a config that no longer names any node.
    let node_id = config
        .node_ids()
        .ok()
        .and_then(|ids| ids.first().copied())
        .unwrap_or_else(|| node(1));
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id,
        user_id: aruna_core::UserId::nil(config.realm_id),
        realm_id: config.realm_id,
    };
    let target = DocumentTarget::RealmConfig {
        realm_id: config.realm_id,
    };
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn owed_row(ctx: &DriverContext) -> Option<Vec<u8>> {
    let read = StorageEffect::Read {
        key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
        key: Key::from(PUBLISH_OWED_KEY.to_vec()),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|v| v.to_vec()),
        other => panic!("owed row read failed: {other:?}"),
    }
}

async fn read_outbox(ctx: &DriverContext) -> Vec<DocumentOutboxRecord> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 256,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| postcard::from_bytes(&value).unwrap())
            .collect(),
        other => panic!("unexpected outbox iter result: {other:?}"),
    }
}

#[test]
fn device_advertises_nothing() {
    // A device never offers the realm a target to dispatch to.
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let device = node(1);
    let server = node(2);
    let mut config = realm_config(realm_id, &[server]);
    config.ensure_node(
        device,
        RealmNodeKind::User {
            owner: aruna_core::UserId::nil(realm_id),
        },
    );

    assert!(!advertises_executors(&config, device));
    assert!(advertises_executors(&config, server));
    assert!(!advertises_executors(&config, node(3)));
}

#[tokio::test]
async fn device_seeds_empty() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[local]);
    config.ensure_node(
        local,
        RealmNodeKind::User {
            owner: aruna_core::UserId::nil(realm_id),
        },
    );
    write_realm_config(&ctx, &config).await;

    seed_info_document(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();

    let stored = node_info_row(&ctx, local)
        .await
        .unwrap()
        .expect("seeded node info document");
    assert_eq!(stored.labels.get(KIND_LABEL_KEY).unwrap(), "user");
    assert!(stored.executors.is_empty());
}

#[tokio::test]
async fn device_publishes_locally() {
    // A device heartbeat revises the stored document but queues no outbox
    // row: it holds no topic the row could ever be published into.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[node(2)]);
    config.ensure_node(
        local,
        RealmNodeKind::User {
            owner: aruna_core::UserId::nil(realm_id),
        },
    );
    write_realm_config(&ctx, &config).await;

    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();

    assert!(node_info_row(&ctx, local).await.unwrap().is_some());
    assert!(read_outbox(&ctx).await.is_empty());
}

#[tokio::test]
async fn seed_writes_document() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[local]);
    config.placement_map.push(NodePlacementEntry {
        node_id: local,
        location: String::new(),
        weight: 100,
        full: false,
        draining: false,
        labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
    });
    write_realm_config(&ctx, &config).await;

    seed_info_document(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: Some("s3.example".to_string()),
        },
    )
    .await
    .unwrap();

    let stored = node_info_row(&ctx, local)
        .await
        .unwrap()
        .expect("seeded node info document");
    assert_eq!(stored.labels.get("tier").unwrap(), "hot");
    assert_eq!(stored.labels.get(KIND_LABEL_KEY).unwrap(), "server");
    assert!(stored.executors.is_empty());
    assert_eq!(stored.utilization.storage_bytes_used, 0);
    assert!(read_outbox(&ctx).await.is_empty());
}

#[tokio::test]
async fn publish_queues_labels() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[local, node(2)]);
    config.placement_map.push(NodePlacementEntry {
        node_id: local,
        location: String::new(),
        weight: 100,
        full: false,
        draining: false,
        labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
    });
    let expected_labels = build_view(&config)
        .nodes
        .into_iter()
        .find(|node| node.node_id == local)
        .unwrap()
        .labels;
    write_realm_config(&ctx, &config).await;

    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: Some("s3.example".to_string()),
        },
    )
    .await
    .unwrap();

    let stored = node_info_row(&ctx, local)
        .await
        .unwrap()
        .expect("node info document persisted");
    assert_eq!(stored.node_id, local);
    assert_eq!(stored.labels, expected_labels);
    assert_eq!(stored.labels.get(KIND_LABEL_KEY).unwrap(), "server");
    assert_eq!(stored.urls.s3.as_deref(), Some("s3.example"));
    assert_eq!(stored.utilization.documents_held, Some(0));
    assert!(stored.utilization.load_permille.is_some());

    let outbox = read_outbox(&ctx).await;
    let record = outbox
        .iter()
        .find(|record| {
            matches!(&record.event, DocumentOutboxEvent::Upsert { .. })
                && record.target
                    == DocumentTarget::NodeInfo {
                        realm_id,
                        node_id: local,
                    }
        })
        .expect("node info upsert queued");
    assert!(!record.allow_genesis);
}

#[tokio::test]
async fn heartbeat_refreshes_labels() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[local]);
    config.placement_map.push(NodePlacementEntry {
        node_id: local,
        location: String::new(),
        weight: 100,
        full: false,
        draining: false,
        labels: BTreeMap::from([
            ("stale".to_string(), "remove-me".to_string()),
            ("zone".to_string(), "a".to_string()),
        ]),
    });
    write_realm_config(&ctx, &config).await;

    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    let first = node_info_row(&ctx, local).await.unwrap().unwrap();

    config.placement_map[0].labels = BTreeMap::from([
        ("rack".to_string(), "r7".to_string()),
        ("zone".to_string(), "b".to_string()),
    ]);
    let expected_labels = build_view(&config).nodes[0].labels.clone();
    write_realm_config(&ctx, &config).await;

    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();
    let second = node_info_row(&ctx, local).await.unwrap().unwrap();

    assert_eq!(second.labels, expected_labels);
    assert_eq!(second.labels.get("zone").unwrap(), "b");
    assert!(!second.labels.contains_key("stale"));
    // A node without a compute plane advertises no execution target, and
    // every republish supersedes its own predecessor.
    assert!(second.executors.is_empty());
    assert!(second.epoch.publisher_generation > first.epoch.publisher_generation);
    assert!(second.supersedes(&first));
    assert!(second.updated_at_ms >= first.updated_at_ms);
    assert!(second.utilization.heartbeat_at_ms >= first.utilization.heartbeat_at_ms);

    let node_info_records: Vec<_> = read_outbox(&ctx)
        .await
        .into_iter()
        .filter(|record| {
            record.target
                == DocumentTarget::NodeInfo {
                    realm_id,
                    node_id: local,
                }
        })
        .collect();
    assert_eq!(node_info_records.len(), 2);
    assert!(node_info_records.iter().all(|record| !record.allow_genesis));
}

#[tokio::test]
async fn unseeded_heartbeat_noop() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;

    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();
    assert!(node_info_row(&ctx, local).await.unwrap().is_none());
}

fn sharded_config(realm_id: RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([5u8; 16]),
        name: "default".to_string(),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy];
    for seed in 1..=4u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    config
}

fn registry_record(realm_id: RealmId, seed: u8, placement: PlacementRef) -> MetadataRegistryRecord {
    let id = Ulid::from_bytes([seed; 16]);
    MetadataRegistryRecord {
        realm_id,
        group_id: id,
        document_id: id,
        document_path: String::new(),
        graph_iri: String::new(),
        public: false,
        permission_path: String::new(),
        placement,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        establishing_event_id: Ulid::nil(),
        last_event_id: Ulid::nil(),
    }
}

async fn write_registry(ctx: &DriverContext, record: &MetadataRegistryRecord) {
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: METADATA_INDEX_KEYSPACE.to_string(),
            key: metadata_registry_key(record.group_id, record.document_id),
            value: postcard::to_allocvec(record).unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn counts_held_documents() {
    // Records whose placement bucket the node holds, plus NIL placements, count.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let local = node(1);
    let config = sharded_config(realm_id);
    let strategy = &config.strategies[0];
    let held = held_buckets(&config, strategy, local);
    assert!(!held.is_empty() && held.len() < strategy.shard_count as usize);
    let unheld = (0..strategy.shard_count)
        .find(|shard| !held.contains(shard))
        .unwrap();
    let placed = |shard| PlacementRef {
        strategy_id: strategy.strategy_id,
        shard,
    };

    write_registry(&ctx, &registry_record(realm_id, 1, placed(held[0]))).await;
    write_registry(&ctx, &registry_record(realm_id, 2, placed(held[0]))).await;
    write_registry(&ctx, &registry_record(realm_id, 3, placed(unheld))).await;
    write_registry(&ctx, &registry_record(realm_id, 4, PlacementRef::NIL)).await;

    let held = HeldDocumentsOperation::new(held_placement_set(&config, local));
    let count = drive(held, &ctx).await.unwrap();
    assert_eq!(count, 3);
}

fn family(seed: u8) -> JobFamilyId {
    JobFamilyId {
        submission_id: aruna_core::structs::execution::job::SubmissionId([seed; 32]),
        request_digest: [seed; 32],
    }
}

fn spec_record(realm_id: RealmId, family: JobFamilyId, group_id: Ulid) -> JobRecordEnvelope {
    let resources = aruna_core::structs::execution::job::EffectiveResources {
        cpu_cores: 2,
        ram_bytes: 1_024,
        disk_bytes: 2_048,
        max_walltime_ms: 60_000,
        preemptible: false,
    };
    let job_id = aruna_core::structs::execution::job::JobId::from_bytes([9u8; 16]);
    let origin = node(1);
    let spec = aruna_core::structs::execution::job::LogicalJobSpec {
        submission_id: family.submission_id,
        job_id,
        origin_node_id: origin,
        ingress_node_id: origin,
        realm_id,
        group_id,
        created_by: aruna_core::UserId::nil(realm_id),
        created_at_ms: 1_000,
        retention_ms: aruna_core::structs::execution::job::RETENTION_MS,
        payload: aruna_core::structs::execution::job::ExecutionSpec {
            group_id,
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        },
        request_digest: family.request_digest,
        spec_digest: [0u8; 32],
        resources,
        retry: aruna_core::structs::execution::job::JobRetryPolicy {
            launches_per_witness: 2,
        },
        admission: aruna_core::structs::execution::job::JobAdmissionRecord {
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            job_id,
            group_id,
            admitting_node_id: origin,
            membership_generation: 1,
            resources,
            admitted_at_ms: 1_000,
        },
        captured_inputs: Vec::new(),
        input_pages: None,
        output_policies: Vec::new(),
        placement: PlacementRef::NIL,
    }
    .store_digest()
    .expect("spec digest stored");
    JobRecordEnvelope::sign(
        realm_id,
        JobFamilyRecord::Spec(Box::new(spec)),
        &iroh::SecretKey::from_bytes(&[1u8; 32]),
    )
    .expect("record signs")
}

async fn write_family(
    ctx: &DriverContext,
    realm_id: RealmId,
    family: JobFamilyId,
    group_id: Ulid,
    state: LogicalJobState,
) {
    let cache = ProjectionCache {
        version: aruna_core::structs::execution::job::record_rows::PROJECTION_CACHE_VERSION,
        revision: 1,
        stale: false,
        projection: Some(aruna_core::structs::execution::job::JobProjection {
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            canonical_job_id: aruna_core::structs::execution::job::JobId::from_bytes([9u8; 16]),
            aliases: Vec::new(),
            state,
            canonical_execution_id: None,
            executions: Vec::new(),
            outputs: aruna_core::structs::execution::job::OutputSet::new(Vec::new())
                .expect("empty outputs"),
            cancel_requested: false,
        }),
    };
    write_row(
        ctx,
        FAMILY_PROJECTION_KEYSPACE,
        Key::from(family.to_bytes().as_slice()),
        postcard::to_allocvec(&cache).unwrap(),
    )
    .await;
    let record = spec_record(realm_id, family, group_id);
    let mut key = kind_prefix(&family, JobRecordKind::Spec).to_vec();
    key.extend_from_slice(&[0u8; 40]);
    write_row(
        ctx,
        FAMILY_RECORD_KEYSPACE,
        Key::from(key.as_slice()),
        postcard::to_allocvec(&record).unwrap(),
    )
    .await;
}

async fn write_reservation(ctx: &DriverContext, execution_id: Ulid, cpu: u32) {
    let record = JobReservationRecord {
        execution_id,
        job_id: aruna_core::structs::execution::job::JobId::from_bytes([9u8; 16]),
        logical_job_id: aruna_core::structs::execution::job::JobId::from_bytes([9u8; 16]),
        resources: aruna_core::structs::execution::job::EffectiveResources {
            cpu_cores: cpu,
            ram_bytes: 512,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        },
        created_at_ms: 5,
        subject_generation: 1,
        subject_digest: [0u8; 32],
    };
    write_row(
        ctx,
        JOB_RESERVATION_KEYSPACE,
        Key::from(execution_id.to_bytes().as_slice()),
        postcard::to_allocvec(&record).unwrap(),
    )
    .await;
}

async fn write_row(ctx: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn publishes_separate_snapshots() {
    // Logical demand and physical reservation are different controls: a
    // terminal family leaves demand while its node still reserves capacity.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    let group_id = Ulid::from_bytes([2u8; 16]);
    write_family(&ctx, realm_id, family(1), group_id, LogicalJobState::Queued).await;
    write_family(
        &ctx,
        realm_id,
        family(2),
        group_id,
        LogicalJobState::Indeterminate,
    )
    .await;
    write_family(
        &ctx,
        realm_id,
        family(3),
        group_id,
        LogicalJobState::Succeeded,
    )
    .await;
    write_reservation(&ctx, Ulid::from_bytes([7u8; 16]), 4).await;

    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();

    let stored = node_info_row(&ctx, local).await.unwrap().expect("document");
    let group = stored.demand.group(&group_id).expect("group demand");
    assert_eq!(group.families.len(), 2);
    assert!(!group.truncated);
    assert_eq!(stored.demand.epoch, stored.epoch);
    // Reservations are exact and local; they never enter logical demand.
    assert_eq!(stored.reservation.reserved.count, 1);
    assert_eq!(stored.reservation.reserved.cpu_cores, 4);
    assert_eq!(stored.reservation.reserved.ram_bytes, 512);
    assert!(stored.validate().is_ok());
}

fn held_demand(node_id: NodeId, group_id: Ulid, families: Vec<DemandFamily>) -> HeldHeartbeat {
    let epoch = AdvertisementEpoch::default();
    HeldHeartbeat {
        node_id,
        heartbeat: NodeHeartbeat {
            realm_id: RealmId::from_bytes([12u8; 32]),
            epoch,
            sequence: 1,
            utilization: NodeUtilization {
                storage_bytes_used: 0,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 1,
                download_slots: None,
                storage_capacity_bytes: None,
            },
            availability: Vec::new(),
            reservation: ComputeReservationSnapshot::default(),
            demand: ComputeDemandSnapshot {
                epoch,
                groups: vec![DemandGroup {
                    group_id,
                    families,
                    truncated: false,
                }],
                truncated: false,
            },
            jobs: None,
        },
        age_ms: 0,
        received_at_ms: 1,
    }
}

#[tokio::test]
async fn merges_group_demand() {
    // Shared families count once, non-members stop counting, and a member
    // without a fresh heartbeat leaves the view understated.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([12u8; 32]);
    let local = node(1);
    let peer = node(2);
    let removed = node(3);
    let group_id = Ulid::from_bytes([2u8; 16]);
    write_family(&ctx, realm_id, family(1), group_id, LogicalJobState::Queued).await;
    let snapshot = demand_snapshot(&ctx).await;

    let shared = DemandFamily {
        submission_id: family(1).submission_id,
        request_digest: family(1).request_digest,
        resources: aruna_core::structs::execution::job::EffectiveResources {
            cpu_cores: 2,
            ram_bytes: 1_024,
            disk_bytes: 2_048,
            max_walltime_ms: 60_000,
            preemptible: false,
        },
    };
    let own = DemandFamily {
        submission_id: family(4).submission_id,
        request_digest: family(4).request_digest,
        ..shared
    };
    let fresh = BTreeMap::from([
        (peer, held_demand(peer, group_id, vec![shared])),
        (removed, held_demand(removed, group_id, vec![own])),
    ]);

    // The peer reports the family this node already holds locally.
    let (totals, truncated) =
        merge_view(snapshot.clone(), &[local, peer], local, &fresh, &group_id);
    assert_eq!(totals.count, 1);
    assert_eq!(totals.cpu_cores, 2);
    assert!(!truncated);
    let other = Ulid::from_bytes([9u8; 16]);
    let (totals, _) = merge_view(snapshot.clone(), &[local, peer], local, &fresh, &other);
    assert_eq!(totals.count, 0);

    let (totals, truncated) =
        merge_view(snapshot, &[local, peer, node(4)], local, &fresh, &group_id);
    assert_eq!(totals.count, 1);
    assert!(truncated);
}

#[tokio::test]
async fn ignores_user_devices() {
    // A user device sends no heartbeat, so it must not leave the view understated.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([15u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[local]);
    config.ensure_node(
        node(2),
        RealmNodeKind::User {
            owner: aruna_core::UserId::nil(realm_id),
        },
    );
    write_realm_config(&ctx, &config).await;
    let group_id = Ulid::from_bytes([2u8; 16]);
    write_family(&ctx, realm_id, family(1), group_id, LogicalJobState::Queued).await;

    let (totals, truncated) = group_demand(&ctx, realm_id, local, &group_id)
        .await
        .unwrap();

    assert_eq!(totals.count, 1);
    assert!(!truncated);
}

#[tokio::test]
async fn departure_stops_offers() {
    // Departure revokes future eligibility and reports what it could not
    // resolve, without deleting a reservation or blocking removal.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([10u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    let execution_id = Ulid::from_bytes([7u8; 16]);
    write_reservation(&ctx, execution_id, 2).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    let before = node_info_row(&ctx, local).await.unwrap().unwrap();

    assert!(
        set_departure_state(&ctx, local, realm_id, true)
            .await
            .unwrap()
    );

    let after = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(after.leaving && after.compute_draining);
    assert!(!after.offers_compute(true));
    assert!(after.supersedes(&before));
    assert_eq!(after.reservation.reserved.count, 1);

    let report = drive(DrainStateOperation::new(), &ctx)
        .await
        .unwrap()
        .departure
        .expect("departure report");
    assert_eq!(report.unresolved, vec![execution_id]);
    assert!(!report.truncated);
    // The reservation itself survives: departing proves nothing about it.
    assert_eq!(read_reservations(&ctx).await.len(), 1);
    // A repeated observation republishes nothing.
    assert!(
        !set_departure_state(&ctx, local, realm_id, true)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn removal_revokes_eligibility() {
    // A node the realm no longer places learns of its removal through config
    // sync. It stops offering execution without ending a receipted one.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([13u8; 32]);
    let local = node(1);
    let mut config = realm_config(realm_id, &[local]);
    config.placement_map.push(NodePlacementEntry {
        node_id: local,
        location: String::new(),
        weight: 100,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    });
    write_realm_config(&ctx, &config).await;
    let subject =
        aruna_core::structs::placement::node_subject::storage_subject(&config.placement_map[0], 1);
    let record = NodeSubjectRecord::seed(subject).expect("subject is valid");
    write_row(
        &ctx,
        NODE_SUBJECT_KEYSPACE,
        Key::from(NODE_SUBJECT_KEY.to_vec()),
        record.to_bytes().unwrap(),
    )
    .await;
    write_reservation(&ctx, Ulid::from_bytes([7u8; 16]), 2).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();

    config.placement_map.clear();
    config.nodes.clear();
    write_realm_config(&ctx, &config).await;
    observe_placement(&ctx, realm_id, local, 10)
        .await
        .expect("observation completes");

    let stored = node_info_row(&ctx, local).await.unwrap().expect("document");
    assert!(stored.leaving && stored.compute_draining);
    assert!(!stored.offers_compute(false));
    // The receipted execution keeps its reservation and its audit trail.
    assert_eq!(read_reservations(&ctx).await.len(), 1);
    assert_eq!(stored.reservation.reserved.count, 1);
}

#[tokio::test]
async fn rejoin_clears_departure() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([11u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    set_departure_state(&ctx, local, realm_id, true)
        .await
        .unwrap();
    let departed = node_info_row(&ctx, local).await.unwrap().unwrap();

    assert!(
        set_departure_state(&ctx, local, realm_id, false)
            .await
            .unwrap()
    );

    let rejoined = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(!rejoined.leaving && !rejoined.compute_draining);
    assert!(rejoined.offers_compute(true));
    assert!(rejoined.supersedes(&departed));
}

#[tokio::test]
async fn drain_survives_placement() {
    // An operator drain must outlive a return-to-placement observation:
    // otherwise the very next placement sync silently undrains the node.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();

    assert!(
        set_operator_drain(&ctx, local, realm_id, true)
            .await
            .unwrap()
    );
    let drained = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(drained.compute_draining && !drained.leaving);

    // A departure and its return leave the operator drain in place.
    set_departure_state(&ctx, local, realm_id, true)
        .await
        .unwrap();
    set_departure_state(&ctx, local, realm_id, false)
        .await
        .unwrap();
    let observed = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(!observed.leaving);
    assert!(observed.compute_draining, "the operator drain must survive");

    assert!(
        set_operator_drain(&ctx, local, realm_id, false)
            .await
            .unwrap()
    );
    let released = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(!released.compute_draining);
}

fn indexed_family(index: usize) -> JobFamilyId {
    let mut request_digest = [0u8; 32];
    request_digest[..8].copy_from_slice(&(index as u64).to_be_bytes());
    JobFamilyId {
        submission_id: aruna_core::structs::execution::job::SubmissionId([1u8; 32]),
        request_digest,
    }
}

#[tokio::test]
async fn truncates_busy_group() {
    // A group that overflows the shared family budget is flagged alone: a
    // quiet group keeps a complete view and admits normally.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([14u8; 32]);
    let busy = Ulid::from_bytes([1u8; 16]);
    let quiet = Ulid::from_bytes([2u8; 16]);
    for index in 0..=MAX_DEMAND_FAMILIES {
        write_family(
            &ctx,
            realm_id,
            indexed_family(index),
            busy,
            LogicalJobState::Queued,
        )
        .await;
    }
    write_family(&ctx, realm_id, family(200), quiet, LogicalJobState::Queued).await;

    let snapshot = demand_snapshot(&ctx).await;

    assert!(!snapshot.truncated);
    assert!(snapshot.validate().is_ok());
    let busy_group = snapshot.group(&busy).expect("busy group");
    assert!(busy_group.truncated);
    assert_eq!(busy_group.families.len(), MAX_DEMAND_FAMILIES - 1);
    let quiet_group = snapshot.group(&quiet).expect("quiet group");
    assert!(!quiet_group.truncated);
    assert_eq!(quiet_group.families.len(), 1);
}

#[tokio::test]
async fn heartbeat_keeps_drain() {
    // A drain recorded while the heartbeat scans must survive its write, and
    // the heartbeat must never undrain a node on its own.
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([16u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();

    drive(SetDrainOperation::new(true), &ctx).await.unwrap();
    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();
    let drained = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(drained.compute_draining && !drained.leaving);

    drive(SetDrainOperation::new(false), &ctx).await.unwrap();
    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();
    let released = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(!released.compute_draining);
    assert!(released.supersedes(&drained));
}

#[tokio::test]
async fn heartbeat_keeps_departure() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([17u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    set_departure_state(&ctx, local, realm_id, true)
        .await
        .unwrap();

    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();

    let stored = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert!(stored.leaving && stored.compute_draining);
    assert!(!stored.offers_compute(true));
}

#[tokio::test]
async fn heartbeat_populates_utilization() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;

    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    refresh_info_heartbeat(&ctx, local, realm_id, 1, None)
        .await
        .unwrap();

    let stored = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert_eq!(stored.utilization.documents_held, Some(0));
    assert!(stored.utilization.load_permille.is_some());
}

/// Ticks with unchanged durable fields write no revision and queue no publish; an owed
/// revision is published by the next tick without a new revision, then cleared.
#[tokio::test]
async fn unchanged_skips_publish() {
    let dir = tempdir().unwrap();
    let ctx = test_ctx(dir.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([18u8; 32]);
    let local = node(1);
    write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
    publish_node_info(
        &ctx,
        local,
        realm_id,
        NodeUrls {
            api: None,
            s3: None,
        },
    )
    .await
    .unwrap();
    let seeded = node_info_row(&ctx, local).await.unwrap().unwrap();
    let queued = read_outbox(&ctx).await.len();

    assert_eq!(owed_row(&ctx).await, None);
    for sequence in 1..=3 {
        refresh_info_heartbeat(&ctx, local, realm_id, sequence, None)
            .await
            .unwrap();
    }
    let stored = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert_eq!(stored, seeded);
    assert_eq!(read_outbox(&ctx).await.len(), queued);

    // A publish that failed after its revision committed leaves the revision owed.
    let owed = seeded.epoch.publisher_generation.to_be_bytes().to_vec();
    ctx.storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            key: Key::from(PUBLISH_OWED_KEY.to_vec()),
            value: Value::from(owed),
            txn_id: None,
        })
        .await;
    refresh_info_heartbeat(&ctx, local, realm_id, 4, None)
        .await
        .unwrap();
    let stored = node_info_row(&ctx, local).await.unwrap().unwrap();
    assert_eq!(stored, seeded);
    assert_eq!(read_outbox(&ctx).await.len(), queued + 1);
    assert_eq!(owed_row(&ctx).await, None);
}
