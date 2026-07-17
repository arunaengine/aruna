use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::compute::ExecutorCapability;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{METADATA_INDEX_KEYSPACE, NODE_INFO_KEYSPACE};
use aruna_core::structs::{
    NodeInfoDocument, NodeUrls, NodeUtilization, RealmConfigDocument, RealmId,
    node_info_storage_key,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::metadata::repository::{REGISTRY_FILL_PAGE_SIZE, parse_registry_iter};
use crate::placement::{build_view, held_buckets};
use crate::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

/// Interval between node-info heartbeat republishes. Peers treat a node's
/// `heartbeat_at_ms` staleness against this cadence when scoring liveness.
pub const NODE_INFO_PUBLISH_INTERVAL: Duration = Duration::from_secs(60);

/// Arms (or shortens toward) the periodic node-info heartbeat publish task.
pub fn schedule_node_info_publish_effect(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::PublishNodeInfo,
        after,
    })
}

/// Assembles this node's info document from its executors, current
/// placement-view labels, given urls, and local usage, then persists it under the
/// single-writer node-info key without queuing replication.
pub async fn seed_node_info_document(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    urls: NodeUrls,
    executors: Vec<ExecutorCapability>,
) -> Result<(), String> {
    let now = unix_timestamp_millis();
    let config = load_realm_config(ctx, realm_id).await?;
    let document = NodeInfoDocument {
        node_id,
        executors,
        labels: placement_labels(&config, node_id)?,
        urls,
        utilization: NodeUtilization {
            storage_bytes_used: local_storage_bytes(ctx).await?,
            documents_held: held_documents(ctx, node_id, &config).await,
            load_permille: read_load_permille(),
            heartbeat_at_ms: now,
        },
        updated_at_ms: now,
    };
    write_node_info_document(&ctx.storage_handle, &document).await
}

/// Seeds this node's current info document and replicates it over the shared
/// realm topic. Bootstrap callers must use [`seed_node_info_document`] before
/// announcing the core documents so the authorized announcement is queued
/// first.
pub async fn publish_node_info(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    urls: NodeUrls,
    executors: Vec<ExecutorCapability>,
) -> Result<(), String> {
    seed_node_info_document(ctx, node_id, realm_id, urls, executors).await?;
    replicate_node_info(ctx, node_id, realm_id).await
}

/// Heartbeat: refreshes the persisted node-info document's placement-view
/// labels, utilization, and timestamps, then republishes it. URLs remain the
/// startup-seeded values. A missing document is a no-op: the startup seed always
/// runs first.
pub async fn refresh_node_info_heartbeat(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
) -> Result<(), String> {
    let Some(mut document) = read_node_info_document(&ctx.storage_handle, node_id).await? else {
        return Ok(());
    };
    let now = unix_timestamp_millis();
    let config = load_realm_config(ctx, realm_id).await?;
    document.labels = placement_labels(&config, node_id)?;
    document.utilization.storage_bytes_used = local_storage_bytes(ctx).await?;
    document.utilization.documents_held = held_documents(ctx, node_id, &config).await;
    document.utilization.load_permille = read_load_permille();
    document.utilization.heartbeat_at_ms = now;
    document.updated_at_ms = now;
    write_node_info_document(&ctx.storage_handle, &document).await?;
    replicate_node_info(ctx, node_id, realm_id).await
}

async fn load_realm_config(
    ctx: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, String> {
    drive(GetRealmConfigOperation::new(realm_id), ctx)
        .await
        .map_err(|error| format!("failed to read realm config for node info: {error}"))
}

fn placement_labels(
    config: &RealmConfigDocument,
    node_id: NodeId,
) -> Result<BTreeMap<String, String>, String> {
    build_view(config)
        .nodes
        .into_iter()
        .find(|node| node.node_id == node_id)
        .map(|node| node.labels)
        .ok_or_else(|| format!("node {node_id} is missing from the realm placement view"))
}

async fn local_storage_bytes(ctx: &DriverContext) -> Result<u64, String> {
    Ok(crate::usage_stats::read_local_global(&ctx.storage_handle)
        .await?
        .stored_bytes)
}

/// Counts documents this node holds, degrading to `None` with a warning so a
/// storage hiccup never fails the heartbeat.
async fn held_documents(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
) -> Option<u64> {
    match count_held_documents(ctx, node_id, config).await {
        Ok(count) => Some(count),
        Err(error) => {
            warn!(%error, "failed to count documents held for node info");
            None
        }
    }
}

/// The `(strategy, shard)` buckets `node_id` holds across every strategy. A
/// document counts as held when its recorded placement bucket is in this set,
/// so everywhere-replicated registry rows are not each counted as local.
fn held_placement_set(config: &RealmConfigDocument, node_id: NodeId) -> HashSet<(Ulid, u32)> {
    let mut held = HashSet::new();
    for strategy in &config.strategies {
        for shard in held_buckets(config, strategy, node_id) {
            held.insert((strategy.strategy_id, shard));
        }
    }
    held
}

async fn count_held_documents(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
) -> Result<u64, String> {
    let held = held_placement_set(config, node_id);
    let mut count = 0u64;
    let mut start_after: Option<Key> = None;
    loop {
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_INDEX_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.map(IterStart::After),
                limit: REGISTRY_FILL_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (page, next) = parse_registry_iter(event)
            .map_err(|error| format!("metadata registry iteration failed: {error:?}"))?;
        for record in &page {
            if held.contains(&(record.placement.strategy_id, record.placement.shard)) {
                count += 1;
            }
        }
        match next {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    Ok(count)
}

/// 1-minute OS load average scaled to permille of logical core capacity, or
/// `None` (with a warning) when the core count is unavailable.
fn read_load_permille() -> Option<u32> {
    match std::thread::available_parallelism() {
        Ok(cores) => Some(permille_of(current_load1(), cores.get() as u64)),
        Err(error) => {
            warn!(%error, "failed to read logical core count for node info load");
            None
        }
    }
}

fn current_load1() -> f64 {
    sysinfo::System::load_average().one
}

/// Load average per core scaled to permille and clamped to `0..=1000`. Zero
/// cores yields `0` instead of dividing by zero.
fn permille_of(load1: f64, cores: u64) -> u32 {
    if cores == 0 {
        return 0;
    }
    (load1 / cores as f64 * 1000.0).round().clamp(0.0, 1000.0) as u32
}

async fn replicate_node_info(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
) -> Result<(), String> {
    drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents: vec![DocumentSyncTarget::NodeInfo { realm_id, node_id }],
            // Shared-topic genesis is bootstrapped by announce_core_documents;
            // explicit publishes and periodic heartbeats only publish into it.
            allow_genesis: false,
        }),
        ctx,
    )
    .await
    .map_err(|error| format!("node info replication failed: {error}"))
}

async fn write_node_info_document(
    storage: &StorageHandle,
    document: &NodeInfoDocument,
) -> Result<(), String> {
    let value = Value::from(postcard::to_allocvec(document).map_err(|error| error.to_string())?);
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: Key::from(node_info_storage_key(document.node_id)),
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("node info write failed: {other:?}")),
    }
}

/// Reads a single node's persisted info document, if present.
pub async fn read_node_info_document(
    storage: &StorageHandle,
    node_id: NodeId,
) -> Result<Option<NodeInfoDocument>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: Key::from(node_info_storage_key(node_id)),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref())
                    .map_err(|error| error.to_string())
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("node info read failed: {other:?}")),
    }
}

/// Reads the persisted info documents for the given nodes, skipping those with
/// no document yet. Keyed by node id for the realm-nodes read surface. Takes the
/// driver context so API routes drive this through the operations layer rather
/// than touching the storage handle directly.
pub async fn read_node_info_documents(
    ctx: &DriverContext,
    node_ids: &[NodeId],
) -> Result<BTreeMap<NodeId, NodeInfoDocument>, String> {
    let mut documents = BTreeMap::new();
    for node_id in node_ids {
        if let Some(document) = read_node_info_document(&ctx.storage_handle, *node_id).await? {
            documents.insert(*node_id, document);
        }
    }
    Ok(documents)
}

/// Arms the periodic node-info heartbeat at startup. `ShortenTimer` (never
/// `ResetTimer`) so the durable-queue re-arm loop cannot push the deadline
/// forward past the handler's own post-run re-arm.
pub async fn restore_node_info_publish_timer(_storage: &StorageHandle, task_handle: &TaskHandle) {
    if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishNodeInfo,
            after: NODE_INFO_PUBLISH_INTERVAL,
        }))
        .await
    {
        warn!(message = %message, "Failed to arm node info heartbeat timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord};
    use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
    use aruna_core::storage_entries::metadata_registry_key;
    use aruna_core::structs::{
        KIND_LABEL_KEY, MetadataRegistryRecord, NodePlacementEntry, PlacementRef,
        PlacementStrategy, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
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
        let node_id = config.node_ids().unwrap()[0];
        let actor = aruna_core::structs::Actor {
            node_id,
            user_id: aruna_core::types::UserId::nil(config.realm_id),
            realm_id: config.realm_id,
        };
        let target = DocumentSyncTarget::RealmConfig {
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

    async fn read_outbox(ctx: &DriverContext) -> Vec<DocumentSyncOutboxRecord> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
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

    #[tokio::test]
    async fn seed_writes_document_without_queuing_outbox() {
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

        seed_node_info_document(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: Some("s3.example".to_string()),
            },
            Vec::new(),
        )
        .await
        .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
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
    async fn publish_uses_selector_labels_and_queues_outbox() {
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
            Vec::new(),
        )
        .await
        .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
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
                matches!(&record.event, DocumentSyncOutboxEvent::Upsert { .. })
                    && record.target
                        == DocumentSyncTarget::NodeInfo {
                            realm_id,
                            node_id: local,
                        }
            })
            .expect("node info upsert queued");
        assert!(!record.allow_genesis);
    }

    #[tokio::test]
    async fn heartbeat_reflects_placement_changes_and_drops_stale_labels() {
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
            vec![ExecutorCapability {
                kind: "docker".to_string(),
                file_staging: true,
                direct_s3: true,
            }],
        )
        .await
        .unwrap();
        let first = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        config.placement_map[0].labels = BTreeMap::from([
            ("rack".to_string(), "r7".to_string()),
            ("zone".to_string(), "b".to_string()),
        ]);
        let expected_labels = build_view(&config).nodes[0].labels.clone();
        write_realm_config(&ctx, &config).await;

        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();
        let second = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(second.labels, expected_labels);
        assert_eq!(second.labels.get("zone").unwrap(), "b");
        assert!(!second.labels.contains_key("stale"));
        assert_eq!(second.executors.len(), 1);
        assert!(second.updated_at_ms >= first.updated_at_ms);
        assert!(second.utilization.heartbeat_at_ms >= first.utilization.heartbeat_at_ms);

        let node_info_records: Vec<_> = read_outbox(&ctx)
            .await
            .into_iter()
            .filter(|record| {
                record.target
                    == DocumentSyncTarget::NodeInfo {
                        realm_id,
                        node_id: local,
                    }
            })
            .collect();
        assert_eq!(node_info_records.len(), 2);
        assert!(node_info_records.iter().all(|record| !record.allow_genesis));
    }

    #[tokio::test]
    async fn heartbeat_without_seed_is_noop() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;

        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();
        assert!(
            read_node_info_document(&ctx.storage_handle, local)
                .await
                .unwrap()
                .is_none()
        );
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

    fn registry_record(
        realm_id: RealmId,
        seed: u8,
        placement: PlacementRef,
    ) -> MetadataRegistryRecord {
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

    #[test]
    fn permille_exact_values() {
        assert_eq!(permille_of(1.0, 4), 250);
        assert_eq!(permille_of(3.0, 4), 750);
        assert_eq!(permille_of(0.0, 8), 0);
        assert_eq!(permille_of(4.0, 4), 1000);
    }

    #[test]
    fn permille_clamps_high() {
        assert_eq!(permille_of(9.0, 4), 1000);
        assert_eq!(permille_of(10.0, 2), 1000);
    }

    #[test]
    fn permille_zero_cores() {
        assert_eq!(permille_of(5.0, 0), 0);
    }

    #[tokio::test]
    async fn counts_held_documents() {
        // Only records whose placement bucket the node holds are counted.
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
            epoch: 0,
            shard,
        };

        write_registry(&ctx, &registry_record(realm_id, 1, placed(held[0]))).await;
        write_registry(&ctx, &registry_record(realm_id, 2, placed(held[0]))).await;
        write_registry(&ctx, &registry_record(realm_id, 3, placed(unheld))).await;

        let count = count_held_documents(&ctx, local, &config).await.unwrap();
        assert_eq!(count, 2);
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
            Vec::new(),
        )
        .await
        .unwrap();
        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.utilization.documents_held, Some(0));
        assert!(stored.utilization.load_permille.is_some());
    }
}
