use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
use aruna_core::structs::{
    NodeInfoDocument, NodeUrls, NodeUtilization, RealmId, node_info_storage_key,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::driver::{DriverContext, drive};
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

/// Assembles this node's info document from the given labels/urls plus current
/// local usage, persists it under the single-writer node-info key, and
/// replicates it over the shared realm topic. Used at startup with
/// config-sourced labels/urls to seed (or refresh) the document.
pub async fn publish_node_info(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    labels: BTreeMap<String, String>,
    urls: NodeUrls,
) -> Result<(), String> {
    let now = unix_timestamp_millis();
    let document = NodeInfoDocument {
        node_id,
        labels,
        urls,
        utilization: NodeUtilization {
            storage_bytes_used: local_storage_bytes(ctx).await?,
            documents_held: 0,
            load_permille: 0,
            heartbeat_at_ms: now,
        },
        updated_at_ms: now,
    };
    write_node_info_document(&ctx.storage_handle, &document).await?;
    replicate_node_info(ctx, node_id, realm_id).await
}

/// Heartbeat: refreshes the persisted node-info document's dynamic fields
/// (utilization + heartbeat/updated timestamps) and republishes it. Labels and
/// urls are preserved from the stored document (seeded at startup from config).
/// A missing document is a no-op: the startup seed always runs first.
pub async fn refresh_node_info_heartbeat(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
) -> Result<(), String> {
    let Some(mut document) = read_node_info_document(&ctx.storage_handle, node_id).await? else {
        return Ok(());
    };
    let now = unix_timestamp_millis();
    document.utilization.storage_bytes_used = local_storage_bytes(ctx).await?;
    document.utilization.heartbeat_at_ms = now;
    document.updated_at_ms = now;
    write_node_info_document(&ctx.storage_handle, &document).await?;
    replicate_node_info(ctx, node_id, realm_id).await
}

async fn local_storage_bytes(ctx: &DriverContext) -> Result<u64, String> {
    Ok(crate::usage_stats::read_local_global(&ctx.storage_handle)
        .await?
        .stored_bytes)
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
    use aruna_core::structs::{RealmConfigDocument, RealmNodeKind};
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
        }
    }

    async fn seed_realm_config(ctx: &DriverContext, realm_id: RealmId, nodes: &[NodeId]) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        let actor = aruna_core::structs::Actor {
            node_id: nodes[0],
            user_id: aruna_core::types::UserId::nil(realm_id),
            realm_id,
        };
        let target = DocumentSyncTarget::RealmConfig { realm_id };
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
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
    async fn publish_writes_document_and_queues_outbox() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let local = node(1);
        seed_realm_config(&ctx, realm_id, &[local, node(2)]).await;

        let labels = BTreeMap::from([("tier".to_string(), "hot".to_string())]);
        publish_node_info(
            &ctx,
            local,
            realm_id,
            labels.clone(),
            NodeUrls {
                api: None,
                s3: Some("s3.example".to_string()),
            },
        )
        .await
        .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .expect("node info document persisted");
        assert_eq!(stored.node_id, local);
        assert_eq!(stored.labels, labels);
        assert_eq!(stored.urls.s3.as_deref(), Some("s3.example"));
        assert_eq!(stored.utilization.documents_held, 0);

        let outbox = read_outbox(&ctx).await;
        assert!(outbox.iter().any(|record| matches!(
            &record.event,
            DocumentSyncOutboxEvent::Upsert { .. }
        ) && record.target
            == DocumentSyncTarget::NodeInfo {
                realm_id,
                node_id: local
            }));
    }

    #[tokio::test]
    async fn heartbeat_preserves_labels_and_advances_timestamp() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let local = node(1);
        seed_realm_config(&ctx, realm_id, &[local]).await;

        let labels = BTreeMap::from([("zone".to_string(), "a".to_string())]);
        publish_node_info(
            &ctx,
            local,
            realm_id,
            labels.clone(),
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();
        let first = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();
        let second = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(second.labels, labels);
        assert!(second.updated_at_ms >= first.updated_at_ms);
        assert!(second.utilization.heartbeat_at_ms >= first.utilization.heartbeat_at_ms);
    }

    #[tokio::test]
    async fn heartbeat_without_seed_is_noop() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let local = node(1);
        seed_realm_config(&ctx, realm_id, &[local]).await;

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
}
