use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::{
    DocumentSyncNetEvent, DocumentSyncReconcileResult, DocumentSyncTarget, shard_topic_id,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::MetadataCreateEventRecord;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use tracing::warn;

use crate::driver::DriverContext;
use crate::metadata::projector::{
    project_metadata_create_events, project_metadata_create_events_from_log,
};
use crate::metadata::prune_queue::process_metadata_graph_tombstones;
use crate::notifications::watch::interest::refresh_watch_interest_for_targets;
use crate::placement::resolve_shard_holders;
use crate::usage_stats::refresh_realm_usage_summary_for_targets;

/// Shared realm-scoped topics every node subscribes to (placement is inert on
/// these; see [`DocumentSyncTarget::sync_topic_id`]).
fn shared_targets(realm_id: RealmId, node_id: NodeId) -> [DocumentSyncTarget; 5] {
    [
        DocumentSyncTarget::RealmAuthorization { realm_id },
        DocumentSyncTarget::RealmConfig { realm_id },
        DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        },
        DocumentSyncTarget::NodeInfo { realm_id, node_id },
        DocumentSyncTarget::WatchInterest { realm_id, node_id },
    ]
}

/// Fixed realm-scoped topics restored on every start (see [`shared_targets`]).
pub const SHARED_RESTORE_TOPIC_COUNT: usize = 5;

/// What a [`restore_shard_subscriptions`] pass touched. The load-bearing
/// invariant is `shard_topics == held_shards`, i.e. one topic per held shard,
/// never one per stored document — asserted by the restart-traffic gate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RestoreShardSummary {
    /// Shards the local node resolves into a holder of.
    pub held_shards: usize,
    /// Shard sync topics ensured (rank-0) or joined that this pass touched.
    pub shard_topics: usize,
    /// Fixed shared realm topics restored ([`SHARED_RESTORE_TOPIC_COUNT`]).
    pub shared_topics: usize,
}

impl RestoreShardSummary {
    /// Total distinct topics the restore ensured or joined.
    pub fn total_topics(&self) -> usize {
        self.shard_topics + self.shared_topics
    }
}

/// Restarts the local node's document-sync subscriptions from the shards it
/// holds instead of re-announcing every stored document.
///
/// Loads the realm config, and for each bound strategy × shard the local node
/// resolves into a holder of, ensures the shard sync topic with its co-holders
/// and runs one anti-entropy pass against them (digest exchange, not a
/// per-document re-announce). The fixed shared realm topics are restored the
/// same way. Topics that share a co-holder set are batched into one ensure and
/// one sync so a restart costs O(held shards), not O(stored documents). The
/// returned [`RestoreShardSummary`] reports the (small) topic count for callers
/// and the restart-traffic gate.
pub async fn restore_shard_subscriptions(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
) -> RestoreShardSummary {
    let mut summary = RestoreShardSummary::default();
    let Some(net_handle) = context.net_handle.clone() else {
        return summary;
    };
    let Some(config) = load_realm_config(context, realm_id).await else {
        // No config yet (fresh/onboarding node): nothing sharded to restore.
        return summary;
    };

    let realm_nodes: Vec<NodeId> = config
        .nodes
        .iter()
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|candidate| *candidate != node_id)
        .collect();

    // Group topics by their co-holder peer set so co-located shards ride one
    // ensure + one sync instead of one round trip each. Only topics the local
    // node may create the genesis of (shared realm topics, and shards it is
    // rank-0 holder of) are ensured; the rest are join-only — synced if their
    // genesis is known or bootstrappable from a co-holder, otherwise left for
    // the rank-0 holder's gossip to deliver.
    let mut ensure_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>> = BTreeMap::new();
    let mut join_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>> = BTreeMap::new();

    let mut shared_peers = realm_nodes.clone();
    shared_peers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    for target in shared_targets(realm_id, node_id) {
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        ensure_groups
            .entry(shared_peers.clone())
            .or_default()
            .push(topic);
        summary.shared_topics += 1;
    }

    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard,
            };
            let holders = resolve_shard_holders(&config, &placement);
            if !holders.contains(&node_id) {
                continue;
            }
            summary.held_shards += 1;
            let local_is_rank0 = holders.first() == Some(&node_id);
            let mut co_holders: Vec<NodeId> = holders
                .into_iter()
                .filter(|candidate| *candidate != node_id)
                .collect();
            co_holders.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            if co_holders.is_empty() {
                continue;
            }
            let topic = shard_topic_id(realm_id, &placement);
            let groups = if local_is_rank0 {
                &mut ensure_groups
            } else {
                &mut join_groups
            };
            groups.entry(co_holders).or_default().push(topic);
            summary.shard_topics += 1;
        }
    }

    restore_held_shard_topics(context, &net_handle, node_id, ensure_groups, join_groups).await;

    // New-holder verification: reconcile each held shard against a co-holder and
    // persist a marker so a restart resumes only unverified shards.
    crate::shard::verify::verify_held_shards(context, node_id, realm_id).await;

    summary
}

async fn restore_held_shard_topics(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    ensure_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>>,
    join_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>>,
) {
    for (groups, may_create) in [(ensure_groups, true), (join_groups, false)] {
        for (peers, topics) in groups {
            if peers.is_empty() || topics.is_empty() {
                continue;
            }
            if may_create {
                // Join-before-create: rank-0 may have just inherited a shard
                // whose genesis a previous rank-0 created — adopt that genesis
                // from a co-holder rather than forking a second one. Only what
                // no peer knows either is created fresh.
                let missing: Vec<::irokle::TopicId> = topics
                    .iter()
                    .copied()
                    .filter(|topic| {
                        !net_handle
                            .document_sync_topic_exists(*topic)
                            .unwrap_or(false)
                    })
                    .collect();
                if !missing.is_empty() {
                    let event = net_handle
                        .sync_document_topics(missing, peers.clone())
                        .await;
                    apply_restored_reconcile(context, node_id, event).await;
                }
                if let Err(error) = net_handle.ensure_document_sync_topics(&topics, peers.clone()) {
                    warn!(error = %error, "Failed to ensure held shard topics on restart");
                }
            }
            let event = net_handle.sync_document_topics(topics, peers).await;
            apply_restored_reconcile(context, node_id, event).await;
        }
    }
}

pub(crate) async fn apply_restored_reconcile(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    event: DocumentSyncNetEvent,
) {
    let result = match event {
        DocumentSyncNetEvent::DocumentsReconciled {
            applied,
            targets,
            metadata_create_events,
            metadata_graph_tombstones,
        } => {
            if applied == 0 {
                return;
            }
            DocumentSyncReconcileResult {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
            }
        }
        DocumentSyncNetEvent::Error { error, .. } => {
            warn!(error = %error, "Failed to sync held shard topics on restart");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected restart shard sync result");
            return;
        }
    };

    let tombstones = result.metadata_graph_tombstones.clone();
    refresh_realm_usage_summary_for_targets(context, node_id, &result.targets).await;
    refresh_watch_interest_for_targets(context, &result.targets).await;
    project_restored_metadata_create_events(
        context,
        node_id,
        result.targets,
        result.metadata_create_events,
    )
    .await;
    process_metadata_graph_tombstones(context, tombstones).await;
}

async fn project_restored_metadata_create_events(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    targets: Vec<DocumentSyncTarget>,
    metadata_create_events: Vec<MetadataCreateEventRecord>,
) {
    if !metadata_create_events.is_empty() {
        if let Err(error) =
            project_metadata_create_events(context, metadata_create_events, Some(node_id)).await
        {
            warn!(error = ?error, "Failed to project restored metadata create events");
        }
        return;
    }

    let mut pairs = Vec::new();
    for target in targets {
        if let DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        } = target
        {
            pairs.push((document_id, event_id));
        }
    }
    if pairs.is_empty() {
        return;
    }
    if let Err(error) = project_metadata_create_events_from_log(context, pairs).await {
        warn!(error = ?error, "Failed to project restored metadata create events from log");
    }
}

async fn load_realm_config(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            value.and_then(|bytes| RealmConfigDocument::from_bytes(&bytes).ok())
        }
        _ => None,
    }
}
