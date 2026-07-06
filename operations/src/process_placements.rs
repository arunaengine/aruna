use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, bucket_topic_id};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use byteview::ByteView;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::placement::resolve_bucket_holders;
use crate::sync_placement::{
    decode_placement, new_placement, placement_prefix, sort_node_ids, write_placement_effect,
};

const PENDING_PLACEMENT_PAGE_SIZE: usize = 256;

/// Eagerly creates the genesis of every bucket topic whose rank-0 holder is the
/// local node, so genesis creation has exactly one origin per bucket (race-free
/// by rank uniqueness). Every other node is join-only: it receives the genesis
/// via gossip from the rank-0 holder or bootstraps it during anti-entropy.
/// Runs on realm-config apply/change and at startup.
///
/// Join-before-create: a config change can move rank-0 (e.g. a new node ranks
/// first for a bucket whose genesis the previous rank-0 already created), so a
/// missing topic is first adopted from a co-holder; only what no co-holder
/// knows either is created fresh.
async fn ensure_rank0_bucket_topics(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
) {
    let mut rank0_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>> = BTreeMap::new();
    let mut member_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>> = BTreeMap::new();
    for strategy in &config.strategies {
        for bucket in 0..strategy.bucket_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                bucket,
            };
            let holders = resolve_bucket_holders(config, &placement);
            if !holders.contains(&local_node_id) {
                continue;
            }
            let local_is_rank0 = holders.first() == Some(&local_node_id);
            let mut co_holders: Vec<NodeId> = holders
                .into_iter()
                .filter(|candidate| *candidate != local_node_id)
                .collect();
            sort_node_ids(&mut co_holders);
            let groups = if local_is_rank0 {
                &mut rank0_groups
            } else {
                &mut member_groups
            };
            groups
                .entry(co_holders)
                .or_default()
                .push(bucket_topic_id(realm_id, &placement));
        }
    }
    for (co_holders, topics) in rank0_groups {
        let missing: Vec<::irokle::TopicId> = topics
            .iter()
            .copied()
            .filter(|topic| {
                !net_handle
                    .document_sync_topic_exists(*topic)
                    .unwrap_or(false)
            })
            .collect();
        if !missing.is_empty() && !co_holders.is_empty() {
            let event = net_handle
                .sync_document_topics(missing, co_holders.clone())
                .await;
            crate::startup::apply_restored_reconcile(context, local_node_id, event).await;
        }
        debug!(
            event = "placement.genesis.ensure",
            topics = topics.len(),
            co_holders = co_holders.len(),
            "Ensuring rank-0 bucket topic geneses"
        );
        if let Err(error) = net_handle.ensure_document_sync_topics(&topics, co_holders) {
            warn!(error = %error, "Failed to ensure rank-0 bucket topics");
        }
    }
    // Non-rank-0 held buckets: a holder that is already a member (e.g. the
    // previous rank-0 after a config change moved the rank) completes the
    // membership of freshly added co-holders — a new holder cannot add itself.
    // Topics not known locally are skipped: their rank-0 creates them with the
    // full holder set as initial peers.
    for (co_holders, topics) in member_groups {
        if co_holders.is_empty() {
            continue;
        }
        let known: Vec<::irokle::TopicId> = topics
            .into_iter()
            .filter(|topic| {
                net_handle
                    .document_sync_topic_exists(*topic)
                    .unwrap_or(false)
            })
            .collect();
        if known.is_empty() {
            continue;
        }
        if let Err(error) = net_handle.allow_document_sync_peers(&known, co_holders) {
            debug!(error = %error, "Could not complete held bucket topic membership");
        }
    }
}

/// Reconciles the local node's held bucket topics with their co-holders.
///
/// First creates the genesis of every bucket the local node is rank-0 holder
/// of (see [`ensure_rank0_bucket_topics`]). Then iterates the
/// [`SYNC_PLACEMENT_KEYSPACE`] records the write path left behind (one per
/// bucket that was not fully replicated at write time), re-resolves each
/// bucket's holder set from the current realm config, and adds every co-holder
/// as a member of the bucket topic. Membership changes schedule an irokle topic
/// recheck, so the resync loop then pushes the bucket's events to any freshly
/// added co-holder. A record whose co-holders are all members is removed; a
/// record the local node no longer holds is dropped; a record whose bucket
/// topic has no genesis locally yet (non-rank-0 holder, genesis in flight) is
/// kept for retry.
pub async fn process_bucket_placements(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
) {
    let Some(config) = load_realm_config(context, realm_id).await else {
        warn!(%realm_id, "Cannot process bucket placements without a realm config");
        return;
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return;
    };

    ensure_rank0_bucket_topics(context, net_handle, &config, realm_id, local_node_id).await;

    let mut start_after: Option<Key> = None;
    let mut retry_needed = false;
    loop {
        let batch = match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
                prefix: Some(placement_prefix(realm_id)),
                start: start_after.take().map(IterStart::After),
                limit: PENDING_PLACEMENT_PAGE_SIZE,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                start_after = next_start_after;
                values
            }
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(error = %error, "Failed to list pending bucket placements");
                return;
            }
            other => {
                warn!(event = ?other, "Unexpected pending bucket placement iter result");
                return;
            }
        };

        for (key, value) in &batch {
            let record = match decode_placement(value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(error = %error, "Deleting malformed bucket placement record");
                    delete_record(context, key.to_vec()).await;
                    continue;
                }
            };
            if record.realm_id != realm_id {
                continue;
            }

            let holders = resolve_bucket_holders(&config, &record.placement);
            if !holders.contains(&local_node_id) {
                // The local node is no longer a holder of this bucket.
                delete_record(context, key.to_vec()).await;
                continue;
            }
            let local_is_rank0 = holders.first() == Some(&local_node_id);
            let mut co_holders: Vec<NodeId> = holders
                .into_iter()
                .filter(|node_id| *node_id != local_node_id)
                .collect();
            sort_node_ids(&mut co_holders);
            if co_holders.is_empty() {
                delete_record(context, key.to_vec()).await;
                continue;
            }

            let topic = bucket_topic_id(realm_id, &record.placement);
            // Rank-0 may create the genesis; every other holder is join-only
            // and can only add members to an already-known topic.
            let membership = if local_is_rank0 {
                net_handle.ensure_document_sync_topics(&[topic], co_holders.clone())
            } else {
                net_handle.allow_document_sync_peers(&[topic], co_holders.clone())
            };
            match membership {
                Ok(()) => {
                    // Every co-holder is now a member; the resync loop delivers
                    // the bucket's events. Record satisfied.
                    delete_record(context, key.to_vec()).await;
                }
                Err(error) => {
                    debug!(error = %error, "Bucket topic membership incomplete; keeping placement record");
                    let refreshed = new_placement(
                        realm_id,
                        record.placement,
                        local_node_id,
                        record.selected_peers.clone(),
                    );
                    if let Ok(effect) = write_placement_effect(&refreshed) {
                        let _ = context.storage_handle.send_effect(effect).await;
                    }
                    retry_needed = true;
                }
            }
        }

        if start_after.is_none() {
            break;
        }
    }

    if retry_needed && let Some(task_handle) = context.task_handle.as_ref() {
        let effect =
            crate::sync_placement::schedule_placement_retry_effect(realm_id, local_node_id);
        let _ = task_handle.send_effect(effect).await;
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

async fn delete_record(context: &Arc<DriverContext>, key: Vec<u8>) {
    let _ = context
        .storage_handle
        .send_effect(aruna_core::effects::Effect::Storage(
            StorageEffect::Delete {
                key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            },
        ))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{PlacementRef, PlacementStrategy, RealmNodeKind};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_with(nodes: &[NodeId], replica: Option<u32>) -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([8u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "default".to_string(),
            replica_count: replica,
            distinct_locations: false,
            affinity: Vec::new(),
            bucket_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                bucket: 3,
            },
        )
    }

    #[test]
    fn bucket_holders_are_deterministic_across_node_ordering() {
        let (config, placement) = config_with(&[node(1), node(2), node(3), node(4)], None);
        let first = resolve_bucket_holders(&config, &placement);

        let (reversed, _) = config_with(&[node(4), node(3), node(2), node(1)], None);
        let second = resolve_bucket_holders(&reversed, &placement);

        assert_eq!(first, second);
        assert_eq!(first.len(), 4);
    }

    #[test]
    fn replica_capped_bucket_holder_set_is_bounded() {
        let (config, placement) = config_with(&[node(1), node(2), node(3), node(4)], Some(2));
        let holders = resolve_bucket_holders(&config, &placement);
        assert_eq!(holders.len(), 2);
    }
}
