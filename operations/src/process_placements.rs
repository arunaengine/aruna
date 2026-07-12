use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, shard_topic_id};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use byteview::ByteView;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::placement::resolve_shard_holders;
use crate::sync_placement::{
    decode_placement, new_placement, placement_prefix, sort_node_ids, write_placement_effect,
};

const PENDING_PLACEMENT_PAGE_SIZE: usize = 256;

/// Reconciles every shard topic the local node holds, whatever its rank.
///
/// Rank-0 is a politeness device for who acts first, never a precondition for
/// the work happening: the rank-0 holder eagerly creates the genesis (so
/// creation has exactly one origin per shard, race-free by rank uniqueness),
/// while every other holder independently pulls the topic from a co-holder and
/// tops up co-holder membership. A freshly added holder therefore converges on
/// its own instead of waiting to be pushed to.
///
/// Join-before-create: a config change can move rank-0 (e.g. a new node ranks
/// first for a shard whose genesis the previous rank-0 already created), so a
/// missing topic is first adopted from a co-holder; only what no co-holder
/// knows either is created fresh.
/// Returns whether any genesis was withheld (a co-holder was unreachable or
/// refused a probe) or a held topic could not be pulled, so the caller can
/// schedule a placement retry.
#[derive(Clone, Copy, Debug, Default)]
struct HeldTopicOutcome {
    /// A rank-0 genesis was withheld (co-holder unreachable or refusing).
    withheld: bool,
    /// A held topic could not be pulled from any co-holder yet.
    pull_pending: bool,
}

async fn ensure_held_shard_topics(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> HeldTopicOutcome {
    let mut rank0_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>> = BTreeMap::new();
    let mut member_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>> = BTreeMap::new();
    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
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
                .push(shard_topic_id(realm_id, &placement));
        }
    }
    let mut outcome = HeldTopicOutcome::default();
    for (co_holders, topics) in rank0_groups {
        debug!(
            event = "placement.genesis.ensure",
            topics = topics.len(),
            co_holders = co_holders.len(),
            "Ensuring rank-0 shard topic geneses"
        );
        outcome.withheld |=
            ensure_rank0_shard_group(context, net_handle, local_node_id, co_holders, topics).await;
    }
    // Non-rank-0 held shards. A topic not known locally is pulled from a
    // co-holder: `sync_document_topics` is join-only (it adopts an existing
    // genesis, it can never mint one), so this is safe at any rank and cannot
    // fork. Without it a freshly added holder would stay passive forever,
    // depending on an existing member pushing to it — and when the shard's
    // origin is drained out of the holder set, nobody does.
    // Topics already known are topped up with the current co-holder set, which
    // is what admits a freshly added holder on the pushing side.
    for (co_holders, topics) in member_groups {
        if co_holders.is_empty() {
            continue;
        }
        let (mut known, missing): (Vec<::irokle::TopicId>, Vec<::irokle::TopicId>) =
            topics.into_iter().partition(|topic| {
                net_handle
                    .document_sync_topic_exists(*topic)
                    .unwrap_or(false)
            });
        if !missing.is_empty() {
            debug!(
                event = "placement.topic.pull",
                topics = missing.len(),
                co_holders = co_holders.len(),
                "Pulling newly held shard topics from co-holders"
            );
            let event = net_handle
                .sync_document_topics(missing.clone(), co_holders.clone())
                .await;
            crate::startup::apply_restored_reconcile(context, local_node_id, event).await;
            for topic in missing {
                if net_handle
                    .document_sync_topic_exists(topic)
                    .unwrap_or(false)
                {
                    known.push(topic);
                } else {
                    // No co-holder served a genesis (unreachable, or rank-0 has
                    // not created it yet); retry rather than stay passive.
                    outcome.pull_pending = true;
                }
            }
        }
        if known.is_empty() {
            continue;
        }
        if let Err(error) = net_handle.allow_document_sync_peers(&known, co_holders) {
            debug!(error = %error, "Could not complete held shard topic membership");
            outcome.withheld = true;
        }
    }
    outcome
}

/// Ensures the shard topics of one rank-0 co-holder group, creating a fresh
/// genesis only with positive confirmation that none exists.
///
/// Topics already known locally are ensured (membership top-up only, never a
/// create). For a missing topic the co-holders are probed: one that a co-holder
/// already holds is adopted via anti-entropy; one that every reached co-holder
/// positively confirmed unknown (an empty summary) is created fresh; but if any
/// co-holder was unreachable, or a reached one refused the topic (holds it but
/// the prober may not open it yet — its summary is silently omitted), creation
/// is withheld and left for the next placement pass — either might hold a
/// genesis, and forking a second one is a permanent split-brain. A sole holder
/// (no co-holders) creates immediately: no peer can hold a divergent genesis.
///
/// Returns whether any genesis was withheld (or an adopt failed to land), so the
/// caller schedules a placement retry instead of deferring writes forever.
pub(crate) async fn ensure_rank0_shard_group(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    local_node_id: NodeId,
    co_holders: Vec<NodeId>,
    topics: Vec<::irokle::TopicId>,
) -> bool {
    let mut to_ensure: Vec<::irokle::TopicId> = Vec::new();
    let mut missing: Vec<::irokle::TopicId> = Vec::new();
    for topic in topics {
        if net_handle
            .document_sync_topic_exists(topic)
            .unwrap_or(false)
        {
            to_ensure.push(topic);
        } else {
            missing.push(topic);
        }
    }

    let mut withheld = false;
    if !missing.is_empty() {
        if co_holders.is_empty() {
            to_ensure.extend(missing);
        } else {
            let probe = net_handle
                .probe_shard_topic_geneses(missing.clone(), co_holders.clone())
                .await;
            let mut to_adopt: Vec<::irokle::TopicId> = Vec::new();
            for topic in missing {
                if probe.known_by_co_holder.contains(&topic) {
                    to_adopt.push(topic);
                } else if probe.unreachable.is_empty() && !probe.unconfirmed.contains(&topic) {
                    to_ensure.push(topic);
                } else {
                    // A co-holder was unreachable, or a reached one refused the
                    // topic (holds it but the prober may not open it yet):
                    // withhold this genesis rather than fork a second one.
                    withheld = true;
                }
            }
            if !to_adopt.is_empty() {
                let event = net_handle
                    .sync_document_topics(to_adopt.clone(), co_holders.clone())
                    .await;
                crate::startup::apply_restored_reconcile(context, local_node_id, event).await;
                // Only ensure membership on topics whose genesis actually landed;
                // an adopt that failed (co-holder now unreachable) must not fall
                // through to a fresh create — retry it on the next pass instead.
                for topic in to_adopt {
                    if net_handle
                        .document_sync_topic_exists(topic)
                        .unwrap_or(false)
                    {
                        to_ensure.push(topic);
                    } else {
                        withheld = true;
                    }
                }
            }
            if !probe.unreachable.is_empty() || !probe.unconfirmed.is_empty() {
                warn!(
                    unreachable = ?probe.unreachable,
                    unconfirmed = ?probe.unconfirmed,
                    "Withholding shard genesis creation: co-holder unreachable or topic possibly-existing"
                );
            }
        }
    }

    if !to_ensure.is_empty()
        && let Err(error) = net_handle.ensure_document_sync_topics(&to_ensure, co_holders)
    {
        warn!(error = %error, "Failed to ensure rank-0 shard topics");
        withheld = true;
    }
    withheld
}

/// What a [`process_shard_placements`] pass decided about follow-up work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacementReconcileOutcome {
    /// A genesis was withheld or a record left incomplete, so a
    /// [`TaskKey::SyncPlacements`] retry timer was scheduled.
    pub retry_scheduled: bool,
}

/// Reconciles the local node's held shard topics with their co-holders.
///
/// First reconciles every held shard topic (see [`ensure_held_shard_topics`]):
/// rank-0 shards get their genesis created, every other held shard is pulled
/// from a co-holder. Then iterates the
/// [`SYNC_PLACEMENT_KEYSPACE`] records the write path left behind (one per
/// shard that was not fully replicated at write time), re-resolves each
/// shard's holder set from the current realm config, and adds every co-holder
/// as a member of the shard topic. Membership changes schedule an irokle topic
/// recheck, so the resync loop then pushes the shard's events to any freshly
/// added co-holder. A record whose co-holders are all members is removed; a
/// record the local node no longer holds is dropped; a record whose shard
/// topic has no genesis locally yet (non-rank-0 holder, genesis in flight) is
/// kept for retry.
///
/// A withheld genesis or an incomplete record schedules a [`TaskKey::SyncPlacements`]
/// retry so a down/refusing co-holder returning re-runs the reconciler; the
/// returned [`PlacementReconcileOutcome`] reports whether that retry was armed.
pub async fn process_shard_placements(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> PlacementReconcileOutcome {
    let mut outcome = PlacementReconcileOutcome::default();
    let Some(config) = load_realm_config(context, realm_id).await else {
        warn!(%realm_id, "Cannot process shard placements without a realm config");
        return outcome;
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return outcome;
    };

    // A withheld genesis or an unpulled held topic leaves no placement record,
    // so it alone must still arm the retry below (otherwise writes defer at 1s
    // forever).
    let held =
        ensure_held_shard_topics(context, net_handle, &config, realm_id, local_node_id).await;
    let mut retry_needed = held.withheld || held.pull_pending;

    let mut start_after: Option<Key> = None;
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
                warn!(error = %error, "Failed to list pending shard placements");
                return outcome;
            }
            other => {
                warn!(event = ?other, "Unexpected pending shard placement iter result");
                return outcome;
            }
        };

        for (key, value) in &batch {
            let record = match decode_placement(value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(error = %error, "Deleting malformed shard placement record");
                    delete_record(context, key.to_vec()).await;
                    continue;
                }
            };
            if record.realm_id != realm_id {
                continue;
            }

            let holders = resolve_shard_holders(&config, &record.placement);
            if !holders.contains(&local_node_id) {
                // The local node is no longer a holder of this shard. Drop the
                // verification marker too so a later re-entry re-verifies.
                delete_record(context, key.to_vec()).await;
                crate::shard::verify::delete_shard_verification(
                    context,
                    realm_id,
                    &record.placement,
                )
                .await;
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

            let topic = shard_topic_id(realm_id, &record.placement);
            // Genesis creation is owned by `ensure_rank0_shard_topics` (gated on
            // positive co-holder confirmation); this loop only tops up membership
            // on a topic already known locally. A topic whose genesis is not yet
            // local — a rank-0 create withheld for a down co-holder, or a
            // non-rank-0 holder still awaiting gossip — is kept for the next pass
            // rather than force-created into a fork.
            if !net_handle
                .document_sync_topic_exists(topic)
                .unwrap_or(false)
            {
                debug!(
                    ?topic,
                    "Shard topic genesis not local yet; keeping placement record"
                );
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
                continue;
            }
            // The topic exists locally, so ensure only adds members (never
            // creates); a non-rank-0 holder likewise only adds members.
            let membership = if local_is_rank0 {
                net_handle.ensure_document_sync_topics(&[topic], co_holders.clone())
            } else {
                net_handle.allow_document_sync_peers(&[topic], co_holders.clone())
            };
            match membership {
                Ok(()) => {
                    // Every co-holder is now a member; the resync loop delivers
                    // the shard's events. Record satisfied.
                    delete_record(context, key.to_vec()).await;
                }
                Err(error) => {
                    debug!(error = %error, "Shard topic membership incomplete; keeping placement record");
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
        // A pending pull is join-only and usually one gossip push away, so it
        // retries on the short cadence; a withheld genesis waits out the full
        // interval (re-probing a down co-holder is expensive).
        let after = if held.pull_pending {
            crate::sync_placement::SHARD_TOPIC_PULL_RETRY_AFTER
        } else {
            crate::sync_placement::SYNC_PLACEMENT_RETRY_AFTER
        };
        let effect =
            crate::sync_placement::schedule_placement_retry_after(realm_id, local_node_id, after);
        let _ = task_handle.send_effect(effect).await;
        outcome.retry_scheduled = true;
    }
    outcome
}

pub(crate) async fn load_realm_config(
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
            shard_count: 64,
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
                shard: 3,
            },
        )
    }

    #[test]
    fn shard_holders_are_deterministic_across_node_ordering() {
        let (config, placement) = config_with(&[node(1), node(2), node(3), node(4)], None);
        let first = resolve_shard_holders(&config, &placement);

        let (reversed, _) = config_with(&[node(4), node(3), node(2), node(1)], None);
        let second = resolve_shard_holders(&reversed, &placement);

        assert_eq!(first, second);
        assert_eq!(first.len(), 4);
    }

    #[test]
    fn replica_capped_shard_holder_set_is_bounded() {
        let (config, placement) = config_with(&[node(1), node(2), node(3), node(4)], Some(2));
        let holders = resolve_shard_holders(&config, &placement);
        assert_eq!(holders.len(), 2);
    }
}
