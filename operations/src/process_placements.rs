use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, shard_topic_id};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::structs::{Actor, PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::placement::{bucket_membership, draining_former_holders, resolve_shard_holders};
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
    verified: &BTreeSet<::irokle::TopicId>,
    now_ms: u64,
) -> HeldTopicOutcome {
    type ShardGroup = (Vec<NodeId>, Vec<NodeId>, BTreeSet<NodeId>);
    let mut rank0_groups: BTreeMap<ShardGroup, Vec<::irokle::TopicId>> = BTreeMap::new();
    let mut member_groups: BTreeMap<ShardGroup, Vec<::irokle::TopicId>> = BTreeMap::new();
    let mut outcome = HeldTopicOutcome::default();
    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
            // Per-bucket membership: admitted targets join for delivery, and
            // retained departing holders stay through their bucket's grace
            // (#399 bounds the peak at |old U new|); publish authority stays
            // with activated plus retained holders only.
            let membership = bucket_membership(config, &placement, now_ms);
            if !membership.members.contains(&local_node_id) {
                continue;
            }
            let active_target = config.placement_transitions.iter().any(|transition| {
                transition.plan.strategy_id == placement.strategy_id
                    && matches!(
                        transition.status,
                        aruna_core::structs::TransitionStatus::Active
                    )
                    && transition
                        .plan
                        .bucket_plan(placement.shard)
                        .is_some_and(|bucket| {
                            transition.completion(placement.shard).is_none()
                                && bucket.target_holders.contains(&local_node_id)
                        })
            });
            if (holders.contains(&local_node_id) || active_target)
                && let Err(error) =
                    net_handle.reopen_sync_topic(shard_topic_id(realm_id, &placement))
            {
                debug!(error = %error, "Failed to reopen a current shard holder topic");
                outcome.pull_pending = true;
            }
            // Rank-0 is an activation role: a target that has not cut over yet
            // never creates a genesis (#400).
            let local_is_rank0 = holders.first() == Some(&local_node_id);
            let mut co_members: Vec<NodeId> = membership
                .members
                .into_iter()
                .filter(|candidate| *candidate != local_node_id)
                .collect();
            sort_node_ids(&mut co_members);
            let mut publishers = membership.publishers;
            sort_node_ids(&mut publishers);
            let retained: BTreeSet<NodeId> = draining_former_holders(config, &placement)
                .into_iter()
                .collect();
            let groups = if local_is_rank0 {
                &mut rank0_groups
            } else {
                &mut member_groups
            };
            groups
                .entry((co_members, publishers, retained))
                .or_default()
                .push(shard_topic_id(realm_id, &placement));
        }
    }
    // Pass-scoped, so a peer that comes back is probed again next reconcile.
    let mut unreachable_peers: BTreeSet<NodeId> = BTreeSet::new();
    for ((co_members, publishers, retained), topics) in rank0_groups {
        debug!(
            event = "placement.genesis.ensure",
            topics = topics.len(),
            co_members = co_members.len(),
            "Ensuring rank-0 shard topic geneses"
        );
        outcome.withheld |= ensure_rank0_shard_group(
            context,
            net_handle,
            local_node_id,
            co_members,
            publishers,
            topics,
            &retained,
            verified,
            &mut unreachable_peers,
        )
        .await;
    }
    // Non-rank-0 held shards. A topic not known locally is pulled from a
    // co-holder: `sync_document_topics` is join-only (it adopts an existing
    // genesis, it can never mint one), so this is safe at any rank and cannot
    // fork. Without it a freshly added holder would stay passive forever,
    // depending on an existing member pushing to it — and when the shard's
    // origin is drained out of the holder set, nobody does.
    // Topics already known are topped up with the current co-holder set, which
    // is what admits a freshly added holder on the pushing side.
    for ((co_members, publishers, retained), topics) in member_groups {
        if co_members.is_empty() {
            continue;
        }
        let mut current_members = co_members.clone();
        current_members.push(local_node_id);
        sort_node_ids(&mut current_members);
        // Install the current publisher policy before pulling any history. A
        // missing topic is expected here; the exact membership pass below is
        // repeated after a successful pull.
        let _ = net_handle
            .reconcile_shard_membership(
                &topics,
                current_members.clone(),
                publishers.clone(),
                &retained,
                verified,
            )
            .await;
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
                co_members = co_members.len(),
                "Pulling newly held shard topics from co-holders"
            );
            let event = net_handle
                .sync_document_topics(missing.clone(), co_members.clone())
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
        if let Err(error) = net_handle
            .reconcile_shard_membership(&known, current_members, publishers, &retained, verified)
            .await
        {
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
/// Splits `topics` into the ones this node may safely hold or create and a flag
/// saying whether any creation was withheld.
///
/// A topic a co-holder already holds is adopted by anti-entropy; one every
/// reached co-holder positively confirmed unknown is safe to create; but if any
/// co-holder was unreachable, or a reached one refused the topic (it holds the
/// genesis but the prober may not open it yet), creation is withheld — either
/// might hold a genesis, and forking a second one is a permanent split-brain.
/// A sole holder creates immediately: no peer can hold a divergent genesis.
/// The withheld flag tells the caller to retry rather than strand the topic.
///
/// `may_mint` is the caller's single-minter decision. Positive absence is a
/// snapshot, not a lock, so a caller that is not the designated minter for these
/// topics adopts only and leaves an absent topic withheld.
pub(crate) async fn resolve_creatable_topics(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    local_node_id: NodeId,
    co_members: &[NodeId],
    topics: Vec<::irokle::TopicId>,
    may_mint: bool,
    unreachable_peers: &mut BTreeSet<NodeId>,
) -> (Vec<::irokle::TopicId>, bool) {
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
    if missing.is_empty() {
        return (to_ensure, withheld);
    }
    if co_members.is_empty() {
        if may_mint {
            to_ensure.extend(missing);
        } else {
            withheld = true;
        }
        return (to_ensure, withheld);
    }

    // A peer already unreachable in this pass is not probed again: waiting its
    // full deadline a second time cannot change the verdict. A stale entry can
    // only withhold, never mint, so a peer that recovers costs one more pass.
    let (live, skipped): (Vec<NodeId>, Vec<NodeId>) = co_members
        .iter()
        .copied()
        .partition(|peer| !unreachable_peers.contains(peer));
    let mut probe = if live.is_empty() {
        aruna_net::ShardGenesisProbe::default()
    } else {
        net_handle
            .probe_shard_topic_geneses(missing.clone(), live)
            .await
    };
    // Skipped peers stay in `unreachable` so an all-dead set still withholds:
    // dropping them would read as "no co-holder to consult" and mint a rival.
    probe.unreachable.extend(skipped);
    unreachable_peers.extend(probe.unreachable.iter().copied());
    let mut to_adopt: Vec<::irokle::TopicId> = Vec::new();
    for topic in missing {
        if probe.known_by_co_holder.contains(&topic) {
            to_adopt.push(topic);
        } else if may_mint && probe.unreachable.is_empty() && !probe.unconfirmed.contains(&topic) {
            to_ensure.push(topic);
        } else {
            withheld = true;
        }
    }
    if !to_adopt.is_empty() {
        let event = net_handle
            .sync_document_topics(to_adopt.clone(), co_members.to_vec())
            .await;
        crate::startup::apply_restored_reconcile(context, local_node_id, event).await;
        // Only keep topics whose genesis actually landed; an adopt that failed
        // must not fall through to a fresh create - retry it on the next pass.
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
            "Withholding genesis creation: co-holder unreachable or topic possibly-existing"
        );
    }
    (to_ensure, withheld)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_rank0_shard_group(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    local_node_id: NodeId,
    co_members: Vec<NodeId>,
    publishers: Vec<NodeId>,
    topics: Vec<::irokle::TopicId>,
    retained: &BTreeSet<NodeId>,
    verified: &BTreeSet<::irokle::TopicId>,
    unreachable_peers: &mut BTreeSet<NodeId>,
) -> bool {
    let mut current_members = co_members.clone();
    current_members.push(local_node_id);
    sort_node_ids(&mut current_members);
    // This first pass installs publisher policy even when a topic still needs
    // to be adopted or created. Exact membership is retried once it exists.
    let _ = net_handle
        .reconcile_shard_membership(
            &topics,
            current_members.clone(),
            publishers.clone(),
            retained,
            verified,
        )
        .await;

    let (to_ensure, mut withheld) = resolve_creatable_topics(
        context,
        net_handle,
        local_node_id,
        &co_members,
        topics,
        true,
        unreachable_peers,
    )
    .await;

    if !to_ensure.is_empty() {
        match net_handle.ensure_document_sync_topics(&to_ensure, co_members) {
            Ok(()) => {
                if let Err(error) = net_handle
                    .reconcile_shard_membership(
                        &to_ensure,
                        current_members,
                        publishers,
                        retained,
                        verified,
                    )
                    .await
                {
                    warn!(error = %error, "Failed to reconcile rank-0 shard membership");
                    withheld = true;
                }
            }
            Err(error) => {
                warn!(error = %error, "Failed to ensure rank-0 shard topics");
                withheld = true;
            }
        }
    }
    withheld
}

/// What a [`process_shard_placements`] pass decided about follow-up work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlacementReconcileStatus {
    /// Reconciliation completed without scheduling more work.
    #[default]
    Clean,
    /// A genesis was withheld or a record left incomplete, so the reconciler
    /// scheduled its own [`TaskKey::SyncPlacements`] retry timer.
    RetryScheduled,
    /// Storage could not provide a trustworthy config or placement scan. A
    /// timer consumer must re-arm the consumed timer.
    StorageFailure,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacementReconcileOutcome {
    /// Compatibility signal for callers that only need to know whether the
    /// reconciler armed its own retry.
    pub retry_scheduled: bool,
    /// The retry includes a held topic that could not be pulled yet.
    pub pull_pending: bool,
    pub status: PlacementReconcileStatus,
}

impl PlacementReconcileOutcome {
    fn clean() -> Self {
        Self::default()
    }

    fn retry_scheduled(pull_pending: bool) -> Self {
        Self {
            retry_scheduled: true,
            pull_pending,
            status: PlacementReconcileStatus::RetryScheduled,
        }
    }

    fn storage_failure() -> Self {
        Self {
            retry_scheduled: false,
            pull_pending: false,
            status: PlacementReconcileStatus::StorageFailure,
        }
    }
}

enum RealmConfigLoadOutcome {
    Found(RealmConfigDocument),
    Absent,
    StorageFailure,
}

/// Reconciles the local node's held shard topics with their co-holders.
///
/// First reconciles every held shard topic (see [`ensure_held_shard_topics`]):
/// rank-0 shards get their genesis created, every other held shard is pulled
/// from a co-holder. Then iterates the
/// [`SYNC_PLACEMENT_KEYSPACE`] records the write path left behind (one per
/// shard that was not fully replicated at write time), re-resolves each
/// shard's holder set from the current realm config, and makes those holders
/// the shard topic's exact members and accepted publishers. Membership changes
/// schedule an irokle topic recheck, so the resync loop then pushes the shard's
/// events to any freshly added co-holder. A satisfied record is removed; a
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
    reconcile_placements(context, realm_id, local_node_id, true).await
}

/// [`process_shard_placements`] minus transition-step execution, for request
/// paths: every placement mutation arms a zero-delay `SyncPlacements` timer,
/// so barriers and completion proofs run in the background instead of inline.
pub async fn reconcile_shard_topics(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> PlacementReconcileOutcome {
    reconcile_placements(context, realm_id, local_node_id, false).await
}

async fn reconcile_placements(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    run_transitions: bool,
) -> PlacementReconcileOutcome {
    let config = match load_realm_config_outcome(context, realm_id).await {
        RealmConfigLoadOutcome::Found(config) => config,
        RealmConfigLoadOutcome::Absent => {
            warn!(%realm_id, "Cannot process shard placements without a realm config");
            return PlacementReconcileOutcome::clean();
        }
        RealmConfigLoadOutcome::StorageFailure => {
            return PlacementReconcileOutcome::storage_failure();
        }
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return PlacementReconcileOutcome::clean();
    };

    // Former-holder history cutoffs are frozen only for durably verified shards.
    let verified = crate::shard::verify::load_verified_shard_topics(context, realm_id).await;

    // A withheld genesis or an unpulled held topic leaves no placement record,
    // so it alone must still arm the retry below (otherwise writes defer at 1s
    // forever).
    let held = ensure_held_shard_topics(
        context,
        net_handle,
        &config,
        realm_id,
        local_node_id,
        &verified,
        unix_timestamp_millis(),
    )
    .await;
    let mut retry_needed = held.withheld || held.pull_pending;
    if run_transitions {
        retry_needed |= crate::process_transitions::process_placement_transitions(
            context,
            realm_id,
            local_node_id,
            &config,
        )
        .await;
    } else if has_transition_work(&config, unix_timestamp_millis())
        && let Some(task_handle) = context.task_handle.as_ref()
    {
        // A pure transition target never mutates the config, so nothing else
        // arms its timer; fire the deferred execution now.
        let effect = crate::sync_placement::schedule_placement_retry_after(
            realm_id,
            local_node_id,
            std::time::Duration::ZERO,
        );
        let _ = task_handle.send_effect(effect).await;
    }

    // Release is a deadline, not an event: a record that is released in the
    // local view is pruned here, because no later event may ever re-materialize
    // the document for it.
    let deadline_now = unix_timestamp_millis();
    retry_needed |=
        prune_released_transitions(context, realm_id, local_node_id, &config, deadline_now).await;
    // Arm the timer for the earliest pending grace end (shorten-only, so a
    // sooner retry is never postponed) and keep the normal retry driving
    // post-grace drain scans.
    retry_needed |= crate::placement::drain_pending(&config, deadline_now);
    if let Some(deadline) = crate::placement::next_release_ms(&config, deadline_now)
        && let Some(task_handle) = context.task_handle.as_ref()
    {
        let effect = crate::sync_placement::schedule_placement_deadline(
            realm_id,
            local_node_id,
            std::time::Duration::from_millis(deadline.saturating_sub(deadline_now)),
        );
        let _ = task_handle.send_effect(effect).await;
    }

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
                return PlacementReconcileOutcome::storage_failure();
            }
            other => {
                warn!(event = ?other, "Unexpected pending shard placement iter result");
                return PlacementReconcileOutcome::storage_failure();
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

            // A resolution failure keeps the durable record: deleting it on a
            // missing or conflicted activation would destroy the only retry.
            let holders =
                match crate::placement::resolve_shard_holders_checked(&config, &record.placement) {
                    Ok(holders) => holders,
                    Err(error) => {
                        debug!(error = %error, "Keeping placement record for unresolvable bucket");
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
                };
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
            let mut co_holders: Vec<NodeId> = holders
                .iter()
                .copied()
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
            // The topic exists locally, so reconcile its exact canonical holder
            // set without creating a genesis or changing shared/default peers.
            let retained = draining_former_holders(&config, &record.placement)
                .into_iter()
                .collect();
            let bucket = bucket_membership(&config, &record.placement, unix_timestamp_millis());
            let membership = net_handle
                .reconcile_shard_membership(
                    &[topic],
                    bucket.members,
                    bucket.publishers,
                    &retained,
                    &verified,
                )
                .await;
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
        return PlacementReconcileOutcome::retry_scheduled(held.pull_pending);
    }
    PlacementReconcileOutcome::clean()
}

/// Whether the transitions engine may have local steps to run: an unsettled
/// transition, or a strategy whose activations nobody initialized yet.
fn has_transition_work(config: &RealmConfigDocument, now_ms: u64) -> bool {
    config
        .placement_transitions
        .iter()
        .any(|transition| !transition.is_terminal())
        || (config.newest_map_epoch().is_some()
            && config
                .strategies
                .iter()
                .any(|strategy| config.activation(&strategy.strategy_id, 0).is_none()))
        || crate::placement::next_release_ms(config, now_ms).is_some()
        || crate::placement::drain_pending(config, now_ms)
        // A cheap over-approximation of a pending successor expansion: an
        // activation trailing the newest map with no transition in flight.
        || config.newest_map_epoch().is_some_and(|newest| {
            config.placement_activations.iter().any(|activation| {
                activation.candidate_map_epoch != newest
                    && !config.placement_transitions.iter().any(|transition| {
                        transition.plan.strategy_id == activation.strategy_id
                            && !transition.is_terminal()
                    })
            })
        })
}

async fn load_realm_config_outcome(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
) -> RealmConfigLoadOutcome {
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
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => match RealmConfigDocument::from_bytes(&bytes) {
            Ok(config) => RealmConfigLoadOutcome::Found(config),
            Err(error) => {
                warn!(%realm_id, error = %error, "Failed to decode realm config for shard placements");
                RealmConfigLoadOutcome::StorageFailure
            }
        },
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            RealmConfigLoadOutcome::Absent
        }
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(%realm_id, error = %error, "Failed to read realm config for shard placements");
            RealmConfigLoadOutcome::StorageFailure
        }
        other => {
            warn!(%realm_id, event = ?other, "Unexpected realm config read result for shard placements");
            RealmConfigLoadOutcome::StorageFailure
        }
    }
}

pub(crate) async fn load_realm_config(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match load_realm_config_outcome(context, realm_id).await {
        RealmConfigLoadOutcome::Found(config) => Some(config),
        RealmConfigLoadOutcome::Absent | RealmConfigLoadOutcome::StorageFailure => None,
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

/// Drops transitions the local view already considers released, by
/// re-materializing the stored document from the reducer state. Returns
/// whether the pass should retry, which it does only when storage failed.
async fn prune_released_transitions(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> bool {
    if !config
        .placement_transitions
        .iter()
        .any(|transition| transition.released(now_ms))
    {
        return false;
    }
    let document = DocumentSyncTarget::RealmConfig { realm_id };
    let target = aruna_core::admin_documents::AdminDocumentTarget::RealmConfig { realm_id };
    let storage = &context.storage_handle;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => {
            warn!(event = ?other, "Failed to start a transition release transaction");
            return true;
        }
    };
    let values = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    aruna_core::storage_entries::admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            warn!(event = ?other, "Failed to read the realm config for transition release");
            abort_release_txn(storage, txn_id).await;
            return true;
        }
    };
    let (Some(stored), Some(state)) = (
        values.first().and_then(|(_, value)| value.as_ref()),
        values.get(1).and_then(|(_, value)| value.as_ref()),
    ) else {
        abort_release_txn(storage, txn_id).await;
        return false;
    };
    let (Ok(mut stored), Ok(state)) = (
        RealmConfigDocument::from_bytes(stored.as_ref()),
        aruna_core::admin_document_reducer::decode_admin_document_reducer_state(state.as_ref()),
    ) else {
        warn!("Undecodable realm config or reducer state; transition release skipped");
        abort_release_txn(storage, txn_id).await;
        return false;
    };
    let before = stored.placement_transitions.len();
    crate::ensure_realm_config::overlay_realm_config_reducer_materialization(
        &mut stored,
        &state,
        now_ms,
    );
    if stored.placement_transitions.len() == before {
        abort_release_txn(storage, txn_id).await;
        return false;
    }
    let actor = Actor {
        node_id: local_node_id,
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    let value = match stored.to_bytes(&actor) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(%error, "Failed to encode the released realm config");
            abort_release_txn(storage, txn_id).await;
            return true;
        }
    };
    let written = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: document.storage_keyspace().to_string(),
            key: document.storage_key(),
            value: value.into(),
            txn_id: Some(txn_id),
        })
        .await;
    if !matches!(written, Event::Storage(StorageEvent::WriteResult { .. })) {
        warn!(event = ?written, "Failed to write the released realm config");
        abort_release_txn(storage, txn_id).await;
        return true;
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
            debug!(
                pruned = before - stored.placement_transitions.len(),
                "Released placement transitions pruned"
            );
            false
        }
        other => {
            warn!(event = ?other, "Failed to commit the released realm config");
            true
        }
    }
}

async fn abort_release_txn(
    storage: &aruna_storage::StorageHandle,
    txn_id: aruna_core::types::TxnId,
) {
    if let Event::Storage(StorageEvent::Error { error }) = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        warn!(%error, "Failed to abort a transition release transaction");
    }
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
