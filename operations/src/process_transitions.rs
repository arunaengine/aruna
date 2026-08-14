//! Per-node execution of placement transitions.
//!
//! Nobody drives a transition centrally (DECISIONS D9): every node acts on its
//! own replicated observation of the record and the reducer settles the result.
//! An old holder freezes its frontier; a target holder joins the existing topic,
//! pulls its complete history, verifies it against an old holder with the shard
//! manifest machinery, and signs the digest it converged on. A target never
//! mints a genesis - with every source unreachable the bucket stalls instead
//! (#400), because a rival genesis is a permanent split-brain.

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::shard_topic_id;
use aruna_core::structs::{
    Actor, PlacementRef, PlacementTransition, ProofClaim, RealmConfigDocument, RealmId,
    TransitionStatus,
};
use aruna_core::types::UserId;
use tracing::{debug, warn};

use crate::driver::{DriverContext, drive};
use crate::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementOperation, RealmPlacementMutation,
};
use crate::placement::transition::{
    TransitionRequest, expansion_buckets, holders_in_map, plan_transition,
};
use crate::shard::assemble_shard_manifest;
use crate::shard::verify::converge_with_barrier;

/// Runs every transition step the local node owns. Returns whether a step is
/// still outstanding, so the caller re-arms the placement retry.
pub async fn process_placement_transitions(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
) -> bool {
    if config.placement_transitions.is_empty() {
        let activations =
            ensure_strategy_activations(context, realm_id, local_node_id, config).await;
        let expansions = ensure_expansions(context, realm_id, local_node_id, config).await;
        return activations || expansions;
    }
    let mut pending = ensure_strategy_activations(context, realm_id, local_node_id, config).await;
    pending |= ensure_expansions(context, realm_id, local_node_id, config).await;
    let mut departed = false;
    // Steps are gathered and committed as one batch: per-bucket commits on a
    // realm-scale transition would grind the config document through hundreds
    // of serialized, conflict-prone transactions (one per bucket per node).
    let mut steps: Vec<RealmPlacementMutation> = Vec::new();
    for transition in &config.placement_transitions {
        if transition_stale(config, transition) {
            // A plan whose incomplete buckets all have stale predecessors can
            // never apply; abort it so the strategy's in-flight slot frees.
            if is_management(config, local_node_id) {
                steps.push(RealmPlacementMutation::AbortTransition(
                    transition.plan.transition_id,
                ));
            }
            continue;
        }
        // One window function decides execution and membership alike: only
        // admitted buckets run, and only their targets join topics.
        let admitted: Vec<u32> = transition
            .plan
            .admitted_buckets(&transition.completed)
            .map(|bucket| bucket.bucket)
            .collect();
        for bucket in &transition.plan.buckets {
            if let Some(completion) = transition.completion(bucket.bucket) {
                let departing = bucket.old_holders.contains(&local_node_id)
                    && !bucket.target_holders.contains(&local_node_id);
                departed |= departing;
                // Past the bucket's grace, a departing holder owes a drain
                // report before its retention may end (F7).
                let grace_passed = aruna_core::util::unix_timestamp_millis()
                    >= completion
                        .completed_at_ms
                        .saturating_add(transition.plan.limits.grace_ms);
                if departing
                    && grace_passed
                    && !transition.drain_reported(bucket.bucket, local_node_id)
                {
                    let placement = PlacementRef {
                        strategy_id: transition.plan.strategy_id,
                        shard: bucket.bucket,
                    };
                    match drain_step(context, transition, &placement, local_node_id).await {
                        StepPlan::Ready(mutation) => steps.push(mutation),
                        StepPlan::Stalled(mutation) => {
                            steps.push(mutation);
                            pending = true;
                        }
                        StepPlan::Pending => pending = true,
                        StepPlan::Done => {}
                    }
                }
                continue;
            }
            // An observed abort stops this node from submitting anything
            // further: the un-cut buckets simply never complete.
            if !matches!(transition.status, TransitionStatus::Active) {
                continue;
            }
            if !admitted.contains(&bucket.bucket) {
                pending = true;
                continue;
            }
            let placement = PlacementRef {
                strategy_id: transition.plan.strategy_id,
                shard: bucket.bucket,
            };
            if bucket.old_holders.contains(&local_node_id) {
                match barrier_step(context, realm_id, local_node_id, transition, &placement).await {
                    StepPlan::Ready(mutation) => steps.push(mutation),
                    StepPlan::Stalled(mutation) => {
                        steps.push(mutation);
                        pending = true;
                    }
                    StepPlan::Pending => pending = true,
                    StepPlan::Done => {}
                }
            }
            if bucket.target_holders.contains(&local_node_id) {
                match completion_step(context, realm_id, local_node_id, transition, &placement)
                    .await
                {
                    StepPlan::Ready(mutation) => steps.push(mutation),
                    StepPlan::Stalled(mutation) => {
                        // A stall is diagnostics: reduce it once and keep the
                        // retry armed for a later convergence.
                        steps.push(mutation);
                        pending = true;
                    }
                    StepPlan::Pending => pending = true,
                    StepPlan::Done => {}
                }
            }
        }
    }
    if !steps.is_empty() {
        pending |= submit_steps(context, realm_id, local_node_id, steps).await;
    }
    // Flush-then-leave (DECISIONS K3): a bucket this node just handed over
    // leaves it a member only until the grace elapses, so whatever it accepted
    // before the cutover has to reach the topic inside that window.
    if departed {
        crate::task_incoming::drive_document_sync_outbox_drain(context.clone()).await;
    }
    pending
}

/// An active plan none of whose incomplete buckets can ever apply: every one
/// was derived from an activation epoch the bucket has since left.
fn transition_stale(config: &RealmConfigDocument, transition: &PlacementTransition) -> bool {
    if !matches!(transition.status, TransitionStatus::Active) {
        return false;
    }
    let mut incomplete = 0usize;
    let mut stale = 0usize;
    for bucket in &transition.plan.buckets {
        if transition.completion(bucket.bucket).is_some() {
            continue;
        }
        incomplete += 1;
        if config
            .activation(&transition.plan.strategy_id, bucket.bucket)
            .is_some_and(|activation| activation.activation_epoch != bucket.predecessor_epoch)
        {
            stale += 1;
        }
    }
    incomplete > 0 && stale == incomplete
}

/// Only a management node's abort passes inbound admission; everyone else
/// keeps watching until one issues it.
fn is_management(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let node_id = node_id.to_string();
    config.nodes.iter().any(|node| {
        node.node_id == node_id
            && matches!(node.kind, aruna_core::structs::RealmNodeKind::Management)
    })
}

/// Reports the bucket drained once no outbox record for its placement
/// remains; until then the pass stays pending and keeps driving the drain.
async fn drain_step(
    context: &Arc<DriverContext>,
    transition: &PlacementTransition,
    placement: &PlacementRef,
    local_node_id: NodeId,
) -> StepPlan {
    for prefix in crate::document_sync_outbox::outbox_stream_prefixes() {
        let mut start_after: Option<Vec<u8>> = None;
        loop {
            let batch = match crate::document_sync_outbox::read_outbox_records(
                &context.storage_handle,
                prefix,
                start_after.take(),
                256,
            )
            .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    debug!(error = %error, "Transition drain scan failed");
                    return StepPlan::Pending;
                }
            };
            if batch
                .records
                .iter()
                .any(|(_, record)| record.placement == *placement)
            {
                return StepPlan::Pending;
            }
            if !batch.has_more {
                break;
            }
            start_after = batch.next_start_after;
        }
    }
    StepPlan::Ready(RealmPlacementMutation::ReportDrained {
        transition_id: transition.plan.transition_id,
        bucket: placement.shard,
        reported_by: local_node_id,
    })
}

/// One bucket step this node owes: a mutation ready to batch, work still
/// blocked on a precondition, a stall report worth reducing while the retry
/// stays armed, or nothing left to do.
enum StepPlan {
    Ready(RealmPlacementMutation),
    Stalled(RealmPlacementMutation),
    Pending,
    Done,
}

/// Freezes this old holder's frontier for the bucket, once.
async fn barrier_step(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    transition: &PlacementTransition,
    placement: &PlacementRef,
) -> StepPlan {
    if transition
        .barriers
        .iter()
        .any(|barrier| barrier.bucket == placement.shard && barrier.reported_by == local_node_id)
    {
        return StepPlan::Done;
    }
    let frontier = match assemble_shard_manifest(context, realm_id, *placement).await {
        Ok(manifest) => manifest.cursor,
        Err(error) => {
            debug!(error = %error, "Cannot assemble a transition barrier frontier yet");
            return StepPlan::Pending;
        }
    };
    StepPlan::Ready(RealmPlacementMutation::ReportBarrier {
        transition_id: transition.plan.transition_id,
        bucket: placement.shard,
        reported_by: local_node_id,
        frontier,
    })
}

/// Joins, pulls, verifies, and proves the bucket from the target side.
async fn completion_step(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    transition: &PlacementTransition,
    placement: &PlacementRef,
) -> StepPlan {
    if transition
        .proofs_for(placement.shard)
        .any(|proof| proof.holder == local_node_id)
    {
        return StepPlan::Done;
    }
    let Some(bucket) = transition.plan.bucket_plan(placement.shard) else {
        return StepPlan::Done;
    };
    // Until every old holder has fenced there is no reference frontier, so a
    // proof would attest to a moving target.
    if !transition.barrier_established(placement.shard, &bucket.old_holders) {
        return StepPlan::Pending;
    }
    let Some(net_handle) = context.net_handle.clone() else {
        return StepPlan::Pending;
    };
    let sources: Vec<NodeId> = bucket
        .old_holders
        .iter()
        .copied()
        .filter(|holder| *holder != local_node_id)
        .collect();

    let topic = shard_topic_id(realm_id, placement);
    if !sources.is_empty()
        && !net_handle
            .document_sync_topic_exists(topic)
            .unwrap_or(false)
    {
        // Join-only: this adopts an existing genesis and can never mint one.
        let event = net_handle
            .sync_document_topics(vec![topic], sources.clone())
            .await;
        crate::startup::apply_restored_reconcile(context, local_node_id, event).await;
    }

    let checkpoint_root = if sources.is_empty() {
        // No source to verify against: the bucket had no holder to lose history
        // to, so the local (empty) manifest is the root.
        match assemble_shard_manifest(context, realm_id, *placement).await {
            Ok(manifest) => manifest.digest,
            Err(error) => {
                debug!(error = %error, "Cannot assemble a source-less transition checkpoint");
                return StepPlan::Pending;
            }
        }
    } else {
        // The proof must cover every old holder's fenced writes, so the local
        // cursor has to dominate the join of all reported frontiers even when
        // some holders are unreachable for pulling (F5).
        let mut required = irokle::ActorClock::default();
        for barrier in transition
            .barriers
            .iter()
            .filter(|barrier| barrier.bucket == placement.shard)
            .filter(|barrier| bucket.old_holders.contains(&barrier.reported_by))
        {
            let Ok(frontier) = postcard::from_bytes::<irokle::ActorClock>(&barrier.frontier) else {
                debug!(
                    reported_by = %barrier.reported_by,
                    "Undecodable barrier frontier; leaving the bucket pending"
                );
                return StepPlan::Pending;
            };
            required.merge(&frontier);
        }
        match converge_with_barrier(
            context,
            &net_handle,
            local_node_id,
            realm_id,
            *placement,
            &sources,
            Some(&required),
        )
        .await
        {
            Some((_, digest)) => digest,
            None => {
                debug!(
                    strategy = %placement.strategy_id,
                    shard = placement.shard,
                    "No old holder served a verifiable shard copy yet"
                );
                // Surface the bounded failure exactly once per reporter; the
                // reduced record keeps the report idempotent across retries.
                if !transition.stalls.iter().any(|stall| {
                    stall.bucket == placement.shard && stall.reported_by == local_node_id
                }) {
                    return StepPlan::Stalled(RealmPlacementMutation::ReportStall {
                        transition_id: transition.plan.transition_id,
                        bucket: placement.shard,
                        reported_by: local_node_id,
                        reason: "no old holder served a verifiable copy".to_string(),
                    });
                }
                return StepPlan::Pending;
            }
        }
    };

    let Some(activation) = config_activation(context, realm_id, placement).await else {
        return StepPlan::Pending;
    };
    // Never sign a tuple the reducer will ignore: the stored activation must
    // still be the epoch this plan moves from.
    if activation != bucket.predecessor_epoch {
        return StepPlan::Pending;
    }
    let claim = ProofClaim {
        realm_id,
        transition_id: transition.plan.transition_id,
        strategy_id: transition.plan.strategy_id,
        bucket: placement.shard,
        old_activation_epoch: activation,
        target_map_epoch: transition.plan.target_map_epoch,
        barrier_digest: transition.barrier_digest(placement.shard),
        checkpoint_root,
        holder: local_node_id,
    };
    let proof = claim.signed_with(|message| net_handle.sign(message));
    StepPlan::Ready(RealmPlacementMutation::SubmitCompletion {
        transition_id: transition.plan.transition_id,
        strategy_id: transition.plan.strategy_id,
        proof,
    })
}

/// Commits one pass's gathered steps in a single transaction.
async fn submit_steps(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    steps: Vec<RealmPlacementMutation>,
) -> bool {
    let actor = Actor {
        node_id: local_node_id,
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    match drive(MutateRealmPlacementOperation::batch(actor, steps), context).await {
        Ok(_) => false,
        Err(error) => {
            warn!(error = %error, "Placement transition steps did not apply");
            true
        }
    }
}

/// Starts the successor expansion for a strategy whose activations trail the
/// newest map once its active transition is terminal (F10): a join during an
/// active expansion publishes the newer map, and this picks it up. Only the
/// strategy's rank-0 node under the newest map issues the plan; everyone else
/// reports pending so their timer keeps watching.
async fn ensure_expansions(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
) -> bool {
    let Some(epoch) = config.newest_map_epoch() else {
        return false;
    };
    let Some(map) = config.candidate_map(epoch) else {
        return false;
    };
    let mut pending = false;
    for strategy in &config.strategies {
        if config.placement_transitions.iter().any(|transition| {
            transition.plan.strategy_id == strategy.strategy_id && !transition.is_terminal()
        }) {
            continue;
        }
        let Ok(buckets) = expansion_buckets(config, strategy.strategy_id, epoch) else {
            continue;
        };
        if buckets.is_empty() {
            continue;
        }
        pending = true;
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: 0,
        };
        if holders_in_map(config, strategy, &placement, map)
            .is_none_or(|holders| holders.first() != Some(&local_node_id))
        {
            continue;
        }
        let transition_id = ulid::Ulid::generate();
        match plan_transition(
            config,
            TransitionRequest {
                transition_id,
                strategy_id: strategy.strategy_id,
                buckets,
                target_map_epoch: epoch,
                // Expansion moves nothing off a holder; every bucket may run.
                limits: aruna_core::structs::TransitionLimits {
                    max_incomplete_buckets: u32::MAX,
                    ..Default::default()
                },
                created_by: local_node_id,
                created_at_ms: aruna_core::util::unix_timestamp_millis(),
            },
        ) {
            Ok(plan) => {
                pending |= submit_mutation(
                    context,
                    realm_id,
                    local_node_id,
                    RealmPlacementMutation::StartTransition(plan),
                )
                .await;
            }
            Err(error) => {
                debug!(error = %error, "Successor expansion could not plan yet");
            }
        }
    }
    pending
}

/// Activates the newest map for a strategy that has none, so a strategy that
/// becomes referenced after the realm's first map never resolves nothing.
/// Only the strategy's rank-0 node under that map issues it; the record is an
/// immutable value, so a concurrent duplicate coalesces.
async fn ensure_strategy_activations(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
) -> bool {
    let Some(epoch) = config.newest_map_epoch() else {
        return false;
    };
    let Some(map) = config.candidate_map(epoch) else {
        return false;
    };
    let mut pending = false;
    for strategy in &config.strategies {
        if config.activation(&strategy.strategy_id, 0).is_some() {
            continue;
        }
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: 0,
        };
        // A map without this strategy's selector cannot activate it; a newer
        // map publication is what resolves that, so just stay pending.
        if holders_in_map(config, strategy, &placement, map)
            .is_none_or(|holders| holders.first() != Some(&local_node_id))
        {
            pending = true;
            continue;
        }
        pending |= submit_mutation(
            context,
            realm_id,
            local_node_id,
            RealmPlacementMutation::InitializeActivations {
                strategy_id: strategy.strategy_id,
                candidate_map_epoch: epoch,
            },
        )
        .await;
    }
    pending
}

/// The bucket's current activation epoch, read back from storage so the proof
/// commits to the epoch the reducer will compare it against.
async fn config_activation(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    placement: &PlacementRef,
) -> Option<u64> {
    let config = crate::process_placements::load_realm_config(context, realm_id).await?;
    config
        .activation(&placement.strategy_id, placement.shard)
        .map(|activation| activation.activation_epoch)
}

/// Drives one placement mutation and reports whether it needs another pass.
async fn submit_mutation(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    mutation: RealmPlacementMutation,
) -> bool {
    let config = MutateRealmPlacementConfig {
        actor: Actor {
            node_id: local_node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        },
        mutation,
    };
    match drive(MutateRealmPlacementOperation::new(config), context).await {
        Ok(_) => false,
        Err(error) => {
            warn!(error = %error, "Placement transition step did not apply");
            true
        }
    }
}
