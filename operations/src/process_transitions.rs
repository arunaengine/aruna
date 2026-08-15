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
    is_management,
};
use crate::placement::fence;
use crate::placement::transition::{
    TransitionRequest, expansion_buckets, holders_in_map, plan_transition,
};
use crate::shard::{assemble_shard_manifest, frontier_root};
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
                    match drain_step(context, realm_id, transition, &placement, local_node_id).await
                    {
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

/// Closes the bucket's write fence, then reports it drained once no journalled
/// eviction and no record of a closed generation remains. The close comes first
/// and is durable: without it an empty scan says nothing about a write that
/// resolved the bucket before the cutover and commits its row afterwards.
async fn drain_step(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    transition: &PlacementTransition,
    placement: &PlacementRef,
    local_node_id: NodeId,
) -> StepPlan {
    let Some(bucket) = transition.plan.bucket_plan(placement.shard) else {
        return StepPlan::Done;
    };
    let closed = bucket.predecessor_epoch;
    if let Err(error) = fence::close(&context.storage_handle, &realm_id, placement, closed).await {
        debug!(error = %error, "Transition drain could not close the write fence");
        return StepPlan::Pending;
    }
    if let Some(net_handle) = context.net_handle.as_ref() {
        match net_handle.seal_sync_topic(shard_topic_id(realm_id, placement)) {
            Ok(true) => return StepPlan::Pending,
            Ok(false) => {}
            Err(error) => {
                debug!(error = %error, "Transition drain could not seal the shard topic");
                return StepPlan::Pending;
            }
        }
    }
    if let Some(blocker) = drain_blocker(context, placement, closed).await {
        debug!(
            ?blocker,
            strategy_id = %placement.strategy_id,
            bucket = placement.shard,
            "Transition drain cannot report the bucket yet"
        );
        return StepPlan::Pending;
    }
    StepPlan::Ready(RealmPlacementMutation::ReportDrained {
        transition_id: transition.plan.transition_id,
        bucket: placement.shard,
        reported_by: local_node_id,
    })
}

/// Why a bucket is not drained yet. Named so the order of the checks is
/// testable: an eviction converted between them enqueues replacement rows a
/// finished scan has already passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainBlocker {
    Eviction,
    OutboxRow,
    ScanFailed,
}

/// The first reason `placement` cannot be reported drained through `closed`.
/// The eviction journal comes first: a scan that already passed a stream proves
/// nothing about rows the eviction hand-off commits afterwards.
async fn drain_blocker(
    context: &Arc<DriverContext>,
    placement: &PlacementRef,
    closed: u64,
) -> Option<DrainBlocker> {
    // No net handle means no eviction source, so nothing can be journalled.
    if context
        .net_handle
        .as_ref()
        .is_some_and(|net_handle| net_handle.eviction_pending(placement))
    {
        return Some(DrainBlocker::Eviction);
    }
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
                    return Some(DrainBlocker::ScanFailed);
                }
            };
            // An unfenced row carries generation zero and always counts.
            if batch
                .records
                .iter()
                .any(|(_, record)| record.placement == *placement && record.generation <= closed)
            {
                return Some(DrainBlocker::OutboxRow);
            }
            if !batch.has_more {
                break;
            }
            start_after = batch.next_start_after;
        }
    }
    None
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
        .any(|proof| {
            proof.holder == local_node_id && transition.proof_valid(placement.shard, proof)
        })
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

    let checkpoint_root = if sources.is_empty() {
        // A source-less bucket still binds its fenced frontier and topic genesis,
        // not its moving live fingerprint.
        match frontier_root(&net_handle, topic, &required) {
            Ok(root) => root,
            Err(error) => {
                debug!(error = %error, "Cannot assemble a source-less transition checkpoint");
                return StepPlan::Pending;
            }
        }
    } else {
        // The proof must cover every old holder's fenced writes, so the local
        // cursor has to dominate the join of all reported frontiers even when
        // some holders are unreachable for pulling (F5).
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
            Some((_, _)) => match frontier_root(&net_handle, topic, &required) {
                Ok(root) => root,
                Err(error) => {
                    debug!(error = %error, "Cannot hash the transition checkpoint frontier");
                    return StepPlan::Pending;
                }
            },
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
    // Management nodes race and the predecessor gate coalesces the winner; a
    // non-management issuer would only ever be rejected by its peers.
    if !is_management(config, local_node_id) {
        return false;
    }
    let mut pending = false;
    for strategy in &config.strategies {
        if config.placement_transitions.iter().any(|transition| {
            transition.plan.strategy_id == strategy.strategy_id && !transition.is_terminal()
        }) {
            continue;
        }
        // Successors only: the first expansion is onboarding's to issue, so a
        // record for the strategy must already exist. A lagging node that has
        // the newest map but not yet the started record can then never
        // double-issue inside the publish-to-start window.
        if !config
            .placement_transitions
            .iter()
            .any(|transition| transition.plan.strategy_id == strategy.strategy_id)
        {
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
        // A map that cannot resolve this strategy's holders cannot plan it; a
        // newer publication resolves that, so just stay pending.
        if holders_in_map(config, strategy, &placement, map).is_none() {
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

/// Activates the newest map for a strategy that has none, publishing the
/// successor map first when that map predates the strategy. Only a management
/// node issues either; the activation record is an immutable value, so a
/// concurrent duplicate coalesces.
async fn ensure_strategy_activations(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
) -> bool {
    let (mut pending, unfrozen) =
        activate_newest_map(context, realm_id, local_node_id, config).await;
    if !unfrozen {
        return pending;
    }
    pending = true;
    let epoch = config.newest_map_epoch().unwrap_or_default();
    if !publish_successor_map(context, realm_id, local_node_id, config, epoch).await {
        return pending;
    }
    // The successor map is durable in the local config the moment it commits,
    // so the activations it unblocks resolve in this pass. Deferring them to
    // the retry interval leaves the node owing work after it reports quiet.
    if let Some(published) = crate::process_placements::load_realm_config(context, realm_id).await {
        activate_newest_map(context, realm_id, local_node_id, &published).await;
    }
    pending
}

/// Initializes activations against `config`'s newest map. Returns whether work
/// is still outstanding and whether some strategy is missing from that map,
/// which only a successor map can freeze.
async fn activate_newest_map(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
) -> (bool, bool) {
    let Some(epoch) = config.newest_map_epoch() else {
        return (false, false);
    };
    let Some(map) = config.candidate_map(epoch) else {
        return (false, false);
    };
    // The activation record is an immutable value, so concurrent management
    // issuers coalesce; a non-management issuer never lands at all.
    if !is_management(config, local_node_id) {
        return (false, false);
    }
    let mut pending = false;
    let mut unfrozen = false;
    for strategy in &config.strategies {
        if config.activation(&strategy.strategy_id, 0).is_some() {
            continue;
        }
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: 0,
        };
        // A map without this strategy's selector cannot activate it; the
        // successor map published by the caller is what freezes it.
        if holders_in_map(config, strategy, &placement, map).is_none() {
            pending = true;
            unfrozen |= map.selector(&strategy.strategy_id).is_none();
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
    (pending, unfrozen)
}

/// Freezes a strategy the newest map predates into its successor, so a
/// strategy created after that map cannot stay unresolvable forever. Returns
/// whether this node published one.
///
/// The epoch is published by one deterministic issuer and derived
/// byte-identically from the config, so a second issuer's copy coalesces in
/// the reducer instead of leaving the epoch conflicted and unusable. A stale
/// caller view cannot stream epochs: admission rejects an occupied epoch.
async fn publish_successor_map(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    config: &RealmConfigDocument,
    epoch: u64,
) -> bool {
    if map_publisher(config) != Some(local_node_id.to_string().as_str()) {
        return false;
    }
    !submit_mutation(
        context,
        realm_id,
        local_node_id,
        RealmPlacementMutation::PublishCandidateMap(config.freeze_map(epoch + 1)),
    )
    .await
}

/// The realm's candidate-map issuer: the lowest-id Management node. Publishing
/// from every Management node at once risks two divergent values at one epoch,
/// which keeps that epoch permanently unusable. A removed issuer hands the
/// role to the next node; an unreachable one defers publication.
fn map_publisher(config: &RealmConfigDocument) -> Option<&str> {
    config
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, aruna_core::structs::RealmNodeKind::Management))
        .map(|node| node.node_id.as_str())
        .min()
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

#[cfg(test)]
mod tests {
    use aruna_core::admin_document_reducer::{
        AdminDocumentReducerState, overlay_realm_config_placement_reducer_materialization,
    };
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use std::path::Path;

    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::{PlacementStrategy, RealmNodeKind};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use tempfile::tempdir;
    use ulid::Ulid;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId, node_id: NodeId) -> Actor {
        Actor {
            node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        }
    }

    fn context(root: &str) -> Arc<DriverContext> {
        Arc::new(DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        })
    }

    /// A drain consults the eviction journal through the net handle, so a
    /// context without one never exercises that gate.
    async fn net_context(root: &Path) -> (Arc<DriverContext>, NetHandle) {
        let storage_handle =
            aruna_storage::FjallStorage::open(root.join("storage").to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                document_sync_storage_path: Some(root.join("document-sync")),
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        (context, net_handle)
    }

    /// One journalled eviction whose re-emitted row would land on `placement`,
    /// authored locally so the decoder keeps it.
    fn local_eviction(net_handle: &NetHandle, placement: PlacementRef) -> irokle::TopicEviction {
        let event_id = Ulid::from_bytes([44; 16]);
        let event = aruna_core::document::DocumentSyncEvent::Delete {
            event_id,
            target: DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: event_id,
            },
            change: aruna_core::document::DocumentSyncChange {
                base: None,
                current: aruna_core::document::DocumentSyncRevision {
                    generation: 0,
                    event_id,
                    actor: node(1),
                    updated_at_ms: 1,
                },
                kind: aruna_core::document::DocumentSyncChangeKind::Delete,
                placement,
            },
        };
        let topic_id = irokle::TopicId::from_bytes([44; 32]);
        let author = net_handle.document_sync_node().peer_id();
        irokle::TopicEviction {
            topic_id,
            losing_genesis: irokle::OpId::from_bytes([45; 32]),
            winning_genesis: irokle::OpId::from_bytes([46; 32]),
            evicted: vec![irokle::EvictedOp {
                op_id: irokle::OpId::from_bytes([47; 32]),
                actor_id: irokle::actor_id_for(topic_id, author),
                author,
                actor_seq: 2,
                payload: irokle::TopicPayload::Event(
                    irokle::EventEnvelope::encode_event(&event).expect("the event encodes"),
                ),
            }],
        }
    }

    fn strategy(seed: u8, name: &str) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id: Ulid::from_bytes([seed; 16]),
            name: name.to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 4,
        }
    }

    /// Map epoch one freezes the first strategy only; the second is created
    /// after it, so nothing in the realm carries its frozen selector.
    fn late_strategy_config(realm_id: RealmId) -> (RealmConfigDocument, Ulid) {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        for seed in [1u8, 2] {
            document.ensure_node(node(seed), RealmNodeKind::Management);
        }
        document.strategies.push(strategy(5, "first"));
        document.default_strategy_id = Some(document.strategies[0].strategy_id);
        document.snapshot_candidate_map();
        let late = strategy(6, "late");
        document.strategies.push(late.clone());
        (document, late.strategy_id)
    }

    async fn store_config(context: &DriverContext, document: &RealmConfigDocument) {
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: document.realm_id,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document
                    .to_bytes(&actor(document.realm_id, node(1)))
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn load_config(context: &Arc<DriverContext>, realm_id: RealmId) -> RealmConfigDocument {
        crate::process_placements::load_realm_config(context, realm_id)
            .await
            .expect("the realm config is stored")
    }

    #[tokio::test]
    async fn late_strategy_gets_map() {
        // One pass publishes the successor map and activates from it: leaving
        // the activation to the retry interval strands the work for a restart.
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([61; 32]);
        let (document, late) = late_strategy_config(realm_id);
        store_config(&context, &document).await;
        assert!(
            document
                .candidate_map(1)
                .expect("epoch one is usable")
                .selector(&late)
                .is_none()
        );

        let issuer = map_publisher(&document)
            .expect("a management issuer")
            .to_string();
        let publisher = [node(1), node(2)]
            .into_iter()
            .find(|candidate| candidate.to_string() == issuer)
            .expect("the issuer is a configured node");
        assert!(process_placement_transitions(&context, realm_id, publisher, &document).await);

        // One successor map, carrying the missing frozen selector, plus every
        // activation it unblocks.
        let published = load_config(&context, realm_id).await;
        assert_eq!(published.newest_map_epoch(), Some(2));
        assert!(
            published
                .candidate_map(2)
                .expect("the successor is usable")
                .selector(&late)
                .is_some()
        );
        for shard in 0..4 {
            assert_eq!(
                published
                    .activation(&late, shard)
                    .map(|activation| activation.candidate_map_epoch),
                Some(2),
                "the publishing pass must activate from its own successor map"
            );
        }

        // Repeating the pass publishes no further epoch.
        process_placement_transitions(&context, realm_id, publisher, &published).await;
        assert_eq!(
            load_config(&context, realm_id).await.newest_map_epoch(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn one_issuer_publishes() {
        // Every Management node reconciles, but only the deterministic issuer
        // may publish: a rival value at one epoch makes it unusable forever.
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([62; 32]);
        let (document, _late) = late_strategy_config(realm_id);
        store_config(&context, &document).await;

        let issuer = map_publisher(&document)
            .expect("a management issuer")
            .to_string();
        let other = [node(1), node(2)]
            .into_iter()
            .find(|candidate| candidate.to_string() != issuer)
            .expect("two management nodes");

        assert!(process_placement_transitions(&context, realm_id, other, &document).await);
        assert_eq!(
            load_config(&context, realm_id).await.newest_map_epoch(),
            Some(1)
        );
    }

    /// Bucket zero handed from the local node to a peer, cut over and past its
    /// grace, so the departing holder owes a drain report.
    fn departing_config(realm_id: RealmId) -> (RealmConfigDocument, PlacementRef) {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(node(1), RealmNodeKind::Server);
        document.ensure_node(node(2), RealmNodeKind::Server);
        document.strategies.push(strategy(5, "first"));
        document.default_strategy_id = Some(document.strategies[0].strategy_id);
        document.snapshot_candidate_map();
        let strategy_id = document.strategies[0].strategy_id;
        let placement = PlacementRef {
            strategy_id,
            shard: 0,
        };
        let mut transition = PlacementTransition::new(aruna_core::structs::TransitionPlan {
            transition_id: Ulid::from_bytes([8; 16]),
            strategy_id,
            buckets: vec![aruna_core::structs::BucketPlan {
                bucket: 0,
                old_holders: vec![node(1)],
                target_holders: vec![node(2)],
                predecessor_epoch: 1,
            }],
            target_map_epoch: 1,
            limits: aruna_core::structs::TransitionLimits {
                max_incomplete_buckets: 1,
                grace_ms: 0,
            },
            created_by: node(2),
            created_at_ms: 1,
        });
        transition
            .completed
            .push(aruna_core::structs::BucketCompletion {
                bucket: 0,
                completed_at_ms: 1,
            });
        document.placement_transitions.push(transition);
        (document, placement)
    }

    async fn write_outbox_row(
        context: &DriverContext,
        placement: PlacementRef,
        generation: u64,
        seed: u8,
    ) -> Vec<u8> {
        let change = aruna_core::document::DocumentSyncChange {
            base: None,
            current: aruna_core::document::DocumentSyncRevision {
                generation,
                event_id: Ulid::from_bytes([seed; 16]),
                actor: node(1),
                updated_at_ms: 1,
            },
            kind: aruna_core::document::DocumentSyncChangeKind::Delete,
            placement,
        };
        let record = crate::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_bytes([seed; 16]),
            node(1),
            DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: Ulid::from_bytes([seed; 16]),
            },
            vec![node(2)],
            aruna_core::document::DocumentSyncOutboxEvent::Delete { change },
            placement,
            false,
        )
        .fenced_at(generation);
        let (key_space, key, value) =
            crate::document_sync_outbox::outbox_write_entry(&record).expect("the row encodes");
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key: key.clone(),
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        key.to_vec()
    }

    #[tokio::test]
    async fn drain_fences_then_reports() {
        // The close comes first, a predecessor-generation row blocks the
        // report, and a successor-generation row never does.
        let directory = tempdir().unwrap();
        let (context, _net_handle) = net_context(directory.path()).await;
        let realm_id = RealmId::from_bytes([64; 32]);
        let (document, placement) = departing_config(realm_id);
        let transition = &document.placement_transitions[0];
        let predecessor = write_outbox_row(&context, placement, 1, 21).await;
        write_outbox_row(&context, placement, 2, 22).await;

        let blocked = drain_step(&context, realm_id, transition, &placement, node(1)).await;
        assert!(
            matches!(blocked, StepPlan::Pending),
            "an undrained predecessor row blocks the report"
        );
        assert_eq!(closed_generation(&context, realm_id, &placement).await, 1);

        crate::document_sync_outbox::delete_outbox_records(
            &context.storage_handle,
            vec![predecessor],
        )
        .await
        .expect("the row is deleted");
        let reported = drain_step(&context, realm_id, transition, &placement, node(1)).await;
        assert!(
            matches!(
                reported,
                StepPlan::Ready(RealmPlacementMutation::ReportDrained { bucket: 0, .. })
            ),
            "a successor-generation row must not block the report"
        );
    }

    #[tokio::test]
    async fn eviction_blocks_first() {
        // The eviction hand-off commits its rows before it releases the entry,
        // so a scan that ran first would pass a stream the hand-off then fills.
        let directory = tempdir().unwrap();
        let (context, net_handle) = net_context(directory.path()).await;
        let realm_id = RealmId::from_bytes([66; 32]);
        let (document, placement) = departing_config(realm_id);
        store_config(&context, &document).await;
        write_outbox_row(&context, placement, 1, 23).await;
        net_handle
            .consume_eviction(local_eviction(&net_handle, placement))
            .await
            .expect("the eviction is journalled");

        assert_eq!(
            drain_blocker(&context, &placement, 1).await,
            Some(DrainBlocker::Eviction),
            "the journal must be consulted before the outbox scan"
        );
    }

    #[tokio::test]
    async fn eviction_scopes_bucket() {
        // A journalled eviction for another bucket must not stall this one.
        let directory = tempdir().unwrap();
        let (context, net_handle) = net_context(directory.path()).await;
        let realm_id = RealmId::from_bytes([67; 32]);
        let (document, placement) = departing_config(realm_id);
        store_config(&context, &document).await;
        let transition = &document.placement_transitions[0];
        let elsewhere = PlacementRef {
            strategy_id: placement.strategy_id,
            shard: 2,
        };
        net_handle
            .consume_eviction(local_eviction(&net_handle, elsewhere))
            .await
            .expect("the eviction is journalled");

        assert!(net_handle.eviction_pending(&elsewhere));
        let reported = drain_step(&context, realm_id, transition, &placement, node(1)).await;
        assert!(
            matches!(
                reported,
                StepPlan::Ready(RealmPlacementMutation::ReportDrained { bucket: 0, .. })
            ),
            "an unrelated bucket's eviction must not block the report"
        );
    }

    #[tokio::test]
    async fn dead_holder_stays_retained() {
        // Removing a dead holder from the realm must never stand in for its
        // drain report: the record keeps retaining it.
        let realm_id = RealmId::from_bytes([65; 32]);
        let (mut document, _placement) = departing_config(realm_id);
        document
            .nodes
            .retain(|entry| entry.node_id != node(1).to_string());
        document
            .placement_map
            .retain(|entry| entry.node_id != node(1));

        assert!(!document.placement_transitions[0].released(u64::MAX));
        assert!(
            crate::placement::retained_departing_holder(&document, &_placement, node(1)),
            "a removed holder is still a retained publisher until it reports"
        );
    }

    async fn closed_generation(
        context: &Arc<DriverContext>,
        realm_id: RealmId,
        placement: &PlacementRef,
    ) -> u64 {
        let (key_space, key) = fence::fence_read(&realm_id, placement);
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                fence::closed_generation(value.as_ref())
            }
            other => panic!("unexpected fence read: {other:?}"),
        }
    }

    #[test]
    fn concurrent_maps_coalesce() {
        // Two issuers racing at one epoch must reduce to one usable value, so
        // the published map is derived byte-identically from the config.
        let realm_id = RealmId::from_bytes([63; 32]);
        let (document, _late) = late_strategy_config(realm_id);
        let map = document.freeze_map(2);
        assert_eq!(map, document.freeze_map(2));

        let publish = |state: &mut AdminDocumentReducerState, seed: u8, origin: NodeId, map| {
            state
                .apply(&AdminDocumentEvent {
                    event_id: Ulid::from_bytes([seed; 16]),
                    target: AdminDocumentTarget::RealmConfig { realm_id },
                    origin_node_id: origin,
                    origin_seq: 1,
                    observed: AdminDocumentClock::default(),
                    actor: actor(realm_id, origin),
                    op: AdminDocumentOperation::RealmConfigCandidateMapPublished { map },
                })
                .expect("the publication applies");
        };
        let materialize = |state: &AdminDocumentReducerState| {
            let mut materialized = document.clone();
            overlay_realm_config_placement_reducer_materialization(&mut materialized, state, 0);
            materialized
        };

        let mut agreed =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
        publish(&mut agreed, 70, node(1), map.clone());
        publish(&mut agreed, 71, node(2), map.clone());
        assert_eq!(materialize(&agreed).candidate_map(2), Some(&map));

        // Divergent values at one epoch are exactly what a single issuer avoids.
        let mut divergent =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
        let mut rival = map.clone();
        rival.nodes.pop();
        publish(&mut divergent, 72, node(1), map);
        publish(&mut divergent, 73, node(2), rival);
        assert!(materialize(&divergent).candidate_map(2).is_none());
    }
}
