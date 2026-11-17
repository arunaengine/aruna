//! Runs node recovery after start and restores shard and shared topic subscriptions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::backoff::capped_backoff;
use aruna_core::document::{DocumentNetEvent, DocumentReconcileResult};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::{DriverContext, drive};
use crate::node::document_sync::apply_metadata;
use crate::node::recovery::{
    PhaseOutcome, RecoveryDecision, RecoveryError, RecoveryOutcome, RecoveryPass, RecoveryRotation,
    RecoveryStatus, apply_recovery_pass,
};
use crate::node::restore_plan::{
    RestoreKind, RestoreRange, RestoreShardSummary, RestoreUnit, RestoreUnitOutcome,
    ShardRestoreCursor, ShardRestorePass, bind_restore_plan, plan_shard_groups, restore_range,
    sync_eligible_node,
};
use crate::node::summary_refresh::RefreshUsageOperation;
use crate::notifications::watch::interest::refresh_target_interest;

/// Shortest wait between two bounded recovery passes while work remains local.
const RECOVERY_RETRY_BASE: Duration = Duration::from_millis(250);
/// Longest wait between passes once the remainder is blocked on absent peers.
const RECOVERY_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
pub struct RecoveryConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// Publish one full node-usage snapshot set on the first pass. Set only for
    /// an initial bootstrap or an actual usage-counter rebuild.
    pub publish_full_usage: bool,
}

/// Runs bounded remote recovery until it converges or shutdown cancels it.
/// Every peer error is retryable background state and never a process-start
/// failure; a panic is reported as degraded rather than as completion.
pub async fn run_recovery(
    context: Arc<DriverContext>,
    config: RecoveryConfig,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    let driver = std::panic::AssertUnwindSafe(recovery_loop(
        context,
        config,
        status.clone(),
        cancelled.clone(),
    ));
    if futures_util::FutureExt::catch_unwind(driver).await.is_err() {
        status.finish_pass(RecoveryOutcome::Failed, Some(RecoveryError::Panicked));
        tracing::error!(
            event = "startup.recovery.degraded",
            error_class = RecoveryError::Panicked.label(),
            "Remote recovery driver panicked; convergence now depends on durable retry timers"
        );
    }
}

async fn recovery_loop(
    context: Arc<DriverContext>,
    config: RecoveryConfig,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    info!(event = "startup.recovery.begin", "Remote recovery started");
    let started = std::time::Instant::now();
    let mut attempts = 0u32;
    let mut cursor = ShardRestoreCursor::default();
    let mut publish_usage = config.publish_full_usage;
    let mut reconcile_placements = true;
    let mut announce_presence = true;
    let mut rotation = RecoveryRotation::default();
    let mut recovery_baseline = None;
    let mut first_rotation = true;
    let mut invocations = 0u64;
    loop {
        if cancelled.is_cancelled() {
            return;
        }
        status.begin_pass();
        let pass = recovery_pass(
            &context,
            &config,
            &cancelled,
            &mut cursor,
            &mut publish_usage,
            &mut reconcile_placements,
            &mut announce_presence,
        )
        .await;
        invocations = invocations.saturating_add(1);
        if cancelled.is_cancelled() {
            return;
        }

        let decision = apply_recovery_pass(
            &mut rotation,
            &mut recovery_baseline,
            &mut first_rotation,
            &pass,
        );
        if finish_recovery_step(
            decision,
            &status,
            &started,
            invocations,
            &mut attempts,
            &cancelled,
        )
        .await
        {
            return;
        }
    }
}

async fn finish_recovery_step(
    decision: RecoveryDecision,
    status: &RecoveryStatus,
    started: &std::time::Instant,
    invocations: u64,
    attempts: &mut u32,
    cancelled: &CancellationToken,
) -> bool {
    match decision {
        RecoveryDecision::ContinueLocal {
            progress,
            topics_remaining,
            error,
        } => {
            if progress {
                status.note_progress(topics_remaining);
            } else {
                status.set_remaining(topics_remaining);
            }
            status.finish_pass(RecoveryOutcome::Partial, error);
            tokio::task::yield_now().await;
            false
        }
        RecoveryDecision::Converged => {
            status.note_progress(0);
            status.finish_pass(RecoveryOutcome::Success, None);
            info!(
                event = "startup.recovery.complete",
                elapsed_ms = started.elapsed().as_millis() as u64,
                passes = invocations,
                "Remote recovery converged"
            );
            true
        }
        RecoveryDecision::Retry {
            failure,
            progress,
            enumerated,
        } => {
            let topics_remaining = failure.unresolved_topics.len() as u64;
            if progress {
                status.note_progress(topics_remaining);
                *attempts = 0;
            } else if enumerated {
                status.set_remaining(topics_remaining);
            }
            status.finish_pass(RecoveryOutcome::Partial, failure.error);
            warn!(
                event = "startup.recovery.degraded",
                topics_remaining = failure.unresolved_topics.len(),
                error_class = failure.error.map(RecoveryError::label).unwrap_or("none"),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Remote recovery left retryable work behind"
            );
            let after = capped_backoff(RECOVERY_RETRY_BASE, RECOVERY_RETRY_MAX, *attempts, 0);
            *attempts = (*attempts).saturating_add(1);
            tokio::select! {
                _ = cancelled.cancelled() => true,
                _ = tokio::time::sleep(after) => false,
            }
        }
    }
}

async fn recovery_pass(
    context: &Arc<DriverContext>,
    config: &RecoveryConfig,
    cancelled: &CancellationToken,
    cursor: &mut ShardRestoreCursor,
    publish_usage: &mut bool,
    reconcile_placements: &mut bool,
    announce_presence: &mut bool,
) -> RecoveryPass {
    let mut pass = RecoveryPass::default();

    let restore =
        restore_shard_pass(context, config.node_id, config.realm_id, cursor, cancelled).await;
    pass.unresolved_topics = restore.unresolved_topics;
    pass.unvisited_topics = restore.unvisited_topics;
    pass.error = restore.error;
    pass.plan_changed = restore.plan_changed;
    pass.topics_completed = restore.topics_completed;
    pass.enumerated = restore.enumerated;
    if pass.plan_changed {
        *reconcile_placements = true;
    }
    // Convergence needs the cursor to have walked every unit.
    if !restore.wrapped {
        pass.more_local_work = true;
        return pass;
    }
    // Without an enumerated plan the pass observed nothing; skip the phases
    // and retry with backoff.
    if !pass.enumerated || cancelled.is_cancelled() {
        return pass;
    }

    pass.merge_phase(reconcile_phase(context, config, reconcile_placements).await);
    if cancelled.is_cancelled() {
        return pass;
    }
    pass.merge_phase(presence_phase(context, config, announce_presence).await);
    if cancelled.is_cancelled() {
        return pass;
    }
    pass.merge_phase(usage_phase(context, config, publish_usage).await);
    pass
}

#[derive(Debug)]
enum RealmConfigLoad {
    Found(RealmConfigDocument),
    Absent,
    StorageFailure,
}

async fn reconcile_phase(
    context: &Arc<DriverContext>,
    config: &RecoveryConfig,
    pending: &mut bool,
) -> PhaseOutcome {
    if !*pending {
        return PhaseOutcome::default();
    }
    // Placement transitions run on their timer so cluster-wide draining cannot block readiness.
    let placements = crate::placement::process_placements::reconcile_shard_topics(
        context,
        config.realm_id,
        config.node_id,
    )
    .await;
    let outcome = match placements.status {
        crate::placement::process_placements::PlacementReconcileStatus::Clean => {
            *pending = false;
            PhaseOutcome {
                progress: true,
                error: None,
            }
        }
        crate::placement::process_placements::PlacementReconcileStatus::RetryScheduled => {
            PhaseOutcome {
                progress: false,
                error: Some(RecoveryError::PeerUnavailable),
            }
        }
        crate::placement::process_placements::PlacementReconcileStatus::StorageFailure => {
            PhaseOutcome {
                progress: false,
                error: Some(RecoveryError::Storage),
            }
        }
    };
    info!(
        event = "startup.recovery.progress",
        phase = "placements",
        retry_scheduled = placements.retry_scheduled,
        pull_pending = placements.pull_pending,
        "Reconciled shard placements"
    );
    outcome
}

async fn presence_phase(
    context: &Arc<DriverContext>,
    config: &RecoveryConfig,
    pending: &mut bool,
) -> PhaseOutcome {
    if !*pending {
        return PhaseOutcome::default();
    }
    // A user device reads the DHT to locate realm nodes but never announces
    // itself as one; without a presence record nothing routes work to it.
    if let RealmConfigLoad::Found(realm_config) = load_realm_config(context, config.realm_id).await
        && !sync_eligible_node(&realm_config, config.node_id)
    {
        *pending = false;
        return PhaseOutcome {
            progress: true,
            error: None,
        };
    }
    let result = drive(
        crate::realm::announce_presence::AnnouncePresenceOperation::new(
            crate::realm::announce_presence::AnnouncePresenceConfig {
                realm_id: config.realm_id,
                node_id: config.node_id,
                schedule_refresh: true,
            },
        ),
        context.as_ref(),
    )
    .await;
    match result {
        Ok(_) => {
            *pending = false;
            PhaseOutcome {
                progress: true,
                error: None,
            }
        }
        Err(error) => {
            warn!(error = %error, "Failed to announce realm presence during recovery");
            PhaseOutcome {
                progress: false,
                error: Some(RecoveryError::PeerUnavailable),
            }
        }
    }
}

async fn usage_phase(
    context: &Arc<DriverContext>,
    config: &RecoveryConfig,
    pending: &mut bool,
) -> PhaseOutcome {
    if !*pending {
        return PhaseOutcome::default();
    }
    let result = crate::node::usage_publish::publish_refresh_snapshots(
        context.as_ref(),
        config.node_id,
        config.realm_id,
        true,
    )
    .await;
    match result {
        Ok(_) => {
            *pending = false;
            PhaseOutcome {
                progress: true,
                error: None,
            }
        }
        Err(error) => {
            warn!(error = %error, "Failed to publish initial node usage snapshots");
            PhaseOutcome {
                progress: false,
                error: Some(RecoveryError::Storage),
            }
        }
    }
}

/// Restarts the local node's document-sync subscriptions from held shards (plus
/// fixed shared realm topics) via batched anti-entropy passes, so restart costs
/// O(held shards), not O(stored documents). Returns the small topic counts.
pub async fn restore_shard_subscriptions(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
) -> RestoreShardSummary {
    let mut cursor = ShardRestoreCursor::default();
    let mut summary = RestoreShardSummary::default();
    let cancelled = CancellationToken::new();
    // Run bounded passes until the cursor wraps, so callers that want the whole
    // restore in one call still get it.
    loop {
        let pass = restore_shard_pass(context, node_id, realm_id, &mut cursor, &cancelled).await;
        summary.held_shards = pass.summary.held_shards;
        summary.shard_topics = pass.summary.shard_topics;
        summary.shared_topics = pass.summary.shared_topics;
        summary.withheld_topics += pass.summary.withheld_topics;
        if pass.wrapped {
            return summary;
        }
    }
}

/// Runs at most [`RESTORE_UNIT_BUDGET`] restore work units from `cursor`,
/// advances it past them, and wraps at the end, so an unavailable first group
/// cannot starve a later healthy one.
pub async fn restore_shard_pass(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
    cursor: &mut ShardRestoreCursor,
    cancelled: &CancellationToken,
) -> ShardRestorePass {
    let config = match load_realm_config(context, realm_id).await {
        RealmConfigLoad::Found(config) => config,
        RealmConfigLoad::Absent => {
            // No config yet (fresh/onboarding node): nothing sharded to restore.
            *cursor = ShardRestoreCursor::default();
            return ShardRestorePass {
                wrapped: true,
                enumerated: true,
                ..ShardRestorePass::default()
            };
        }
        RealmConfigLoad::StorageFailure => {
            // Keep the cursor: a failed config read says nothing about the
            // plan, and the next successful load re-binds by hash anyway.
            return ShardRestorePass {
                error: Some(RecoveryError::Storage),
                wrapped: true,
                ..ShardRestorePass::default()
            };
        }
    };
    let Some(net_handle) = context.net_handle.clone() else {
        *cursor = ShardRestoreCursor::default();
        return ShardRestorePass {
            wrapped: true,
            enumerated: true,
            ..ShardRestorePass::default()
        };
    };
    let plan = plan_shard_groups(&config, node_id, realm_id, unix_timestamp_millis());
    let mut summary = plan.summary;
    let units = plan.into_units();
    let units_total = units.len();
    let plan_changed = bind_restore_plan(cursor, &units);
    if units_total == 0 {
        cursor.next_unit = 0;
        return ShardRestorePass {
            summary,
            units_processed: 0,
            units_total: 0,
            plan_changed,
            wrapped: true,
            enumerated: true,
            ..ShardRestorePass::default()
        };
    }

    // Former-holder history cutoffs are frozen only for shards a prior run
    // durably verified; join verification below happens after this restore.
    let verified = crate::shard::verify::load_verified_topics(context, realm_id).await;

    let range = restore_range(*cursor, units_total);
    let start = range.start;
    let batch = restore_batch(
        context,
        &net_handle,
        node_id,
        &units,
        range,
        &verified,
        cancelled,
    )
    .await;
    summary.withheld_topics = batch.unresolved_topics.len();
    cursor.next_unit = if batch.wrapped { 0 } else { batch.end };
    let unvisited_topics = units[batch.end..]
        .iter()
        .map(|unit| unit.topics.len())
        .sum();
    let unresolved_topics = batch.unresolved_topics;

    // A withheld genesis has no placement record to trigger another pass.
    // Arm the retry so a returning co-holder unblocks writes without restart.
    if !unresolved_topics.is_empty()
        && let Some(task_handle) = context.task_handle.as_ref()
    {
        let effect = crate::sync::shard_placement::schedule_retry_effect(realm_id, node_id);
        let _ = task_handle.send_effect(effect).await;
    }

    if batch.wrapped {
        // New-holder verification: reconcile each held shard against a co-holder
        // and persist a marker so a restart resumes only unverified shards.
        crate::shard::verify::verify_held_shards(context, node_id, realm_id).await;
    }

    ShardRestorePass {
        summary,
        units_processed: batch.end - start,
        topics_completed: batch.topics_completed,
        units_total,
        unvisited_topics,
        unresolved_topics,
        error: batch.error,
        plan_changed,
        wrapped: batch.wrapped,
        enumerated: true,
    }
}

#[derive(Default)]
struct RestoreBatch {
    end: usize,
    wrapped: bool,
    topics_completed: usize,
    unresolved_topics: BTreeSet<::irokle::TopicId>,
    error: Option<RecoveryError>,
}

async fn restore_batch(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    units: &[RestoreUnit],
    range: RestoreRange,
    verified: &BTreeSet<::irokle::TopicId>,
    cancelled: &CancellationToken,
) -> RestoreBatch {
    let RestoreRange {
        start,
        end,
        wrapped,
    } = range;
    let mut batch_end = end;
    let mut batch_wrapped = wrapped;
    let mut topics_completed = 0usize;
    let mut unresolved_topics = BTreeSet::new();
    let mut error = None;
    for (offset, unit) in units[start..end].iter().enumerate() {
        // Cancellation lands between work units, so an interrupted pass leaves
        // the cursor on the first unprocessed unit.
        if cancelled.is_cancelled() {
            batch_end = start + offset;
            batch_wrapped = false;
            break;
        }
        let outcome = process_restore_unit(context, net_handle, node_id, unit, verified).await;
        let completed = unit
            .topics
            .len()
            .saturating_sub(outcome.unresolved_topics.len());
        topics_completed = topics_completed.saturating_add(completed);
        if completed > 0 {
            info!(
                event = "startup.recovery.progress",
                phase = "shard_restore",
                topics = completed,
                withheld_topics = outcome.unresolved_topics.len(),
                unit = start + offset + 1,
                units_total = units.len(),
                "Completed recovery topics"
            );
        }
        unresolved_topics.extend(outcome.unresolved_topics);
        error = RecoveryError::merge(error, outcome.error);
    }
    RestoreBatch {
        end: batch_end,
        wrapped: batch_wrapped,
        topics_completed,
        unresolved_topics,
        error,
    }
}

/// Installs publisher and membership policy for the shard topics this node
/// already holds a genesis for, without contacting a peer. Missing geneses and
/// anti-entropy are remote convergence and belong to [`run_recovery`].
pub async fn prepare_shard_policy(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
) -> RestoreShardSummary {
    let Some(net_handle) = context.net_handle.clone() else {
        return RestoreShardSummary::default();
    };
    let RealmConfigLoad::Found(config) = load_realm_config(context, realm_id).await else {
        return RestoreShardSummary::default();
    };
    let plan = plan_shard_groups(&config, node_id, realm_id, unix_timestamp_millis());
    let verified = crate::shard::verify::load_verified_topics(context, realm_id).await;

    let mut prepared = 0usize;
    for ((co_members, publishers, retained), topics) in
        plan.rank0_groups.iter().chain(plan.join_groups.iter())
    {
        let present: Vec<::irokle::TopicId> = topics
            .iter()
            .copied()
            .filter(|topic| net_handle.sync_topic_exists(*topic).unwrap_or(false))
            .collect();
        if present.is_empty() {
            continue;
        }
        let mut members = co_members.clone();
        members.push(node_id);
        members.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        prepared += present.len();
        if let Err(error) = net_handle
            .reconcile_shard_membership(&present, members, publishers.clone(), retained, &verified)
            .await
        {
            // Local policy is installed before this returns.
            warn!(error = %error, "Deferring shard membership top-up to remote recovery");
        }
    }
    info!(
        held_shards = plan.summary.held_shards,
        prepared_topics = prepared,
        "Prepared local shard topic policy"
    );
    plan.summary
}

async fn process_restore_unit(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
    verified: &BTreeSet<::irokle::TopicId>,
) -> RestoreUnitOutcome {
    match unit.kind {
        RestoreKind::Shared => restore_shared(context, net_handle, node_id, unit, true).await,
        RestoreKind::SharedJoin => restore_shared(context, net_handle, node_id, unit, false).await,
        RestoreKind::Rank0 => restore_rank0(context, net_handle, node_id, unit, verified).await,
        RestoreKind::Join => restore_join(context, net_handle, node_id, unit, verified).await,
    }
}

async fn restore_shared(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
    may_mint: bool,
) -> RestoreUnitOutcome {
    let mut outcome = RestoreUnitOutcome::default();
    // An unreachable or refusing peer may hold the genesis, so minting would fork it.
    // Keep withheld topics unresolved for recovery to retry.
    let (to_ensure, withheld) = crate::placement::process_placements::resolve_creatable_topics(
        context,
        net_handle,
        node_id,
        &unit.peers,
        unit.topics.clone(),
        may_mint,
        &mut BTreeSet::new(),
    )
    .await;
    if withheld {
        outcome.fail_topics(
            unit.topics
                .iter()
                .copied()
                .filter(|topic| !to_ensure.contains(topic)),
        );
    }
    if to_ensure.is_empty() {
        return outcome;
    }
    // A unit without peers holds topics this node is alone on: they are minted
    // here and no peer joins their membership.
    if unit.peers.is_empty() {
        if let Err(error) = net_handle.ensure_local_topics(&to_ensure).await {
            warn!(error = %error, "Failed to mint this node's own shared topics");
            outcome.fail_topics(to_ensure.iter().copied());
        }
        return outcome;
    }
    if let Err(error) = net_handle
        .ensure_sync_topics(&to_ensure, unit.peers.clone())
        .await
    {
        warn!(error = %error, "Failed to ensure shared realm topics on restart");
        outcome.fail_topics(to_ensure.iter().copied());
    }
    let event = net_handle
        .sync_document_topics(to_ensure.clone(), unit.peers.clone())
        .await;
    if !apply_restored_reconcile(context, node_id, event).await {
        outcome.fail_topics(to_ensure);
    }
    outcome
}

async fn restore_rank0(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
    verified: &BTreeSet<::irokle::TopicId>,
) -> RestoreUnitOutcome {
    let mut outcome = RestoreUnitOutcome::default();
    // An unreachable co-holder might hold the genesis; never fork a second one.
    let group_withheld = crate::placement::process_placements::ensure_genesis_group(
        context,
        net_handle,
        node_id,
        unit.peers.clone(),
        unit.publishers.clone(),
        unit.topics.clone(),
        &unit.retained,
        verified,
        &mut BTreeSet::new(),
    )
    .await;
    let present: Vec<::irokle::TopicId> = unit
        .topics
        .iter()
        .copied()
        .filter(|topic| net_handle.sync_topic_exists(*topic).unwrap_or(false))
        .collect();
    if group_withheld {
        outcome.error = RecoveryError::merge(outcome.error, Some(RecoveryError::PeerUnavailable));
    }
    outcome.fail_topics(
        unit.topics
            .iter()
            .copied()
            .filter(|topic| !present.contains(topic)),
    );
    if !present.is_empty() {
        let event = net_handle
            .sync_document_topics(present.clone(), unit.peers.clone())
            .await;
        if !apply_restored_reconcile(context, node_id, event).await {
            outcome.fail_topics(present);
        }
    }
    outcome
}

async fn restore_join(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
    verified: &BTreeSet<::irokle::TopicId>,
) -> RestoreUnitOutcome {
    let mut outcome = RestoreUnitOutcome::default();
    let mut current_members = unit.peers.clone();
    current_members.push(node_id);
    current_members.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    if let Err(error) = net_handle
        .reconcile_shard_membership(
            &unit.topics,
            current_members.clone(),
            unit.publishers.clone(),
            &unit.retained,
            verified,
        )
        .await
    {
        warn!(error = %error, "Failed to prepare joined shard membership on restart");
        outcome.fail_topics(unit.topics.iter().copied());
    }
    let event = net_handle
        .sync_document_topics(unit.topics.clone(), unit.peers.clone())
        .await;
    if !apply_restored_reconcile(context, node_id, event).await {
        outcome.fail_topics(unit.topics.iter().copied());
    }
    let present: Vec<::irokle::TopicId> = unit
        .topics
        .iter()
        .copied()
        .filter(|topic| net_handle.sync_topic_exists(*topic).unwrap_or(false))
        .collect();
    outcome.fail_topics(
        unit.topics
            .iter()
            .copied()
            .filter(|topic| !present.contains(topic)),
    );
    if !present.is_empty()
        && let Err(error) = net_handle
            .reconcile_shard_membership(
                &present,
                current_members,
                unit.publishers.clone(),
                &unit.retained,
                verified,
            )
            .await
    {
        warn!(error = %error, "Failed to reconcile joined shard membership on restart");
        outcome.fail_topics(present);
    }
    outcome
}

pub(crate) async fn apply_restored_reconcile(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    event: DocumentNetEvent,
) -> bool {
    let result = match event {
        DocumentNetEvent::DocumentsReconciled {
            applied,
            targets,
            metadata_create_events,
            metadata_graph_tombstones,
        } => {
            if applied == 0 {
                return true;
            }
            DocumentReconcileResult {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
            }
        }
        DocumentNetEvent::Error { error, .. } => {
            warn!(error = %error, "Failed to sync held shard topics on restart");
            return false;
        }
        other => {
            warn!(event = ?other, "Unexpected restart shard sync result");
            return false;
        }
    };

    let realm_id = context.node_and_realm().map(|(_, realm_id)| realm_id);
    let refresh = RefreshUsageOperation::new(node_id, realm_id, &result.targets, Ulid::generate());
    if let Err(error) = drive(refresh, context.as_ref()).await {
        warn!(%error, "Realm usage refresh stopped after the shard restore");
    }
    refresh_target_interest(context, &result.targets).await;
    crate::notifications::email::config_applied(context, &result.targets).await;
    apply_metadata(context, Some(node_id), result, false).await;
    true
}

async fn load_realm_config(context: &Arc<DriverContext>, realm_id: RealmId) -> RealmConfigLoad {
    match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => RealmConfigLoad::Found(config),
        Err(GetConfigError::DocumentNotFound) => RealmConfigLoad::Absent,
        Err(error) => {
            warn!(%realm_id, error = %error, "Failed to load realm config for recovery");
            RealmConfigLoad::StorageFailure
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::recovery::RecoveryState;
    use crate::node::restore_plan::{
        RESTORE_CHUNK_TOPICS, RESTORE_TOPIC_COUNT, RESTORE_UNIT_BUDGET, shared_topic_peers,
    };
    use aruna_core::UserId;
    use aruna_core::document::{DocumentTarget, shard_topic_id};
    use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, NetEffect, StorageEffect};
    use aruna_core::events::{DhtEntry, DhtEvent, Event, NetEvent, StorageEvent};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::placement::record::{PlacementOverride, PlacementStrategy};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    const RECOVERY_TEST_GUARD: Duration = Duration::from_secs(5 * 60);
    /// Real-time grace before a pass that published nothing counts as stuck.
    const RECOVERY_PASS_GRACE: Duration = Duration::from_secs(5);

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn sharded_config(nodes: &[NodeId], shards: u32) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId([7; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: ulid::Ulid::from_bytes([5u8; 16]),
            name: "default".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: shards,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    // Every node chooses the lowest sync-eligible realm minter.
    // A lower device id must never mint a competing genesis.
    #[test]
    fn realm_minter_agrees() {
        let realm_id = RealmId([7; 32]);
        let mut ids = [node(1), node(2), node(3)];
        ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let (user, minter, follower) = (ids[0], ids[1], ids[2]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.ensure_node(
            user,
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        );
        config.ensure_node(minter, RealmNodeKind::Server);
        config.ensure_node(follower, RealmNodeKind::Server);

        let mints_realm_topics = |node_id| {
            plan_shard_groups(&config, node_id, realm_id, 0)
                .into_units()
                .iter()
                .any(|unit| {
                    unit.kind == RestoreKind::Shared
                        && unit.topics.contains(
                            &DocumentTarget::RealmConfig { realm_id }
                                .sync_topic_id(realm_id, &PlacementRef::NIL),
                        )
                })
        };
        assert!(mints_realm_topics(minter));
        assert!(!mints_realm_topics(follower));
        assert!(
            !mints_realm_topics(user),
            "a node outside the sync-eligible set is never the realm minter"
        );
    }

    // Devices may remain offline, so restore excludes them from blocking peer sets.
    // They receive separate best-effort pushes.
    #[test]
    fn units_skip_devices() {
        let realm_id = RealmId([7; 32]);
        let (server, other, device) = (node(1), node(2), node(3));
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.ensure_node(server, RealmNodeKind::Server);
        config.ensure_node(other, RealmNodeKind::Server);
        config.ensure_node(
            device,
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        );

        for unit in plan_shard_groups(&config, server, realm_id, 0).into_units() {
            assert!(
                !unit.peers.contains(&device),
                "{:?} targets a device",
                unit.kind
            );
        }
        // The device plans no realm-wide topic and reaches nobody with the ones
        // that are its own.
        let planned = plan_shard_groups(&config, device, realm_id, 0);
        assert_eq!(planned.summary.shared_topics, RESTORE_TOPIC_COUNT - 2);
        assert!(
            planned
                .into_units()
                .iter()
                .all(|unit| unit.peers.is_empty()),
            "a device reaches no peer for its own topics"
        );
    }

    #[test]
    fn restore_units_chunk() {
        let local = node(1);
        let config = sharded_config(&[local, node(2), node(3)], 256);
        let plan = plan_shard_groups(&config, local, RealmId([7; 32]), 0);
        let held = plan.summary.held_shards;
        let units = plan.into_units();

        assert!(held > 0, "the local node should hold shards");
        assert!(
            units
                .iter()
                .all(|unit| unit.topics.len() <= RESTORE_CHUNK_TOPICS),
            "a work unit must never exceed its topic chunk"
        );
        // Grouping keeps a restart at one unit per topic chunk, never one per
        // held shard.
        assert!(
            units.len() < held,
            "{} units for {held} held shards is not batched",
            units.len()
        );
    }

    // One pass never exceeds its work-unit limit; the cursor resumes after the
    // last processed unit and wraps at the end so no group is skipped forever.
    #[test]
    fn restore_cursor_wraps() {
        let units_total = RESTORE_UNIT_BUDGET * 2 + 3;
        let mut cursor = ShardRestoreCursor::default();
        let mut visited = Vec::new();
        for _ in 0..3 {
            let RestoreRange {
                start,
                end,
                wrapped,
            } = restore_range(cursor, units_total);
            assert!(end - start <= RESTORE_UNIT_BUDGET);
            visited.extend(start..end);
            cursor.next_unit = if wrapped { 0 } else { end };
            if wrapped {
                break;
            }
        }
        assert_eq!(visited, (0..units_total).collect::<Vec<_>>());
        assert_eq!(cursor, ShardRestoreCursor::default(), "cursor must wrap");
    }

    #[test]
    fn cursor_rebinds_plan() {
        let topic = |seed| ::irokle::TopicId::from_bytes([seed; 32]);
        let unit = |seed| RestoreUnit {
            publishers: Vec::new(),
            kind: RestoreKind::Shared,
            peers: vec![node(seed)],
            retained: BTreeSet::new(),
            topics: vec![topic(seed)],
        };
        let mut cursor = ShardRestoreCursor::default();
        let original = vec![unit(1), unit(2), unit(3)];

        assert!(bind_restore_plan(&mut cursor, &original));
        cursor.next_unit = 2;
        assert!(!bind_restore_plan(&mut cursor, &original));
        assert_eq!(cursor.next_unit, 2);

        let mut reordered = original.clone();
        reordered.swap(0, 2);
        assert!(bind_restore_plan(&mut cursor, &reordered));
        assert_eq!(cursor.next_unit, 0);

        cursor.next_unit = 2;
        assert!(bind_restore_plan(&mut cursor, &original[..2]));
        assert_eq!(cursor.next_unit, 0);

        cursor.next_unit = 1;
        let mut added = original;
        added.push(unit(4));
        assert!(bind_restore_plan(&mut cursor, &added));
        assert_eq!(cursor.next_unit, 0);
    }

    #[test]
    fn cursor_rebinds_config() {
        let local = node(1);
        let mut config = sharded_config(&[local, node(2), node(3)], 16);
        let original = plan_shard_groups(&config, local, RealmId([7; 32]), 0).into_units();
        let mut cursor = ShardRestoreCursor::default();
        assert!(bind_restore_plan(&mut cursor, &original));
        cursor.next_unit = 1;

        config.strategies[0].shard_count = 32;
        let changed = plan_shard_groups(&config, local, RealmId([7; 32]), 0).into_units();
        assert!(bind_restore_plan(&mut cursor, &changed));
        assert_eq!(cursor.next_unit, 0);
    }

    fn assert_rebind(
        before: &RealmConfigDocument,
        after: &RealmConfigDocument,
        expected_change: bool,
    ) {
        let local = node(1);
        let realm_id = RealmId([7; 32]);
        let before_units = plan_shard_groups(before, local, realm_id, 0).into_units();
        let after_units = plan_shard_groups(after, local, realm_id, 0).into_units();
        let mut cursor = ShardRestoreCursor::default();
        assert!(bind_restore_plan(&mut cursor, &before_units));
        cursor.next_unit = 1;

        assert_eq!(
            bind_restore_plan(&mut cursor, &after_units),
            expected_change
        );
        assert_eq!(cursor.next_unit, if expected_change { 0 } else { 1 });
    }

    #[test]
    fn config_rebinds_matrix() {
        let local = node(1);
        let peer_one = node(2);
        let peer_two = node(3);
        let base = sharded_config(&[local, peer_one, peer_two], 16);

        let mut add_strategy = base.clone();
        add_strategy.strategies.push(PlacementStrategy {
            strategy_id: ulid::Ulid::from_bytes([6u8; 16]),
            name: "added".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 8,
        });
        assert_rebind(&base, &add_strategy, true);

        let mut add_group = base.clone();
        add_group.ensure_node(node(4), RealmNodeKind::Server);
        assert_rebind(&base, &add_group, true);

        let mut shrink = base.clone();
        shrink.strategies[0].shard_count = 8;
        assert_rebind(&base, &shrink, true);

        let mut remove = base.clone();
        remove.strategies.clear();
        remove.default_strategy_id = None;
        assert_rebind(&base, &remove, true);

        let mut reorder_base = base.clone();
        reorder_base.strategies.push(PlacementStrategy {
            strategy_id: ulid::Ulid::from_bytes([7u8; 16]),
            name: "second".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 8,
        });
        let mut reorder = reorder_base.clone();
        reorder.strategies.reverse();
        assert_rebind(&reorder_base, &reorder, true);

        let mut rebind = base.clone();
        let strategy_id = ulid::Ulid::from_bytes([8u8; 16]);
        rebind.strategies[0].strategy_id = strategy_id;
        rebind.default_strategy_id = Some(strategy_id);
        assert_rebind(&base, &rebind, true);

        let mut description = base.clone();
        description.description = "metadata-only edit".to_string();
        assert_rebind(&base, &description, false);

        let mut rename = base.clone();
        rename.strategies[0].name = "renamed".to_string();
        assert_rebind(&base, &rename, false);

        let mut add_user = base.clone();
        add_user.ensure_node(
            node(4),
            RealmNodeKind::User {
                owner: UserId::nil(RealmId([7; 32])),
            },
        );
        assert_rebind(&base, &add_user, false);
    }

    struct RecoveryNode {
        _dir: tempfile::TempDir,
        net: NetHandle,
        task_handle: TaskHandle,
        context: Arc<DriverContext>,
    }

    async fn make_node(realm_id: RealmId, seed: u8) -> RecoveryNode {
        let dir = tempdir().expect("tempdir must open");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage must open");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
                secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                sync_storage_path: Some(dir.path().join("document-sync")),
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net must open");
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
            outbound_handle: None,
        });
        crate::sync::incoming::initialize_incoming_fixture(context.clone());
        RecoveryNode {
            _dir: dir,
            net,
            task_handle,
            context,
        }
    }

    fn test_config(
        realm_id: RealmId,
        local: NodeId,
        peers: &[NodeId],
    ) -> (RealmConfigDocument, Vec<PlacementRef>) {
        let strategy_id = ulid::Ulid::from_bytes([6u8; 16]);
        let strategy = PlacementStrategy {
            strategy_id,
            name: "recovery-test".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: peers.len() as u32,
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.default_strategy_id = Some(strategy_id);
        config.strategies = vec![strategy];
        config.ensure_node(local, RealmNodeKind::Server);
        for peer in peers {
            config.ensure_node(*peer, RealmNodeKind::Server);
        }

        let mut placements = Vec::with_capacity(peers.len());
        for (shard, peer) in peers.iter().enumerate() {
            let placement = PlacementRef {
                strategy_id,
                shard: shard as u32,
            };
            config.placement_overrides.push(PlacementOverride {
                subject: aruna_core::placement::shard_subject_bytes(&placement),
                pinned: vec![local, *peer],
                excluded: Vec::new(),
                strategy_id: None,
            });
            placements.push(placement);
        }
        (config, placements)
    }

    fn flat_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    async fn save_config(node: &RecoveryNode, config: &RealmConfigDocument) {
        let realm_id = config.realm_id;
        let target = DocumentTarget::RealmConfig { realm_id };
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let event = node
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).expect("config serializes").into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        node.net
            .refresh_document_peers(config)
            .await
            .expect("realm peers refresh");
    }

    async fn mesh_nodes(first: &RecoveryNode, second: &RecoveryNode) {
        first.net.add_peer_addr(second.net.endpoint_addr()).await;
        second.net.add_peer_addr(first.net.endpoint_addr()).await;
    }

    async fn seed_topic(node: &RecoveryNode, topic: ::irokle::TopicId, peers: &[NodeId]) {
        node.net
            .ensure_sync_topics(&[topic], peers.to_vec())
            .await
            .expect("blocked peer topic creates");
    }

    fn spawn_driver(
        node: &RecoveryNode,
        realm_id: RealmId,
        publish_full_usage: bool,
    ) -> (
        RecoveryStatus,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let status = RecoveryStatus::new();
        let cancelled = CancellationToken::new();
        let driver = tokio::spawn(run_recovery(
            node.context.clone(),
            RecoveryConfig {
                realm_id,
                node_id: node.net.node_id(),
                publish_full_usage,
            },
            status.clone(),
            cancelled.clone(),
        ));
        (status, cancelled, driver)
    }

    /// Holds virtual time still while a pass runs, so deadlines guarding real
    /// work inside it cannot fire spuriously. A pass may itself wait on a
    /// deadline, so the clock resumes once one has stalled for real seconds.
    #[derive(Default)]
    struct PassClock {
        running_since: Option<std::time::Instant>,
    }

    impl PassClock {
        async fn tick(&mut self, status: &RecoveryStatus, step: Duration) {
            tokio::task::yield_now().await;
            if status.snapshot().state == RecoveryState::Degraded {
                self.running_since = None;
                tokio::time::advance(step).await;
                return;
            }
            let since = *self
                .running_since
                .get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= RECOVERY_PASS_GRACE {
                tokio::time::advance(step).await;
            }
        }
    }

    async fn wait_state(status: &RecoveryStatus, expected: RecoveryState) {
        let wait = async {
            let mut clock = PassClock::default();
            loop {
                if status.snapshot().state == expected {
                    return;
                }
                clock.tick(status, Duration::from_millis(100)).await;
            }
        };
        tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
            .await
            .unwrap_or_else(|_| panic!("recovery did not reach {expected:?}"));
    }

    async fn wait_partial(status: &RecoveryStatus, target: u64) {
        let wait = async {
            let mut clock = PassClock::default();
            loop {
                if status.pass_total(RecoveryOutcome::Partial) >= target {
                    return;
                }
                clock.tick(status, Duration::from_millis(100)).await;
            }
        };
        tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
            .await
            .unwrap_or_else(|_| panic!("recovery did not reach partial pass {target}"));
    }

    async fn wait_stall(status: &RecoveryStatus) -> (u64, u64, u64) {
        let wait = async {
            let mut observed = 0;
            let mut previous = None;
            let mut clock = PassClock::default();
            loop {
                let partial = status.pass_total(RecoveryOutcome::Partial);
                if partial <= observed || status.snapshot().state != RecoveryState::Degraded {
                    clock.tick(status, Duration::from_millis(100)).await;
                    continue;
                }
                clock.tick(status, RECOVERY_RETRY_BASE / 2).await;
                if status.snapshot().state == RecoveryState::Degraded
                    && status.pass_total(RecoveryOutcome::Partial) == partial
                {
                    observed = partial;
                    let progress = status.progress_ms();
                    let remaining = status.snapshot().topics_remaining;
                    if previous == Some((progress, remaining)) {
                        return (partial, progress, remaining);
                    }
                    previous = Some((progress, remaining));
                }
            }
        };
        tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
            .await
            .expect("recovery did not reach a stable retry")
    }

    async fn finish_driver(cancelled: CancellationToken, driver: tokio::task::JoinHandle<()>) {
        cancelled.cancel();
        driver.await.expect("recovery task must stop");
    }

    async fn stop_node(node: RecoveryNode) {
        let _ = node.task_handle.shutdown(RECOVERY_RETRY_MAX).await;
        node.net.shutdown().await;
    }

    async fn presence_values(node: &RecoveryNode, realm_id: RealmId) -> Vec<DhtEntry> {
        let event = node
            .net
            .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Get {
                key: aruna_core::keys::realm_presence_key(&realm_id),
                realm_filter: Some(realm_id),
                options: DhtGetOptions::exhaustive(RECOVERY_TEST_GUARD),
            })))
            .await;
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) => values,
            _ => Vec::new(),
        }
    }

    #[tokio::test]
    async fn device_skips_presence() {
        // A User-kind node completes the presence phase without publishing; the
        // same node publishes once the realm config makes it a Server.
        let realm_id = RealmId([31u8; 32]);
        let device = make_node(realm_id, 31).await;
        let recovery = RecoveryConfig {
            realm_id,
            node_id: device.net.node_id(),
            publish_full_usage: false,
        };

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        config.ensure_node(
            device.net.node_id(),
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        );
        save_config(&device, &config).await;

        let mut pending = true;
        let outcome = presence_phase(&device.context, &recovery, &mut pending).await;
        assert!(outcome.progress);
        assert!(outcome.error.is_none());
        assert!(!pending);
        assert!(presence_values(&device, realm_id).await.is_empty());

        let mut promoted = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        promoted.ensure_node(device.net.node_id(), RealmNodeKind::Server);
        save_config(&device, &promoted).await;

        let mut pending = true;
        let outcome = presence_phase(&device.context, &recovery, &mut pending).await;
        assert!(outcome.progress);
        assert!(!pending);
        assert!(!presence_values(&device, realm_id).await.is_empty());

        stop_node(device).await;
    }

    #[tokio::test]
    async fn restore_suffix() {
        let realm_id = RealmId([10; 32]);
        let local = make_node(realm_id, 1).await;
        let blocked = make_node(realm_id, 2).await;
        let healthy = make_node(realm_id, 3).await;
        let (config, placements) = test_config(
            realm_id,
            local.net.node_id(),
            &[blocked.net.node_id(), healthy.net.node_id()],
        );
        save_config(&local, &config).await;
        save_config(&blocked, &config).await;
        save_config(&healthy, &config).await;
        mesh_nodes(&local, &healthy).await;

        let blocked_topic = shard_topic_id(realm_id, &placements[0]);
        let healthy_topic = shard_topic_id(realm_id, &placements[1]);
        seed_topic(&blocked, blocked_topic, &[blocked.net.node_id()]).await;
        seed_topic(&healthy, healthy_topic, &[local.net.node_id()]).await;
        let pass = restore_shard_pass(
            &local.context,
            local.net.node_id(),
            realm_id,
            &mut ShardRestoreCursor::default(),
            &CancellationToken::new(),
        )
        .await;

        assert!(pass.wrapped);
        assert!(pass.unresolved_topics.contains(&blocked_topic));
        assert!(!pass.unresolved_topics.contains(&healthy_topic));
        assert!(
            local
                .net
                .sync_topic_exists(healthy_topic)
                .expect("healthy topic lookup")
        );
        assert!(pass.topics_completed > 0);

        stop_node(local).await;
        stop_node(blocked).await;
        stop_node(healthy).await;
    }

    #[tokio::test]
    async fn heal_rotation() {
        let realm_id = RealmId([11; 32]);
        let local = make_node(realm_id, 4).await;
        let blocked = make_node(realm_id, 5).await;
        let (config, placements) =
            test_config(realm_id, local.net.node_id(), &[blocked.net.node_id()]);
        save_config(&local, &config).await;
        save_config(&blocked, &config).await;
        let topic = shard_topic_id(realm_id, &placements[0]);
        seed_topic(&blocked, topic, &[blocked.net.node_id()]).await;

        tokio::time::pause();
        let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
        wait_state(&status, RecoveryState::Degraded).await;
        assert!(status.snapshot().topics_remaining > 0);
        assert!(
            !local
                .net
                .sync_topic_exists(topic)
                .expect("local topic lookup")
        );

        mesh_nodes(&local, &blocked).await;
        // Every configured peer restores because only the designated minter creates shared topics.
        restore_shard_subscriptions(&blocked.context, blocked.net.node_id(), realm_id).await;
        crate::placement::process_placements::process_shard_placements(
            &blocked.context,
            realm_id,
            blocked.net.node_id(),
        )
        .await;
        wait_state(&status, RecoveryState::Converged).await;
        assert_eq!(status.snapshot().state, RecoveryState::Converged);
        assert_eq!(status.snapshot().topics_remaining, 0);
        assert_eq!(status.snapshot().last_error, None);
        assert!(status.pass_total(RecoveryOutcome::Success) > 0);
        assert!(
            local
                .net
                .sync_topic_exists(topic)
                .expect("healed topic lookup")
        );

        finish_driver(cancelled, driver).await;
        stop_node(local).await;
        stop_node(blocked).await;
    }

    #[tokio::test]
    async fn shared_retry() {
        let realm_id = RealmId([13; 32]);
        let local = make_node(realm_id, 8).await;
        let blocked = make_node(realm_id, 9).await;
        let config = flat_config(realm_id, &[local.net.node_id(), blocked.net.node_id()]);
        save_config(&local, &config).await;
        save_config(&blocked, &config).await;

        tokio::time::pause();
        let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
        wait_state(&status, RecoveryState::Degraded).await;
        assert_eq!(
            status.snapshot().last_error,
            Some(RecoveryError::PeerUnavailable)
        );
        assert!(status.snapshot().topics_remaining > 0);
        let partial = status.pass_total(RecoveryOutcome::Partial);

        wait_partial(&status, partial.saturating_add(1)).await;
        assert_eq!(status.snapshot().state, RecoveryState::Degraded);
        assert_eq!(
            status.snapshot().last_error,
            Some(RecoveryError::PeerUnavailable)
        );

        finish_driver(cancelled, driver).await;
        stop_node(local).await;
        stop_node(blocked).await;
    }

    #[tokio::test]
    async fn usage_retry() {
        let realm_id = RealmId([14; 32]);
        let local = make_node(realm_id, 10).await;
        let config = flat_config(realm_id, &[local.net.node_id()]);
        save_config(&local, &config).await;
        let group_id = ulid::Ulid::from_bytes([15; 16]);
        let event = local
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::USAGE_STATS_KEYSPACE.to_string(),
                key: aruna_core::structs::storage::usage::usage_group_key(group_id).into(),
                value: vec![0xff].into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        tokio::time::pause();
        let (status, cancelled, driver) = spawn_driver(&local, realm_id, true);
        wait_state(&status, RecoveryState::Degraded).await;
        assert_eq!(status.snapshot().last_error, Some(RecoveryError::Storage));
        assert_eq!(status.snapshot().topics_remaining, 0);
        let partial = status.pass_total(RecoveryOutcome::Partial);

        wait_partial(&status, partial.saturating_add(1)).await;
        assert_eq!(status.snapshot().state, RecoveryState::Degraded);
        assert_eq!(status.snapshot().last_error, Some(RecoveryError::Storage));
        assert_eq!(status.snapshot().topics_remaining, 0);

        finish_driver(cancelled, driver).await;
        stop_node(local).await;
    }

    #[tokio::test]
    async fn restore_stops_cancelled() {
        let dir = tempdir().expect("tempdir must open");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage must open");
        let net = NetHandle::new(NetConfig::default(), storage.clone())
            .await
            .expect("net must open");
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let units = vec![RestoreUnit {
            kind: RestoreKind::Shared,
            peers: vec![node(2)],
            publishers: Vec::new(),
            retained: BTreeSet::new(),
            topics: vec![::irokle::TopicId::from_bytes([5; 32])],
        }];
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let batch = restore_batch(
            &context,
            &net,
            node(1),
            &units,
            RestoreRange {
                start: 0,
                end: 1,
                wrapped: false,
            },
            &BTreeSet::new(),
            &cancelled,
        )
        .await;
        assert_eq!(batch.end, 0);
        assert!(!batch.wrapped);
        net.shutdown().await;
    }

    #[tokio::test]
    async fn bad_config_degrades() {
        let dir = tempdir().expect("tempdir must open");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage must open");
        let realm_id = RealmId([8; 32]);
        let target = DocumentTarget::RealmConfig { realm_id };
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: vec![0xff].into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        // A failed config read must not unbind the cursor from its plan.
        let bound = ShardRestoreCursor {
            next_unit: 3,
            plan_hash: Some([7; 32]),
        };
        let mut cursor = bound;
        let pass = restore_shard_pass(
            &context,
            node(1),
            realm_id,
            &mut cursor,
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(pass.error, Some(RecoveryError::Storage));
        assert!(pass.wrapped);
        assert!(!pass.enumerated);
        assert_eq!(cursor, bound);
    }

    #[tokio::test]
    async fn stall_recovery() {
        let realm_id = RealmId([12; 32]);
        let local = make_node(realm_id, 6).await;
        let blocked = make_node(realm_id, 7).await;
        let (config, placements) =
            test_config(realm_id, local.net.node_id(), &[blocked.net.node_id()]);
        save_config(&local, &config).await;
        save_config(&blocked, &config).await;
        let topic = shard_topic_id(realm_id, &placements[0]);
        seed_topic(&blocked, topic, &[blocked.net.node_id()]).await;

        tokio::time::pause();
        let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
        let (partial, progress, remaining) = wait_stall(&status).await;
        // A stalled recovery must keep reporting its outstanding topics.
        assert!(remaining > 0);

        wait_partial(&status, partial.saturating_add(3)).await;
        assert_eq!(status.progress_ms(), progress);
        assert_eq!(status.snapshot().topics_remaining, remaining);

        finish_driver(cancelled, driver).await;
        stop_node(local).await;
        stop_node(blocked).await;
    }

    // Backoff climbs from the base to the cap and never past it, so a peer that
    // stays down cannot keep the driver hot.
    #[test]
    fn recovery_backoff_caps() {
        let recovery_backoff =
            |attempts| capped_backoff(RECOVERY_RETRY_BASE, RECOVERY_RETRY_MAX, attempts, 0);
        assert_eq!(recovery_backoff(0), RECOVERY_RETRY_BASE);
        assert!(recovery_backoff(1) > recovery_backoff(0));
        assert_eq!(recovery_backoff(40), RECOVERY_RETRY_MAX);
    }

    #[test]
    fn shared_peers_filter() {
        let self_id = node(1);
        let management = node(2);
        let server = node(3);
        let user = node(5);
        let realm_id = RealmId([9; 32]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.nodes = vec![
            RealmNode {
                node_id: self_id.to_string(),
                kind: RealmNodeKind::Management,
            },
            RealmNode {
                node_id: management.to_string(),
                kind: RealmNodeKind::Management,
            },
            RealmNode {
                node_id: user.to_string(),
                kind: RealmNodeKind::User {
                    owner: UserId::nil(realm_id),
                },
            },
            RealmNode {
                node_id: "malformed-eligible-id".to_string(),
                kind: RealmNodeKind::Server,
            },
            RealmNode {
                node_id: server.to_string(),
                kind: RealmNodeKind::Server,
            },
        ];

        assert_eq!(
            shared_topic_peers(&config, self_id),
            vec![management, server]
        );
    }
}
