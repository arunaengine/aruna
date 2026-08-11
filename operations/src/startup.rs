use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{
    DocumentSyncNetEvent, DocumentSyncReconcileResult, DocumentSyncTarget, shard_topic_id,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::metadata::MetadataCreateEventRecord;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::util::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::driver::DriverContext;
use crate::metadata::projector::{
    project_metadata_create_events, project_metadata_create_events_from_log,
};
use crate::metadata::prune_queue::process_metadata_graph_tombstones;
use crate::notifications::watch::interest::refresh_watch_interest_for_targets;
use crate::placement::{draining_former_holders, resolve_shard_holders};
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

fn shared_topic_peers(config: &RealmConfigDocument, node_id: NodeId) -> Vec<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|candidate| *candidate != node_id)
        .collect()
}

/// Fixed realm-scoped topics restored on every start (see [`shared_targets`]).
pub const SHARED_RESTORE_TOPIC_COUNT: usize = 6;

/// Work units one bounded restore pass may process. A work-unit limit, never a
/// wall-clock deadline, so the bound holds however many shards share a
/// co-holder set.
pub const SHARD_RESTORE_UNIT_BUDGET: usize = 8;
/// Topics per work unit; one unit still fills a document sync topic batch.
pub const SHARD_RESTORE_CHUNK_TOPICS: usize = 64;

/// Shortest wait between two bounded recovery passes while work remains local.
const RECOVERY_RETRY_BASE: Duration = Duration::from_millis(250);
/// Longest wait between passes once the remainder is blocked on absent peers.
const RECOVERY_RETRY_MAX: Duration = Duration::from_secs(30);

/// Remote-recovery lifecycle. A locally safe node keeps serving while this is
/// [`RecoveryState::Degraded`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryState {
    /// Remote recovery has not started.
    #[default]
    Pending,
    /// A bounded recovery pass is active.
    Running,
    /// A pass completed with retryable unavailable-peer work.
    Degraded,
    /// The latest pass has no known remote-recovery remainder.
    Converged,
}

impl RecoveryState {
    /// Every state, so the one-hot metric can seed all of them.
    pub const ALL: [Self; 4] = [
        Self::Pending,
        Self::Running,
        Self::Degraded,
        Self::Converged,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Degraded => "degraded",
            Self::Converged => "converged",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Pending => 0,
            Self::Running => 1,
            Self::Degraded => 2,
            Self::Converged => 3,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Running,
            2 => Self::Degraded,
            3 => Self::Converged,
            _ => Self::Pending,
        }
    }
}

/// Why the last pass left work behind. Closed set: readiness and metrics never
/// carry peer ids, topic ids, or raw error strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryError {
    /// A co-holder could not be reached or refused a genesis probe.
    PeerUnavailable,
    /// Local storage could not supply a trustworthy config or scan.
    Storage,
    /// The recovery driver panicked and stopped.
    Panicked,
}

impl RecoveryError {
    pub fn label(self) -> &'static str {
        match self {
            Self::PeerUnavailable => "peer_unavailable",
            Self::Storage => "storage",
            Self::Panicked => "panicked",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::PeerUnavailable => 1,
            Self::Storage => 2,
            Self::Panicked => 3,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::PeerUnavailable),
            2 => Some(Self::Storage),
            3 => Some(Self::Panicked),
            _ => None,
        }
    }
}

/// How a completed pass is counted in `aruna_recovery_pass_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Success,
    Partial,
    Failed,
}

impl RecoveryOutcome {
    pub const ALL: [Self; 3] = [Self::Success, Self::Partial, Self::Failed];

    pub fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Partial => 1,
            Self::Failed => 2,
        }
    }
}

/// Low-cardinality recovery view shared by the driver, `/readyz`, and the
/// scrape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoverySnapshot {
    pub state: RecoveryState,
    pub topics_remaining: u64,
    /// Unix seconds of the last completed work unit; 0 when none has completed.
    pub last_progress_timestamp: u64,
    pub last_error: Option<RecoveryError>,
}

#[derive(Debug, Default)]
struct RecoveryInner {
    state: AtomicU8,
    topics_remaining: AtomicU64,
    last_progress_ms: AtomicU64,
    last_error: AtomicU8,
    passes: [AtomicU64; 3],
}

/// Handle onto the recovery status. Cloning shares one status.
#[derive(Clone, Debug, Default)]
pub struct RecoveryStatus {
    inner: Arc<RecoveryInner>,
}

impl RecoveryStatus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            state: RecoveryState::from_code(self.inner.state.load(Ordering::Relaxed)),
            topics_remaining: self.inner.topics_remaining.load(Ordering::Relaxed),
            last_progress_timestamp: self.inner.last_progress_ms.load(Ordering::Relaxed) / 1_000,
            last_error: RecoveryError::from_code(self.inner.last_error.load(Ordering::Relaxed)),
        }
    }

    pub fn pass_total(&self, outcome: RecoveryOutcome) -> u64 {
        self.inner.passes[outcome.index()].load(Ordering::Relaxed)
    }

    /// Marks a bounded pass as started.
    pub fn begin_pass(&self) {
        self.inner
            .state
            .store(RecoveryState::Running.code(), Ordering::Relaxed);
    }

    /// Publishes progress after a completed work unit.
    pub fn note_progress(&self, topics_remaining: u64) {
        self.inner
            .topics_remaining
            .store(topics_remaining, Ordering::Relaxed);
        self.inner
            .last_progress_ms
            .store(unix_timestamp_millis(), Ordering::Relaxed);
    }

    pub fn finish_pass(&self, outcome: RecoveryOutcome, error: Option<RecoveryError>) {
        self.inner.passes[outcome.index()].fetch_add(1, Ordering::Relaxed);
        self.inner
            .last_error
            .store(error.map_or(0, RecoveryError::code), Ordering::Relaxed);
        let state = match outcome {
            RecoveryOutcome::Success => RecoveryState::Converged,
            RecoveryOutcome::Partial | RecoveryOutcome::Failed => RecoveryState::Degraded,
        };
        self.inner.state.store(state.code(), Ordering::Relaxed);
    }
}

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
    loop {
        if cancelled.is_cancelled() {
            return;
        }
        status.begin_pass();
        let pass = recovery_pass(
            &context,
            &config,
            &status,
            &cancelled,
            &mut cursor,
            &mut publish_usage,
            &mut reconcile_placements,
        )
        .await;
        if cancelled.is_cancelled() {
            return;
        }
        if pass.remaining == 0 && pass.error.is_none() {
            status.note_progress(0);
            status.finish_pass(RecoveryOutcome::Success, None);
            info!(
                event = "startup.recovery.complete",
                elapsed_ms = started.elapsed().as_millis() as u64,
                passes = attempts.saturating_add(1),
                "Remote recovery converged"
            );
            return;
        }
        if pass.error.is_none() && pass.more_local_work {
            tokio::task::yield_now().await;
            continue;
        }
        status.finish_pass(RecoveryOutcome::Partial, pass.error);
        warn!(
            event = "startup.recovery.degraded",
            topics_remaining = pass.remaining,
            error_class = pass.error.map(RecoveryError::label).unwrap_or("none"),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Remote recovery left retryable work behind"
        );
        let after = recovery_backoff(attempts);
        attempts = attempts.saturating_add(1);
        tokio::select! {
            _ = cancelled.cancelled() => return,
            _ = tokio::time::sleep(after) => {}
        }
    }
}

/// Doubles from [`RECOVERY_RETRY_BASE`] up to [`RECOVERY_RETRY_MAX`].
fn recovery_backoff(attempts: u32) -> Duration {
    let base = RECOVERY_RETRY_BASE.as_millis() as u64;
    let max = RECOVERY_RETRY_MAX.as_millis() as u64;
    Duration::from_millis(base.saturating_mul(1u64 << attempts.min(16)).min(max))
}

#[derive(Clone, Copy, Debug, Default)]
struct RecoveryPass {
    remaining: u64,
    error: Option<RecoveryError>,
    /// Unvisited units are local work, not a peer problem: retry at once.
    more_local_work: bool,
}

async fn recovery_pass(
    context: &Arc<DriverContext>,
    config: &RecoveryConfig,
    status: &RecoveryStatus,
    cancelled: &CancellationToken,
    cursor: &mut ShardRestoreCursor,
    publish_usage: &mut bool,
    reconcile_placements: &mut bool,
) -> RecoveryPass {
    let mut pass = RecoveryPass::default();

    let restore =
        restore_shard_pass(context, config.node_id, config.realm_id, cursor, cancelled).await;
    info!(
        event = "startup.recovery.progress",
        phase = "shard_restore",
        held_shards = restore.summary.held_shards,
        shard_topics = restore.summary.shard_topics,
        shared_topics = restore.summary.shared_topics,
        withheld_topics = restore.summary.withheld_topics,
        units_processed = restore.units_processed,
        units_total = restore.units_total,
        rotation_complete = restore.wrapped,
        "Restored held shard subscriptions"
    );
    pass.remaining += restore.summary.withheld_topics as u64;
    if restore.summary.withheld_topics > 0 {
        pass.error = Some(RecoveryError::PeerUnavailable);
    }
    // Convergence needs the cursor to have walked every unit.
    if !restore.wrapped {
        pass.more_local_work = true;
        pass.remaining += (restore.units_total - restore.units_processed) as u64;
    }
    status.note_progress(pass.remaining);
    if cancelled.is_cancelled() {
        return pass;
    }

    // One scan per driver run: it arms its own durable retry timer, so later
    // passes must not repeat the whole placement walk.
    if *reconcile_placements {
        *reconcile_placements = false;
        let placements = crate::process_placements::process_shard_placements(
            context,
            config.realm_id,
            config.node_id,
        )
        .await;
        match placements.status {
            crate::process_placements::PlacementReconcileStatus::Clean => {}
            crate::process_placements::PlacementReconcileStatus::RetryScheduled => {
                pass.remaining += 1;
                pass.error.get_or_insert(RecoveryError::PeerUnavailable);
            }
            crate::process_placements::PlacementReconcileStatus::StorageFailure => {
                pass.remaining += 1;
                pass.error = Some(RecoveryError::Storage);
            }
        }
        info!(
            event = "startup.recovery.progress",
            phase = "placements",
            retry_scheduled = placements.retry_scheduled,
            pull_pending = placements.pull_pending,
            "Reconciled shard placements"
        );
        status.note_progress(pass.remaining);
        if cancelled.is_cancelled() {
            return pass;
        }
    }

    if let Err(error) = crate::driver::drive(
        crate::announce_realm_presence::AnnounceRealmPresenceOperation::new(
            crate::announce_realm_presence::AnnounceRealmPresenceConfig {
                realm_id: config.realm_id,
                node_id: config.node_id,
                schedule_refresh: true,
            },
        ),
        context.as_ref(),
    )
    .await
    {
        warn!(error = %error, "Failed to announce realm presence during recovery");
        pass.remaining += 1;
        pass.error.get_or_insert(RecoveryError::PeerUnavailable);
    }

    // A failure here leaves its own durable retry marker, so a later pass must
    // not repeat it.
    if *publish_usage {
        *publish_usage = false;
        if let Err(error) = crate::usage_stats::publish_and_refresh_usage_snapshots(
            context.as_ref(),
            config.node_id,
            config.realm_id,
            true,
        )
        .await
        {
            warn!(error = %error, "Failed to publish initial node usage snapshots");
        }
    }
    status.note_progress(pass.remaining);
    pass
}

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
    /// Held topics this pass could not finish because a co-holder was absent or
    /// refused a genesis probe. Retryable background state, never a start error.
    pub withheld_topics: usize,
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
/// same way. Shard topics sharing co-holder and retained sets are batched into
/// one ensure and one sync so a restart costs O(held shards), not O(stored
/// documents). The returned [`RestoreShardSummary`] reports the (small) topic
/// count for callers and the restart-traffic gate.
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

/// Where a bounded restore resumes. Reconstructed from the realm config on every
/// pass, so a config change simply re-derives the unit list.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardRestoreCursor {
    next_unit: usize,
}

/// What one bounded [`restore_shard_pass`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardRestorePass {
    pub summary: RestoreShardSummary,
    /// Never more than [`SHARD_RESTORE_UNIT_BUDGET`].
    pub units_processed: usize,
    pub units_total: usize,
    /// Whether the cursor reached the end and wrapped back to the head.
    pub wrapped: bool,
}

/// Runs at most [`SHARD_RESTORE_UNIT_BUDGET`] restore work units from `cursor`,
/// advances it past them, and wraps at the end, so an unavailable first group
/// cannot starve a later healthy one.
pub async fn restore_shard_pass(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
    cursor: &mut ShardRestoreCursor,
    cancelled: &CancellationToken,
) -> ShardRestorePass {
    let Some(net_handle) = context.net_handle.clone() else {
        *cursor = ShardRestoreCursor::default();
        return ShardRestorePass {
            wrapped: true,
            ..ShardRestorePass::default()
        };
    };
    let Some(config) = load_realm_config(context, realm_id).await else {
        // No config yet (fresh/onboarding node): nothing sharded to restore.
        *cursor = ShardRestoreCursor::default();
        return ShardRestorePass {
            wrapped: true,
            ..ShardRestorePass::default()
        };
    };
    let plan = plan_shard_groups(&config, node_id, realm_id);
    let mut summary = plan.summary;
    let units = plan.into_units();
    let units_total = units.len();
    if units_total == 0 {
        *cursor = ShardRestoreCursor::default();
        return ShardRestorePass {
            summary,
            units_processed: 0,
            units_total: 0,
            wrapped: true,
        };
    }

    // Former-holder history cutoffs are frozen only for shards a prior run
    // durably verified; join verification below happens after this restore.
    let verified = crate::shard::verify::load_verified_shard_topics(context, realm_id).await;

    let (start, mut end, mut wrapped) = restore_range(*cursor, units_total);
    let mut withheld = 0usize;
    for (offset, unit) in units[start..end].iter().enumerate() {
        // Cancellation lands between work units, so an interrupted pass leaves
        // the cursor on the first unprocessed unit.
        if cancelled.is_cancelled() {
            end = start + offset;
            wrapped = false;
            break;
        }
        withheld += process_restore_unit(context, &net_handle, node_id, unit, &verified).await;
    }
    summary.withheld_topics = withheld;
    cursor.next_unit = if wrapped { 0 } else { end };

    // A withheld genesis (co-holder down or refusing) has no placement record to
    // drive a re-run, so arm the retry timer here — the reconciler re-probes when
    // the co-holder returns instead of deferring writes at 1s until restart.
    if withheld > 0
        && let Some(task_handle) = context.task_handle.as_ref()
    {
        let effect = crate::sync_placement::schedule_placement_retry_effect(realm_id, node_id);
        let _ = task_handle.send_effect(effect).await;
    }

    if wrapped {
        // New-holder verification: reconcile each held shard against a co-holder
        // and persist a marker so a restart resumes only unverified shards.
        crate::shard::verify::verify_held_shards(context, node_id, realm_id).await;
    }

    ShardRestorePass {
        summary,
        units_processed: end - start,
        units_total,
        wrapped,
    }
}

type ShardGroup = (Vec<NodeId>, BTreeSet<NodeId>);

/// Restore work grouped by co-holder and retained peer sets, so matching shards
/// ride one ensure and one sync instead of one round trip each.
struct ShardPlan {
    summary: RestoreShardSummary,
    shared_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>>,
    rank0_groups: BTreeMap<ShardGroup, Vec<::irokle::TopicId>>,
    join_groups: BTreeMap<ShardGroup, Vec<::irokle::TopicId>>,
}

/// Derives the restore plan from the realm config alone. I/O-free, so the local
/// serving gate and the bounded remote pass read the same view.
fn plan_shard_groups(
    config: &RealmConfigDocument,
    node_id: NodeId,
    realm_id: RealmId,
) -> ShardPlan {
    let mut plan = ShardPlan {
        summary: RestoreShardSummary::default(),
        shared_groups: BTreeMap::new(),
        rank0_groups: BTreeMap::new(),
        join_groups: BTreeMap::new(),
    };

    let mut shared_peers = shared_topic_peers(config, node_id);
    shared_peers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    for target in shared_targets(realm_id, node_id) {
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        plan.shared_groups
            .entry(shared_peers.clone())
            .or_default()
            .push(topic);
        plan.summary.shared_topics += 1;
    }

    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
            if !holders.contains(&node_id) {
                continue;
            }
            plan.summary.held_shards += 1;
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
            let retained: BTreeSet<NodeId> = draining_former_holders(config, &placement)
                .into_iter()
                .collect();
            let groups = if local_is_rank0 {
                &mut plan.rank0_groups
            } else {
                &mut plan.join_groups
            };
            groups
                .entry((co_holders, retained))
                .or_default()
                .push(topic);
            plan.summary.shard_topics += 1;
        }
    }
    plan
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
    let Some(config) = load_realm_config(context, realm_id).await else {
        return RestoreShardSummary::default();
    };
    let plan = plan_shard_groups(&config, node_id, realm_id);
    let verified = crate::shard::verify::load_verified_shard_topics(context, realm_id).await;

    let mut prepared = 0usize;
    for ((co_holders, retained), topics) in plan.rank0_groups.iter().chain(plan.join_groups.iter())
    {
        let present: Vec<::irokle::TopicId> = topics
            .iter()
            .copied()
            .filter(|topic| {
                net_handle
                    .document_sync_topic_exists(*topic)
                    .unwrap_or(false)
            })
            .collect();
        if present.is_empty() {
            continue;
        }
        let mut holders = co_holders.clone();
        holders.push(node_id);
        holders.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        prepared += present.len();
        if let Err(error) = net_handle
            .reconcile_shard_membership(&present, holders, retained, &verified)
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

/// The half-open unit range one bounded pass processes, and whether it wraps.
fn restore_range(cursor: ShardRestoreCursor, units_total: usize) -> (usize, usize, bool) {
    let start = cursor.next_unit.min(units_total);
    let end = (start + SHARD_RESTORE_UNIT_BUDGET).min(units_total);
    (start, end, end >= units_total)
}

/// One bounded unit of restore work: one co-holder group's ensure and sync over
/// a chunk of topics.
#[derive(Clone, Debug)]
struct RestoreUnit {
    kind: RestoreKind,
    peers: Vec<NodeId>,
    retained: BTreeSet<NodeId>,
    topics: Vec<::irokle::TopicId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreKind {
    /// Fixed realm-scoped topics; their genesis is deterministic, so they are
    /// ensured directly.
    Shared,
    /// Rank-0: create a genesis only with positive co-holder confirmation.
    Rank0,
    /// Held at another rank: join-only.
    Join,
}

impl ShardPlan {
    /// Flattens the plan into a deterministic list of bounded work units.
    fn into_units(self) -> Vec<RestoreUnit> {
        let mut units = Vec::new();
        let mut push = |kind, peers: Vec<NodeId>, retained: BTreeSet<NodeId>, topics: Vec<_>| {
            if peers.is_empty() || topics.is_empty() {
                return;
            }
            for chunk in topics.chunks(SHARD_RESTORE_CHUNK_TOPICS) {
                units.push(RestoreUnit {
                    kind,
                    peers: peers.clone(),
                    retained: retained.clone(),
                    topics: chunk.to_vec(),
                });
            }
        };
        for (peers, topics) in self.shared_groups {
            push(RestoreKind::Shared, peers, BTreeSet::new(), topics);
        }
        for ((peers, retained), topics) in self.rank0_groups {
            push(RestoreKind::Rank0, peers, retained, topics);
        }
        for ((peers, retained), topics) in self.join_groups {
            push(RestoreKind::Join, peers, retained, topics);
        }
        units
    }
}

/// Returns how many topics of `unit` were left withheld, so the caller can arm
/// a retry instead of deferring writes until the next restart.
async fn process_restore_unit(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
    verified: &BTreeSet<::irokle::TopicId>,
) -> usize {
    match unit.kind {
        RestoreKind::Shared => {
            let missing: Vec<::irokle::TopicId> = unit
                .topics
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
                    .sync_document_topics(missing, unit.peers.clone())
                    .await;
                apply_restored_reconcile(context, node_id, event).await;
            }
            if let Err(error) =
                net_handle.ensure_document_sync_topics(&unit.topics, unit.peers.clone())
            {
                warn!(error = %error, "Failed to ensure shared realm topics on restart");
            }
            let event = net_handle
                .sync_document_topics(unit.topics.clone(), unit.peers.clone())
                .await;
            apply_restored_reconcile(context, node_id, event).await;
            0
        }
        RestoreKind::Rank0 => {
            // An unreachable co-holder might still hold the genesis, so creation
            // is withheld rather than forking a second one.
            let group_withheld = crate::process_placements::ensure_rank0_shard_group(
                context,
                net_handle,
                node_id,
                unit.peers.clone(),
                unit.topics.clone(),
                &unit.retained,
                verified,
            )
            .await;
            // Only topics whose genesis is now local; withheld ones retry later.
            let present: Vec<::irokle::TopicId> = unit
                .topics
                .iter()
                .copied()
                .filter(|topic| {
                    net_handle
                        .document_sync_topic_exists(*topic)
                        .unwrap_or(false)
                })
                .collect();
            let withheld = if group_withheld {
                unit.topics.len().saturating_sub(present.len()).max(1)
            } else {
                0
            };
            if !present.is_empty() {
                let event = net_handle
                    .sync_document_topics(present, unit.peers.clone())
                    .await;
                apply_restored_reconcile(context, node_id, event).await;
            }
            withheld
        }
        RestoreKind::Join => {
            let mut current_holders = unit.peers.clone();
            current_holders.push(node_id);
            current_holders.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            // Install publisher policy before pulling history. Missing topics are
            // expected and get an exact membership pass after a successful join.
            let _ = net_handle
                .reconcile_shard_membership(
                    &unit.topics,
                    current_holders.clone(),
                    &unit.retained,
                    verified,
                )
                .await;
            let event = net_handle
                .sync_document_topics(unit.topics.clone(), unit.peers.clone())
                .await;
            apply_restored_reconcile(context, node_id, event).await;
            let present: Vec<::irokle::TopicId> = unit
                .topics
                .iter()
                .copied()
                .filter(|topic| {
                    net_handle
                        .document_sync_topic_exists(*topic)
                        .unwrap_or(false)
                })
                .collect();
            let mut withheld = unit.topics.len().saturating_sub(present.len());
            if !present.is_empty()
                && let Err(error) = net_handle
                    .reconcile_shard_membership(&present, current_holders, &unit.retained, verified)
                    .await
            {
                warn!(error = %error, "Failed to reconcile joined shard membership on restart");
                withheld += present.len();
            }
            withheld
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{PlacementStrategy, RealmNode, RealmNodeKind};

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

    #[test]
    fn restore_units_chunk() {
        let local = node(1);
        let config = sharded_config(&[local, node(2), node(3)], 256);
        let plan = plan_shard_groups(&config, local, RealmId([7; 32]));
        let held = plan.summary.held_shards;
        let units = plan.into_units();

        assert!(held > 0, "the local node should hold shards");
        assert!(
            units
                .iter()
                .all(|unit| unit.topics.len() <= SHARD_RESTORE_CHUNK_TOPICS),
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
        let units_total = SHARD_RESTORE_UNIT_BUDGET * 2 + 3;
        let mut cursor = ShardRestoreCursor::default();
        let mut visited = Vec::new();
        for _ in 0..3 {
            let (start, end, wrapped) = restore_range(cursor, units_total);
            assert!(end - start <= SHARD_RESTORE_UNIT_BUDGET);
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
    fn recovery_tracks_passes() {
        let status = RecoveryStatus::new();
        assert_eq!(status.snapshot().state, RecoveryState::Pending);
        assert_eq!(status.snapshot().last_progress_timestamp, 0);

        status.begin_pass();
        assert_eq!(status.snapshot().state, RecoveryState::Running);

        status.note_progress(4);
        let snapshot = status.snapshot();
        assert_eq!(snapshot.topics_remaining, 4);
        assert!(snapshot.last_progress_timestamp > 0);

        status.finish_pass(
            RecoveryOutcome::Partial,
            Some(RecoveryError::PeerUnavailable),
        );
        let snapshot = status.snapshot();
        assert_eq!(snapshot.state, RecoveryState::Degraded);
        assert_eq!(snapshot.last_error, Some(RecoveryError::PeerUnavailable));
        assert_eq!(status.pass_total(RecoveryOutcome::Partial), 1);

        status.finish_pass(RecoveryOutcome::Success, None);
        let snapshot = status.snapshot();
        assert_eq!(snapshot.state, RecoveryState::Converged);
        assert_eq!(snapshot.last_error, None);
        assert_eq!(status.pass_total(RecoveryOutcome::Success), 1);
    }

    // Backoff climbs from the base to the cap and never past it, so a peer that
    // stays down cannot keep the driver hot.
    #[test]
    fn recovery_backoff_caps() {
        assert_eq!(recovery_backoff(0), RECOVERY_RETRY_BASE);
        assert!(recovery_backoff(1) > recovery_backoff(0));
        assert_eq!(recovery_backoff(40), RECOVERY_RETRY_MAX);
    }

    #[test]
    fn shared_peers_filter() {
        let self_id = node(1);
        let management = node(2);
        let server = node(3);
        let local = node(4);
        let user = node(5);
        let mut config = RealmConfigDocument::default_for_realm(RealmId([9; 32]), Vec::new());
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
                kind: RealmNodeKind::User,
            },
            RealmNode {
                node_id: "malformed-eligible-id".to_string(),
                kind: RealmNodeKind::Server,
            },
            RealmNode {
                node_id: server.to_string(),
                kind: RealmNodeKind::Server,
            },
            RealmNode {
                node_id: local.to_string(),
                kind: RealmNodeKind::Local,
            },
        ];

        assert_eq!(
            shared_topic_peers(&config, self_id),
            vec![management, server, local]
        );
    }
}
