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
pub const SHARED_RESTORE_TOPIC_COUNT: usize = 5;

fn shared_targets(
    realm_id: RealmId,
    node_id: NodeId,
) -> [DocumentSyncTarget; SHARED_RESTORE_TOPIC_COUNT] {
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

    fn merge(current: Option<Self>, next: Option<Self>) -> Option<Self> {
        match (current, next) {
            (Some(Self::Panicked), _) | (_, Some(Self::Panicked)) => Some(Self::Panicked),
            (Some(Self::Storage), _) | (_, Some(Self::Storage)) => Some(Self::Storage),
            (Some(Self::PeerUnavailable), _) | (_, Some(Self::PeerUnavailable)) => {
                Some(Self::PeerUnavailable)
            }
            (None, None) => None,
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
    /// Unix seconds when recovery started or last made measurable progress.
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
        let _ = self.inner.last_progress_ms.compare_exchange(
            0,
            unix_timestamp_millis(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    /// Publishes progress after a completed work unit.
    pub fn note_progress(&self, topics_remaining: u64) {
        self.set_remaining(topics_remaining);
        self.inner
            .last_progress_ms
            .store(unix_timestamp_millis(), Ordering::Relaxed);
    }

    fn set_remaining(&self, topics_remaining: u64) {
        self.inner
            .topics_remaining
            .store(topics_remaining, Ordering::Relaxed);
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
            let after = recovery_backoff(*attempts);
            *attempts = (*attempts).saturating_add(1);
            tokio::select! {
                _ = cancelled.cancelled() => true,
                _ = tokio::time::sleep(after) => false,
            }
        }
    }
}

/// Doubles from [`RECOVERY_RETRY_BASE`] up to [`RECOVERY_RETRY_MAX`].
fn recovery_backoff(attempts: u32) -> Duration {
    let base = RECOVERY_RETRY_BASE.as_millis() as u64;
    let max = RECOVERY_RETRY_MAX.as_millis() as u64;
    Duration::from_millis(base.saturating_mul(1u64 << attempts.min(16)).min(max))
}

#[derive(Clone, Debug, Default)]
struct RecoveryPass {
    unresolved_topics: BTreeSet<::irokle::TopicId>,
    unvisited_topics: usize,
    error: Option<RecoveryError>,
    /// Unvisited units are local work, not a peer problem: retry at once.
    more_local_work: bool,
    plan_changed: bool,
    units_processed: usize,
    topics_completed: usize,
    phase_progress: bool,
    /// False when the pass could not enumerate outstanding work.
    enumerated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RecoveryFailure {
    unresolved_topics: BTreeSet<::irokle::TopicId>,
    error: Option<RecoveryError>,
}

#[derive(Clone, Copy, Debug)]
struct RecoveryBaseline {
    fewest_topics: usize,
    error_cleared: bool,
}

impl RecoveryBaseline {
    fn new(failure: &RecoveryFailure) -> Self {
        Self {
            fewest_topics: failure.unresolved_topics.len(),
            error_cleared: failure.error.is_none(),
        }
    }

    fn note_failure(&mut self, failure: &RecoveryFailure) -> bool {
        let topics = failure.unresolved_topics.len();
        let improved =
            topics < self.fewest_topics || !self.error_cleared && failure.error.is_none();
        self.fewest_topics = self.fewest_topics.min(topics);
        self.error_cleared |= failure.error.is_none();
        improved
    }
}

#[derive(Clone, Debug, Default)]
struct RecoveryRotation {
    unresolved_topics: BTreeSet<::irokle::TopicId>,
    error: Option<RecoveryError>,
}

impl RecoveryRotation {
    fn merge(&mut self, pass: &RecoveryPass) {
        self.unresolved_topics
            .extend(pass.unresolved_topics.iter().copied());
        self.error = RecoveryError::merge(self.error, pass.error);
    }

    fn remaining_topics(&self, unvisited_topics: usize) -> u64 {
        self.unresolved_topics
            .len()
            .saturating_add(unvisited_topics) as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RecoveryDecision {
    ContinueLocal {
        progress: bool,
        topics_remaining: u64,
        error: Option<RecoveryError>,
    },
    Converged,
    Retry {
        failure: RecoveryFailure,
        progress: bool,
        /// False when the pass never measured outstanding work, so the
        /// published remaining count must keep its last enumerated value.
        enumerated: bool,
    },
}

fn apply_recovery_pass(
    rotation: &mut RecoveryRotation,
    recovery_baseline: &mut Option<RecoveryBaseline>,
    first_rotation: &mut bool,
    pass: &RecoveryPass,
) -> RecoveryDecision {
    if pass.plan_changed {
        *rotation = RecoveryRotation::default();
        *recovery_baseline = None;
        *first_rotation = true;
    }
    rotation.merge(pass);
    let topics_remaining = rotation.remaining_topics(pass.unvisited_topics);
    let pass_progress = pass.phase_progress || *first_rotation && pass.topics_completed > 0;
    if pass.more_local_work {
        return RecoveryDecision::ContinueLocal {
            progress: pass_progress,
            topics_remaining,
            error: rotation.error,
        };
    }
    // A pass that failed to enumerate work observed nothing: keep the
    // rotation and baseline, and retry without claiming progress.
    if !pass.enumerated {
        return RecoveryDecision::Retry {
            failure: RecoveryFailure {
                unresolved_topics: rotation.unresolved_topics.clone(),
                error: rotation.error,
            },
            progress: false,
            enumerated: false,
        };
    }

    let failure = RecoveryFailure {
        unresolved_topics: rotation.unresolved_topics.clone(),
        error: rotation.error,
    };
    if failure.unresolved_topics.is_empty() && failure.error.is_none() {
        return RecoveryDecision::Converged;
    }

    let improved = match recovery_baseline {
        Some(baseline) => baseline.note_failure(&failure),
        None => {
            *recovery_baseline = Some(RecoveryBaseline::new(&failure));
            false
        }
    };
    let progress = pass_progress || improved;
    *rotation = RecoveryRotation::default();
    *first_rotation = false;
    RecoveryDecision::Retry {
        failure,
        progress,
        enumerated: true,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PhaseOutcome {
    progress: bool,
    error: Option<RecoveryError>,
}

impl RecoveryPass {
    fn merge_phase(&mut self, outcome: PhaseOutcome) {
        self.phase_progress |= outcome.progress;
        self.error = RecoveryError::merge(self.error, outcome.error);
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
    pass.units_processed = restore.units_processed;
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
    // Transition steps run on the SyncPlacements timer instead of inline:
    // recovery must not gate presence and readiness on a cluster-wide
    // transition draining.
    let placements =
        crate::process_placements::reconcile_shard_topics(context, config.realm_id, config.node_id)
            .await;
    let outcome = match placements.status {
        crate::process_placements::PlacementReconcileStatus::Clean => {
            *pending = false;
            PhaseOutcome {
                progress: true,
                error: None,
            }
        }
        crate::process_placements::PlacementReconcileStatus::RetryScheduled => PhaseOutcome {
            progress: false,
            error: Some(RecoveryError::PeerUnavailable),
        },
        crate::process_placements::PlacementReconcileStatus::StorageFailure => PhaseOutcome {
            progress: false,
            error: Some(RecoveryError::Storage),
        },
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
    let result = crate::driver::drive(
        crate::announce_realm_presence::AnnounceRealmPresenceOperation::new(
            crate::announce_realm_presence::AnnounceRealmPresenceConfig {
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
    let result = crate::usage_stats::publish_and_refresh_usage_snapshots(
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

/// Where a bounded restore resumes, bound to the exact current work plan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardRestoreCursor {
    next_unit: usize,
    plan_hash: Option<[u8; 32]>,
}

/// What one bounded [`restore_shard_pass`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardRestorePass {
    pub summary: RestoreShardSummary,
    /// Never more than [`SHARD_RESTORE_UNIT_BUDGET`].
    pub units_processed: usize,
    pub topics_completed: usize,
    pub units_total: usize,
    pub unvisited_topics: usize,
    pub unresolved_topics: BTreeSet<::irokle::TopicId>,
    pub error: Option<RecoveryError>,
    pub plan_changed: bool,
    /// Whether the cursor reached the end and wrapped back to the head.
    pub wrapped: bool,
    /// False when storage failed before the plan could be read: the pass
    /// observed nothing about outstanding work.
    pub enumerated: bool,
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
    let verified = crate::shard::verify::load_verified_shard_topics(context, realm_id).await;

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

    // A withheld genesis (co-holder down or refusing) has no placement record to
    // drive a re-run, so arm the retry timer here — the reconciler re-probes when
    // the co-holder returns instead of deferring writes at 1s until restart.
    if !unresolved_topics.is_empty()
        && let Some(task_handle) = context.task_handle.as_ref()
    {
        let effect = crate::sync_placement::schedule_placement_retry_effect(realm_id, node_id);
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

type ShardGroup = (Vec<NodeId>, Vec<NodeId>, BTreeSet<NodeId>);

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
    now_ms: u64,
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
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
            // Restart is transition-aware: a target or retained departing
            // holder stays in its bucket's membership, publishers stay the
            // authority set.
            let membership = crate::placement::bucket_membership(config, &placement, now_ms);
            if !membership.members.contains(&node_id) {
                continue;
            }
            plan.summary.held_shards += 1;
            let local_is_rank0 = holders.first() == Some(&node_id);
            let mut co_members: Vec<NodeId> = membership
                .members
                .into_iter()
                .filter(|candidate| *candidate != node_id)
                .collect();
            co_members.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            if co_members.is_empty() {
                continue;
            }
            let mut publishers = membership.publishers;
            publishers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
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
                .entry((co_members, publishers, retained))
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
    let RealmConfigLoad::Found(config) = load_realm_config(context, realm_id).await else {
        return RestoreShardSummary::default();
    };
    let plan = plan_shard_groups(&config, node_id, realm_id, unix_timestamp_millis());
    let verified = crate::shard::verify::load_verified_shard_topics(context, realm_id).await;

    let mut prepared = 0usize;
    for ((co_members, publishers, retained), topics) in
        plan.rank0_groups.iter().chain(plan.join_groups.iter())
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

/// The half-open unit range one bounded pass processes, and whether it wraps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RestoreRange {
    start: usize,
    end: usize,
    wrapped: bool,
}

fn restore_range(cursor: ShardRestoreCursor, units_total: usize) -> RestoreRange {
    let start = cursor.next_unit.min(units_total);
    let end = (start + SHARD_RESTORE_UNIT_BUDGET).min(units_total);
    RestoreRange {
        start,
        end,
        wrapped: end >= units_total,
    }
}

/// One bounded unit of restore work: one co-holder group's ensure and sync over
/// a chunk of topics.
#[derive(Clone, Debug)]
struct RestoreUnit {
    kind: RestoreKind,
    peers: Vec<NodeId>,
    publishers: Vec<NodeId>,
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

fn bind_restore_plan(cursor: &mut ShardRestoreCursor, units: &[RestoreUnit]) -> bool {
    let plan_hash = restore_plan_hash(units);
    let changed = cursor.plan_hash != Some(plan_hash);
    if changed {
        cursor.next_unit = 0;
        cursor.plan_hash = Some(plan_hash);
    }
    changed
}

fn restore_plan_hash(units: &[RestoreUnit]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for unit in units {
        let kind = match unit.kind {
            RestoreKind::Shared => 0,
            RestoreKind::Rank0 => 1,
            RestoreKind::Join => 2,
        };
        hasher.update(&[kind]);
        hasher.update(&(unit.peers.len() as u64).to_be_bytes());
        for peer in &unit.peers {
            hasher.update(peer.as_bytes());
        }
        hasher.update(&(unit.publishers.len() as u64).to_be_bytes());
        for peer in &unit.publishers {
            hasher.update(peer.as_bytes());
        }
        hasher.update(&(unit.retained.len() as u64).to_be_bytes());
        for peer in &unit.retained {
            hasher.update(peer.as_bytes());
        }
        hasher.update(&(unit.topics.len() as u64).to_be_bytes());
        for topic in &unit.topics {
            hasher.update(topic.as_bytes());
        }
    }
    *hasher.finalize().as_bytes()
}

impl ShardPlan {
    /// Flattens the plan into a deterministic list of bounded work units.
    fn into_units(self) -> Vec<RestoreUnit> {
        let mut units = Vec::new();
        let mut push = |kind,
                        peers: Vec<NodeId>,
                        publishers: Vec<NodeId>,
                        retained: BTreeSet<NodeId>,
                        topics: Vec<_>| {
            if peers.is_empty() || topics.is_empty() {
                return;
            }
            for chunk in topics.chunks(SHARD_RESTORE_CHUNK_TOPICS) {
                units.push(RestoreUnit {
                    kind,
                    peers: peers.clone(),
                    publishers: publishers.clone(),
                    retained: retained.clone(),
                    topics: chunk.to_vec(),
                });
            }
        };
        for (peers, topics) in self.shared_groups {
            push(
                RestoreKind::Shared,
                peers,
                Vec::new(),
                BTreeSet::new(),
                topics,
            );
        }
        for ((peers, publishers, retained), topics) in self.rank0_groups {
            push(RestoreKind::Rank0, peers, publishers, retained, topics);
        }
        for ((peers, publishers, retained), topics) in self.join_groups {
            push(RestoreKind::Join, peers, publishers, retained, topics);
        }
        units
    }
}

#[derive(Default)]
struct RestoreUnitOutcome {
    unresolved_topics: BTreeSet<::irokle::TopicId>,
    error: Option<RecoveryError>,
}

impl RestoreUnitOutcome {
    fn fail_topics(&mut self, topics: impl IntoIterator<Item = ::irokle::TopicId>) {
        let before = self.unresolved_topics.len();
        self.unresolved_topics.extend(topics);
        if self.unresolved_topics.len() > before {
            self.error = RecoveryError::merge(self.error, Some(RecoveryError::PeerUnavailable));
        }
    }
}

async fn process_restore_unit(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
    verified: &BTreeSet<::irokle::TopicId>,
) -> RestoreUnitOutcome {
    match unit.kind {
        RestoreKind::Shared => restore_shared(context, net_handle, node_id, unit).await,
        RestoreKind::Rank0 => restore_rank0(context, net_handle, node_id, unit, verified).await,
        RestoreKind::Join => restore_join(context, net_handle, node_id, unit, verified).await,
    }
}

async fn restore_shared(
    context: &Arc<DriverContext>,
    net_handle: &aruna_net::NetHandle,
    node_id: NodeId,
    unit: &RestoreUnit,
) -> RestoreUnitOutcome {
    let mut outcome = RestoreUnitOutcome::default();
    // A peer that is unreachable, or that refuses a topic it already holds,
    // might hold this genesis; minting a second one forks the realm document
    // permanently. Withheld topics stay unresolved so recovery retries them.
    let (to_ensure, withheld) = crate::process_placements::resolve_creatable_topics(
        context,
        net_handle,
        node_id,
        &unit.peers,
        unit.topics.clone(),
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
    if let Err(error) = net_handle.ensure_document_sync_topics(&to_ensure, unit.peers.clone()) {
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
    let group_withheld = crate::process_placements::ensure_rank0_shard_group(
        context,
        net_handle,
        node_id,
        unit.peers.clone(),
        unit.publishers.clone(),
        unit.topics.clone(),
        &unit.retained,
        verified,
    )
    .await;
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
        .filter(|topic| {
            net_handle
                .document_sync_topic_exists(*topic)
                .unwrap_or(false)
        })
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
    event: DocumentSyncNetEvent,
) -> bool {
    let result = match event {
        DocumentSyncNetEvent::DocumentsReconciled {
            applied,
            targets,
            metadata_create_events,
            metadata_graph_tombstones,
        } => {
            if applied == 0 {
                return true;
            }
            DocumentSyncReconcileResult {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
            }
        }
        DocumentSyncNetEvent::Error { error, .. } => {
            warn!(error = %error, "Failed to sync held shard topics on restart");
            return false;
        }
        other => {
            warn!(event = ?other, "Unexpected restart shard sync result");
            return false;
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
    true
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

async fn load_realm_config(context: &Arc<DriverContext>, realm_id: RealmId) -> RealmConfigLoad {
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
            Ok(config) => RealmConfigLoad::Found(config),
            Err(error) => {
                warn!(%realm_id, error = %error, "Failed to decode realm config for recovery");
                RealmConfigLoad::StorageFailure
            }
        },
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => RealmConfigLoad::Absent,
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(%realm_id, error = %error, "Failed to read realm config for recovery");
            RealmConfigLoad::StorageFailure
        }
        other => {
            warn!(%realm_id, event = ?other, "Unexpected realm config read result for recovery");
            RealmConfigLoad::StorageFailure
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{
        Actor, PlacementOverride, PlacementStrategy, RealmNode, RealmNodeKind,
    };
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    const RECOVERY_TEST_GUARD: Duration = Duration::from_secs(5 * 60);

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
        let plan = plan_shard_groups(&config, local, RealmId([7; 32]), 0);
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
            let RestoreRange {
                start,
                end,
                wrapped,
            } = restore_range(cursor, units_total);
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
        add_user.ensure_node(node(4), RealmNodeKind::User);
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
                document_sync_storage_path: Some(dir.path().join("document-sync")),
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
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        crate::incoming::initialize_net_incoming(context.clone());
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
                subject: crate::placement::shard_subject_bytes(&placement),
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
        let target = DocumentSyncTarget::RealmConfig { realm_id };
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
            .refresh_realm_peers_from_document(config)
            .await
            .expect("realm peers refresh");
    }

    async fn mesh_nodes(first: &RecoveryNode, second: &RecoveryNode) {
        first.net.add_peer_addr(second.net.endpoint_addr()).await;
        second.net.add_peer_addr(first.net.endpoint_addr()).await;
    }

    fn seed_topic(node: &RecoveryNode, topic: ::irokle::TopicId, peers: &[NodeId]) {
        node.net
            .ensure_document_sync_topics(&[topic], peers.to_vec())
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

    /// Advances virtual time only while the driver sleeps between passes, so
    /// deadlines guarding real work inside a pass can never fire spuriously.
    async fn advance_quiet(status: &RecoveryStatus, step: Duration) {
        tokio::task::yield_now().await;
        if status.snapshot().state == RecoveryState::Degraded {
            tokio::time::advance(step).await;
        }
    }

    async fn wait_state(status: &RecoveryStatus, expected: RecoveryState) {
        let wait = async {
            loop {
                if status.snapshot().state == expected {
                    return;
                }
                advance_quiet(status, Duration::from_millis(100)).await;
            }
        };
        tokio::time::timeout(RECOVERY_TEST_GUARD, wait)
            .await
            .unwrap_or_else(|_| panic!("recovery did not reach {expected:?}"));
    }

    async fn wait_partial(status: &RecoveryStatus, target: u64) {
        let wait = async {
            loop {
                if status.pass_total(RecoveryOutcome::Partial) >= target {
                    return;
                }
                advance_quiet(status, Duration::from_millis(100)).await;
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
            loop {
                let partial = status.pass_total(RecoveryOutcome::Partial);
                if partial <= observed || status.snapshot().state != RecoveryState::Degraded {
                    advance_quiet(status, Duration::from_millis(100)).await;
                    continue;
                }
                advance_quiet(status, RECOVERY_RETRY_BASE / 2).await;
                if status.snapshot().state == RecoveryState::Degraded
                    && status.pass_total(RecoveryOutcome::Partial) == partial
                {
                    observed = partial;
                    let progress = status
                        .inner
                        .last_progress_ms
                        .load(std::sync::atomic::Ordering::Relaxed);
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
        seed_topic(&blocked, blocked_topic, &[blocked.net.node_id()]);
        seed_topic(&healthy, healthy_topic, &[local.net.node_id()]);
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
                .document_sync_topic_exists(healthy_topic)
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
        seed_topic(&blocked, topic, &[blocked.net.node_id()]);

        tokio::time::pause();
        let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
        wait_state(&status, RecoveryState::Degraded).await;
        assert!(status.snapshot().topics_remaining > 0);
        assert!(
            !local
                .net
                .document_sync_topic_exists(topic)
                .expect("local topic lookup")
        );

        mesh_nodes(&local, &blocked).await;
        crate::process_placements::process_shard_placements(
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
                .document_sync_topic_exists(topic)
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
                key: aruna_core::structs::usage_group_key(group_id).into(),
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

    #[test]
    fn remaining_topics_exact() {
        let first = ::irokle::TopicId::from_bytes([3; 32]);
        let second = ::irokle::TopicId::from_bytes([4; 32]);
        let mut rotation = RecoveryRotation::default();
        rotation.merge(&RecoveryPass {
            unresolved_topics: BTreeSet::from([first, second]),
            ..RecoveryPass::default()
        });
        rotation.merge(&RecoveryPass {
            unresolved_topics: BTreeSet::from([first]),
            ..RecoveryPass::default()
        });
        assert_eq!(rotation.remaining_topics(5), 7);
    }

    #[test]
    fn baseline_is_monotonic() {
        let first = ::irokle::TopicId::from_bytes([5; 32]);
        let second = ::irokle::TopicId::from_bytes([6; 32]);
        let failure = |topics| RecoveryFailure {
            unresolved_topics: topics,
            error: Some(RecoveryError::PeerUnavailable),
        };
        let mut baseline = RecoveryBaseline::new(&failure(BTreeSet::from([first])));

        assert!(!baseline.note_failure(&failure(BTreeSet::from([first]))));
        assert!(!baseline.note_failure(&failure(BTreeSet::from([first, second]))));
        assert!(baseline.note_failure(&failure(BTreeSet::new())));
        assert!(!baseline.note_failure(&failure(BTreeSet::from([first]))));

        let cleared = RecoveryFailure {
            unresolved_topics: BTreeSet::from([first]),
            error: None,
        };
        assert!(baseline.note_failure(&cleared));
        assert!(!baseline.note_failure(&cleared));
    }

    #[test]
    fn storage_fail_stalls() {
        // A pass that could not enumerate work must not claim progress or
        // wipe the baseline, so the stall signal survives storage hiccups.
        let topic = ::irokle::TopicId::from_bytes([8; 32]);
        let mut rotation = RecoveryRotation::default();
        let mut baseline = None;
        let mut first_rotation = true;
        let stalled = RecoveryPass {
            unresolved_topics: BTreeSet::from([topic]),
            error: Some(RecoveryError::PeerUnavailable),
            enumerated: true,
            ..RecoveryPass::default()
        };
        let seeded =
            apply_recovery_pass(&mut rotation, &mut baseline, &mut first_rotation, &stalled);
        assert!(matches!(
            seeded,
            RecoveryDecision::Retry {
                progress: false,
                ..
            }
        ));

        let blind = RecoveryPass {
            error: Some(RecoveryError::Storage),
            ..RecoveryPass::default()
        };
        let decision =
            apply_recovery_pass(&mut rotation, &mut baseline, &mut first_rotation, &blind);
        assert_eq!(
            decision,
            RecoveryDecision::Retry {
                failure: RecoveryFailure {
                    unresolved_topics: BTreeSet::new(),
                    error: Some(RecoveryError::Storage),
                },
                progress: false,
                enumerated: false,
            }
        );

        let repeat =
            apply_recovery_pass(&mut rotation, &mut baseline, &mut first_rotation, &stalled);
        assert!(matches!(
            repeat,
            RecoveryDecision::Retry {
                progress: false,
                enumerated: true,
                ..
            }
        ));
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
            task_handle: None,
            compute_handle: None,
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
        let target = DocumentSyncTarget::RealmConfig { realm_id };
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
            task_handle: None,
            compute_handle: None,
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

    #[test]
    fn rotation_keeps_failure() {
        let first = ::irokle::TopicId::from_bytes([1; 32]);
        let mut rotation = RecoveryRotation::default();
        rotation.merge(&RecoveryPass {
            unresolved_topics: BTreeSet::from([first]),
            error: Some(RecoveryError::PeerUnavailable),
            more_local_work: true,
            ..RecoveryPass::default()
        });
        rotation.merge(&RecoveryPass::default());

        assert_eq!(rotation.unresolved_topics, BTreeSet::from([first]));
        assert_eq!(rotation.error, Some(RecoveryError::PeerUnavailable));
    }

    #[test]
    fn recovery_tracks_passes() {
        let status = RecoveryStatus::new();
        assert_eq!(status.snapshot().state, RecoveryState::Pending);
        assert_eq!(status.snapshot().last_progress_timestamp, 0);

        status.begin_pass();
        let started = status.snapshot();
        assert_eq!(started.state, RecoveryState::Running);
        assert!(started.last_progress_timestamp > 0);

        status.set_remaining(5);
        let unchanged = status.snapshot();
        assert_eq!(unchanged.topics_remaining, 5);
        assert_eq!(
            unchanged.last_progress_timestamp,
            started.last_progress_timestamp
        );

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
        seed_topic(&blocked, topic, &[blocked.net.node_id()]);

        tokio::time::pause();
        let (status, cancelled, driver) = spawn_driver(&local, realm_id, false);
        let (partial, progress, remaining) = wait_stall(&status).await;
        // A stalled recovery must keep reporting its outstanding topics.
        assert!(remaining > 0);

        wait_partial(&status, partial.saturating_add(3)).await;
        assert_eq!(
            status
                .inner
                .last_progress_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            progress
        );
        assert_eq!(status.snapshot().topics_remaining, remaining);

        finish_driver(cancelled, driver).await;
        stop_node(local).await;
        stop_node(blocked).await;
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
