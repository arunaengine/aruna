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
    let mut previous_failure: Option<RecoveryFailure> = None;
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
            &mut previous_failure,
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
    let topics_remaining = match &decision {
        RecoveryDecision::ContinueLocal {
            topics_remaining, ..
        } => *topics_remaining,
        RecoveryDecision::Converged => 0,
        RecoveryDecision::Retry { failure, .. } => failure.unresolved_topics.len() as u64,
    };
    status.set_remaining(topics_remaining);
    match decision {
        RecoveryDecision::ContinueLocal {
            progress, error, ..
        } => {
            if progress {
                status.note_progress(topics_remaining);
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
        RecoveryDecision::Retry { failure, progress } => {
            if progress {
                status.note_progress(topics_remaining);
                *attempts = 0;
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
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RecoveryFailure {
    unresolved_topics: BTreeSet<::irokle::TopicId>,
    error: Option<RecoveryError>,
}

impl RecoveryFailure {
    fn improves(&self, previous: &Self) -> bool {
        self.unresolved_topics.len() < previous.unresolved_topics.len()
            || previous.error.is_some() && self.error.is_none()
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
    },
}

fn apply_recovery_pass(
    rotation: &mut RecoveryRotation,
    previous_failure: &mut Option<RecoveryFailure>,
    first_rotation: &mut bool,
    pass: &RecoveryPass,
) -> RecoveryDecision {
    if pass.plan_changed {
        *rotation = RecoveryRotation::default();
        *previous_failure = None;
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

    let failure = RecoveryFailure {
        unresolved_topics: rotation.unresolved_topics.clone(),
        error: rotation.error,
    };
    if failure.unresolved_topics.is_empty() && failure.error.is_none() {
        return RecoveryDecision::Converged;
    }

    let improved = previous_failure
        .as_ref()
        .is_some_and(|previous| failure.improves(previous));
    let progress = pass_progress || improved;
    *previous_failure = Some(failure.clone());
    *rotation = RecoveryRotation::default();
    *first_rotation = false;
    RecoveryDecision::Retry { failure, progress }
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
    if pass.plan_changed {
        *reconcile_placements = true;
    }
    // Convergence needs the cursor to have walked every unit.
    if !restore.wrapped {
        pass.more_local_work = true;
        return pass;
    }
    if cancelled.is_cancelled() {
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
    let placements = crate::process_placements::process_shard_placements(
        context,
        config.realm_id,
        config.node_id,
    )
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
                ..ShardRestorePass::default()
            };
        }
        RealmConfigLoad::StorageFailure => {
            *cursor = ShardRestoreCursor::default();
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
            ..ShardRestorePass::default()
        };
    };
    let plan = plan_shard_groups(&config, node_id, realm_id);
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
            ..ShardRestorePass::default()
        };
    }

    // Former-holder history cutoffs are frozen only for shards a prior run
    // durably verified; join verification below happens after this restore.
    let verified = crate::shard::verify::load_verified_shard_topics(context, realm_id).await;

    let (start, end, wrapped) = restore_range(*cursor, units_total);
    let batch = restore_batch(
        context,
        &net_handle,
        node_id,
        &units,
        start,
        end,
        wrapped,
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
    start: usize,
    end: usize,
    wrapped: bool,
    verified: &BTreeSet<::irokle::TopicId>,
    cancelled: &CancellationToken,
) -> RestoreBatch {
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
    let RealmConfigLoad::Found(config) = load_realm_config(context, realm_id).await else {
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
            .sync_document_topics(missing.clone(), unit.peers.clone())
            .await;
        if !apply_restored_reconcile(context, node_id, event).await {
            outcome.fail_topics(missing);
        }
    }
    if let Err(error) = net_handle.ensure_document_sync_topics(&unit.topics, unit.peers.clone()) {
        warn!(error = %error, "Failed to ensure shared realm topics on restart");
        outcome.fail_topics(unit.topics.iter().copied());
    }
    let event = net_handle
        .sync_document_topics(unit.topics.clone(), unit.peers.clone())
        .await;
    if !apply_restored_reconcile(context, node_id, event).await {
        outcome.fail_topics(unit.topics.iter().copied());
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
    let mut current_holders = unit.peers.clone();
    current_holders.push(node_id);
    current_holders.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    if let Err(error) = net_handle
        .reconcile_shard_membership(
            &unit.topics,
            current_holders.clone(),
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
            .reconcile_shard_membership(&present, current_holders, &unit.retained, verified)
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
    use aruna_core::structs::{PlacementStrategy, RealmNode, RealmNodeKind};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage::FjallStorage;
    use tempfile::tempdir;

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
    fn cursor_rebinds_plan() {
        let topic = |seed| ::irokle::TopicId::from_bytes([seed; 32]);
        let unit = |seed| RestoreUnit {
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
        let original = plan_shard_groups(&config, local, RealmId([7; 32])).into_units();
        let mut cursor = ShardRestoreCursor::default();
        assert!(bind_restore_plan(&mut cursor, &original));
        cursor.next_unit = 1;

        config.strategies[0].shard_count = 32;
        let changed = plan_shard_groups(&config, local, RealmId([7; 32])).into_units();
        assert!(bind_restore_plan(&mut cursor, &changed));
        assert_eq!(cursor.next_unit, 0);
    }

    #[test]
    fn rotation_visits_suffix() {
        let topic = ::irokle::TopicId::from_bytes([1; 32]);
        let mut rotation = RecoveryRotation::default();
        let mut previous = None;
        let mut first = true;
        let blocked = apply_recovery_pass(
            &mut rotation,
            &mut previous,
            &mut first,
            &RecoveryPass {
                unresolved_topics: BTreeSet::from([topic]),
                error: Some(RecoveryError::PeerUnavailable),
                more_local_work: true,
                units_processed: SHARD_RESTORE_UNIT_BUDGET,
                topics_completed: SHARD_RESTORE_UNIT_BUDGET - 1,
                unvisited_topics: 3,
                ..RecoveryPass::default()
            },
        );
        assert_eq!(
            blocked,
            RecoveryDecision::ContinueLocal {
                progress: true,
                topics_remaining: 4,
                error: Some(RecoveryError::PeerUnavailable),
            }
        );

        let suffix = apply_recovery_pass(
            &mut rotation,
            &mut previous,
            &mut first,
            &RecoveryPass {
                units_processed: 3,
                topics_completed: 3,
                ..RecoveryPass::default()
            },
        );
        assert!(matches!(
            suffix,
            RecoveryDecision::Retry {
                failure: RecoveryFailure {
                    error: Some(RecoveryError::PeerUnavailable),
                    ..
                },
                progress: true,
            }
        ));
    }

    #[test]
    fn heal_needs_rotation() {
        let topic = ::irokle::TopicId::from_bytes([2; 32]);
        let mut rotation = RecoveryRotation::default();
        let mut previous = None;
        let mut first = true;
        let blocked = apply_recovery_pass(
            &mut rotation,
            &mut previous,
            &mut first,
            &RecoveryPass {
                unresolved_topics: BTreeSet::from([topic]),
                error: Some(RecoveryError::PeerUnavailable),
                units_processed: 1,
                ..RecoveryPass::default()
            },
        );
        assert!(matches!(blocked, RecoveryDecision::Retry { .. }));
        let healed = apply_recovery_pass(
            &mut rotation,
            &mut previous,
            &mut first,
            &RecoveryPass {
                units_processed: 1,
                ..RecoveryPass::default()
            },
        );
        assert_eq!(healed, RecoveryDecision::Converged);
    }

    #[test]
    fn failed_work_stalls() {
        let topic = ::irokle::TopicId::from_bytes([3; 32]);
        let mut rotation = RecoveryRotation::default();
        let mut previous = None;
        let mut first = true;
        let decision = apply_recovery_pass(
            &mut rotation,
            &mut previous,
            &mut first,
            &RecoveryPass {
                unresolved_topics: BTreeSet::from([topic]),
                error: Some(RecoveryError::PeerUnavailable),
                units_processed: 1,
                ..RecoveryPass::default()
            },
        );

        assert!(matches!(
            decision,
            RecoveryDecision::Retry {
                progress: false,
                ..
            }
        ));
    }

    #[test]
    fn phase_failure_visible() {
        let mut pass = RecoveryPass::default();
        pass.merge_phase(PhaseOutcome {
            progress: false,
            error: Some(RecoveryError::PeerUnavailable),
        });
        pass.merge_phase(PhaseOutcome {
            progress: false,
            error: Some(RecoveryError::Storage),
        });
        let mut rotation = RecoveryRotation::default();
        let mut previous = None;
        let mut first = false;
        let decision = apply_recovery_pass(&mut rotation, &mut previous, &mut first, &pass);
        assert!(matches!(
            decision,
            RecoveryDecision::Retry {
                failure: RecoveryFailure {
                    error: Some(RecoveryError::Storage),
                    ..
                },
                ..
            }
        ));
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
            0,
            1,
            false,
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
        let pass = restore_shard_pass(
            &context,
            node(1),
            realm_id,
            &mut ShardRestoreCursor::default(),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(pass.error, Some(RecoveryError::Storage));
        assert!(pass.wrapped);
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

    #[test]
    fn failed_pass_stalls() {
        let status = RecoveryStatus::new();
        status.begin_pass();
        status.note_progress(1);
        let progress = status
            .inner
            .last_progress_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        status.begin_pass();
        status.set_remaining(1);
        status.finish_pass(
            RecoveryOutcome::Partial,
            Some(RecoveryError::PeerUnavailable),
        );
        assert_eq!(
            status
                .inner
                .last_progress_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            progress
        );
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
