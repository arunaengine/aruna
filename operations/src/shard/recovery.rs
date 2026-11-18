//! Holds the shared remote-recovery status that the recovery driver updates and readiness and
//! metrics read, and decides after each pass whether recovery converged, continues or retries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use aruna_core::time::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;

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

    pub(crate) fn merge(current: Option<Self>, next: Option<Self>) -> Option<Self> {
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

/// Two handles are equal when they share one status.
impl PartialEq for RecoveryStatus {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
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

    /// The last progress time in milliseconds, finer than the snapshot's seconds.
    #[cfg(test)]
    pub(crate) fn progress_ms(&self) -> u64 {
        self.inner.last_progress_ms.load(Ordering::Relaxed)
    }

    pub fn pass_total(&self, outcome: RecoveryOutcome) -> u64 {
        self.inner.passes[outcome.index()].load(Ordering::Relaxed)
    }

    /// Marks a bounded pass as started.
    pub(crate) fn begin_pass(&self) {
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

    pub(crate) fn set_remaining(&self, topics_remaining: u64) {
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

/// Asks a running recovery to stop at its next step; restore work stops between units.
#[derive(Clone, Debug, Default)]
pub struct RecoveryStop(CancellationToken);

impl RecoveryStop {
    pub fn new(token: CancellationToken) -> Self {
        Self(token)
    }

    pub(crate) fn requested(&self) -> bool {
        self.0.is_cancelled()
    }
}

impl PartialEq for RecoveryStop {
    fn eq(&self, other: &Self) -> bool {
        self.requested() == other.requested()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct RecoveryPass {
    pub(crate) unresolved_topics: BTreeSet<::irokle::TopicId>,
    pub(crate) unvisited_topics: usize,
    pub(crate) error: Option<RecoveryError>,
    /// Unvisited units are local work, not a peer problem: retry at once.
    pub(crate) more_local_work: bool,
    pub(crate) plan_changed: bool,
    pub(crate) topics_completed: usize,
    phase_progress: bool,
    /// False when the pass could not enumerate outstanding work.
    pub(crate) enumerated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecoveryFailure {
    pub(crate) unresolved_topics: BTreeSet<::irokle::TopicId>,
    pub(crate) error: Option<RecoveryError>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RecoveryBaseline {
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

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct RecoveryRotation {
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
pub(crate) enum RecoveryDecision {
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

pub(crate) fn apply_recovery_pass(
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

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct PhaseOutcome {
    pub(crate) progress: bool,
    pub(crate) error: Option<RecoveryError>,
}

impl RecoveryPass {
    pub(crate) fn merge_phase(&mut self, outcome: PhaseOutcome) {
        self.phase_progress |= outcome.progress;
        self.error = RecoveryError::merge(self.error, outcome.error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
