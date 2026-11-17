//! Holds the shared remote-recovery status that the recovery driver updates and readiness and
//! metrics read: its state, remaining topics, last error and pass counts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use aruna_core::time::unix_timestamp_millis;

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

    pub(super) fn merge(current: Option<Self>, next: Option<Self>) -> Option<Self> {
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
    pub(super) fn begin_pass(&self) {
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

    pub(super) fn set_remaining(&self, topics_remaining: u64) {
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
