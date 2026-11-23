//! Targets a witness set aside after only temporary answers, and when it considers them again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::ExecutionTargetId;
use aruna_core::events::LaunchDecline;
use aruna_core::structs::execution::job::family::LAUNCH_WINDOW_MS;
use serde::{Deserialize, Serialize};

/// The last temporary answer of a target whose receipt wait expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeferKind {
    /// No answer. A target behind its start fence cannot decide and answers this way too.
    Silent,
    Capacity,
    Draining,
    /// Any other temporary decline.
    Other,
}

impl DeferKind {
    pub fn of(reason: &LaunchDecline) -> Self {
        match reason {
            LaunchDecline::Capacity => Self::Capacity,
            LaunchDecline::Draining => Self::Draining,
            _ => Self::Other,
        }
    }
}

/// One deferred target; the list holds at most one entry per target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredTarget {
    pub target: ExecutionTargetId,
    pub kind: DeferKind,
    pub at_ms: u64,
}

/// What the witness sees of one deferred target in this round.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Signal {
    /// When its newest fresh heartbeat was sent.
    pub heartbeat_ms: Option<u64>,
    /// When a fresh availability showed room for the request, every field known.
    pub fits_ms: Option<u64>,
    /// Whether its advertisement is draining; unknown without an advertisement.
    pub draining: Option<bool>,
}

/// Whether a deferred target is a candidate again: not before `floor_ms`, then on the recovery
/// signal of its answer, and after one launch window in any case.
pub fn reconsidered(entry: &DeferredTarget, signal: &Signal, now_ms: u64, floor_ms: u64) -> bool {
    let age_ms = now_ms.saturating_sub(entry.at_ms);
    if age_ms < floor_ms {
        return false;
    }
    if age_ms >= LAUNCH_WINDOW_MS {
        return true;
    }
    let newer = |time_ms: Option<u64>| time_ms.is_some_and(|time_ms| time_ms > entry.at_ms);
    match entry.kind {
        DeferKind::Capacity => newer(signal.fits_ms),
        DeferKind::Silent => newer(signal.heartbeat_ms),
        DeferKind::Draining => signal.draining == Some(false),
        DeferKind::Other => false,
    }
}

/// Adds `entry`, replacing an older deferral of the same target.
pub fn defer(deferred: &mut Vec<DeferredTarget>, entry: DeferredTarget) {
    deferred.retain(|old| old.target != entry.target);
    deferred.push(entry);
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOOR_MS: u64 = 300_000;

    fn entry(kind: DeferKind) -> DeferredTarget {
        let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        DeferredTarget {
            target: ExecutionTargetId {
                node_id,
                backend: "lab".to_string(),
                executor_kind: "docker".to_string(),
            },
            kind,
            at_ms: 1_000,
        }
    }

    // Even a strong signal waits for the floor after the deferral.
    #[test]
    fn floor_holds_target() {
        let signal = Signal {
            heartbeat_ms: Some(2_000),
            fits_ms: Some(2_000),
            draining: Some(false),
        };
        for kind in [DeferKind::Silent, DeferKind::Capacity, DeferKind::Draining] {
            assert!(!reconsidered(
                &entry(kind),
                &signal,
                1_000 + FLOOR_MS - 1,
                FLOOR_MS
            ));
            assert!(reconsidered(
                &entry(kind),
                &signal,
                1_000 + FLOOR_MS,
                FLOOR_MS
            ));
        }
    }

    // Unchanged heartbeats of a busy target do not bring it back at each floor; only a
    // fitting availability newer than the deferral does, or the end of a launch window.
    #[test]
    fn capacity_needs_fit() {
        let busy = Signal {
            heartbeat_ms: Some(400_000),
            fits_ms: None,
            draining: Some(false),
        };
        let now_ms = 1_000 + FLOOR_MS * 2;
        assert!(!reconsidered(
            &entry(DeferKind::Capacity),
            &busy,
            now_ms,
            FLOOR_MS
        ));
        let old_fit = Signal {
            fits_ms: Some(900),
            ..busy
        };
        assert!(!reconsidered(
            &entry(DeferKind::Capacity),
            &old_fit,
            now_ms,
            FLOOR_MS
        ));
        let fits = Signal {
            fits_ms: Some(400_000),
            ..busy
        };
        assert!(reconsidered(
            &entry(DeferKind::Capacity),
            &fits,
            now_ms,
            FLOOR_MS
        ));
        let late = 1_000 + LAUNCH_WINDOW_MS;
        assert!(reconsidered(
            &entry(DeferKind::Capacity),
            &busy,
            late,
            FLOOR_MS
        ));
    }

    // Silence ends with a newer heartbeat, a drain with an undrained advertisement, and any
    // other answer only with the launch window.
    #[test]
    fn signals_per_kind() {
        let now_ms = 1_000 + FLOOR_MS;
        let heard = Signal {
            heartbeat_ms: Some(5_000),
            ..Signal::default()
        };
        assert!(reconsidered(
            &entry(DeferKind::Silent),
            &heard,
            now_ms,
            FLOOR_MS
        ));
        let stale = Signal {
            heartbeat_ms: Some(500),
            ..Signal::default()
        };
        assert!(!reconsidered(
            &entry(DeferKind::Silent),
            &stale,
            now_ms,
            FLOOR_MS
        ));
        let draining = Signal {
            draining: Some(true),
            ..heard
        };
        assert!(!reconsidered(
            &entry(DeferKind::Draining),
            &draining,
            now_ms,
            FLOOR_MS
        ));
        assert!(!reconsidered(
            &entry(DeferKind::Draining),
            &heard,
            now_ms,
            FLOOR_MS
        ));
        let undrained = Signal {
            draining: Some(false),
            ..heard
        };
        assert!(reconsidered(
            &entry(DeferKind::Draining),
            &undrained,
            now_ms,
            FLOOR_MS
        ));
        assert!(!reconsidered(
            &entry(DeferKind::Other),
            &undrained,
            now_ms,
            FLOOR_MS
        ));
    }

    #[test]
    fn replaces_old_deferral() {
        let mut deferred = vec![entry(DeferKind::Silent)];
        let newer = DeferredTarget {
            kind: DeferKind::Capacity,
            at_ms: 9_000,
            ..entry(DeferKind::Silent)
        };
        defer(&mut deferred, newer.clone());
        assert_eq!(deferred, vec![newer]);
    }
}
