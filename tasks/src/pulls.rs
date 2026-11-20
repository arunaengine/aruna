//! Tracks the synced-folder pulls this node runs, by receipt key. It lives as long as the
//! process, so a restart forgets a running pull and the device asks for it again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use aruna_core::structs::synced_folder::{SyncPullAck, SyncPullStatus, SyncRefusal};
use aruna_core::task::{PullEffect, PullEvent, PullProgress};

/// How long a refusal waits for its device; a later status answers none, so it claims again.
const REFUSAL_KEPT: Duration = Duration::from_secs(3600);
/// How often a claim drops ended runs and stale refusals; between sweeps they only wait.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

#[derive(Debug)]
enum PullTask {
    /// The run holds the only strong counter, so a dropped run leaves this entry dead.
    Running {
        bytes_done: Weak<AtomicU64>,
        total: u64,
    },
    /// Kept until the device asks once, so it learns why the pull ended.
    Refused { refusal: SyncRefusal, at: Instant },
}

#[derive(Debug, Default)]
pub(crate) struct PullRegistry(Mutex<Pulls>);

/// The pulls by receipt key and when they were last swept.
#[derive(Debug, Default)]
struct Pulls {
    tasks: HashMap<Vec<u8>, PullTask>,
    swept: Option<Instant>,
}

impl PullRegistry {
    fn pulls(&self) -> MutexGuard<'_, Pulls> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn answer(&self, effect: PullEffect) -> PullEvent {
        let now = Instant::now();
        match effect {
            PullEffect::Claim { key, total } => PullEvent::Claimed(self.claim(key, total, now)),
            PullEffect::Finish { key, result } => {
                self.finish(key, result, now);
                PullEvent::Finished
            }
            PullEffect::Status { key } => PullEvent::Status(self.status(&key, now)),
        }
    }

    // A new run of `key`, or the status of the live run that holds it.
    fn claim(
        &self,
        key: Vec<u8>,
        total: u64,
        now: Instant,
    ) -> Result<PullProgress, SyncPullStatus> {
        let mut pulls = self.pulls();
        pulls.sweep(now);
        if let Some(PullTask::Running { bytes_done, total }) = pulls.tasks.get(&key)
            && let Some(bytes_done) = bytes_done.upgrade()
        {
            let bytes_done = bytes_done.load(Ordering::Relaxed);
            let total = *total;
            return Err(SyncPullStatus::Pending { bytes_done, total });
        }
        let progress = PullProgress::default();
        let bytes_done = Arc::downgrade(&progress.0);
        pulls.tasks.insert(key, PullTask::Running { bytes_done, total });
        Ok(progress)
    }

    // A commit is answered by its receipt from now on, a refusal by its entry.
    fn finish(&self, key: Vec<u8>, result: Result<SyncPullAck, SyncRefusal>, now: Instant) {
        let tasks = &mut self.pulls().tasks;
        match result {
            Ok(_) => tasks.remove(&key),
            Err(refusal) => tasks.insert(key, PullTask::Refused { refusal, at: now }),
        };
    }

    // The run `key` names; a refusal is handed out once and a dead run is forgotten.
    fn status(&self, key: &[u8], now: Instant) -> Option<SyncPullStatus> {
        let tasks = &mut self.pulls().tasks;
        if let Some(PullTask::Running { bytes_done, total }) = tasks.get(key)
            && let Some(bytes_done) = bytes_done.upgrade()
        {
            let bytes_done = bytes_done.load(Ordering::Relaxed);
            return Some(SyncPullStatus::Pending {
                bytes_done,
                total: *total,
            });
        }
        match tasks.remove(key) {
            Some(PullTask::Refused { refusal, at }) if kept(at, now) => {
                Some(SyncPullStatus::Refused(refusal))
            }
            _ => None,
        }
    }
}

impl Pulls {
    /// Drops runs that ended without finishing and refusals nobody asked for in time, at most
    /// once per `SWEEP_EVERY`, so a burst of pulls does not scan the registry each time.
    fn sweep(&mut self, now: Instant) {
        if (self.swept).is_some_and(|at| now.saturating_duration_since(at) < SWEEP_EVERY) {
            return;
        }
        self.swept = Some(now);
        self.tasks.retain(|_, task| match task {
            PullTask::Running { bytes_done, .. } => bytes_done.strong_count() > 0,
            PullTask::Refused { at, .. } => kept(*at, now),
        });
    }
}

fn kept(at: Instant, now: Instant) -> bool {
    now.saturating_duration_since(at) < REFUSAL_KEPT
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claimed(registry: &PullRegistry) -> PullProgress {
        let claim = PullEffect::Claim {
            key: vec![1],
            total: 10,
        };
        match registry.answer(claim) {
            PullEvent::Claimed(Ok(progress)) => progress,
            other => panic!("expected a claim, got {other:?}"),
        }
    }

    #[test]
    fn running_claim_held() {
        let registry = PullRegistry::default();
        let progress = claimed(&registry);
        progress.0.store(4, Ordering::Relaxed);
        let again = PullEffect::Claim {
            key: vec![1],
            total: 10,
        };
        let pending = SyncPullStatus::Pending {
            bytes_done: 4,
            total: 10,
        };
        assert_eq!(registry.answer(again), PullEvent::Claimed(Err(pending)));
    }

    #[test]
    fn dropped_run_released() {
        // A run that ends without finishing, for example by a panic, frees its key.
        let registry = PullRegistry::default();
        drop(claimed(&registry));
        let status = PullEffect::Status { key: vec![1] };
        assert_eq!(registry.answer(status), PullEvent::Status(None));
        claimed(&registry);
    }

    #[test]
    fn abandoned_entries_swept() {
        let registry = PullRegistry::default();
        let start = Instant::now();
        drop(registry.claim(vec![1], 10, start));
        registry.finish(vec![2], Err(SyncRefusal::Unavailable), start);
        let later = start + REFUSAL_KEPT;
        let progress = registry.claim(vec![3], 10, later);
        assert_eq!(registry.pulls().tasks.len(), 1, "the dropped run and stale refusal are gone");
        assert_eq!(registry.status(&[2], later), None);
        drop(progress);
    }

    // Claims within one sweep interval leave ended runs alone; the next one after it drops them.
    #[test]
    fn sweep_runs_rarely() {
        let registry = PullRegistry::default();
        let start = Instant::now();
        drop(registry.claim(vec![1], 10, start));
        drop(registry.claim(vec![2], 10, start + Duration::from_secs(1)));
        assert_eq!(registry.pulls().tasks.len(), 2, "no sweep within the interval");
        drop(registry.claim(vec![3], 10, start + SWEEP_EVERY));
        assert_eq!(registry.pulls().tasks.len(), 1, "the next interval sweeps");
    }

    #[test]
    fn refusal_told_once() {
        let registry = PullRegistry::default();
        let progress = claimed(&registry);
        let result = Err(SyncRefusal::Unavailable);
        registry.answer(PullEffect::Finish {
            key: vec![1],
            result,
        });
        drop(progress);
        let refused = SyncPullStatus::Refused(SyncRefusal::Unavailable);
        let status = || PullEffect::Status { key: vec![1] };
        assert_eq!(registry.answer(status()), PullEvent::Status(Some(refused)));
        assert_eq!(registry.answer(status()), PullEvent::Status(None));
    }
}
