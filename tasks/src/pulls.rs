//! Tracks the synced-folder pulls this node runs, by receipt key. It lives as long as the
//! process, so a restart forgets a running pull and the device asks for it again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use aruna_core::structs::synced_folder::{SyncPullAck, SyncPullStatus, SyncRefusal};
use aruna_core::task::{PullEffect, PullEvent, PullProgress};

#[derive(Debug)]
enum PullTask {
    /// The run holds the only strong counter, so a dropped run leaves this entry dead.
    Running {
        bytes_done: Weak<AtomicU64>,
        total: u64,
    },
    /// Kept until the device asks once, so it learns why the pull ended.
    Refused(SyncRefusal),
}

#[derive(Debug, Default)]
pub(crate) struct PullRegistry(Mutex<HashMap<Vec<u8>, PullTask>>);

impl PullRegistry {
    fn pulls(&self) -> MutexGuard<'_, HashMap<Vec<u8>, PullTask>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn answer(&self, effect: PullEffect) -> PullEvent {
        match effect {
            PullEffect::Claim { key, total } => PullEvent::Claimed(self.claim(key, total)),
            PullEffect::Finish { key, result } => {
                self.finish(key, result);
                PullEvent::Finished
            }
            PullEffect::Status { key } => PullEvent::Status(self.status(&key)),
        }
    }

    // A new run of `key`, or the status of the live run that holds it.
    fn claim(&self, key: Vec<u8>, total: u64) -> Result<PullProgress, SyncPullStatus> {
        let mut pulls = self.pulls();
        if let Some(PullTask::Running { bytes_done, total }) = pulls.get(&key)
            && let Some(bytes_done) = bytes_done.upgrade()
        {
            let bytes_done = bytes_done.load(Ordering::Relaxed);
            let total = *total;
            return Err(SyncPullStatus::Pending { bytes_done, total });
        }
        let progress = PullProgress::default();
        let bytes_done = Arc::downgrade(&progress.0);
        pulls.insert(key, PullTask::Running { bytes_done, total });
        Ok(progress)
    }

    // A commit is answered by its receipt from now on, a refusal by its entry.
    fn finish(&self, key: Vec<u8>, result: Result<SyncPullAck, SyncRefusal>) {
        let mut pulls = self.pulls();
        match result {
            Ok(_) => pulls.remove(&key),
            Err(refusal) => pulls.insert(key, PullTask::Refused(refusal)),
        };
    }

    // The run `key` names; a refusal is handed out once and a dead run is forgotten.
    fn status(&self, key: &[u8]) -> Option<SyncPullStatus> {
        let mut pulls = self.pulls();
        if let Some(PullTask::Running { bytes_done, total }) = pulls.get(key)
            && let Some(bytes_done) = bytes_done.upgrade()
        {
            let bytes_done = bytes_done.load(Ordering::Relaxed);
            return Some(SyncPullStatus::Pending {
                bytes_done,
                total: *total,
            });
        }
        match pulls.remove(key) {
            Some(PullTask::Refused(refusal)) => Some(SyncPullStatus::Refused(refusal)),
            _ => None,
        }
    }
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
