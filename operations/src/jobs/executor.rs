//! Holds the job run outcome, its progress reporter and the turns of distribution ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use aruna_core::structs::execution::job::{JobError, JobPayload, JobProgress, JobResultPayload};
use tokio::sync::futures::Notified;
use ulid::Ulid;

/// In-memory progress with a persisted snapshot taken on flush and lease renew.
#[derive(Clone)]
pub struct ProgressReporter {
    inner: Arc<ProgressInner>,
}

struct ProgressInner {
    current: AtomicU64,
    total: Mutex<Option<u64>>,
    unit: String,
    pause: Mutex<Option<SystemTime>>,
    /// Set once this execution committed a resumable checkpoint.
    checkpointed: AtomicBool,
    /// Wakes the heartbeat to persist a committed checkpoint's progress at once.
    flush: tokio::sync::Notify,
}

impl ProgressReporter {
    pub fn from_progress(progress: &JobProgress) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                current: AtomicU64::new(progress.current),
                total: Mutex::new(progress.total),
                unit: progress.unit.clone(),
                pause: Mutex::new(None),
                checkpointed: AtomicBool::new(false),
                flush: tokio::sync::Notify::new(),
            }),
        }
    }

    pub fn set_total(&self, total: u64) {
        *self.inner.total.lock().expect("progress mutex poisoned") = Some(total);
    }

    pub fn set_current(&self, current: u64) {
        self.inner.current.store(current, Ordering::Relaxed);
    }

    /// Records the end of a pause a source asked for, `None` once the job goes on.
    pub fn set_pause(&self, until: Option<SystemTime>) {
        *self
            .inner
            .pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = until;
    }

    pub fn advance(&self, delta: u64) {
        self.inner.current.fetch_add(delta, Ordering::Relaxed);
    }

    /// Records that a durable checkpoint advanced, so a later transient failure costs no attempt.
    pub fn checkpoint(&self) {
        self.inner.checkpointed.store(true, Ordering::Relaxed);
        self.inner.flush.notify_one();
    }

    /// Completes once a checkpoint asked for its progress to be persisted.
    pub fn flush_requested(&self) -> Notified<'_> {
        self.inner.flush.notified()
    }

    pub fn checkpointed(&self) -> bool {
        self.inner.checkpointed.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> JobProgress {
        JobProgress {
            current: self.inner.current.load(Ordering::Relaxed),
            total: *self.inner.total.lock().expect("progress mutex poisoned"),
            unit: self.inner.unit.clone(),
            paused_until: *self
                .inner
                .pause
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        }
    }
}

/// One turn per distribution range, shared by every job on this node: page imports into one
/// range commit one at a time instead of conflicting on the range's key sample.
#[derive(Clone, Debug, Default)]
pub struct RangeTurns(Arc<Mutex<HashMap<(Ulid, Ulid), Turn>>>);

pub(crate) type Turn = Arc<tokio::sync::Mutex<()>>;

impl RangeTurns {
    /// The turns of `ranges`, keyed by set and range, in key order. Holding every lock in this
    /// order is what lets jobs share ranges; the turns end when the guards drop.
    pub(crate) fn turn_locks(&self, ranges: &BTreeSet<(Ulid, Ulid)>) -> Vec<Turn> {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        // A turn nobody holds or waits for is dropped.
        map.retain(|_, turn| Arc::strong_count(turn) > 1);
        (ranges.iter())
            .map(|range| map.entry(*range).or_default().clone())
            .collect()
    }
}

#[derive(Debug, PartialEq)]
pub enum JobRunOutcome {
    Succeeded(JobResultPayload),
    Failed(JobError),
    /// A dependency such as metadata projection is not readable yet. The
    /// runtime requeues without incrementing the bounded execution attempts.
    Deferred(JobError),
    /// Like `Deferred`, but runs again only after the given delay.
    DeferredFor(JobError, Duration),
    Cancelled,
    Interrupted,
    /// Needs these locked bucket keys; the runtime parks the job without spending an attempt.
    AwaitingKey(Vec<aruna_core::structs::execution::job::KeyWait>),
    /// The outcome cannot be known and the effect must not run again: the job ends
    /// `Indeterminate` and locally exhausted, never `Failed`.
    Undecided(JobError),
}

/// Idempotent cleanup hook run on the cancellation path. Execution/crate payloads
/// carry no in-process side effects here.
pub fn run_cleanup(payload: &JobPayload) {
    match payload {
        JobPayload::Probe { cleanup_marker, .. } => {
            if let Some(marker) = cleanup_marker {
                let _ = std::fs::remove_file(marker);
            }
        }
        JobPayload::Execution(_)
        | JobPayload::Staging(_)
        | JobPayload::ImportRoCrate(_)
        | JobPayload::ExportRoCrate(_)
        | JobPayload::Harvest(_)
        | JobPayload::StoragePurge(_)
        | JobPayload::StoragePurgeFiles(_)
        | JobPayload::MintPersistentId(_)
        | JobPayload::WriteRunCrate { .. }
        | JobPayload::TerminalCleanup { .. }
        | JobPayload::CopyObject(_)
        | JobPayload::RegisterIdentifiers(_)
        | JobPayload::BackendImport(_)
        | JobPayload::WebhookDelivery(_)
        | JobPayload::EmailDelivery(_)
        | JobPayload::PluginWork(_) => {}
    }
}
