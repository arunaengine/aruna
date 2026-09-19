//! Counts storage requests, errors and in-flight work, and latches the close and drain flags.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;

use aruna_core::telemetry::LatencyAggregator;
use tokio::sync::Notify;

// Unbiased queue-wait vs service histograms for every storage effect, keyed
// by operation kind and keyspace, flushed as `latency.summary` INFO lines.
static STORAGE_LATENCY: LazyLock<LatencyAggregator> =
    LazyLock::new(|| LatencyAggregator::new("storage"));

pub(super) fn record_storage_call(
    operation: &'static str,
    key_space: Option<&str>,
    queue_wait: Duration,
    service: Duration,
) {
    match key_space {
        Some(key_space) => {
            STORAGE_LATENCY.record_split(&format!("{operation}:{key_space}"), queue_wait, service)
        }
        None => STORAGE_LATENCY.record_split(operation, queue_wait, service),
    }
}

#[derive(Debug, Default)]
pub(super) struct StorageMetrics {
    pub(super) requests_total: AtomicU64,
    pub(super) errors_total: AtomicU64,
    pub(super) conflicts_total: AtomicU64,
    /// Errors that end the request. Conflicts are excluded: callers retry them,
    /// and the retry is counted again under `requests_total`.
    pub(super) failed_total: AtomicU64,
    pub(super) in_flight: AtomicU64,
    pub(super) channel_closed: Arc<AtomicBool>,
    pub(super) closed: AtomicBool,
    /// Latched when the shutdown drain timed out. The worker reads it before it
    /// starts any mutation, so nothing already queued can commit behind the
    /// final `sync_all`.
    pub(super) mutations_fenced: Arc<AtomicBool>,
    /// Serializes closing with mutating enqueues. Closing takes the write lock;
    /// dispatch holds a read lock through its check and send, so writes are queued
    /// before the final `SyncAll` or rejected after the close.
    pub(super) close_lock: RwLock<()>,
    pub(super) rejected_writes: AtomicU64,
    pub(super) last_error: Mutex<Option<String>>,
    /// Woken when `in_flight` falls to zero, so the shutdown barrier does not poll.
    pub(super) drained: Notify,
}

/// Decrements `in_flight` when an accepted effect completes or is discarded.
#[doc(hidden)]
pub struct InFlightGuard(Arc<StorageMetrics>);

impl InFlightGuard {
    pub(super) fn acquire(metrics: &Arc<StorageMetrics>) -> Self {
        metrics.in_flight.fetch_add(1, Ordering::AcqRel);
        Self(metrics.clone())
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

pub(super) struct WorkerLifecycleGuard(pub(super) Arc<AtomicBool>);

impl Drop for WorkerLifecycleGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageMetricsSnapshot {
    pub requests_total: u64,
    pub errors_total: u64,
    pub conflicts_total: u64,
    /// Errors that ended the request, excluding the conflicts callers retry.
    pub failed_total: u64,
    pub channel_closed: bool,
    pub closed: bool,
    pub rejected_writes: u64,
    pub last_error: Option<String>,
}
