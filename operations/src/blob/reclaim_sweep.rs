//! Drains the reclaim queue page by page and reports what a backend still has to reclaim.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::handle::Handle;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_RECLAIM_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::structs::storage::blob::{ArchiveKey, BackendRef, BlobCleanupWork};
use aruna_core::structs::storage::cleanup::{
    CleanupStrategy, ReclaimCandidate, ReclaimCandidateKey,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::{info, warn};
use ulid::Ulid;

use super::reclaim::{ReclaimBlobOperation, ReclaimVerdict};
use crate::blob::pending_reclaim::ReclaimPendingOperation;
use crate::driver::{DriverContext, drive, node_routing};
use crate::groups::backends::{backend_key, parse_read};
use crate::storage_read::iter_prefix_page;
use crate::tasks::task_persistence::persist_task_effect;

pub const RECLAIM_SWEEP_AFTER: Duration = Duration::from_secs(15 * 60);
pub const RECLAIM_SWEEP_RETRY: Duration = Duration::from_secs(60);

const RECLAIM_PAGE_SIZE: usize = 128;

/// Candidates one tick may drive. A longer queue is picked up at
/// `RECLAIM_SWEEP_RETRY` rather than held against the task loop.
pub(super) const RECLAIM_TICK_LIMIT: usize = 1024;

pub async fn restore_reclaim_sweep(storage: &StorageHandle, task_handle: &TaskHandle) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::DrainReclaimQueue,
        after: Duration::ZERO,
    };
    if let Err(message) = persist_task_effect(storage, &effect).await {
        warn!(message = %message, "Failed to persist blob reclaim timer");
        return;
    }
    match task_handle.send_effect(Effect::Task(effect)).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => {}
        Event::Task(TaskEvent::Error { message, .. }) => {
            warn!(message = %message, "Failed to schedule blob reclaim sweep");
        }
        other => warn!(event = ?other, "Unexpected blob reclaim timer result"),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReclaimOutcome {
    pub freed: usize,
    pub freed_bytes: u64,
    pub pinned: usize,
    pub dropped: usize,
    pub not_due: usize,
    pub failed: usize,
    /// The tick stopped at its work cap with candidates still queued.
    pub capped: bool,
    /// Where the next tick resumes. `None` means the queue ended, so the next
    /// tick starts from the head again.
    pub next_start_after: Option<Key>,
}

/// Pages the queue from `start_after` so a failing candidate cannot starve rows
/// behind it: the cap moves the cursor past it either way.
// Deferred (#359): recount and candidate-persistence hardening.
pub async fn process_reclaim_batch(
    context: &DriverContext,
    start_after: Option<Key>,
) -> Result<ReclaimOutcome, String> {
    sweep_at(context, SystemTime::now(), start_after).await
}

pub(super) async fn sweep_at(
    context: &DriverContext,
    now: SystemTime,
    mut start_after: Option<Key>,
) -> Result<ReclaimOutcome, String> {
    let catalog = node_routing(context).catalog;
    let mut records: HashMap<Ulid, Option<GroupStorage>> = HashMap::new();
    let mut outcome = ReclaimOutcome::default();
    let mut driven = 0usize;

    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_RECLAIM_KEYSPACE,
            None,
            start_after,
            RECLAIM_PAGE_SIZE,
            None,
        )
        .await?;

        let mut stale = Vec::new();
        for (key, value) in values {
            let Some(candidate) = decode_candidate(&key, &value) else {
                stale.push(key);
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            };
            let backend = candidate.0.backend.clone();
            let strategy = match &candidate.0.backend {
                BackendRef::Node(name) => catalog.cleanup_of(name).unwrap_or_default(),
                BackendRef::Group(id) => group_strategy(context, &mut records, *id)
                    .await?
                    .unwrap_or_default(),
            };
            let Some(after) = strategy.grace() else {
                stale.push(key);
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            };
            if candidate
                .1
                .enqueued_at
                .checked_add(after)
                .is_none_or(|due| due > now)
            {
                outcome.not_due = outcome.not_due.saturating_add(1);
                continue;
            }
            let operation = ReclaimBlobOperation::new(candidate.0, candidate.1.enqueued_at, now);
            match drive(operation, context).await {
                Ok(ReclaimVerdict::Freed { bytes }) => {
                    outcome.freed = outcome.freed.saturating_add(1);
                    outcome.freed_bytes = outcome.freed_bytes.saturating_add(bytes);
                }
                Ok(ReclaimVerdict::Pinned) => outcome.pinned = outcome.pinned.saturating_add(1),
                Ok(ReclaimVerdict::Dropped) => outcome.dropped = outcome.dropped.saturating_add(1),
                Ok(ReclaimVerdict::NotDue) => outcome.not_due = outcome.not_due.saturating_add(1),
                Err(error) => {
                    warn!(backend = %backend, error = %error, "Blob reclaim candidate failed");
                    outcome.failed = outcome.failed.saturating_add(1);
                }
            }
            driven = driven.saturating_add(1);
            if driven >= RECLAIM_TICK_LIMIT {
                outcome.capped = true;
                outcome.next_start_after = Some(key);
                break;
            }
        }

        delete_candidates(context, stale).await?;

        match next {
            Some(next) if !outcome.capped => start_after = Some(next),
            _ => break,
        }
    }

    if !outcome.capped {
        let mut start = None;
        loop {
            let (rows, next) = iter_prefix_page(
                &context.storage_handle,
                PENDING_LOCATION_KEYSPACE,
                None,
                start,
                RECLAIM_PAGE_SIZE,
                None,
            )
            .await?;
            for (key, _) in &rows {
                let Ok(archive) = ArchiveKey::from_bytes(key) else {
                    continue;
                };
                // Retain keeps ownerless pending archives; only a reclaim grace frees them.
                let strategy = match &archive.backend {
                    BackendRef::Node(name) => catalog.cleanup_of(name).unwrap_or_default(),
                    BackendRef::Group(id) => group_strategy(context, &mut records, *id)
                        .await?
                        .unwrap_or_default(),
                };
                let Some(grace) = strategy.grace() else {
                    continue;
                };
                let operation = ReclaimPendingOperation::new(archive, grace, now);
                match drive(operation, context).await {
                    Ok(ReclaimVerdict::Freed { bytes }) => {
                        outcome.freed = outcome.freed.saturating_add(1);
                        outcome.freed_bytes = outcome.freed_bytes.saturating_add(bytes);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(error = %error, "Pending archive reclaim failed");
                        outcome.failed = outcome.failed.saturating_add(1);
                    }
                }
            }
            match next {
                Some(next) if !rows.is_empty() => start = Some(next),
                _ => break,
            }
        }
    }

    info!(
        freed = outcome.freed,
        freed_bytes = outcome.freed_bytes,
        pinned = outcome.pinned,
        dropped = outcome.dropped,
        not_due = outcome.not_due,
        failed = outcome.failed,
        capped = outcome.capped,
        "Blob reclaim sweep finished"
    );
    Ok(outcome)
}

fn decode_candidate(key: &Key, value: &[u8]) -> Option<(ReclaimCandidateKey, ReclaimCandidate)> {
    match (
        ReclaimCandidateKey::from_bytes(key.as_ref()),
        ReclaimCandidate::from_bytes(value),
    ) {
        (Ok(key), Ok(candidate)) => Some((key, candidate)),
        _ => {
            warn!("Dropping undecodable reclaim candidate");
            None
        }
    }
}

/// Reads a tenant record once per drain. A missing record resolves to retain,
/// which is also forced: without the record there are no credentials to delete
/// the bytes with.
async fn group_strategy(
    context: &DriverContext,
    records: &mut HashMap<Ulid, Option<GroupStorage>>,
    backend_id: Ulid,
) -> Result<Option<CleanupStrategy>, String> {
    if let std::collections::hash_map::Entry::Vacant(slot) = records.entry(backend_id) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
                key: backend_key(backend_id),
                txn_id: None,
            })
            .await;
        slot.insert(
            parse_read(event, GroupStorage::from_bytes).map_err(|error| error.to_string())?,
        );
    }
    Ok(records
        .get(&backend_id)
        .and_then(|record| record.as_ref())
        .map(|record| record.cleanup))
}

async fn delete_candidates(context: &DriverContext, keys: Vec<Key>) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let deletes = keys
        .into_iter()
        .map(|key| (BLOB_RECLAIM_KEYSPACE.to_string(), key))
        .collect();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected candidate delete event: {}",
            other.label()
        )),
    }
}

/// Queue depth for one backend, computed from the queues themselves so it can
/// never drift. `truncated` reports that a scan hit its cap.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReclaimStatus {
    pub pending_candidates: usize,
    /// Physical deletes still owed to this backend. The drain runs on its own
    /// timer, so a non-zero count is normal; `oldest_enqueued_at` is what says
    /// whether the queue is moving.
    pub queued_cleanups: usize,
    /// Oldest of both queues: candidates by their stored enqueue time, queued
    /// cleanups by the ULID they are keyed under.
    pub oldest_enqueued_at: Option<SystemTime>,
    pub truncated: bool,
}

fn fold_oldest(oldest: &mut Option<SystemTime>, time: SystemTime) {
    *oldest = Some(oldest.map_or(time, |current| current.min(time)));
}

/// Cleanup keys are generated ULIDs, so a row carries its own queue time.
fn cleanup_time(key: &Key) -> Option<SystemTime> {
    <[u8; 16]>::try_from(key.as_ref())
        .ok()
        .map(|bytes| Ulid::from_bytes(bytes).datetime())
}

const STATUS_SCAN_LIMIT: usize = 10_000;

/// The cleanup queue has no backend order, so this scan decodes every row it
/// reads. It runs per request, so it is capped far lower than the prefix scan.
const CLEANUP_SCAN_LIMIT: usize = 1_000;

/// Counts one backend's queued candidates and the physical deletes still owed
/// to it. The candidate side is a bounded prefix scan; the cleanup queue has no
/// backend order, so that side is a capped filtered scan.
pub async fn backend_status(
    context: &DriverContext,
    backend: &BackendRef,
) -> Result<ReclaimStatus, String> {
    let mut status = ReclaimStatus::default();
    let (candidates, next) = iter_prefix_page(
        &context.storage_handle,
        BLOB_RECLAIM_KEYSPACE,
        Some(ReclaimCandidateKey::prefix(backend).into()),
        None,
        STATUS_SCAN_LIMIT,
        None,
    )
    .await?;
    status.pending_candidates = candidates.len();
    status.truncated = next.is_some();
    for (_, value) in candidates {
        if let Ok(candidate) = ReclaimCandidate::from_bytes(value.as_ref()) {
            fold_oldest(&mut status.oldest_enqueued_at, candidate.enqueued_at);
        }
    }

    let (cleanups, next) = iter_prefix_page(
        &context.storage_handle,
        BLOB_CLEANUP_KEYSPACE,
        None,
        None,
        CLEANUP_SCAN_LIMIT,
        None,
    )
    .await?;
    status.truncated = status.truncated || next.is_some();
    for (key, value) in cleanups {
        let Ok(
            BlobCleanupWork::DeleteBlob { location }
            | BlobCleanupWork::ReconcileWrite { location, .. }
            | BlobCleanupWork::ReconcileReservation { location }
            | BlobCleanupWork::ReleaseRenamed { location, .. },
        ) = BlobCleanupWork::from_bytes(value.as_ref())
        else {
            continue;
        };
        if &location.backend != backend {
            continue;
        }
        status.queued_cleanups = status.queued_cleanups.saturating_add(1);
        if let Some(queued_at) = cleanup_time(&key) {
            fold_oldest(&mut status.oldest_enqueued_at, queued_at);
        }
    }

    Ok(status)
}
