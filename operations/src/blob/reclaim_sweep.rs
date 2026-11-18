//! Drains the reclaim queue page by page and reports what a backend still has to reclaim.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_RECLAIM_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::storage::blob::{ArchiveKey, BackendRef, BlobCleanupWork};
use aruna_core::structs::storage::cleanup::{
    CleanupStrategy, ReclaimCandidate, ReclaimCandidateKey,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::BackendCatalog;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::reclaim::{ReclaimBlobError, ReclaimBlobOperation, ReclaimVerdict};
use crate::blob::pending_reclaim::ReclaimPendingOperation;
use crate::groups::backends::backend_key;

pub const RECLAIM_SWEEP_AFTER: Duration = Duration::from_secs(15 * 60);
pub const RECLAIM_SWEEP_RETRY: Duration = Duration::from_secs(60);

const RECLAIM_PAGE_SIZE: usize = 128;

/// Candidates one tick may drive. A longer queue is picked up at
/// `RECLAIM_SWEEP_RETRY` rather than held against the task loop.
pub(super) const RECLAIM_TICK_LIMIT: usize = 1024;

#[derive(Debug, Error, PartialEq)]
pub enum ReclaimSweepError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
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

/// What the sweep looks at next, once the strategy of its backend is known.
#[derive(Debug, PartialEq)]
enum SweepItem {
    Candidate(Key, ReclaimCandidateKey, ReclaimCandidate),
    Pending(ArchiveKey),
}

#[derive(Debug, PartialEq)]
enum SweepStep {
    Queue,
    Record(Ulid),
    Reclaim(Option<Key>, BackendRef),
    Stale,
    Pending,
    ReclaimPending,
}

type SweepTurn = Result<Next<ReclaimOutcome>, ReclaimSweepError>;

/// Pages the queue from `start_after` so a failing candidate cannot starve rows
/// behind it: the cap moves the cursor past it either way.
// Deferred (#359): recount and candidate-persistence hardening.
#[derive(Debug, PartialEq)]
pub struct ReclaimSweepOperation {
    catalog: BackendCatalog,
    now: SystemTime,
    after: Option<Key>,
    step: SweepStep,
    /// Strategies of tenant backends, read once per drain. A missing record resolves to retain,
    /// which is also forced: without it there are no credentials to delete the bytes with.
    strategies: HashMap<Ulid, CleanupStrategy>,
    outcome: ReclaimOutcome,
    driven: usize,
    rows: VecDeque<(Key, Value)>,
    more: Option<Key>,
    stale: Vec<Key>,
    item: Option<SweepItem>,
    pending: PrefixPages,
}

impl ReclaimSweepOperation {
    pub fn new(catalog: BackendCatalog, now: SystemTime, start_after: Option<Key>) -> Self {
        Self {
            catalog,
            now,
            after: start_after,
            step: SweepStep::Queue,
            strategies: HashMap::new(),
            outcome: ReclaimOutcome::default(),
            driven: 0,
            rows: VecDeque::new(),
            more: None,
            stale: Vec::new(),
            item: None,
            pending: PrefixPages::new(
                PENDING_LOCATION_KEYSPACE,
                Key::from(Vec::new()),
                RECLAIM_PAGE_SIZE,
            ),
        }
    }

    fn call(&mut self, step: SweepStep, call: IoCall) -> SweepTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn read_queue(&mut self) -> SweepTurn {
        let page = IoCall::Page {
            space: BLOB_RECLAIM_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.take(),
            limit: RECLAIM_PAGE_SIZE,
        };
        self.call(SweepStep::Queue, page)
    }

    fn drop_row(&mut self, key: Key) {
        self.stale.push(key);
        self.outcome.dropped = self.outcome.dropped.saturating_add(1);
    }

    /// The next queued candidate, or the end of this page.
    fn next_candidate(&mut self) -> SweepTurn {
        while let Some((key, value)) = self.rows.pop_front() {
            let Some((candidate, queued)) = decode_candidate(&key, &value) else {
                self.drop_row(key);
                continue;
            };
            let backend = candidate.backend.clone();
            self.item = Some(SweepItem::Candidate(key, candidate, queued));
            return self.with_strategy(&backend);
        }
        self.page_done()
    }

    /// Goes on with the current item once the cleanup strategy of `backend` is known.
    fn with_strategy(&mut self, backend: &BackendRef) -> SweepTurn {
        let strategy = match backend {
            BackendRef::Node(name) => self.catalog.cleanup_of(name).unwrap_or_default(),
            BackendRef::Group(id) => match self.strategies.get(id) {
                Some(strategy) => *strategy,
                None => {
                    let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(*id));
                    return self.call(SweepStep::Record(*id), read);
                }
            },
        };
        self.strategy_known(strategy)
    }

    fn strategy_known(&mut self, strategy: CleanupStrategy) -> SweepTurn {
        match self.item.take() {
            Some(SweepItem::Candidate(key, candidate, queued)) => {
                self.candidate_ready(key, candidate, queued, strategy)
            }
            Some(SweepItem::Pending(archive)) => {
                // Retain keeps ownerless pending archives; only a reclaim grace frees them.
                let Some(grace) = strategy.grace() else {
                    return self.next_pending();
                };
                let reclaim = ReclaimPendingOperation::new(archive, grace, self.now);
                self.call(SweepStep::ReclaimPending, IoCall::child(reclaim))
            }
            None => self.next_candidate(),
        }
    }

    fn candidate_ready(
        &mut self,
        key: Key,
        candidate: ReclaimCandidateKey,
        queued: ReclaimCandidate,
        strategy: CleanupStrategy,
    ) -> SweepTurn {
        let Some(after) = strategy.grace() else {
            self.drop_row(key);
            return self.next_candidate();
        };
        let due = queued.enqueued_at.checked_add(after);
        if due.is_none_or(|due| due > self.now) {
            self.outcome.not_due = self.outcome.not_due.saturating_add(1);
            return self.next_candidate();
        }
        self.driven = self.driven.saturating_add(1);
        let capped = self.driven >= RECLAIM_TICK_LIMIT;
        let backend = candidate.backend.clone();
        let reclaim = ReclaimBlobOperation::new(candidate, queued.enqueued_at, self.now);
        let cursor = capped.then_some(key);
        self.call(SweepStep::Reclaim(cursor, backend), IoCall::child(reclaim))
    }

    fn reclaimed(
        &mut self,
        cursor: Option<Key>,
        backend: BackendRef,
        verdict: Result<ReclaimVerdict, ReclaimBlobError>,
    ) -> SweepTurn {
        let outcome = &mut self.outcome;
        match verdict {
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
        if let Some(cursor) = cursor {
            outcome.capped = true;
            outcome.next_start_after = Some(cursor);
            self.rows.clear();
            return self.page_done();
        }
        self.next_candidate()
    }

    /// Removes the undecodable and retained rows of the page, then goes on.
    fn page_done(&mut self) -> SweepTurn {
        if self.stale.is_empty() {
            return self.after_page();
        }
        let deletes = (self.stale.drain(..))
            .map(|key| (BLOB_RECLAIM_KEYSPACE.to_string(), key))
            .collect();
        self.call(SweepStep::Stale, IoCall::Delete(deletes))
    }

    fn after_page(&mut self) -> SweepTurn {
        match self.more.take() {
            Some(next) if !self.outcome.capped => {
                self.after = Some(next);
                self.read_queue()
            }
            _ if self.outcome.capped => self.finish(),
            _ => self.read_pending(),
        }
    }

    fn read_pending(&mut self) -> SweepTurn {
        let page = self.pending.call();
        self.call(SweepStep::Pending, page)
    }

    fn next_pending(&mut self) -> SweepTurn {
        while let Some((key, _)) = self.rows.pop_front() {
            let Ok(archive) = ArchiveKey::from_bytes(&key) else {
                continue;
            };
            let backend = archive.backend.clone();
            self.item = Some(SweepItem::Pending(archive));
            return self.with_strategy(&backend);
        }
        match self.pending.done() {
            true => self.finish(),
            false => self.read_pending(),
        }
    }

    fn pending_reclaimed(
        &mut self,
        verdict: Result<ReclaimVerdict, ReclaimBlobError>,
    ) -> SweepTurn {
        let outcome = &mut self.outcome;
        match verdict {
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
        self.next_pending()
    }

    fn record_read(&mut self, id: Ulid, value: Option<Value>) -> SweepTurn {
        let record = value.as_deref().map(GroupStorage::from_bytes).transpose()?;
        let strategy = record.map(|record| record.cleanup).unwrap_or_default();
        self.strategies.insert(id, strategy);
        self.strategy_known(strategy)
    }

    fn finish(&mut self) -> SweepTurn {
        let outcome = std::mem::take(&mut self.outcome);
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
        Ok(Next::Done(outcome))
    }
}

impl Operation for ReclaimSweepOperation {
    type Output = ReclaimOutcome;
    type Error = ReclaimSweepError;

    fn first(&mut self) -> SweepTurn {
        self.read_queue()
    }

    fn next(&mut self, result: IoResult) -> SweepTurn {
        match std::mem::replace(&mut self.step, SweepStep::Queue) {
            SweepStep::Queue => {
                let (rows, next) = result.page()?;
                self.rows = rows.into();
                self.more = next;
                self.next_candidate()
            }
            SweepStep::Record(id) => self.record_read(id, result.value()?),
            SweepStep::Reclaim(cursor, backend) => self.reclaimed(cursor, backend, result.child()?),
            SweepStep::Stale => {
                result.done()?;
                self.after_page()
            }
            SweepStep::Pending => {
                self.rows = self.pending.take(result)?.into();
                self.next_pending()
            }
            SweepStep::ReclaimPending => self.pending_reclaimed(result.child()?),
        }
    }
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
#[derive(Debug, PartialEq)]
pub struct BackendStatusOperation {
    backend: BackendRef,
    status: ReclaimStatus,
    candidates_read: bool,
}

type StatusTurn = Result<Next<ReclaimStatus>, ReclaimSweepError>;

impl BackendStatusOperation {
    pub fn new(backend: &BackendRef) -> Self {
        Self {
            backend: backend.clone(),
            status: ReclaimStatus::default(),
            candidates_read: false,
        }
    }

    fn candidates(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> StatusTurn {
        self.status.pending_candidates = rows.len();
        self.status.truncated = next.is_some();
        for (_, value) in rows {
            if let Ok(candidate) = ReclaimCandidate::from_bytes(value.as_ref()) {
                fold_oldest(&mut self.status.oldest_enqueued_at, candidate.enqueued_at);
            }
        }
        self.candidates_read = true;
        Ok(Next::Call(IoCall::Page {
            space: BLOB_CLEANUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: CLEANUP_SCAN_LIMIT,
        }))
    }

    fn cleanups(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> StatusTurn {
        self.status.truncated = self.status.truncated || next.is_some();
        for (key, value) in rows {
            let Ok(
                BlobCleanupWork::DeleteBlob { location }
                | BlobCleanupWork::ReconcileWrite { location, .. }
                | BlobCleanupWork::ReconcileReservation { location }
                | BlobCleanupWork::ReleaseRenamed { location, .. },
            ) = BlobCleanupWork::from_bytes(value.as_ref())
            else {
                continue;
            };
            if location.backend != self.backend {
                continue;
            }
            self.status.queued_cleanups = self.status.queued_cleanups.saturating_add(1);
            if let Some(queued_at) = cleanup_time(&key) {
                fold_oldest(&mut self.status.oldest_enqueued_at, queued_at);
            }
        }
        Ok(Next::Done(std::mem::take(&mut self.status)))
    }
}

impl Operation for BackendStatusOperation {
    type Output = ReclaimStatus;
    type Error = ReclaimSweepError;

    fn first(&mut self) -> StatusTurn {
        Ok(Next::Call(IoCall::Page {
            space: BLOB_RECLAIM_KEYSPACE.to_string(),
            prefix: ReclaimCandidateKey::prefix(&self.backend).into(),
            after: None,
            limit: STATUS_SCAN_LIMIT,
        }))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        let (rows, next) = result.page()?;
        match self.candidates_read {
            true => self.cleanups(rows, next),
            false => self.candidates(rows, next),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sweep() -> ReclaimSweepOperation {
        let catalog = BackendCatalog::new("default").with_backend("default", None);
        ReclaimSweepOperation::new(catalog, SystemTime::UNIX_EPOCH, None)
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    #[test]
    fn empty_queues_finish() {
        let mut operation = sweep();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let pending = operation.next(page(Vec::new()));
        assert!(matches!(pending, Ok(Next::Call(IoCall::Page { .. }))));
        let done = operation.next(page(Vec::new()));
        assert_eq!(done, Ok(Next::Done(ReclaimOutcome::default())));
    }

    #[test]
    fn undecodable_row_dropped() {
        let mut operation = sweep();
        operation.first().unwrap();
        let row = (Key::from(vec![1]), Value::from(vec![2]));
        let turn = operation.next(page(vec![row]));
        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("expected the stale row delete, got {turn:?}");
        };
        assert_eq!(deletes[0].0, BLOB_RECLAIM_KEYSPACE);
        assert_eq!(operation.outcome.dropped, 1);
    }

    #[test]
    fn status_counts_queues() {
        let backend = BackendRef::node_default();
        let mut status = BackendStatusOperation::new(&backend);
        status.first().unwrap();
        let turn = status.next(page(Vec::new()));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        assert_eq!(
            status.next(page(Vec::new())),
            Ok(Next::Done(ReclaimStatus::default()))
        );
    }
}
