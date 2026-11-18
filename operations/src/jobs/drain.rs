//! Scans the job schedule index, claims due jobs per class budget and rearms the drain timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::SCHEDULE_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    DUE_INDEX_PREFIX, JobError, JobExecutionClass, JobId, JobRecord, LEASE_INDEX_PREFIX,
    lease_index_key, parse_schedule_key,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;

use super::store::job_calls::{ClaimOutcome, RequeueOutcome, claim_job, requeue_job};
use super::store::mutation_error::JobMutationError;
use super::store::row_calls::{SchedulePage, first_schedule_entry, read_job_record, schedule_page};
use super::store::store_call::RowBatch;
use super::{DRAIN_BATCH_SIZE, JOB_RECONCILE_REARM};

/// Per-class claim budget. Both classes share one due index, so a saturated class
/// must be skipped during the scan rather than claimed and released again.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JobClassBudget {
    pub in_process: usize,
    pub external: usize,
}

impl JobClassBudget {
    fn remaining(&self, class: JobExecutionClass) -> usize {
        match class {
            JobExecutionClass::InProcess => self.in_process,
            JobExecutionClass::ExternalAttempt => self.external,
        }
    }

    fn take(&mut self, class: JobExecutionClass) {
        match class {
            JobExecutionClass::InProcess => self.in_process = self.in_process.saturating_sub(1),
            JobExecutionClass::ExternalAttempt => self.external = self.external.saturating_sub(1),
        }
    }

    fn is_empty(&self) -> bool {
        self.in_process == 0 && self.external == 0
    }
}

/// What one drain pass did.
#[derive(Debug, Default, PartialEq)]
pub struct JobDrainResult {
    pub claimed: Vec<JobRecord>,
    pub terminal: Vec<JobRecord>,
    pub cancelled_fresh: usize,
    pub swept: usize,
    /// Expired external attempts routed to the reconcile hook instead of a requeue.
    pub reconciled: usize,
    /// The records of those attempts, for the caller's reconcile hook. A node without a
    /// hook must not charge the attempt: it cannot even observe the container.
    pub lost_attempts: Vec<JobRecord>,
    pub next_due_after: Option<Duration>,
    /// A per-job error stopped the batch early; the caller should re-drive after a
    /// backoff. Set so `claimed` is never discarded by a later failure.
    pub retry_after_error: bool,
    /// A due job was left for a later pass because its execution class has no free
    /// slot. Re-arming at zero would busy-loop until a slot opens.
    pub deferred_saturated: bool,
}

/// The first due page could not be read, so nothing was handed off yet.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct DrainError(pub String);

impl From<IoError> for DrainError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum DrainStep {
    DuePage,
    DueDrop(&'static str),
    DueRecord { key: Key, job_id: JobId },
    DueClaim { key: Key, job_id: JobId },
    LeasePage,
    LeaseDrop,
    Requeue { ts: u64, job_id: JobId },
    LeaseOrphan,
    DueHead,
    LeaseHead(Option<Duration>),
}

/// Claims due jobs within each class's budget and re-queues expired leases. Only records owned
/// by `owner_node_id` are claimed: a JobId executes on its immutable owner. A job whose class is
/// saturated is skipped without a write, since claiming it would only release it again.
#[derive(Debug, PartialEq)]
pub struct DrainJobsOperation {
    owner_node_id: NodeId,
    budget: JobClassBudget,
    now_ms: u64,
    result: JobDrainResult,
    step: DrainStep,
    /// Rows of the current page still to judge, the next one last.
    rows: Vec<(Key, Value)>,
    next_page: Option<Key>,
    /// Expired leases of the current page still to requeue, the next one last.
    expired: Vec<(u64, JobId)>,
    expired_count: usize,
    reconciled_before: usize,
    lease_pages: u8,
}

type DrainNext = Result<Next<JobDrainResult>, DrainError>;

fn drop_row(key: Key) -> IoCall {
    let row = (SCHEDULE_INDEX_KEYSPACE.to_string(), key);
    IoCall::child(RowBatch::delete(vec![row]))
}

fn delay(ts: u64, now_ms: u64) -> Duration {
    Duration::from_millis(ts.saturating_sub(now_ms))
}

/// A reconciled attempt keeps its expired lease row, which would pin the lease head at zero;
/// floor only an already-due head, a future one fires on time.
fn lease_delay(ts: u64, now_ms: u64) -> Duration {
    let raw = delay(ts, now_ms);
    if raw.is_zero() {
        JOB_RECONCILE_REARM
    } else {
        raw
    }
}

fn min_delay(due: Option<Duration>, lease: Option<Duration>) -> Option<Duration> {
    match (due, lease) {
        (Some(due), Some(lease)) => Some(due.min(lease)),
        (due, None) => due,
        (None, lease) => lease,
    }
}

type ScheduleHead = Result<Option<(u64, JobId)>, JobMutationError>;

fn schedule_head(result: IoResult) -> Result<ScheduleHead, IoError> {
    result.child::<Option<(u64, JobId)>, JobMutationError>()
}

impl DrainJobsOperation {
    pub fn new(owner_node_id: NodeId, budget: JobClassBudget, now_ms: u64) -> Self {
        Self {
            owner_node_id,
            budget,
            now_ms,
            result: JobDrainResult::default(),
            step: DrainStep::DuePage,
            rows: Vec::new(),
            next_page: None,
            expired: Vec::new(),
            expired_count: 0,
            reconciled_before: 0,
            lease_pages: 0,
        }
    }

    fn call(&mut self, step: DrainStep, call: IoCall) -> DrainNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// Stops the batch for a later retry; what was claimed is still handed off.
    fn stop(&mut self, message: &str, error: impl std::fmt::Display) -> DrainNext {
        warn!(error = %error, "{message}");
        self.result.retry_after_error = true;
        self.read_heads()
    }

    fn due_page(&mut self, after: Option<Key>) -> DrainNext {
        let page = schedule_page(DUE_INDEX_PREFIX, after, DRAIN_BATCH_SIZE);
        self.call(DrainStep::DuePage, IoCall::child(page))
    }

    fn due_paged(&mut self, page: Result<SchedulePage, JobMutationError>) -> DrainNext {
        let (mut rows, next) = match page {
            Ok(page) => page,
            // Nothing to hand off yet, so a plain failure is enough.
            Err(error) if self.result.claimed.is_empty() => {
                return Err(DrainError(error.to_string()));
            }
            Err(error) => return self.stop("Failed to page due jobs", error),
        };
        if rows.is_empty() {
            return self.sweep();
        }
        rows.reverse();
        self.rows = rows;
        self.next_page = next;
        self.next_due()
    }

    fn drop_due(&mut self, key: Key, failure: &'static str) -> DrainNext {
        self.call(DrainStep::DueDrop(failure), drop_row(key))
    }

    fn next_due(&mut self) -> DrainNext {
        let Some((key, _)) = self.rows.pop() else {
            return match self.next_page.take() {
                Some(next) => self.due_page(Some(next)),
                None => self.sweep(),
            };
        };
        let (ts, job_id) = match parse_schedule_key(key.as_ref()) {
            Ok(parsed) => parsed,
            Err(error) => {
                warn!(error = %error, "Deleting malformed job due index row");
                return self.drop_due(key, "Failed to drop malformed job due index row");
            }
        };
        // The index is timestamp-ordered, so the first future row ends the scan.
        if ts > self.now_ms {
            return self.sweep();
        }
        let read = IoCall::child(read_job_record(job_id));
        self.call(DrainStep::DueRecord { key, job_id }, read)
    }

    fn due_record(&mut self, key: Key, job_id: JobId, record: JobRecord) -> DrainNext {
        // A foreign-owned due row is an index inconsistency: only the immutable owner may
        // run the job, so drop it rather than claim it or pin the timer.
        if record.owner_node_id != self.owner_node_id {
            let owner = record.owner_node_id;
            warn!(job_id = %job_id, owner = %owner, "Dropping due row for a foreign-owned job");
            return self.drop_due(key, "Failed to drop foreign-owned job due row");
        }
        if self.budget.remaining(record.execution_class) == 0 {
            self.result.deferred_saturated = true;
            return self.next_due();
        }
        let claim = claim_job(job_id, self.owner_node_id, self.now_ms);
        self.call(DrainStep::DueClaim { key, job_id }, IoCall::child(claim))
    }

    fn claimed(
        &mut self,
        key: Key,
        job_id: JobId,
        outcome: Result<ClaimOutcome, JobMutationError>,
    ) -> DrainNext {
        match outcome {
            Ok(ClaimOutcome::Claimed(record)) => {
                self.budget.take(record.execution_class);
                self.result.claimed.push(record);
                if self.budget.is_empty() {
                    return self.sweep();
                }
                self.next_due()
            }
            Ok(ClaimOutcome::CancelledFresh(record)) => {
                self.result.cancelled_fresh = self.result.cancelled_fresh.saturating_add(1);
                self.result.terminal.push(record);
                self.next_due()
            }
            Ok(ClaimOutcome::NotEligible) => self.next_due(),
            Err(JobMutationError::NotFound) => {
                self.drop_due(key, "Failed to drop orphaned job due index row")
            }
            // Never discard jobs already claimed in this batch: stop here, hand them off,
            // and let the caller re-drive the remainder after a backoff.
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Failed to claim due job");
                self.result.retry_after_error = true;
                self.read_heads()
            }
        }
    }

    fn sweep(&mut self) -> DrainNext {
        self.rows.clear();
        self.next_page = None;
        self.lease_page(None)
    }

    fn lease_page(&mut self, after: Option<Key>) -> DrainNext {
        self.lease_pages += 1;
        let page = schedule_page(LEASE_INDEX_PREFIX, after, DRAIN_BATCH_SIZE);
        self.call(DrainStep::LeasePage, IoCall::child(page))
    }

    fn scan_leases(&mut self) -> DrainNext {
        while let Some((key, _)) = self.rows.pop() {
            match parse_schedule_key(key.as_ref()) {
                Ok((ts, job_id)) if ts <= self.now_ms => self.expired.push((ts, job_id)),
                Ok(_) => {
                    self.rows.clear();
                    break;
                }
                Err(error) => {
                    warn!(error = %error, "Deleting malformed job schedule index row during scan");
                    return self.call(DrainStep::LeaseDrop, drop_row(key));
                }
            }
        }
        self.expired_count = self.expired.len();
        self.reconciled_before = self.result.reconciled;
        self.expired.reverse();
        self.next_requeue()
    }

    fn next_requeue(&mut self) -> DrainNext {
        let Some((ts, job_id)) = self.expired.pop() else {
            return self.lease_round_done();
        };
        let error = JobError::retryable("lease expired");
        let requeue = requeue_job(job_id, None, self.now_ms, Some(self.now_ms), Some(error));
        self.call(DrainStep::Requeue { ts, job_id }, IoCall::child(requeue))
    }

    /// Only a full page of reconciled attempts hides later rows, so only then look further.
    fn lease_round_done(&mut self) -> DrainNext {
        let reconciled = self
            .result
            .reconciled
            .saturating_sub(self.reconciled_before);
        let full = self.expired_count == DRAIN_BATCH_SIZE && reconciled == self.expired_count;
        match self.next_page.take() {
            Some(next) if full && self.lease_pages < 2 => self.lease_page(Some(next)),
            _ => self.read_heads(),
        }
    }

    fn requeued(
        &mut self,
        ts: u64,
        job_id: JobId,
        outcome: Result<RequeueOutcome, JobMutationError>,
    ) -> DrainNext {
        match outcome {
            Ok(RequeueOutcome::NeedsReconcile(record)) => {
                self.result.lost_attempts.push(record);
                self.result.reconciled = self.result.reconciled.saturating_add(1);
            }
            Ok(RequeueOutcome::Exhausted(record)) => {
                self.result.swept = self.result.swept.saturating_add(1);
                self.result.terminal.push(record);
            }
            Ok(_) => self.result.swept = self.result.swept.saturating_add(1),
            Err(JobMutationError::NotFound) => {
                let orphan = drop_row(lease_index_key(ts, job_id));
                return self.call(DrainStep::LeaseOrphan, orphan);
            }
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Failed to requeue expired lease");
                self.result.retry_after_error = true;
                return self.read_heads();
            }
        }
        self.next_requeue()
    }

    fn read_heads(&mut self) -> DrainNext {
        let head = first_schedule_entry(DUE_INDEX_PREFIX);
        self.call(DrainStep::DueHead, IoCall::child(head))
    }

    fn finish(mut self_result: JobDrainResult, next: Option<Duration>) -> DrainNext {
        self_result.next_due_after = next;
        Ok(Next::Done(self_result))
    }

    fn take_result(&mut self) -> JobDrainResult {
        std::mem::take(&mut self.result)
    }

    fn heads_failed(&mut self, error: JobMutationError) -> DrainNext {
        warn!(error = %error, "Failed to compute next job drain timer");
        self.result.retry_after_error = true;
        Ok(Next::Done(self.take_result()))
    }
}

impl Operation for DrainJobsOperation {
    type Output = JobDrainResult;
    type Error = DrainError;

    fn first(&mut self) -> DrainNext {
        if self.budget.is_empty() {
            self.result.deferred_saturated = true;
            return self.sweep();
        }
        self.due_page(None)
    }

    fn next(&mut self, result: IoResult) -> DrainNext {
        match std::mem::replace(&mut self.step, DrainStep::DuePage) {
            DrainStep::DuePage => {
                let page = result.child::<SchedulePage, JobMutationError>()?;
                self.due_paged(page)
            }
            DrainStep::DueDrop(failure) => match result.child::<(), JobMutationError>()? {
                Ok(()) => self.next_due(),
                Err(error) => self.stop(failure, error),
            },
            DrainStep::DueRecord { key, job_id } => {
                match result.child::<Option<JobRecord>, JobMutationError>()? {
                    Ok(Some(record)) => self.due_record(key, job_id, record),
                    // Orphaned index row (record gone or quarantined): drop it so it cannot
                    // pin the drain timer at zero forever.
                    Ok(None) => self.drop_due(key, "Failed to drop orphaned job due index row"),
                    Err(error) => {
                        warn!(job_id = %job_id, error = %error, "Failed to read due job");
                        self.result.retry_after_error = true;
                        self.read_heads()
                    }
                }
            }
            DrainStep::DueClaim { key, job_id } => {
                let outcome = result.child::<ClaimOutcome, JobMutationError>()?;
                self.claimed(key, job_id, outcome)
            }
            DrainStep::LeasePage => match result.child::<SchedulePage, JobMutationError>()? {
                Ok((mut rows, next)) => {
                    rows.reverse();
                    self.rows = rows;
                    self.next_page = next;
                    self.scan_leases()
                }
                Err(error) => self.stop("Failed to scan expired job leases", error),
            },
            DrainStep::LeaseDrop => match result.child::<(), JobMutationError>()? {
                Ok(()) => self.scan_leases(),
                Err(error) => self.stop("Failed to scan expired job leases", error),
            },
            DrainStep::Requeue { ts, job_id } => {
                let outcome = result.child::<RequeueOutcome, JobMutationError>()?;
                self.requeued(ts, job_id, outcome)
            }
            DrainStep::LeaseOrphan => match result.child::<(), JobMutationError>()? {
                Ok(()) => self.next_requeue(),
                Err(error) => self.stop("Failed to drop orphaned job lease index row", error),
            },
            DrainStep::DueHead => match schedule_head(result)? {
                Ok(head) => {
                    let due = head.map(|(ts, _)| delay(ts, self.now_ms));
                    let lease = first_schedule_entry(LEASE_INDEX_PREFIX);
                    self.call(DrainStep::LeaseHead(due), IoCall::child(lease))
                }
                Err(error) => self.heads_failed(error),
            },
            DrainStep::LeaseHead(due) => match schedule_head(result)? {
                Ok(head) => {
                    let lease = head.map(|(ts, _)| lease_delay(ts, self.now_ms));
                    Self::finish(self.take_result(), min_delay(due, lease))
                }
                Err(error) => self.heads_failed(error),
            },
        }
    }
}

/// Moves the drain timer up to the earliest `due/` or `lease/` head; never later.
#[derive(Debug, PartialEq)]
pub struct DrainTimerOperation {
    now_ms: u64,
    due: Option<Option<Duration>>,
}

impl DrainTimerOperation {
    pub fn new(now_ms: u64) -> Self {
        Self { now_ms, due: None }
    }
}

impl Operation for DrainTimerOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let head = first_schedule_entry(DUE_INDEX_PREFIX);
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let head = match schedule_head(result)? {
            Ok(head) => head,
            Err(error) => {
                warn!(error = %error, "Failed to scan job schedule index");
                return Ok(Next::Done(()));
            }
        };
        let Some(due) = self.due else {
            self.due = Some(head.map(|(ts, _)| delay(ts, self.now_ms)));
            let lease = first_schedule_entry(LEASE_INDEX_PREFIX);
            return Ok(Next::Call(IoCall::child(lease)));
        };
        let lease = head.map(|(ts, _)| lease_delay(ts, self.now_ms));
        let Some(after) = min_delay(due, lease) else {
            return Ok(Next::Done(()));
        };
        let timer = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainJobQueue,
            after,
        });
        Ok(Next::Then {
            output: (),
            calls: vec![IoCall::Effect(Box::new(timer))],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::JOB_LEASE_MS;
    use crate::jobs::store::test_calls::insert_job;
    use crate::jobs::store::test_calls::read_job_record;
    use crate::storage_read::iter_prefix_page;
    use crate::tasks::job_reconcile::ExternalReconciler;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::events::Event;
    use aruna_core::handle::Handle;
    use aruna_core::structs::execution::job::{
        AttemptIntent, JobClaim, JobPayload, JobState, due_index_key,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::task::TaskEvent;
    use aruna_core::time::unix_timestamp_millis;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use byteview::ByteView;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use ulid::Ulid;

    async fn drain_job_batch(
        context: &DriverContext,
        owner: NodeId,
        budget: JobClassBudget,
        reconciler: Option<&Arc<dyn ExternalReconciler>>,
    ) -> Result<JobDrainResult, DrainError> {
        let drain = DrainJobsOperation::new(owner, budget, unix_timestamp_millis());
        let mut result = drive(drain, context).await?;
        for record in std::mem::take(&mut result.lost_attempts) {
            if let Some(reconciler) = reconciler {
                reconciler.reconcile_lost_attempt(context, record).await;
            }
        }
        Ok(result)
    }

    async fn next_drain_delay(context: &DriverContext) -> Result<Option<Duration>, IoError> {
        let now_ms = unix_timestamp_millis();
        let due = drive(first_schedule_entry(DUE_INDEX_PREFIX), context)
            .await
            .map_err(|error| IoError::Codec(error.to_string()))?;
        let lease = drive(first_schedule_entry(LEASE_INDEX_PREFIX), context)
            .await
            .map_err(|error| IoError::Codec(error.to_string()))?;
        Ok(min_delay(
            due.map(|(ts, _)| delay(ts, now_ms)),
            lease.map(|(ts, _)| lease_delay(ts, now_ms)),
        ))
    }

    async fn restore_drain_timer(context: &DriverContext) {
        let timer = DrainTimerOperation::new(unix_timestamp_millis());
        drive(timer, context).await.expect("timer scan");
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn page_of(rows: Vec<Key>, next: Option<Key>) -> IoResult {
        let rows = rows
            .into_iter()
            .map(|key| (key, Value::from(Vec::new())))
            .collect::<Vec<_>>();
        child(Ok::<SchedulePage, JobMutationError>((rows, next)))
    }

    fn is_child(next: &DrainNext) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    fn drain() -> DrainJobsOperation {
        DrainJobsOperation::new(node_id(3), budget_of(1), 100)
    }

    // A first due page that cannot be read fails, since nothing was claimed yet.
    #[test]
    fn first_page_fails() {
        let mut pass = drain();
        assert!(is_child(&pass.first()));
        let failed = child(Err::<SchedulePage, _>(JobMutationError::NotFound));
        assert!(pass.next(failed).is_err());
    }

    // An empty budget leaves due jobs alone and still sweeps the leases.
    #[test]
    fn empty_budget_sweeps() {
        let mut pass = DrainJobsOperation::new(node_id(3), JobClassBudget::default(), 100);
        assert!(is_child(&pass.first()));
        assert_eq!(pass.step, DrainStep::LeasePage);
        assert!(pass.result.deferred_saturated);
    }

    // A future due row ends the claim pass, and a malformed one is dropped first.
    #[test]
    fn due_rows_judged() {
        let job = job_id(1);
        let mut pass = drain();
        let future = due_index_key(200, job);
        assert!(is_child(&pass.next(page_of(vec![future], None))));
        assert_eq!(pass.step, DrainStep::LeasePage);

        let mut pass = drain();
        let bad = Key::from(vec![1u8]);
        assert!(is_child(&pass.next(page_of(vec![bad], None))));
        assert!(matches!(pass.step, DrainStep::DueDrop(_)));
    }

    // A claim failure stops the batch but keeps what it already claimed.
    #[test]
    fn claim_failure_stops() {
        let job = job_id(1);
        let mut pass = drain();
        pass.result.claimed.push(queued_record(job, 1));
        pass.step = DrainStep::DueClaim {
            key: due_index_key(1, job),
            job_id: job,
        };
        let failed = child(Err::<ClaimOutcome, _>(JobMutationError::TokenMismatch));
        assert!(is_child(&pass.next(failed)));
        assert_eq!(pass.step, DrainStep::DueHead);
        assert!(pass.result.retry_after_error);
        assert_eq!(pass.result.claimed.len(), 1);
    }

    // A lost external attempt goes to the caller's hook and is not charged here.
    #[test]
    fn lost_attempt_reported() {
        let job = job_id(1);
        let mut pass = drain();
        pass.step = DrainStep::Requeue { ts: 1, job_id: job };
        let lost = RequeueOutcome::NeedsReconcile(queued_record(job, 1));
        assert!(is_child(&pass.next(child(Ok::<_, JobMutationError>(lost)))));
        assert_eq!(pass.result.reconciled, 1);
        assert_eq!(pass.result.lost_attempts.len(), 1);
        assert_eq!(pass.result.swept, 0);
    }

    // Only a page made up entirely of reconciled attempts looks at the next page.
    #[test]
    fn lease_paging_bounded() {
        let mut pass = drain();
        pass.lease_pages = 1;
        pass.expired_count = DRAIN_BATCH_SIZE;
        pass.result.reconciled = DRAIN_BATCH_SIZE;
        pass.next_page = Some(Key::from(vec![1u8]));
        assert!(is_child(&pass.lease_round_done()));
        assert_eq!(pass.step, DrainStep::LeasePage);
        assert_eq!(pass.lease_pages, 2);

        pass.expired_count = DRAIN_BATCH_SIZE;
        pass.next_page = Some(Key::from(vec![1u8]));
        assert!(is_child(&pass.lease_round_done()));
        assert_eq!(pass.step, DrainStep::DueHead);
    }

    // The next delay is the nearer head, and an already-due lease waits for the floor.
    #[test]
    fn heads_set_delay() {
        let mut pass = drain();
        pass.step = DrainStep::DueHead;
        let due = child(Ok::<_, JobMutationError>(Some((150, job_id(1)))));
        assert!(is_child(&pass.next(due)));
        let lease = child(Ok::<_, JobMutationError>(Some((50, job_id(2)))));
        let Next::Done(result) = pass.next(lease).unwrap() else {
            panic!("expected the result");
        };
        assert_eq!(
            result.next_due_after,
            Some(Duration::from_millis(50).min(JOB_RECONCILE_REARM))
        );
    }

    // The timer moves only when a head exists.
    #[test]
    fn timer_follows_heads() {
        let mut timer = DrainTimerOperation::new(100);
        assert!(matches!(timer.first(), Ok(Next::Call(IoCall::Child(_)))));
        let none = || child(Ok::<Option<(u64, JobId)>, JobMutationError>(None));
        assert!(matches!(
            timer.next(none()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(timer.next(none()), Ok(Next::Done(()))));

        let mut timer = DrainTimerOperation::new(100);
        let due = child(Ok::<_, JobMutationError>(Some((150, job_id(1)))));
        timer.next(due).unwrap();
        assert!(matches!(timer.next(none()), Ok(Next::Then { .. })));
    }

    #[derive(Default)]
    struct RecordingReconciler {
        seen: Mutex<Vec<JobId>>,
    }

    #[async_trait::async_trait]
    impl ExternalReconciler for RecordingReconciler {
        async fn reconcile_lost_attempt(&self, _context: &DriverContext, record: JobRecord) {
            self.seen.lock().unwrap().push(record.job_id);
        }
    }

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn job_id(timestamp_ms: u64) -> JobId {
        JobId::from_parts(
            timestamp_ms,
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
            0,
        )
        .unwrap()
    }

    fn budget_of(slots: usize) -> JobClassBudget {
        JobClassBudget {
            in_process: slots,
            external: slots,
        }
    }

    fn queued_record(job_id: JobId, due_at_ms: u64) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            due_at_ms,
            due_at_ms,
            None,
        )
    }

    #[tokio::test]
    async fn claims_within_capacity() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        for seq in 1..=3u128 {
            insert_job(&context, &queued_record(job_id(seq as u64), 1))
                .await
                .unwrap();
        }

        let budget = JobClassBudget {
            in_process: 2,
            external: 0,
        };
        let result = drain_job_batch(&context, node_id(7), budget, None)
            .await
            .unwrap();
        assert_eq!(result.claimed.len(), 2, "capacity caps claims");
        // A due job remains, so the re-arm is immediate.
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
    }

    #[tokio::test]
    async fn skips_saturated_class() {
        // External slots are gone while in-process slots are free: the shared due
        // index must not claim and release the external row it cannot run.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let external = job_id(1);
        let mut record = queued_record(external, 1);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        insert_job(&context, &record).await.unwrap();
        let internal = job_id(2);
        insert_job(&context, &queued_record(internal, 1))
            .await
            .unwrap();

        let budget = JobClassBudget {
            in_process: 4,
            external: 0,
        };
        let result = drain_job_batch(&context, node_id(7), budget, None)
            .await
            .unwrap();

        assert_eq!(
            result
                .claimed
                .iter()
                .map(|record| record.job_id)
                .collect::<Vec<_>>(),
            vec![internal],
            "a saturated class must not starve the free one"
        );
        assert!(
            result.deferred_saturated,
            "the skipped row must be reported"
        );
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
        let stored = read_job_record(&context, external, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued, "no claim/release churn");
        assert!(stored.claim.is_none());
        assert_eq!(stored.updated_at_ms, 1, "the record was never rewritten");
        assert_eq!(stored.due_at_ms, 1);
    }

    #[tokio::test]
    async fn empty_budget_defers() {
        // Both classes saturated with a due row present: the caller must be told to
        // back off instead of re-arming the drain at zero.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = job_id(1);
        insert_job(&context, &queued_record(job_id, 1))
            .await
            .unwrap();

        let result = drain_job_batch(&context, node_id(3), JobClassBudget::default(), None)
            .await
            .unwrap();

        assert!(result.claimed.is_empty());
        assert!(result.deferred_saturated);
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
        let stored = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued);
        assert_eq!(stored.updated_at_ms, 1);
    }

    #[tokio::test]
    async fn drops_foreign_rows() {
        // A due row for a job owned elsewhere is dropped, never claimed: only the
        // immutable owner may run a JobId, and the dead row must not pin the timer.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let foreign = job_id(1);
        insert_job(&context, &queued_record(foreign, 1))
            .await
            .unwrap();

        let result = drain_job_batch(&context, node_id(3), budget_of(8), None)
            .await
            .unwrap();

        assert!(result.claimed.is_empty(), "no claim on a foreign owner");
        let stored = read_job_record(&context, foreign, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued, "the record is untouched");
        assert!(stored.claim.is_none());
        assert_eq!(next_drain_delay(&context).await.unwrap(), None);
    }

    #[tokio::test]
    async fn expired_lease_requeued() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([4u8; 16]);
        let mut record = queued_record(job_id, 1);
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::generate(),
            lease_expires_ms: 1,
        });
        insert_job(&context, &record).await.unwrap();

        let result = drain_job_batch(&context, node_id(3), budget_of(8), None)
            .await
            .unwrap();
        assert_eq!(result.swept, 1);
        let requeued = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(requeued.state, JobState::Queued);
        assert_eq!(requeued.attempts, 1);
        assert!(requeued.claim.is_none());
    }

    // An external attempt with an expired lease routes to reconcile and is not
    // requeued; its lease row stays, so re-arm is floored instead of busy-looping.
    #[tokio::test]
    async fn external_lease_reconciled() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([0xE1; 16]);
        let mut record = queued_record(job_id, 1);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        record.state = JobState::Running;
        record.attempt_intent = Some(AttemptIntent {
            attempt_no: 1,
            external_name: "attempt".to_string(),
            executor_kind: "docker".to_string(),
            pinned_image: "alpine@sha256:digest".to_string(),
            attempt_epoch: 1,
        });
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::generate(),
            lease_expires_ms: 1,
        });
        insert_job(&context, &record).await.unwrap();

        let recorder = Arc::new(RecordingReconciler::default());
        let reconciler: Arc<dyn ExternalReconciler> = recorder.clone();
        let result = drain_job_batch(&context, node_id(3), budget_of(8), Some(&reconciler))
            .await
            .unwrap();
        assert_eq!(result.swept, 0, "external attempt is not swept");
        assert_eq!(result.reconciled, 1, "external attempt is reconciled");
        let after = result.next_due_after.expect("lease row still present");
        assert!(
            after >= JOB_RECONCILE_REARM,
            "reconciled row must not re-arm the drain at zero, got {after:?}"
        );

        let stored = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Running, "not requeued");
        assert_eq!(stored.attempts, 0, "the sweep charges nothing");
        assert!(stored.claim.is_some());
        // The hook saw exactly this job, proving no blind re-run path was taken.
        assert_eq!(recorder.seen.lock().unwrap().as_slice(), &[job_id]);
    }

    #[tokio::test]
    async fn future_lease_kept() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut record = queued_record(job_id, 1);
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::generate(),
            lease_expires_ms: unix_timestamp_millis() + JOB_LEASE_MS,
        });
        insert_job(&context, &record).await.unwrap();

        let result = drain_job_batch(&context, node_id(3), budget_of(8), None)
            .await
            .unwrap();
        assert_eq!(result.swept, 0);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn restore_shortens_timer() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut context = crate::testing::test_context(storage.clone());
        let task_handle = TaskHandle::new();
        context.task_handle = Some(task_handle.clone());
        let due_far_out = unix_timestamp_millis() + 7_200_000;
        insert_job(
            &context,
            &queued_record(JobId::from_bytes([1u8; 16]), due_far_out),
        )
        .await
        .unwrap();

        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainJobQueue,
                after: Duration::from_secs(3600),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };

        restore_drain_timer(&context).await;

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainJobQueue,
                after: Duration::from_secs(10_000),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };
        assert!(
            after <= Duration::from_secs(3600),
            "restore must ShortenTimer, not push the deadline out"
        );
    }

    #[tokio::test]
    async fn orphan_row_heals() {
        use aruna_core::effects::StorageEffect;
        use aruna_core::events::StorageEvent;

        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let orphan = JobId::from_bytes([9u8; 16]);
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: SCHEDULE_INDEX_KEYSPACE.to_string(),
                key: due_index_key(1, orphan),
                value: ByteView::from(Vec::new()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let result = drain_job_batch(&context, node_id(3), budget_of(8), None)
            .await
            .unwrap();
        assert!(result.claimed.is_empty());
        // Self-healed: the orphan is gone and the drain timer stops returning zero.
        assert_eq!(next_drain_delay(&context).await.unwrap(), None);
        let (rows, _) = iter_prefix_page(&storage, SCHEDULE_INDEX_KEYSPACE, None, None, 8, None)
            .await
            .unwrap();
        assert!(rows.is_empty());
    }
}
