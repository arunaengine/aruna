//! The decisions of the job runtime as operations: how a finished run settles, how a parked job
//! waits for its keys, and what a restart does with the jobs it finds in flight.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::SCHEDULE_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobError, JobErrorKind, JobExecutionClass, JobId, JobProgress, JobRecord, JobResultPayload,
    KeyWait, LEASE_INDEX_PREFIX, parse_schedule_key,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::Key;
use tracing::{info, warn};
use ulid::Ulid;

use super::DRAIN_BATCH_SIZE;
use super::executor::JobRunOutcome;
use super::key_locks::KeyUnlockedOperation;
use super::store::JobMutationError;
use super::store::job_calls::{
    ReleaseOutcome, RequeueOutcome, cancel_running_job, complete_job, defer_job, exhaust_job,
    fail_job, release_job, requeue_job, requeue_progressed,
};
use super::store::park_wake::AwaitOutcome;
use super::store::row_calls::{park_job, read_job_record, satisfy_key_wait};
use super::store::store_call::StoreCall;

const WRITE_MAX_ATTEMPTS: u32 = 5;
/// How long a deferred job waits before it may run again.
pub(crate) const RETRY_AFTER_MS: u64 = 1_000;

/// The terminal state one write gives a job.
#[derive(Debug, PartialEq)]
pub(crate) enum TerminalWrite {
    Complete {
        result: JobResultPayload,
        progress: JobProgress,
    },
    Fail(JobError),
    Cancel,
    Exhaust(JobError),
}

/// Writes a terminal state past transient storage failures: the execution already finished, so
/// a storage error would otherwise leave it `Running` until the sweep runs it again. Token and
/// transition races are legitimate and end the write at once.
#[derive(Debug, PartialEq)]
pub(crate) struct TerminalWriteOperation {
    job_id: JobId,
    token: Ulid,
    write: TerminalWrite,
    now_ms: u64,
    attempt: u32,
    waiting: bool,
}

impl TerminalWriteOperation {
    pub(crate) fn new(job_id: JobId, token: Ulid, write: TerminalWrite, now_ms: u64) -> Self {
        Self {
            job_id,
            token,
            write,
            now_ms,
            attempt: 0,
            waiting: false,
        }
    }

    fn call(&self) -> Next<JobRecord> {
        let (job_id, token, now_ms) = (self.job_id, self.token, self.now_ms);
        let store = match &self.write {
            TerminalWrite::Complete { result, progress } => {
                complete_job(job_id, token, result.clone(), progress.clone(), now_ms)
            }
            TerminalWrite::Fail(error) => fail_job(job_id, token, error.clone(), now_ms),
            TerminalWrite::Cancel => cancel_running_job(job_id, token, now_ms),
            TerminalWrite::Exhaust(error) => exhaust_job(job_id, token, error.clone(), now_ms),
        };
        Next::Call(IoCall::child(store))
    }
}

impl Operation for TerminalWriteOperation {
    type Output = JobRecord;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<JobRecord>, JobMutationError> {
        Ok(self.call())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobRecord>, JobMutationError> {
        if self.waiting {
            self.waiting = false;
            return Ok(self.call());
        }
        match result.child::<JobRecord, JobMutationError>()? {
            Err(JobMutationError::Storage(error)) if self.attempt + 1 < WRITE_MAX_ATTEMPTS => {
                warn!(error = %error, "Retrying job terminal write after storage failure");
                let pause = Duration::from_millis(20u64 << self.attempt);
                self.attempt += 1;
                self.waiting = true;
                Ok(Next::Call(IoCall::Delay(pause)))
            }
            other => other.map(Next::Done),
        }
    }
}

/// Logs how a terminal write ended; a lost race leaves the winner's state.
pub(crate) fn log_terminal(result: &Result<JobRecord, JobMutationError>, job_id: JobId) {
    match result {
        Ok(_) => {}
        Err(JobMutationError::TokenMismatch) => {
            warn!(job_id = %job_id, "Terminal write rejected: job was re-claimed")
        }
        Err(JobMutationError::IllegalTransition(_)) => {
            info!(job_id = %job_id, "Lost terminal transition race; leaving winner's state")
        }
        Err(error) => warn!(job_id = %job_id, error = %error, "Failed terminal transition"),
    }
}

/// A transient failure after a durable checkpoint advanced spends no attempt of a long job.
pub(crate) fn requeue_call(
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    error: JobError,
    checkpointed: bool,
) -> StoreCall<RequeueOutcome> {
    match checkpointed {
        true => requeue_progressed(job_id, token, now_ms, error),
        false => requeue_job(job_id, Some(token), now_ms, None, Some(error)),
    }
}

#[derive(Debug, PartialEq)]
enum KeyStep {
    Park,
    Unlocked(BucketKeyRef),
    Wake,
    Cancel,
    Defer,
}

/// Parks a job that needs locked keys, then wakes it for any key unlocked meanwhile. The output
/// says whether the job was cancelled, so its cleanup handler is due.
#[derive(Debug, PartialEq)]
pub(crate) struct AwaitKeysOperation {
    job_id: JobId,
    token: Ulid,
    waits: Vec<KeyWait>,
    keys: VecDeque<BucketKeyRef>,
    now_ms: u64,
    step: KeyStep,
}

impl AwaitKeysOperation {
    pub(crate) fn new(job_id: JobId, token: Ulid, waits: Vec<KeyWait>, now_ms: u64) -> Self {
        Self {
            job_id,
            token,
            keys: waits.iter().map(|wait| wait.key).collect(),
            waits,
            now_ms,
            step: KeyStep::Park,
        }
    }

    fn next_key(&mut self) -> Next<bool> {
        let Some(key) = self.keys.pop_front() else {
            return Next::Done(false);
        };
        self.step = KeyStep::Unlocked(key);
        Next::Call(IoCall::child(KeyUnlockedOperation::new(key)))
    }

    fn parked(&mut self, outcome: Result<AwaitOutcome, JobMutationError>) -> Next<bool> {
        match outcome {
            Ok(AwaitOutcome::Parked(_)) => self.next_key(),
            Ok(AwaitOutcome::Skipped) => {
                self.step = KeyStep::Cancel;
                let cancel = TerminalWriteOperation::new(
                    self.job_id,
                    self.token,
                    TerminalWrite::Cancel,
                    self.now_ms,
                );
                Next::Call(IoCall::child(cancel))
            }
            Err(JobMutationError::TokenMismatch) => Next::Done(false),
            Err(error) => {
                let job_id = self.job_id;
                warn!(job_id = %job_id, error = %error, "Failed to park job for a bucket key");
                self.step = KeyStep::Defer;
                Next::Call(IoCall::child(defer_job(
                    job_id,
                    self.token,
                    self.now_ms,
                    RETRY_AFTER_MS,
                    JobError::retryable("bucket key is locked"),
                )))
            }
        }
    }
}

impl Operation for AwaitKeysOperation {
    type Output = bool;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<bool>, JobMutationError> {
        let waits = std::mem::take(&mut self.waits);
        let park = park_job(self.job_id, self.token, self.now_ms, waits);
        Ok(Next::Call(IoCall::child(park)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, JobMutationError> {
        let job_id = self.job_id;
        match std::mem::replace(&mut self.step, KeyStep::Wake) {
            KeyStep::Park => Ok(self.parked(result.child()?)),
            KeyStep::Unlocked(key) => {
                // A key that cannot be read as unlocked keeps the job parked.
                if result.child::<bool, IoError>()?.unwrap_or(false) {
                    self.step = KeyStep::Wake;
                    let wake = satisfy_key_wait(job_id, key, self.now_ms);
                    return Ok(Next::Call(IoCall::child(wake)));
                }
                Ok(self.next_key())
            }
            KeyStep::Wake => {
                if let Err(error) = result.child::<bool, JobMutationError>()? {
                    warn!(job_id = %job_id, error = %error, "Failed to wake parked job");
                }
                Ok(self.next_key())
            }
            KeyStep::Cancel => {
                let terminal = result.child::<JobRecord, JobMutationError>()?;
                let cleanup = !matches!(&terminal, Err(JobMutationError::TokenMismatch));
                log_terminal(&terminal, job_id);
                Ok(Next::Done(cleanup))
            }
            KeyStep::Defer => {
                if let Err(error) = result.child::<ReleaseOutcome, JobMutationError>()? {
                    warn!(job_id = %job_id, error = %error, "Failed to defer job");
                }
                Ok(Next::Done(false))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettleStep {
    Start,
    Terminal { cleanup: bool },
    Requeue,
    Defer,
    Release,
    Keys,
}

/// Writes what a finished run of an in-process job leaves behind. The output says whether the
/// job's cleanup handler is due, which the caller runs.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleRunOperation {
    job_id: JobId,
    token: Ulid,
    outcome: Option<JobRunOutcome>,
    progress: JobProgress,
    checkpointed: bool,
    now_ms: u64,
    step: SettleStep,
}

impl SettleRunOperation {
    /// `now_ms` is read after the payload ran, so finish time, backoff and retention reflect
    /// its completion and not its start.
    pub(crate) fn new(
        (job_id, token): (JobId, Ulid),
        outcome: JobRunOutcome,
        (progress, checkpointed): (JobProgress, bool),
        now_ms: u64,
    ) -> Self {
        Self {
            job_id,
            token,
            outcome: Some(outcome),
            progress,
            checkpointed,
            now_ms,
            step: SettleStep::Start,
        }
    }

    fn terminal(&mut self, write: TerminalWrite, cleanup: bool) -> Next<bool> {
        self.step = SettleStep::Terminal { cleanup };
        let write = TerminalWriteOperation::new(self.job_id, self.token, write, self.now_ms);
        Next::Call(IoCall::child(write))
    }

    fn defer(&mut self, error: JobError, delay_ms: u64) -> Next<bool> {
        self.step = SettleStep::Defer;
        let defer = defer_job(self.job_id, self.token, self.now_ms, delay_ms, error);
        Next::Call(IoCall::child(defer))
    }
}

impl Operation for SettleRunOperation {
    type Output = bool;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<bool>, JobMutationError> {
        let (job_id, token, now_ms) = (self.job_id, self.token, self.now_ms);
        let Some(outcome) = self.outcome.take() else {
            return Ok(Next::Done(false));
        };
        Ok(match outcome {
            JobRunOutcome::Succeeded(result) => {
                let progress = self.progress.clone();
                self.terminal(TerminalWrite::Complete { result, progress }, false)
            }
            // External attempts return before supervision and cannot reach this requeue.
            JobRunOutcome::Failed(error) if error.kind == JobErrorKind::Retryable => {
                self.step = SettleStep::Requeue;
                let requeue = requeue_call(job_id, token, now_ms, error, self.checkpointed);
                Next::Call(IoCall::child(requeue))
            }
            JobRunOutcome::Failed(error) => self.terminal(TerminalWrite::Fail(error), false),
            JobRunOutcome::Deferred(error) => self.defer(error, RETRY_AFTER_MS),
            JobRunOutcome::DeferredFor(error, delay) => {
                self.defer(error, u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
            }
            JobRunOutcome::Cancelled => self.terminal(TerminalWrite::Cancel, true),
            // Shutdown, not cancellation: no cleanup handler, no attempt spent.
            JobRunOutcome::Interrupted => {
                self.step = SettleStep::Release;
                Next::Call(IoCall::child(release_job(job_id, token, now_ms)))
            }
            JobRunOutcome::AwaitingKey(waits) => {
                self.step = SettleStep::Keys;
                let wait = AwaitKeysOperation::new(job_id, token, waits, now_ms);
                Next::Call(IoCall::child(wait))
            }
            JobRunOutcome::Undecided(error) => self.terminal(TerminalWrite::Exhaust(error), false),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, JobMutationError> {
        let job_id = self.job_id;
        Ok(Next::Done(match self.step {
            SettleStep::Start => false,
            SettleStep::Terminal { cleanup } => {
                let terminal = result.child::<JobRecord, JobMutationError>()?;
                let due = cleanup && !matches!(&terminal, Err(JobMutationError::TokenMismatch));
                log_terminal(&terminal, job_id);
                due
            }
            SettleStep::Requeue => {
                let requeued = result.child::<RequeueOutcome, JobMutationError>()?;
                if let Err(error) = requeued
                    && error != JobMutationError::TokenMismatch
                {
                    warn!(job_id = %job_id, error = %error, "Failed to requeue job");
                }
                false
            }
            SettleStep::Defer => {
                let deferred = result.child::<ReleaseOutcome, JobMutationError>()?;
                if let Err(error) = deferred
                    && error != JobMutationError::TokenMismatch
                {
                    warn!(job_id = %job_id, error = %error, "Failed to defer job");
                }
                false
            }
            SettleStep::Release => {
                match result.child::<ReleaseOutcome, JobMutationError>()? {
                    Ok(_) => {}
                    Err(JobMutationError::TokenMismatch) => {
                        info!(job_id = %job_id, "Lease already handed back or re-claimed")
                    }
                    Err(error) => {
                        warn!(job_id = %job_id, error = %error, "Failed to hand back job lease")
                    }
                }
                false
            }
            SettleStep::Keys => match result.child::<bool, JobMutationError>()? {
                Ok(cleanup) => cleanup,
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Failed to wait for job keys");
                    false
                }
            },
        }))
    }
}

/// Reads one page of the jobs that hold a lease: their ids and the key to continue after.
#[derive(Debug, PartialEq)]
pub(crate) struct LeasePageOperation {
    after: Option<Key>,
}

impl LeasePageOperation {
    pub(crate) fn new(after: Option<Key>) -> Self {
        Self { after }
    }
}

impl Operation for LeasePageOperation {
    type Output = (Vec<JobId>, Option<Key>);
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: SCHEDULE_INDEX_KEYSPACE.to_string(),
            prefix: Key::from(LEASE_INDEX_PREFIX.to_vec()),
            after: self.after.take(),
            limit: DRAIN_BATCH_SIZE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let (rows, next) = result.page()?;
        let jobs = (rows.iter())
            .filter_map(|(key, _)| parse_schedule_key(key.as_ref()).ok())
            .map(|(_, job_id)| job_id)
            .collect();
        Ok(Next::Done((jobs, next)))
    }
}

/// What a restart did with one job it found holding a lease.
#[derive(Debug, PartialEq)]
pub(crate) enum Recovery {
    Recovered,
    Untouched,
    /// A submitted external attempt: only the reconciler may decide about it.
    Reconcile(Box<JobRecord>),
}

#[derive(Debug, PartialEq)]
enum RecoverStep {
    Read,
    Release,
    Requeue,
}

/// At startup every holder of a lease is dead. An in-process job goes back to the queue; a
/// blind requeue of an external attempt would run it twice, so it goes to the reconciler.
#[derive(Debug, PartialEq)]
pub(crate) struct RecoverJobOperation {
    job_id: JobId,
    now_ms: u64,
    step: RecoverStep,
}

impl RecoverJobOperation {
    pub(crate) fn new(job_id: JobId, now_ms: u64) -> Self {
        Self {
            job_id,
            now_ms,
            step: RecoverStep::Read,
        }
    }
}

impl Operation for RecoverJobOperation {
    type Output = Recovery;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<Recovery>, JobMutationError> {
        Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Recovery>, JobMutationError> {
        let (job_id, now_ms) = (self.job_id, self.now_ms);
        match self.step {
            RecoverStep::Read => {
                let Some(record) = result.child::<Option<JobRecord>, JobMutationError>()?? else {
                    return Ok(Next::Done(Recovery::Untouched));
                };
                if record.execution_class != JobExecutionClass::InProcess {
                    self.step = RecoverStep::Requeue;
                    let error = JobError::retryable("node restarted while job was in flight");
                    let requeue = requeue_job(job_id, None, now_ms, None, Some(error));
                    return Ok(Next::Call(IoCall::child(requeue)));
                }
                let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
                    return Ok(Next::Done(Recovery::Untouched));
                };
                self.step = RecoverStep::Release;
                Ok(Next::Call(IoCall::child(release_job(
                    job_id, token, now_ms,
                ))))
            }
            RecoverStep::Release => match result.child::<ReleaseOutcome, JobMutationError>()? {
                Ok(ReleaseOutcome::Released(_)) => Ok(Next::Done(Recovery::Recovered)),
                Ok(ReleaseOutcome::Skipped)
                | Err(JobMutationError::NotFound | JobMutationError::TokenMismatch) => {
                    Ok(Next::Done(Recovery::Untouched))
                }
                Err(error) => Err(error),
            },
            RecoverStep::Requeue => match result.child::<RequeueOutcome, JobMutationError>()? {
                Ok(RequeueOutcome::NeedsReconcile(record)) => {
                    Ok(Next::Done(Recovery::Reconcile(Box::new(record))))
                }
                Ok(_) => Ok(Next::Done(Recovery::Recovered)),
                Err(JobMutationError::NotFound) => Ok(Next::Done(Recovery::Untouched)),
                Err(error) => Err(error),
            },
        }
    }
}

#[cfg(test)]
#[path = "runtime_steps_tests.rs"]
mod tests;
