//! The call forms of every change to a job row and its attempt control. Each function builds an
//! operation; callers drive it or run it as a typed child.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::hint::{SetHint, mint_version};

use super::attempt_intent::{AttemptCommit, RecordIntentOperation};
use super::control_mutation::{ControlChange, MutateControlOperation};
use super::job_rows::{PutEntryOperation, PutRowsOperation};
use super::mark_start::MarkStartOperation;
use super::mutation::{ChangeJobOperation, MutateJobOperation, NoGuard};
use super::row_calls::read_job_record;
use super::run_delete::DeleteRunOperation;
use super::store_call::StoreCall;
use super::*;

const JOB_EXHAUSTED: &str = "job mutation exhausted conflict retries";
const ATTEMPT_EXHAUSTED: &str = "attempt mutation exhausted conflict retries";

/// Changes one job and its index rows in one transaction, retrying a lost conflict on the
/// current row. A skipped change answers the row as read.
pub fn change_job<F>(job_id: JobId, mutate: F) -> StoreCall<JobRecord>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Clone + Send + 'static,
{
    let build = move || MutateJobOperation::new(job_id, mutate.clone(), None::<NoGuard>);
    StoreCall::of(build, Ok, JOB_EXHAUSTED)
}

/// [`change_job`], with `guard` re-checking the changed record against the attempt control read
/// in that transaction; a rejection aborts before any write.
pub fn change_guarded<F, G>(job_id: JobId, mutate: F, guard: G) -> StoreCall<JobRecord>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Clone + Send + 'static,
    G: FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError>
        + Clone
        + Send
        + 'static,
{
    let build = move || MutateJobOperation::new(job_id, mutate.clone(), Some(guard.clone()));
    StoreCall::of(build, Ok, JOB_EXHAUSTED)
}

/// [`change_job`], with `map` told the final row and whether the change was written.
fn change_mapped<T, F, M>(job_id: JobId, mutate: F, mut map: M) -> StoreCall<T>
where
    T: 'static,
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Clone + Send + 'static,
    M: FnMut(JobRecord, bool) -> T + Send + 'static,
{
    let build = move || ChangeJobOperation::new(job_id, mutate.clone());
    let map = move |(record, persisted)| Ok(map(record, persisted));
    StoreCall::of(build, map, JOB_EXHAUSTED)
}

/// Changes a job and the control row of its current attempt, with `extra` rows that must land
/// with them, and answers what was stored.
fn change_control<F>(job_id: JobId, extra: JobWrites, mutate: F) -> StoreCall<ControlChange>
where
    F: FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError>
        + Clone
        + Send
        + 'static,
{
    let build = move || MutateControlOperation::new(job_id, extra.clone(), mutate.clone());
    StoreCall::of(build, Ok, ATTEMPT_EXHAUSTED)
}

fn control_of(job_id: JobId, change: impl ControlEdit) -> StoreCall<AttemptControl> {
    let build = move || MutateControlOperation::new(job_id, Vec::new(), change.clone());
    StoreCall::of(
        build,
        |changed: ControlChange| Ok(changed.control),
        ATTEMPT_EXHAUSTED,
    )
}

/// A change to a job and its attempt control that can run again on a fresh attempt.
trait ControlEdit:
    FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError>
    + Clone
    + Send
    + 'static
{
}

impl<F> ControlEdit for F where
    F: FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError>
        + Clone
        + Send
        + 'static
{
}

fn renew(record: &mut JobRecord, now_ms: u64) {
    if let Some(claim) = record.claim.as_mut() {
        claim.lease_expires_ms = now_ms.saturating_add(JOB_LEASE_MS);
    }
}

pub fn transition_to_running(job_id: JobId, token: Ulid, now_ms: u64) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Running;
        record.has_run = true;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
}

pub fn complete_job(
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.progress = final_progress.clone();
        record.result = Some(result.clone());
        // An error of an earlier run is no outcome of the job.
        record.last_error = None;
        record.claim = None;
        Ok(JobMutation::Persist)
    })
}

pub fn fail_job(job_id: JobId, token: Ulid, error: JobError, now_ms: u64) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Failed;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.last_error = Some(error.clone());
        record.claim = None;
        Ok(JobMutation::Persist)
    })
}

/// Ends the attempts of a job whose outcome cannot be known: `Indeterminate` and locally
/// exhausted, so nothing tries it again here and nothing claims it failed.
pub fn exhaust_job(
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.last_error = Some(JobError::retryable(error.message.clone()));
        record.updated_at_ms = now_ms;
        exhaust_attempts(record, now_ms);
        Ok(JobMutation::Persist)
    })
}

pub fn cancel_running_job(job_id: JobId, token: Ulid, now_ms: u64) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Cancelled;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.claim = None;
        Ok(JobMutation::Persist)
    })
}

/// Advance a claimed execution job to `Preparing`, renewing the lease. The
/// workspace is built and inputs staged in this phase.
pub fn transition_to_preparing(job_id: JobId, token: Ulid, now_ms: u64) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Preparing;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
}

/// `Preparing -> Ready`: inputs staged and credentials minted.
pub fn transition_to_ready(job_id: JobId, token: Ulid, now_ms: u64) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Ready;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
}

/// Persist backend start evidence without changing execution state or timestamps.
pub fn record_attempt_started(
    job_id: JobId,
    token: Ulid,
    started_at_ms: u64,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        if record.started_at_ms.is_some() {
            return Ok(JobMutation::Skip);
        }
        record.started_at_ms = Some(started_at_ms);
        Ok(JobMutation::Persist)
    })
}

/// `Running/Indeterminate -> Cancelling`: a durable cancel intent precedes the
/// backend stop so no evidence is lost across a crash.
pub fn transition_to_cancelling(job_id: JobId, token: Ulid, now_ms: u64) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Cancelling;
        record.updated_at_ms = now_ms;
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_ms = now_ms.saturating_add(JOB_LEASE_MS);
        }
        Ok(JobMutation::Persist)
    })
}

/// Terminal `Failed` for an execution job, capturing the exit evidence in the
/// result so a failed run still yields a crate.
pub fn fail_execution(
    job_id: JobId,
    token: Ulid,
    error: JobError,
    result: JobResultPayload,
    now_ms: u64,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Failed;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.last_error = Some(error.clone());
        record.result = Some(result.clone());
        record.claim = None;
        Ok(JobMutation::Persist)
    })
}

/// Terminal `Cancelled` for an execution job (from `Cancelling`), recording the
/// exit evidence.
pub fn cancel_execution(
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    now_ms: u64,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.state = JobState::Cancelled;
        record.finished_at_ms = Some(now_ms);
        record.updated_at_ms = now_ms;
        record.result = Some(result.clone());
        record.claim = None;
        Ok(JobMutation::Persist)
    })
}

/// Requeue an execution job that failed BEFORE its attempt was submitted (image
/// pull, prepare error): from `Preparing`/`Ready` back to `Queued` with backoff,
/// or terminal `Failed` once attempts are spent. Safe because no container exists.
pub fn requeue_before_attempt(
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    error: JobError,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        guard_token(record, token)?;
        record.last_error = Some(error.clone());
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = now_ms;
        record.claim = None;
        record.attempt_intent = None;
        if record.attempts >= JOB_MAX_ATTEMPTS {
            exhaust_attempts(record, now_ms);
        } else {
            record.state = JobState::Queued;
            record.due_at_ms = now_ms.saturating_add(retry_delay_ms(record.attempts));
        }
        Ok(JobMutation::Persist)
    })
}

#[derive(Debug, PartialEq)]
pub enum ClaimOutcome {
    Claimed(JobRecord),
    CancelledFresh(JobRecord),
    NotEligible,
}

/// Claim a queued job, or cancel it directly if it never ran and is cancel-requested. Only a
/// claim won here returns a token; an already claimed job is not eligible.
pub fn claim_job(job_id: JobId, holder_node_id: NodeId, now_ms: u64) -> StoreCall<ClaimOutcome> {
    let mutate = move |record: &mut JobRecord| {
        if record.state != JobState::Queued {
            return Ok(JobMutation::Skip);
        }
        record.updated_at_ms = now_ms;
        if record.cancel_requested && !record.has_run && record.attempt_intent.is_none() {
            record.state = JobState::Cancelled;
            record.finished_at_ms = Some(now_ms);
            record.claim = None;
            return Ok(JobMutation::Persist);
        }
        record.state = JobState::Claimed;
        record.claim = Some(JobClaim {
            holder_node_id,
            claim_token: Ulid::generate(),
            lease_expires_ms: now_ms.saturating_add(JOB_LEASE_MS),
        });
        Ok(JobMutation::Persist)
    };
    change_mapped(job_id, mutate, |record, persisted| match persisted {
        false => ClaimOutcome::NotEligible,
        true if record.state == JobState::Claimed => ClaimOutcome::Claimed(record),
        true => ClaimOutcome::CancelledFresh(record),
    })
}

#[derive(Debug, PartialEq)]
pub struct RenewOutcome {
    pub cancel_requested: bool,
}

fn renewed(record: JobRecord) -> Result<RenewOutcome, JobMutationError> {
    Ok(RenewOutcome {
        cancel_requested: record.cancel_requested,
    })
}

pub fn renew_lease(
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    progress: Option<JobProgress>,
) -> StoreCall<RenewOutcome> {
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        record.updated_at_ms = now_ms;
        renew(record, now_ms);
        if let Some(progress) = &progress {
            record.progress = progress.clone();
        }
        Ok(JobMutation::Persist)
    };
    let build = move || MutateJobOperation::new(job_id, mutate.clone(), None::<NoGuard>);
    StoreCall::of(build, renewed, JOB_EXHAUSTED)
}

/// Persist an in-memory progress snapshot without touching the lease or state.
pub fn flush_progress(
    job_id: JobId,
    token: Ulid,
    progress: JobProgress,
    now_ms: u64,
) -> StoreCall<RenewOutcome> {
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        record.progress = progress.clone();
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    };
    let build = move || MutateJobOperation::new(job_id, mutate.clone(), None::<NoGuard>);
    StoreCall::of(build, renewed, JOB_EXHAUSTED)
}

/// The final fields of a successful run, and what proves its outputs.
#[derive(Clone, Debug, PartialEq)]
pub struct RunSuccess {
    pub result: JobResultPayload,
    pub progress: JobProgress,
    pub now_ms: u64,
}

fn succeed(record: &mut JobRecord, success: &RunSuccess) {
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(success.now_ms);
    record.updated_at_ms = success.now_ms;
    record.progress = success.progress.clone();
    record.result = Some(success.result.clone());
    record.last_error = None;
    record.claim = None;
}

/// The storage-level success invariant: `Succeeded` commits in the transaction that reads the
/// attempt control, and only when every output binds the active ExecutionId, names a reserved
/// version, and the exact output record is durable.
fn outputs_proven(
    proof: JobResultPayload,
) -> impl FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError>
+ Clone
+ Send
+ 'static {
    move |record, control| match record.state {
        JobState::Succeeded => {
            let control = control.ok_or(JobMutationError::MissingControl)?;
            Ok(proof.proves_outputs(control)?)
        }
        _ => Ok(()),
    }
}

#[derive(Debug, PartialEq)]
pub enum ExecutionCompleteOutcome {
    Completed(JobRecord),
    CancelRequested(JobRecord),
}

/// Complete an execution only when cancellation has not already committed.
pub fn complete_execution(
    job_id: JobId,
    token: Ulid,
    success: RunSuccess,
) -> StoreCall<ExecutionCompleteOutcome> {
    let guard = outputs_proven(success.result.clone());
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        if record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        succeed(record, &success);
        Ok(JobMutation::Persist)
    };
    let build = move || ChangeJobOperation::guarded(job_id, mutate.clone(), guard.clone());
    let map = |(record, persisted)| {
        Ok(match persisted {
            true => ExecutionCompleteOutcome::Completed(record),
            false => ExecutionCompleteOutcome::CancelRequested(record),
        })
    };
    StoreCall::of(build, map, JOB_EXHAUSTED)
}

/// Complete an execution that finished successfully after cancellation was requested. It
/// carries the same durable-output invariant as an ordinary success.
pub fn complete_cancelled(job_id: JobId, token: Ulid, success: RunSuccess) -> StoreCall<JobRecord> {
    let guard = outputs_proven(success.result.clone());
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        succeed(record, &success);
        Ok(JobMutation::Persist)
    };
    change_guarded(job_id, mutate, guard)
}

/// Commit `Running` for an external attempt: from `Ready` after the backend accepts the fenced
/// attempt, or from `Indeterminate` when reconcile re-adopts a still-running container. The
/// launch record of the attempt is marked as executed in the same transaction.
pub fn begin_external_running(
    job_id: JobId,
    token: Ulid,
    started_at_ms: Option<u64>,
    now_ms: u64,
) -> StoreCall<JobRecord> {
    let attempt_started = started_at_ms.unwrap_or(now_ms);
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        if record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        // Running must always carry a start: it anchors the walltime cap, and a
        // backend that reports none still started when the controller saw it.
        record.started_at_ms.get_or_insert(attempt_started);
        record.state = JobState::Running;
        record.updated_at_ms = now_ms;
        renew(record, now_ms);
        Ok(JobMutation::Persist)
    };
    let build = move || MarkStartOperation::new(job_id, mutate.clone(), attempt_started);
    StoreCall::of(build, Ok, JOB_EXHAUSTED)
}

#[derive(Debug, PartialEq)]
pub enum ParkOutcome {
    Parked(JobRecord),
    /// The park spent the last attempt, so no further attempt runs on this node.
    Exhausted(JobRecord),
}

/// Park an ambiguous external attempt in `Indeterminate`, keeping the claim so the lease sweep
/// later re-routes it to reconciliation. Exits only on evidence or the attempt cap.
pub fn mark_indeterminate(
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> StoreCall<ParkOutcome> {
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        record.last_error = Some(error.clone());
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = now_ms;
        if record.attempts >= JOB_MAX_ATTEMPTS {
            exhaust_attempts(record, now_ms);
        } else {
            record.state = JobState::Indeterminate;
        }
        Ok(JobMutation::Persist)
    };
    change_mapped(job_id, mutate, |record, _| {
        match record.attempts >= JOB_MAX_ATTEMPTS {
            true => ParkOutcome::Exhausted(record),
            false => ParkOutcome::Parked(record),
        }
    })
}

/// How a job is requeued.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Requeue {
    token: Option<Ulid>,
    now_ms: u64,
    require_expired_before: Option<u64>,
    progressed: bool,
}

#[derive(Debug, PartialEq)]
pub enum RequeueOutcome {
    Requeued(JobRecord),
    /// `JOB_MAX_ATTEMPTS` spent: terminal `Failed` on a permanent verdict, otherwise
    /// `Indeterminate` and locally exhausted.
    Exhausted(JobRecord),
    /// A submitted external attempt lost its lease or the node restarted: requeuing
    /// would double-run the container, so the record is left for reconciliation.
    NeedsReconcile(JobRecord),
    Skipped,
}

/// Whether a requeue leaves `record` as it is, for a settled job or a lease still alive.
fn requeue_passes(record: &JobRecord, requeue: Requeue) -> bool {
    if record.is_settled() {
        return true;
    }
    let Some(now) = requeue.require_expired_before else {
        return false;
    };
    record
        .claim
        .as_ref()
        .is_none_or(|claim| claim.lease_expires_ms > now)
}

/// A submitted external attempt must not run twice. Checked after the lease: a live renewed
/// attempt is a plain skip and is not routed to the reconciler.
fn needs_reconcile(record: &JobRecord) -> bool {
    record.execution_class == JobExecutionClass::ExternalAttempt && record.attempt_intent.is_some()
}

fn requeue_counted(
    job_id: JobId,
    requeue: Requeue,
    error: Option<JobError>,
) -> StoreCall<RequeueOutcome> {
    let mutate = move |record: &mut JobRecord| {
        if let Some(token) = requeue.token {
            guard_token(record, token)?;
        }
        if requeue_passes(record, requeue) || needs_reconcile(record) {
            return Ok(JobMutation::Skip);
        }
        if let Some(error) = error.clone() {
            record.last_error = Some(error);
        }
        if requeue.progressed {
            record.attempts = 0;
        }
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at_ms = requeue.now_ms;
        record.claim = None;
        if record.attempts >= JOB_MAX_ATTEMPTS
            && !matches!(&record.payload, JobPayload::TerminalCleanup { .. })
        {
            exhaust_attempts(record, requeue.now_ms);
        } else {
            record.state = JobState::Queued;
            record.due_at_ms = requeue
                .now_ms
                .saturating_add(retry_delay_ms(record.attempts));
            record.progress = JobProgress::new(record.payload.progress_unit());
        }
        Ok(JobMutation::Persist)
    };
    change_mapped(job_id, mutate, move |record, persisted| {
        if persisted {
            return match record.is_settled() {
                true => RequeueOutcome::Exhausted(record),
                false => RequeueOutcome::Requeued(record),
            };
        }
        match !requeue_passes(&record, requeue) && needs_reconcile(&record) {
            true => RequeueOutcome::NeedsReconcile(record),
            false => RequeueOutcome::Skipped,
        }
    })
}

/// Re-queue with backoff, or fail once `JOB_MAX_ATTEMPTS` is spent; `token` is `None` for the
/// sweep and recovery, and `require_expired_before` re-checks the expired lease in the
/// transaction. External attempts answer `NeedsReconcile` untouched.
pub fn requeue_job(
    job_id: JobId,
    token: Option<Ulid>,
    now_ms: u64,
    require_expired_before: Option<u64>,
    error: Option<JobError>,
) -> StoreCall<RequeueOutcome> {
    let requeue = Requeue {
        token,
        now_ms,
        require_expired_before,
        progressed: false,
    };
    requeue_counted(job_id, requeue, error)
}

/// Re-queue after a transient failure of an execution that advanced a durable checkpoint:
/// attempts count consecutive failures without progress, so this one starts a new count.
pub fn requeue_progressed(
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    error: JobError,
) -> StoreCall<RequeueOutcome> {
    let requeue = Requeue {
        token: Some(token),
        now_ms,
        require_expired_before: None,
        progressed: true,
    };
    requeue_counted(job_id, requeue, Some(error))
}

#[derive(Debug, PartialEq)]
pub enum ReleaseOutcome {
    Released(JobRecord),
    Skipped,
}

fn released(record: JobRecord, persisted: bool) -> ReleaseOutcome {
    match persisted {
        true => ReleaseOutcome::Released(record),
        false => ReleaseOutcome::Skipped,
    }
}

/// Hand a lease back without spending an attempt: clear the claim, re-queue, make it due now.
/// Guarded by `token`, so a job another node already took over is left alone.
pub fn release_job(job_id: JobId, token: Ulid, now_ms: u64) -> StoreCall<ReleaseOutcome> {
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        record.state = JobState::Queued;
        record.claim = None;
        record.due_at_ms = now_ms;
        record.updated_at_ms = now_ms;
        record.progress = JobProgress::new(record.payload.progress_unit());
        Ok(JobMutation::Persist)
    };
    change_mapped(job_id, mutate, released)
}

/// Requeue a dependency-blocked job without spending an attempt. Unlike a shutdown release,
/// this records the reason and delays the next claim so a lagging projection is not polled hot.
pub fn defer_job(
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    retry_after_ms: u64,
    error: JobError,
) -> StoreCall<ReleaseOutcome> {
    let mutate = move |record: &mut JobRecord| {
        guard_token(record, token)?;
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        record.state = JobState::Queued;
        record.claim = None;
        record.due_at_ms = now_ms.saturating_add(retry_after_ms);
        record.updated_at_ms = now_ms;
        record.last_error = Some(error.clone());
        record.progress = JobProgress::new(record.payload.progress_unit());
        Ok(JobMutation::Persist)
    };
    change_mapped(job_id, mutate, released)
}

#[derive(Debug, PartialEq)]
pub enum CancelRequestOutcome {
    Cancelled(JobRecord),
    Flagged(JobRecord),
    AlreadyTerminal(JobRecord),
}

/// Idempotently set `cancel_requested`; a terminal job is a no-op. A queued job that never ran
/// is terminalized right here: it owns no side effects to clean up, and leaving it for the
/// drain would strand it `Queued` for as long as the executor stays at capacity.
pub fn set_cancel_requested(job_id: JobId, now_ms: u64) -> StoreCall<CancelRequestOutcome> {
    let mutate = move |record: &mut JobRecord| {
        let cancel = |record: &mut JobRecord| {
            record.cancel_requested = true;
            record.state = JobState::Cancelled;
            record.finished_at_ms = Some(now_ms);
            record.updated_at_ms = now_ms;
        };
        // A locally exhausted job runs nowhere on this node; a container it may have left is
        // removed by the cleanup queued when it was exhausted, so cancelling settles it.
        if record.state == JobState::Indeterminate && record.locally_exhausted {
            cancel(record);
            record.locally_exhausted = false;
            record.claim = None;
            return Ok(JobMutation::Persist);
        }
        if record.is_settled() {
            return Ok(JobMutation::Skip);
        }
        // `has_run`, not `attempts == 0`: a job interrupted by a shutdown hands its lease
        // back without spending an attempt, so attempts alone cannot prove it never ran.
        if record.state == JobState::Queued && !record.has_run && record.attempt_intent.is_none() {
            cancel(record);
            record.claim = None;
            return Ok(JobMutation::Persist);
        }
        // A parked job holds no claim, so its cancellation runs through the queue at once.
        if record.state == JobState::AwaitingKey {
            if !record.has_run {
                cancel(record);
            } else {
                record.cancel_requested = true;
                record.updated_at_ms = now_ms;
                record.state = JobState::Queued;
                record.due_at_ms = now_ms;
            }
            return Ok(JobMutation::Persist);
        }
        if record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        record.cancel_requested = true;
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    };
    change_mapped(job_id, mutate, |record, persisted| {
        if persisted && record.state == JobState::Cancelled {
            CancelRequestOutcome::Cancelled(record)
        } else if record.is_settled() {
            CancelRequestOutcome::AlreadyTerminal(record)
        } else {
            CancelRequestOutcome::Flagged(record)
        }
    })
}

/// Write-ahead one VersionId per output destination in the attempt-control transaction that
/// fences this physical execution; it commits before any output write, so a replayed capture
/// reuses the reserved identities. `hints` maps distributed destinations to their set hints.
pub fn reserve_output_commits(
    job_id: JobId,
    destinations: Vec<(NodeId, String, String)>,
    hints: BTreeMap<(NodeId, String), SetHint>,
) -> StoreCall<AttemptControl> {
    control_of(
        job_id,
        move |_: &mut JobRecord, control: &mut AttemptControl| {
            let mint = |(node_id, bucket, _): &(NodeId, String, String)| {
                mint_version(hints.get(&(*node_id, bucket.clone())).copied())
            };
            match control.reserve_outputs(&destinations, mint) {
                true => Ok(JobMutation::Persist),
                false => Ok(JobMutation::Skip),
            }
        },
    )
}

pub fn authorize_cleanup(
    job_id: JobId,
    intent: AttemptIntent,
    token: Ulid,
) -> StoreCall<FenceContext> {
    let (attempt_no, attempt_epoch) = (intent.attempt_no, intent.attempt_epoch);
    let mutate = move |record: &mut JobRecord, control: &mut AttemptControl| {
        if record
            .attempt_intent
            .as_ref()
            .is_none_or(|stored| stored.attempt_epoch != attempt_epoch)
        {
            return Err(JobMutationError::EpochMismatch);
        }
        bump_generation(control)?;
        control.bound_token = Some(token);
        Ok(JobMutation::Persist)
    };
    let build = move || MutateControlOperation::new(job_id, Vec::new(), mutate.clone());
    let map = move |changed: ControlChange| {
        Ok(FenceContext {
            attempt: AttemptRef::new(job_id.to_string().to_lowercase(), attempt_no),
            attempt_epoch,
            controller_generation: changed.control.controller_generation,
        })
    };
    StoreCall::of(build, map, ATTEMPT_EXHAUSTED)
}

pub fn record_attempt_tombstone(
    job_id: JobId,
    token: Ulid,
    attempt_epoch: u64,
    tombstone_ref: String,
) -> StoreCall<AttemptControl> {
    control_of(
        job_id,
        move |_: &mut JobRecord, control: &mut AttemptControl| {
            if control.attempt_epoch != attempt_epoch {
                return Err(JobMutationError::EpochMismatch);
            }
            if control.bound_token != Some(token) {
                return Err(JobMutationError::TokenMismatch);
            }
            control.tombstone_ref = Some(tombstone_ref.clone());
            Ok(JobMutation::Persist)
        },
    )
}

#[derive(Debug, PartialEq)]
pub enum AdoptOutcome {
    Adopted(JobRecord, AttemptControl),
    /// Terminal, or the lease is still live: leave the current holder alone.
    Skipped,
}

/// Take over a lost external attempt with a fresh claim token so the reconciler can supervise
/// it. No token guard: the previous holder is provably dead (lease swept or node restarted).
/// State is preserved for the reconcile decision.
pub fn adopt_external_attempt(
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> StoreCall<AdoptOutcome> {
    let mutate = move |record: &mut JobRecord, control: &mut AttemptControl| {
        if record.state.is_terminal() {
            return Ok(JobMutation::Skip);
        }
        // Re-check the lease inside the transaction: a holder that renewed since the
        // sweep's read is alive, and stealing its claim would double-run the container.
        if let Some(claim) = &record.claim
            && claim.lease_expires_ms > now_ms
        {
            return Ok(JobMutation::Skip);
        }
        record.updated_at_ms = now_ms;
        let claim_token = Ulid::generate();
        record.claim = Some(JobClaim {
            holder_node_id,
            claim_token,
            lease_expires_ms: now_ms.saturating_add(JOB_LEASE_MS),
        });
        bump_generation(control)?;
        control.bound_token = Some(claim_token);
        Ok(JobMutation::Persist)
    };
    let build = move || MutateControlOperation::new(job_id, Vec::new(), mutate.clone());
    let map = |changed: ControlChange| {
        Ok(match changed.persisted {
            true => AdoptOutcome::Adopted(changed.record, changed.control),
            false => AdoptOutcome::Skipped,
        })
    };
    StoreCall::of(build, map, ATTEMPT_EXHAUSTED)
}

/// Make this execution's signed output record durable and name its digest on the attempt
/// control in one transaction, so terminal success can never observe the digest without the
/// record it names.
pub fn persist_output_record(
    job_id: JobId,
    control: &AttemptControl,
    digest: [u8; 32],
    envelope: Vec<u8>,
) -> StoreCall<()> {
    let write = vec![(
        OUTPUT_RECORD_KEYSPACE.to_string(),
        ByteView::from(attempt_control_key(job_id, control.attempt_epoch)),
        ByteView::from(envelope),
    )];
    let execution_id = control.execution_id;
    let mutate = move |_: &mut JobRecord, stored: &mut AttemptControl| {
        if stored.execution_id != execution_id {
            return Err(JobMutationError::EpochMismatch);
        }
        if let Some(existing) = stored.output_record {
            return match existing == digest {
                true => Ok(JobMutation::Skip),
                false => Err(JobMutationError::OutputRecordConflict),
            };
        }
        stored.output_record = Some(digest);
        Ok(JobMutation::Persist)
    };
    let build = move || MutateControlOperation::new(job_id, write.clone(), mutate.clone());
    StoreCall::of(build, |_: ControlChange| Ok(()), ATTEMPT_EXHAUSTED)
}

#[derive(Debug, PartialEq)]
enum HandoffStep {
    Control,
    Job,
    Release,
}

/// Hands an external execution to reconciliation without re-queuing it. Rotating the token
/// fences the old supervisor; the expired replacement claim keeps the unchanged state and
/// attempt intent discoverable by the lease sweep. A job interrupted while still staging has no
/// attempt intent, so its lease is released instead of waiting for the sweep's attempt charge.
#[derive(Debug, PartialEq)]
pub struct HandoffOperation {
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    step: HandoffStep,
}

/// See [`HandoffOperation`].
pub fn handoff_external_attempt(job_id: JobId, token: Ulid, now_ms: u64) -> HandoffOperation {
    HandoffOperation {
        job_id,
        token,
        now_ms,
        step: HandoffStep::Control,
    }
}

impl HandoffOperation {
    fn rotate(&self) -> StoreCall<ControlChange> {
        let (token, now_ms) = (self.token, self.now_ms);
        change_control(
            self.job_id,
            Vec::new(),
            move |record: &mut JobRecord, control: &mut AttemptControl| {
                guard_token(record, token)?;
                if record.execution_class != JobExecutionClass::ExternalAttempt
                    || record.state.is_terminal()
                {
                    return Ok(JobMutation::Skip);
                }
                if let Some(claim) = record.claim.as_mut() {
                    claim.claim_token = Ulid::generate();
                    claim.lease_expires_ms = now_ms;
                }
                bump_generation(control)?;
                control.bound_token = None;
                record.updated_at_ms = now_ms;
                Ok(JobMutation::Persist)
            },
        )
    }
}

impl Operation for HandoffOperation {
    type Output = ReleaseOutcome;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<ReleaseOutcome>, JobMutationError> {
        Ok(Next::Call(IoCall::child(self.rotate())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReleaseOutcome>, JobMutationError> {
        match self.step {
            HandoffStep::Control => match result.child::<ControlChange, JobMutationError>()? {
                Ok(changed) => Ok(Next::Done(released(changed.record, changed.persisted))),
                Err(JobMutationError::MissingControl) => {
                    self.step = HandoffStep::Job;
                    Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
                }
                Err(error) => Err(error),
            },
            HandoffStep::Job => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()??;
                if !record.is_some_and(|record| record.attempt_intent.is_none()) {
                    return Err(JobMutationError::MissingControl);
                }
                self.step = HandoffStep::Release;
                let release = release_job(self.job_id, self.token, self.now_ms);
                Ok(Next::Call(IoCall::child(release)))
            }
            HandoffStep::Release => {
                let outcome = result.child::<ReleaseOutcome, JobMutationError>()??;
                Ok(Next::Done(outcome))
            }
        }
    }
}

/// Write-ahead attempt intent: record the deterministic external identity BEFORE any external
/// submit so a lost attempt can be adopted by name. A committed cancellation wins this gate.
/// `execution_id` binds the fenced attempt to the physical execution a target already
/// receipted; `None` mints a fresh one for a purely local attempt.
pub fn record_attempt_intent(
    job_id: JobId,
    token: Ulid,
    draft: AttemptIntent,
    execution_id: Option<Ulid>,
    launch: Option<LaunchRecord>,
    now_ms: u64,
) -> StoreCall<AttemptCommit> {
    let execution_id = execution_id.unwrap_or_else(Ulid::generate);
    let build = move || {
        let ids = (job_id, token);
        RecordIntentOperation::new(ids, draft.clone(), execution_id, launch.clone(), now_ms)
    };
    StoreCall::of(build, Ok, "attempt commit exhausted conflict retries")
}

/// Removes a finished run of `user_id` from this node in one transaction: its row, list index
/// and every local row pruning removes. An execution run goes only once its deletion was
/// `recorded` in its replicated family, whose records stay.
pub fn delete_finished_run(user_id: UserId, job_id: JobId, recorded: bool) -> StoreCall<RunDelete> {
    let build = move || DeleteRunOperation::new(user_id, job_id, recorded);
    StoreCall::of(build, Ok, "run delete exhausted conflict retries")
}

/// Writes and deletes a running job's own rows in one transaction, guarded by its claim token.
pub fn put_job_rows(
    job_id: JobId,
    token: Ulid,
    writes: JobWrites,
    deletes: JobDeletes,
) -> StoreCall<()> {
    let build = move || PutRowsOperation::new(job_id, token, writes.clone(), deletes.clone());
    StoreCall::of(build, Ok, "checkpoint write exhausted conflict retries")
}

fn encoded<T: Serialize>(row: &T) -> Result<Value, JobMutationError> {
    postcard::to_allocvec(row)
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))
}

/// Encodes `row` and writes it through the token-guarded checkpoint write.
pub fn put_state<T: Serialize>(
    job_id: JobId,
    token: Ulid,
    key_space: &str,
    key: Key,
    row: &T,
) -> StoreCall<()> {
    match encoded(row) {
        Ok(value) => put_job_rows(
            job_id,
            token,
            vec![(key_space.to_string(), key, value)],
            Vec::new(),
        ),
        Err(error) => StoreCall::refused(error),
    }
}

pub fn put_purge_checkpoint(
    job_id: JobId,
    token: Ulid,
    checkpoint: &StoragePurgeCheckpoint,
) -> StoreCall<()> {
    let key = ByteView::from(job_id.to_bytes().to_vec());
    match checkpoint.to_bytes() {
        Ok(value) => put_job_rows(
            job_id,
            token,
            vec![(
                PURGE_CHECKPOINT_KEYSPACE.to_string(),
                key,
                ByteView::from(value),
            )],
            Vec::new(),
        ),
        Err(error) => StoreCall::refused(JobMutationError::Storage(error.to_string())),
    }
}

pub fn put_job_entry<T: Serialize>(
    job_id: JobId,
    token: Ulid,
    entry_key: &[u8],
    row: &T,
) -> StoreCall<()> {
    let value = match encoded(row) {
        Ok(value) => value,
        Err(error) => return StoreCall::refused(error),
    };
    let entry_key = entry_key.to_vec();
    let build = move || PutEntryOperation::new(job_id, token, &entry_key, value.clone());
    StoreCall::of(build, Ok, "job entry write exhausted conflict retries")
}

pub(super) fn bump_generation(control: &mut AttemptControl) -> Result<(), JobMutationError> {
    control.controller_generation = control
        .controller_generation
        .checked_add(1)
        .ok_or(JobMutationError::GenerationExhausted)?;
    Ok(())
}

/// `JOB_MAX_ATTEMPTS` are spent. Only a permanent verdict terminalizes; a
/// retryable or absent one leaves the outcome `Indeterminate`, since exhausting
/// this node's attempts proves nothing. An execution keeps a result payload.
pub(super) fn exhaust_attempts(record: &mut JobRecord, now_ms: u64) {
    let permanent = record
        .last_error
        .as_ref()
        .is_some_and(|error| error.kind == JobErrorKind::Permanent);
    record.claim = None;
    if permanent {
        record.state = JobState::Failed;
        record.finished_at_ms = Some(now_ms);
    } else {
        record.state = JobState::Indeterminate;
        record.locally_exhausted = true;
    }
    if record.result.is_some() || !matches!(record.payload, JobPayload::Execution(_)) {
        return;
    }
    let result = JobResultPayload::Execution {
        exit_code: None,
        workspace_bucket: record.workspace_bucket.clone(),
        outputs: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    };
    record.result = Some(result);
}
