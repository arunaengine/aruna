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
