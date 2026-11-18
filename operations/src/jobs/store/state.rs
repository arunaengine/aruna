//! Moves a job record through its states: claim, run, complete, fail, requeue, release.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::attempt_intent::AttemptCommit;
use super::job_calls::{self, RunSuccess};
pub use super::job_calls::{
    ClaimOutcome, ExecutionCompleteOutcome, ReleaseOutcome, RenewOutcome, RequeueOutcome,
};
use super::row_calls;
use super::*;

/// Read a job record, retaining malformed rows for offline recovery.
pub async fn read_job_record(
    context: &DriverContext,
    job_id: JobId,
    _txn_id: Option<TxnId>,
) -> Result<Option<JobRecord>, String> {
    drive(row_calls::read_job_record(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

/// Claim a queued job, or cancel it directly if it never ran and is cancel-requested.
pub async fn claim_job(
    context: &DriverContext,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<ClaimOutcome, JobMutationError> {
    drive(
        job_calls::claim_job(job_id, holder_node_id, now_ms),
        context,
    )
    .await
}

pub async fn transition_to_running(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::transition_to_running(job_id, token, now_ms),
        context,
    )
    .await
}

pub async fn renew_lease(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    progress: Option<JobProgress>,
) -> Result<RenewOutcome, JobMutationError> {
    drive(
        job_calls::renew_lease(job_id, token, now_ms, progress),
        context,
    )
    .await
}

/// Persist an in-memory progress snapshot without touching the lease or state.
pub async fn flush_progress(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    progress: JobProgress,
    now_ms: u64,
) -> Result<RenewOutcome, JobMutationError> {
    drive(
        job_calls::flush_progress(job_id, token, progress, now_ms),
        context,
    )
    .await
}

pub async fn complete_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::complete_job(job_id, token, result, final_progress, now_ms),
        context,
    )
    .await
}

/// Complete an execution only when cancellation has not already committed.
pub async fn complete_execution(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<ExecutionCompleteOutcome, JobMutationError> {
    drive(
        job_calls::complete_execution(
            job_id,
            token,
            RunSuccess {
                result,
                progress: final_progress,
                now_ms,
            },
        ),
        context,
    )
    .await
}

/// Complete an execution that finished successfully after cancellation was
/// requested. It carries the same durable-output invariant as an ordinary
/// success, because it publishes the same terminal result.
pub async fn complete_cancelled(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::complete_cancelled(
            job_id,
            token,
            RunSuccess {
                result,
                progress: final_progress,
                now_ms,
            },
        ),
        context,
    )
    .await
}

/// The signed output record the attempt already stored, if any. It shares the
/// attempt-control key, so pruning the job removes both.
pub async fn read_output_record(
    context: &DriverContext,
    job_id: JobId,
    attempt_epoch: u64,
) -> Result<Option<JobRecordEnvelope>, JobMutationError> {
    drive(
        row_calls::read_output_record(job_id, attempt_epoch),
        context,
    )
    .await
}

/// Make this execution's signed output record durable and name its digest on
/// the attempt control in one transaction, so terminal success can never
/// observe the digest without the record it names.
pub async fn persist_output_record(
    context: &DriverContext,
    job_id: JobId,
    control: &AttemptControl,
    digest: [u8; 32],
    envelope: Vec<u8>,
) -> Result<(), JobMutationError> {
    drive(
        job_calls::persist_output_record(job_id, control, digest, envelope),
        context,
    )
    .await
}

pub async fn fail_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(job_calls::fail_job(job_id, token, error, now_ms), context).await
}

/// Ends the attempts of a job whose outcome cannot be known: `Indeterminate` and locally
/// exhausted, so nothing tries it again here and nothing claims it failed.
pub async fn exhaust_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::exhaust_job(job_id, token, error, now_ms),
        context,
    )
    .await
}

pub async fn cancel_running_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::cancel_running_job(job_id, token, now_ms),
        context,
    )
    .await
}

/// Re-queue with backoff, or fail once `JOB_MAX_ATTEMPTS` is spent; `token` is
/// `None` for the sweep and recovery, and `require_expired_before` re-checks the
/// expired lease in-txn. External attempts return `NeedsReconcile` untouched.
pub async fn requeue_job(
    context: &DriverContext,
    job_id: JobId,
    token: Option<Ulid>,
    now_ms: u64,
    require_expired_before: Option<u64>,
    error: Option<JobError>,
) -> Result<RequeueOutcome, JobMutationError> {
    drive(
        job_calls::requeue_job(job_id, token, now_ms, require_expired_before, error),
        context,
    )
    .await
}

/// Re-queue after a transient failure of an execution that advanced a durable checkpoint:
/// attempts count consecutive failures without progress, so this one starts a new count.
pub async fn requeue_progressed(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    error: JobError,
) -> Result<RequeueOutcome, JobMutationError> {
    drive(
        job_calls::requeue_progressed(job_id, token, now_ms, error),
        context,
    )
    .await
}

/// Hand a lease back without spending an attempt: clear the claim, re-queue, make it
/// due now. Guarded by `token`, so a job another node already took over is left alone.
/// Distinct from `requeue_job`, whose attempt increment is unconditional.
pub async fn release_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(job_calls::release_job(job_id, token, now_ms), context).await
}

/// Requeue a dependency-blocked job without spending an attempt. Unlike a
/// shutdown release, this records the reason and delays the next claim so a
/// lagging projection is not hot-polled.
pub async fn defer_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    retry_after_ms: u64,
    error: JobError,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(
        job_calls::defer_job(job_id, token, now_ms, retry_after_ms, error),
        context,
    )
    .await
}

/// Hand an external execution to reconciliation without re-queuing it.
/// Rotating the token fences the old supervisor; the expired replacement claim keeps
/// the unchanged state and attempt intent discoverable by the lease sweep.
pub async fn handoff_external_attempt(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(
        job_calls::handoff_external_attempt(job_id, token, now_ms),
        context,
    )
    .await
}

/// Write-ahead attempt intent: record the deterministic external identity BEFORE any
/// external submit so a lost attempt can be adopted by name. A committed cancellation
/// wins this gate, so no new attempt is submitted afterward.
/// `execution_id` binds the fenced attempt to the physical execution a target
/// already receipted; `None` mints a fresh one for a purely local attempt.
pub async fn record_attempt_intent(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    draft: AttemptIntent,
    execution_id: Option<Ulid>,
    launch: Option<&LaunchRecord>,
    now_ms: u64,
) -> Result<AttemptCommit, JobMutationError> {
    drive(
        job_calls::record_attempt_intent(
            job_id,
            token,
            draft,
            execution_id,
            launch.cloned(),
            now_ms,
        ),
        context,
    )
    .await
}
