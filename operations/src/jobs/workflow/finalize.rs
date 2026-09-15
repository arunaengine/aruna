use std::sync::Arc;

use aruna_compute::ExecutorBackend;
use aruna_compute::session::{EndReason, Session};
use aruna_core::compute::{
    AttemptPhase, AttemptStatus, BackendError, CancelEvidence, FenceContext, LogLimits, LogTails,
};
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::tail_str;
use aruna_core::structs::execution::job::{
    AttemptControl, ExecutionSpec, JobError, JobId, JobPayload, JobRecord, JobRecordError,
    JobResultPayload, MAX_MESSAGE_BYTES, OutputObject,
};
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use tracing::{info, warn};

use super::super::output_record::store_outputs;
use super::super::store::{
    ExecutionCompleteOutcome, cancel_execution, complete_cancelled, complete_execution,
    fail_execution, read_attempt_control, read_job_record, record_attempt_started,
    transition_to_cancelling,
};
use super::super::submit::schedule_drain_effect;
use super::prepare::job_bucket;
use super::recovery::park_attempt;
use super::session::write_session_report;
use super::workspace::{capture_outputs, collect_outputs, merge_outputs};
use crate::driver::DriverContext;
use crate::jobs::lifecycle::updates::{
    SETTLE_RETRY_AFTER, publish_terminal, schedule_terminal_settle,
};

/// A session the client ended, or that went idle, is a finished job: the work
/// it did is already in the workspace bucket.
#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_session(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    bucket: &str,
    session: Option<&Arc<Session>>,
    reason: EndReason,
) {
    let storage = &context.storage_handle;
    let _ = transition_to_cancelling(storage, job_id, token, unix_timestamp_millis()).await;
    let logs = match backend.cancel(fence).await {
        Ok(CancelEvidence::Stopped(_)) => {
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            logs
        }
        Ok(CancelEvidence::AlreadyGone) => LogTails::default(),
        Ok(CancelEvidence::Requested) | Err(_) => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable("session stop lacks evidence"),
            ))
            .await;
            return;
        }
    };
    info!(job_id = %job_id, reason = reason.as_str(), "Session finished");
    Box::pin(write_session_report(
        storage, job_id, token, session, reason,
    ))
    .await;
    // A kernel that died, or a helper that never answered, is a permanent
    // execution failure, not a session the caller finished.
    if reason == EndReason::KernelExit {
        let result = execution_result_for(bucket, Some(1), Vec::new(), logs);
        let terminal = Box::pin(terminal_fail(
            storage,
            job_id,
            token,
            JobError::permanent("session kernel exited"),
            result,
        ))
        .await;
        Box::pin(cleanup_and_crate(context, job_id, terminal)).await;
        return;
    }
    // A terminal success needs a named output record, even with no outputs.
    let Some(control) = Box::pin(control_or_park(context, job_id, token, fence)).await else {
        return;
    };
    let mut result = execution_result_for(bucket, Some(0), Vec::new(), logs);
    let Some(digest) = Box::pin(store_or_fail(
        context, job_id, token, bucket, &control, &result,
    ))
    .await
    else {
        return;
    };
    name_output_record(&mut result, digest);
    let record = Box::pin(terminal_complete(storage, job_id, token, result)).await;
    Box::pin(cleanup_and_crate(context, job_id, record)).await;
}

pub(super) async fn finalize_walltime(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    bucket: &str,
) {
    let storage = &context.storage_handle;
    let _ = transition_to_cancelling(storage, job_id, token, unix_timestamp_millis()).await;
    let logs = match backend.cancel(fence).await {
        Ok(CancelEvidence::Stopped(_)) => {
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            logs
        }
        Ok(CancelEvidence::AlreadyGone) => LogTails::default(),
        Ok(CancelEvidence::Requested) | Err(_) => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable("walltime stop lacks evidence"),
            ))
            .await;
            return;
        }
    };
    let result = execution_result_for(bucket, None, Vec::new(), logs);
    let record = Box::pin(terminal_fail(
        storage,
        job_id,
        token,
        JobError::permanent("walltime limit exceeded"),
        result,
    ))
    .await;
    Box::pin(cleanup_and_crate(context, job_id, record)).await;
}
/// Evidence-based terminalization correlating the durable cancel intent with the
/// backend exit (Stage-1 flag: Docker cannot distinguish a SIGKILL 137 from a
/// natural exit, so the intent in the record decides).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_attempt(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    bucket: &str,
    result: Result<AttemptStatus, BackendError>,
) {
    let storage = &context.storage_handle;
    let cancel_requested = read_job_record(storage, job_id, None)
        .await
        .ok()
        .flatten()
        .map(|record| record.cancel_requested)
        .unwrap_or(false);

    let status = match result {
        Ok(status) => status,
        // Post-submit NotFound / unreachable backend is ambiguous: park in
        // Indeterminate, never requeue (spec 16.7).
        Err(error) => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable(format!("attempt unobservable: {error}")),
            ))
            .await;
            return;
        }
    };

    if let Some(started_at_ms) = status.started_at_ms
        && let Err(error) = record_attempt_started(storage, job_id, token, started_at_ms).await
    {
        warn!(job_id = %job_id, error = %error, "Attempt start evidence write failed");
        return;
    }

    if cancel_requested {
        Box::pin(finalize_cancel(
            context, job_id, token, backend, fence, spec, bucket,
        ))
        .await;
        return;
    }

    match status.phase {
        AttemptPhase::Exited { code: 0 } => {
            // Export first: inventory attributes only versions this execution
            // already reserved, so the reservations must be durable before it.
            let Some(captured) = Box::pin(export_or_park(
                context, job_id, token, backend, fence, spec, bucket,
            ))
            .await
            else {
                return;
            };
            let Some(control) = Box::pin(control_or_park(context, job_id, token, fence)).await
            else {
                return;
            };
            let execution_id = control.execution_id;
            let Some(inventoried) = Box::pin(collect_or_park(
                context, job_id, token, spec, bucket, &control,
            ))
            .await
            else {
                return;
            };
            let Some(outputs) = Box::pin(merge_or_park(
                context,
                job_id,
                token,
                bucket,
                inventoried,
                captured,
            ))
            .await
            else {
                return;
            };
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            let mut result = execution_result_for(bucket, Some(0), outputs, logs);
            if let Err(error) = result.check_outputs(execution_id) {
                Box::pin(fail_bad_outputs(context, job_id, token, bucket, error)).await;
                return;
            }
            let Some(digest) = Box::pin(store_or_fail(
                context, job_id, token, bucket, &control, &result,
            ))
            .await
            else {
                return;
            };
            name_output_record(&mut result, digest);
            match Box::pin(terminal_execution(storage, job_id, token, result)).await {
                Some(ExecutionCompleteOutcome::Completed(record)) => {
                    Box::pin(cleanup_and_crate(context, job_id, Some(record))).await;
                }
                Some(ExecutionCompleteOutcome::CancelRequested(_)) => {
                    Box::pin(finalize_cancel(
                        context, job_id, token, backend, fence, spec, bucket,
                    ))
                    .await;
                }
                None => {}
            }
        }
        AttemptPhase::Exited { code } => {
            let Some(control) = Box::pin(control_or_park(context, job_id, token, fence)).await
            else {
                return;
            };
            let Some(outputs) = Box::pin(collect_or_park(
                context, job_id, token, spec, bucket, &control,
            ))
            .await
            else {
                return;
            };
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            let detail = status.detail.as_deref();
            warn!(
                job_id = %job_id,
                code,
                detail_bytes = detail.map_or(0, str::len),
                stdout_bytes = logs.stdout.len(),
                stderr_bytes = logs.stderr.len(),
                "Container exited non-zero"
            );
            let message = exit_message(code, detail);
            let result = execution_result_for(bucket, Some(code), outputs, logs);
            let record = Box::pin(terminal_fail(
                storage,
                job_id,
                token,
                JobError::permanent(message),
                result,
            ))
            .await;
            Box::pin(cleanup_and_crate(context, job_id, record)).await;
        }
        AttemptPhase::Failed { reason } => {
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            let result = execution_result_for(bucket, None, Vec::new(), logs);
            let record = Box::pin(terminal_fail(
                storage,
                job_id,
                token,
                JobError::permanent(format!("backend failure: {reason}")),
                result,
            ))
            .await;
            Box::pin(cleanup_and_crate(context, job_id, record)).await;
        }
        // Infrastructure evidence proves nothing about the job: park it so the
        // family may still run it elsewhere instead of writing a terminal row.
        AttemptPhase::SystemError { reason } => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable(format!("backend infrastructure failure: {reason}")),
            ))
            .await;
        }
        AttemptPhase::Cancelled => {
            Box::pin(finalize_cancel(
                context, job_id, token, backend, fence, spec, bucket,
            ))
            .await;
        }
        AttemptPhase::Submitted | AttemptPhase::Running => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable("attempt returned non-terminal"),
            ))
            .await;
        }
    }
}

pub(super) async fn finalize_cancel(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    bucket: &str,
) {
    let storage = &context.storage_handle;
    // Running -> Cancelling (idempotent: a re-entry may already be Cancelling).
    let _ = transition_to_cancelling(storage, job_id, token, unix_timestamp_millis()).await;
    // Stop the container first: the identity is only needed to record outputs.
    let evidence = backend.cancel(fence).await;
    let Some(control) = Box::pin(control_or_park(context, job_id, token, fence)).await else {
        return;
    };
    let execution_id = control.execution_id;
    match evidence {
        Ok(CancelEvidence::Stopped(status)) => {
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            match status.phase {
                AttemptPhase::Exited { code: 0 } => {
                    let Some(captured) = Box::pin(export_or_park(
                        context, job_id, token, backend, fence, spec, bucket,
                    ))
                    .await
                    else {
                        return;
                    };
                    let Some(control) =
                        Box::pin(control_or_park(context, job_id, token, fence)).await
                    else {
                        return;
                    };
                    let Some(inventoried) = Box::pin(collect_or_park(
                        context, job_id, token, spec, bucket, &control,
                    ))
                    .await
                    else {
                        return;
                    };
                    let Some(outputs) = Box::pin(merge_or_park(
                        context,
                        job_id,
                        token,
                        bucket,
                        inventoried,
                        captured,
                    ))
                    .await
                    else {
                        return;
                    };
                    let mut result = execution_result_for(bucket, Some(0), outputs, logs);
                    if let Err(error) = result.check_outputs(execution_id) {
                        Box::pin(fail_bad_outputs(context, job_id, token, bucket, error)).await;
                        return;
                    }
                    let Some(digest) = Box::pin(store_or_fail(
                        context, job_id, token, bucket, &control, &result,
                    ))
                    .await
                    else {
                        return;
                    };
                    name_output_record(&mut result, digest);
                    let record = Box::pin(terminal_complete(storage, job_id, token, result)).await;
                    Box::pin(cleanup_and_crate(context, job_id, record)).await;
                }
                AttemptPhase::Exited { code } => {
                    let Some(outputs) = Box::pin(collect_or_park(
                        context, job_id, token, spec, bucket, &control,
                    ))
                    .await
                    else {
                        return;
                    };
                    let message = exit_message(code, status.detail.as_deref());
                    let result = execution_result_for(bucket, Some(code), outputs, logs);
                    let record = Box::pin(terminal_fail(
                        storage,
                        job_id,
                        token,
                        JobError::permanent(message),
                        result,
                    ))
                    .await;
                    Box::pin(cleanup_and_crate(context, job_id, record)).await;
                }
                AttemptPhase::Failed { reason } => {
                    let result = execution_result_for(bucket, None, Vec::new(), logs);
                    let record = Box::pin(terminal_fail(
                        storage,
                        job_id,
                        token,
                        JobError::permanent(format!("backend failure: {reason}")),
                        result,
                    ))
                    .await;
                    Box::pin(cleanup_and_crate(context, job_id, record)).await;
                }
                // Infrastructure evidence is no verdict on the job, so a confirmed
                // stop terminalizes as the requested cancellation, never as Failed.
                AttemptPhase::SystemError { reason } => {
                    warn!(job_id = %job_id, %reason, "Cancelled attempt lost backend evidence");
                    let result = execution_result_for(bucket, None, Vec::new(), logs);
                    let record = Box::pin(terminal_cancel(storage, job_id, token, result)).await;
                    Box::pin(cleanup_and_crate(context, job_id, record)).await;
                }
                AttemptPhase::Cancelled | AttemptPhase::Submitted | AttemptPhase::Running => {
                    let Some(outputs) = Box::pin(collect_or_park(
                        context, job_id, token, spec, bucket, &control,
                    ))
                    .await
                    else {
                        return;
                    };
                    let result = execution_result_for(bucket, None, outputs, logs);
                    let record = Box::pin(terminal_cancel(storage, job_id, token, result)).await;
                    Box::pin(cleanup_and_crate(context, job_id, record)).await;
                }
            }
        }
        Ok(CancelEvidence::AlreadyGone) => {
            let Some(outputs) = Box::pin(collect_or_park(
                context, job_id, token, spec, bucket, &control,
            ))
            .await
            else {
                return;
            };
            let result = execution_result_for(bucket, None, outputs, LogTails::default());
            let record = Box::pin(terminal_cancel(storage, job_id, token, result)).await;
            Box::pin(cleanup_and_crate(context, job_id, record)).await;
        }
        // No definitive stop evidence yet: park in Indeterminate.
        Ok(CancelEvidence::Requested) | Err(_) => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable("cancel requested without stop evidence"),
            ))
            .await;
        }
    }
}

async fn terminal_complete(
    storage: &aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    result: JobResultPayload,
) -> Option<JobRecord> {
    let progress = read_job_record(storage, job_id, None)
        .await
        .ok()
        .flatten()
        .map(|record| record.progress)
        .unwrap_or_else(|| aruna_core::structs::execution::job::JobProgress::new("phases"));
    match complete_cancelled(
        storage,
        job_id,
        token,
        result,
        progress,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(record) => Some(record),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Execution complete write failed");
            None
        }
    }
}

/// Name the durable output record on a terminal success result.
fn name_output_record(result: &mut JobResultPayload, digest: [u8; 32]) {
    if let JobResultPayload::Execution { output_digest, .. } = result {
        *output_digest = Some(digest);
    }
}

/// Store this execution's exact output set before success is attempted. A
/// permanent failure terminalizes the job: a success whose immutable output
/// record is not durable must never be published.
async fn store_or_fail(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    bucket: &str,
    control: &AttemptControl,
    result: &JobResultPayload,
) -> Option<[u8; 32]> {
    let JobResultPayload::Execution { outputs, .. } = result else {
        return None;
    };
    let record = match read_job_record(&context.storage_handle, job_id, None).await {
        Ok(Some(record)) => record,
        _ => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable("output record job lookup failed"),
            ))
            .await;
            return None;
        }
    };
    match Box::pin(store_outputs(context, &record, control, outputs)).await {
        Ok(digest) => Some(digest),
        Err(error) if error.kind == aruna_core::structs::execution::job::JobErrorKind::Permanent => {
            warn!(job_id = %job_id, bucket = %bucket, error = ?error, "Output record store failed; failing");
            Box::pin(fail_and_crate(context, job_id, token, &record, error)).await;
            None
        }
        Err(error) => {
            Box::pin(park_attempt(context, job_id, token, error)).await;
            None
        }
    }
}

async fn terminal_execution(
    storage: &aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    result: JobResultPayload,
) -> Option<ExecutionCompleteOutcome> {
    let progress = read_job_record(storage, job_id, None)
        .await
        .ok()
        .flatten()
        .map(|record| record.progress)
        .unwrap_or_else(|| aruna_core::structs::execution::job::JobProgress::new("phases"));
    match complete_execution(
        storage,
        job_id,
        token,
        result,
        progress,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(outcome) => Some(outcome),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Execution complete write failed");
            None
        }
    }
}

async fn terminal_fail(
    storage: &aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    error: JobError,
    result: JobResultPayload,
) -> Option<JobRecord> {
    match fail_execution(
        storage,
        job_id,
        token,
        error,
        result,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(record) => Some(record),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Execution fail write failed");
            None
        }
    }
}

async fn terminal_cancel(
    storage: &aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    result: JobResultPayload,
) -> Option<JobRecord> {
    match cancel_execution(storage, job_id, token, result, unix_timestamp_millis()).await {
        Ok(record) => Some(record),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Execution cancel write failed");
            None
        }
    }
}

/// Wake the drain for the terminal obligations persisted with terminalization.
pub(super) async fn cleanup_and_crate(
    context: &DriverContext,
    job_id: JobId,
    record: Option<JobRecord>,
) {
    // Only act on a terminal record WE wrote (a lost race returns None).
    let Some(record) = record else { return };
    log_compute_summary(&record);
    // A receipted execution publishes its terminal state and frees its exact
    // reservation here; a purely local job publishes nothing.
    if !Box::pin(publish_terminal(context, &record)).await {
        arm_terminal_settle(context, job_id).await;
    }
    finalize_followups(context, job_id).await;
}

/// Hands a deferred terminal publication to the settle task. The reservation
/// row keeps the obligation durable until that retry succeeds.
async fn arm_terminal_settle(context: &DriverContext, job_id: JobId) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        warn!(job_id = %job_id, "Terminal publication deferred with no task handle to retry it");
        return;
    };
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle
        .send_effect(schedule_terminal_settle(SETTLE_RETRY_AFTER))
        .await
    {
        warn!(job_id = %job_id, message = %message, "Failed to arm the terminal settle retry");
    }
}

fn log_compute_summary(record: &JobRecord) {
    let JobPayload::Execution(spec) = &record.payload else {
        return;
    };
    let attempt = record
        .attempt_intent
        .as_ref()
        .map(|intent| intent.attempt_no)
        .unwrap_or(record.attempts);
    let executor_kind = record
        .attempt_intent
        .as_ref()
        .map(|intent| intent.executor_kind.as_str())
        .or(spec.executor_constraint.as_deref())
        .unwrap_or("unresolved");
    let outcome = match record.state {
        aruna_core::structs::execution::job::JobState::Succeeded => "success",
        aruna_core::structs::execution::job::JobState::Failed => "failure",
        aruna_core::structs::execution::job::JobState::Cancelled => "cancelled",
        _ => return,
    };
    info!(
        event = "pipeline.compute.summary",
        job_id = %record.job_id,
        attempt,
        image = %spec.image,
        executor_kind,
        state = record.state.name(),
        outcome,
        "Compute job summary"
    );
}

/// Wake the drain for internal jobs persisted with terminalization.
pub(crate) async fn finalize_followups(context: &DriverContext, job_id: JobId) {
    if let Some(task_handle) = context.task_handle.as_ref()
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_drain_effect()).await
    {
        warn!(job_id = %job_id, message = %message, "Failed to kick run-crate drain");
    }
}

pub(super) async fn fail_and_crate(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: JobError,
) {
    let result = execution_result(record, None, Vec::new());
    let terminal = Box::pin(terminal_fail(
        &context.storage_handle,
        job_id,
        token,
        error,
        result,
    ))
    .await;
    Box::pin(cleanup_and_crate(context, job_id, terminal)).await;
}
/// Physical execution identity of the fenced attempt. Without it no output can
/// be named exactly, so the attempt is parked instead of terminalized.
async fn control_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    fence: &FenceContext,
) -> Option<AttemptControl> {
    let control =
        read_attempt_control(&context.storage_handle, job_id, fence.attempt_epoch, None).await;
    let error = match control {
        Ok(Some(control)) if !control.execution_id.is_nil() => return Some(control),
        Ok(_) => JobError::retryable("attempt control carries no execution identity"),
        Err(error) => JobError::retryable(format!("attempt control lookup failed: {error}")),
    };
    Box::pin(park_attempt(context, job_id, token, error)).await;
    None
}

/// Terminal success is refused when an output cannot be named exactly: the job
/// fails instead of claiming a success whose outputs nobody can retrieve.
async fn fail_bad_outputs(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    bucket: &str,
    error: JobRecordError,
) {
    warn!(job_id = %job_id, error = %error, "Output identity incomplete; failing");
    let result = execution_result_for(bucket, None, Vec::new(), LogTails::default());
    let terminal = Box::pin(terminal_fail(
        &context.storage_handle,
        job_id,
        token,
        JobError::permanent(format!("output identity incomplete: {error}")),
        result,
    ))
    .await;
    Box::pin(cleanup_and_crate(context, job_id, terminal)).await;
}

/// Inventory the declared outputs. A permanent inventory failure terminalizes
/// the job so cleanup runs; a transient one parks it `Indeterminate` instead of
/// terminalizing with a false-empty output manifest.
pub(super) async fn collect_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    spec: &ExecutionSpec,
    bucket: &str,
    control: &AttemptControl,
) -> Option<Vec<OutputObject>> {
    match Box::pin(collect_outputs(context, spec, bucket, control)).await {
        Ok(outputs) => Some(outputs),
        Err(error) if error.kind == aruna_core::structs::execution::job::JobErrorKind::Permanent => {
            warn!(job_id = %job_id, bucket = %bucket, error = ?error, "Output inventory failed permanently; failing");
            Box::pin(fail_or_park(context, job_id, token, error)).await;
            None
        }
        Err(error) => {
            warn!(job_id = %job_id, bucket = %bucket, error = ?error, "Output inventory failed; parking");
            Box::pin(park_attempt(context, job_id, token, error)).await;
            None
        }
    }
}

/// Terminalize on a permanent output error so cleanup runs; a job record that
/// cannot be read is parked instead, leaving the terminal write to a later pass.
async fn fail_or_park(context: &DriverContext, job_id: JobId, token: ulid::Ulid, error: JobError) {
    match read_job_record(&context.storage_handle, job_id, None).await {
        Ok(Some(record)) => Box::pin(fail_and_crate(context, job_id, token, &record, error)).await,
        _ => Box::pin(park_attempt(context, job_id, token, error)).await,
    }
}

/// Merge the inventoried and exported manifests. Overflowing the keyed limit is a
/// permanent error, exactly as it is during inventory.
async fn merge_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    bucket: &str,
    inventoried: Vec<OutputObject>,
    captured: Vec<OutputObject>,
) -> Option<Vec<OutputObject>> {
    match merge_outputs(inventoried, captured) {
        Ok(outputs) => Some(outputs),
        Err(error) => {
            warn!(job_id = %job_id, bucket = %bucket, error = ?error, "Output manifest merge failed permanently; failing");
            Box::pin(fail_or_park(context, job_id, token, error)).await;
            None
        }
    }
}

async fn export_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Option<Vec<OutputObject>> {
    if spec.file_outputs.is_empty() {
        return Some(Vec::new());
    }
    let record = match read_job_record(&context.storage_handle, job_id, None).await {
        Ok(Some(record)) => record,
        Ok(None) => return None,
        Err(error) => {
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable(format!("output job lookup failed: {error}")),
            ))
            .await;
            return None;
        }
    };
    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        let error = JobError::permanent("output capture needs a net handle");
        let result = execution_result_for(bucket, Some(0), Vec::new(), LogTails::default());
        let terminal = Box::pin(terminal_fail(
            &context.storage_handle,
            job_id,
            token,
            error,
            result,
        ))
        .await;
        Box::pin(cleanup_and_crate(context, job_id, terminal)).await;
        return None;
    };
    match Box::pin(capture_outputs(
        context, backend, fence, spec, &record, node_id,
    ))
    .await
    {
        Ok(outputs) => Some(outputs),
        Err(error) if error.kind == aruna_core::structs::execution::job::JobErrorKind::Retryable => {
            warn!(job_id = %job_id, error = ?error, "Output capture failed; parking");
            Box::pin(park_attempt(context, job_id, token, error)).await;
            None
        }
        Err(error) => {
            let result = execution_result_for(bucket, Some(0), Vec::new(), LogTails::default());
            let terminal = Box::pin(terminal_fail(
                &context.storage_handle,
                job_id,
                token,
                error,
                result,
            ))
            .await;
            Box::pin(cleanup_and_crate(context, job_id, terminal)).await;
            None
        }
    }
}

fn execution_result(
    record: &JobRecord,
    exit_code: Option<i32>,
    outputs: Vec<OutputObject>,
) -> JobResultPayload {
    let bucket = job_bucket(record);
    execution_result_for(&bucket, exit_code, outputs, LogTails::default())
}

fn execution_result_for(
    bucket: &str,
    exit_code: Option<i32>,
    outputs: Vec<OutputObject>,
    logs: LogTails,
) -> JobResultPayload {
    JobResultPayload::Execution {
        exit_code,
        workspace_bucket: (!bucket.is_empty()).then(|| bucket.to_string()),
        outputs,
        stdout: log_tail(logs.stdout, logs.stdout_truncated),
        stderr: log_tail(logs.stderr, logs.stderr_truncated),
        output_digest: None,
    }
}

/// Failure message for a non-zero exit, keeping the exit code readable when the
/// backend detail would not fit the stored message.
pub(super) fn exit_message(code: i32, detail: Option<&str>) -> String {
    let head = format!("container exited with code {code}");
    let Some(detail) = detail.map(str::trim).filter(|text| !text.is_empty()) else {
        return head;
    };
    let room = MAX_MESSAGE_BYTES.saturating_sub(head.len() + 2);
    format!("{head}: {}", tail_str(detail, room))
}

async fn capture_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
) -> Option<LogTails> {
    let default_limits = LogLimits::default();
    let limits = LogLimits {
        max_stream_bytes: default_limits.inline_tail_bytes,
        ..default_limits
    };
    match backend.fetch_logs(fence, &limits).await {
        Ok(logs) => Some(logs),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Container log capture failed");
            Box::pin(park_attempt(
                context,
                job_id,
                token,
                JobError::retryable(format!("container log capture failed: {error}")),
            ))
            .await;
            None
        }
    }
}

fn log_tail(bytes: Vec<u8>, truncated: bool) -> String {
    let tail = String::from_utf8_lossy(&bytes);
    if truncated {
        format!("[truncated]\n{tail}")
    } else {
        tail.into_owned()
    }
}
