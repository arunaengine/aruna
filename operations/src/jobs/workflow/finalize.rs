//! Finalizes a compute attempt: stores outputs and writes the terminal job state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_compute::ExecutorBackend;
use aruna_compute::session::Session;
use aruna_core::compute::session::EndReason;
use aruna_core::compute::{
    AttemptPhase, AttemptStatus, BackendError, CancelEvidence, FenceContext, LogLimits, LogTails,
};
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::tail_str;
use aruna_core::structs::execution::job::{
    AttemptControl, ExecutionSpec, JobError, JobId, JobRecord, JobRecordError, JobResultPayload,
    MAX_MESSAGE_BYTES, OutputObject,
};
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::output_record::StoreOutputsOperation;
use super::super::store::attempt::{
    read_attempt_control, record_attempt_started, transition_to_cancelling,
};
use super::super::store::state::{ExecutionCompleteOutcome, complete_execution, read_job_record};
use super::super::submit::schedule_drain_effect;
use super::inventory::CollectOutputsOperation;
use super::prepare::job_bucket;
use super::recovery::park_attempt;
use super::session::report_session;
use super::settle::{
    AnnounceTerminalOperation, EndRunOperation, RunEnding, SettleScope, execution_result,
};
use super::workspace::{capture_outputs, merge_outputs};
use crate::driver::{DriverContext, drive};

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
    let _ = transition_to_cancelling(context, job_id, token, unix_timestamp_millis()).await;
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
    Box::pin(report_session(context, job_id, token, session, reason)).await;
    // A kernel that died, or a helper that never answered, is a permanent
    // execution failure, not a session the caller finished.
    if reason == EndReason::KernelExit {
        let result = execution_result(bucket, Some(1), Vec::new(), logs);
        end_run(
            context,
            job_id,
            token,
            RunEnding::Fail {
                error: JobError::permanent("session kernel exited"),
                result,
            },
        )
        .await;
        return;
    }
    // A terminal success needs a named output record, even with no outputs.
    let Some(control) = Box::pin(control_or_park(context, job_id, token, fence)).await else {
        return;
    };
    let mut result = execution_result(bucket, Some(0), Vec::new(), logs);
    let Some(digest) = Box::pin(store_or_fail(
        context, job_id, token, bucket, &control, &result,
    ))
    .await
    else {
        return;
    };
    name_output_record(&mut result, digest);
    end_run(context, job_id, token, RunEnding::Complete { result }).await;
}

pub(super) async fn finalize_walltime(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    bucket: &str,
) {
    let _ = transition_to_cancelling(context, job_id, token, unix_timestamp_millis()).await;
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
    let result = execution_result(bucket, None, Vec::new(), logs);
    end_run(
        context,
        job_id,
        token,
        RunEnding::Fail {
            error: JobError::permanent("walltime limit exceeded"),
            result,
        },
    )
    .await;
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
    cancel: &CancellationToken,
) {
    let cancel_requested = read_job_record(context, job_id, None)
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
        && let Err(error) = record_attempt_started(context, job_id, token, started_at_ms).await
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
                context,
                job_id,
                token,
                backend,
                fence,
                spec,
                bucket,
                Some(cancel),
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
            let mut result = execution_result(bucket, Some(0), outputs, logs);
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
            match Box::pin(terminal_execution(context, job_id, token, result)).await {
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
            let result = execution_result(bucket, Some(code), outputs, logs);
            end_run(
                context,
                job_id,
                token,
                RunEnding::Fail {
                    error: JobError::permanent(message),
                    result,
                },
            )
            .await;
        }
        AttemptPhase::Failed { reason } => {
            let Some(logs) =
                Box::pin(capture_or_park(context, job_id, token, backend, fence)).await
            else {
                return;
            };
            let result = execution_result(bucket, None, Vec::new(), logs);
            end_run(
                context,
                job_id,
                token,
                RunEnding::Fail {
                    error: JobError::permanent(format!("backend failure: {reason}")),
                    result,
                },
            )
            .await;
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
    // Running -> Cancelling (idempotent: a re-entry may already be Cancelling).
    let _ = transition_to_cancelling(context, job_id, token, unix_timestamp_millis()).await;
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
                        context, job_id, token, backend, fence, spec, bucket, None,
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
                    let mut result = execution_result(bucket, Some(0), outputs, logs);
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
                    end_run(context, job_id, token, RunEnding::Complete { result }).await;
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
                    let result = execution_result(bucket, Some(code), outputs, logs);
                    end_run(
                        context,
                        job_id,
                        token,
                        RunEnding::Fail {
                            error: JobError::permanent(message),
                            result,
                        },
                    )
                    .await;
                }
                AttemptPhase::Failed { reason } => {
                    let result = execution_result(bucket, None, Vec::new(), logs);
                    end_run(
                        context,
                        job_id,
                        token,
                        RunEnding::Fail {
                            error: JobError::permanent(format!("backend failure: {reason}")),
                            result,
                        },
                    )
                    .await;
                }
                // Infrastructure evidence is no verdict on the job, so a confirmed
                // stop terminalizes as the requested cancellation, never as Failed.
                AttemptPhase::SystemError { reason } => {
                    warn!(job_id = %job_id, %reason, "Cancelled attempt lost backend evidence");
                    let result = execution_result(bucket, None, Vec::new(), logs);
                    end_run(context, job_id, token, RunEnding::Cancel { result }).await;
                }
                AttemptPhase::Cancelled | AttemptPhase::Submitted | AttemptPhase::Running => {
                    let Some(outputs) = Box::pin(collect_or_park(
                        context, job_id, token, spec, bucket, &control,
                    ))
                    .await
                    else {
                        return;
                    };
                    let result = execution_result(bucket, None, outputs, logs);
                    end_run(context, job_id, token, RunEnding::Cancel { result }).await;
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
            let result = execution_result(bucket, None, outputs, LogTails::default());
            end_run(context, job_id, token, RunEnding::Cancel { result }).await;
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
    let record = match read_job_record(&context, job_id, None).await {
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
    let store = StoreOutputsOperation::new(
        &record,
        control,
        outputs,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    match Box::pin(drive(store, context)).await {
        Ok(digest) => Some(digest),
        Err(error)
            if error.kind == aruna_core::structs::execution::job::JobErrorKind::Permanent =>
        {
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
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    result: JobResultPayload,
) -> Option<ExecutionCompleteOutcome> {
    let progress = read_job_record(context, job_id, None)
        .await
        .ok()
        .flatten()
        .map(|record| record.progress)
        .unwrap_or_else(|| aruna_core::structs::execution::job::JobProgress::new("phases"));
    match complete_execution(
        context,
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

async fn cancel_requested(context: &DriverContext, job_id: JobId) -> bool {
    read_job_record(context, job_id, None)
        .await
        .ok()
        .flatten()
        .is_some_and(|record| record.cancel_requested)
}

/// Settles a cancelled run without its outputs; cleanup removes its capture helpers.
async fn cancel_capture(context: &DriverContext, job_id: JobId, token: ulid::Ulid, bucket: &str) {
    let _ = transition_to_cancelling(context, job_id, token, unix_timestamp_millis()).await;
    let result = execution_result(bucket, Some(0), Vec::new(), LogTails::default());
    end_run(context, job_id, token, RunEnding::Cancel { result }).await;
}

/// The clock and node a settling run works with, read now.
pub(super) fn settle_scope(context: &DriverContext) -> SettleScope {
    SettleScope {
        serving: context.node_and_realm(),
        now_ms: unix_timestamp_millis(),
    }
}

/// Ends the run with `ending` and announces the terminal state it wrote.
async fn end_run(context: &DriverContext, job_id: JobId, token: ulid::Ulid, ending: RunEnding) {
    let end = EndRunOperation::new(job_id, token, ending, settle_scope(context));
    if let Err(error) = Box::pin(drive(end, context)).await {
        warn!(job_id = %job_id, error = %error, "Execution end failed");
    }
}

/// Announces a terminal state this run wrote; a lost race has nothing to announce.
pub(super) async fn cleanup_and_crate(
    context: &DriverContext,
    job_id: JobId,
    record: Option<JobRecord>,
) {
    let Some(record) = record else { return };
    let announce = AnnounceTerminalOperation::new(record, settle_scope(context));
    if let Err(error) = Box::pin(drive(announce, context)).await {
        warn!(job_id = %job_id, error = %error, "Terminal announcement failed");
    }
}

pub(super) async fn fail_and_crate(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: JobError,
) {
    let result = execution_result(&job_bucket(record), None, Vec::new(), LogTails::default());
    end_run(context, job_id, token, RunEnding::Fail { error, result }).await;
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

/// Physical execution identity of the fenced attempt. Without it no output can
/// be named exactly, so the attempt is parked instead of terminalized.
async fn control_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    fence: &FenceContext,
) -> Option<AttemptControl> {
    let control = read_attempt_control(context, job_id, fence.attempt_epoch, None).await;
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
    let result = execution_result(bucket, None, Vec::new(), LogTails::default());
    end_run(
        context,
        job_id,
        token,
        RunEnding::Fail {
            error: JobError::permanent(format!("output identity incomplete: {error}")),
            result,
        },
    )
    .await;
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
    let node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let collect = CollectOutputsOperation::new(spec, bucket, control.clone(), node_id);
    match Box::pin(drive(collect, context)).await {
        Ok(outputs) => Some(outputs),
        Err(error)
            if error.kind == aruna_core::structs::execution::job::JobErrorKind::Permanent =>
        {
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
    match read_job_record(context, job_id, None).await {
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

/// A cancel wins over capture: `cancel` stops a running capture, and a failed capture of a
/// cancelled run settles it as cancelled instead of retrying it.
#[allow(clippy::too_many_arguments)]
async fn export_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    bucket: &str,
    cancel: Option<&CancellationToken>,
) -> Option<Vec<OutputObject>> {
    if spec.file_outputs.is_empty() {
        return Some(Vec::new());
    }
    let record = match read_job_record(context, job_id, None).await {
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
        let result = execution_result(bucket, Some(0), Vec::new(), LogTails::default());
        end_run(
            context,
            job_id,
            token,
            RunEnding::Fail {
                error: error,
                result,
            },
        )
        .await;
        return None;
    };
    // The cancel path itself captures what finished before the cancel, so it is never stopped.
    let never = CancellationToken::new();
    let stop = cancel.unwrap_or(&never);
    let captured = Box::pin(capture_outputs(
        context,
        backend,
        fence,
        spec,
        &record,
        (node_id, stop),
    ))
    .await;
    // A cancelled run settles once its capture has returned, after any put cleaned up.
    let cancelled = || async {
        cancel.is_none_or(CancellationToken::is_cancelled)
            || cancel_requested(context, job_id).await
    };
    match captured {
        Ok(outputs) => Some(outputs),
        Err(error) if cancelled().await => {
            info!(job_id = %job_id, error = ?error, "Output capture of a cancelled run stopped");
            Box::pin(cancel_capture(context, job_id, token, bucket)).await;
            None
        }
        Err(error)
            if error.kind == aruna_core::structs::execution::job::JobErrorKind::Retryable =>
        {
            warn!(job_id = %job_id, error = ?error, "Output capture failed; parking");
            Box::pin(park_attempt(context, job_id, token, error)).await;
            None
        }
        Err(error) => {
            let result = execution_result(bucket, Some(0), Vec::new(), LogTails::default());
            end_run(
                context,
                job_id,
                token,
                RunEnding::Fail {
                    error: error,
                    result,
                },
            )
            .await;
            None
        }
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
