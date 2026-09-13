use std::sync::Arc;

use aruna_compute::ExecutorBackend;
use aruna_core::compute::{
    AttemptStatus, BackendError, FenceContext, ReconcileEvidence, TombstoneSpec,
};
use aruna_core::structs::{ExecutionSpec, JobError, JobId, JobRecord};
use aruna_core::time::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::super::store::{
    ParkOutcome, begin_external_running, mark_indeterminate, read_job_record,
    record_attempt_tombstone, requeue_before_attempt,
};
use super::compute::{RecoveryAction, recovery_action};
use super::finalize::{cleanup_and_crate, fail_and_crate, finalize_attempt, finalize_cancel};
use super::prepare::{build_task_spec, prepare_inputs};
use crate::driver::DriverContext;

/// A submit error after the write-ahead intent is ambiguous: the container may
/// already exist or run. Requeueing would erase the intent and launch a second
/// container under the next attempt name, so the deterministic name is used instead.
#[allow(clippy::too_many_arguments)]
pub(super) async fn recover_failed_submit(
    context: &Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    bucket: &str,
    cancel: &CancellationToken,
    error: BackendError,
) -> bool {
    let storage = &context.storage_handle;
    if matches!(&error, BackendError::Cancelled)
        || cancel.is_cancelled()
        || read_job_record(storage, job_id, None)
            .await
            .ok()
            .flatten()
            .is_some_and(|record| record.cancel_requested)
    {
        Box::pin(finalize_cancel(
            context, job_id, token, backend, fence, spec, bucket,
        ))
        .await;
        return false;
    }
    let record = match read_job_record(storage, job_id, None).await {
        Ok(Some(record)) => record,
        _ => return false,
    };
    let record = &record;
    let evidence = backend.reconcile(fence).await;
    match recovery_action(&evidence) {
        RecoveryAction::Observe => {
            let ReconcileEvidence::Adoptable(evidence) = evidence else {
                return false;
            };
            let status = evidence.status;
            if status.is_terminal() {
                let running = match begin_external_running(
                    storage,
                    job_id,
                    token,
                    status.started_at_ms,
                    unix_timestamp_millis(),
                )
                .await
                {
                    Ok(record) => record,
                    Err(_) => return false,
                };
                if running.cancel_requested {
                    Box::pin(finalize_cancel(
                        context, job_id, token, backend, fence, spec, bucket,
                    ))
                    .await;
                    return false;
                }
                Box::pin(finalize_attempt(
                    context,
                    job_id,
                    token,
                    backend,
                    fence,
                    spec,
                    bucket,
                    Ok(status),
                ))
                .await;
                return false;
            }
            let running = match begin_external_running(
                storage,
                job_id,
                token,
                status.started_at_ms,
                unix_timestamp_millis(),
            )
            .await
            {
                Ok(record) => record,
                Err(_) => return false,
            };
            if running.cancel_requested {
                Box::pin(finalize_cancel(
                    context, job_id, token, backend, fence, spec, bucket,
                ))
                .await;
                return false;
            }
            true
        }
        RecoveryAction::RetrySame => {
            let status = match Box::pin(retry_same_submit(
                context, backend, fence, spec, record, cancel,
            ))
            .await
            {
                Ok(status) => status,
                Err(BackendError::Cancelled) => {
                    Box::pin(finalize_cancel(
                        context, job_id, token, backend, fence, spec, bucket,
                    ))
                    .await;
                    return false;
                }
                Err(retry_error) if retry_error.retryable() => {
                    Box::pin(park_failed_submit(context, job_id, token, &retry_error)).await;
                    return false;
                }
                Err(retry_error) => {
                    Box::pin(retire_failed_submit(
                        context,
                        job_id,
                        token,
                        backend,
                        fence,
                        record,
                        &retry_error,
                    ))
                    .await;
                    return false;
                }
            };
            let running = match begin_external_running(
                storage,
                job_id,
                token,
                status.started_at_ms,
                unix_timestamp_millis(),
            )
            .await
            {
                Ok(record) => record,
                Err(_) => return false,
            };
            if running.cancel_requested {
                Box::pin(finalize_cancel(
                    context, job_id, token, backend, fence, spec, bucket,
                ))
                .await;
                return false;
            }
            true
        }
        RecoveryAction::Cleanup => {
            let _ = backend.cancel(fence).await;
            Box::pin(park_failed_submit(context, job_id, token, &error)).await;
            false
        }
        RecoveryAction::Retire => {
            let ReconcileEvidence::Tombstoned(tombstone) = evidence else {
                return false;
            };
            if record_attempt_tombstone(
                storage,
                job_id,
                token,
                fence.attempt_epoch,
                tombstone.backend_ref,
            )
            .await
            .is_err()
            {
                return false;
            }
            Box::pin(requeue_after_tombstone(
                context, job_id, token, record, &error,
            ))
            .await;
            false
        }
        RecoveryAction::Park => {
            Box::pin(park_failed_submit(context, job_id, token, &error)).await;
            false
        }
    }
}

async fn retry_same_submit(
    context: &Arc<DriverContext>,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    cancel: &CancellationToken,
) -> Result<AttemptStatus, BackendError> {
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or_else(|| BackendError::Unavailable("execution needs a net handle".to_string()))?;
    let prepared = Box::pin(prepare_inputs(context, spec, record, node_id))
        .await
        .map_err(|error| BackendError::Unavailable(error.message))?;
    let pinned_image = record
        .attempt_intent
        .as_ref()
        .filter(|intent| intent.attempt_epoch == fence.attempt_epoch)
        .map(|intent| intent.pinned_image.as_str())
        .ok_or_else(|| BackendError::Conflict("attempt intent mismatch".to_string()))?;
    let task_spec = build_task_spec(
        spec,
        &fence.attempt,
        pinned_image,
        prepared,
        backend.run_identity(),
    );
    backend.submit(fence, &task_spec, cancel).await
}

async fn retire_failed_submit(
    context: &Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    record: &JobRecord,
    error: &BackendError,
) {
    let Ok(tombstone) = backend
        .tombstone(fence, &TombstoneSpec { terminal_ref: None })
        .await
    else {
        Box::pin(park_failed_submit(context, job_id, token, error)).await;
        return;
    };
    if record_attempt_tombstone(
        &context.storage_handle,
        job_id,
        token,
        fence.attempt_epoch,
        tombstone.backend_ref,
    )
    .await
    .is_err()
    {
        return;
    }
    Box::pin(requeue_after_tombstone(
        context, job_id, token, record, error,
    ))
    .await;
}

pub(super) async fn requeue_after_tombstone(
    context: &Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: &BackendError,
) {
    let job_error = if error.retryable() {
        JobError::retryable(format!("submit failed: {error}"))
    } else {
        JobError::permanent(format!("submit failed: {error}"))
    };
    Box::pin(pre_submit_failure(
        context, job_id, token, record, job_error, false,
    ))
    .await;
}

async fn park_failed_submit(
    context: &Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    error: &BackendError,
) {
    park_attempt(
        context,
        job_id,
        token,
        JobError::retryable(format!("submit failed ambiguously: {error}")),
    )
    .await;
}

/// Park an ambiguous attempt for a later reconcile pass. The park charges an
/// attempt, so a failure that repeats on every pass terminalizes at the cap
/// instead of re-driving the job forever.
pub(super) async fn park_attempt(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    error: JobError,
) {
    match mark_indeterminate(
        &context.storage_handle,
        job_id,
        token,
        error,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(ParkOutcome::Parked(_)) => {}
        Ok(ParkOutcome::Exhausted(record)) => {
            warn!(job_id = %job_id, "Local attempts exhausted; parking job");
            Box::pin(cleanup_and_crate(context, job_id, Some(record))).await;
        }
        Err(error) => warn!(job_id = %job_id, error = %error, "Attempt park write failed"),
    }
}
pub(super) async fn pre_submit_failure(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: JobError,
    error_logged: bool,
) {
    if error.kind == aruna_core::structs::JobErrorKind::Permanent {
        if !error_logged {
            warn!(job_id = %job_id, error = ?error, "Permanent pre-submit failure");
        }
        Box::pin(fail_and_crate(context, job_id, token, record, error)).await;
        return;
    }
    match requeue_before_attempt(
        &context.storage_handle,
        job_id,
        token,
        unix_timestamp_millis(),
        error,
    )
    .await
    {
        Ok(record) if record.is_settled() => {
            Box::pin(cleanup_and_crate(context, job_id, Some(record))).await;
        }
        Ok(_) => {}
        Err(error) => warn!(job_id = %job_id, error = %error, "Pre-submit requeue failed"),
    }
}
