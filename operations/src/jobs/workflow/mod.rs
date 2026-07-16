pub mod cleanup;
pub mod compute;
pub mod reconcile;
pub mod run_crate;
pub mod workspace;

use std::sync::Arc;
use std::time::Duration;

use aruna_compute::ExecutorBackend;
use aruna_compute::executor::logs::NullSink;
use aruna_core::compute::{
    AttemptPhase, AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind,
    FenceContext, LogLimits, LogTails, ReconcileEvidence, ResourceRequest, StagingMode, TaskInput,
    TaskSpec, TombstoneSpec,
};
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::structs::{
    AttemptIntent, ExecutionSpec, JobError, JobId, JobPayload, JobRecord, JobResultPayload,
    OutputObject,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::JOB_HEARTBEAT_MS;
use super::store::{
    ExecutionCompleteOutcome, JobMutationError, cancel_execution, cancel_running_job,
    complete_execution, complete_job, fail_execution, mark_indeterminate, read_job_record,
    record_attempt_intent, record_attempt_started, record_attempt_tombstone, renew_lease,
    requeue_before_attempt, set_workspace_bucket, transition_external_to_running,
    transition_to_cancelling, transition_to_preparing, transition_to_ready,
};
use super::submit::schedule_job_drain_effect;
use crate::driver::DriverContext;
use compute::{RecoveryAction, recovery_action};
use workspace::{
    capture_outputs, collect_outputs, ensure_group_write, ensure_workspace_bucket, load_inputs,
    stage_inputs,
};

/// Drive a claimed execution job through prepare -> submit -> supervise -> finalize.
/// External attempts never share the generic in-process supervisor because a lost
/// lease must not requeue a container (spec 16.7); this owns the fenced lifecycle.
pub async fn run_execution_job(
    context: Arc<DriverContext>,
    record: JobRecord,
    cancel: CancellationToken,
) {
    let storage = &context.storage_handle;
    let job_id = record.job_id;
    let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
        warn!(job_id = %job_id, "Execution job has no claim token; skipping");
        return;
    };
    let JobPayload::Execution(spec) = record.payload.clone() else {
        return;
    };

    // A fresh cancel before any attempt was submitted: no container exists, so
    // terminalize directly (Claimed -> Cancelled).
    if record.cancel_requested && record.attempt_intent.is_none() {
        match cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await {
            Ok(_) => finalize_followups(&context, job_id).await,
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Fresh cancellation write failed")
            }
        }
        return;
    }

    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        fail_and_crate(
            &context,
            job_id,
            token,
            &record,
            JobError::permanent("execution needs a net handle"),
        )
        .await;
        return;
    };

    let backend = match resolve_backend(&context, &spec) {
        Ok(backend) => backend,
        Err(error) => {
            fail_and_crate(&context, job_id, token, &record, error).await;
            return;
        }
    };

    // Claimed -> Preparing.
    if transition_to_preparing(storage, job_id, token, unix_timestamp_millis())
        .await
        .is_err()
    {
        warn!(job_id = %job_id, "Lost claim before preparing; aborting");
        return;
    }

    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(execution_heartbeat(
        storage.clone(),
        job_id,
        token,
        cancel.clone(),
        stop.clone(),
    ));
    tokio::pin!(heartbeat);

    let prepare_and_submit = async {
        let bucket = JobRecord::workspace_bucket_name(job_id);
        let inputs =
            match prepare_workspace(&context, &spec, &record, node_id, &bucket, token).await {
                Ok(prepared) => prepared,
                Err(error) => {
                    requeue_or_fail_pre_submit(&context, job_id, token, &record, error).await;
                    return None;
                }
            };

        // Preparing -> Ready.
        if transition_to_ready(storage, job_id, token, unix_timestamp_millis())
            .await
            .is_err()
        {
            return None;
        }

        let attempt_no = record.attempts;
        // Lowercased: attempt ids must be injective under the backend name mapping.
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), attempt_no);
        let pinned_image = match backend.resolve_image(&spec.image).await {
            Ok(image) => image,
            Err(error) => {
                let job_error = if error.retryable() {
                    JobError::retryable(format!("image resolution failed: {error}"))
                } else {
                    JobError::permanent(format!("image resolution failed: {error}"))
                };
                requeue_or_fail_pre_submit(&context, job_id, token, &record, job_error).await;
                return None;
            }
        };
        let task_spec = build_task_spec(&spec, &attempt, &pinned_image, inputs);

        // Write-ahead the attempt intent BEFORE submit so a lost attempt is adoptable.
        let intent = AttemptIntent {
            attempt_no,
            external_name: attempt.external_name(),
            executor_kind: backend.kind().as_wire(),
            pinned_image,
            attempt_epoch: 0,
        };
        let intent_commit =
            match record_attempt_intent(storage, job_id, token, intent, unix_timestamp_millis())
                .await
            {
                Ok(record) => record,
                Err(JobMutationError::IntentConflict) => {
                    if read_job_record(storage, job_id, None)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|record| record.cancel_requested)
                    {
                        match cancel_running_job(storage, job_id, token, unix_timestamp_millis())
                            .await
                        {
                            Ok(_) => finalize_followups(&context, job_id).await,
                            Err(error) => {
                                warn!(job_id = %job_id, error = %error, "Pre-submit cancellation write failed")
                            }
                        }
                    }
                    return None;
                }
                Err(_) => return None,
            };
        if intent_commit.record.cancel_requested {
            match cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await {
                Ok(_) => finalize_followups(&context, job_id).await,
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Pre-submit cancellation write failed")
                }
            }
            return None;
        }

        let fence = FenceContext {
            attempt: attempt.clone(),
            attempt_epoch: intent_commit.control.attempt_epoch,
            controller_generation: intent_commit.control.controller_generation,
        };
        if backend.fence(&fence).await.is_err() {
            return None;
        }

        let submitted = match backend.submit(&fence, &task_spec, &cancel).await {
            Ok(status) => status,
            Err(BackendError::Cancelled) => {
                finalize_cancel(&context, job_id, token, &backend, &fence, &spec, &bucket).await;
                return None;
            }
            Err(error) => {
                return Some(Err((backend, fence, spec, bucket, cancel, error)));
            }
        };

        // Ready -> Running only after the backend accepted the fenced attempt.
        let running = match transition_external_to_running(
            storage,
            job_id,
            token,
            submitted.started_at_ms,
            unix_timestamp_millis(),
        )
        .await
        {
            Ok(record) => record,
            Err(_) => return None,
        };
        if running.cancel_requested {
            finalize_cancel(&context, job_id, token, &backend, &fence, &spec, &bucket).await;
            return None;
        }

        Some(Ok((backend, fence, spec, bucket, cancel)))
    };
    tokio::pin!(prepare_and_submit);

    let prepared = tokio::select! {
        result = &mut prepare_and_submit => result,
        _ = &mut heartbeat => return,
    };
    let Some(prepared) = prepared else {
        stop.cancel();
        let _ = (&mut heartbeat).await;
        return;
    };
    match prepared {
        Ok((backend, fence, spec, bucket, cancel)) => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            supervise_and_finalize(
                context.clone(),
                job_id,
                token,
                backend,
                fence,
                spec,
                bucket,
                cancel,
            )
            .await;
        }
        Err((backend, fence, spec, bucket, cancel, error)) => {
            let resumed = {
                let recovery = recover_failed_submit(
                    &context, job_id, token, &backend, &fence, &spec, &bucket, &cancel, error,
                );
                tokio::pin!(recovery);
                tokio::select! {
                    result = &mut recovery => {
                        stop.cancel();
                        let _ = (&mut heartbeat).await;
                        Some(result)
                    }
                    _ = &mut heartbeat => None,
                }
            };
            if resumed == Some(true) {
                supervise_and_finalize(
                    context.clone(),
                    job_id,
                    token,
                    backend,
                    fence,
                    spec,
                    bucket,
                    cancel,
                )
                .await;
            }
        }
    }
}

/// Resolve the backend for a spec, or a permanent error when none is eligible.
pub fn resolve_backend(
    context: &DriverContext,
    spec: &ExecutionSpec,
) -> Result<Arc<dyn ExecutorBackend>, JobError> {
    let Some(registry) = context.compute_handle.as_ref() else {
        return Err(JobError::permanent("no compute backend configured"));
    };
    let constraint = spec
        .executor_constraint
        .as_deref()
        .map(ExecutorKind::from_wire);
    registry
        .select(constraint.as_ref())
        .cloned()
        .ok_or_else(|| JobError::permanent("no eligible executor for job"))
}

async fn prepare_workspace(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
    token: ulid::Ulid,
) -> Result<Vec<TaskInput>, JobError> {
    ensure_group_write(context, spec, record, node_id).await?;
    ensure_workspace_bucket(context, spec, record, bucket).await?;
    set_workspace_bucket(
        &context.storage_handle,
        record.job_id,
        token,
        bucket.to_string(),
        unix_timestamp_millis(),
    )
    .await
    .map_err(|error| JobError::retryable(format!("workspace bucket record failed: {error}")))?;
    stage_inputs(context, spec, record, bucket, node_id).await?;
    load_inputs(context, spec, record, bucket).await
}

fn build_task_spec(
    spec: &ExecutionSpec,
    attempt: &AttemptRef,
    pinned_image: &str,
    inputs: Vec<TaskInput>,
) -> TaskSpec {
    let resources = ResourceRequest {
        cpu_cores: spec.resources.cpu_cores,
        ram_bytes: spec.resources.ram_bytes,
        disk_bytes: spec.resources.disk_bytes,
        max_walltime: spec.resources.max_walltime_ms.map(Duration::from_millis),
        preemptible: spec.resources.preemptible,
        backend_extensions: std::collections::BTreeMap::new(),
    };
    TaskSpec {
        attempt: attempt.clone(),
        image: pinned_image.to_string(),
        entrypoint: spec.entrypoint.clone(),
        command: spec.command.clone(),
        workdir: spec.workdir.clone(),
        env: spec.env.clone(),
        secret_env: std::collections::BTreeMap::new(),
        resources,
        workspace: None,
        security: Default::default(),
        log_limits: Default::default(),
        inputs,
        staging_mode: StagingMode::Files,
        output_paths: spec
            .file_outputs
            .iter()
            .map(|output| output.container_path.clone())
            .collect(),
    }
}

/// A submit error after the write-ahead intent is ambiguous: the container may
/// already exist (created, or even running when the final status read faulted).
/// Requeueing would erase the intent and launch a second container under the
/// next attempt name, so the error is resolved by the deterministic name instead.
#[allow(clippy::too_many_arguments)]
async fn recover_failed_submit(
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
        finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
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
                let running = match transition_external_to_running(
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
                    finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
                    return false;
                }
                finalize_attempt(
                    context,
                    job_id,
                    token,
                    backend,
                    fence,
                    spec,
                    bucket,
                    Ok(status),
                )
                .await;
                return false;
            }
            let running = match transition_external_to_running(
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
                finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
                return false;
            }
            true
        }
        RecoveryAction::RetrySame => {
            let status = match retry_same_submit(
                context, backend, fence, spec, bucket, record, cancel,
            )
            .await
            {
                Ok(status) => status,
                Err(BackendError::Cancelled) => {
                    finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
                    return false;
                }
                Err(retry_error) if retry_error.retryable() => {
                    park_failed_submit(context, job_id, token, &retry_error).await;
                    return false;
                }
                Err(retry_error) => {
                    retire_failed_submit(
                        context,
                        job_id,
                        token,
                        backend,
                        fence,
                        record,
                        &retry_error,
                    )
                    .await;
                    return false;
                }
            };
            let running = match transition_external_to_running(
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
                finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
                return false;
            }
            true
        }
        RecoveryAction::Cleanup => {
            let _ = backend.cancel(fence).await;
            park_failed_submit(context, job_id, token, &error).await;
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
            requeue_after_tombstone(context, job_id, token, record, &error).await;
            false
        }
        RecoveryAction::Park => {
            park_failed_submit(context, job_id, token, &error).await;
            false
        }
    }
}

async fn retry_same_submit(
    context: &Arc<DriverContext>,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    bucket: &str,
    record: &JobRecord,
    cancel: &CancellationToken,
) -> Result<AttemptStatus, BackendError> {
    let inputs = load_inputs(context, spec, record, bucket)
        .await
        .map_err(|error| BackendError::Unavailable(error.message))?;
    let pinned_image = record
        .attempt_intent
        .as_ref()
        .filter(|intent| intent.attempt_epoch == fence.attempt_epoch)
        .map(|intent| intent.pinned_image.as_str())
        .ok_or_else(|| BackendError::Conflict("attempt intent mismatch".to_string()))?;
    let task_spec = build_task_spec(spec, &fence.attempt, pinned_image, inputs);
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
        park_failed_submit(context, job_id, token, error).await;
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
    requeue_after_tombstone(context, job_id, token, record, error).await;
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
    requeue_or_fail_pre_submit(context, job_id, token, record, job_error).await;
}

async fn park_failed_submit(
    context: &Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    error: &BackendError,
) {
    let _ = mark_indeterminate(
        &context.storage_handle,
        job_id,
        token,
        JobError::retryable(format!("submit failed ambiguously: {error}")),
        unix_timestamp_millis(),
    )
    .await;
}

/// Heartbeat + backend wait race, then evidence-based terminalization. Shared by
/// the fresh path and reconcile adoption.
#[allow(clippy::too_many_arguments)]
pub async fn supervise_and_finalize(
    context: Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    backend: Arc<dyn ExecutorBackend>,
    fence: FenceContext,
    spec: ExecutionSpec,
    bucket: String,
    cancel: CancellationToken,
) {
    let storage = context.storage_handle.clone();
    let walltime_ms = spec
        .resources
        .max_walltime_ms
        .unwrap_or(24 * 60 * 60 * 1_000);
    let walltime_left = read_job_record(&storage, job_id, None)
        .await
        .ok()
        .flatten()
        .and_then(|record| record.started_at_ms)
        .map(|started_at_ms| {
            Duration::from_millis(
                started_at_ms
                    .saturating_add(walltime_ms)
                    .saturating_sub(unix_timestamp_millis()),
            )
        });
    let wait_and_finalize = async {
        let result = if let Some(walltime_left) = walltime_left {
            tokio::select! {
                result = backend.wait(&fence, &cancel) => Some(result),
                _ = tokio::time::sleep(walltime_left) => None,
            }
        } else {
            Some(backend.wait(&fence, &cancel).await)
        };
        if let Some(result) = result {
            finalize_attempt(
                &context, job_id, token, &backend, &fence, &spec, &bucket, result,
            )
            .await;
        } else {
            finalize_cancel(&context, job_id, token, &backend, &fence, &spec, &bucket).await;
        }
    };
    if with_execution_heartbeat(storage, job_id, token, cancel.clone(), wait_and_finalize)
        .await
        .is_none()
    {
        info!(job_id = %job_id, "Execution supervisor superseded; abandoning");
    }
}

pub(super) async fn with_execution_heartbeat<T>(
    storage: aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    cancel: CancellationToken,
    work: impl std::future::Future<Output = T>,
) -> Option<T> {
    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(execution_heartbeat(
        storage,
        job_id,
        token,
        cancel,
        stop.clone(),
    ));
    tokio::pin!(heartbeat);
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            Some(result)
        }
        _ = &mut heartbeat => None,
    }
}

/// Renew the lease and surface `cancel_requested`. Returns on a lost token so the
/// supervisor treats it as a takeover.
async fn execution_heartbeat(
    storage: aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    cancel: CancellationToken,
    stop: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(JOB_HEARTBEAT_MS));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = interval.tick() => {
                match renew_lease(&storage, job_id, token, unix_timestamp_millis(), None).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Execution lease renew failed"),
                }
            }
        }
    }
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
            let _ = mark_indeterminate(
                storage,
                job_id,
                token,
                JobError::retryable(format!("attempt unobservable: {error}")),
                unix_timestamp_millis(),
            )
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
        finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
        return;
    }

    match status.phase {
        AttemptPhase::Exited { code: 0 } => {
            let Some(mut outputs) = collect_or_park(context, job_id, token, spec, bucket).await
            else {
                return;
            };
            let Some(captured) =
                export_or_park(context, job_id, token, backend, fence, spec, bucket).await
            else {
                return;
            };
            outputs.extend(captured);
            let Some(logs) = capture_or_park(context, job_id, token, backend, fence).await else {
                return;
            };
            let result = execution_result_for(bucket, Some(0), outputs, logs);
            match terminal_execution(storage, job_id, token, result).await {
                Some(ExecutionCompleteOutcome::Completed(record)) => {
                    cleanup_and_crate(context, job_id, Some(record)).await;
                }
                Some(ExecutionCompleteOutcome::CancelRequested(_)) => {
                    finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
                }
                None => {}
            }
        }
        AttemptPhase::Exited { code } => {
            let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await else {
                return;
            };
            let Some(logs) = capture_or_park(context, job_id, token, backend, fence).await else {
                return;
            };
            let result = execution_result_for(bucket, Some(code), outputs, logs);
            let record = terminal_fail(
                storage,
                job_id,
                token,
                JobError::permanent(format!("container exited with code {code}")),
                result,
            )
            .await;
            cleanup_and_crate(context, job_id, record).await;
        }
        AttemptPhase::Failed { reason } => {
            let Some(logs) = capture_or_park(context, job_id, token, backend, fence).await else {
                return;
            };
            let result = execution_result_for(bucket, None, Vec::new(), logs);
            let record = terminal_fail(
                storage,
                job_id,
                token,
                JobError::permanent(format!("backend failure: {reason}")),
                result,
            )
            .await;
            cleanup_and_crate(context, job_id, record).await;
        }
        AttemptPhase::Cancelled => {
            finalize_cancel(context, job_id, token, backend, fence, spec, bucket).await;
        }
        AttemptPhase::Submitted | AttemptPhase::Running => {
            let _ = mark_indeterminate(
                storage,
                job_id,
                token,
                JobError::retryable("attempt returned non-terminal"),
                unix_timestamp_millis(),
            )
            .await;
        }
    }
}

async fn finalize_cancel(
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
    let evidence = backend.cancel(fence).await;
    match evidence {
        Ok(CancelEvidence::Stopped(status)) => {
            let Some(logs) = capture_or_park(context, job_id, token, backend, fence).await else {
                return;
            };
            match status.phase {
                AttemptPhase::Exited { code: 0 } => {
                    let Some(mut outputs) =
                        collect_or_park(context, job_id, token, spec, bucket).await
                    else {
                        return;
                    };
                    let Some(captured) =
                        export_or_park(context, job_id, token, backend, fence, spec, bucket).await
                    else {
                        return;
                    };
                    outputs.extend(captured);
                    let result = execution_result_for(bucket, Some(0), outputs, logs);
                    let record = terminal_complete(storage, job_id, token, result).await;
                    cleanup_and_crate(context, job_id, record).await;
                }
                AttemptPhase::Exited { code } => {
                    let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await
                    else {
                        return;
                    };
                    let result = execution_result_for(bucket, Some(code), outputs, logs);
                    let record = terminal_fail(
                        storage,
                        job_id,
                        token,
                        JobError::permanent(format!("container exited with code {code}")),
                        result,
                    )
                    .await;
                    cleanup_and_crate(context, job_id, record).await;
                }
                AttemptPhase::Failed { reason } => {
                    let result = execution_result_for(bucket, None, Vec::new(), logs);
                    let record = terminal_fail(
                        storage,
                        job_id,
                        token,
                        JobError::permanent(format!("backend failure: {reason}")),
                        result,
                    )
                    .await;
                    cleanup_and_crate(context, job_id, record).await;
                }
                AttemptPhase::Cancelled | AttemptPhase::Submitted | AttemptPhase::Running => {
                    let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await
                    else {
                        return;
                    };
                    let result = execution_result_for(bucket, None, outputs, logs);
                    let record = terminal_cancel(storage, job_id, token, result).await;
                    cleanup_and_crate(context, job_id, record).await;
                }
            }
        }
        Ok(CancelEvidence::AlreadyGone) => {
            let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await else {
                return;
            };
            let result = execution_result_for(bucket, None, outputs, LogTails::default());
            let record = terminal_cancel(storage, job_id, token, result).await;
            cleanup_and_crate(context, job_id, record).await;
        }
        // No definitive stop evidence yet: park in Indeterminate.
        Ok(CancelEvidence::Requested) | Err(_) => {
            let _ = mark_indeterminate(
                storage,
                job_id,
                token,
                JobError::retryable("cancel requested without stop evidence"),
                unix_timestamp_millis(),
            )
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
        .unwrap_or_else(|| aruna_core::structs::JobProgress::new("phases"));
    match complete_job(
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
        .unwrap_or_else(|| aruna_core::structs::JobProgress::new("phases"));
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
async fn cleanup_and_crate(context: &DriverContext, job_id: JobId, record: Option<JobRecord>) {
    // Only act on a terminal record WE wrote (a lost race returns None).
    let Some(_) = record else { return };
    finalize_followups(context, job_id).await;
}

/// Wake the drain for internal jobs persisted with terminalization.
pub(super) async fn finalize_followups(context: &DriverContext, job_id: JobId) {
    if let Some(task_handle) = context.task_handle.as_ref()
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_job_drain_effect()).await
    {
        warn!(job_id = %job_id, message = %message, "Failed to kick run-crate drain");
    }
}

async fn fail_and_crate(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: JobError,
) {
    let result = execution_result(record, None, Vec::new());
    let terminal = terminal_fail(&context.storage_handle, job_id, token, error, result).await;
    if terminal.is_some() {
        finalize_followups(context, job_id).await;
    }
}

async fn requeue_or_fail_pre_submit(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: JobError,
) {
    if error.kind == aruna_core::structs::JobErrorKind::Permanent {
        fail_and_crate(context, job_id, token, record, error).await;
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
        Ok(record) if record.state == aruna_core::structs::JobState::Failed => {
            finalize_followups(context, job_id).await;
        }
        Ok(_) => {}
        Err(error) => warn!(job_id = %job_id, error = %error, "Pre-submit requeue failed"),
    }
}

/// Inventory the declared outputs. A permanent inventory failure terminalizes
/// the job so cleanup runs; a transient one parks it `Indeterminate` instead of
/// terminalizing with a false-empty output manifest.
async fn collect_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Option<Vec<OutputObject>> {
    match collect_outputs(context, spec, bucket).await {
        Ok(outputs) => Some(outputs),
        Err(error) if error.kind == aruna_core::structs::JobErrorKind::Permanent => {
            warn!(job_id = %job_id, bucket = %bucket, error = ?error, "Output inventory failed permanently; failing");
            match read_job_record(&context.storage_handle, job_id, None).await {
                Ok(Some(record)) => {
                    fail_and_crate(context, job_id, token, &record, error).await;
                }
                _ => {
                    let _ = mark_indeterminate(
                        &context.storage_handle,
                        job_id,
                        token,
                        error,
                        unix_timestamp_millis(),
                    )
                    .await;
                }
            }
            None
        }
        Err(error) => {
            warn!(job_id = %job_id, bucket = %bucket, error = ?error, "Output inventory failed; parking");
            let _ = mark_indeterminate(
                &context.storage_handle,
                job_id,
                token,
                error,
                unix_timestamp_millis(),
            )
            .await;
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
            let _ = mark_indeterminate(
                &context.storage_handle,
                job_id,
                token,
                JobError::retryable(format!("output job lookup failed: {error}")),
                unix_timestamp_millis(),
            )
            .await;
            return None;
        }
    };
    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        let error = JobError::permanent("output capture needs a net handle");
        let result = execution_result_for(bucket, Some(0), Vec::new(), LogTails::default());
        let terminal = terminal_fail(&context.storage_handle, job_id, token, error, result).await;
        cleanup_and_crate(context, job_id, terminal).await;
        return None;
    };
    match capture_outputs(context, backend, fence, spec, &record, node_id).await {
        Ok(outputs) => Some(outputs),
        Err(error) if error.kind == aruna_core::structs::JobErrorKind::Retryable => {
            warn!(job_id = %job_id, error = ?error, "Output capture failed; parking");
            let _ = mark_indeterminate(
                &context.storage_handle,
                job_id,
                token,
                error,
                unix_timestamp_millis(),
            )
            .await;
            None
        }
        Err(error) => {
            let result = execution_result_for(bucket, Some(0), Vec::new(), LogTails::default());
            let terminal =
                terminal_fail(&context.storage_handle, job_id, token, error, result).await;
            cleanup_and_crate(context, job_id, terminal).await;
            None
        }
    }
}

fn execution_result(
    record: &JobRecord,
    exit_code: Option<i32>,
    outputs: Vec<OutputObject>,
) -> JobResultPayload {
    let bucket = record
        .workspace_bucket
        .clone()
        .unwrap_or_else(|| JobRecord::workspace_bucket_name(record.job_id));
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
        workspace_bucket: bucket.to_string(),
        outputs,
        stdout: log_tail(logs.stdout, logs.stdout_truncated),
        stderr: log_tail(logs.stderr, logs.stderr_truncated),
    }
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
        max_bytes_per_stream: default_limits.inline_tail_bytes,
        ..default_limits
    };
    match backend.fetch_logs(fence, &limits, &NullSink).await {
        Ok(logs) => Some(logs),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Container log capture failed");
            let _ = mark_indeterminate(
                &context.storage_handle,
                job_id,
                token,
                JobError::retryable(format!("container log capture failed: {error}")),
                unix_timestamp_millis(),
            )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
    use crate::jobs::store::{
        ClaimOutcome, claim_job, insert_job, put_run_crate_status, set_cancel_requested,
    };
    use crate::jobs::workflow::workspace::mint_workspace_credential;
    use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
    use aruna_compute::executor::logs::LogSink;
    use aruna_core::compute::{LogTails, TaskOutput};
    use aruna_core::structs::{ComputeResources, JobErrorKind, JobState, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use aruna_tasks::TaskHandle;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use ulid::Ulid;

    enum StubReconcile {
        NotFound,
        Unavailable,
    }

    struct StubBackend {
        reconcile: StubReconcile,
        submits: Mutex<Vec<String>>,
        logs_started: Notify,
        logs_release: Notify,
    }

    impl StubBackend {
        fn new(reconcile: StubReconcile) -> Arc<Self> {
            Arc::new(Self {
                reconcile,
                submits: Mutex::new(Vec::new()),
                logs_started: Notify::new(),
                logs_release: Notify::new(),
            })
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for StubBackend {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Docker
        }
        async fn health(&self) -> Result<(), BackendError> {
            Ok(())
        }
        async fn resolve_image(&self, _image: &str) -> Result<String, BackendError> {
            Ok(
                "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            )
        }
        async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
            Ok(())
        }
        async fn submit(
            &self,
            _context: &FenceContext,
            spec: &TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            self.submits
                .lock()
                .unwrap()
                .push(spec.attempt.external_name());
            Err(BackendError::Unavailable("stub submit".to_string()))
        }
        async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
            Err(BackendError::Unavailable("stub status".to_string()))
        }
        async fn wait(
            &self,
            _context: &FenceContext,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            Ok(AttemptStatus {
                phase: AttemptPhase::Exited { code: 0 },
                backend_ref: "done".to_string(),
                started_at_ms: Some(1),
                finished_at_ms: Some(2),
            })
        }
        async fn cancel(&self, _context: &FenceContext) -> Result<CancelEvidence, BackendError> {
            Ok(CancelEvidence::AlreadyGone)
        }
        async fn fetch_logs(
            &self,
            _context: &FenceContext,
            _limits: &LogLimits,
            _sink: &dyn LogSink,
        ) -> Result<LogTails, BackendError> {
            self.logs_started.notify_one();
            self.logs_release.notified().await;
            Ok(LogTails::default())
        }
        async fn fetch_output(
            &self,
            _context: &FenceContext,
            _path: &str,
        ) -> Result<TaskOutput, BackendError> {
            Err(BackendError::InvalidSpec("no output".to_string()))
        }
        async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
            match self.reconcile {
                StubReconcile::NotFound => ReconcileEvidence::Absent,
                StubReconcile::Unavailable => {
                    ReconcileEvidence::Unavailable(BackendError::Unavailable("down".to_string()))
                }
            }
        }
        async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
            Ok(())
        }
    }

    fn node_id(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn fence(attempt: &AttemptRef) -> FenceContext {
        FenceContext {
            attempt: attempt.clone(),
            attempt_epoch: 1,
            controller_generation: 1,
        }
    }

    fn context(storage: StorageHandle) -> Arc<DriverContext> {
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        })
    }

    fn execution_spec() -> ExecutionSpec {
        ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: Default::default(),
            resources: ComputeResources {
                cpu_cores: None,
                ram_bytes: None,
                disk_bytes: None,
                max_walltime_ms: None,
                preemptible: false,
            },
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            output_prefixes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn denied_write_blocks() {
        // Queued execution must not outlive the submitter's group write access.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = context(storage);
        let spec = execution_spec();
        let job_id = JobId::new();
        let record = JobRecord::new(
            job_id,
            JobPayload::Execution(spec.clone()),
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        let bucket = JobRecord::workspace_bucket_name(job_id);

        let Err(error) =
            prepare_workspace(&context, &spec, &record, node_id(7), &bucket, Ulid::r#gen()).await
        else {
            panic!("workspace preparation unexpectedly succeeded");
        };
        assert_eq!(error.kind, JobErrorKind::Permanent);
        assert!(
            matches!(
                drive(
                    GetBucketInfoOperation::new(bucket.clone()),
                    context.as_ref()
                )
                .await
                .unwrap(),
                Some(Err(GetBucketInfoError::NotFound))
            ),
            "authorization must fail before workspace creation"
        );

        let Err(error) =
            mint_workspace_credential(&context, &spec, &record, node_id(7), &bucket).await
        else {
            panic!("workspace credential mint unexpectedly succeeded");
        };
        assert_eq!(error.kind, JobErrorKind::Permanent);
    }

    /// A claimed execution job in `Ready` with its attempt intent written, i.e.
    /// the exact state at the moment `backend.submit()` fails.
    async fn ready_with_intent(storage: &StorageHandle) -> (JobRecord, ulid::Ulid, AttemptRef) {
        let job_id = JobId::new();
        let record = JobRecord::new(
            job_id,
            JobPayload::Execution(execution_spec()),
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        insert_job(storage, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(storage, job_id, node_id(7), 2).await.unwrap()
        else {
            panic!("claim failed");
        };
        let token = claimed.claim.as_ref().unwrap().claim_token;
        transition_to_preparing(storage, job_id, token, 3)
            .await
            .unwrap();
        transition_to_ready(storage, job_id, token, 4)
            .await
            .unwrap();
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), claimed.attempts);
        let intent = AttemptIntent {
            attempt_no: claimed.attempts,
            external_name: attempt.external_name(),
            executor_kind: "docker".to_string(),
            pinned_image:
                "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            attempt_epoch: 0,
        };
        let record = record_attempt_intent(storage, job_id, token, intent, 5)
            .await
            .unwrap();
        (record.record, token, attempt)
    }

    // A submit error with an unobservable backend parks the job Indeterminate and
    // keeps the write-ahead intent, so the possibly-started container a{N} stays
    // adoptable and no second container is ever launched under a{N+1}.
    #[tokio::test]
    async fn ambiguous_submit_parks() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::Unavailable);

        recover_failed_submit(
            &ctx,
            record.job_id,
            token,
            &backend,
            &fence(&attempt),
            &execution_spec(),
            "ws-test",
            &CancellationToken::new(),
            BackendError::Unavailable("io fault".to_string()),
        )
        .await;

        let stored = read_job_record(&storage, record.job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Indeterminate);
        assert_eq!(stored.attempts, 0, "no attempt charged");
        let intent = stored.attempt_intent.expect("intent retained");
        assert_eq!(intent.external_name, attempt.external_name());
    }

    // An output-inventory failure at finalize parks the job for a retry instead
    // of terminalizing Succeeded with a false-empty output manifest.
    #[tokio::test]
    async fn inventory_failure_parks() {
        use aruna_core::effects::StorageEffect;
        use aruna_core::events::{Event, StorageEvent};
        use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
        use byteview::ByteView;

        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let job_id = record.job_id;
        transition_external_to_running(&storage, job_id, token, None, 6)
            .await
            .unwrap();

        // Poison the workspace listing: an undecodable head row fails the scan.
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: ByteView::from(b"ws-test/poison".to_vec()),
                value: ByteView::from(vec![0xFF]),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::NotFound);
        let mut spec = execution_spec();
        spec.output_prefixes = vec!["poison".to_string()];
        finalize_attempt(
            &ctx,
            job_id,
            token,
            &backend,
            &fence(&attempt),
            &spec,
            "ws-test",
            Ok(AttemptStatus {
                phase: AttemptPhase::Exited { code: 0 },
                backend_ref: "c1".to_string(),
                started_at_ms: Some(1),
                finished_at_ms: Some(2),
            }),
        )
        .await;

        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Indeterminate);
        assert!(stored.result.is_none(), "no false-empty manifest recorded");
    }

    #[tokio::test]
    async fn finalize_renews_lease() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut ctx = context(storage.clone());
        Arc::get_mut(&mut ctx).unwrap().task_handle = None;
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let job_id = record.job_id;
        transition_external_to_running(&storage, job_id, token, None, 6)
            .await
            .unwrap();
        let backend = StubBackend::new(StubReconcile::NotFound);

        let task = tokio::spawn(supervise_and_finalize(
            ctx,
            job_id,
            token,
            backend.clone(),
            fence(&attempt),
            execution_spec(),
            "ws-test".to_string(),
            CancellationToken::new(),
        ));
        backend.logs_started.notified().await;
        let before = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap()
            .claim
            .unwrap()
            .lease_expires_at_ms;

        tokio::time::pause();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(JOB_HEARTBEAT_MS)).await;
        tokio::task::yield_now().await;
        tokio::time::resume();
        let mut after = before;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            after = read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .unwrap()
                .claim
                .unwrap()
                .lease_expires_at_ms;
            if after > before {
                break;
            }
        }
        assert!(after > before);

        backend.logs_release.notify_one();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_beats_success() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut ctx = context(storage.clone());
        Arc::get_mut(&mut ctx).unwrap().task_handle = None;
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let job_id = record.job_id;
        transition_external_to_running(&storage, job_id, token, None, 6)
            .await
            .unwrap();
        let backend = StubBackend::new(StubReconcile::NotFound);

        let task = tokio::spawn(supervise_and_finalize(
            ctx,
            job_id,
            token,
            backend.clone(),
            fence(&attempt),
            execution_spec(),
            "ws-test".to_string(),
            CancellationToken::new(),
        ));
        backend.logs_started.notified().await;
        set_cancel_requested(&storage, job_id, 7).await.unwrap();
        backend.logs_release.notify_one();
        task.await.unwrap();

        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Cancelled);
    }

    #[tokio::test]
    async fn reconcile_skips_submit() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut ctx = context(storage.clone());
        Arc::get_mut(&mut ctx).unwrap().task_handle = None;
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let job_id = record.job_id;
        set_cancel_requested(&storage, job_id, 6).await.unwrap();
        let backend = StubBackend::new(StubReconcile::NotFound);

        reconcile::resume_attempt(
            ctx,
            job_id,
            token,
            backend.clone(),
            fence(&attempt),
            AttemptPhase::Submitted,
            execution_spec(),
            "ws-test".to_string(),
            record,
            CancellationToken::new(),
        )
        .await;

        assert!(backend.submits.lock().unwrap().is_empty());
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Cancelled);
    }

    // Absence after intent retries the same name and retains the lineage.
    #[tokio::test]
    async fn absent_retries_same() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::NotFound);

        recover_failed_submit(
            &ctx,
            record.job_id,
            token,
            &backend,
            &fence(&attempt),
            &execution_spec(),
            "ws-test",
            &CancellationToken::new(),
            BackendError::Unavailable("io fault".to_string()),
        )
        .await;

        let stored = read_job_record(&storage, record.job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Indeterminate);
        assert_eq!(stored.attempts, 0);
        assert!(stored.attempt_intent.is_some());
    }

    // A re-driven crate job must return the already-written resource instead of minting a
    // second document. Without the durable-status early return it would fall through and
    // fail here on the missing net handle.
    #[tokio::test]
    async fn crate_write_idempotent() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let (record, token, _attempt) = ready_with_intent(&storage).await;
        let job_id = record.job_id;

        put_run_crate_status(
            &storage,
            job_id,
            &aruna_core::structs::RunCrateStatus::Written {
                resource: "already-there".to_string(),
            },
        )
        .await
        .unwrap();

        let ctx = JobContext {
            driver: context(storage.clone()),
            claim_token: token,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&record.progress),
        };
        let outcome = super::run_crate::run_write_run_crate(&ctx, job_id).await;
        match outcome {
            JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource }) => {
                assert_eq!(resource, "already-there");
            }
            _ => panic!("re-drive must return the already-written resource"),
        }
    }
}
