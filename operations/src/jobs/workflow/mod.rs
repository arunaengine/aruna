pub mod reconcile;
pub mod run_crate;
pub mod workspace;

use std::sync::Arc;
use std::time::Duration;

use aruna_compute::backend::{BackendError, ExecutorBackend, ExecutorKind};
use aruna_compute::logs::{LogTails, NullSink};
use aruna_compute::spec::{
    AttemptRef, LogLimits, ResourceRequest, Secret, TaskSpec, WorkspaceBinding,
};
use aruna_compute::status::{AttemptPhase, AttemptStatus, CancelEvidence, ReconcileOutcome};
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
    JobMutationError, cancel_execution, cancel_running_job, complete_job, fail_execution,
    mark_indeterminate, read_job_record, record_attempt_intent, renew_lease,
    requeue_before_attempt, set_workspace_bucket, transition_external_to_running,
    transition_to_cancelling, transition_to_preparing, transition_to_ready,
};
use super::submit::schedule_job_drain_effect;
use crate::driver::DriverContext;
use workspace::{
    collect_outputs, ensure_workspace_bucket, mint_workspace_credential, stage_inputs,
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
        let credential =
            match prepare_workspace(&context, &spec, &record, node_id, &bucket, token).await {
                Ok(credential) => credential,
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
        let task_spec = build_task_spec(&context, &spec, &attempt, &credential, &bucket, node_id);

        // Write-ahead the attempt intent BEFORE submit so a lost attempt is adoptable.
        let intent = AttemptIntent {
            attempt_no,
            external_name: attempt.external_name(),
            executor_kind: backend.kind().as_wire(),
        };
        if record_attempt_intent(storage, job_id, token, intent, unix_timestamp_millis())
            .await
            .is_err()
        {
            return None;
        }

        if let Err(error) = backend.submit(&task_spec).await {
            return Some(Err((
                backend, attempt, task_spec, spec, bucket, cancel, error,
            )));
        }

        // Ready -> Running only after the backend accepted the fenced attempt.
        if transition_external_to_running(storage, job_id, token, unix_timestamp_millis())
            .await
            .is_err()
        {
            return None;
        }

        Some(Ok((backend, attempt, spec, bucket, cancel)))
    };
    tokio::pin!(prepare_and_submit);

    let prepared = tokio::select! {
        result = &mut prepare_and_submit => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            result
        }
        _ = &mut heartbeat => None,
    };
    let Some(prepared) = prepared else {
        return;
    };
    let (backend, attempt, spec, bucket, cancel) = match prepared {
        Ok(prepared) => prepared,
        Err((backend, attempt, task_spec, spec, bucket, cancel, error)) => {
            recover_failed_submit(
                &context, job_id, token, &backend, &attempt, &task_spec, &spec, &bucket, &record,
                cancel, error,
            )
            .await;
            return;
        }
    };

    supervise_and_finalize(
        context.clone(),
        job_id,
        token,
        backend,
        attempt,
        spec,
        bucket,
        cancel,
    )
    .await;
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
) -> Result<workspace::WorkspaceCredential, JobError> {
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
    mint_workspace_credential(context, spec, record, node_id, bucket).await
}

fn build_task_spec(
    context: &DriverContext,
    spec: &ExecutionSpec,
    attempt: &AttemptRef,
    credential: &workspace::WorkspaceCredential,
    bucket: &str,
    _node_id: NodeId,
) -> TaskSpec {
    let mut env = spec.env.clone();
    env.insert("ARUNA_WORKSPACE_BUCKET".to_string(), bucket.to_string());
    let mut secret_env = std::collections::BTreeMap::new();
    secret_env.insert(
        "AWS_ACCESS_KEY_ID".to_string(),
        Secret::new(credential.access_key.clone()),
    );
    secret_env.insert(
        "AWS_SECRET_ACCESS_KEY".to_string(),
        Secret::new(credential.secret.clone()),
    );
    let workspace_binding = context.compute_handle.as_ref().and_then(|registry| {
        let endpoint = registry.workspace_endpoint();
        endpoint.endpoint.clone().map(|url| WorkspaceBinding {
            s3_endpoint: url,
            bucket_name: bucket.to_string(),
            region: endpoint.region.clone(),
        })
    });
    let resources = ResourceRequest {
        cpu_cores: spec.resources.cpu_cores,
        ram_bytes: spec.resources.ram_bytes,
        disk_bytes: None,
        max_walltime: spec.resources.max_walltime_ms.map(Duration::from_millis),
        preemptible: false,
        backend_extensions: std::collections::BTreeMap::new(),
    };
    TaskSpec {
        attempt: attempt.clone(),
        image: spec.image.clone(),
        entrypoint: spec.entrypoint.clone(),
        command: spec.command.clone(),
        workdir: None,
        env,
        secret_env,
        resources,
        workspace: workspace_binding,
        log_limits: Default::default(),
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
    attempt: &AttemptRef,
    task_spec: &TaskSpec,
    spec: &ExecutionSpec,
    bucket: &str,
    record: &JobRecord,
    cancel: CancellationToken,
    error: BackendError,
) {
    let storage = &context.storage_handle;
    match backend.reconcile(attempt).await {
        // Confirmed absent: the submit failed before a container could exist, so
        // the pre-submit routing (requeue with backoff, or terminal fail) is safe.
        ReconcileOutcome::NotFound => {
            let job_error = if error.retryable() {
                JobError::retryable(format!("submit failed: {error}"))
            } else {
                JobError::permanent(format!("submit failed: {error}"))
            };
            requeue_or_fail_pre_submit(context, job_id, token, record, job_error).await;
        }
        // The attempt already finished: commit its evidence.
        ReconcileOutcome::Found(status) if status.is_terminal() => {
            if transition_external_to_running(storage, job_id, token, unix_timestamp_millis())
                .await
                .is_err()
            {
                return;
            }
            finalize_attempt(
                context,
                job_id,
                token,
                backend,
                attempt,
                spec,
                bucket,
                Ok(status),
            )
            .await;
        }
        // The container exists and is live: adopt it, never launch a second one.
        // A created-but-never-started container is completed in place; the name
        // collision makes the re-submit idempotent.
        ReconcileOutcome::Found(status) => {
            if matches!(status.phase, AttemptPhase::Submitted)
                && backend.submit(task_spec).await.is_err()
            {
                park_failed_submit(context, job_id, token, &error).await;
                return;
            }
            if transition_external_to_running(storage, job_id, token, unix_timestamp_millis())
                .await
                .is_err()
            {
                return;
            }
            supervise_and_finalize(
                context.clone(),
                job_id,
                token,
                backend.clone(),
                attempt.clone(),
                spec.clone(),
                bucket.to_string(),
                cancel,
            )
            .await;
        }
        // Backend unobservable: park; the retained intent keeps the attempt
        // adoptable when the lease sweep routes the job to the reconciler.
        ReconcileOutcome::Unavailable(_) => {
            park_failed_submit(context, job_id, token, &error).await;
        }
    }
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
    attempt: AttemptRef,
    spec: ExecutionSpec,
    bucket: String,
    cancel: CancellationToken,
) {
    let storage = context.storage_handle.clone();
    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(execution_heartbeat(
        storage.clone(),
        job_id,
        token,
        cancel.clone(),
        stop.clone(),
    ));
    tokio::pin!(heartbeat);

    let wait = backend.wait(&attempt, &cancel);
    tokio::pin!(wait);

    let outcome = tokio::select! {
        result = &mut wait => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            Some(result)
        }
        // Heartbeat returned first: the claim was lost to an adopter. Abandon
        // without writing, exactly like the in-process zombie guard.
        _ = &mut heartbeat => None,
    };

    let Some(result) = outcome else {
        info!(job_id = %job_id, "Execution supervisor superseded; abandoning");
        return;
    };

    finalize_attempt(
        &context, job_id, token, &backend, &attempt, &spec, &bucket, result,
    )
    .await;
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
    attempt: &AttemptRef,
    spec: &ExecutionSpec,
    bucket: &str,
    result: Result<AttemptStatus, aruna_compute::backend::BackendError>,
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

    if cancel_requested {
        finalize_cancel(context, job_id, token, backend, attempt, spec, bucket).await;
        return;
    }

    match status.phase {
        AttemptPhase::Exited { code: 0 } => {
            let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await else {
                return;
            };
            let Some(logs) = capture_or_park(context, job_id, token, backend, attempt).await else {
                return;
            };
            let result = execution_result_for(bucket, Some(0), outputs, logs);
            let record = terminal_complete(storage, job_id, token, result).await;
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
        }
        AttemptPhase::Exited { code } => {
            let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await else {
                return;
            };
            let Some(logs) = capture_or_park(context, job_id, token, backend, attempt).await else {
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
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
        }
        AttemptPhase::Failed { reason } => {
            let Some(logs) = capture_or_park(context, job_id, token, backend, attempt).await else {
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
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
        }
        AttemptPhase::Cancelled => {
            finalize_cancel(context, job_id, token, backend, attempt, spec, bucket).await;
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
    attempt: &AttemptRef,
    spec: &ExecutionSpec,
    bucket: &str,
) {
    let storage = &context.storage_handle;
    // Running -> Cancelling (idempotent: a re-entry may already be Cancelling).
    let _ = transition_to_cancelling(storage, job_id, token, unix_timestamp_millis()).await;
    let evidence = backend.cancel(attempt).await;
    match evidence {
        Ok(CancelEvidence::Stopped(status)) => {
            let Some(logs) = capture_or_park(context, job_id, token, backend, attempt).await else {
                return;
            };
            let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await else {
                return;
            };
            let code = match status.phase {
                AttemptPhase::Exited { code } => Some(code),
                _ => None,
            };
            let result = execution_result_for(bucket, code, outputs, logs);
            let record = terminal_cancel(storage, job_id, token, result).await;
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
        }
        Ok(CancelEvidence::AlreadyGone) => {
            let Some(outputs) = collect_or_park(context, job_id, token, spec, bucket).await else {
                return;
            };
            let result = execution_result_for(bucket, None, outputs, LogTails::default());
            let record = terminal_cancel(storage, job_id, token, result).await;
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
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

/// Remove the container after terminal evidence is durable, then wake the drain
/// for the run-crate obligation persisted with terminalization.
async fn cleanup_and_crate(
    context: &DriverContext,
    job_id: JobId,
    backend: &Arc<dyn ExecutorBackend>,
    attempt: &AttemptRef,
    record: Option<JobRecord>,
) {
    // Only act on a terminal record WE wrote (a lost race returns None).
    let Some(record) = record else { return };
    if let Err(error) = backend.cleanup(attempt).await {
        warn!(job_id = %job_id, error = %error, "Container cleanup failed");
    }
    let _ = record;
    finalize_followups(context, job_id).await;
}

/// Wake the drain for the follow-on `WriteRunCrate` job persisted with terminalization.
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

/// Inventory the declared outputs, or park the job `Indeterminate` for a retry:
/// a transient listing failure must never terminalize the job with a
/// false-empty output manifest.
async fn collect_or_park(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Option<Vec<OutputObject>> {
    match collect_outputs(context, spec, bucket).await {
        Ok(outputs) => Some(outputs),
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
    attempt: &AttemptRef,
) -> Option<LogTails> {
    let default_limits = LogLimits::default();
    let limits = LogLimits {
        max_bytes_per_stream: default_limits.inline_tail_bytes,
        ..default_limits
    };
    match backend.fetch_logs(attempt, &limits, &NullSink).await {
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
    use crate::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
    use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, put_run_crate_status};
    use aruna_compute::logs::{LogSink, LogTails};
    use aruna_compute::spec::LogLimits;
    use aruna_core::structs::{ComputeResources, JobState, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use aruna_tasks::TaskHandle;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use ulid::Ulid;

    enum StubReconcile {
        NotFound,
        Unavailable,
    }

    struct StubBackend {
        reconcile: StubReconcile,
        submits: Mutex<Vec<String>>,
    }

    impl StubBackend {
        fn new(reconcile: StubReconcile) -> Arc<Self> {
            Arc::new(Self {
                reconcile,
                submits: Mutex::new(Vec::new()),
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
        async fn submit(&self, spec: &TaskSpec) -> Result<AttemptStatus, BackendError> {
            self.submits
                .lock()
                .unwrap()
                .push(spec.attempt.external_name());
            Err(BackendError::Unavailable("stub submit".to_string()))
        }
        async fn status(&self, _attempt: &AttemptRef) -> Result<AttemptStatus, BackendError> {
            Err(BackendError::Unavailable("stub status".to_string()))
        }
        async fn cancel(&self, _attempt: &AttemptRef) -> Result<CancelEvidence, BackendError> {
            Ok(CancelEvidence::AlreadyGone)
        }
        async fn fetch_logs(
            &self,
            _attempt: &AttemptRef,
            _limits: &LogLimits,
            _sink: &dyn LogSink,
        ) -> Result<LogTails, BackendError> {
            unimplemented!()
        }
        async fn reconcile(&self, _attempt: &AttemptRef) -> ReconcileOutcome {
            match self.reconcile {
                StubReconcile::NotFound => ReconcileOutcome::NotFound,
                StubReconcile::Unavailable => {
                    ReconcileOutcome::Unavailable(BackendError::Unavailable("down".to_string()))
                }
            }
        }
        async fn cleanup(&self, _attempt: &AttemptRef) -> Result<(), BackendError> {
            Ok(())
        }
    }

    fn node_id(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
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
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            env: Default::default(),
            resources: ComputeResources {
                cpu_cores: None,
                ram_bytes: None,
                max_walltime_ms: None,
            },
            executor_constraint: None,
            inputs: Vec::new(),
            output_prefixes: Vec::new(),
        }
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
        };
        let record = record_attempt_intent(storage, job_id, token, intent, 5)
            .await
            .unwrap();
        (record, token, attempt)
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
        let task_spec = TaskSpec::new(attempt.clone(), "alpine:3");

        recover_failed_submit(
            &ctx,
            record.job_id,
            token,
            &backend,
            &attempt,
            &task_spec,
            &execution_spec(),
            "ws-test",
            &record,
            CancellationToken::new(),
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
        transition_external_to_running(&storage, job_id, token, 6)
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
        finalize_attempt(
            &ctx,
            job_id,
            token,
            &backend,
            &attempt,
            &execution_spec(),
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

    // A submit error with the container confirmed absent keeps the pre-submit
    // routing: requeue with backoff, intent cleared, attempt charged.
    #[tokio::test]
    async fn absent_submit_requeues() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let (record, token, attempt) = ready_with_intent(&storage).await;
        let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::NotFound);
        let task_spec = TaskSpec::new(attempt.clone(), "alpine:3");

        recover_failed_submit(
            &ctx,
            record.job_id,
            token,
            &backend,
            &attempt,
            &task_spec,
            &execution_spec(),
            "ws-test",
            &record,
            CancellationToken::new(),
            BackendError::Unavailable("io fault".to_string()),
        )
        .await;

        let stored = read_job_record(&storage, record.job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued);
        assert_eq!(stored.attempts, 1);
        assert!(stored.attempt_intent.is_none());
    }

    // A re-driven crate job must return the already-written resource instead of minting a
    // second document. Without the durable-status early return it would fall through and
    // fail here on the missing net handle.
    #[tokio::test]
    async fn crate_write_idempotent() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let (record, _token, _attempt) = ready_with_intent(&storage).await;
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
