pub mod reconcile;
pub mod run_crate;
pub mod workspace;

use std::sync::Arc;
use std::time::Duration;

use aruna_compute::backend::{ExecutorBackend, ExecutorKind};
use aruna_compute::spec::{AttemptRef, ResourceRequest, Secret, TaskSpec, WorkspaceBinding};
use aruna_compute::status::{AttemptPhase, AttemptStatus, CancelEvidence};
use aruna_core::structs::{
    AttemptIntent, ExecutionSpec, JobError, JobId, JobPayload, JobRecord, JobResultPayload,
    OutputObject, run_crate_dedup_key,
};
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
use super::submit::{SubmitJobOperation, SubmitJobSpec};
use crate::driver::{DriverContext, drive};
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
        let _ = cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await;
        finalize_followups(&context, job_id).await;
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

    let bucket = JobRecord::workspace_bucket_name(job_id);
    let credential =
        match prepare_workspace(&context, &spec, &record, node_id, &bucket, token).await {
            Ok(credential) => credential,
            Err(error) => {
                requeue_or_fail_pre_submit(&context, job_id, token, &record, error).await;
                return;
            }
        };

    // Preparing -> Ready.
    if transition_to_ready(storage, job_id, token, unix_timestamp_millis())
        .await
        .is_err()
    {
        return;
    }

    let attempt_no = record.attempts;
    let attempt = AttemptRef::new(job_id.to_string(), attempt_no);
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
        return;
    }

    match backend.submit(&task_spec).await {
        Ok(_) => {}
        Err(error) => {
            // The attempt never started; reconcile-by-name would confirm absence,
            // but Docker submit is name-idempotent so a bare error is safe to retry.
            let job_error = if error.retryable() {
                JobError::retryable(format!("submit failed: {error}"))
            } else {
                JobError::permanent(format!("submit failed: {error}"))
            };
            requeue_or_fail_pre_submit(&context, job_id, token, &record, job_error).await;
            return;
        }
    }

    // Ready -> Running only after the backend accepted the fenced attempt.
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
            let outputs = collect_or_empty(context, spec, bucket).await;
            let result = execution_result_for(bucket, Some(0), outputs);
            let record = terminal_complete(storage, job_id, token, result).await;
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
        }
        AttemptPhase::Exited { code } => {
            let outputs = collect_or_empty(context, spec, bucket).await;
            let result = execution_result_for(bucket, Some(code), outputs);
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
            let result = execution_result_for(bucket, None, Vec::new());
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
    let outputs = collect_or_empty(context, spec, bucket).await;
    match evidence {
        Ok(CancelEvidence::Stopped(status)) => {
            let code = match status.phase {
                AttemptPhase::Exited { code } => Some(code),
                _ => None,
            };
            let result = execution_result_for(bucket, code, outputs);
            let record = terminal_cancel(storage, job_id, token, result).await;
            cleanup_and_crate(context, job_id, backend, attempt, record).await;
        }
        Ok(CancelEvidence::AlreadyGone) => {
            let result = execution_result_for(bucket, None, outputs);
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

/// Remove the container after terminal evidence is durable, then submit the
/// run-crate obligation. A failed cleanup or crate submit never affects the job.
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

/// Submit the follow-on `WriteRunCrate` internal job (dedup `run-crate/{JobId}`).
async fn finalize_followups(context: &DriverContext, job_id: JobId) {
    let owner_node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .unwrap_or_else(default_node_id);
    let created_by = match read_job_record(&context.storage_handle, job_id, None).await {
        Ok(Some(record)) => record.created_by,
        _ => return,
    };
    let spec = SubmitJobSpec {
        payload: JobPayload::WriteRunCrate { for_job: job_id },
        created_by,
        owner_node_id,
        dedup_key: Some(run_crate_dedup_key(job_id)),
        now_ms: unix_timestamp_millis(),
    };
    if let Err(error) = drive(SubmitJobOperation::new(spec), context).await {
        warn!(job_id = %job_id, error = %error, "Failed to submit run-crate obligation");
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

async fn collect_or_empty(
    context: &DriverContext,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Vec<OutputObject> {
    match collect_outputs(context, spec, bucket).await {
        Ok(outputs) => outputs,
        Err(error) => {
            warn!(bucket = %bucket, error = ?error, "Output inventory failed");
            Vec::new()
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
    execution_result_for(&bucket, exit_code, outputs)
}

fn execution_result_for(
    bucket: &str,
    exit_code: Option<i32>,
    outputs: Vec<OutputObject>,
) -> JobResultPayload {
    JobResultPayload::Execution {
        exit_code,
        workspace_bucket: bucket.to_string(),
        outputs,
    }
}

fn default_node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[0u8; 32]).public()
}
