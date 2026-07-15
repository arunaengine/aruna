use std::sync::{Arc, Weak};

use aruna_compute::spec::AttemptRef;
use aruna_compute::status::{AttemptPhase, ReconcileOutcome};
use aruna_core::structs::{ExecutionSpec, JobError, JobPayload, JobRecord, JobState};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::reconcile::ExternalReconciler;
use super::super::runtime::JobsRuntime;
use super::super::store::{
    AdoptOutcome, adopt_external_attempt, handoff_external_attempt, mark_indeterminate,
    requeue_before_attempt, transition_external_to_running,
};
use super::workspace::mint_workspace_credential;
use super::{build_task_spec, finalize_attempt, resolve_backend, supervise_and_finalize};
use crate::driver::DriverContext;

/// The real Stage-0 reconcile seam: a lost external attempt is adopted by name and
/// resolved by evidence (adopt-and-resume, terminalize, or Indeterminate). It never
/// submits a new attempt name, so a container is never double-run.
pub struct ComputeReconciler {
    context: Arc<DriverContext>,
    runtime: Weak<JobsRuntime>,
}

impl ComputeReconciler {
    pub fn new(context: Arc<DriverContext>, runtime: Weak<JobsRuntime>) -> Arc<Self> {
        Arc::new(Self { context, runtime })
    }
}

#[async_trait::async_trait]
impl ExternalReconciler for ComputeReconciler {
    async fn reconcile_lost_attempt(&self, storage: &StorageHandle, record: JobRecord) {
        let job_id = record.job_id;
        let JobPayload::Execution(spec) = record.payload.clone() else {
            return;
        };

        let backend = match resolve_backend(&self.context, &spec) {
            Ok(backend) => backend,
            Err(error) => {
                warn!(job_id = %job_id, error = ?error, "Reconcile has no backend for job");
                return;
            }
        };

        // Take over with a fresh claim token, but only if the old holder is really gone.
        let adopted = match adopt_external_attempt(
            storage,
            job_id,
            holder(&self.context),
            unix_timestamp_millis(),
        )
        .await
        {
            Ok(AdoptOutcome::Adopted(record)) => record,
            Ok(AdoptOutcome::Skipped) => {
                info!(job_id = %job_id, "Attempt is terminal or still held; not adopting");
                return;
            }
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Adoption failed");
                return;
            }
        };
        let Some(token) = adopted.claim.as_ref().map(|claim| claim.claim_token) else {
            return;
        };

        // No durable attempt intent means the attempt was never submitted; re-drive
        // from scratch (no container can exist).
        let Some(intent) = adopted.attempt_intent.clone() else {
            let _ = requeue_before_attempt(
                storage,
                job_id,
                token,
                unix_timestamp_millis(),
                JobError::retryable("lost before attempt submit"),
            )
            .await;
            return;
        };

        let bucket = adopted
            .workspace_bucket
            .clone()
            .unwrap_or_else(|| JobRecord::workspace_bucket_name(job_id));
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), intent.attempt_no);

        let outcome = backend.reconcile(&attempt).await;
        if matches!(outcome, ReconcileOutcome::Found(_))
            && matches!(adopted.state, JobState::Indeterminate | JobState::Ready)
            && transition_external_to_running(storage, job_id, token, unix_timestamp_millis())
                .await
                .is_err()
        {
            return;
        }

        match outcome {
            ReconcileOutcome::Found(status) if !status.is_terminal() => {
                self.resume(
                    job_id,
                    token,
                    backend,
                    attempt,
                    status.phase,
                    spec,
                    bucket,
                    adopted,
                )
                .await;
            }
            // Terminal evidence: commit it now (correlates the cancel intent).
            ReconcileOutcome::Found(mut status) => {
                let max_walltime_ms = spec
                    .resources
                    .max_walltime_ms
                    .unwrap_or(24 * 60 * 60 * 1_000);
                if status.started_at_ms.zip(status.finished_at_ms).is_some_and(
                    |(started, finished)| finished.saturating_sub(started) > max_walltime_ms,
                ) {
                    status.phase = AttemptPhase::Failed {
                        reason: "walltime limit exceeded while unobserved".to_string(),
                    };
                }
                finalize_attempt(
                    &self.context,
                    job_id,
                    token,
                    &backend,
                    &attempt,
                    &spec,
                    &bucket,
                    Ok(status),
                )
                .await;
            }
            // Post-submit absence is ambiguous: never requeue, park in Indeterminate.
            ReconcileOutcome::NotFound => {
                info!(job_id = %job_id, "Adopted attempt not found; parking Indeterminate");
                let _ = mark_indeterminate(
                    storage,
                    job_id,
                    token,
                    JobError::retryable("adopted attempt not found"),
                    unix_timestamp_millis(),
                )
                .await;
            }
            ReconcileOutcome::Unavailable(error) => {
                let _ = mark_indeterminate(
                    storage,
                    job_id,
                    token,
                    JobError::retryable(format!("backend unavailable: {error}")),
                    unix_timestamp_millis(),
                )
                .await;
            }
        }
    }
}

impl ComputeReconciler {
    /// Adopt a live container without blocking the drain loop, registering its
    /// supervisor with `JobsRuntime` for caps, cancellation, and shutdown.
    #[allow(clippy::too_many_arguments)]
    async fn resume(
        &self,
        job_id: aruna_core::structs::JobId,
        token: ulid::Ulid,
        backend: Arc<dyn aruna_compute::backend::ExecutorBackend>,
        attempt: AttemptRef,
        phase: AttemptPhase,
        spec: ExecutionSpec,
        bucket: String,
        record: JobRecord,
    ) {
        let context = self.context.clone();
        let supervisor_record = record.clone();
        let supervisor = move |cancel| {
            resume_attempt(
                context,
                job_id,
                token,
                backend,
                attempt,
                phase,
                spec,
                bucket,
                supervisor_record,
                cancel,
            )
        };
        if !self.runtime.upgrade().is_some_and(|runtime| {
            runtime.spawn_recovered(self.context.clone(), &record, supervisor)
        }) && let Err(error) = handoff_external_attempt(
            &self.context.storage_handle,
            job_id,
            token,
            unix_timestamp_millis(),
        )
        .await
        {
            warn!(job_id = %job_id, error = %error, "Failed to hand off unregistered recovered attempt");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn resume_attempt(
    context: Arc<DriverContext>,
    job_id: aruna_core::structs::JobId,
    token: ulid::Ulid,
    backend: Arc<dyn aruna_compute::backend::ExecutorBackend>,
    attempt: AttemptRef,
    phase: AttemptPhase,
    spec: ExecutionSpec,
    bucket: String,
    record: JobRecord,
    cancel: CancellationToken,
) {
    if matches!(phase, AttemptPhase::Submitted) {
        let node_id = holder(&context);
        let credential =
            match mint_workspace_credential(&context, &spec, &record, node_id, &bucket).await {
                Ok(credential) => credential,
                Err(error) => {
                    let _ = mark_indeterminate(
                        &context.storage_handle,
                        job_id,
                        token,
                        error,
                        unix_timestamp_millis(),
                    )
                    .await;
                    return;
                }
            };
        let task_spec = build_task_spec(&context, &spec, &attempt, &credential, &bucket, node_id);
        if let Err(error) = backend.submit(&task_spec).await {
            let _ = mark_indeterminate(
                &context.storage_handle,
                job_id,
                token,
                JobError::retryable(format!("resubmit failed: {error}")),
                unix_timestamp_millis(),
            )
            .await;
            return;
        }
    }
    supervise_and_finalize(
        context, job_id, token, backend, attempt, spec, bucket, cancel,
    )
    .await;
}

fn holder(context: &DriverContext) -> aruna_core::types::NodeId {
    context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[0u8; 32]).public())
}
