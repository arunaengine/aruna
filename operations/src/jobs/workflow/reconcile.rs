use std::sync::Arc;

use aruna_compute::spec::AttemptRef;
use aruna_compute::status::{AttemptPhase, ReconcileOutcome};
use aruna_core::structs::{ExecutionSpec, JobError, JobPayload, JobRecord, JobState};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::reconcile::ExternalReconciler;
use super::super::store::{
    adopt_external_attempt, mark_indeterminate, requeue_before_attempt,
    transition_external_to_running,
};
use super::workspace::mint_workspace_credential;
use super::{build_task_spec, finalize_attempt, resolve_backend, supervise_and_finalize};
use crate::driver::DriverContext;

/// The real Stage-0 reconcile seam: a lost external attempt is adopted by name and
/// resolved by evidence (adopt-and-resume, terminalize, or Indeterminate). It never
/// submits a new attempt name, so a container is never double-run.
pub struct ComputeReconciler {
    context: Arc<DriverContext>,
}

impl ComputeReconciler {
    pub fn new(context: Arc<DriverContext>) -> Arc<Self> {
        Arc::new(Self { context })
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

        // Take over with a fresh claim token; the old holder is provably dead.
        let adopted = match adopt_external_attempt(
            storage,
            job_id,
            holder(&self.context),
            unix_timestamp_millis(),
        )
        .await
        {
            Ok(record) => record,
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
    /// Adopt a live container and resume supervision on a detached task so the drain
    /// loop is never blocked.
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
        if matches!(phase, AttemptPhase::Submitted) {
            let node_id = holder(&self.context);
            let credential =
                match mint_workspace_credential(&self.context, &spec, &record, node_id, &bucket)
                    .await
                {
                    Ok(credential) => credential,
                    Err(error) => {
                        let _ = mark_indeterminate(
                            &self.context.storage_handle,
                            job_id,
                            token,
                            error,
                            unix_timestamp_millis(),
                        )
                        .await;
                        return;
                    }
                };
            let task_spec = build_task_spec(
                &self.context,
                &spec,
                &attempt,
                &credential,
                &bucket,
                node_id,
            );
            if let Err(error) = backend.submit(&task_spec).await {
                let _ = mark_indeterminate(
                    &self.context.storage_handle,
                    job_id,
                    token,
                    JobError::retryable(format!("resubmit failed: {error}")),
                    unix_timestamp_millis(),
                )
                .await;
                return;
            }
        }
        let context = self.context.clone();
        tokio::spawn(async move {
            supervise_and_finalize(
                context,
                job_id,
                token,
                backend,
                attempt,
                spec,
                bucket,
                CancellationToken::new(),
            )
            .await;
        });
    }
}

fn holder(context: &DriverContext) -> aruna_core::types::NodeId {
    context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[0u8; 32]).public())
}
