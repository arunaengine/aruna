use aruna_compute::backend::{BackendError, ExecutorKind};
use aruna_compute::spec::AttemptRef;
use aruna_core::structs::{AttemptIntent, JobError, JobErrorKind, JobId, JobResultPayload};

use super::super::executor::{JobContext, JobRunOutcome};
use crate::driver::drive;
use crate::s3::revoke_user_access::{RevokeUserAccessError, RevokeUserAccessOperation};

pub async fn run_terminal_cleanup(
    ctx: &JobContext,
    for_job: JobId,
    intent: Option<&AttemptIntent>,
    access_key: &str,
) -> JobRunOutcome {
    let revoke_error = revoke_credential(ctx, access_key).await.err();
    let backend_error = cleanup_attempt(ctx, for_job, intent).await.err();
    match cleanup_error(revoke_error, backend_error) {
        Some(error) => JobRunOutcome::Failed(error),
        None => JobRunOutcome::Succeeded(JobResultPayload::Cleanup),
    }
}

async fn revoke_credential(ctx: &JobContext, access_key: &str) -> Result<(), JobError> {
    match drive(
        RevokeUserAccessOperation::new(access_key.to_string()),
        &ctx.driver,
    )
    .await
    {
        Ok(Some(Ok(_)))
        | Ok(None)
        | Ok(Some(Err(RevokeUserAccessError::NotFound)))
        | Err(RevokeUserAccessError::NotFound) => Ok(()),
        Ok(Some(Err(error))) | Err(error) => Err(revoke_error(error)),
    }
}

async fn cleanup_attempt(
    ctx: &JobContext,
    for_job: JobId,
    intent: Option<&AttemptIntent>,
) -> Result<(), JobError> {
    let Some(intent) = intent else {
        return Ok(());
    };
    let attempt = AttemptRef::new(for_job.to_string().to_lowercase(), intent.attempt_no);
    if attempt.external_name() != intent.external_name {
        return Err(JobError::permanent("cleanup attempt identity mismatch"));
    }
    let Some(registry) = ctx.driver.compute_handle.as_ref() else {
        return Err(JobError::retryable("cleanup needs a compute backend"));
    };
    let kind = ExecutorKind::from_wire(&intent.executor_kind);
    let Some(backend) = registry.get(&kind) else {
        return Err(JobError::retryable(format!(
            "cleanup backend {} is unavailable",
            intent.executor_kind
        )));
    };
    match backend.cleanup(&attempt).await {
        Ok(()) | Err(BackendError::NotFound(_)) => Ok(()),
        Err(error) if error.retryable() => Err(JobError::retryable(format!(
            "backend cleanup failed: {error}"
        ))),
        Err(error) => Err(JobError::permanent(format!(
            "backend cleanup failed: {error}"
        ))),
    }
}

fn revoke_error(error: RevokeUserAccessError) -> JobError {
    match &error {
        RevokeUserAccessError::StorageError(_) => {
            JobError::retryable(format!("workspace credential revoke failed: {error}"))
        }
        _ => JobError::permanent(format!("workspace credential revoke failed: {error}")),
    }
}

fn cleanup_error(first: Option<JobError>, second: Option<JobError>) -> Option<JobError> {
    let errors: Vec<_> = [first, second].into_iter().flatten().collect();
    if errors.is_empty() {
        return None;
    }
    let retryable = errors
        .iter()
        .any(|error| error.kind == JobErrorKind::Retryable);
    let message = errors
        .into_iter()
        .map(|error| error.message)
        .collect::<Vec<_>>()
        .join("; ");
    Some(if retryable {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use aruna_compute::ExecutorRegistry;
    use aruna_compute::backend::ExecutorBackend;
    use aruna_compute::logs::{LogSink, LogTails};
    use aruna_compute::spec::{LogLimits, TaskSpec};
    use aruna_compute::status::{AttemptStatus, CancelEvidence, ReconcileOutcome};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
    use aruna_core::structs::{JobProgress, RealmId, UserAccess};
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;
    use ulid::Ulid;

    use super::*;
    use crate::driver::DriverContext;
    use crate::jobs::executor::ProgressReporter;
    use crate::s3::get_user_access::GetUserAccessOperation;

    struct StubBackend {
        kind: ExecutorKind,
        results: Mutex<VecDeque<Result<(), BackendError>>>,
        calls: AtomicUsize,
    }

    impl StubBackend {
        fn new(kind: ExecutorKind, results: Vec<Result<(), BackendError>>) -> Arc<Self> {
            Arc::new(Self {
                kind,
                results: Mutex::new(results.into()),
                calls: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for StubBackend {
        fn kind(&self) -> ExecutorKind {
            self.kind.clone()
        }

        async fn health(&self) -> Result<(), BackendError> {
            Ok(())
        }

        async fn submit(
            &self,
            _spec: &TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn status(&self, _attempt: &AttemptRef) -> Result<AttemptStatus, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn cancel(&self, _attempt: &AttemptRef) -> Result<CancelEvidence, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn fetch_logs(
            &self,
            _attempt: &AttemptRef,
            _limits: &LogLimits,
            _sink: &dyn LogSink,
        ) -> Result<LogTails, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn fetch_output(
            &self,
            _attempt: &AttemptRef,
            _path: &str,
        ) -> Result<aruna_compute::backend::TaskOutput, BackendError> {
            Err(BackendError::InvalidSpec("no output".to_string()))
        }

        async fn reconcile(&self, _attempt: &AttemptRef) -> ReconcileOutcome {
            ReconcileOutcome::NotFound
        }

        async fn cleanup(&self, _attempt: &AttemptRef) -> Result<(), BackendError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.results.lock().unwrap().pop_front().unwrap_or(Ok(()))
        }
    }

    fn job_context(storage: StorageHandle, backends: &[Arc<StubBackend>]) -> JobContext {
        let mut registry = ExecutorRegistry::new();
        for backend in backends {
            registry.register(backend.clone());
        }
        JobContext {
            driver: Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: Some(Arc::new(registry)),
            }),
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&JobProgress::new("steps")),
        }
    }

    fn intent(job_id: JobId, kind: &str) -> AttemptIntent {
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), 1);
        AttemptIntent {
            attempt_no: 1,
            external_name: attempt.external_name(),
            executor_kind: kind.to_string(),
        }
    }

    async fn write_access(storage: &StorageHandle, access: &UserAccess) {
        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access.access_key.as_bytes().into(),
                value: access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    fn user_access(access_key: &str) -> UserAccess {
        UserAccess {
            access_key: access_key.to_string(),
            user_identity: UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            group_id: Ulid::from_bytes([3; 16]),
            secret: "secret".to_string(),
            expiry: SystemTime::now() + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: [4; 32],
            revoked_at: None,
        }
    }

    #[tokio::test]
    async fn revoke_failure_cleans() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let access_key = "malformed-access";
        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access_key.as_bytes().into(),
                value: vec![0xff].into(),
                txn_id: None,
            })
            .await;
        let backend = StubBackend::new(ExecutorKind::Docker, vec![Ok(())]);
        let ctx = job_context(storage, std::slice::from_ref(&backend));
        let job_id = JobId::from_bytes([5; 16]);

        let outcome =
            run_terminal_cleanup(&ctx, job_id, Some(&intent(job_id, "docker")), access_key).await;

        assert!(matches!(
            outcome,
            JobRunOutcome::Failed(JobError {
                kind: JobErrorKind::Permanent,
                ..
            })
        ));
        assert_eq!(backend.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn missing_revoke_succeeds() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = job_context(storage, &[]);

        let outcome =
            run_terminal_cleanup(&ctx, JobId::from_bytes([6; 16]), None, "missing-access").await;

        assert!(matches!(
            outcome,
            JobRunOutcome::Succeeded(JobResultPayload::Cleanup)
        ));
    }

    #[tokio::test]
    async fn revoked_revoke_stable() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut access = user_access("revoked-access");
        let revoked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        access.revoked_at = Some(revoked_at);
        write_access(&storage, &access).await;
        let ctx = job_context(storage, &[]);

        for _ in 0..2 {
            assert!(matches!(
                run_terminal_cleanup(&ctx, JobId::from_bytes([7; 16]), None, &access.access_key,)
                    .await,
                JobRunOutcome::Succeeded(JobResultPayload::Cleanup)
            ));
        }
        let stored = drive(GetUserAccessOperation::new(access.access_key), &ctx.driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stored.revoked_at, Some(revoked_at));
    }

    #[tokio::test]
    async fn cleanup_uses_kind() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let docker = StubBackend::new(ExecutorKind::Docker, vec![Ok(())]);
        let slurm = StubBackend::new(ExecutorKind::Slurm, vec![Ok(())]);
        let ctx = job_context(storage, &[docker.clone(), slurm.clone()]);
        let job_id = JobId::from_bytes([8; 16]);

        let outcome = run_terminal_cleanup(
            &ctx,
            job_id,
            Some(&intent(job_id, "slurm")),
            "missing-access",
        )
        .await;

        assert!(matches!(
            outcome,
            JobRunOutcome::Succeeded(JobResultPayload::Cleanup)
        ));
        assert_eq!(docker.calls.load(Ordering::Relaxed), 0);
        assert_eq!(slurm.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cleanup_retry_succeeds() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let backend = StubBackend::new(
            ExecutorKind::Docker,
            vec![Err(BackendError::Unavailable("down".to_string())), Ok(())],
        );
        let ctx = job_context(storage, std::slice::from_ref(&backend));
        let job_id = JobId::from_bytes([9; 16]);
        let intent = intent(job_id, "docker");

        assert!(matches!(
            run_terminal_cleanup(&ctx, job_id, Some(&intent), "missing-access").await,
            JobRunOutcome::Failed(JobError {
                kind: JobErrorKind::Retryable,
                ..
            })
        ));
        assert!(matches!(
            run_terminal_cleanup(&ctx, job_id, Some(&intent), "missing-access").await,
            JobRunOutcome::Succeeded(JobResultPayload::Cleanup)
        ));
        assert_eq!(backend.calls.load(Ordering::Relaxed), 2);
    }
}
