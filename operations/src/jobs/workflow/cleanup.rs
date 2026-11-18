//! Runs the terminal cleanup job that revokes the run credential and tombstones the attempt.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeRequest};
use aruna_core::compute::{BackendError, ExecutorKind, FenceContext, TombstoneSpec};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    AttemptControl, AttemptIntent, JobError, JobErrorKind, JobId, JobResultPayload,
};
use aruna_core::structs::storage::blob::UserAccess;
use ulid::Ulid;

use super::super::executor::JobRunOutcome;
use super::super::store::JobMutationError;
use super::super::store::job_calls::{authorize_cleanup, record_attempt_tombstone};
use super::compute::{attempt_call, compute_answer};
use crate::s3::access::revoke::{RevokeUserError, RevokeUserOperation};

#[derive(Debug, PartialEq)]
enum CleanupStep {
    Authorize,
    Fence(FenceContext),
    Cleanup(FenceContext),
    Tombstone(FenceContext),
    Record,
    Revoke,
}

/// Removes a finished attempt from its backend and revokes the run's credential. The revoke runs
/// even when the backend work failed, so one failure cannot strand a live workspace key.
#[derive(Debug, PartialEq)]
pub struct TerminalCleanupOperation {
    for_job: JobId,
    token: Ulid,
    intent: Option<AttemptIntent>,
    access_key: String,
    backend_error: Option<JobError>,
    step: CleanupStep,
}

impl TerminalCleanupOperation {
    pub fn new(
        for_job: JobId,
        token: Ulid,
        intent: Option<AttemptIntent>,
        access_key: &str,
    ) -> Self {
        Self {
            for_job,
            token,
            intent,
            access_key: access_key.to_string(),
            backend_error: None,
            step: CleanupStep::Authorize,
        }
    }

    fn revoke(&mut self) -> Next<()> {
        self.step = CleanupStep::Revoke;
        let revoke = RevokeUserOperation::new(self.access_key.clone());
        Next::Call(IoCall::child(revoke))
    }

    /// Keeps the failure of the backend work and goes on to the revoke.
    fn failed(&mut self, error: JobError) -> Next<()> {
        self.backend_error = Some(error);
        self.revoke()
    }

    fn backend_call(&self, fence: &FenceContext, request: ComputeRequest) -> Next<()> {
        let kind = self.intent.as_ref().map_or_else(
            || ExecutorKind::from_wire(""),
            |intent| ExecutorKind::from_wire(&intent.executor_kind),
        );
        Next::Call(attempt_call(&kind, fence, request))
    }

    /// The error of an answer that names no usable backend.
    fn no_backend(&self, answer: &ComputeAnswer) -> Option<JobError> {
        let kind = self
            .intent
            .as_ref()
            .map_or("", |intent| &intent.executor_kind);
        match answer {
            ComputeAnswer::NoCompute => {
                Some(JobError::retryable("cleanup needs a compute backend"))
            }
            ComputeAnswer::NoBackend => Some(JobError::retryable(format!(
                "cleanup backend {kind} is unavailable"
            ))),
            _ => None,
        }
    }

    fn authorized(&mut self, fence: Result<FenceContext, JobMutationError>) -> Next<()> {
        let fence = match fence {
            Ok(fence) => fence,
            Err(JobMutationError::NotFound) => {
                return self.failed(JobError::permanent("cleanup parent no longer exists"));
            }
            Err(error) => {
                let message = format!("cleanup fence claim failed: {error}");
                return self.failed(JobError::retryable(message));
            }
        };
        let expected = self
            .intent
            .as_ref()
            .map(|intent| intent.external_name.as_str());
        if expected != Some(fence.attempt.external_name().as_str()) {
            return self.failed(JobError::permanent("cleanup attempt identity mismatch"));
        }
        let call = self.backend_call(&fence, ComputeRequest::Fence);
        self.step = CleanupStep::Fence(fence);
        call
    }

    fn fenced(&mut self, fence: FenceContext, answer: ComputeAnswer) -> Next<()> {
        if let Some(error) = self.no_backend(&answer) {
            return self.failed(error);
        }
        match answer {
            ComputeAnswer::Done(Ok(())) => {
                let call = self.backend_call(&fence, ComputeRequest::Cleanup);
                self.step = CleanupStep::Cleanup(fence);
                call
            }
            ComputeAnswer::Done(Err(error)) => self.failed(JobError::retryable(format!(
                "backend fence failed: {error}"
            ))),
            _ => self.failed(unexpected_answer()),
        }
    }

    fn cleaned(&mut self, fence: FenceContext, answer: ComputeAnswer) -> Next<()> {
        if let Some(error) = self.no_backend(&answer) {
            return self.failed(error);
        }
        match answer {
            ComputeAnswer::Done(Ok(())) | ComputeAnswer::Done(Err(BackendError::NotFound(_))) => {
                let spec = TombstoneSpec { terminal_ref: None };
                let call = self.backend_call(&fence, ComputeRequest::Tombstone(spec));
                self.step = CleanupStep::Tombstone(fence);
                call
            }
            ComputeAnswer::Done(Err(error)) if error.retryable() => self.failed(
                JobError::retryable(format!("backend cleanup failed: {error}")),
            ),
            ComputeAnswer::Done(Err(error)) => self.failed(JobError::permanent(format!(
                "backend cleanup failed: {error}"
            ))),
            _ => self.failed(unexpected_answer()),
        }
    }

    fn tombstoned(&mut self, fence: FenceContext, answer: ComputeAnswer) -> Next<()> {
        if let Some(error) = self.no_backend(&answer) {
            return self.failed(error);
        }
        match answer {
            ComputeAnswer::Tombstone(Ok(tombstone)) => {
                self.step = CleanupStep::Record;
                Next::Call(IoCall::child(record_attempt_tombstone(
                    self.for_job,
                    self.token,
                    fence.attempt_epoch,
                    tombstone.backend_ref,
                )))
            }
            ComputeAnswer::Tombstone(Err(error)) => self.failed(JobError::retryable(format!(
                "backend tombstone failed: {error}"
            ))),
            _ => self.failed(unexpected_answer()),
        }
    }
}

fn unexpected_answer() -> JobError {
    JobError::retryable("the compute backend gave an unexpected answer")
}

impl Operation for TerminalCleanupOperation {
    type Output = ();
    type Error = JobError;

    fn first(&mut self) -> Result<Next<()>, JobError> {
        let Some(intent) = self.intent.clone() else {
            return Ok(self.revoke());
        };
        let authorize = authorize_cleanup(self.for_job, intent, self.token);
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobError> {
        match std::mem::replace(&mut self.step, CleanupStep::Revoke) {
            CleanupStep::Authorize => {
                let fence = result.child::<FenceContext, JobMutationError>()?;
                Ok(self.authorized(fence))
            }
            CleanupStep::Fence(fence) => {
                let answer = compute_answer(result)?;
                Ok(self.fenced(fence, answer))
            }
            CleanupStep::Cleanup(fence) => {
                let answer = compute_answer(result)?;
                Ok(self.cleaned(fence, answer))
            }
            CleanupStep::Tombstone(fence) => {
                let answer = compute_answer(result)?;
                Ok(self.tombstoned(fence, answer))
            }
            CleanupStep::Record => match result.child::<AttemptControl, JobMutationError>()? {
                Ok(_) => Ok(self.revoke()),
                Err(error) => Ok(self.failed(JobError::retryable(format!(
                    "tombstone record failed: {error}"
                )))),
            },
            CleanupStep::Revoke => {
                let revoke_error = match result.child::<UserAccess, RevokeUserError>()? {
                    Ok(_) | Err(RevokeUserError::NotFound) => None,
                    Err(error) => Some(revoke_error(error)),
                };
                match cleanup_error([revoke_error, self.backend_error.take()]) {
                    Some(error) => Err(error),
                    None => Ok(Next::Done(())),
                }
            }
        }
    }
}

/// How the job runtime takes the end of a cleanup run.
pub(crate) fn cleanup_outcome(result: Result<(), JobError>) -> JobRunOutcome {
    match result {
        Ok(()) => JobRunOutcome::Succeeded(JobResultPayload::Cleanup),
        Err(error) => JobRunOutcome::Failed(error),
    }
}

/// A live workspace key outlives a terminal job, so no revocation failure is
/// treated as permanent: the only proven-final case is `NotFound`, already Ok.
fn revoke_error(error: RevokeUserError) -> JobError {
    JobError::retryable(format!("workspace credential revoke failed: {error}"))
}

fn cleanup_error(errors: [Option<JobError>; 2]) -> Option<JobError> {
    let errors: Vec<_> = errors.into_iter().flatten().collect();
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
#[path = "cleanup_tests.rs"]
mod call_tests;

#[cfg(test)]
mod tests {
    use aruna_core::structs::storage::format::Compression;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use aruna_compute::ExecutorBackend;
    use aruna_compute::ExecutorRegistry;
    use aruna_core::UserId;
    use aruna_core::compute::{
        AttemptRef, AttemptStatus, CancelEvidence, FenceContext, LogLimits, LogTails, NOBODY,
        ReconcileEvidence, TaskOutput, TaskSpec, TombstoneEvidence, UserSpec,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, USER_ACCESS_KEYSPACE};
    use aruna_core::structs::execution::job::{
        ComputeResources, ExecutionSpec, JobClaim, JobPayload, JobProgress, JobRecord, JobState,
        WorkspaceMode,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{
        BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, UserAccess, VersionKey,
    };
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;
    use ulid::Ulid;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::executor::{JobContext, ProgressReporter};
    use crate::jobs::store::query::insert_job;
    use crate::jobs::store::state::record_attempt_intent;
    use crate::s3::access::get::GetAccessOperation;
    use crate::s3::bucket::create::CreateBucketOperation;
    use crate::s3::bucket::get::GetBucketOperation;

    async fn run_terminal_cleanup(
        ctx: &JobContext,
        for_job: JobId,
        intent: Option<&AttemptIntent>,
        access_key: &str,
    ) -> JobRunOutcome {
        let cleanup =
            TerminalCleanupOperation::new(for_job, ctx.claim_token, intent.cloned(), access_key);
        cleanup_outcome(drive(cleanup, &ctx.driver).await)
    }

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

        fn run_identity(&self) -> UserSpec {
            NOBODY
        }

        async fn health(&self) -> Result<(), BackendError> {
            Ok(())
        }

        async fn resolve_image(
            &self,
            image: &str,
            _cancel: &CancellationToken,
        ) -> Result<String, BackendError> {
            Ok(image.to_string())
        }

        async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
            Ok(())
        }

        async fn submit(
            &self,
            _context: &FenceContext,
            _spec: &TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn cancel(&self, _context: &FenceContext) -> Result<CancelEvidence, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn fetch_logs(
            &self,
            _context: &FenceContext,
            _limits: &LogLimits,
        ) -> Result<LogTails, BackendError> {
            Err(BackendError::Unavailable("unused".to_string()))
        }

        async fn fetch_output(
            &self,
            _context: &FenceContext,
            _path: &str,
        ) -> Result<TaskOutput, BackendError> {
            Err(BackendError::InvalidSpec("no output".to_string()))
        }

        async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
            ReconcileEvidence::Absent
        }

        async fn tombstone(
            &self,
            context: &FenceContext,
            _spec: &TombstoneSpec,
        ) -> Result<TombstoneEvidence, BackendError> {
            Ok(TombstoneEvidence {
                backend_ref: "stub-tombstone".to_string(),
                attempt_epoch: context.attempt_epoch,
            })
        }

        async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
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
                graph_handle: None,
                task_handle: None,
                compute_handle: Some(Arc::new(registry)),
                outbound_handle: None,
            }),
            job_id: JobId::from_bytes([0xCB; 16]),
            owner_node_id: iroh::SecretKey::from_bytes(&[0xCD; 32]).public(),
            claim_token: Ulid::from_bytes([0xCC; 16]),
            final_attempt: false,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&JobProgress::new("steps")),
            range_turns: Default::default(),
            handlers: Default::default(),
        }
    }

    fn intent(job_id: JobId, kind: &str) -> AttemptIntent {
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), 1);
        AttemptIntent {
            attempt_no: 1,
            external_name: attempt.external_name(),
            executor_kind: kind.to_string(),
            pinned_image:
                "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            attempt_epoch: 1,
        }
    }

    async fn seed_attempt(
        context: &DriverContext,
        job_id: JobId,
        kind: &str,
        token: Ulid,
    ) -> AttemptIntent {
        let node_id = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Execution(ExecutionSpec {
                group_id: Ulid::from_bytes([3; 16]),
                name: None,
                description: None,
                tags: Default::default(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: Vec::new(),
                workdir: None,
                env: Default::default(),
                resources: ComputeResources::default(),
                executor_constraint: None,
                inputs: Vec::new(),
                file_outputs: Vec::new(),
                workspace_outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
            }),
            UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            node_id,
            1,
            1,
            None,
        );
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id,
            claim_token: token,
            lease_expires_ms: 10_000,
        });
        insert_job(context, &record).await.unwrap();
        record_attempt_intent(context, job_id, token, intent(job_id, kind), None, None, 2)
            .await
            .unwrap()
            .record
            .attempt_intent
            .unwrap()
    }

    async fn seed_workspace(
        ctx: &JobContext,
        job_id: JobId,
        state: JobState,
        mode: WorkspaceMode,
        bucket: &str,
        with_object: bool,
    ) {
        let node_id = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group_id = Ulid::from_bytes([3; 16]);
        let user_id = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Execution(ExecutionSpec {
                group_id,
                name: None,
                description: None,
                tags: Default::default(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: Vec::new(),
                workdir: None,
                env: Default::default(),
                resources: ComputeResources::default(),
                executor_constraint: None,
                inputs: Vec::new(),
                file_outputs: Vec::new(),
                workspace_outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
            }),
            user_id,
            node_id,
            1,
            1,
            None,
        );
        record.state = state;
        record.workspace_mode = mode;
        record.workspace_bucket = Some(bucket.to_string());
        insert_job(&ctx.driver, &record).await.unwrap();
        drive(
            CreateBucketOperation::new(
                bucket.to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: user_id,
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                },
            ),
            &ctx.driver,
        )
        .await
        .unwrap();

        if with_object {
            let key = "result.txt";
            let version_id = Ulid::from_bytes([11; 16]);
            for (key_space, key, value) in [
                (
                    BLOB_HEAD_KEYSPACE,
                    BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
                    CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
                ),
                (
                    BLOB_VERSIONS_KEYSPACE,
                    VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
                    BlobVersion::deleted(SystemTime::now(), user_id)
                        .to_bytes()
                        .unwrap(),
                ),
            ] {
                let _ = ctx
                    .driver
                    .storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: key_space.to_string(),
                        key: key.into(),
                        value: value.into(),
                        txn_id: None,
                    })
                    .await;
            }
        }
    }

    async fn bucket_exists(ctx: &JobContext, bucket: &str) -> bool {
        drive(GetBucketOperation::new(bucket.to_string()), &ctx.driver)
            .await
            .is_ok()
    }

    async fn write_access(context: &DriverContext, access: &UserAccess) {
        let _ = context
            .storage_handle
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
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
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
        let context = crate::testing::test_context(storage.clone());
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
        let ctx = job_context(storage.clone(), std::slice::from_ref(&backend));
        let job_id = JobId::from_bytes([5; 16]);
        let intent = seed_attempt(&context, job_id, "docker", ctx.claim_token).await;

        let outcome = run_terminal_cleanup(&ctx, job_id, Some(&intent), access_key).await;

        // A live workspace key must not be stranded by one revoke failure.
        assert!(matches!(
            outcome,
            JobRunOutcome::Failed(JobError {
                kind: JobErrorKind::Retryable,
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
    async fn orphan_cleanup_fails() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = job_context(storage, &[]);
        let job_id = JobId::from_bytes([10; 16]);

        let outcome =
            run_terminal_cleanup(&ctx, job_id, Some(&intent(job_id, "docker")), "missing").await;

        assert!(matches!(
            outcome,
            JobRunOutcome::Failed(JobError {
                kind: JobErrorKind::Permanent,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn revoked_revoke_stable() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let mut access = user_access("revoked-access");
        let revoked_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        access.revoked_at = Some(revoked_at);
        write_access(&context, &access).await;
        let ctx = job_context(storage, &[]);

        for _ in 0..2 {
            assert!(matches!(
                run_terminal_cleanup(&ctx, JobId::from_bytes([7; 16]), None, &access.access_key,)
                    .await,
                JobRunOutcome::Succeeded(JobResultPayload::Cleanup)
            ));
        }
        let stored = drive(GetAccessOperation::new(access.access_key), &ctx.driver).await;
        assert!(matches!(
            stored,
            Err(crate::s3::access::get::GetAccessError::NotFound)
        ));
    }

    #[tokio::test]
    async fn cleanup_uses_kind() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let docker = StubBackend::new(ExecutorKind::Docker, vec![Ok(())]);
        let slurm = StubBackend::new(ExecutorKind::Slurm, vec![Ok(())]);
        let ctx = job_context(storage.clone(), &[docker.clone(), slurm.clone()]);
        let job_id = JobId::from_bytes([8; 16]);
        let intent = seed_attempt(&context, job_id, "slurm", ctx.claim_token).await;

        let outcome = run_terminal_cleanup(&ctx, job_id, Some(&intent), "missing-access").await;

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
        let context = crate::testing::test_context(storage.clone());
        let backend = StubBackend::new(
            ExecutorKind::Docker,
            vec![Err(BackendError::Unavailable("down".to_string())), Ok(())],
        );
        let ctx = job_context(storage.clone(), std::slice::from_ref(&backend));
        let job_id = JobId::from_bytes([9; 16]);
        let intent = seed_attempt(&context, job_id, "docker", ctx.claim_token).await;

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

    #[tokio::test]
    async fn existing_never_deletes() {
        // The bucket a run works inside is the caller's; cleanup never drops it.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = job_context(storage, &[]);
        let job_id = JobId::from_bytes([14; 16]);
        let bucket = "existing-workspace";
        seed_workspace(
            &ctx,
            job_id,
            JobState::Succeeded,
            WorkspaceMode::Existing,
            bucket,
            false,
        )
        .await;

        let outcome = run_terminal_cleanup(&ctx, job_id, None, "missing-access").await;

        assert!(matches!(
            outcome,
            JobRunOutcome::Succeeded(JobResultPayload::Cleanup)
        ));
        assert!(bucket_exists(&ctx, bucket).await);
    }
}
