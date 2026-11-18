//! Queues one job on this node: mints its id, then admits it in one local transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::execution::harvest::HarvestJobSpec;
use aruna_core::structs::execution::job::{
    CopyJobSpec, ExportRoCrateSpec, ImportRoCrateSpec, JobId, JobPayload, JobRecord,
    StagingJobCheckpoint, StagingJobSpec, WorkspaceMode, pid_dedup_key, user_dedup_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use aruna_core::structs::storage::backend_import::BackendImportSpec;
use aruna_core::structs::storage::storage_purge::StoragePurgeSpec;
use aruna_core::{NodeId, UserId};
use ulid::Ulid;

use super::JOB_RETENTION_MS;
use super::records::pages::ServingNode;
use super::staging::{SeedCheckpointOperation, StagingStoreError};
use super::store::JobMutationError;
use super::store::row_calls::{find_dedup_plan, read_job_record};
use super::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, mint_configured_job,
};
use super::submit_execution::mint_unplaced;
use crate::document_repository::{GetConfigError, GetConfigOperation};

fn unavailable(reason: &str) -> SubmitJobError {
    SubmitJobError::PlacementUnavailable(reason.to_string())
}

/// Mints a job id whose handle is the serving node's job control handle, so the owner is encoded
/// in the id itself. `dedup_key` makes the id repeatable; without one it is fresh. A node without
/// a network mints an unplaced id.
#[derive(Debug, PartialEq)]
pub struct MintJobOperation {
    realm_id: RealmId,
    owner_node_id: NodeId,
    subject: Vec<u8>,
    serving: ServingNode,
}

impl MintJobOperation {
    pub fn new(
        realm_id: RealmId,
        owner_node_id: NodeId,
        dedup_key: Option<&[u8]>,
        serving: ServingNode,
    ) -> Self {
        let subject = match dedup_key {
            Some(key) => key.to_vec(),
            None => Ulid::generate().to_bytes().to_vec(),
        };
        Self {
            realm_id,
            owner_node_id,
            subject,
            serving,
        }
    }
}

impl Operation for MintJobOperation {
    type Output = JobId;
    type Error = SubmitJobError;

    fn first(&mut self) -> Result<Next<JobId>, SubmitJobError> {
        let Some((local, serving_realm)) = self.serving else {
            return mint_unplaced(&self.subject).map(Next::Done);
        };
        if local != self.owner_node_id || serving_realm != self.realm_id {
            return Err(unavailable("job owner does not match the serving node"));
        }
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobId>, SubmitJobError> {
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config.map_err(|_| unavailable("realm config unavailable"))?;
        mint_configured_job(&config, self.owner_node_id, &self.subject).map(Next::Done)
    }
}

/// Queues a job under a minted id. The accepting node is the immutable owner: creation, listing,
/// dedup, active cap and schedule rows commit in one local transaction, a failed submit leaves
/// no rows, and nothing is reserved on any other node.
#[derive(Debug, PartialEq)]
pub struct QueueJobOperation {
    spec: SubmitJobSpec,
    job_id: JobId,
}

impl QueueJobOperation {
    pub fn new(mut spec: SubmitJobSpec, job_id: JobId, serving: ServingNode) -> Self {
        if let Some((local, _)) = serving {
            spec.owner_node_id = local;
        }
        Self { spec, job_id }
    }
}

impl Operation for QueueJobOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn first(&mut self) -> SubmitNext {
        let (spec, job_id) = (self.spec.clone(), self.job_id);
        let build = move || SubmitJobOperation::new(spec.clone(), job_id);
        let child = boxed_retry(build, &job_id.to_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> SubmitNext {
        let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
        Ok(Next::Done(submitted))
    }
}

/// What differs between the kinds of job queued on this node.
struct LocalJob {
    payload: JobPayload,
    created_by: UserId,
    owner_node_id: NodeId,
    dedup_key: Option<Vec<u8>>,
    now_ms: u64,
    retention_ms: u64,
    workspace_mode: WorkspaceMode,
}

/// Mints an id for `spec` and queues it on this node.
#[derive(Debug, PartialEq)]
pub struct SubmitLocalOperation {
    spec: SubmitJobSpec,
    mint: Option<MintJobOperation>,
    serving: ServingNode,
    minted: bool,
}

type SubmitNext = Result<Next<SubmitJobResult>, SubmitJobError>;

impl SubmitLocalOperation {
    pub fn new(spec: SubmitJobSpec, realm_id: RealmId, serving: ServingNode) -> Self {
        let dedup = spec.dedup_key.as_deref();
        let mint = MintJobOperation::new(realm_id, spec.owner_node_id, dedup, serving);
        Self {
            mint: Some(mint),
            spec,
            serving,
            minted: false,
        }
    }

    fn queued(job: LocalJob, realm_id: RealmId, serving: ServingNode) -> Self {
        let spec = SubmitJobSpec {
            payload: job.payload,
            created_by: job.created_by,
            owner_node_id: job.owner_node_id,
            dedup_key: job.dedup_key,
            now_ms: job.now_ms,
            retention_ms: job.retention_ms,
            workspace_mode: job.workspace_mode,
            workspace_bucket: None,
            active_cap: None,
        };
        Self::new(spec, realm_id, serving)
    }

    /// One run of a backend import. A scheduled run passes its due time in `dedup`, so a
    /// repeated timer never starts the same run twice.
    pub fn backend_import(
        spec: BackendImportSpec,
        owner_node_id: NodeId,
        (dedup, retention_ms): (Option<String>, u64),
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let created_by = spec.auth_context.user_id;
        let realm_id = spec.auth_context.realm_id;
        let job = LocalJob {
            payload: JobPayload::BackendImport(spec),
            created_by,
            owner_node_id,
            dedup_key: dedup.map(|key| user_dedup_key(created_by, &key)),
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }

    /// One staging job for the caller in `spec`.
    pub fn staging(
        spec: StagingJobSpec,
        owner_node_id: NodeId,
        retention_ms: u64,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let job = LocalJob {
            payload: JobPayload::Staging(spec),
            created_by,
            owner_node_id,
            dedup_key: None,
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }

    /// One background object copy for the caller in `spec`.
    pub fn copy(
        spec: CopyJobSpec,
        owner_node_id: NodeId,
        retention_ms: u64,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let job = LocalJob {
            payload: JobPayload::CopyObject(spec),
            created_by,
            owner_node_id,
            dedup_key: None,
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }

    /// One run of a harvest source for `created_by`; provenance keeps a rerun idempotent.
    pub fn harvest(
        spec: HarvestJobSpec,
        (created_by, realm_id): (UserId, RealmId),
        owner_node_id: NodeId,
        idempotency_key: Option<String>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let job = LocalJob {
            payload: JobPayload::Harvest(spec),
            created_by,
            owner_node_id,
            dedup_key: idempotency_key.map(|key| user_dedup_key(created_by, &key)),
            now_ms,
            retention_ms: JOB_RETENTION_MS,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }

    /// A purge; with `files` it also deletes the linked-storage files of the versions.
    pub fn purge(
        (spec, files): (StoragePurgeSpec, bool),
        owner_node_id: NodeId,
        (idempotency_key, retention_ms): (Option<String>, u64),
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let job = LocalJob {
            payload: match files {
                true => JobPayload::StoragePurgeFiles(spec),
                false => JobPayload::StoragePurge(spec),
            },
            created_by,
            owner_node_id,
            dedup_key: idempotency_key.map(|key| user_dedup_key(created_by, &key)),
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }

    /// The PID authority's own submission: the dedup row and the execution share one owner.
    pub fn mint_pid(
        spec: MintPersistentSpec,
        realm_id: RealmId,
        owner_node_id: NodeId,
        retention_ms: u64,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let job = LocalJob {
            created_by: spec.minted_by,
            dedup_key: Some(pid_dedup_key(spec.document_id)),
            payload: JobPayload::MintPersistentId(spec),
            owner_node_id,
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::None,
        };
        Self::queued(job, realm_id, serving)
    }

    /// Identifier registration. The key names the calling job, so a replayed step joins the job
    /// it already queued.
    pub fn identifiers(
        spec: RegisterIdentifiersSpec,
        owner_node_id: NodeId,
        key: String,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let job = LocalJob {
            payload: JobPayload::RegisterIdentifiers(spec),
            created_by,
            owner_node_id,
            dedup_key: Some(key.into_bytes()),
            now_ms,
            retention_ms: JOB_RETENTION_MS,
            workspace_mode: WorkspaceMode::None,
        };
        Self::queued(job, realm_id, serving)
    }

    /// One RO-Crate import.
    pub fn rocrate_import(
        spec: ImportRoCrateSpec,
        owner_node_id: NodeId,
        idempotency_key: Option<String>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let job = LocalJob {
            retention_ms: spec.limits.artifact_retention_ms,
            payload: JobPayload::ImportRoCrate(spec),
            created_by,
            owner_node_id,
            dedup_key: idempotency_key.map(|key| user_dedup_key(created_by, &key)),
            now_ms,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }

    /// One RO-Crate export.
    pub fn rocrate_export(
        spec: ExportRoCrateSpec,
        owner_node_id: NodeId,
        idempotency_key: Option<String>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let job = LocalJob {
            retention_ms: spec.limits.artifact_retention_ms,
            payload: JobPayload::ExportRoCrate(spec),
            created_by,
            owner_node_id,
            dedup_key: idempotency_key.map(|key| user_dedup_key(created_by, &key)),
            now_ms,
            workspace_mode: WorkspaceMode::default(),
        };
        Self::queued(job, realm_id, serving)
    }
}

impl Operation for SubmitLocalOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn first(&mut self) -> SubmitNext {
        let mint = self
            .mint
            .take()
            .ok_or_else(|| unavailable("job submit already started"))?;
        Ok(Next::Call(IoCall::child(mint)))
    }

    fn next(&mut self, result: IoResult) -> SubmitNext {
        if self.minted {
            let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
            return Ok(Next::Done(submitted));
        }
        let job_id = result.child::<JobId, SubmitJobError>()??;
        self.minted = true;
        let queue = QueueJobOperation::new(self.spec.clone(), job_id, self.serving);
        Ok(Next::Call(IoCall::child(queue)))
    }
}

#[derive(Debug, PartialEq)]
enum HandedStep {
    Mint,
    Seed(JobId),
    Queue,
}

/// Queues a staging job a range's former coordinator handed over, starting from `resume`. The
/// checkpoint is stored under the new job id before the job can run.
#[derive(Debug, PartialEq)]
pub struct HandedJobOperation {
    spec: SubmitJobSpec,
    realm_id: RealmId,
    resume: Option<StagingJobCheckpoint>,
    serving: ServingNode,
    step: HandedStep,
}

impl HandedJobOperation {
    pub fn new(
        spec: StagingJobSpec,
        resume: Option<StagingJobCheckpoint>,
        owner_node_id: NodeId,
        retention_ms: u64,
        (serving, now_ms): (ServingNode, u64),
    ) -> Self {
        let (created_by, realm_id) = (spec.auth_context.user_id, spec.auth_context.realm_id);
        let spec = SubmitJobSpec {
            payload: JobPayload::Staging(spec),
            created_by,
            owner_node_id,
            dedup_key: None,
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            active_cap: None,
        };
        Self {
            spec,
            realm_id,
            resume,
            serving,
            step: HandedStep::Mint,
        }
    }

    fn queue(&mut self, job_id: JobId) -> SubmitNext {
        self.step = HandedStep::Queue;
        let queue = QueueJobOperation::new(self.spec.clone(), job_id, self.serving);
        Ok(Next::Call(IoCall::child(queue)))
    }

    fn minted(&mut self, job_id: JobId) -> SubmitNext {
        let Some(resume) = self.resume.take() else {
            return self.queue(job_id);
        };
        self.step = HandedStep::Seed(job_id);
        let seed = SeedCheckpointOperation::new(job_id, resume);
        Ok(Next::Call(IoCall::child(seed)))
    }
}

impl Operation for HandedJobOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn first(&mut self) -> SubmitNext {
        let owner = self.spec.owner_node_id;
        let mint = MintJobOperation::new(self.realm_id, owner, None, self.serving);
        Ok(Next::Call(IoCall::child(mint)))
    }

    fn next(&mut self, result: IoResult) -> SubmitNext {
        match self.step {
            HandedStep::Mint => {
                let job_id = result.child::<JobId, SubmitJobError>()??;
                self.minted(job_id)
            }
            HandedStep::Seed(job_id) => {
                let seeded = result.child::<(), StagingStoreError>()?;
                seeded.map_err(|error| {
                    SubmitJobError::UnexpectedEvent(format!(
                        "staging checkpoint not seeded: {}",
                        error.0
                    ))
                })?;
                self.queue(job_id)
            }
            HandedStep::Queue => {
                let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
                Ok(Next::Done(submitted))
            }
        }
    }
}

/// The job and plan digest a dedup key names, if that job's record still exists.
#[derive(Debug, PartialEq)]
pub struct DedupJobOperation {
    created_by: UserId,
    dedup_key: Vec<u8>,
    found: Option<(JobId, [u8; 32])>,
}

type DedupNext = Result<Next<Option<(JobId, [u8; 32])>>, SubmitJobError>;

fn store_failed(error: JobMutationError) -> SubmitJobError {
    SubmitJobError::UnexpectedEvent(error.to_string())
}

impl DedupJobOperation {
    pub fn new(created_by: UserId, dedup_key: &[u8]) -> Self {
        Self {
            created_by,
            dedup_key: dedup_key.to_vec(),
            found: None,
        }
    }
}

impl Operation for DedupJobOperation {
    type Output = Option<(JobId, [u8; 32])>;
    type Error = SubmitJobError;

    fn first(&mut self) -> DedupNext {
        let plan = find_dedup_plan(self.created_by, &self.dedup_key);
        Ok(Next::Call(IoCall::child(plan)))
    }

    fn next(&mut self, result: IoResult) -> DedupNext {
        if let Some(found) = self.found.take() {
            let record = result
                .child::<Option<JobRecord>, JobMutationError>()?
                .map_err(store_failed)?;
            return Ok(Next::Done(record.map(|_| found)));
        }
        let plan = result
            .child::<Option<(JobId, [u8; 32])>, JobMutationError>()?
            .map_err(store_failed)?;
        let Some(found) = plan else {
            return Ok(Next::Done(None));
        };
        self.found = Some(found);
        Ok(Next::Call(IoCall::child(read_job_record(found.0))))
    }
}

/// The job an idempotency key already names. The same key with another plan is a conflict.
#[derive(Debug, PartialEq)]
pub struct DedupLookupOperation {
    created_by: UserId,
    dedup_key: Vec<u8>,
    plan_digest: [u8; 32],
}

impl DedupLookupOperation {
    pub fn new(created_by: UserId, idempotency_key: &str, plan_digest: [u8; 32]) -> Self {
        Self {
            created_by,
            dedup_key: user_dedup_key(created_by, idempotency_key),
            plan_digest,
        }
    }
}

impl Operation for DedupLookupOperation {
    type Output = Option<SubmitJobResult>;
    type Error = SubmitJobError;

    fn first(&mut self) -> Result<Next<Self::Output>, SubmitJobError> {
        let find = DedupJobOperation::new(self.created_by, &self.dedup_key);
        Ok(Next::Call(IoCall::child(find)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SubmitJobError> {
        let found = result.child::<Option<(JobId, [u8; 32])>, SubmitJobError>()??;
        let Some((job_id, digest)) = found else {
            return Ok(Next::Done(None));
        };
        if digest != self.plan_digest {
            return Err(SubmitJobError::JobPlanConflict {
                existing_job_id: job_id,
            });
        }
        Ok(Next::Done(Some(SubmitJobResult {
            job_id,
            created: false,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, REALM, node, user};
    use aruna_core::errors::StorageError;

    fn spec(owner: NodeId) -> SubmitJobSpec {
        SubmitJobSpec {
            payload: JobPayload::Execution(crate::tests::records::payload()),
            created_by: user(),
            owner_node_id: owner,
            dedup_key: None,
            now_ms: 1,
            retention_ms: 1,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            active_cap: None,
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn job() -> JobId {
        JobId::from_bytes([4; 16])
    }

    // A node without a network mints an unplaced id without reading anything.
    #[test]
    fn offline_mints_directly() {
        let mut mint = MintJobOperation::new(REALM, node(1), Some(b"key"), None);
        assert!(matches!(mint.first(), Ok(Next::Done(_))));
    }

    // Only the serving node of its realm can own the job.
    #[test]
    fn owner_must_serve() {
        let serving = Some((node(2), REALM));
        let mut mint = MintJobOperation::new(REALM, node(1), None, serving);
        assert!(matches!(
            mint.first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // An unreadable realm config, or a node without a binding, leaves the mint retryable.
    #[test]
    fn config_gives_binding() {
        let answer = |config| {
            let serving = Some((node(1), REALM));
            let mut mint = MintJobOperation::new(REALM, node(1), None, serving);
            mint.first().expect("the config is read");
            mint.next(child(config))
        };
        let failed = GetConfigError::StorageError(StorageError::Timeout);
        assert!(matches!(
            answer(Err::<RealmConfigDocument, _>(failed)),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
        let mut config = Family::new([1; 32]).config;
        assert!(matches!(
            answer(Ok::<_, GetConfigError>(config.clone())),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
        config.seed_job_control(node(1), 0);
        assert!(matches!(
            answer(Ok::<_, GetConfigError>(config)),
            Ok(Next::Done(_))
        ));
    }

    // The serving node is always the owner of what it queues.
    #[test]
    fn queue_owns_locally() {
        let queue = QueueJobOperation::new(spec(node(1)), job(), Some((node(2), REALM)));
        assert_eq!(queue.spec.owner_node_id, node(2));
    }

    // A submit mints first, queues second, and answers what the queue answered.
    #[test]
    fn submit_mints_then_queues() {
        let mut submit = SubmitLocalOperation::new(spec(node(1)), REALM, None);
        assert!(matches!(submit.first(), Ok(Next::Call(IoCall::Child(_)))));
        let queued = submit.next(child(Ok::<_, SubmitJobError>(job())));
        assert!(matches!(queued, Ok(Next::Call(IoCall::Child(_)))));
        let result = || SubmitJobResult {
            job_id: job(),
            created: true,
        };
        let done = submit.next(child(Ok::<_, SubmitJobError>(result())));
        assert_eq!(done, Ok(Next::Done(result())));
    }

    fn child<T: Send + 'static, E: Send + 'static>(result: Result<T, E>) -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    // A dedup row without its job record names nothing.
    #[test]
    fn dedup_needs_record() {
        let job_id = JobId::from_bytes([3u8; 16]);
        let mut find = DedupJobOperation::new(user(), b"key");
        assert!(matches!(find.first(), Ok(Next::Call(IoCall::Child(_)))));
        let plan = child(Ok::<_, JobMutationError>(Some((job_id, [1u8; 32]))));
        assert!(matches!(find.next(plan), Ok(Next::Call(IoCall::Child(_)))));
        let missing = child(Ok::<Option<JobRecord>, JobMutationError>(None));
        assert!(matches!(find.next(missing), Ok(Next::Done(None))));
    }

    // The same key with the same plan joins the job, and another plan conflicts.
    #[test]
    fn dedup_compares_plan() {
        let job_id = JobId::from_bytes([3u8; 16]);
        let found = || child(Ok::<_, SubmitJobError>(Some((job_id, [1u8; 32]))));
        let mut same = DedupLookupOperation::new(user(), "key", [1u8; 32]);
        assert!(matches!(
            same.next(found()),
            Ok(Next::Done(Some(result))) if result.job_id == job_id && !result.created
        ));
        let mut other = DedupLookupOperation::new(user(), "key", [2u8; 32]);
        assert_eq!(
            other.next(found()).unwrap_err(),
            SubmitJobError::JobPlanConflict {
                existing_job_id: job_id
            }
        );
    }
}
