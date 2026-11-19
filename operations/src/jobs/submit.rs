//! Submits one new job record, minting its id and honoring the dedup index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    ACTIVE_USER_KEYSPACE, BACKEND_IMPORT_KEYSPACE, DEDUP_INDEX_KEYSPACE, DISTRIBUTION_KEYSPACE,
    HARVEST_SOURCE_KEYSPACE, JOB_KEYSPACE, ROCRATE_UPLOAD_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{AfterConflict, Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::execution::harvest::HarvestSource;
use aruna_core::structs::execution::job::{
    ActiveJobKind, ImportRoCrateSource, ImportRoCrateSpec, JobId, JobPayload, JobRecord,
    RoCrateUploadRecord, WorkspaceMode, job_active_prefix, job_record_key, parse_dedup_value,
};
use aruna_core::structs::placement::record::shard_for_subject;
use aruna_core::structs::storage::backend_import::{BackendImport, import_key};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::structured_id::{
    BucketId, ClockHealthError, JobId as RoutableJobId, PlacementHandle, StructuredIdGenerator,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use super::import::upload::{UploadClaimError, claim_upload};
use crate::harvest::repository::source_key;
use crate::s3::imports::cron::is_running;
use aruna_core::structs::execution::job::upload_key;
use aruna_core::structs::execution::job::{dedup_index_key, job_insert_entries};

/// Kick the drain so a submitted job is claimed promptly; this timer is never persisted.
pub fn schedule_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainJobQueue,
        after: Duration::ZERO,
    })
}

pub fn mint_job_id(handle: PlacementHandle, bucket: BucketId) -> Result<JobId, ClockHealthError> {
    let mut generator = job_id_generator()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    generator
        .mint::<RoutableJobId>(handle, bucket)
        .map(JobId::from_routable)
}

/// Synchronous form used by producer transactions that already fenced and read
/// the realm config. The resulting job id and dedup shard can therefore be
/// written atomically with the producer's own records.
pub(crate) fn mint_configured_job(
    config: &aruna_core::structs::identity::realm::RealmConfigDocument,
    owner_node_id: NodeId,
    subject: &[u8],
) -> Result<JobId, SubmitJobError> {
    let handle = config.job_control_handle(&owner_node_id).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable(
            "serving node has no job-control binding yet".to_string(),
        )
    })?;
    let tuple = config
        .binding_directory()
        .resolve(handle)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let strategy = config.strategy(&tuple.strategy_id).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job-control strategy unavailable".to_string())
    })?;
    let shard = shard_for_subject(subject, strategy.shard_count);
    let bucket = u16::try_from(shard)
        .ok()
        .and_then(|shard| BucketId::new(shard).ok())
        .ok_or_else(|| {
            SubmitJobError::PlacementUnavailable("job bucket is out of range".to_string())
        })?;
    Ok(mint_job_id(handle, bucket)?)
}

fn job_id_generator() -> &'static Mutex<StructuredIdGenerator> {
    static GENERATOR: OnceLock<Mutex<StructuredIdGenerator>> = OnceLock::new();
    GENERATOR.get_or_init(|| Mutex::new(StructuredIdGenerator::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJobSpec {
    pub payload: JobPayload,
    pub created_by: UserId,
    pub owner_node_id: NodeId,
    pub dedup_key: Option<Vec<u8>>,
    pub now_ms: u64,
    pub retention_ms: u64,
    pub workspace_mode: WorkspaceMode,
    pub workspace_bucket: Option<String>,
    /// Unfinished jobs of this user this node admits, counted in the admitting
    /// transaction. `None` leaves the payload's own limit in charge.
    pub active_cap: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJobResult {
    pub job_id: JobId,
    /// `false` when a job with the same `dedup_key` already existed.
    pub created: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SubmitJobError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    ClockHealth(#[from] ClockHealthError),
    #[error("job placement unavailable: {0}")]
    PlacementUnavailable(String),
    /// The routed authority no longer has the document the job is scoped to.
    #[error("persistent id target document is absent")]
    DocumentMissing,
    /// The routed authority refused the caller's permission on the document.
    #[error("persistent id submission is not authorized")]
    AuthorityDenied,
    #[error("{0}: input read denied")]
    InputDenied(String),
    #[error("idempotency key already bound to job {existing_job_id} with a different plan")]
    JobPlanConflict { existing_job_id: JobId },
    #[error("invalid workspace: {0}")]
    InvalidWorkspace(String),
    #[error("a job may declare at most {limit} outputs")]
    TooManyOutputs { limit: usize },
    /// Standing compute quota refused the new logical admission.
    #[error("compute quota denied: {0}")]
    QuotaDenied(#[from] aruna_core::compute::quota::QuotaDenied),
    #[error(transparent)]
    Composition(#[from] aruna_core::structs::execution::job::CompositionError),
    #[error("active RO-Crate job limit reached ({limit})")]
    ActiveJobLimit { limit: u32 },
    #[error(transparent)]
    Upload(#[from] UploadClaimError),
    #[error("the import rule does not exist")]
    ImportMissing,
    #[error("the last run of this import has not finished")]
    ImportRunning,
    #[error("the harvest source does not exist")]
    SourceMissing,
    #[error("the last run of this harvest source has not finished")]
    SourceRunning,
    #[error("unexpected event while submitting job: {0}")]
    UnexpectedEvent(String),
    #[error("a key range of distributed bucket {0} is moving away from this node; retry later")]
    RangeMoving(ulid::Ulid),
}

impl From<IoError> for SubmitJobError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            other => Self::UnexpectedEvent(other.to_string()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SubmitState {
    Start,
    GroupFence,
    ReadDedup,
    VerifyDedup {
        job_id: JobId,
        digest_matches: bool,
    },
    CheckActive,
    ClaimUpload,
    /// Reads the rule of a backend import run, then the run it last started.
    ReadImport,
    CheckImportRun,
    /// Reads a harvest source, then the run it last started.
    ReadSource,
    CheckSourceRun,
    /// Reads the destination of a staging job, then the record of its distributed set.
    ReadBucket,
    ReadRecord {
        set_id: ulid::Ulid,
        member_id: ulid::Ulid,
    },
    WriteJob,
}

type Step = Result<Next<SubmitJobResult>, SubmitJobError>;

/// Call-driven submit; a live `job_dedup_index` entry short-circuits to the
/// existing id (matching plan digest) or raises `JobPlanConflict` (differing
/// digest), a dangling entry creates fresh; at-least-once, so consumers must be idempotent.
/// Every check reads in the write transaction the job row commits in.
#[derive(Debug, PartialEq)]
pub struct SubmitJobOperation {
    record: JobRecord,
    group_checked: bool,
    upload_checked: bool,
    /// The claim of the upload an upload-backed import reads, written with the job.
    upload_claim: Option<(String, Key, Value)>,
    import_checked: bool,
    import_rule: Option<BackendImport>,
    /// The rule of a backend import run with this job as its last run, written with the job.
    import_claim: Option<(String, Key, Value)>,
    source_checked: bool,
    harvest_source: Option<HarvestSource>,
    source_claim: Option<(String, Key, Value)>,
    staging_checked: bool,
    active_cap: Option<u32>,
    state: SubmitState,
}

impl SubmitJobOperation {
    pub fn new(spec: SubmitJobSpec, job_id: JobId) -> Self {
        let active_cap = spec.active_cap.or_else(|| {
            spec.payload
                .rocrate_limits()
                .map(|limits| limits.max_active_jobs)
        });
        let mut record = JobRecord::new(
            job_id,
            spec.payload,
            spec.created_by,
            spec.owner_node_id,
            spec.now_ms,
            spec.now_ms,
            spec.dedup_key,
        );
        record.retention_ms = spec.retention_ms;
        if matches!(&record.payload, JobPayload::Execution(_)) {
            record.workspace_mode = spec.workspace_mode;
            record.workspace_bucket = match spec.workspace_mode {
                WorkspaceMode::Existing => spec.workspace_bucket,
                WorkspaceMode::None => None,
            };
            record.plan_digest = Some(workspace_plan_digest(
                &record.payload,
                record.workspace_mode,
                record.workspace_bucket.as_deref(),
            ));
        }
        Self {
            record,
            group_checked: false,
            upload_checked: false,
            upload_claim: None,
            import_checked: false,
            import_rule: None,
            import_claim: None,
            source_checked: false,
            harvest_source: None,
            source_claim: None,
            staging_checked: false,
            active_cap,
            state: SubmitState::Start,
        }
    }

    /// Whether a check reads before the job is written, so the job commits with what it read.
    fn checks(&self) -> bool {
        self.record.dedup_key.is_some()
            || self.active_cap.is_some()
            || self.record.payload.owner_group().is_some()
            || self.upload().is_some()
            || matches!(
                self.record.payload,
                JobPayload::BackendImport(_) | JobPayload::Harvest(_) | JobPayload::Staging(_)
            )
    }

    /// Reads one row in the next state.
    fn read(&mut self, state: SubmitState, key_space: &str, key: Key) -> Step {
        self.state = state;
        Ok(Next::Call(IoCall::Read(vec![(key_space.to_string(), key)])))
    }

    fn read_dedup(&mut self) -> Step {
        let Some(dedup_key) = self.record.dedup_key.clone() else {
            return self.check_active();
        };
        // Must use the same index-key builder `job_insert_entries` uses, which decides
        // owner prefixing per key, or the reservation read misses its own row.
        let key = dedup_index_key(self.record.created_by, &dedup_key);
        self.read(SubmitState::ReadDedup, DEDUP_INDEX_KEYSPACE, key)
    }

    fn check_active(&mut self) -> Step {
        let Some(limit) = self.active_cap else {
            return self.write_job();
        };
        // Only the kinds that hold a slot can be counted against one.
        let Some(kind) = ActiveJobKind::of(&self.record.payload) else {
            return self.write_job();
        };
        if limit == 0 {
            return Err(SubmitJobError::ActiveJobLimit { limit });
        }
        self.state = SubmitState::CheckActive;
        Ok(Next::Call(IoCall::Page {
            space: ACTIVE_USER_KEYSPACE.to_string(),
            prefix: job_active_prefix(self.record.created_by, kind),
            after: None,
            limit: limit as usize,
        }))
    }

    fn verify_dedup(&mut self, job_id: JobId, digest_matches: bool) -> Step {
        let state = SubmitState::VerifyDedup {
            job_id,
            digest_matches,
        };
        self.read(state, JOB_KEYSPACE, job_record_key(job_id))
    }

    fn write_job(&mut self) -> Step {
        if !self.group_checked
            && let Some(group_id) = self.record.payload.owner_group()
        {
            let (key_space, key) = crate::groups::fence::group_fence_key(group_id);
            return self.read(SubmitState::GroupFence, &key_space, key);
        }
        // An upload-backed import claims its upload with the job, so it keeps it across expiry.
        if !self.upload_checked
            && let Some(upload_id) = self.upload()
        {
            let key = upload_key(upload_id);
            return self.read(SubmitState::ClaimUpload, ROCRATE_UPLOAD_KEYSPACE, key);
        }
        // A rule records its run with the job, so two starts never both pass the check.
        if !self.import_checked
            && let JobPayload::BackendImport(spec) = &self.record.payload
        {
            let key = import_key(spec.group_id, spec.import_id);
            return self.read(SubmitState::ReadImport, BACKEND_IMPORT_KEYSPACE, key);
        }
        // A harvest source records its run with the job, the same way.
        if !self.source_checked
            && let JobPayload::Harvest(spec) = &self.record.payload
        {
            let key = source_key(spec.group_id, spec.source_id);
            return self.read(SubmitState::ReadSource, HARVEST_SOURCE_KEYSPACE, key);
        }
        // A staging job waits while a range of its distributed destination moves away.
        if !self.staging_checked
            && let JobPayload::Staging(spec) = &self.record.payload
        {
            let key = spec.bucket.as_bytes().into();
            return self.read(SubmitState::ReadBucket, S3_BUCKET_KEYSPACE, key);
        }
        let mut writes = job_insert_entries(&self.record)?;
        writes.extend(self.upload_claim.clone());
        writes.extend(self.import_claim.clone());
        writes.extend(self.source_claim.clone());
        self.state = SubmitState::WriteJob;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn upload(&self) -> Option<ulid::Ulid> {
        match &self.record.payload {
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                source: ImportRoCrateSource::Upload { upload_id },
                ..
            }) => Some(*upload_id),
            _ => None,
        }
    }

    /// Applies the upload claim rule to the stored upload `value` for this job.
    fn claim(
        &mut self,
        upload_id: ulid::Ulid,
        value: Option<&[u8]>,
    ) -> Result<(), UploadClaimError> {
        let value = value.ok_or(UploadClaimError::NotFound)?;
        let invalid = |error: postcard::Error| UploadClaimError::Invalid(error.to_string());
        let mut upload: RoCrateUploadRecord = postcard::from_bytes(value).map_err(invalid)?;
        let record = &self.record;
        if claim_upload(
            &mut upload,
            record.created_by,
            record.job_id,
            record.created_at_ms,
        )? {
            let value = postcard::to_allocvec(&upload).map_err(invalid)?;
            let key_space = ROCRATE_UPLOAD_KEYSPACE.to_string();
            self.upload_claim = Some((key_space, upload_key(upload_id), value.into()));
        }
        self.upload_checked = true;
        Ok(())
    }

    /// Reads the run the rule last started, or records this job when it has none.
    fn import_read(&mut self, value: Option<&[u8]>) -> Step {
        let value = value.ok_or(SubmitJobError::ImportMissing)?;
        let rule = BackendImport::from_bytes(value)?;
        let Some(last_job) = rule.last_job else {
            return self.claim_import(rule);
        };
        self.import_rule = Some(rule);
        self.read(
            SubmitState::CheckImportRun,
            JOB_KEYSPACE,
            job_record_key(last_job),
        )
    }

    /// A pruned last run no longer holds the rule.
    fn import_run_read(&mut self, value: Option<&[u8]>) -> Step {
        let rule = self.import_rule.take().ok_or_else(|| {
            SubmitJobError::UnexpectedEvent("import run check without a rule".to_string())
        })?;
        match value.map(JobRecord::from_bytes).transpose()? {
            Some(last) if is_running(last.state) => Err(SubmitJobError::ImportRunning),
            _ => self.claim_import(rule),
        }
    }

    fn claim_import(&mut self, mut rule: BackendImport) -> Step {
        rule.last_job = Some(self.record.job_id);
        rule.last_started_ms = Some(self.record.created_at_ms);
        let value = rule.to_bytes()?;
        let key_space = BACKEND_IMPORT_KEYSPACE.to_string();
        self.import_claim = Some((key_space, rule.key(), value.into()));
        self.import_checked = true;
        self.write_job()
    }

    /// Reads the run the source last started, or records this job when it has none.
    fn source_read(&mut self, value: Option<&[u8]>) -> Step {
        let value = value.ok_or(SubmitJobError::SourceMissing)?;
        let source = HarvestSource::from_bytes(value)?;
        let Some(last_job) = source.last_job else {
            return self.claim_source(source);
        };
        self.harvest_source = Some(source);
        self.read(
            SubmitState::CheckSourceRun,
            JOB_KEYSPACE,
            job_record_key(last_job),
        )
    }

    fn source_run_read(&mut self, value: Option<&[u8]>) -> Step {
        let source = self.harvest_source.take().ok_or_else(|| {
            SubmitJobError::UnexpectedEvent("source run check without a source".to_string())
        })?;
        match value.map(JobRecord::from_bytes).transpose()? {
            Some(last) if is_running(last.state) => Err(SubmitJobError::SourceRunning),
            _ => self.claim_source(source),
        }
    }

    fn claim_source(&mut self, mut source: HarvestSource) -> Step {
        source.last_job = Some(self.record.job_id);
        let value = source.to_bytes()?;
        let key = source_key(source.group_id, source.source_id);
        let key_space = HARVEST_SOURCE_KEYSPACE.to_string();
        self.source_claim = Some((key_space, key, value.into()));
        self.source_checked = true;
        self.write_job()
    }

    /// A destination that is gone or not distributed needs no record.
    fn bucket_read(&mut self, value: Option<&[u8]>) -> Step {
        let bucket = value.map(BucketInfo::from_bytes).transpose()?;
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution) else {
            self.staging_checked = true;
            return self.write_job();
        };
        let state = SubmitState::ReadRecord {
            set_id: binding.set_id,
            member_id: binding.member_id,
        };
        let key = binding.set_id.to_bytes().to_vec().into();
        self.read(state, DISTRIBUTION_KEYSPACE, key)
    }

    /// Refuses the job while the record lists a transfer away from this member: the transfer
    /// hands over only jobs admitted before it opened.
    fn record_read(
        &mut self,
        (set_id, member_id): (ulid::Ulid, ulid::Ulid),
        value: Option<&[u8]>,
    ) -> Step {
        let view = value.map(DistributionView::from_bytes).transpose()?;
        let transfers = (view.as_ref()).map(|view| &view.snapshot().lifecycle.transfers);
        if transfers.is_some_and(|open| open.iter().any(|transfer| transfer.from == member_id)) {
            return Err(SubmitJobError::RangeMoving(set_id));
        }
        self.staging_checked = true;
        self.write_job()
    }

    /// Reads the job a live dedup row points at, or checks the active cap when there is none.
    fn dedup_read(&mut self, value: Option<Value>) -> Step {
        let Some(value) = value else {
            return self.check_active();
        };
        match parse_dedup_value(value.as_ref()) {
            // Same key and plan is idempotent; a different plan is a conflict. Either way
            // the target record is verified first so no ghost row answers for a dead job.
            Ok((existing_job_id, existing_digest)) => {
                let digest_matches = self.record.plan_digest.unwrap_or_default() == existing_digest;
                self.verify_dedup(existing_job_id, digest_matches)
            }
            Err(_) => self.check_active(),
        }
    }

    fn verified(&mut self, job_id: JobId, digest_matches: bool, value: Option<Value>) -> Step {
        let Some(value) = value else {
            warn!(job_id = %job_id, "Dedup entry points at a missing job; creating fresh");
            return self.check_active();
        };
        match JobRecord::from_bytes(&value) {
            Ok(_) if digest_matches => Ok(Next::Done(SubmitJobResult {
                job_id,
                created: false,
            })),
            Ok(_) => Err(SubmitJobError::JobPlanConflict {
                existing_job_id: job_id,
            }),
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Dedup entry points at an undecodable job; creating fresh");
                self.check_active()
            }
        }
    }

    /// Applies the upload claim rule to the stored upload `value` for this job.
    fn claim_read(&mut self, value: Option<&[u8]>) -> Step {
        let upload_id = self.upload().ok_or_else(|| {
            SubmitJobError::UnexpectedEvent("upload claim without an upload".to_string())
        })?;
        self.claim(upload_id, value)?;
        self.write_job()
    }

    fn active_read(&mut self, rows: usize) -> Step {
        let limit = self.active_cap.unwrap_or_default();
        if rows >= limit as usize {
            return Err(SubmitJobError::ActiveJobLimit { limit });
        }
        self.write_job()
    }
}

/// The default workspace leaves the plan digest alone; only the bucket an
/// `Existing`-mode run names changes what an idempotency key stands for.
fn workspace_plan_digest(
    payload: &JobPayload,
    mode: WorkspaceMode,
    bucket: Option<&str>,
) -> [u8; 32] {
    let digest = payload.plan_digest();
    if mode != WorkspaceMode::Existing {
        return digest;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna/job-workspace/v1");
    hasher.update(&digest);
    hasher.update(mode.name().as_bytes());
    if let Some(bucket) = bucket {
        hasher.update(bucket.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

impl Operation for SubmitJobOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn transaction(&self) -> TransactionMode {
        match self.checks() {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
    }

    /// A lost commit always runs again with fresh reads, as the submit has always done.
    fn on_conflict(&mut self, _attempt: u32) -> AfterConflict<SubmitJobResult> {
        AfterConflict::Rerun
    }

    fn first(&mut self) -> Step {
        match self.checks() {
            true => self.read_dedup(),
            false => self.write_job(),
        }
    }

    fn next(&mut self, result: IoResult) -> Step {
        let state = self.state.clone();
        if let SubmitState::CheckActive = state {
            let IoResult::Page { rows, .. } = result else {
                return Err(SubmitJobError::UnexpectedEvent(result.label().to_string()));
            };
            return self.active_read(rows.len());
        }
        if let SubmitState::WriteJob = state {
            let IoResult::Done = result else {
                return Err(SubmitJobError::UnexpectedEvent(result.label().to_string()));
            };
            // Kick the drain so the job is claimed promptly; a lost timer only delays it.
            let output = SubmitJobResult {
                job_id: self.record.job_id,
                created: true,
            };
            let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
            return Ok(Next::Then { output, calls });
        }
        let [value] = values::<1>(result)?;
        match state {
            SubmitState::GroupFence => {
                aruna_core::structs::identity::group_delete::check_group_write(value.as_deref())?;
                self.group_checked = true;
                self.write_job()
            }
            SubmitState::ReadDedup => self.dedup_read(value),
            SubmitState::VerifyDedup {
                job_id,
                digest_matches,
            } => self.verified(job_id, digest_matches, value),
            SubmitState::ClaimUpload => self.claim_read(value.as_deref()),
            SubmitState::ReadImport => self.import_read(value.as_deref()),
            SubmitState::CheckImportRun => self.import_run_read(value.as_deref()),
            SubmitState::ReadSource => self.source_read(value.as_deref()),
            SubmitState::CheckSourceRun => self.source_run_read(value.as_deref()),
            SubmitState::ReadBucket => self.bucket_read(value.as_deref()),
            SubmitState::ReadRecord { set_id, member_id } => {
                self.record_read((set_id, member_id), value.as_deref())
            }
            SubmitState::Start | SubmitState::CheckActive | SubmitState::WriteJob => Err(
                SubmitJobError::UnexpectedEvent("a result before the first call".to_string()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::store::test_calls::read_job_record;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{JOB_INDEX_KEYSPACE, JOB_KEYSPACE, SCHEDULE_INDEX_KEYSPACE};
    use aruna_core::structs::execution::job::{
        ComputeResources, ExecutionSpec, ImportMetadataTarget, ImportRoCrateSource,
        ImportRoCrateSpec, ImportRoCrateTarget, JobState, RoCrateLimits, encode_dedup_value,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
    use aruna_core::structs::storage::backend_import::BackendImportSpec;
    use aruna_storage::{FjallStorage, StorageHandle};
    use aruna_tasks::TaskHandle;
    use byteview::ByteView;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn context(storage: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        }
    }

    fn operation(spec: SubmitJobSpec) -> SubmitJobOperation {
        let job_id = mint_job_id(
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
        )
        .unwrap();
        SubmitJobOperation::new(spec, job_id)
    }

    // A plain job writes its rows without a transaction, then kicks the drain after them.
    #[test]
    fn plain_job_writes() {
        let mut operation = operation(spec(None));
        assert_eq!(operation.transaction(), TransactionMode::None);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("the write ends the submit");
        };
        assert!(output.created);
        assert_eq!(
            calls,
            vec![IoCall::Effect(Box::new(schedule_drain_effect()))]
        );
    }

    // A deduplicated job reads its dedup row first, in a write transaction that reruns on conflict.
    #[test]
    fn dedup_reads_first() {
        let mut operation = operation(spec(Some(b"key".to_vec())));
        assert_eq!(operation.transaction(), TransactionMode::Write);
        assert!(
            matches!(operation.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 1)
        );
        assert_eq!(operation.on_conflict(9), AfterConflict::Rerun);
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(SubmitJobError::UnexpectedEvent(_))
        ));
    }

    fn spec(dedup_key: Option<Vec<u8>>) -> SubmitJobSpec {
        SubmitJobSpec {
            payload: JobPayload::Probe {
                steps: 2,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            created_by: UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            owner_node_id: node_id(7),
            dedup_key,
            now_ms: 1_000,
            retention_ms: aruna_core::structs::execution::job::RETENTION_MS,
            workspace_mode: WorkspaceMode::None,
            workspace_bucket: None,
            active_cap: None,
        }
    }

    fn execution_payload() -> JobPayload {
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
        })
    }

    fn rocrate_spec(limit: u32, dedup_key: Option<Vec<u8>>) -> SubmitJobSpec {
        let base = spec(dedup_key);
        let limits = RoCrateLimits {
            max_active_jobs: limit,
            ..RoCrateLimits::default()
        };
        SubmitJobSpec {
            payload: JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: AuthContext {
                    user_id: base.created_by,
                    realm_id: RealmId([1u8; 32]),
                    path_restrictions: None,
                    session: None,
                },
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([3u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: "crate".to_string(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([4u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits,
                document_id: Ulid::from_bytes([5u8; 16]),
            }),
            ..base
        }
    }

    /// Points the import of `spec` at upload `[seed; 16]` and stores it unclaimed until `expires`.
    async fn seed_upload(
        context: &DriverContext,
        spec: &mut SubmitJobSpec,
        seed: u8,
        expires: u64,
    ) -> Ulid {
        let JobPayload::ImportRoCrate(import) = &mut spec.payload else {
            panic!("expected import payload");
        };
        let upload_id = Ulid::from_bytes([seed; 16]);
        import.source = ImportRoCrateSource::Upload { upload_id };
        let record =
            crate::jobs::import::upload::tests::upload_record(spec.created_by, upload_id, expires);
        crate::driver::drive(
            crate::jobs::import::upload_store::WriteUploadOperation::new(record.clone()),
            context,
        )
        .await
        .map_err(|error| error.0)
        .unwrap();
        upload_id
    }

    #[test]
    fn import_fence_ignored() {
        let mut first = rocrate_spec(4, Some(b"key".to_vec()));
        let mut second = first.clone();
        let JobPayload::ImportRoCrate(first_spec) = &mut first.payload else {
            panic!("expected import payload");
        };
        let JobPayload::ImportRoCrate(second_spec) = &mut second.payload else {
            panic!("expected import payload");
        };
        first_spec.document_id = Ulid::from_bytes([5; 16]);
        second_spec.document_id = Ulid::from_bytes([6; 16]);

        assert_eq!(first.payload.plan_digest(), second.payload.plan_digest());
    }

    async fn count_keyspace(context: &DriverContext, key_space: &str) -> usize {
        aruna_storage::scan::iter_prefix_page(
            &context.storage_handle,
            key_space,
            None,
            None,
            64,
            None,
        )
        .await
        .expect("scan")
        .0
        .len()
    }

    #[tokio::test]
    async fn existing_bucket_persisted() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let submission = SubmitJobSpec {
            payload: execution_payload(),
            workspace_mode: WorkspaceMode::Existing,
            workspace_bucket: Some("shared-workspace".to_string()),
            ..spec(None)
        };

        let result = drive(operation(submission), &ctx).await.unwrap();
        let record = read_job_record(&ctx, result.job_id, None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.workspace_mode, WorkspaceMode::Existing);
        assert_eq!(record.workspace_bucket.as_deref(), Some("shared-workspace"));
    }

    #[tokio::test]
    async fn retention_is_stored() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let mut submission = spec(None);
        submission.retention_ms = 42;

        let result = drive(operation(submission), &ctx).await.unwrap();
        let record = read_job_record(&ctx, result.job_id, None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.retention_ms, 42);
    }

    #[tokio::test]
    async fn existing_dedup_stable() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage);
        let submission = SubmitJobSpec {
            payload: execution_payload(),
            dedup_key: Some(b"existing".to_vec()),
            workspace_mode: WorkspaceMode::Existing,
            workspace_bucket: Some("shared-workspace".to_string()),
            ..spec(None)
        };

        let first = drive(operation(submission.clone()), &ctx).await.unwrap();
        let second = drive(operation(submission), &ctx).await.unwrap();

        assert!(first.created);
        assert!(!second.created);
        assert_eq!(second.job_id, first.job_id);
    }

    async fn write_raw(context: &DriverContext, key_space: &str, key: ByteView, value: ByteView) {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn records_source_run() {
        use aruna_core::structs::execution::harvest::{HarvestJobSpec, HarvestSelector};
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let source = HarvestSource::new(
            Ulid::from_bytes([6u8; 16]),
            Ulid::from_bytes([3u8; 16]),
            Ulid::from_bytes([5u8; 16]),
            "zenodo".to_string(),
            "imported".to_string(),
            HarvestSelector::default(),
            None,
            std::time::SystemTime::UNIX_EPOCH,
            spec(None).created_by,
        );
        let run = || SubmitJobSpec {
            payload: JobPayload::Harvest(HarvestJobSpec {
                source_id: source.source_id,
                group_id: source.group_id,
            }),
            ..spec(None)
        };
        let missing = drive(operation(run()), &ctx).await;
        assert_eq!(missing, Err(SubmitJobError::SourceMissing));

        let key = source_key(source.group_id, source.source_id);
        let value = ByteView::from(source.to_bytes().unwrap());
        write_raw(&ctx, HARVEST_SOURCE_KEYSPACE, key.clone(), value).await;
        let first = drive(operation(run()), &ctx).await.unwrap();
        let event = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: HARVEST_SOURCE_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = event
        else {
            panic!("the source is missing: {event:?}")
        };
        let stored = HarvestSource::from_bytes(&value).unwrap();
        assert_eq!(stored.last_job, Some(first.job_id));

        // While the first run is queued, a second run of the source is refused.
        let second = drive(operation(run()), &ctx).await;
        assert_eq!(second, Err(SubmitJobError::SourceRunning));
        assert_eq!(count_keyspace(&ctx, JOB_KEYSPACE).await, 1);
    }

    #[tokio::test]
    async fn records_import_run() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let base = spec(None);
        let rule = BackendImport {
            import_id: Ulid::from_bytes([6u8; 16]),
            group_id: Ulid::from_bytes([3u8; 16]),
            backend_id: Ulid::from_bytes([5u8; 16]),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "in/".to_string(),
            schedule: None,
            created_by: base.created_by,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{name}".to_string(),
        };
        let run = || SubmitJobSpec {
            payload: JobPayload::BackendImport(BackendImportSpec {
                auth_context: AuthContext {
                    user_id: base.created_by,
                    realm_id: RealmId([1u8; 32]),
                    path_restrictions: None,
                    session: None,
                },
                node_id: node_id(7),
                group_id: rule.group_id,
                import_id: rule.import_id,
            }),
            ..spec(None)
        };
        let missing = drive(operation(run()), &ctx).await;
        assert_eq!(missing, Err(SubmitJobError::ImportMissing));

        let value = ByteView::from(rule.to_bytes().unwrap());
        write_raw(&ctx, BACKEND_IMPORT_KEYSPACE, rule.key(), value).await;
        let first = drive(operation(run()), &ctx).await.unwrap();
        let rule_key = rule.key();
        let stored = |storage: StorageHandle| {
            let key = rule_key.clone();
            async move {
                let event = storage
                    .send_storage_effect(StorageEffect::Read {
                        key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                        key,
                        txn_id: None,
                    })
                    .await;
                let Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) = event
                else {
                    panic!("the rule is missing: {event:?}")
                };
                BackendImport::from_bytes(&value).unwrap()
            }
        };
        let recorded = stored(storage.clone()).await;
        assert_eq!(recorded.last_job, Some(first.job_id));
        assert_eq!(recorded.last_started_ms, Some(1_000));

        // While the first run is queued, a second start is refused and records nothing.
        let second = drive(operation(run()), &ctx).await;
        assert_eq!(second, Err(SubmitJobError::ImportRunning));
        assert_eq!(count_keyspace(&ctx, JOB_KEYSPACE).await, 1);
        assert_eq!(stored(storage).await.last_job, Some(first.job_id));
    }

    #[tokio::test]
    async fn submit_creates_job() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let result = drive(operation(spec(None)), &ctx)
            .await
            .expect("submit succeeds");
        assert!(result.created);

        let record = read_job_record(&ctx, result.job_id, None)
            .await
            .unwrap()
            .expect("job persisted");
        assert_eq!(record.state, JobState::Queued);
        assert_eq!(count_keyspace(&ctx, SCHEDULE_INDEX_KEYSPACE).await, 1);
        assert_eq!(count_keyspace(&ctx, JOB_INDEX_KEYSPACE).await, 1);
        assert_eq!(count_keyspace(&ctx, DEDUP_INDEX_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn dedup_returns_existing() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let first = drive(operation(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(first.created);
        assert_eq!(count_keyspace(&ctx, DEDUP_INDEX_KEYSPACE).await, 1);

        let second = drive(operation(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(!second.created);
        assert_eq!(second.job_id, first.job_id);
        // No duplicate record created.
        assert_eq!(count_keyspace(&ctx, JOB_KEYSPACE).await, 1);
    }

    #[tokio::test]
    async fn rocrate_cap_enforced() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let mut first = rocrate_spec(1, None);
        seed_upload(&ctx, &mut first, 3, u64::MAX).await;
        let first = drive(operation(first), &ctx).await.unwrap();

        assert_eq!(
            drive(operation(rocrate_spec(1, None)), &ctx).await,
            Err(SubmitJobError::ActiveJobLimit { limit: 1 })
        );

        let crate::jobs::store::job_calls::ClaimOutcome::Claimed(claimed) =
            crate::jobs::store::test_calls::claim_job(&ctx, first.job_id, node_id(3), 2_000)
                .await
                .unwrap()
        else {
            panic!("job must be claimed");
        };
        let token = claimed.claim.unwrap().claim_token;
        crate::jobs::store::test_calls::transition_to_running(&ctx, first.job_id, token, 2_100)
            .await
            .unwrap();
        crate::jobs::store::test_calls::cancel_running_job(&ctx, first.job_id, token, 2_200)
            .await
            .unwrap();

        let mut third = rocrate_spec(1, None);
        seed_upload(&ctx, &mut third, 6, u64::MAX).await;
        assert!(drive(operation(third), &ctx).await.unwrap().created);
    }

    #[tokio::test]
    async fn claims_upload_atomically() {
        // An upload-backed import claims its upload in the creating transaction, so the job
        // keeps it across expiry; a refused claim leaves no job or dedup row behind.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let mut first = rocrate_spec(4, Some(b"first".to_vec()));
        let upload_id = seed_upload(&ctx, &mut first, 3, 2_000).await;
        let created = drive(operation(first.clone()), &ctx).await.unwrap();
        let stored = crate::driver::drive(
            crate::jobs::import::upload_store::ReadUploadOperation::new(upload_id),
            &ctx,
        )
        .await
        .map_err(|error| error.0);
        assert_eq!(stored.unwrap().unwrap().claimed_by, Some(created.job_id));
        let owner = first.created_by;
        let reclaimed = crate::driver::drive_retry(
            || {
                crate::jobs::import::upload::ClaimUploadOperation::new(
                    upload_id,
                    owner,
                    created.job_id,
                    3_000,
                )
            },
            &ctx,
            &upload_id.to_bytes(),
        )
        .await;
        assert!(reclaimed.is_ok());
        let mut other = first;
        other.dedup_key = Some(b"other".to_vec());
        let claimed = Err(SubmitJobError::Upload(UploadClaimError::AlreadyClaimed));
        assert_eq!(drive(operation(other), &ctx).await, claimed);
        let mut late = rocrate_spec(4, Some(b"late".to_vec()));
        seed_upload(&ctx, &mut late, 6, 1_000).await;
        let expired = Err(SubmitJobError::Upload(UploadClaimError::Expired));
        assert_eq!(drive(operation(late), &ctx).await, expired);
        assert_eq!(count_keyspace(&ctx, JOB_KEYSPACE).await, 1);
        assert_eq!(count_keyspace(&ctx, DEDUP_INDEX_KEYSPACE).await, 1);
    }

    #[tokio::test]
    async fn rocrate_dedup_first() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let mut submission = rocrate_spec(1, Some(b"import".to_vec()));
        seed_upload(&ctx, &mut submission, 3, u64::MAX).await;
        let first = drive(operation(submission.clone()), &ctx).await.unwrap();
        let second = drive(operation(submission.clone()), &ctx).await.unwrap();

        assert!(first.created);
        assert!(!second.created);
        assert_eq!(second.job_id, first.job_id);

        let crate::jobs::store::job_calls::ClaimOutcome::Claimed(claimed) =
            crate::jobs::store::test_calls::claim_job(&ctx, first.job_id, node_id(3), 2_000)
                .await
                .unwrap()
        else {
            panic!("job must be claimed");
        };
        let token = claimed.claim.unwrap().claim_token;
        crate::jobs::store::test_calls::transition_to_running(&ctx, first.job_id, token, 2_100)
            .await
            .unwrap();
        crate::jobs::store::test_calls::cancel_running_job(&ctx, first.job_id, token, 2_200)
            .await
            .unwrap();

        let terminal = drive(operation(submission), &ctx).await.unwrap();
        assert!(!terminal.created);
        assert_eq!(terminal.job_id, first.job_id);
    }

    // Equal logical keys from different owners must not share a dedup row.
    #[tokio::test]
    async fn dedup_scoped() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let first = drive(operation(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        let mut other = spec(Some(b"k".to_vec()));
        other.created_by = UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([1u8; 32]));
        let second = drive(operation(other), &ctx).await.unwrap();

        assert!(first.created);
        assert!(second.created);
        assert_ne!(second.job_id, first.job_id);
        assert_eq!(count_keyspace(&ctx, DEDUP_INDEX_KEYSPACE).await, 2);
    }

    // Same idempotency key + a different plan is a JobPlanConflict, not a silent reuse.
    #[tokio::test]
    async fn dedup_plan_conflict() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let mut first_spec = spec(Some(b"k".to_vec()));
        let JobPayload::Probe { steps, .. } = &mut first_spec.payload else {
            unreachable!()
        };
        *steps = 2;
        let first = drive(operation(first_spec), &ctx).await.unwrap();
        assert!(first.created);

        let mut conflicting = spec(Some(b"k".to_vec()));
        let JobPayload::Probe { steps, .. } = &mut conflicting.payload else {
            unreachable!()
        };
        *steps = 9;
        let error = drive(operation(conflicting), &ctx)
            .await
            .expect_err("differing plan must conflict");
        assert_eq!(
            error,
            SubmitJobError::JobPlanConflict {
                existing_job_id: first.job_id
            }
        );
        // The original job is untouched and no second record was written.
        assert_eq!(count_keyspace(&ctx, JOB_KEYSPACE).await, 1);
    }

    // Perf budget: a non-dedup submit is one atomic batch write of <=4 keys plus a
    // non-persisted drain kick.
    #[tokio::test]
    async fn submit_writes_once() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let before = storage.snapshot_metrics().requests_total;
        drive(operation(spec(None)), &ctx).await.unwrap();
        let after = storage.snapshot_metrics().requests_total;
        assert_eq!(
            after - before,
            1,
            "submit performs exactly one storage request"
        );

        let keys = count_keyspace(&ctx, JOB_KEYSPACE).await
            + count_keyspace(&ctx, SCHEDULE_INDEX_KEYSPACE).await
            + count_keyspace(&ctx, JOB_INDEX_KEYSPACE).await
            + count_keyspace(&ctx, DEDUP_INDEX_KEYSPACE).await;
        assert!(keys <= 4, "submit writes at most four keys, got {keys}");
    }

    // A dedup row left dangling by a quarantined record must not ghost future submits.
    #[tokio::test]
    async fn ghost_dedup_heals() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let ghost = JobId::from_bytes([9u8; 16]);
        let submission = spec(Some(b"k".to_vec()));
        let created_by = submission.created_by;
        let digest = submission.payload.plan_digest();
        write_raw(
            &ctx,
            DEDUP_INDEX_KEYSPACE,
            dedup_index_key(created_by, b"k"),
            ByteView::from(encode_dedup_value(ghost, digest)),
        )
        .await;

        let result = drive(operation(submission), &ctx).await.unwrap();
        assert!(result.created);
        assert_ne!(result.job_id, ghost);
        assert_eq!(
            crate::jobs::store::test_calls::find_dedup_job(&ctx, created_by, b"k", None)
                .await
                .unwrap(),
            Some(result.job_id)
        );
    }

    // A dangling dedup row with a DIFFERING digest must self-heal too, not raise
    // a plan conflict against a job that no longer exists.
    #[tokio::test]
    async fn ghost_conflict_heals() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let ghost = JobId::from_bytes([9u8; 16]);
        let submission = spec(Some(b"k".to_vec()));
        let created_by = submission.created_by;
        write_raw(
            &ctx,
            DEDUP_INDEX_KEYSPACE,
            dedup_index_key(created_by, b"k"),
            ByteView::from(encode_dedup_value(ghost, [0xAB; 32])),
        )
        .await;

        let result = drive(operation(submission), &ctx)
            .await
            .expect("ghost row must not conflict");
        assert!(result.created);
        assert_ne!(result.job_id, ghost);
        assert_eq!(
            crate::jobs::store::test_calls::find_dedup_job(&ctx, created_by, b"k", None)
                .await
                .unwrap(),
            Some(result.job_id)
        );
    }

    // Same when the record row still exists but no longer decodes.
    #[tokio::test]
    async fn corrupt_dedup_heals() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());
        let ghost = JobId::from_bytes([9u8; 16]);
        let submission = spec(Some(b"k".to_vec()));
        let created_by = submission.created_by;
        let digest = submission.payload.plan_digest();
        write_raw(
            &ctx,
            JOB_KEYSPACE,
            job_record_key(ghost),
            ByteView::from(vec![1, 2, 3]),
        )
        .await;
        write_raw(
            &ctx,
            DEDUP_INDEX_KEYSPACE,
            dedup_index_key(created_by, b"k"),
            ByteView::from(encode_dedup_value(ghost, digest)),
        )
        .await;

        let result = drive(operation(submission), &ctx).await.unwrap();
        assert!(result.created);
        assert_ne!(result.job_id, ghost);
        assert_eq!(
            crate::jobs::store::test_calls::find_dedup_job(&ctx, created_by, b"k", None)
                .await
                .unwrap(),
            Some(result.job_id)
        );
    }

    #[tokio::test]
    async fn dedup_after_terminal() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = context(storage.clone());

        let first = drive(operation(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        // Take the job through claim + terminal so the dedup entry is cleared.
        crate::jobs::store::test_calls::claim_job(&ctx, first.job_id, node_id(3), 2_000)
            .await
            .unwrap();
        let claimed = read_job_record(&ctx, first.job_id, None)
            .await
            .unwrap()
            .unwrap();
        let token = claimed.claim.unwrap().claim_token;
        crate::jobs::store::test_calls::transition_to_running(&ctx, first.job_id, token, 2_100)
            .await
            .unwrap();
        crate::jobs::store::test_calls::cancel_running_job(&ctx, first.job_id, token, 2_200)
            .await
            .unwrap();

        let second = drive(operation(spec(Some(b"k".to_vec()))), &ctx)
            .await
            .unwrap();
        assert!(second.created);
        assert_ne!(second.job_id, first.job_id);
    }
}
