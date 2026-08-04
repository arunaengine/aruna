use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ArtifactRef, DEFAULT_SHARD_COUNT, ExecutionSpec, ExportRoCrateSpec, FIRST_GRANTABLE_HANDLE,
    ImportRoCrateSpec, JobId, JobOwnerError, JobPayload, JobRecord, JobResultPayload, JobState,
    RealmId, RunCrateStatus, StagingJobCheckpoint, StagingJobSpec, WorkspaceMode,
    shard_for_subject, user_dedup_key,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::task::TaskEvent;
use aruna_core::types::{NodeId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use bytes::Bytes;
use serde_json::Value as JsonValue;
use std::ops::Range;
use std::path::Path;
use tracing::warn;

use super::JOB_REPORT_MAX_ROWS;
use super::protocol::{JobRequest, JobResponse, JobRouteError, WireRange, send_job_request};
use super::runtime::JobsRuntime;
use super::staging::read_staging_checkpoint;
use super::store::{
    CancelRequestOutcome, JobMutationError, find_dedup_plan, list_job_entries, list_jobs_for_user,
    read_artifact_tombstone, read_job_record, read_run_crate_status, set_cancel_requested,
};
use super::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, mint_job_id,
    schedule_job_drain_effect,
};
use super::workflow::finalize_followups;
use crate::driver::{DriverContext, drive};
use crate::metadata::api::load_realm_config;
use crate::metadata::repository::StorageReadError;

use super::route::{JobRouteOperation, JobRouteOutcome};

pub use aruna_core::jobs::{JobKind, JobReportView, JobStatusView};

/// Mints a JobId whose handle is the serving node's JobControl handle, so the
/// owner is encoded in the id itself. The bucket is a local queue shard only;
/// it never selects a remote owner.
async fn mint_local_job(
    context: &DriverContext,
    realm_id: RealmId,
    owner_node_id: NodeId,
    dedup_key: Option<&[u8]>,
) -> Result<JobId, SubmitJobError> {
    let entropy = ulid::Ulid::generate().to_bytes();
    let subject = dedup_key.unwrap_or(&entropy);
    let Some(net_handle) = context.net_handle.as_ref() else {
        let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE)
            .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
        let shard = shard_for_subject(subject, DEFAULT_SHARD_COUNT);
        let bucket = BucketId::new(shard as u16)
            .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
        return Ok(mint_job_id(handle, bucket)?);
    };
    if net_handle.node_id() != owner_node_id || *net_handle.realm_id() != realm_id {
        return Err(SubmitJobError::PlacementUnavailable(
            "job owner does not match the serving node".to_string(),
        ));
    }
    let config = load_realm_config(context, realm_id).await.ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("realm config unavailable".to_string())
    })?;
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

/// The accepting node is the immutable owner: creation, listing, dedup,
/// active-cap, and schedule rows commit in one local transaction, so a failed
/// submit leaves no ghost rows and nothing is reserved on any other node.
pub(crate) async fn submit_local_job(
    context: &DriverContext,
    mut spec: SubmitJobSpec,
    job_id: JobId,
) -> Result<SubmitJobResult, SubmitJobError> {
    if let Some(net_handle) = context.net_handle.as_ref() {
        spec.owner_node_id = net_handle.node_id();
    }
    drive(SubmitJobOperation::new(spec, job_id), context).await
}

/// Submit a container execution job on behalf of `created_by`. The drain claims it
/// and drives the fenced external attempt lifecycle. The idempotency key is
/// namespaced per user, disjoint from internal obligation keys.
#[allow(clippy::too_many_arguments)]
pub async fn submit_execution_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    match workspace_mode {
        WorkspaceMode::None if workspace_bucket.is_some() => {
            return Err(SubmitJobError::InvalidWorkspace(
                "none mode does not accept a bucket".to_string(),
            ));
        }
        WorkspaceMode::Existing
            if workspace_bucket
                .as_deref()
                .is_none_or(|bucket| bucket.trim().is_empty()) =>
        {
            return Err(SubmitJobError::InvalidWorkspace(
                "existing mode requires a bucket".to_string(),
            ));
        }
        WorkspaceMode::Temporary | WorkspaceMode::Kept if workspace_bucket.is_some() => {
            return Err(SubmitJobError::InvalidWorkspace(
                "bucket is only valid for existing mode".to_string(),
            ));
        }
        _ => {}
    }
    if workspace_mode == WorkspaceMode::None
        && (spec
            .inputs
            .iter()
            .any(|input| input.mode != aruna_core::structs::InputMode::Mount)
            || !spec.workspace_outputs.is_empty()
            || !spec.output_prefixes.is_empty())
    {
        return Err(SubmitJobError::InvalidWorkspace(
            "none mode requires mounted inputs and explicit output destinations".to_string(),
        ));
    }
    if workspace_mode != WorkspaceMode::None
        && spec
            .inputs
            .iter()
            .any(|input| input.mode == aruna_core::structs::InputMode::Mount)
    {
        return Err(SubmitJobError::InvalidWorkspace(
            "mounted inputs require none workspace mode".to_string(),
        ));
    }
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    let job_id = mint_local_job(
        context,
        created_by.realm_id,
        owner_node_id,
        dedup_key.as_deref(),
    )
    .await?;
    submit_local_job(
        context,
        SubmitJobSpec {
            payload: JobPayload::Execution(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
            retention_ms,
            workspace_mode,
            workspace_bucket,
        },
        job_id,
    )
    .await
}

pub async fn submit_staging_job(
    context: &DriverContext,
    spec: StagingJobSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let created_by = spec.auth_context.user_id;
    let job_id = mint_local_job(context, created_by.realm_id, owner_node_id, None).await?;
    submit_local_job(
        context,
        SubmitJobSpec {
            payload: JobPayload::Staging(spec),
            created_by,
            owner_node_id,
            dedup_key: None,
            now_ms: unix_timestamp_millis(),
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
        },
        job_id,
    )
    .await
}

pub async fn submit_rocrate_import(
    context: &DriverContext,
    spec: ImportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let created_by = spec.auth_context.user_id;
    let retention_ms = spec.limits.artifact_retention_ms;
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    let job_id = mint_local_job(
        context,
        created_by.realm_id,
        owner_node_id,
        dedup_key.as_deref(),
    )
    .await?;
    submit_local_job(
        context,
        SubmitJobSpec {
            payload: JobPayload::ImportRoCrate(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
        },
        job_id,
    )
    .await
}

pub async fn lookup_job_dedup(
    context: &DriverContext,
    created_by: UserId,
    idempotency_key: &str,
    plan_digest: [u8; 32],
) -> Result<Option<SubmitJobResult>, SubmitJobError> {
    let dedup_key = user_dedup_key(created_by, idempotency_key);
    let Some((job_id, existing_digest)) =
        find_dedup_plan(&context.storage_handle, created_by, &dedup_key, None)
            .await
            .map_err(SubmitJobError::UnexpectedEvent)?
    else {
        return Ok(None);
    };
    if read_job_record(&context.storage_handle, job_id, None)
        .await
        .map_err(SubmitJobError::UnexpectedEvent)?
        .is_none()
    {
        return Ok(None);
    }
    if existing_digest != plan_digest {
        return Err(SubmitJobError::JobPlanConflict {
            existing_job_id: job_id,
        });
    }
    Ok(Some(SubmitJobResult {
        job_id,
        created: false,
    }))
}

pub async fn submit_export_job(
    context: &DriverContext,
    spec: ExportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let created_by = spec.auth_context.user_id;
    let retention_ms = spec.limits.artifact_retention_ms;
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    let job_id = mint_local_job(
        context,
        created_by.realm_id,
        owner_node_id,
        dedup_key.as_deref(),
    )
    .await?;
    submit_local_job(
        context,
        SubmitJobSpec {
            payload: JobPayload::ExportRoCrate(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            retention_ms,
        },
        job_id,
    )
    .await
}

/// Read the run-crate obligation status surfaced alongside an execution job.
pub async fn read_job_run_crate_status(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<RunCrateStatus>, String> {
    read_run_crate_status(&context.storage_handle, job_id).await
}

/// Node-local listing: returns only jobs owned by the serving node (every job
/// the user submitted through it). There is no realm-wide aggregation, so jobs
/// owned by other nodes are omitted; listings are per-origin by contract.
pub async fn list_owned_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    list_jobs_for_user(&context.storage_handle, user_id, cursor, limit, filter).await
}

pub async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    Ok(
        match read_job_record(&context.storage_handle, job_id, None).await? {
            Some(record) if record.created_by == user_id && !record.payload.is_internal() => {
                Some(record)
            }
            _ => None,
        },
    )
}

/// Full owner record for API projections that the status view cannot rebuild
/// off-owner. Routes to the derived owner like `read_job_routed`; an unreachable
/// owner is `Unavailable` (503), only the owner answers absence (`Ok(None)`).
pub async fn read_record_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<Option<JobRecord>, JobRouteError> {
    Ok(route_record(context, user_id, job_id, auth_token)
        .await?
        .map(|(record, _)| record))
}

/// Returns a staging record and its checkpoint from the immutable owner.
pub async fn read_staging_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<Option<(JobRecord, Option<StagingJobCheckpoint>)>, JobRouteError> {
    route_record(context, user_id, job_id, auth_token).await
}

async fn read_record_data(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<(JobRecord, Option<StagingJobCheckpoint>)>, String> {
    let record = read_owned_job(context, user_id, job_id).await?;
    let checkpoint = match &record {
        Some(record) if matches!(&record.payload, JobPayload::Staging(_)) => {
            read_staging_checkpoint(context, job_id).await?
        }
        _ => None,
    };
    Ok(record.map(|record| (record, checkpoint)))
}

async fn route_record(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<Option<(JobRecord, Option<StagingJobCheckpoint>)>, JobRouteError> {
    let Some(net) = context.net_handle.as_ref() else {
        return read_record_data(context, user_id, job_id)
            .await
            .map_err(JobRouteError::Internal);
    };
    let request = auth_token.map(|auth_token| JobRequest::Record { auth_token, job_id });
    let operation = JobRouteOperation::new(*net.realm_id(), net.node_id(), job_id, request);
    match drive(operation, context).await? {
        JobRouteOutcome::Local => read_record_data(context, user_id, job_id)
            .await
            .map_err(JobRouteError::Internal),
        JobRouteOutcome::Remote(response) => match response {
            JobResponse::Record { record, checkpoint }
                if record.job_id == job_id
                    && record.created_by == user_id
                    && !record.payload.is_internal() =>
            {
                Ok(Some((*record, checkpoint)))
            }
            JobResponse::Record { .. } => Err(JobRouteError::NotFound),
            JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => Err(JobRouteError::Forbidden),
            JobResponse::NotFound => Ok(None),
            JobResponse::Unavailable(error) => Err(JobRouteError::Unavailable(error)),
            response => Err(JobRouteError::Unavailable(format!(
                "unexpected record response from the job owner: {response:?}"
            ))),
        },
    }
}

/// Derives the immutable owner from the JobId alone: replicated placement
/// state is the only input, so resolution never asks another node and can
/// never be stranded by a placement rebalance. A missing or unsynced binding is
/// `Unavailable` (503); only a provably invalid id maps to `NotFound`.
pub(crate) async fn resolve_job_owner(
    context: &DriverContext,
    job_id: JobId,
) -> Result<NodeId, JobRouteError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobRouteError::Unavailable("network handle unavailable".to_string()))?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or_else(|| JobRouteError::Unavailable("realm config unavailable".to_string()))?;
    config.job_owner(job_id).map_err(|error| match error {
        JobOwnerError::NotJobControl => JobRouteError::NotFound,
        JobOwnerError::Unavailable(message) => JobRouteError::Unavailable(message),
    })
}

fn owner_unreachable(error: JobRouteError) -> JobRouteError {
    match error {
        JobRouteError::Unavailable(message) => {
            JobRouteError::Unavailable(format!("job owner unreachable: {message}"))
        }
        error => error,
    }
}

#[derive(Debug)]
pub struct RoutedJobStatus {
    pub job: JobStatusView,
    pub run_crate: Option<JsonValue>,
}

async fn local_status(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<RoutedJobStatus, JobRouteError> {
    let record = read_owned_job(context, user_id, job_id)
        .await
        .map_err(JobRouteError::Internal)?
        .ok_or(JobRouteError::NotFound)?;
    let run_crate = read_job_run_crate_status(context, job_id)
        .await
        .map_err(JobRouteError::Internal)?
        .map(|status| status.to_public_json());
    Ok(RoutedJobStatus {
        job: JobStatusView::from(&record),
        run_crate,
    })
}

pub async fn read_job_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<RoutedJobStatus, JobRouteError> {
    let Some(net) = context.net_handle.as_ref() else {
        return local_status(context, user_id, job_id).await;
    };
    let request = auth_token.map(|auth_token| JobRequest::Status { auth_token, job_id });
    let operation = JobRouteOperation::new(*net.realm_id(), net.node_id(), job_id, request);
    match drive(operation, context).await? {
        JobRouteOutcome::Local => local_status(context, user_id, job_id).await,
        JobRouteOutcome::Remote(response) => match response {
            JobResponse::Status { job, run_crate } if routed_job_matches(&job, user_id, job_id) => {
                let run_crate = run_crate
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| JobRouteError::Internal(error.to_string()))?;
                Ok(RoutedJobStatus { job, run_crate })
            }
            JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => Err(JobRouteError::Forbidden),
            JobResponse::NotFound => Err(JobRouteError::NotFound),
            JobResponse::Unavailable(error) => Err(JobRouteError::Unavailable(error)),
            response => Err(JobRouteError::Unavailable(format!(
                "unexpected status response from the job owner: {response:?}"
            ))),
        },
    }
}

pub enum JobReportLookup {
    NotFound,
    Pending(JobState),
    CursorConflict,
    Ready {
        job: JobReportView,
        rows: Vec<(Vec<u8>, Value)>,
        next_key: Option<Vec<u8>>,
    },
}

pub async fn read_owned_report(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<JobReportLookup, String> {
    let Some(record) = read_owned_job(context, user_id, job_id).await? else {
        return Ok(JobReportLookup::NotFound);
    };
    let key_limit = match &record.payload {
        JobPayload::ImportRoCrate(spec) => spec.limits.key_bytes,
        JobPayload::ExportRoCrate(spec) => spec.limits.key_bytes,
        _ => return Ok(JobReportLookup::NotFound),
    };
    if last_key
        .as_ref()
        .is_some_and(|key| u64::try_from(key.len()).unwrap_or(u64::MAX) > key_limit)
    {
        return Ok(JobReportLookup::CursorConflict);
    }
    if !record.state.is_terminal() {
        return Ok(JobReportLookup::Pending(record.state));
    }
    let finished_at_ms = record.finished_at_ms.unwrap_or(record.updated_at_ms);
    if finished_at_ms.saturating_add(record.retention_ms) <= unix_timestamp_millis() {
        return Ok(JobReportLookup::NotFound);
    }
    let report_digest = record
        .report_digest
        .ok_or_else(|| "terminal RO-Crate job is missing its report digest".to_string())?;
    if expected_digest.is_some_and(|expected| expected != report_digest) {
        return Ok(JobReportLookup::CursorConflict);
    }
    let limit = limit.clamp(1, usize::from(JOB_REPORT_MAX_ROWS));
    let (rows, next_key) =
        list_job_entries(&context.storage_handle, job_id, last_key, limit).await?;
    Ok(JobReportLookup::Ready {
        job: JobReportView {
            job_id: record.job_id,
            created_by: record.created_by,
            kind: JobKind::from(&record.payload),
            report_digest,
        },
        rows,
        next_key,
    })
}

pub async fn read_report_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<JobReportLookup, JobRouteError> {
    let Some(net) = context.net_handle.as_ref() else {
        return read_owned_report(context, user_id, job_id, expected_digest, last_key, limit)
            .await
            .map_err(JobRouteError::Internal);
    };
    let wire_limit = u16::try_from(limit.min(usize::from(JOB_REPORT_MAX_ROWS)))
        .map_err(|error| JobRouteError::Internal(error.to_string()))?;
    let request = auth_token.map(|auth_token| JobRequest::Report {
        auth_token,
        job_id,
        expected_digest,
        last_key: last_key.clone(),
        limit: wire_limit,
    });
    let operation = JobRouteOperation::new(*net.realm_id(), net.node_id(), job_id, request);
    match drive(operation, context).await? {
        JobRouteOutcome::Local => {
            read_owned_report(context, user_id, job_id, expected_digest, last_key, limit)
                .await
                .map_err(JobRouteError::Internal)
        }
        JobRouteOutcome::Remote(response) => match response {
            JobResponse::ReportPending(state) => Ok(JobReportLookup::Pending(state)),
            JobResponse::ReportConflict => Ok(JobReportLookup::CursorConflict),
            JobResponse::ReportReady {
                job,
                rows,
                next_key,
            } if report_job_matches(&job, user_id, job_id, expected_digest) => {
                Ok(JobReportLookup::Ready {
                    job,
                    rows: rows
                        .into_iter()
                        .map(|(key, value)| (key, Value::from(value)))
                        .collect(),
                    next_key,
                })
            }
            JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => Err(JobRouteError::Forbidden),
            JobResponse::NotFound => Ok(JobReportLookup::NotFound),
            JobResponse::Unavailable(error) => Err(JobRouteError::Unavailable(error)),
            response => Err(JobRouteError::Unavailable(format!(
                "unexpected report response from the job owner: {response:?}"
            ))),
        },
    }
}

pub struct OwnedArtifact {
    pub job_id: JobId,
    pub created_by: UserId,
    pub blake3: [u8; 32],
    pub size: u64,
    pub filename: String,
    pub(crate) artifact: Option<ArtifactRef>,
}

impl OwnedArtifact {
    pub(crate) fn source(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }

    pub fn same_content(&self, other: &Self) -> bool {
        self.blake3 == other.blake3 && self.size == other.size
    }
}

pub enum ArtifactLookup {
    NotFound,
    Pending(JobState),
    Gone,
    Ready(OwnedArtifact),
}

fn artifact_filename(document_path: Option<&str>, document_id: ulid::Ulid) -> String {
    let stem = document_path
        .and_then(|path| Path::new(path.trim_end_matches('/')).file_stem())
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| document_id.to_string());
    format!("{stem}.zip")
}

pub async fn read_owned_artifact(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<ArtifactLookup, String> {
    let Some(record) = read_owned_job(context, user_id, job_id).await? else {
        return Ok(
            match read_artifact_tombstone(&context.storage_handle, job_id, now_ms).await? {
                Some(owner) if owner == user_id => ArtifactLookup::Gone,
                _ => ArtifactLookup::NotFound,
            },
        );
    };
    let JobPayload::ExportRoCrate(spec) = &record.payload else {
        return Ok(ArtifactLookup::NotFound);
    };
    if !record.state.is_terminal() {
        return Ok(ArtifactLookup::Pending(record.state));
    }
    let Some(JobResultPayload::ExportRoCrate(result)) = &record.result else {
        return Ok(ArtifactLookup::NotFound);
    };
    let Some(artifact) = result.artifact.clone() else {
        return Ok(ArtifactLookup::NotFound);
    };
    if artifact.expires_at_ms <= now_ms {
        return Ok(ArtifactLookup::Gone);
    }
    let location_hash: [u8; 32] = artifact
        .location
        .get_blake3()
        .ok_or_else(|| "artifact location is missing its BLAKE3 hash".to_string())?
        .try_into()
        .map_err(|_| "artifact location has an invalid BLAKE3 hash".to_string())?;
    if location_hash != artifact.blake3 {
        return Err("artifact record does not match its blob location".to_string());
    }
    let document_path =
        crate::get_metadata_document::load_metadata_record_by_document(context, spec.document_id)
            .await
            .map_err(|error| match error {
                StorageReadError::Storage(error) => error.to_string(),
                StorageReadError::Conversion(error) => error.to_string(),
            })?
            .map(|record| record.document_path);
    Ok(ArtifactLookup::Ready(OwnedArtifact {
        job_id: record.job_id,
        created_by: record.created_by,
        blake3: artifact.blake3,
        size: artifact.size,
        artifact: Some(artifact),
        filename: artifact_filename(document_path.as_deref(), spec.document_id),
    }))
}

pub async fn read_artifact_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    range: Option<Range<u64>>,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<(ArtifactLookup, Option<ArtifactRead>), JobRouteError> {
    if context.net_handle.is_none() {
        let lookup = read_owned_artifact(context, user_id, job_id, now_ms)
            .await
            .map_err(JobRouteError::Internal)?;
        let read = match (&lookup, range) {
            (ArtifactLookup::Ready(owned), Some(range)) => {
                let artifact = owned.source().ok_or_else(|| {
                    JobRouteError::Internal("local artifact source is unavailable".to_string())
                })?;
                let read = read_artifact_range(context, artifact, range.clone())
                    .await
                    .map_err(JobRouteError::Internal)?;
                if !artifact_size_matches(Some(&range), read.stream_size) {
                    return Err(JobRouteError::Internal(
                        "artifact reader returned an unexpected range size".to_string(),
                    ));
                }
                Some(read)
            }
            _ => None,
        };
        return Ok((lookup, read));
    }
    let owner = resolve_job_owner(context, job_id).await?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    if Some(owner) == local_node {
        let lookup = read_owned_artifact(context, user_id, job_id, now_ms)
            .await
            .map_err(JobRouteError::Internal)?;
        let read = match (&lookup, range) {
            (ArtifactLookup::Ready(owned), Some(range)) => {
                let artifact = owned.source().ok_or_else(|| {
                    JobRouteError::Internal("local artifact source is unavailable".to_string())
                })?;
                let read = read_artifact_range(context, artifact, range.clone())
                    .await
                    .map_err(JobRouteError::Internal)?;
                if !artifact_size_matches(Some(&range), read.stream_size) {
                    return Err(JobRouteError::Internal(
                        "artifact reader returned an unexpected range size".to_string(),
                    ));
                }
                Some(read)
            }
            _ => None,
        };
        return Ok((lookup, read));
    }
    let token = auth_token.ok_or(JobRouteError::Unauthorized)?;
    let reply = send_job_request(
        context,
        owner,
        JobRequest::Artifact {
            auth_token: token,
            job_id,
            range: range.clone().map(WireRange::from),
        },
    )
    .await
    .map_err(owner_unreachable)?;
    match reply.response {
        JobResponse::ArtifactPending(state) => Ok((ArtifactLookup::Pending(state), None)),
        JobResponse::ArtifactGone => Ok((ArtifactLookup::Gone, None)),
        JobResponse::ArtifactReady { owned, stream_size } => {
            let owned = OwnedArtifact::from(owned);
            if !artifact_job_matches(&owned, user_id, job_id) {
                return Err(JobRouteError::Unavailable(
                    "job owner returned a mismatched artifact".to_string(),
                ));
            }
            let read = match (range.as_ref(), reply.body) {
                (Some(range), Some(blob)) if artifact_size_matches(Some(range), stream_size) => {
                    Some(ArtifactRead { blob, stream_size })
                }
                (None, None) if artifact_size_matches(None, stream_size) => None,
                _ => {
                    return Err(JobRouteError::Unavailable(
                        "job owner returned a mismatched artifact stream".to_string(),
                    ));
                }
            };
            Ok((ArtifactLookup::Ready(owned), read))
        }
        JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
        JobResponse::Forbidden => Err(JobRouteError::Forbidden),
        JobResponse::NotFound => Ok((ArtifactLookup::NotFound, None)),
        JobResponse::Unavailable(error) => Err(JobRouteError::Unavailable(error)),
        response => Err(JobRouteError::Unavailable(format!(
            "unexpected artifact response from the job owner: {response:?}"
        ))),
    }
}

fn artifact_size_matches(range: Option<&Range<u64>>, stream_size: u64) -> bool {
    match range {
        Some(range) => range.end.checked_sub(range.start) == Some(stream_size),
        None => stream_size == 0,
    }
}

pub struct ArtifactRead {
    pub blob: BackendStream<Result<Bytes, StreamError>>,
    pub stream_size: u64,
}

pub async fn read_artifact_range(
    context: &DriverContext,
    artifact: &ArtifactRef,
    range: Range<u64>,
) -> Result<ArtifactRead, String> {
    let blob_handle = context
        .blob_handle
        .as_ref()
        .ok_or_else(|| "blob handle unavailable".to_string())?;
    match blob_handle
        .send_blob_effect(BlobEffect::ReadHiddenRange {
            location: artifact.location.clone(),
            range,
        })
        .await
    {
        Event::Blob(BlobEvent::HiddenRead { blob, stream_size }) => {
            Ok(ArtifactRead { blob, stream_size })
        }
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        event => Err(format!("unexpected hidden artifact read event: {event:?}")),
    }
}

pub enum CancelJobOutcome {
    NotFound,
    AlreadyTerminal(JobRecord),
    Requested(JobRecord),
}

pub enum RoutedCancelOutcome {
    NotFound,
    AlreadyTerminal(JobStatusView),
    Requested(JobStatusView),
}

pub async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<CancelJobOutcome, String> {
    if read_owned_job(context, user_id, job_id).await?.is_none() {
        return Ok(CancelJobOutcome::NotFound);
    }
    // The job may be pruned between the ownership read and here; treat that as a 404
    // rather than a 500.
    let outcome = match set_cancel_requested(
        &context.storage_handle,
        job_id,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(JobMutationError::NotFound) => return Ok(CancelJobOutcome::NotFound),
        Err(error) => return Err(error.to_string()),
    };
    Ok(match outcome {
        CancelRequestOutcome::AlreadyTerminal(record) => CancelJobOutcome::AlreadyTerminal(record),
        // Already terminalized in the store transaction: wake the durable run-crate child.
        CancelRequestOutcome::Cancelled(record) => {
            if matches!(&record.payload, JobPayload::Execution(_)) {
                finalize_followups(context, job_id).await;
            }
            CancelJobOutcome::Requested(record)
        }
        CancelRequestOutcome::Flagged(record) => {
            runtime.request_cancel(job_id);
            kick_drain(context).await;
            CancelJobOutcome::Requested(record)
        }
    })
}

fn cancel_outcome(outcome: CancelJobOutcome) -> RoutedCancelOutcome {
    match outcome {
        CancelJobOutcome::NotFound => RoutedCancelOutcome::NotFound,
        CancelJobOutcome::AlreadyTerminal(record) => {
            RoutedCancelOutcome::AlreadyTerminal(JobStatusView::from(&record))
        }
        CancelJobOutcome::Requested(record) => {
            RoutedCancelOutcome::Requested(JobStatusView::from(&record))
        }
    }
}

/// Cancellation is owner-anchored: it either executes on the immutable owner or
/// fails `Unavailable`; no passive copy is ever terminalized in its stead.
pub async fn cancel_job_routed(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<RoutedCancelOutcome, JobRouteError> {
    let Some(net) = context.net_handle.as_ref() else {
        return cancel_owned_job(context, runtime, user_id, job_id)
            .await
            .map(cancel_outcome)
            .map_err(JobRouteError::Internal);
    };
    let request = auth_token.map(|auth_token| JobRequest::Cancel { auth_token, job_id });
    let operation = JobRouteOperation::new(*net.realm_id(), net.node_id(), job_id, request);
    match drive(operation, context).await? {
        JobRouteOutcome::Local => cancel_owned_job(context, runtime, user_id, job_id)
            .await
            .map(cancel_outcome)
            .map_err(JobRouteError::Internal),
        JobRouteOutcome::Remote(response) => match response {
            JobResponse::Cancelled { job, terminal }
                if routed_job_matches(&job, user_id, job_id) =>
            {
                Ok(if terminal {
                    RoutedCancelOutcome::AlreadyTerminal(job)
                } else {
                    RoutedCancelOutcome::Requested(job)
                })
            }
            JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => Err(JobRouteError::Forbidden),
            JobResponse::NotFound => Err(JobRouteError::NotFound),
            JobResponse::Unavailable(error) => Err(JobRouteError::Unavailable(error)),
            response => Err(JobRouteError::Unavailable(format!(
                "unexpected cancel response from the job owner: {response:?}"
            ))),
        },
    }
}

fn routed_job_matches(job: &JobStatusView, user_id: UserId, job_id: JobId) -> bool {
    job.job_id == job_id && job.created_by == user_id && !job.kind.is_internal()
}

fn report_job_matches(
    job: &JobReportView,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
) -> bool {
    job.job_id == job_id
        && job.created_by == user_id
        && job.kind.is_report()
        && expected_digest.is_none_or(|digest| digest == job.report_digest)
}

fn artifact_job_matches(artifact: &OwnedArtifact, user_id: UserId, job_id: JobId) -> bool {
    artifact.job_id == job_id && artifact.created_by == user_id
}

async fn kick_drain(context: &DriverContext) {
    if let Some(task_handle) = context.task_handle.as_ref()
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_job_drain_effect()).await
    {
        warn!(message = %message, "Failed to kick job drain");
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::{insert_job, preserve_artifact_tombstone, read_job_record};
    use super::*;
    use aruna_core::structs::{
        AuthContext, ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec,
        ImportRoCrateTarget, JobState, RealmId, RoCrateLimits,
    };
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    #[test]
    fn validates_artifact_size() {
        assert!(artifact_size_matches(None, 0));
        assert!(!artifact_size_matches(None, 1));
        assert!(artifact_size_matches(Some(&(2..5)), 3));
        assert!(!artifact_size_matches(Some(&(2..5)), 2));
        let malformed = Range { start: 5, end: 2 };
        assert!(!artifact_size_matches(Some(&malformed), 0));
    }

    #[test]
    fn rejects_wrong_records() {
        let realm_id = RealmId([1u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
        let job_id = JobId::from_bytes([3u8; 16]);
        let record = JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            owner,
            node_id(),
            1,
            1,
            None,
        );
        let mut view = JobStatusView::from(&record);
        assert!(routed_job_matches(&view, owner, job_id));
        assert!(!routed_job_matches(
            &view,
            UserId::new(Ulid::from_bytes([4u8; 16]), realm_id),
            job_id
        ));
        assert!(!routed_job_matches(
            &view,
            owner,
            JobId::from_bytes([5u8; 16])
        ));
        view.kind = JobKind::WriteRunCrate;
        assert!(!routed_job_matches(&view, owner, job_id));

        let report = JobReportView {
            job_id,
            created_by: owner,
            kind: JobKind::ImportRoCrate,
            report_digest: [6u8; 32],
        };
        assert!(report_job_matches(&report, owner, job_id, Some([6u8; 32])));
        assert!(!report_job_matches(&report, owner, job_id, Some([7u8; 32])));

        let artifact = OwnedArtifact {
            job_id,
            created_by: owner,
            blake3: [8u8; 32],
            size: 1,
            filename: "artifact.zip".to_string(),
            artifact: None,
        };
        assert!(artifact_job_matches(&artifact, owner, job_id));
        assert!(!artifact_job_matches(
            &artifact,
            owner,
            JobId::from_bytes([9u8; 16])
        ));
        let matching = OwnedArtifact {
            job_id,
            created_by: owner,
            blake3: artifact.blake3,
            size: artifact.size,
            filename: "renamed.zip".to_string(),
            artifact: None,
        };
        assert!(artifact.same_content(&matching));
        let changed = OwnedArtifact {
            size: artifact.size + 1,
            ..matching
        };
        assert!(!artifact.same_content(&changed));
    }

    #[tokio::test]
    async fn dedup_lookup_matches() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let mut record = JobRecord::new(
            JobId::from_bytes([3u8; 16]),
            payload.clone(),
            owner,
            node_id(),
            1,
            1,
            Some(user_dedup_key(owner, "key")),
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(1);
        insert_job(&storage, &record).await.unwrap();

        let found = lookup_job_dedup(&context, owner, "key", payload.plan_digest())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.job_id, record.job_id);
        assert!(!found.created);
        assert!(matches!(
            lookup_job_dedup(&context, owner, "key", [9u8; 32]).await,
            Err(SubmitJobError::JobPlanConflict { existing_job_id })
                if existing_job_id == record.job_id
        ));
    }

    #[test]
    fn artifact_uses_stem() {
        assert_eq!(
            artifact_filename(
                Some("datasets/experiment.crate"),
                Ulid::from_bytes([1u8; 16])
            ),
            "experiment.zip"
        );
    }

    #[tokio::test]
    async fn tombstone_preserves_gone() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let job_id = JobId::from_bytes([3u8; 16]);
        preserve_artifact_tombstone(&storage, job_id, owner, 10)
            .await
            .unwrap();

        assert!(matches!(
            read_owned_artifact(&context, owner, job_id, 9).await,
            Ok(ArtifactLookup::Gone)
        ));
        assert!(matches!(
            read_owned_artifact(
                &context,
                UserId::new(Ulid::from_bytes([4u8; 16]), RealmId([1u8; 32])),
                job_id,
                9,
            )
            .await,
            Ok(ArtifactLookup::NotFound)
        ));
        assert!(matches!(
            read_owned_artifact(&context, owner, job_id, 10).await,
            Ok(ArtifactLookup::NotFound)
        ));
    }

    #[tokio::test]
    async fn report_expiry_hides() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId([1u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
        let job_id = JobId::from_bytes([3u8; 16]);
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: AuthContext {
                    user_id: owner,
                    realm_id,
                    path_restrictions: None,
                },
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([4u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: String::new(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([5u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits: RoCrateLimits::default(),
                document_id: Ulid::from_bytes([6u8; 16]),
            }),
            owner,
            node_id(),
            1,
            1,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(1);
        record.retention_ms = 1;
        record.report_digest = Some([7u8; 32]);
        insert_job(&storage, &record).await.unwrap();

        assert!(matches!(
            read_owned_report(&context, owner, job_id, None, None, 1).await,
            Ok(JobReportLookup::NotFound)
        ));
    }

    #[tokio::test]
    async fn internal_access_hidden() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let runtime = JobsRuntime::new();
        for (job_id, payload) in [
            (
                JobId::from_bytes([0xC3; 16]),
                JobPayload::WriteRunCrate {
                    for_job: JobId::from_bytes([0xC4; 16]),
                },
            ),
            (
                JobId::from_bytes([0xC5; 16]),
                JobPayload::TerminalCleanup {
                    for_job: JobId::from_bytes([0xC6; 16]),
                    attempt: None,
                    access_key: "access".to_string(),
                },
            ),
        ] {
            let record = JobRecord::new(job_id, payload, owner, node_id(), 1_000, 1_000, None);
            insert_job(&storage, &record).await.unwrap();

            assert!(
                read_owned_job(&context, owner, job_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                cancel_owned_job(&context, &runtime, owner, job_id)
                    .await
                    .unwrap(),
                CancelJobOutcome::NotFound
            ));
            let stored = read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .unwrap();
            assert!(!stored.cancel_requested);
            assert_eq!(stored.state, JobState::Queued);
        }
    }
}
