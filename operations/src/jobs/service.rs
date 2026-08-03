use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ArtifactRef, AuthContext, DEFAULT_SHARD_COUNT, DocumentClass, ExecutionSpec, ExportRoCrateSpec,
    ImportRoCrateSpec, JOBCONTROL_HANDLE, JobError, JobId, JobPayload, JobProgress, JobRecord,
    JobResultPayload, JobState, PlacementScope, RealmId, RunCrateStatus, StagingJobSpec,
    WorkspaceMode, job_owner_cursor, shard_for_subject, user_dedup_key,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::task::TaskEvent;
use aruna_core::types::{NodeId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::ops::Range;
use std::path::Path;
use tracing::warn;

use super::JOB_REPORT_MAX_ROWS;
use super::protocol::{
    JobRequest, JobResponse, JobRouteError, WireRange, resolve_job_holders, resolve_user_route,
    send_job_request,
};
use super::runtime::JobsRuntime;
use super::store::{
    CancelRequestOutcome, JobMutationError, UserIndexError, UserIndexReservation, find_dedup_plan,
    list_job_entries, list_jobs_for_user, read_artifact_tombstone, read_job_record,
    read_run_crate_status, reserve_user_index, set_cancel_requested, update_user_index,
};
use super::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, mint_job_id,
    schedule_job_drain_effect,
};
use super::workflow::finalize_followups;
use crate::driver::{DriverContext, drive};
use crate::metadata::MetadataAuthToken;
use crate::metadata::api::load_realm_config;
use crate::metadata::repository::StorageReadError;
use crate::placement::choose_origin_bucket;

async fn mint_local_job(
    context: &DriverContext,
    realm_id: RealmId,
    owner_node_id: NodeId,
    dedup_key: Option<&[u8]>,
) -> Result<JobId, SubmitJobError> {
    let handle = PlacementHandle::new(JOBCONTROL_HANDLE)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let entropy = ulid::Ulid::generate().to_bytes();
    let subject = dedup_key.unwrap_or(&entropy);
    let Some(net_handle) = context.net_handle.as_ref() else {
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
    let tuple = config
        .binding_directory()
        .resolve(handle)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    if tuple.document_class != DocumentClass::JobControl
        || tuple.scope != PlacementScope::Realm(realm_id)
    {
        return Err(SubmitJobError::PlacementUnavailable(
            "reserved job-control binding does not match this realm".to_string(),
        ));
    }
    let strategy = config.strategy(&tuple.strategy_id).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job-control strategy unavailable".to_string())
    })?;
    let shard = match dedup_key {
        Some(key) => shard_for_subject(key, strategy.shard_count),
        None => {
            choose_origin_bucket(&config, strategy, owner_node_id, &entropy)
                .ok_or_else(|| {
                    SubmitJobError::PlacementUnavailable(
                        "serving node holds no job-control bucket".to_string(),
                    )
                })?
                .shard
        }
    };
    let bucket = u16::try_from(shard)
        .ok()
        .and_then(|shard| BucketId::new(shard).ok())
        .ok_or_else(|| {
            SubmitJobError::PlacementUnavailable("job bucket is out of range".to_string())
        })?;
    Ok(mint_job_id(handle, bucket)?)
}

fn map_index_error(error: UserIndexError) -> SubmitJobError {
    match error {
        UserIndexError::ActiveLimit { limit } => SubmitJobError::ActiveJobLimit { limit },
        UserIndexError::PlanConflict { existing_job_id } => {
            SubmitJobError::JobPlanConflict { existing_job_id }
        }
        error => SubmitJobError::UnexpectedEvent(error.to_string()),
    }
}

async fn reserve_job_index(
    context: &DriverContext,
    record: &JobRecord,
) -> Result<UserIndexReservation, SubmitJobError> {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return reserve_user_index(&context.storage_handle, record)
            .await
            .map_err(map_index_error);
    };
    let route = resolve_user_route(context, record.created_by)
        .await
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let holder = route.holders[0];
    if holder == net_handle.node_id() {
        return reserve_user_index(&context.storage_handle, record)
            .await
            .map_err(map_index_error);
    }
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id: record.created_by,
        realm_id: record.created_by.realm_id,
        path_restrictions: None,
    });
    let reply = send_job_request(
        context,
        holder,
        JobRequest::Index {
            auth_token,
            record: record.clone(),
            config_digest: route.config_digest,
        },
    )
    .await
    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    match reply.response {
        JobResponse::Indexed { job_id, created } => Ok(UserIndexReservation { job_id, created }),
        JobResponse::SubmitConflict(existing_job_id) => {
            Err(SubmitJobError::JobPlanConflict { existing_job_id })
        }
        JobResponse::SubmitCap(limit) => Err(SubmitJobError::ActiveJobLimit { limit }),
        JobResponse::Unavailable(error) => Err(SubmitJobError::UnexpectedEvent(error)),
        response => Err(SubmitJobError::UnexpectedEvent(format!(
            "unexpected user index response: {response:?}"
        ))),
    }
}

pub(crate) async fn submit_local_job(
    context: &DriverContext,
    mut spec: SubmitJobSpec,
    job_id: JobId,
) -> Result<SubmitJobResult, SubmitJobError> {
    let operation = if let Some(net_handle) = context.net_handle.as_ref() {
        spec.owner_node_id = net_handle.node_id();
        let preview = SubmitJobOperation::reserved(spec.clone(), job_id);
        let reservation = reserve_job_index(context, preview.record()).await?;
        // An equivalent job already exists on its immutable owner: reuse it and
        // never materialize a second record for the same JobId on this node.
        if !reservation.created {
            return Ok(SubmitJobResult {
                job_id: reservation.job_id,
                created: false,
            });
        }
        SubmitJobOperation::reserved(spec, reservation.job_id)
    } else {
        SubmitJobOperation::new(spec, job_id)
    };
    let result = drive(operation, context).await?;
    replicate_job_record(context, result.job_id).await;
    if result.created {
        kick_drain(context).await;
    }
    Ok(result)
}

async fn submit_job_routed(
    context: &DriverContext,
    spec: SubmitJobSpec,
    job_id: JobId,
) -> Result<SubmitJobResult, SubmitJobError> {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return submit_local_job(context, spec, job_id).await;
    };
    let route = resolve_job_holders(context, job_id)
        .await
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let runner = route.holders[0];
    if runner == net_handle.node_id() {
        return submit_local_job(context, spec, job_id).await;
    }
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id: spec.created_by,
        realm_id: spec.created_by.realm_id,
        path_restrictions: None,
    });
    let reply = send_job_request(
        context,
        runner,
        JobRequest::Submit {
            auth_token,
            job_id,
            spec,
            config_digest: route.config_digest,
        },
    )
    .await
    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    match reply.response {
        JobResponse::Submitted(result) => Ok(result),
        JobResponse::SubmitConflict(existing_job_id) => {
            Err(SubmitJobError::JobPlanConflict { existing_job_id })
        }
        JobResponse::SubmitCap(limit) => Err(SubmitJobError::ActiveJobLimit { limit }),
        JobResponse::Unauthorized => Err(SubmitJobError::UnexpectedEvent(
            "rank-0 job holder rejected forwarding authentication".to_string(),
        )),
        JobResponse::Forbidden => Err(SubmitJobError::UnexpectedEvent(
            "rank-0 job holder rejected the forwarded principal".to_string(),
        )),
        JobResponse::Unavailable(error) => Err(SubmitJobError::UnexpectedEvent(error)),
        response => Err(SubmitJobError::UnexpectedEvent(format!(
            "unexpected submit response: {response:?}"
        ))),
    }
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
    submit_job_routed(
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
    submit_job_routed(
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
    submit_job_routed(
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
    submit_job_routed(
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

/// API-facing helpers so REST handlers never orchestrate storage/task effects directly.
pub async fn list_owned_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    if limit == 0 || context.net_handle.is_none() {
        return list_jobs_for_user(&context.storage_handle, user_id, cursor, limit, filter).await;
    }
    let route = resolve_user_route(context, user_id)
        .await
        .map_err(|error| error.to_string())?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let holder = route.holders[0];
    if Some(holder) == local_node {
        return list_jobs_for_user(&context.storage_handle, user_id, cursor, limit, filter).await;
    }
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id,
        realm_id: user_id.realm_id,
        path_restrictions: None,
    });
    let batch_limit = u16::try_from(limit.min(usize::from(u16::MAX))).unwrap_or(u16::MAX);
    let mut scan_cursor = cursor;
    let mut records = Vec::new();
    let mut page_cursor = None;
    loop {
        let reply = send_job_request(
            context,
            holder,
            JobRequest::List {
                auth_token: auth_token.clone(),
                user_id,
                cursor: scan_cursor,
                limit: batch_limit,
                config_digest: route.config_digest,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
        let (page, next_cursor) = match reply.response {
            JobResponse::Listed {
                records,
                next_cursor,
            } => (records, next_cursor),
            JobResponse::Unauthorized => return Err("job listing is unauthorized".to_string()),
            JobResponse::Forbidden => return Err("job listing is forbidden".to_string()),
            JobResponse::Unavailable(error) => return Err(error),
            response => return Err(format!("unexpected job listing response: {response:?}")),
        };
        for record in page {
            if record.created_by != user_id || record.payload.is_internal() || !filter(&record) {
                continue;
            }
            if records.len() == limit {
                return Ok((records, page_cursor));
            }
            let cursor = job_owner_cursor(record.created_at_ms, record.job_id);
            records.push(record);
            if records.len() == limit {
                page_cursor = Some(cursor);
            }
        }
        let Some(next_cursor) = next_cursor else {
            return Ok((records, None));
        };
        scan_cursor = Some(next_cursor);
    }
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

pub(crate) async fn replicate_job_record(context: &DriverContext, job_id: JobId) {
    let record = match read_job_record(&context.storage_handle, job_id, None).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            warn!(job_id = %job_id, "Cannot replicate a missing job record");
            return;
        }
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Failed to read job for replication");
            return;
        }
    };
    if record.state != JobState::Queued && !record.state.is_terminal() {
        return;
    }
    sync_user_record(context, &record).await;
    let route = match resolve_job_holders(context, job_id).await {
        Ok(route) => route,
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Failed to resolve job replicas");
            return;
        }
    };
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id: record.created_by,
        realm_id: record.created_by.realm_id,
        path_restrictions: None,
    });
    for holder in route.holders {
        if Some(holder) == local_node {
            continue;
        }
        match send_job_request(
            context,
            holder,
            JobRequest::Replicate {
                auth_token: auth_token.clone(),
                record: record.clone(),
                config_digest: route.config_digest,
            },
        )
        .await
        {
            Ok(reply) if matches!(reply.response, JobResponse::Replicated) => {}
            Ok(reply) => {
                warn!(job_id = %job_id, %holder, response = ?reply.response, "Job replica was rejected")
            }
            Err(error) => {
                warn!(job_id = %job_id, %holder, error = %error, "Failed to replicate job record")
            }
        }
    }
}

async fn sync_user_record(context: &DriverContext, record: &JobRecord) {
    if record.payload.is_internal() {
        return;
    }
    let route = match resolve_user_route(context, record.created_by).await {
        Ok(route) => route,
        Err(error) => {
            warn!(job_id = %record.job_id, error = %error, "Failed to resolve job owner index");
            return;
        }
    };
    let holder = route.holders[0];
    if context
        .net_handle
        .as_ref()
        .is_some_and(|net| net.node_id() == holder)
    {
        if let Err(error) = update_user_index(&context.storage_handle, record).await {
            warn!(job_id = %record.job_id, error = %error, "Failed to update job owner index");
        }
        return;
    }
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id: record.created_by,
        realm_id: record.created_by.realm_id,
        path_restrictions: None,
    });
    match send_job_request(
        context,
        holder,
        JobRequest::Index {
            auth_token,
            record: record.clone(),
            config_digest: route.config_digest,
        },
    )
    .await
    {
        Ok(reply) if matches!(reply.response, JobResponse::Indexed { .. }) => {}
        Ok(reply) => {
            warn!(job_id = %record.job_id, response = ?reply.response, "Job owner index was rejected")
        }
        Err(error) => {
            warn!(job_id = %record.job_id, error = %error, "Failed to update remote job owner index")
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    Probe,
    Execution,
    WriteRunCrate,
    TerminalCleanup,
    Staging,
    ImportRoCrate,
    ExportRoCrate,
}

impl JobKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Execution => "execution",
            Self::WriteRunCrate => "write_run_crate",
            Self::TerminalCleanup => "terminal_cleanup",
            Self::Staging => "staging",
            Self::ImportRoCrate => "import_rocrate",
            Self::ExportRoCrate => "export_rocrate",
        }
    }

    fn is_internal(self) -> bool {
        matches!(self, Self::WriteRunCrate | Self::TerminalCleanup)
    }

    fn is_report(self) -> bool {
        matches!(self, Self::ImportRoCrate | Self::ExportRoCrate)
    }
}

impl From<&JobPayload> for JobKind {
    fn from(payload: &JobPayload) -> Self {
        match payload {
            JobPayload::Probe { .. } => Self::Probe,
            JobPayload::Execution(_) => Self::Execution,
            JobPayload::WriteRunCrate { .. } => Self::WriteRunCrate,
            JobPayload::TerminalCleanup { .. } => Self::TerminalCleanup,
            JobPayload::Staging(_) => Self::Staging,
            JobPayload::ImportRoCrate(_) => Self::ImportRoCrate,
            JobPayload::ExportRoCrate(_) => Self::ExportRoCrate,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobStatusView {
    pub job_id: JobId,
    pub created_by: UserId,
    pub kind: JobKind,
    pub state: JobState,
    pub attempts: u32,
    pub cancel_requested: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub progress: JobProgress,
    pub last_error: Option<JobError>,
    #[serde(with = "json_value")]
    pub result: Option<JsonValue>,
    pub workspace_bucket: Option<String>,
    pub workspace_mode: WorkspaceMode,
}

mod json_value {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::Value;

    pub fn serialize<S>(value: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_ref().map(Value::to_string).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|value| serde_json::from_str(&value).map_err(serde::de::Error::custom))
            .transpose()
    }
}

impl From<&JobRecord> for JobStatusView {
    fn from(record: &JobRecord) -> Self {
        Self {
            job_id: record.job_id,
            created_by: record.created_by,
            kind: JobKind::from(&record.payload),
            state: record.state,
            attempts: record.attempts,
            cancel_requested: record.cancel_requested,
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
            finished_at_ms: record.finished_at_ms,
            progress: record.progress.clone(),
            last_error: record.last_error.clone(),
            result: record.result.as_ref().map(JobResultPayload::to_public_json),
            workspace_bucket: record.workspace_bucket.clone(),
            workspace_mode: record.workspace_mode,
        }
    }
}

pub struct RoutedJobStatus {
    pub job: JobStatusView,
    pub run_crate: Option<JsonValue>,
}

pub async fn read_job_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<RoutedJobStatus, JobRouteError> {
    if context.net_handle.is_none() {
        let record = read_owned_job(context, user_id, job_id)
            .await
            .map_err(JobRouteError::Internal)?
            .ok_or(JobRouteError::NotFound)?;
        let run_crate = read_job_run_crate_status(context, job_id)
            .await
            .map_err(JobRouteError::Internal)?
            .map(|status| status.to_public_json());
        return Ok(RoutedJobStatus {
            job: JobStatusView::from(&record),
            run_crate,
        });
    }
    let route = resolve_job_holders(context, job_id).await?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut not_found = 0usize;
    let mut freshest = None;
    for holder in &route.holders {
        if Some(*holder) == local_node {
            let record = match read_owned_job(context, user_id, job_id).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    not_found += 1;
                    continue;
                }
                Err(_) => continue,
            };
            let run_crate = match read_job_run_crate_status(context, job_id).await {
                Ok(status) => status.map(|status| status.to_public_json()),
                Err(_) => continue,
            };
            let candidate = RoutedJobStatus {
                job: JobStatusView::from(&record),
                run_crate,
            };
            if freshest.as_ref().is_none_or(|current: &RoutedJobStatus| {
                candidate.job.updated_at_ms > current.job.updated_at_ms
            }) {
                freshest = Some(candidate);
            }
            continue;
        }
        let token = auth_token.clone().ok_or(JobRouteError::Unauthorized)?;
        let reply = match send_job_request(
            context,
            *holder,
            JobRequest::Status {
                auth_token: token,
                job_id,
                config_digest: route.config_digest,
            },
        )
        .await
        {
            Ok(reply) => reply,
            Err(_) => continue,
        };
        match reply.response {
            JobResponse::Status { job, run_crate } if routed_job_matches(&job, user_id, job_id) => {
                let Ok(run_crate) = run_crate
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                else {
                    continue;
                };
                let candidate = RoutedJobStatus { job, run_crate };
                if freshest.as_ref().is_none_or(|current: &RoutedJobStatus| {
                    candidate.job.updated_at_ms > current.job.updated_at_ms
                }) {
                    freshest = Some(candidate);
                }
            }
            JobResponse::Unauthorized => return Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => return Err(JobRouteError::Forbidden),
            JobResponse::NotFound => not_found += 1,
            _ => {}
        }
    }
    if let Some(freshest) = freshest {
        return Ok(freshest);
    }
    route_miss(not_found, route.holders.len())
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobReportView {
    pub job_id: JobId,
    pub created_by: UserId,
    pub kind: JobKind,
    pub report_digest: [u8; 32],
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
    if context.net_handle.is_none() {
        return read_owned_report(context, user_id, job_id, expected_digest, last_key, limit)
            .await
            .map_err(JobRouteError::Internal);
    }
    let route = resolve_job_holders(context, job_id).await?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut not_found = 0usize;
    for holder in &route.holders {
        if Some(*holder) == local_node {
            match read_owned_report(
                context,
                user_id,
                job_id,
                expected_digest,
                last_key.clone(),
                limit,
            )
            .await
            {
                Ok(JobReportLookup::NotFound) => {
                    not_found += 1;
                    continue;
                }
                Ok(lookup) => return Ok(lookup),
                Err(_) => continue,
            }
        }
        let token = auth_token.clone().ok_or(JobRouteError::Unauthorized)?;
        let wire_limit = u16::try_from(limit.min(usize::from(JOB_REPORT_MAX_ROWS)))
            .map_err(|error| JobRouteError::Internal(error.to_string()))?;
        let reply = match send_job_request(
            context,
            *holder,
            JobRequest::Report {
                auth_token: token,
                job_id,
                expected_digest,
                last_key: last_key.clone(),
                limit: wire_limit,
                config_digest: route.config_digest,
            },
        )
        .await
        {
            Ok(reply) => reply,
            Err(_) => continue,
        };
        match reply.response {
            JobResponse::ReportPending(state) => return Ok(JobReportLookup::Pending(state)),
            JobResponse::ReportConflict => return Ok(JobReportLookup::CursorConflict),
            JobResponse::ReportReady {
                job,
                rows,
                next_key,
            } if report_job_matches(&job, user_id, job_id, expected_digest) => {
                return Ok(JobReportLookup::Ready {
                    job,
                    rows: rows
                        .into_iter()
                        .map(|(key, value)| (key, Value::from(value)))
                        .collect(),
                    next_key,
                });
            }
            JobResponse::Unauthorized => return Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => return Err(JobRouteError::Forbidden),
            JobResponse::NotFound => not_found += 1,
            _ => {}
        }
    }
    route_miss(not_found, route.holders.len())
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
    let route = resolve_job_holders(context, job_id).await?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut not_found = 0usize;
    for holder in &route.holders {
        if Some(*holder) == local_node {
            let lookup = match read_owned_artifact(context, user_id, job_id, now_ms).await {
                Ok(lookup) => lookup,
                Err(_) => continue,
            };
            if matches!(lookup, ArtifactLookup::NotFound) {
                not_found += 1;
                continue;
            }
            let read = match (&lookup, range.clone()) {
                (ArtifactLookup::Ready(owned), Some(range)) => {
                    let Some(artifact) = owned.source() else {
                        continue;
                    };
                    let Ok(read) = read_artifact_range(context, artifact, range.clone()).await
                    else {
                        continue;
                    };
                    if !artifact_size_matches(Some(&range), read.stream_size) {
                        continue;
                    }
                    Some(read)
                }
                _ => None,
            };
            return Ok((lookup, read));
        }
        let token = auth_token.clone().ok_or(JobRouteError::Unauthorized)?;
        let reply = match send_job_request(
            context,
            *holder,
            JobRequest::Artifact {
                auth_token: token,
                job_id,
                range: range.clone().map(WireRange::from),
                config_digest: route.config_digest,
            },
        )
        .await
        {
            Ok(reply) => reply,
            Err(_) => continue,
        };
        match reply.response {
            JobResponse::ArtifactPending(state) => {
                return Ok((ArtifactLookup::Pending(state), None));
            }
            JobResponse::ArtifactGone => return Ok((ArtifactLookup::Gone, None)),
            JobResponse::ArtifactReady { owned, stream_size } => {
                let owned = OwnedArtifact::from(owned);
                if !artifact_job_matches(&owned, user_id, job_id) {
                    continue;
                }
                let read = match (range.as_ref(), reply.body) {
                    (Some(range), Some(blob))
                        if artifact_size_matches(Some(range), stream_size) =>
                    {
                        Some(ArtifactRead { blob, stream_size })
                    }
                    (None, None) if artifact_size_matches(None, stream_size) => None,
                    _ => continue,
                };
                return Ok((ArtifactLookup::Ready(owned), read));
            }
            JobResponse::Unauthorized => return Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => return Err(JobRouteError::Forbidden),
            JobResponse::NotFound => not_found += 1,
            _ => {}
        }
    }
    route_miss(not_found, route.holders.len())
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
            if can_replicate_terminal(context, &record).await {
                replicate_job_record(context, job_id).await;
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

async fn can_replicate_terminal(context: &DriverContext, record: &JobRecord) -> bool {
    let Some(local_node) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return false;
    };
    let runner = record
        .claim
        .as_ref()
        .map_or(record.owner_node_id, |claim| claim.holder_node_id);
    if runner == local_node {
        return true;
    }
    resolve_job_holders(context, record.job_id)
        .await
        .is_ok_and(|route| {
            route.holders.first().copied() == Some(local_node) && !route.holders.contains(&runner)
        })
}

pub async fn cancel_job_routed(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<crate::metadata::MetadataAuthToken>,
) -> Result<RoutedCancelOutcome, JobRouteError> {
    if context.net_handle.is_none() {
        return cancel_owned_job(context, runtime, user_id, job_id)
            .await
            .map(|outcome| match outcome {
                CancelJobOutcome::NotFound => RoutedCancelOutcome::NotFound,
                CancelJobOutcome::AlreadyTerminal(record) => {
                    RoutedCancelOutcome::AlreadyTerminal(JobStatusView::from(&record))
                }
                CancelJobOutcome::Requested(record) => {
                    RoutedCancelOutcome::Requested(JobStatusView::from(&record))
                }
            })
            .map_err(JobRouteError::Internal);
    }
    let route = resolve_job_holders(context, job_id).await?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut not_found = 0usize;
    let mut freshest = None;
    for holder in &route.holders {
        if Some(*holder) == local_node {
            match cancel_owned_job(context, runtime, user_id, job_id).await {
                Ok(CancelJobOutcome::NotFound) => {
                    not_found += 1;
                    continue;
                }
                Ok(CancelJobOutcome::AlreadyTerminal(record)) => {
                    let job = JobStatusView::from(&record);
                    if freshest
                        .as_ref()
                        .is_none_or(|(current, _): &(JobStatusView, bool)| {
                            job.updated_at_ms > current.updated_at_ms
                        })
                    {
                        freshest = Some((job, true));
                    }
                    continue;
                }
                Ok(CancelJobOutcome::Requested(record)) => {
                    let job = JobStatusView::from(&record);
                    if freshest
                        .as_ref()
                        .is_none_or(|(current, _): &(JobStatusView, bool)| {
                            job.updated_at_ms > current.updated_at_ms
                        })
                    {
                        freshest = Some((job, false));
                    }
                    continue;
                }
                Err(_) => continue,
            }
        }
        let token = auth_token.clone().ok_or(JobRouteError::Unauthorized)?;
        let reply = match send_job_request(
            context,
            *holder,
            JobRequest::Cancel {
                auth_token: token,
                job_id,
                config_digest: route.config_digest,
            },
        )
        .await
        {
            Ok(reply) => reply,
            Err(_) => continue,
        };
        match reply.response {
            JobResponse::Cancelled { job, terminal }
                if routed_job_matches(&job, user_id, job_id) =>
            {
                if freshest
                    .as_ref()
                    .is_none_or(|(current, _): &(JobStatusView, bool)| {
                        job.updated_at_ms > current.updated_at_ms
                    })
                {
                    freshest = Some((job, terminal));
                }
            }
            JobResponse::Unauthorized => return Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => return Err(JobRouteError::Forbidden),
            JobResponse::NotFound => not_found += 1,
            _ => {}
        }
    }
    if let Some((job, terminal)) = freshest {
        return Ok(if terminal {
            RoutedCancelOutcome::AlreadyTerminal(job)
        } else {
            RoutedCancelOutcome::Requested(job)
        });
    }
    route_miss(not_found, route.holders.len())
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

fn route_miss<T>(not_found: usize, holder_count: usize) -> Result<T, JobRouteError> {
    if holder_count > 0 && not_found == holder_count {
        Err(JobRouteError::NotFound)
    } else {
        Err(JobRouteError::Unavailable(
            "no job-control holder returned an authoritative response".to_string(),
        ))
    }
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
    fn miss_requires_all() {
        assert!(matches!(
            route_miss::<()>(3, 3),
            Err(JobRouteError::NotFound)
        ));
        assert!(matches!(
            route_miss::<()>(2, 3),
            Err(JobRouteError::Unavailable(_))
        ));
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
