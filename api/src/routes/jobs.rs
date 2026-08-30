use std::collections::BTreeMap;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use aruna_core::compute::normalize_container_path;
use aruna_core::scheduling::MAX_PLAN_INPUTS;
use aruna_core::structs::{
    AuthContext, CollisionPolicy, CompositionError, ComputeResources, ExecutionSpec,
    ExportReportRow, ImportReportRow, InputMode, InputSelection, InputSource,
    JOB_SYSTEM_ENTRY_PREFIX, JobId, JobRecord, JobState, MAX_EXECUTION_OUTPUTS, NodeCapabilities,
    Permission, WorkspaceMode, WorkspaceOutput, blob_bucket_permission_path,
    blob_group_permission_path,
};
use aruna_core::types::NodeId;
use aruna_operations::device::compute::{
    LocalExecutionConfig, LocalExecutionError, submit_local_execution,
};
use aruna_operations::jobs::lifecycle::{FamilyReport, family_report, submit_external_job};
use aruna_operations::jobs::service::{
    ArtifactLookup, JobKind, JobReportLookup, JobStatusView, OwnedArtifact, RoutedCancelOutcome,
    cancel_job_routed, list_owned_jobs, read_artifact_routed, read_job_routed, read_owned_job,
    read_report_routed,
};
use aruna_operations::jobs::{JOB_REPORT_MAX_ROWS, JobRouteError};
use aruna_operations::request_policy::PolicyRequestExtras;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_object::ObjectRangeRequest;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, RANGE,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedArunaBearerTokenCarrier, require_unrestricted_realm_auth};
use crate::download::{self, AdmissionError};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::rate_limit::LocalKey;
use crate::routes::device::require_owner;
use crate::server_state::ServerState;
use aruna_operations::driver::drive;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 200;
const DEFAULT_REPORT_LIMIT: usize = 200;
const MAX_OUTPUT_PREFIXES: usize = 32;

#[derive(OpenApi)]
#[openapi(
    tags((name = "jobs", description = "Durable background jobs"))
)]
pub struct JobsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(JobsApiDoc::openapi())
        .routes(routes!(list_jobs, submit_job))
        .routes(routes!(get_job))
        .routes(routes!(cancel_job))
        .routes(routes!(get_job_report))
        .routes(routes!(get_job_artifact, head_job_artifact))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
pub struct ExecutionInputRequest {
    /// Source bucket holding the object, for example `project-data`.
    pub bucket: String,
    /// Source object key inside `bucket`, for example `inputs/reads.fastq.gz`.
    /// A relative key without a leading slash and without `..` segments.
    pub key: String,
    /// Exact object version to stage. Required by `exact_reference` mode and
    /// refused by `floating_reference` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    /// Realm node holding this object. Only a `local` run accepts it: the named
    /// version is copied onto the device before the run and `version_id` is
    /// then required. A realm run refuses it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
    /// Destination key inside the workspace bucket, for example `reads.fastq.gz`.
    /// Must not be empty and must be unique across the declared inputs.
    pub dest_key: String,
    /// Absolute container path; defaults to `/inputs/<dest_key>`. It must be
    /// absolute, must not be `/`, must carry no `.` or `..` component, and must
    /// be unique across the declared inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_path: Option<String>,
    /// Composition mode; defaults to `snapshot`.
    #[serde(default)]
    pub mode: InputModeRequest,
}

/// Per-input composition mode. `exact_reference` requires `version_id`,
/// `floating_reference` rejects it.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum InputModeRequest {
    #[default]
    Snapshot,
    FloatingReference,
    ExactReference,
}

/// How a claimed destination key is resolved. `reject` refuses the submission,
/// `replace` lets the later declaration win, and `keep_existing` keeps the first.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CollisionPolicyRequest {
    #[default]
    Reject,
    Replace,
    KeepExisting,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
pub struct ExecutionOutputRequest {
    /// Absolute container path captured after the task exits, for example
    /// `/work/result.json`. Must be unique across the declared outputs.
    pub container_path: String,
    /// Destination key inside the workspace bucket, for example
    /// `results/result.json`. Must not be empty and must be unique.
    pub dest_key: String,
}

/// Where a run's staged inputs and captured outputs live. `temporary` discards
/// the workspace after the run, `kept` retains it in a new bucket, and
/// `existing` reuses the named bucket.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceModeRequest {
    Temporary,
    Kept,
    Existing,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
pub struct WorkspaceRequest {
    /// `temporary` discards the workspace after the run, `kept` retains it in a
    /// new bucket, and `existing` reuses the named bucket.
    pub mode: WorkspaceModeRequest,
    /// Required by `existing` mode and refused by the other two. The bucket must
    /// exist, belong to the same group, and be writable by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

/// Where a submission runs. `local` is served by a user device only, and runs
/// the job on that machine for its owner.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    ToSchema,
    rmcp::schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionTarget {
    #[default]
    Realm,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
pub struct SubmitExecutionRequest {
    /// Owning group's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. The caller needs write permission on it.
    pub group_id: String,
    /// OCI image the task runs, for example `docker.io/library/python:3.13-slim`.
    /// Must not be blank.
    pub image: String,
    /// Replaces the image ENTRYPOINT. Omit to keep the image default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    /// Argument vector appended after the entrypoint, for example
    /// `["python", "/work/script.py"]`. Defaults to empty.
    #[serde(default)]
    pub command: Vec<String>,
    /// Environment variables for the task. Defaults to empty.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Scheduling tags. `aruna-engine.org/label/<key>` demands a matching target
    /// label, at most 16 of them. The workspace tags of that namespace are
    /// reserved and refused. Defaults to empty.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Absolute working directory inside the container, for example `/work`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Whole CPU cores reserved. Defaults to 1; `0` is refused and the group's
    /// compute quota may cap it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    /// RAM reserved in bytes, for example `1073741824` for 1 GiB. Defaults to
    /// 1 GiB; `0` and anything above 9223372036854775807 are refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_bytes: Option<u64>,
    /// Wall-clock limit in milliseconds, for example `600000` for ten minutes.
    /// Defaults to 86400000, one day; the group's compute quota may cap it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_walltime_ms: Option<u64>,
    /// Optional executor selector. Leave unset unless the realm documents one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_constraint: Option<String>,
    /// Objects staged into the container before the run, at most 512.
    #[serde(default)]
    pub inputs: Vec<ExecutionInputRequest>,
    /// Container paths captured into the workspace bucket after the run, at
    /// most 1024.
    #[serde(default)]
    pub outputs: Vec<ExecutionOutputRequest>,
    #[serde(default)]
    /// Workspace prefixes to inventory at completion. Only objects this
    /// execution itself wrote under a prefix are reported: a version another
    /// writer produced is never attributed to this job. This route declares no
    /// file outputs, so a non-empty list is refused; leave it empty.
    pub output_prefixes: Vec<String>,
    /// How a destination key already claimed by another declared input or by an
    /// object in the workspace bucket is resolved. Defaults to `reject`.
    #[serde(default)]
    pub collision_policy: CollisionPolicyRequest,
    /// Caller-chosen key that makes a resubmission return the same job instead
    /// of starting a second one. A different request under a used key is a 409.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Where staged inputs and captured outputs live. Absent means a kept
    /// workspace in a new bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceRequest>,
    /// `realm` (the default) admits the job into the realm; `local` runs it on
    /// this machine and is served by a user device only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<ExecutionTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitJobResponse {
    /// The alias this responder bound the request to. Stable for the caller.
    pub job_id: String,
    pub created: bool,
    /// The replicated identity of the request itself, hex encoded. Two aliases
    /// of one request always share it. Absent for a local run, which is never
    /// replicated and belongs to no submission family.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
    /// The alias the responder currently reduces as canonical. It may change
    /// once a partitioned lower claim is learned; `job_id` never does.
    pub canonical_job_id: String,
    /// The family's state at this accept: `queued` for a fresh admission, and
    /// what the responder currently reduces for an idempotent replay, so a
    /// replay of a running or finished request reports that instead. It is a
    /// point-in-time value; poll `status_url` for the live state.
    pub state: String,
    /// Preferred route, not an owner: any node that reduced the family answers.
    pub origin_node_url: String,
    pub status_url: String,
}

/// One object exactly one execution wrote, named by its own VersionId.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobOutputResponse {
    pub bucket: String,
    pub key: String,
    /// The exact version this execution created; the object's latest version
    /// may be a later, unrelated write.
    pub version_id: String,
    pub execution_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub container_path: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Node-local S3 endpoint owning this exact version. Use it with the
    /// bucket, key, and version_id above; the responder is not necessarily the
    /// execution node. Null when this responder does not yet know the owning
    /// node's endpoint, which never withholds the rest of the output.
    pub endpoint_url: Option<String>,
}

/// The plan this responder sealed when it planned the request itself. Absent
/// when another node planned it; node identities are never disclosed here.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobPlacementResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_kind: Option<String>,
    pub estimated_transfer_bytes: u64,
    pub estimated_transfer_ms: u64,
    pub alternatives: u32,
    pub rejected: u32,
    /// Rejections the bound dropped, so truncation never reads as agreement.
    pub omitted: u32,
    pub sealed_at_ms: u64,
}

/// The replicated family behind one external job, as this responder reduces it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobFamilyResponse {
    pub submission_id: String,
    pub request_digest: String,
    pub canonical_job_id: String,
    pub aliases: Vec<String>,
    pub alias_count: u32,
    /// Other request families of the same submission: idempotency conflicts a
    /// partition may have accepted elsewhere. Counted from a bounded scan, so a
    /// very large family may understate it.
    pub conflict_count: u32,
    pub logical_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_execution_id: Option<String>,
    pub executions: u32,
    pub duplicate_successes: u32,
    pub outputs: Vec<JobOutputResponse>,
    pub revision: u64,
    pub projection_digest: String,
    /// Always true: the family is replicated, so this is one node's view.
    pub eventually_consistent: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responder_node_id: Option<String>,
    /// The family holds more records than one projection may reduce.
    pub partial: bool,
    /// Responder-local diagnostic, outside the projection digest: every known
    /// execution is terminal without success and no retry is armed here. It is
    /// not evidence of a permanent failure.
    pub locally_exhausted: bool,
    pub cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<JobPlacementResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobUrls {
    pub owner_node_url: String,
    pub status_url: String,
    pub report_url: String,
    pub artifact_url: String,
}

pub async fn job_urls(state: &ServerState, job_id: JobId) -> ServerResult<JobUrls> {
    let interface = state.interface_state().await;
    let rest = interface.rest.ok_or_else(|| {
        ServerError::InternalError("REST interface public URL is unavailable".to_string())
    })?;
    let api_base_url = rest.api_base_url.trim_end_matches('/');
    Ok(JobUrls {
        owner_node_url: rest.api_base_url.clone(),
        status_url: format!("{api_base_url}/jobs/{job_id}"),
        report_url: format!("{api_base_url}/jobs/{job_id}/report"),
        artifact_url: format!("{api_base_url}/jobs/{job_id}/artifacts/rocrate"),
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListJobsQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ReportQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobReportResponse {
    pub rows: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub report_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReportPendingResponse {
    pub code: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ReportUnavailableResponse {
    Pending(ReportPendingResponse),
    NotFound(ErrorResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportCursor {
    job_id: JobId,
    report_digest: [u8; 32],
    last_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobProgressResponse {
    pub current: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobErrorResponse {
    pub message: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub kind: String,
    pub state: String,
    pub attempts: u32,
    pub cancel_requested: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    pub progress: JobProgressResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JobErrorResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_bucket: Option<String>,
    pub workspace_mode: String,
    /// This node spent its attempts without a job-specific verdict: no further
    /// automatic attempt runs here and the outcome is not a proven failure.
    #[serde(default)]
    pub locally_exhausted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_crate: Option<serde_json::Value>,
    /// Present only for a distributed external job, whose truth is the
    /// replicated family rather than this node's own row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family: Option<JobFamilyResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobListResponse {
    pub jobs: Vec<JobStatusResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

pub(crate) fn job_status_response(record: &JobRecord) -> JobStatusResponse {
    job_view_response(&JobStatusView::from(record))
}

pub(crate) fn job_view_response(job: &JobStatusView) -> JobStatusResponse {
    JobStatusResponse {
        job_id: job.job_id.to_string(),
        kind: job.kind.name().to_string(),
        state: job.state.name().to_string(),
        attempts: job.attempts,
        cancel_requested: job.cancel_requested,
        created_at: rfc3339(job.created_at_ms),
        updated_at: rfc3339(job.updated_at_ms),
        finished_at: job.finished_at_ms.map(rfc3339),
        progress: JobProgressResponse {
            current: job.progress.current,
            total: job.progress.total,
            unit: job.progress.unit.clone(),
        },
        error: job.last_error.as_ref().map(|error| JobErrorResponse {
            message: error.message.clone(),
            kind: error.kind.name().to_string(),
        }),
        result: job.result.clone(),
        workspace_bucket: job.workspace_bucket.clone(),
        workspace_mode: job.workspace_mode.name().to_string(),
        locally_exhausted: job.locally_exhausted,
        run_crate: None,
        family: None,
    }
}

pub(crate) fn hex32(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn output_response(
    output: &aruna_core::structs::OutputObject,
    endpoint_url: Option<&String>,
) -> JobOutputResponse {
    JobOutputResponse {
        bucket: output.bucket.clone(),
        key: output.key.clone(),
        version_id: output.version_id.to_string(),
        execution_id: output.execution_id.to_string(),
        container_path: output.container_path.clone(),
        size: output.size,
        digest: output.digest.clone(),
        endpoint_url: endpoint_url.cloned(),
    }
}

/// Projects the reduced family without disclosing node identities.
pub(crate) fn family_response(report: &FamilyReport) -> JobFamilyResponse {
    // A missing endpoint only leaves that output unaddressable; the succeeded
    // family stays readable on any responder.
    let outputs = report
        .outputs
        .iter()
        .map(|output| output_response(output, report.output_endpoints.get(&output.node_id)))
        .collect::<Vec<_>>();
    JobFamilyResponse {
        submission_id: hex32(&report.submission_id.0),
        request_digest: hex32(&report.request_digest),
        canonical_job_id: report.canonical_job_id.to_string(),
        aliases: report.aliases.iter().map(JobId::to_string).collect(),
        alias_count: report.aliases.len() as u32,
        conflict_count: report.conflicts,
        logical_state: report.state.name().to_string(),
        canonical_execution_id: report
            .canonical_execution_id
            .map(|execution| execution.to_string()),
        executions: report.executions,
        duplicate_successes: report.duplicate_successes,
        outputs,
        revision: report.revision,
        projection_digest: hex32(&report.digest),
        eventually_consistent: true,
        responder_node_id: report.responder.map(|node| node.to_string()),
        partial: report.partial,
        locally_exhausted: report.locally_exhausted,
        cancel_requested: report.cancel_requested,
        placement: report.plan.as_ref().map(|plan| JobPlacementResponse {
            executor_kind: plan
                .target
                .as_ref()
                .map(|target| target.executor_kind.clone()),
            estimated_transfer_bytes: plan.estimated_transfer_bytes,
            estimated_transfer_ms: plan.estimated_transfer_ms,
            alternatives: plan.alternatives,
            rejected: plan.rejected,
            omitted: plan.omitted,
            sealed_at_ms: plan.sealed_at_ms,
        }),
    }
}

pub(crate) fn bind_output_routes(
    result: &mut Option<serde_json::Value>,
    outputs: &[JobOutputResponse],
) -> Result<(), JobRouteError> {
    let Some(serde_json::Value::Object(result)) = result else {
        return Ok(());
    };
    let outputs = serde_json::to_value(outputs)
        .map_err(|error| JobRouteError::Internal(error.to_string()))?;
    result.insert("outputs".to_string(), outputs);
    Ok(())
}

pub(crate) fn parse_state(value: &str) -> ServerResult<JobState> {
    match value {
        "queued" => Ok(JobState::Queued),
        "claimed" => Ok(JobState::Claimed),
        "preparing" => Ok(JobState::Preparing),
        "ready" => Ok(JobState::Ready),
        "running" => Ok(JobState::Running),
        "cancelling" => Ok(JobState::Cancelling),
        "indeterminate" => Ok(JobState::Indeterminate),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "cancelled" => Ok(JobState::Cancelled),
        _ => Err(ServerError::BadRequest),
    }
}

pub(crate) fn decode_cursor(cursor: Option<&str>) -> ServerResult<Option<Vec<u8>>> {
    match cursor {
        Some(cursor) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)?;
            if bytes.len() != 24 {
                return Err(ServerError::BadRequest);
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

pub(crate) fn encode_cursor(cursor: Option<Vec<u8>>) -> Option<String> {
    cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor))
}

fn decode_report_cursor(cursor: Option<&str>) -> ServerResult<Option<ReportCursor>> {
    cursor
        .map(|cursor| {
            URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)
                .and_then(|bytes| postcard::from_bytes(&bytes).map_err(|_| ServerError::BadRequest))
        })
        .transpose()
}

fn encode_report_cursor(
    job_id: JobId,
    report_digest: [u8; 32],
    last_key: Option<Vec<u8>>,
) -> ServerResult<Option<String>> {
    last_key
        .map(|last_key| {
            postcard::to_allocvec(&ReportCursor {
                job_id,
                report_digest,
                last_key,
            })
            .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
            .map_err(|error| ServerError::InternalError(error.to_string()))
        })
        .transpose()
}

pub(crate) fn parse_job_id(raw: &str) -> ServerResult<JobId> {
    JobId::from_str(raw).map_err(|_| ServerError::NotFound)
}

pub(crate) fn forwarded_job_auth(
    bearer: Option<ValidatedArunaBearerTokenCarrier>,
) -> ServerResult<Option<aruna_operations::metadata::MetadataAuthToken>> {
    aruna_operations::metadata::api::forwarded_bearer(
        bearer
            .as_ref()
            .map(ValidatedArunaBearerTokenCarrier::as_str),
    )
    .map_err(super::metadata::map_metadata_api_error)
}

pub(crate) fn map_job_route(error: JobRouteError) -> ServerError {
    match error {
        JobRouteError::Unauthorized => ServerError::Unauthorized,
        JobRouteError::Forbidden => ServerError::Forbidden,
        JobRouteError::NotFound => ServerError::NotFound,
        JobRouteError::Unavailable(_) => {
            ServerError::ServiceUnavailableReason("job_read_unavailable".to_string())
        }
        JobRouteError::Internal(error) => ServerError::InternalError(error),
    }
}

pub(crate) fn map_submit_error(
    error: aruna_operations::jobs::submit::SubmitJobError,
) -> ServerError {
    use aruna_operations::jobs::submit::SubmitJobError;
    match error {
        SubmitJobError::JobPlanConflict { existing_job_id } => ServerError::JobPlanConflict(
            format!("idempotency key already bound to job {existing_job_id}"),
        ),
        SubmitJobError::ActiveJobLimit { limit } => {
            ServerError::Conflict(format!("active job limit of {limit} reached"))
        }
        SubmitJobError::InvalidWorkspace(_) => ServerError::BadRequest,
        SubmitJobError::TooManyOutputs { limit } => {
            ServerError::BadRequestMessage(format!("a job may declare at most {limit} outputs"))
        }
        SubmitJobError::Composition(CompositionError::KeyConflict(key)) => {
            ServerError::Conflict(format!("composition key conflict on {key}"))
        }
        SubmitJobError::Composition(other) => ServerError::BadRequestMessage(other.to_string()),
        SubmitJobError::ClockHealth(_) => {
            ServerError::ServiceUnavailableReason("structured_id_clock_unhealthy".to_string())
        }
        SubmitJobError::PlacementUnavailable(_) => {
            ServerError::ServiceUnavailableReason("job_placement_unavailable".to_string())
        }
        SubmitJobError::QuotaDenied(denied) => ServerError::ComputeQuotaDenied(denied),
        SubmitJobError::AuthorityDenied => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn workspace_request(
    workspace: Option<WorkspaceRequest>,
) -> ServerResult<(WorkspaceMode, Option<String>)> {
    let Some(workspace) = workspace else {
        return Ok((WorkspaceMode::Kept, None));
    };
    match (workspace.mode, workspace.bucket) {
        (WorkspaceModeRequest::Temporary, None) => Ok((WorkspaceMode::Temporary, None)),
        (WorkspaceModeRequest::Kept, None) => Ok((WorkspaceMode::Kept, None)),
        (WorkspaceModeRequest::Existing, Some(bucket)) if !bucket.trim().is_empty() => {
            Ok((WorkspaceMode::Existing, Some(bucket)))
        }
        _ => Err(ServerError::BadRequest),
    }
}

async fn validate_existing_workspace(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    bucket: &str,
    extras: PolicyRequestExtras,
) -> ServerResult<()> {
    let info = match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(info)) => info,
        Ok(None) | Err(GetBucketInfoError::NotFound) => return Err(ServerError::BadRequest),
        Err(error) => return Err(ServerError::InternalError(error.to_string())),
    };
    if info.group_id != group_id {
        return Err(ServerError::BadRequest);
    }
    crate::auth::ensure_permission_with(
        state,
        auth,
        blob_bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
        Permission::WRITE,
        extras,
    )
    .await
}

/// Canonical absolute container path or 400.
fn container_path(path: &str) -> ServerResult<String> {
    let normalized = normalize_container_path(path).map_err(|_| ServerError::BadRequest)?;
    normalized
        .to_str()
        .map(str::to_string)
        .ok_or(ServerError::BadRequest)
}

/// Native inputs land in the container at the given path, defaulting to
/// `/inputs/<dest_key>` so `load_inputs` always stages them.
fn native_input(
    input: ExecutionInputRequest,
    target: ExecutionTarget,
) -> ServerResult<InputSelection> {
    if input.dest_key.is_empty() {
        return Err(ServerError::BadRequest);
    }
    // A realm submission resolves its inputs through the planner, which seals
    // the holder itself; naming one there would claim an unverified fact.
    let source_node_id = match (&input.source_node_id, target) {
        (Some(node_id), ExecutionTarget::Local) => {
            Some(NodeId::from_str(node_id).map_err(|_| ServerError::BadRequest)?)
        }
        (Some(_), ExecutionTarget::Realm) => {
            return Err(ServerError::BadRequestMessage(
                "source_node_id is only accepted by a local run".to_string(),
            ));
        }
        (None, _) => None,
    };
    let path = match &input.container_path {
        Some(path) => container_path(path)?,
        None => container_path(&format!("/inputs/{}", input.dest_key))?,
    };
    Ok(InputSelection {
        source: InputSource::S3 {
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
        },
        source_node_id,
        dest_key: input.dest_key,
        mode: match input.mode {
            InputModeRequest::Snapshot => InputMode::Snapshot,
            InputModeRequest::FloatingReference => InputMode::FloatingReference,
            InputModeRequest::ExactReference => InputMode::ExactReference,
        },
        container_path: Some(path),
        name: None,
        description: None,
    })
}

fn collision_policy(policy: CollisionPolicyRequest) -> CollisionPolicy {
    match policy {
        CollisionPolicyRequest::Reject => CollisionPolicy::Reject,
        CollisionPolicyRequest::Replace => CollisionPolicy::Replace,
        CollisionPolicyRequest::KeepExisting => CollisionPolicy::KeepExisting,
    }
}

fn native_output(output: ExecutionOutputRequest) -> ServerResult<WorkspaceOutput> {
    if output.dest_key.is_empty() {
        return Err(ServerError::BadRequest);
    }
    Ok(WorkspaceOutput {
        container_path: container_path(&output.container_path)?,
        dest_key: output.dest_key,
    })
}

fn validate_output_prefixes(prefixes: Vec<String>) -> ServerResult<Vec<String>> {
    if prefixes.len() > MAX_OUTPUT_PREFIXES || prefixes.iter().any(String::is_empty) {
        return Err(ServerError::BadRequest);
    }
    let mut deduplicated = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if !deduplicated.contains(&prefix) {
            deduplicated.push(prefix);
        }
    }
    Ok(deduplicated)
}

#[utoipa::path(
    get,
    path = "/jobs/",
    tag = "jobs",
    summary = "List the caller's jobs on this node",
    description = r#"Pages the jobs the caller submitted to this node, newest first.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused even for
its own jobs.

**Behavior**
- The page is self-scoped and node-local: it holds only jobs the caller submitted and only jobs
  this node owns. On a user device that includes every local run the owner started there; there is
  no separate listing for them.
- Jobs recorded on other nodes are never merged in, so a caller that submitted against another
  node pages that node's listing instead (submission answers with the `origin_node_url` it was
  accepted at).
- A distributed execution job is listed where it was admitted or is running; read one by id for
  the replicated family view, which any node holding its records can answer.
- Jobs the system creates for its own bookkeeping are never listed.
- A page without `next_cursor` is the last one.

**Limits**
- `limit` defaults to 50 and is capped at 200; `0` is treated as unset.
- `cursor` is a previous page's `next_cursor`: 24 bytes, base64url without padding, and anything
  else is rejected with 400.
- `state` selects one of `queued`, `claimed`, `preparing`, `ready`, `running`, `cancelling`,
  `indeterminate`, `succeeded`, `failed` or `cancelled`; any other value is rejected with 400."#,
    params(
        ("limit" = Option<usize>, Query, description = "Maximum jobs in one page; default 50, at most 200, and 0 is treated as unset"),
        ("cursor" = Option<String>, Query, description = "Opaque `next_cursor` from a previous page; absent starts at the newest job"),
        ("state" = Option<String>, Query, description = "Restrict the page to one job state; absent returns every state")
    ),
    responses(
        (
            status = 200,
            description = "Page of the caller's jobs on this node, newest first; jobs owned by other nodes are omitted and `next_cursor` is absent on the last page",
            body = JobListResponse,
            example = json!({
                "jobs": [
                    {
                        "job_id": "01JJRSTVWXYZ0123456789ABCD",
                        "kind": "execution",
                        "state": "running",
                        "attempts": 1,
                        "cancel_requested": false,
                        "created_at": "2026-04-09T14:23:11.123+00:00",
                        "updated_at": "2026-04-09T14:24:02.481+00:00",
                        "progress": {
                            "current": 2,
                            "total": 5,
                            "unit": "phases"
                        },
                        "workspace_bucket": "ws-01jjrstvwxyz0123456789abcd",
                        "workspace_mode": "kept"
                    }
                ],
                "next_cursor": "RqTuvSDYgez8DstU9tg0ZST62xQ3JtJW"
            })
        ),
        (status = 400, description = "Cursor is not a valid continuation token, or `state` is not a known job state", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_jobs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListJobsQuery>,
) -> ServerResult<(StatusCode, Json<JobListResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(MAX_LIST_LIMIT);
    let state_filter = query.state.as_deref().map(parse_state).transpose()?;

    let (records, next_cursor) = list_owned_jobs(
        &state.get_ctx(),
        auth.user_id,
        cursor,
        limit,
        move |record| state_filter.is_none_or(|state| record.state == state),
    )
    .await
    .map_err(ServerError::InternalError)?;

    let jobs = records.iter().map(job_status_response).collect();
    Ok((
        StatusCode::OK,
        Json(JobListResponse {
            jobs,
            next_cursor: encode_cursor(next_cursor),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/jobs/",
    tag = "jobs",
    summary = "Submit a container execution job",
    description = r#"Accepts a container execution job for asynchronous execution and returns the id to poll.

**Authentication**: realm bearer token with WRITE on the target group's data; a path-restricted
(delegated) token is refused. Running in a bucket that already exists additionally requires WRITE
on that bucket and that it belongs to the same group.

**Behavior**
- A 2xx means the request is durably admitted into its replicated submission family and queued,
  never that it started, finished or produced outputs.
- The job is not anchored to one node: any node that reduced the family answers for it, and
  `origin_node_url` is a preferred route rather than an owner.
- The response carries the opaque `submission_id` of the request itself and the alias this
  responder currently reduces as canonical; `job_id` stays the caller's stable handle even when a
  later merge moves the canonical alias.
- Execution is at-least-once: a partition may admit and run duplicates, whose outputs stay
  retrievable and auditable while one canonical success supplies the result.
- `idempotency_key` is scoped to the caller: replaying the same key with the same plan answers 200
  with the job that already exists and `created` false, while the same key with a different plan
  is a 409 conflict.
- On a replay `state` reports what the responder currently reduces for that family, so a running
  or finished request reads `running`, `succeeded`, `failed`, `cancelled` or `indeterminate`
  rather than `queued`.
- Set `workspace.mode` to `existing` to run in a bucket that already exists; omitting `workspace`
  keeps a per-job workspace bucket.
- The group's standing quota is decided against a replicated demand view. A replay of an
  idempotency key this node already claimed is settled before any quota is read and is never
  quota-refused.
- On a user device a `realm` request is always forwarded: the inputs are referenced rather than
  resolved here, the outputs land on the admitting realm holder, and the device itself never
  executes, admits or stores any part of the job. A device forwards the caller's own bearer token,
  so a submission it cannot back with one is a 403 rather than a forwarded request.

- `target` `local` is served by a user device only; any other node answers 400. The job runs on this machine for the user the device is enrolled for, and is refused for anyone
  else with a 403. Nothing about it is forwarded, replicated or offered to the realm, and the
  response carries no `submission_id`: a local run belongs to no submission family.
- Inputs must be readable on the device. An input naming `source_node_id` and `version_id` is a
  realm object: staging fetches that exact version into the run's own workspace bucket, as an
  ordinary local object and never as a reference, so an unreachable holder fails the run rather
  than the submission. Any other input this device does not hold is a 400 naming it.
- Outputs stay in the node-local workspace bucket until the owner publishes them, and the run is
  listed by this device's own `GET /jobs/`.
- Mounted inputs and `workspace.mode` `none` are refused, because a device stages files and exposes
  no S3 endpoint a container could reach, and so is `workspace.mode` `existing`, because a local
  run's outputs stay in its own workspace bucket.
- A paused compute plane, a device without a compute backend, and a device that already holds as
  many unfinished jobs as `ARUNA_COMPUTE_MAX_CONCURRENT` all answer 409; the ceiling is counted in
  the admitting transaction, so two submissions cannot both pass it.

**Limits** (all refused with 400)
- An empty image, a `cpu_cores` of 0, or a `ram_bytes` of 0 or above 2^63-1.
- More than 512 inputs, more than 1024 outputs, or more than 32 output prefixes.
- An empty `dest_key`, or a container path that is not absolute and traversal-free.
- Two inputs sharing a container path, or two outputs sharing a `dest_key` or container path.

**Errors**
- Two inputs sharing a `dest_key` are not a transport error: `mode` and `collision_policy` decide
  the staged result, and only `reject` refuses them, as a 409 from the composition.
- A quota refusal is a 409 carrying the exact scope, dimension and numbers in `quota`; a demand
  view that understates the group is refused like an exceeded cap, with `observed` reported at the
  limit, because a cap that cannot be shown to hold is not evidence of room.
- A 503 is retryable and the caller may submit again with the same idempotency key: no family
  holder could admit the request, the demand view could not be read or kept moving under three
  reads, three admission transactions in a row lost to a concurrent submission of the same group,
  or the id clock is unhealthy.
- A device submission whose inputs sit on no holder of its family answers that same 503, and it is
  the one case retrying does not clear. The family is picked by hashing the request, the holder
  resolves the referenced inputs against its own objects only, and a holder that cannot read one
  refuses without naming it, so an unstaged input is indistinguishable here from an unreachable
  realm. Submit from a node that holds the inputs, or replicate them first."#,
    request_body(
        content = SubmitExecutionRequest,
        description = "Container image, command and the inputs and outputs to stage around it",
        example = json!({
            "group_id": "01JABCDEF0123456789ABCDEFG",
            "image": "registry.example.test/tools/fastqc:0.12.1",
            "command": ["fastqc", "--outdir", "/outputs", "/inputs/reads.fastq"],
            "env": {
                "FASTQC_THREADS": "2"
            },
            "tags": {
                "aruna-engine.org/label/accelerator": "gpu"
            },
            "workdir": "/work",
            "cpu_cores": 2,
            "ram_bytes": 4294967296_i64,
            "max_walltime_ms": 3600000,
            "inputs": [
                {
                    "bucket": "project-data",
                    "key": "raw/reads.fastq",
                    "dest_key": "reads.fastq"
                }
            ],
            "outputs": [
                {
                    "container_path": "/outputs/reads_fastqc.html",
                    "dest_key": "reports/reads_fastqc.html"
                }
            ],
            "output_prefixes": ["reports/"],
            "idempotency_key": "fastqc-reads-2026-04-09",
            "workspace": {
                "mode": "kept"
            }
        })
    ),
    responses(
        (
            status = 201,
            description = "Job durably accepted and queued on the owning node; poll `status_url` for progress",
            body = SubmitJobResponse,
            example = json!({
                "job_id": "01JJRSTVWXYZ0123456789ABCD",
                "created": true,
                "submission_id": "6b1f8c9d0e2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4",
                "canonical_job_id": "01JJRSTVWXYZ0123456789ABCD",
                "state": "queued",
                "origin_node_url": "https://node.example.test/api/v1",
                "status_url": "https://node.example.test/api/v1/jobs/01JJRSTVWXYZ0123456789ABCD"
            })
        ),
        (
            status = 200,
            description = "Replay of an idempotency key naming this exact plan; nothing new was admitted and `state` reports what the responder currently reduces",
            body = SubmitJobResponse,
            example = json!({
                "job_id": "01JJRSTVWXYZ0123456789ABCD",
                "created": false,
                "submission_id": "6b1f8c9d0e2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4",
                "canonical_job_id": "01JJRSTVWXYZ0123456789ABCD",
                "state": "running",
                "origin_node_url": "https://node.example.test/api/v1",
                "status_url": "https://node.example.test/api/v1/jobs/01JJRSTVWXYZ0123456789ABCD"
            })
        ),
        (status = 400, description = "Malformed group id, empty image, out-of-range resources, an invalid or duplicated input, output or container path, a workspace request that names no usable bucket, `target` `local` on a node that serves no device plane, or a local run naming an input this device cannot read", body = ErrorResponse),
        (status = 409, description = "The idempotency key is bound to a different plan, the group's compute quota refuses the admission, the composition conflicts on a staged key, the active RO-Crate job limit is reached, or this device's compute plane is paused, absent or already at its run ceiling", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted, the caller lacks WRITE on the group or on the named existing workspace bucket, or a local run was requested by someone other than this device's owner", body = ErrorResponse),
        (status = 503, description = "No family holder could admit the request, an unreadable or unsettled demand view, three lost admission transactions, or an unhealthy id clock; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<SubmitExecutionRequest>,
) -> ServerResult<(StatusCode, Json<SubmitJobResponse>)> {
    let (status, response) = submit_execution(
        state.as_ref(),
        auth,
        bearer,
        request,
        PolicyRequestExtras::rest(),
    )
    .await?;
    Ok((status, Json(response)))
}

pub(crate) async fn submit_execution(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer: Option<ValidatedArunaBearerTokenCarrier>,
    request: SubmitExecutionRequest,
    extras: PolicyRequestExtras,
) -> ServerResult<(StatusCode, SubmitJobResponse)> {
    let target = request.target.unwrap_or_default();
    let auth = match target {
        ExecutionTarget::Realm => require_unrestricted_realm_auth(state, auth)?,
        ExecutionTarget::Local => local_auth(state, auth).await?,
    };
    let group_id = Ulid::from_string(&request.group_id).map_err(|_| ServerError::BadRequest)?;
    let (workspace_mode, workspace_bucket) = workspace_request(request.workspace)?;
    if request.image.trim().is_empty() {
        return Err(ServerError::BadRequest);
    }
    // RAM above i64::MAX would wrap negative in the Docker HostConfig cast.
    if request.cpu_cores == Some(0)
        || request
            .ram_bytes
            .is_some_and(|bytes| bytes == 0 || i64::try_from(bytes).is_err())
    {
        return Err(ServerError::BadRequest);
    }
    let output_prefixes = validate_output_prefixes(request.output_prefixes)?;
    crate::auth::ensure_permission_with(
        state,
        &auth,
        blob_group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
        Permission::WRITE,
        extras.clone(),
    )
    .await?;
    if let Some(bucket) = workspace_bucket.as_deref() {
        validate_existing_workspace(state, &auth, group_id, bucket, extras).await?;
    }

    if request.inputs.len() > MAX_PLAN_INPUTS || request.outputs.len() > MAX_EXECUTION_OUTPUTS {
        return Err(ServerError::BadRequest);
    }
    // Destination-key overlaps are the composition's collision policy to resolve.
    let mut inputs: Vec<InputSelection> = Vec::with_capacity(request.inputs.len());
    for input in request.inputs {
        let input = native_input(input, target)?;
        if inputs
            .iter()
            .any(|existing| existing.container_path == input.container_path)
        {
            return Err(ServerError::BadRequest);
        }
        inputs.push(input);
    }
    let mut workspace_outputs: Vec<WorkspaceOutput> = Vec::with_capacity(request.outputs.len());
    for output in request.outputs {
        let output = native_output(output)?;
        if workspace_outputs.iter().any(|existing| {
            existing.dest_key == output.dest_key || existing.container_path == output.container_path
        }) {
            return Err(ServerError::BadRequest);
        }
        workspace_outputs.push(output);
    }

    let spec = ExecutionSpec {
        group_id,
        name: None,
        description: None,
        tags: request.tags,
        image: request.image,
        entrypoint: request.entrypoint,
        command: request.command,
        workdir: request.workdir,
        env: request.env,
        resources: ComputeResources {
            cpu_cores: request.cpu_cores,
            ram_bytes: request.ram_bytes,
            disk_bytes: None,
            max_walltime_ms: request.max_walltime_ms,
            preemptible: false,
        },
        executor_constraint: request.executor_constraint,
        inputs,
        file_outputs: Vec::new(),
        workspace_outputs,
        output_prefixes,
        collision_policy: collision_policy(request.collision_policy),
    };
    let accepted = match target {
        ExecutionTarget::Local => {
            local_submit(state, &auth, spec, request.idempotency_key, workspace_mode).await?
        }
        ExecutionTarget::Realm => {
            let result = submit_external_job(
                &state.get_ctx(),
                spec,
                auth.user_id,
                request.idempotency_key,
                workspace_mode,
                workspace_bucket,
                state.rocrate_limits().artifact_retention_ms,
                forwarded_job_auth(bearer)?,
            )
            .await
            .map_err(map_submit_error)?;
            AcceptedJob {
                job_id: result.job_id,
                created: result.created,
                submission_id: Some(hex32(&result.submission_id.0)),
                state: result.state.name().to_string(),
            }
        }
    };

    let status = if accepted.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let urls = job_urls(state, accepted.job_id).await?;
    // The accepting holder's canonical binding is the alias it answered with;
    // a later merge may move it, which the status surface then reports.
    Ok((
        status,
        SubmitJobResponse {
            job_id: accepted.job_id.to_string(),
            created: accepted.created,
            submission_id: accepted.submission_id,
            canonical_job_id: accepted.job_id.to_string(),
            state: accepted.state,
            origin_node_url: urls.owner_node_url,
            status_url: urls.status_url,
        },
    ))
}

/// What both submission paths answer with. A local run has no submission
/// family, so it names none.
struct AcceptedJob {
    job_id: JobId,
    created: bool,
    submission_id: Option<String>,
    state: String,
}

/// The owner of this device, for a run that must stay on this machine. A node
/// that serves no device plane refuses the target itself, not the caller.
async fn local_auth(state: &ServerState, auth: Option<AuthContext>) -> ServerResult<AuthContext> {
    if !matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(ServerError::BadRequestMessage(
            "target `local` is served by a user device only".to_string(),
        ));
    }
    require_owner(state, auth).await
}

async fn local_submit(
    state: &ServerState,
    auth: &AuthContext,
    spec: ExecutionSpec,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
) -> ServerResult<AcceptedJob> {
    let context = state.get_ctx();
    let result = submit_local_execution(
        &context,
        LocalExecutionConfig {
            spec,
            owner: auth.user_id,
            node_id: state.get_node_id(),
            idempotency_key,
            workspace_mode,
            retention_ms: state.rocrate_limits().artifact_retention_ms,
        },
    )
    .await
    .map_err(map_local_error)?;
    // A replay answers with the state the device already reduced for that job.
    let state = read_owned_job(&context, auth.user_id, result.job_id)
        .await
        .ok()
        .flatten()
        .map_or(JobState::Queued, |record| record.state);
    Ok(AcceptedJob {
        job_id: result.job_id,
        created: result.created,
        submission_id: None,
        state: state.name().to_string(),
    })
}

pub(crate) fn map_local_error(error: LocalExecutionError) -> ServerError {
    match error {
        LocalExecutionError::NotADevice => ServerError::BadRequestMessage(
            "target `local` is served by a user device only".to_string(),
        ),
        LocalExecutionError::NotOwner => ServerError::Forbidden,
        LocalExecutionError::Paused | LocalExecutionError::NoExecutor => {
            ServerError::Conflict(error.to_string())
        }
        LocalExecutionError::Unsupported(_)
        | LocalExecutionError::InputNotLocal { .. }
        | LocalExecutionError::InputRefused { .. } => {
            ServerError::BadRequestMessage(error.to_string())
        }
        LocalExecutionError::Unavailable(_) => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        LocalExecutionError::Submit(error) => map_submit_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}",
    tag = "jobs",
    summary = "Read one job's status",
    description = r#"Returns one job's current status, with the replicated family view for a distributed execution job.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Reads are
self-scoped: only the job's own submitter may read it, and a job belonging to somebody else
answers 404 rather than 403, so the surface never confirms that an id exists.

**Behavior**
- The one exception to self-scoping is a persistent-id minting job the caller joined, which stays
  readable while the caller holds WRITE on the document it mints for.
- `state` is a point-in-time value that keeps moving until it reaches `succeeded`, `failed` or
  `cancelled`.
- A distributed execution job carries a `family` block reduced from the replicated records: it
  names the request's `submission_id`, the currently canonical alias, the canonical execution and
  its exact output VersionIds, how many physical executions and duplicate successes are known, the
  projection `revision` and digest to detect that the view changed, and the responder that
  answered.
- `family.partial` means this responder could not reduce every record.
- `family.locally_exhausted` is a responder-local diagnostic (outside the projection digest)
  meaning every known execution ended without success and no retry is armed here; it is not
  evidence of a permanent failure.
- Node identities of other nodes are never disclosed; only the responder names itself, and an
  output whose owning node's S3 endpoint is unknown here carries `endpoint_url: null` rather than
  failing the read.
- A distributed execution job is answered here from the replicated family projection, without
  routing. Every other kind is answered by the node that owns the job, derived from the id itself:
  when that is another node the request is forwarded under the caller's own bearer token.
- `run_crate` appears only for jobs that owe a run crate and reports that side obligation, not the
  job itself.

**Errors**: a bearer token that cannot be forwarded to the owning node is a 400, and an owner that
cannot be reached is a retryable 503."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID; an unparseable id is 404")),
    responses(
        (
            status = 200,
            description = "Current status of the caller's job, including its terminal result once it has one",
            body = JobStatusResponse,
            example = json!({
                "job_id": "01JJRSTVWXYZ0123456789ABCD",
                "kind": "execution",
                "state": "succeeded",
                "attempts": 1,
                "cancel_requested": false,
                "created_at": "2026-04-09T14:23:11.123+00:00",
                "updated_at": "2026-04-09T14:31:47.902+00:00",
                "finished_at": "2026-04-09T14:31:47.902+00:00",
                "progress": {
                    "current": 5,
                    "total": 5,
                    "unit": "phases"
                },
                "result": {
                    "exit_code": 0,
                    "workspace_bucket": "ws-01jjrstvwxyz0123456789abcd",
                    "stdout": "",
                    "stderr": "",
                    "outputs": [
                        {
                            "bucket": "ws-01jjrstvwxyz0123456789abcd",
                            "key": "reports/reads_fastqc.html",
                            "version_id": "01JJRSVERSION0123456789ABC",
                            "execution_id": "01JJRSEXEC0123456789ABCDEF",
                            "endpoint_url": "https://s3.example",
                            "container_path": "/outputs/reads_fastqc.html",
                            "size": 20480,
                            "digest": "fa2c8cc4f28176bbeed4b736df569a34c79cd3723e9ec42f9674b4d46ac6b8b8"
                        }
                    ]
                },
                "workspace_bucket": "ws-01jjrstvwxyz0123456789abcd",
                "workspace_mode": "kept",
                "run_crate": {
                    "status": "written",
                    "resource": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE#run/01JJRSTVWXYZ0123456789ABCD"
                },
                "family": {
                    "submission_id": "6b1f8c9d0e2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4",
                    "request_digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
                    "canonical_job_id": "01JJRSTVWXYZ0123456789ABCD",
                    "aliases": ["01JJRSTVWXYZ0123456789ABCD"],
                    "alias_count": 1,
                    "conflict_count": 0,
                    "logical_state": "succeeded",
                    "canonical_execution_id": "01JJRSEXEC0123456789ABCDEF",
                    "executions": 2,
                    "duplicate_successes": 1,
                    "outputs": [
                        {
                            "bucket": "ws-01jjrstvwxyz0123456789abcd",
                            "key": "reports/reads_fastqc.html",
                            "version_id": "01JJRSVERSION0123456789ABC",
                            "execution_id": "01JJRSEXEC0123456789ABCDEF",
                            "endpoint_url": "https://s3.example",
                            "container_path": "/outputs/reads_fastqc.html",
                            "size": 20480,
                            "digest": "fa2c8cc4f28176bbeed4b736df569a34c79cd3723e9ec42f9674b4d46ac6b8b8"
                        }
                    ],
                    "revision": 7,
                    "projection_digest": "1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f",
                    "eventually_consistent": true,
                    "responder_node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                    "partial": false,
                    "locally_exhausted": false,
                    "cancel_requested": false,
                    "placement": {
                        "executor_kind": "docker",
                        "estimated_transfer_bytes": 4194304,
                        "estimated_transfer_ms": 340,
                        "alternatives": 2,
                        "rejected": 1,
                        "omitted": 0,
                        "sealed_at_ms": 1755500000000u64
                    }
                }
            })
        ),
        (status = 400, description = "The bearer token cannot be forwarded to the owning node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such job, or it was submitted by somebody else; absence and foreign ownership are deliberately indistinguishable", body = ErrorResponse),
        (status = 503, description = "The node owning this job could not be reached or is not yet known here; retryable, the caller may repeat the read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(job_id): Path<String>,
) -> ServerResult<(StatusCode, Json<JobStatusResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    // A distributed external job is answered from the replicated family; every
    // other job keeps the owner-routed view.
    if let Some(report) = family_report(&state.get_ctx(), &auth, job_id).await {
        let report = report.map_err(map_job_route)?;
        let mut response = job_view_response(&report.job);
        let family = family_response(&report);
        bind_output_routes(&mut response.result, &family.outputs).map_err(map_job_route)?;
        response.family = Some(family);
        return Ok((StatusCode::OK, Json(response)));
    }
    let routed = read_job_routed(&state.get_ctx(), &auth, job_id, forwarded_job_auth(bearer)?)
        .await
        .map_err(map_job_route)?;
    let mut response = job_view_response(&routed.job);
    response.run_crate = routed.run_crate;
    Ok((StatusCode::OK, Json(response)))
}

fn coded_response(status: StatusCode, error: &str, code: &str) -> Response {
    (
        status,
        Json(ErrorResponse::new(error).with_code(code.to_string())),
    )
        .into_response()
}

fn decode_report_row(
    kind: JobKind,
    entry_key: &[u8],
    value: &[u8],
) -> ServerResult<serde_json::Value> {
    let row = match kind {
        JobKind::ImportRoCrate => {
            let row: ImportReportRow = postcard::from_bytes(value)
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            let visible_key = entry_key
                .strip_prefix(&[JOB_SYSTEM_ENTRY_PREFIX])
                .unwrap_or(entry_key);
            if row.entry_key.as_bytes() != visible_key {
                return Err(ServerError::InternalError(
                    "stored import report entry key does not match its row".to_string(),
                ));
            }
            serde_json::to_value(row)
        }
        JobKind::ExportRoCrate => {
            let row: ExportReportRow = postcard::from_bytes(value)
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            if row.entry_key.as_bytes() != entry_key {
                return Err(ServerError::InternalError(
                    "stored export report entry key does not match its row".to_string(),
                ));
            }
            serde_json::to_value(row)
        }
        _ => return Err(ServerError::NotFound),
    };
    row.map_err(|error| ServerError::InternalError(error.to_string()))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/report",
    tag = "jobs",
    summary = "Page a finished RO-Crate job's report",
    description = r#"Pages the frozen per-entry report of a finished RO-Crate import or export job.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: a job submitted by somebody else answers 404 instead of 403.

**Behavior**
- Only RO-Crate import and export jobs keep a per-entry report; every other kind answers 404.
- The report exists only once the job is terminal, so while it is still running the answer is 404
  carrying a pending marker with the job's current state, and the caller should poll.
- It is then frozen and immutable, and it disappears again once the job's retention window passes,
  which is a plain 404.
- Paging is stable against that frozen snapshot: `report_digest` names it and a cursor carries
  both the job and that digest.
- The read is answered by the node that owns the job, forwarded under the caller's own bearer
  token when this node is not the owner.

**Limits**
- `limit` defaults to 200 and is capped at 1000; `0` is treated as unset.

**Errors**: a cursor from another job or another report is a 409 rather than a silently different
page, a malformed cursor or a token that cannot be forwarded is a 400, and an unreachable owner is
a retryable 503."#,
    params(
        ("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID; an unparseable id is 404"),
        ("limit" = Option<usize>, Query, description = "Maximum report rows in one page; default 200, at most 1000, and 0 is treated as unset"),
        ("cursor" = Option<String>, Query, description = "Opaque `next_cursor` from a previous page, bound to this job and its frozen report; absent starts at the first row")
    ),
    responses(
        (
            status = 200,
            description = "Page of the frozen report; rows are entry-keyed outcomes and `report_digest` identifies the snapshot this page belongs to. No `next_cursor` means the last page",
            body = JobReportResponse,
            example = json!({
                "rows": [
                    {
                        "entry_key": "data/reads.fastq",
                        "code": "imported",
                        "message": null,
                        "detail": {
                            "archive_path": "data/reads.fastq",
                            "target_key": "crate/data/reads.fastq",
                            "version_id": "01JMETADATA0123456789ABCDE",
                            "blake3": "fa2c8cc4f28176bbeed4b736df569a34c79cd3723e9ec42f9674b4d46ac6b8b8",
                            "size": 1048576,
                            "arn": null,
                            "w3id": null,
                            "validation": null
                        }
                    }
                ],
                "next_cursor": "vPr_5UvGEO5Zc2Jc1Vn7NPw9toS74HXJPkSW5Mouj9k",
                "report_digest": "5c15818ae224f9a918b32cc1ae79a2b9ff3d251b1d88412df04eb67250e2d3d1"
            })
        ),
        (status = 400, description = "The cursor is not a decodable continuation token, or the bearer token cannot be forwarded to the owning node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (
            status = 404,
            description = "A pending marker naming the job's current state, or the standard error body: unknown job, a foreign job, a kind that keeps no report, or retention passed",
            body = ReportUnavailableResponse,
            examples(
                ("Report pending" = (
                    summary = "The job has not reached a terminal state, so no report is frozen yet",
                    value = json!({
                        "code": "report_pending",
                        "state": "running"
                    })
                ))
            )
        ),
        (status = 409, description = "The cursor was issued for a different job or a different frozen report, so it cannot continue this one", body = ErrorResponse),
        (status = 503, description = "The node owning this job could not be reached or is not yet known here; retryable, the caller may repeat the read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job_report(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(job_id): Path<String>,
    Query(query): Query<ReportQuery>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    let cursor = decode_report_cursor(query.cursor.as_deref())?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.job_id != job_id)
    {
        return Ok(coded_response(
            StatusCode::CONFLICT,
            "report cursor belongs to a different job",
            "report_cursor_conflict",
        ));
    }
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_REPORT_LIMIT)
        .min(usize::from(JOB_REPORT_MAX_ROWS));
    let expected_digest = cursor.as_ref().map(|cursor| cursor.report_digest);
    let last_key = cursor.map(|cursor| cursor.last_key);
    match read_report_routed(
        &state.get_ctx(),
        auth.user_id,
        job_id,
        expected_digest,
        last_key,
        limit,
        forwarded_job_auth(bearer)?,
    )
    .await
    .map_err(map_job_route)?
    {
        JobReportLookup::NotFound => Err(ServerError::NotFound),
        JobReportLookup::Pending(state) => Ok((
            StatusCode::NOT_FOUND,
            Json(ReportPendingResponse {
                code: "report_pending".to_string(),
                state: state.name().to_string(),
            }),
        )
            .into_response()),
        JobReportLookup::CursorConflict => Ok(coded_response(
            StatusCode::CONFLICT,
            "report cursor does not match the frozen report",
            "report_cursor_conflict",
        )),
        JobReportLookup::Ready {
            job,
            rows,
            next_key,
        } => {
            let report_digest = job.report_digest;
            let rows = rows
                .into_iter()
                .map(|(entry_key, value)| decode_report_row(job.kind, &entry_key, value.as_ref()))
                .collect::<ServerResult<Vec<_>>>()?;
            Ok((
                StatusCode::OK,
                Json(JobReportResponse {
                    rows,
                    next_cursor: encode_report_cursor(job_id, report_digest, next_key)?,
                    report_digest: hex::encode(report_digest),
                }),
            )
                .into_response())
        }
    }
}

fn range_request(headers: &HeaderMap) -> Result<Option<ObjectRangeRequest>, ()> {
    let Some(value) = headers.get(RANGE) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| ())?;
    let range = value.strip_prefix("bytes=").ok_or(())?;
    if range.contains(',') {
        return Err(());
    }
    let (start, end) = range.split_once('-').ok_or(())?;
    match (start, end) {
        ("", "") => Err(()),
        ("", end) => end
            .parse::<u64>()
            .map(|length| Some(ObjectRangeRequest::Suffix { length }))
            .map_err(|_| ()),
        (start, "") => start
            .parse::<u64>()
            .map(|start| Some(ObjectRangeRequest::Start { start }))
            .map_err(|_| ()),
        (start, end) => {
            let start = start.parse::<u64>().map_err(|_| ())?;
            let end = end.parse::<u64>().map_err(|_| ())?;
            Ok(Some(ObjectRangeRequest::StartEnd { start, end }))
        }
    }
}

fn ascii_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn artifact_headers(owned: &OwnedArtifact) -> ServerResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{}\"", hex::encode(owned.blake3)))
            .map_err(|error| ServerError::InternalError(error.to_string()))?,
    );
    let encoded = utf8_percent_encode(&owned.filename, NON_ALPHANUMERIC);
    let fallback = ascii_filename(&owned.filename);
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
        ))
        .map_err(|error| ServerError::InternalError(error.to_string()))?,
    );
    Ok(headers)
}

fn range_error(size: u64) -> Response {
    let mut response = coded_response(
        StatusCode::RANGE_NOT_SATISFIABLE,
        "requested artifact range is not satisfiable",
        "invalid_range",
    );
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{size}")) {
        response.headers_mut().insert(CONTENT_RANGE, value);
    }
    response
}

async fn artifact_response(
    state: Arc<ServerState>,
    auth: Option<AuthContext>,
    bearer: Option<ValidatedArunaBearerTokenCarrier>,
    job_id: String,
    headers: HeaderMap,
    download: bool,
) -> ServerResult<Response> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    let auth_token = forwarded_job_auth(bearer)?;
    let now_ms = aruna_core::util::unix_timestamp_millis();
    let owned = match read_artifact_routed(
        &state.get_ctx(),
        auth.user_id,
        job_id,
        now_ms,
        None,
        auth_token.clone(),
    )
    .await
    .map_err(map_job_route)?
    .0
    {
        ArtifactLookup::NotFound => return Err(ServerError::NotFound),
        ArtifactLookup::Pending(state) => {
            return Ok((
                StatusCode::NOT_FOUND,
                Json(
                    ErrorResponse::new("RO-Crate artifact is not ready")
                        .with_code("artifact_pending")
                        .with_details(state.name()),
                ),
            )
                .into_response());
        }
        ArtifactLookup::Gone => {
            return Ok(coded_response(
                StatusCode::GONE,
                "RO-Crate artifact has expired",
                "artifact_expired",
            ));
        }
        ArtifactLookup::Ready(owned) => owned,
    };
    let range_request = match range_request(&headers) {
        Ok(range) => range,
        Err(()) => return Ok(range_error(owned.size)),
    };
    let (status, range, content_range) = match range_request {
        Some(request) => match request.resolve(owned.size) {
            Ok(resolved) => (
                StatusCode::PARTIAL_CONTENT,
                resolved.range,
                Some(resolved.content_range),
            ),
            Err(_) => return Ok(range_error(owned.size)),
        },
        None => (
            StatusCode::OK,
            Range {
                start: 0,
                end: owned.size,
            },
            None,
        ),
    };
    let content_length = range.end - range.start;
    let mut response_headers = artifact_headers(&owned)?;
    response_headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| ServerError::InternalError(error.to_string()))?,
    );
    if let Some(content_range) = content_range {
        response_headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&content_range)
                .map_err(|error| ServerError::InternalError(error.to_string()))?,
        );
    }
    let body = if download && content_length > 0 {
        let permit = match download::admit(state.as_ref(), LocalKey::User(auth.user_id)) {
            Ok(permit) => permit,
            Err(AdmissionError::Total) => {
                return Err(ServerError::ServiceUnavailableReason(
                    "download_capacity".to_string(),
                ));
            }
            Err(AdmissionError::User) => {
                return Ok(coded_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "download capacity exhausted",
                    "download_capacity",
                ));
            }
        };
        let (lookup, read) = read_artifact_routed(
            &state.get_ctx(),
            auth.user_id,
            job_id,
            now_ms,
            Some(range),
            auth_token,
        )
        .await
        .map_err(map_job_route)?;
        let read = match lookup {
            ArtifactLookup::Ready(current) if owned.same_content(&current) => {
                read.ok_or_else(|| {
                    ServerError::InternalError(
                        "artifact owner omitted the response body".to_string(),
                    )
                })?
            }
            ArtifactLookup::Ready(_) => {
                return Err(ServerError::ServiceUnavailableReason(
                    "job_read_unavailable".to_string(),
                ));
            }
            ArtifactLookup::NotFound => return Err(ServerError::NotFound),
            ArtifactLookup::Pending(state) => {
                return Ok((
                    StatusCode::NOT_FOUND,
                    Json(
                        ErrorResponse::new("RO-Crate artifact is not ready")
                            .with_code("artifact_pending")
                            .with_details(state.name()),
                    ),
                )
                    .into_response());
            }
            ArtifactLookup::Gone => {
                return Ok(coded_response(
                    StatusCode::GONE,
                    "RO-Crate artifact has expired",
                    "artifact_expired",
                ));
            }
        };
        if read.stream_size != content_length {
            return Err(ServerError::InternalError(
                "artifact reader returned an unexpected range size".to_string(),
            ));
        }
        download::body(read.blob, permit)
    } else {
        Body::empty()
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/artifacts/rocrate",
    tag = "jobs",
    summary = "Download a finished export job's RO-Crate",
    description = r#"Downloads the packaged RO-Crate an export job produced as a binary `application/zip` body, not JSON.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: a job submitted by somebody else answers 404 instead of 403, and
a job kind that produces no crate answers 404 as well.

**Behavior**
- A successful answer always carries `Content-Type: application/zip`, `Content-Length`,
  `Accept-Ranges: bytes`, an `ETag` that is the artifact's quoted hex BLAKE3 digest, and a
  `Content-Disposition: attachment` naming the crate file with both an ASCII fallback and a UTF-8
  form; a partial answer adds `Content-Range`.
- While the export job has not finished the crate is not ready and the answer is 404 with the code
  `artifact_pending` and the job's current state in its details, so the caller should poll the
  status instead of retrying blindly.
- Downloads are admission-limited, so a saturated node refuses rather than queueing.

**Limits**
- `Range` accepts one byte range only.

**Errors**: once the artifact's retention window passes it is a 410 and never comes back, a range
that is not a single satisfiable one is a 416, and a saturated or unreachable owner is a retryable
503."#,
    params(
        ("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID; an unparseable id is 404"),
        ("Range" = Option<String>, Header, description = "One byte range: `bytes=<first>-<last>`, `bytes=<first>-` or `bytes=-<suffix length>`; absent returns the whole crate")
    ),
    responses(
        (status = 200, description = "The complete RO-Crate as an `application/zip` byte stream, with `Content-Length`, `ETag`, `Accept-Ranges: bytes` and an attachment `Content-Disposition`"),
        (status = 206, description = "The requested byte range of the crate as `application/zip`, with `Content-Range` and a `Content-Length` covering the range only"),
        (status = 400, description = "The bearer token cannot be forwarded to the owning node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No downloadable crate: unknown job, a foreign job, a kind that produces none, or an unfinished export, coded `artifact_pending` with the job's state", body = ErrorResponse),
        (status = 410, description = "The crate's retention window has passed and it has been deleted; a retry will not bring it back", body = ErrorResponse),
        (status = 416, description = "The requested range is not a single satisfiable byte range; the answer repeats `Accept-Ranges` and reports the crate size in `Content-Range`", body = ErrorResponse),
        (status = 503, description = "The node owning this job could not be reached, or this node's download capacity is exhausted; retryable, the caller may repeat the download", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job_artifact(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    artifact_response(state, auth, bearer, job_id, headers, true).await
}

#[utoipa::path(
    head,
    path = "/jobs/{job_id}/artifacts/rocrate",
    tag = "jobs",
    summary = "Probe a finished export job's RO-Crate headers",
    description = r#"Answers exactly what the download would answer, with the headers but no body.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: a job submitted by somebody else answers 404 instead of 403.

**Behavior**
- A client can learn a crate's size, digest and filename before fetching it.
- The headers describe an `application/zip` crate: `Content-Type`, `Content-Length` for the whole
  crate or for the requested range, `Accept-Ranges: bytes`, an `ETag` that is the artifact's
  quoted hex BLAKE3 digest, an attachment `Content-Disposition`, and `Content-Range` when a range
  was asked for.
- Readiness behaves as it does for the download: a crate whose export has not finished is a 404
  coded `artifact_pending` with the job's current state, and one past its retention window is a
  410.
- Probing does not consume download capacity.

**Limits**
- `Range` accepts one byte range only; multiple ranges and unparseable or out-of-bounds values are
  refused with 416."#,
    params(
        ("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID; an unparseable id is 404"),
        ("Range" = Option<String>, Header, description = "One byte range: `bytes=<first>-<last>`, `bytes=<first>-` or `bytes=-<suffix length>`; absent describes the whole crate")
    ),
    responses(
        (status = 200, description = "Headers for the complete `application/zip` crate: `Content-Length`, `ETag`, `Accept-Ranges: bytes` and an attachment `Content-Disposition`. No body is sent"),
        (status = 206, description = "Headers for the requested byte range, including `Content-Range` and a `Content-Length` covering the range only. No body is sent"),
        (status = 400, description = "The bearer token cannot be forwarded to the owning node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No crate to describe: unknown job, a foreign job, a kind that produces none, or an unfinished export, coded `artifact_pending` with the job's state", body = ErrorResponse),
        (status = 410, description = "The crate's retention window has passed and it has been deleted; a retry will not bring it back", body = ErrorResponse),
        (status = 416, description = "The requested range is not a single satisfiable byte range; the answer repeats `Accept-Ranges` and reports the crate size in `Content-Range`", body = ErrorResponse),
        (status = 503, description = "The node owning this job could not be reached or is not yet known here; retryable, the caller may repeat the probe", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn head_job_artifact(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    artifact_response(state, auth, bearer, job_id, headers, false).await
}

#[utoipa::path(
    post,
    path = "/jobs/{job_id}/cancel",
    tag = "jobs",
    summary = "Request cancellation of the caller's job",
    description = r#"Records a cancellation request on the caller's job; it does not stop the work synchronously.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: only the submitter may cancel, and a job belonging to somebody
else answers 404 rather than 403.

**Behavior**
- Cancellation is asynchronous: 202 means the request was durably recorded on the job, not that
  work has stopped.
- A job that never started is settled immediately, while one already running is asked to stop and
  reaches `cancelled` some time later, and may still finish on its own first, so the caller polls
  the status to learn the outcome.
- The call is idempotent: repeating it on a job that is still live keeps answering 202 with
  `cancel_requested` true, and a job that has already reached a terminal state answers 200 with
  that state unchanged.
- Cancelling a distributed execution job publishes an append-only cancellation intent into its
  replicated family, which every holder observes and which suppresses further launches; it never
  claims that a partitioned execution stopped.
- An execution that already holds a receipt may still finish, and that late success stays visible
  with `cancel_requested` true rather than being erased.
- Cancellation of any other job stays anchored to the node that owns it.

**Errors**: when the owning node cannot be reached the answer is a retryable 503 and nothing was
recorded anywhere else."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID; an unparseable id is 404")),
    responses(
        (
            status = 202,
            description = "Cancellation was recorded on the job; it is not stopped yet, poll the status for the outcome",
            body = JobStatusResponse,
            example = json!({
                "job_id": "01JJRSTVWXYZ0123456789ABCD",
                "kind": "execution",
                "state": "running",
                "attempts": 1,
                "cancel_requested": true,
                "created_at": "2026-04-09T14:23:11.123+00:00",
                "updated_at": "2026-04-09T14:29:55.004+00:00",
                "progress": {
                    "current": 3,
                    "total": 5,
                    "unit": "phases"
                },
                "workspace_bucket": "ws-01jjrstvwxyz0123456789abcd",
                "workspace_mode": "kept"
            })
        ),
        (
            status = 200,
            description = "The job had already finished, so nothing was cancelled and its terminal state is returned unchanged",
            body = JobStatusResponse,
            example = json!({
                "job_id": "01JJRSTVWXYZ0123456789ABCD",
                "kind": "execution",
                "state": "succeeded",
                "attempts": 1,
                "cancel_requested": false,
                "created_at": "2026-04-09T14:23:11.123+00:00",
                "updated_at": "2026-04-09T14:31:47.902+00:00",
                "finished_at": "2026-04-09T14:31:47.902+00:00",
                "progress": {
                    "current": 5,
                    "total": 5,
                    "unit": "phases"
                },
                "workspace_bucket": "ws-01jjrstvwxyz0123456789abcd",
                "workspace_mode": "kept"
            })
        ),
        (status = 400, description = "The bearer token cannot be forwarded to the owning node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such job, or it was submitted by somebody else; absence and foreign ownership are deliberately indistinguishable", body = ErrorResponse),
        (status = 503, description = "The node owning this job could not be reached, so no cancellation was recorded; retryable, the caller may repeat the request", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn cancel_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(job_id): Path<String>,
) -> ServerResult<(StatusCode, Json<JobStatusResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;

    let outcome = cancel_job_routed(
        &state.get_ctx(),
        &state.jobs_runtime(),
        auth.user_id,
        job_id,
        forwarded_job_auth(bearer)?,
    )
    .await
    .map_err(map_job_route)?;

    match outcome {
        RoutedCancelOutcome::NotFound => Err(ServerError::NotFound),
        RoutedCancelOutcome::AlreadyTerminal(job) => {
            Ok((StatusCode::OK, Json(job_view_response(&job))))
        }
        RoutedCancelOutcome::Requested(job) => {
            Ok((StatusCode::ACCEPTED, Json(job_view_response(&job))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::{
        ArtifactRef, BackendLocation, BackendRef, ExportOmissionCounts, ExportRoCrateResult,
        ExportRoCrateSpec, FIRST_GRANTABLE_HANDLE, ImportMetadataTarget, ImportReportDetail,
        ImportRoCrateResult, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget,
        JobPayload, JobProgress, JobResultPayload, NodeCapabilities, PathRestriction, Permission,
        RealmId, ReasonCode, RoCrateLimits,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::types::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::store::{
        ClaimOutcome, claim_job, complete_job, insert_job, put_job_entry, transition_to_running,
    };
    use aruna_storage::FjallStorage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    /// One reduced family with a duplicate success and a locally exhausted view.
    fn family_report_fixture() -> FamilyReport {
        use aruna_core::jobs::{JobKind, JobStatusView};
        use aruna_core::structs::{
            EffectiveResources, ExecutionSpec, JobAdmissionRecord, JobRetryPolicy, LogicalJobSpec,
            LogicalJobState, OutputObject, PlacementRef, SubmissionId,
        };

        let created_by = user(2);
        let job_id = JobId::from_bytes([3u8; 16]);
        let submission_id = SubmissionId([5u8; 32]);
        let resources = EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        };
        let payload = ExecutionSpec {
            group_id: Ulid::from_bytes([6u8; 16]),
            name: None,
            description: None,
            tags: BTreeMap::new(),
            image: "img".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: BTreeMap::new(),
            resources: aruna_core::structs::ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        FamilyReport {
            job: JobStatusView {
                job_id,
                created_by,
                kind: JobKind::Execution,
                state: JobState::Indeterminate,
                attempts: 2,
                cancel_requested: false,
                created_at_ms: 10,
                updated_at_ms: 20,
                finished_at_ms: None,
                progress: JobProgress::new("phases"),
                last_error: None,
                result: None,
                workspace_bucket: None,
                workspace_mode: WorkspaceMode::Kept,
                locally_exhausted: false,
            },
            spec: LogicalJobSpec {
                submission_id,
                job_id,
                origin_node_id: node_id(),
                ingress_node_id: node_id(),
                realm_id: realm(),
                group_id: payload.group_id,
                created_by,
                created_at_ms: 10,
                retention_ms: aruna_core::structs::DEFAULT_JOB_RETENTION_MS,
                payload,
                request_digest: [7u8; 32],
                spec_digest: [8u8; 32],
                resources,
                retry: JobRetryPolicy {
                    max_launches_per_witness: 3,
                },
                admission: JobAdmissionRecord {
                    submission_id,
                    request_digest: [7u8; 32],
                    job_id,
                    group_id: Ulid::from_bytes([6u8; 16]),
                    admitting_node_id: node_id(),
                    membership_generation: 0,
                    resources,
                    admitted_at_ms: 10,
                },
                input_facts: Vec::new(),
                output_policies: Vec::new(),
                placement: PlacementRef::NIL,
            },
            submission_id,
            request_digest: [7u8; 32],
            canonical_job_id: job_id,
            aliases: vec![job_id, JobId::from_bytes([4u8; 16])],
            conflicts: 1,
            state: LogicalJobState::Indeterminate,
            canonical_execution_id: None,
            canonical_result: None,
            executions: 2,
            duplicate_successes: 1,
            outputs: vec![OutputObject {
                node_id: node_id(),
                bucket: "dest".to_string(),
                key: "out/r.txt".to_string(),
                version_id: Ulid::from_bytes([9u8; 16]),
                execution_id: Ulid::from_bytes([10u8; 16]),
                container_path: "/out/r.txt".to_string(),
                size: 3,
                digest: None,
            }],
            output_endpoints: BTreeMap::from([(node_id(), "https://s3.example".to_string())]),
            revision: 4,
            digest: [11u8; 32],
            cancel_requested: false,
            responder: Some(node_id()),
            partial: true,
            locally_exhausted: true,
            plan: None,
        }
    }

    #[test]
    fn partitioned_view_is_marked() {
        // A partitioned read must name its responder and say that it is local
        // and exhausted here, without ever reading as a converged failure.
        let response = family_response(&family_report_fixture());

        assert_eq!(response.logical_state, "indeterminate");
        assert!(response.locally_exhausted);
        assert!(response.partial);
        assert!(response.eventually_consistent);
        assert_eq!(
            response.responder_node_id.as_deref(),
            Some(&*node_id().to_string())
        );
        assert_eq!(response.alias_count, 2);
        assert_eq!(response.conflict_count, 1);
        assert_eq!(response.duplicate_successes, 1);
        assert_eq!(response.revision, 4);
        assert_eq!(response.projection_digest.len(), 64);
        assert!(response.canonical_execution_id.is_none());
    }

    #[test]
    fn outputs_keep_exact_versions() {
        // The exact VersionId and its producing execution are the identity of a
        // job output; the object's current version is a different question.
        let response = family_response(&family_report_fixture());
        let mut result = Some(serde_json::json!({ "outputs": [] }));
        bind_output_routes(&mut result, &response.outputs).expect("routes bind");

        assert_eq!(response.outputs.len(), 1);
        assert_eq!(
            response.outputs[0].version_id,
            Ulid::from_bytes([9u8; 16]).to_string()
        );
        assert_eq!(
            response.outputs[0].execution_id,
            Ulid::from_bytes([10u8; 16]).to_string()
        );
        assert_eq!(response.outputs[0].bucket, "dest");
        assert_eq!(
            result.as_ref().unwrap()["outputs"][0]["endpoint_url"],
            "https://s3.example"
        );
    }

    #[test]
    fn reads_without_endpoint() {
        // A node info document this responder lacks must not make a succeeded
        // family unreadable; only the address of that output is unknown.
        let mut report = family_report_fixture();
        report.output_endpoints.clear();

        let response = family_response(&report);
        assert_eq!(response.outputs.len(), 1);
        assert!(response.outputs[0].endpoint_url.is_none());
    }

    fn realm() -> RealmId {
        RealmId([1u8; 32])
    }

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    fn user(byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([byte; 16]), realm())
    }

    fn job_id(timestamp_ms: u64) -> JobId {
        JobId::from_parts(
            timestamp_ms,
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
            0,
        )
        .unwrap()
    }

    fn auth_for(user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        })
    }

    fn restricted_auth_for(user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: Some(vec![PathRestriction {
                pattern: "/realm/g/group/data/**".to_string(),
                permission: Permission::READ,
            }]),
            session: None,
        })
    }

    async fn build_state() -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let state = ServerState::new(
            ctx,
            realm(),
            node_id(),
            NodeCapabilities::user_node(realm()).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        (dir, Arc::new(state))
    }

    fn job_for(job_id: JobId, owner: UserId, created_at_ms: u64) -> JobRecord {
        JobRecord::new(
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
            created_at_ms,
            created_at_ms,
            None,
        )
    }

    fn import_job(job_id: JobId, owner: UserId) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: auth_for(owner).unwrap(),
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
                limits: RoCrateLimits::default(),
                document_id: Ulid::from_bytes([5u8; 16]),
            }),
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        )
    }

    fn report_row(entry_key: &str) -> ImportReportRow {
        ImportReportRow {
            entry_key: entry_key.to_string(),
            code: ReasonCode::Imported,
            message: None,
            detail: ImportReportDetail {
                archive_path: entry_key.to_string(),
                target_key: Some(entry_key.to_string()),
                version_id: None,
                blake3: None,
                size: None,
                arn: None,
                w3id: None,
                validation: None,
            },
        }
    }

    #[test]
    fn decodes_system_key() {
        let owner = user(2);
        let payload = import_job(JobId::from_bytes([9u8; 16]), owner).payload;
        for entry_key in [
            "signature/ro-crate-metadata.json.minisig",
            "warning/00000000",
            "failure/write",
        ] {
            let row = report_row(entry_key);
            let value = postcard::to_allocvec(&row).unwrap();
            let mut stored_key = vec![JOB_SYSTEM_ENTRY_PREFIX];
            stored_key.extend_from_slice(entry_key.as_bytes());
            let decoded = decode_report_row(JobKind::from(&payload), &stored_key, &value).unwrap();
            assert_eq!(decoded["entry_key"], entry_key);
        }
    }

    fn export_job(job_id: JobId, owner: UserId, expires_at_ms: u64) -> JobRecord {
        let document_id = Ulid::from_bytes([6u8; 16]);
        let blake3 = [7u8; 32];
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), blake3.to_vec());
        let artifact = ArtifactRef {
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/tmp".to_string(),
                storage_bucket: "hidden".to_string(),
                backend_path: format!("_jobs/{job_id}/artifact.zip"),
                ulid: Ulid::from_bytes([8u8; 16]),
                compressed: false,
                encrypted: false,
                created_by: owner,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 5,
                hashes,
            },
            blake3,
            size: 5,
            expires_at_ms,
        };
        let digest = *blake3::hash(&[]).as_bytes();
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ExportRoCrate(ExportRoCrateSpec {
                auth_context: auth_for(owner).unwrap(),
                document_id,
                limits: RoCrateLimits::default(),
            }),
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        record.report_digest = Some(digest);
        record.result = Some(JobResultPayload::ExportRoCrate(ExportRoCrateResult {
            artifact: Some(artifact),
            included: 1,
            omitted: ExportOmissionCounts::default(),
            report_digest: digest,
        }));
        record
    }

    async fn finish_report(state: &ServerState, job_id: JobId, owner: UserId) -> [u8; 32] {
        let context = state.get_ctx();
        let storage = &context.storage_handle;
        insert_job(storage, &import_job(job_id, owner))
            .await
            .unwrap();
        let ClaimOutcome::Claimed(record) =
            claim_job(storage, job_id, node_id(), 2_000).await.unwrap()
        else {
            panic!("job was not claimed")
        };
        let token = record.claim.unwrap().claim_token;
        put_job_entry(storage, job_id, token, b"b", &report_row("b"))
            .await
            .unwrap();
        put_job_entry(storage, job_id, token, b"a", &report_row("a"))
            .await
            .unwrap();
        transition_to_running(storage, job_id, token, 2_500)
            .await
            .unwrap();
        complete_job(
            storage,
            job_id,
            token,
            JobResultPayload::ImportRoCrate(ImportRoCrateResult {
                document_id: Some(Ulid::from_bytes([5u8; 16])),
                entries_total: 2,
                imported: 2,
                unlisted: 0,
                failed: 0,
                report_digest: [0u8; 32],
            }),
            JobProgress {
                current: 2,
                total: Some(2),
                unit: "entries".to_string(),
            },
            aruna_core::util::unix_timestamp_millis(),
        )
        .await
        .unwrap()
        .report_digest
        .unwrap()
    }

    async fn response_json<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn list_newest_first() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        for seq in 1..=3u64 {
            insert_job(
                &state.get_ctx().storage_handle,
                &job_for(job_id(seq), owner, seq * 1000),
            )
            .await
            .unwrap();
        }

        let (_, Json(page1)) = list_jobs(
            State(state.clone()),
            Extension(auth_for(owner)),
            Query(ListJobsQuery {
                limit: Some(2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(page1.jobs.len(), 2);
        // Newest first: seq 3 then seq 2.
        assert_eq!(page1.jobs[0].job_id, job_id(3).to_string());
        let cursor = page1.next_cursor.clone().expect("cursor for next page");

        let (_, Json(page2)) = list_jobs(
            State(state.clone()),
            Extension(auth_for(owner)),
            Query(ListJobsQuery {
                limit: Some(2),
                cursor: Some(cursor),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(page2.jobs[0].job_id, job_id(1).to_string());
        assert!(page2.next_cursor.is_none());
    }

    #[tokio::test]
    async fn report_pages_frozen() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([9u8; 16]);
        let digest = finish_report(&state, job_id, owner).await;

        let first = get_job_report(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
            Query(ReportQuery {
                limit: Some(1),
                cursor: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first: JobReportResponse = response_json(first).await;
        assert_eq!(first.rows[0]["entry_key"], "a");
        assert_eq!(first.report_digest, hex::encode(digest));

        let second = get_job_report(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
            Query(ReportQuery {
                limit: Some(1),
                cursor: first.next_cursor,
            }),
        )
        .await
        .unwrap();
        let second: JobReportResponse = response_json(second).await;
        assert_eq!(second.rows[0]["entry_key"], "b");
        assert!(second.next_cursor.is_none());

        let conflict = ReportCursor {
            job_id,
            report_digest: [0u8; 32],
            last_key: b"a".to_vec(),
        };
        let conflict = URL_SAFE_NO_PAD.encode(postcard::to_allocvec(&conflict).unwrap());
        let response = get_job_report(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
            Query(ReportQuery {
                limit: None,
                cursor: Some(conflict),
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let foreign = get_job_report(
            State(state),
            Extension(auth_for(user(3))),
            Extension(None),
            Path(job_id.to_string()),
            Query(ReportQuery::default()),
        )
        .await;
        assert!(matches!(foreign, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn report_pending_typed() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([10u8; 16]);
        insert_job(&state.get_ctx().storage_handle, &import_job(job_id, owner))
            .await
            .unwrap();

        let response = get_job_report(
            State(state),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
            Query(ReportQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: ReportPendingResponse = response_json(response).await;
        assert_eq!(body.code, "report_pending");
        assert_eq!(body.state, "queued");
    }

    #[test]
    fn submit_conflict_typed() {
        let existing_job_id = JobId::from_bytes([15u8; 16]);
        let error = map_submit_error(
            aruna_operations::jobs::submit::SubmitJobError::JobPlanConflict { existing_job_id },
        );
        assert!(matches!(
            error,
            ServerError::JobPlanConflict(message)
                if message.contains(&existing_job_id.to_string())
        ));
    }

    #[test]
    fn report_openapi_union() {
        let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
        let schema = &openapi["paths"]["/jobs/{job_id}/report"]["get"]["responses"]["404"]["content"]
            ["application/json"]["schema"];
        assert_eq!(
            schema["$ref"],
            "#/components/schemas/ReportUnavailableResponse"
        );
        let variants = openapi["components"]["schemas"]["ReportUnavailableResponse"]["oneOf"]
            .as_array()
            .unwrap();
        assert!(
            variants
                .iter()
                .any(|variant| variant["$ref"] == "#/components/schemas/ReportPendingResponse")
        );
        assert!(
            variants
                .iter()
                .any(|variant| variant["$ref"] == "#/components/schemas/ErrorResponse")
        );
    }

    #[tokio::test]
    async fn artifact_head_headers() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([11u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &export_job(
                job_id,
                owner,
                aruna_core::util::unix_timestamp_millis() + 60_000,
            ),
        )
        .await
        .unwrap();

        let response = head_job_artifact(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
        assert_eq!(response.headers()[CONTENT_LENGTH], "5");
        assert_eq!(
            response.headers()[ETAG].to_str().unwrap(),
            format!("\"{}\"", hex::encode([7u8; 32]))
        );
        let document_id = Ulid::from_bytes([6u8; 16]);
        let disposition = response.headers()[CONTENT_DISPOSITION].to_str().unwrap();
        assert!(disposition.contains(&format!("filename=\"{document_id}.zip\"")));
        assert!(disposition.contains(&format!("filename*=UTF-8''{document_id}%2Ezip")));

        let foreign = head_job_artifact(
            State(state),
            Extension(auth_for(user(3))),
            Extension(None),
            Path(job_id.to_string()),
            HeaderMap::new(),
        )
        .await;
        assert!(matches!(foreign, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn artifact_expiry_gone() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([12u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &export_job(job_id, owner, 1),
        )
        .await
        .unwrap();

        let response = head_job_artifact(
            State(state),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::GONE);
    }

    #[test]
    fn range_parses_single() {
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_static("bytes=2-4"));
        assert_eq!(
            range_request(&headers),
            Ok(Some(ObjectRangeRequest::StartEnd { start: 2, end: 4 }))
        );
        headers.insert(RANGE, HeaderValue::from_static("bytes=-3"));
        assert_eq!(
            range_request(&headers),
            Ok(Some(ObjectRangeRequest::Suffix { length: 3 }))
        );
        headers.insert(RANGE, HeaderValue::from_static("bytes=2-3,5-6"));
        assert_eq!(range_request(&headers), Err(()));
    }

    #[test]
    fn forwarded_auth_bounds() {
        assert!(forwarded_job_auth(None).unwrap().is_none());
        let accepted = ValidatedArunaBearerTokenCarrier::new_for_test("a".repeat(4_096));
        assert!(forwarded_job_auth(Some(accepted)).unwrap().is_some());
        let rejected = ValidatedArunaBearerTokenCarrier::new_for_test("a".repeat(4_097));
        assert!(matches!(
            forwarded_job_auth(Some(rejected)),
            Err(ServerError::BadRequest)
        ));
    }

    #[tokio::test]
    async fn urls_are_absolute() {
        let (_dir, state) = build_state().await;
        state
            .register_rest_interface_with_public_url(
                "127.0.0.1:3000".parse().unwrap(),
                Some("https://owner.example/"),
            )
            .await;
        let job_id = JobId::from_bytes([13u8; 16]);
        let urls = job_urls(&state, job_id).await.unwrap();
        assert_eq!(urls.owner_node_url, "https://owner.example/api/v1");
        assert_eq!(
            urls.status_url,
            format!("https://owner.example/api/v1/jobs/{job_id}")
        );
        assert_eq!(
            urls.report_url,
            format!("https://owner.example/api/v1/jobs/{job_id}/report")
        );
        assert_eq!(
            urls.artifact_url,
            format!("https://owner.example/api/v1/jobs/{job_id}/artifacts/rocrate")
        );
    }

    #[tokio::test]
    async fn foreign_not_found() {
        let (_dir, state) = build_state().await;
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &job_for(job_id, user(2), 1000),
        )
        .await
        .unwrap();

        let result = get_job(
            State(state.clone()),
            Extension(auth_for(user(3))),
            Extension(None),
            Path(job_id.to_string()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn cancel_is_idempotent() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([9u8; 16]);
        // has_run keeps this job off the never-run direct-cancel fast path (see
        // set_cancel_requested), so it stays live and cancel_requested-flagged across
        // repeated cancel calls, which is what this test exercises.
        let mut record = job_for(job_id, owner, 1000);
        record.has_run = true;
        insert_job(&state.get_ctx().storage_handle, &record)
            .await
            .unwrap();

        let (status, _) = cancel_job(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);

        // Repeated cancel of a still-live job stays 202.
        let (status, Json(body)) = cancel_job(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.cancel_requested);
    }

    #[tokio::test]
    async fn cancel_terminal_noop() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([4u8; 16]);
        let mut record = job_for(job_id, owner, 1000);
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2000);
        insert_job(&state.get_ctx().storage_handle, &record)
            .await
            .unwrap();

        let (status, _) = cancel_job(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn openapi_has_jobs() {
        let openapi = crate::openapi::ApiDoc::openapi();
        assert!(openapi.paths.paths.contains_key("/jobs/"));
        assert!(openapi.paths.paths.contains_key("/jobs/{job_id}"));
        assert!(openapi.paths.paths.contains_key("/jobs/{job_id}/cancel"));
        assert!(openapi.paths.paths.contains_key("/jobs/{job_id}/report"));
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/jobs/{job_id}/artifacts/rocrate")
        );
    }

    // A path-restricted (delegated) token must not reach any user-scoped job surface.
    #[tokio::test]
    async fn restricted_token_rejected() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &job_for(job_id, owner, 1000),
        )
        .await
        .unwrap();

        let list = list_jobs(
            State(state.clone()),
            Extension(restricted_auth_for(owner)),
            Query(ListJobsQuery::default()),
        )
        .await;
        assert!(matches!(list, Err(ServerError::Forbidden)));

        let get = get_job(
            State(state.clone()),
            Extension(restricted_auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
        )
        .await;
        assert!(matches!(get, Err(ServerError::Forbidden)));

        let cancel = cancel_job(
            State(state.clone()),
            Extension(restricted_auth_for(owner)),
            Extension(None),
            Path(job_id.to_string()),
        )
        .await;
        assert!(matches!(cancel, Err(ServerError::Forbidden)));
    }

    fn local_request() -> SubmitExecutionRequest {
        SubmitExecutionRequest {
            group_id: Ulid::from_bytes([5u8; 16]).to_string(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            env: BTreeMap::new(),
            tags: BTreeMap::new(),
            workdir: None,
            cpu_cores: None,
            ram_bytes: None,
            max_walltime_ms: None,
            executor_constraint: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
            idempotency_key: None,
            workspace: None,
            target: Some(ExecutionTarget::Local),
        }
    }

    async fn management_state() -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let capabilities =
            NodeCapabilities::management_node(aruna_core::keys::generate_signing_key()).unwrap();
        let state = ServerState::new(
            ctx,
            realm(),
            node_id(),
            capabilities,
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        (dir, Arc::new(state))
    }

    async fn enroll_device(state: &ServerState, owner: UserId) {
        use aruna_core::structs::{Actor, RealmConfigDocument, RealmNodeKind};
        let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
        config.seed_default_placement();
        config.ensure_node(node_id(), RealmNodeKind::User { owner });
        let actor = Actor {
            node_id: node_id(),
            user_id: UserId::nil(realm()),
            realm_id: realm(),
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        let event = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: realm().as_bytes().to_vec().into(),
                value: bytes.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            aruna_core::events::Event::Storage(
                aruna_core::events::StorageEvent::WriteResult { .. }
            )
        ));
    }

    #[tokio::test]
    async fn local_needs_device() {
        // The realm never runs a job locally on behalf of a target it has not
        // enrolled as somebody's machine.
        let (_dir, state) = management_state().await;

        let result = submit_job(
            State(state),
            Extension(auth_for(user(2))),
            Extension(None),
            Json(local_request()),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
    }

    #[tokio::test]
    async fn device_checks_group() {
        // A device caches the group documents, so its group check is the realm's
        // own: an owner holding no grant is refused here as on a realm node.
        let (_dir, state) = build_state().await;
        enroll_device(&state, user(2)).await;

        for target in [None, Some(ExecutionTarget::Local)] {
            let result = submit_job(
                State(state.clone()),
                Extension(auth_for(user(2))),
                Extension(None),
                Json(SubmitExecutionRequest {
                    target,
                    ..local_request()
                }),
            )
            .await;
            assert!(
                matches!(result, Err(ServerError::Forbidden)),
                "{target:?} must run the local group check"
            );
        }
    }

    #[tokio::test]
    async fn local_refuses_stranger() {
        let (_dir, state) = build_state().await;
        enroll_device(&state, user(2)).await;

        let result = submit_job(
            State(state),
            Extension(auth_for(user(3))),
            Extension(None),
            Json(local_request()),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[test]
    fn local_names_holder() {
        // Only a local run may name the holder: the planner seals it otherwise.
        let input = ExecutionInputRequest {
            bucket: "src".to_string(),
            key: "data.csv".to_string(),
            version_id: Some(Ulid::from_bytes([4u8; 16]).to_string()),
            source_node_id: Some(node_id().to_string()),
            dest_key: "data.csv".to_string(),
            container_path: None,
            mode: InputModeRequest::Snapshot,
        };

        assert!(matches!(
            native_input(input.clone(), ExecutionTarget::Realm),
            Err(ServerError::BadRequestMessage(_))
        ));
        assert_eq!(
            native_input(input, ExecutionTarget::Local)
                .unwrap()
                .source_node_id,
            Some(node_id())
        );
    }

    #[tokio::test]
    async fn rejects_huge_ram() {
        // ram_bytes above i64::MAX would wrap negative in the Docker backend.
        let (_dir, state) = build_state().await;
        for ram_bytes in [u64::MAX, i64::MAX as u64 + 1, 0] {
            let request = SubmitExecutionRequest {
                group_id: Ulid::from_bytes([5u8; 16]).to_string(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: vec!["true".to_string()],
                env: BTreeMap::new(),
                tags: BTreeMap::new(),
                workdir: None,
                cpu_cores: None,
                ram_bytes: Some(ram_bytes),
                max_walltime_ms: None,
                executor_constraint: None,
                inputs: Vec::new(),
                outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
                idempotency_key: None,
                workspace: None,
                target: None,
            };
            let result = submit_job(
                State(state.clone()),
                Extension(auth_for(user(2))),
                Extension(None),
                Json(request),
            )
            .await;
            assert!(
                matches!(result, Err(ServerError::BadRequest)),
                "ram_bytes {ram_bytes} must be rejected"
            );
        }
    }

    #[test]
    fn maps_native_input() {
        // Missing container_path defaults to /inputs/<dest_key>.
        let input = ExecutionInputRequest {
            bucket: "src".to_string(),
            key: "data.csv".to_string(),
            version_id: None,
            source_node_id: None,
            dest_key: "in/data.csv".to_string(),
            container_path: None,
            mode: InputModeRequest::Snapshot,
        };
        let mapped = native_input(input.clone(), ExecutionTarget::Realm).unwrap();
        assert_eq!(
            mapped.container_path.as_deref(),
            Some("/inputs/in/data.csv")
        );

        let explicit = ExecutionInputRequest {
            container_path: Some("/data/input.csv".to_string()),
            ..input.clone()
        };
        assert_eq!(
            native_input(explicit, ExecutionTarget::Realm)
                .unwrap()
                .container_path
                .as_deref(),
            Some("/data/input.csv")
        );

        let traversal = ExecutionInputRequest {
            container_path: Some("/in/../etc".to_string()),
            ..input
        };
        assert!(native_input(traversal, ExecutionTarget::Realm).is_err());
    }

    #[test]
    fn maps_native_output() {
        let mapped = native_output(ExecutionOutputRequest {
            container_path: "/out/report.txt".to_string(),
            dest_key: "outputs/report.txt".to_string(),
        })
        .unwrap();
        assert_eq!(mapped.container_path, "/out/report.txt");
        assert_eq!(mapped.dest_key, "outputs/report.txt");

        assert!(
            native_output(ExecutionOutputRequest {
                container_path: "relative/path".to_string(),
                dest_key: "k".to_string(),
            })
            .is_err()
        );
        assert!(
            native_output(ExecutionOutputRequest {
                container_path: "/out".to_string(),
                dest_key: String::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn normalizes_prefixes() {
        assert_eq!(
            validate_output_prefixes(vec!["results/".to_string(), "results/".to_string()]).unwrap(),
            ["results/"]
        );
        assert!(validate_output_prefixes(vec![String::new()]).is_err());
        assert!(
            validate_output_prefixes(vec!["result".to_string(); MAX_OUTPUT_PREFIXES + 1]).is_err()
        );
    }

    #[test]
    fn workspace_defaults_kept() {
        assert_eq!(
            workspace_request(None).unwrap(),
            (WorkspaceMode::Kept, None)
        );
        let record = job_for(job_id(1), user(2), 1);
        assert_eq!(job_status_response(&record).workspace_mode, "kept");
        assert!(
            workspace_request(Some(WorkspaceRequest {
                mode: WorkspaceModeRequest::Existing,
                bucket: None,
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_cursor_rejected() {
        let (_dir, state) = build_state().await;
        let result = list_jobs(
            State(state.clone()),
            Extension(auth_for(user(2))),
            Query(ListJobsQuery {
                cursor: Some("not-base64!".to_string()),
                ..Default::default()
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }
}
