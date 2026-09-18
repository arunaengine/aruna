//! Serves the compute job routes to submit, list, read, report on, cancel and delete runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use aruna_core::structs::execution::job::{
    CompositionError, ExportReportRow, ImportReportRow, JobId, JobRecord, JobState,
    SYSTEM_ENTRY_PREFIX,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::device::compute::LocalExecutionError;
use aruna_operations::jobs::command::{
    CollisionPolicy as CommandCollisionPolicy, ExecutionInput, ExecutionOutput,
    ExecutionTarget as CommandExecutionTarget, InputMode as CommandInputMode, SessionMountSpec,
    SubmitExecutionCommand, WorkspaceMode as CommandWorkspaceMode, WorkspaceSpec,
};
use aruna_operations::jobs::lifecycle::{FamilyReport, family_report};
use aruna_operations::jobs::service::{
    ArtifactLookup, JobKind, JobReportLookup, JobStatusView, OwnedArtifact, RoutedCancelOutcome,
    cancel_job_routed, delete_owned_run, list_owned_jobs, read_artifact_routed, read_job_routed,
    read_report_routed,
};
use aruna_operations::jobs::store::RunDelete;
use aruna_operations::jobs::{JobRouteError, REPORT_MAX_ROWS};
use aruna_operations::s3::object::get::ObjectRangeRequest;
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
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, require_unrestricted_auth};
use crate::download::{self, AdmissionError};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::jobs::{JobRequestError, admit_execution, hex32};
use crate::rate_limit::LocalKey;
use crate::server::state::ServerState;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 200;
const DEFAULT_REPORT_LIMIT: usize = 200;

#[derive(OpenApi)]
#[openapi(
    tags((name = "compute/jobs", description = "Durable background jobs"))
)]
pub struct JobsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(JobsApiDoc::openapi())
        .routes(routes!(list_jobs, submit_job))
        .routes(routes!(get_job, delete_job))
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
    /// Input name, for example `reads.fastq.gz`; it names no bucket key. Must
    /// not be empty and must be unique across the declared inputs.
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
    /// Destination key this output is written to, for example
    /// `results/result.json`. Must not be empty and must be unique per bucket.
    pub dest_key: String,
    /// Destination bucket of this output, for example `project-data`. Required
    /// when `workspace.mode` is `none`, and defaults to the workspace bucket
    /// under `existing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

/// Which bucket a run works inside. `none` creates and touches no bucket of its
/// own, and `existing` runs inside the named bucket the caller already owns.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceModeRequest {
    #[default]
    None,
    Existing,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
pub struct WorkspaceRequest {
    /// `none` runs without a bucket of its own, `existing` runs inside the named
    /// bucket. An omitted workspace block is `none`.
    pub mode: WorkspaceModeRequest,
    /// Required by `existing` mode and refused by `none`. The bucket must exist,
    /// belong to the same group, and be writable by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

/// The slice of the workspace bucket a session sees as a folder. Everything
/// written below that folder lands in the bucket and every object under the
/// prefix shows up in it, when the executing backend has an S3 mount driver.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, rmcp::schemars::JsonSchema)]
pub struct SessionMountRequest {
    /// Folder in the bucket, for example `raw/2024`; an empty string mounts the
    /// whole bucket. Omitted, the `data/` folder is mounted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Absolute folder below the working directory the prefix appears at, for
    /// example `/work/raw`. Omitted, it is `data` below the working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
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
    /// Short human name for the run, for example `GC content by sample`. Shown
    /// wherever the run is listed. Defaults to absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Longer note about what the run does and why. Defaults to absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// OCI image the task runs, for example `docker.io/library/python:3.13-slim`.
    /// Must not be blank unless `runtime` names a session runtime, which fills
    /// the image, entrypoint and command instead.
    #[serde(default)]
    pub image: String,
    /// Session runtime catalog id, for example `python-notebook`. Required by a
    /// submission carrying the tag `aruna-engine.org/session`, refused without
    /// it. It fills `image`, `entrypoint` and `command`, which must be empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    /// Idle wait of a session job in milliseconds, for example `600000` for ten
    /// minutes. The executing node clamps it to the realm's value, so a longer
    /// request never extends the session. Refused outside a session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "session_idle_after_ms")]
    pub session_idle_ms: Option<u64>,
    /// Which part of the workspace bucket a session mounts, and where. Refused
    /// outside a session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_mount: Option<SessionMountRequest>,
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
    /// Container paths captured after the run, at most 1024. Each names the
    /// bucket it lands in, or falls back to the workspace bucket.
    #[serde(default)]
    pub outputs: Vec<ExecutionOutputRequest>,
    #[serde(default)]
    /// Workspace prefixes whose versions this execution wrote and inventories at completion.
    /// Requires `workspace.mode` `existing` and at least one bucket-qualified output.
    pub output_prefixes: Vec<String>,
    /// How a destination key already claimed by another declared input or by an
    /// object in the workspace bucket is resolved. Defaults to `reject`.
    #[serde(default)]
    pub collision_policy: CollisionPolicyRequest,
    /// Caller-chosen key that makes a resubmission return the same job instead
    /// of starting a second one. A different request under a used key is a 409.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Which bucket the run works inside. Absent means `none`: the run touches
    /// no bucket of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceRequest>,
    /// `realm` (the default) admits the job into the realm; `local` runs it on
    /// this machine and is served by a user device only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<ExecutionTarget>,
}

/// REST maps its schema-bearing request body into the transport-independent
/// application command the admission entry point consumes.
impl From<SubmitExecutionRequest> for SubmitExecutionCommand {
    fn from(request: SubmitExecutionRequest) -> Self {
        Self {
            group_id: request.group_id,
            name: request.name,
            description: request.description,
            image: request.image,
            runtime: request.runtime,
            session_idle_ms: request.session_idle_ms,
            session_mount: request.session_mount.map(Into::into),
            entrypoint: request.entrypoint,
            command: request.command,
            env: request.env,
            tags: request.tags,
            workdir: request.workdir,
            cpu_cores: request.cpu_cores,
            ram_bytes: request.ram_bytes,
            max_walltime_ms: request.max_walltime_ms,
            executor_constraint: request.executor_constraint,
            inputs: request.inputs.into_iter().map(Into::into).collect(),
            outputs: request.outputs.into_iter().map(Into::into).collect(),
            output_prefixes: request.output_prefixes,
            collision_policy: request.collision_policy.into(),
            idempotency_key: request.idempotency_key,
            workspace: request.workspace.map(Into::into),
            target: request.target.map(Into::into),
        }
    }
}

impl From<ExecutionInputRequest> for ExecutionInput {
    fn from(input: ExecutionInputRequest) -> Self {
        Self {
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            source_node_id: input.source_node_id,
            dest_key: input.dest_key,
            container_path: input.container_path,
            mode: input.mode.into(),
        }
    }
}

impl From<ExecutionOutputRequest> for ExecutionOutput {
    fn from(output: ExecutionOutputRequest) -> Self {
        Self {
            container_path: output.container_path,
            dest_key: output.dest_key,
            bucket: output.bucket,
        }
    }
}

impl From<InputModeRequest> for CommandInputMode {
    fn from(mode: InputModeRequest) -> Self {
        match mode {
            InputModeRequest::Snapshot => Self::Snapshot,
            InputModeRequest::FloatingReference => Self::FloatingReference,
            InputModeRequest::ExactReference => Self::ExactReference,
        }
    }
}

impl From<CollisionPolicyRequest> for CommandCollisionPolicy {
    fn from(policy: CollisionPolicyRequest) -> Self {
        match policy {
            CollisionPolicyRequest::Reject => Self::Reject,
            CollisionPolicyRequest::Replace => Self::Replace,
            CollisionPolicyRequest::KeepExisting => Self::KeepExisting,
        }
    }
}

impl From<WorkspaceModeRequest> for CommandWorkspaceMode {
    fn from(mode: WorkspaceModeRequest) -> Self {
        match mode {
            WorkspaceModeRequest::None => Self::None,
            WorkspaceModeRequest::Existing => Self::Existing,
        }
    }
}

impl From<WorkspaceRequest> for WorkspaceSpec {
    fn from(workspace: WorkspaceRequest) -> Self {
        Self {
            mode: workspace.mode.into(),
            bucket: workspace.bucket,
        }
    }
}

impl From<SessionMountRequest> for SessionMountSpec {
    fn from(mount: SessionMountRequest) -> Self {
        Self {
            prefix: mount.prefix,
            path: mount.path,
        }
    }
}

impl From<ExecutionTarget> for CommandExecutionTarget {
    fn from(target: ExecutionTarget) -> Self {
        match target {
            ExecutionTarget::Realm => Self::Realm,
            ExecutionTarget::Local => Self::Local,
        }
    }
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
    /// Family state at acceptance or the current reduced state for an idempotent replay.
    /// This is a snapshot; poll `status_url` for the live state.
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
    /// Type the key's extension implies, so a caller can tell a chart from a
    /// log without a second call. `stat_object` reports the stored type.
    #[serde(default)]
    pub content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Node-local S3 endpoint owning this version, which may differ from the execution node.
    /// Null when the responder does not know the endpoint; other output remains available.
    pub endpoint_url: Option<String>,
}

/// One input the plan moves to the target, so a caller can show where the data
/// comes from and what the move was expected to cost.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = JobPlacementInputResponse)]
pub struct JobInputResponse {
    pub destination_key: String,
    pub bytes: u64,
    /// Null when the target already holds the compliant copy, so nothing moves.
    pub source_node_id: Option<String>,
    pub transfer_ms: u64,
}

/// One target a planning round looked at, in report order: the selected target
/// first, then the alternatives by rank, then the rejected ones.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = JobPlacementCandidateResponse)]
pub struct JobCandidateResponse {
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_kind: Option<String>,
    /// `selected`, `ranked` or `rejected`.
    pub verdict: String,
    /// Place among the alternatives, counted from one. Absent for the selected
    /// target and for a rejected one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
    /// Why the target was rejected, in plain words. Absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Uses this responder's stored plan, or otherwise the newest witnessed launch record.
/// Only a local planning round supplies `alternatives`, `rejected`, and `omitted` counts.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobPlacementResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_kind: Option<String>,
    /// The node the execution was planned to run on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    /// The node that planned the launch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler_node_id: Option<String>,
    pub estimated_transfer_bytes: u64,
    pub estimated_transfer_ms: u64,
    pub alternatives: u32,
    pub rejected: u32,
    /// Rejections the bound dropped, so truncation never reads as agreement.
    pub omitted: u32,
    pub stored_at_ms: u64,
    pub inputs: Vec<JobInputResponse>,
    /// Every target of the planning round. Filled only on the node that planned
    /// the request; empty when the placement comes from a launch record.
    pub candidates: Vec<JobCandidateResponse>,
}

/// One physical execution of the family, canonical or not.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobExecutionResponse {
    pub execution_id: String,
    pub executor_node_id: String,
    pub state: String,
    /// When the work itself started: the first running update, else the moment
    /// the target accepted the launch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// When this responder last saw an update of the execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at_ms: Option<u64>,
    /// The execution the family's outcome is taken from.
    pub canonical: bool,
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
    /// Every known execution of the family, next to the count above.
    pub execution_list: Vec<JobExecutionResponse>,
    pub duplicate_successes: u32,
    pub outputs: Vec<JobOutputResponse>,
    pub revision: u64,
    pub projection_digest: String,
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
        status_url: format!("{api_base_url}/compute/jobs/{job_id}"),
        report_url: format!("{api_base_url}/compute/jobs/{job_id}/report"),
        artifact_url: format!("{api_base_url}/compute/jobs/{job_id}/artifacts/rocrate"),
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
    /// Present for a notebook session: the catalog runtime it runs, for example
    /// `python-notebook`. Absent for every other job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_runtime: Option<String>,
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
        session_runtime: job.session_runtime.clone(),
        run_crate: None,
        family: None,
    }
}

pub(crate) fn output_response(
    output: &aruna_core::structs::execution::job::OutputObject,
    endpoint_url: Option<&String>,
) -> JobOutputResponse {
    JobOutputResponse {
        bucket: output.bucket.clone(),
        version_id: output.version_id.to_string(),
        execution_id: output.execution_id.to_string(),
        container_path: output.container_path.clone(),
        size: output.size,
        content_type: aruna_core::structs::storage::blob::key_content_type(&output.key).to_string(),
        key: output.key.clone(),
        digest: output.digest.clone(),
        endpoint_url: endpoint_url.cloned(),
    }
}

/// Projects the reduced family. Node ids of the executions and of the placement
/// are disclosed, because a caller needs them to tell one run from another.
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
        execution_list: report
            .execution_list
            .iter()
            .map(|execution| JobExecutionResponse {
                execution_id: execution.execution_id.to_string(),
                executor_node_id: execution.executor_node_id.to_string(),
                state: execution.state.name().to_string(),
                started_at_ms: execution.started_at_ms,
                observed_at_ms: execution.observed_at_ms,
                canonical: report.canonical_execution_id == Some(execution.execution_id),
            })
            .collect(),
        duplicate_successes: report.duplicate_successes,
        outputs,
        revision: report.revision,
        projection_digest: hex32(&report.digest),
        responder_node_id: report.responder.map(|node| node.to_string()),
        partial: report.partial,
        locally_exhausted: report.locally_exhausted,
        cancel_requested: report.cancel_requested,
        placement: report.plan.as_ref().map(|plan| JobPlacementResponse {
            executor_kind: plan
                .target
                .as_ref()
                .map(|target| target.executor_kind.clone()),
            target_node_id: plan
                .target
                .as_ref()
                .map(|target| target.node_id.to_string()),
            scheduler_node_id: plan.scheduler_node_id.map(|node| node.to_string()),
            estimated_transfer_bytes: plan.estimated_transfer_bytes,
            estimated_transfer_ms: plan.estimated_transfer_ms,
            alternatives: plan.alternatives,
            rejected: plan.rejected,
            omitted: plan.omitted,
            stored_at_ms: plan.stored_at_ms,
            inputs: plan
                .inputs
                .iter()
                .map(|input| JobInputResponse {
                    destination_key: input.destination_key.clone(),
                    bytes: input.bytes,
                    source_node_id: input.source_node_id.map(|node| node.to_string()),
                    transfer_ms: input.transfer_ms,
                })
                .collect(),
            candidates: plan
                .candidates
                .iter()
                .map(|candidate| JobCandidateResponse {
                    node_id: candidate.node_id.to_string(),
                    executor_kind: candidate.executor_kind.clone(),
                    verdict: candidate.verdict.name().to_string(),
                    rank: candidate.rank,
                    reason: candidate.reason.clone(),
                })
                .collect(),
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

pub(crate) fn forwarded_job_auth(
    bearer: Option<ValidatedBearer>,
) -> ServerResult<Option<aruna_operations::metadata::AuthToken>> {
    aruna_operations::metadata::api::forwarded_bearer(bearer.as_ref().map(ValidatedBearer::as_str))
        .map_err(crate::metadata::map_api_error)
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

/// The REST status and body of a transport-independent job refusal.
pub(crate) fn map_job_request(error: JobRequestError) -> ServerError {
    match error {
        JobRequestError::BadRequest => ServerError::BadRequest,
        JobRequestError::BadRequestMessage(message) => ServerError::BadRequestMessage(message),
        JobRequestError::Unauthorized => ServerError::Unauthorized,
        JobRequestError::Forbidden => ServerError::Forbidden,
        JobRequestError::NotFound => ServerError::NotFound,
        JobRequestError::Conflict(message) => ServerError::Conflict(message),
        JobRequestError::JobPlanConflict(message) => ServerError::JobPlanConflict(message),
        JobRequestError::ComputeQuotaDenied(denied) => ServerError::ComputeQuotaDenied(denied),
        JobRequestError::ServiceUnavailableReason(message) => {
            ServerError::ServiceUnavailableReason(message)
        }
        JobRequestError::InternalError(message) => ServerError::InternalError(message),
    }
}

#[utoipa::path(
    get,
    path = "/compute/jobs",
    tag = "compute/jobs",
    summary = "List the caller's jobs on this node",
    description = r#"Pages the jobs the caller submitted to this node, newest first.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused even for the
caller's own jobs.

**Behavior**
- The page is self-scoped and node-local: only jobs the caller submitted, and only jobs this node
  owns. On a user device that includes every local run the owner started there; they have no
  separate listing.
- Jobs owned by another node are never merged in, so page that node instead (submission answers
  with the `origin_node_url` it was accepted at).
- A distributed execution job is listed where it was admitted or is running; read one by id for
  the replicated family view, which any node holding its records can answer.
- Jobs the system creates for its own bookkeeping are never listed.
- A page without `next_cursor` is the last one.

**Limits**
- `limit` defaults to 50 and is capped at 200; `0` is treated as unset.
- `cursor` is a previous page's `next_cursor`: 24 bytes, base64url without padding.
- `state` is one of `queued`, `claimed`, `preparing`, `ready`, `running`, `cancelling`,
  `indeterminate`, `succeeded`, `failed` or `cancelled`."#,
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
                        "workspace_mode": "none"
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
    let auth = require_unrestricted_auth(&state, auth)?;
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
    path = "/compute/jobs",
    tag = "compute/jobs",
    summary = "Submit a container execution job",
    description = r#"Accepts a container execution job for asynchronous execution and returns the id to poll.

**Authentication**: realm bearer token with WRITE on the target group's data; a path-restricted
(delegated) token is refused. An existing workspace bucket and every bucket an output names
additionally need WRITE on that bucket, which must belong to the same group.

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
- `idempotency_key` is scoped to the caller: replaying it with the same plan answers 200 with
  `created` false and the state this responder currently reduces, so a running or finished request
  reads `running`, `succeeded`, `failed`, `cancelled` or `indeterminate` rather than `queued`.
- The group's standing quota is decided against a replicated demand view, but a replay of a key
  this node already claimed is settled before any quota is read and is never quota-refused.
- A run never creates a bucket. Omitting `workspace` (or `workspace.mode` `none`) captures the
  declared outputs to the buckets they name, so every output needs its own `bucket`;
  `workspace.mode` `existing` runs inside a bucket the caller already owns, which is where an
  output without a `bucket` and `output_prefixes` resolve their `dest_key`.
- Every bucket a run writes into, the workspace and each output destination alike, must exist,
  belong to the execution group, and grant the caller WRITE.
- On a user device a `realm` request is always forwarded under the caller's own bearer token: the
  inputs stay referenced, the outputs land on the admitting realm holder, and the device itself
  never executes, admits or stores any part of the job.
- `target` `local` is served by a user device only and runs the job on that machine for the user
  the device is enrolled for. Nothing about it is forwarded, replicated or offered to the realm,
  and the response carries no `submission_id`: a local run belongs to no submission family.
- A local run stages inputs the device holds, each pinned to the version it resolves to at accept
  time. An input naming `source_node_id` and `version_id` is read at that exact version, so an
  unreachable holder fails the run rather than the submission.
- A local run's outputs stay node-local until the owner publishes them. A mounted input and
  `workspace.mode` `existing` are both refused: a device stages files, exposes no S3 endpoint a
  container could reach, and names no workspace bucket.

- A submission tagged `aruna-engine.org/session` with value `notebook` starts an interactive
  session. It names a `runtime` from the session catalog instead of an image, runs inside an
  existing workspace bucket, and stays running until the caller ends it, the idle wait passes, the
  walltime is reached, or it is cancelled. `session_idle_after_ms` asks for a shorter idle wait
  than the realm's; the executing node clamps it, so a longer request never extends the session.
  Omitted resource limits default to 2 CPU cores and 4 GB RAM (4,000,000,000 bytes).
- `session_mount` picks the slice of the workspace bucket the kernel sees as a folder: `prefix`
  is a folder in the bucket (an empty string is the whole bucket) and `path` a folder below the
  working directory. Omitted, the `data/` folder appears at `<workdir>/data`. The caller needs
  write permission on that bucket folder, checked on its own path so a policy can refuse the
  folder alone (403 otherwise). The folder is served by the executing backend's S3 mount driver;
  without one the session reaches the bucket over S3 only.

**Limits** (all refused with 400)
- An empty image without a `runtime`, a `cpu_cores` of 0, or a `ram_bytes` of 0 or above 2^63-1.
- A `runtime`, `session_idle_after_ms` or `session_mount` without the session tag, an unknown
  runtime id, a session without an existing workspace bucket, or a session that also names an
  image, entrypoint or command.
- A `session_mount.prefix` that is absolute or carries an empty or `..` segment, or a
  `session_mount.path` that is not a folder below the working directory or lies under `.aruna`.
- More than 512 inputs, more than 1024 outputs, or more than 32 output prefixes.
- An empty `dest_key`, or a container path that is not absolute and traversal-free.
- An output without a `bucket` under `workspace.mode` `none`, named in the message.
- Two inputs sharing a container path, or two outputs sharing a container path or one destination.
  Two inputs sharing a `dest_key` are instead resolved by `mode` and `collision_policy`."#,
    request_body(
        content = SubmitExecutionRequest,
        description = "Container image, command and the inputs and outputs to stage around it. Without a workspace every output names the bucket it lands in; inside an existing workspace an output may leave `bucket` out and resolve against it.",
        examples(
            ("Capture outputs without a workspace" = (
                summary = "Workspace mode `none`: the run touches no bucket of its own and each output names its destination",
                value = json!({
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "image": "registry.example.test/tools/fastqc:0.12.1",
                    "command": ["fastqc", "--outdir", "/outputs", "/inputs/reads.fastq"],
                    "env": {
                        "FASTQC_THREADS": "2"
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
                            "dest_key": "reports/reads_fastqc.html",
                            "bucket": "project-results"
                        }
                    ],
                    "idempotency_key": "fastqc-reads-2026-04-09",
                    "workspace": {
                        "mode": "none"
                    }
                })
            )),
            ("Run inside an existing bucket" = (
                summary = "Workspace mode `existing`: an output without a `bucket` resolves its `dest_key` in the workspace",
                value = json!({
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "image": "registry.example.test/tools/fastqc:0.12.1",
                    "command": ["fastqc", "--outdir", "/outputs", "/inputs/reads.fastq"],
                    "tags": {
                        "aruna-engine.org/label/accelerator": "gpu"
                    },
                    "workdir": "/work",
                    "cpu_cores": 2,
                    "ram_bytes": 4294967296_i64,
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
                    "idempotency_key": "fastqc-reads-2026-04-09-ws",
                    "workspace": {
                        "mode": "existing",
                        "bucket": "project-data"
                    }
                })
            ))
        )
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
                "status_url": "https://node.example.test/api/v1/compute/jobs/01JJRSTVWXYZ0123456789ABCD"
            })
        ),
        (
            status = 200,
            description = "Replay of an idempotency key naming this exact plan; nothing new was admitted and `state` reports what this responder currently reduces",
            body = SubmitJobResponse,
            example = json!({
                "job_id": "01JJRSTVWXYZ0123456789ABCD",
                "created": false,
                "submission_id": "6b1f8c9d0e2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4",
                "canonical_job_id": "01JJRSTVWXYZ0123456789ABCD",
                "state": "running",
                "origin_node_url": "https://node.example.test/api/v1",
                "status_url": "https://node.example.test/api/v1/compute/jobs/01JJRSTVWXYZ0123456789ABCD"
            })
        ),
        (status = 400, description = "Malformed group id, empty image, out-of-range resources, an invalid or duplicated input, output or container path, an output without a `bucket` while `workspace.mode` is `none`, a workspace or output bucket that does not exist or belongs to another group, `target` `local` on a node that serves no device plane, or a local run naming an input this device cannot read", body = ErrorResponse),
        (status = 409, description = "The idempotency key is bound to a different plan, the composition conflicts on a staged key under `collision_policy` `reject`, the active RO-Crate job limit is reached, or this device's compute plane is paused, absent or already at its run ceiling, which is counted in the admitting transaction so two submissions cannot both pass it. A quota refusal carries the exact scope, dimension and numbers in `quota`; a demand view that understates the group is refused like an exceeded cap, with `observed` at the limit", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted, the caller lacks WRITE on the group, on the named existing workspace bucket or on a bucket an output writes into, or a local run was requested by someone other than this device's owner", body = ErrorResponse),
        (status = 503, description = "Retryable, and the caller may submit again with the same idempotency key: no family holder could admit the request, the demand view could not be read or kept moving under three reads, three admission transactions in a row lost to a concurrent submission of the same group, or the id clock is unhealthy. A device submission whose referenced inputs sit on no holder of its family answers the same 503 and retrying does not clear it; submit from a node that holds the inputs, or replicate them first", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<SubmitExecutionRequest>,
) -> ServerResult<(StatusCode, Json<SubmitJobResponse>)> {
    let response = admit_execution(
        state.as_ref(),
        auth,
        bearer,
        request.into(),
        PolicyRequestExtras::rest(),
    )
    .await
    .map_err(map_job_request)?;
    // The transport owns the HTTP status; the application outcome is `created`.
    let status = if response.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let urls = job_urls(state.as_ref(), response.job_id).await?;
    Ok((
        status,
        Json(SubmitJobResponse {
            job_id: response.job_id.to_string(),
            created: response.created,
            submission_id: response.submission_id,
            canonical_job_id: response.canonical_job_id.to_string(),
            state: response.state,
            origin_node_url: urls.owner_node_url,
            status_url: urls.status_url,
        }),
    ))
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
    path = "/compute/jobs/{job_id}",
    tag = "compute/jobs",
    summary = "Read one job's status",
    description = r#"Returns one job's current status, with the replicated family view for a distributed execution job.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Reads are
self-scoped: only the job's own submitter may read it, and anybody else's job answers 404, so the
surface never confirms that an id exists. The one exception is a persistent-id minting job the
caller joined, readable while the caller holds WRITE on the document it mints for.

**Behavior**
- `state` is a point-in-time value that keeps moving until it reaches `succeeded`, `failed` or
  `cancelled`.
- A distributed execution job carries a `family` block reduced from the replicated records: the
  request's `submission_id`, the currently canonical alias, the canonical execution and its exact
  output VersionIds, how many physical executions and duplicate successes are known, the projection
  `revision` and digest to detect that the view changed, and the responder that answered.
- `family.partial` means this responder could not reduce every record.
- `family.locally_exhausted` is a responder-local diagnostic outside the projection digest: every
  known execution ended without success and no retry is armed here. It is not evidence of a
  permanent failure, so poll again or ask another node.
- `family.execution_list` names every known execution with the node that ran it, and
  `family.placement` names the node that planned the launch, the node it was planned to run on, and
  the inputs that had to move. An output whose owning node's S3 endpoint is unknown here carries
  `endpoint_url: null` rather than failing the read.
- A distributed execution job is answered from the replicated family projection, without routing.
  Every other kind is answered by the node that owns the job, derived from the id itself, and
  forwarded under the caller's own bearer token when that is another node.
- Scheduling is planner-first: the node that admitted the request plans it, and every other
  holder of the family waits for its own turn. A launch without a receipt, and an executor node
  whose heartbeat stopped, are both left alone for the realm's `catch_up_after_ms` before
  another holder plans again, so a normal run has exactly one execution and a lost node is
  still caught up on.
- A target that already runs, or already ran successfully, an execution of the family declines a
  second launch, so one node never runs the same request twice.
- `run_crate` reports a side obligation of jobs that owe a run crate, not the job itself."#,
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
                    "stdout": "",
                    "stderr": "",
                    "outputs": [
                        {
                            "bucket": "project-reports",
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
                "workspace_mode": "none",
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
                    "execution_list": [
                        {
                            "execution_id": "01JJRSEXEC0123456789ABCDEF",
                            "executor_node_id": "b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c",
                            "state": "succeeded",
                            "started_at_ms": 1755500001000u64,
                            "observed_at_ms": 1755500123000u64,
                            "canonical": true
                        }
                    ],
                    "duplicate_successes": 1,
                    "outputs": [
                        {
                            "bucket": "project-reports",
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
                    "responder_node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                    "partial": false,
                    "locally_exhausted": false,
                    "cancel_requested": false,
                    "placement": {
                        "executor_kind": "docker",
                        "target_node_id": "b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c",
                        "scheduler_node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                        "estimated_transfer_bytes": 4194304,
                        "estimated_transfer_ms": 340,
                        "alternatives": 2,
                        "rejected": 1,
                        "omitted": 0,
                        "stored_at_ms": 1755500000000u64,
                        "inputs": [
                            {
                                "destination_key": "reads.fastq",
                                "bytes": 4194304,
                                "source_node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                                "transfer_ms": 340
                            }
                        ],
                        "candidates": [
                            {
                                "node_id": "b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c",
                                "executor_kind": "docker",
                                "verdict": "selected"
                            },
                            {
                                "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                                "executor_kind": "docker",
                                "verdict": "ranked",
                                "rank": 1
                            },
                            {
                                "node_id": "0a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f8",
                                "executor_kind": "apptainer",
                                "verdict": "rejected",
                                "reason": "no executor of that kind"
                            }
                        ]
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
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(job_id): Path<String>,
) -> ServerResult<(StatusCode, Json<JobStatusResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(map_job_request)?;
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

pub(crate) fn coded_response(status: StatusCode, error: &str, code: &str) -> Response {
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
                .strip_prefix(&[SYSTEM_ENTRY_PREFIX])
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
        JobKind::Execution => {
            use aruna_core::structs::execution::job::{SessionReportDetail, SessionReportRow};

            let row: SessionReportRow = postcard::from_bytes(value)
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            if row.entry_key.as_bytes() != entry_key {
                return Err(ServerError::InternalError(
                    "stored session report entry key does not match its row".to_string(),
                ));
            }
            let (code, message) = match &row.detail {
                SessionReportDetail::Input {
                    dest_key,
                    bytes,
                    version_id,
                    ..
                } => (
                    "input",
                    format!("{dest_key} ({bytes} bytes, source version {version_id})"),
                ),
                SessionReportDetail::Touched {
                    bucket,
                    key,
                    operation,
                } => (operation.as_str(), format!("{bucket}/{key}")),
                SessionReportDetail::End { reason } => ("ended", reason.clone()),
            };
            Ok(
                serde_json::json!({ "entry_key": row.entry_key, "code": code, "message": message, "detail": row.detail }),
            )
        }
        _ => return Err(ServerError::NotFound),
    };
    row.map_err(|error| ServerError::InternalError(error.to_string()))
}

#[utoipa::path(
    get,
    path = "/compute/jobs/{job_id}/report",
    tag = "compute/jobs",
    summary = "Page a finished job's report",
    description = r#"Pages the frozen per-entry report of a finished RO-Crate import, export or notebook session job.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: a job submitted by somebody else answers 404.

**Behavior**
- RO-Crate imports, exports and notebook sessions keep per-entry reports; other jobs answer 404.
- The report exists only once the job is terminal, so while it is still running the answer is a 404
  carrying a pending marker with the job's current state, and the caller should poll.
- It is then frozen and immutable, and disappears again once the job's retention window passes.
- Paging is stable against that frozen snapshot: `report_digest` names it and a cursor carries
  both the job and that digest.
- The read is answered by the node that owns the job, forwarded under the caller's own bearer
  token when this node is not the owner.

**Limits**
- `limit` defaults to 200 and is capped at 1000; `0` is treated as unset."#,
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
        (status = 409, description = "The cursor was issued for a different job or a different frozen report, so it cannot continue this one rather than silently returning a different page", body = ErrorResponse),
        (status = 503, description = "The node owning this job could not be reached or is not yet known here; retryable, the caller may repeat the read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job_report(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(job_id): Path<String>,
    Query(query): Query<ReportQuery>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(map_job_request)?;
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
        .min(usize::from(REPORT_MAX_ROWS));
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
    bearer: Option<ValidatedBearer>,
    job_id: String,
    headers: HeaderMap,
    download: bool,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(map_job_request)?;
    let auth_token = forwarded_job_auth(bearer)?;
    let now_ms = aruna_core::time::unix_timestamp_millis();
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
    path = "/compute/jobs/{job_id}/artifacts/rocrate",
    tag = "compute/jobs",
    summary = "Download a finished export job's RO-Crate",
    description = r#"Downloads the packaged RO-Crate an export job produced, as a binary `application/zip` body.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: a job submitted by somebody else answers 404, and so does a job
kind that produces no crate.

**Behavior**
- A successful answer always carries `Content-Type: application/zip`, `Content-Length`,
  `Accept-Ranges: bytes`, an `ETag` that is the artifact's quoted hex BLAKE3 digest, and a
  `Content-Disposition: attachment` naming the crate file with both an ASCII fallback and a UTF-8
  form; a partial answer adds `Content-Range`.
- While the export job has not finished the crate is a 404 coded `artifact_pending` with the job's
  current state, so the caller should poll the status instead of retrying blindly.
- Downloads are admission-limited, so a saturated node refuses rather than queueing.

**Limits**
- `Range` accepts one byte range only."#,
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
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    artifact_response(state, auth, bearer, job_id, headers, true).await
}

#[utoipa::path(
    head,
    path = "/compute/jobs/{job_id}/artifacts/rocrate",
    tag = "compute/jobs",
    summary = "Probe a finished export job's RO-Crate headers",
    description = r#"Answers exactly what the download would answer, with the headers but no body.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: a job submitted by somebody else answers 404.

**Behavior**
- Lets a client learn a crate's size, digest and filename before fetching it.
- The headers describe an `application/zip` crate: `Content-Type`, `Content-Length` for the whole
  crate or for the requested range, `Accept-Ranges: bytes`, an `ETag` that is the artifact's
  quoted hex BLAKE3 digest, an attachment `Content-Disposition`, and `Content-Range` when a range
  was asked for.
- Readiness behaves as it does for the download: a crate whose export has not finished is a 404
  coded `artifact_pending` with the job's current state, and one past its retention window is a
  410.
- Probing does not consume download capacity.

**Limits**
- `Range` accepts one byte range only."#,
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
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    artifact_response(state, auth, bearer, job_id, headers, false).await
}

#[utoipa::path(
    delete,
    path = "/compute/jobs/{job_id}",
    tag = "compute/jobs",
    summary = "Delete the caller's finished run",
    description = r#"Removes a finished run from the run and job lists of this node.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: only the submitter may delete, and anybody else's run answers
404.

**Behavior**
- A run is listed only by the node that admitted it, so this call is sent to the node whose list
  showed the run. Every client listing through that node stops seeing it on its next read.
- The node removes the run's row, its list entry, its report rows and its local attempt records in
  one transaction, the same rows retention pruning removes.
- The run's replicated history stays on the nodes that hold it, so a read by id still answers with
  its final state. Captured outputs and the run dataset are not deleted.
- Repeating the call answers 404 once the run is gone.

**Limits**
- Only an execution that succeeded, failed or was cancelled can be deleted; a run still in flight,
  one with an undecided outcome, or one whose container cleanup is still queued answers 409.
- System jobs such as imports, exports and copies are not deleted here."#,
    params(("job_id" = String, Path, description = "Job id as returned at submission, for example `01JJRSTVWXYZ0123456789ABCD`")),
    responses(
        (status = 204, description = "The run was removed from this node's lists"),
        (status = 400, description = "The job id is malformed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "This node lists no such run of the caller; absence and foreign ownership are deliberately indistinguishable", body = ErrorResponse),
        (status = 409, description = "The run has not finished, or its container cleanup is still queued; cancel it or retry once it settled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(map_job_request)?;
    let outcome = delete_owned_run(&state.get_ctx(), auth.user_id, job_id)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    match outcome {
        RunDelete::Deleted => Ok(StatusCode::NO_CONTENT),
        RunDelete::NotFound => Err(ServerError::NotFound),
        RunDelete::Unfinished => Err(ServerError::Conflict(
            "the run has not finished or its cleanup is still queued".to_string(),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/compute/jobs/{job_id}/cancel",
    tag = "compute/jobs",
    summary = "Request cancellation of the caller's job",
    description = r#"Records a cancellation request on the caller's job; it does not stop the work synchronously.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused.
Self-scoped like the status read: only the submitter may cancel, and anybody else's job answers
404.

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
- Cancellation of any other job stays anchored to the node that owns it."#,
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
                "workspace_mode": "none"
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
                "workspace_mode": "none"
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
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(job_id): Path<String>,
) -> ServerResult<(StatusCode, Json<JobStatusResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let job_id = crate::jobs::parse_job_id(&job_id).map_err(map_job_request)?;

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
#[path = "jobs_tests.rs"]
mod tests;
