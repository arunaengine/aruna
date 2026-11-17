//! Admits compute jobs and looks up sessions for both the REST and MCP transports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::sync::Arc;

use aruna_compute::session::Session;
use aruna_core::compute::normalize_container_path;
use aruna_core::compute::runtimes::MOUNT_PREFIX_TAG;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{
    CollisionPolicy, CompositionError, ComputeResources, ExecutionSpec, InputMode, InputSelection,
    InputSource, JobId, JobRecord, JobState, MAX_EXECUTION_OUTPUTS, OutputDestination,
    OutputSelection, WorkspaceMode, WorkspaceOutput,
};
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, group_permission_path};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::device::compute::{
    LocalExecutionConfig, LocalExecutionError, submit_local_execution,
};
use aruna_operations::jobs::JobRouteError;
use aruna_operations::jobs::command::{
    AcceptedExecution, CollisionPolicy as CommandCollisionPolicy, ExecutionInput, ExecutionOutput,
    ExecutionTarget, InputMode as CommandInputMode, SubmitExecutionCommand,
    WorkspaceMode as CommandWorkspaceMode, WorkspaceSpec,
};
use aruna_operations::jobs::lifecycle::routing::session_job;
use aruna_operations::jobs::lifecycle::submit_external_job;
use aruna_operations::jobs::service::read_owned_job;
use aruna_operations::jobs::submit::SubmitJobError;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use ulid::Ulid;

use crate::auth::{ValidatedBearer, require_owner, require_unrestricted_auth};
use crate::error::ServerError;
use crate::error::ServerResult;
use crate::server::state::ServerState;
use aruna_core::structs::execution::job::KeyWait;
use aruna_operations::driver::drive;
use aruna_operations::jobs::lifecycle::FamilyReport;
use aruna_operations::jobs::service::JobStatusView;
use serde::Deserialize;
use serde::Serialize;
use utoipa::ToSchema;

/// An output path count bound shared with the transport documentation.
pub(crate) const MAX_OUTPUT_PREFIXES: usize = 32;

/// The transport-independent outcome of a refused admission or session read.
/// Each transport maps it to its own status or tool result.
#[derive(Debug)]
pub(crate) enum JobRequestError {
    /// Malformed caller input with no reason beyond "bad request".
    BadRequest,
    /// Malformed caller input whose reason the caller can act on.
    BadRequestMessage(String),
    Unauthorized,
    Forbidden,
    NotFound,
    /// A refused plan, limit or composition.
    Conflict(String),
    /// The idempotency key is bound to a different plan.
    JobPlanConflict(String),
    /// Standing compute quota refused the admission.
    ComputeQuotaDenied(aruna_core::compute::quota::QuotaDenied),
    /// A retryable dependency did not answer.
    ServiceUnavailableReason(String),
    /// A fault that is not the caller's to fix.
    InternalError(String),
}

/// The REST status and body of a transport-independent job refusal.
impl From<JobRequestError> for ServerError {
    fn from(error: JobRequestError) -> Self {
        match error {
            JobRequestError::BadRequest => Self::BadRequest,
            JobRequestError::BadRequestMessage(message) => Self::BadRequestMessage(message),
            JobRequestError::Unauthorized => Self::Unauthorized,
            JobRequestError::Forbidden => Self::Forbidden,
            JobRequestError::NotFound => Self::NotFound,
            JobRequestError::Conflict(message) => Self::Conflict(message),
            JobRequestError::JobPlanConflict(message) => Self::JobPlanConflict(message),
            JobRequestError::ComputeQuotaDenied(denied) => Self::ComputeQuotaDenied(denied),
            JobRequestError::ServiceUnavailableReason(message) => {
                Self::ServiceUnavailableReason(message)
            }
            JobRequestError::InternalError(message) => Self::InternalError(message.into()),
        }
    }
}

impl JobRequestError {
    /// Folds a refusal raised by a shared authorization or forwarding helper;
    /// the transports never see the helper's own error type.
    fn from_server(error: ServerError) -> Self {
        match error {
            ServerError::BadRequest => Self::BadRequest,
            ServerError::BadRequestReason(message) | ServerError::BadRequestMessage(message) => {
                Self::BadRequestMessage(message)
            }
            ServerError::Unauthorized => Self::Unauthorized,
            ServerError::Forbidden => Self::Forbidden,
            ServerError::NotFound => Self::NotFound,
            ServerError::ServiceUnavailable => {
                Self::ServiceUnavailableReason("Service unavailable".to_string())
            }
            ServerError::ServiceUnavailableReason(message) => {
                Self::ServiceUnavailableReason(message)
            }
            other => Self::InternalError(other.to_string()),
        }
    }

    /// Categorizes an admission refusal from the job store.
    fn from_submit(error: SubmitJobError) -> Self {
        match error {
            SubmitJobError::JobPlanConflict { existing_job_id } => Self::JobPlanConflict(format!(
                "idempotency key already bound to job {existing_job_id}"
            )),
            SubmitJobError::ActiveJobLimit { limit } => {
                Self::Conflict(format!("active job limit of {limit} reached"))
            }
            SubmitJobError::InvalidWorkspace(reason) => Self::BadRequestMessage(reason),
            SubmitJobError::TooManyOutputs { limit } => {
                Self::BadRequestMessage(format!("a job may declare at most {limit} outputs"))
            }
            SubmitJobError::Composition(CompositionError::KeyConflict(key)) => {
                Self::Conflict(format!("composition key conflict on {key}"))
            }
            SubmitJobError::Composition(other) => Self::BadRequestMessage(other.to_string()),
            SubmitJobError::ClockHealth(_) => {
                Self::ServiceUnavailableReason("structured_id_clock_unhealthy".to_string())
            }
            SubmitJobError::PlacementUnavailable(_) => {
                Self::ServiceUnavailableReason("job_placement_unavailable".to_string())
            }
            SubmitJobError::QuotaDenied(denied) => Self::ComputeQuotaDenied(denied),
            SubmitJobError::AuthorityDenied | SubmitJobError::InputDenied(_) => Self::Forbidden,
            other => Self::InternalError(other.to_string()),
        }
    }

    /// Categorizes a refusal of a local device execution.
    fn from_local(error: LocalExecutionError) -> Self {
        match error {
            LocalExecutionError::NotADevice => Self::BadRequestMessage(
                "target `local` is served by a user device only".to_string(),
            ),
            LocalExecutionError::NotOwner => Self::Forbidden,
            LocalExecutionError::Paused | LocalExecutionError::NoExecutor => {
                Self::Conflict(error.to_string())
            }
            LocalExecutionError::Unsupported(_)
            | LocalExecutionError::InputNotLocal { .. }
            | LocalExecutionError::InputRefused { .. } => {
                Self::BadRequestMessage(error.to_string())
            }
            LocalExecutionError::Unavailable(_) => {
                Self::ServiceUnavailableReason(error.to_string())
            }
            LocalExecutionError::Submit(error) => Self::from_submit(error),
        }
    }

    /// Categorizes a routed session read that did not answer.
    fn from_route(error: JobRouteError) -> Self {
        match error {
            JobRouteError::Unauthorized => Self::Unauthorized,
            JobRouteError::Forbidden => Self::Forbidden,
            JobRouteError::NotFound => Self::NotFound,
            JobRouteError::Unavailable(_) => {
                Self::ServiceUnavailableReason("job_read_unavailable".to_string())
            }
            JobRouteError::Internal(message) => Self::InternalError(message),
        }
    }
}

/// The transport-independent admission decision shared by REST and MCP. The
/// outcome carries the application answer; each transport maps it to its own
/// status/response contract.
pub(crate) async fn admit_execution(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer: Option<ValidatedBearer>,
    mut command: SubmitExecutionCommand,
    extras: PolicyRequestExtras,
) -> Result<AcceptedExecution, JobRequestError> {
    command
        .resolve_session(
            bearer
                .as_ref()
                .map(|bearer| bearer.expires_at_secs().saturating_mul(1_000)),
        )
        .map_err(|error| JobRequestError::BadRequestMessage(error.to_string()))?;
    let target = command.target.unwrap_or_default();
    let auth = match target {
        ExecutionTarget::Realm => {
            require_unrestricted_auth(state, auth).map_err(JobRequestError::from_server)?
        }
        ExecutionTarget::Local => local_auth(state, auth).await?,
    };
    let group_id = Ulid::from_string(&command.group_id).map_err(|_| JobRequestError::BadRequest)?;
    let (workspace_mode, workspace_bucket) = workspace_request(command.workspace.take())?;
    if command.image.trim().is_empty() {
        return Err(JobRequestError::BadRequest);
    }
    // RAM above i64::MAX would wrap negative in the Docker HostConfig cast.
    if command.cpu_cores == Some(0)
        || command
            .ram_bytes
            .is_some_and(|bytes| bytes == 0 || i64::try_from(bytes).is_err())
    {
        return Err(JobRequestError::BadRequest);
    }
    let output_prefixes = validate_output_prefixes(std::mem::take(&mut command.output_prefixes))?;
    crate::auth::ensure_permission_with(
        state,
        &auth,
        group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
        Permission::WRITE,
        extras.clone(),
    )
    .await
    .map_err(JobRequestError::from_server)?;

    if command.outputs.len() > MAX_EXECUTION_OUTPUTS {
        return Err(JobRequestError::BadRequest);
    }
    // Destination-key overlaps are the composition's collision policy to resolve.
    let mut inputs: Vec<InputSelection> = Vec::with_capacity(command.inputs.len());
    for input in std::mem::take(&mut command.inputs) {
        let input = native_input(input)?;
        if inputs
            .iter()
            .any(|existing| existing.container_path == input.container_path)
        {
            return Err(JobRequestError::BadRequest);
        }
        inputs.push(input);
    }
    let (file_outputs, workspace_outputs) =
        native_outputs(std::mem::take(&mut command.outputs), workspace_mode)?;
    for bucket in output_buckets(workspace_bucket.as_deref(), &file_outputs) {
        validate_owned_bucket(state, &auth, group_id, &bucket, extras.clone()).await?;
    }
    // The mounted folder is written freely from the kernel, so the caller needs
    // WRITE on that folder itself, not only on the bucket.
    if let (Some(prefix), Some(bucket)) = (
        command.tags.get(MOUNT_PREFIX_TAG),
        workspace_bucket.as_deref(),
    ) {
        crate::auth::ensure_permission_with(
            state,
            &auth,
            mount_permission_path(
                bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
                prefix,
            ),
            Permission::WRITE,
            extras.clone(),
        )
        .await
        .map_err(JobRequestError::from_server)?;
    }

    let spec = ExecutionSpec {
        group_id,
        name: trimmed(command.name),
        description: trimmed(command.description),
        tags: std::mem::take(&mut command.tags),
        image: std::mem::take(&mut command.image),
        entrypoint: command.entrypoint,
        command: std::mem::take(&mut command.command),
        workdir: command.workdir,
        env: std::mem::take(&mut command.env),
        resources: ComputeResources {
            cpu_cores: command.cpu_cores,
            ram_bytes: command.ram_bytes,
            disk_bytes: None,
            max_walltime_ms: command.max_walltime_ms,
            preemptible: false,
        },
        executor_constraint: command.executor_constraint,
        inputs,
        file_outputs,
        workspace_outputs,
        output_prefixes,
        collision_policy: collision_policy(command.collision_policy),
    };
    let accepted = match target {
        ExecutionTarget::Local => {
            local_submit(state, &auth, spec, command.idempotency_key, workspace_mode).await?
        }
        ExecutionTarget::Realm => {
            let result = submit_external_job(
                &state.get_ctx(),
                spec,
                auth.user_id,
                command.idempotency_key,
                workspace_mode,
                workspace_bucket,
                state.rocrate_limits().artifact_retention_ms,
                crate::metadata::forwarded_auth_token(bearer)
                    .map_err(JobRequestError::from_server)?,
            )
            .await
            .map_err(JobRequestError::from_submit)?;
            AcceptedExecution {
                job_id: result.job_id,
                created: result.created,
                submission_id: Some(hex32(&result.submission_id.0)),
                canonical_job_id: result.job_id,
                state: result.state.name().to_string(),
            }
        }
    };

    Ok(accepted)
}

/// The owner of this device, for a run that must stay on this machine. A node
/// that serves no device plane refuses the target itself, not the caller.
async fn local_auth(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> Result<AuthContext, JobRequestError> {
    if !matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(JobRequestError::BadRequestMessage(
            "target `local` is served by a user device only".to_string(),
        ));
    }
    require_owner(state, auth)
        .await
        .map_err(JobRequestError::from_server)
}

async fn local_submit(
    state: &ServerState,
    auth: &AuthContext,
    spec: ExecutionSpec,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
) -> Result<AcceptedExecution, JobRequestError> {
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
    .map_err(JobRequestError::from_local)?;
    // A replay answers with the state the device already reduced for that job.
    let state = read_owned_job(&context, auth.user_id, result.job_id)
        .await
        .ok()
        .flatten()
        .map_or(JobState::Queued, |record| record.state);
    Ok(AcceptedExecution {
        job_id: result.job_id,
        created: result.created,
        submission_id: None,
        canonical_job_id: result.job_id,
        state: state.name().to_string(),
    })
}

/// The caller's session job on this node. Absence and foreign ownership are
/// deliberately indistinguishable.
pub(crate) async fn owned_session_job(
    state: &ServerState,
    auth: &AuthContext,
    raw_job_id: &str,
) -> Result<(JobRecord, Option<JobId>), JobRequestError> {
    session_job(&state.get_ctx(), auth.user_id, parse_job_id(raw_job_id)?)
        .await
        .map_err(JobRequestError::from_route)
}

/// The caller's live session on this node, for callers that have no coded
/// answer of their own. Absence reads as 404 like every other session route.
pub(crate) async fn caller_session(
    state: &ServerState,
    auth: &AuthContext,
    raw_job_id: &str,
) -> Result<Arc<Session>, JobRequestError> {
    let (record, physical_job_id) = owned_session_job(state, auth, raw_job_id).await?;
    if record.owner_node_id != state.get_node_id() {
        return Err(JobRequestError::NotFound);
    }
    let job_id = physical_job_id.ok_or(JobRequestError::NotFound)?;
    state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&job_id.to_string()))
        .ok_or(JobRequestError::NotFound)
}

/// A blank label carries no more than an absent one, so it is stored as absent.
fn trimmed(value: Option<String>) -> Option<String> {
    value
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

/// The permission path of the mounted bucket folder: the bucket path itself for
/// the whole bucket, else the folder below it, shaped like an object path.
pub(crate) fn mount_permission_path(bucket_path: String, prefix: &str) -> String {
    match prefix.trim_end_matches('/') {
        "" => bucket_path,
        folder => format!("{bucket_path}/{folder}"),
    }
}

/// An omitted workspace block runs without a bucket of the run's own.
pub(crate) fn workspace_request(
    workspace: Option<WorkspaceSpec>,
) -> Result<(WorkspaceMode, Option<String>), JobRequestError> {
    let Some(workspace) = workspace else {
        return Ok((WorkspaceMode::None, None));
    };
    match (workspace.mode, workspace.bucket) {
        (CommandWorkspaceMode::None, None) => Ok((WorkspaceMode::None, None)),
        (CommandWorkspaceMode::Existing, Some(bucket)) if !bucket.trim().is_empty() => {
            Ok((WorkspaceMode::Existing, Some(bucket)))
        }
        _ => Err(JobRequestError::BadRequest),
    }
}

/// A bucket the run writes into: it must exist, belong to the execution group,
/// and grant the caller WRITE. Both the workspace and every explicit output
/// destination pass this gate.
async fn validate_owned_bucket(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    bucket: &str,
    extras: PolicyRequestExtras,
) -> Result<(), JobRequestError> {
    let info = match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => info,
        Err(GetBucketError::NotFound) => return Err(JobRequestError::BadRequest),
        Err(error) => return Err(JobRequestError::InternalError(error.to_string())),
    };
    if info.group_id != group_id {
        return Err(JobRequestError::BadRequest);
    }
    crate::auth::ensure_permission_with(
        state,
        auth,
        bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
        Permission::WRITE,
        extras,
    )
    .await
    .map_err(JobRequestError::from_server)
}

/// Canonical absolute container path or 400.
fn container_path(path: &str) -> Result<String, JobRequestError> {
    let normalized = normalize_container_path(path).map_err(|_| JobRequestError::BadRequest)?;
    normalized
        .to_str()
        .map(str::to_string)
        .ok_or(JobRequestError::BadRequest)
}

/// Native inputs land in the container at the given path, defaulting to
/// `/inputs/<dest_key>` so `load_inputs` always stages them.
pub(crate) fn native_input(input: ExecutionInput) -> Result<InputSelection, JobRequestError> {
    if input.dest_key.is_empty() {
        return Err(JobRequestError::BadRequest);
    }
    // A realm submission resolves a named node's inputs at that node, which checks them.
    let source_node_id = input
        .source_node_id
        .as_deref()
        .map(NodeId::from_str)
        .transpose()
        .map_err(|_| JobRequestError::BadRequest)?;
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
            CommandInputMode::Snapshot => InputMode::Snapshot,
            CommandInputMode::FloatingReference => InputMode::FloatingReference,
            CommandInputMode::ExactReference => InputMode::ExactReference,
        },
        container_path: Some(path),
        name: None,
        description: None,
    })
}

fn collision_policy(policy: CommandCollisionPolicy) -> CollisionPolicy {
    match policy {
        CommandCollisionPolicy::Reject => CollisionPolicy::Reject,
        CommandCollisionPolicy::Replace => CollisionPolicy::Replace,
        CommandCollisionPolicy::KeepExisting => CollisionPolicy::KeepExisting,
    }
}

/// A declared output either names its own destination bucket or resolves its
/// key against the workspace bucket the run works inside.
enum MappedOutput {
    Explicit(OutputSelection),
    Workspace(WorkspaceOutput),
}

fn native_output(
    output: ExecutionOutput,
    mode: WorkspaceMode,
) -> Result<MappedOutput, JobRequestError> {
    if output.dest_key.is_empty() {
        return Err(JobRequestError::BadRequest);
    }
    let container_path = container_path(&output.container_path)?;
    let bucket = output
        .bucket
        .map(|bucket| bucket.trim().to_string())
        .filter(|bucket| !bucket.is_empty());
    match (bucket, mode) {
        (Some(bucket), _) => Ok(MappedOutput::Explicit(OutputSelection {
            container_path,
            path_prefix: None,
            destination_node_id: None,
            destination: OutputDestination::S3 {
                bucket,
                key: output.dest_key,
            },
            name: None,
            description: None,
        })),
        (None, WorkspaceMode::Existing) => Ok(MappedOutput::Workspace(WorkspaceOutput {
            container_path,
            dest_key: output.dest_key,
        })),
        (None, WorkspaceMode::None) => Err(JobRequestError::BadRequestMessage(format!(
            "output `{container_path}` needs a bucket when `workspace.mode` is `none`"
        ))),
    }
}

/// Splits the declared outputs into bucket-qualified destinations and workspace
/// intents. Container paths are unique across both, destinations within each.
pub(crate) fn native_outputs(
    outputs: Vec<ExecutionOutput>,
    mode: WorkspaceMode,
) -> Result<(Vec<OutputSelection>, Vec<WorkspaceOutput>), JobRequestError> {
    let mut explicit: Vec<OutputSelection> = Vec::new();
    let mut workspace: Vec<WorkspaceOutput> = Vec::new();
    let mut paths: Vec<String> = Vec::with_capacity(outputs.len());
    for output in outputs {
        let mapped = native_output(output, mode)?;
        let path = match &mapped {
            MappedOutput::Explicit(output) => &output.container_path,
            MappedOutput::Workspace(output) => &output.container_path,
        };
        if paths.iter().any(|existing| existing == path) {
            return Err(JobRequestError::BadRequest);
        }
        paths.push(path.clone());
        match mapped {
            MappedOutput::Explicit(output) => {
                if explicit
                    .iter()
                    .any(|existing| existing.destination == output.destination)
                {
                    return Err(JobRequestError::BadRequest);
                }
                explicit.push(output);
            }
            MappedOutput::Workspace(output) => {
                if workspace
                    .iter()
                    .any(|existing| existing.dest_key == output.dest_key)
                {
                    return Err(JobRequestError::BadRequest);
                }
                workspace.push(output);
            }
        }
    }
    Ok((explicit, workspace))
}

/// Every bucket the run writes into, the workspace bucket first.
pub(crate) fn output_buckets(workspace: Option<&str>, outputs: &[OutputSelection]) -> Vec<String> {
    let mut buckets: Vec<String> = workspace.map(str::to_string).into_iter().collect();
    for output in outputs {
        let OutputDestination::S3 { bucket, .. } = &output.destination;
        if !buckets.contains(bucket) {
            buckets.push(bucket.clone());
        }
    }
    buckets
}

pub(crate) fn validate_output_prefixes(
    prefixes: Vec<String>,
) -> Result<Vec<String>, JobRequestError> {
    if prefixes.len() > MAX_OUTPUT_PREFIXES || prefixes.iter().any(String::is_empty) {
        return Err(JobRequestError::BadRequest);
    }
    let mut deduplicated = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if !deduplicated.contains(&prefix) {
            deduplicated.push(prefix);
        }
    }
    Ok(deduplicated)
}

/// A caller-supplied job id; a malformed id is absence to a reader.
pub(crate) fn parse_job_id(raw: &str) -> Result<JobId, JobRequestError> {
    JobId::from_str(raw).map_err(|_| JobRequestError::NotFound)
}

pub(crate) fn hex32(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
        ServerError::InternalError(
            "REST interface public URL is unavailable"
                .to_string()
                .into(),
        )
    })?;
    let api_base_url = rest.api_base_url.trim_end_matches('/');
    Ok(JobUrls {
        owner_node_url: rest.api_base_url.clone(),
        status_url: format!("{api_base_url}/compute/jobs/{job_id}"),
        report_url: format!("{api_base_url}/compute/jobs/{job_id}/report"),
        artifact_url: format!("{api_base_url}/compute/jobs/{job_id}/artifacts/rocrate"),
    })
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
    /// Present while `awaiting_key`: the locked bucket keys the job waits for, as
    /// `{node_id, bucket, group_id?}` objects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub awaiting_keys: Vec<serde_json::Value>,
    /// Api url of the node that owns an owner-routed job, as its submission named it.
    /// Absent for a distributed execution job and when this node does not know the url.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobListResponse {
    pub jobs: Vec<JobStatusResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

pub(crate) fn rfc3339(ms: u64) -> String {
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
        awaiting_keys: job
            .awaiting_keys
            .iter()
            .map(KeyWait::to_public_json)
            .collect(),
        owner_node_url: None,
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
