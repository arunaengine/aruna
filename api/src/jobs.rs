//! Transport-independent job admission and session lookup. REST and MCP both
//! build a command and call here; each transport maps the outcome to its own
//! status or tool result.

use std::str::FromStr;
use std::sync::Arc;

use aruna_compute::session::Session;
use aruna_core::compute::normalize_container_path;
use aruna_core::compute::runtimes::SESSION_MOUNT_PREFIX_TAG;
use aruna_core::id::NodeId;
use aruna_core::scheduling::MAX_PLAN_INPUTS;
use aruna_core::structs::{
    AuthContext, CollisionPolicy, ComputeResources, ExecutionSpec, InputMode, InputSelection,
    InputSource, JobId, JobRecord, JobState, MAX_EXECUTION_OUTPUTS, NodeCapabilities,
    OutputDestination, OutputSelection, Permission, WorkspaceMode, WorkspaceOutput,
    bucket_permission_path, group_permission_path,
};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::device::compute::{LocalExecutionConfig, submit_local_execution};
use aruna_operations::jobs::command::{
    AcceptedExecution, CollisionPolicy as CommandCollisionPolicy, ExecutionInput, ExecutionOutput,
    ExecutionTarget, InputMode as CommandInputMode, SubmitExecutionCommand,
    WorkspaceMode as CommandWorkspaceMode, WorkspaceSpec,
};
use aruna_operations::jobs::lifecycle::routing::session_job;
use aruna_operations::jobs::lifecycle::submit_external_job;
use aruna_operations::jobs::service::read_owned_job;
use aruna_operations::s3::get_bucket::{GetBucketInfoError, GetBucketInfoOperation};
use ulid::Ulid;

use crate::auth::{ValidatedArunaBearerTokenCarrier, require_unrestricted_auth};
use crate::error::{ServerError, ServerResult};
use crate::routes::device::require_owner;
use crate::routes::jobs::{
    forwarded_job_auth, hex32, map_job_route, map_local_error, map_submit_error, parse_job_id,
};
use crate::server_state::ServerState;
use aruna_operations::driver::drive;

/// An output path count bound shared with the transport documentation.
pub(crate) const MAX_OUTPUT_PREFIXES: usize = 32;

/// The transport-independent admission decision shared by REST and MCP. The
/// outcome carries the application answer; each transport maps it to its own
/// status/response contract.
pub(crate) async fn admit_execution(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer: Option<ValidatedArunaBearerTokenCarrier>,
    mut command: SubmitExecutionCommand,
    extras: PolicyRequestExtras,
) -> ServerResult<AcceptedExecution> {
    command
        .resolve_session(
            bearer
                .as_ref()
                .map(|bearer| bearer.expires_at_secs().saturating_mul(1_000)),
        )
        .map_err(|error| ServerError::BadRequestMessage(error.to_string()))?;
    let target = command.target.unwrap_or_default();
    let auth = match target {
        ExecutionTarget::Realm => require_unrestricted_auth(state, auth)?,
        ExecutionTarget::Local => local_auth(state, auth).await?,
    };
    let group_id = Ulid::from_string(&command.group_id).map_err(|_| ServerError::BadRequest)?;
    let (workspace_mode, workspace_bucket) = workspace_request(command.workspace.take())?;
    if command.image.trim().is_empty() {
        return Err(ServerError::BadRequest);
    }
    // RAM above i64::MAX would wrap negative in the Docker HostConfig cast.
    if command.cpu_cores == Some(0)
        || command
            .ram_bytes
            .is_some_and(|bytes| bytes == 0 || i64::try_from(bytes).is_err())
    {
        return Err(ServerError::BadRequest);
    }
    let output_prefixes = validate_output_prefixes(std::mem::take(&mut command.output_prefixes))?;
    crate::auth::ensure_permission_with(
        state,
        &auth,
        group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
        Permission::WRITE,
        extras.clone(),
    )
    .await?;

    if command.inputs.len() > MAX_PLAN_INPUTS || command.outputs.len() > MAX_EXECUTION_OUTPUTS {
        return Err(ServerError::BadRequest);
    }
    // Destination-key overlaps are the composition's collision policy to resolve.
    let mut inputs: Vec<InputSelection> = Vec::with_capacity(command.inputs.len());
    for input in std::mem::take(&mut command.inputs) {
        let input = native_input(input, target)?;
        if inputs
            .iter()
            .any(|existing| existing.container_path == input.container_path)
        {
            return Err(ServerError::BadRequest);
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
        command.tags.get(SESSION_MOUNT_PREFIX_TAG),
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
        .await?;
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
                forwarded_job_auth(bearer)?,
            )
            .await
            .map_err(map_submit_error)?;
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
) -> ServerResult<AcceptedExecution> {
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
) -> ServerResult<(JobRecord, Option<JobId>)> {
    session_job(&state.get_ctx(), auth.user_id, parse_job_id(raw_job_id)?)
        .await
        .map_err(map_job_route)
}

/// The caller's live session on this node, for callers that have no coded
/// answer of their own. Absence reads as 404 like every other session route.
pub(crate) async fn caller_session(
    state: &ServerState,
    auth: &AuthContext,
    raw_job_id: &str,
) -> ServerResult<Arc<Session>> {
    let (record, physical_job_id) = owned_session_job(state, auth, raw_job_id).await?;
    if record.owner_node_id != state.get_node_id() {
        return Err(ServerError::NotFound);
    }
    let job_id = physical_job_id.ok_or(ServerError::NotFound)?;
    state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&job_id.to_string()))
        .ok_or(ServerError::NotFound)
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
) -> ServerResult<(WorkspaceMode, Option<String>)> {
    let Some(workspace) = workspace else {
        return Ok((WorkspaceMode::None, None));
    };
    match (workspace.mode, workspace.bucket) {
        (CommandWorkspaceMode::None, None) => Ok((WorkspaceMode::None, None)),
        (CommandWorkspaceMode::Existing, Some(bucket)) if !bucket.trim().is_empty() => {
            Ok((WorkspaceMode::Existing, Some(bucket)))
        }
        _ => Err(ServerError::BadRequest),
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
) -> ServerResult<()> {
    let info = match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => info,
        Err(GetBucketInfoError::NotFound) => return Err(ServerError::BadRequest),
        Err(error) => return Err(ServerError::InternalError(error.to_string())),
    };
    if info.group_id != group_id {
        return Err(ServerError::BadRequest);
    }
    crate::auth::ensure_permission_with(
        state,
        auth,
        bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
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
pub(crate) fn native_input(
    input: ExecutionInput,
    target: ExecutionTarget,
) -> ServerResult<InputSelection> {
    if input.dest_key.is_empty() {
        return Err(ServerError::BadRequest);
    }
    // A realm submission resolves its inputs through the planner, which stores
    // the holder itself; naming one there would claim an unverified value.
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

fn native_output(output: ExecutionOutput, mode: WorkspaceMode) -> ServerResult<MappedOutput> {
    if output.dest_key.is_empty() {
        return Err(ServerError::BadRequest);
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
        (None, WorkspaceMode::None) => Err(ServerError::BadRequestMessage(format!(
            "output `{container_path}` needs a bucket when `workspace.mode` is `none`"
        ))),
    }
}

/// Splits the declared outputs into bucket-qualified destinations and workspace
/// intents. Container paths are unique across both, destinations within each.
pub(crate) fn native_outputs(
    outputs: Vec<ExecutionOutput>,
    mode: WorkspaceMode,
) -> ServerResult<(Vec<OutputSelection>, Vec<WorkspaceOutput>)> {
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
            return Err(ServerError::BadRequest);
        }
        paths.push(path.clone());
        match mapped {
            MappedOutput::Explicit(output) => {
                if explicit
                    .iter()
                    .any(|existing| existing.destination == output.destination)
                {
                    return Err(ServerError::BadRequest);
                }
                explicit.push(output);
            }
            MappedOutput::Workspace(output) => {
                if workspace
                    .iter()
                    .any(|existing| existing.dest_key == output.dest_key)
                {
                    return Err(ServerError::BadRequest);
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

pub(crate) fn validate_output_prefixes(prefixes: Vec<String>) -> ServerResult<Vec<String>> {
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
