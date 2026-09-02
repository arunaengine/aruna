use super::data::{WriteObjectInput, write_text};
use super::{
    JsonPayload, McpServer, authorize_tool, bad_request, empty_extras, explained, internal_error,
    parse_ulid, request_auth, server_error, tool_extras,
};
use aruna_core::compute::runtimes::{QUICK_RUNTIMES, QuickRuntime, quick_runtime};
use aruna_core::structs::{
    JobPayload, OBJECT_CONTENT_TYPE_KEY, Permission, blob_group_permission_path, key_content_type,
};
use aruna_operations::driver::drive;
use aruna_operations::jobs::lifecycle::family_report;
use aruna_operations::jobs::service::{
    RoutedCancelOutcome, cancel_job_routed, list_owned_jobs, read_job_routed,
};
use aruna_operations::s3::head_object::{HeadObjectInput, HeadObjectOperation};
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

const NETWORK_TAG: &str = "aruna-engine.org/network";
const WORKDIR: &str = "/work";

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct RuntimeOutput {
    pub id: String,
    pub label: String,
    pub hint: String,
    pub image: String,
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub file: String,
    pub lang: String,
    pub content_type: String,
    pub template: String,
}

impl From<&QuickRuntime> for RuntimeOutput {
    fn from(runtime: &QuickRuntime) -> Self {
        Self {
            id: runtime.id.to_string(),
            label: runtime.label.to_string(),
            hint: runtime.hint.to_string(),
            image: runtime.image.to_string(),
            command: runtime
                .command
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            env: runtime
                .env
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            file: runtime.file.to_string(),
            lang: runtime.lang.to_string(),
            content_type: runtime.content_type.to_string(),
            template: runtime.template.to_string(),
        }
    }
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct RuntimesOutput {
    pub runtimes: Vec<RuntimeOutput>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct RunScriptInput {
    /// Owning group's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. Call `list_groups` for the ids the caller
    /// may use; the caller needs write permission on the group.
    pub group_id: String,
    /// Name of an existing bucket in that group, for example `project-data`.
    /// It is the run's workspace: the script is staged under
    /// `.aruna/scripts/<run id>/` and outputs are written back into it. Call
    /// `list_buckets` for readable names.
    pub bucket: String,
    /// Runtime id from `list_runtimes`: `python-uv`, `deno`, or `bash`.
    pub runtime: String,
    /// Full source text of the script, for example
    /// `print("hello from aruna")`. It is stored as the runtime's file name and
    /// executed from the container's `/work` directory.
    pub script: String,
    /// Packages to resolve before the run: PyPI requirements for `python-uv`
    /// such as `httpx>=0.27`, npm specifications for `deno` such as `chalk@5`.
    /// `bash` refuses any. A non-empty list opens container network access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<String>>,
    /// Objects staged into the container next to the script. The script itself
    /// is added automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<Vec<crate::routes::jobs::ExecutionInputRequest>>,
    /// Container paths written back after the run, into `bucket` unless an
    /// entry names one of its own. Omit when the script only prints to stdout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Vec<crate::routes::jobs::ExecutionOutputRequest>>,
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
    /// `realm` (the default) admits the run into the realm; `local` runs it on
    /// this machine and is served by a user device node only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<crate::routes::jobs::ExecutionTarget>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SubmitJobInput {
    /// The complete native execution request. `group_id` and `image` are
    /// required; every other field has a default. Prefer `run_script` for a
    /// plain Python, Deno, or Bash script.
    pub spec: crate::routes::jobs::SubmitExecutionRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct GetJobInput {
    /// The job's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. Read it from the `job_id` returned by
    /// `run_script` or `submit_job`, or from a `list_jobs` entry.
    pub id: String,
}

/// One object a job wrote, with everything a caller needs to render or fetch it.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct JobArtifactOutput {
    pub job_id: String,
    /// Physical execution that produced the object.
    pub execution_id: String,
    pub bucket: String,
    pub key: String,
    /// The exact version this execution created; the key's latest version may be
    /// a later, unrelated write, so always fetch this version.
    pub version_id: String,
    pub filename: String,
    pub container_path: String,
    /// The stored type, or the type the key's extension implies. Never null.
    pub content_type: String,
    pub size: u64,
    pub digest: Option<String>,
    /// Node that owns this version. Null when the answer does not name it.
    pub node_id: Option<String>,
    /// Node-local S3 endpoint owning this exact version. Use it with bucket,
    /// key, and version_id to retrieve the bytes.
    pub endpoint_url: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct JobOutputsOutput {
    pub job_id: String,
    pub state: String,
    pub workspace_bucket: Option<String>,
    pub outputs: Vec<JobArtifactOutput>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ListJobsInput {
    /// Optional filter: keep only jobs submitted for this group's bare
    /// 26-character ULID. Call `list_groups` for the ids the caller may use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// Optional filter on the job state. One of `queued`, `claimed`,
    /// `preparing`, `ready`, `running`, `cancelling`, `indeterminate`,
    /// `succeeded`, `failed`, or `cancelled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Maximum number of jobs to return, newest first. Defaults to 50 and is
    /// capped at 200. Only the first page is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

struct ScriptPlan {
    script_key: String,
    script_text: String,
    script_type: String,
    dependency: Option<(String, String)>,
    request: crate::routes::jobs::SubmitExecutionRequest,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::compute_router()
}

#[tool_router(router = compute_router)]
impl McpServer {
    #[tool(
        description = "List the pinned quick-run runtimes that run_script accepts. Each entry carries the runtime id, its container image, the script file name it writes, the content type, and a starter template. Call this before run_script to choose a runtime id and to see which runtimes resolve dependencies. Takes no arguments.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_runtimes(
        &self,
        Extension(parts): Extension<http::request::Parts>,
    ) -> Result<Json<RuntimesOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        compute_probe(self, &auth, Permission::READ, empty_extras("list_runtimes")).await?;
        Ok(Json(RuntimesOutput {
            runtimes: QUICK_RUNTIMES.iter().map(Into::into).collect(),
        }))
    }

    #[tool(
        description = "Stage one script into an existing bucket and submit it as a container job, returning job_id and the state at accept. Use it for plain Python, Deno, or Bash work, and submit_job when a specific image or entrypoint is needed, or when the run should touch no bucket at all (workspace mode `none`). The script is written under `.aruna/scripts/<run id>/` in `bucket`, which is the run's workspace (mode `existing`), so the caller needs write permission on that bucket and on the group. Poll get_job with the returned job_id for state, result, and log tails. A file the script writes is kept only when it is declared in outputs as {container_path, dest_key}: to produce a chart, choose runtime `python-uv` with dependencies [\"matplotlib\"], write the figure to /work/chart.png, and pass outputs [{\"container_path\": \"/work/chart.png\", \"dest_key\": \"results/<run>/chart.png\"}]. Once the job succeeds, call list_job_outputs for the captured objects with their content type and version_id.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn run_script(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<RunScriptInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("run_script", &input)?;
        let run_id = Ulid::generate().to_string();
        let plan = build_script(input, &run_id)?;
        write_text(
            self,
            &auth,
            WriteObjectInput {
                bucket: plan
                    .request
                    .workspace
                    .as_ref()
                    .and_then(|workspace| workspace.bucket.clone())
                    .ok_or_else(|| internal_error("script workspace bucket is missing"))?,
                key: plan.script_key,
                text: plan.script_text,
                content_type: Some(plan.script_type),
            },
            extras.clone(),
        )
        .await?;
        if let Some((key, text)) = plan.dependency {
            let bucket = plan
                .request
                .workspace
                .as_ref()
                .and_then(|workspace| workspace.bucket.clone())
                .ok_or_else(|| internal_error("script workspace bucket is missing"))?;
            write_text(
                self,
                &auth,
                WriteObjectInput {
                    bucket,
                    key,
                    text,
                    content_type: Some("application/json".to_string()),
                },
                extras.clone(),
            )
            .await?;
        }
        let (_, response) = crate::routes::jobs::submit_execution(
            &self.state,
            Some(auth),
            request_bearer(&parts),
            plan.request,
            extras,
        )
        .await
        .map_err(submit_error)?;
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Submit the complete native execution request: image, command, environment, resources, staged inputs, captured outputs, and workspace mode, which is `none` (the default: the run touches no bucket of its own and every input is read from where it lives) or `existing` (the run works inside a bucket the caller already owns; a run never creates a bucket). Under `none` every entry in outputs must name the bucket it lands in, as {container_path, dest_key, bucket}; under `existing` an output that leaves bucket out resolves its dest_key in the workspace bucket, which is also what output_prefixes resolve against. Every bucket an output names must belong to the group and grant the caller write permission. Use run_script instead for a plain Python, Deno, or Bash script. Returns job_id, whether this call created the job, and the family state at accept. Poll get_job with job_id; a replay under the same idempotency_key returns the existing job rather than a second one.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn submit_job(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<SubmitJobInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("submit_job", &input)?;
        let (_, response) = crate::routes::jobs::submit_execution(
            &self.state,
            Some(auth),
            request_bearer(&parts),
            input.spec,
            extras,
        )
        .await
        .map_err(submit_error)?;
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Read one owned job by id: state, attempts, timestamps, progress, result, workspace bucket, and the bounded stdout and stderr tails the result carries. Use list_jobs to find an id and this tool to follow one run. The states queued, claimed, preparing, ready, running, and cancelling are still in flight, while succeeded, failed, and cancelled are terminal and indeterminate means the outcome is not yet proven. Poll it rather than assuming a submission finished.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_job(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<GetJobInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        compute_probe(
            self,
            &auth,
            Permission::READ,
            tool_extras("get_job", &input)?,
        )
        .await?;
        let job_id = parse_job(&input.id)?;
        let response =
            if let Some(report) = family_report(&self.state.get_ctx(), &auth, job_id).await {
                let report = report
                    .map_err(crate::routes::jobs::map_job_route)
                    .map_err(job_error)?;
                let mut response = crate::routes::jobs::job_view_response(&report.job);
                let family = crate::routes::jobs::family_response(&report);
                crate::routes::jobs::bind_output_routes(&mut response.result, &family.outputs)
                    .map_err(crate::routes::jobs::map_job_route)
                    .map_err(server_error)?;
                response.family = Some(family);
                response
            } else {
                let routed = read_job_routed(
                    &self.state.get_ctx(),
                    &auth,
                    job_id,
                    crate::routes::jobs::forwarded_job_auth(request_bearer(&parts))
                        .map_err(server_error)?,
                )
                .await
                .map_err(crate::routes::jobs::map_job_route)
                .map_err(job_error)?;
                let mut response = crate::routes::jobs::job_view_response(&routed.job);
                response.run_crate = routed.run_crate;
                response
            };
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "List the objects one owned job wrote, each with content type, filename, size, bucket, key, the exact version_id, the owning node's S3 endpoint, and the execution that produced it. Call it once get_job reports succeeded. The entries are metadata only: fetch the bytes through the S3 surface with bucket, key, and version_id, which is how an image such as a captured chart is displayed. A job that captured nothing answers with an empty list rather than an error.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_job_outputs(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<GetJobInput>,
    ) -> Result<Json<JobOutputsOutput>, CallToolResult> {
        let auth = request_auth(&parts)?;
        compute_probe(
            self,
            &auth,
            Permission::READ,
            tool_extras("list_job_outputs", &input)?,
        )
        .await?;
        let job_id = parse_job(&input.id)?;
        let (view, outputs) =
            if let Some(report) = family_report(&self.state.get_ctx(), &auth, job_id).await {
                let report = report
                    .map_err(crate::routes::jobs::map_job_route)
                    .map_err(job_error)?;
                let outputs = report
                    .outputs
                    .iter()
                    .map(|output| {
                        (
                            crate::routes::jobs::output_response(
                                output,
                                report.output_endpoints.get(&output.node_id),
                            ),
                            Some(output.node_id),
                        )
                    })
                    .collect::<Vec<_>>();
                (crate::routes::jobs::job_view_response(&report.job), outputs)
            } else {
                let routed = read_job_routed(
                    &self.state.get_ctx(),
                    &auth,
                    job_id,
                    crate::routes::jobs::forwarded_job_auth(request_bearer(&parts))
                        .map_err(server_error)?,
                )
                .await
                .map_err(crate::routes::jobs::map_job_route)
                .map_err(job_error)?;
                let view = crate::routes::jobs::job_view_response(&routed.job);
                let outputs = result_outputs(view.result.as_ref())?
                    .into_iter()
                    .map(|output| (output, None))
                    .collect::<Vec<_>>();
                (view, outputs)
            };
        let mut artifacts = Vec::with_capacity(outputs.len());
        for (output, node_id) in outputs {
            artifacts.push(artifact_output(self, &view.job_id, output, node_id).await);
        }
        Ok(Json(JobOutputsOutput {
            job_id: view.job_id,
            state: view.state,
            workspace_bucket: view.workspace_bucket,
            outputs: artifacts,
        }))
    }

    #[tool(
        description = "List the caller's own jobs on this node, newest first, with optional group, state, and limit filters. Each entry carries job_id, kind, state, attempts, timestamps, progress, and any error, but never the run crate or the family detail; call get_job for those. Only the first page is returned, at most 200 entries. Use it to recover a job_id that was not kept from the submission.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_jobs(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<ListJobsInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("list_jobs", &input)?;
        let group_id = input
            .group_id
            .as_deref()
            .map(|group_id| {
                parse_ulid(
                    "group_id",
                    group_id,
                    "call list_groups for the ids the caller may use, or omit it to list every owned job",
                )
            })
            .transpose()?;
        match group_id {
            Some(group_id) => {
                authorize_tool(
                    &self.state,
                    &auth,
                    blob_group_permission_path(
                        self.state.get_realm_id(),
                        group_id,
                        self.state.get_node_id(),
                    ),
                    Permission::READ,
                    extras,
                )
                .await
                .map_err(server_error)?;
            }
            None => compute_probe(self, &auth, Permission::READ, extras).await?,
        }
        let state_filter = input.state.as_deref().map(parse_job_state).transpose()?;
        let limit = input
            .limit
            .filter(|limit| *limit > 0)
            .unwrap_or(50)
            .min(200);
        let (records, next_cursor) = list_owned_jobs(
            &self.state.get_ctx(),
            auth.user_id,
            None,
            limit,
            move |record| {
                let matches_state = state_filter.is_none_or(|state| record.state == state);
                let matches_group = group_id.is_none_or(|group_id| {
                    matches!(&record.payload, JobPayload::Execution(spec) if spec.group_id == group_id)
                });
                matches_state && matches_group
            },
        )
        .await
        .map_err(internal_error)?;
        let response = crate::routes::jobs::JobListResponse {
            jobs: records
                .iter()
                .map(crate::routes::jobs::job_status_response)
                .collect(),
            next_cursor: crate::routes::jobs::encode_cursor(next_cursor),
        };
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }

    #[tool(
        description = "Request cancellation of one owned job and return its current view. Cancellation is a request, not an immediate stop: an in-flight job moves to cancelling and reaches cancelled once the executor confirms. A job already in a terminal state comes back unchanged. Poll get_job for the settled state.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true
        )
    )]
    pub async fn cancel_job(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<GetJobInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        compute_probe(
            self,
            &auth,
            Permission::WRITE,
            tool_extras("cancel_job", &input)?,
        )
        .await?;
        let job_id = parse_job(&input.id)?;
        let outcome = cancel_job_routed(
            &self.state.get_ctx(),
            &self.state.jobs_runtime(),
            auth.user_id,
            job_id,
            crate::routes::jobs::forwarded_job_auth(request_bearer(&parts))
                .map_err(server_error)?,
        )
        .await
        .map_err(crate::routes::jobs::map_job_route)
        .map_err(job_error)?;
        let job = match outcome {
            RoutedCancelOutcome::NotFound => {
                return Err(job_error(crate::error::ServerError::NotFound));
            }
            RoutedCancelOutcome::AlreadyTerminal(job) | RoutedCancelOutcome::Requested(job) => job,
        };
        let response = crate::routes::jobs::job_view_response(&job);
        Ok(Json(JsonPayload(
            serde_json::to_value(response).map_err(internal_error)?,
        )))
    }
}

/// Outputs of a job with no replicated family live in its own result payload.
fn result_outputs(
    result: Option<&serde_json::Value>,
) -> Result<Vec<crate::routes::jobs::JobOutputResponse>, CallToolResult> {
    let Some(outputs) = result.and_then(|result| result.get("outputs")) else {
        return Ok(Vec::new());
    };
    serde_json::from_value(outputs.clone()).map_err(internal_error)
}

/// Adds the facts a caller needs to render an object: the stored content type,
/// the filename, and when the version was written. A version owned by another
/// node keeps the type its key implies, since only its owner can be asked.
async fn artifact_output(
    server: &McpServer,
    job_id: &str,
    output: crate::routes::jobs::JobOutputResponse,
    node_id: Option<aruna_core::NodeId>,
) -> JobArtifactOutput {
    let local = node_id.is_none_or(|node_id| node_id == server.state.get_node_id());
    let head = match (local, Ulid::from_string(&output.version_id)) {
        (true, Ok(version_id)) => drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: output.bucket.clone(),
                key: output.key.clone(),
                version_id: Some(version_id),
            }),
            &server.state.get_ctx(),
        )
        .await
        .and_then(|result| result.transpose())
        .ok()
        .flatten(),
        _ => None,
    };
    let content_type = head
        .as_ref()
        .and_then(|head| head.metadata.get(OBJECT_CONTENT_TYPE_KEY).cloned())
        .or_else(|| {
            head.as_ref()
                .and_then(|head| head.source_metadata.as_ref())
                .and_then(|metadata| metadata.content_type.clone())
        })
        .unwrap_or_else(|| {
            if output.content_type.is_empty() {
                key_content_type(&output.key).to_string()
            } else {
                output.content_type.clone()
            }
        });
    let last_modified = head
        .as_ref()
        .and_then(|head| head.version_created_at)
        .map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339());
    JobArtifactOutput {
        job_id: job_id.to_string(),
        execution_id: output.execution_id,
        filename: super::data::filename_of(&output.key),
        bucket: output.bucket,
        key: output.key,
        version_id: output.version_id,
        container_path: output.container_path,
        content_type,
        size: output.size,
        digest: output.digest,
        node_id: node_id.map(|node_id| node_id.to_string()),
        endpoint_url: output.endpoint_url,
        last_modified,
    }
}

fn request_bearer(
    parts: &http::request::Parts,
) -> Option<crate::auth::ValidatedArunaBearerTokenCarrier> {
    parts
        .extensions
        .get::<Option<crate::auth::ValidatedArunaBearerTokenCarrier>>()
        .cloned()
        .flatten()
}

/// The REST parser answers a malformed job id with "Not found", which reads to
/// a caller as a missing job rather than a wrong argument.
fn parse_job(id: &str) -> Result<aruna_core::structs::JobId, CallToolResult> {
    crate::routes::jobs::parse_job_id(id).map_err(|_| {
        bad_request(
            "id must be a 26-character job ULID such as 01JZ8Y6T0K4W7M2N9Q5R3S8V1X; read job_id \
             from run_script, submit_job, or a list_jobs entry",
        )
    })
}

fn parse_job_state(value: &str) -> Result<aruna_core::structs::JobState, CallToolResult> {
    crate::routes::jobs::parse_state(value).map_err(|_| {
        bad_request(
            "state must be one of queued, claimed, preparing, ready, running, cancelling, \
             indeterminate, succeeded, failed, or cancelled",
        )
    })
}

fn job_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::NotFound => explained(
            error,
            "no job with that id belongs to the caller on this node; call list_jobs for visible ids",
        ),
        crate::error::ServerError::Forbidden => {
            explained(error, "the caller does not own that job")
        }
        // Only a provably invalid id is absence here; an owner this node cannot
        // reach is unavailable, which a caller may retry.
        error @ crate::error::ServerError::ServiceUnavailableReason(_) => explained(
            error,
            "the node that owns that job did not answer; retry, and call list_jobs to confirm the \
             id",
        ),
        error => server_error(error),
    }
}

/// The submit route refuses several argument shapes with a bare "Bad request".
fn submit_error(error: crate::error::ServerError) -> CallToolResult {
    match error {
        crate::error::ServerError::BadRequest => explained(
            error,
            "check group_id as a 26-character ULID, a non-blank image, cpu_cores above 0, \
             ram_bytes between 1 and 9223372036854775807, unique input container_path and output \
             dest_key values, an existing same-group bucket for workspace mode `existing`, and no \
             outputs or output_prefixes under workspace mode `none`",
        ),
        crate::error::ServerError::Forbidden => explained(
            error,
            "the caller needs write permission on the group and on the workspace bucket",
        ),
        error => server_error(error),
    }
}

async fn compute_probe(
    server: &McpServer,
    auth: &aruna_core::structs::AuthContext,
    permission: Permission,
    extras: aruna_operations::request_policy::PolicyRequestExtras,
) -> Result<(), CallToolResult> {
    super::authorize_self(&server.state, auth, permission, extras)
        .await
        .map_err(server_error)
}

fn build_script(input: RunScriptInput, run_id: &str) -> Result<ScriptPlan, CallToolResult> {
    parse_ulid(
        "group_id",
        &input.group_id,
        "call list_groups for the ids the caller may use",
    )?;
    let runtime = quick_runtime(&input.runtime).ok_or_else(|| {
        let ids = QUICK_RUNTIMES
            .iter()
            .map(|runtime| runtime.id)
            .collect::<Vec<_>>()
            .join(", ");
        bad_request(format!(
            "runtime must be one of {ids}; call list_runtimes for the full entries"
        ))
    })?;
    let dependencies = input
        .dependencies
        .unwrap_or_default()
        .into_iter()
        .map(|dependency| dependency.trim().to_string())
        .filter(|dependency| !dependency.is_empty())
        .collect::<Vec<_>>();
    if runtime.id == "bash" && !dependencies.is_empty() {
        return Err(bad_request(
            "the bash runtime resolves no dependencies; omit dependencies or choose the python-uv \
             or deno runtime",
        ));
    }
    let prefix = format!(".aruna/scripts/{run_id}");
    let script_key = format!("{prefix}/{}", runtime.file);
    let script_path = format!("{WORKDIR}/{}", runtime.file);
    let mut script_text = input.script;
    if runtime.id == "python-uv" && !dependencies.is_empty() {
        let lines = dependencies
            .iter()
            .map(|dependency| {
                serde_json::to_string(dependency)
                    .map(|dependency| format!("#   {dependency},"))
                    .map_err(internal_error)
            })
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        script_text = format!(
            "# /// script\n# requires-python = \">=3.13\"\n# dependencies = [\n{lines}\n# ]\n# ///\n{script_text}"
        );
    }
    let dependency = if runtime.id == "deno" && !dependencies.is_empty() {
        let imports = dependencies
            .iter()
            .map(|dependency| {
                (
                    npm_package_name(dependency).to_string(),
                    format!("npm:{dependency}"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let text = serde_json::to_string_pretty(&serde_json::json!({ "imports": imports }))
            .map_err(internal_error)?;
        Some((format!("{prefix}/deno.json"), text))
    } else {
        None
    };
    let mut command = runtime
        .command
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    if dependency.is_some() {
        command.push(format!("--config={WORKDIR}/deno.json"));
    }
    command.push(script_path.clone());
    let mut inputs = vec![crate::routes::jobs::ExecutionInputRequest {
        bucket: input.bucket.clone(),
        key: script_key.clone(),
        version_id: None,
        source_node_id: None,
        dest_key: runtime.file.to_string(),
        container_path: Some(script_path),
        mode: crate::routes::jobs::InputModeRequest::Snapshot,
    }];
    if dependency.is_some() {
        inputs.push(crate::routes::jobs::ExecutionInputRequest {
            bucket: input.bucket.clone(),
            key: format!("{prefix}/deno.json"),
            version_id: None,
            source_node_id: None,
            dest_key: "deno.json".to_string(),
            container_path: Some(format!("{WORKDIR}/deno.json")),
            mode: crate::routes::jobs::InputModeRequest::Snapshot,
        });
    }
    inputs.extend(input.inputs.unwrap_or_default());
    let env = runtime
        .env
        .iter()
        .map(|(key, value)| ((*key).to_string(), format!("{WORKDIR}/{value}")))
        .collect();
    let tags = if dependencies.is_empty() {
        BTreeMap::new()
    } else {
        BTreeMap::from([(NETWORK_TAG.to_string(), "open".to_string())])
    };
    Ok(ScriptPlan {
        script_key,
        script_text,
        script_type: runtime.content_type.to_string(),
        dependency,
        request: crate::routes::jobs::SubmitExecutionRequest {
            group_id: input.group_id,
            image: runtime.image.to_string(),
            entrypoint: None,
            command,
            env,
            tags,
            workdir: Some(WORKDIR.to_string()),
            cpu_cores: input.cpu_cores,
            ram_bytes: input.ram_bytes,
            max_walltime_ms: input.max_walltime_ms,
            executor_constraint: None,
            inputs,
            outputs: input.outputs.unwrap_or_default(),
            output_prefixes: Vec::new(),
            collision_policy: crate::routes::jobs::CollisionPolicyRequest::Reject,
            idempotency_key: Some(run_id.to_string()),
            workspace: Some(crate::routes::jobs::WorkspaceRequest {
                mode: crate::routes::jobs::WorkspaceModeRequest::Existing,
                bucket: Some(input.bucket),
            }),
            target: Some(input.target.unwrap_or_default()),
        },
    })
}

fn npm_package_name(spec: &str) -> &str {
    if !spec.starts_with('@') {
        return spec.split('@').next().unwrap_or(spec);
    }
    let Some(slash) = spec.find('/') else {
        return spec;
    };
    spec[slash + 1..]
        .find('@')
        .map(|version| &spec[..slash + 1 + version])
        .unwrap_or(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_script_request() {
        let plan = build_script(
            RunScriptInput {
                group_id: "01J00000000000000000000000".to_string(),
                bucket: "scripts".to_string(),
                runtime: "deno".to_string(),
                script: "console.log('ok');\n".to_string(),
                dependencies: Some(vec!["chalk@5".to_string()]),
                inputs: None,
                outputs: None,
                cpu_cores: Some(2),
                ram_bytes: Some(1_000_000_000),
                max_walltime_ms: Some(60_000),
                target: None,
            },
            "01JTEST0000000000000000000",
        )
        .unwrap();

        assert_eq!(
            plan.script_key,
            ".aruna/scripts/01JTEST0000000000000000000/script.ts"
        );
        assert_eq!(
            plan.request.command,
            [
                "deno",
                "run",
                "-A",
                "--config=/work/deno.json",
                "/work/script.ts"
            ]
        );
        assert_eq!(
            plan.request.tags.get(NETWORK_TAG).map(String::as_str),
            Some("open")
        );
    }

    fn error_body(result: CallToolResult) -> serde_json::Value {
        assert_eq!(result.is_error, Some(true));
        result.structured_content.expect("structured error body")
    }

    fn build_err(input: RunScriptInput) -> CallToolResult {
        match build_script(input, "01JTEST0000000000000000000") {
            Ok(_) => panic!("expected build_script to refuse the input"),
            Err(result) => result,
        }
    }

    fn script_input(runtime: &str, deps: Option<Vec<String>>) -> RunScriptInput {
        RunScriptInput {
            group_id: "01J00000000000000000000000".to_string(),
            bucket: "scripts".to_string(),
            runtime: runtime.to_string(),
            script: "print('hi')\n".to_string(),
            dependencies: deps,
            inputs: None,
            outputs: None,
            cpu_cores: None,
            ram_bytes: None,
            max_walltime_ms: None,
            target: None,
        }
    }

    #[test]
    fn python_inline_deps() {
        let plan = build_script(
            script_input("python-uv", Some(vec!["httpx>=0.27".to_string()])),
            "01JTEST0000000000000000000",
        )
        .unwrap();
        assert!(plan.script_text.starts_with("# /// script"));
        assert!(plan.script_text.contains("requires-python"));
        assert!(plan.dependency.is_none());
        assert_eq!(
            plan.request.tags.get(NETWORK_TAG).map(String::as_str),
            Some("open")
        );
    }

    #[test]
    fn bash_refuses_dependencies() {
        let text = error_body(build_err(script_input(
            "bash",
            Some(vec!["jq".to_string()]),
        )));
        assert!(
            text["error"]
                .as_str()
                .unwrap_or_default()
                .contains("resolves no dependencies")
        );
    }

    #[test]
    fn unknown_runtime_refused() {
        let text = error_body(build_err(script_input("ruby", None)));
        assert!(
            text["error"]
                .as_str()
                .unwrap_or_default()
                .contains("list_runtimes")
        );
    }

    #[test]
    fn rejects_bad_group() {
        let mut input = script_input("bash", None);
        input.group_id = "not-a-ulid".to_string();
        let text = error_body(build_err(input));
        assert_eq!(text["code"], "Bad request");
    }

    #[test]
    fn npm_strips_versions() {
        assert_eq!(npm_package_name("chalk@5"), "chalk");
        assert_eq!(npm_package_name("plain"), "plain");
        assert_eq!(npm_package_name("@scope/pkg@1.2.3"), "@scope/pkg");
        assert_eq!(npm_package_name("@scope/pkg"), "@scope/pkg");
    }

    #[test]
    fn parse_job_reasons() {
        let text = error_body(parse_job("bad").unwrap_err());
        assert!(
            text["error"]
                .as_str()
                .unwrap_or_default()
                .contains("job ULID")
        );
        let id = aruna_core::structs::JobId::from_bytes([7u8; 16]).to_string();
        assert!(parse_job(&id).is_ok());
    }

    #[test]
    fn job_state_names() {
        assert!(parse_job_state("running").is_ok());
        assert!(parse_job_state("bogus").is_err());
    }

    #[test]
    fn job_submit_errors() {
        assert!(
            error_body(job_error(crate::error::ServerError::NotFound))["error"]
                .as_str()
                .unwrap_or_default()
                .contains("list_jobs")
        );
        assert!(
            error_body(submit_error(crate::error::ServerError::BadRequest))["error"]
                .as_str()
                .unwrap_or_default()
                .contains("group_id")
        );
        assert!(
            error_body(submit_error(crate::error::ServerError::Forbidden))["error"]
                .as_str()
                .unwrap_or_default()
                .contains("write permission")
        );
    }

    #[test]
    fn runtime_output_ids() {
        let ids = QUICK_RUNTIMES
            .iter()
            .map(RuntimeOutput::from)
            .map(|runtime| runtime.id)
            .collect::<Vec<_>>();
        assert!(ids.iter().any(|id| id == "bash"));
        assert!(ids.iter().any(|id| id == "deno"));
        assert!(ids.iter().any(|id| id == "python-uv"));
    }
}
