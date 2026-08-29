use super::data::{WriteObjectInput, write_text};
use super::{
    McpServer, authorize_tool, bad_request, empty_extras, internal_error, request_auth,
    server_error, tool_extras,
};
use aruna_core::compute::runtimes::{QUICK_RUNTIMES, QuickRuntime, quick_runtime};
use aruna_core::structs::{JobPayload, Permission, blob_group_permission_path};
use aruna_operations::jobs::lifecycle::family_report;
use aruna_operations::jobs::service::{
    RoutedCancelOutcome, cancel_job_routed, list_owned_jobs, read_job_routed,
};
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    pub group_id: String,
    pub bucket: String,
    pub runtime: String,
    pub script: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<Vec<crate::routes::jobs::ExecutionInputRequest>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<Vec<crate::routes::jobs::ExecutionOutputRequest>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_walltime_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<crate::routes::jobs::ExecutionTarget>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SubmitJobInput {
    pub spec: crate::routes::jobs::SubmitExecutionRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct GetJobInput {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ListJobsInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
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
        description = "List the pinned Aruna quick-run runtimes",
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
        description = "Stage and submit a Python, Deno, or Bash script with bounded resources",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn run_script(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<RunScriptInput>,
    ) -> Result<Json<Value>, CallToolResult> {
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
        .map_err(server_error)?;
        Ok(Json(
            serde_json::to_value(response).map_err(internal_error)?,
        ))
    }

    #[tool(
        description = "Submit the full native Aruna execution request shape",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn submit_job(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        rmcp::handler::server::wrapper::Parameters(input): rmcp::handler::server::wrapper::Parameters<SubmitJobInput>,
    ) -> Result<Json<Value>, CallToolResult> {
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
        .map_err(server_error)?;
        Ok(Json(
            serde_json::to_value(response).map_err(internal_error)?,
        ))
    }

    #[tool(
        description = "Get an owned job's state, timestamps, result, and bounded log tails",
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
    ) -> Result<Json<Value>, CallToolResult> {
        let auth = request_auth(&parts)?;
        compute_probe(
            self,
            &auth,
            Permission::READ,
            tool_extras("get_job", &input)?,
        )
        .await?;
        let job_id = crate::routes::jobs::parse_job_id(&input.id).map_err(server_error)?;
        let response =
            if let Some(report) = family_report(&self.state.get_ctx(), &auth, job_id).await {
                let report = report
                    .map_err(crate::routes::jobs::map_job_route)
                    .map_err(server_error)?;
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
                .map_err(server_error)?;
                let mut response = crate::routes::jobs::job_view_response(&routed.job);
                response.run_crate = routed.run_crate;
                response
            };
        Ok(Json(
            serde_json::to_value(response).map_err(internal_error)?,
        ))
    }

    #[tool(
        description = "List the authenticated user's jobs with optional group, state, and limit filters",
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
    ) -> Result<Json<Value>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let extras = tool_extras("list_jobs", &input)?;
        let group_id = input
            .group_id
            .as_deref()
            .map(|group_id| {
                Ulid::from_string(group_id).map_err(|_| bad_request("invalid group_id"))
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
        let state_filter = input
            .state
            .as_deref()
            .map(crate::routes::jobs::parse_state)
            .transpose()
            .map_err(server_error)?;
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
        Ok(Json(
            serde_json::to_value(response).map_err(internal_error)?,
        ))
    }

    #[tool(
        description = "Request cancellation of an owned job",
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
    ) -> Result<Json<Value>, CallToolResult> {
        let auth = request_auth(&parts)?;
        compute_probe(
            self,
            &auth,
            Permission::WRITE,
            tool_extras("cancel_job", &input)?,
        )
        .await?;
        let job_id = crate::routes::jobs::parse_job_id(&input.id).map_err(server_error)?;
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
        .map_err(server_error)?;
        let job = match outcome {
            RoutedCancelOutcome::NotFound => {
                return Err(server_error(crate::error::ServerError::NotFound));
            }
            RoutedCancelOutcome::AlreadyTerminal(job) | RoutedCancelOutcome::Requested(job) => job,
        };
        let response = crate::routes::jobs::job_view_response(&job);
        Ok(Json(
            serde_json::to_value(response).map_err(internal_error)?,
        ))
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
    let runtime = quick_runtime(&input.runtime)
        .ok_or_else(|| bad_request("runtime must be python-uv, deno, or bash"))?;
    let dependencies = input
        .dependencies
        .unwrap_or_default()
        .into_iter()
        .map(|dependency| dependency.trim().to_string())
        .filter(|dependency| !dependency.is_empty())
        .collect::<Vec<_>>();
    if runtime.id == "bash" && !dependencies.is_empty() {
        return Err(bad_request("bash does not support dependency declarations"));
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
    let tags = (!dependencies.is_empty())
        .then(|| BTreeMap::from([(NETWORK_TAG.to_string(), "open".to_string())]))
        .unwrap_or_default();
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
}
