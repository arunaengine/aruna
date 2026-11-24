//! MCP tools that list, inspect, validate, plan, start, follow and cancel workflow runs through
//! the same handlers as the REST routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{JsonPayload, McpServer, bad_request, internal_error, request_auth, tool_extras};
use crate::auth::RealmAuth;
use crate::error::ServerResult;
use crate::routes::execution::workflow::run_wire::TaskLogQuery;
use crate::routes::execution::workflow::wire::WorkflowQuery;
use crate::routes::execution::workflow::{plan, registry, reports, runs, tasks, versions};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use axum::extract::{Path, Query, State};
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

type ToolResult = Result<Json<JsonPayload>, CallToolResult>;

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ListWorkflowsInput {
    /// Optional filter: only this group's workflows, as its bare 26-character ULID. Call
    /// `list_groups` for the ids the caller may use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// Optional words of the workflow's name or description, in any case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    /// The `next_cursor` of the page before; omit it for the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct VersionInput {
    /// The workflow's bare 26-character ULID, from `list_workflows`.
    pub workflow_id: String,
    /// The version number as text, like `"1"`.
    pub version: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct BodyInput {
    /// The JSON body the matching REST route takes; the tool description names its fields.
    pub body: JsonPayload,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct RunInput {
    /// The run's bare 26-character ULID, from `start_workflow_run` or a list of runs.
    pub run_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct TaskInput {
    /// The run's bare 26-character ULID.
    pub run_id: String,
    /// The task's number in the run, as text, from `get_workflow_run`.
    pub task_id: String,
    /// For `read_task_logs`: `stdout` (the default) or `stderr`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// For `read_task_logs`: the byte offset to read from, 0 when left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::workflow_router()
}

#[tool_router(router = workflow_router)]
impl McpServer {
    #[tool(
        description = "List the workflows the caller may run: every workflow of the groups the caller reads and every public one, newest first, with id, name, language, group, and newest version. Optional group_id and q narrow the list; pass the answer's next_cursor as cursor for the next page.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn list_workflows(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<ListWorkflowsInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "list_workflows", &input, Permission::READ)
            .await?;
        let query = WorkflowQuery {
            group_id: input.group_id,
            q: input.q,
            cursor: input.cursor,
            ..WorkflowQuery::default()
        };
        payload(registry::list_workflows(self.handler_state(), RealmAuth(auth), Query(query)).await)
    }

    #[tool(
        description = "Inspect one workflow version before running it: its sources and form, the check of what uwe can run with its diagnostics, and its step graph. Call list_workflows for the workflow_id and version.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn inspect_workflow(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<VersionInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "inspect_workflow", &input, Permission::READ)
            .await?;
        let path = || Path((input.workflow_id.clone(), input.version.clone()));
        let realm = || RealmAuth(auth.clone());
        let version = versions::get_version(self.handler_state(), realm(), path()).await;
        let check = versions::get_check(self.handler_state(), realm(), path()).await;
        let graph = versions::get_graph(self.handler_state(), realm(), path()).await;
        let (version, check, graph) = (value(version)?, value(check)?, value(graph)?);
        Ok(Json(JsonPayload(
            json!({"version": version, "check": check, "graph": graph}),
        )))
    }

    #[tool(
        description = "Check a workflow source without storing it. The body is that of POST /compute/workflows/validate: language (for example cwl, nextflow, snakemake or uwe), uwe_yaml or files with entrypoint, and optionally group_id for catalog tools. Answers the check with its diagnostics and, when it checks, the step graph.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn validate_workflow(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<BodyInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "validate_workflow", &input, Permission::READ)
            .await?;
        let body = body_of(input.body)?;
        payload(
            registry::validate_workflow(self.handler_state(), RealmAuth(auth), axum::Json(body))
                .await,
        )
    }

    #[tool(
        description = "Preview where each step of a run would run and what it would stage, without starting anything. The body is that of start_workflow_run. Answers whether the plan is complete, the planned steps with node, mode and bytes, the value diagnostics, and the nodes that could not run a task.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn plan_workflow_run(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<BodyInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "plan_workflow_run", &input, Permission::READ)
            .await?;
        let body = body_of(input.body)?;
        payload(plan::plan_run(self.handler_state(), RealmAuth(auth), axum::Json(body)).await)
    }

    #[tool(
        description = "Start a run of one workflow version on this node. The body is that of POST /compute/workflow-runs: group_id, workflow_id, version as text, values (an object of the workflow's form), optionally inputs (pinned objects by input name), output (bucket and prefix, required), placement and provenance. Answers the queued run; follow it with get_workflow_run.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    pub async fn start_workflow_run(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<BodyInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "start_workflow_run", &input, Permission::WRITE)
            .await?;
        let body = body_of(input.body)?;
        let headers = axum::http::HeaderMap::new();
        let started = runs::start_run(
            self.handler_state(),
            RealmAuth(auth),
            headers,
            axum::Json(body),
        );
        payload(started.await.map(|(_, body)| body))
    }

    #[tool(
        description = "Read one workflow run: its state (queued, planning, running, succeeded, failed, cancelling, cancelled or unknown), times, steps with their task counts, values, inputs, delivered outputs and crate status. Poll it to follow a run; a step with at most 25 tasks lists them for explain_workflow_task and read_task_logs.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn get_workflow_run(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<RunInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "get_workflow_run", &input, Permission::READ)
            .await?;
        payload(runs::get_run(self.handler_state(), RealmAuth(auth), Path(input.run_id)).await)
    }

    #[tool(
        description = "Cancel a workflow run that has not ended: it turns cancelling, and cancelled once its tasks stopped. Answers the run as it stands; poll get_workflow_run for the settled state.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true
        )
    )]
    pub async fn cancel_workflow_run(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<RunInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "cancel_workflow_run", &input, Permission::WRITE)
            .await?;
        let cancelled =
            runs::cancel_run(self.handler_state(), RealmAuth(auth), Path(input.run_id)).await;
        payload(cancelled.map(|(_, body)| body))
    }

    #[tool(
        description = "Explain one task of a workflow run: why it ran or was reused from the cache, what its cache key covers, and what differs from the result it could have reused.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn explain_workflow_task(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<TaskInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "explain_workflow_task", &input, Permission::READ)
            .await?;
        let path = Path((input.run_id, input.task_id));
        payload(reports::task_why(self.handler_state(), RealmAuth(auth), path).await)
    }

    #[tool(
        description = "Read a task's stored stdout or stderr from a byte offset, at most 64 KiB per call. Pass the answer's next_offset as offset to continue; complete is true once the task ended and nothing follows. Only the last 64 KiB of each stream of an ended attempt are kept.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub async fn read_task_logs(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<TaskInput>,
    ) -> ToolResult {
        let auth = self
            .checked(&parts, "read_task_logs", &input, Permission::READ)
            .await?;
        let query = TaskLogQuery {
            stream: input.stream,
            offset: input.offset,
        };
        let path = Path((input.run_id, input.task_id));
        payload(tasks::task_logs(self.handler_state(), RealmAuth(auth), path, Query(query)).await)
    }
}

impl McpServer {
    fn handler_state(&self) -> State<std::sync::Arc<crate::server::state::ServerState>> {
        State(self.state.clone())
    }

    /// The caller, once the realm deny policies allow the tool; the handler checks the rest.
    async fn checked<T: Serialize>(
        &self,
        parts: &http::request::Parts,
        tool: &str,
        input: &T,
        permission: Permission,
    ) -> Result<AuthContext, CallToolResult> {
        let auth = request_auth(parts)?;
        let extras = tool_extras(tool, input)?;
        super::authorize_self(&self.state, &auth, permission, extras)
            .await
            .map_err(super::server_error)?;
        Ok(auth)
    }
}

/// The REST body a tool passes on, refused with the route's own field names.
fn body_of<T: DeserializeOwned>(body: JsonPayload) -> Result<T, CallToolResult> {
    serde_json::from_value(body.0).map_err(|error| bad_request(format!("body: {error}")))
}

fn value<T: Serialize>(
    answer: ServerResult<axum::Json<T>>,
) -> Result<serde_json::Value, CallToolResult> {
    let axum::Json(body) = answer.map_err(super::server_error)?;
    serde_json::to_value(body).map_err(internal_error)
}

fn payload<T: Serialize>(answer: ServerResult<axum::Json<T>>) -> ToolResult {
    Ok(Json(JsonPayload(value(answer)?)))
}

#[cfg(test)]
#[path = "workflows_tests.rs"]
mod tests;
