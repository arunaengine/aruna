//! MCP tools for workflow automations: create, update, read, dry-run check, request approval
//! and pause. Agents can never approve an automation or an approval stage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::extract::{Path, State};
use rmcp::Json;
use rmcp::handler::server::tool::Extension;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};

use super::{JsonPayload, McpServer, request_auth, server_error};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::routes::execution::automations::{lifecycle, records, wire};

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CreateInput {
    /// 26-character ULID of the group that owns the automation.
    pub group_id: String,
    /// Human-readable automation name (1 to 120 characters).
    pub name: String,
    /// Optional plain-text description of what the automation does.
    #[serde(default)]
    pub description: String,
    /// Automation spec JSON object with `triggers`, `stages` (up to 16) and `scope`.
    pub spec: JsonPayload,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct UpdateInput {
    /// 26-character ULID of the automation to update.
    pub automation_id: String,
    /// Current revision number for optimistic concurrency control.
    pub expected_revision: u64,
    /// Optional replacement name.
    pub name: Option<String>,
    /// Optional replacement description.
    pub description: Option<String>,
    /// Optional replacement spec JSON object; widening changes on an active automation wait for admin approval.
    pub spec: Option<JsonPayload>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct AutoIdInput {
    /// 26-character ULID of the automation.
    pub automation_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CheckInput {
    /// 26-character ULID of the automation to dry-run check.
    pub automation_id: String,
    /// Optional sample trigger values JSON object used to validate workflow input forms.
    pub sample_values: Option<JsonPayload>,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct RevisionInput {
    /// 26-character ULID of the automation.
    pub automation_id: String,
    /// Optional current revision number for optimistic concurrency control.
    pub expected_revision: Option<u64>,
}

fn payload<T: Serialize>(value: T) -> Result<Json<JsonPayload>, CallToolResult> {
    let value = serde_json::to_value(value).map_err(super::internal_error)?;
    Ok(Json(JsonPayload(value)))
}

pub(crate) fn toolset() -> rmcp::handler::server::router::tool::ToolRouter<McpServer> {
    McpServer::automation_router()
}

#[tool_router(router = automation_router)]
impl McpServer {
    #[tool(
        description = "Create a draft workflow automation in a group. Draft automations never run until a human group admin approves them.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn automation_create(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<CreateInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let spec: wire::SpecBody =
            serde_json::from_value(input.spec.0).map_err(super::bad_request)?;
        let req = wire::CreateAutoRequest {
            group_id: input.group_id,
            name: input.name,
            description: input.description,
            spec,
        };
        let (_, axum::Json(created)) = records::create_automation(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            axum::Json(req),
        )
        .await
        .map_err(server_error)?;
        payload(created)
    }

    #[tool(
        description = "Update a workflow automation using optimistic expected_revision. Widening changes on an active automation are staged as pending until a human group admin approves them.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn automation_update(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<UpdateInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let spec = input
            .spec
            .map(|p| serde_json::from_value(p.0))
            .transpose()
            .map_err(super::bad_request)?;
        let req = wire::UpdateAutoRequest {
            expected_revision: input.expected_revision,
            name: input.name,
            description: input.description,
            spec,
        };
        let axum::Json(updated) = records::update_automation(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.automation_id),
            axum::Json(req),
        )
        .await
        .map_err(server_error)?;
        payload(updated)
    }

    #[tool(
        description = "Read a workflow automation record including its active spec, pending change and approval state.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    pub async fn automation_get(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<AutoIdInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let axum::Json(auto) = records::get_automation(
            State(self.state.clone()),
            RealmAuth(auth),
            Path(input.automation_id),
        )
        .await
        .map_err(server_error)?;
        payload(auto)
    }

    #[tool(
        description = "Dry-run check a workflow automation's DAG, scope, workflow values, copy destination previews and sandbox requirements without side effects.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    pub async fn automation_check(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<CheckInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let req = wire::CheckRequest {
            sample_values: input.sample_values.map(|v| v.0),
        };
        let axum::Json(report) = lifecycle::check_automation(
            State(self.state.clone()),
            RealmAuth(auth),
            Path(input.automation_id),
            axum::Json(req),
        )
        .await
        .map_err(server_error)?;
        payload(report)
    }

    #[tool(
        description = "Submit a draft or pending workflow automation change for human group admin approval. Agents cannot approve automations.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn automation_request_approval(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<RevisionInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let req = wire::RevisionBody {
            expected_revision: input.expected_revision,
        };
        let axum::Json(auto) = lifecycle::request_approval(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.automation_id),
            axum::Json(req),
        )
        .await
        .map_err(server_error)?;
        payload(auto)
    }

    #[tool(
        description = "Pause an active workflow automation so new matched events are kept without starting runs.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    pub async fn automation_pause(
        &self,
        Extension(parts): Extension<http::request::Parts>,
        Parameters(input): Parameters<RevisionInput>,
    ) -> Result<Json<JsonPayload>, CallToolResult> {
        let auth = request_auth(&parts)?;
        let req = wire::RevisionBody {
            expected_revision: input.expected_revision,
        };
        let axum::Json(auto) = lifecycle::pause_automation(
            State(self.state.clone()),
            UnrestrictedAuth(auth),
            Path(input.automation_id),
            axum::Json(req),
        )
        .await
        .map_err(server_error)?;
        payload(auto)
    }
}
