//! Single-transaction unified save route for a workflow version and its paired automation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::automations::save::{
    SaveAutomation, SaveBundle, SaveBundleOperation, SaveWorkflow,
};
use aruna_ops_work::workflows::reads::WorkflowView;
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{AutomationBody, SpecBody};
use super::{auto_error, param_id};
use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerResult};
use crate::routes::execution::workflow::registry::created_body;
use crate::routes::execution::workflow::source_input;
use crate::routes::execution::workflow::wire::{
    Visibility, WorkflowCreated, WorkflowLanguage, WorkflowSourceBody, WorkflowSummary, present,
};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(save_bundle))
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = SaveWorkflowInput)]
pub struct SaveWorkflowRequest {
    pub workflow_id: Option<String>,
    pub name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub description: Option<Option<String>>,
    pub language: Option<WorkflowLanguage>,
    pub visibility: Option<Visibility>,
    pub message: Option<String>,
    pub source: Option<WorkflowSourceBody>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = SaveAutomationInput)]
pub struct SaveAutomationRequest {
    #[serde(default)]
    pub automation_id: Option<String>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub spec: SpecBody,
    /// Pin matching `run` stages (and `scope.workflows`) to the saved workflow version.
    #[serde(default = "default_true")]
    pub pin_version: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = SaveWorkflowBundleRequest, example = json!({
    "group_id": "01JABCDEF0123456789ABCDEFG",
    "workflow": {
        "name": "qc-flow",
        "language": "wir",
        "source": {"wir": {"wir_version": "1"}}
    },
    "automation": {
        "name": "qc-auto",
        "spec": {
            "triggers": [{"kind": "manual"}],
            "conditions": {"mode": "all", "items": []},
            "stages": [{"name": "alert", "kind": "notify", "audience": "group_admins", "message": "Done"}],
            "scope": {"audiences": ["group_admins"]}
        }
    }
}))]
pub struct SaveBundleRequest {
    pub group_id: String,
    pub workflow: SaveWorkflowRequest,
    pub automation: SaveAutomationRequest,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = SaveWorkflowBundleResponse, example = json!({
    "workflow": {
        "workflow_id": "01K8W2A0000000000000000000",
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "name": "qc-flow",
        "description": null,
        "language": "wir",
        "visibility": "group",
        "latest_version": "1",
        "version_count": 1,
        "edam_formats": [],
        "updated_at": "2026-11-26T12:00:00+00:00",
        "created_by": "01K8W1A0000000000000000000",
        "can_write": true,
        "automation_id": "01K8W3A0000000000000000000"
    },
    "version": {
        "workflow_id": "01K8W2A0000000000000000000",
        "version": "1",
        "commit": null,
        "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1"
    },
    "automation": {
        "automation_id": "01K8W3A0000000000000000000",
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "name": "qc-auto",
        "description": "",
        "host_node": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
        "lifecycle": "draft",
        "pause_reason": null,
        "spec": {
            "triggers": [{"kind": "manual"}],
            "conditions": {"mode": "all", "items": []},
            "stages": [{"name": "alert", "kind": "notify", "audience": "group_admins", "message": "Done"}],
            "scope": {
                "read": [], "write": [], "metadata": [], "workflows": [],
                "audiences": ["group_admins"], "secrets": [], "allow_downgrade": false,
                "limits": {"max_active_firings": null, "max_concurrent_workflows": null, "per_hour": null, "per_day": null, "bytes_per_day": null, "cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "max_chain_depth": null, "effective": {"max_active_firings": 4, "max_concurrent_workflows": 4, "per_hour": 30, "per_day": 200, "bytes_per_day": 10737418240u64, "max_chain_depth": 3}}
            }
        },
        "pending": null,
        "account": null,
        "approved_by": null,
        "approved_at": null,
        "created_by": "01K8W1A0000000000000000000",
        "created_at": "2026-11-26T12:00:00+00:00",
        "updated_at": "2026-11-26T12:00:00+00:00",
        "revision": 1,
        "workflow_id": "01K8W2A0000000000000000000"
    },
    "identity_synced": true
}))]
pub struct SaveBundleResponse {
    pub workflow: WorkflowSummary,
    pub version: Option<WorkflowCreated>,
    pub automation: AutomationBody,
    /// Whether the automation's service account role already matches the saved revision.
    pub identity_synced: bool,
}

#[utoipa::path(
    post,
    path = "/compute/automations/save",
    tag = "automations",
    summary = "Atomically save a workflow version and its automation",
    description = r#"Preflights the workflow source when provided, pins matching `run` stages to the saved workflow version, and commits the workflow version and automation spec in one atomic write transaction.

**Authentication**: unrestricted realm bearer token with WRITE on the group's workflows and data."#,
    request_body = SaveBundleRequest,
    responses(
        (status = 200, description = "Saved workflow, version and automation", body = SaveBundleResponse),
        (status = 400, description = "Invalid workflow source, DAG, condition or scope", body = ErrorResponse),
        (status = 403, description = "Permission denied or feature disabled", body = ErrorResponse),
        (status = 404, description = "Referenced workflow or automation not found", body = ErrorResponse),
        (status = 409, description = "Revision conflict", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn save_bundle(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Json(body): Json<SaveBundleRequest>,
) -> ServerResult<Json<SaveBundleResponse>> {
    let group_id = param_id(&body.group_id, "group_id")?;
    let spec = body.automation.spec.to_domain()?;
    let workflow_id = body
        .workflow
        .workflow_id
        .as_deref()
        .map(|id| param_id(id, "workflow_id"))
        .transpose()?;
    let automation_id = body
        .automation
        .automation_id
        .as_deref()
        .map(|id| param_id(id, "automation_id"))
        .transpose()?;
    let source = body.workflow.source.map(source_input).transpose()?;

    let input = SaveBundle {
        group_id,
        workflow: SaveWorkflow {
            workflow_id,
            name: body.workflow.name,
            description: body.workflow.description,
            language: body.workflow.language.map(Into::into),
            visibility: body.workflow.visibility.map(Into::into),
            message: body.workflow.message,
            source,
        },
        automation: SaveAutomation {
            automation_id,
            expected_revision: body.automation.expected_revision,
            name: body.automation.name,
            description: body.automation.description,
            spec,
            pin_version: body.automation.pin_version,
        },
    };
    let operation = SaveBundleOperation::new(&auth, state.get_node_id(), input)
        .with_disabled_features(state.disabled_features());
    let saved = drive(operation, &state.get_ctx())
        .await
        .map_err(auto_error)?;
    let workflow = WorkflowSummary::from(WorkflowView {
        record: saved.workflow,
        can_write: true,
    });
    Ok(Json(SaveBundleResponse {
        workflow,
        version: saved.created.map(created_body),
        automation: AutomationBody::from_domain(&saved.automation),
        identity_synced: saved.identity_synced,
    }))
}
