//! Lifecycle transitions, approval diff, dry-run check and static graph routes for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::automation::graph::build_graph;
use aruna_core::automation::record::PauseReason;
use aruna_ops_work::automations::change::AutoChange;
use aruna_ops_work::automations::check::CheckAutoOperation;
use aruna_ops_work::automations::diff::DiffAutoOperation;
use aruna_ops_work::automations::lifecycle::ChangeAutoOperation;
use aruna_ops_work::automations::{AutoAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, State};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{AutomationBody, CheckBody, CheckRequest, DiffBody, RevisionBody, graph_body};
use super::{auto_error, path_id};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::error::{ErrorResponse, ServerResult};
use crate::routes::execution::workflow::wire::WorkflowGraphBody;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(request_approval))
        .routes(routes!(approve_automation))
        .routes(routes!(reject_automation))
        .routes(routes!(pause_automation))
        .routes(routes!(resume_automation))
        .routes(routes!(diff_automation))
        .routes(routes!(check_automation))
        .routes(routes!(graph_automation))
}

async fn apply_change(
    state: &ServerState,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    id: &str,
    expected: Option<u64>,
    change: AutoChange,
) -> ServerResult<Json<AutomationBody>> {
    let id = path_id(id)?;
    let operation = ChangeAutoOperation::new(auth, (id, expected), change, state.get_node_id())
        .with_disabled_features(state.disabled_features());
    let updated = drive(operation, &state.get_ctx())
        .await
        .map_err(auto_error)?;
    Ok(Json(AutomationBody::from_domain(&updated)))
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/request-approval",
    tag = "automations",
    summary = "Submit an automation for approval",
    description = r#"Submits a draft automation or pending widening change for human group admin approval.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "Automation waiting for approval", body = AutomationBody),
        (status = 403, description = "Permission denied or feature disabled", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Revision conflict", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn request_approval(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AutomationBody>> {
    apply_change(
        &state,
        &auth,
        &id,
        body.expected_revision,
        AutoChange::RequestApproval,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/approve",
    tag = "automations",
    summary = "Approve a workflow automation",
    description = r#"Approves a workflow automation or its pending change and activates its scoped service account.

**Authentication**: unrestricted human group admin bearer token."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "Approved active automation", body = AutomationBody),
        (status = 403, description = "Human group admin required or feature disabled", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Revision conflict", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn approve_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AutomationBody>> {
    apply_change(
        &state,
        &auth,
        &id,
        body.expected_revision,
        AutoChange::Approve,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/reject",
    tag = "automations",
    summary = "Reject a pending automation approval",
    description = r#"Rejects a pending automation approval request and keeps the prior approved specification unchanged.

**Authentication**: unrestricted human group admin bearer token."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "Automation with pending change rejected", body = AutomationBody),
        (status = 403, description = "Human group admin required", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn reject_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AutomationBody>> {
    apply_change(
        &state,
        &auth,
        &id,
        body.expected_revision,
        AutoChange::Reject,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/pause",
    tag = "automations",
    summary = "Pause an active workflow automation",
    description = r#"Pauses an active workflow automation so matched events are held without starting new firings.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "Paused automation", body = AutomationBody),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Not active or revision conflict", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn pause_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AutomationBody>> {
    apply_change(
        &state,
        &auth,
        &id,
        body.expected_revision,
        AutoChange::Pause(PauseReason::Owner),
    )
    .await
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/resume",
    tag = "automations",
    summary = "Resume a paused workflow automation",
    description = r#"Resumes a paused approved workflow automation so future matching events start firings again.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "Active automation", body = AutomationBody),
        (status = 403, description = "Permission denied or feature disabled", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Not paused or revision conflict", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resume_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AutomationBody>> {
    apply_change(
        &state,
        &auth,
        &id,
        body.expected_revision,
        AutoChange::Resume,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/compute/automations/{automation_id}/diff",
    tag = "automations",
    summary = "Read automation approval diff",
    description = r#"Computes the permission, trigger and stage diff along with loop and sandbox warnings.

**Authentication**: realm bearer token with READ on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    responses(
        (status = 200, description = "Approval diff and warnings", body = DiffBody),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn diff_automation(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<DiffBody>> {
    let id = path_id(&id)?;
    let op = DiffAutoOperation::new(&auth, id, state.get_node_id(), None);
    let res = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(DiffBody::from_result(res)))
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/check",
    tag = "automations",
    summary = "Dry-run validate an automation",
    description = r#"Validates the automation DAG, pinned workflows, copy templates and sandbox flags without running stages.

**Authentication**: realm bearer token with READ on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = CheckRequest,
    responses(
        (status = 200, description = "Dry-run check diagnostics", body = CheckBody),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn check_automation(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
    Json(body): Json<CheckRequest>,
) -> ServerResult<Json<CheckBody>> {
    let id = path_id(&id)?;
    let op = CheckAutoOperation::new(&auth, id, state.get_node_id(), body.sample_values);
    let rep = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(CheckBody::from_report(rep)))
}

#[utoipa::path(
    get,
    path = "/compute/automations/{automation_id}/graph",
    tag = "automations",
    summary = "Read automation static DAG",
    description = r#"Returns the static trigger and stage DAG of an automation in workflow graph format.

**Authentication**: realm bearer token with READ on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    responses(
        (status = 200, description = "Static automation DAG", body = WorkflowGraphBody, example = json!({
            "nodes": [{"id": "trigger:0", "kind": "input", "label": "manual", "tool": null, "version": null}],
            "edges": [{"from": {"node": "trigger:0", "port": "out"}, "to": {"node": "stage:alert", "port": "in"}, "scatter": null, "fit": null}],
            "complete": true
        })),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn graph_automation(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<WorkflowGraphBody>> {
    let id = path_id(&id)?;
    let op = AutoAccessOperation::new(&auth, id, state.get_node_id(), Need::Read);
    let auto = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    let spec = auto.pending.as_ref().map(|p| &p.spec).unwrap_or(&auto.spec);
    Ok(Json(graph_body(&build_graph(spec))))
}
