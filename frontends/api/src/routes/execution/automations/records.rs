//! CRUD routes for workflow automations under `/compute/automations`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::automations::change::{AutoChange, CreateAutoOperation, NewAuto};
use aruna_ops_work::automations::lifecycle::ChangeAutoOperation;
use aruna_ops_work::automations::reads::ListAutosOperation;
use aruna_ops_work::automations::{AutoAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{
    AutomationBody, AutomationPage, CreateAutoRequest, ListAutoQuery, UpdateAutoRequest,
    parse_lifecycle,
};
use super::{auto_error, limit, param_id, path_id, require_switches};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_automations, create_automation))
        .routes(routes!(
            get_automation,
            update_automation,
            delete_automation
        ))
}

#[utoipa::path(
    get,
    path = "/compute/automations",
    tag = "automations",
    summary = "List workflow automations of a group",
    description = r#"Lists workflow automations belonging to a group, optionally filtered by lifecycle state.

**Authentication**: realm bearer token with READ on the group."#,
    params(ListAutoQuery),
    responses(
        (status = 200, description = "One page of automations", body = AutomationPage),
        (status = 400, description = "Invalid query parameter", body = ErrorResponse),
        (status = 404, description = "Group not readable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_automations(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<ListAutoQuery>,
) -> ServerResult<Json<AutomationPage>> {
    let group_id = param_id(&query.group_id, "group_id")?;
    let lifecycle = query
        .lifecycle
        .as_deref()
        .map(parse_lifecycle)
        .transpose()?;
    let after = query
        .cursor
        .as_deref()
        .map(|c| param_id(c, "cursor"))
        .transpose()?;
    let op = ListAutosOperation::new(
        &auth,
        (group_id, state.get_node_id()),
        lifecycle,
        after,
        limit(query.limit),
    );
    let page = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(AutomationPage {
        items: page.items.iter().map(AutomationBody::from_domain).collect(),
        next_cursor: page.next.map(|id| id.to_string()),
    }))
}

#[utoipa::path(
    post,
    path = "/compute/automations",
    tag = "automations",
    summary = "Create a draft workflow automation",
    description = r#"Creates a new draft workflow automation after validating its DAG, triggers and scope.

**Authentication**: unrestricted realm bearer token with WRITE on the group."#,
    request_body = CreateAutoRequest,
    responses(
        (status = 201, description = "Created draft automation", body = AutomationBody),
        (status = 400, description = "Invalid DAG, trigger or scope", body = ErrorResponse),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 409, description = "Automations or applets feature disabled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Json(body): Json<CreateAutoRequest>,
) -> ServerResult<(StatusCode, Json<AutomationBody>)> {
    let group_id = param_id(&body.group_id, "group_id")?;
    let spec = body.spec.to_domain()?;
    require_switches(&state, group_id, spec.uses_applets()).await?;
    let input = NewAuto {
        group_id,
        name: body.name,
        description: body.description,
        spec,
    };
    let op = CreateAutoOperation::new(&auth, state.get_node_id(), input);
    let auto = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok((
        StatusCode::CREATED,
        Json(AutomationBody::from_domain(&auto)),
    ))
}

#[utoipa::path(
    get,
    path = "/compute/automations/{automation_id}",
    tag = "automations",
    summary = "Read one workflow automation",
    description = r#"Returns the current specification, lifecycle state and pending changes of one workflow automation.

**Authentication**: realm bearer token with READ on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    responses(
        (status = 200, description = "The automation record", body = AutomationBody),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_automation(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<AutomationBody>> {
    let id = path_id(&id)?;
    let op = AutoAccessOperation::new(&auth, id, state.get_node_id(), Need::Read);
    let auto = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(AutomationBody::from_domain(&auto)))
}

#[utoipa::path(
    patch,
    path = "/compute/automations/{automation_id}",
    tag = "automations",
    summary = "Update a workflow automation",
    description = r#"Updates a workflow automation under optimistic revision checking, queueing widening edits for approval.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = UpdateAutoRequest,
    responses(
        (status = 200, description = "Updated automation (widening changes become pending)", body = AutomationBody),
        (status = 400, description = "Invalid DAG, trigger or scope", body = ErrorResponse),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Revision conflict or feature disabled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn update_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<UpdateAutoRequest>,
) -> ServerResult<Json<AutomationBody>> {
    let id = path_id(&id)?;
    let spec = body.spec.as_ref().map(|s| s.to_domain()).transpose()?;
    let existing = drive(
        AutoAccessOperation::new(&auth, id, state.get_node_id(), Need::Write),
        &state.get_ctx(),
    )
    .await
    .map_err(auto_error)?;
    let applets = spec
        .as_ref()
        .map_or(existing.spec.uses_applets(), |s| s.uses_applets());
    require_switches(&state, existing.group_id, applets).await?;
    let change = AutoChange::Update {
        name: body.name,
        description: body.description,
        spec,
    };
    let op = ChangeAutoOperation::new(
        &auth,
        (id, Some(body.expected_revision)),
        change,
        state.get_node_id(),
    );
    let auto = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(AutomationBody::from_domain(&auto)))
}

#[utoipa::path(
    delete,
    path = "/compute/automations/{automation_id}",
    tag = "automations",
    summary = "Delete a workflow automation",
    description = r#"Marks a workflow automation as removed and revokes its dedicated service account.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    responses(
        (status = 204, description = "Automation removed"),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let id = path_id(&id)?;
    let op = ChangeAutoOperation::new(&auth, (id, None), AutoChange::Delete, state.get_node_id());
    drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(StatusCode::NO_CONTENT)
}
