//! Routes for listing, reading, cancelling, resolving, resuming and approving automation runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::automations::control::{
    CancelFiringOperation, DecideStageOperation, ResolveStageOperation, ResumeFiringOperation,
};
use aruna_ops_work::automations::firing_reads::{GetFiringOperation, ListFiringsOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::{
    DecideRequest, FiringBody, FiringPageBody, FiringQuery, ResolveRequest, parse_firing_state,
};
use super::{auto_error, limit, param_id, path_id, require_switches};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_firings))
        .routes(routes!(get_firing))
        .routes(routes!(cancel_firing))
        .routes(routes!(resolve_stage))
        .routes(routes!(resume_firing))
        .routes(routes!(approve_stage))
        .routes(routes!(reject_stage))
}

#[utoipa::path(
    get,
    path = "/compute/automation-runs",
    tag = "automations",
    summary = "List automation runs for a group or automation",
    params(FiringQuery),
    responses(
        (status = 200, description = "One page of automation runs", body = FiringPageBody),
        (status = 400, description = "Invalid query filter", body = ErrorResponse),
        (status = 404, description = "Group or automation not readable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_firings(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<FiringQuery>,
) -> ServerResult<Json<FiringPageBody>> {
    let group_id = query
        .group_id
        .as_deref()
        .map(|s| param_id(s, "group_id"))
        .transpose()?;
    let auto_id = query
        .automation_id
        .as_deref()
        .map(|s| param_id(s, "automation_id"))
        .transpose()?;
    let st = query.state.as_deref().map(parse_firing_state).transpose()?;
    let after = query
        .cursor
        .as_deref()
        .map(|s| param_id(s, "cursor"))
        .transpose()?;
    let op = ListFiringsOperation::new(
        &auth,
        (group_id, auto_id),
        st,
        state.get_node_id(),
        after,
        limit(query.limit),
    );
    let page = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(FiringPageBody {
        items: page.items.iter().map(FiringBody::from_domain).collect(),
        next_cursor: page.next.map(|id| id.to_string()),
    }))
}

#[utoipa::path(
    get,
    path = "/compute/automation-runs/{firing_id}",
    tag = "automations",
    summary = "Read one automation run with stage states, receipts and resolutions",
    params(("firing_id" = String, Path, description = "Firing ULID")),
    responses(
        (status = 200, description = "The automation run", body = FiringBody),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_firing(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<FiringBody>> {
    let id = path_id(&id)?;
    let op = GetFiringOperation::new(&auth, id, state.get_node_id());
    let (firing, _) = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(FiringBody::from_domain(&firing)))
}

#[utoipa::path(
    post,
    path = "/compute/automation-runs/{firing_id}/cancel",
    tag = "automations",
    summary = "Asynchronously cancel an active automation run and its child workflow runs",
    params(("firing_id" = String, Path, description = "Firing ULID")),
    responses(
        (status = 200, description = "Cancelling or cancelled automation run", body = FiringBody),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn cancel_firing(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<FiringBody>> {
    let id = path_id(&id)?;
    let local = (state.get_node_id(), state.get_realm_id());
    let op = CancelFiringOperation::new(&auth, id, local);
    let firing = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(FiringBody::from_domain(&firing)))
}

#[utoipa::path(
    post,
    path = "/compute/automation-runs/{firing_id}/resolve",
    tag = "automations",
    summary = "Record an audited human resolution for a needs_attention stage",
    params(("firing_id" = String, Path, description = "Firing ULID")),
    request_body = ResolveRequest,
    responses(
        (status = 200, description = "Updated automation run", body = FiringBody),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Stage is not in needs_attention", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resolve_stage(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<ResolveRequest>,
) -> ServerResult<Json<FiringBody>> {
    let id = path_id(&id)?;
    let op = ResolveStageOperation::new(
        &auth,
        id,
        body.stage,
        body.done,
        body.note,
        state.get_node_id(),
    );
    let firing = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(FiringBody::from_domain(&firing)))
}

#[utoipa::path(
    post,
    path = "/compute/automation-runs/{firing_id}/resume",
    tag = "automations",
    summary = "Resume a failed automation run while preserving committed receipts",
    params(("firing_id" = String, Path, description = "Firing ULID")),
    responses(
        (status = 200, description = "Resumed automation run", body = FiringBody),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Not resumable or needs_attention unresolved", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resume_firing(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<FiringBody>> {
    let id = path_id(&id)?;
    let (_, auto) = drive(
        GetFiringOperation::new(&auth, id, state.get_node_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(auto_error)?;
    require_switches(&state, auto.group_id, false).await?;
    let op = ResumeFiringOperation::new(&auth, id, state.get_node_id());
    let firing = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(FiringBody::from_domain(&firing)))
}

async fn decide_stage(
    state: &ServerState,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    (id, stage): (String, String),
    approve: bool,
    note: Option<String>,
) -> ServerResult<Json<FiringBody>> {
    let id = path_id(&id)?;
    if approve {
        let (_, auto) = drive(
            GetFiringOperation::new(auth, id, state.get_node_id()),
            &state.get_ctx(),
        )
        .await
        .map_err(auto_error)?;
        require_switches(state, auto.group_id, false).await?;
    }
    let op = DecideStageOperation::new(auth, id, stage, approve, note, state.get_node_id());
    let firing = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(FiringBody::from_domain(&firing)))
}

#[utoipa::path(
    post,
    path = "/compute/automation-runs/{firing_id}/stages/{stage}/approve",
    tag = "automations",
    summary = "Approve a waiting approval stage as a human group admin",
    params(
        ("firing_id" = String, Path, description = "Firing ULID"),
        ("stage" = String, Path, description = "Approval stage name")
    ),
    request_body = DecideRequest,
    responses(
        (status = 200, description = "Updated automation run", body = FiringBody),
        (status = 403, description = "Human group admin required", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Stage not waiting for approval", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn approve_stage(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(path): Path<(String, String)>,
    Json(body): Json<DecideRequest>,
) -> ServerResult<Json<FiringBody>> {
    decide_stage(&state, &auth, path, true, body.note).await
}

#[utoipa::path(
    post,
    path = "/compute/automation-runs/{firing_id}/stages/{stage}/reject",
    tag = "automations",
    summary = "Reject a waiting approval stage as a human group admin",
    params(
        ("firing_id" = String, Path, description = "Firing ULID"),
        ("stage" = String, Path, description = "Approval stage name")
    ),
    request_body = DecideRequest,
    responses(
        (status = 200, description = "Updated automation run", body = FiringBody),
        (status = 403, description = "Human group admin required", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Stage not waiting for approval", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn reject_stage(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(path): Path<(String, String)>,
    Json(body): Json<DecideRequest>,
) -> ServerResult<Json<FiringBody>> {
    decide_stage(&state, &auth, path, false, body.note).await
}
