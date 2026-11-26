//! Manual firing start and kept-event inspection/replay routes for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::automations::firing_reads::ListEventsOperation;
use aruna_ops_work::automations::start::{StartEventsOperation, StartManualOperation};
use aruna_ops_work::automations::{AutoAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::{FiringBody, KeptEventBody, StartEventsRequest, StartRequest};
use super::{auto_error, param_id, path_id, require_switches};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(start_automation))
        .routes(routes!(list_events))
        .routes(routes!(start_events))
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/start",
    tag = "automations",
    summary = "Manually start an active automation that declares a manual trigger",
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = StartRequest,
    responses(
        (status = 202, description = "Started firing", body = FiringBody),
        (status = 400, description = "Automation has no manual trigger", body = ErrorResponse),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Automation not active or feature disabled", body = ErrorResponse),
        (status = 429, description = "Rate or active-firing cap reached", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<StartRequest>,
) -> ServerResult<(StatusCode, Json<FiringBody>)> {
    let id = path_id(&id)?;
    let auto = drive(
        AutoAccessOperation::new(&auth, id, state.get_node_id(), Need::Write),
        &state.get_ctx(),
    )
    .await
    .map_err(auto_error)?;
    require_switches(&state, auto.group_id, false).await?;
    let values = body
        .values
        .map(|v| v.to_string())
        .unwrap_or_else(|| "{}".into());
    let op = StartManualOperation::new(&auth, id, state.get_node_id(), values);
    let firing = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok((StatusCode::ACCEPTED, Json(FiringBody::from_domain(&firing))))
}

#[utoipa::path(
    get,
    path = "/compute/automations/{automation_id}/events",
    tag = "automations",
    summary = "List kept matched events waiting to be started for an automation",
    params(("automation_id" = String, Path, description = "Automation ULID")),
    responses(
        (status = 200, description = "Kept matched events, newest first", body = Vec<KeptEventBody>),
        (status = 404, description = "Not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_events(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<Vec<KeptEventBody>>> {
    let id = path_id(&id)?;
    let op = ListEventsOperation::new(&auth, id, state.get_node_id(), 100);
    let events = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok(Json(
        events.iter().map(KeptEventBody::from_domain).collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/events/start",
    tag = "automations",
    summary = "Start firings for chosen kept matched events",
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = StartEventsRequest,
    responses(
        (status = 202, description = "Started firings", body = Vec<FiringBody>),
        (status = 400, description = "Invalid event ids", body = ErrorResponse),
        (status = 403, description = "Permission denied", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Automation not active or feature disabled", body = ErrorResponse),
        (status = 429, description = "Rate or active-firing cap reached", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_events(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<StartEventsRequest>,
) -> ServerResult<(StatusCode, Json<Vec<FiringBody>>)> {
    let id = path_id(&id)?;
    let auto = drive(
        AutoAccessOperation::new(&auth, id, state.get_node_id(), Need::Write),
        &state.get_ctx(),
    )
    .await
    .map_err(auto_error)?;
    require_switches(&state, auto.group_id, auto.spec.uses_applets()).await?;
    let event_ids = body
        .event_ids
        .iter()
        .map(|s| param_id(s, "event_id"))
        .collect::<ServerResult<Vec<_>>>()?;
    let op = StartEventsOperation::new(&auth, id, state.get_node_id(), event_ids);
    let firings = drive(op, &state.get_ctx()).await.map_err(auto_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(firings.iter().map(FiringBody::from_domain).collect()),
    ))
}
