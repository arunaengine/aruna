//! Manual firing start and kept-event inspection/replay routes for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::automations::AutoError;
use aruna_ops_work::automations::firing_reads::ListEventsOperation;
use aruna_ops_work::automations::start::{ManualStart, StartEventsOperation, StartManualOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::{
    ConditionsRefusalBody, FiringBody, KeptEventBody, StartConflictBody, StartEventsRequest,
    StartRequest, StateConflictBody,
};
use super::{auto_error, param_id, path_id};
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
    summary = "Manually start an automation",
    description = r#"Starts one firing of an active automation that declares a manual trigger.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group.

**Limits**: returns 429 when rate or active-firing limits are reached."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = StartRequest,
    responses(
        (status = 202, description = "Started firing", body = FiringBody),
        (status = 400, description = "Automation has no manual trigger", body = ErrorResponse),
        (status = 403, description = "Permission denied or feature disabled", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (
            status = 409,
            description = "Conditions not met or automation not active",
            body = StartConflictBody,
            examples(
                ("Conditions not met" = (
                    summary = "Manual start refused because automation conditions did not hold",
                    value = json!({
                        "error": "conditions_not_met",
                        "code": "conditions_not_met",
                        "message": "It was 22:00 UTC, outside 08:00 to 18:00 UTC.",
                        "firing": {
                            "firing_id": "01K8W4A0000000000000000000",
                            "automation_id": "01K8W3A0000000000000000000",
                            "group_id": "01JABCDEF0123456789ABCDEFG",
                            "revision": 2,
                            "attempt": 1,
                            "state": "skipped",
                            "trigger_kind": "manual",
                            "trigger_event": {"kind": "manual", "actor": "01K8W1A0000000000000000000", "values": {}},
                            "stages": [],
                            "resolutions": [],
                            "created_at": "2026-11-26T22:00:00+00:00",
                            "updated_at": "2026-11-26T22:00:00+00:00",
                            "finished_at": "2026-11-26T22:00:00+00:00",
                            "error": null,
                            "reason": "It was 22:00 UTC, outside 08:00 to 18:00 UTC.",
                            "conditions_checked": [{
                                "kind": "time_of_day",
                                "text": "Between 08:00 and 18:00 UTC",
                                "held": false,
                                "why": "It was 22:00 UTC, outside 08:00 to 18:00 UTC."
                            }]
                        }
                    })
                )),
                ("Not active" = (
                    summary = "Manual start refused because the automation is not active",
                    value = json!({"error": "automation must be active to start"})
                ))
            )
        ),
        (status = 429, description = "Rate or active-firing limit reached", body = ErrorResponse,
            headers(("Retry-After" = u32, description = "Seconds before retrying")))
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_automation(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<StartRequest>,
) -> ServerResult<Response> {
    let id = path_id(&id)?;
    let values = body
        .values
        .map(|value| value.to_string())
        .unwrap_or_else(|| "{}".into());
    let operation = StartManualOperation::new(&auth, id, state.get_node_id(), values)
        .with_disabled_features(state.disabled_features());
    match drive(operation, &state.get_ctx()).await {
        Ok(ManualStart::Started(firing)) => {
            Ok((StatusCode::ACCEPTED, Json(FiringBody::from_domain(&firing))).into_response())
        }
        Ok(ManualStart::Skipped(firing)) => {
            let message = firing
                .reason
                .clone()
                .unwrap_or_else(|| "Conditions were not met.".into());
            let body = StartConflictBody::Conditions(Box::new(ConditionsRefusalBody {
                error: "conditions_not_met".into(),
                code: "conditions_not_met".into(),
                message,
                firing: FiringBody::from_domain(&firing),
            }));
            Ok((StatusCode::CONFLICT, Json(body)).into_response())
        }
        Err(AutoError::Conflict(message)) => {
            let body = StartConflictBody::Conflict(StateConflictBody {
                error: message,
                code: Some("Conflict".into()),
                details: None,
            });
            Ok((StatusCode::CONFLICT, Json(body)).into_response())
        }
        Err(error) => Err(auto_error(error)),
    }
}

#[utoipa::path(
    get,
    path = "/compute/automations/{automation_id}/events",
    tag = "automations",
    summary = "List kept automation events",
    description = r#"Lists matched events held while the automation was paused or capped, newest first.

**Authentication**: realm bearer token with READ on the automation's group."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    responses(
        (status = 200, description = "Kept matched events, newest first", body = Vec<KeptEventBody>, example = json!([{
            "event_id": "01K8W4B0000000000000000000",
            "kind": "data_uploaded",
            "resource": "reads/run1/a.fastq",
            "revision": "01K8W1C0000000000000000000",
            "reason": "automation paused",
            "kept_at": "2026-11-26T12:04:00+00:00",
            "actor": null,
            "event": {
                "kind": "data_uploaded",
                "bucket": "reads",
                "key": "run1/a.fastq",
                "version_id": "01K8W1C0000000000000000000",
                "path": "/realm/g/01JABCDEF0123456789ABCDEFG/data/node/reads/run1/a.fastq",
                "size_bytes": 1024
            }
        }])),
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
    let operation = ListEventsOperation::new(&auth, id, state.get_node_id(), 100);
    let events = drive(operation, &state.get_ctx())
        .await
        .map_err(auto_error)?;
    Ok(Json(
        events.iter().map(KeptEventBody::from_domain).collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/compute/automations/{automation_id}/events/start",
    tag = "automations",
    summary = "Start firings for kept matched events",
    description = r#"Starts firings for the chosen kept matched events of an active automation.

**Authentication**: unrestricted realm bearer token with WRITE on the automation's group.

**Limits**: returns 429 when rate or active-firing limits are reached."#,
    params(("automation_id" = String, Path, description = "Automation ULID")),
    request_body = StartEventsRequest,
    responses(
        (status = 202, description = "Started firings", body = Vec<FiringBody>, example = json!([{
            "firing_id": "01K8W4A0000000000000000000",
            "automation_id": "01K8W3A0000000000000000000",
            "group_id": "01JABCDEF0123456789ABCDEFG",
            "revision": 2,
            "attempt": 1,
            "state": "running",
            "trigger_kind": "data_uploaded",
            "trigger_event": {
                "kind": "data_uploaded",
                "bucket": "reads",
                "key": "run1/a.fastq",
                "version_id": "01K8W1C0000000000000000000",
                "path": "/realm/g/01JABCDEF0123456789ABCDEFG/data/node/reads/run1/a.fastq",
                "size_bytes": 1024
            },
            "stages": [],
            "resolutions": [],
            "created_at": "2026-11-26T12:05:00+00:00",
            "updated_at": "2026-11-26T12:05:00+00:00",
            "finished_at": null,
            "error": null,
            "reason": null,
            "conditions_checked": []
        }])),
        (status = 400, description = "Invalid event ids", body = ErrorResponse),
        (status = 403, description = "Permission denied or feature disabled", body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 409, description = "Automation not active or kept event no longer eligible", body = ErrorResponse),
        (status = 429, description = "Rate or active-firing limit reached", body = ErrorResponse,
            headers(("Retry-After" = u32, description = "Seconds before retrying")))
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
    let event_ids = body
        .event_ids
        .iter()
        .map(|text| param_id(text, "event_id"))
        .collect::<ServerResult<Vec<_>>>()?;
    let operation = StartEventsOperation::new(&auth, id, state.get_node_id(), event_ids)
        .with_disabled_features(state.disabled_features());
    let firings = drive(operation, &state.get_ctx())
        .await
        .map_err(auto_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(firings.iter().map(FiringBody::from_domain).collect()),
    ))
}
