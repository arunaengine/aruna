//! Serves the routes that change a distributed bucket after its creation: join and leave of
//! members, fenced transfers of ranges, bucket-wide settings and deletion of the set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::distribution::{
    DistributionRevision, MetadataCoverage, TransferRole,
};
use aruna_operations::distribution::change::{
    JoinInput, change_settings, delete_set, join_member, leave_member, transfer_range,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::distributed::{
    AdmissionResponse, BehaviourName, SettingsRequest, StrategyName, admission_response, behaviour,
    map_create_error, node, owned_status, settings_of, strategy,
};
use super::routing::ensure_group_admin;
use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct SetChangesDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SetChangesDoc::openapi())
        .routes(routes!(join_distributed))
        .routes(routes!(leave_distributed))
        .routes(routes!(transfer_distributed))
        .routes(routes!(settings_distributed))
        .routes(routes!(delete_distributed))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoverageName {
    /// The member keeps the records of every range.
    All,
    /// The member keeps the records of the ranges it coordinates or replicates.
    Assigned,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoleName {
    /// The former coordinator keeps a copy of the range.
    Replica,
    /// The former coordinator gives up its copy once the new one is ready.
    Leave,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct JoinRequest {
    pub node_id: String,
    pub coverage: CoverageName,
    pub strategy: StrategyName,
    /// Overrides the bucket's range behaviour for this member.
    #[serde(default)]
    pub range_behaviour: Option<BehaviourName>,
    /// Ranges the member keeps a copy of; required for `assigned` coverage.
    #[serde(default)]
    pub ranges: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct TransferRequest {
    pub range_id: String,
    /// The member that coordinates the range afterwards; a ready `copy` replica of it.
    pub to: String,
    pub role: RoleName,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ChangeResponse {
    pub set_id: String,
    /// The record revision that took the change.
    pub revision: u64,
}

fn id(value: &str) -> ServerResult<Ulid> {
    value
        .parse()
        .map_err(|_| ServerError::BadRequestMessage(format!("`{value}` is not an id")))
}

fn change_response(set_id: Ulid, revision: DistributionRevision) -> ChangeResponse {
    ChangeResponse {
        set_id: set_id.to_string(),
        revision: revision.number,
    }
}

/// The caller after the group admin check, with the bearer token the publisher checks again.
async fn admin(
    state: &ServerState,
    auth: Option<AuthContext>,
    bearer: Option<ValidatedBearer>,
    (group_id, set_id): (Ulid, Ulid),
) -> ServerResult<(AuthContext, Option<aruna_operations::metadata::AuthToken>)> {
    let auth = require_realm_auth(state, auth)?;
    ensure_group_admin(state, &auth, group_id).await?;
    owned_status(state, group_id, set_id).await?;
    let bearer = crate::metadata::forwarded_auth_token(bearer)?;
    Ok((auth, bearer))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}/members",
    tag = "data/storage",
    summary = "Add a member to a distributed bucket",
    description = r#"Adds a member node to an active distributed bucket, then creates and admits its local bucket there.

**Authentication**: realm bearer token with WRITE on the group's admin path, and WRITE on the bucket path of the new member node. The publisher and the member node check the forwarded token again.

**Behavior**
- The member starts as `created`, admits itself and catches up from the coordinators of the
  ranges it holds before it becomes `ready`. Reads of those ranges avoid it until then.
- A node whose earlier member is leaving joins again as a new member.
- An existing other bucket of the name on the node fails the admission; retry it through the
  admission route.

**Limits**
- The node must be a realm infrastructure node. `assigned` coverage needs at least one range."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id")
    ),
    request_body(
        content = JoinRequest,
        description = "The node, its coverage, strategy and copied ranges",
        example = json!({
            "node_id": "3f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
            "coverage": "assigned",
            "strategy": "copy",
            "ranges": ["01JMPRT2123456789ABCDEFGHJ"]
        })
    ),
    responses(
        (status = 201, description = "The member's admission", body = AdmissionResponse),
        (status = 400, description = "Invalid node, range or coverage", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change", body = ErrorResponse),
        (status = 503, description = "Configuration changes unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn join_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, set_id)): Path<(Ulid, Ulid)>,
    Json(request): Json<JoinRequest>,
) -> ServerResult<(StatusCode, Json<AdmissionResponse>)> {
    let (auth, bearer) = admin(&state, auth, bearer, (group_id, set_id)).await?;
    let input = JoinInput {
        node_id: node(&request.node_id)?,
        coverage: match request.coverage {
            CoverageName::All => MetadataCoverage::All,
            CoverageName::Assigned => MetadataCoverage::Assigned,
        },
        strategy: strategy(request.strategy),
        range_behaviour: request.range_behaviour.map(behaviour),
        ranges: request
            .ranges
            .iter()
            .map(|value| id(value))
            .collect::<ServerResult<_>>()?,
    };
    let report = join_member(&state.get_ctx(), (&auth, bearer), set_id, input)
        .await
        .map_err(map_create_error)?;
    Ok((StatusCode::CREATED, Json(admission_response(report))))
}

#[utoipa::path(
    delete,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}/members/{member_id}",
    tag = "data/storage",
    summary = "Remove a member from a distributed bucket",
    description = r#"Marks a member leaving: coordinators stop publishing to it and it refuses their publications.

**Authentication**: realm bearer token with WRITE on the group's admin path; the publisher checks the forwarded token again.

**Behavior**
- A member that coordinates a range is refused; transfer its ranges first.
- The member's node releases its local bucket as an ordinary bucket with the data it holds;
  delete it there if it is not needed."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id"),
        ("member_id" = String, Path, description = "Member id")
    ),
    responses(
        (status = 200, description = "The revision that took the change", body = ChangeResponse),
        (status = 400, description = "The member still coordinates a range", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change", body = ErrorResponse),
        (status = 503, description = "Configuration changes unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn leave_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, set_id, member_id)): Path<(Ulid, Ulid, Ulid)>,
) -> ServerResult<Json<ChangeResponse>> {
    let (auth, bearer) = admin(&state, auth, bearer, (group_id, set_id)).await?;
    let revision = leave_member(&state.get_ctx(), (&auth, bearer), set_id, member_id)
        .await
        .map_err(map_create_error)?;
    Ok(Json(change_response(set_id, revision)))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}/transfers",
    tag = "data/storage",
    summary = "Move a range of a distributed bucket to another coordinator",
    description = r#"Opens a fenced transfer of one range to a ready `copy` replica of it.

**Authentication**: realm bearer token with WRITE on the group's admin path; the publisher checks the forwarded token again.

**Behavior**
- Only a ready `copy` replica of the range can take it; a refusal names the eligible members.
- The new coordinator takes a fresh epoch above every epoch of the set.
- The current coordinator fences the range: writes to it fail with a retryable "range moving"
  until the transfer activates. Open uploads of the range are aborted.
- The new coordinator completes once its copy holds everything up to the fence; then it
  writes the range and the former coordinator forwards to it.
- With role `leave` the former coordinator removes its copy once the new one is ready.
- The answer is 202 once the transfer is open; its progress shows in the bucket status.
- While the current coordinator is unreachable the transfer is refused with 503."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id")
    ),
    request_body(
        content = TransferRequest,
        description = "The range, the new coordinator and the former coordinator's role",
        example = json!({
            "range_id": "01JMPRT2123456789ABCDEFGHJ",
            "to": "01JMPRT3123456789ABCDEFGHJ",
            "role": "replica"
        })
    ),
    responses(
        (status = 202, description = "The revision that opened the transfer", body = ChangeResponse),
        (status = 400, description = "Unknown range, or the member is not a ready `copy` replica of it; the message names the eligible members", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change", body = ErrorResponse),
        (status = 503, description = "Coordinator or publisher unreachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn transfer_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, set_id)): Path<(Ulid, Ulid)>,
    Json(request): Json<TransferRequest>,
) -> ServerResult<(StatusCode, Json<ChangeResponse>)> {
    let (auth, bearer) = admin(&state, auth, bearer, (group_id, set_id)).await?;
    let role = match request.role {
        RoleName::Replica => TransferRole::Replica,
        RoleName::Leave => TransferRole::Leave,
    };
    let plan = (id(&request.range_id)?, id(&request.to)?, role);
    let revision = transfer_range(&state.get_ctx(), (&auth, bearer), set_id, plan)
        .await
        .map_err(map_create_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(change_response(set_id, revision)),
    ))
}

#[utoipa::path(
    put,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}/settings",
    tag = "data/storage",
    summary = "Change a distributed bucket's settings",
    description = r#"Replaces the bucket-wide settings of a distributed bucket.

**Authentication**: realm bearer token with WRITE on the group's admin path; the publisher checks the forwarded token again.

**Behavior**
- Omitted fields take their defaults; the whole settings object is replaced.
- Refused while the set is being deleted."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id")
    ),
    request_body(
        content = SettingsRequest,
        description = "The new settings",
        example = json!({ "refresh_secs": 3600, "public": true, "split_objects": 250000 })
    ),
    responses(
        (status = 200, description = "The revision that took the change", body = ChangeResponse),
        (status = 400, description = "Invalid settings", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change", body = ErrorResponse),
        (status = 503, description = "Configuration changes unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn settings_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, set_id)): Path<(Ulid, Ulid)>,
    Json(request): Json<SettingsRequest>,
) -> ServerResult<Json<ChangeResponse>> {
    let (auth, bearer) = admin(&state, auth, bearer, (group_id, set_id)).await?;
    let settings = settings_of(&request);
    let revision = change_settings(&state.get_ctx(), (&auth, bearer), set_id, settings)
        .await
        .map_err(map_create_error)?;
    Ok(Json(change_response(set_id, revision)))
}

#[utoipa::path(
    delete,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}",
    tag = "data/storage",
    summary = "Delete a distributed bucket",
    description = r#"Deletes a distributed bucket with every member bucket and its data.

**Authentication**: realm bearer token with WRITE on the group's admin path; the publisher checks the forwarded token again.

**Behavior**
- Writes stop at once. Every coordinator fences its ranges; once all have, the deletion commits
  and every member purges its local bucket range by range, then deletes it.
- A member bucket that was replaced in the meantime is left alone.
- Members that are offline purge when they return.
- The answer is 202 once the deletion is recorded; its progress shows in the bucket status.
  An unreachable coordinator keeps the deletion waiting for its fence."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id")
    ),
    responses(
        (status = 202, description = "The revision that recorded the deletion", body = ChangeResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change", body = ErrorResponse),
        (status = 503, description = "Configuration changes unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, set_id)): Path<(Ulid, Ulid)>,
) -> ServerResult<(StatusCode, Json<ChangeResponse>)> {
    let (auth, bearer) = admin(&state, auth, bearer, (group_id, set_id)).await?;
    let revision = delete_set(&state.get_ctx(), (&auth, bearer), set_id)
        .await
        .map_err(map_create_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(change_response(set_id, revision)),
    ))
}
