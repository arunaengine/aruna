//! Sets or clears the display name and map place of a realm node, by a realm admin or a
//! device's owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ensure_permission, require_realm_auth, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::NodeId;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::NodeCoordinates;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_operations::driver::drive;
use aruna_operations::realm::node_name::{
    NameScope, NodeChange, SetNodeNameConfig, SetNodeNameError, SetNodeNameOperation,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// A point on a map in degrees: latitude -90 to 90, longitude -180 to 180, kept to 7 decimals.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(as = NodeCoordinates)]
pub struct CoordinatesBody {
    pub lat: f64,
    pub lon: f64,
}

impl From<NodeCoordinates> for CoordinatesBody {
    fn from(coordinates: NodeCoordinates) -> Self {
        let (lat, lon) = coordinates.degrees();
        Self { lat, lon }
    }
}

/// What a node shows people. An omitted field keeps its value and `null` clears it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NodeNameRequest {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>)]
    pub display_name: Option<Option<String>>,
    /// A readable place name for maps, such as `Giessen`.
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<String>)]
    pub place: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<CoordinatesBody>)]
    pub coordinates: Option<Option<CoordinatesBody>>,
}

/// A field present in the body, `null` included, as `Some`; an omitted one stays `None`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

/// What the realm stores for the node after the change; `null` for a field that is not set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NodeNameResponse {
    pub node_id: String,
    pub display_name: Option<String>,
    pub place: Option<String>,
    pub coordinates: Option<CoordinatesBody>,
}

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(name_realm_node))
        .routes(routes!(name_own_device))
}

#[utoipa::path(
    patch,
    path = "/system/realm/nodes/{node_id}",
    tag = "system/realm",
    summary = "Set a node's name and map place",
    description = r#"Sets or clears the human readable name and map place of a management, server or device node.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The fields are stored in the replicated realm configuration, so every realm node reports them
  as `display_name`, `place` and `coordinates` in `GET /system/realm`. They are durable here when
  the response is sent and reach the other nodes asynchronously.
- They are for people only, such as a map of the realm. The node id, the node's labels and
  placement location, `distinct_locations`, routing and policies stay unchanged.
- An omitted field keeps its value and `null` clears it. Texts are trimmed. Coordinates are kept
  to 7 decimals.

**Limits**
- `display_name` and `place` must be non-empty after trimming, at most 64 characters, and free of
  control characters.
- `coordinates` need a finite `lat` from -90 to 90 and `lon` from -180 to 180.
- A body that names no field is refused."#,
    params(("node_id" = String, Path, description = "Node id of the realm node, as the realm configuration lists it")),
    request_body(
        content = NodeNameRequest,
        examples(
            ("Set a name" = (value = json!({"display_name": "Giessen storage 1"}))),
            ("Place on a map" = (value = json!({"place": "Giessen", "coordinates": {"lat": 50.5841, "lon": 8.6784}}))),
            ("Clear the name" = (value = json!({"display_name": null})))
        )
    ),
    responses(
        (status = 200, description = "The name and map place stored after the change", body = NodeNameResponse, example = json!({"node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978", "display_name": "Giessen storage 1", "place": "Giessen", "coordinates": {"lat": 50.5841, "lon": 8.6784}})),
        (status = 400, description = "Malformed body, a body naming no field, a name or place that is empty, longer than 64 characters or holds control characters, or coordinates out of range", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "No realm node carries this node id, or this node holds no realm configuration", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn name_realm_node(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(node_id): Path<String>,
    request: Result<Json<NodeNameRequest>, JsonRejection>,
) -> ServerResult<(StatusCode, Json<NodeNameResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    // Request policies live at this boundary; the operation only checks roles.
    let path = policy_admin_path(state.get_realm_id());
    ensure_permission(&state, &auth, path, Permission::WRITE).await?;
    set_name(&state, auth, &node_id, request, NameScope::RealmAdmin).await
}

#[utoipa::path(
    patch,
    path = "/access/users/me/devices/{id}",
    tag = "access/users",
    summary = "Name one of the calling user's devices",
    description = r#"Sets or clears the human readable name and map place of an enrolled device the calling user owns.

**Authentication**: realm bearer token without path restrictions. Only a device owned by the caller
can be named; any other node answers 404 rather than admitting it exists. A management node serves
the call and every other node relays it to one.

**Behavior**
- `id` is the node id `GET /access/users/me/devices` reported for an enrolled device. An enrollment
  still in flight has no node yet and cannot be named.
- The fields are the same realm-wide `display_name`, `place` and `coordinates` a realm admin sets
  through `PATCH /system/realm/nodes/{node_id}`; the later change wins.
- An omitted field keeps its value and `null` clears it. Texts are trimmed. Coordinates are kept
  to 7 decimals.

**Limits**
- `display_name` and `place` must be non-empty after trimming, at most 64 characters, and free of
  control characters.
- `coordinates` need a finite `lat` from -90 to 90 and `lon` from -180 to 180.
- A body that names no field is refused."#,
    params(("id" = String, Path, description = "Node id of the enrolled device, as reported by `GET /access/users/me/devices`")),
    request_body(
        content = NodeNameRequest,
        examples(("Name a laptop" = (value = json!({"display_name": "Lab laptop"}))))
    ),
    responses(
        (status = 200, description = "The name and map place stored after the change", body = NodeNameResponse, example = json!({"node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978", "display_name": "Lab laptop", "place": null, "coordinates": null})),
        (status = 400, description = "Malformed body, a body naming no field, a name or place that is empty, longer than 64 characters or holds control characters, or coordinates out of range", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No enrolled device of the calling user carries this id", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn name_own_device(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    request: Result<Json<NodeNameRequest>, JsonRejection>,
) -> ServerResult<(StatusCode, Json<NodeNameResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    set_name(&state, auth, &id, request, NameScope::Owner).await
}

async fn set_name(
    state: &ServerState,
    auth: AuthContext,
    node_id: &str,
    request: NodeNameRequest,
    scope: NameScope,
) -> ServerResult<(StatusCode, Json<NodeNameResponse>)> {
    let node_id = NodeId::from_str(node_id).map_err(|_| ServerError::NotFound)?;
    let coordinates = match request.coordinates {
        Some(Some(body)) => Some(Some(
            NodeCoordinates::from_degrees(body.lat, body.lon).ok_or_else(|| {
                ServerError::BadRequestReason(SetNodeNameError::InvalidCoordinates.to_string())
            })?,
        )),
        Some(None) => Some(None),
        None => None,
    };
    let change = NodeChange {
        display_name: request.display_name,
        place: request.place,
        coordinates,
    };
    let config = SetNodeNameConfig {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        auth_context: auth,
        node_id,
        change,
        scope,
    };
    let shown = drive(SetNodeNameOperation::new(config), &state.get_ctx())
        .await
        .map_err(map_name_error)?;
    let place = shown.place.unwrap_or_default();
    Ok((
        StatusCode::OK,
        Json(NodeNameResponse {
            node_id: node_id.to_string(),
            display_name: shown.name,
            place: place.name,
            coordinates: place.coordinates.map(CoordinatesBody::from),
        }),
    ))
}

fn map_name_error(error: SetNodeNameError) -> ServerError {
    match error {
        error @ (SetNodeNameError::InvalidName
        | SetNodeNameError::InvalidCoordinates
        | SetNodeNameError::EmptyChange) => ServerError::BadRequestReason(error.to_string()),
        SetNodeNameError::UnknownNode { .. } | SetNodeNameError::ConfigMissing => {
            ServerError::NotFound
        }
        SetNodeNameError::Unauthorized | SetNodeNameError::NotManagementNode => {
            ServerError::Forbidden
        }
        SetNodeNameError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm configuration update; retry".to_string())
        }
        SetNodeNameError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}
