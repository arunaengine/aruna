//! Serves the owner route that creates a bucket on a realm node without binding a folder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{parse_group_id, require_owner};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::id::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::group_backend::valid_bucket;
use aruna_operations::device::sync::folders::{RealmBucketInput, create_realm_bucket};

use super::folders::map_folder_error;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(create_device_bucket))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateBucketRequest {
    /// Bucket name: 3 to 63 lowercase letters, digits, dots or hyphens.
    pub name: String,
    /// Group the bucket belongs to, as a ULID.
    pub group_id: String,
    /// Realm node that creates and holds the bucket.
    pub node_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreatedBucket {
    pub name: String,
    pub node_id: String,
    pub group_id: String,
}

#[utoipa::path(
    post,
    path = "/device/buckets",
    tag = "device",
    summary = "Create a bucket on a realm node",
    description = r#"Creates an empty bucket on one realm node without binding a folder to it.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for. The realm node checks the owner's WRITE on the bucket in `group_id`, exactly as for a folder
bind with `create_bucket`.

**Behavior**
- The device forwards the creation through the node plane; the realm node creates and holds the
  bucket.
- A bucket of that name the node already holds is refused with 409 and code `bucket_exists`, also
  when it belongs to another group. The node decides this inside its create transaction, so of two
  concurrent calls for one name exactly one creates the bucket.
- This computer and nodes that are not realm servers cannot hold the bucket."#,
    request_body(
        content = CreateBucketRequest,
        description = "Bucket name, its group and the realm node that holds it",
        example = json!({
            "name": "lab-data",
            "group_id": "01JGROUP0123456789ABCDEFGH",
            "node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga"
        })
    ),
    responses(
        (status = 201, description = "The realm node created the bucket", body = CreatedBucket,
            example = json!({
                "name": "lab-data",
                "node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga",
                "group_id": "01JGROUP0123456789ABCDEFGH"
            })),
        (status = 400, description = "Malformed ids, an invalid bucket name, or a node that is not a realm server", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not this device's owner, or the realm node refuses the owner", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 409, description = "The node already holds a bucket of that name", body = ErrorResponse),
        (status = 502, description = "The realm node is unreachable", body = ErrorResponse),
        (status = 503, description = "The device or the realm configuration is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub(super) async fn create_device_bucket(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateBucketRequest>,
) -> ServerResult<(StatusCode, Json<CreatedBucket>)> {
    let auth = require_owner(&state, auth).await?;
    let group_id = parse_group_id(&request.group_id)?;
    let node_id = NodeId::from_str(&request.node_id)
        .map_err(|_| ServerError::BadRequestReason("node_id is not a node id".to_string()))?;
    if !valid_bucket(&request.name) {
        return Err(ServerError::BadRequestReason(
            "a bucket name has 3 to 63 lowercase letters, digits, dots or hyphens and starts and \
             ends with a letter or digit"
                .to_string(),
        ));
    }
    create_realm_bucket(
        &state.get_ctx(),
        RealmBucketInput {
            bucket: request.name.clone(),
            group_id,
            node_id,
            realm_id: state.get_realm_id(),
            user_id: auth.user_id,
        },
    )
    .await
    .map_err(map_folder_error)?;
    Ok((
        StatusCode::CREATED,
        Json(CreatedBucket {
            name: request.name,
            node_id: node_id.to_string(),
            group_id: group_id.to_string(),
        }),
    ))
}
