//! Erasing this device and stopping the node.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::AuthContext;
use aruna_operations::device::wipe::{
    WIPED_EXIT_CODE, WipeDeviceConfig, WipeDeviceError, WipeDeviceOperation,
};
use aruna_operations::driver::drive;

use super::require_owner;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(wipe_device))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WipeDeviceRequest {
    /// This node's own id, typed back as confirmation.
    pub confirm_node_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct WipeDeviceResponse {
    pub node_id: String,
    /// Process exit status the supervisor sees once the wipe completes. A wipe
    /// that leaves paths behind exits with 80 instead.
    pub exit_code: i32,
}

#[utoipa::path(
    post,
    path = "/device/wipe",
    tag = "device",
    summary = "Erase this device and stop the node",
    description = r#"Erases everything this device stores locally and stops the node.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for. A node that is not a user node answers 404.

**Behavior**
- Realm-side eviction is a separate, earlier step. The desktop calls `DELETE /users/me/devices/{id}` on a management node first, so the realm drops the membership; this route only erases what the device holds.
- The node accepts the wipe, answers, then runs its ordinary shutdown, erases the contents of its storage roots including the persisted identity, and exits with status 79 so a supervisor can tell an erased device from a crash or an ordinary stop.
- Status 79 is claimed only when every root was emptied. A wipe that left paths behind logs them and exits with status 80: data may still be on disk, so the device must not be treated as erased.
- The storage roots themselves are kept, so a mounted volume stays mounted.
- Everything local is lost: queued drafts, blobs, credentials and the node identity. Re-enrolling mints a new node id.

**Limits**
- `confirm_node_id` must equal this node's own id.

**Errors**
- 400 when the confirmation names a different node."#,
    request_body(
        content = WipeDeviceRequest,
        description = "This node's own id, typed back as confirmation",
        example = json!({"confirm_node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga"})
    ),
    responses(
        (status = 202, description = "The wipe is accepted; the node shuts down, erases its roots and exits", body = WipeDeviceResponse,
            example = json!({
                "node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga",
                "exit_code": 79
            })),
        (status = 400, description = "The confirmation does not name this node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn wipe_device(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<WipeDeviceRequest>,
) -> ServerResult<(StatusCode, Json<WipeDeviceResponse>)> {
    require_owner(&state, auth).await?;
    let wipe = state.device_wipe().ok_or(ServerError::NotFound)?.clone();
    let node_id = drive(
        WipeDeviceOperation::new(WipeDeviceConfig {
            node_id: state.get_node_id(),
            confirm_node_id: request.confirm_node_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        WipeDeviceError::ConfirmationMismatch => ServerError::BadRequestReason(error.to_string()),
        other => ServerError::InternalError(other.to_string()),
    })?;
    wipe.arm();
    Ok((
        StatusCode::ACCEPTED,
        Json(WipeDeviceResponse {
            node_id: node_id.to_string(),
            exit_code: WIPED_EXIT_CODE,
        }),
    ))
}
