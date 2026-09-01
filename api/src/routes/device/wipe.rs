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
    WIPE_INCOMPLETE_EXIT_CODE, WIPED_EXIT_CODE, WipeDeviceConfig, WipeDeviceError,
    WipeDeviceOperation,
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
    /// Why this wipe cannot erase everything this node stores, when it cannot.
    /// It names the configured backends whose bytes stay where they are.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
}

#[utoipa::path(
    post,
    path = "/device/wipe",
    tag = "device",
    summary = "Erase this device and stop the node",
    description = r#"Erases everything this device stores locally and stops the node.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- Realm-side eviction is a separate, earlier step: the desktop calls `DELETE /users/me/devices/{id}`
  on a management node so the realm drops the membership. This route only erases what the device
  holds.
- The node answers first, then runs its ordinary shutdown, erases the contents of its storage roots
  including the persisted identity, and exits 79 so a supervisor can tell an erased device from a
  crash or an ordinary stop.
- Exit 79 is claimed only when every root was emptied. A wipe that leaves paths behind logs them and
  exits 80: data may still be on disk, so the device must not be treated as erased.
- Every configured node-local filesystem backend is erased, including one relocated outside the
  store root. A backend this process cannot erase, such as object storage, is known before the wipe
  runs: the answer carries `incomplete_reason` and `exit_code` 80.
- The storage roots themselves are kept, so a mounted volume stays mounted.
- Everything local is lost, including queued drafts, blobs and credentials. Re-enrolling mints a new
  node id.

**Limits**
- `confirm_node_id` must equal this node's own id."#,
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
    // A backend this process cannot erase is known before the wipe runs, so the
    // owner is told now rather than left to read a successful status.
    let incomplete_reason = (!wipe.unsupported().is_empty()).then(|| {
        format!(
            "this device stores data on backends it cannot erase: {}",
            wipe.unsupported().join(", ")
        )
    });
    let exit_code = match incomplete_reason {
        Some(_) => WIPE_INCOMPLETE_EXIT_CODE,
        None => WIPED_EXIT_CODE,
    };
    Ok((
        StatusCode::ACCEPTED,
        Json(WipeDeviceResponse {
            node_id: node_id.to_string(),
            exit_code,
            incomplete_reason,
        }),
    ))
}
