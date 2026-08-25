//! The compute plane a user device runs for its owner.

use std::sync::Arc;

use axum::extract::State;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use aruna_core::structs::AuthContext;
use aruna_operations::device::compute::{ComputeStatus, compute_status};

use crate::error::{ErrorResponse, ServerResult};
use crate::routes::device::require_owner;
use crate::routes::jobs::map_local_error;
use crate::server_state::ServerState;

/// The device tag itself is declared by the device plane's own document.
#[derive(OpenApi)]
#[openapi()]
pub struct DeviceComputeApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DeviceComputeApiDoc::openapi()).routes(routes!(get_device_compute))
}

/// The local executor and the owner's runs on it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceComputeResponse {
    /// Whether this device has an executor backend at all.
    pub enabled: bool,
    /// Wire kind of the backend a local run would use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    /// Whether that backend answered its health probe just now.
    pub healthy: bool,
    /// A paused plane accepts no local run.
    pub paused: bool,
    pub limits: DeviceComputeLimits,
    pub running: u32,
    pub queued: u32,
    /// Why a local run would be refused right now, if it would be.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// The owner's configured ceilings. An absent dimension is unmeasured, never
/// zero, and bounds nothing.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceComputeLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpu_cores: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ram_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_disk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
}

impl From<ComputeStatus> for DeviceComputeResponse {
    fn from(status: ComputeStatus) -> Self {
        Self {
            enabled: status.enabled,
            backend: status.backend,
            healthy: status.healthy,
            paused: status.paused,
            limits: DeviceComputeLimits {
                max_cpu_cores: status.limits.max_cpu_cores,
                max_ram_bytes: status.limits.max_ram_bytes,
                max_disk_bytes: status.limits.max_disk_bytes,
                max_concurrent: status.limits.max_concurrent,
            },
            running: status.running,
            queued: status.queued,
            message: status.message,
        }
    }
}

#[utoipa::path(
    get,
    path = "/device/compute",
    tag = "device",
    summary = "Report this device's compute plane",
    description = r#"Describes the executor this machine runs the owner's jobs on.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- `enabled` reflects the executor the desktop app configured, and `healthy` is probed on the spot,
  so a stopped container daemon reads as enabled but unhealthy.
- `paused` is the device's compute drain: while it is set, a local submission is refused.
- `message` names why a local run would be refused right now: the pause, a missing backend, or the
  health probe's own reason. It is absent when a run would be accepted.
- `limits` are the owner's configured ceilings; an absent dimension is unmeasured and bounds
  nothing, while `max_concurrent` also refuses a submission beyond it.
- `running` and `queued` count the owner's unfinished runs on this device from its durable job
  records, so they survive a restart. Both come from one bounded scan and are counted, not paged.
- The runs themselves are listed by this device's own `GET /jobs/`; there is no separate listing.
- Nothing here is realm state: the plane is never advertised and the realm never dispatches to it."#,
    responses(
        (status = 200, description = "The device's compute plane", body = DeviceComputeResponse,
            example = json!({
                "enabled": true,
                "backend": "docker",
                "healthy": true,
                "paused": false,
                "limits": {"max_cpu_cores": 4, "max_ram_bytes": 8589934592_i64, "max_concurrent": 1},
                "running": 1,
                "queued": 0
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_device_compute(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<DeviceComputeResponse>> {
    let auth = require_owner(&state, auth).await?;
    let status = compute_status(&state.get_ctx(), auth.user_id)
        .await
        .map_err(map_local_error)?;
    Ok(Json(status.into()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn openapi_lists_route() {
        let openapi = crate::openapi::ApiDoc::openapi();
        assert!(openapi.paths.paths.contains_key("/device/compute"));
    }
}
