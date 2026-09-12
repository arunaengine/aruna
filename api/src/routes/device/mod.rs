//! Device-local plane of a user node. It is served only by a User-kind node
//! and only to the owner that node is bound to, so the desktop app and the
//! headless CLI share one authenticated surface.

pub mod documents;
pub mod drafts;
pub mod dto;
pub mod folders;
pub mod sync;
pub mod transfers;
pub mod wipe;

use std::sync::Arc;

use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;

use crate::auth::require_unrestricted_auth;
use crate::error::{ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, NodeCapabilities};
use aruna_operations::driver::drive;
use aruna_operations::realm::get_config::{GetRealmConfigError, GetRealmConfigOperation};

#[derive(OpenApi)]
#[openapi(tags((
    name = "device",
    description = "Owner-only controls a user node serves for the machine it runs on"
)))]
pub struct DeviceApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DeviceApiDoc::openapi())
        .merge(documents::router())
        .merge(drafts::router())
        .merge(folders::router())
        .merge(sync::router())
        .merge(transfers::router())
        .merge(wipe::router())
}

/// Reads the device owner from local replicated realm configuration.
/// This authorizes the device surface only for its bound user on a User-kind node.
/// Local reads keep the device plane available while the realm is unreachable.
pub(crate) async fn require_owner(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    if !matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(ServerError::NotFound);
    }
    let auth = require_unrestricted_auth(state, auth)?;
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        // A device that enrolled but has not received the configuration yet
        // cannot resolve its owner; that resolves on its own.
        GetRealmConfigError::DocumentNotFound => ServerError::ServiceUnavailableReason(
            "the realm configuration has not reached this device yet".to_string(),
        ),
        other => ServerError::InternalError(other.to_string()),
    })?;
    let node_id = state.get_node_id().to_string();
    let owner = config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .and_then(|node| node.kind.owner())
        .ok_or(ServerError::Forbidden)?;
    if owner != auth.user_id {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

#[cfg(test)]
mod tests {
    #[test]
    fn lists_device_routes() {
        let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
        for path in [
            "/device/documents",
            "/device/documents/{document_id}/selection",
            "/device/sync/status",
            "/device/sync/run",
            "/device/drafts",
            "/device/drafts/preview",
            "/device/drafts/{draft_id}",
            "/device/folders",
            "/device/folders/{folder_id}",
            "/device/folders/{folder_id}/entries",
            "/device/folders/{folder_id}/entries/{path}/actions",
            "/device/folders/{folder_id}/actions",
            "/device/folders/{folder_id}/pause",
            "/device/folders/{folder_id}/resume",
            "/device/folders/{folder_id}/sync",
            "/device/transfers",
            "/device/wipe",
        ] {
            assert!(openapi["paths"][path].is_object(), "{path} is undocumented");
        }
        // Offered directories became upload-only synced folders.
        assert!(openapi["paths"]["/device/offers"].is_null());
    }
}
