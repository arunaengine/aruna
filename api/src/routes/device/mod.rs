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

use crate::server_state::ServerState;

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
