use crate::server_state::ServerState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "info", description = "Node information endpoints")),
    paths(get_info)
)]
pub struct InfoApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/info", get(get_info))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InfoResponse {
    pub node_id: String,
}

#[utoipa::path(
    get,
    path = "/info",
    tag = "info",
    responses(
        (status = 200, description = "Node information", body = InfoResponse)
    )
)]
pub async fn get_info(State(state): State<Arc<ServerState>>) -> (StatusCode, Json<InfoResponse>) {
    (
        StatusCode::OK,
        Json(InfoResponse {
            node_id: state.get_node_id().to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::{InfoResponse, get_info};
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use axum::Json;
    use axum::extract::State;
    use axum::http::StatusCode;
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};

    async fn setup_state() -> (Arc<ServerState>, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
            )
            .await,
        );

        (state, tempdir)
    }

    #[tokio::test]
    async fn get_info_returns_local_node_id() {
        let (state, _tempdir) = setup_state().await;
        let expected_node_id = state.get_node_id().to_string();

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            InfoResponse {
                node_id: expected_node_id,
            }
        );
    }

    #[test]
    fn openapi_includes_info_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/info"));
    }
}
