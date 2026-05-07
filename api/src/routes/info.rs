use crate::server_state::ServerState;
use aruna_core::structs::Status;
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
    pub net_state: NetStatus,
    pub blob_status: BlobStatus,
    pub interface_status: InterfaceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum NetStatus {
    Available {
        realm_id: String,
        node_id: String,
        boostrap_nodes: Vec<String>,
        endpoint_addr: serde_json::Value,
        open_connections: Vec<OpenConnection>,
    },
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum BlobStatus {
    Available {
        backend_type: String,
        max_bucket_size: Option<u64>,
        multipart_bucket: Option<String>,
        timeouts: TimeoutConfig,
        status: String,
    },
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TimeoutConfig {
    pub control_plane_connect_timeout: String,
    pub control_plane_io_timeout: String,
    pub transfer_idle_timeout: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceStatus {
    pub s3_status: String,
    pub rest_status: String,
    pub db_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OpenConnection {
    pub alpn: Option<String>,
    pub remote_id: String,
    pub side: String,
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
    let net_state = match &state.get_ctx().net_handle {
        Some(net) => {
            let info = net.get_status().await;
            NetStatus::Available {
                realm_id: info.realm_id.to_string(),
                node_id: info.node_id.to_string(),
                boostrap_nodes: info.boostrap_nodes.iter().map(|n| n.to_string()).collect(),
                endpoint_addr: serde_json::to_value(&info.endpoint_addr).unwrap(),
                open_connections: info
                    .open_connections
                    .iter()
                    .map(|c| OpenConnection {
                        alpn: c.alpn.map(|a| a.to_string()),
                        remote_id: c.remote_id.to_string(),
                        side: match c.side {
                            iroh::endpoint::Side::Client => "Client".to_string(),
                            iroh::endpoint::Side::Server => "Server".to_string(),
                        },
                    })
                    .collect(),
            }
        }
        None => NetStatus::Unavailable,
    };
    let blob_status = match &state.get_ctx().blob_handle {
        Some(blob) => {
            let info = blob.get_status().await;
            BlobStatus::Available {
                backend_type: info.backend_type.to_string(),
                max_bucket_size: info.max_bucket_size,
                multipart_bucket: info.multipart_bucket,
                timeouts: TimeoutConfig {
                    control_plane_connect_timeout: format!(
                        "{}s",
                        info.timeouts.control_plane_connect_timeout.as_secs()
                    ),
                    control_plane_io_timeout: format!(
                        "{}s",
                        info.timeouts.control_plane_io_timeout.as_secs()
                    ),
                    transfer_idle_timeout: format!(
                        "{}s",
                        info.timeouts.transfer_idle_timeout.as_secs()
                    ),
                },
                status: info.status.to_string(),
            }
        }
        None => BlobStatus::Unavailable,
    };
    let interface_status = InterfaceStatus {
        s3_status: Status::Available.to_string(),
        rest_status: Status::Available.to_string(),
        db_status: Status::Available.to_string(),
    };
    (
        StatusCode::OK,
        Json(InfoResponse {
            net_state,
            blob_status,
            interface_status,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::{BlobStatus, InfoResponse, InterfaceStatus, NetStatus, get_info};
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::structs::{NodeCapabilities, RealmId, Status};
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
    async fn get_info_returns_unavailable_optional_statuses_when_handles_are_missing() {
        let (state, _tempdir) = setup_state().await;

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            InfoResponse {
                net_state: NetStatus::Unavailable,
                blob_status: BlobStatus::Unavailable,
                interface_status: InterfaceStatus {
                    s3_status: Status::Available.to_string(),
                    rest_status: Status::Available.to_string(),
                    db_status: Status::Available.to_string(),
                },
            }
        );
    }

    #[test]
    fn openapi_includes_info_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/info"));
    }
}
