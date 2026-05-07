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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InfoResponse {
    pub net_state: NetStatus,
    pub blob_status: BlobStatus,
    pub interface_status: InterfaceStatus,
    pub database_status: DatabaseStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum NetStatus {
    Available {
        realm_id: String,
        node_id: String,
        bootstrap_nodes: Vec<String>,
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
    pub rest: RestInterfaceStatus,
    pub s3: S3InterfaceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "status")]
pub enum RestInterfaceStatus {
    Available {
        bind_address: String,
        base_url: String,
        api_base_url: String,
        info_url: String,
        swagger_ui_url: String,
    },
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "status")]
pub enum S3InterfaceStatus {
    Available {
        bind_address: String,
        base_url: String,
    },
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DatabaseStatus {
    pub requests_total: u64,
    pub errors_total: u64,
    pub conflicts_total: u64,
    pub error_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OpenConnection {
    pub alpn: Option<String>,
    pub connection_id: u64,
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
                bootstrap_nodes: info.bootstrap_nodes.iter().map(|n| n.to_string()).collect(),
                endpoint_addr: serde_json::to_value(&info.endpoint_addr).unwrap(),
                open_connections: info
                    .monitor
                    .open_connections
                    .iter()
                    .map(|c| OpenConnection {
                        connection_id: c.connection_id,
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
    let interface_runtime = state.interface_state().await;
    let interface_status = InterfaceStatus {
        rest: match interface_runtime.rest {
            Some(rest) => RestInterfaceStatus::Available {
                bind_address: rest.bind_address.to_string(),
                base_url: rest.base_url,
                api_base_url: rest.api_base_url,
                info_url: rest.info_url,
                swagger_ui_url: rest.swagger_ui_url,
            },
            None => RestInterfaceStatus::Unavailable,
        },
        s3: match interface_runtime.s3 {
            Some(s3) => S3InterfaceStatus::Available {
                bind_address: s3.bind_address.to_string(),
                base_url: s3.base_url,
            },
            None => S3InterfaceStatus::Unavailable,
        },
    };
    let storage_metrics = state.get_ctx().storage_handle.snapshot_metrics();
    let database_status = DatabaseStatus {
        requests_total: storage_metrics.requests_total,
        errors_total: storage_metrics.errors_total,
        conflicts_total: storage_metrics.conflicts_total,
        error_rate: if storage_metrics.requests_total == 0 {
            0.0
        } else {
            storage_metrics.errors_total as f64 / storage_metrics.requests_total as f64
        },
    };
    (
        StatusCode::OK,
        Json(InfoResponse {
            net_state,
            blob_status,
            interface_status,
            database_status,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        BlobStatus, DatabaseStatus, InfoResponse, InterfaceStatus, NetStatus,
        RestInterfaceStatus, S3InterfaceStatus, get_info,
    };
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::effects::StorageEffect;
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
    async fn get_info_returns_unavailable_optional_statuses_when_handles_are_missing() {
        let (state, _tempdir) = setup_state().await;
        let baseline = state.get_ctx().storage_handle.snapshot_metrics();

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            InfoResponse {
                net_state: NetStatus::Unavailable,
                blob_status: BlobStatus::Unavailable,
                interface_status: InterfaceStatus {
                    rest: RestInterfaceStatus::Unavailable,
                    s3: S3InterfaceStatus::Unavailable,
                },
                database_status: DatabaseStatus {
                    requests_total: baseline.requests_total,
                    errors_total: baseline.errors_total,
                    conflicts_total: baseline.conflicts_total,
                    error_rate: if baseline.requests_total == 0 {
                        0.0
                    } else {
                        baseline.errors_total as f64 / baseline.requests_total as f64
                    },
                },
            }
        );
    }

    #[tokio::test]
    async fn get_info_reports_registered_interface_paths() {
        let (state, _tempdir) = setup_state().await;
        state
            .register_rest_interface("0.0.0.0:3000".parse().unwrap())
            .await;
        state
            .register_s3_interface("0.0.0.0:1337".parse().unwrap(), "localhost")
            .await;

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response.interface_status,
            InterfaceStatus {
                rest: RestInterfaceStatus::Available {
                    bind_address: "0.0.0.0:3000".to_string(),
                    base_url: "http://127.0.0.1:3000".to_string(),
                    api_base_url: "http://127.0.0.1:3000/api/v1".to_string(),
                    info_url: "http://127.0.0.1:3000/api/v1/info".to_string(),
                    swagger_ui_url: "http://127.0.0.1:3000/swagger-ui".to_string(),
                },
                s3: S3InterfaceStatus::Available {
                    bind_address: "0.0.0.0:1337".to_string(),
                    base_url: "http://localhost:1337".to_string(),
                },
            }
        );
    }

    #[tokio::test]
    async fn get_info_reports_storage_error_metrics() {
        let (state, _tempdir) = setup_state().await;
        let ctx = state.get_ctx();
        let baseline = ctx.storage_handle.snapshot_metrics();

        let _ = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "missing".to_string(),
                key: b"key".to_vec().into(),
                txn_id: Some(ulid::Ulid::new()),
            })
            .await;

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response.database_status,
            DatabaseStatus {
                requests_total: baseline.requests_total + 1,
                errors_total: baseline.errors_total + 1,
                conflicts_total: baseline.conflicts_total,
                error_rate: (baseline.errors_total + 1) as f64 / (baseline.requests_total + 1) as f64,
            }
        );
    }

    #[test]
    fn openapi_includes_info_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/info"));
    }
}
