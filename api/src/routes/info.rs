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
    pub node_info: LocalNodeInfo,
    pub net_state: NetStatus,
    pub blob_status: BlobStatus,
    pub interface_status: InterfaceStatus,
    pub database_status: DatabaseStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LocalNodeInfo {
    pub realm_id: String,
    pub node_id: String,
    pub capabilities: NodeCapabilityKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapabilityKind {
    Management,
    Server,
    Local,
}

impl From<&aruna_core::structs::NodeCapabilities> for NodeCapabilityKind {
    fn from(capabilities: &aruna_core::structs::NodeCapabilities) -> Self {
        match capabilities {
            aruna_core::structs::NodeCapabilities::Management { .. } => Self::Management,
            aruna_core::structs::NodeCapabilities::Server { .. } => Self::Server,
            aruna_core::structs::NodeCapabilities::Local { .. } => Self::Local,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    Available,
    NotConfigured,
    Unavailable,
}

impl From<aruna_core::structs::Status> for ServiceStatus {
    fn from(status: aruna_core::structs::Status) -> Self {
        match status {
            aruna_core::structs::Status::Available => Self::Available,
            aruna_core::structs::Status::NotConfigured => Self::NotConfigured,
            aruna_core::structs::Status::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NetStatus {
    pub status: ServiceStatus,
    pub realm_id: Option<String>,
    pub node_id: Option<String>,
    pub bootstrap_nodes: Vec<String>,
    pub endpoint_addr: Option<serde_json::Value>,
    pub monitor: ConnectionMonitorStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionMonitorStatus {
    pub open_connections: Vec<OpenConnection>,
    pub observed_connections_total: u64,
    pub dropped_observations_total: u64,
    pub closed_connections_total: u64,
    pub close_task_errors_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BlobStatus {
    pub status: ServiceStatus,
    pub backend_type: Option<String>,
    pub max_bucket_size: Option<u64>,
    pub multipart_bucket: Option<String>,
    pub timeouts: Option<TimeoutConfig>,
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
pub struct RestInterfaceStatus {
    pub status: ServiceStatus,
    pub bind_address: Option<String>,
    pub base_url: Option<String>,
    pub api_base_url: Option<String>,
    pub info_url: Option<String>,
    pub swagger_ui_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct S3InterfaceStatus {
    pub status: ServiceStatus,
    pub bind_address: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DatabaseStatus {
    pub status: ServiceStatus,
    pub requests_total: u64,
    pub errors_total: u64,
    pub conflicts_total: u64,
    pub failed_total: u64,
    pub error_rate: f64,
    pub channel_closed: bool,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OpenConnection {
    pub connection_id: u64,
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
            NetStatus {
                status: ServiceStatus::Available,
                realm_id: Some(info.realm_id.to_string()),
                node_id: Some(info.node_id.to_string()),
                bootstrap_nodes: info.bootstrap_nodes.iter().map(|n| n.to_string()).collect(),
                endpoint_addr: serde_json::to_value(&info.endpoint_addr).ok(),
                monitor: ConnectionMonitorStatus {
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
                    observed_connections_total: info.monitor.observed_connections_total,
                    dropped_observations_total: info.monitor.dropped_observations_total,
                    closed_connections_total: info.monitor.closed_connections_total,
                    close_task_errors_total: info.monitor.close_task_errors_total,
                },
            }
        }
        None => NetStatus {
            status: ServiceStatus::Unavailable,
            realm_id: None,
            node_id: None,
            bootstrap_nodes: Vec::new(),
            endpoint_addr: None,
            monitor: ConnectionMonitorStatus::default(),
        },
    };
    let blob_status = match &state.get_ctx().blob_handle {
        Some(blob) => {
            let info = blob.get_status().await;
            BlobStatus {
                status: ServiceStatus::from(info.status),
                backend_type: Some(info.backend_type.to_string()),
                max_bucket_size: info.max_bucket_size,
                multipart_bucket: info.multipart_bucket,
                timeouts: Some(TimeoutConfig {
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
                }),
            }
        }
        None => BlobStatus {
            status: ServiceStatus::NotConfigured,
            backend_type: None,
            max_bucket_size: None,
            multipart_bucket: None,
            timeouts: None,
        },
    };
    let interface_runtime = state.interface_state().await;
    let interface_status = InterfaceStatus {
        rest: match interface_runtime.rest {
            Some(rest) => RestInterfaceStatus {
                status: ServiceStatus::Available,
                bind_address: Some(rest.bind_address.to_string()),
                base_url: Some(rest.base_url),
                api_base_url: Some(rest.api_base_url),
                info_url: Some(rest.info_url),
                swagger_ui_url: Some(rest.swagger_ui_url),
            },
            None => RestInterfaceStatus {
                status: ServiceStatus::Unavailable,
                bind_address: None,
                base_url: None,
                api_base_url: None,
                info_url: None,
                swagger_ui_url: None,
            },
        },
        s3: match interface_runtime.s3 {
            Some(s3) => S3InterfaceStatus {
                status: ServiceStatus::Available,
                bind_address: Some(s3.bind_address.to_string()),
                base_url: Some(s3.base_url),
            },
            None => S3InterfaceStatus {
                status: ServiceStatus::Unavailable,
                bind_address: None,
                base_url: None,
            },
        },
    };
    let storage_metrics = state.get_ctx().storage_handle.snapshot_metrics();
    let database_status = DatabaseStatus {
        status: if storage_metrics.channel_closed {
            ServiceStatus::Unavailable
        } else {
            ServiceStatus::Available
        },
        requests_total: storage_metrics.requests_total,
        errors_total: storage_metrics.errors_total,
        conflicts_total: storage_metrics.conflicts_total,
        failed_total: storage_metrics.failed_total,
        error_rate: if storage_metrics.requests_total == 0 {
            0.0
        } else {
            storage_metrics.failed_total as f64 / storage_metrics.requests_total as f64
        },
        channel_closed: storage_metrics.channel_closed,
        last_error: storage_metrics.last_error,
    };
    (
        StatusCode::OK,
        Json(InfoResponse {
            node_info: LocalNodeInfo {
                realm_id: state.get_realm_id().to_string(),
                node_id: state.get_node_id().to_string(),
                capabilities: NodeCapabilityKind::from(state.node_capabilities()),
            },
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
        BlobStatus, ConnectionMonitorStatus, DatabaseStatus, InfoResponse, InterfaceStatus,
        LocalNodeInfo, NetStatus, NodeCapabilityKind, RestInterfaceStatus, S3InterfaceStatus,
        ServiceStatus, get_info,
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
        let expected_node_info = LocalNodeInfo {
            realm_id: state.get_realm_id().to_string(),
            node_id: state.get_node_id().to_string(),
            capabilities: NodeCapabilityKind::Local,
        };

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            InfoResponse {
                node_info: expected_node_info,
                net_state: NetStatus {
                    status: ServiceStatus::Unavailable,
                    realm_id: None,
                    node_id: None,
                    bootstrap_nodes: Vec::new(),
                    endpoint_addr: None,
                    monitor: ConnectionMonitorStatus::default(),
                },
                blob_status: BlobStatus {
                    status: ServiceStatus::NotConfigured,
                    backend_type: None,
                    max_bucket_size: None,
                    multipart_bucket: None,
                    timeouts: None,
                },
                interface_status: InterfaceStatus {
                    rest: RestInterfaceStatus {
                        status: ServiceStatus::Unavailable,
                        bind_address: None,
                        base_url: None,
                        api_base_url: None,
                        info_url: None,
                        swagger_ui_url: None,
                    },
                    s3: S3InterfaceStatus {
                        status: ServiceStatus::Unavailable,
                        bind_address: None,
                        base_url: None,
                    },
                },
                database_status: DatabaseStatus {
                    status: ServiceStatus::Available,
                    requests_total: baseline.requests_total,
                    errors_total: baseline.errors_total,
                    conflicts_total: baseline.conflicts_total,
                    failed_total: baseline.failed_total,
                    error_rate: if baseline.requests_total == 0 {
                        0.0
                    } else {
                        baseline.failed_total as f64 / baseline.requests_total as f64
                    },
                    channel_closed: baseline.channel_closed,
                    last_error: baseline.last_error,
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
                rest: RestInterfaceStatus {
                    status: ServiceStatus::Available,
                    bind_address: Some("0.0.0.0:3000".to_string()),
                    base_url: Some("http://127.0.0.1:3000".to_string()),
                    api_base_url: Some("http://127.0.0.1:3000/api/v1".to_string()),
                    info_url: Some("http://127.0.0.1:3000/api/v1/info".to_string()),
                    swagger_ui_url: Some("http://127.0.0.1:3000/swagger-ui".to_string()),
                },
                s3: S3InterfaceStatus {
                    status: ServiceStatus::Available,
                    bind_address: Some("0.0.0.0:1337".to_string()),
                    base_url: Some("http://localhost:1337".to_string()),
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
                status: ServiceStatus::Available,
                requests_total: baseline.requests_total + 1,
                errors_total: baseline.errors_total + 1,
                conflicts_total: baseline.conflicts_total,
                failed_total: baseline.failed_total + 1,
                error_rate: (baseline.failed_total + 1) as f64
                    / (baseline.requests_total + 1) as f64,
                channel_closed: false,
                last_error: Some("Transaction not found".to_string()),
            }
        );
    }

    #[test]
    fn openapi_includes_info_path() {
        let openapi = ApiDoc::openapi();

        assert!(openapi.paths.paths.contains_key("/info"));
    }
}
