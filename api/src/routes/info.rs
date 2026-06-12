use crate::error::{ServerError, ServerResult};
pub use crate::server_state::PortalStatus;
use crate::server_state::ServerState;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{REALM_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::{ConnectionAddressStatus, PeerConnectionStatus, RequestSummaryState};
use aruna_core::structs::{Realm, RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::{USAGE_GLOBAL_KEY, UsageCounters};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::status::load_node_observability_status;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::warn;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "info", description = "Node information endpoints")),
    paths(get_info, get_realm_info, get_usage)
)]
pub struct InfoApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/info", get(get_info))
        .route("/info/realm", get(get_realm_info))
        .route("/info/usage", get(get_usage))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InfoResponse {
    pub node: NodeStatus,
    pub api_version: String,
    pub portal: PortalStatus,
    pub my_addresses: Vec<String>,
    pub connections: Vec<PeerConnectionInfo>,
    pub services: ServicesStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeStatus {
    pub status: ServiceStatus,
    pub realm_id: String,
    pub peer_id: String,
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
pub struct PeerConnectionInfo {
    pub peer_id: String,
    pub status: PeerStatus,
    pub active_addresses: Vec<ConnectionAddressInfo>,
    pub last_error: Option<String>,
    pub next_retry_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Connected,
    Known,
    Unreachable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionAddressInfo {
    pub status: AddressStatus,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rtt_ms: Option<u64>,
    pub protocol_connections: Vec<ProtocolConnectionInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AddressStatus {
    Active,
    NotAssigned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProtocolConnectionInfo {
    pub connection_id: u64,
    pub protocol: Option<String>,
    pub side: String,
    pub status: ProtocolConnectionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolConnectionStatus {
    Open,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ServicesStatus {
    pub network: NetworkServiceStatus,
    pub blob: BlobServiceStatus,
    pub database: DatabaseServiceStatus,
    pub interfaces: InterfaceServicesStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NetworkServiceStatus {
    pub status: ServiceStatus,
    pub discovery: Vec<String>,
    pub relay: Option<String>,
    pub relay_urls: Vec<String>,
    pub routing_table_size: Option<usize>,
    pub requests: RequestSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RequestSummary {
    pub total: u64,
    pub failure_rate: f64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BlobServiceStatus {
    pub status: ServiceStatus,
    pub backend: Option<String>,
    pub max_bucket_size: Option<u64>,
    pub multipart_bucket: Option<String>,
    pub timeouts_secs: Option<TimeoutConfigSecs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TimeoutConfigSecs {
    pub connect: u64,
    pub io: u64,
    pub transfer_idle: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DatabaseServiceStatus {
    pub status: ServiceStatus,
    pub requests: RequestSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceServicesStatus {
    pub rest: InterfaceStatus,
    pub s3: InterfaceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceStatus {
    pub status: ServiceStatus,
    pub bind: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RealmInfoResponse {
    pub realm_id: String,
    pub description: String,
    pub metadata_replication: RealmMetadataReplicationResponse,
    pub oidc_providers: Vec<RealmOidcProviderResponse>,
    #[schema(value_type = Object)]
    pub discovery: Value,
    pub nodes: Vec<RealmNodeInfoResponse>,
    pub interfaces: InterfaceServicesStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmMetadataReplicationResponse {
    pub default_replication_factor: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmOidcProviderResponse {
    pub id: String,
    pub issuer: String,
    pub audience: String,
    pub discovery_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmNodeInfoResponse {
    pub node_id: String,
    pub kind: RealmNodeKindInfo,
    pub configured: bool,
    pub present: bool,
    pub connection_status: RealmNodeConnectionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmNodeKindInfo {
    Management,
    Server,
    Local,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RealmNodeConnectionStatus {
    Connected,
    Configured,
}

impl From<&RealmNodeKind> for RealmNodeKindInfo {
    fn from(value: &RealmNodeKind) -> Self {
        match value {
            RealmNodeKind::Management => Self::Management,
            RealmNodeKind::Server => Self::Server,
            RealmNodeKind::Local => Self::Local,
            RealmNodeKind::User => Self::User,
        }
    }
}

#[utoipa::path(
    get,
    path = "/info/realm",
    tag = "info",
    responses(
        (status = 200, description = "Realm information", body = RealmInfoResponse),
        (status = 404, description = "Realm config not found", body = crate::error::ErrorResponse)
    )
)]
pub async fn get_realm_info(
    State(state): State<Arc<ServerState>>,
) -> ServerResult<(StatusCode, Json<RealmInfoResponse>)> {
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        aruna_operations::get_realm_config::GetRealmConfigError::DocumentNotFound => {
            ServerError::NotFound
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    let present_nodes = load_realm_presence_best_effort(&state).await;
    let response = map_realm_info_response(
        &state,
        config,
        present_nodes,
        interface_services_status(&state).await,
    )?;
    Ok((StatusCode::OK, Json(response)))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UsageResponse {
    pub buckets: u64,
    pub objects: u64,
    pub stored_blobs: u64,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
}

impl From<UsageCounters> for UsageResponse {
    fn from(counters: UsageCounters) -> Self {
        Self {
            buckets: counters.buckets,
            objects: counters.objects,
            stored_blobs: counters.stored_blobs,
            stored_bytes: counters.stored_bytes,
            logical_bytes: counters.logical_bytes,
        }
    }
}

pub async fn load_usage_counters(state: &ServerState, key: Vec<u8>) -> ServerResult<UsageCounters> {
    match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => UsageCounters::from_bytes(&bytes)
            .map_err(|error| ServerError::InternalError(error.to_string())),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Ok(UsageCounters::default())
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(ServerError::InternalError(error.to_string()))
        }
        other => Err(ServerError::InternalError(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

#[utoipa::path(
    get,
    path = "/info/usage",
    tag = "info",
    responses(
        (status = 200, description = "Aggregate storage usage on this node", body = UsageResponse)
    )
)]
pub async fn get_usage(
    State(state): State<Arc<ServerState>>,
) -> ServerResult<(StatusCode, Json<UsageResponse>)> {
    let counters = load_usage_counters(&state, USAGE_GLOBAL_KEY.to_vec()).await?;
    Ok((StatusCode::OK, Json(UsageResponse::from(counters))))
}

fn map_realm_info_response(
    state: &ServerState,
    config: RealmConfigDocument,
    present_nodes: HashSet<aruna_core::NodeId>,
    interfaces: InterfaceServicesStatus,
) -> ServerResult<RealmInfoResponse> {
    let discovery = serde_json::to_value(&config.discovery)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let current_node = state.get_node_id();
    let nodes = config
        .nodes
        .iter()
        .map(|node| {
            let is_current = node.node_id == current_node.to_string();
            let present = is_current
                || node
                    .node_id
                    .parse::<aruna_core::NodeId>()
                    .ok()
                    .is_some_and(|node_id| present_nodes.contains(&node_id));
            RealmNodeInfoResponse {
                node_id: node.node_id.clone(),
                kind: RealmNodeKindInfo::from(&node.kind),
                configured: true,
                present,
                connection_status: if present {
                    RealmNodeConnectionStatus::Connected
                } else {
                    RealmNodeConnectionStatus::Configured
                },
            }
        })
        .collect();

    Ok(RealmInfoResponse {
        realm_id: config.realm_id.to_string(),
        description: config.description,
        metadata_replication: RealmMetadataReplicationResponse {
            default_replication_factor: config.metadata_replication.default_replication_factor,
        },
        oidc_providers: config
            .oidc_providers
            .into_iter()
            .map(|provider| RealmOidcProviderResponse {
                id: provider.id,
                issuer: provider.issuer,
                audience: provider.audience,
                discovery_url: provider.discovery_url,
            })
            .collect(),
        discovery,
        nodes,
        interfaces,
    })
}

async fn load_realm_presence_best_effort(state: &ServerState) -> HashSet<aruna_core::NodeId> {
    match drive(
        GetRealmNodesOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(mut nodes) => {
            nodes.insert(state.get_node_id());
            nodes
        }
        Err(error) => {
            warn!(error = %error, "realm node discovery failed for realm info response");
            HashSet::from([state.get_node_id()])
        }
    }
}

async fn interface_services_status(state: &ServerState) -> InterfaceServicesStatus {
    let interface_runtime = state.interface_state().await;
    InterfaceServicesStatus {
        rest: match interface_runtime.rest {
            Some(rest) => InterfaceStatus {
                status: ServiceStatus::Available,
                bind: Some(rest.bind_address.to_string()),
                url: Some(rest.api_base_url),
            },
            None => InterfaceStatus {
                status: ServiceStatus::Unavailable,
                bind: None,
                url: None,
            },
        },
        s3: match interface_runtime.s3 {
            Some(s3) => InterfaceStatus {
                status: ServiceStatus::Available,
                bind: Some(s3.bind_address.to_string()),
                url: Some(s3.base_url),
            },
            None => InterfaceStatus {
                status: ServiceStatus::Unavailable,
                bind: None,
                url: None,
            },
        },
    }
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
    let ctx = state.get_ctx();
    let observability = load_node_observability_status(ctx.as_ref()).await;

    let (network, my_addresses, connections, warnings) = match observability.network {
        Some(info) => (
            NetworkServiceStatus {
                status: ServiceStatus::Available,
                discovery: info.discovery_methods,
                relay: Some(info.relay_method),
                relay_urls: info.relay_urls,
                routing_table_size: info.routing_table_size,
                requests: RequestSummary::from_state(&info.requests),
            },
            info.endpoint_addr
                .addrs
                .iter()
                .map(transport_addr_to_string)
                .collect(),
            info.connections
                .iter()
                .map(|peer| PeerConnectionInfo {
                    peer_id: peer.node_id.to_string(),
                    status: PeerStatus::from(peer.status),
                    active_addresses: peer
                        .active_addresses
                        .iter()
                        .map(|address| ConnectionAddressInfo {
                            status: AddressStatus::from(address.status),
                            address: address.address.clone(),
                            rtt_ms: address.rtt_ms,
                            protocol_connections: address
                                .protocol_connections
                                .iter()
                                .map(|connection| ProtocolConnectionInfo {
                                    connection_id: connection.connection_id,
                                    protocol: protocol_name(connection.alpn),
                                    side: side_name(connection.side),
                                    status: ProtocolConnectionStatus::Open,
                                })
                                .collect(),
                        })
                        .collect(),
                    last_error: peer.last_error.clone(),
                    next_retry_secs: peer.next_retry_in_secs,
                })
                .collect(),
            info.warnings,
        ),
        None => (
            NetworkServiceStatus {
                status: ServiceStatus::Unavailable,
                discovery: Vec::new(),
                relay: None,
                relay_urls: Vec::new(),
                routing_table_size: None,
                requests: RequestSummary::default(),
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
    };

    let blob = match observability.blob {
        Some(info) => BlobServiceStatus {
            status: ServiceStatus::from(info.status),
            backend: Some(info.backend_type.to_string()),
            max_bucket_size: info.max_bucket_size,
            multipart_bucket: info.multipart_bucket,
            timeouts_secs: Some(TimeoutConfigSecs {
                connect: info.timeouts.control_plane_connect_timeout.as_secs(),
                io: info.timeouts.control_plane_io_timeout.as_secs(),
                transfer_idle: info.timeouts.transfer_idle_timeout.as_secs(),
            }),
        },
        None => BlobServiceStatus {
            status: ServiceStatus::NotConfigured,
            backend: None,
            max_bucket_size: None,
            multipart_bucket: None,
            timeouts_secs: None,
        },
    };

    let interfaces = interface_services_status(&state).await;

    let database = DatabaseServiceStatus {
        status: ServiceStatus::from(observability.database.status),
        requests: RequestSummary::from_state(&observability.database.requests),
    };

    (
        StatusCode::OK,
        Json(InfoResponse {
            node: NodeStatus {
                status: ServiceStatus::Available,
                realm_id: state.get_realm_id().to_string(),
                peer_id: state.get_node_id().to_string(),
                capabilities: NodeCapabilityKind::from(state.node_capabilities()),
            },
            api_version: env!("CARGO_PKG_VERSION").to_string(),
            portal: state.portal_status().await,
            my_addresses,
            connections,
            services: ServicesStatus {
                network,
                blob,
                database,
                interfaces,
            },
            warnings,
        }),
    )
}

impl RequestSummary {
    fn default() -> Self {
        Self::from_counts(0, 0, None)
    }

    fn from_state(state: &RequestSummaryState) -> Self {
        Self::from_counts(state.total, state.failures, state.last_error.clone())
    }

    fn from_counts(total: u64, failures: u64, last_error: Option<String>) -> Self {
        Self {
            total,
            failure_rate: if total == 0 {
                0.0
            } else {
                failures as f64 / total as f64
            },
            last_error,
        }
    }
}

impl From<PeerConnectionStatus> for PeerStatus {
    fn from(status: PeerConnectionStatus) -> Self {
        match status {
            PeerConnectionStatus::Connected => Self::Connected,
            PeerConnectionStatus::Known => Self::Known,
            PeerConnectionStatus::Unreachable => Self::Unreachable,
        }
    }
}

impl From<ConnectionAddressStatus> for AddressStatus {
    fn from(status: ConnectionAddressStatus) -> Self {
        match status {
            ConnectionAddressStatus::Active => Self::Active,
            ConnectionAddressStatus::NotAssigned => Self::NotAssigned,
        }
    }
}

fn protocol_name(alpn: Option<Alpn>) -> Option<String> {
    alpn.map(|alpn| match alpn {
        Alpn::Dht => "dht".to_string(),
        Alpn::Bao => "bao".to_string(),
        Alpn::DocumentSync => "document_sync".to_string(),
        Alpn::Metadata => "metadata".to_string(),
    })
}

fn side_name(side: iroh::endpoint::Side) -> String {
    match side {
        iroh::endpoint::Side::Client => "client".to_string(),
        iroh::endpoint::Side::Server => "server".to_string(),
    }
}

fn transport_addr_to_string(addr: &iroh::TransportAddr) -> String {
    match addr {
        iroh::TransportAddr::Ip(addr) => addr.to_string(),
        iroh::TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlobServiceStatus, DatabaseServiceStatus, InfoResponse, InterfaceServicesStatus,
        InterfaceStatus, NetworkServiceStatus, NodeCapabilityKind, NodeStatus, PortalStatus,
        RequestSummary, ServiceStatus, ServicesStatus, get_info,
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
        let expected_node = NodeStatus {
            status: ServiceStatus::Available,
            realm_id: state.get_realm_id().to_string(),
            peer_id: state.get_node_id().to_string(),
            capabilities: NodeCapabilityKind::Local,
        };

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            InfoResponse {
                node: expected_node,
                api_version: env!("CARGO_PKG_VERSION").to_string(),
                portal: PortalStatus {
                    installed: false,
                    mode: "disabled".to_string(),
                    version: None,
                    source: None,
                    url: None,
                    checksum: None,
                    fetched_at: None,
                    last_error: None,
                },
                my_addresses: Vec::new(),
                connections: Vec::new(),
                services: ServicesStatus {
                    network: NetworkServiceStatus {
                        status: ServiceStatus::Unavailable,
                        discovery: Vec::new(),
                        relay: None,
                        relay_urls: Vec::new(),
                        routing_table_size: None,
                        requests: RequestSummary {
                            total: 0,
                            failure_rate: 0.0,
                            last_error: None,
                        },
                    },
                    blob: BlobServiceStatus {
                        status: ServiceStatus::NotConfigured,
                        backend: None,
                        max_bucket_size: None,
                        multipart_bucket: None,
                        timeouts_secs: None,
                    },
                    database: DatabaseServiceStatus {
                        status: ServiceStatus::Available,
                        requests: RequestSummary {
                            total: baseline.requests_total,
                            failure_rate: if baseline.requests_total == 0 {
                                0.0
                            } else {
                                baseline.failed_total as f64 / baseline.requests_total as f64
                            },
                            last_error: baseline.last_error,
                        },
                    },
                    interfaces: InterfaceServicesStatus {
                        rest: InterfaceStatus {
                            status: ServiceStatus::Unavailable,
                            bind: None,
                            url: None,
                        },
                        s3: InterfaceStatus {
                            status: ServiceStatus::Unavailable,
                            bind: None,
                            url: None,
                        },
                    },
                },
                warnings: Vec::new(),
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
            .register_s3_interface("0.0.0.0:1337".parse().unwrap(), "127.0.0.1:1337")
            .await;

        let (status, Json(response)) = get_info(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response.services.interfaces,
            InterfaceServicesStatus {
                rest: InterfaceStatus {
                    status: ServiceStatus::Available,
                    bind: Some("0.0.0.0:3000".to_string()),
                    url: Some("http://127.0.0.1:3000/api/v1".to_string()),
                },
                s3: InterfaceStatus {
                    status: ServiceStatus::Available,
                    bind: Some("0.0.0.0:1337".to_string()),
                    url: Some("http://127.0.0.1:1337".to_string()),
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
            response.services.database,
            DatabaseServiceStatus {
                status: ServiceStatus::Available,
                requests: RequestSummary {
                    total: baseline.requests_total + 1,
                    failure_rate: (baseline.failed_total + 1) as f64
                        / (baseline.requests_total + 1) as f64,
                    last_error: Some("Transaction not found".to_string()),
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
