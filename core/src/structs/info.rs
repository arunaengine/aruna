use crate::NodeId;
use crate::alpn::Alpn;
use crate::structs::{Backend, BlobTimeoutConfig, RealmId};
use iroh::EndpointAddr;

#[derive(Debug, Clone)]
pub struct OpenConnection {
    pub connection_id: u64,
    pub alpn: Option<Alpn>,
    pub remote_id: iroh::EndpointId,
    pub side: iroh::endpoint::Side,
}

#[derive(Debug, Clone)]
pub struct ConnectionMonitorState {
    pub open_connections: Vec<OpenConnection>,
    pub outbound_connection_attempts_total: u64,
    pub observed_connections_total: u64,
    pub dropped_observations_total: u64,
    pub closed_connections_total: u64,
    pub close_task_errors_total: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BootstrapDiagnosticsState {
    pub attempts_total: u64,
    pub successes_total: u64,
    pub failures_total: u64,
    pub last_attempted_peer_count: usize,
    pub last_error: Option<String>,
    pub last_successful: bool,
    pub routing_table_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct KnownPeerAddressState {
    pub node_id: NodeId,
    pub source: String,
    pub endpoint_addr: Option<EndpointAddr>,
    pub addresses: Vec<String>,
    pub has_direct_ip: bool,
    pub has_relay: bool,
    pub active_addresses: usize,
    pub inactive_addresses: usize,
}

#[derive(Debug, Clone)]
pub struct PeerConnectivityState {
    pub node_id: NodeId,
    pub source: String,
    pub attempts_total: u64,
    pub successes_total: u64,
    pub failures_total: u64,
    pub consecutive_failures: u64,
    pub last_error: Option<String>,
    pub last_successful: bool,
    pub next_retry_in_secs: Option<u64>,
}

pub struct NetState {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub discovery_methods: Vec<String>,
    pub discovery_dns_origins: Vec<String>,
    pub relay_method: String,
    pub relay_urls: Vec<String>,
    pub bootstrap_nodes: Vec<NodeId>,
    pub bootstrap_endpoints: Vec<String>,
    pub endpoint_addr: EndpointAddr,
    pub monitor: ConnectionMonitorState,
    pub bootstrap: BootstrapDiagnosticsState,
    pub peer_connectivity: Vec<PeerConnectivityState>,
    pub known_peer_addresses: Vec<KnownPeerAddressState>,
    pub warnings: Vec<String>,
}

pub struct BlobState {
    pub backend_type: Backend,
    pub max_bucket_size: Option<u64>,
    pub multipart_bucket: Option<String>,
    pub timeouts: BlobTimeoutConfig,
    pub status: Status,
}

pub struct InterfaceState {
    pub s3_status: Status,
    pub rest_status: Status,
    pub db_status: Status,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Available,
    NotConfigured,
    Unavailable,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Status::Available => "Available",
                Status::NotConfigured => "NotConfigured",
                Status::Unavailable => "Unavailable",
            }
        )
    }
}
