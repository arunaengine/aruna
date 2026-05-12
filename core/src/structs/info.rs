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
    pub selected_address: Option<String>,
    pub rtt_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionMonitorState {
    pub open_connections: Vec<OpenConnection>,
}

#[derive(Debug, Clone, Default)]
pub struct RequestSummaryState {
    pub total: u64,
    pub failures: u64,
    pub last_error: Option<String>,
}

impl RequestSummaryState {
    pub fn record_success(&mut self) {
        self.total = self.total.saturating_add(1);
    }

    pub fn record_failure(&mut self, error: impl ToString) {
        self.total = self.total.saturating_add(1);
        self.failures = self.failures.saturating_add(1);
        self.last_error = Some(error.to_string());
    }
}

#[derive(Debug, Clone, Default)]
pub struct NetworkDiagnosticsState {
    pub requests: RequestSummaryState,
    pub routing_table_size: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerConnectionStatus {
    Connected,
    Known,
    Unreachable,
}

#[derive(Debug, Clone)]
pub struct ProtocolConnectionState {
    pub connection_id: u64,
    pub alpn: Option<Alpn>,
    pub side: iroh::endpoint::Side,
}

#[derive(Debug, Clone)]
pub struct ConnectionAddressState {
    pub address: String,
    pub rtt_ms: Option<u64>,
    pub protocol_connections: Vec<ProtocolConnectionState>,
}

#[derive(Debug, Clone)]
pub struct PeerConnectionState {
    pub node_id: NodeId,
    pub status: PeerConnectionStatus,
    pub active_addresses: Vec<ConnectionAddressState>,
    pub last_error: Option<String>,
    pub next_retry_in_secs: Option<u64>,
}

pub struct NetState {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub discovery_methods: Vec<String>,
    pub relay_method: String,
    pub relay_urls: Vec<String>,
    pub endpoint_addr: EndpointAddr,
    pub connections: Vec<PeerConnectionState>,
    pub requests: RequestSummaryState,
    pub routing_table_size: Option<usize>,
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
