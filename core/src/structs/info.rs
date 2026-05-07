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
    pub observed_connections_total: u64,
    pub dropped_observations_total: u64,
    pub closed_connections_total: u64,
    pub close_task_errors_total: u64,
}

pub struct NetState {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bootstrap_nodes: Vec<NodeId>,
    pub endpoint_addr: EndpointAddr,
    pub monitor: ConnectionMonitorState,
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
