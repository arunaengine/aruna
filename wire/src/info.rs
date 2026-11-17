//! Node information as `GET /info` answers it, including the portal status.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::{
    ConnectionAddressStatus, PeerConnectionStatus, RequestSummaryState, Status,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Node information. `node.status`, `node.realm_id`, `node.peer_id` and `api_version` are
/// public; capabilities, addresses, peer topology, backend detail and warnings need a realm
/// token or config admin, and gated values are absent, never restructured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InfoResponse {
    pub node: NodeStatus,
    pub api_version: String,
    /// Portal deployment detail. Realm config admins only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub portal: Option<PortalStatus>,
    /// Node listen addresses. Realm-authenticated callers only, else empty.
    pub my_addresses: Vec<String>,
    /// Peer topology. Realm-authenticated callers only, else empty.
    pub connections: Vec<PeerConnectionInfo>,
    pub services: ServicesStatus,
    /// Operational warnings. Realm config admins only, else empty.
    pub warnings: Vec<String>,
}

/// Node health and identity. `status`, `realm_id` and `peer_id` are public, as the
/// realm node list is; `capabilities`, `started_at` and `uptime_secs` need a token of this realm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeStatus {
    pub status: ServiceStatus,
    pub realm_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<NodeCapabilityKind>,
    /// RFC 3339 time at which this node process started serving.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "2026-04-09T14:23:11Z")]
    pub started_at: Option<String>,
    /// Whole seconds since `started_at`, from a monotonic clock.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 86400)]
    pub uptime_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapabilityKind {
    Management,
    Server,
    User,
}

impl From<&NodeCapabilities> for NodeCapabilityKind {
    fn from(capabilities: &NodeCapabilities) -> Self {
        match capabilities {
            NodeCapabilities::Management { .. } => Self::Management,
            NodeCapabilities::Server { .. } => Self::Server,
            NodeCapabilities::User { .. } => Self::User,
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

impl From<Status> for ServiceStatus {
    fn from(status: Status) -> Self {
        match status {
            Status::Available => Self::Available,
            Status::NotConfigured => Self::NotConfigured,
            Status::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PeerConnectionInfo {
    pub peer_id: String,
    pub status: PeerStatus,
    pub active_addresses: Vec<ConnectionAddressInfo>,
    /// Populated for realm config admins only.
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

/// Node backend services. `interfaces` is always present; `network` needs a
/// token of this realm, `blob` and `database` a realm config admin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ServicesStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobServiceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseServiceStatus>,
    pub interfaces: InterfaceServicesStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NetworkServiceStatus {
    pub status: ServiceStatus,
    pub discovery: Vec<String>,
    pub relay: Option<String>,
    pub relay_urls: Vec<String>,
    pub routing_table_size: Option<usize>,
    /// Request metrics and last error. Realm config admins only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<RequestSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
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
    /// Every registered backend. The aggregate `status` above is the default
    /// backend's, so single-backend consumers keep one headline signal.
    pub backends: Vec<BackendStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BackendStatus {
    pub name: String,
    pub backend: String,
    pub class: Option<String>,
    /// Whether tenant routing rules may target this backend's class.
    pub allow_tenants: bool,
    /// Operator allowance for user data on this backend. A write routed here is
    /// refused once `used_bytes` reaches it.
    pub quota_bytes: Option<u64>,
    /// Stored user-data bytes on this backend, from the maintained counters.
    /// Absent when those counters could not be read.
    pub used_bytes: Option<u64>,
    pub default: bool,
    pub status: ServiceStatus,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp: Option<InterfaceStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InterfaceStatus {
    pub status: ServiceStatus,
    /// Local listen address. Realm-authenticated callers only.
    pub bind: Option<String>,
    pub url: Option<String>,
}

impl RequestSummary {
    pub fn from_state(state: &RequestSummaryState) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PortalStatus {
    pub installed: bool,
    pub mode: String,
    pub version: Option<String>,
    pub source: Option<String>,
    pub url: Option<String>,
    pub checksum: Option<String>,
    pub fetched_at: Option<String>,
    pub last_error: Option<String>,
}

impl Default for PortalStatus {
    fn default() -> Self {
        Self {
            installed: false,
            mode: "disabled".to_string(),
            version: None,
            source: None,
            url: None,
            checksum: None,
            fetched_at: None,
            last_error: None,
        }
    }
}
