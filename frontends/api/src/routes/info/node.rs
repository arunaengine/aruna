//! Serves the node info route and maps realm nodes, peers and services.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::realm::map_node_document;
use super::{InfoAccess, info_access};
use crate::routes::node_names::CoordinatesBody;
use crate::server::state::ServerState;
use aruna_core::alpn::Alpn;
use aruna_core::heartbeat::{HeldHeartbeat, MAX_HEARTBEAT_AGE, seen_recently};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::info::{format_transport_addr, side_name};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_ops_identity::groups::routing::UsageOperation;
use aruna_ops_identity::realm::get_nodes::{
    GetNodesOperation, REALM_DISCOVERY_TIMEOUT, RealmPresence,
};
use aruna_ops_identity::realm::node_name::NodeDisplays;
use aruna_runner::driver::drive;
use aruna_wire::info::{
    AddressStatus, BackendStatus, BlobServiceStatus, ConnectionAddressInfo, DatabaseServiceStatus,
    InfoResponse, InterfaceServicesStatus, InterfaceStatus, NetworkServiceStatus,
    NodeCapabilityKind, NodeStatus, PeerConnectionInfo, PeerStatus, ProtocolConnectionInfo,
    ProtocolConnectionStatus, RequestSummary, ServiceStatus, ServicesStatus, TimeoutConfigSecs,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeInfoResponse)]
pub struct NodeInfoResponse {
    pub node_id: String,
    pub kind: NodeKindInfo,
    /// Owner of a `user` node; null for infrastructure nodes.
    pub owner: Option<String>,
    pub configured: bool,
    pub present: bool,
    pub connection_status: RealmConnectionStatus,
    /// When a `user` node last reached this node, in unix milliseconds. This
    /// node's own observation; absent for other kinds and for a device it has
    /// not seen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    /// Realm-administered display name; absent when unset. A label only, never an identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Readable place name for maps, such as `Giessen`; absent when unset. Presentation only,
    /// unlike the placement `location`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub place: Option<String>,
    /// Map position of the node; absent when unset. Presentation only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<CoordinatesBody>,
    /// Placement map entry (location/weight/status) when the node is mapped.
    pub placement: Option<NodePlacementResponse>,
    /// Latest published node info document (capabilities/labels/urls/utilization) if received.
    pub info: Option<NodeDocumentResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodePlacementResponse)]
pub struct NodePlacementResponse {
    pub location: String,
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeInfoDocumentResponse)]
pub struct NodeDocumentResponse {
    pub executors: Vec<ExecutorCapabilityResponse>,
    pub labels: std::collections::BTreeMap<String, String>,
    pub urls: RealmUrlsResponse,
    /// Live values of the held heartbeat, or those of the last published document without one.
    pub utilization: RealmUtilizationResponse,
    pub telemetry: TelemetryState,
    /// Age of the held heartbeat on this node's clock; absent without one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telemetry_age_ms: Option<u64>,
    pub updated_at_ms: u64,
}

/// Where the live telemetry of a node comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = NodeTelemetryState)]
pub enum TelemetryState {
    /// A heartbeat at most three heartbeat intervals old.
    Fresh,
    /// An older heartbeat: the node may be down or unreachable.
    Stale,
    /// No heartbeat held since this node started: values of the last published document.
    Absent,
}

impl TelemetryState {
    pub(crate) fn of(held: Option<&HeldHeartbeat>) -> Self {
        match held {
            None => Self::Absent,
            Some(held) if u128::from(held.age_ms) <= MAX_HEARTBEAT_AGE.as_millis() => Self::Fresh,
            Some(_) => Self::Stale,
        }
    }
}

/// One compute backend the node offers. Read-only: only node or realm admins configure backends,
/// at node start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutorCapabilityResponse {
    /// The backend's node-local name; `default` is the one built from the environment.
    pub backend: String,
    pub kind: String,
    pub file_staging: bool,
    pub direct_s3: bool,
    /// Where the backend's workers run; empty when the backend cannot prove it.
    pub location: String,
    pub labels: std::collections::BTreeMap<String, String>,
    #[schema(inline)]
    pub limits: BackendLimits,
    /// The longest run the operator allows here; absent is unbounded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_walltime_ms: Option<u64>,
    /// What the operator declared the backend offers.
    #[schema(inline)]
    pub capabilities: BackendCapabilities,
    /// How well the backend fits each workload kind: `refused`, `possible`, `good` or `best`.
    #[schema(inline)]
    pub suitability: BackendSuitability,
    /// How many groups the backend is limited to; 0 means open to every group.
    pub group_count: u32,
    /// Advances whenever the backend's settings change on its node.
    pub config_generation: u64,
}

/// The backend's resource ceilings. An absent dimension is unmeasured and bounds nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BackendLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_cpu_cores: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ram_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_disk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BackendCapabilities {
    pub gpus: u32,
    pub architectures: Vec<String>,
    pub stdin: bool,
    pub multi_container: bool,
    pub services: bool,
    pub ingress: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BackendSuitability {
    pub batch: String,
    pub sessions: String,
    pub services: String,
}

/// One advertised backend as the node document shows it; group ids are never shown.
pub(super) fn executor_response(
    executor: &aruna_core::compute::ExecutorCapability,
) -> ExecutorCapabilityResponse {
    let name = |value: aruna_core::compute::backend::Suitability| {
        serde_json::to_value(value)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_default()
    };
    let declared = &executor.declared;
    ExecutorCapabilityResponse {
        backend: executor.backend.clone(),
        kind: executor.kind.clone(),
        file_staging: executor.file_staging,
        direct_s3: executor.direct_s3,
        location: executor.subject.location.clone(),
        labels: executor.subject.labels.clone(),
        limits: BackendLimits {
            max_cpu_cores: executor.limits.max_cpu_cores,
            max_ram_bytes: executor.limits.max_ram_bytes,
            max_disk_bytes: executor.limits.max_disk_bytes,
            max_concurrent: executor.limits.max_concurrent,
        },
        max_walltime_ms: executor.max_walltime_ms,
        capabilities: BackendCapabilities {
            gpus: declared.gpus,
            architectures: declared.architectures.clone(),
            stdin: declared.stdin,
            multi_container: declared.multi_container,
            services: declared.services,
            ingress: declared.ingress,
        },
        suitability: BackendSuitability {
            batch: name(executor.suitability.batch),
            sessions: name(executor.suitability.sessions),
            services: name(executor.suitability.services),
        },
        group_count: executor.groups.len() as u32,
        config_generation: executor.config_generation,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeUrlsResponse)]
pub struct RealmUrlsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeUtilizationResponse)]
pub struct RealmUtilizationResponse {
    pub storage_bytes_used: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents_held: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_permille: Option<u32>,
    pub heartbeat_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_slots: Option<DownloadSlotsResponse>,
    /// Background jobs the node runs right now, from its heartbeat; absent without one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jobs_running: Option<u32>,
    /// The staging imports among `jobs_running`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imports_running: Option<u32>,
}

/// DRS download slot use from the node's heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmNodeDownloadSlots)]
pub struct DownloadSlotsResponse {
    pub active: u32,
    pub max: u32,
    /// Downloads refused for lack of a slot since the previous heartbeat.
    pub refused: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RealmNodeKindInfo)]
pub enum NodeKindInfo {
    Management,
    Server,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RealmNodeConnectionStatus)]
pub enum RealmConnectionStatus {
    Connected,
    Configured,
    /// A device that reached this node recently. Not a connection: it is what
    /// this node itself saw, and only this node saw it.
    Seen,
    /// Presence does not describe this node: a device publishes none.
    Unknown,
}

impl From<&RealmNodeKind> for NodeKindInfo {
    fn from(value: &RealmNodeKind) -> Self {
        match value {
            RealmNodeKind::Management => Self::Management,
            RealmNodeKind::Server => Self::Server,
            RealmNodeKind::User { .. } => Self::User,
        }
    }
}

pub(super) fn map_realm_nodes(
    state: &ServerState,
    config: &RealmConfigDocument,
    present_nodes: HashSet<aruna_core::id::NodeId>,
    node_info_docs: BTreeMap<
        aruna_core::id::NodeId,
        aruna_core::structs::storage::node_info::NodeInfoDocument,
    >,
    contacts: &BTreeMap<aruna_core::id::NodeId, u64>,
    names: &NodeDisplays,
    now_ms: u64,
) -> Vec<NodeInfoResponse> {
    let current_node = state.get_node_id();
    let heartbeats = state.get_ctx().held_heartbeats();
    config
        .nodes
        .iter()
        .map(|node| {
            let parsed = node.node_id.parse::<aruna_core::id::NodeId>().ok();
            let is_current = node.node_id == current_node.to_string();
            let kind = NodeKindInfo::from(&node.kind);
            // A device publishes no realm presence, so presence carries no
            // statement about it, not even on the device's own node.
            let is_device = matches!(kind, NodeKindInfo::User);
            let present = !is_device
                && (is_current || parsed.is_some_and(|node_id| present_nodes.contains(&node_id)));
            let placement = parsed
                .and_then(|node_id| config.placement_entry(node_id))
                .map(|entry| NodePlacementResponse {
                    location: entry.effective_location().to_string(),
                    weight: entry.weight,
                    full: entry.full,
                    draining: entry.draining,
                });
            let info = parsed.and_then(|node_id| {
                node_info_docs
                    .get(&node_id)
                    .map(|document| map_node_document(document, heartbeats.get(&node_id)))
            });
            // A device answering for itself is in contact by definition.
            let last_seen_ms = match (is_device, is_current) {
                (true, true) => Some(now_ms),
                (true, false) => parsed.and_then(|node_id| contacts.get(&node_id).copied()),
                (false, _) => None,
            };
            let seen_recently = is_device
                && (is_current
                    || parsed
                        .and_then(|node_id| contacts.get(&node_id))
                        .is_some_and(|seen_ms| seen_recently(*seen_ms, now_ms)));
            let shown = parsed.map(|node_id| names.of(&node_id)).unwrap_or_default();
            let place = shown.place.unwrap_or_default();
            NodeInfoResponse {
                node_id: node.node_id.clone(),
                kind,
                owner: node.kind.owner().map(|owner| owner.to_string()),
                configured: true,
                present,
                connection_status: match (is_device, seen_recently, present) {
                    (true, true, _) => RealmConnectionStatus::Seen,
                    (true, false, _) => RealmConnectionStatus::Unknown,
                    (false, _, true) => RealmConnectionStatus::Connected,
                    (false, _, false) => RealmConnectionStatus::Configured,
                },
                last_seen_ms,
                display_name: shown.name,
                place: place.name,
                coordinates: place.coordinates.map(CoordinatesBody::from),
                placement,
                info,
            }
        })
        .collect()
}

/// Stale presence is candidate data, so it may not report a peer as connected;
/// only the local node stays present until a fresh lookup confirms the rest.
pub(super) fn presence_nodes(
    presence: RealmPresence,
    local: aruna_core::id::NodeId,
) -> HashSet<aruna_core::id::NodeId> {
    if presence.is_stale() {
        return HashSet::from([local]);
    }
    let mut nodes = presence.into_nodes();
    nodes.insert(local);
    nodes
}

pub(super) async fn load_realm_presence(state: &ServerState) -> HashSet<aruna_core::id::NodeId> {
    // A realm with offline nodes must degrade to local-only presence rather
    // than stall the dashboard.
    let discovery = tokio::time::timeout(
        REALM_DISCOVERY_TIMEOUT,
        drive(
            GetNodesOperation::new(state.get_realm_id()),
            &state.get_ctx(),
        ),
    )
    .await;
    match discovery {
        Ok(Ok(presence)) => presence_nodes(presence, state.get_node_id()),
        Ok(Err(error)) => {
            warn!(error = %error, "realm node discovery failed for realm info response");
            HashSet::from([state.get_node_id()])
        }
        Err(_) => {
            warn!("realm node discovery timed out for realm info response");
            HashSet::from([state.get_node_id()])
        }
    }
}

pub(super) async fn interface_services_status(state: &ServerState) -> InterfaceServicesStatus {
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
        mcp: interface_runtime.mcp.map(|mcp| InterfaceStatus {
            status: ServiceStatus::Available,
            bind: Some(mcp.bind_address.to_string()),
            url: Some(mcp.url),
        }),
    }
}

/// Adds the usage each backend's quota is measured against. Read once per
/// request from the maintained counters, not from the store itself.
async fn backend_statuses(
    state: &ServerState,
    backends: Vec<aruna_core::structs::info::BackendState>,
) -> Vec<BackendStatus> {
    let context = state.get_ctx();
    let mut statuses = Vec::with_capacity(backends.len());
    for backend in backends {
        let used_bytes = drive(
            UsageOperation::new(BackendRef::Node(backend.name.clone())),
            &context,
        )
        .await
        .inspect_err(
            |error| warn!(backend = %backend.name, error = %error, "Backend usage unavailable"),
        )
        .ok();
        statuses.push(BackendStatus {
            name: backend.name,
            backend: backend.backend_type.to_string(),
            class: backend.class,
            allow_tenants: backend.allow_tenants,
            quota_bytes: backend.quota_bytes,
            used_bytes,
            default: backend.default,
            status: ServiceStatus::from(backend.status),
        });
    }
    statuses
}

#[utoipa::path(
    get,
    path = "/system/info",
    tag = "system/info",
    summary = "Report this node's health, version and service status",
    description = r#"Health-checks this single node, answered locally and never routed to a peer.

**Authentication**: optional bearer token; every caller is answered, but the amount of detail
depends on the token, and a token of another realm or one that cannot be validated here counts as
anonymous.

**Behavior**
- The route always returns 200 for a node that is serving requests at all.
- An anonymous caller sees only what health-checks the node and says where to authenticate: node
  status, realm id, api version and the public interface urls.
- A bearer token of this realm adds the node's own identity and capability kind, its start time
  and uptime, its listen addresses, its peer connections and the network service summary.
- A token with WRITE on the realm configuration admin path additionally reveals the blob and
  database services, every registered storage backend with its quota and used bytes, the portal
  deployment state, operational warnings and the last error of each peer connection.
- Gated values are absent or empty rather than restructured, so the same parser works at every
  level.
- Backend `used_bytes` comes from maintained counters and is absent when they cannot be read, which
  is not the same as zero."#,
    responses(
        (
            status = 200,
            description = "Node health, version and node id; capabilities, addresses and topology for realm-authenticated callers, backend detail for realm admins",
            body = InfoResponse,
            examples(
                ("Anonymous" = (
                    summary = "Public health check: status, realm and public urls only",
                    value = json!({
                        "node": {"status": "available", "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"},
                        "api_version": "3.0.0-alpha.41",
                        "my_addresses": [],
                        "connections": [],
                        "services": {
                            "interfaces": {
                                "rest": {"status": "available", "bind": null, "url": "https://node.example.test/api/v1"},
                                "s3": {"status": "unavailable", "bind": null, "url": null}
                            }
                        },
                        "warnings": []
                    })
                )),
                ("Realm token" = (
                    summary = "A realm member also sees node identity, addresses and peer topology",
                    value = json!({
                        "node": {
                            "status": "available",
                            "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                            "peer_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                            "capabilities": "server",
                            "started_at": "2026-04-09T14:23:11Z",
                            "uptime_secs": 86400
                        },
                        "api_version": "3.0.0-alpha.41",
                        "my_addresses": ["192.0.2.10:4433", "https://relay.example.test/"],
                        "connections": [
                            {
                                "peer_id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                                "status": "connected",
                                "active_addresses": [
                                    {
                                        "status": "active",
                                        "address": "192.0.2.11:4433",
                                        "rtt_ms": 12,
                                        "protocol_connections": [
                                            {"connection_id": 7, "protocol": "document_sync", "side": "client", "status": "open"}
                                        ]
                                    }
                                ],
                                "last_error": null,
                                "next_retry_secs": null
                            }
                        ],
                        "services": {
                            "network": {
                                "status": "available",
                                "discovery": ["dns"],
                                "relay": "default",
                                "relay_urls": ["https://relay.example.test/"],
                                "routing_table_size": 24
                            },
                            "interfaces": {
                                "rest": {"status": "available", "bind": "0.0.0.0:3000", "url": "https://node.example.test/api/v1"},
                                "s3": {"status": "available", "bind": "0.0.0.0:1337", "url": "https://s3.example.test"}
                            }
                        },
                        "warnings": []
                    })
                ))
            )
        )
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> (StatusCode, Json<InfoResponse>) {
    (StatusCode::OK, Json(run_node_info(&state, auth).await))
}

pub(crate) async fn run_node_info(state: &ServerState, auth: Option<AuthContext>) -> InfoResponse {
    let access = info_access(state, auth.as_ref()).await;
    let realm = access != InfoAccess::Public;
    let admin = access == InfoAccess::Admin;

    let mut interfaces = interface_services_status(state).await;
    if !realm {
        interfaces.rest.bind = None;
        interfaces.s3.bind = None;
        if let Some(mcp) = interfaces.mcp.as_mut() {
            mcp.bind = None;
        }
    }

    let mut response = InfoResponse {
        node: NodeStatus {
            status: ServiceStatus::Available,
            realm_id: state.get_realm_id().to_string(),
            peer_id: Some(state.get_node_id().to_string()),
            capabilities: realm.then(|| NodeCapabilityKind::from(state.node_capabilities())),
            started_at: realm.then(|| {
                chrono::DateTime::<chrono::Utc>::from(state.started_at())
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            }),
            uptime_secs: realm.then(|| state.uptime().as_secs()),
        },
        api_version: env!("CARGO_PKG_VERSION").to_string(),
        portal: None,
        my_addresses: Vec::new(),
        connections: Vec::new(),
        services: ServicesStatus {
            network: None,
            blob: None,
            database: None,
            interfaces,
        },
        warnings: Vec::new(),
    };

    if !realm {
        return response;
    }

    let ctx = state.get_ctx();
    let snapshot = ctx.status().await;

    let (network, warnings) = match snapshot.network {
        Some(info) => {
            response.my_addresses = info
                .endpoint_addr
                .addrs
                .iter()
                .map(format_transport_addr)
                .collect();
            response.connections = info
                .connections
                .iter()
                .map(|peer| map_peer_connection(peer, admin))
                .collect();
            (
                NetworkServiceStatus {
                    status: ServiceStatus::Available,
                    discovery: info.discovery_methods,
                    relay: Some(info.relay_method),
                    relay_urls: info.relay_urls,
                    routing_table_size: info.routing_table_size,
                    requests: admin.then(|| RequestSummary::from_state(&info.requests)),
                },
                info.warnings,
            )
        }
        None => (
            NetworkServiceStatus {
                status: ServiceStatus::Unavailable,
                discovery: Vec::new(),
                relay: None,
                relay_urls: Vec::new(),
                routing_table_size: None,
                requests: admin.then(RequestSummary::default),
            },
            Vec::new(),
        ),
    };
    response.services.network = Some(network);

    if admin {
        let blob = match snapshot.blob {
            Some(info) => BlobServiceStatus {
                status: ServiceStatus::from(info.status),
                backend: Some(info.backend_type.to_string()),
                max_bucket_size: info.max_bucket_size,
                multipart_bucket: info.multipart_bucket,
                timeouts_secs: Some(TimeoutConfigSecs {
                    connect: info.timeouts.control_connect_timeout.as_secs(),
                    io: info.timeouts.control_io_timeout.as_secs(),
                    transfer_idle: info.timeouts.transfer_idle_timeout.as_secs(),
                }),
                backends: backend_statuses(state, info.backends).await,
            },
            None => BlobServiceStatus {
                status: ServiceStatus::NotConfigured,
                backend: None,
                max_bucket_size: None,
                multipart_bucket: None,
                timeouts_secs: None,
                backends: Vec::new(),
            },
        };
        response.services.blob = Some(blob);
        response.services.database = Some(DatabaseServiceStatus {
            status: ServiceStatus::from(snapshot.database.status),
            requests: RequestSummary::from_state(&snapshot.database.requests),
        });
        response.portal = Some(state.portal_status().await);
        response.warnings = warnings;
    }

    response
}

/// Maps a live peer connection to its wire form. `last_error` leaks internal
/// diagnostics, so it is populated for realm config admins only.
pub(super) fn map_peer_connection(
    peer: &aruna_core::structs::info::PeerConnectionState,
    admin: bool,
) -> PeerConnectionInfo {
    PeerConnectionInfo {
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
        last_error: admin.then(|| peer.last_error.clone()).flatten(),
        next_retry_secs: peer.retry_in_secs,
    }
}

fn protocol_name(alpn: Option<Alpn>) -> Option<String> {
    alpn.map(|alpn| match alpn {
        Alpn::Dht => "dht".to_string(),
        Alpn::Bao => "bao".to_string(),
        Alpn::DocumentSync => "document_sync".to_string(),
        Alpn::Metadata => "metadata".to_string(),
        Alpn::NativeReference => "native_reference".to_string(),
        Alpn::Notification => "notification".to_string(),
        Alpn::Shard => "shard".to_string(),
        Alpn::JobControl => "job_control".to_string(),
        Alpn::Heartbeat => "heartbeat".to_string(),
    })
}
