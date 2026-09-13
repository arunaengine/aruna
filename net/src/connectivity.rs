//! Peer connectivity management: retry/backoff decisions over the peer set,
//! the run loop that waits and probes, and the diagnostic projection of
//! managed peers.
//!
//! `PeerConnectivityManagerState` holds the pure scheduling decisions
//! (due peers, next wait, success/failure transitions); the `run_*` functions
//! own the waits and network I/O. Handle-side peer registration lives on
//! `NetHandle`; this module never touches admission.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::id::NodeId;
use aruna_core::structs::{
    ConnectionAddressState, ConnectionAddressStatus, ConnectionMonitorState,
    NetworkDiagnosticsState, PeerConnectionState, PeerConnectionStatus, ProtocolConnectionState,
    RealmId,
};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::TransportAddrUsage;
use iroh::{Endpoint, TransportAddr};
use parking_lot::RwLock;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::config::DiscoveryMethod;
use crate::connection_pool::ConnectionPool;
use crate::dht::DhtHandle;
use crate::discovery::{install_signed_endpoint, resolve_signed_endpoint};
use crate::unique_peer_nodes;

const PEER_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(5);
const PEER_MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
const PEER_SUCCESS_REFRESH_DELAY: Duration = Duration::from_secs(300);
const PEER_MANAGER_IDLE_DELAY: Duration = Duration::from_secs(300);

#[derive(Debug)]
pub(crate) struct PeerConnectivityManagerState {
    peers: Vec<ManagedPeer>,
}

#[derive(Debug, Clone)]
struct ManagedPeer {
    node_id: NodeId,
    source: String,
    consecutive_failures: u64,
    last_error: Option<String>,
    next_attempt: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct PeerConnectivityState {
    node_id: NodeId,
    consecutive_failures: u64,
    last_error: Option<String>,
    next_retry_in_secs: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum PeerConnectivityEvent {
    ManagePeer {
        node_id: NodeId,
        source: String,
        immediate: bool,
    },
    ConnectionSuccess {
        node_id: NodeId,
        source: String,
    },
    ConnectionFailure {
        node_id: NodeId,
        source: String,
        error: String,
    },
}

impl PeerConnectivityManagerState {
    pub(crate) fn new(nodes: &[NodeId], source: &str) -> Self {
        let now = Instant::now();
        let mut peers = nodes
            .iter()
            .copied()
            .map(|node_id| ManagedPeer::new(node_id, source, now))
            .collect::<Vec<_>>();
        peers.sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        peers.dedup_by(|a, b| a.node_id == b.node_id);
        Self { peers }
    }

    fn manage_peer(&mut self, node_id: NodeId, source: &str, now: Instant, immediate: bool) {
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            if immediate && peer.next_attempt > now {
                peer.next_attempt = now;
            }
            return;
        }

        let next_attempt = if immediate {
            now
        } else {
            now + PEER_SUCCESS_REFRESH_DELAY
        };
        self.peers
            .push(ManagedPeer::new(node_id, source, next_attempt));
        self.peers
            .sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
    }

    fn record_success(&mut self, node_id: NodeId, source: &str, now: Instant) {
        if self.peer_mut(node_id).is_none() {
            self.manage_peer(node_id, source, now, false);
        }
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            peer.consecutive_failures = 0;
            peer.last_error = None;
            peer.next_attempt = now + PEER_SUCCESS_REFRESH_DELAY;
        }
    }

    fn record_failure(&mut self, node_id: NodeId, source: &str, error: String, now: Instant) {
        if self.peer_mut(node_id).is_none() {
            self.manage_peer(node_id, source, now, false);
        }
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            peer.consecutive_failures = peer.consecutive_failures.saturating_add(1);
            peer.last_error = Some(error);
            peer.next_attempt = now + peer_retry_delay(node_id, peer.consecutive_failures);
        }
    }

    fn due_peers(&self, now: Instant) -> Vec<NodeId> {
        self.peers
            .iter()
            .filter(|peer| peer.next_attempt <= now)
            .map(|peer| peer.node_id)
            .collect()
    }

    fn next_wait(&self, now: Instant) -> Duration {
        self.peers
            .iter()
            .map(|peer| peer.next_attempt.saturating_duration_since(now))
            .min()
            .unwrap_or(PEER_MANAGER_IDLE_DELAY)
    }

    fn peer_source(&self, node_id: NodeId) -> String {
        self.peers
            .iter()
            .find(|peer| peer.node_id == node_id)
            .map(|peer| peer.source.clone())
            .unwrap_or_else(|| "managed_peer".to_string())
    }

    fn status(&self, now: Instant) -> Vec<PeerConnectivityState> {
        self.peers
            .iter()
            .map(|peer| PeerConnectivityState {
                node_id: peer.node_id,
                consecutive_failures: peer.consecutive_failures,
                last_error: peer.last_error.clone(),
                next_retry_in_secs: Some(
                    peer.next_attempt.saturating_duration_since(now).as_secs(),
                ),
            })
            .collect()
    }

    fn peer_mut(&mut self, node_id: NodeId) -> Option<&mut ManagedPeer> {
        self.peers.iter_mut().find(|peer| peer.node_id == node_id)
    }
}

impl ManagedPeer {
    fn new(node_id: NodeId, source: &str, next_attempt: Instant) -> Self {
        Self {
            node_id,
            source: source.to_string(),
            consecutive_failures: 0,
            last_error: None,
            next_attempt,
        }
    }
}

fn merge_source(existing: &mut String, source: &str) {
    if existing.split(',').any(|part| part == source) {
        return;
    }
    if existing.is_empty() {
        existing.push_str(source);
    } else {
        existing.push(',');
        existing.push_str(source);
    }
}

fn peer_retry_delay(node_id: NodeId, consecutive_failures: u64) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(8) as u32;
    let base = PEER_INITIAL_RETRY_DELAY
        .saturating_mul(2u32.saturating_pow(exponent))
        .min(PEER_MAX_RETRY_DELAY);
    let base_ms = base.as_millis() as i128;
    let jitter_window = (base_ms / 5).max(1);
    let jitter_range = (jitter_window * 2 + 1) as u64;
    let jitter_seed = peer_jitter_seed(node_id, consecutive_failures);
    let jitter = (jitter_seed % jitter_range) as i128 - jitter_window;
    let delayed_ms = (base_ms + jitter).max(1_000) as u64;
    Duration::from_millis(delayed_ms)
}

fn peer_jitter_seed(node_id: NodeId, attempt: u64) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&node_id.as_bytes()[..8]);
    u64::from_le_bytes(bytes) ^ attempt.rotate_left(17)
}

pub(crate) fn send_connectivity_event(
    tx: &mpsc::Sender<PeerConnectivityEvent>,
    event: PeerConnectivityEvent,
) {
    if let Err(err) = tx.try_send(event) {
        match err {
            mpsc::error::TrySendError::Full(_) => debug!("peer connectivity event queue full"),
            mpsc::error::TrySendError::Closed(_) => debug!("peer connectivity task stopped"),
        }
    }
}

pub(crate) async fn peer_connectivity_status(
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
) -> Vec<PeerConnectivityState> {
    state.lock().await.status(Instant::now())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_connectivity_manager(
    dht: Arc<DhtHandle>,
    address_lookup: MemoryLookup,
    connection_pool: ConnectionPool,
    discovery_method: DiscoveryMethod,
    realm_id: RealmId,
    dht_signed_authorized_nodes: Arc<RwLock<Vec<NodeId>>>,
    state: Arc<Mutex<PeerConnectivityManagerState>>,
    diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    mut event_rx: mpsc::Receiver<PeerConnectivityEvent>,
    shutdown: CancellationToken,
) {
    loop {
        drain_connectivity_events(&state, &mut event_rx).await;

        let now = Instant::now();
        let due_peers = state.lock().await.due_peers(now);
        if !due_peers.is_empty() {
            for peer in due_peers {
                if shutdown.is_cancelled() {
                    return;
                }
                let authorized_nodes = dht_signed_authorized_nodes.read().clone();
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = run_connectivity_attempt(
                        &dht,
                        &address_lookup,
                        &connection_pool,
                        &discovery_method,
                        realm_id,
                        &authorized_nodes,
                        &state,
                        &diagnostics,
                        peer,
                    ) => {}
                }
            }
            continue;
        }

        let wait = state.lock().await.next_wait(now);

        tokio::select! {
            _ = shutdown.cancelled() => break,
            maybe_event = event_rx.recv() => {
                let Some(event) = maybe_event else { break };
                apply_connectivity_event(&state, event).await;
                while let Ok(event) = event_rx.try_recv() {
                    apply_connectivity_event(&state, event).await;
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

async fn drain_connectivity_events(
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    event_rx: &mut mpsc::Receiver<PeerConnectivityEvent>,
) {
    while let Ok(event) = event_rx.try_recv() {
        apply_connectivity_event(state, event).await;
    }
}

async fn apply_connectivity_event(
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    event: PeerConnectivityEvent,
) {
    let now = Instant::now();
    let mut guard = state.lock().await;
    match event {
        PeerConnectivityEvent::ManagePeer {
            node_id,
            source,
            immediate,
        } => {
            guard.manage_peer(node_id, &source, now, immediate);
        }
        PeerConnectivityEvent::ConnectionSuccess { node_id, source } => {
            guard.record_success(node_id, &source, now);
        }
        PeerConnectivityEvent::ConnectionFailure {
            node_id,
            source,
            error,
        } => {
            guard.record_failure(node_id, &source, error, now);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_connectivity_attempt(
    dht: &DhtHandle,
    address_lookup: &MemoryLookup,
    connection_pool: &ConnectionPool,
    discovery_method: &DiscoveryMethod,
    realm_id: RealmId,
    dht_signed_authorized_nodes: &[NodeId],
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    diagnostics: &Arc<Mutex<NetworkDiagnosticsState>>,
    peer: NodeId,
) {
    let source = state.lock().await.peer_source(peer);

    let mut result = dht.bootstrap_nodes(&[peer]).await;
    if result.is_err() {
        match resolve_signed_endpoint(
            dht,
            realm_id,
            dht_signed_authorized_nodes,
            peer,
            discovery_method.dht_signed_config(),
            diagnostics,
        )
        .await
        {
            Ok(Some(endpoint_addr)) => {
                install_signed_endpoint(address_lookup, dht, connection_pool, endpoint_addr).await;
                result = dht.bootstrap_nodes(&[peer]).await;
            }
            Ok(None) => {}
            Err(err) => {
                debug!(
                    peer = %peer,
                    error = %err,
                    "DHT-signed endpoint resolution failed during connectivity check"
                );
            }
        }
    }
    let routing_table_size = dht.routing_table_size().await.ok();
    match result {
        Ok(()) => {
            state
                .lock()
                .await
                .record_success(peer, "connectivity_probe", Instant::now());
            let mut diagnostics = diagnostics.lock().await;
            diagnostics.requests.record_success();
            diagnostics.routing_table_size = routing_table_size;
            debug!(
                peer = %peer,
                source = %source,
                routing_table_size = ?routing_table_size,
                "Managed peer connectivity check succeeded"
            );
        }
        Err(err) => {
            state.lock().await.record_failure(
                peer,
                "connectivity_probe",
                err.to_string(),
                Instant::now(),
            );
            let mut diagnostics = diagnostics.lock().await;
            diagnostics
                .requests
                .record_failure(format!("peer {peer}: {err}"));
            diagnostics.routing_table_size = routing_table_size;
            warn!(
                error = %err,
                peer = %peer,
                source = %source,
                routing_table_size = ?routing_table_size,
                "Managed peer connectivity check failed; will retry with backoff"
            );
        }
    }
}

pub(crate) async fn peer_connection_states(
    endpoint: &Endpoint,
    monitor: &ConnectionMonitorState,
    peer_connectivity: &[PeerConnectivityState],
    peer_nodes: &[NodeId],
    local_id: NodeId,
) -> Vec<PeerConnectionState> {
    let mut peers = peer_nodes.to_vec();
    peers.extend(peer_connectivity.iter().map(|peer| peer.node_id));
    peers.extend(
        monitor
            .open_connections
            .iter()
            .map(|connection| connection.remote_id),
    );
    let mut peers = unique_peer_nodes(peers, local_id);
    let mut states = Vec::with_capacity(peers.len());

    for peer in peers.drain(..) {
        let health = peer_connectivity.iter().find(|state| state.node_id == peer);
        let open_connections = monitor
            .open_connections
            .iter()
            .filter(|connection| connection.remote_id == peer)
            .collect::<Vec<_>>();
        let mut active_addresses = active_address_rows(endpoint, peer).await;

        for connection in &open_connections {
            let (address, status) = match &connection.selected_address {
                Some(address) => (address.clone(), ConnectionAddressStatus::Active),
                None => (
                    "not assigned".to_string(),
                    ConnectionAddressStatus::NotAssigned,
                ),
            };
            let row = upsert_address_row(&mut active_addresses, address, status);
            merge_rtt(&mut row.rtt_ms, connection.rtt_ms);
            row.protocol_connections.push(ProtocolConnectionState {
                connection_id: connection.connection_id,
                alpn: connection.alpn,
                side: connection.side,
            });
        }

        for address in &mut active_addresses {
            address
                .protocol_connections
                .sort_unstable_by_key(|connection| connection.connection_id);
        }
        active_addresses.sort_unstable_by(|a, b| a.address.cmp(&b.address));

        let status = if !open_connections.is_empty() {
            PeerConnectionStatus::Connected
        } else if health.is_some_and(|state| state.consecutive_failures > 0) {
            PeerConnectionStatus::Unreachable
        } else {
            PeerConnectionStatus::Known
        };

        states.push(PeerConnectionState {
            node_id: peer,
            status,
            active_addresses,
            last_error: health.and_then(|state| state.last_error.clone()),
            next_retry_in_secs: health.and_then(|state| state.next_retry_in_secs),
        });
    }

    states
}

async fn active_address_rows(endpoint: &Endpoint, peer: NodeId) -> Vec<ConnectionAddressState> {
    let mut rows = Vec::new();
    if let Some(remote_info) = endpoint.remote_info(peer).await {
        for addr in remote_info.addrs() {
            if matches!(addr.usage(), TransportAddrUsage::Active) {
                upsert_address_row(
                    &mut rows,
                    format_transport_addr(addr.addr()),
                    ConnectionAddressStatus::Active,
                );
            }
        }
    }
    rows
}

fn upsert_address_row(
    rows: &mut Vec<ConnectionAddressState>,
    address: String,
    status: ConnectionAddressStatus,
) -> &mut ConnectionAddressState {
    if let Some(index) = rows
        .iter()
        .position(|row| row.address == address && row.status == status)
    {
        return &mut rows[index];
    }

    rows.push(ConnectionAddressState {
        status,
        address,
        rtt_ms: None,
        protocol_connections: Vec::new(),
    });
    rows.last_mut().expect("inserted address row")
}

fn merge_rtt(existing: &mut Option<u64>, candidate: Option<u64>) {
    let Some(candidate) = candidate else {
        return;
    };
    *existing = Some(existing.map_or(candidate, |current| current.min(candidate)));
}

fn format_transport_addr(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(addr) => addr.to_string(),
        TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

pub(crate) fn net_warnings(
    peer_nodes: &[NodeId],
    connections: &[PeerConnectionState],
    routing_table_size: Option<usize>,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if connections
        .iter()
        .any(|connection| connection.status == PeerConnectionStatus::Unreachable)
        && !connections
            .iter()
            .any(|connection| connection.status == PeerConnectionStatus::Connected)
    {
        warnings
            .push("no open p2p connections after one or more managed peer failures".to_string());
    }

    if routing_table_size == Some(0) && !peer_nodes.is_empty() {
        warnings.push("DHT routing table is empty despite configured realm peers".to_string());
    }

    for node_id in peer_nodes {
        let has_active_address = connections
            .iter()
            .any(|state| state.node_id == *node_id && !state.active_addresses.is_empty());
        if !has_active_address {
            warnings.push(format!(
                "realm peer {node_id} has no active addresses; add peer endpoints or enable realm discovery"
            ));
        }
    }

    for peer in connections {
        if peer.status == PeerConnectionStatus::Unreachable {
            let error = peer
                .last_error
                .as_deref()
                .unwrap_or("unknown connection error");
            warnings.push(format!(
                "managed peer {} unreachable: {}",
                peer.node_id, error
            ));
        } else if peer.active_addresses.is_empty() {
            warnings.push(format!(
                "managed peer {} has no active addresses; add peer endpoints or enable realm discovery",
                peer.node_id
            ));
        }
    }

    warnings.sort();
    warnings.dedup();
    warnings
}
