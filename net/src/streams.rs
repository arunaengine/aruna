use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use iroh::Endpoint;
use iroh::endpoint::Connection;
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, field, info_span, trace, warn};

use crate::connection_pool::{ConnectionLease, ConnectionPool};
use crate::document_sync::DocumentSyncService;
use crate::error::{NetError, Result};
use crate::telemetry::{
    duration_ms, record_duration_ms, warn_if_slow_iroh_phase, warn_if_slow_iroh_request,
};

const STREAM_IO_TIMEOUT: Duration = Duration::from_secs(10);
const INBOUND_CONNECTION_GLOBAL_LIMIT: usize = 256;
const INBOUND_CONNECTION_PEER_LIMIT: usize = 8;
const INBOUND_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const INBOUND_CONNECTION_LIFETIME: Duration = Duration::from_secs(6 * 60 * 60);

#[derive(Clone, Debug)]
pub struct InboundAdmission {
    realm_peers: Arc<RwLock<Vec<NodeId>>>,
    bootstrap_peers: Arc<RwLock<BTreeSet<NodeId>>>,
    realm_config_materialized: Arc<AtomicBool>,
    materialized_signal: watch::Sender<bool>,
    membership_signal: watch::Sender<u64>,
}

impl InboundAdmission {
    pub fn new(
        realm_peers: Arc<RwLock<Vec<NodeId>>>,
        bootstrap_peers: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        let (materialized_signal, _) = watch::channel(false);
        let (membership_signal, _) = watch::channel(0);
        Self {
            realm_peers,
            bootstrap_peers: Arc::new(RwLock::new(bootstrap_peers.into_iter().collect())),
            realm_config_materialized: Arc::new(AtomicBool::new(false)),
            materialized_signal,
            membership_signal,
        }
    }

    fn allows_peer(&self, peer: NodeId) -> bool {
        let realm_peers = self.realm_peers.read();
        if self.realm_config_materialized.load(Ordering::Acquire) {
            realm_peers.contains(&peer)
        } else {
            realm_peers.contains(&peer) || self.bootstrap_peers.read().contains(&peer)
        }
    }

    pub(crate) fn add_bootstrap(&self, peer: NodeId) {
        let mut bootstrap_peers = self.bootstrap_peers.write();
        if !self.realm_config_materialized.load(Ordering::Acquire) {
            bootstrap_peers.insert(peer);
        }
    }

    pub(crate) fn mark_materialized(&self) {
        self.bootstrap_peers.write().clear();
        self.realm_config_materialized
            .store(true, Ordering::Release);
        self.materialized_signal.send_replace(true);
        self.membership_signal
            .send_modify(|generation| *generation = generation.saturating_add(1));
    }

    fn materialized(&self) -> bool {
        self.realm_config_materialized.load(Ordering::Acquire)
    }

    fn materialized_watch(&self) -> watch::Receiver<bool> {
        self.materialized_signal.subscribe()
    }

    fn membership_watch(&self) -> watch::Receiver<u64> {
        self.membership_signal.subscribe()
    }
}

struct ConnectionTimers {
    idle: Pin<Box<tokio::time::Sleep>>,
    lifetime: Pin<Box<tokio::time::Sleep>>,
}

impl ConnectionTimers {
    fn new() -> Self {
        Self {
            idle: Box::pin(tokio::time::sleep(INBOUND_CONNECTION_IDLE_TIMEOUT)),
            lifetime: Box::pin(tokio::time::sleep(INBOUND_CONNECTION_LIFETIME)),
        }
    }

    fn activity(&mut self) {
        self.idle
            .as_mut()
            .reset(tokio::time::Instant::now() + INBOUND_CONNECTION_IDLE_TIMEOUT);
    }
}

#[derive(Debug, Default)]
struct InboundConnectionBudget {
    state: Mutex<InboundConnectionState>,
}

#[derive(Debug, Default)]
struct InboundConnectionState {
    global: usize,
    per_peer: BTreeMap<NodeId, usize>,
}

impl InboundConnectionBudget {
    fn acquire(self: &Arc<Self>) -> Option<InboundConnectionPermit> {
        let mut state = self.state.lock();
        if state.global >= INBOUND_CONNECTION_GLOBAL_LIMIT {
            return None;
        }
        state.global += 1;
        Some(InboundConnectionPermit {
            budget: self.clone(),
            peer: None,
        })
    }
}

struct InboundConnectionPermit {
    budget: Arc<InboundConnectionBudget>,
    peer: Option<NodeId>,
}

impl InboundConnectionPermit {
    fn admit(&mut self, peer: NodeId) -> bool {
        let mut state = self.budget.state.lock();
        let held = state.per_peer.get(&peer).copied().unwrap_or(0);
        if held >= INBOUND_CONNECTION_PEER_LIMIT {
            return false;
        }
        *state.per_peer.entry(peer).or_insert(0) += 1;
        self.peer = Some(peer);
        true
    }
}

impl Drop for InboundConnectionPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.global = state.global.saturating_sub(1);
        if let Some(peer) = self.peer.take() {
            if let Some(held) = state.per_peer.get_mut(&peer) {
                *held = held.saturating_sub(1);
                if *held == 0 {
                    state.per_peer.remove(&peer);
                }
            }
        }
    }
}

pub use iroh::endpoint::{RecvStream, SendStream};

#[derive(Debug)]
pub struct BiStream(
    pub SendStream,
    pub RecvStream,
    #[allow(dead_code)] pub(crate) Option<ConnectionLease>,
);

impl BiStream {
    pub fn into_recv(self) -> impl AsyncRead + Send + Sync + Unpin + 'static {
        LeasedRecvStream {
            recv: self.1,
            _lease: self.2,
        }
    }
}

struct LeasedRecvStream {
    recv: RecvStream,
    _lease: Option<ConnectionLease>,
}

impl AsyncRead for LeasedRecvStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().recv).poll_read(cx, buf)
    }
}

pub struct StreamsService {
    connection_pool: ConnectionPool,
    #[allow(dead_code)]
    shutdown: CancellationToken,
}

impl StreamsService {
    pub fn new(connection_pool: ConnectionPool, shutdown: CancellationToken) -> Self {
        Self {
            connection_pool,
            shutdown,
        }
    }

    #[tracing::instrument(
        name = "iroh.stream.open.request",
        level = "debug",
        skip(self),
        fields(peer = %node_id, alpn = %alpn)
    )]
    pub async fn open(&self, node_id: NodeId, alpn: Alpn) -> Result<BiStream> {
        let connection_pool = self.connection_pool.clone();
        let span = info_span!(
            "iroh.stream.open",
            "otel.kind" = "client",
            "otel.status_code" = field::Empty,
            "otel.status_description" = field::Empty,
            "network.transport" = "quic",
            "iroh.connect_ms" = field::Empty,
            "iroh.open_bi_ms" = field::Empty,
            "iroh.total_ms" = field::Empty,
            "iroh.selected_address" = field::Empty,
            "iroh.rtt_ms" = field::Empty,
            peer = %node_id,
            alpn = %alpn,
        );

        async move {
            let span = Span::current();
            let total_started = Instant::now();

            let connect_started = Instant::now();
            let conn = match connection_pool.get_or_connect(node_id, alpn).await {
                Ok(conn) => {
                    let elapsed = connect_started.elapsed();
                    record_duration_ms(&span, "iroh.connect_ms", elapsed);
                    warn_if_slow_iroh_phase("stream.open", "connect", elapsed);
                    trace!(
                        event = "iroh.stream.open_phase",
                        peer = %node_id,
                        alpn = %alpn,
                        iroh_phase = "connect",
                        duration_ms = duration_ms(elapsed),
                        "Completed Iroh stream open phase"
                    );
                    conn
                }
                Err(error) => {
                    let elapsed = connect_started.elapsed();
                    record_duration_ms(&span, "iroh.connect_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.connect_failed",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        error = %error,
                        "Iroh stream connect failed"
                    );
                    return Err(NetError::Connection(error.to_string()));
                }
            };

            record_selected_path(&span, &conn);

            let open_started = Instant::now();
            let stream = match tokio::time::timeout(STREAM_IO_TIMEOUT, conn.open_bi()).await {
                Ok(Ok(stream)) => {
                    let elapsed = open_started.elapsed();
                    record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
                    warn_if_slow_iroh_phase("stream.open", "open_bi", elapsed);
                    trace!(
                        event = "iroh.stream.open_phase",
                        peer = %node_id,
                        alpn = %alpn,
                        iroh_phase = "open_bi",
                        duration_ms = duration_ms(elapsed),
                        "Completed Iroh stream open phase"
                    );
                    BiStream(stream.0, stream.1, Some(conn))
                }
                Ok(Err(error)) => {
                    let elapsed = open_started.elapsed();
                    record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.open_bi_failed",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        error = %error,
                        "Iroh bidirectional stream open failed"
                    );
                    return Err(NetError::Stream(error.to_string()));
                }
                Err(error) => {
                    let elapsed = open_started.elapsed();
                    record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.open_bi_timeout",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                        error = %error,
                        "Iroh bidirectional stream open timed out"
                    );
                    return Err(NetError::Stream(error.to_string()));
                }
            };

            let total_elapsed = total_started.elapsed();
            record_duration_ms(&span, "iroh.total_ms", total_elapsed);
            warn_if_slow_iroh_request("stream.open", total_elapsed);
            span.record("otel.status_code", "OK");
            trace!(
                event = "iroh.stream.open_completed",
                peer = %node_id,
                alpn = %alpn,
                duration_ms = duration_ms(total_elapsed),
                "Opened Iroh stream"
            );
            Ok(stream)
        }
        .instrument(span)
        .await
    }
}

fn record_selected_path(span: &Span, conn: &Connection) {
    let paths = conn.paths();
    let Some(path) = paths.iter().find(|path| path.is_selected()) else {
        return;
    };

    span.record(
        "iroh.selected_address",
        field::display(format!("{:?}", path.remote_addr())),
    );
    span.record("iroh.rtt_ms", duration_ms(path.rtt()));
}

impl std::fmt::Debug for StreamsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsService").finish()
    }
}

#[tracing::instrument(
    name = "iroh.stream.accept_loop",
    level = "debug",
    skip(endpoint, dht_handler, stream_handler, shutdown)
)]
pub async fn run_accept_loop(
    endpoint: Endpoint,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    stream_handler: mpsc::Sender<(Alpn, BiStream, NodeId)>,
    document_sync: std::sync::Arc<DocumentSyncService>,
    inbound_admission: InboundAdmission,
    shutdown: CancellationToken,
) {
    let inbound_budget = Arc::new(InboundConnectionBudget::default());
    let handshake_budget = Arc::new(Semaphore::new(INBOUND_CONNECTION_GLOBAL_LIMIT));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };

                let dht_handler = dht_handler.clone();
                let stream_handler = stream_handler.clone();
                let document_sync = document_sync.clone();
                let inbound_admission = inbound_admission.clone();
                let inbound_budget = inbound_budget.clone();
                let Ok(handshake_permit) = handshake_budget.clone().try_acquire_owned() else {
                    warn!("Dropping inbound Iroh connection: handshake limit reached");
                    incoming.refuse();
                    continue;
                };

                tokio::spawn(async move {
                    let accepting = match incoming.accept() {
                        Ok(accepting) => accepting,
                        Err(_) => return,
                    };

                    let conn = match tokio::time::timeout(STREAM_IO_TIMEOUT, accepting).await {
                        Ok(Ok(conn)) => conn,
                        Ok(Err(err)) => {
                            warn!(error = %err, "Failed to accept incoming Iroh connection");
                            return;
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                                "Timed out accepting incoming Iroh connection"
                            );
                            return;
                        }
                    };

                    let alpn_bytes = conn.alpn().to_vec();
                    let peer_id = conn.remote_id();
                    let Some(alpn) = Alpn::from_bytes(&alpn_bytes) else {
                        warn!(
                            node_id = %peer_id,
                            "Dropping incoming connection with unknown ALPN"
                        );
                        conn.close(0u32.into(), b"unknown ALPN");
                        return;
                    };
                    let known_peer = inbound_admission.allows_peer(peer_id);
                    if !known_peer && inbound_admission.materialized() {
                        warn!(
                            node_id = %peer_id,
                            "Dropping inbound Iroh connection: realm admission rejected"
                        );
                        conn.close(0u32.into(), b"realm admission");
                        return;
                    }
                    if known_peer {
                        drop(handshake_permit);
                        let Some(mut permit) = inbound_budget.acquire() else {
                            warn!("Dropping inbound Iroh connection: connection limit reached");
                            conn.close(0u32.into(), b"connection limit");
                            return;
                        };
                        if !permit.admit(peer_id) {
                            warn!(
                                node_id = %peer_id,
                                "Dropping inbound Iroh connection: peer limit reached"
                            );
                            return;
                        }

                        run_admitted(
                            conn,
                            alpn,
                            dht_handler,
                            stream_handler,
                            document_sync,
                            peer_id,
                            inbound_admission,
                        )
                        .await;
                    } else {
                        warn!(
                            node_id = %peer_id,
                            alpn = %alpn,
                            timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                            "Serving provisional inbound Iroh session"
                        );
                        let timeout_conn = conn.clone();
                        let materialized_conn = conn.clone();
                        let mut materialized = inbound_admission.materialized_watch();
                        tokio::select! {
                            result = tokio::time::timeout(
                                STREAM_IO_TIMEOUT,
                                run_connection(
                                    conn,
                                    alpn,
                                    dht_handler,
                                    stream_handler,
                                    document_sync,
                                    peer_id,
                                ),
                            ) => {
                                if result.is_err() {
                                    warn!(
                                        node_id = %peer_id,
                                        alpn = %alpn,
                                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                                        "Timed out provisional inbound Iroh session"
                                    );
                                    timeout_conn.close(0u32.into(), b"provisional timeout");
                                }
                            }
                            _ = async {
                                loop {
                                    if *materialized.borrow() {
                                        break;
                                    }
                                    if materialized.changed().await.is_err() {
                                        break;
                                    }
                                }
                            } => {
                                warn!(
                                    node_id = %peer_id,
                                    alpn = %alpn,
                                    "Closed provisional inbound Iroh session after realm admission"
                                );
                                materialized_conn.close(0u32.into(), b"realm admission");
                            }
                        }
                    }
                });
            }
        }
    }
}

async fn run_admitted(
    conn: Connection,
    alpn: Alpn,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    stream_handler: mpsc::Sender<(Alpn, BiStream, NodeId)>,
    document_sync: std::sync::Arc<DocumentSyncService>,
    peer_id: NodeId,
    admission: InboundAdmission,
) {
    let close_conn = conn.clone();
    let mut membership = admission.membership_watch();
    if !admission.allows_peer(peer_id) {
        close_conn.close(0u32.into(), b"realm membership");
        return;
    }

    let session = run_connection(
        conn,
        alpn,
        dht_handler,
        stream_handler,
        document_sync,
        peer_id,
    );
    tokio::pin!(session);
    loop {
        tokio::select! {
            _ = &mut session => return,
            changed = membership.changed() => {
                if changed.is_err() || !admission.allows_peer(peer_id) {
                    close_conn.close(0u32.into(), b"realm membership");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn global_cap() {
        let budget = Arc::new(InboundConnectionBudget::default());
        let mut permits = Vec::new();
        for _ in 0..INBOUND_CONNECTION_GLOBAL_LIMIT {
            permits.push(budget.acquire().expect("within global limit"));
        }
        assert!(budget.acquire().is_none());
    }

    #[test]
    fn handshake_cap() {
        let budget = Arc::new(Semaphore::new(INBOUND_CONNECTION_GLOBAL_LIMIT));
        let mut permits = Vec::new();
        for _ in 0..INBOUND_CONNECTION_GLOBAL_LIMIT {
            permits.push(
                budget
                    .clone()
                    .try_acquire_owned()
                    .expect("within handshake limit"),
            );
        }
        assert!(budget.clone().try_acquire_owned().is_err());
    }

    #[test]
    fn peer_cap() {
        let budget = Arc::new(InboundConnectionBudget::default());
        let mut permits = Vec::new();
        for _ in 0..INBOUND_CONNECTION_PEER_LIMIT {
            let mut permit = budget.acquire().expect("within global limit");
            assert!(permit.admit(peer(1)));
            permits.push(permit);
        }

        let mut blocked = budget.acquire().expect("global capacity remains");
        assert!(!blocked.admit(peer(1)));
    }

    #[test]
    fn peer_fairness() {
        let budget = Arc::new(InboundConnectionBudget::default());
        let mut first = Vec::new();
        for _ in 0..INBOUND_CONNECTION_PEER_LIMIT {
            let mut permit = budget.acquire().expect("within global limit");
            assert!(permit.admit(peer(1)));
            first.push(permit);
        }

        let mut second = budget.acquire().expect("global capacity remains");
        assert!(second.admit(peer(2)));
        drop(first.pop());
        let mut retry = budget.acquire().expect("released global capacity");
        assert!(retry.admit(peer(1)));
    }

    #[test]
    fn permit_release() {
        let budget = Arc::new(InboundConnectionBudget::default());
        {
            let mut permit = budget.acquire().expect("within global limit");
            assert!(permit.admit(peer(1)));
        }

        let state = budget.state.lock();
        assert_eq!(state.global, 0);
        assert!(state.per_peer.is_empty());
    }

    #[test]
    fn admission_switches() {
        let realm_peers = Arc::new(RwLock::new(vec![peer(1)]));
        let admission = InboundAdmission::new(realm_peers.clone(), [peer(2)]);

        assert!(admission.allows_peer(peer(1)));
        assert!(admission.allows_peer(peer(2)));

        admission.mark_materialized();
        assert!(admission.allows_peer(peer(1)));
        assert!(!admission.allows_peer(peer(2)));

        *realm_peers.write() = vec![peer(3)];
        assert!(admission.allows_peer(peer(3)));
        assert!(!admission.allows_peer(peer(1)));
    }

    #[test]
    fn unknown_is_provisional() {
        let admission = InboundAdmission::new(Arc::new(RwLock::new(Vec::new())), []);
        let materialized = admission.materialized_watch();

        assert!(!admission.allows_peer(peer(1)));
        assert!(!admission.materialized());
        admission.mark_materialized();
        assert!(admission.materialized());
        assert!(*materialized.borrow());
        assert!(!admission.allows_peer(peer(1)));
    }

    #[test]
    fn bootstrap_updates() {
        let realm_peers = Arc::new(RwLock::new(Vec::new()));
        let admission = InboundAdmission::new(realm_peers, []);

        admission.add_bootstrap(peer(1));
        assert!(admission.allows_peer(peer(1)));

        admission.mark_materialized();
        admission.add_bootstrap(peer(2));
        assert!(!admission.allows_peer(peer(2)));
    }

    #[test]
    fn membership_retains() {
        let realm_peers = Arc::new(RwLock::new(vec![peer(1)]));
        let admission = InboundAdmission::new(realm_peers.clone(), []);
        admission.mark_materialized();
        let mut changes = admission.membership_watch();

        *realm_peers.write() = vec![peer(1), peer(2)];
        admission.mark_materialized();

        assert!(changes.has_changed().expect("membership watch open"));
        assert!(admission.allows_peer(peer(1)));
    }

    #[test]
    fn membership_removes() {
        let realm_peers = Arc::new(RwLock::new(vec![peer(1), peer(2)]));
        let admission = InboundAdmission::new(realm_peers.clone(), []);
        admission.mark_materialized();
        let mut changes = admission.membership_watch();

        *realm_peers.write() = vec![peer(2)];
        admission.mark_materialized();

        assert!(changes.has_changed().expect("membership watch open"));
        assert!(!admission.allows_peer(peer(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn activity_resets_idle() {
        let mut timers = ConnectionTimers::new();
        tokio::time::advance(INBOUND_CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        timers.activity();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!timers.idle.is_elapsed());
        tokio::time::advance(INBOUND_CONNECTION_IDLE_TIMEOUT).await;
        assert!(timers.idle.is_elapsed());
    }

    #[tokio::test(start_paused = true)]
    async fn lifetime_ignores_activity() {
        let mut timers = ConnectionTimers::new();
        timers.activity();
        tokio::time::advance(INBOUND_CONNECTION_LIFETIME).await;
        assert!(timers.lifetime.is_elapsed());
    }
}

async fn run_dht_connection(
    conn: Connection,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    peer_id: NodeId,
) {
    let mut timers = ConnectionTimers::new();
    loop {
        tokio::select! {
            _ = &mut timers.lifetime => {
                conn.close(0u32.into(), b"inbound lifetime");
                return;
            }
            _ = &mut timers.idle => {
                conn.close(0u32.into(), b"inbound idle");
                return;
            }
            incoming = conn.accept_bi() => match incoming {
                Ok((send, recv)) => match dht_handler.try_send((send, recv, peer_id)) {
                    Ok(()) => timers.activity(),
                    Err(TrySendError::Full((mut send, mut recv, _))) => {
                        warn!(node_id = %peer_id, "Dropping inbound DHT stream: queue full");
                        let _ = send.finish();
                        let _ = recv.stop(0u32.into());
                    }
                    Err(TrySendError::Closed((mut send, mut recv, _))) => {
                        warn!(node_id = %peer_id, "Dropping inbound DHT stream: queue closed");
                        let _ = send.finish();
                        let _ = recv.stop(0u32.into());
                        return;
                    }
                },
                Err(err) => {
                    trace!(
                        node_id = %peer_id,
                        error = %err,
                        "Inbound DHT connection stopped accepting streams"
                    );
                    return;
                }
            }
        }
    }
}

async fn run_connection(
    conn: Connection,
    alpn: Alpn,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    stream_handler: mpsc::Sender<(Alpn, BiStream, NodeId)>,
    document_sync: std::sync::Arc<DocumentSyncService>,
    peer_id: NodeId,
) {
    match alpn {
        Alpn::Dht => run_dht_connection(conn, dht_handler, peer_id).await,
        alpn @ (Alpn::Bao
        | Alpn::DocumentSync
        | Alpn::Metadata
        | Alpn::NativeReference
        | Alpn::Notification
        | Alpn::Shard
        | Alpn::JobControl) => {
            if alpn == Alpn::DocumentSync {
                document_sync.register_inbound_connection(&conn);
            }
            run_app_connection(conn, alpn, stream_handler, peer_id).await;
        }
    }
}

async fn run_app_connection(
    conn: Connection,
    alpn: Alpn,
    stream_handler: mpsc::Sender<(Alpn, BiStream, NodeId)>,
    peer_id: NodeId,
) {
    let mut timers = ConnectionTimers::new();
    loop {
        tokio::select! {
            _ = &mut timers.lifetime => {
                conn.close(0u32.into(), b"inbound lifetime");
                return;
            }
            _ = &mut timers.idle => {
                conn.close(0u32.into(), b"inbound idle");
                return;
            }
            incoming = conn.accept_bi() => match incoming {
                Ok((send, recv)) => {
                    match stream_handler.try_send((alpn, BiStream(send, recv, None), peer_id)) {
                        Ok(()) => timers.activity(),
                        Err(TrySendError::Full((_, mut stream, _))) => {
                            warn!(node_id = %peer_id, alpn = %alpn, "Dropping inbound app stream: queue full");
                            let _ = stream.0.finish();
                            let _ = stream.1.stop(0u32.into());
                        }
                        Err(TrySendError::Closed((_, mut stream, _))) => {
                            warn!(node_id = %peer_id, alpn = %alpn, "Dropping inbound app stream: queue closed");
                            let _ = stream.0.finish();
                            let _ = stream.1.stop(0u32.into());
                            return;
                        }
                    }
                }
                Err(err) => {
                    trace!(
                        node_id = %peer_id,
                        alpn = %alpn,
                        error = %err,
                        "Inbound app connection stopped accepting streams"
                    );
                    return;
                }
            }
        }
    }
}
