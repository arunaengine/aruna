use aruna_core::NodeId;
use aruna_core::alpn::{Alpn, AlpnRole};
use aruna_core::structs::RealmNodeKind;
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
use crate::device_limits::{DeviceLimiter, DeviceLimits, DevicePermit, DeviceRefusal};
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

/// Configured node kind per realm key, shared with every subsystem that has to
/// apply the ALPN x kind matrix. One writer: the realm-config refresh.
pub type PeerKinds = Arc<RwLock<BTreeMap<NodeId, RealmNodeKind>>>;

#[derive(Clone, Debug)]
pub struct InboundAdmission {
    realm_peers: Arc<RwLock<Vec<NodeId>>>,
    admitted_peers: Arc<RwLock<Vec<NodeId>>>,
    bootstrap_peers: Arc<RwLock<BTreeSet<NodeId>>>,
    peer_kinds: PeerKinds,
    local_kind: Arc<RwLock<Option<RealmNodeKind>>>,
    device_limiter: Arc<DeviceLimiter>,
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
            admitted_peers: Arc::new(RwLock::new(Vec::new())),
            bootstrap_peers: Arc::new(RwLock::new(bootstrap_peers.into_iter().collect())),
            peer_kinds: Arc::new(RwLock::new(BTreeMap::new())),
            local_kind: Arc::new(RwLock::new(None)),
            device_limiter: Arc::new(DeviceLimiter::default()),
            realm_config_materialized: Arc::new(AtomicBool::new(false)),
            materialized_signal,
            membership_signal,
        }
    }

    /// Every registered realm node, User kind included: user nodes forward
    /// metadata and job-control requests. Sync trust stays with `realm_peers`.
    pub(crate) fn set_admitted(&self, peers: Vec<NodeId>) {
        *self.admitted_peers.write() = peers;
    }

    /// Configured kind of every other realm node plus this node's own kind.
    /// Both halves come from the same realm-config refresh that publishes the
    /// membership bump, so a kind change re-runs the live connection check.
    pub(crate) fn set_kinds(
        &self,
        local: Option<RealmNodeKind>,
        peers: BTreeMap<NodeId, RealmNodeKind>,
    ) {
        *self.peer_kinds.write() = peers;
        *self.local_kind.write() = local;
    }

    /// Shared handle for subsystems outside the accept loop that apply the same
    /// node-kind boundary.
    pub(crate) fn peer_kinds(&self) -> PeerKinds {
        self.peer_kinds.clone()
    }

    /// Publishes the realm's per-device limits, which come from the same config
    /// refresh as the kinds they are keyed by.
    pub(crate) fn set_device_limits(&self, limits: DeviceLimits) {
        self.device_limiter.configure(limits);
    }

    /// Charges one inbound stream from a user device against its configured
    /// budget. A realm node is never charged: its volume is bounded by the
    /// responsibilities its kind carries.
    pub(crate) fn admit_stream(
        &self,
        peer: NodeId,
    ) -> std::result::Result<Option<DevicePermit>, DeviceRefusal> {
        if self
            .peer_kinds
            .read()
            .get(&peer)
            .is_none_or(RealmNodeKind::is_sync_eligible)
        {
            return Ok(None);
        }
        self.device_limiter.admit(peer).map(Some)
    }

    fn allows_peer(&self, peer: NodeId) -> bool {
        if self.realm_peers.read().contains(&peer) || self.admitted_peers.read().contains(&peer) {
            return true;
        }
        !self.realm_config_materialized.load(Ordering::Acquire)
            && self.bootstrap_peers.read().contains(&peer)
    }

    /// Both endpoints must be allowed to speak `alpn`: this node must serve it
    /// and the dialing key's kind must be allowed to open it here.
    fn allows_alpn(&self, peer: NodeId, alpn: Alpn) -> bool {
        alpn.accepts(
            self.local_kind.read().as_ref(),
            self.peer_kinds.read().get(&peer),
        )
    }

    /// The outbound half of the matrix: what this node's own kind may dial.
    pub(crate) fn local_dials(&self, alpn: Alpn) -> bool {
        alpn.permits(self.local_kind.read().as_ref(), AlpnRole::LocalDial)
    }

    fn admits(&self, peer: NodeId, alpn: Alpn) -> bool {
        self.allows_peer(peer) && self.allows_alpn(peer, alpn)
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
        if let Some(peer) = self.peer.take()
            && let Some(held) = state.per_peer.get_mut(&peer)
        {
            *held = held.saturating_sub(1);
            if *held == 0 {
                state.per_peer.remove(&peer);
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
                    if !inbound_admission.allows_alpn(peer_id, alpn) {
                        warn!(
                            node_id = %peer_id,
                            alpn = %alpn,
                            "Dropping inbound Iroh connection: node kind refuses this protocol"
                        );
                        conn.close(0u32.into(), b"node kind");
                        return;
                    }
                    // A provisional session takes the same budget as an admitted
                    // one, so one unknown identity cannot hold every handshake
                    // permit and starve inbound admission for configured peers.
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
                        conn.close(0u32.into(), b"peer limit");
                        return;
                    }

                    if known_peer {
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
                        return;
                    }

                    warn!(
                        node_id = %peer_id,
                        alpn = %alpn,
                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                        "Serving provisional inbound Iroh session"
                    );
                    let timeout_conn = conn.clone();
                    let admitted_conn = conn.clone();
                    let mut materialized = inbound_admission.materialized_watch();
                    let mut provisional = Box::pin(tokio::time::timeout(
                        STREAM_IO_TIMEOUT,
                        run_connection(
                            conn,
                            alpn,
                            dht_handler.clone(),
                            stream_handler.clone(),
                            document_sync.clone(),
                            peer_id,
                        ),
                    ));
                    let materialized_now = tokio::select! {
                        result = &mut provisional => {
                            if result.is_err() {
                                warn!(
                                    node_id = %peer_id,
                                    alpn = %alpn,
                                    timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                                    "Timed out provisional inbound Iroh session"
                                );
                                timeout_conn.close(0u32.into(), b"provisional timeout");
                            }
                            false
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
                        } => true,
                    };
                    drop(provisional);
                    if materialized_now {
                        // The fresh config decides: an admitted peer keeps this
                        // session instead of losing its in-flight requests, and
                        // run_admitted closes the connection for everyone else.
                        run_admitted(
                            admitted_conn,
                            alpn,
                            dht_handler,
                            stream_handler,
                            document_sync,
                            peer_id,
                            inbound_admission,
                        )
                        .await;
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
    if !admission.admits(peer_id, alpn) {
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
                if changed.is_err() || !admission.admits(peer_id, alpn) {
                    close_conn.close(0u32.into(), b"realm membership");
                    return;
                }
            }
        }
    }
}

/// Application stream frames in both directions. QUIC keepalive keeps arriving
/// on an idle connection, so counting raw datagrams would pin it for its whole
/// lifetime and hold an inbound permit with it.
fn stream_frames(conn: &Connection) -> u64 {
    let stats = conn.stats();
    stats.frame_rx.stream.saturating_add(stats.frame_tx.stream)
}

async fn run_dht_connection(
    conn: Connection,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    peer_id: NodeId,
) {
    let mut timers = ConnectionTimers::new();
    let mut seen_frames = stream_frames(&conn);
    loop {
        tokio::select! {
            _ = &mut timers.lifetime => {
                conn.close(0u32.into(), b"inbound lifetime");
                return;
            }
            _ = &mut timers.idle => {
                // Data on an already-accepted stream counts as activity, so a
                // long transfer is not cut down as an idle connection.
                let frames = stream_frames(&conn);
                if frames > seen_frames {
                    seen_frames = frames;
                    timers.activity();
                    continue;
                }
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
    let mut seen_frames = stream_frames(&conn);
    loop {
        tokio::select! {
            _ = &mut timers.lifetime => {
                conn.close(0u32.into(), b"inbound lifetime");
                return;
            }
            _ = &mut timers.idle => {
                // Data on an already-accepted stream counts as activity, so a
                // long transfer is not cut down as an idle connection.
                let frames = stream_frames(&conn);
                if frames > seen_frames {
                    seen_frames = frames;
                    timers.activity();
                    continue;
                }
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

    fn user_kind() -> RealmNodeKind {
        RealmNodeKind::User {
            owner: aruna_core::types::UserId::nil(aruna_core::structs::RealmId::from_bytes(
                [7u8; 32],
            )),
        }
    }

    #[test]
    fn kind_matrix() {
        // Contract: the full ALPN x kind x role matrix in `Alpn::ALL` order.
        // Flipping a cell changes a protocol boundary and must be deliberate.
        let user = user_kind();
        let cases: [(Option<&RealmNodeKind>, [[bool; 8]; 3]); 4] = [
            (None, [[true; 8]; 3]),
            (Some(&RealmNodeKind::Management), [[true; 8]; 3]),
            (Some(&RealmNodeKind::Server), [[true; 8]; 3]),
            (
                Some(&user),
                [
                    // PeerInbound: a realm node accepts a device's job control;
                    // document sync is decided by the pair, see `accepts`.
                    [true, true, false, true, true, true, false, true],
                    // LocalServe: a device takes the shared realm documents but
                    // never serves job control.
                    [true, true, true, true, true, true, false, false],
                    // LocalDial: the device dials job control for its owner and
                    // document sync to fetch the realm documents it was sent.
                    [true, true, true, true, true, true, false, true],
                ],
            ),
        ];
        for (kind, roles) in cases {
            for (role, expected) in AlpnRole::ALL.into_iter().zip(roles) {
                for (alpn, allowed) in Alpn::ALL.into_iter().zip(expected) {
                    assert_eq!(
                        alpn.permits(kind, role),
                        allowed,
                        "{alpn} for {kind:?} as {role:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn admission_kind_gate() {
        let realm_peers = Arc::new(RwLock::new(vec![peer(1)]));
        let admission = InboundAdmission::new(realm_peers, []);
        admission.mark_materialized();
        for alpn in Alpn::ALL {
            assert!(admission.admits(peer(1), alpn));
        }

        admission.set_kinds(
            Some(RealmNodeKind::Management),
            BTreeMap::from([(peer(1), user_kind())]),
        );
        assert!(admission.admits(peer(1), Alpn::Metadata));
        assert!(admission.admits(peer(1), Alpn::Bao));
        // A realm node accepts a device's routed job control, and its fetch of
        // the realm documents; the realm's own shard exchange stays closed.
        assert!(admission.admits(peer(1), Alpn::JobControl));
        assert!(admission.admits(peer(1), Alpn::DocumentSync));
        assert!(!admission.admits(peer(1), Alpn::Shard));
        assert!(!admission.admits(peer(2), Alpn::Metadata));
    }

    #[test]
    fn local_kind_gate() {
        // A device dials job control but never serves it, and exchanges the
        // shared realm documents with realm infrastructure in both directions.
        let realm_peers = Arc::new(RwLock::new(vec![peer(1)]));
        let admission = InboundAdmission::new(realm_peers, []);
        admission.mark_materialized();
        admission.set_kinds(
            Some(user_kind()),
            BTreeMap::from([(peer(1), RealmNodeKind::Server)]),
        );

        assert!(admission.local_dials(Alpn::Bao));
        assert!(admission.local_dials(Alpn::JobControl));
        // The device fetches the realm documents itself when it was away.
        assert!(admission.local_dials(Alpn::DocumentSync));
        assert!(!admission.local_dials(Alpn::Shard));
        assert!(!admission.admits(peer(1), Alpn::JobControl));
        assert!(admission.admits(peer(1), Alpn::Bao));
        // A realm node's push of the realm configuration reaches the device.
        assert!(admission.admits(peer(1), Alpn::DocumentSync));
        assert!(!admission.admits(peer(1), Alpn::Shard));
    }

    #[test]
    fn device_sync_pairs() {
        // A device takes the realm documents from realm infrastructure and from
        // nothing else: not from another device, and not from a key the realm
        // configuration does not name.
        let realm_peers = Arc::new(RwLock::new(vec![peer(1), peer(2)]));
        let admission = InboundAdmission::new(realm_peers, []);
        admission.mark_materialized();
        admission.set_kinds(
            Some(user_kind()),
            BTreeMap::from([(peer(1), RealmNodeKind::Server), (peer(2), user_kind())]),
        );

        assert!(admission.admits(peer(1), Alpn::DocumentSync));
        assert!(!admission.admits(peer(2), Alpn::DocumentSync));
        assert!(!admission.admits(peer(3), Alpn::DocumentSync));
    }

    #[test]
    fn realm_serves_devices() {
        // The other half of the pair: a realm node answers a device's fetch of
        // the realm documents, which is how one that was offline catches up.
        let realm_peers = Arc::new(RwLock::new(vec![peer(1)]));
        let admission = InboundAdmission::new(realm_peers, []);
        admission.mark_materialized();
        admission.set_kinds(
            Some(RealmNodeKind::Server),
            BTreeMap::from([(peer(1), user_kind())]),
        );

        assert!(admission.admits(peer(1), Alpn::DocumentSync));
        assert!(!admission.admits(peer(1), Alpn::Shard));
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
        let changes = admission.membership_watch();

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
        let changes = admission.membership_watch();

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
        // `is_elapsed` needs a poll; the biased ready-race polls without waiting.
        let mut timers = ConnectionTimers::new();
        timers.activity();
        tokio::time::advance(INBOUND_CONNECTION_LIFETIME).await;
        tokio::select! {
            biased;
            _ = &mut timers.lifetime => {}
            _ = std::future::ready(()) => panic!("lifetime must fire despite activity"),
        }
    }
}
