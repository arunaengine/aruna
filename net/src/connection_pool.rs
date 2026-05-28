use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aruna_core::alpn::Alpn;
use aruna_core::id::NodeId;
use aruna_core::structs::{ConnectionMonitorState, OpenConnection};
use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks, Side,
    WeakConnectionHandle,
};
use iroh::{Endpoint, TransportAddr};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, debug, info_span, trace, warn};

const MONITOR_CHANNEL_CAPACITY: usize = 4096;
const PARKED_IDLE_TIMER_SECS: u64 = 365 * 24 * 60 * 60;

#[derive(Clone, Debug)]
pub struct ConnectionPoolOptions {
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_connections: usize,
}

impl Default for ConnectionPoolOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            max_connections: 128,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ConnectionKey {
    node_id: NodeId,
    alpn: Alpn,
}

#[derive(Debug, Clone, Error)]
pub enum PoolConnectError {
    #[error("connection pool is shut down")]
    Shutdown,
    #[error("connection attempt timed out")]
    Timeout,
    #[error("too many pooled connections")]
    TooManyConnections,
    #[error("gossip connections are not pooled")]
    GossipUnsupported,
    #[error("connection failed: {0}")]
    Connection(String),
}

#[derive(Debug, Clone, Error)]
pub enum ConnectionPoolError {
    #[error("connection pool is shut down")]
    Shutdown,
}

#[derive(Debug)]
pub struct ConnectionLease {
    connection: Connection,
    _active: ActiveConnectionLease,
}

impl ConnectionLease {
    fn new(connection: Connection, active: ActiveConnectionLease) -> Self {
        Self {
            connection,
            _active: active,
        }
    }
}

impl Deref for ConnectionLease {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

enum ActorMessage {
    RequestLease(RequestLease),
    ConnectionIdle { key: ConnectionKey },
    ConnectionClosed { key: ConnectionKey },
    Shutdown,
}

struct RequestLease {
    key: ConnectionKey,
    tx: oneshot::Sender<std::result::Result<ConnectionLease, PoolConnectError>>,
}

struct PoolContext {
    endpoint: Endpoint,
    options: ConnectionPoolOptions,
    owner: ConnectionPool,
}

impl PoolContext {
    async fn run_connection_actor(
        self: Arc<Self>,
        key: ConnectionKey,
        mut rx: Receiver<RequestLease>,
    ) {
        let connect = async {
            self.endpoint
                .connect(key.node_id, key.alpn.as_bytes())
                .await
                .map_err(|error| PoolConnectError::Connection(error.to_string()))
        };
        let state = match tokio::time::timeout(self.options.connect_timeout, connect).await {
            Ok(result) => result,
            Err(_) => Err(PoolConnectError::Timeout),
        };

        if let Err(error) = &state {
            debug!(
                peer = %key.node_id,
                alpn = %key.alpn,
                error = %error,
                "pooled connection attempt failed"
            );
            let _ = self.owner.close_key(key).await;
        }

        let counter = ConnectionCounter::new();
        let mut idle_timer = Box::pin(tokio::time::sleep(Duration::from_secs(
            PARKED_IDLE_TIMER_SECS,
        )));
        let mut idle_timer_active = false;

        let mut close_fut = state.as_ref().ok().map(|connection| {
            let connection = connection.clone();
            Box::pin(async move {
                let _ = connection.closed().await;
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        loop {
            tokio::select! {
                biased;

                maybe_request = rx.recv() => {
                    let Some(request) = maybe_request else { break };
                    match &state {
                        Ok(connection) => {
                            idle_timer_active = false;
                            let lease = ConnectionLease::new(connection.clone(), counter.get_one());
                            trace!(
                                peer = %key.node_id,
                                alpn = %key.alpn,
                                active_leases = counter.current(),
                                "handing out pooled connection lease"
                            );
                            let _ = request.tx.send(Ok(lease));
                        }
                        Err(error) => {
                            let _ = request.tx.send(Err(error.clone()));
                        }
                    }
                }

                _ = async {
                    if let Some(close_fut) = close_fut.as_mut() {
                        close_fut.as_mut().await;
                    }
                }, if close_fut.is_some() => {
                    close_fut = None;
                    let _ = self.owner.close_key(key).await;
                }

                _ = counter.notified() => {
                    if counter.is_idle() {
                        trace!(peer = %key.node_id, alpn = %key.alpn, "pooled connection is idle");
                        if self.owner.idle(key).await.is_err() {
                            break;
                        }
                        idle_timer_active = true;
                        idle_timer.as_mut().reset(Instant::now() + self.options.idle_timeout);
                    }
                }

                _ = &mut idle_timer, if idle_timer_active => {
                    trace!(peer = %key.node_id, alpn = %key.alpn, "pooled connection idle timeout expired");
                    idle_timer_active = false;
                    let _ = self.owner.close_key(key).await;
                }
            }
        }

        if let Ok(connection) = state {
            let reason = if counter.is_idle() { b"idle" } else { b"drop" };
            connection.close(0u32.into(), reason);
        }
    }
}

struct Actor {
    rx: Receiver<ActorMessage>,
    connections: HashMap<ConnectionKey, Sender<RequestLease>>,
    idle: VecDeque<ConnectionKey>,
    context: Arc<PoolContext>,
}

impl Actor {
    fn new(endpoint: Endpoint, options: ConnectionPoolOptions) -> (Self, Sender<ActorMessage>) {
        let (tx, rx) = mpsc::channel(256);
        let context = Arc::new(PoolContext {
            endpoint,
            options,
            owner: ConnectionPool { tx: tx.clone() },
        });
        (
            Self {
                rx,
                connections: HashMap::new(),
                idle: VecDeque::new(),
                context,
            },
            tx,
        )
    }

    async fn run(mut self) {
        while let Some(message) = self.rx.recv().await {
            match message {
                ActorMessage::RequestLease(request) => self.handle_request(request).await,
                ActorMessage::ConnectionIdle { key } => self.add_idle(key),
                ActorMessage::ConnectionClosed { key } => self.remove_connection(key),
                ActorMessage::Shutdown => break,
            }
        }
        self.connections.clear();
        self.idle.clear();
    }

    async fn handle_request(&mut self, mut request: RequestLease) {
        let key = request.key;
        self.remove_idle(key);

        if let Some(connection_tx) = self.connections.get(&key) {
            match connection_tx.send(request).await {
                Ok(()) => return,
                Err(error) => {
                    request = error.0;
                    self.remove_connection(key);
                }
            }
        }

        if self.connections.len() >= self.context.options.max_connections {
            if let Some(idle) = self.idle.pop_front() {
                trace!(
                    peer = %idle.node_id,
                    alpn = %idle.alpn,
                    "evicting oldest idle pooled connection"
                );
                self.remove_connection(idle);
            } else {
                let _ = request.tx.send(Err(PoolConnectError::TooManyConnections));
                return;
            }
        }

        let (connection_tx, connection_rx) = mpsc::channel(128);
        self.connections.insert(key, connection_tx.clone());

        let context = self.context.clone();
        tokio::spawn(async move {
            context.run_connection_actor(key, connection_rx).await;
        });

        if connection_tx.send(request).await.is_err() {
            self.remove_connection(key);
        }
    }

    fn add_idle(&mut self, key: ConnectionKey) {
        self.remove_idle(key);
        if self.connections.contains_key(&key) {
            self.idle.push_back(key);
        }
    }

    fn remove_idle(&mut self, key: ConnectionKey) {
        self.idle.retain(|candidate| *candidate != key);
    }

    fn remove_connection(&mut self, key: ConnectionKey) {
        self.connections.remove(&key);
        self.remove_idle(key);
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    tx: Sender<ActorMessage>,
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool").finish()
    }
}

impl ConnectionPool {
    pub fn new(endpoint: Endpoint, options: ConnectionPoolOptions) -> Self {
        let (actor, tx) = Actor::new(endpoint, options);
        tokio::spawn(actor.run());
        Self { tx }
    }

    pub async fn get_or_connect(
        &self,
        node_id: NodeId,
        alpn: Alpn,
    ) -> std::result::Result<ConnectionLease, PoolConnectError> {
        if alpn == Alpn::Gossip {
            return Err(PoolConnectError::GossipUnsupported);
        }

        let key = ConnectionKey { node_id, alpn };
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::RequestLease(RequestLease { key, tx }))
            .await
            .map_err(|_| PoolConnectError::Shutdown)?;
        rx.await.map_err(|_| PoolConnectError::Shutdown)?
    }

    pub async fn shutdown(&self) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::Shutdown)
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)
    }

    async fn close_key(&self, key: ConnectionKey) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionClosed { key })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)
    }

    async fn idle(&self, key: ConnectionKey) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionIdle { key })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)
    }
}

#[derive(Debug)]
struct ConnectionCounterInner {
    count: AtomicUsize,
    notify: Notify,
}

#[derive(Debug, Clone)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new() -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                count: AtomicUsize::new(0),
                notify: Notify::new(),
            }),
        }
    }

    fn current(&self) -> usize {
        self.inner.count.load(Ordering::SeqCst)
    }

    fn get_one(&self) -> ActiveConnectionLease {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        ActiveConnectionLease {
            inner: self.inner.clone(),
        }
    }

    fn is_idle(&self) -> bool {
        self.inner.count.load(Ordering::SeqCst) == 0
    }

    fn notified(&self) -> impl Future<Output = ()> + '_ {
        self.inner.notify.notified()
    }
}

#[derive(Debug)]
struct ActiveConnectionLease {
    inner: Arc<ConnectionCounterInner>,
}

impl Drop for ActiveConnectionLease {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.notify.notify_waiters();
        }
    }
}

#[derive(Clone, Debug)]
pub struct Monitor {
    connections: Arc<tokio::sync::Mutex<Vec<TrackedConnection>>>,
    tx: Sender<ObservedConnection>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
struct ObservedConnection {
    alpn: Vec<u8>,
    remote_id: NodeId,
    side: Side,
    handle: WeakConnectionHandle,
}

#[derive(Debug, Clone)]
struct TrackedConnection {
    connection_id: u64,
    alpn: Option<Alpn>,
    remote_id: NodeId,
    side: Side,
    handle: WeakConnectionHandle,
}

impl EndpointHooks for Monitor {
    async fn before_connect(
        &self,
        remote_addr: &iroh::EndpointAddr,
        alpn: &[u8],
    ) -> BeforeConnectOutcome {
        trace!(
            node_id = %remote_addr.id,
            ?alpn,
            addrs = ?remote_addr.addrs,
            "outbound connection attempt"
        );
        BeforeConnectOutcome::Accept
    }

    async fn after_handshake(&self, conn: &Connection) -> AfterHandshakeOutcome {
        let observed = ObservedConnection {
            alpn: conn.alpn().to_vec(),
            remote_id: conn.remote_id(),
            side: conn.side(),
            handle: conn.weak_handle(),
        };
        if let Err(error) = self.tx.try_send(observed) {
            match error {
                TrySendError::Full(_) => trace!("connection monitor channel full"),
                TrySendError::Closed(_) => trace!("connection monitor task is unavailable"),
            }
        }
        AfterHandshakeOutcome::Accept
    }
}

impl Monitor {
    pub fn new() -> Self {
        let connections = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel(MONITOR_CHANNEL_CAPACITY);
        let task_connections = connections.clone();
        let task = tokio::spawn(Self::run(task_connections, rx).instrument(info_span!("watcher")));
        Self {
            connections,
            tx,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    async fn run(
        connections: Arc<tokio::sync::Mutex<Vec<TrackedConnection>>>,
        mut rx: Receiver<ObservedConnection>,
    ) {
        let mut tasks = JoinSet::new();
        let mut next_connection_id = 1u64;

        loop {
            tokio::select! {
                Some(ObservedConnection { alpn, remote_id, side, handle }) = rx.recv() => {
                    let connection_id = next_connection_id;
                    next_connection_id = next_connection_id.saturating_add(1);
                    connections.lock().await.push(TrackedConnection {
                        connection_id,
                        alpn: Alpn::from_bytes(&alpn),
                        remote_id,
                        side,
                        handle: handle.clone(),
                    });

                    let connections = connections.clone();
                    tasks.spawn(async move {
                        let closed = handle.closed().await;
                        trace!(
                            connection_id,
                            remote_id = %remote_id,
                            side = ?side,
                            closed = ?closed.as_ref().map(|closed| &closed.reason),
                            "connection closed"
                        );
                        connections.lock().await.retain(|connection| connection.connection_id != connection_id);
                    }.instrument(tracing::Span::current()));
                }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(error) = result {
                        warn!(?error, "connection close watcher task failed");
                    }
                }
                else => break,
            }
        }
    }

    pub async fn get_status(&self) -> ConnectionMonitorState {
        let mut connections = self.connections.lock().await;
        let mut stale = Vec::new();
        let open_connections = connections
            .iter()
            .filter_map(|connection| {
                let Some(handle) = connection.handle.upgrade() else {
                    stale.push(connection.connection_id);
                    return None;
                };

                let paths = handle.paths();
                let selected_path = paths.iter().find(|path| path.is_selected());
                let selected_address = selected_path
                    .as_ref()
                    .map(|path| transport_addr_to_string(path.remote_addr()));
                let rtt_ms = selected_path
                    .as_ref()
                    .map(|path| path.rtt())
                    .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64);

                Some(OpenConnection {
                    connection_id: connection.connection_id,
                    alpn: connection.alpn,
                    remote_id: connection.remote_id,
                    side: connection.side,
                    selected_address,
                    rtt_ms,
                })
            })
            .collect();

        if !stale.is_empty() {
            connections.retain(|connection| !stale.contains(&connection.connection_id));
        }

        ConnectionMonitorState { open_connections }
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

fn transport_addr_to_string(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(addr) => addr.to_string(),
        TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}
