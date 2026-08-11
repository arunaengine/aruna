use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use aruna_core::alpn::Alpn;
use aruna_core::id::NodeId;
use aruna_core::structs::{ConnectionMonitorState, OpenConnection};
use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks, Side,
    WeakConnectionHandle,
};
use iroh::{Endpoint, TransportAddr};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, debug, info_span, trace, warn};

const MONITOR_CHANNEL_CAPACITY: usize = 4096;
const PARKED_IDLE_TIMER_SECS: u64 = 365 * 24 * 60 * 60;
// Matches the initial peer retry interval, so a cooled peer is re-probed on the
// same cadence the connectivity manager already uses.
const FAILURE_COOLDOWN: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct ConnectionPoolOptions {
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_connections: usize,
    /// How long an eligible connect failure suppresses new dials for its
    /// `(NodeId, Alpn)` key. A latency optimization only: it never proves a peer
    /// is offline and never grants health, membership, or write authority.
    pub failure_cooldown: Duration,
}

impl Default for ConnectionPoolOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            max_connections: 128,
            failure_cooldown: FAILURE_COOLDOWN,
        }
    }
}

/// Attempt counters behind aggregate and bounded per-key diagnostics.
#[derive(Debug)]
struct PoolCounters {
    dials: AtomicU64,
    cooldown_hits: AtomicU64,
    cooldown_records: AtomicU64,
    cooldown_expiries: AtomicU64,
    by_key: Mutex<HashMap<ConnectionKey, KeyCounters>>,
    capacity: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolCounts {
    pub dials: u64,
    pub cooldown_hits: u64,
    pub cooldown_records: u64,
    pub cooldown_expiries: u64,
}

#[derive(Debug, Default)]
struct KeyCounters {
    dials: u64,
    cooldown_hits: u64,
    cooldown_records: u64,
    cooldown_expiries: u64,
}

impl PoolCounters {
    fn new(capacity: usize) -> Self {
        Self {
            dials: AtomicU64::new(0),
            cooldown_hits: AtomicU64::new(0),
            cooldown_records: AtomicU64::new(0),
            cooldown_expiries: AtomicU64::new(0),
            by_key: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    fn update_key(&self, key: ConnectionKey, update: impl FnOnce(&mut KeyCounters)) {
        if self.capacity == 0 {
            return;
        }
        let mut by_key = self.by_key.lock();
        if !by_key.contains_key(&key) && by_key.len() >= self.capacity {
            let Some(oldest) = by_key.keys().next().copied() else {
                return;
            };
            by_key.remove(&oldest);
        }
        update(by_key.entry(key).or_default());
    }

    fn record_dial(&self, key: ConnectionKey) {
        self.dials.fetch_add(1, Ordering::Relaxed);
        self.update_key(key, |counts| counts.dials += 1);
    }

    fn record_hit(&self, key: ConnectionKey) {
        self.cooldown_hits.fetch_add(1, Ordering::Relaxed);
        self.update_key(key, |counts| counts.cooldown_hits += 1);
    }

    fn record_failure(&self, key: ConnectionKey) {
        self.cooldown_records.fetch_add(1, Ordering::Relaxed);
        self.update_key(key, |counts| counts.cooldown_records += 1);
    }

    fn record_expiry(&self, key: ConnectionKey) {
        self.cooldown_expiries.fetch_add(1, Ordering::Relaxed);
        self.update_key(key, |counts| counts.cooldown_expiries += 1);
    }

    fn counts_for(&self, key: ConnectionKey) -> PoolCounts {
        let by_key = self.by_key.lock();
        let Some(counts) = by_key.get(&key) else {
            return PoolCounts::default();
        };
        PoolCounts {
            dials: counts.dials,
            cooldown_hits: counts.cooldown_hits,
            cooldown_records: counts.cooldown_records,
            cooldown_expiries: counts.cooldown_expiries,
        }
    }
}

impl Default for PoolCounters {
    fn default() -> Self {
        Self::new(0)
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
    ConnectionIdle {
        key: ConnectionKey,
    },
    ConnectionClosed {
        key: ConnectionKey,
    },
    ConnectionFailed {
        key: ConnectionKey,
        error: PoolConnectError,
    },
    ConnectionReady {
        key: ConnectionKey,
    },
    ClearFailures {
        node_id: NodeId,
    },
    #[cfg(test)]
    Barrier(oneshot::Sender<()>),
    #[cfg(test)]
    Hold {
        ready: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    Shutdown,
}

/// Only a transport-connect failure or a connect timeout is evidence about
/// reachability. Shutdown, local capacity, and caller-side failures say nothing
/// about the peer and must never start a cooldown.
fn cooldown_eligible(error: &PoolConnectError) -> bool {
    matches!(
        error,
        PoolConnectError::Connection(_) | PoolConnectError::Timeout
    )
}

#[derive(Debug, Clone)]
struct FailureEntry {
    error: PoolConnectError,
    recorded: Instant,
    expires: Instant,
}

#[derive(Debug)]
struct FailureCache {
    entries: HashMap<ConnectionKey, FailureEntry>,
    cooldown: Duration,
    capacity: usize,
    counters: Arc<PoolCounters>,
}

impl FailureCache {
    fn new(cooldown: Duration, capacity: usize, counters: Arc<PoolCounters>) -> Self {
        Self {
            entries: HashMap::new(),
            cooldown,
            capacity,
            counters,
        }
    }

    fn record(&mut self, key: ConnectionKey, error: PoolConnectError, now: Instant) {
        if self.cooldown.is_zero() || self.capacity == 0 || !cooldown_eligible(&error) {
            return;
        }
        self.evict(now);
        self.entries.insert(
            key,
            FailureEntry {
                error,
                recorded: now,
                expires: now + self.cooldown,
            },
        );
        self.counters.record_failure(key);
    }

    /// Returns the cached error while the key is still cooling. An expired entry
    /// is dropped here, so exactly one caller re-probes after expiry.
    fn hit(&mut self, key: &ConnectionKey, now: Instant) -> Option<PoolConnectError> {
        let entry = self.entries.get(key)?;
        if entry.expires > now {
            self.counters.record_hit(*key);
            return Some(entry.error.clone());
        }
        self.entries.remove(key);
        self.counters.record_expiry(*key);
        None
    }

    fn clear(&mut self, key: &ConnectionKey) {
        self.entries.remove(key);
    }

    fn clear_node(&mut self, node_id: NodeId) {
        self.entries.retain(|key, _| key.node_id != node_id);
    }

    /// Keeps the retained failures inside the pool's connection bound, dropping
    /// expired entries before the oldest live one.
    fn evict(&mut self, now: Instant) {
        if self.entries.len() < self.capacity {
            return;
        }
        self.entries.retain(|_, entry| entry.expires > now);
        while self.entries.len() >= self.capacity {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.recorded)
                .map(|(key, _)| *key)
            else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }
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
        self.owner.counters.record_dial(key);
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

        match &state {
            Err(error) => {
                debug!(
                    peer = %key.node_id,
                    alpn = %key.alpn,
                    error = %error,
                    outcome = if cooldown_eligible(error) { "dial_failed_cooled" } else { "dial_failed" },
                    "pooled connection attempt failed"
                );
                let _ = self.owner.record_failure(key, error.clone()).await;
            }
            Ok(_) => {
                let _ = self.owner.connection_ready(key).await;
            }
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
    failures: FailureCache,
    context: Arc<PoolContext>,
}

impl Actor {
    fn new(
        endpoint: Endpoint,
        options: ConnectionPoolOptions,
        counters: Arc<PoolCounters>,
    ) -> (Self, Sender<ActorMessage>) {
        let (tx, rx) = mpsc::channel(256);
        let request_timeout = options.connect_timeout;
        let failures = FailureCache::new(
            options.failure_cooldown,
            options.max_connections,
            counters.clone(),
        );
        let context = Arc::new(PoolContext {
            endpoint,
            options,
            owner: ConnectionPool {
                tx: tx.clone(),
                request_timeout,
                counters,
            },
        });
        (
            Self {
                rx,
                connections: HashMap::new(),
                idle: VecDeque::new(),
                failures,
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
                ActorMessage::ConnectionFailed { key, error } => {
                    self.failures.record(key, error, Instant::now());
                    self.remove_connection(key);
                }
                ActorMessage::ConnectionReady { key } => self.failures.clear(&key),
                ActorMessage::ClearFailures { node_id } => self.failures.clear_node(node_id),
                #[cfg(test)]
                ActorMessage::Barrier(done) => {
                    let _ = done.send(());
                }
                #[cfg(test)]
                ActorMessage::Hold { ready, release } => {
                    let _ = ready.send(());
                    let _ = release.await;
                }
                ActorMessage::Shutdown => break,
            }
        }
        self.connections.clear();
        self.idle.clear();
        self.failures.entries.clear();
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

        if let Some(error) = self.failures.hit(&key, Instant::now()) {
            debug!(
                peer = %key.node_id,
                alpn = %key.alpn,
                outcome = "cooldown_hit",
                "suppressing dial to a recently unreachable peer"
            );
            let _ = request.tx.send(Err(error));
            return;
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
    request_timeout: Duration,
    counters: Arc<PoolCounters>,
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool").finish()
    }
}

impl ConnectionPool {
    pub fn new(endpoint: Endpoint, options: ConnectionPoolOptions) -> Self {
        let request_timeout = options.connect_timeout;
        let counters = Arc::new(PoolCounters::new(options.max_connections));
        let (actor, tx) = Actor::new(endpoint, options, counters.clone());
        tokio::spawn(actor.run());
        Self {
            tx,
            request_timeout,
            counters,
        }
    }

    pub fn counts(&self) -> PoolCounts {
        PoolCounts {
            dials: self.counters.dials.load(Ordering::Relaxed),
            cooldown_hits: self.counters.cooldown_hits.load(Ordering::Relaxed),
            cooldown_records: self.counters.cooldown_records.load(Ordering::Relaxed),
            cooldown_expiries: self.counters.cooldown_expiries.load(Ordering::Relaxed),
        }
    }

    pub fn counts_for(&self, node_id: NodeId, alpn: Alpn) -> PoolCounts {
        self.counters.counts_for(ConnectionKey { node_id, alpn })
    }

    /// Drops every cooldown for a peer whose endpoint address was just
    /// validated and installed, so the next request dials the new address.
    pub async fn clear_failures(
        &self,
        node_id: NodeId,
    ) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ClearFailures { node_id })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)
    }

    pub async fn get_or_connect(
        &self,
        node_id: NodeId,
        alpn: Alpn,
    ) -> std::result::Result<ConnectionLease, PoolConnectError> {
        let key = ConnectionKey { node_id, alpn };
        let (tx, rx) = oneshot::channel();
        match tokio::time::timeout(self.request_timeout, async {
            self.tx
                .send(ActorMessage::RequestLease(RequestLease { key, tx }))
                .await
                .map_err(|_| PoolConnectError::Shutdown)?;
            rx.await.map_err(|_| PoolConnectError::Shutdown)?
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(PoolConnectError::Timeout),
        }
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

    async fn record_failure(
        &self,
        key: ConnectionKey,
        error: PoolConnectError,
    ) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionFailed { key, error })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)
    }

    async fn connection_ready(
        &self,
        key: ConnectionKey,
    ) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionReady { key })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn key(seed: u8, alpn: Alpn) -> ConnectionKey {
        ConnectionKey {
            node_id: node(seed),
            alpn,
        }
    }

    fn cache(cooldown: Duration, capacity: usize) -> FailureCache {
        FailureCache::new(cooldown, capacity, Arc::new(PoolCounters::default()))
    }

    async fn test_endpoint() -> Endpoint {
        Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr(
                "127.0.0.1:0"
                    .parse::<std::net::SocketAddr>()
                    .expect("valid bind addr"),
            )
            .expect("valid bind addr")
            .bind()
            .await
            .expect("endpoint binds")
    }

    async fn pending_endpoint(peer: NodeId) -> (Endpoint, tokio::net::UdpSocket) {
        let lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("blackhole socket");
        lookup.add_endpoint_info(iroh::EndpointAddr::from_parts(
            peer,
            [TransportAddr::Ip(
                blackhole.local_addr().expect("blackhole address"),
            )],
        ));
        let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .address_lookup(lookup)
            .bind_addr(
                "127.0.0.1:0"
                    .parse::<std::net::SocketAddr>()
                    .expect("valid bind addr"),
            )
            .expect("valid bind addr")
            .bind()
            .await
            .expect("endpoint binds");
        (endpoint, blackhole)
    }

    async fn actor_barrier(pool: &ConnectionPool) {
        let (done, ready) = oneshot::channel();
        pool.tx
            .send(ActorMessage::Barrier(done))
            .await
            .expect("pool actor");
        ready.await.expect("pool barrier");
    }

    async fn hold_actor(pool: &ConnectionPool) -> oneshot::Sender<()> {
        let (ready, held) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        pool.tx
            .send(ActorMessage::Hold {
                ready,
                release: wait,
            })
            .await
            .expect("pool actor");
        held.await.expect("pool hold");
        release
    }

    async fn wait_dial(pool: &ConnectionPool, before: u64) {
        for _ in 0..1024 {
            if pool.counts().dials > before {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("connection actor did not start");
    }

    fn fill_mailbox(pool: &ConnectionPool, key: ConnectionKey) {
        let mut full = false;
        for _ in 0..512 {
            let (reply, _response) = oneshot::channel();
            let request = RequestLease { key, tx: reply };
            match pool.tx.try_send(ActorMessage::RequestLease(request)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    full = true;
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => panic!("pool actor stopped"),
            }
        }
        assert!(full, "pool actor mailbox did not saturate");
    }

    #[tokio::test]
    async fn lease_request_times_out_when_actor_does_not_reply() {
        let (tx, _rx) = mpsc::channel(1);
        let pool = ConnectionPool {
            tx,
            request_timeout: Duration::from_millis(10),
            counters: Arc::new(PoolCounters::default()),
        };

        let result = pool.get_or_connect(node(1), Alpn::Bao).await;

        assert!(matches!(result, Err(PoolConnectError::Timeout)));
    }

    #[tokio::test(start_paused = true)]
    async fn caches_transport_only() {
        // Only reachability evidence may cool a key; a shutdown or a local
        // capacity refusal says nothing about the peer.
        let mut cache = cache(Duration::from_secs(5), 8);
        let now = Instant::now();

        cache.record(key(1, Alpn::Bao), PoolConnectError::Shutdown, now);
        cache.record(key(2, Alpn::Bao), PoolConnectError::TooManyConnections, now);
        cache.record(key(3, Alpn::Bao), PoolConnectError::Timeout, now);
        cache.record(
            key(4, Alpn::Bao),
            PoolConnectError::Connection("refused".to_string()),
            now,
        );

        assert!(cache.hit(&key(1, Alpn::Bao), now).is_none());
        assert!(cache.hit(&key(2, Alpn::Bao), now).is_none());
        assert!(matches!(
            cache.hit(&key(3, Alpn::Bao), now),
            Some(PoolConnectError::Timeout)
        ));
        assert!(matches!(
            cache.hit(&key(4, Alpn::Bao), now),
            Some(PoolConnectError::Connection(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn keys_by_alpn() {
        let mut cache = cache(Duration::from_secs(5), 8);
        let now = Instant::now();

        cache.record(key(1, Alpn::Bao), PoolConnectError::Timeout, now);

        assert!(cache.hit(&key(1, Alpn::Bao), now).is_some());
        assert!(cache.hit(&key(1, Alpn::Metadata), now).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn expires_once() {
        let mut cache = cache(Duration::from_secs(5), 8);
        let start = Instant::now();
        cache.record(key(1, Alpn::Bao), PoolConnectError::Timeout, start);

        assert!(cache.hit(&key(1, Alpn::Bao), start).is_some());
        let expired = start + Duration::from_secs(5);
        assert!(cache.hit(&key(1, Alpn::Bao), expired).is_none());
        // The expired entry is gone, so the re-probe is not repeated per caller.
        assert!(cache.hit(&key(1, Alpn::Bao), expired).is_none());
        assert_eq!(cache.counters.cooldown_expiries.load(Ordering::Relaxed), 1);
        assert_eq!(cache.counters.cooldown_hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn evicts_expired_first() {
        let mut cache = cache(Duration::from_secs(5), 2);
        let start = Instant::now();
        cache.record(key(1, Alpn::Bao), PoolConnectError::Timeout, start);
        cache.record(key(2, Alpn::Bao), PoolConnectError::Timeout, start);

        let later = start + Duration::from_secs(6);
        cache.record(key(3, Alpn::Bao), PoolConnectError::Timeout, later);

        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.contains_key(&key(3, Alpn::Bao)));

        cache.record(
            key(4, Alpn::Bao),
            PoolConnectError::Timeout,
            later + Duration::from_secs(1),
        );
        cache.record(
            key(5, Alpn::Bao),
            PoolConnectError::Timeout,
            later + Duration::from_secs(2),
        );

        assert_eq!(cache.entries.len(), 2);
        assert!(!cache.entries.contains_key(&key(3, Alpn::Bao)));
    }

    #[tokio::test(start_paused = true)]
    async fn clears_by_node() {
        let mut cache = cache(Duration::from_secs(5), 8);
        let now = Instant::now();
        cache.record(key(1, Alpn::Bao), PoolConnectError::Timeout, now);
        cache.record(key(1, Alpn::Metadata), PoolConnectError::Timeout, now);
        cache.record(key(2, Alpn::Bao), PoolConnectError::Timeout, now);

        cache.clear_node(node(1));

        assert!(cache.hit(&key(1, Alpn::Bao), now).is_none());
        assert!(cache.hit(&key(1, Alpn::Metadata), now).is_none());
        assert!(cache.hit(&key(2, Alpn::Bao), now).is_some());
    }

    #[tokio::test]
    async fn dials_once() {
        // An unreachable peer must cost one dial for the whole cooldown, and the
        // suppressed calls must return the same retryable error class.
        let pool = ConnectionPool::new(
            test_endpoint().await,
            ConnectionPoolOptions {
                failure_cooldown: Duration::from_secs(30),
                ..ConnectionPoolOptions::default()
            },
        );
        let peer = node(9);

        let first = pool.get_or_connect(peer, Alpn::Bao).await;
        assert!(first.is_err());
        let second = pool.get_or_connect(peer, Alpn::Bao).await;
        assert!(second.is_err());

        let counts = pool.counts();
        assert_eq!(counts.dials, 1);
        assert_eq!(counts.cooldown_records, 1);
        assert_eq!(counts.cooldown_hits, 1);

        // A validated endpoint address replaces the cooldown with one re-probe.
        pool.clear_failures(peer).await.unwrap();
        let mut requests = JoinSet::new();
        for _ in 0..8 {
            let pool = pool.clone();
            requests.spawn(async move { pool.get_or_connect(peer, Alpn::Bao).await.is_err() });
        }
        while let Some(result) = requests.join_next().await {
            assert!(result.expect("request task"));
        }
        assert_eq!(pool.counts().dials, 2);
    }

    #[tokio::test]
    async fn clear_after_pressure() {
        let blocked = node(20);
        let target = node(21);
        let (endpoint, _blackhole) = pending_endpoint(blocked).await;
        tokio::time::pause();
        let options = ConnectionPoolOptions {
            connect_timeout: Duration::from_secs(30),
            failure_cooldown: Duration::from_secs(30),
            ..ConnectionPoolOptions::default()
        };
        let connect_timeout = options.connect_timeout;
        let pool = ConnectionPool::new(endpoint.clone(), options);
        pool.record_failure(
            ConnectionKey {
                node_id: target,
                alpn: Alpn::Bao,
            },
            PoolConnectError::Connection("offline".to_string()),
        )
        .await
        .expect("record failure");
        actor_barrier(&pool).await;

        let before = pool.counts();
        let blocked_pool = pool.clone();
        let blocked_task =
            tokio::spawn(async move { blocked_pool.get_or_connect(blocked, Alpn::Bao).await });
        wait_dial(&pool, before.dials).await;
        let release = hold_actor(&pool).await;
        fill_mailbox(
            &pool,
            ConnectionKey {
                node_id: blocked,
                alpn: Alpn::Bao,
            },
        );

        let clear_pool = pool.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let clear = tokio::spawn(async move {
            let _ = started_tx.send(());
            clear_pool.clear_failures(target).await
        });
        started_rx.await.expect("clear task started");
        assert_eq!(pool.tx.capacity(), 0);
        assert!(!clear.is_finished());
        release.send(()).expect("release pool actor");
        tokio::time::advance(connect_timeout + Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(5), clear)
            .await
            .expect("clear request made progress")
            .expect("clear task")
            .expect("clear failures");
        actor_barrier(&pool).await;
        assert!(blocked_task.await.expect("blocked request").is_err());

        let before_group = pool.counts_for(target, Alpn::Bao);
        let mut requests = JoinSet::new();
        for _ in 0..8 {
            let pool = pool.clone();
            requests.spawn(async move { pool.get_or_connect(target, Alpn::Bao).await });
        }
        while let Some(result) = requests.join_next().await {
            assert!(result.expect("request task").is_err());
        }
        actor_barrier(&pool).await;
        let after_group = pool.counts_for(target, Alpn::Bao);
        assert_eq!(after_group.dials - before_group.dials, 1);

        pool.shutdown().await.expect("pool shutdown");
        endpoint.close().await;
    }

    #[tokio::test]
    async fn clear_waits_capacity() {
        let (tx, mut rx) = mpsc::channel(1);
        let first = node(1);
        let second = node(2);
        tx.send(ActorMessage::ClearFailures { node_id: first })
            .await
            .unwrap();
        let pool = ConnectionPool {
            tx,
            request_timeout: Duration::from_secs(1),
            counters: Arc::new(PoolCounters::default()),
        };

        let pending = tokio::spawn(async move { pool.clear_failures(second).await });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        assert!(matches!(
            rx.recv().await,
            Some(ActorMessage::ClearFailures { node_id }) if node_id == first
        ));
        pending.await.unwrap().unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(ActorMessage::ClearFailures { node_id }) if node_id == second
        ));
    }

    #[tokio::test]
    async fn coalesces_requests() {
        let pool = ConnectionPool::new(
            test_endpoint().await,
            ConnectionPoolOptions {
                failure_cooldown: Duration::from_secs(30),
                ..ConnectionPoolOptions::default()
            },
        );
        let peer = node(10);

        let mut requests = JoinSet::new();
        for _ in 0..8 {
            let pool = pool.clone();
            requests.spawn(async move { pool.get_or_connect(peer, Alpn::Bao).await.is_err() });
        }
        while let Some(result) = requests.join_next().await {
            assert!(result.expect("request task"));
        }

        assert_eq!(pool.counts().dials, 1);
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
