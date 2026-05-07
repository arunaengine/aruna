use aruna_core::{
    alpn::Alpn,
    structs::{ConnectionMonitorState, OpenConnection},
};
use iroh::endpoint::{AfterHandshakeOutcome, ConnectionInfo, EndpointHooks, Side};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    sync::{
        Mutex,
        mpsc::{Receiver, Sender, error::TrySendError},
    },
    task::JoinSet,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, info_span, trace, warn};

const MONITOR_CHANNEL_CAPACITY: usize = 4096;

#[derive(Clone, Debug)]
pub struct Monitor {
    connections: Arc<Mutex<Vec<OpenConnection>>>,
    stats: Arc<MonitorStats>,
    tx: Sender<ObservedConnection>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug, Default)]
struct MonitorStats {
    observed_connections_total: AtomicU64,
    dropped_observations_total: AtomicU64,
    closed_connections_total: AtomicU64,
    close_task_errors_total: AtomicU64,
}

#[derive(Debug, Clone)]
struct ObservedConnection {
    alpn: Vec<u8>,
    remote_id: iroh::EndpointId,
    side: Side,
    handle: ConnectionInfo,
}

impl EndpointHooks for Monitor {
    async fn after_handshake(&self, conn: &ConnectionInfo) -> AfterHandshakeOutcome {
        let info = ObservedConnection {
            alpn: conn.alpn().to_vec(),
            remote_id: conn.remote_id(),
            side: conn.side(),
            handle: conn.clone(),
        };
        self.stats
            .observed_connections_total
            .fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.tx.try_send(info) {
            self.stats
                .dropped_observations_total
                .fetch_add(1, Ordering::Relaxed);
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
        let connections = Arc::new(Mutex::new(Vec::new()));
        let stats = Arc::new(MonitorStats::default());
        let (tx, rx) = tokio::sync::mpsc::channel(MONITOR_CHANNEL_CAPACITY);
        let conn = connections.clone();
        let task =
            tokio::spawn(Self::run(conn, stats.clone(), rx).instrument(info_span!("watcher")));
        Self {
            connections,
            stats,
            tx,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    async fn run(
        connections: Arc<Mutex<Vec<OpenConnection>>>,
        stats: Arc<MonitorStats>,
        mut rx: Receiver<ObservedConnection>,
    ) {
        let mut tasks = JoinSet::new();
        let next_connection_id = AtomicU64::new(1);
        let conn = connections.clone();
        loop {
            tokio::select! {
                Some(ObservedConnection { alpn, remote_id, side, handle }) = rx.recv() => {
                    let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                    conn.lock().await.push(OpenConnection{ connection_id, alpn: Alpn::from_bytes(&alpn), remote_id, side });
                    let conn_clone = conn.clone();
                    let stats = stats.clone();
                    tasks.spawn(async move {
                        match handle.closed().await {
                            Some((error, state)) => {
                                // We have access to the final stats of the connection!
                                trace!(%remote_id, ?alpn, ?error, udp_rx=state.udp_rx.bytes, udp_tx=state.udp_tx.bytes, "connection closed");
                            }
                            None => {
                                // The connection was closed before we could register our stats-on-close listener.
                                trace!(%remote_id, ?alpn, "connection closed before tracking started");
                            }

                        };
                        stats.closed_connections_total.fetch_add(1, Ordering::Relaxed);
                        conn_clone.lock().await.retain(|mx| mx.connection_id != connection_id);
                    }.instrument(tracing::Span::current()));
                }
                Some(res) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(error) = res {
                        stats.close_task_errors_total.fetch_add(1, Ordering::Relaxed);
                        warn!(?error, "connection close watcher task failed");
                    }
                },
                else => break,
            }
        }
    }

    pub async fn get_status(&self) -> ConnectionMonitorState {
        ConnectionMonitorState {
            open_connections: self.connections.lock().await.clone(),
            observed_connections_total: self
                .stats
                .observed_connections_total
                .load(Ordering::Relaxed),
            dropped_observations_total: self
                .stats
                .dropped_observations_total
                .load(Ordering::Relaxed),
            closed_connections_total: self.stats.closed_connections_total.load(Ordering::Relaxed),
            close_task_errors_total: self.stats.close_task_errors_total.load(Ordering::Relaxed),
        }
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}
