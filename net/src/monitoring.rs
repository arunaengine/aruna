use aruna_core::{
    alpn::Alpn,
    structs::{ConnectionMonitorState, OpenConnection},
};
use iroh::TransportAddr;
use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, ConnectionInfo, EndpointHooks, Side,
};
use std::sync::Arc;
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
    connections: Arc<Mutex<Vec<TrackedConnection>>>,
    tx: Sender<ObservedConnection>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug, Clone)]
struct ObservedConnection {
    alpn: Vec<u8>,
    remote_id: iroh::EndpointId,
    side: Side,
    handle: ConnectionInfo,
}

#[derive(Debug, Clone)]
struct TrackedConnection {
    connection_id: u64,
    alpn: Option<Alpn>,
    remote_id: iroh::EndpointId,
    side: Side,
    handle: ConnectionInfo,
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

    async fn after_handshake(&self, conn: &ConnectionInfo) -> AfterHandshakeOutcome {
        let info = ObservedConnection {
            alpn: conn.alpn().to_vec(),
            remote_id: conn.remote_id(),
            side: conn.side(),
            handle: conn.clone(),
        };
        if let Err(error) = self.tx.try_send(info) {
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
        let (tx, rx) = tokio::sync::mpsc::channel(MONITOR_CHANNEL_CAPACITY);
        let conn = connections.clone();
        let task = tokio::spawn(Self::run(conn, rx).instrument(info_span!("watcher")));
        Self {
            connections,
            tx,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    async fn run(
        connections: Arc<Mutex<Vec<TrackedConnection>>>,
        mut rx: Receiver<ObservedConnection>,
    ) {
        let mut tasks = JoinSet::new();
        let mut next_connection_id = 1u64;
        let conn = connections.clone();
        loop {
            tokio::select! {
                Some(ObservedConnection { alpn, remote_id, side, handle }) = rx.recv() => {
                    let connection_id = next_connection_id;
                    next_connection_id = next_connection_id.saturating_add(1);
                    conn.lock().await.push(TrackedConnection{ connection_id, alpn: Alpn::from_bytes(&alpn), remote_id, side, handle: handle.clone() });
                    let conn_clone = conn.clone();
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
                        conn_clone.lock().await.retain(|mx| mx.connection_id != connection_id);
                    }.instrument(tracing::Span::current()));
                }
                Some(res) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(error) = res {
                        warn!(?error, "connection close watcher task failed");
                    }
                },
                else => break,
            }
        }
    }

    pub async fn get_status(&self) -> ConnectionMonitorState {
        let connections = self.connections.lock().await;
        ConnectionMonitorState {
            open_connections: connections
                .iter()
                .map(|connection| {
                    let selected_path = connection.handle.selected_path();
                    let selected_address = selected_path
                        .as_ref()
                        .map(|path| transport_addr_to_string(path.remote_addr()));
                    let rtt_ms = selected_path
                        .as_ref()
                        .and_then(|path| path.rtt())
                        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64);

                    OpenConnection {
                        connection_id: connection.connection_id,
                        alpn: connection.alpn,
                        remote_id: connection.remote_id,
                        side: connection.side,
                        selected_address,
                        rtt_ms,
                    }
                })
                .collect(),
        }
    }
}

fn transport_addr_to_string(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(addr) => addr.to_string(),
        TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}
