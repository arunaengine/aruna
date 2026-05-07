use aruna_core::{alpn::Alpn, structs::OpenConnection};
use iroh::endpoint::{AfterHandshakeOutcome, ConnectionInfo, EndpointHooks, Side};
use std::sync::Arc;
use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinSet,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, info_span, trace};

#[derive(Clone, Debug)]
pub struct Monitor {
    connections: Arc<Mutex<Vec<OpenConnection>>>,
    tx: UnboundedSender<ObservedConnection>,
    _task: Arc<AbortOnDropHandle<()>>,
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
        self.tx.send(info).ok();
        AfterHandshakeOutcome::Accept
    }
}

impl Monitor {
    pub fn new() -> Self {
        let connections = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let conn = connections.clone();
        let task = tokio::spawn(Self::run(conn, rx).instrument(info_span!("watcher")));
        Self {
            connections,
            tx,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    async fn run(
        connections: Arc<Mutex<Vec<OpenConnection>>>,
        mut rx: UnboundedReceiver<ObservedConnection>,
    ) {
        let mut tasks = JoinSet::new();
        let conn = connections.clone();
        loop {
            tokio::select! {
                Some(ObservedConnection { alpn, remote_id, side, handle }) = rx.recv() => {
                    conn.lock().await.push(OpenConnection{ alpn: Alpn::from_bytes(&alpn), remote_id: remote_id.clone(), side });
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
                        conn_clone.lock().await.retain(|mx| mx.remote_id != remote_id);
                    }.instrument(tracing::Span::current()));
                }
                Some(res) = tasks.join_next(), if !tasks.is_empty() => res.expect("conn close task panicked"),
                else => break,
            }
            while let Some(res) = tasks.join_next().await {
                res.expect("conn close task panicked");
            }
        }
    }

    pub async fn get_connections(&self) -> Vec<OpenConnection> {
        self.connections.lock().await.clone()
    }
}
