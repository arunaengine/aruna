use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use iroh::Endpoint;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh_gossip::net::GOSSIP_ALPN;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::dht::rpc::DHT_ALPN;
use crate::error::{NetError, Result};

pub type BiStream = (SendStream, RecvStream);

pub struct StreamRegistry {
    streams: RwLock<HashMap<u64, StreamState>>,
    next_id: AtomicU64,
}

enum StreamState {
    Active {
        send: Arc<Mutex<SendStream>>,
        recv: Arc<Mutex<RecvStream>>,
    },
    Owned,
    Closed,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn register(&self, send: SendStream, recv: RecvStream) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.streams.write().insert(
            id,
            StreamState::Active {
                send: Arc::new(Mutex::new(send)),
                recv: Arc::new(Mutex::new(recv)),
            },
        );
        id
    }

    pub fn take_owned(&self, id: u64) -> Option<BiStream> {
        let mut streams = self.streams.write();
        match streams.get(&id) {
            Some(StreamState::Active { send, recv }) => {
                if Arc::strong_count(send) != 1 || Arc::strong_count(recv) != 1 {
                    return None;
                }
            }
            _ => return None,
        }

        if let Some(state) = streams.get_mut(&id) {
            let old = std::mem::replace(state, StreamState::Owned);
            if let StreamState::Active { send, recv } = old {
                let send = Arc::try_unwrap(send).ok().map(|m| m.into_inner());
                let recv = Arc::try_unwrap(recv).ok().map(|m| m.into_inner());
                if let (Some(send), Some(recv)) = (send, recv) {
                    return Some((send, recv));
                }
            }
        }

        None
    }

    pub fn close(&self, id: u64) {
        self.streams.write().insert(id, StreamState::Closed);
    }

    pub async fn send(&self, id: u64, data: &[u8]) -> Result<usize> {
        let send_stream = {
            let streams = self.streams.read();
            match streams.get(&id) {
                Some(StreamState::Active { send, .. }) => send.clone(),
                Some(StreamState::Owned) => {
                    return Err(NetError::Stream("Stream is owned".to_string()));
                }
                Some(StreamState::Closed) | None => {
                    return Err(NetError::Stream("Stream not found or closed".to_string()));
                }
            }
        };

        let mut stream = send_stream.lock().await;
        stream
            .write_all(data)
            .await
            .map_err(|e| NetError::Stream(e.to_string()))?;

        Ok(data.len())
    }

    pub async fn recv(&self, id: u64, max_bytes: usize) -> Result<Vec<u8>> {
        let recv_stream = {
            let streams = self.streams.read();
            match streams.get(&id) {
                Some(StreamState::Active { recv, .. }) => recv.clone(),
                Some(StreamState::Owned) => {
                    return Err(NetError::Stream("Stream is owned".to_string()));
                }
                Some(StreamState::Closed) | None => {
                    return Err(NetError::Stream("Stream not found or closed".to_string()));
                }
            }
        };

        let mut stream = recv_stream.lock().await;
        match stream
            .read_chunk(max_bytes.max(1))
            .await
            .map_err(|e| NetError::Stream(e.to_string()))?
        {
            Some(chunk) => Ok(chunk.bytes.to_vec()),
            None => Err(NetError::Stream("Stream closed".to_string())),
        }
    }
}

impl Default for StreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for StreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRegistry")
            .field("stream_count", &self.streams.read().len())
            .finish()
    }
}

pub struct StreamsService {
    endpoint: Endpoint,
    registry: Arc<StreamRegistry>,
    owned_streams: Arc<ParkingMutex<HashMap<u64, BiStream>>>,
    #[allow(dead_code)]
    shutdown: CancellationToken,
}

impl StreamsService {
    pub fn new(endpoint: Endpoint, shutdown: CancellationToken) -> Self {
        Self {
            endpoint,
            registry: Arc::new(StreamRegistry::new()),
            owned_streams: Arc::new(ParkingMutex::new(HashMap::new())),
            shutdown,
        }
    }

    pub fn registry(&self) -> Arc<StreamRegistry> {
        self.registry.clone()
    }

    pub async fn open(&self, node_id: NodeId, alpn: Alpn) -> Result<u64> {
        let conn = self
            .endpoint
            .connect(node_id, alpn.as_bytes())
            .await
            .map_err(|e| NetError::Connection(e.to_string()))?;

        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::Stream(e.to_string()))?;

        Ok(self.registry.register(send, recv))
    }

    pub fn request_owned(&self, stream_id: u64) -> bool {
        if self.owned_streams.lock().contains_key(&stream_id) {
            return true;
        }

        if let Some(streams) = self.registry.take_owned(stream_id) {
            self.owned_streams.lock().insert(stream_id, streams);
            true
        } else {
            false
        }
    }

    pub fn take_owned_stream(&self, stream_id: u64) -> Option<BiStream> {
        self.owned_streams.lock().remove(&stream_id)
    }

    pub fn close(&self, stream_id: u64) {
        self.registry.close(stream_id);
        self.owned_streams.lock().remove(&stream_id);
    }

    pub async fn send(&self, stream_id: u64, data: Vec<u8>) -> Result<usize> {
        self.registry.send(stream_id, &data).await
    }

    pub async fn recv(&self, stream_id: u64, max_bytes: usize) -> Result<Vec<u8>> {
        self.registry.recv(stream_id, max_bytes).await
    }
}

impl std::fmt::Debug for StreamsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsService")
            .field("registry", &self.registry)
            .finish()
    }
}

pub async fn run_accept_loop(
    endpoint: Endpoint,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    gossip_handler: mpsc::Sender<(Connection, NodeId)>,
    stream_handler: mpsc::Sender<(Alpn, SendStream, RecvStream, NodeId)>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };

                let dht_handler = dht_handler.clone();
                let gossip_handler = gossip_handler.clone();
                let stream_handler = stream_handler.clone();

                tokio::spawn(async move {
                    let accepting = match incoming.accept() {
                        Ok(accepting) => accepting,
                        Err(_) => return,
                    };

                    let conn = match accepting.await {
                        Ok(conn) => conn,
                        Err(_) => return,
                    };

                    let alpn_bytes = conn.alpn().to_vec();
                    let peer_id = conn.remote_id();

                    if alpn_bytes == DHT_ALPN {
                        let (send, recv) = match conn.accept_bi().await {
                            Ok(streams) => streams,
                            Err(_) => return,
                        };
                        let _ = dht_handler.send((send, recv, peer_id)).await;
                    } else if alpn_bytes == GOSSIP_ALPN {
                        let _ = gossip_handler.send((conn, peer_id)).await;
                    } else if let Some(alpn) = Alpn::from_bytes(&alpn_bytes) {
                        let (send, recv) = match conn.accept_bi().await {
                            Ok(streams) => streams,
                            Err(_) => return,
                        };
                        let _ = stream_handler.send((alpn, send, recv, peer_id)).await;
                    } else {
                        warn!(
                            "Dropping incoming connection with unknown ALPN: {:?}",
                            alpn_bytes
                        );
                    }
                });
            }
        }
    }
}
