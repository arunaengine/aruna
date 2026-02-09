use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use iroh::Endpoint;
use iroh::endpoint::{RecvStream, SendStream};
use parking_lot::RwLock;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

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

    /// Takes ownership of a stream, removing it from the registry.
    ///
    /// Returns `Some((SendStream, RecvStream))` if the stream exists, is active,
    /// and no concurrent operations are holding references to it.
    ///
    /// Returns `None` if:
    /// - The stream ID doesn't exist
    /// - The stream is already owned or closed
    /// - A concurrent operation (e.g., `send`) is in progress (detected via `Arc::strong_count`)
    ///
    /// This method is safe against race conditions: it checks that no other references
    /// exist before modifying state, ensuring the stream remains usable if ownership
    /// transfer fails.
    pub fn take_owned(&self, id: u64) -> Option<BiStream> {
        let mut streams = self.streams.write();
        match streams.get(&id) {
            Some(StreamState::Active { send, recv }) => {
                // Check that we're the only reference holder before modifying state.
                // If there's a concurrent operation (e.g., send) holding a reference,
                // we must not change the state or the stream becomes unusable.
                if Arc::strong_count(send) != 1 || Arc::strong_count(recv) != 1 {
                    return None; // Concurrent operation in progress
                }
            }
            _ => return None,
        }

        // Now safe to proceed - we verified no concurrent references exist
        if let Some(state) = streams.get_mut(&id) {
            let old = std::mem::replace(state, StreamState::Owned);
            if let StreamState::Active { send, recv } = old {
                // Extract streams from Arc<Mutex> - this will succeed since we verified strong_count == 1
                let send = Arc::try_unwrap(send)
                    .ok()
                    .and_then(|m| m.into_inner().into());
                let recv = Arc::try_unwrap(recv)
                    .ok()
                    .and_then(|m| m.into_inner().into());
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

    /// Send data on the specified stream.
    /// Returns the number of bytes sent on success.
    pub async fn send(&self, id: u64, data: &[u8]) -> Result<usize> {
        // Get a clone of the Arc<Mutex<SendStream>> while holding the read lock briefly
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

        // Now we can await without holding the RwLock
        let mut stream = send_stream.lock().await;
        stream
            .write_all(data)
            .await
            .map_err(|e| NetError::Stream(e.to_string()))?;

        Ok(data.len())
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
    #[allow(dead_code)]
    shutdown: CancellationToken,
}

impl StreamsService {
    pub fn new(endpoint: Endpoint, shutdown: CancellationToken) -> Self {
        Self {
            endpoint,
            registry: Arc::new(StreamRegistry::new()),
            shutdown,
        }
    }

    pub fn registry(&self) -> Arc<StreamRegistry> {
        self.registry.clone()
    }

    pub async fn open(&self, node_id: NodeId, alpn: Alpn) -> Result<u64> {
        // NodeId is now iroh::PublicKey directly - no conversion needed
        let conn = self
            .endpoint
            .connect(node_id, alpn.as_bytes())
            .await
            .map_err(|e| NetError::Connection(e.to_string()))?;

        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::Stream(e.to_string()))?;

        let id = self.registry.register(send, recv);
        Ok(id)
    }

    pub fn take_owned(&self, stream_id: u64) -> Option<BiStream> {
        self.registry.take_owned(stream_id)
    }

    pub fn close(&self, stream_id: u64) {
        self.registry.close(stream_id);
    }

    /// Send data on the specified stream.
    /// Returns the number of bytes sent on success.
    pub async fn send(&self, stream_id: u64, data: Vec<u8>) -> Result<usize> {
        self.registry.send(stream_id, &data).await
    }
}

impl std::fmt::Debug for StreamsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsService")
            .field("registry", &self.registry)
            .finish()
    }
}

/// Run the stream accept loop - spawns handlers for incoming connections
pub async fn run_accept_loop(
    endpoint: Endpoint,
    dht_handler: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    stream_handler: mpsc::Sender<(Alpn, SendStream, RecvStream, NodeId)>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };

                let dht_handler = dht_handler.clone();
                let stream_handler = stream_handler.clone();

                tokio::spawn(async move {
                    // In iroh 0.96, we must accept first, then get ALPN from connection
                    let accepting = match incoming.accept() {
                        Ok(a) => a,
                        Err(_) => return,
                    };

                    let conn = match accepting.await {
                        Ok(c) => c,
                        Err(_) => return,
                    };

                    // Get ALPN from the established connection
                    let alpn_bytes = conn.alpn().to_vec();

                    // NodeId is now iroh::PublicKey directly
                    let peer_id = conn.remote_id();

                    let (send, recv) = match conn.accept_bi().await {
                        Ok(streams) => streams,
                        Err(_) => return,
                    };

                    if alpn_bytes == DHT_ALPN {
                        let _ = dht_handler.send((send, recv, peer_id)).await;
                    } else if let Some(alpn) = Alpn::from_bytes(&alpn_bytes) {
                        let _ = stream_handler.send((alpn, send, recv, peer_id)).await;
                    }
                    // Unknown ALPN - drop silently
                });
            }
        }
    }
}
