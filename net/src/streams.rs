use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use iroh::Endpoint;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::{NetError, Result};

pub type BiStream = (SendStream, RecvStream);

pub struct StreamsService {
    endpoint: Endpoint,
    #[allow(dead_code)]
    shutdown: CancellationToken,
}

impl StreamsService {
    pub fn new(endpoint: Endpoint, shutdown: CancellationToken) -> Self {
        Self { endpoint, shutdown }
    }

    pub async fn open(&self, node_id: NodeId, alpn: Alpn) -> Result<BiStream> {
        let conn = self
            .endpoint
            .connect(node_id, alpn.as_bytes())
            .await
            .map_err(|e| NetError::Connection(e.to_string()))?;

        conn.open_bi()
            .await
            .map_err(|e| NetError::Stream(e.to_string()))
    }
}

impl std::fmt::Debug for StreamsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsService").finish()
    }
}

pub async fn run_accept_loop(
    endpoint: Endpoint,
    dht_handler: mpsc::Sender<(Connection, SendStream, RecvStream, NodeId)>,
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

                    match Alpn::from_bytes(&alpn_bytes) {
                        Some(Alpn::Dht) => {
                            let (send, recv) = match conn.accept_bi().await {
                                Ok(streams) => streams,
                                Err(_) => return,
                            };
                            if let Err(err) = dht_handler.send((conn, send, recv, peer_id)).await {
                                warn!(
                                    node_id = %peer_id,
                                    error = %err,
                                    "Failed to forward inbound DHT stream"
                                );
                            }
                        }
                        Some(Alpn::Gossip) => {
                            if let Err(err) = gossip_handler.send((conn, peer_id)).await {
                                warn!(
                                    node_id = %peer_id,
                                    error = %err,
                                    "Failed to forward inbound gossip connection"
                                );
                            }
                        }
                        Some(alpn @ (Alpn::Bao | Alpn::Automerge | Alpn::Metadata)) => {
                            let (send, recv) = match conn.accept_bi().await {
                                Ok(streams) => streams,
                                Err(_) => return,
                            };
                            if let Err(err) = stream_handler.send((alpn, send, recv, peer_id)).await {
                                warn!(
                                    node_id = %peer_id,
                                    error = %err,
                                    "Failed to forward inbound app stream"
                                );
                            }
                        }
                        None => {
                            warn!(
                                "Dropping incoming connection with unknown ALPN: {:?}",
                                alpn_bytes
                            );
                        }
                    }
                });
            }
        }
    }
}
