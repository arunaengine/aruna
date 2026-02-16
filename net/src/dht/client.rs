// net/src/dht/client.rs
use aruna_core::id::{DhtKeyId, NodeId};
use iroh::Endpoint;
use std::time::Duration;
use tokio::time::timeout;

use super::rpc::{DHT_ALPN, DhtRequest, DhtResponse, StoredValue, decode_response, encode_request};
use crate::error::{NetError, Result};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_MESSAGE_SIZE: usize = 64 * 1024; // 64KB

/// Client for making DHT RPC calls to other nodes
#[derive(Clone)]
pub struct DhtClient {
    endpoint: Endpoint,
}

impl DhtClient {
    pub fn new(endpoint: Endpoint) -> Self {
        Self { endpoint }
    }

    /// Send a request and get a response from a peer
    async fn rpc(&self, peer: &NodeId, request: DhtRequest) -> Result<DhtResponse> {
        // NodeId is now iroh::PublicKey directly - no conversion needed
        let conn = timeout(RPC_TIMEOUT, self.endpoint.connect(*peer, DHT_ALPN))
            .await
            .map_err(|_| NetError::Timeout(RPC_TIMEOUT))?
            .map_err(|e| NetError::Connection(e.to_string()))?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| NetError::Connection(e.to_string()))?;

        // Send request (length-prefixed)
        let req_bytes = encode_request(&request).map_err(|e| NetError::Dht(e.to_string()))?;
        let len = (req_bytes.len() as u32).to_be_bytes();
        send.write_all(&len)
            .await
            .map_err(|e| NetError::Io(e.to_string()))?;
        send.write_all(&req_bytes)
            .await
            .map_err(|e| NetError::Io(e.to_string()))?;
        send.finish().map_err(|e| NetError::Io(e.to_string()))?;

        // Read response
        let mut len_buf = [0u8; 4];
        timeout(RPC_TIMEOUT, recv.read_exact(&mut len_buf))
            .await
            .map_err(|_| NetError::Timeout(RPC_TIMEOUT))?
            .map_err(|e| NetError::Io(e.to_string()))?;

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(NetError::Dht("response too large".to_string()));
        }

        let mut resp_bytes = vec![0u8; len];
        timeout(RPC_TIMEOUT, recv.read_exact(&mut resp_bytes))
            .await
            .map_err(|_| NetError::Timeout(RPC_TIMEOUT))?
            .map_err(|e| NetError::Io(e.to_string()))?;

        decode_response(&resp_bytes).map_err(|e| NetError::Dht(e.to_string()))
    }

    /// Ping a node to check if it's alive
    pub async fn ping(&self, peer: &NodeId) -> Result<bool> {
        match self.rpc(peer, DhtRequest::Ping).await {
            Ok(DhtResponse::Pong) => Ok(true),
            Ok(_) => Ok(false),
            Err(_) => Ok(false),
        }
    }

    /// Find nodes closest to target (target is raw bytes for XOR distance)
    pub async fn find_node(&self, peer: &NodeId, target: &[u8; 32]) -> Result<Vec<NodeId>> {
        let response = self
            .rpc(peer, DhtRequest::FindNode { target: *target })
            .await?;

        match response {
            DhtResponse::Nodes { nodes } => Ok(nodes),
            DhtResponse::Error { message, .. } => Err(NetError::Dht(message)),
            _ => Err(NetError::Dht("unexpected response".to_string())),
        }
    }

    /// Get value for key from a peer
    pub async fn get_value(
        &self,
        peer: &NodeId,
        key: &DhtKeyId,
    ) -> Result<(Vec<StoredValue>, Vec<NodeId>)> {
        let response = self.rpc(peer, DhtRequest::GetValue { key: *key }).await?;

        match response {
            DhtResponse::Value {
                entries,
                closer_nodes,
            } => Ok((entries, closer_nodes)),
            DhtResponse::Error { message, .. } => Err(NetError::Dht(message)),
            _ => Err(NetError::Dht("unexpected response".to_string())),
        }
    }

    /// Store value at key on a peer
    pub async fn put_value(
        &self,
        peer: &NodeId,
        key: &DhtKeyId,
        value: Vec<u8>,
        ttl_secs: u64,
        publisher: &NodeId,
        signature: Option<iroh::Signature>,
    ) -> Result<()> {
        let response = self
            .rpc(
                peer,
                DhtRequest::PutValue {
                    key: *key,
                    value,
                    ttl_secs,
                    publisher: *publisher,
                    signature,
                },
            )
            .await?;

        match response {
            DhtResponse::Stored => Ok(()),
            DhtResponse::Error { message, .. } => Err(NetError::Dht(message)),
            _ => Err(NetError::Dht("unexpected response".to_string())),
        }
    }
}

impl std::fmt::Debug for DhtClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtClient").finish()
    }
}
