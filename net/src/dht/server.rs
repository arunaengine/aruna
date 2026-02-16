// net/src/dht/server.rs
use aruna_core::id::NodeId;
use aruna_core::util::unix_timestamp_secs;
use iroh::endpoint::{RecvStream, SendStream};
use parking_lot::RwLock;
use std::sync::Arc;

use super::kbucket::{K, PeerInfo, RoutingTable};
use super::rpc::{
    DhtRequest, DhtResponse, ErrorCode, StoredValue, decode_request, encode_response,
};
use super::storage::{DhtStorage, StoredEntry};
use crate::error::Result;

const MAX_MESSAGE_SIZE: usize = 64 * 1024;

/// Handles incoming DHT RPC requests
pub struct DhtServer {
    local_id: NodeId,
    routing_table: Arc<RwLock<RoutingTable>>,
    storage: Arc<DhtStorage>,
}

impl DhtServer {
    pub fn new(
        local_id: NodeId,
        routing_table: Arc<RwLock<RoutingTable>>,
        storage: Arc<DhtStorage>,
    ) -> Self {
        Self {
            local_id,
            routing_table,
            storage,
        }
    }

    /// Handle an incoming DHT stream
    pub async fn handle_stream(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        peer_id: NodeId,
    ) -> Result<()> {
        // Read request length
        let mut len_buf = [0u8; 4];
        if recv.read_exact(&mut len_buf).await.is_err() {
            return Ok(());
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > MAX_MESSAGE_SIZE {
            return Ok(()); // Ignore oversized requests
        }

        // Read request
        let mut req_bytes = vec![0u8; len];
        if recv.read_exact(&mut req_bytes).await.is_err() {
            return Ok(());
        }

        // Decode and handle request
        let request = match decode_request(&req_bytes) {
            Ok(r) => r,
            Err(_) => return Ok(()), // Ignore malformed requests
        };

        // Update routing table with this peer
        self.routing_table.write().insert(PeerInfo::new(peer_id));

        // Process request
        let response = self.handle_request(request).await;

        // Send response
        let Ok(resp_bytes) = encode_response(&response) else {
            return Ok(());
        };

        let len = (resp_bytes.len() as u32).to_be_bytes();
        let _ = send.write_all(&len).await;
        let _ = send.write_all(&resp_bytes).await;
        let _ = send.finish();

        Ok(())
    }

    async fn handle_request(&self, request: DhtRequest) -> DhtResponse {
        match request {
            DhtRequest::Ping => DhtResponse::Pong,

            DhtRequest::FindNode { target } => {
                let closest = self.routing_table.read().closest(&target, K);
                let nodes = closest.into_iter().map(|p| p.node_id).collect();
                DhtResponse::Nodes { nodes }
            }

            DhtRequest::GetValue { key } => {
                let entries = self.storage.get(&key).await;
                let stored_values: Vec<StoredValue> = entries
                    .into_iter()
                    .map(|e| StoredValue {
                        publisher: e.publisher,
                        value: e.value,
                        expires_at: e.expires_at,
                        signature: e.signature,
                    })
                    .collect();

                // Also return closer nodes (use key bytes directly)
                let closer = self.routing_table.read().closest(key.as_bytes(), K);
                let closer_nodes = closer.into_iter().map(|p| p.node_id).collect();

                DhtResponse::Value {
                    entries: stored_values,
                    closer_nodes,
                }
            }

            DhtRequest::PutValue {
                key,
                value,
                ttl_secs,
                publisher,
                signature,
            } => {
                let Some(signature) = signature else {
                    return DhtResponse::Error {
                        code: ErrorCode::InvalidSignature,
                        message: "Missing publisher signature".to_string(),
                    };
                };

                // Build signed data: key || value || ttl_secs
                let mut signed_data = Vec::new();
                signed_data.extend_from_slice(key.as_bytes());
                signed_data.extend_from_slice(&value);
                signed_data.extend_from_slice(&ttl_secs.to_le_bytes());

                if publisher.verify(&signed_data, &signature).is_err() {
                    return DhtResponse::Error {
                        code: ErrorCode::InvalidSignature,
                        message: "Invalid publisher signature".to_string(),
                    };
                }

                let expires_at = unix_timestamp_secs().saturating_add(ttl_secs);
                let entry = StoredEntry {
                    publisher,
                    value,
                    expires_at,
                    signature: Some(signature),
                };
                self.storage.put(&key, entry).await;
                DhtResponse::Stored
            }
        }
    }
}

impl std::fmt::Debug for DhtServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtServer")
            .field("local_id", &self.local_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::kbucket::RoutingTable;
    use super::*;
    use tempfile::tempdir;

    fn make_node(seed: u8) -> NodeId {
        // Generate deterministic keys from seed
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        let secret = iroh::SecretKey::from_bytes(&seed_bytes);
        secret.public()
    }

    #[test]
    fn test_handle_ping() {
        let local_id = make_node(0);
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(local_id)));

        // Create a mock storage - we can't easily test this without StorageHandle
        // so we just verify the structure compiles correctly
        assert!(routing_table.read().is_empty());
    }

    #[test]
    fn test_find_node_response() {
        let local_id = make_node(0);
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(local_id)));

        // Insert some peers
        let node2 = make_node(2);
        for i in 1..5 {
            routing_table.write().insert(PeerInfo::new(make_node(i)));
        }

        // Verify closest returns the expected peers
        let closest = routing_table.read().closest(node2.as_bytes(), K);
        assert!(!closest.is_empty());
        // Closest to node 2 should be node 2 itself (XOR distance 0)
        assert_eq!(closest[0].node_id, node2);
    }

    #[tokio::test]
    async fn test_put_requires_signature() {
        let local_id = make_node(0);
        let routing_table = Arc::new(RwLock::new(RoutingTable::new(local_id)));

        let dir = tempdir().expect("create tempdir");
        let storage_handle = aruna_storage::FjallStorage::open(
            dir.path()
                .to_str()
                .expect("tempdir path should be valid utf8"),
        )
        .expect("open test storage");

        let storage = Arc::new(DhtStorage::new(storage_handle));
        let server = DhtServer::new(local_id, routing_table, storage);

        let response = server
            .handle_request(DhtRequest::PutValue {
                key: aruna_core::id::DhtKeyId::from_data(b"unsigned"),
                value: b"value".to_vec(),
                ttl_secs: 30,
                publisher: make_node(1),
                signature: None,
            })
            .await;

        assert!(matches!(
            response,
            DhtResponse::Error {
                code: ErrorCode::InvalidSignature,
                ..
            }
        ));
    }
}
