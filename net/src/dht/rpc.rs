use aruna_core::id::{DhtKeyId, NodeId};
use serde::{Deserialize, Serialize};

pub const DHT_ALPN: &[u8] = aruna_core::alpn::Alpn::Dht.as_bytes();

/// DHT RPC request messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DhtRequest {
    Ping,
    FindNode {
        /// Target is raw bytes - could be a node ID or key hash
        target: [u8; 32],
    },
    GetValue {
        key: DhtKeyId,
    },
    PutValue {
        key: DhtKeyId,
        value: Vec<u8>,
        ttl_secs: u64,
        publisher: NodeId,
        /// Optional Ed25519 signature over (key || value || ttl_secs)
        signature: Option<iroh::Signature>,
    },
}

/// DHT RPC response messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DhtResponse {
    Pong,
    /// Response to FindNode - returns node IDs closest to target.
    /// Connection info is looked up via iroh discovery.
    Nodes {
        nodes: Vec<NodeId>,
    },
    Value {
        /// The stored values (may be multiple from different publishers)
        entries: Vec<StoredValue>,
        /// Closer nodes if we don't have the value (or in addition to it)
        closer_nodes: Vec<NodeId>,
    },
    Stored,
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    InvalidRequest,
    KeyNotFound,
    StorageFull,
    Internal,
    InvalidSignature,
}

/// A stored DHT value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredValue {
    pub publisher: NodeId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    /// Optional Ed25519 signature for publisher verification
    pub signature: Option<iroh::Signature>,
}

/// Serialize a request to bytes
pub fn encode_request(req: &DhtRequest) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(req)
}

/// Deserialize a request from bytes
pub fn decode_request(bytes: &[u8]) -> Result<DhtRequest, postcard::Error> {
    postcard::from_bytes(bytes)
}

/// Serialize a response to bytes
pub fn encode_response(resp: &DhtResponse) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(resp)
}

/// Deserialize a response from bytes
pub fn decode_response(bytes: &[u8]) -> Result<DhtResponse, postcard::Error> {
    postcard::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(bytes: [u8; 32]) -> NodeId {
        NodeId::from_bytes(&bytes).expect("valid test key")
    }

    #[test]
    fn test_request_roundtrip() {
        let req = DhtRequest::FindNode { target: [42u8; 32] };
        let bytes = encode_request(&req).unwrap();
        let decoded = decode_request(&bytes).unwrap();

        if let DhtRequest::FindNode { target } = decoded {
            assert_eq!(target, [42u8; 32]);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_response_roundtrip() {
        let resp = DhtResponse::Nodes {
            nodes: vec![make_node([1u8; 32])],
        };
        let bytes = encode_response(&resp).unwrap();
        let decoded = decode_response(&bytes).unwrap();

        if let DhtResponse::Nodes { nodes } = decoded {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0], make_node([1u8; 32]));
        } else {
            panic!("wrong variant");
        }
    }
}
