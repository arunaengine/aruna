use aruna_core::id::{DhtKeyId, NodeId};
use serde::{Deserialize, Serialize};

pub const DHT_ALPN: &[u8] = aruna_core::alpn::Alpn::Dht.as_bytes();

/// Serde helper for NodeId (iroh::PublicKey)
mod node_id_serde {
    use super::NodeId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(key: &NodeId, s: S) -> Result<S::Ok, S::Error> {
        key.as_bytes().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<NodeId, D::Error> {
        let bytes: [u8; 32] = Deserialize::deserialize(d)?;
        NodeId::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

/// Serde helper for Vec<NodeId>
mod node_id_vec_serde {
    use super::NodeId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(keys: &[NodeId], s: S) -> Result<S::Ok, S::Error> {
        let bytes: Vec<[u8; 32]> = keys.iter().map(|k| *k.as_bytes()).collect();
        bytes.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<NodeId>, D::Error> {
        let bytes_vec: Vec<[u8; 32]> = Deserialize::deserialize(d)?;
        bytes_vec
            .into_iter()
            .map(|b| NodeId::from_bytes(&b).map_err(serde::de::Error::custom))
            .collect()
    }
}

/// Serde helper for Option<[u8; 64]> (Ed25519 signature)
mod opt_signature_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(sig: &Option<[u8; 64]>, s: S) -> Result<S::Ok, S::Error> {
        match sig {
            Some(bytes) => {
                let vec: Vec<u8> = bytes.to_vec();
                Some(vec).serialize(s)
            }
            None => None::<Vec<u8>>.serialize(s),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<[u8; 64]>, D::Error> {
        let opt: Option<Vec<u8>> = Deserialize::deserialize(d)?;
        match opt {
            Some(vec) => {
                if vec.len() != 64 {
                    return Err(serde::de::Error::custom("signature must be 64 bytes"));
                }
                let mut arr = [0u8; 64];
                arr.copy_from_slice(&vec);
                Ok(Some(arr))
            }
            None => Ok(None),
        }
    }
}

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
        #[serde(with = "node_id_serde")]
        publisher: NodeId,
        /// Optional Ed25519 signature over (key || value || ttl_secs)
        #[serde(with = "opt_signature_serde")]
        signature: Option<[u8; 64]>,
    },
}

/// DHT RPC response messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DhtResponse {
    Pong,
    /// Response to FindNode - returns node IDs closest to target.
    /// Connection info is looked up via iroh discovery.
    Nodes {
        #[serde(with = "node_id_vec_serde")]
        nodes: Vec<NodeId>,
    },
    Value {
        /// The stored values (may be multiple from different publishers)
        entries: Vec<StoredValue>,
        /// Closer nodes if we don't have the value (or in addition to it)
        #[serde(with = "node_id_vec_serde")]
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
    #[serde(with = "node_id_serde")]
    pub publisher: NodeId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    /// Optional Ed25519 signature for publisher verification
    #[serde(with = "opt_signature_serde")]
    pub signature: Option<[u8; 64]>,
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
