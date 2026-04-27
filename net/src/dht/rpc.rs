use aruna_core::alpn::Alpn;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::structs::RealmId;
use serde::{Deserialize, Serialize};

pub const DHT_ALPN: &[u8] = Alpn::Dht.as_bytes();

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
        realm_filter: Option<RealmId>,
    },
    PutValue {
        key: DhtKeyId,
        realm_id: RealmId,
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
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    /// Optional Ed25519 signature for publisher verification
    pub signature: Option<iroh::Signature>,
}

pub fn signed_put_value_bytes(
    key: &DhtKeyId,
    realm_id: &RealmId,
    value: &[u8],
    ttl_secs: u64,
) -> Vec<u8> {
    let mut signed_data = Vec::with_capacity(32 + 32 + value.len() + 8);
    signed_data.extend_from_slice(key.as_bytes());
    signed_data.extend_from_slice(realm_id.as_bytes());
    signed_data.extend_from_slice(value);
    signed_data.extend_from_slice(&ttl_secs.to_le_bytes());
    signed_data
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

    #[test]
    fn test_get_value_request_roundtrip_with_realm_filter() {
        let key = DhtKeyId::from_data(b"realm-filtered-get");
        let realm_id = RealmId::from_bytes([9u8; 32]);

        let req = DhtRequest::GetValue {
            key,
            realm_filter: Some(realm_id),
        };
        let bytes = encode_request(&req).expect("encode request");
        let decoded = decode_request(&bytes).expect("decode request");

        match decoded {
            DhtRequest::GetValue {
                key: decoded_key,
                realm_filter,
            } => {
                assert_eq!(decoded_key, key);
                assert_eq!(realm_filter, Some(realm_id));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn test_put_value_request_roundtrip_with_signature() {
        let publisher_secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
        let publisher = publisher_secret.public();

        let key = DhtKeyId::from_data(b"signed-put");
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let value = b"payload".to_vec();
        let ttl_secs: u64 = 42;

        let signed_data = signed_put_value_bytes(&key, &realm_id, &value, ttl_secs);
        let signature = publisher_secret.sign(&signed_data);

        let req = DhtRequest::PutValue {
            key,
            realm_id,
            value: value.clone(),
            ttl_secs,
            publisher,
            signature: Some(signature),
        };
        let bytes = encode_request(&req).expect("encode request");
        let decoded = decode_request(&bytes).expect("decode request");

        match decoded {
            DhtRequest::PutValue {
                key: decoded_key,
                realm_id: decoded_realm_id,
                value: decoded_value,
                ttl_secs: decoded_ttl,
                publisher: decoded_publisher,
                signature: Some(decoded_signature),
            } => {
                assert_eq!(decoded_key, key);
                assert_eq!(decoded_realm_id, realm_id);
                assert_eq!(decoded_value, value);
                assert_eq!(decoded_ttl, ttl_secs);
                assert_eq!(decoded_publisher, publisher);

                let verify_data = signed_put_value_bytes(
                    &decoded_key,
                    &decoded_realm_id,
                    &decoded_value,
                    decoded_ttl,
                );
                assert!(
                    decoded_publisher
                        .verify(&verify_data, &decoded_signature)
                        .is_ok()
                );
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
