use aruna_core::DistributedTraceContext;
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
        expires_at: u64,
        revision: u64,
        publisher: NodeId,
        /// Required Ed25519 signature over the complete record envelope.
        signature: Option<iroh::Signature>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DhtRequestEnvelope {
    trace_context: Option<DistributedTraceContext>,
    request: DhtRequest,
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
    pub revision: u64,
    pub signature: iroh::Signature,
}

pub fn signed_record_bytes(
    key: &DhtKeyId,
    publisher: &NodeId,
    realm_id: &RealmId,
    value: &[u8],
    expires_at: u64,
    revision: u64,
) -> Vec<u8> {
    const SIGNING_CONTEXT: &[u8] = b"aruna/dht/2/record";

    let mut signed_data =
        Vec::with_capacity(SIGNING_CONTEXT.len() + 32 + 32 + 32 + 8 + 8 + 8 + value.len());
    signed_data.extend_from_slice(SIGNING_CONTEXT);
    signed_data.extend_from_slice(key.as_bytes());
    signed_data.extend_from_slice(publisher.as_bytes());
    signed_data.extend_from_slice(realm_id.as_bytes());
    signed_data.extend_from_slice(&expires_at.to_le_bytes());
    signed_data.extend_from_slice(&revision.to_le_bytes());
    signed_data.extend_from_slice(&(value.len() as u64).to_le_bytes());
    signed_data.extend_from_slice(value);
    signed_data
}

pub fn verify_stored_value(key: &DhtKeyId, entry: &StoredValue) -> bool {
    verify_record(
        key,
        &entry.publisher,
        &entry.realm_id,
        &entry.value,
        entry.expires_at,
        entry.revision,
        &entry.signature,
    )
}

pub fn verify_record(
    key: &DhtKeyId,
    publisher: &NodeId,
    realm_id: &RealmId,
    value: &[u8],
    expires_at: u64,
    revision: u64,
    signature: &iroh::Signature,
) -> bool {
    let signed_data = signed_record_bytes(key, publisher, realm_id, value, expires_at, revision);
    publisher.verify(&signed_data, signature).is_ok()
}

/// Serialize a request to bytes
pub fn encode_request(req: &DhtRequest) -> Result<Vec<u8>, postcard::Error> {
    encode_request_with_trace_context(req, None)
}

pub fn encode_request_with_trace_context(
    req: &DhtRequest,
    trace_context: Option<DistributedTraceContext>,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&DhtRequestEnvelope {
        trace_context,
        request: req.clone(),
    })
}

/// Deserialize a request from bytes
pub fn decode_request(bytes: &[u8]) -> Result<DhtRequest, postcard::Error> {
    decode_request_with_trace_context(bytes).map(|(_, request)| request)
}

pub fn decode_request_with_trace_context(
    bytes: &[u8],
) -> Result<(Option<DistributedTraceContext>, DhtRequest), postcard::Error> {
    postcard::from_bytes::<DhtRequestEnvelope>(bytes)
        .map(|envelope| (envelope.trace_context, envelope.request))
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
    use crate::dht::constants::{MAX_MESSAGE_SIZE, MAX_STORED_VALUE_SIZE};
    use crate::dht::kbucket::K;

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
    fn test_request_roundtrip_with_trace_context() {
        let trace_context = DistributedTraceContext::new(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            Some("congo=t61rcWkgMzE".to_string()),
        );
        let req = DhtRequest::Ping;

        let bytes = encode_request_with_trace_context(&req, Some(trace_context.clone()))
            .expect("encode request");
        let (decoded_trace_context, decoded_request) =
            decode_request_with_trace_context(&bytes).expect("decode request");

        assert_eq!(decoded_trace_context, Some(trace_context));
        assert!(matches!(decoded_request, DhtRequest::Ping));
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
        let expires_at: u64 = 42;
        let revision = 7;

        let signed_data =
            signed_record_bytes(&key, &publisher, &realm_id, &value, expires_at, revision);
        let signature = publisher_secret.sign(&signed_data);

        let req = DhtRequest::PutValue {
            key,
            realm_id,
            value: value.clone(),
            expires_at,
            revision,
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
                expires_at: decoded_expiry,
                revision: decoded_revision,
                publisher: decoded_publisher,
                signature: Some(decoded_signature),
            } => {
                assert_eq!(decoded_key, key);
                assert_eq!(decoded_realm_id, realm_id);
                assert_eq!(decoded_value, value);
                assert_eq!(decoded_expiry, expires_at);
                assert_eq!(decoded_revision, revision);
                assert_eq!(decoded_publisher, publisher);

                let verify_data = signed_record_bytes(
                    &decoded_key,
                    &decoded_publisher,
                    &decoded_realm_id,
                    &decoded_value,
                    decoded_expiry,
                    decoded_revision,
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

    #[test]
    fn wire_limit_enforced() {
        const { assert!(MAX_STORED_VALUE_SIZE + K * 32 + 1024 <= MAX_MESSAGE_SIZE) };
    }
}
