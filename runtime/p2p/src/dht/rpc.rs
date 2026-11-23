//! Re-exports the DHT wire messages from core and names their kinds for telemetry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub use aruna_core::dht_protocol::{
    DHT_ALPN, DhtRequest, DhtResponse, ErrorCode, StoredValue, decode_exact, decode_request,
    decode_response, decode_traced_request, encode_request, encode_response, encode_traced_request,
    signed_record_bytes, verify_record, verify_stored_value,
};

pub(super) const fn request_kind(request: &DhtRequest) -> &'static str {
    match request {
        DhtRequest::Ping => "ping",
        DhtRequest::FindNode { .. } => "find_node",
        DhtRequest::GetValue { .. } => "get_value",
        DhtRequest::PutValue { .. } => "put_value",
    }
}

pub(super) const fn response_kind(response: &DhtResponse) -> &'static str {
    match response {
        DhtResponse::Pong => "pong",
        DhtResponse::Nodes { .. } => "nodes",
        DhtResponse::Value { .. } => "value",
        DhtResponse::Stored => "stored",
        DhtResponse::Error { .. } => "error",
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::id::{DhtKeyId, NodeId};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::trace_context::DistributedTraceContext;

    use super::*;
    use crate::dht::constants::{MAX_MESSAGE_SIZE, MAX_STORED_SIZE};
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
    fn rejects_request_suffix() {
        let mut bytes = encode_request(&DhtRequest::Ping).expect("encode request");
        bytes.push(0);

        assert!(decode_request(&bytes).is_err());
    }

    #[test]
    fn rejects_response_suffix() {
        let mut bytes = encode_response(&DhtResponse::Pong).expect("encode response");
        bytes.push(0);

        assert!(decode_response(&bytes).is_err());
    }

    #[test]
    fn request_trace_roundtrip() {
        let trace_context = DistributedTraceContext::new(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            Some("congo=t61rcWkgMzE".to_string()),
        );
        let req = DhtRequest::Ping;

        let bytes =
            encode_traced_request(&req, Some(trace_context.clone())).expect("encode request");
        let (decoded_trace_context, decoded_request) =
            decode_traced_request(&bytes).expect("decode request");

        assert_eq!(decoded_trace_context, Some(trace_context));
        assert!(matches!(decoded_request, DhtRequest::Ping));
    }

    #[test]
    fn get_filter_roundtrip() {
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
    fn put_signature_roundtrip() {
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
        const { assert!(MAX_STORED_SIZE + K * 32 + 1024 <= MAX_MESSAGE_SIZE) };
    }
}
