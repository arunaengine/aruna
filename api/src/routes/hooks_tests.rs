//! Tests webhook ingress: bounds refuse a request before it is read or relayed, a relay drops
//! credentials no check reads or relays nothing, and an unknown install or long body is refused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use axum::http::HeaderName;

use super::*;
use crate::tests::routes::{seed_realm_config, test_context, test_state, test_storage};

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    (pairs.iter())
        .map(|(name, value)| {
            let name = HeaderName::from_bytes(name.as_bytes()).unwrap();
            (name, HeaderValue::from_str(value).unwrap())
        })
        .collect()
}

fn status(refused: Option<Response>) -> Option<StatusCode> {
    refused.map(|response| response.status())
}

#[test]
fn bounds_before_reading() {
    let plain = headers(&[("content-type", "application/json")]);
    assert_eq!(status(over_bounds(&plain, Some("a=1"))), None);
    let long = headers(&[("content-length", "262145")]);
    assert_eq!(
        status(over_bounds(&long, None)),
        Some(StatusCode::PAYLOAD_TOO_LARGE)
    );
    let names: Vec<String> = (0..65).map(|n| format!("x-h{n}")).collect();
    let many: Vec<(&str, &str)> = names.iter().map(|name| (name.as_str(), "v")).collect();
    let large = "v".repeat(MAX_HEADER_BYTES);
    for crowded in [headers(&many), headers(&[("x-large", &large)])] {
        assert_eq!(
            status(over_bounds(&crowded, None)),
            Some(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE)
        );
    }
    let query = "q".repeat(MAX_HOOK_QUERY + 1);
    assert_eq!(
        status(over_bounds(&plain, Some(&query))),
        Some(StatusCode::URI_TOO_LONG)
    );
}

#[test]
fn relay_drops_credentials() {
    let received = headers(&[
        ("cookie", "canary-cookie"),
        ("proxy-authorization", "Basic canary-proxy"),
        ("authorization", "Bearer canary-auth"),
        ("connection", "close"),
        ("x-aruna-relay-hop", "spoofed"),
        ("x-hub-signature-256", "sha256=ab"),
        ("x-delivery", "d-1"),
        ("content-type", "application/json"),
    ]);
    let sent = relayed(&received, false);
    let mut names: Vec<&str> = sent.keys().map(HeaderName::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, ["content-type", "x-delivery", "x-hub-signature-256"]);
    let checked = relayed(&received, true);
    let kept = checked.get(header::AUTHORIZATION);
    assert_eq!(
        kept.and_then(|value| value.to_str().ok()),
        Some("Bearer canary-auth")
    );
    assert!(!checked.contains_key(header::COOKIE));
}

#[tokio::test]
async fn refuses_unknown_installs() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let ctx = Arc::new(test_context(storage));
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(ctx.clone(), realm_id, node_id, capabilities).await);
    let actor = Actor {
        node_id,
        user_id: UserId::new(Ulid::from_bytes([3u8; 16]), realm_id),
        realm_id,
    };
    seed_realm_config(&ctx, realm_id, &actor).await;
    let send = |id: String, body: Vec<u8>| {
        let state = state.clone();
        async move {
            let path = Path(id);
            let body = Body::from(body);
            receive(
                State(state),
                path,
                RawQuery(None),
                Method::POST,
                HeaderMap::new(),
                body,
            )
            .await
            .status()
        }
    };
    let unknown = Ulid::from_parts(9, 9).to_string();
    assert_eq!(
        send(unknown.clone(), b"{}".to_vec()).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send("not-an-id".into(), Vec::new()).await,
        StatusCode::NOT_FOUND
    );
    // Without a length header the body is still cut at its bound while it is read.
    let long = vec![b'x'; MAX_WEBHOOK + 1];
    assert_eq!(send(unknown, long).await, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn undetermined_relays_nothing() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let ctx = Arc::new(test_context(storage));
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = test_state(ctx.clone(), realm_id, node_id, capabilities).await;
    let actor = Actor {
        node_id,
        user_id: UserId::new(Ulid::from_bytes([3u8; 16]), realm_id),
        realm_id,
    };
    seed_realm_config(&ctx, realm_id, &actor).await;
    // No install record here, so whether the check needs `authorization` cannot be told.
    let receiver = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
    let sent = headers(&[("authorization", "Bearer canary-auth")]);
    let install = Ulid::from_parts(9, 9);
    let answer = relay(
        &state,
        (receiver, install),
        "/plugin-hooks/x",
        &sent,
        Bytes::new(),
    )
    .await;
    assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(answer.headers().contains_key(header::RETRY_AFTER));
}
