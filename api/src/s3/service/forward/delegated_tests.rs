//! Tests how a member accepts a forwarded request: the delegation must name exactly this
//! request, come from a realm node, reach its target once, and frame the body it binds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::structs::storage::distribution::delegation::{
    DELEGATION_HEADER, Delegation, Payload,
};
use http::{HeaderMap, Method};
use s3s::dto::StreamingBlob;

use super::forwarded_digest;
use super::members::{BUCKET, Members, members};
use crate::s3::service::payload::frame_body;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// What one delegated request names; `key` is the key the delegation binds.
struct Delegated<'a> {
    method: Method,
    operation: &'a str,
    key: &'a str,
    payload: Payload,
}

/// A delegation node `from` signs for a request to node `to`, bound to `headers`.
fn delegation(
    members: &Members,
    (from, to): (usize, usize),
    request: &Delegated<'_>,
    headers: &HeaderMap,
) -> Delegation {
    let range = members.snapshot.range_for(request.key).unwrap();
    let user = &members.user;
    Delegation {
        realm_id: members.realm_id,
        issuer: members.nodes[from].node_id(),
        target: members.nodes[to].node_id(),
        set_id: members.snapshot.set_id(),
        range_id: range.range_id,
        user_id: user.user_identity,
        group_id: user.group_id,
        credential_id: user.access_key.clone(),
        restrictions: None,
        credential_expiry: now() + 3600,
        operation: request.operation.to_string(),
        bucket: BUCKET.to_string(),
        key: request.key.to_string(),
        copy_source: None,
        request_digest: forwarded_digest(&request.method, BUCKET, request.key, None, headers),
        payload: request.payload.clone(),
        nonce: rand::random(),
        issued_at: now(),
        valid_secs: 10,
    }
}

fn accept() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(http::header::ACCEPT, "*/*".parse().unwrap());
    headers
}

/// Sends `method` on `key` to node `to` with the given delegation header values.
async fn send(
    members: &Members,
    to: usize,
    (method, key): (Method, &str),
    values: &[String],
    body: Option<reqwest::Body>,
) -> (http::StatusCode, String) {
    let url = format!("{}/{BUCKET}/{key}", members.nodes[to].url);
    let mut outgoing = reqwest::Client::new()
        .request(method, url)
        .headers(accept());
    for value in values {
        outgoing = outgoing.header(DELEGATION_HEADER, value);
    }
    if let Some(body) = body {
        outgoing = outgoing.body(body);
    }
    let response = outgoing.send().await.unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

fn get(key: &str) -> Delegated<'_> {
    Delegated {
        method: Method::GET,
        operation: "GetObject",
        key,
        payload: Payload::Empty,
    }
}

async fn stored(members: &Members, key: &str) {
    let node = &members.nodes[1];
    let input = s3s::dto::PutObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        content_length: Some(4),
        body: Some(StreamingBlob::from(bytes::Bytes::from_static(b"held"))),
        ..Default::default()
    };
    let mut extensions = http::Extensions::new();
    extensions.insert(members.user_on(1));
    extensions.insert(node.bucket().await);
    extensions.insert(aruna_operations::auth::request_policy::PolicyRequestExtras::rest());
    let request = s3s::S3Request {
        input,
        method: Method::PUT,
        uri: http::Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    s3s::S3::put_object(&node.service, request).await.unwrap();
}

// A delegation serves its one request once; the same header again is a replay.
#[tokio::test]
async fn serves_once() {
    let members = members(3, &["h", "p"], 0).await;
    stored(&members, "k/held").await;
    let signed =
        delegation(&members, (0, 1), &get("k/held"), &accept()).sign(&members.nodes[0].secret);
    let value = signed.encode().unwrap();
    let (status, body) = send(&members, 1, (Method::GET, "k/held"), &[value.clone()], None).await;
    assert_eq!((status, body.as_str()), (http::StatusCode::OK, "held"));
    let (status, body) = send(&members, 1, (Method::GET, "k/held"), &[value], None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(body.contains("already used"), "{body}");
}

// The delegation binds the request: another key, a missing or doubled header, or a broken
// value is refused before anything is read.
#[tokio::test]
async fn binds_request() {
    let members = members(3, &["h", "p"], 0).await;
    stored(&members, "k/held").await;
    let secret = &members.nodes[0].secret;
    let sign = || delegation(&members, (0, 1), &get("k/held"), &accept()).sign(secret);
    let other = sign().encode().unwrap();
    let (status, body) = send(&members, 1, (Method::GET, "k/other"), &[other], None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(body.contains("names another request"), "{body}");
    let twice = [sign().encode().unwrap(), sign().encode().unwrap()];
    let (status, body) = send(&members, 1, (Method::GET, "k/held"), &twice, None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(body.contains("one delegation header"), "{body}");
    let broken = ["not a delegation".to_string()];
    let (status, body) = send(&members, 1, (Method::GET, "k/held"), &broken, None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(body.contains("malformed delegation"), "{body}");
}

// Only a realm infrastructure node delegates, only to the node it names, and only for an
// operation members forward.
#[tokio::test]
async fn refuses_strangers() {
    let members = members(3, &["h", "p"], 0).await;
    stored(&members, "k/held").await;
    let request = get("k/held");
    let stranger = iroh::SecretKey::from_bytes(&[99; 32]);
    let mut spoofed = delegation(&members, (0, 1), &request, &accept());
    spoofed.issuer = stranger.public();
    let value = spoofed.sign(&stranger).encode().unwrap();
    let (status, _) = send(&members, 1, (Method::GET, "k/held"), &[value], None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    let elsewhere = delegation(&members, (0, 2), &request, &accept());
    let value = elsewhere.sign(&members.nodes[0].secret).encode().unwrap();
    let (status, _) = send(&members, 1, (Method::GET, "k/held"), &[value], None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    let mut expired = delegation(&members, (0, 1), &request, &accept());
    expired.issued_at = now() - 3600;
    let value = expired.sign(&members.nodes[0].secret).encode().unwrap();
    let (status, _) = send(&members, 1, (Method::GET, "k/held"), &[value], None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);

    let tagging = Delegated {
        method: Method::GET,
        operation: "GetObjectTagging",
        key: "k/held",
        payload: Payload::Empty,
    };
    let query_headers = accept();
    let mut tags = delegation(&members, (0, 1), &tagging, &query_headers);
    tags.request_digest = forwarded_digest(
        &Method::GET,
        BUCKET,
        "k/held",
        Some("tagging"),
        &query_headers,
    );
    let value = tags.sign(&members.nodes[0].secret).encode().unwrap();
    let (status, body) = send(&members, 1, (Method::GET, "k/held?tagging"), &[value], None).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(body.contains("not forwarded"), "{body}");
}

// A write is taken only by the coordinator the delegation's range names, and only with the
// body binding its operation needs.
#[tokio::test]
async fn writes_need_coordinator() {
    let members = members(3, &["h", "p"], 0).await;
    let put = Delegated {
        method: Method::PUT,
        operation: "PutObject",
        key: "k/new",
        payload: Payload::Framed { checksum: None },
    };
    let mut headers = accept();
    headers.insert("content-length", "1".parse().unwrap());
    let wrong = delegation(&members, (0, 2), &put, &headers);
    let value = wrong.sign(&members.nodes[0].secret).encode().unwrap();
    let (status, body) = send(
        &members,
        2,
        (Method::PUT, "k/new"),
        &[value],
        Some(b"x".to_vec().into()),
    )
    .await;
    assert_eq!(status, http::StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let unframed = Delegated {
        payload: Payload::Empty,
        ..put
    };
    let value = (delegation(&members, (0, 1), &unframed, &headers))
        .sign(&members.nodes[0].secret)
        .encode()
        .unwrap();
    let (status, body) = send(
        &members,
        1,
        (Method::PUT, "k/new"),
        &[value],
        Some(b"x".to_vec().into()),
    )
    .await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(body.contains("names another request"), "{body}");
}

// A forwarded body must end with the issuer's signed trailer over exactly its bytes: the
// framed body is stored, a body with a forged trailer never is.
#[tokio::test]
async fn framed_body_checked() {
    let members = members(3, &["h", "p"], 0).await;
    let issuer = &members.nodes[0];
    for (key, forged) in [("k/framed", false), ("k/forged", true)] {
        let request = Delegated {
            method: Method::PUT,
            operation: "PutObject",
            key,
            payload: Payload::Framed { checksum: None },
        };
        let length = 5 + aruna_core::structs::storage::distribution::delegation::TRAILER_BYTES;
        let mut headers = accept();
        headers.insert("content-length", length.to_string().parse().unwrap());
        let signed = delegation(&members, (0, 1), &request, &headers).sign(&issuer.secret);
        let body = StreamingBlob::from(bytes::Bytes::from_static(b"frame"));
        let framed = frame_body(body, issuer.context.clone(), signed.delegation.nonce, None);
        use futures_util::TryStreamExt;
        let mut bytes: Vec<u8> =
            (framed.try_collect::<Vec<bytes::Bytes>>().await.unwrap()).concat();
        if forged {
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
        }
        let value = signed.encode().unwrap();
        let url = format!("{}/{BUCKET}/{key}", members.nodes[1].url);
        let response = reqwest::Client::new()
            .put(url)
            .headers(headers)
            .header(DELEGATION_HEADER, value)
            .body(bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().is_success(), !forged, "{key}");
    }
    let held = get("k/framed");
    let value = (delegation(&members, (0, 1), &held, &accept()))
        .sign(&issuer.secret)
        .encode()
        .unwrap();
    let (status, body) = send(&members, 1, (Method::GET, "k/framed"), &[value], None).await;
    assert_eq!((status, body.as_str()), (http::StatusCode::OK, "frame"));
    let forged = get("k/forged");
    let value = (delegation(&members, (0, 1), &forged, &accept()))
        .sign(&issuer.secret)
        .encode()
        .unwrap();
    let (status, _) = send(&members, 1, (Method::GET, "k/forged"), &[value], None).await;
    assert_eq!(status, http::StatusCode::NOT_FOUND);
}
