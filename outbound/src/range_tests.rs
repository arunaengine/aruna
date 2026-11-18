//! Tests range listings against a local server: both request forms and their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::egress::EgressPolicy;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::KeyRange;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use ulid::Ulid;

use super::*;

const TOKEN: &str = "listing-token";

type Seen = Arc<Mutex<Vec<String>>>;

/// A server that keeps each request head and answers it with `answer`.
async fn server(answer: String) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Seen::default();
    let requests = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut head = [0u8; 8192];
            let read = socket.read(&mut head).await.unwrap_or(0);
            let text = String::from_utf8_lossy(&head[..read]).into_owned();
            requests.lock().unwrap().push(text);
            let _ = socket.write_all(answer.as_bytes()).await;
        }
    });
    (format!("http://{address}"), seen)
}

fn answer(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn handle() -> OutboundHandle {
    OutboundHandle::new(reqwest::Client::new(), EgressPolicy::loopback())
}

fn listing(base_url: String, access: RangeAccess) -> RangeListing {
    RangeListing {
        base_url,
        local_node: node(1),
        node_id: node(2),
        set_id: Ulid::from_bytes([3; 16]),
        range: KeyRange {
            range_id: Ulid::from_bytes([4; 16]),
            lower: "a".to_string(),
            upper: Some("m".to_string()),
            lineage: None,
            coordinator: Ulid::from_bytes([5; 16]),
            epoch: 1,
            replicas: Vec::new(),
        },
        copy: true,
        bucket: "spire".to_string(),
        prefix: Some("db/".to_string()),
        delimiter: None,
        limit: 10,
        position: Some("next".to_string()),
        access,
    }
}

fn bearer() -> RangeAccess {
    let token = SecretBytes::new(TOKEN.as_bytes().to_vec());
    RangeAccess::Bearer(Some(SharedSecret::new(token)))
}

fn delegated() -> RangeAccess {
    let realm_id = RealmId::from_bytes([6; 32]);
    RangeAccess::Delegated(Box::new(Delegation {
        realm_id,
        issuer: node(1),
        target: node(2),
        set_id: Ulid::from_bytes([3; 16]),
        range_id: Ulid::from_bytes([4; 16]),
        user_id: UserId::nil(realm_id),
        group_id: Ulid::from_bytes([7; 16]),
        credential_id: String::new(),
        restrictions: None,
        credential_expiry: 100,
        operation: String::new(),
        bucket: String::new(),
        key: String::new(),
        copy_source: None,
        request_digest: [0; 32],
        payload: Payload::Empty,
        nonce: [8; 16],
        issued_at: 10,
        valid_secs: 15,
    }))
}

struct KeySigner;

impl ListingSigner for KeySigner {
    fn sign(&self, delegation: Delegation) -> Option<SignedDelegation> {
        Some(delegation.sign(&iroh::SecretKey::from_bytes(&[1; 32])))
    }
}

#[tokio::test]
async fn rest_page_read() {
    let body = r#"{"bucket":"spire","objects":[{"key":"db/a","version_id":"v","size":3,
        "etag":null,"last_modified":null,"content_type":null,"referenced":false,
        "content_id":null}],"prefixes":["db/x/"],"next_cursor":"more"}"#;
    let (base, seen) = server(answer("200 OK", body)).await;
    let found = handle()
        .list_range(listing(base, bearer()), &NoSigner)
        .await;
    let RangeAnswer::Page(page) = found else {
        panic!("a page is answered: {found:?}");
    };
    assert_eq!(page.objects[0].key, "db/a");
    assert_eq!(page.next.as_deref(), Some("more"));
    let head = seen.lock().unwrap().remove(0).to_lowercase();
    assert!(head.starts_with("get /api/v1/data/buckets/spire/objects?limit=10"));
    assert!(head.contains("cursor=next") && head.contains("copy=true"));
    assert!(head.contains(&format!("authorization: bearer {TOKEN}")));
    assert!(head.contains("x-aruna-relay-set:"));
}

#[tokio::test]
async fn rest_refusal_kept() {
    let (base, _) = server(answer("403 Forbidden", r#"{"error":"no"}"#)).await;
    let found = handle()
        .list_range(listing(base, bearer()), &NoSigner)
        .await;
    let RangeAnswer::Refused(refusal) = found else {
        panic!("a refusal is answered: {found:?}");
    };
    assert_eq!((refusal.status, refusal.message.as_str()), (403, "no"));
}

#[tokio::test]
async fn s3_page_read() {
    let body = "<ListBucketResult><Contents><Key>db/a&amp;b</Key><Size>3</Size>\
        <ETag>e</ETag></Contents><CommonPrefixes><Prefix>db/x/</Prefix></CommonPrefixes>\
        <NextContinuationToken>tok</NextContinuationToken></ListBucketResult>";
    let (base, seen) = server(answer("200 OK", body)).await;
    let found = handle()
        .list_range(listing(base, delegated()), &KeySigner)
        .await;
    let RangeAnswer::Page(page) = found else {
        panic!("a page is answered: {found:?}");
    };
    assert_eq!(page.objects[0].key, "db/a&b");
    assert_eq!(page.objects[0].size, Some(3));
    assert_eq!(page.prefixes, ["db/x/"]);
    assert_eq!(page.next.as_deref(), Some("tok"));
    let head = seen.lock().unwrap().remove(0);
    let line = head.lines().next().unwrap();
    let query = line.split_once('?').unwrap().1.split(' ').next().unwrap();
    assert!(query.starts_with("list-type=2&max-keys=10&prefix=db%2F"));
    assert!(query.ends_with("aruna-lower=a&aruna-upper=m&aruna-copy=1"));
    let lower = head.to_lowercase();
    let header = (lower.lines())
        .find_map(|line| line.strip_prefix("x-aruna-delegation: "))
        .is_some();
    assert!(header);
    let original = (head.lines())
        .find_map(|line| {
            let (name, value) = line.split_once(": ")?;
            name.eq_ignore_ascii_case(DELEGATION_HEADER)
                .then_some(value)
        })
        .unwrap();
    let signed = SignedDelegation::decode(original).unwrap();
    let accept = [("accept", ACCEPT_ANY)];
    let digest = request_digest("GET", "spire", "", Some(query), &accept);
    assert_eq!(signed.delegation.request_digest, digest);
    assert_eq!(signed.delegation.operation, "ListObjectsV2");
}

#[tokio::test]
async fn s3_refusal_kept() {
    let body = "<Error><Code>AccessDenied</Code><Message>no</Message></Error>";
    let (base, _) = server(answer("403 Forbidden", body)).await;
    let found = handle()
        .list_range(listing(base, delegated()), &KeySigner)
        .await;
    let RangeAnswer::Refused(refusal) = found else {
        panic!("a refusal is answered: {found:?}");
    };
    assert_eq!(refusal.code.as_deref(), Some("AccessDenied"));
    assert_eq!(refusal.message, "no");
}

#[tokio::test]
async fn unsigned_delegation_silent() {
    let found = handle()
        .list_range(
            listing("http://127.0.0.1:9".to_string(), delegated()),
            &NoSigner,
        )
        .await;
    assert_eq!(found, RangeAnswer::Silent);
}

#[test]
fn access_stays_hidden() {
    let shown = format!("{:?}", listing("http://node".to_string(), bearer()));
    assert!(!shown.contains(TOKEN));
    assert!(shown.contains("RangeAccess::Bearer"));
}
