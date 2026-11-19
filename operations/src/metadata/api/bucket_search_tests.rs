//! Drives the bucket search through its cursor, fanout and retired node answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::effects::NetEffect;
use aruna_core::structs::identity::auth::AuthContext;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn request(cursor: Option<String>) -> BucketSearchRequest {
    BucketSearchRequest {
        auth: AuthContext {
            user_id: UserId::nil(realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        },
        bearer_token: None,
        query: "reads".to_string(),
        key: None,
        extras: None,
        limit: 500,
        group_id: None,
        cursor,
        target_nodes: Some(vec![node(1), node(2)]),
        database: false,
    }
}

fn fanout(parts: Vec<(NodeId, BucketPage)>) -> IoResult {
    let answer: Result<FanoutParts<BucketPage>, MetadataApiError> =
        Ok((parts, super::super::MetadataFanoutStats::default()));
    IoResult::Child(ChildOutcome::new(answer))
}

#[test]
fn search_asks_realm() {
    let mut search = search_buckets_distributed(realm(), node(1), request(None));
    assert_eq!(search.limit, 50);
    assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
    let Ok(Next::Done(execution)) = search.next(fanout(vec![(node(1), BucketPage::default())]))
    else {
        panic!("expected the merged page");
    };
    assert!(execution.hits.is_empty());
    assert_eq!(execution.next_cursor, None);
}

#[test]
fn bad_cursor_refused() {
    let mut search = search_buckets_distributed(realm(), node(1), request(Some("!".to_string())));
    assert!(matches!(
        search.first(),
        Err(MetadataApiError::InvalidCursor(_))
    ));
}

#[test]
fn retired_node_fails() {
    let mut search = search_buckets_distributed(realm(), node(1), request(None));
    search.first().unwrap();
    let retired = BucketPage {
        retired: true,
        ..BucketPage::default()
    };
    let Ok(Next::Done(execution)) = search.next(fanout(vec![(node(2), retired)])) else {
        panic!("expected the merged page");
    };
    assert_eq!(execution.fanout_stats.nodes_failed, 1);
    assert_eq!(execution.fanout_stats.failed_partitions, vec![node(2)]);
}

#[test]
fn remote_asks_peer() {
    let calls = BucketCalls {
        auth_token: None,
        input: SearchBucketsInput {
            auth: request(None).auth,
            realm_id: realm(),
            node_id: node(1),
            query: "reads".to_string(),
            key: None,
            extras: None,
            limit: 5,
            group_id: None,
            start_after: None,
            database: false,
        },
        positions: Vec::new(),
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) =
        calls.remote(node(2), Duration::from_secs(2))
    else {
        panic!("expected a metadata request");
    };
    assert_eq!(
        (request.peer, request.timeout),
        (node(2), Duration::from_secs(2))
    );
}
