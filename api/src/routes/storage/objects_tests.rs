//! Tests the object listing route: pages, cursors, permissions, relayed bounds and the merged
//! listing of a distributed bucket whose other range a peer lists.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::relay::RELAY_SET_HEADER;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
};
use axum::http::StatusCode;

async fn store(node: &Node, keys: &[&str]) {
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    for key in keys {
        let input = WriteObjectInput {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            text: "x".to_string(),
            content_type: None,
        };
        store_text(&node.state, &node.owner, &info, input)
            .await
            .unwrap();
    }
}

fn query(prefix: Option<&str>, cursor: Option<String>, limit: usize) -> ObjectListQuery {
    ObjectListQuery {
        prefix: prefix.map(str::to_string),
        delimiter: Some("/".to_string()),
        cursor,
        limit: Some(limit),
        ..Default::default()
    }
}

async fn list(
    node: &Node,
    auth: &AuthContext,
    headers: HeaderMap,
    query: ObjectListQuery,
) -> ServerResult<ObjectListResponse> {
    let listed = list_objects(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Path(BUCKET.to_string()),
        Query(query),
    )
    .await?;
    Ok(listed.0)
}

fn keys(page: &ObjectListResponse) -> Vec<&str> {
    page.objects
        .iter()
        .map(|object| object.key.as_str())
        .collect()
}

/// The headers of a listing another member of `set_id` relays.
fn relayed(set_id: ulid::Ulid) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    headers.insert(RELAY_SET_HEADER, set_id.to_string().parse().unwrap());
    headers
}

#[tokio::test]
async fn pages_local_bucket() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md", "dir/c.md", "dir/d.md"]).await;

    let first = list(&node, &node.owner, HeaderMap::new(), query(None, None, 2))
        .await
        .unwrap();
    assert_eq!(keys(&first), vec!["a.md", "b.md"]);
    let cursor = first.next_cursor.clone().expect("more entries follow");
    let rest = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(cursor), 2),
    )
    .await;
    let rest = rest.unwrap();
    assert!(rest.objects.is_empty());
    assert_eq!(rest.prefixes, vec!["dir/".to_string()]);
    assert!(rest.next_cursor.is_none());

    let inside = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(Some("dir/"), None, 10),
    )
    .await;
    assert_eq!(keys(&inside.unwrap()), vec!["dir/c.md", "dir/d.md"]);
}

#[tokio::test]
async fn refuses_bad_listings() {
    let node = node_state().await;
    let stranger = list(
        &node,
        &node.stranger,
        HeaderMap::new(),
        query(None, None, 5),
    )
    .await;
    assert!(matches!(stranger, Err(ServerError::Forbidden)));
    let from = encode_resume(&Resume::From("m".to_string())).unwrap();
    let cursor = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(from), 5),
    )
    .await;
    assert!(
        cursor.is_err(),
        "a range position is no cursor of a local listing"
    );
    let mut upper = query(None, None, 5);
    upper.upper = Some("m".to_string());
    assert!(matches!(
        list(&node, &node.owner, HeaderMap::new(), upper).await,
        Err(ServerError::BadRequestReason(_))
    ));
    let mut lower = query(None, None, 5);
    lower.lower = Some(String::new());
    assert!(matches!(
        list(&node, &node.owner, HeaderMap::new(), lower).await,
        Err(ServerError::BadRequestReason(_))
    ));
}

#[tokio::test]
async fn relayed_range_listing() {
    let node = node_state().await;
    store(&node, &["a.md", "z.md"]).await;
    let set_id = distribute(&node.state, node.group_id, BUCKET)
        .await
        .set_id();
    let mut bounded = query(None, None, 10);
    bounded.lower = Some(String::new());
    bounded.upper = Some("m".to_string());
    // Bounds come only from another member of the set.
    let direct = list(&node, &node.owner, HeaderMap::new(), bounded.clone()).await;
    assert!(matches!(direct, Err(ServerError::BadRequestReason(_))));

    let page = list(&node, &node.owner, relayed(set_id), bounded)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md"]);

    let mut other = query(None, None, 10);
    other.lower = Some("m".to_string());
    let foreign = list(&node, &node.owner, relayed(set_id), other).await;
    assert!(matches!(
        foreign,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
}

#[tokio::test]
async fn merges_peer_range() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md"]).await;
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":["y/"],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;

    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md", "b.md", "x.md"]);
    assert_eq!(page.prefixes, vec!["y/".to_string()]);
    assert!(page.next_cursor.is_none());
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), 1);
    assert!(paths[0].starts_with("/api/v1/data/buckets/routed/objects?limit=8"));
    assert!(paths[0].contains("lower=m"));

    // A full page stops before the peer's range and resumes after the last local key.
    let first = list(&node, &node.owner, HeaderMap::new(), query(None, None, 2))
        .await
        .unwrap();
    assert_eq!(keys(&first), vec!["a.md", "b.md"]);
    let cursor = first.next_cursor.expect("the peer's range follows");
    let rest = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(cursor), 2),
    )
    .await;
    assert_eq!(keys(&rest.unwrap()), vec!["x.md"]);
}

#[tokio::test]
async fn unanswered_range_fails() {
    let node = node_state().await;
    distribute(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let Err(ServerError::ServiceUnavailableReason(reason)) =
        list(&node, &node.owner, HeaderMap::new(), query(None, None, 10)).await
    else {
        panic!("a range no holder answers fails the page");
    };
    assert!(reason.contains("incomplete"));
    let refusal = mock_peer(StatusCode::FORBIDDEN, r#"{"error":"no grant"}"#).await;
    publish_api(&node.state, node_id(PEER), &refusal.url).await;
    assert!(matches!(
        list(&node, &node.owner, HeaderMap::new(), query(None, None, 10)).await,
        Err(ServerError::Forbidden)
    ));
}
