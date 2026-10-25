//! Tests the bucket usage route: permissions, the held ranges a member answers to another one,
//! and the usage of a distributed bucket merged with the ranges a peer holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::storage::relay::RELAY_SET_HEADER;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
};
use axum::http::StatusCode;

async fn usage(
    node: &Node,
    auth: &AuthContext,
    headers: HeaderMap,
    held: bool,
) -> ServerResult<BucketUsageResponse> {
    let answer = get_bucket_usage(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Path(BUCKET.to_string()),
        Query(UsageQuery { held: Some(held) }),
    )
    .await?;
    Ok(answer.0)
}

/// A distributed bucket whose lower range this node holds.
async fn distributed(node: &Node) -> DistributionSnapshot {
    distribute(&node.state, node.group_id, BUCKET).await
}

/// The usage answer of the peer for the upper range, with three objects in one counter row.
fn peer_answer(snapshot: &DistributionSnapshot) -> String {
    let range_id = snapshot.config.ranges[1].range_id;
    serde_json::json!({
        "bucket": BUCKET,
        "objects": 0, "versions": 0, "delete_markers": 0, "open_multipart_uploads": 0,
        "logical_bytes": 0, "referenced_bytes": 0, "complete": false,
        "held_ranges": [{
            "range_id": range_id.to_string(),
            "complete": true,
            "rows": [{
                "row": "0a0b", "objects": 3, "current_bytes": 30, "versions": 3,
                "delete_markers": 0, "logical_bytes": 30, "referenced_bytes": 0,
                "open_uploads": 0
            }]
        }]
    })
    .to_string()
}

#[tokio::test]
async fn ordinary_usage_checked() {
    let node = node_state().await;
    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!((answer.bucket.as_str(), answer.objects), (BUCKET, 0));
    assert!(answer.ranges.is_none());
    assert!(matches!(
        usage(&node, &node.stranger, HeaderMap::new(), false).await,
        Err(ServerError::Forbidden)
    ));
    // Held ranges are an answer to another member only.
    assert!(matches!(
        usage(&node, &node.owner, HeaderMap::new(), true).await,
        Err(ServerError::BadRequestReason(_))
    ));
}

#[tokio::test]
async fn merges_peer_usage() {
    let node = node_state().await;
    let snapshot = distributed(&node).await;
    let peer = mock_peer(StatusCode::OK, &peer_answer(&snapshot)).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;

    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();

    assert_eq!((answer.ranges, answer.ranges_counted), (Some(2), Some(2)));
    assert_eq!((answer.objects, answer.logical_bytes), (3, 30));
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(
        paths,
        vec!["/api/v1/data/buckets/routed/usage?held=true".to_string()]
    );
}

#[tokio::test]
async fn silent_peer_incomplete() {
    let node = node_state().await;
    distributed(&node).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!((answer.ranges, answer.ranges_counted), (Some(2), Some(1)));
    assert!(!answer.complete);
}

#[tokio::test]
async fn answers_held_ranges() {
    let node = node_state().await;
    let snapshot = distributed(&node).await;
    let mut headers = HeaderMap::new();
    headers.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    headers.insert(
        RELAY_SET_HEADER,
        snapshot.set_id().to_string().parse().unwrap(),
    );
    let answer = usage(&node, &node.owner, headers, true).await.unwrap();
    let held = answer
        .held_ranges
        .expect("a member answers its held ranges");
    assert_eq!(held.len(), 1);
    assert_eq!(
        held[0].range_id,
        snapshot.config.ranges[0].range_id.to_string()
    );
    // A relay that names another set reaches no member bucket.
    let mut other = HeaderMap::new();
    other.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    other.insert(RELAY_SET_HEADER, Ulid::nil().to_string().parse().unwrap());
    assert!(matches!(
        usage(&node, &node.owner, other, true).await,
        Err(ServerError::Conflict(_))
    ));
}
