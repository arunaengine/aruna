//! Tests the text write route: input refusals, permissions, local writes and relayed writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
};
use axum::http::StatusCode;

fn request(key: &str) -> TextObjectRequest {
    TextObjectRequest {
        key: key.to_string(),
        text: "# Notes\n".to_string(),
        content_type: Some("text/markdown".to_string()),
    }
}

async fn write(
    node: &Node,
    auth: &AuthContext,
    bucket: &str,
    body: TextObjectRequest,
) -> ServerResult<Json<WriteObjectOutput>> {
    write_text_object(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        HeaderMap::new(),
        Path(bucket.to_string()),
        Json(body),
    )
    .await
}

#[tokio::test]
async fn refuses_bad_writes() {
    let node = node_state().await;
    let owner = &node.owner;
    assert!(matches!(
        write(&node, owner, BUCKET, request("")).await,
        Err(ServerError::BadRequestReason(_))
    ));
    assert!(matches!(
        write(&node, owner, BUCKET, request("../escape.md")).await,
        Err(ServerError::BadRequestReason(_))
    ));
    let mut large = request("large.md");
    large.text = "x".repeat(MAX_TEXT_BYTES + 1);
    assert!(matches!(
        write(&node, owner, BUCKET, large).await,
        Err(ServerError::PayloadTooLarge(_))
    ));
    assert!(matches!(
        write(&node, owner, "missing", request("a.md")).await,
        Err(ServerError::NotFound)
    ));
    assert!(matches!(
        write(&node, &node.stranger, BUCKET, request("a.md")).await,
        Err(ServerError::Forbidden)
    ));
    let anonymous = write_text_object(
        State(node.state.clone()),
        Extension(None),
        Extension(None),
        HeaderMap::new(),
        Path(BUCKET.to_string()),
        Json(request("a.md")),
    )
    .await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn stores_text_locally() {
    let node = node_state().await;
    let Json(stored) = write(&node, &node.owner, BUCKET, request("notes/a.md"))
        .await
        .unwrap();
    assert_eq!(
        (stored.bucket.as_str(), stored.key.as_str()),
        (BUCKET, "notes/a.md")
    );
    assert_eq!(stored.size, 8);
    assert_eq!(stored.content_type, "text/markdown");
    // A key whose range this node coordinates is written here, never relayed.
    distribute(&node.state, node.group_id, BUCKET).await;
    let peer = mock_peer(StatusCode::OK, "{}").await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let Json(local) = write(&node, &node.owner, BUCKET, request("a.md"))
        .await
        .unwrap();
    assert_eq!(local.key, "a.md");
    assert!(peer.paths.lock().unwrap().is_empty());
}

#[tokio::test]
async fn relays_to_coordinator() {
    let node = node_state().await;
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","key":"z.md","version_id":"v","size":8,"content_type":"text/markdown"}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;

    let Json(relayed) = write(&node, &node.owner, BUCKET, request("z.md"))
        .await
        .unwrap();

    assert_eq!(
        (relayed.key.as_str(), relayed.version_id.as_str()),
        ("z.md", "v")
    );
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(
        paths,
        vec!["/api/v1/data/buckets/routed/objects/text".to_string()]
    );
}

#[tokio::test]
async fn relay_failures_answer() {
    let node = node_state().await;
    distribute(&node.state, node.group_id, BUCKET).await;
    // Without a published api the coordinator cannot be reached: the range is degraded.
    let Err(ServerError::ServiceUnavailableReason(reason)) =
        write(&node, &node.owner, BUCKET, request("z.md")).await
    else {
        panic!("an unknown coordinator api answers 503");
    };
    assert!(reason.contains("degraded"));
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    assert!(matches!(
        write(&node, &node.owner, BUCKET, request("z.md")).await,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let refusal = mock_peer(StatusCode::FORBIDDEN, r#"{"error":"no grant"}"#).await;
    publish_api(&node.state, node_id(PEER), &refusal.url).await;
    assert!(matches!(
        write(&node, &node.owner, BUCKET, request("z.md")).await,
        Err(ServerError::Forbidden)
    ));
    let broken = mock_peer(StatusCode::OK, "not json").await;
    publish_api(&node.state, node_id(PEER), &broken.url).await;
    assert!(matches!(
        write(&node, &node.owner, BUCKET, request("z.md")).await,
        Err(ServerError::BadGatewayReason(_))
    ));
    // A relay needs the caller's token to pass on.
    let tokenless = write_text_object(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(None),
        HeaderMap::new(),
        Path(BUCKET.to_string()),
        Json(request("z.md")),
    )
    .await;
    assert!(matches!(tokenless, Err(ServerError::Unauthorized)));
}
