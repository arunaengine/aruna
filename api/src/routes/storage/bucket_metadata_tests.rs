//! Tests that only an explicit `metadata` field changes a bucket's metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::extract::FromRequest;
use serde_json::json;

use super::*;
use crate::tests::distributed::{BUCKET, Node, node_state};

async fn put(node: &Node, body: serde_json::Value) -> ServerResult<BucketMetadataResponse> {
    let request = axum::http::Request::builder()
        .method("PUT")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let parsed = Json::<PutBucketMetadataRequest>::from_request(request, &()).await;
    put_bucket_metadata(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        Path(BUCKET.to_string()),
        parsed,
    )
    .await
    .map(|Json(response)| response)
}

async fn title(node: &Node) -> Option<String> {
    let Json(read) = get_bucket_metadata(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(BUCKET.to_string()),
    )
    .await
    .unwrap();
    read.metadata.and_then(|metadata| metadata.title)
}

// A body without `metadata`, or with an unknown field, is refused and never clears what is set.
#[tokio::test]
async fn requires_metadata_field() {
    let node = node_state().await;
    put(&node, json!({"metadata": {"title": "Bakta database"}}))
        .await
        .unwrap();

    let flat = put(&node, json!({"title": "Bakta", "database": true})).await;
    assert!(
        matches!(flat, Err(ServerError::BadRequestReason(_))),
        "{flat:?}"
    );
    let extra = put(&node, json!({"metadata": {"title": "x"}, "dataset": "y"})).await;
    assert!(
        matches!(extra, Err(ServerError::BadRequestReason(_))),
        "{extra:?}"
    );
    assert_eq!(title(&node).await.as_deref(), Some("Bakta database"));

    let cleared = put(&node, json!({"metadata": null})).await.unwrap();
    assert!(cleared.metadata.is_none());
    assert_eq!(title(&node).await, None);
}
