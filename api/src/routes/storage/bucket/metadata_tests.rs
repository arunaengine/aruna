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
    let parsed = Json::<PutMetadataRequest>::from_request(request, &()).await;
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

async fn stored(node: &Node) -> Option<BucketMetadataBody> {
    let Json(read) = get_bucket_metadata(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(None),
        Path(BUCKET.to_string()),
    )
    .await
    .unwrap();
    read.metadata
}

// A body without `metadata`, or with an unknown field, is refused and never clears what is set.
#[tokio::test]
async fn requires_metadata_field() {
    let node = node_state().await;
    put(&node, json!({"metadata": {"parser": null}}))
        .await
        .unwrap();

    let flat = put(&node, json!({"database": false})).await;
    assert!(
        matches!(flat, Err(ServerError::BadRequestReason(_))),
        "{flat:?}"
    );
    let extra = put(&node, json!({"metadata": {}, "dataset": "y"})).await;
    assert!(
        matches!(extra, Err(ServerError::BadRequestReason(_))),
        "{extra:?}"
    );
    assert!(stored(&node).await.is_some());

    let cleared = put(&node, json!({"metadata": null})).await.unwrap();
    assert!(cleared.metadata.is_none());
    assert!(stored(&node).await.is_none());
}

// The description lives only in the dataset: former description fields are refused, a database
// needs a dataset, and a dataset the caller cannot read is never linked.
#[tokio::test]
async fn description_in_dataset() {
    let node = node_state().await;
    for field in ["title", "description", "version", "license", "identifiers"] {
        let body = json!({"metadata": {"database": false, field: "x"}});
        let refused = put(&node, body).await;
        assert!(
            matches!(refused, Err(ServerError::BadRequestReason(_))),
            "{field}: {refused:?}"
        );
    }
    let without = put(&node, json!({"metadata": {"database": true}})).await;
    assert!(
        matches!(without, Err(ServerError::BadRequestReason(_))),
        "{without:?}"
    );
    let unknown = Ulid::from_bytes([77; 16]).to_string();
    let missing = put(&node, json!({"metadata": {"dataset": unknown}})).await;
    assert!(missing.is_err(), "{missing:?}");
    assert!(stored(&node).await.is_none(), "nothing was linked");
}
