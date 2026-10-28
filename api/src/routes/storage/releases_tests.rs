//! Tests publishing and listing releases, replay of an identical publish, and the releases an
//! origin read names for an exact version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::storage::origin::{OriginParams, OriginResponse, get_origin};
use crate::tests::distributed::{BUCKET, Node, node_state};
use axum::extract::Query;

async fn store(node: &Node, key: &str, text: &str) {
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    let input = WriteObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        text: text.to_string(),
        content_type: None,
    };
    store_text(&node.state, &node.owner, &info, input, HashMap::new())
        .await
        .unwrap();
}

fn request(url: &str) -> PublishReleaseRequest {
    PublishReleaseRequest {
        key: "db/release.json".to_string(),
        label: "6.0".to_string(),
        prefix: "db/".to_string(),
        upstream: vec![BucketIdentifierBody {
            name: "DOI".to_string(),
            value: "10.5281/zenodo.1".to_string(),
            url: url.to_string(),
        }],
        preparation: Vec::new(),
    }
}

async fn publish(
    node: &Node,
    auth: &AuthContext,
    request: PublishReleaseRequest,
) -> ServerResult<PublishReleaseResponse> {
    publish_release(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        HeaderMap::new(),
        Path(BUCKET.to_string()),
        Json(request),
    )
    .await
    .map(|Json(response)| response)
}

async fn origin(node: &Node, key: &str) -> OriginResponse {
    let params = OriginParams {
        key: key.to_string(),
        version_id: None,
    };
    let Json(answer) = get_origin(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        HeaderMap::new(),
        Path(BUCKET.to_string()),
        Query(params),
    )
    .await
    .unwrap();
    answer
}

#[tokio::test]
async fn release_pins_current() {
    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    store(&node, "other/b.fasta", ">b").await;
    let doi = "https://doi.org/10.5281/zenodo.1";

    let first = publish(&node, &node.owner, request(doi)).await.unwrap();
    assert_eq!(first.files, 1, "only files under the prefix are pinned");
    assert!(!first.replayed);
    let again = publish(&node, &node.owner, request(doi)).await.unwrap();
    assert!(again.replayed);
    assert_eq!(again.version_id, first.version_id);

    let Json(listed) = list_releases(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(BUCKET.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(listed.releases.len(), 1);
    assert_eq!(listed.releases[0].label, "6.0");
    assert!(listed.complete);

    let pinned = origin(&node, "db/a.fasta").await;
    assert_eq!(pinned.releases.len(), 1);
    assert_eq!(pinned.releases[0].upstream[0].url, doi);
    assert!(pinned.releases_complete);
    assert!(origin(&node, "other/b.fasta").await.releases.is_empty());
    // A newer version of a pinned key belongs to no release.
    store(&node, "db/a.fasta", ">a2").await;
    assert!(origin(&node, "db/a.fasta").await.releases.is_empty());

    let stranger = publish(&node, &node.stranger, request(doi)).await;
    assert!(matches!(stranger, Err(ServerError::Forbidden)));
    let unsafe_link = publish(&node, &node.owner, request("javascript:alert(1)")).await;
    assert!(matches!(unsafe_link, Err(ServerError::BadRequestReason(_))));
}
