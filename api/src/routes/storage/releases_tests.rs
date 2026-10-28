//! Tests publishing and listing releases, replay of an identical publish, and the releases an
//! origin read names for an exact version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::BUCKET_RELEASE_KEYSPACE;
use aruna_core::structs::identity::auth::{PathRestriction, Permission};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::bucket_metadata::{ReleasePointer, release_key};
use aruna_operations::s3::object::origin::read_origin;

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
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

async fn list(node: &Node) -> ReleaseListResponse {
    let Json(listed) = list_releases(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        Path(BUCKET.to_string()),
    )
    .await
    .unwrap();
    listed
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

    let listed = list(&node).await;
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

/// The owner, limited to the given permission on each path.
fn limited(node: &Node, grants: &[(String, Permission)]) -> AuthContext {
    let restrictions = (grants.iter())
        .map(|(pattern, permission)| PathRestriction {
            pattern: pattern.clone(),
            permission: permission.clone(),
        })
        .collect();
    AuthContext {
        path_restrictions: Some(restrictions),
        ..node.owner.clone()
    }
}

#[tokio::test]
async fn publish_needs_reads() {
    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    let state = &node.state;
    let (realm, node_id) = (state.get_realm_id(), state.get_node_id());
    let object = |key: &str| object_permission_path(realm, node.group_id, node_id, BUCKET, key);
    let bucket = bucket_permission_path(realm, node.group_id, node_id, BUCKET);
    let admin = (group_admin_path(realm, node.group_id), Permission::WRITE);
    let release = (object("db/release.json"), Permission::WRITE);
    let cases = [
        (
            vec![admin.clone(), release.clone()],
            "no WRITE on the bucket",
        ),
        (
            vec![(bucket, Permission::WRITE), admin, release],
            "a file under the prefix is not readable",
        ),
    ];
    let info = bucket_info(state, BUCKET).await.unwrap();
    let scope = BucketScope {
        realm_id: realm,
        node_id,
        bucket: BUCKET,
        info: &info,
    };
    for (grants, reason) in cases {
        // The route answers each refusal with the generic 403, which names no file.
        let doi = "https://doi.org/10.5281/zenodo.1";
        let routed = publish(&node, &limited(&node, &grants), request(doi)).await;
        assert!(matches!(routed, Err(ServerError::Forbidden)), "{routed:?}");
        let input = PublishInput {
            auth: limited(&node, &grants),
            bearer: None,
            key: "db/release.json".to_string(),
            spec: release_spec(&request("https://doi.org/10.5281/zenodo.1")).unwrap(),
        };
        let refused =
            release_publish::publish_release(state.get_ctx().as_ref(), scope, input).await;
        assert!(matches!(refused, Err(PublishError::Forbidden(why)) if why == reason));
    }
    assert!(list(&node).await.releases.is_empty());
}

#[tokio::test]
async fn publish_once_concurrently() {
    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    let doi = "https://doi.org/10.5281/zenodo.1";
    let (first, second) = tokio::join!(
        publish(&node, &node.owner, request(doi)),
        publish(&node, &node.owner, request(doi)),
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.version_id, second.version_id, "one release is made");
    assert!(first.replayed != second.replayed);
}

#[tokio::test]
async fn pins_one_version() {
    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    store(&node, "db/a.fasta", ">a22").await;
    let place = (node.state.get_realm_id(), node.state.get_node_id());
    let context = node.state.get_ctx();
    let view = read_origin(context.as_ref(), place, BUCKET, "db/a.fasta", None)
        .await
        .unwrap();
    assert_eq!(view.size, Some(4), "the size is the current version's");
}

#[tokio::test]
async fn lists_only_read_crates() {
    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    publish(
        &node,
        &node.owner,
        request("https://doi.org/10.5281/zenodo.1"),
    )
    .await
    .unwrap();
    // A row whose version does not exist is no release.
    let pointer = ReleasePointer {
        key: "db/gone.json".to_string(),
        label: "5.0".to_string(),
        prefix: "db/".to_string(),
    };
    let write = StorageEffect::Write {
        key_space: BUCKET_RELEASE_KEYSPACE.to_string(),
        key: release_key(BUCKET, Ulid::from_bytes([9; 16])).into(),
        value: pointer.to_bytes().unwrap().into(),
        txn_id: None,
    };
    let context = node.state.get_ctx();
    context.storage_handle.send_storage_effect(write).await;
    let listed = list(&node).await;
    assert_eq!(listed.releases.len(), 1);
    assert_eq!(listed.releases[0].label, "6.0");
    assert!(listed.complete);
}
