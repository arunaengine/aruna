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

async fn files(
    node: &Node,
    auth: &AuthContext,
    version_id: &str,
    params: ReleaseFilesParams,
) -> ServerResult<ReleaseFilesResponse> {
    list_release_files(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        HeaderMap::new(),
        Path((BUCKET.to_string(), version_id.to_string())),
        Query(params),
    )
    .await
    .map(|Json(answer)| answer)
}

fn params(prefix: Option<&str>, delimiter: Option<&str>) -> ReleaseFilesParams {
    ReleaseFilesParams {
        key: "db/release.json".to_string(),
        prefix: prefix.map(str::to_string),
        delimiter: delimiter.map(str::to_string),
        after: None,
        limit: None,
    }
}

#[tokio::test]
async fn browses_release_files() {
    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    store(&node, "db/sub/b.fasta", ">bb").await;
    store(&node, "other/c.fasta", ">c").await;
    let doi = "https://doi.org/10.5281/zenodo.1";
    let release = publish(&node, &node.owner, request(doi)).await.unwrap();
    let version = release.version_id.as_str();

    let folders = files(&node, &node.owner, version, params(Some("db/"), Some("/")))
        .await
        .unwrap();
    assert_eq!(
        (folders.label.as_str(), folders.prefix.as_str()),
        ("6.0", "db/")
    );
    let keys: Vec<&str> = folders.files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(
        (keys, folders.folders.clone()),
        (vec!["db/a.fasta"], vec!["db/sub/".to_string()])
    );
    assert_eq!(folders.files[0].size, 2);
    let pinned = folders.files[0].version_id.clone();
    // A newer version of a pinned key leaves the release as it was.
    store(&node, "db/a.fasta", ">a2").await;
    let all = files(&node, &node.owner, version, params(None, None))
        .await
        .unwrap();
    let keys: Vec<&str> = all.files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(keys, ["db/a.fasta", "db/sub/b.fasta"]);
    assert_eq!(all.files[0].version_id, pinned);
    assert_eq!(all.next, None);

    let stranger = files(&node, &node.stranger, version, params(None, None)).await;
    assert!(
        matches!(stranger, Err(ServerError::Forbidden)),
        "{stranger:?}"
    );
    let unknown = files(
        &node,
        &node.owner,
        &Ulid::from_bytes([77; 16]).to_string(),
        params(None, None),
    )
    .await;
    assert!(matches!(unknown, Err(ServerError::NotFound)), "{unknown:?}");
    let pinned_file = ReleaseFilesParams {
        key: "db/a.fasta".to_string(),
        ..params(None, None)
    };
    let untagged = files(&node, &node.owner, &pinned, pinned_file).await;
    assert!(
        matches!(untagged, Err(ServerError::NotFound)),
        "{untagged:?}"
    );
    for bad in [
        ReleaseFilesParams {
            limit: Some(0),
            ..params(None, None)
        },
        params(None, Some(",")),
    ] {
        let refused = files(&node, &node.owner, version, bad).await;
        assert!(matches!(refused, Err(ServerError::BadRequestMessage(_))));
    }
}

#[tokio::test]
async fn purge_keeps_pinned() {
    use crate::routes::storage::deletion::{PurgeScopeRequest, SubmitPurgeRequest, submit_purge};
    use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
    use aruna_operations::driver::drive;
    use aruna_operations::s3::object::delete::pinned::{PinnedError, refuse_pinned, refuse_scope};
    use aruna_operations::s3::object::delete::{
        DeleteObjectError, DeleteObjectInput, DeleteObjectOperation,
    };

    let node = node_state().await;
    store(&node, "db/a.fasta", ">a").await;
    store(&node, "other/c.fasta", ">c").await;
    let doi = "https://doi.org/10.5281/zenodo.1";
    let release = publish(&node, &node.owner, request(doi)).await.unwrap();
    let listed = files(&node, &node.owner, &release.version_id, params(None, None))
        .await
        .unwrap();
    let pinned: Ulid = listed.files[0].version_id.parse().unwrap();
    store(&node, "db/a.fasta", ">a2").await;
    let current = origin(&node, "db/a.fasta")
        .await
        .version_id
        .parse()
        .unwrap();

    let state = &node.state;
    let info = bucket_info(state, BUCKET).await.unwrap();
    let scope = BucketScope {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        bucket: BUCKET,
        info: &info,
    };
    let caller = &node.owner;
    let context = state.get_ctx();
    let refused = refuse_pinned(context.as_ref(), scope, caller, ("db/a.fasta", pinned)).await;
    assert!(
        matches!(refused, Err(PinnedError::Version { .. })),
        "the pinned version is kept: {refused:?}"
    );
    // A caller who cannot read the release crate is refused too, without its name.
    let (realm, node_id) = (state.get_realm_id(), state.get_node_id());
    let path = object_permission_path(realm, node.group_id, node_id, BUCKET, "db/a.fasta");
    let blind = limited(&node, &[(path, Permission::WRITE)]);
    let refused = refuse_pinned(context.as_ref(), scope, &blind, ("db/a.fasta", pinned)).await;
    assert!(matches!(refused, Err(PinnedError::Version { .. })));
    let file = StoragePurgeScope::File {
        bucket: BUCKET.to_string(),
        key: "db/a.fasta".to_string(),
    };
    let refused = refuse_scope(context.as_ref(), scope, &blind, &file).await;
    assert_eq!(refused, Err(PinnedError::Scope));
    // The newer version and the release crate itself belong to no release.
    let newer = refuse_pinned(context.as_ref(), scope, caller, ("db/a.fasta", current)).await;
    let guards = newer.expect("no release pins the newer version");
    let crate_version = release.version_id.parse().unwrap();
    let own = refuse_pinned(
        context.as_ref(),
        scope,
        caller,
        ("db/release.json", crate_version),
    );
    assert!(own.await.is_ok());
    // A release published after the check pins the newer version: the checked delete fails.
    let mut later = request(doi);
    later.label = "6.1".to_string();
    publish(&node, &node.owner, later).await.unwrap();
    let delete = DeleteObjectOperation::new(DeleteObjectInput {
        bucket: BUCKET.to_string(),
        key: "db/a.fasta".to_string(),
        version_id: Some(current),
        group_id: info.group_id,
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        deleted_by: node.owner.user_id,
    })
    .with_fence(guards, Vec::new());
    let raced = drive(delete, context.as_ref()).await;
    assert_eq!(raced, Err(DeleteObjectError::FenceTaken));
    let again = refuse_pinned(context.as_ref(), scope, caller, ("db/a.fasta", current)).await;
    assert!(matches!(again, Err(PinnedError::Version { .. })));

    let shared = context.as_ref();
    let purge = |purged: StoragePurgeScope| async move {
        refuse_scope(shared, scope, caller, &purged).await
    };
    let file = StoragePurgeScope::File {
        bucket: BUCKET.to_string(),
        key: "db/a.fasta".to_string(),
    };
    assert_eq!(purge(file).await, Err(PinnedError::Scope));
    let folder = |prefix: &str| StoragePurgeScope::Prefix {
        bucket: BUCKET.to_string(),
        prefix: prefix.to_string(),
    };
    assert!(
        purge(folder("d")).await.is_err(),
        "a folder holding the release"
    );
    assert_eq!(purge(folder("other/")).await, Ok(()));
    let bucket = StoragePurgeScope::Bucket {
        bucket: BUCKET.to_string(),
    };
    assert_eq!(purge(bucket).await, Ok(()));

    let submitted = submit_purge(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Json(SubmitPurgeRequest {
            scope: PurgeScopeRequest::File {
                bucket: BUCKET.to_string(),
                key: "db/a.fasta".to_string(),
            },
            idempotency_key: None,
            delete_storage_files: false,
            confirm: None,
        }),
    )
    .await;
    let Err(ServerError::Conflict(message)) = submitted else {
        panic!("the purge of a pinned file is refused: {submitted:?}");
    };
    assert!(!message.contains("6.0"), "{message}");
}
