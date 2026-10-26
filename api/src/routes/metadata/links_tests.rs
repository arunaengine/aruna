//! Tests the data link routes: which datasets link an identifier, a folder or a file, the
//! datasets a file is part of and the bucket a part resolves to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::metadata::tests::{TestState, setup_state};
use crate::tests::datasets::{describe, store_bucket};
use aruna_core::UserId;
use aruna_core::structs::storage::data::link::LinkScope;

const BUCKET: &str = "reads";

fn scope(test: &TestState) -> LinkScope {
    LinkScope::Node {
        realm_id: test.state.get_realm_id(),
        node_id: test.state.get_node_id(),
        bucket: BUCKET.to_string(),
    }
}

fn folder(test: &TestState, prefix: &str) -> String {
    scope(test).link(prefix).to_id()
}

fn stranger(test: &TestState) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([60; 16]), test.state.get_realm_id()),
        realm_id: test.state.get_realm_id(),
        path_restrictions: None,
        session: None,
    }
}

fn bucket_params(key: Option<&str>, prefix: Option<&str>, parts_only: bool) -> BucketLinksParams {
    BucketLinksParams {
        key: key.map(str::to_string),
        version_id: None,
        prefix: prefix.map(str::to_string),
        parts_only,
        after: None,
        limit: None,
    }
}

async fn bucket(
    test: &TestState,
    auth: &AuthContext,
    name: &str,
    params: BucketLinksParams,
) -> ServerResult<DataLinksResponse> {
    bucket_links(
        State(test.state.clone()),
        Extension(Some(auth.clone())),
        Extension(None),
        Path(name.to_string()),
        Query(params),
    )
    .await
    .map(|Json(response)| response)
}

async fn by_id(test: &TestState, id: &str, parts_only: bool) -> ServerResult<DataLinksResponse> {
    data_links(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Query(DataLinksParams {
            id: id.to_string(),
            parts_only,
            after: None,
            limit: Some(500),
        }),
    )
    .await
    .map(|Json(response)| response)
}

#[tokio::test]
async fn finds_linking_datasets() {
    let test = setup_state().await;
    store_bucket(&test, BUCKET).await;
    let raw = folder(&test, "raw/");
    let dataset = describe(&test, "datasets/reads", true, &raw).await;

    let found = by_id(&test, &format!(" {raw} "), false).await.unwrap();
    assert_eq!(found.documents.len(), 1);
    let document = &found.documents[0];
    assert_eq!(document.document_id, dataset.to_string());
    assert_eq!(document.title.as_deref(), Some("Described data"));
    assert!(document.links.iter().any(|hit| hit.id == raw && hit.part));
    assert!(found.next_after.is_none());
    assert!(!found.hidden_references_exist);

    let other = by_id(&test, &folder(&test, "other/"), true).await.unwrap();
    assert!(other.documents.is_empty());
    let hash = format!("https://w3id.org/aruna/data/{}", hex::encode([9u8; 32]));
    assert!(
        by_id(&test, &hash, false)
            .await
            .unwrap()
            .documents
            .is_empty()
    );
    let unknown = by_id(&test, "https://example.org/other", false).await;
    assert!(matches!(unknown, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn links_by_folder() {
    let test = setup_state().await;
    store_bucket(&test, BUCKET).await;
    let dataset = describe(&test, "datasets/reads", true, &folder(&test, "raw/")).await;
    let root = describe(&test, "datasets/all", true, &folder(&test, "")).await;

    let raw = bucket(
        &test,
        &test.auth,
        BUCKET,
        bucket_params(None, Some("raw/"), false),
    )
    .await;
    let ids: Vec<_> = raw
        .unwrap()
        .documents
        .into_iter()
        .map(|d| d.document_id)
        .collect();
    assert_eq!(ids, vec![dataset.to_string()]);
    let whole = bucket(
        &test,
        &test.auth,
        BUCKET,
        bucket_params(None, Some(""), true),
    )
    .await;
    let ids: Vec<_> = whole
        .unwrap()
        .documents
        .into_iter()
        .map(|d| d.document_id)
        .collect();
    assert_eq!(ids, vec![root.to_string()]);

    // A file is covered by every linked part above it, never by a sibling folder.
    let file = bucket(
        &test,
        &test.auth,
        BUCKET,
        bucket_params(Some("raw/a.fq"), None, true),
    )
    .await
    .unwrap();
    let ids: Vec<_> = file
        .documents
        .iter()
        .map(|d| d.document_id.clone())
        .collect();
    assert!(ids.contains(&dataset.to_string()));
    assert!(ids.contains(&root.to_string()));
    let sibling = bucket(
        &test,
        &test.auth,
        BUCKET,
        bucket_params(Some("tmp/b.fq"), None, true),
    )
    .await
    .unwrap();
    let ids: Vec<_> = sibling
        .documents
        .iter()
        .map(|d| d.document_id.clone())
        .collect();
    assert_eq!(ids, vec![root.to_string()]);
}

#[tokio::test]
async fn bucket_links_refused() {
    let test = setup_state().await;
    store_bucket(&test, BUCKET).await;
    let both = bucket(
        &test,
        &test.auth,
        BUCKET,
        bucket_params(Some("a"), Some("b"), false),
    )
    .await;
    assert!(matches!(both, Err(ServerError::BadRequestReason(_))));
    let neither = bucket(&test, &test.auth, BUCKET, bucket_params(None, None, false)).await;
    assert!(matches!(neither, Err(ServerError::BadRequestReason(_))));
    let other = stranger(&test);
    let by_key = bucket(&test, &other, BUCKET, bucket_params(Some("a"), None, false)).await;
    assert!(matches!(by_key, Err(ServerError::Forbidden)));
    let by_prefix = bucket(&test, &other, BUCKET, bucket_params(None, Some(""), false)).await;
    assert!(matches!(by_prefix, Err(ServerError::Forbidden)));
    let absent = bucket(
        &test,
        &test.auth,
        "absent",
        bucket_params(None, Some(""), false),
    )
    .await;
    assert!(matches!(absent, Err(ServerError::NotFound)));
}

async fn part_of_file(
    test: &TestState,
    auth: &AuthContext,
    name: &str,
    key: &str,
) -> ServerResult<PartOfResponse> {
    file_part_of(
        State(test.state.clone()),
        Extension(Some(auth.clone())),
        Extension(None),
        Path(name.to_string()),
        Query(PartOfParams {
            key: key.to_string(),
        }),
    )
    .await
    .map(|Json(response)| response)
}

#[tokio::test]
async fn file_names_datasets() {
    let test = setup_state().await;
    store_bucket(&test, BUCKET).await;
    let raw = folder(&test, "raw/");
    let dataset = describe(&test, "datasets/reads", true, &raw).await;

    let inside = part_of_file(&test, &test.auth, BUCKET, "raw/a b.fq")
        .await
        .unwrap();
    assert_eq!(inside.datasets.len(), 1);
    assert_eq!(inside.datasets[0].document_id, dataset.to_string());
    assert_eq!(inside.datasets[0].part_ids, vec![raw]);
    assert!(!inside.more);
    assert_eq!(
        inside.more_datasets,
        format!("/api/v1/data/buckets/{BUCKET}/links?key=raw%2Fa+b.fq&parts_only=true")
    );
    let outside = part_of_file(&test, &test.auth, BUCKET, "tmp/a.fq")
        .await
        .unwrap();
    assert!(outside.datasets.is_empty());
    let denied = part_of_file(&test, &stranger(&test), BUCKET, "raw/a.fq").await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
    let absent = part_of_file(&test, &test.auth, "absent", "raw/a.fq").await;
    assert!(matches!(absent, Err(ServerError::NotFound)));
}

async fn target_of(test: &TestState, id: &str) -> ServerResult<PartTargetResponse> {
    part_target(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Query(PartTargetParams { id: id.to_string() }),
    )
    .await
    .map(|Json(response)| response)
}

#[tokio::test]
async fn parts_resolve_buckets() {
    let test = setup_state().await;
    store_bucket(&test, BUCKET).await;
    let raw = folder(&test, "raw/");
    let part = target_of(&test, &raw).await.unwrap();
    assert_eq!(part.id, raw);
    assert_eq!(part.bucket.as_deref(), Some(BUCKET));
    assert_eq!(part.prefix, "raw/");
    assert_eq!(part.node_id, Some(test.state.get_node_id().to_string()));
    assert!(part.set_id.is_none());
    assert!(part.held_here);

    let remote = LinkScope::Node {
        realm_id: test.state.get_realm_id(),
        node_id: iroh::SecretKey::from_bytes(&[61; 32]).public(),
        bucket: BUCKET.to_string(),
    };
    let elsewhere = target_of(&test, &remote.link("").to_id()).await.unwrap();
    assert!(!elsewhere.held_here);

    let set = LinkScope::Set {
        realm_id: test.state.get_realm_id(),
        set_id: Ulid::from_bytes([62; 16]),
    };
    let unknown = target_of(&test, &set.link("x/").to_id()).await.unwrap();
    assert_eq!(unknown.set_id, Some(Ulid::from_bytes([62; 16]).to_string()));
    assert!(unknown.bucket.is_none());
    assert!(!unknown.held_here);

    let version = format!("https://w3id.org/aruna/data/{}", hex::encode([9u8; 32]));
    assert!(matches!(
        target_of(&test, &version).await,
        Err(ServerError::BadRequestReason(_))
    ));
}

#[tokio::test]
async fn confirms_hidden_dataset() {
    // Only the named dataset a caller may not read is confirmed, by id and without its metadata.
    use aruna_operations::metadata::api::links_local;
    let test = setup_state().await;
    store_bucket(&test, BUCKET).await;
    let raw = folder(&test, "raw/");
    let dataset = describe(&test, "datasets/private", false, &raw).await;
    let request = |confirm| LinkNodeRequest {
        queries: vec![LinkQuery::Exact(DataLink::parse(&raw).unwrap())],
        parts_only: true,
        after: None,
        limit: 10,
        confirm,
    };
    let context = test.state.get_ctx();
    let realm_id = test.state.get_realm_id();
    let other = stranger(&test);

    let named = links_local(&context, realm_id, Some(&other), request(Some(dataset)))
        .await
        .unwrap();
    assert!(named.documents.is_empty());
    assert!(named.hidden && named.confirmed);
    let unrelated = Some(Ulid::from_bytes([61; 16]));
    let page = links_local(&context, realm_id, Some(&other), request(unrelated))
        .await
        .unwrap();
    assert!(page.hidden && !page.confirmed);
    let reader = links_local(&context, realm_id, Some(&test.auth), request(Some(dataset)))
        .await
        .unwrap();
    assert_eq!(reader.documents.len(), 1);
    assert!(!reader.confirmed);
}
