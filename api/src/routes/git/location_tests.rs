//! Tests the dataset storage location routes: default, choice, validation and access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::error::ServerError;
use crate::metadata::{CreateMetadataRequest, CreateScaffoldRequest};
use crate::routes::metadata::documents::create_metadata_document;
use crate::routes::metadata::tests::{TestState, drain_metadata_background, setup_network_state};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use std::time::SystemTime;
use ulid::Ulid;

async fn create(test: &TestState, chosen: Option<StorageLocationRequest>) -> Ulid {
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: format!("datasets/{}", Ulid::generate()),
            name: "Located".to_string(),
            description: "Storage location test".to_string(),
            date_published: "2026-09-28".to_string(),
            license: None,
            public: false,
            message: None,
            storage_location: chosen,
        })),
    )
    .await
    .expect("create");
    drain_metadata_background(test.state.as_ref()).await;
    created.summary.document_id.parse().expect("document id")
}

async fn bucket(test: &TestState, name: &str) {
    let info = BucketInfo {
        group_id: test.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    let operation = CreateBucketOperation::new(name.to_string(), info);
    drive(operation, &test.state.get_ctx())
        .await
        .expect("bucket");
}

async fn read(
    test: &TestState,
    auth: &AuthContext,
    id: Ulid,
) -> Result<StorageLocation, ServerError> {
    get_location(
        State(test.state.clone()),
        Extension(Some(auth.clone())),
        Path(id),
    )
    .await
    .map(|Json(location)| location)
}

async fn choose(
    test: &TestState,
    auth: &AuthContext,
    id: Ulid,
    (bucket, prefix): (&str, &str),
) -> Result<StorageLocation, ServerError> {
    let request = StorageLocationRequest {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
    };
    put_location(
        State(test.state.clone()),
        Extension(Some(auth.clone())),
        Path(id),
        Json(request),
    )
    .await
    .map(|Json(location)| location)
}

#[tokio::test]
async fn location_routes() {
    let test = setup_network_state().await;
    let id = create(&test, None).await;
    let default = read(&test, &test.auth, id).await.expect("default");
    let group = test.group_id.to_string().to_lowercase();
    assert_eq!(default.bucket, format!("datasets-{group}"));
    assert_eq!(default.prefix, format!("{id}/"));
    assert!(default.default);
    for invalid in [("lab-data", "a/../b"), ("Lab_Data", "a")] {
        let refused = choose(&test, &test.auth, id, invalid).await;
        assert!(
            matches!(refused, Err(ServerError::BadRequestMessage(_))),
            "{invalid:?}"
        );
    }
    let missing = choose(&test, &test.auth, id, ("absent-bucket", "a")).await;
    assert!(matches!(missing, Err(ServerError::NotFound)));
    let realm_id = test.state.get_realm_id();
    let stranger = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    bucket(&test, "lab-data").await;
    let denied = choose(&test, &stranger, id, ("lab-data", "runs")).await;
    assert!(matches!(denied, Err(ServerError::Forbidden)), "{denied:?}");
    let chosen = choose(&test, &test.auth, id, ("lab-data", "/runs/liver")).await;
    let chosen = chosen.expect("chosen");
    assert_eq!(
        (chosen.prefix.as_str(), chosen.default),
        ("runs/liver/", false)
    );
    assert_eq!(read(&test, &test.auth, id).await.expect("read"), chosen);
}

#[tokio::test]
async fn create_chooses_location() {
    let test = setup_network_state().await;
    bucket(&test, "lab-data").await;
    let request = StorageLocationRequest {
        bucket: "lab-data".to_string(),
        prefix: "created".to_string(),
    };
    let id = create(&test, Some(request)).await;
    let chosen = read(&test, &test.auth, id).await.expect("read");
    assert_eq!(
        chosen,
        StorageLocation {
            bucket: "lab-data".to_string(),
            prefix: "created/".to_string(),
            default: false,
        }
    );
}
