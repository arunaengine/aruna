//! Tests that group backend routes demand group admin and round trip the cleanup policy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    CleanupPolicy, CleanupStrategy, CreateBackendRequest, backend_reclaim_status,
    create_group_backend, delete_group_backend, enable_group_backend, list_group_backends,
};
use crate::error::ServerError;
use crate::tests::storage_routing::setup_state;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use std::collections::HashMap;
use ulid::Ulid;

#[tokio::test]
async fn requires_group_admin() {
    // Write rights are not enough: a backend receives the group's data.
    let test = setup_state().await;

    let result = list_group_backends(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path(test.group_id.to_string()),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));

    let disabled = delete_group_backend(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path((test.group_id.to_string(), Ulid::generate().to_string())),
    )
    .await;

    assert!(matches!(disabled, Err(ServerError::Forbidden)));

    let enabled = enable_group_backend(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path((test.group_id.to_string(), Ulid::generate().to_string())),
    )
    .await;

    assert!(matches!(enabled, Err(ServerError::Forbidden)));

    let status = backend_reclaim_status(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path((test.group_id.to_string(), Ulid::generate().to_string())),
    )
    .await;

    assert!(matches!(status, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn openapi_lists_enable() {
    let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();

    for path in ["enable", "reclaim/status"] {
        assert!(
            openapi["paths"]
                .get(format!(
                    "/data/groups/{{group_id}}/storage/backends/{{backend_id}}/{path}"
                ))
                .is_some(),
            "{path}"
        );
    }
    for field in ["disabled", "cleanup"] {
        assert!(
            openapi["components"]["schemas"]["GroupBackendResponse"]["properties"]
                .get(field)
                .is_some()
        );
    }
}

#[test]
fn policy_round_trips() {
    // Omitted means retain, and a reclaim without a grace takes the default.
    assert_eq!(
        CleanupPolicy::resolve(None).unwrap(),
        CleanupStrategy::Retain
    );
    assert_eq!(
        CleanupPolicy::resolve(Some(CleanupPolicy {
            mode: "reclaim".to_string(),
            after_secs: None,
        }))
        .unwrap(),
        CleanupStrategy::Reclaim {
            after: CleanupStrategy::DEFAULT_RECLAIM_AFTER
        }
    );
    assert_eq!(
        CleanupPolicy::from(CleanupStrategy::Reclaim {
            after: std::time::Duration::from_secs(60)
        })
        .after_secs,
        Some(60)
    );
    for bad in [("reclaim", Some(0)), ("retain", Some(60)), ("purge", None)] {
        assert!(
            CleanupPolicy::resolve(Some(CleanupPolicy {
                mode: bad.0.to_string(),
                after_secs: bad.1,
            }))
            .is_err()
        );
    }
}

#[tokio::test]
async fn status_needs_backend() {
    // The record must exist and belong to the group before any queue scan.
    let test = setup_state().await;

    let result = backend_reclaim_status(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path((test.group_id.to_string(), Ulid::generate().to_string())),
    )
    .await;

    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn rejects_unknown_kind() {
    // WebDAV is deliberately not a write backend.
    let test = setup_state().await;

    let result = create_group_backend(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(CreateBackendRequest {
            name: "tenant".to_string(),
            kind: "webdav".to_string(),
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
            cleanup: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn lists_empty_backends() {
    let test = setup_state().await;

    let Json(listed) = list_group_backends(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
    )
    .await
    .unwrap();

    assert!(listed.backends.is_empty());
}

#[test]
fn request_hides_secrets() {
    let request = CreateBackendRequest {
        name: "n".into(),
        kind: "s3".into(),
        public_config: HashMap::new(),
        secret_config: HashMap::from([(
            "secret_access_key".to_string(),
            "canary-8d4f".to_string(),
        )]),
        cleanup: None,
    };
    let text = format!("{request:?}");
    assert!(!text.contains("canary-8d4f"), "{text}");
    assert!(text.contains("secret_access_key"), "{text}");
}
