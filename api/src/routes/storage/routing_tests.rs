//! Tests routing rule routes for round trips, admin checks and rejected rule targets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::openapi::ApiDoc;
use crate::tests::storage_routing::setup_state;

fn class_rule(class: &str) -> RoutingRuleRequest {
    RoutingRuleRequest {
        key_prefix: "archive/".to_string(),
        exact: false,
        target: RoutingTargetRequest {
            backend_id: None,
            class: Some(class.to_string()),
        },
    }
}

#[tokio::test]
async fn bucket_rules_roundtrip() {
    let test = setup_state().await;

    let Json(stored) = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest {
            rules: vec![class_rule("cold")],
        }),
    )
    .await
    .unwrap();

    assert_eq!(stored.rules, vec![class_rule("cold")]);
    // The node offers no cold backend, so the rule is stored with a warning.
    assert_eq!(stored.warnings.len(), 1);

    let Json(fetched) = get_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
    )
    .await
    .unwrap();

    assert_eq!(fetched.rules, stored.rules);
}

#[tokio::test]
async fn rejects_operator_backend() {
    let test = setup_state().await;

    let result = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest {
            rules: vec![RoutingRuleRequest {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTargetRequest {
                    backend_id: Some("cold".to_string()),
                    class: None,
                },
            }],
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn rejects_non_admin() {
    let test = setup_state().await;

    let result = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest { rules: Vec::new() }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn group_default_roundtrip() {
    let test = setup_state().await;

    let Json(empty) = get_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(empty.default_target, None);

    let Json(stored) = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            default_target: Some(RoutingTargetRequest {
                backend_id: None,
                class: Some("cold".to_string()),
            }),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        stored
            .default_target
            .and_then(|target| target.class)
            .as_deref(),
        Some("cold")
    );

    let Json(cleared) = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            default_target: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(cleared.default_target, None);
}

#[tokio::test]
async fn rejects_invalid_class() {
    let test = setup_state().await;

    let result = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            default_target: Some(RoutingTargetRequest {
                backend_id: None,
                class: Some("NOT VALID".to_string()),
            }),
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn rejects_foreign_backend() {
    // Nothing registered this id for the group, so the rule must not store.
    let test = setup_state().await;
    let foreign = Ulid::generate();

    let result = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest {
            rules: vec![RoutingRuleRequest {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTargetRequest {
                    backend_id: Some(foreign.to_string()),
                    class: None,
                },
            }],
        }),
    )
    .await;

    let Err(ServerError::BadRequestReason(reason)) = result else {
        panic!("expected a 400 naming the backend, got {result:?}")
    };
    assert!(reason.contains(&foreign.to_string()), "{reason}");
}

#[test]
fn openapi_lists_routes() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

    assert!(openapi["paths"]["/data/buckets/{bucket}/storage/routing"]["put"].is_object());
    assert!(openapi["paths"]["/data/groups/{group_id}/storage/routing"]["put"].is_object());
    assert!(
        openapi["components"]["schemas"]["BucketRoutingResponse"]["properties"]
            .get("warnings")
            .is_some()
    );
}
