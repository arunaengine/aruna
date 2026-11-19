//! Tests the order of relay targets, the routes the relay names and the answer of a failed relay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{MANAGEMENT_URL_TTL, healthy_first};
use crate::error::{ErrorResponse, ServerError};
use aruna_operations::relay::management::{RELAYED_ROUTES, relay_route};
use aruna_operations::s3::object::member_rest::API_PREFIX;
use axum::body::to_bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::collections::BTreeMap;
use std::time::Instant;

#[test]
fn failed_target_last() {
    let targets = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let failed = BTreeMap::from([("a".to_string(), Instant::now())]);
    assert_eq!(healthy_first(targets.clone(), &failed), ["b", "c", "a"]);
    // An old failure no longer demotes its target.
    let old = Instant::now().checked_sub(MANAGEMENT_URL_TTL).unwrap();
    let failed = BTreeMap::from([("a".to_string(), old)]);
    assert_eq!(healthy_first(targets, &failed), ["a", "b", "c"]);
}

#[tokio::test]
async fn failed_relay_named() {
    let response = ServerError::RelayFailed.into_response();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: ErrorResponse =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body.code.as_deref(), Some("relay_failed"));
}

#[test]
fn allowlist_matches_router() {
    // A renamed or removed route must not leave a dead allowlist entry
    // behind, silently turning a relayed route into a local 403.
    let documented = crate::routes::rest_openapi().paths.paths;
    for (method, route) in RELAYED_ROUTES {
        let item = documented
            .get(*route)
            .unwrap_or_else(|| panic!("{route} is not a registered route"));
        let registered = match *method {
            "DELETE" => item.delete.is_some(),
            "GET" => item.get.is_some(),
            "PATCH" => item.patch.is_some(),
            "POST" => item.post.is_some(),
            "PUT" => item.put.is_some(),
            other => panic!("{other} is not covered by the allowlist check"),
        };
        assert!(registered, "{method} {route} is not a registered route");
        assert!(
            relay_route(method, Some(&format!("{API_PREFIX}{route}")), false).is_some(),
            "{method} {route} must match the relay allowlist"
        );
    }
}
