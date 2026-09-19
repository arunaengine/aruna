//! Tests relay route matching, target selection, header filtering and retry decisions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    API_PREFIX, RELAYED_ROUTES, may_try_next, may_try_response, relay_route, relay_targets,
    relay_url, relayed_headers,
};
use crate::error::{ErrorResponse, ServerError};
use axum::body::to_bytes;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::IntoResponse;

#[test]
fn matches_allowlisted_routes() {
    assert_eq!(
        relay_route(
            &Method::PUT,
            Some("/api/v1/system/realm/quota"),
            false,
            false
        ),
        Some("/system/realm/quota")
    );
    assert_eq!(
        relay_route(
            &Method::DELETE,
            Some("/api/v1/access/onboarding/secrets/{id}"),
            false,
            false
        ),
        Some("/access/onboarding/secrets/{id}")
    );
    assert_eq!(
        relay_route(&Method::GET, Some("/api/v1/access/token"), false, false),
        Some("/access/token")
    );
    assert_eq!(
        relay_route(&Method::POST, Some("/api/v1/access/sessions"), false, false),
        Some("/access/sessions")
    );
    // Node-local admin routes stay on the node they were called on.
    assert_eq!(
        relay_route(&Method::POST, Some("/api/v1/compute/drain"), false, false),
        None
    );
    assert_eq!(
        relay_route(
            &Method::GET,
            Some("/api/v1/data/placement/diagnostics"),
            false,
            false
        ),
        None
    );
    // The method is part of the match: only PUT on the quota route relays.
    assert_eq!(
        relay_route(
            &Method::GET,
            Some("/api/v1/system/realm/quota"),
            false,
            false
        ),
        None
    );
}

#[test]
fn hop_stops_relay() {
    assert_eq!(
        relay_route(
            &Method::PUT,
            Some("/api/v1/system/realm/quota"),
            false,
            true
        ),
        None
    );
}

#[test]
fn management_answers_itself() {
    assert_eq!(
        relay_route(
            &Method::PUT,
            Some("/api/v1/system/realm/quota"),
            true,
            false
        ),
        None
    );
}

#[test]
fn builds_target_url() {
    // Node info documents publish `API_PUBLIC_URL` as a bare origin.
    let uri: Uri = "/api/v1/access/onboarding/secrets?limit=5".parse().unwrap();
    assert_eq!(
        relay_url("http://127.0.0.1:43001", &uri),
        "http://127.0.0.1:43001/api/v1/access/onboarding/secrets?limit=5"
    );
    assert_eq!(
        relay_url("https://mgmt.example.test/api/v1/", &uri),
        "https://mgmt.example.test/api/v1/access/onboarding/secrets?limit=5"
    );
}

#[test]
fn uses_installed_urls() {
    // A device resolves no peer from node-info documents it never holds.
    let installed = vec!["https://mgmt.example.test/api/v1".to_string()];
    assert_eq!(
        relay_targets(Vec::new(), installed.clone()),
        installed,
        "a device relays to the list its realm installed"
    );
    let peers = vec!["https://peer.example.test/api/v1".to_string()];
    assert_eq!(
        relay_targets(peers.clone(), installed),
        peers,
        "a resolved peer is never displaced by an installed copy"
    );
    assert!(relay_targets(Vec::new(), Vec::new()).is_empty());
}

#[test]
fn connect_failure_advances() {
    assert!(may_try_next(&Method::POST, true));
    assert!(may_try_next(&Method::GET, true));
}

#[test]
fn enrollment_miss_advances() {
    assert!(may_try_response(
        "/access/onboarding/secrets/{id}",
        StatusCode::NOT_FOUND
    ));
    assert!(may_try_response(
        "/access/onboarding/secrets/{id}/status",
        StatusCode::NOT_FOUND
    ));
    assert!(may_try_response(
        "/access/users/me/devices/{id}",
        StatusCode::NOT_FOUND
    ));
    assert!(may_try_response(
        "/access/onboarding/bootstrap",
        StatusCode::UNAUTHORIZED
    ));
    assert!(!may_try_response(
        "/access/onboarding/secrets",
        StatusCode::NOT_FOUND
    ));
}

#[test]
fn keeps_retry_after() {
    let mut headers = HeaderMap::new();
    headers.insert(header::RETRY_AFTER, HeaderValue::from_static("7"));
    headers.insert(header::SET_COOKIE, HeaderValue::from_static("secret=value"));

    assert_eq!(
        relayed_headers(&headers),
        vec![(header::RETRY_AFTER, HeaderValue::from_static("7"))]
    );
}

#[test]
fn get_failure_advances() {
    assert!(may_try_next(&Method::GET, false));
}

#[tokio::test]
async fn post_failure_stops() {
    // A failure after the connect succeeded may already have minted a secret.
    assert!(!may_try_next(&Method::POST, false));
    assert!(!may_try_next(&Method::DELETE, false));

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
            relay_route(
                &Method::from_bytes(method.as_bytes()).expect("valid method"),
                Some(&format!("{API_PREFIX}{route}")),
                false,
                false
            )
            .is_some(),
            "{method} {route} must match the relay allowlist"
        );
    }
}
