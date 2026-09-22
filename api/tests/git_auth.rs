//! Keeps Git credential translation and challenges confined to the Git transport.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_api::routes::git::credentials;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::{Router, middleware, routing::get};
use tower::ServiceExt;

#[tokio::test]
async fn basic_scoped() {
    let router = Router::new()
        .route(
            "/git/repository/info/refs",
            get(|headers: HeaderMap| async move {
                headers[header::AUTHORIZATION].to_str().unwrap().to_string()
            }),
        )
        .route(
            "/metadata",
            get(|headers: HeaderMap| async move {
                headers[header::AUTHORIZATION].to_str().unwrap().to_string()
            }),
        )
        .layer(middleware::from_fn(credentials));
    for (path, expected) in [
        ("/git/repository/info/refs", "Bearer token"),
        ("/metadata", "Basic dXNlcjp0b2tlbg=="),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::AUTHORIZATION, "Basic dXNlcjp0b2tlbg==")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn challenge_scoped() {
    let router = Router::new()
        .route(
            "/git/repository/info/refs",
            get(|| async { StatusCode::UNAUTHORIZED }),
        )
        .route("/metadata", get(|| async { StatusCode::UNAUTHORIZED }))
        .layer(middleware::from_fn(credentials));
    for (path, challenge) in [("/git/repository/info/refs", true), ("/metadata", false)] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::AUTHORIZATION, "Basic invalid!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().contains_key(header::WWW_AUTHENTICATE),
            challenge
        );
    }
}
