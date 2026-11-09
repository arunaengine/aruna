//! Redirects plain HTTP requests for a configured hostname to the same path on its HTTPS origin.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::{HostRoutes, request_host};

/// Answers every request with `308` to its route's origin, or `400` for an unknown host.
pub fn router(routes: Arc<HostRoutes>) -> Router {
    Router::new().fallback(redirect).with_state(routes)
}

/// Serves until `shutdown` is cancelled, then lets requests in flight finish.
pub async fn serve(
    listener: TcpListener,
    routes: Arc<HostRoutes>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    axum::serve(listener, router(routes))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

/// `Location` is the configured origin followed by the raw path and query, so
/// the request's host string never reaches it and nothing is decoded or joined.
async fn redirect(State(routes): State<Arc<HostRoutes>>, request: Request) -> Response {
    let origin = request_host(request.uri(), request.headers())
        .and_then(|host| routes.exact_route(&host))
        .and_then(|route| routes.origin(route));
    let target = request
        .uri()
        .path_and_query()
        .map(|target| target.as_str())
        .filter(|target| target.starts_with('/'));
    let location = origin
        .zip(target)
        .and_then(|(origin, target)| HeaderValue::from_str(&format!("{origin}{target}")).ok());
    match location {
        Some(location) => (
            StatusCode::PERMANENT_REDIRECT,
            [(header::LOCATION, location)],
        )
            .into_response(),
        None => StatusCode::BAD_REQUEST.into_response(),
    }
}
