//! Serves a route set only on its configured host name, so one service answers two names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::Router;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;

/// `router` answering only requests for `host`, any other host gets 404; without a host it
/// answers on every host, as in tests and local use.
pub fn on_host(router: Router, host: Option<String>) -> Router {
    let Some(host) = host else {
        return router;
    };
    router.route_layer(middleware::from_fn(move |request: Request, next: Next| {
        let matches = request_host(&request).is_some_and(|named| named.eq_ignore_ascii_case(&host));
        async move {
            match matches {
                true => next.run(request).await,
                false => StatusCode::NOT_FOUND.into_response(),
            }
        }
    }))
}

/// The host a request names, without its port: the `Host` header, or the HTTP/2 authority.
fn request_host(request: &Request) -> Option<&str> {
    let named = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            request
                .uri()
                .authority()
                .map(|authority| authority.as_str())
        })?;
    match named.strip_prefix('[') {
        Some(bracketed) => bracketed.split_once(']').map(|(address, _)| address),
        None => Some(named.split_once(':').map_or(named, |(name, _)| name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    fn app(registry: Option<&str>, resolver: Option<&str>) -> Router {
        let registry_routes = Router::new().route("/v1/realms", get(|| async { "realms" }));
        let resolver_routes = Router::new().route("/aruna/{*path}", get(|| async { "resolve" }));
        on_host(registry_routes, registry.map(str::to_string))
            .merge(on_host(resolver_routes, resolver.map(str::to_string)))
    }

    async fn status(app: &Router, host: Option<&str>, path: &str) -> StatusCode {
        let mut request = Request::builder().uri(path);
        if let Some(host) = host {
            request = request.header(header::HOST, host);
        }
        let request = request.body(Body::empty()).unwrap();
        app.clone().oneshot(request).await.unwrap().status()
    }

    #[tokio::test]
    async fn routes_split_hosts() {
        let app = app(Some("registry.example.org"), Some("resolve.example.org"));
        let cases = [
            (Some("registry.example.org"), "/v1/realms", StatusCode::OK),
            (
                Some("resolve.example.org"),
                "/v1/realms",
                StatusCode::NOT_FOUND,
            ),
            (Some("RESOLVE.example.org:443"), "/aruna/x", StatusCode::OK),
            (
                Some("registry.example.org"),
                "/aruna/x",
                StatusCode::NOT_FOUND,
            ),
            (Some("other.example.org"), "/aruna/x", StatusCode::NOT_FOUND),
            (Some("[::1]:8080"), "/v1/realms", StatusCode::NOT_FOUND),
            (None, "/aruna/x", StatusCode::NOT_FOUND),
            (
                Some("resolve.example.org"),
                "/missing",
                StatusCode::NOT_FOUND,
            ),
        ];
        for (host, path, expected) in cases {
            assert_eq!(status(&app, host, path).await, expected, "{host:?} {path}");
        }
    }

    #[tokio::test]
    async fn unset_hosts_answer() {
        let app = app(None, None);
        for host in [None, Some("registry.example.org"), Some("anything.example")] {
            assert_eq!(status(&app, host, "/v1/realms").await, StatusCode::OK);
            assert_eq!(status(&app, host, "/aruna/x").await, StatusCode::OK);
        }
    }
}
