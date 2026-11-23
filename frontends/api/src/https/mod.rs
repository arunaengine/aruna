//! Native HTTPS: the public routes by hostname and the request host they accept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod certificate;
pub mod listener;
pub mod redirect;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use aruna_core::listener::{HostRoute, HostRoutes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The host of a request, from the URI authority (HTTP/2 or absolute form) or else
/// `Host`, in lower case and without a port.
pub fn request_host(uri: &Uri, headers: &HeaderMap) -> Option<String> {
    let parsed;
    let authority = match uri.authority() {
        Some(authority) => authority,
        None => {
            parsed = headers
                .get(header::HOST)?
                .to_str()
                .ok()?
                .parse::<http::uri::Authority>()
                .ok()?;
            &parsed
        }
    };
    let host = authority.host().trim_end_matches('.');
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

/// The route a TLS connection was accepted for. Requests on it must name a host
/// of the same route; others get `421` so a client retries on a new connection.
#[derive(Clone, Debug)]
pub struct HostCheck {
    routes: Arc<HostRoutes>,
    route: HostRoute,
}

impl HostCheck {
    pub fn new(routes: Arc<HostRoutes>, route: HostRoute) -> Self {
        Self { routes, route }
    }

    pub fn route(&self) -> HostRoute {
        self.route
    }

    pub fn routes(&self) -> &Arc<HostRoutes> {
        &self.routes
    }

    /// Whether a request with this URI and headers belongs to the connection's route.
    pub fn allows(&self, uri: &Uri, headers: &HeaderMap) -> bool {
        request_host(uri, headers)
            .and_then(|host| self.routes.route_for(&host))
            .is_some_and(|route| route == self.route)
    }
}

/// Answers `421 Misdirected Request` for a request whose host belongs to another route.
pub async fn check_host(State(check): State<HostCheck>, request: Request, next: Next) -> Response {
    if !check.allows(request.uri(), request.headers()) {
        return StatusCode::MISDIRECTED_REQUEST.into_response();
    }
    next.run(request).await
}
