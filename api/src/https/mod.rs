//! Native HTTPS: the public routes by hostname and the request host they accept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod certificate;
pub mod listener;
pub mod redirect;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The service a TLS connection reaches, chosen by its SNI name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HostRoute {
    Rest,
    S3,
    Portal,
}

/// One routed service with its public origin, such as `https://api.example.org:8443`.
#[derive(Clone, Debug)]
struct RouteOrigin {
    route: HostRoute,
    host: String,
    origin: String,
}

/// The configured public hostnames. S3 also owns every subdomain of its host,
/// for virtual-hosted buckets.
#[derive(Clone, Debug)]
pub struct HostRoutes {
    origins: Vec<RouteOrigin>,
}

impl HostRoutes {
    /// Takes the HTTPS origins the settings validated: a host and an optional port only.
    pub fn new(rest: &url::Url, s3: Option<&url::Url>, portal: Option<&url::Url>) -> Self {
        let origins = [
            (HostRoute::Rest, Some(rest)),
            (HostRoute::S3, s3),
            (HostRoute::Portal, portal),
        ]
        .into_iter()
        .filter_map(|(route, url)| {
            let url = url?;
            Some(RouteOrigin {
                route,
                host: url.host_str()?.to_ascii_lowercase(),
                origin: url.origin().ascii_serialization(),
            })
        })
        .collect();
        Self { origins }
    }

    /// The route for an SNI name or request host: an exact host, or an S3 subdomain.
    pub fn route_for(&self, host: &str) -> Option<HostRoute> {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        self.exact_route(&host).or_else(|| {
            self.origins
                .iter()
                .find(|origin| {
                    origin.route == HostRoute::S3
                        && host
                            .strip_suffix(origin.host.as_str())
                            .is_some_and(|rest| rest.len() > 1 && rest.ends_with('.'))
                })
                .map(|origin| origin.route)
        })
    }

    /// The route whose configured host equals `host` exactly, ignoring case.
    pub fn exact_route(&self, host: &str) -> Option<HostRoute> {
        self.origins
            .iter()
            .find(|origin| origin.host.eq_ignore_ascii_case(host))
            .map(|origin| origin.route)
    }

    /// The configured origin of a route, without a trailing slash.
    pub fn origin(&self, route: HostRoute) -> Option<&str> {
        self.origins
            .iter()
            .find(|origin| origin.route == route)
            .map(|origin| origin.origin.as_str())
    }

    /// The configured host of a route, in lower case.
    pub fn host(&self, route: HostRoute) -> Option<&str> {
        self.origins
            .iter()
            .find(|origin| origin.route == route)
            .map(|origin| origin.host.as_str())
    }

    /// The exact hosts every certificate must cover.
    pub fn hosts(&self) -> impl Iterator<Item = &str> {
        self.origins.iter().map(|origin| origin.host.as_str())
    }

    /// Whether a route is configured.
    pub fn contains(&self, route: HostRoute) -> bool {
        self.origins.iter().any(|origin| origin.route == route)
    }
}

/// The host of a request, from the URI authority (HTTP/2 or absolute form) or else
/// `Host`, in lower case and without a port.
pub fn request_host(uri: &Uri, headers: &HeaderMap) -> Option<String> {
    let authority = match uri.authority() {
        Some(authority) => authority.clone(),
        None => headers
            .get(header::HOST)?
            .to_str()
            .ok()?
            .parse::<http::uri::Authority>()
            .ok()?,
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
