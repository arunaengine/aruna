//! Native HTTPS: the public routes by hostname and the request host they accept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod certificate;

#[cfg(test)]
mod tests;

use axum::http::{HeaderMap, Uri, header};

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
