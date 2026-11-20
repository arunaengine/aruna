//! The listener settings the node configuration and the api share: the public host routes, the
//! CORS origin rule, the S3 listener deadlines and the REST body limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

/// The REST request body size the api accepts unless the operator sets another.
pub const MAX_BODY_SIZE: usize = 1024 * 1024;
/// How long an S3 connection may stay silent before its first request, by default.
pub const INITIAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The idle bound an S3 connection falls back to when no configured one was installed.
pub const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// The total lifetime of one streamed S3 request, by default.
pub const STREAM_LIFETIME_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// A CORS origin as it is compared: without surrounding space or a trailing slash.
pub fn cors_origin(entry: &str) -> &str {
    entry.trim().trim_end_matches('/')
}

/// Whether the configured CORS `entries` admit `origin`: `*` admits every origin, and any other
/// entry the one origin it names.
pub fn cors_allows<'a>(entries: impl IntoIterator<Item = &'a str>, origin: &str) -> bool {
    let origin = cors_origin(origin);
    (entries.into_iter())
        .map(cors_origin)
        .any(|entry| entry == "*" || entry == origin)
}

/// Listener deadlines of the S3 plane: how long a connection may stay silent
/// before its first request, how long a request or response may make no I/O
/// progress, and the total lifetime of one streamed request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct S3ServerTimeouts {
    pub initial_request: Duration,
    pub connection_idle: Duration,
    pub stream_lifetime: Duration,
}

impl Default for S3ServerTimeouts {
    fn default() -> Self {
        Self {
            initial_request: INITIAL_REQUEST_TIMEOUT,
            connection_idle: CONNECTION_IDLE_TIMEOUT,
            stream_lifetime: STREAM_LIFETIME_TIMEOUT,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::cors_allows;

    #[test]
    fn cors_rule_matches() {
        let origin = "https://portal.example.org";
        assert!(cors_allows([" https://portal.example.org/ "], origin));
        assert!(cors_allows(["https://other.example.org", "*"], origin));
        assert!(cors_allows(["https://portal.example.org"], "https://portal.example.org/"));
        assert!(!cors_allows(["https://other.example.org"], origin));
        assert!(!cors_allows(["https://portal.example.org:8443"], origin));
        assert!(!cors_allows([], origin));
    }
}
