//! Per-IP and per-principal request limits and local slot permits, with a retry time on denial.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::ErrorResponse;
use crate::forwarded::client_ip;
use crate::server::state::ServerState;
use aruna_admission::RateLimits;
pub(crate) use aruna_admission::{LocalKey, LocalLease, LocalPermit};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::user::id::UserId;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use std::net::IpAddr;
use std::sync::Arc;

impl std::fmt::Debug for ApiRateLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ApiRateLimits").finish()
    }
}

/// The HTTP and S3 view of the node's shared limits; other frontends hold the same `Arc`.
pub struct ApiRateLimits {
    shared: Arc<RateLimits>,
}

impl Default for ApiRateLimits {
    fn default() -> Self {
        Self::from_shared(Arc::new(RateLimits::default()))
    }
}

impl ApiRateLimits {
    /// Operator-configurable quotas; each value floors at one so a limiter is
    /// always valid.
    pub fn new(ip_per_minute: u32, ip_burst: u32, principal_per_minute: u32, burst: u32) -> Self {
        Self::from_shared(Arc::new(RateLimits::new(
            ip_per_minute,
            ip_burst,
            principal_per_minute,
            burst,
        )))
    }

    /// Wraps limits that the node shares with its filesystem frontends.
    pub fn from_shared(shared: Arc<RateLimits>) -> Self {
        Self { shared }
    }

    pub fn shared(&self) -> &Arc<RateLimits> {
        &self.shared
    }

    pub(crate) fn try_acquire_local(&self, key: LocalKey) -> Option<LocalPermit> {
        self.shared.try_acquire_local(key)
    }

    /// Tight quotas for tests that must observe a denial quickly.
    #[cfg(test)]
    pub(crate) fn for_test(burst: u32) -> Self {
        Self::new(60, burst, 60, burst)
    }

    /// Charges the caller's IP bucket. `Err` carries the seconds after which a
    /// retry can conform (never zero).
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), u64> {
        self.shared.check_ip(ip)
    }

    /// Charges the authenticated principal's bucket, independent of the IP
    /// bucket so a request is never double-charged for its address.
    pub fn check_principal(&self, principal: UserId) -> Result<(), u64> {
        self.shared.check_principal(principal)
    }

    /// Charges one share link open to the client address and to the link, so neither many
    /// addresses nor many links speed up password guessing.
    pub fn check_share_open(&self, ip: IpAddr, share_id: ulid::Ulid) -> Result<(), u64> {
        self.shared.check_share_open(ip, share_id)
    }

    /// Charges one account merge or sign-in attach to the calling account.
    pub fn check_account_link(&self, user_id: UserId) -> Result<(), u64> {
        self.shared.check_account_link(user_id)
    }
}

/// REST IP limiter. Runs at the outer transport boundary, before CORS, bearer
/// parsing, or extraction, so an invalid or expensive authentication attempt
/// still consumes IP capacity.
pub async fn ip_middleware(
    State(state): State<std::sync::Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    // Direct router calls (tests) carry no connect info; production serves
    // with connect info and always attributes the transport peer.
    let peer = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0.ip())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let ip = client_ip(state.trusted_proxies(), peer, request.headers());
    match state.rate_limits().check_ip(ip) {
        Ok(()) => next.run(request).await,
        Err(retry_after) => too_many_requests(retry_after),
    }
}

/// REST principal limiter. Runs after authentication so an authenticated caller
/// is charged once to its principal, on top of the outer IP charge.
pub async fn principal_middleware(
    State(state): State<std::sync::Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let principal = request
        .extensions()
        .get::<Option<AuthContext>>()
        .and_then(Option::as_ref)
        .map(|auth| auth.user_id);
    let Some(principal) = principal else {
        return next.run(request).await;
    };
    match state.rate_limits().check_principal(principal) {
        Ok(()) => next.run(request).await,
        Err(retry_after) => too_many_requests(retry_after),
    }
}

fn too_many_requests(retry_after: u64) -> Response {
    let body = serde_json::to_vec(&ErrorResponse {
        error: "too many requests".to_string(),
        code: Some("rate_limited".to_string()),
        details: None,
        violations: None,
        findings: None,
        omitted_findings: None,
        quota: None,
    })
    .unwrap_or_else(|_| b"{\"error\":\"too many requests\"}".to_vec());
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry_after));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}
