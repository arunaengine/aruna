//! Per-IP and per-principal token buckets shared by a request plane. Limits
//! bound abuse, not normal use: quotas are generous and identical for every
//! caller, and a denied request reports when to retry.

use crate::error::ErrorResponse;
use crate::forwarded::client_ip;
use crate::server_state::ServerState;
use aruna_core::structs::AuthContext;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use governor::clock::{Clock, DefaultClock};
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};

const IP_REQUESTS_PER_MINUTE: u32 = 6_000;
const IP_BURST: u32 = 1_000;
const PRINCIPAL_REQUESTS_PER_MINUTE: u32 = 3_000;
const PRINCIPAL_BURST: u32 = 500;
/// Every N checks the keyed stores drop entries that are fully replenished.
const MAINTENANCE_INTERVAL: u64 = 4_096;

type Keyed<K> = RateLimiter<K, DefaultKeyedStateStore<K>, DefaultClock>;

impl std::fmt::Debug for ApiRateLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ApiRateLimits").finish()
    }
}

pub struct ApiRateLimits {
    per_ip: Keyed<IpAddr>,
    per_principal: Keyed<String>,
    clock: DefaultClock,
    checks: AtomicU64,
}

impl Default for ApiRateLimits {
    fn default() -> Self {
        Self::new(
            IP_REQUESTS_PER_MINUTE,
            IP_BURST,
            PRINCIPAL_REQUESTS_PER_MINUTE,
            PRINCIPAL_BURST,
        )
    }
}

fn quota(per_minute: u32, burst: u32) -> Quota {
    Quota::per_minute(NonZeroU32::new(per_minute.max(1)).expect("nonzero rate"))
        .allow_burst(NonZeroU32::new(burst.max(1)).expect("nonzero burst"))
}

impl ApiRateLimits {
    /// Operator-configurable quotas; each value floors at one so a limiter is
    /// always valid.
    pub fn new(ip_per_minute: u32, ip_burst: u32, principal_per_minute: u32, burst: u32) -> Self {
        Self {
            per_ip: RateLimiter::keyed(quota(ip_per_minute, ip_burst)),
            per_principal: RateLimiter::keyed(quota(principal_per_minute, burst)),
            clock: DefaultClock::default(),
            checks: AtomicU64::new(0),
        }
    }

    /// Tight quotas for tests that must observe a denial quickly.
    #[cfg(test)]
    pub(crate) fn for_test(burst: u32) -> Self {
        Self::new(60, burst, 60, burst)
    }

    fn maybe_maintain(&self) {
        if self
            .checks
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(MAINTENANCE_INTERVAL)
        {
            self.per_ip.retain_recent();
            self.per_principal.retain_recent();
        }
    }

    /// Charges the caller's IP bucket. `Err` carries the seconds after which a
    /// retry can conform (never zero).
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), u64> {
        self.maybe_maintain();
        let now = self.clock.now();
        self.per_ip
            .check_key(&ip)
            .map_err(|not_until| retry_secs(not_until.wait_time_from(now).as_secs()))
    }

    /// Charges the authenticated principal's bucket, independent of the IP
    /// bucket so a request is never double-charged for its address.
    pub fn check_principal(&self, principal: &str) -> Result<(), u64> {
        self.maybe_maintain();
        let now = self.clock.now();
        self.per_principal
            .check_key(&principal.to_string())
            .map_err(|not_until| retry_secs(not_until.wait_time_from(now).as_secs()))
    }
}

fn retry_secs(secs: u64) -> u64 {
    secs.max(1)
}

/// REST IP limiter. Runs at the outer transport boundary, before CORS, bearer
/// parsing, or extraction, so an invalid or expensive authentication attempt
/// still consumes IP capacity.
pub async fn rate_limit_ip_middleware(
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
pub async fn rate_limit_principal_middleware(
    State(state): State<std::sync::Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let principal = request
        .extensions()
        .get::<Option<AuthContext>>()
        .cloned()
        .flatten()
        .map(|auth| auth.user_id.to_string());
    let Some(principal) = principal else {
        return next.run(request).await;
    };
    match state.rate_limits().check_principal(&principal) {
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

#[cfg(test)]
mod tests {
    use super::ApiRateLimits;
    use std::net::IpAddr;
    use std::str::FromStr;

    #[test]
    fn limits_by_ip() {
        // The burst drains, the denial names a retry delay, other IPs still pass.
        let limits = ApiRateLimits::for_test(3);
        let ip = IpAddr::from_str("203.0.113.7").unwrap();
        for _ in 0..3 {
            assert!(limits.check_ip(ip).is_ok());
        }
        let retry_after = limits.check_ip(ip).expect_err("over budget");
        assert!(retry_after >= 1);
        assert!(
            limits
                .check_ip(IpAddr::from_str("203.0.113.8").unwrap())
                .is_ok()
        );
    }

    #[test]
    fn limits_by_principal() {
        // A principal is bounded independently of any source address.
        let limits = ApiRateLimits::for_test(3);
        for _ in 0..3 {
            assert!(limits.check_principal("user-a").is_ok());
        }
        assert!(limits.check_principal("user-a").is_err());
        assert!(limits.check_principal("user-b").is_ok());
    }

    #[test]
    fn ip_and_principal_independent() {
        // Draining the IP bucket must not spend the principal's, and vice versa.
        let limits = ApiRateLimits::for_test(2);
        let ip = IpAddr::from_str("203.0.113.10").unwrap();
        assert!(limits.check_ip(ip).is_ok());
        assert!(limits.check_ip(ip).is_ok());
        assert!(limits.check_ip(ip).is_err());
        // The principal bucket is untouched by the exhausted IP bucket.
        assert!(limits.check_principal("user-a").is_ok());
        assert!(limits.check_principal("user-a").is_ok());
        assert!(limits.check_principal("user-a").is_err());
    }
}
