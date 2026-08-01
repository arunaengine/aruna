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
        Self::with_quotas(
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
    fn with_quotas(ip_per_minute: u32, ip_burst: u32, per_minute: u32, burst: u32) -> Self {
        Self {
            per_ip: RateLimiter::keyed(quota(ip_per_minute, ip_burst)),
            per_principal: RateLimiter::keyed(quota(per_minute, burst)),
            clock: DefaultClock::default(),
            checks: AtomicU64::new(0),
        }
    }

    /// Tight quotas for tests that must observe a denial quickly.
    #[cfg(test)]
    pub(crate) fn for_test(burst: u32) -> Self {
        Self::with_quotas(60, burst, 60, burst)
    }

    /// One request against the caller's buckets. `Err` carries the seconds
    /// after which a retry can conform (never zero).
    pub fn check(&self, ip: IpAddr, principal: Option<&str>) -> Result<(), u64> {
        if self.checks.fetch_add(1, Ordering::Relaxed) % MAINTENANCE_INTERVAL == 0 {
            self.per_ip.retain_recent();
            self.per_principal.retain_recent();
        }
        let now = self.clock.now();
        if let Err(not_until) = self.per_ip.check_key(&ip) {
            return Err(retry_secs(not_until.wait_time_from(now).as_secs()));
        }
        if let Some(principal) = principal
            && let Err(not_until) = self.per_principal.check_key(&principal.to_string())
        {
            return Err(retry_secs(not_until.wait_time_from(now).as_secs()));
        }
        Ok(())
    }
}

fn retry_secs(secs: u64) -> u64 {
    secs.max(1)
}

/// REST-plane limiter. Runs inside the auth middleware so an authenticated
/// caller is attributed to its principal on top of its client address.
pub async fn rate_limit_middleware(
    State(state): State<std::sync::Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let ip = client_ip(state.trusted_proxies(), peer.ip(), request.headers());
    let principal = request
        .extensions()
        .get::<Option<AuthContext>>()
        .cloned()
        .flatten()
        .map(|auth| auth.user_id.to_string());
    match state.rate_limits().check(ip, principal.as_deref()) {
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
            assert!(limits.check(ip, None).is_ok());
        }
        let retry_after = limits.check(ip, None).expect_err("over budget");
        assert!(retry_after >= 1);
        assert!(
            limits
                .check(IpAddr::from_str("203.0.113.8").unwrap(), None)
                .is_ok()
        );
    }

    #[test]
    fn limits_by_principal() {
        // A principal is bounded across source addresses.
        let limits = ApiRateLimits::for_test(3);
        for i in 0..3u8 {
            let ip = IpAddr::from_str(&format!("198.51.100.{i}")).unwrap();
            assert!(limits.check(ip, Some("user-a")).is_ok());
        }
        let ip = IpAddr::from_str("198.51.100.9").unwrap();
        assert!(limits.check(ip, Some("user-a")).is_err());
        assert!(limits.check(ip, Some("user-b")).is_ok());
    }
}
