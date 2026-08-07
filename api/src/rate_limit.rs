//! Per-IP and per-principal token buckets shared by a request plane. Limits
//! bound abuse, not normal use: quotas are generous and identical for every
//! caller, and a denied request reports when to retry.

use crate::error::ErrorResponse;
use crate::forwarded::client_ip;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::AuthContext;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use governor::clock::{Clock, DefaultClock};
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter};
use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const IP_REQUESTS_PER_MINUTE: u32 = 6_000;
const IP_BURST: u32 = 1_000;
const PRINCIPAL_REQUESTS_PER_MINUTE: u32 = 3_000;
const PRINCIPAL_BURST: u32 = 500;
/// Every N checks the keyed stores drop entries that are fully replenished.
const MAINTENANCE_INTERVAL: u64 = 4_096;
const LOCAL_PERMITS: usize = 16;
const LOCAL_TABLE_LIMIT: usize = 4_096;
const LOCAL_LEASE_LIMIT: usize = 2;

type Keyed<K> = RateLimiter<K, DefaultKeyedStateStore<K>, DefaultClock>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum LocalKey {
    User(UserId),
    Ip(IpAddr),
}

#[derive(Default)]
struct LocalTable {
    entries: HashMap<LocalKey, Arc<Semaphore>>,
}

impl LocalTable {
    fn try_acquire(&mut self, key: LocalKey, table: &Arc<Mutex<Self>>) -> Option<LocalPermit> {
        let semaphore = match self.entries.get(&key) {
            Some(semaphore) => Arc::clone(semaphore),
            None => {
                if self.entries.len() >= LOCAL_TABLE_LIMIT {
                    return None;
                }
                let semaphore = Arc::new(Semaphore::new(LOCAL_PERMITS));
                self.entries.insert(key, Arc::clone(&semaphore));
                semaphore
            }
        };
        let permit = semaphore.clone().try_acquire_owned().ok()?;
        Some(LocalPermit {
            permit: Some(permit),
            table: Arc::downgrade(table),
            key,
            semaphore,
        })
    }
}

pub(crate) struct LocalPermit {
    permit: Option<OwnedSemaphorePermit>,
    table: Weak<Mutex<LocalTable>>,
    key: LocalKey,
    semaphore: Arc<Semaphore>,
}

impl Drop for LocalPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        drop(permit);
        let Some(table) = self.table.upgrade() else {
            return;
        };
        let mut table = table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.semaphore.available_permits() == LOCAL_PERMITS
            && table
                .entries
                .get(&self.key)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.semaphore))
        {
            table.entries.remove(&self.key);
        }
    }
}

/// A request extension that keeps local admission permits alive through the
/// response body. The server may clone this slot before handing the request to
/// s3s and move its clone into the response wrapper.
#[derive(Clone, Default)]
pub(crate) struct LocalLease(Arc<Mutex<Vec<LocalPermit>>>);

impl LocalLease {
    pub(crate) fn hold(&self, permit: LocalPermit) -> bool {
        let mut slot = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.len() >= LOCAL_LEASE_LIMIT {
            return false;
        }
        slot.push(permit);
        true
    }

    pub(crate) fn replace(&self, permit: LocalPermit) {
        let mut slot = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::mem::replace(&mut *slot, vec![permit]);
        drop(slot);
        drop(previous);
    }
}

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
    local: Arc<Mutex<LocalTable>>,
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
            local: Arc::new(Mutex::new(LocalTable::default())),
        }
    }

    pub(crate) fn try_acquire_local(&self, key: LocalKey) -> Option<LocalPermit> {
        let local = Arc::clone(&self.local);
        local
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_acquire(key, &local)
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
    pub fn check_principal(&self, principal: UserId) -> Result<(), u64> {
        self.maybe_maintain();
        let now = self.clock.now();
        let principal = principal.to_string();
        self.per_principal
            .check_key(&principal)
            .map_err(|not_until| retry_secs(not_until.wait_time_from(now).as_secs()))
    }
}

fn retry_secs(secs: u64) -> u64 {
    secs.max(1)
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
        .cloned()
        .flatten()
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
    use super::{ApiRateLimits, LOCAL_PERMITS, LOCAL_TABLE_LIMIT, LocalKey, LocalLease};
    use aruna_core::UserId;
    use aruna_core::structs::RealmId;
    use std::net::IpAddr;
    use std::str::FromStr;
    use ulid::Ulid;

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
            assert!(limits.check_principal(user(1)).is_ok());
        }
        assert!(limits.check_principal(user(1)).is_err());
        assert!(limits.check_principal(user(2)).is_ok());
    }

    #[test]
    fn buckets_stay_independent() {
        // Draining the IP bucket must not spend the principal's, and vice versa.
        let limits = ApiRateLimits::for_test(2);
        let ip = IpAddr::from_str("203.0.113.10").unwrap();
        assert!(limits.check_ip(ip).is_ok());
        assert!(limits.check_ip(ip).is_ok());
        assert!(limits.check_ip(ip).is_err());
        // The principal bucket is untouched by the exhausted IP bucket.
        assert!(limits.check_principal(user(1)).is_ok());
        assert!(limits.check_principal(user(1)).is_ok());
        assert!(limits.check_principal(user(1)).is_err());
    }

    fn user(number: u128) -> UserId {
        UserId::local(Ulid::from_u128(number), RealmId([1u8; 32]))
    }

    #[test]
    fn limits_local_user() {
        let limits = ApiRateLimits::default();
        let key = LocalKey::User(user(1));
        let mut permits = Vec::new();
        for _ in 0..LOCAL_PERMITS {
            permits.push(limits.try_acquire_local(key).expect("local permit"));
        }
        assert!(limits.try_acquire_local(key).is_none());
        drop(permits.pop());
        assert!(limits.try_acquire_local(key).is_some());
    }

    #[test]
    fn permits_distinct_users() {
        let limits = ApiRateLimits::default();
        assert!(limits.try_acquire_local(LocalKey::User(user(1))).is_some());
        assert!(limits.try_acquire_local(LocalKey::User(user(2))).is_some());
    }

    #[test]
    fn reclaims_local() {
        let limits = ApiRateLimits::default();
        let mut permits = Vec::with_capacity(LOCAL_TABLE_LIMIT);
        for number in 0..LOCAL_TABLE_LIMIT as u128 {
            permits.push(
                limits
                    .try_acquire_local(LocalKey::User(user(number)))
                    .expect("table entry permit"),
            );
        }
        assert!(
            limits
                .try_acquire_local(LocalKey::User(user(LOCAL_TABLE_LIMIT as u128)))
                .is_none()
        );
        drop(permits);
        assert!(
            limits
                .try_acquire_local(LocalKey::User(user(LOCAL_TABLE_LIMIT as u128)))
                .is_some()
        );
    }

    #[test]
    fn clone_keeps_lease() {
        let limits = ApiRateLimits::default();
        let key = LocalKey::User(user(3));
        let mut permits = Vec::new();
        for _ in 0..LOCAL_PERMITS - 1 {
            permits.push(limits.try_acquire_local(key).expect("local permit"));
        }
        let lease = LocalLease::default();
        assert!(lease.hold(limits.try_acquire_local(key).expect("local permit")));
        let clone = lease.clone();
        drop(lease);
        assert!(limits.try_acquire_local(key).is_none());
        drop(clone);
        assert!(limits.try_acquire_local(key).is_some());
        drop(permits);
    }

    #[test]
    fn replaces_lease() {
        let limits = ApiRateLimits::default();
        let ip = LocalKey::Ip(IpAddr::from_str("203.0.113.11").unwrap());
        let user_key = LocalKey::User(user(4));
        let lease = LocalLease::default();
        assert!(lease.hold(limits.try_acquire_local(ip).expect("ip permit")));
        lease.replace(limits.try_acquire_local(user_key).expect("user permit"));
        assert!(limits.try_acquire_local(ip).is_some());

        let mut permits = Vec::new();
        for _ in 0..LOCAL_PERMITS - 1 {
            permits.push(limits.try_acquire_local(user_key).expect("user permit"));
        }
        assert!(limits.try_acquire_local(user_key).is_none());
        drop(lease);
        assert!(limits.try_acquire_local(user_key).is_some());
        drop(permits);
    }
}
