//! Per-IP, per-principal and per-peer request limits and local slot permits shared by every
//! frontend, with a retry time on denial.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::slots::{LocalKey, LocalPermit, SlotPermit, SlotTable};
use aruna_core::user::id::UserId;
use governor::clock::{Clock, DefaultClock};
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const IP_REQUESTS_MINUTE: u32 = 24_000;
const IP_BURST: u32 = 4_000;
const PRINCIPAL_REQUESTS_MINUTE: u32 = 12_000;
const PRINCIPAL_BURST: u32 = 2_000;
/// Every N checks the keyed stores drop entries that are fully replenished.
const MAINTENANCE_INTERVAL: u64 = 4_096;
pub(crate) const LOCAL_PERMITS: usize = 16;
/// Most keys each slot table tracks at once; a new key beyond it is refused.
pub(crate) const TABLE_LIMIT: usize = 4_096;
/// Share link opens per address and per link: each may test a password.
const SHARE_OPENS_MINUTE: u32 = 30;
const SHARE_OPENS_BURST: u32 = 10;
/// Account merges and sign-in attaches per account: each checks a token of another account.
const ACCOUNT_LINKS_MINUTE: u32 = 6;
const ACCOUNT_LINKS_BURST: u32 = 5;

type Keyed<K> = RateLimiter<K, DefaultKeyedStateStore<K>, DefaultClock>;

/// Filesystem frontend limits that hold across every connection and session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VfsLimits {
    /// Most open connections of one peer address.
    pub peer_connections: usize,
    /// Most outstanding operations of one principal over all its connections.
    pub principal_operations: usize,
    pub handshake_per_minute: u32,
    pub handshake_burst: u32,
}

impl Default for VfsLimits {
    fn default() -> Self {
        Self {
            peer_connections: 16,
            principal_operations: 1_024,
            handshake_per_minute: 120,
            handshake_burst: 30,
        }
    }
}

pub struct RateLimits {
    per_ip: Keyed<IpAddr>,
    per_principal: Keyed<String>,
    share_opens: Keyed<String>,
    account_links: Keyed<String>,
    handshakes: Keyed<IpAddr>,
    clock: DefaultClock,
    checks: AtomicU64,
    local: Arc<Mutex<SlotTable>>,
    peers: Arc<Mutex<SlotTable>>,
    work: Arc<Mutex<SlotTable>>,
}

impl std::fmt::Debug for RateLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RateLimits").finish()
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self::new(
            IP_REQUESTS_MINUTE,
            IP_BURST,
            PRINCIPAL_REQUESTS_MINUTE,
            PRINCIPAL_BURST,
        )
    }
}

fn quota(per_minute: u32, burst: u32) -> Quota {
    let rate = NonZeroU32::new(per_minute).unwrap_or(NonZeroU32::MIN);
    let burst = NonZeroU32::new(burst).unwrap_or(NonZeroU32::MIN);
    Quota::per_minute(rate).allow_burst(burst)
}

impl RateLimits {
    /// Operator-configurable quotas; each value floors at one so a limiter is
    /// always valid. Filesystem limits start at their defaults.
    pub fn new(ip_per_minute: u32, ip_burst: u32, principal_per_minute: u32, burst: u32) -> Self {
        let vfs = VfsLimits::default();
        Self {
            per_ip: RateLimiter::keyed(quota(ip_per_minute, ip_burst)),
            per_principal: RateLimiter::keyed(quota(principal_per_minute, burst)),
            share_opens: RateLimiter::keyed(quota(SHARE_OPENS_MINUTE, SHARE_OPENS_BURST)),
            account_links: RateLimiter::keyed(quota(ACCOUNT_LINKS_MINUTE, ACCOUNT_LINKS_BURST)),
            handshakes: RateLimiter::keyed(quota(vfs.handshake_per_minute, vfs.handshake_burst)),
            clock: DefaultClock::default(),
            checks: AtomicU64::new(0),
            local: SlotTable::shared(LOCAL_PERMITS, TABLE_LIMIT),
            peers: SlotTable::shared(vfs.peer_connections, TABLE_LIMIT),
            work: SlotTable::shared(vfs.principal_operations, TABLE_LIMIT),
        }
    }

    /// Replaces the filesystem handshake quota and slot capacities; each value floors at one.
    pub fn with_vfs_limits(mut self, vfs: VfsLimits) -> Self {
        self.handshakes = RateLimiter::keyed(quota(vfs.handshake_per_minute, vfs.handshake_burst));
        self.peers = SlotTable::shared(vfs.peer_connections, TABLE_LIMIT);
        self.work = SlotTable::shared(vfs.principal_operations, TABLE_LIMIT);
        self
    }

    /// Tight quotas for tests that must observe a denial quickly.
    #[cfg(test)]
    pub fn for_test(burst: u32) -> Self {
        Self::new(60, burst, 60, burst)
    }

    pub fn try_acquire_local(&self, key: LocalKey) -> Option<LocalPermit> {
        SlotTable::acquire(&self.local, key)
    }

    /// Takes one connection slot of `peer`, or `None` when the peer or the table is full.
    pub fn try_acquire_peer(&self, peer: IpAddr) -> Option<SlotPermit> {
        SlotTable::acquire(&self.peers, LocalKey::Ip(peer)).map(SlotPermit::new)
    }

    /// Takes one outstanding operation slot of `principal`, shared by all its connections
    /// and sessions, or `None` when the principal or the table is full.
    pub fn try_acquire_work(&self, principal: UserId) -> Option<SlotPermit> {
        SlotTable::acquire(&self.work, LocalKey::User(principal)).map(SlotPermit::new)
    }

    fn maybe_maintain(&self) {
        if self
            .checks
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(MAINTENANCE_INTERVAL)
        {
            self.per_ip.retain_recent();
            self.per_principal.retain_recent();
            self.share_opens.retain_recent();
            self.account_links.retain_recent();
            self.handshakes.retain_recent();
        }
    }

    fn charge<K>(&self, limiter: &Keyed<K>, key: &K) -> Result<(), u64>
    where
        K: Clone + Eq + std::hash::Hash,
    {
        let now = self.clock.now();
        limiter
            .check_key(key)
            .map_err(|not_until| not_until.wait_time_from(now).as_secs().max(1))
    }

    /// Charges the caller's IP bucket. `Err` carries the seconds after which a
    /// retry can conform (never zero).
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), u64> {
        self.maybe_maintain();
        self.charge(&self.per_ip, &ip)
    }

    /// Charges the authenticated principal's bucket, independent of the IP
    /// bucket so a request is never double-charged for its address.
    pub fn check_principal(&self, principal: UserId) -> Result<(), u64> {
        self.maybe_maintain();
        self.charge(&self.per_principal, &principal.to_string())
    }

    /// Charges one share link open to the client address and to the link, so neither many
    /// addresses nor many links speed up password guessing.
    pub fn check_share_open(&self, ip: IpAddr, share_id: ulid::Ulid) -> Result<(), u64> {
        self.maybe_maintain();
        for key in [format!("ip:{ip}"), format!("link:{share_id}")] {
            self.charge(&self.share_opens, &key)?;
        }
        Ok(())
    }

    /// Charges one account merge or sign-in attach to the calling account.
    pub fn check_account_link(&self, user_id: UserId) -> Result<(), u64> {
        self.maybe_maintain();
        self.charge(&self.account_links, &user_id.to_string())
    }

    /// Charges one SMB or NFS handshake to the peer address, before any credential check.
    pub fn check_handshake(&self, peer: IpAddr) -> Result<(), u64> {
        self.maybe_maintain();
        self.charge(&self.handshakes, &peer)
    }
}
