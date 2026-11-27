//! Unit tests for the shared rate limits, local slots, leases and filesystem limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::limits::{LOCAL_PERMITS, TABLE_LIMIT};
use crate::{LocalKey, LocalLease, RateLimits, VfsLimits};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::user::id::UserId;
use std::net::IpAddr;
use std::str::FromStr;
use ulid::Ulid;

fn user(number: u128) -> UserId {
    UserId::local(Ulid::from(number), RealmId([1u8; 32]))
}

fn address(text: &str) -> IpAddr {
    IpAddr::from_str(text).expect("valid test address")
}

fn small_vfs(connections: usize, operations: usize, burst: u32) -> RateLimits {
    RateLimits::for_test(1).with_vfs_limits(VfsLimits {
        peer_connections: connections,
        principal_operations: operations,
        handshake_per_minute: 60,
        handshake_burst: burst,
    })
}

#[test]
fn limits_by_ip() {
    // The burst drains, the denial names a retry delay, other IPs still pass.
    let limits = RateLimits::for_test(3);
    let ip = address("203.0.113.7");
    for _ in 0..3 {
        assert!(limits.check_ip(ip).is_ok());
    }
    let retry_after = limits.check_ip(ip).expect_err("over budget");
    assert!(retry_after >= 1);
    assert!(limits.check_ip(address("203.0.113.8")).is_ok());
}

#[test]
fn limits_by_principal() {
    let limits = RateLimits::for_test(3);
    for _ in 0..3 {
        assert!(limits.check_principal(user(1)).is_ok());
    }
    assert!(limits.check_principal(user(1)).is_err());
    assert!(limits.check_principal(user(2)).is_ok());
}

#[test]
fn buckets_stay_independent() {
    // Draining the IP bucket must not spend the principal's, and vice versa.
    let limits = RateLimits::for_test(2);
    let ip = address("203.0.113.10");
    assert!(limits.check_ip(ip).is_ok());
    assert!(limits.check_ip(ip).is_ok());
    assert!(limits.check_ip(ip).is_err());
    assert!(limits.check_principal(user(1)).is_ok());
    assert!(limits.check_principal(user(1)).is_ok());
    assert!(limits.check_principal(user(1)).is_err());
}

#[test]
fn limits_share_opens() {
    // The address budget holds across links, and the link budget across addresses.
    let limits = RateLimits::default();
    let ip = address("203.0.113.20");
    let mut denied = false;
    for number in 0..64u128 {
        if limits.check_share_open(ip, Ulid::from(number)).is_err() {
            denied = true;
            break;
        }
    }
    assert!(denied, "one address cannot open links without bound");

    let link = Ulid::from(1_000u128);
    let mut denied = false;
    for number in 0..64u8 {
        let other = IpAddr::from([198, 51, 100, number]);
        if limits.check_share_open(other, link).is_err() {
            denied = true;
            break;
        }
    }
    assert!(denied, "one link cannot be opened without bound");
}

#[test]
fn limits_account_links() {
    let limits = RateLimits::default();
    let mut denied = false;
    for _ in 0..64 {
        if limits.check_account_link(user(5)).is_err() {
            denied = true;
            break;
        }
    }
    assert!(denied);
    assert!(limits.check_account_link(user(6)).is_ok());
}

#[test]
fn limits_local_user() {
    let limits = RateLimits::default();
    let key = LocalKey::User(user(1));
    let mut permits = Vec::new();
    for _ in 0..LOCAL_PERMITS {
        permits.push(limits.try_acquire_local(key).expect("local permit"));
    }
    assert!(limits.try_acquire_local(key).is_none());
    drop(permits.pop());
    assert!(limits.try_acquire_local(key).is_some());
    assert!(limits.try_acquire_local(LocalKey::User(user(2))).is_some());
}

#[test]
fn reclaims_local() {
    let limits = RateLimits::default();
    let mut permits = Vec::with_capacity(TABLE_LIMIT);
    for number in 0..TABLE_LIMIT as u128 {
        permits.push(
            limits
                .try_acquire_local(LocalKey::User(user(number)))
                .expect("table entry permit"),
        );
    }
    let extra = LocalKey::User(user(TABLE_LIMIT as u128));
    assert!(limits.try_acquire_local(extra).is_none());
    drop(permits);
    assert!(limits.try_acquire_local(extra).is_some());
}

#[test]
fn clone_keeps_lease() {
    let limits = RateLimits::default();
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
    let limits = RateLimits::default();
    let ip = LocalKey::Ip(address("203.0.113.11"));
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

#[test]
fn limits_peer_handshakes() {
    let limits = small_vfs(4, 4, 2);
    let peer = address("192.0.2.1");
    assert!(limits.check_handshake(peer).is_ok());
    assert!(limits.check_handshake(peer).is_ok());
    let retry_after = limits.check_handshake(peer).expect_err("over budget");
    assert!(retry_after >= 1);
    assert!(limits.check_handshake(address("192.0.2.2")).is_ok());
    // Handshakes spend their own bucket, not the request bucket of the address.
    assert!(limits.check_ip(peer).is_ok());
}

#[test]
fn caps_peer_connections() {
    let limits = small_vfs(2, 4, 2);
    let peer = address("192.0.2.3");
    let first = limits.try_acquire_peer(peer).expect("first connection");
    let second = limits.try_acquire_peer(peer).expect("second connection");
    assert!(limits.try_acquire_peer(peer).is_none());
    assert!(limits.try_acquire_peer(address("192.0.2.4")).is_some());
    drop(first);
    let third = limits.try_acquire_peer(peer).expect("freed connection");
    drop((second, third));
    // Both slots are free again once every connection closes.
    let reopened = [limits.try_acquire_peer(peer), limits.try_acquire_peer(peer)];
    assert!(reopened.iter().all(Option::is_some));
}

#[test]
fn shares_principal_work() {
    // Two sessions of one user draw from one budget; a new session adds no allowance.
    let limits = small_vfs(8, 3, 2);
    let owner = user(7);
    let mut first_session = Vec::new();
    let mut second_session = Vec::new();
    first_session.push(limits.try_acquire_work(owner).expect("first operation"));
    second_session.push(limits.try_acquire_work(owner).expect("second operation"));
    first_session.push(limits.try_acquire_work(owner).expect("third operation"));
    assert!(limits.try_acquire_work(owner).is_none());
    assert!(limits.try_acquire_work(user(8)).is_some());
    drop(second_session);
    assert!(limits.try_acquire_work(owner).is_some());
    drop(first_session);
}

#[test]
fn floors_vfs_limits() {
    // Zero values floor at one slot and one handshake instead of refusing everything.
    let limits = small_vfs(0, 0, 0);
    let peer = address("192.0.2.5");
    let connection = limits.try_acquire_peer(peer).expect("one connection");
    assert!(limits.try_acquire_peer(peer).is_none());
    let operation = limits.try_acquire_work(user(9)).expect("one operation");
    assert!(limits.try_acquire_work(user(9)).is_none());
    assert!(limits.check_handshake(peer).is_ok());
    assert!(limits.check_handshake(peer).is_err());
    drop((connection, operation));
}
