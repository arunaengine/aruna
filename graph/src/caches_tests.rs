//! Tests that the query cache keeps results only while their stamp holds, within its bounds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const TTL: Duration = Duration::from_secs(30);

fn cache() -> QueryCache {
    QueryCache::with_limits(8, 1 << 20, TTL)
}

fn key(seed: u8) -> [u8; 32] {
    [seed; 32]
}

fn rows(count: usize) -> (MetadataQueryResults, usize) {
    let rows = (0..count)
        .map(|index| BTreeMap::from([("name".to_string(), format!("value-{index:06}"))]))
        .collect();
    (MetadataQueryResults::Solutions(rows), 1)
}

#[test]
fn returns_stored_entry() {
    let cache = cache();
    let stamp = cache.stamp(7);
    let now = Instant::now();
    assert!(cache.insert(key(1), rows(3), (stamp, 7), now));
    assert_eq!(cache.get(&key(1), stamp, now), Some(rows(3)));
    assert_eq!(cache.get(&key(2), stamp, now), None);
}

#[test]
fn stale_generation_misses() {
    let cache = cache();
    let now = Instant::now();
    cache.insert(key(1), rows(1), (cache.stamp(1), 1), now);
    assert_eq!(cache.get(&key(1), cache.stamp(2), now), None);
    // The miss also drops the entry instead of resurrecting it later.
    assert_eq!(cache.get(&key(1), cache.stamp(1), now), None);
}

#[test]
fn apply_bump_invalidates() {
    let cache = cache();
    let now = Instant::now();
    cache.insert(key(1), rows(1), (cache.stamp(1), 1), now);
    cache.bump_apply();
    assert_eq!(cache.get(&key(1), cache.stamp(1), now), None);
}

#[test]
fn insert_checks_generation() {
    // Keys are shared by digest, so a result evaluated across a visibility change would leak.
    let cache = cache();
    let now = Instant::now();
    let stamp = cache.stamp(1);
    assert!(!cache.insert(key(1), rows(1), (stamp, 2), now));
    assert_eq!(cache.get(&key(1), stamp, now), None);
    assert!(cache.insert(key(1), rows(1), (stamp, 1), now));
}

#[test]
fn byte_budget_evicts() {
    let cache = QueryCache::with_limits(64, 4096, TTL);
    let stamp = cache.stamp(1);
    let now = Instant::now();
    for seed in 0..16 {
        assert!(cache.insert(key(seed), rows(4), (stamp, 1), now));
    }
    assert!(cache.get(&key(15), stamp, now).is_some());
    assert_eq!(cache.get(&key(0), stamp, now), None);
    // A result larger than the whole budget is never admitted.
    assert!(!cache.insert(key(99), rows(4096), (stamp, 1), now));
}

#[test]
fn capacity_evicts_bytes() {
    let entry_bytes = ENTRY_OVERHEAD + results_bytes(&rows(4).0);
    let cache = QueryCache::with_limits(2, 2 * entry_bytes, TTL);
    let stamp = cache.stamp(1);
    let now = Instant::now();
    for seed in 0..3 {
        assert!(cache.insert(key(seed), rows(4), (stamp, 1), now));
    }
    // The capacity eviction releases the evicted bytes; leaked bytes would evict a live entry.
    assert_eq!(cache.get(&key(0), stamp, now), None);
    assert!(cache.get(&key(1), stamp, now).is_some());
    assert!(cache.get(&key(2), stamp, now).is_some());
    // Replacing a key subtracts the old entry's bytes exactly once.
    assert!(cache.insert(key(1), rows(4), (stamp, 1), now));
    assert!(cache.get(&key(1), stamp, now).is_some());
    assert!(cache.get(&key(2), stamp, now).is_some());
}

#[test]
fn ttl_expires_entry() {
    let cache = cache();
    let stamp = cache.stamp(1);
    let now = Instant::now();
    cache.insert(key(1), rows(1), (stamp, 1), now);
    let before = now + TTL - Duration::from_secs(1);
    assert!(cache.get(&key(1), stamp, before).is_some());
    assert_eq!(cache.get(&key(1), stamp, now + TTL), None);
}
