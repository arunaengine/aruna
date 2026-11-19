//! Tests the query cache bounds and stamps, and the registry and deletion-state cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;

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

fn record(document_path: &str) -> MetadataRegistryRecord {
    let document_id = Ulid::generate();
    MetadataRegistryRecord {
        realm_id: RealmId([7u8; 32]),
        group_id: Ulid::generate(),
        document_id,
        document_path: document_path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: format!("/metadata/{document_path}"),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        establishing_event_id: Ulid::nil(),
        last_event_id: Ulid::nil(),
    }
}

fn filled(records: Vec<MetadataRegistryRecord>) -> VisibilityCache {
    let cache = VisibilityCache::new();
    assert!(cache.store_fill(records, Vec::new(), cache.current_generation()));
    cache
}

// The registry snapshot while it is fresh.
fn fresh(cache: &VisibilityCache) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
    let (records, fresh) = cache.registry_records_any()?;
    fresh.then_some(records)
}

// The deletion state of a graph while it is fresh.
fn deleted(cache: &VisibilityCache, graph_iri: &str) -> Option<bool> {
    let (deleted, fresh) = cache.lifecycle_deleted_any(graph_iri)?;
    fresh.then_some(deleted)
}

#[test]
fn upsert_replaces_existing() {
    let mut existing = record("datasets/a");
    let cache = filled(vec![existing.clone()]);
    existing.public = false;
    existing.updated_at_ms = 42;
    let added = record("datasets/b");
    cache.upsert_at(&[existing.clone(), added.clone()], None);

    let records = fresh(&cache).expect("cache entry");
    assert_eq!(records.len(), 2);
    let updated = (records.iter())
        .find(|record| record.document_id == existing.document_id)
        .expect("updated record");
    assert!(!updated.public);
    assert_eq!(updated.updated_at_ms, 42);
    assert!(
        records
            .iter()
            .any(|record| record.document_id == added.document_id)
    );
}

#[test]
fn upsert_discards_cache() {
    let records = (0..REGISTRY_CANDIDATE_LIMIT)
        .map(|index| record(&format!("datasets/{index}")))
        .collect();
    let cache = filled(records);
    cache.upsert_at(&[record("datasets/overflow")], None);
    assert!(fresh(&cache).is_none());
}

#[test]
fn upsert_filled_cache() {
    let cache = VisibilityCache::new();
    cache.upsert_at(&[record("datasets/a")], None);
    assert!(fresh(&cache).is_none());
}

#[test]
fn stale_cache_callback() {
    let raced = record("datasets/race");
    let cache = filled(vec![raced.clone()]);
    let generation = cache.current_generation();
    cache.remove_registry_record(raced.document_id);
    cache.upsert_at(std::slice::from_ref(&raced), Some(generation));
    assert!(fresh(&cache).is_some_and(|records| records.is_empty()));
}

#[test]
fn remove_drop_records() {
    let by_document = record("datasets/a");
    let by_graph = record("datasets/b");
    let kept = record("datasets/c");
    let cache = filled(vec![by_document.clone(), by_graph.clone(), kept.clone()]);
    cache.remove_registry_record(by_document.document_id);
    cache.remove_graph(&by_graph.graph_iri);

    let records = fresh(&cache).expect("cache entry");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, kept.document_id);
}

#[test]
fn group_snapshots_scoped() {
    let (group_a, group_b) = (Ulid::generate(), Ulid::generate());
    let mut record_a = record("datasets/a");
    record_a.group_id = group_a;
    let mut record_b = record("datasets/b");
    record_b.group_id = group_b;
    let cache = filled(vec![record_a.clone(), record_b.clone()]);

    let (listed_a, fresh) = cache.records_group_any(group_a).expect("group A snapshot");
    assert!(fresh);
    assert_eq!(listed_a.as_ref(), &vec![record_a.clone()]);

    let mut added_b = record("datasets/b2");
    added_b.group_id = group_b;
    cache.upsert_at(std::slice::from_ref(&added_b), None);
    let (listed_again, _) = cache.records_group_any(group_a).expect("group A snapshot");
    assert!(Arc::ptr_eq(&listed_a, &listed_again));

    let mut added_a = record("datasets/a2");
    added_a.group_id = group_a;
    cache.upsert_at(std::slice::from_ref(&added_a), None);
    let (listed_after, _) = cache.records_group_any(group_a).expect("group A snapshot");
    assert_eq!(listed_after.len(), 2);
    assert!(!Arc::ptr_eq(&listed_a, &listed_after));
}

#[test]
fn upsert_not_extend() {
    let cache = filled(vec![record("datasets/a")]);
    cache.expire_now();
    cache.upsert_at(&[record("datasets/b")], None);
    assert!(fresh(&cache).is_none());
}

#[test]
fn lifecycle_entry_removal() {
    let cache = VisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:a".to_string(), false);
    assert_eq!(deleted(&cache, "urn:graph:a"), Some(false));
    cache.remove_graph("urn:graph:a");
    assert_eq!(deleted(&cache, "urn:graph:a"), None);
}

#[test]
fn store_prunes_expired() {
    let cache = VisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:old".to_string(), false);
    cache.expire_now();
    cache.store_lifecycle_deleted("urn:graph:new".to_string(), false);
    assert_eq!(cache.lifecycle_deleted_any("urn:graph:old"), None);
    assert_eq!(deleted(&cache, "urn:graph:new"), Some(false));
}

#[test]
fn refresh_keeps_tombstone() {
    let cache = VisibilityCache::new();
    let old = (0..REGISTRY_CANDIDATE_LIMIT)
        .map(|index| (format!("urn:graph:old:{index}"), false))
        .collect();
    assert!(cache.refresh_if_current(old, cache.current_generation()));
    let tombstone = vec![("urn:graph:deleted".to_string(), true)];
    assert!(cache.refresh_if_current(tombstone, cache.current_generation()));
    assert_eq!(deleted(&cache, "urn:graph:deleted"), Some(true));
    assert_eq!(
        locked(&cache.lifecycle_deleted).len(),
        REGISTRY_CANDIDATE_LIMIT
    );
}

#[test]
fn eviction_keeps_tombstone() {
    let gone = record("datasets/deleted");
    let cache = VisibilityCache::new();
    cache.store_lifecycle_deleted(gone.graph_iri.clone(), true);
    let current = (0..REGISTRY_CANDIDATE_LIMIT)
        .map(|index| (format!("urn:graph:current:{index}"), false))
        .collect();
    assert!(cache.refresh_if_current(current, cache.current_generation()));
    assert!(cache.lifecycle_deleted_any(&gone.graph_iri).is_none());
}

#[test]
fn expired_entry_served() {
    let kept = record("datasets/a");
    let cache = filled(vec![kept.clone()]);
    cache.expire_now();
    assert!(fresh(&cache).is_none());
    let (records, is_fresh) = cache.registry_records_any().expect("stale entry kept");
    assert!(!is_fresh);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, kept.document_id);
    assert!(cache.store_fill(vec![kept], Vec::new(), cache.current_generation()));
    assert!(fresh(&cache).is_some());
}

#[test]
fn fill_preserves_upsert() {
    let mut stale = record("datasets/a");
    let cache = filled(vec![stale.clone()]);
    let fill_generation = cache.current_generation();
    let mut updated = stale.clone();
    updated.public = false;
    updated.updated_at_ms = 42;
    cache.upsert_at(std::slice::from_ref(&updated), None);

    stale.updated_at_ms = 1;
    assert!(!cache.store_fill(vec![stale], Vec::new(), fill_generation));
    let records = fresh(&cache).expect("cache entry");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, updated.document_id);
    assert!(!records[0].public);
    assert_eq!(records[0].updated_at_ms, 42);
}

#[test]
fn fill_preserves_removal() {
    let removed = record("datasets/removed");
    let kept = record("datasets/kept");
    let cache = filled(vec![removed.clone(), kept.clone()]);
    let fill_generation = cache.current_generation();
    cache.remove_registry_record(removed.document_id);

    let refill = vec![removed.clone(), kept.clone()];
    assert!(!cache.store_fill(refill, Vec::new(), fill_generation));
    let records = fresh(&cache).expect("cache entry");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, kept.document_id);
}

#[test]
fn fill_preserves_tombstone() {
    let gone = record("datasets/deleted");
    let cache = filled(vec![gone.clone()]);
    let fill_generation = cache.current_generation();
    cache.store_lifecycle_deleted(gone.graph_iri.clone(), true);

    let states = vec![(gone.graph_iri.clone(), false)];
    assert!(!cache.store_fill(vec![gone.clone()], states, fill_generation));
    assert_eq!(deleted(&cache, &gone.graph_iri), Some(true));
}

#[test]
fn rejected_cold_group() {
    let group_a = Ulid::generate();
    let mut record_a = record("datasets/a");
    record_a.group_id = group_a;
    let cache = VisibilityCache::new();
    let fill_generation = cache.current_generation();
    cache.advance_generation();
    let records = vec![record_a, record("datasets/b")];
    assert!(!cache.store_fill(records, Vec::new(), fill_generation));
    assert!(cache.records_group_any(group_a).is_none());
}

#[test]
fn expired_lifecycle_entry() {
    let cache = VisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:a".to_string(), true);
    cache.expire_now();
    assert_eq!(deleted(&cache, "urn:graph:a"), None);
    assert_eq!(
        cache.lifecycle_deleted_any("urn:graph:a"),
        Some((true, false))
    );
}
