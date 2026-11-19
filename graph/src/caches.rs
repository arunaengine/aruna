//! Keeps this node's registry snapshot, graph deletion states and query results in memory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aruna_core::metadata::{MetadataQueryResults, REGISTRY_CANDIDATE_LIMIT};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use lru::LruCache;
use tracing::warn;
use ulid::Ulid;

/// How long a registry snapshot or deletion state counts as fresh.
const VISIBILITY_CACHE_TTL: Duration = Duration::from_secs(30);
/// Backstop staleness of a cached result; matches the visibility TTL so a query never promises
/// fresher data than the listing path does.
const QUERY_CACHE_TTL: Duration = Duration::from_secs(30);
const CACHE_MAX_ENTRIES: usize = 512;
const CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
const ENTRY_OVERHEAD: usize = 64;

/// The registry rows and graph deletion states the read paths decide visibility from, with a
/// generation that every change advances so a fill started before it is refused.
pub(crate) struct VisibilityCache {
    registry: Mutex<Option<RegistryEntry>>,
    lifecycle_deleted: Mutex<HashMap<String, DeletedEntry>>,
    generation: AtomicU64,
}

struct RegistryEntry {
    records: BTreeMap<Ulid, MetadataRegistryRecord>,
    snapshot: Option<Arc<Vec<MetadataRegistryRecord>>>,
    group_snapshots: HashMap<GroupId, Arc<Vec<MetadataRegistryRecord>>>,
    expires_at: Instant,
}

impl RegistryEntry {
    fn snapshot(&mut self) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        if self.records.len() > REGISTRY_CANDIDATE_LIMIT {
            return None;
        }
        let records = &self.records;
        let snapshot =
            (self.snapshot).get_or_insert_with(|| Arc::new(records.values().cloned().collect()));
        Some(snapshot.clone())
    }

    fn group_snapshot(&mut self, group_id: GroupId) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        if let Some(records) = self.group_snapshots.get(&group_id) {
            return (records.len() <= REGISTRY_CANDIDATE_LIMIT).then(|| records.clone());
        }
        let mut group_records = Vec::new();
        for record in (self.records.values()).filter(|record| record.group_id == group_id) {
            if group_records.len() == REGISTRY_CANDIDATE_LIMIT {
                return None;
            }
            group_records.push(record.clone());
        }
        let records = Arc::new(group_records);
        self.group_snapshots.insert(group_id, records.clone());
        Some(records)
    }
}

struct DeletedEntry {
    deleted: bool,
    expires_at: Instant,
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|lock| lock.into_inner())
}

impl VisibilityCache {
    pub(crate) fn new() -> Self {
        Self {
            registry: Mutex::new(None),
            lifecycle_deleted: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    pub(crate) fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn advance_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// The registry snapshot, expired ones included, and whether it is still fresh.
    pub(crate) fn registry_records_any(&self) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = locked(&self.registry);
        registry.as_mut().and_then(|entry| {
            let fresh = entry.expires_at > now;
            entry.snapshot().map(|records| (records, fresh))
        })
    }

    pub(crate) fn records_group_any(
        &self,
        group_id: GroupId,
    ) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = locked(&self.registry);
        registry.as_mut().and_then(|entry| {
            let fresh = entry.expires_at > now;
            entry
                .group_snapshot(group_id)
                .map(|records| (records, fresh))
        })
    }

    /// Stores a full fill unless anything changed since `fill_generation`.
    pub(crate) fn store_fill(
        &self,
        records: Vec<MetadataRegistryRecord>,
        lifecycle_entries: Vec<(String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if records.len() > REGISTRY_CANDIDATE_LIMIT
            || lifecycle_entries.len() > REGISTRY_CANDIDATE_LIMIT
            || self.current_generation() != fill_generation
        {
            return false;
        }
        let map: BTreeMap<_, _> = (records.iter())
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let now = Instant::now();
        let expires_at = now + VISIBILITY_CACHE_TTL;
        let mut registry = locked(&self.registry);
        let mut lifecycle = locked(&self.lifecycle_deleted);
        if self.current_generation() != fill_generation {
            return false;
        }
        Self::store_states(&mut lifecycle, lifecycle_entries, now);
        *registry = Some(RegistryEntry {
            records: map,
            snapshot: Some(Arc::new(records)),
            group_snapshots: HashMap::new(),
            expires_at,
        });
        true
    }

    /// The cached deletion state of one graph, expired ones included, and its freshness.
    pub(crate) fn lifecycle_deleted_any(&self, graph_iri: &str) -> Option<(bool, bool)> {
        let now = Instant::now();
        let lifecycle = locked(&self.lifecycle_deleted);
        (lifecycle.get(graph_iri)).map(|entry| (entry.deleted, entry.expires_at > now))
    }

    pub(crate) fn store_lifecycle_deleted(&self, graph_iri: String, deleted: bool) {
        let mut lifecycle = locked(&self.lifecycle_deleted);
        self.advance_generation();
        let now = Instant::now();
        let protected = HashSet::from([graph_iri.clone()]);
        Self::trim(&mut lifecycle, &protected, now);
        let expires_at = now + VISIBILITY_CACHE_TTL;
        lifecycle.insert(
            graph_iri,
            DeletedEntry {
                deleted,
                expires_at,
            },
        );
    }

    /// Re-stamps the deletion states a read resolved, unless anything changed since
    /// `fill_generation`.
    pub(crate) fn refresh_if_current(
        &self,
        entries: Vec<(String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if self.current_generation() != fill_generation || entries.len() > REGISTRY_CANDIDATE_LIMIT
        {
            return false;
        }
        let mut lifecycle = locked(&self.lifecycle_deleted);
        if self.current_generation() != fill_generation {
            return false;
        }
        Self::store_states(&mut lifecycle, entries, Instant::now());
        true
    }

    fn store_states(
        lifecycle: &mut HashMap<String, DeletedEntry>,
        entries: Vec<(String, bool)>,
        now: Instant,
    ) {
        let protected = (entries.iter())
            .map(|(graph_iri, _)| graph_iri.clone())
            .collect::<HashSet<_>>();
        Self::trim(lifecycle, &protected, now);
        let expires_at = now + VISIBILITY_CACHE_TTL;
        for (graph_iri, deleted) in entries {
            lifecycle.insert(
                graph_iri,
                DeletedEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
    }

    fn trim(
        lifecycle: &mut HashMap<String, DeletedEntry>,
        protected: &HashSet<String>,
        now: Instant,
    ) {
        lifecycle.retain(|_, entry| entry.expires_at > now);
        let protected_count = (lifecycle.keys())
            .filter(|graph_iri| protected.contains(*graph_iri))
            .count();
        let available = REGISTRY_CANDIDATE_LIMIT.saturating_sub(protected.len());
        let remove_count = (lifecycle.len())
            .saturating_sub(protected_count)
            .saturating_sub(available);
        let evicted = (lifecycle.keys())
            .filter(|graph_iri| !protected.contains(*graph_iri))
            .take(remove_count)
            .cloned()
            .collect::<Vec<_>>();
        for graph_iri in evicted {
            lifecycle.remove(&graph_iri);
        }
    }

    /// Keeps the cached registry usable under writes; with a generation, only when nothing
    /// changed since.
    pub(crate) fn upsert_at(
        &self,
        updates: &[MetadataRegistryRecord],
        expected_generation: Option<u64>,
    ) {
        if updates.is_empty() {
            return;
        }
        let mut registry = locked(&self.registry);
        if expected_generation.is_some_and(|generation| self.current_generation() != generation) {
            return;
        }
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        let new_records = (updates.iter())
            .filter(|update| !entry.records.contains_key(&update.document_id))
            .count();
        if entry.records.len().saturating_add(new_records) > REGISTRY_CANDIDATE_LIMIT {
            *registry = None;
            return;
        }
        let mut touched_groups = HashSet::new();
        for update in updates {
            entry.records.insert(update.document_id, update.clone());
            touched_groups.insert(update.group_id);
        }
        for group_id in touched_groups {
            entry.group_snapshots.remove(&group_id);
        }
        entry.snapshot = None;
    }

    pub(crate) fn remove_registry_record(&self, document_id: Ulid) {
        let mut registry = locked(&self.registry);
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        if let Some(removed) = entry.records.remove(&document_id) {
            entry.group_snapshots.remove(&removed.group_id);
            entry.snapshot = None;
        }
    }

    /// Forgets a deleted graph's rows and deletion state.
    pub(crate) fn remove_graph(&self, graph_iri: &str) {
        {
            let mut registry = locked(&self.registry);
            self.advance_generation();
            if let Some(entry) = registry.as_mut() {
                let removed_groups = (entry.records.values())
                    .filter(|record| record.graph_iri == graph_iri)
                    .map(|record| record.group_id)
                    .collect::<HashSet<_>>();
                if !removed_groups.is_empty() {
                    (entry.records).retain(|_, record| record.graph_iri != graph_iri);
                    for group_id in removed_groups {
                        entry.group_snapshots.remove(&group_id);
                    }
                    entry.snapshot = None;
                }
            }
        }
        let mut lifecycle = locked(&self.lifecycle_deleted);
        self.advance_generation();
        lifecycle.remove(graph_iri);
    }

    /// Marks every entry expired, so the next read takes the stale path.
    pub(crate) fn expire_now(&self) {
        let expired = Instant::now() - Duration::from_secs(1);
        if let Some(entry) = locked(&self.registry).as_mut() {
            entry.expires_at = expired;
        }
        for entry in locked(&self.lifecycle_deleted).values_mut() {
            entry.expires_at = expired;
        }
    }
}

struct QueryEntry {
    results: Arc<MetadataQueryResults>,
    nodes_queried: usize,
    stamp: [u64; 2],
    expires_at: Instant,
    bytes: usize,
}

struct QueryState {
    entries: LruCache<[u8; 32], QueryEntry>,
    bytes: usize,
}

/// Bounded LRU of SPARQL results shared by every caller whose authorization resolves to the
/// same scope. An entry holds only while its stamp, the visibility generation and the count of
/// applied graph writes, is the current one.
pub(crate) struct QueryCache {
    state: Mutex<QueryState>,
    applies: AtomicU64,
    ttl: Duration,
    max_bytes: usize,
}

impl QueryCache {
    pub(crate) fn new() -> Self {
        Self::with_limits(CACHE_MAX_ENTRIES, CACHE_MAX_BYTES, QUERY_CACHE_TTL)
    }

    fn with_limits(max_entries: usize, max_bytes: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::MIN);
        Self {
            state: Mutex::new(QueryState {
                entries: LruCache::new(capacity),
                bytes: 0,
            }),
            applies: AtomicU64::new(0),
            ttl,
            max_bytes,
        }
    }

    /// Marks every cached result stale after a graph-mutating effect applied.
    pub(crate) fn bump_apply(&self) {
        self.applies.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn stamp(&self, generation: u64) -> [u64; 2] {
        [generation, self.applies.load(Ordering::Acquire)]
    }

    pub(crate) fn get(
        &self,
        key: &[u8; 32],
        stamp: [u64; 2],
        now: Instant,
    ) -> Option<(MetadataQueryResults, usize)> {
        let mut state = locked(&self.state);
        let entry = state.entries.get(key)?;
        if entry.stamp != stamp || entry.expires_at <= now {
            let bytes = entry.bytes;
            state.entries.pop(key);
            state.bytes = state.bytes.saturating_sub(bytes);
            return None;
        }
        Some(((*entry.results).clone(), entry.nodes_queried))
    }

    /// Keeps a result unless a change landed since `stamp`; shared entries make that check
    /// mandatory.
    pub(crate) fn insert(
        &self,
        key: [u8; 32],
        (results, nodes_queried): (MetadataQueryResults, usize),
        (stamp, generation): ([u64; 2], u64),
        now: Instant,
    ) -> bool {
        if stamp != self.stamp(generation) {
            return false;
        }
        let bytes = ENTRY_OVERHEAD.saturating_add(results_bytes(&results));
        if bytes > self.max_bytes {
            return false;
        }
        let entry = QueryEntry {
            results: Arc::new(results),
            nodes_queried,
            stamp,
            expires_at: now + self.ttl,
            bytes,
        };
        let mut state = locked(&self.state);
        if let Some((_, replaced)) = state.entries.push(key, entry) {
            state.bytes = state.bytes.saturating_sub(replaced.bytes);
        }
        state.bytes = state.bytes.saturating_add(bytes);
        while state.bytes > self.max_bytes {
            let Some((_, evicted)) = state.entries.pop_lru() else {
                break;
            };
            state.bytes = state.bytes.saturating_sub(evicted.bytes);
        }
        true
    }
}

fn results_bytes(results: &MetadataQueryResults) -> usize {
    const CELL_OVERHEAD: usize = 48;
    match results {
        MetadataQueryResults::Boolean(_) => 0,
        MetadataQueryResults::Solutions(rows) => (rows.iter())
            .map(|row| {
                (row.iter())
                    .map(|(name, value)| name.len() + value.len() + CELL_OVERHEAD)
                    .sum::<usize>()
                    + CELL_OVERHEAD
            })
            .sum(),
        MetadataQueryResults::Graph(triples) => (triples.iter())
            .map(|(subject, predicate, object)| {
                subject.len() + predicate.len() + object.len() + CELL_OVERHEAD
            })
            .sum(),
    }
}

impl crate::GraphHandle {
    /// Answers a cache effect, or hands any other effect back.
    pub(crate) fn run_cache(
        &self,
        effect: aruna_core::metadata::MetadataEffect,
    ) -> Result<aruna_core::metadata::MetadataEvent, aruna_core::metadata::MetadataEffect> {
        use aruna_core::metadata::{MetadataEffect, MetadataEvent};
        let visibility = &self.inner.visibility;
        let queries = &self.inner.queries;
        let generation = || visibility.current_generation();
        Ok(match effect {
            MetadataEffect::QueryCacheGet { key } => {
                let stamp = queries.stamp(generation());
                let entry = queries.get(&key, stamp, Instant::now());
                MetadataEvent::QueryCacheEntry { stamp, entry }
            }
            MetadataEffect::QueryCachePut {
                key,
                stamp,
                results,
                nodes_queried,
            } => {
                let entry = (results, nodes_queried);
                let accepted = queries.insert(key, entry, (stamp, generation()), Instant::now());
                MetadataEvent::QueryCacheStored { accepted }
            }
            MetadataEffect::QueryCacheStamp => MetadataEvent::QueryCacheEntry {
                stamp: queries.stamp(generation()),
                entry: None,
            },
            MetadataEffect::CacheGeneration => MetadataEvent::CacheGeneration {
                generation: generation(),
            },
            MetadataEffect::CacheRecords {
                records,
                generation: expected,
            } => {
                visibility.upsert_at(&records, expected);
                MetadataEvent::CacheGeneration {
                    generation: generation(),
                }
            }
            MetadataEffect::UncacheRecord { document_id } => {
                visibility.remove_registry_record(document_id);
                MetadataEvent::CacheGeneration {
                    generation: generation(),
                }
            }
            MetadataEffect::CachedGroup { group_id } => {
                let records = match visibility.records_group_any(group_id) {
                    Some((records, true)) => Some(records.as_ref().clone()),
                    _ => None,
                };
                MetadataEvent::CachedGroup { records }
            }
            MetadataEffect::RegistrySnapshot => {
                let generation = generation();
                let (records, fresh) = match visibility.registry_records_any() {
                    Some((records, fresh)) => (Some(records), fresh),
                    None => (None, false),
                };
                MetadataEvent::RegistrySnapshot {
                    records,
                    fresh,
                    generation,
                }
            }
            MetadataEffect::StoreRegistry {
                records,
                lifecycle,
                generation,
            } => MetadataEvent::RegistryStored {
                accepted: visibility.store_fill(records, lifecycle, generation),
            },
            MetadataEffect::RefreshLifecycle {
                lifecycle,
                generation,
            } => MetadataEvent::RegistryStored {
                accepted: visibility.refresh_if_current(lifecycle, generation),
            },
            MetadataEffect::DeletionMark { graph_iri } => MetadataEvent::DeletionMark {
                deleted: matches!(
                    visibility.lifecycle_deleted_any(&graph_iri),
                    Some((true, _))
                ),
            },
            MetadataEffect::MarkDeletion { graph_iri, deleted } => {
                visibility.store_lifecycle_deleted(graph_iri, deleted);
                MetadataEvent::DeletionMarked
            }
            MetadataEffect::ForgetGraph { graph_iri } => {
                visibility.remove_graph(&graph_iri);
                visibility.store_lifecycle_deleted(graph_iri, true);
                MetadataEvent::DeletionMarked
            }
            other => return Err(other),
        })
    }

    /// Marks every registry and deletion entry expired, so the next read takes the stale path.
    pub fn expire_caches(&self) {
        self.inner.visibility.expire_now();
    }

    /// Warns about registry records and stored graphs that disagree, before a registry fill is
    /// kept: graphs without a record, records without a graph, and stale graph policies.
    pub async fn warn_unprojected(&self, records: &[MetadataRegistryRecord]) {
        let projected = records
            .iter()
            .map(|record| record.graph_iri.clone())
            .collect::<BTreeSet<_>>();
        let policies = match self.graph_policies().await {
            Ok(policies) => policies,
            Err(error) => {
                warn!(
                    event = "metadata.authorization.graph_scan_failed",
                    error = %error,
                    "Could not audit metadata graphs against the authorization projection"
                );
                return;
            }
        };
        let graphs = policies.keys().cloned().collect::<BTreeSet<_>>();
        let missing = graphs.difference(&projected).cloned().collect::<Vec<_>>();
        if !missing.is_empty() {
            warn!(
                event = "metadata.authorization.projection_missing",
                count = missing.len() as u64,
                sample_graph_iris = ?missing.iter().take(8).collect::<Vec<_>>(),
                "excluding metadata graphs missing from the authorization projection"
            );
        }
        let unmaterialized = projected.difference(&graphs).cloned().collect::<Vec<_>>();
        if !unmaterialized.is_empty() {
            warn!(
                event = "metadata.authorization.graph_missing",
                count = unmaterialized.len() as u64,
                sample_graph_iris = ?unmaterialized.iter().take(8).collect::<Vec<_>>(),
                "metadata authorization projection references graphs missing from Craqle"
            );
        }
        let stale = records
            .iter()
            .filter(|record| {
                policies.get(&record.graph_iri).is_some_and(|policy| {
                    policy.public != record.public
                        || policy.permission_paths != [record.permission_path.clone()]
                })
            })
            .map(|record| record.graph_iri.as_str())
            .collect::<Vec<_>>();
        if !stale.is_empty() {
            warn!(
                event = "metadata.authorization.projection_stale",
                count = stale.len() as u64,
                sample_graph_iris = ?stale.iter().take(8).collect::<Vec<_>>(),
                "metadata authorization projection disagrees with Craqle graph policy"
            );
        }
    }
}

#[cfg(test)]
#[path = "caches_tests.rs"]
mod tests;
