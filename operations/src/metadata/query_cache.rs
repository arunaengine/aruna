use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::structs::{AuthContext, RealmId};
use lru::LruCache;

use super::api::MetadataFanoutStats;

/// Backstop staleness of a cached result. Matches the visibility cache TTL so
/// a query never promises fresher data than the listing path does.
pub(super) const METADATA_QUERY_CACHE_TTL: Duration = Duration::from_secs(30);
pub(super) const METADATA_QUERY_CACHE_MAX_ENTRIES: usize = 512;
pub(super) const METADATA_QUERY_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

const TAG_EAGER: u8 = 1;
const TAG_LAZY: u8 = 2;
const TAG_FANOUT: u8 = 3;
const NO_CREDENTIAL: &[u8] = b"aruna.metadata.query.anonymous";
const ENTRY_OVERHEAD: usize = 64;

// Length prefixes keep concatenated components unambiguous, so no two
// different component lists can hash to the same key.
fn push_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// Incremental digest over the graph IRIs a query is allowed to evaluate.
#[derive(Default)]
pub(super) struct ScopeDigest(blake3::Hasher);

impl ScopeDigest {
    pub(super) fn push(&mut self, graph_iri: &str) {
        push_bytes(&mut self.0, graph_iri.as_bytes());
    }

    pub(super) fn finish(self) -> [u8; 32] {
        *self.0.finalize().as_bytes()
    }
}

/// Digest of an unordered graph set.
pub(super) fn graphs_digest(graph_iris: &[String]) -> [u8; 32] {
    let mut graphs = graph_iris.iter().map(String::as_str).collect::<Vec<_>>();
    graphs.sort_unstable();
    graphs.dedup();
    let mut digest = ScopeDigest::default();
    for graph in graphs {
        digest.push(graph);
    }
    digest.finish()
}

/// Fingerprint of the caller's credential. The bearer token is only ever
/// hashed here; neither the token nor this digest is logged or stored.
pub(super) fn credential_digest(
    auth: Option<&AuthContext>,
    bearer_token: Option<&str>,
) -> Option<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    match auth {
        Some(auth) => push_bytes(&mut hasher, &serde_json::to_vec(auth).ok()?),
        None => push_bytes(&mut hasher, NO_CREDENTIAL),
    }
    match bearer_token {
        Some(token) => push_bytes(&mut hasher, token.as_bytes()),
        None => push_bytes(&mut hasher, NO_CREDENTIAL),
    }
    Some(*hasher.finalize().as_bytes())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct QueryCacheKey([u8; 32]);

#[derive(Clone, Copy)]
pub(super) enum LocalScopeKind {
    /// Caller-supplied graph filter: exactly the named graphs are evaluated.
    Eager,
    /// No graph filter: the digest covers the resolved visible graph set.
    Lazy,
}

impl LocalScopeKind {
    fn tag(self) -> u8 {
        match self {
            Self::Eager => TAG_EAGER,
            Self::Lazy => TAG_LAZY,
        }
    }
}

/// Key for a locally evaluated query: an entry is shared only between callers
/// whose authorization resolved to the identical graph set, so a hit can never
/// expose a graph the caller could not have evaluated itself.
pub(super) fn local_key(kind: LocalScopeKind, scope: &[u8; 32], sparql: &str) -> QueryCacheKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[kind.tag()]);
    push_bytes(&mut hasher, scope);
    push_bytes(&mut hasher, sparql.as_bytes());
    QueryCacheKey(*hasher.finalize().as_bytes())
}

pub(super) struct RemoteKeyInput<'a> {
    pub(super) distributed: bool,
    /// Realm the request resolved against; the credential digest alone cannot
    /// separate realms, so it must not depend on one process serving one realm.
    pub(super) realm_id: RealmId,
    pub(super) credential: &'a [u8; 32],
    pub(super) graph_iris: Option<&'a [String]>,
    pub(super) sparql: &'a str,
    pub(super) allow_partial: bool,
    pub(super) target_nodes: Option<&'a [NodeId]>,
}

/// Key for a merged fan-out result. Remote partitions authorize on the
/// forwarded credential, so entries are partitioned by realm and credential
/// digest and never shared across callers.
pub(super) fn remote_key(input: &RemoteKeyInput<'_>) -> QueryCacheKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[
        TAG_FANOUT,
        u8::from(input.distributed),
        u8::from(input.allow_partial),
    ]);
    push_bytes(&mut hasher, input.realm_id.as_bytes());
    push_bytes(&mut hasher, input.credential);
    match input.graph_iris {
        Some(graph_iris) => {
            hasher.update(&[1u8]);
            push_bytes(&mut hasher, &graphs_digest(graph_iris));
        }
        None => {
            hasher.update(&[0u8]);
        }
    }
    push_bytes(&mut hasher, input.sparql.as_bytes());
    match input.target_nodes {
        Some(nodes) => {
            hasher.update(&[1u8]);
            let mut nodes = nodes.iter().map(NodeId::as_bytes).collect::<Vec<_>>();
            nodes.sort_unstable();
            nodes.dedup();
            hasher.update(&(nodes.len() as u64).to_le_bytes());
            for node in nodes {
                push_bytes(&mut hasher, node);
            }
        }
        None => {
            hasher.update(&[0u8]);
        }
    }
    QueryCacheKey(*hasher.finalize().as_bytes())
}

/// Invalidation stamp: the visibility generation covers registry and
/// lifecycle changes, the apply counter covers graph content written by
/// replayed or already-durable effects that skip the lifecycle read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CacheStamp {
    generation: u64,
    applies: u64,
}

#[derive(Clone)]
pub(super) struct CachedQuery {
    pub(super) results: Arc<MetadataQueryResults>,
    /// Partitions merged into this result; zero for a local evaluation.
    pub(super) nodes_queried: usize,
}

struct CacheEntry {
    value: CachedQuery,
    stamp: CacheStamp,
    expires_at: Instant,
    bytes: usize,
}

struct CacheState {
    entries: LruCache<QueryCacheKey, CacheEntry>,
    bytes: usize,
}

/// Bounded LRU of SPARQL results shared by every caller whose authorization
/// resolves to the same scope.
pub(super) struct MetadataQueryCache {
    state: Mutex<CacheState>,
    applies: AtomicU64,
    ttl: Duration,
    max_bytes: usize,
}

impl Default for MetadataQueryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataQueryCache {
    pub(super) fn new() -> Self {
        Self::with_limits(
            METADATA_QUERY_CACHE_MAX_ENTRIES,
            METADATA_QUERY_CACHE_MAX_BYTES,
            METADATA_QUERY_CACHE_TTL,
        )
    }

    pub(super) fn with_limits(max_entries: usize, max_bytes: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::MIN);
        Self {
            state: Mutex::new(CacheState {
                entries: LruCache::new(capacity),
                bytes: 0,
            }),
            applies: AtomicU64::new(0),
            ttl,
            max_bytes,
        }
    }

    /// Marks every cached result stale after a graph-mutating effect applied.
    pub(super) fn bump_apply(&self) {
        self.applies.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn stamp(&self, generation: u64) -> CacheStamp {
        CacheStamp {
            generation,
            applies: self.applies.load(Ordering::Acquire),
        }
    }

    pub(super) fn get(
        &self,
        key: &QueryCacheKey,
        stamp: CacheStamp,
        now: Instant,
    ) -> Option<CachedQuery> {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let entry = state.entries.get(key)?;
        if entry.stamp != stamp || entry.expires_at <= now {
            let bytes = entry.bytes;
            state.entries.pop(key);
            state.bytes = state.bytes.saturating_sub(bytes);
            return None;
        }
        Some(entry.value.clone())
    }

    pub(super) fn insert(
        &self,
        key: QueryCacheKey,
        value: CachedQuery,
        stamp: CacheStamp,
        generation: u64,
        now: Instant,
    ) -> bool {
        // A mutation that landed while the query ran already invalidated this
        // result; storing it would only evict a fresher entry. The generation
        // is checked too, because entries are shared across callers.
        if stamp != self.stamp(generation) {
            return false;
        }
        let bytes = ENTRY_OVERHEAD.saturating_add(results_bytes(&value.results));
        if bytes > self.max_bytes {
            return false;
        }
        let entry = CacheEntry {
            value,
            stamp,
            expires_at: now + self.ttl,
            bytes,
        };
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        if let Some(replaced) = state.entries.put(key, entry) {
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

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .entries
            .len()
    }
}

/// Caches a fan-out result only when every partition answered. A partial
/// answer would pin the missing partitions for the whole TTL and could not be
/// replayed with an honest envelope.
pub(super) fn store_complete(
    cache: &MetadataQueryCache,
    key: QueryCacheKey,
    results: &MetadataQueryResults,
    stats: &MetadataFanoutStats,
    stamp: CacheStamp,
    generation: u64,
    now: Instant,
) -> bool {
    if stats.nodes_failed > 0 || stats.discovery_failed || !stats.failed_partitions.is_empty() {
        return false;
    }
    cache.insert(
        key,
        CachedQuery {
            results: Arc::new(results.clone()),
            nodes_queried: stats.nodes_queried,
        },
        stamp,
        generation,
        now,
    )
}

/// Fan-out envelope replayed from a cached entry: only complete results are
/// cached, so no partition can be reported as failed.
pub(super) fn cached_stats(cached: &CachedQuery) -> MetadataFanoutStats {
    MetadataFanoutStats {
        nodes_queried: cached.nodes_queried,
        nodes_failed: 0,
        failed_partitions: Vec::new(),
        discovery_failed: false,
    }
}

fn results_bytes(results: &MetadataQueryResults) -> usize {
    const CELL_OVERHEAD: usize = 48;
    match results {
        MetadataQueryResults::Boolean(_) => 0,
        MetadataQueryResults::Solutions(rows) => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(name, value)| name.len() + value.len() + CELL_OVERHEAD)
                    .sum::<usize>()
                    + CELL_OVERHEAD
            })
            .sum(),
        MetadataQueryResults::Graph(triples) => triples
            .iter()
            .map(|(subject, predicate, object)| {
                subject.len() + predicate.len() + object.len() + CELL_OVERHEAD
            })
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const TTL: Duration = Duration::from_secs(30);

    fn cache() -> MetadataQueryCache {
        MetadataQueryCache::with_limits(8, 1 << 20, TTL)
    }

    fn key(sparql: &str) -> QueryCacheKey {
        local_key(LocalScopeKind::Lazy, &graphs_digest(&[]), sparql)
    }

    fn rows(count: usize) -> MetadataQueryResults {
        MetadataQueryResults::Solutions(
            (0..count)
                .map(|index| BTreeMap::from([("name".to_string(), format!("value-{index:06}"))]))
                .collect(),
        )
    }

    fn cached(results: MetadataQueryResults) -> CachedQuery {
        CachedQuery {
            results: Arc::new(results),
            nodes_queried: 1,
        }
    }

    fn complete_stats() -> MetadataFanoutStats {
        MetadataFanoutStats {
            nodes_queried: 2,
            nodes_failed: 0,
            failed_partitions: Vec::new(),
            discovery_failed: false,
        }
    }

    fn remote(realm: RealmId) -> QueryCacheKey {
        remote_key(&RemoteKeyInput {
            distributed: true,
            realm_id: realm,
            credential: &[9u8; 32],
            graph_iris: None,
            sparql: "ASK { ?s ?p ?o }",
            allow_partial: false,
            target_nodes: None,
        })
    }

    #[test]
    fn returns_stored_entry() {
        let cache = cache();
        let stamp = cache.stamp(7);
        let now = Instant::now();
        assert!(cache.insert(key("ASK { ?s ?p ?o }"), cached(rows(3)), stamp, 7, now));
        let hit = cache
            .get(&key("ASK { ?s ?p ?o }"), stamp, now)
            .expect("entry must be cached");
        assert_eq!(*hit.results, rows(3));
        assert!(cache.get(&key("ASK { ?x ?y ?z }"), stamp, now).is_none());
    }

    #[test]
    fn stale_generation_misses() {
        let cache = cache();
        let now = Instant::now();
        cache.insert(key("q"), cached(rows(1)), cache.stamp(1), 1, now);
        assert!(cache.get(&key("q"), cache.stamp(2), now).is_none());
        // The miss must also drop the entry instead of resurrecting it later.
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn apply_bump_invalidates() {
        let cache = cache();
        let now = Instant::now();
        let stamp = cache.stamp(1);
        cache.insert(key("q"), cached(rows(1)), stamp, 1, now);
        cache.bump_apply();
        assert!(cache.get(&key("q"), cache.stamp(1), now).is_none());
    }

    #[test]
    fn insert_checks_generation() {
        // A visibility-only change during evaluation must not be stored: keys
        // are shared by digest, so the entry would leak across callers.
        let cache = cache();
        let now = Instant::now();
        let stamp = cache.stamp(1);
        assert!(!cache.insert(key("q"), cached(rows(1)), stamp, 2, now));
        assert_eq!(cache.len(), 0);
        assert!(cache.insert(key("q"), cached(rows(1)), stamp, 1, now));
    }

    #[test]
    fn realm_splits_key() {
        // One process serving one realm must not be the only thing keeping
        // fan-out entries apart.
        assert_ne!(remote(RealmId([1u8; 32])), remote(RealmId([2u8; 32])));
        assert_eq!(remote(RealmId([1u8; 32])), remote(RealmId([1u8; 32])));
    }

    #[test]
    fn scope_changes_key() {
        let visible = ["urn:a".to_string(), "urn:b".to_string()];
        let narrowed = ["urn:a".to_string()];
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";
        let wide = local_key(LocalScopeKind::Lazy, &graphs_digest(&visible), sparql);
        let narrow = local_key(LocalScopeKind::Lazy, &graphs_digest(&narrowed), sparql);
        assert_ne!(wide, narrow);
        // Order and duplicates must not split the entry for one scope.
        let reordered = [
            "urn:b".to_string(),
            "urn:a".to_string(),
            "urn:a".to_string(),
        ];
        assert_eq!(
            wide,
            local_key(LocalScopeKind::Lazy, &graphs_digest(&reordered), sparql)
        );
        assert_ne!(
            wide,
            local_key(LocalScopeKind::Eager, &graphs_digest(&visible), sparql)
        );
    }

    #[test]
    fn byte_budget_evicts() {
        let cache = MetadataQueryCache::with_limits(64, 4096, TTL);
        let stamp = cache.stamp(1);
        let now = Instant::now();
        for index in 0..16 {
            assert!(cache.insert(key(&format!("q{index}")), cached(rows(4)), stamp, 1, now));
        }
        assert!(cache.len() < 16, "byte budget never evicted");
        assert!(cache.get(&key("q15"), stamp, now).is_some());
        assert!(cache.get(&key("q0"), stamp, now).is_none());
        // A result larger than the whole budget is never admitted.
        assert!(!cache.insert(key("huge"), cached(rows(4096)), stamp, 1, now));
    }

    #[test]
    fn ttl_expires_entry() {
        let cache = cache();
        let stamp = cache.stamp(1);
        let now = Instant::now();
        cache.insert(key("q"), cached(rows(1)), stamp, 1, now);
        assert!(
            cache
                .get(&key("q"), stamp, now + TTL - Duration::from_secs(1))
                .is_some()
        );
        assert!(cache.get(&key("q"), stamp, now + TTL).is_none());
    }

    #[test]
    fn partial_result_uncached() {
        let cache = cache();
        let stamp = cache.stamp(1);
        let now = Instant::now();
        let results = rows(2);
        for stats in [
            MetadataFanoutStats {
                nodes_failed: 1,
                ..complete_stats()
            },
            MetadataFanoutStats {
                discovery_failed: true,
                ..complete_stats()
            },
            MetadataFanoutStats {
                failed_partitions: vec![NodeId::from_bytes(&[1u8; 32]).expect("node id")],
                ..complete_stats()
            },
        ] {
            assert!(!store_complete(
                &cache,
                key("q"),
                &results,
                &stats,
                stamp,
                1,
                now
            ));
            assert!(cache.get(&key("q"), stamp, now).is_none());
        }
        assert!(store_complete(
            &cache,
            key("q"),
            &results,
            &complete_stats(),
            stamp,
            1,
            now
        ));
        let hit = cache.get(&key("q"), stamp, now).expect("complete result");
        assert_eq!(cached_stats(&hit).nodes_queried, 2);
        assert_eq!(cached_stats(&hit).nodes_failed, 0);
    }
}
