use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use lru::LruCache;
use ulid::Ulid;

// Two maximum-size list pages worth of documents, capped by bytes so a few
// unusually large summaries cannot grow the cache without bound. The pair
// encodes a 16 KiB average summary and roughly 32 MiB of resident memory.
const SUMMARY_CACHE_ENTRIES: usize = 2_048;
const SUMMARY_CACHE_BYTES: usize = 32 * 1024 * 1024;
/// Backstop staleness bound, matching the visibility and query cache TTLs, so
/// any coherence hole between the registry cursor and the local graph content
/// expires instead of surviving until the next event or eviction.
const SUMMARY_CACHE_TTL: Duration = Duration::from_secs(30);

static SUMMARY_CACHE: OnceLock<MetadataSummaryCache> = OnceLock::new();

pub(crate) fn summary_cache() -> &'static MetadataSummaryCache {
    SUMMARY_CACHE.get_or_init(|| {
        MetadataSummaryCache::new(
            SUMMARY_CACHE_ENTRIES,
            SUMMARY_CACHE_BYTES,
            SUMMARY_CACHE_TTL,
        )
    })
}

/// RO-Crate summary cache keyed by `(graph_iri, cursor)`, where the cursor is
/// the document's `last_event_id`. A replicated cursor can lead the local graph
/// content, so document sync invalidates and the TTL bounds what it misses.
pub(crate) struct MetadataSummaryCache {
    state: Mutex<SummaryCacheState>,
    ttl: Duration,
    max_bytes: usize,
}

struct SummaryCacheState {
    entries: LruCache<String, SummaryEntry>,
    bytes: usize,
}

struct SummaryEntry {
    cursor: Ulid,
    summary: Arc<str>,
    expires_at: Instant,
}

impl MetadataSummaryCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::MIN);
        Self {
            state: Mutex::new(SummaryCacheState {
                entries: LruCache::new(capacity),
                bytes: 0,
            }),
            ttl,
            max_bytes,
        }
    }

    /// Only one summary per graph is retained, so a cursor advance both misses
    /// and frees the superseded entry.
    pub(crate) fn get(&self, graph_iri: &str, cursor: Ulid, now: Instant) -> Option<Arc<str>> {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let entry = state.entries.get(graph_iri)?;
        if entry.expires_at <= now {
            let size = entry_size(graph_iri, &entry.summary);
            state.entries.pop(graph_iri);
            state.bytes = state.bytes.saturating_sub(size);
            return None;
        }
        (entry.cursor == cursor).then(|| entry.summary.clone())
    }

    pub(crate) fn insert(&self, graph_iri: &str, cursor: Ulid, summary: &str, now: Instant) {
        let size = entry_size(graph_iri, summary);
        if size > self.max_bytes {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let entry = SummaryEntry {
            cursor,
            summary: Arc::from(summary),
            expires_at: now + self.ttl,
        };
        if let Some((key, replaced)) = state.entries.push(graph_iri.to_string(), entry) {
            state.bytes = state
                .bytes
                .saturating_sub(entry_size(&key, &replaced.summary));
        }
        state.bytes = state.bytes.saturating_add(size);
        while state.bytes > self.max_bytes {
            let Some((key, evicted)) = state.entries.pop_lru() else {
                break;
            };
            state.bytes = state
                .bytes
                .saturating_sub(entry_size(&key, &evicted.summary));
        }
    }

    pub(crate) fn remove(&self, graph_iri: &str) {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        if let Some(removed) = state.entries.pop(graph_iri) {
            state.bytes = state
                .bytes
                .saturating_sub(entry_size(graph_iri, &removed.summary));
        }
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

fn entry_size(graph_iri: &str, summary: &str) -> usize {
    graph_iri.len() + summary.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(30);

    fn cache(max_entries: usize, max_bytes: usize) -> MetadataSummaryCache {
        MetadataSummaryCache::new(max_entries, max_bytes, TTL)
    }

    #[test]
    fn hit_returns_entry() {
        let cache = cache(4, 1024);
        let cursor = Ulid::generate();
        let now = Instant::now();
        cache.insert("urn:graph:a", cursor, "{\"@graph\":[]}", now);

        assert_eq!(
            cache.get("urn:graph:a", cursor, now).as_deref(),
            Some("{\"@graph\":[]}")
        );
        assert!(cache.get("urn:graph:b", cursor, now).is_none());
    }

    #[test]
    fn cursor_advance_misses() {
        // A stale summary must never survive an update of the same document.
        let cache = cache(4, 1024);
        let first = Ulid::generate();
        let second = Ulid::generate();
        let now = Instant::now();
        cache.insert("urn:graph:a", first, "stale", now);

        assert!(cache.get("urn:graph:a", second, now).is_none());
        cache.insert("urn:graph:a", second, "fresh", now);
        assert_eq!(
            cache.get("urn:graph:a", second, now).as_deref(),
            Some("fresh")
        );
        assert!(cache.get("urn:graph:a", first, now).is_none());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn budget_evicts_lru() {
        let cache = cache(8, 32);
        let cursor = Ulid::generate();
        let now = Instant::now();
        cache.insert("a", cursor, &"x".repeat(15), now);
        cache.insert("b", cursor, &"y".repeat(15), now);
        assert_eq!(cache.len(), 2);

        cache.insert("c", cursor, &"z".repeat(15), now);
        assert_eq!(cache.len(), 2);
        assert!(cache.get("a", cursor, now).is_none());
        assert!(cache.get("b", cursor, now).is_some());
        assert!(cache.get("c", cursor, now).is_some());
    }

    #[test]
    fn oversized_entry_skipped() {
        let cache = cache(4, 16);
        let cursor = Ulid::generate();
        let now = Instant::now();
        cache.insert("a", cursor, &"x".repeat(64), now);

        assert!(cache.get("a", cursor, now).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn remove_frees_budget() {
        let cache = cache(4, 32);
        let cursor = Ulid::generate();
        let now = Instant::now();
        cache.insert("a", cursor, &"x".repeat(20), now);
        cache.remove("a");
        cache.insert("b", cursor, &"y".repeat(20), now);

        assert!(cache.get("b", cursor, now).is_some());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn sync_drops_summaries() {
        // Document sync can land content under a cursor a listing already
        // cached, so only the synced graph loses its entry and its bytes.
        let cache = cache(4, 32);
        let cursor = Ulid::generate();
        let now = Instant::now();
        let kept = "y".repeat(10);
        cache.insert("a", cursor, &"x".repeat(10), now);
        cache.insert("b", cursor, &kept, now);
        cache.remove("a");

        assert!(cache.get("a", cursor, now).is_none());
        assert_eq!(
            cache.get("b", cursor, now).as_deref(),
            Some(kept.as_str()),
            "a graph the sync never touched must keep its summary"
        );
        assert_eq!(cache.len(), 1);
        // Only fits if removing "a" released its bytes as well as its slot.
        cache.insert("c", cursor, &"z".repeat(20), now);
        assert!(cache.get("c", cursor, now).is_some());
        assert!(cache.get("b", cursor, now).is_some());
    }

    #[test]
    fn expired_entry_misses() {
        // The TTL bounds any coherence hole the cursor key cannot see.
        let cache = cache(4, 1024);
        let cursor = Ulid::generate();
        let now = Instant::now();
        cache.insert("a", cursor, "summary", now);
        let fresh = now + TTL - Duration::from_secs(1);

        assert!(cache.get("a", cursor, fresh).is_some());
        assert!(cache.get("a", cursor, now + TTL).is_none());
        assert_eq!(cache.len(), 0);
    }
}
