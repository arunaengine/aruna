use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use lru::LruCache;
use ulid::Ulid;

/// Resolved Profiles kept per node. A realm holds few Profiles and every
/// Dataset write and merged render resolves one, so a small cache absorbs the
/// repeated fetches while a rarely used Profile is simply refetched.
const PROFILE_CACHE_ENTRIES: usize = 64;

/// Shape sources of one Profile revision.
///
/// The key carries the revision, so a republished Profile is a different entry
/// and a cached one is never served for a revision that moved on. Usability is
/// decided from the registry row before every lookup, never from this cache.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct ProfileCacheKey {
    profile_id: Ulid,
    revision: Ulid,
}

pub(super) struct ProfileCache {
    entries: Mutex<LruCache<ProfileCacheKey, Arc<Vec<String>>>>,
    loads: AtomicU64,
}

impl Default for ProfileCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileCache {
    pub(super) fn new() -> Self {
        let capacity = NonZeroUsize::new(PROFILE_CACHE_ENTRIES).unwrap_or(NonZeroUsize::MIN);
        Self {
            entries: Mutex::new(LruCache::new(capacity)),
            loads: AtomicU64::new(0),
        }
    }

    pub(super) fn get(&self, profile_id: Ulid, revision: Ulid) -> Option<Arc<Vec<String>>> {
        let key = ProfileCacheKey {
            profile_id,
            revision,
        };
        let mut entries = self.entries.lock().unwrap_or_else(|lock| lock.into_inner());
        entries.get(&key).cloned()
    }

    /// Records one fetched revision. Every call means a fetch happened, which
    /// is what [`ProfileCache::loads`] reports.
    pub(super) fn insert(&self, profile_id: Ulid, revision: Ulid, shapes: Arc<Vec<String>>) {
        let key = ProfileCacheKey {
            profile_id,
            revision,
        };
        let mut entries = self.entries.lock().unwrap_or_else(|lock| lock.into_inner());
        entries.put(key, shapes);
        self.loads.fetch_add(1, Ordering::AcqRel);
    }

    /// Profile revisions this node fetched since start.
    pub(super) fn loads(&self) -> u64 {
        self.loads.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_keys_cache() {
        // A republished Profile must never be answered from the older entry.
        let cache = ProfileCache::new();
        let profile_id = Ulid::from_bytes([1u8; 16]);
        let revision = Ulid::from_bytes([2u8; 16]);
        cache.insert(profile_id, revision, Arc::new(vec!["shape".to_string()]));

        assert!(cache.get(profile_id, revision).is_some());
        assert!(cache.get(profile_id, Ulid::from_bytes([3u8; 16])).is_none());
        assert!(cache.get(Ulid::from_bytes([4u8; 16]), revision).is_none());
        assert_eq!(cache.loads(), 1);
    }

    #[test]
    fn evicts_beyond_capacity() {
        let cache = ProfileCache::new();
        let revision = Ulid::from_bytes([9u8; 16]);
        for index in 0..=PROFILE_CACHE_ENTRIES {
            let mut bytes = [0u8; 16];
            bytes[0] = u8::try_from(index).unwrap_or(u8::MAX);
            cache.insert(Ulid::from_bytes(bytes), revision, Arc::new(Vec::new()));
        }

        assert!(cache.get(Ulid::from_bytes([0u8; 16]), revision).is_none());
        assert_eq!(cache.loads() as usize, PROFILE_CACHE_ENTRIES + 1);
    }
}
