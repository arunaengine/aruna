//! Node-local durable cache of resolved placement policies.
//!
//! The key is `(policy_id, digest)`: an id-only key could accept changed bytes
//! under a known id, which policy immutability forbids. A stored positive entry
//! is bytes, never a trusted document, so every lookup verifies the definition
//! and its publication signature again before it may be matched against a
//! subject. The realm-admin authority behind that publication was verified
//! before the entry was written, and the retained publication keeps it
//! auditable afterwards.

use aruna_core::errors::ConversionError;
use aruna_core::structs::{
    PlacementPolicyDocument, PlacementPolicyError, PlacementPolicyRef, PolicyAuthorityError,
    RealmId, VerifiedPolicy,
};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// How long an availability miss suppresses another holder round trip. It is a
/// hint only: it expires quickly and never turns a later valid policy into a
/// denial.
pub const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(10);

/// Entries the cache retains. A definition is immutable, so eviction only costs
/// a refetch and never changes what a subject is allowed to do.
pub const MAX_CACHE_ENTRIES: usize = 256;

/// Encoded bytes the cache retains across all of its entries.
pub const MAX_CACHE_BYTES: usize = 2 * 1024 * 1024;

/// Encoded bytes of one entry. A verified policy stays well below this through
/// its own name, selector, and label bounds; a larger row is unusable.
pub const MAX_CACHE_ENTRY_BYTES: usize = 256 * 1024;

pub const CACHE_KEY_LEN: usize = 16 + 32;

/// A single entry must always fit, or eviction could never make room for it.
const _: () = assert!(MAX_CACHE_ENTRY_BYTES < MAX_CACHE_BYTES);

#[derive(Debug, Error, PartialEq)]
pub enum PolicyCacheError {
    #[error("cache entry exceeds {MAX_CACHE_ENTRY_BYTES} bytes")]
    EntryBytes,
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Authority(#[from] PolicyAuthorityError),
    /// The stored bytes are not the requested definition, so they are discarded
    /// instead of served.
    #[error("cached policy does not match the requested ref")]
    Mismatch,
}

/// Cache row. Positive entries need no correctness TTL because the definition
/// behind `(policy_id, digest)` can never change; negative entries carry an
/// explicit expiry because they are availability hints only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyCacheEntry {
    /// The authenticated document, so the entry keeps the provenance its
    /// verification rested on instead of bare policy bytes.
    Verified {
        document: PlacementPolicyDocument,
        stored_at_ms: u64,
    },
    Unavailable {
        stored_at_ms: u64,
        expires_at_ms: u64,
    },
}

/// What a lookup found. A corrupt, foreign, or mismatched row is a miss: it is
/// never reported as a denial and is replaced by the next resolve.
#[derive(Debug, PartialEq)]
pub enum CacheLookup {
    Hit {
        document: Box<PlacementPolicyDocument>,
        policy: Box<VerifiedPolicy>,
    },
    /// A live availability hint; the caller reports unavailable without a fetch.
    Negative,
    Miss,
}

/// Hit, miss, and eviction counts of one resolve or gate run. Ids and counts
/// only: policy contents never reach a log or a span.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PolicyCacheStats {
    pub hits: u32,
    pub misses: u32,
    pub evictions: u32,
}

impl PolicyCacheStats {
    pub fn merge(&mut self, other: Self) {
        self.hits = self.hits.saturating_add(other.hits);
        self.misses = self.misses.saturating_add(other.misses);
        self.evictions = self.evictions.saturating_add(other.evictions);
    }
}

impl PolicyCacheEntry {
    pub fn verified(document: &PlacementPolicyDocument, stored_at_ms: u64) -> Self {
        Self::Verified {
            document: document.clone(),
            stored_at_ms,
        }
    }

    pub fn unavailable(stored_at_ms: u64) -> Self {
        Self::Unavailable {
            stored_at_ms,
            expires_at_ms: stored_at_ms.saturating_add(NEGATIVE_CACHE_TTL.as_millis() as u64),
        }
    }

    pub fn stored_at_ms(&self) -> u64 {
        match self {
            Self::Verified { stored_at_ms, .. } | Self::Unavailable { stored_at_ms, .. } => {
                *stored_at_ms
            }
        }
    }

    /// Whether the entry no longer carries usable information. Positive entries
    /// never expire; a negative hint does.
    pub fn expired(&self, now_ms: u64) -> bool {
        match self {
            Self::Verified { .. } => false,
            Self::Unavailable { expires_at_ms, .. } => *expires_at_ms <= now_ms,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, PolicyCacheError> {
        let bytes = postcard::to_allocvec(self).map_err(ConversionError::from)?;
        if bytes.len() > MAX_CACHE_ENTRY_BYTES {
            return Err(PolicyCacheError::EntryBytes);
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PolicyCacheError> {
        if bytes.len() > MAX_CACHE_ENTRY_BYTES {
            return Err(PolicyCacheError::EntryBytes);
        }
        Ok(postcard::from_bytes(bytes).map_err(ConversionError::from)?)
    }

    /// Re-verifies stored bytes against the realm and ref that were asked for,
    /// including the publication that authenticated them. Construction from a
    /// row is never enough to make a document matchable.
    fn accept(
        self,
        realm_id: RealmId,
        policy_ref: &PlacementPolicyRef,
    ) -> Result<(PlacementPolicyDocument, VerifiedPolicy), PolicyCacheError> {
        let Self::Verified { document, .. } = self else {
            return Err(PolicyCacheError::Mismatch);
        };
        if document.realm_id != realm_id {
            return Err(PolicyCacheError::Mismatch);
        }
        let verified = VerifiedPolicy::verify(document.policy.clone())?;
        if verified.policy_ref() != *policy_ref {
            return Err(PolicyCacheError::Mismatch);
        }
        document.verify_publication()?;
        Ok((document, verified))
    }
}

/// Key: `policy_id || digest`, so a definition change can never be served under
/// the id it replaced.
pub fn cache_key(policy_ref: &PlacementPolicyRef) -> Key {
    let mut key = Vec::with_capacity(CACHE_KEY_LEN);
    key.extend_from_slice(&policy_ref.policy_id.to_bytes());
    key.extend_from_slice(&policy_ref.digest);
    ByteView::from(key)
}

/// Classifies one stored row for the requested ref.
pub fn lookup(
    value: Option<&Value>,
    realm_id: RealmId,
    policy_ref: &PlacementPolicyRef,
    now_ms: u64,
) -> CacheLookup {
    let Some(value) = value else {
        return CacheLookup::Miss;
    };
    let Ok(entry) = PolicyCacheEntry::from_bytes(value) else {
        return CacheLookup::Miss;
    };
    if entry.expired(now_ms) {
        return CacheLookup::Miss;
    }
    if matches!(entry, PolicyCacheEntry::Unavailable { .. }) {
        return CacheLookup::Negative;
    }
    match entry.accept(realm_id, policy_ref) {
        Ok((document, policy)) => CacheLookup::Hit {
            document: Box::new(document),
            policy: Box::new(policy),
        },
        Err(_) => CacheLookup::Miss,
    }
}

/// Keys to delete so the scanned rows plus `incoming` stay inside both bounds.
/// Unusable rows go first, then the oldest, so eviction is deterministic for
/// any scan order and only ever costs a refetch.
pub fn plan_eviction(
    rows: &[(Key, Value)],
    incoming: &Key,
    incoming_bytes: usize,
    now_ms: u64,
) -> Vec<Key> {
    let mut entries = 1usize;
    let mut bytes = incoming_bytes;
    let mut candidates = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        if key == incoming {
            continue;
        }
        entries += 1;
        bytes += value.len();
        let live = match PolicyCacheEntry::from_bytes(value) {
            Ok(entry) if !entry.expired(now_ms) => Some(entry.stored_at_ms()),
            _ => None,
        };
        candidates.push((live.is_some(), live.unwrap_or_default(), key, value.len()));
    }
    candidates
        .sort_unstable_by(|left, right| (left.0, left.1, left.2).cmp(&(right.0, right.1, right.2)));

    let mut evicted = Vec::new();
    for (_, _, key, size) in candidates {
        if entries <= MAX_CACHE_ENTRIES && bytes <= MAX_CACHE_BYTES {
            break;
        }
        entries -= 1;
        bytes -= size;
        evicted.push(key.clone());
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement_policy::tests::signed_document;
    use aruna_core::structs::{PlacementPolicy, PlacementSelector};
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn policy(seed: u8, location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn document(policy: &VerifiedPolicy) -> PlacementPolicyDocument {
        signed_document(realm(), policy, 1)
    }

    fn stored(entry: &PolicyCacheEntry) -> Value {
        ByteView::from(entry.to_bytes().expect("entry encodes"))
    }

    #[test]
    fn key_binds_digest() {
        // Two definitions can never share a row, so an id reuse cannot be served
        // from the cache of the definition it replaced.
        let first = policy(1, "eu-west");
        let second = policy(1, "us-east");
        assert_eq!(first.policy().policy_id, second.policy().policy_id);
        assert_ne!(
            cache_key(&first.policy_ref()),
            cache_key(&second.policy_ref())
        );
        assert_eq!(cache_key(&first.policy_ref()).len(), CACHE_KEY_LEN);
    }

    #[test]
    fn lookup_reverifies() {
        let policy = policy(1, "eu-west");
        let entry = PolicyCacheEntry::verified(&document(&policy), 10);
        let value = stored(&entry);
        assert_eq!(
            lookup(Some(&value), realm(), &policy.policy_ref(), 10_000),
            CacheLookup::Hit {
                document: Box::new(document(&policy)),
                policy: Box::new(policy.clone()),
            }
        );

        let other = policy_ref_of(2);
        assert_eq!(
            lookup(Some(&value), realm(), &other, 10_000),
            CacheLookup::Miss,
            "bytes stored under one ref must never answer another"
        );
        assert_eq!(
            lookup(
                Some(&value),
                RealmId::from_bytes([9u8; 32]),
                &policy.policy_ref(),
                10_000
            ),
            CacheLookup::Miss,
            "a foreign realm row is not this realm's rule"
        );
        assert_eq!(
            lookup(
                Some(&ByteView::from(vec![0xff; 8])),
                realm(),
                &policy.policy_ref(),
                10_000
            ),
            CacheLookup::Miss,
            "a corrupt row must fail into a refetch, not a denial"
        );
    }

    fn policy_ref_of(seed: u8) -> PlacementPolicyRef {
        policy(seed, "eu-west").policy_ref()
    }

    #[test]
    fn rejects_tampered_entry() {
        // A row whose provenance was rewritten resolves again instead of being
        // served: the cache may only answer with an authenticated publication.
        let policy = policy(1, "eu-west");
        let mut document = document(&policy);
        document.publication.created_at_ms += 1;
        let value = stored(&PolicyCacheEntry::verified(&document, 10));
        assert_eq!(
            lookup(Some(&value), realm(), &policy.policy_ref(), 10_000),
            CacheLookup::Miss
        );
    }

    #[test]
    fn negative_expires() {
        let entry = PolicyCacheEntry::unavailable(1_000);
        let value = stored(&entry);
        let policy_ref = policy_ref_of(1);
        assert_eq!(
            lookup(Some(&value), realm(), &policy_ref, 1_500),
            CacheLookup::Negative
        );
        let ttl = NEGATIVE_CACHE_TTL.as_millis() as u64;
        assert_eq!(
            lookup(Some(&value), realm(), &policy_ref, 1_000 + ttl),
            CacheLookup::Miss,
            "an expired hint must resolve again instead of denying"
        );
    }

    fn row_key(index: usize) -> Key {
        let mut key = vec![0u8; CACHE_KEY_LEN];
        key[..8].copy_from_slice(&(index as u64).to_be_bytes());
        ByteView::from(key)
    }

    #[test]
    fn eviction_drops_oldest() {
        // An unusable row goes first, then the oldest live row; the incoming key
        // replaces its own row, so it never counts twice.
        let policy = policy(1, "eu-west");
        let mut rows: Vec<(Key, Value)> = (0..MAX_CACHE_ENTRIES)
            .map(|index| {
                let entry = PolicyCacheEntry::verified(&document(&policy), 100 + index as u64);
                (row_key(index), stored(&entry))
            })
            .collect();
        let expired_key = row_key(MAX_CACHE_ENTRIES);
        rows.push((
            expired_key.clone(),
            stored(&PolicyCacheEntry::unavailable(0)),
        ));

        let evicted = plan_eviction(&rows, &row_key(MAX_CACHE_ENTRIES + 1), 64, 1_000_000);
        assert_eq!(evicted, vec![expired_key, row_key(0)]);

        assert!(
            plan_eviction(&rows, &row_key(0), 64, 1_000_000).len() < evicted.len(),
            "replacing an existing key frees its own slot"
        );
    }

    #[test]
    fn eviction_bounds_bytes() {
        let policy = policy(1, "eu-west");
        let entry = PolicyCacheEntry::verified(&document(&policy), 100);
        let padding = ByteView::from(vec![0u8; MAX_CACHE_BYTES / 4]);
        let rows: Vec<(Key, Value)> = (0..4)
            .map(|index| (row_key(index), padding.clone()))
            .collect();
        let evicted = plan_eviction(
            &rows,
            &row_key(9),
            entry.to_bytes().expect("entry encodes").len(),
            1_000,
        );
        assert_eq!(
            evicted,
            vec![row_key(0)],
            "the byte bound must evict even while the entry count fits"
        );
    }

    #[test]
    fn entry_bytes_bounded() {
        assert_eq!(
            PolicyCacheEntry::from_bytes(&vec![0u8; MAX_CACHE_ENTRY_BYTES + 1]),
            Err(PolicyCacheError::EntryBytes)
        );
    }
}
