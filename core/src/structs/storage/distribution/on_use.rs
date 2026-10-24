//! Describes the bytes a member keeps on use: copy-on-use attachments and cache-on-use entries,
//! each bound to a version's BLAKE3 and published only after a complete verified read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{DataStrategy, RangeBehaviour};
use crate::errors::ConversionError;
use crate::structs::storage::blob::BackendLocation;

/// Hidden blob namespace of copy-on-use attachments; the attachment row protects each one.
pub const ATTACHMENT_NAMESPACE: Ulid = Ulid::from_bytes(*b"attach-on-use-v1");
/// Hidden blob namespace of cache-on-use entries; the entry row protects each one.
pub const CACHE_NAMESPACE: Ulid = Ulid::from_bytes(*b"cache-on-use--v1");
/// Bytes the cache of one node keeps; admitting more evicts the oldest entries first.
pub const CACHE_CAPACITY_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// A fill claim older than this is taken over: its filler stopped without releasing it.
pub const FILL_CLAIM_TTL_MS: u64 = 15 * 60 * 1000;
/// The key of the cache totals row; entry keys are 32 bytes long.
pub const CACHE_TOTALS_KEY: &[u8] = b"totals";

/// A verified local copy of one version, attached without changing the version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attachment {
    pub blake3: [u8; 32],
    pub location: BackendLocation,
}

/// One cache entry, stored by its key and again by its admission order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheEntry {
    /// The logical identity of its version: the BLAKE3 of stored bytes, or the observation
    /// digest of an external reference.
    pub key: [u8; 32],
    /// The BLAKE3 of the kept bytes.
    pub blake3: [u8; 32],
    pub location: BackendLocation,
    pub size: u64,
    /// Its key in the admission order.
    pub order: Ulid,
}

/// What the cache holds now.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheTotals {
    pub bytes: u64,
    pub entries: u64,
}

/// One running fill of a kept copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FillClaim {
    pub claim_id: Ulid,
    pub claimed_at_ms: u64,
}

impl FillClaim {
    /// Whether another filler may take this claim over at `now_ms`.
    pub fn stale(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.claimed_at_ms) >= FILL_CLAIM_TTL_MS
    }
}

/// Which kind of kept copy a fill produces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum KeptKind {
    Attachment,
    Cache,
}

impl KeptKind {
    /// What a member with `strategy` keeps of bytes it reads through a holder.
    pub fn of(strategy: DataStrategy) -> Option<Self> {
        match strategy {
            DataStrategy::CopyOnUse => Some(Self::Attachment),
            DataStrategy::CacheOnUse => Some(Self::Cache),
            DataStrategy::Copy | DataStrategy::Stream => None,
        }
    }

    pub fn namespace(self) -> Ulid {
        match self {
            Self::Attachment => ATTACHMENT_NAMESPACE,
            Self::Cache => CACHE_NAMESPACE,
        }
    }
}

/// The attachment key of a version: set id, then version id.
pub fn attachment_key(set_id: Ulid, version_id: Ulid) -> Vec<u8> {
    let mut key = set_id.to_bytes().to_vec();
    key.extend_from_slice(&version_id.to_bytes());
    key
}

/// The claim key of one fill: its kind, then the attachment key or the cache entry key.
pub fn claim_key(kind: KeptKind, set_id: Ulid, version_id: Ulid, identity: [u8; 32]) -> Vec<u8> {
    match kind {
        KeptKind::Attachment => [&[0u8][..], &attachment_key(set_id, version_id)].concat(),
        KeptKind::Cache => [&[1u8][..], &identity[..]].concat(),
    }
}

/// How one read of bytes this member does not keep is answered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PullPlan {
    /// Streams the bytes, or only the range, from a holder; nothing is kept.
    Stream,
    /// Reads the whole object into a local spool, then serves the range of it.
    SpoolFirst,
    /// Keeps the whole object first, then serves the range from the kept copy.
    FillFirst(KeptKind),
    /// Streams the range at once and keeps the whole object in the background.
    ServeAndFill(KeptKind),
}

impl PullPlan {
    /// The plan for a read with or without a byte range. A whole read of a member that keeps
    /// bytes fills first: serving and filling at once would transfer the object twice.
    pub fn choose(
        strategy: DataStrategy,
        behaviour: RangeBehaviour,
        ranged: bool,
        keeping: bool,
    ) -> Self {
        let kept = KeptKind::of(strategy).filter(|_| keeping);
        match (kept, behaviour, ranged) {
            (Some(kind), _, false) | (Some(kind), RangeBehaviour::FillFirst, true) => {
                Self::FillFirst(kind)
            }
            (Some(kind), RangeBehaviour::ServeAndFill, true) => Self::ServeAndFill(kind),
            (None, RangeBehaviour::FillFirst, true) => Self::SpoolFirst,
            _ => Self::Stream,
        }
    }
}

macro_rules! postcard_codec {
    ($($kind:ty),+) => {$(
        impl $kind {
            pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
                Ok(postcard::to_allocvec(self)?)
            }

            pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
                Ok(postcard::from_bytes(bytes)?)
            }
        }
    )+};
}

postcard_codec!(Attachment, CacheEntry, CacheTotals, FillClaim);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_follow_strategy() {
        use DataStrategy::{CacheOnUse, CopyOnUse};
        use RangeBehaviour::{FillFirst, Passthrough, ServeAndFill};
        let plan =
            |strategy, behaviour, ranged| PullPlan::choose(strategy, behaviour, ranged, true);
        assert_eq!(
            plan(DataStrategy::Stream, Passthrough, true),
            PullPlan::Stream
        );
        assert_eq!(
            plan(DataStrategy::Stream, FillFirst, true),
            PullPlan::SpoolFirst
        );
        assert_eq!(
            plan(DataStrategy::Stream, FillFirst, false),
            PullPlan::Stream
        );
        assert_eq!(
            plan(CopyOnUse, ServeAndFill, true),
            PullPlan::ServeAndFill(KeptKind::Attachment)
        );
        assert_eq!(
            plan(CacheOnUse, Passthrough, false),
            PullPlan::FillFirst(KeptKind::Cache),
            "a whole read of a caching member keeps the bytes"
        );
        assert_eq!(plan(CacheOnUse, Passthrough, true), PullPlan::Stream);
        assert_eq!(
            plan(CopyOnUse, FillFirst, true),
            PullPlan::FillFirst(KeptKind::Attachment)
        );
        assert_eq!(
            plan(DataStrategy::Copy, ServeAndFill, true),
            PullPlan::Stream
        );
    }

    #[test]
    fn unkept_never_fills() {
        assert_eq!(
            PullPlan::choose(
                DataStrategy::CopyOnUse,
                RangeBehaviour::ServeAndFill,
                true,
                false
            ),
            PullPlan::Stream
        );
        assert_eq!(
            PullPlan::choose(
                DataStrategy::CacheOnUse,
                RangeBehaviour::FillFirst,
                true,
                false
            ),
            PullPlan::SpoolFirst
        );
    }

    #[test]
    fn claims_expire() {
        let claim = FillClaim {
            claim_id: Ulid::nil(),
            claimed_at_ms: 1_000,
        };
        assert!(!claim.stale(1_000 + FILL_CLAIM_TTL_MS - 1));
        assert!(claim.stale(1_000 + FILL_CLAIM_TTL_MS));
        let set = Ulid::from_bytes([1; 16]);
        let version = Ulid::from_bytes([2; 16]);
        assert_ne!(
            claim_key(KeptKind::Attachment, set, version, [3; 32]),
            claim_key(KeptKind::Cache, set, version, [3; 32])
        );
    }
}
