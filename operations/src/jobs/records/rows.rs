//! Stored rows of the append-only job-record store. Only the immutable record
//! keyspace is authority; every other row is retained evidence or derived cache.

use aruna_core::NodeId;
use aruna_core::errors::ConversionError;
use aruna_core::structs::{JobProjection, JobRecordEnvelope, JobRecordKind};
use serde::{Deserialize, Serialize};

/// What a pending record is still waiting for. A pending record is never
/// projected, never relayed, and never counted as evidence for another record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingNeed {
    /// The named predecessor is not stored as authentic here yet.
    Evidence(JobRecordKind),
    /// The local holder view is missing or conflicted, so no author rule can be
    /// judged. Verifying against an empty view would reject instead of retry.
    LocalView,
    /// The local view does not rank the publisher as a holder. Holder authority
    /// moves with membership, so the record waits for a view that grants it.
    HolderView,
}

/// One record retained until its evidence or the local view arrives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRecord {
    pub envelope: JobRecordEnvelope,
    pub need: PendingNeed,
    pub first_seen_ms: u64,
    /// Bounded admission attempts, so a permanently unresolvable record cannot
    /// be retried forever.
    pub attempts: u32,
}

/// Explicit same-key/different-digest evidence. Both records stay addressable:
/// the stored one keeps its key and the refused one keeps this row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictRecord {
    pub envelope: JobRecordEnvelope,
    /// Digest of the record already stored under the same key.
    pub retained: [u8; 32],
    pub observed_at_ms: u64,
    /// Peer that relayed it, for audit only; the publisher is in the envelope.
    pub relayed_by: Option<NodeId>,
}

/// Row format this build writes and reads. A row tagged otherwise is a row this
/// build cannot read, which only forces a rebuild.
pub const PROJECTION_CACHE_VERSION: u16 = 1;

/// Derived per-family projection with its bounded revision. It is a cache: a
/// missing, stale, or undecodable row only forces a rebuild from the records.
/// A truncated projection is never stored, so a fresh row is always complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCache {
    pub version: u16,
    pub revision: u64,
    pub stale: bool,
    pub projection: Option<JobProjection>,
}

impl ProjectionCache {
    /// A newly observed record invalidates the cache without losing the
    /// revision, so a client can still tell that its view changed.
    pub fn invalidated(previous: Option<&Self>) -> Self {
        Self {
            version: PROJECTION_CACHE_VERSION,
            revision: previous.map_or(0, |cache| cache.revision),
            stale: true,
            projection: previous.and_then(|cache| cache.projection.clone()),
        }
    }

    pub fn updated(&self, projection: Option<JobProjection>) -> Self {
        Self {
            version: PROJECTION_CACHE_VERSION,
            revision: self.revision.saturating_add(1),
            stale: false,
            projection,
        }
    }

    /// Reads a stored row, discarding one this build did not write.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        from_bytes::<Self>(bytes)
            .ok()
            .filter(|cache| cache.version == PROJECTION_CACHE_VERSION)
    }
}

/// A locally published authentic record and its per-holder replication state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub queued_at_ms: u64,
    /// Holders that durably accepted this record. A row remains until every
    /// current holder is accounted for, so one acknowledgement is not quorum.
    pub delivered: Vec<NodeId>,
    /// Index at which the next bounded holder fan-out starts.
    pub next_holder: u32,
    /// Definitive refusals this record collected. A record every holder refuses
    /// is dropped instead of being offered forever.
    pub rejections: u32,
}

pub fn to_bytes<T: Serialize>(row: &T) -> Result<Vec<u8>, ConversionError> {
    Ok(postcard::to_allocvec(row)?)
}

pub fn from_bytes<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T, ConversionError> {
    Ok(postcard::from_bytes(bytes)?)
}
