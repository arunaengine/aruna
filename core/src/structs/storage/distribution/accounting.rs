//! Defines per-range accounting of a distributed bucket: delta-merged counter rows, the
//! account row with its active generation and rebuild cursors, the range index and key sample.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::OriginalEpoch;
use super::inventory::{HeadEntry, VersionEntry};
use crate::errors::ConversionError;

/// Distinct keys a range's key sample holds at most.
pub const SAMPLE_SIZE: u16 = 1024;
const GENERATION_TAG: u8 = b'g';
const PENDING_TAG: u8 = b'p';
const SLOT_TAG: u8 = b's';
const REFILL_TAG: u8 = b'r';
/// Above every valid UTF-8 byte, so it is the range index key of the open end.
const OPEN_END: u8 = 0xFF;

/// Maintained counters of one range. Rows only ever receive these as commit-time deltas, so
/// every field is signed and a pending split row may go below zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeCounters {
    /// Current heads that are not delete markers.
    pub objects: i64,
    /// Sizes of those heads.
    pub current_bytes: i64,
    /// Version rows that are not delete markers, noncurrent ones included.
    pub versions: i64,
    pub delete_markers: i64,
    /// Sizes of stored version rows.
    pub logical_bytes: i64,
    /// Sizes of version rows reached through an external reference.
    pub referenced_bytes: i64,
    pub open_uploads: i64,
    /// The highest original epoch of any version counted; it never decreases.
    pub max_epoch: OriginalEpoch,
}

fn signed(size: u64) -> i64 {
    i64::try_from(size).unwrap_or(i64::MAX)
}

impl RangeCounters {
    pub fn merge(self, other: Self) -> Self {
        Self {
            objects: self.objects.saturating_add(other.objects),
            current_bytes: self.current_bytes.saturating_add(other.current_bytes),
            versions: self.versions.saturating_add(other.versions),
            delete_markers: self.delete_markers.saturating_add(other.delete_markers),
            logical_bytes: self.logical_bytes.saturating_add(other.logical_bytes),
            referenced_bytes: self.referenced_bytes.saturating_add(other.referenced_bytes),
            open_uploads: self.open_uploads.saturating_add(other.open_uploads),
            max_epoch: self.max_epoch.max(other.max_epoch),
        }
    }

    /// The same change with every count reversed; the epoch maximum stays.
    pub fn negate(self) -> Self {
        Self {
            objects: -self.objects,
            current_bytes: -self.current_bytes,
            versions: -self.versions,
            delete_markers: -self.delete_markers,
            logical_bytes: -self.logical_bytes,
            referenced_bytes: -self.referenced_bytes,
            open_uploads: -self.open_uploads,
            max_epoch: OriginalEpoch::default(),
        }
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }

    /// What a version row at `epoch` contributes.
    pub fn of_version(epoch: OriginalEpoch, entry: &VersionEntry) -> Self {
        let size = signed(entry.size);
        let mut counters = Self {
            max_epoch: epoch,
            ..Self::default()
        };
        match (entry.marker, entry.referenced) {
            (true, _) => counters.delete_markers = 1,
            (false, false) => (counters.versions, counters.logical_bytes) = (1, size),
            (false, true) => (counters.versions, counters.referenced_bytes) = (1, size),
        }
        counters
    }

    /// What a key's head row contributes; a delete marker head counts as no object.
    pub fn of_head(head: &HeadEntry) -> Self {
        match head.version.marker {
            true => Self::default(),
            false => Self {
                objects: 1,
                current_bytes: signed(head.version.size),
                ..Self::default()
            },
        }
    }

    pub fn of_upload(uploads: i64) -> Self {
        Self {
            open_uploads: uploads,
            ..Self::default()
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// How far a rebuild walk has counted. `At` is inclusive: the position itself is counted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Cursor {
    Start,
    At(Vec<u8>),
    /// The walk read through the range's upper bound; every contribution is behind it.
    End,
}

impl Cursor {
    /// Whether a contribution at `position` is already counted by the walk.
    pub fn covers(&self, position: &[u8]) -> bool {
        match self {
            Self::Start => false,
            Self::At(cursor) => position <= cursor.as_slice(),
            Self::End => true,
        }
    }
}

/// A running rebuild: the new generation row it fills and one cursor per inventory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Rebuild {
    pub generation: u64,
    pub versions: Cursor,
    pub uploads: Cursor,
}

impl Rebuild {
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            versions: Cursor::Start,
            uploads: Cursor::Start,
        }
    }

    pub fn is_done(&self) -> bool {
        self.versions == Cursor::End && self.uploads == Cursor::End
    }
}

/// Who carries a range's charge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Charge {
    /// The range's own generation row is its total.
    Active(u64),
    /// A split child before the handoff: its parent's frozen row plus both pending rows are
    /// charged, and the child's own deltas go to its pending row.
    Child { parent: Ulid, sibling: Ulid },
    /// A split parent: its frozen `generation` row stays charged until the handoff.
    Retired {
        generation: u64,
        left: Ulid,
        right: Ulid,
    },
}

/// The one transactional accounting row of a range. Every write reads it, so changing it
/// (rebuild page, activation, split, handoff) conflicts every write that read the old row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeAccount {
    pub lower: String,
    pub upper: Option<String>,
    pub charge: Charge,
    pub rebuild: Option<Rebuild>,
}

impl RangeAccount {
    /// A new range with exact counters from zero.
    pub fn fresh(lower: String, upper: Option<String>) -> Self {
        Self {
            lower,
            upper,
            charge: Charge::Active(1),
            rebuild: None,
        }
    }

    /// Exact counters: active and not being rebuilt.
    pub fn is_complete(&self) -> bool {
        matches!(self.charge, Charge::Active(_)) && self.rebuild.is_none()
    }

    pub fn contains(&self, key: &str) -> bool {
        key >= self.lower.as_str() && self.upper.as_deref().is_none_or(|upper| key < upper)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

fn range_prefix(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut out = Vec::with_capacity(42);
    out.extend_from_slice(&set_id.to_bytes());
    out.extend_from_slice(&range_id.to_bytes());
    out
}

pub fn account_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    range_prefix(set_id, range_id)
}

/// The counter row a generation of a range accumulates.
pub fn generation_key(set_id: Ulid, range_id: Ulid, generation: u64) -> Vec<u8> {
    let mut out = range_prefix(set_id, range_id);
    out.push(GENERATION_TAG);
    out.extend_from_slice(&generation.to_be_bytes());
    out
}

/// The signed deltas a split child received before the handoff.
pub fn pending_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut out = range_prefix(set_id, range_id);
    out.push(PENDING_TAG);
    out
}

/// One range of the range index, keyed by its exclusive upper bound.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeSlot {
    pub range_id: Ulid,
    pub lower: String,
}

impl RangeSlot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The range index key of a range with exclusive `upper`.
pub fn slot_key(set_id: Ulid, upper: Option<&str>) -> Vec<u8> {
    let mut out = set_id.to_bytes().to_vec();
    match upper {
        Some(upper) => out.extend_from_slice(upper.as_bytes()),
        None => out.push(OPEN_END),
    }
    out
}

/// The first range index row after this key is the range that contains `key`.
pub fn slot_seek(set_id: Ulid, key: &str) -> Vec<u8> {
    slot_key(set_id, Some(key))
}

/// One occupied slot of a key sample.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SampleSlot {
    pub key: String,
    pub priority: u64,
}

impl SampleSlot {
    /// The slot a key may occupy and its priority there; both depend on the key alone, so
    /// replays and refills place it the same way.
    pub fn of_key(key: &str) -> (u16, Self) {
        let hash = blake3::hash(key.as_bytes());
        let bytes = hash.as_bytes();
        let slot = u16::from_be_bytes([bytes[0], bytes[1]]) % SAMPLE_SIZE;
        let priority = u64::from_be_bytes(bytes[2..10].try_into().unwrap_or_default());
        let entry = Self {
            key: key.to_string(),
            priority,
        };
        (slot, entry)
    }

    /// Whether this entry takes a slot `occupant` holds: the lowest priority wins.
    pub fn displaces(&self, occupant: Option<&Self>) -> bool {
        occupant.is_none_or(|occupant| {
            (self.priority, self.key.as_str()) < (occupant.priority, occupant.key.as_str())
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

pub fn sample_prefix(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut out = range_prefix(set_id, range_id);
    out.push(SLOT_TAG);
    out
}

pub fn sample_key(set_id: Ulid, range_id: Ulid, slot: u16) -> Vec<u8> {
    let mut out = sample_prefix(set_id, range_id);
    out.extend_from_slice(&slot.to_be_bytes());
    out
}

/// The durable cursor of a sample refill, kept apart from the account row so a refill page
/// never conflicts with writes.
pub fn refill_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut out = range_prefix(set_id, range_id);
    out.push(REFILL_TAG);
    out
}

/// A sample below half of what the range can fill needs a refill before a split uses it.
pub fn needs_refill(occupied: usize, objects: i64) -> bool {
    let reachable = usize::try_from(objects.max(0))
        .unwrap_or(usize::MAX)
        .min(usize::from(SAMPLE_SIZE));
    occupied < reachable / 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(marker: bool, size: u64, referenced: bool) -> VersionEntry {
        VersionEntry {
            marker,
            size,
            referenced,
        }
    }

    #[test]
    fn counts_contributions() {
        let stored = RangeCounters::of_version(OriginalEpoch(2), &entry(false, 10, false));
        let linked = RangeCounters::of_version(OriginalEpoch(1), &entry(false, 4, true));
        let marker = RangeCounters::of_version(OriginalEpoch(3), &entry(true, 0, false));
        let total = stored.merge(linked).merge(marker);
        assert_eq!(
            (
                total.versions,
                total.delete_markers,
                total.logical_bytes,
                total.referenced_bytes
            ),
            (2, 1, 10, 4)
        );
        assert_eq!(total.max_epoch, OriginalEpoch(3));
        let removed = total.merge(stored.negate());
        assert_eq!((removed.versions, removed.logical_bytes), (1, 0));
        assert_eq!(
            removed.max_epoch,
            OriginalEpoch(3),
            "the epoch maximum never decreases"
        );
    }

    #[test]
    fn cursor_is_inclusive() {
        let cursor = Cursor::At(b"m".to_vec());
        assert!(cursor.covers(b"a") && cursor.covers(b"m"));
        assert!(!cursor.covers(b"m\0") && !Cursor::Start.covers(b""));
        assert!(Cursor::End.covers(&[0xFF; 4]));
    }

    #[test]
    fn seek_finds_range() {
        let set = Ulid::from_bytes([1; 16]);
        let uppers = [
            slot_key(set, Some("g")),
            slot_key(set, Some("p")),
            slot_key(set, None),
        ];
        let containing = |key: &str| uppers.iter().position(|upper| *upper > slot_seek(set, key));
        assert_eq!(containing(""), Some(0));
        assert_eq!(containing("f\u{10FFFF}"), Some(0));
        assert_eq!(containing("g"), Some(1), "an upper bound is exclusive");
        assert_eq!(containing("\u{10FFFF}"), Some(2));
    }

    #[test]
    fn lowest_priority_wins() {
        let (slot, first) = SampleSlot::of_key("a/1");
        assert_eq!(SampleSlot::of_key("a/1"), (slot, first.clone()));
        assert!(slot < SAMPLE_SIZE && first.displaces(None));
        let lower = SampleSlot {
            priority: first.priority.saturating_sub(1),
            key: "z".to_string(),
        };
        assert!(lower.displaces(Some(&first)) && !first.displaces(Some(&lower)));
        assert!(
            !first.displaces(Some(&first)),
            "a replay keeps its slot unchanged"
        );
    }

    #[test]
    fn refills_below_half() {
        assert!(!needs_refill(0, 1));
        assert!(needs_refill(0, 2));
        assert!(!needs_refill(512, 1_000_000));
        assert!(needs_refill(511, 1_000_000));
    }
}
