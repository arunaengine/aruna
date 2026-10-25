//! Defines per-range accounting of a distributed bucket: delta-merged counter rows, the
//! account row with its active generation and rebuild cursors, the range index and key sample.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use super::OriginalEpoch;
use super::inventory::{HeadEntry, VersionEntry};
use crate::errors::ConversionError;

/// Distinct keys a range's key sample holds; concurrent writers may exceed it briefly.
pub const SAMPLE_SIZE: u16 = 1024;
const GENERATION_TAG: u8 = b'g';
const PENDING_TAG: u8 = b'p';
const SLOT_TAG: u8 = b's';
const COUNT_TAG: u8 = b'n';
const REFILL_TAG: u8 = b'r';
/// Above every valid UTF-8 byte, so it is the range index key of the open end.
const OPEN_END: u8 = 0xFF;

/// A range counter that left the `i128` range; the change is refused, never clamped.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("range counter {0} overflowed")]
pub struct CounterOverflow(pub &'static str);

/// Maintained counters of one range. Rows only ever receive these as commit-time deltas, so
/// every field is signed and a pending split row may go below zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeCounters {
    /// Current heads that are not delete markers.
    pub objects: i128,
    /// Sizes of those heads.
    pub current_bytes: i128,
    /// Version rows that are not delete markers, noncurrent ones included.
    pub versions: i128,
    pub delete_markers: i128,
    /// Sizes of stored version rows.
    pub logical_bytes: i128,
    /// Sizes of version rows reached through an external reference.
    pub referenced_bytes: i128,
    pub open_uploads: i128,
    /// Key sample entries; only the sample count row of a range carries them.
    pub sampled: i128,
    /// The highest original epoch of any version counted; it never decreases.
    pub max_epoch: OriginalEpoch,
}

fn add(field: &'static str, left: i128, right: i128) -> Result<i128, CounterOverflow> {
    left.checked_add(right).ok_or(CounterOverflow(field))
}

fn neg(field: &'static str, value: i128) -> Result<i128, CounterOverflow> {
    value.checked_neg().ok_or(CounterOverflow(field))
}

impl RangeCounters {
    pub fn merge(self, other: Self) -> Result<Self, CounterOverflow> {
        Ok(Self {
            objects: add("objects", self.objects, other.objects)?,
            current_bytes: add("current_bytes", self.current_bytes, other.current_bytes)?,
            versions: add("versions", self.versions, other.versions)?,
            delete_markers: add("delete_markers", self.delete_markers, other.delete_markers)?,
            logical_bytes: add("logical_bytes", self.logical_bytes, other.logical_bytes)?,
            referenced_bytes: add(
                "referenced_bytes",
                self.referenced_bytes,
                other.referenced_bytes,
            )?,
            open_uploads: add("open_uploads", self.open_uploads, other.open_uploads)?,
            sampled: add("sampled", self.sampled, other.sampled)?,
            max_epoch: self.max_epoch.max(other.max_epoch),
        })
    }

    /// The same change with every count reversed; the epoch maximum stays.
    pub fn negate(self) -> Result<Self, CounterOverflow> {
        Ok(Self {
            objects: neg("objects", self.objects)?,
            current_bytes: neg("current_bytes", self.current_bytes)?,
            versions: neg("versions", self.versions)?,
            delete_markers: neg("delete_markers", self.delete_markers)?,
            logical_bytes: neg("logical_bytes", self.logical_bytes)?,
            referenced_bytes: neg("referenced_bytes", self.referenced_bytes)?,
            open_uploads: neg("open_uploads", self.open_uploads)?,
            sampled: neg("sampled", self.sampled)?,
            max_epoch: OriginalEpoch::default(),
        })
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }

    /// What a version row at `epoch` contributes.
    pub fn of_version(epoch: OriginalEpoch, entry: &VersionEntry) -> Self {
        let size = i128::from(entry.size);
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
                current_bytes: i128::from(head.version.size),
                ..Self::default()
            },
        }
    }

    pub fn of_upload(uploads: i128) -> Self {
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
    /// A split child before the handoff: its parent's `frozen` generation row plus both
    /// pending rows are charged, and the child's own deltas go to its pending row.
    Child {
        parent: Ulid,
        sibling: Ulid,
        frozen: u64,
    },
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

/// One entry of a range's key sample. Rows sort by (priority, key), so the sample holds the
/// lowest priorities and its largest entry is the last row; both depend on the key alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SampleEntry {
    pub priority: u64,
    pub key: String,
}

impl SampleEntry {
    pub fn of_key(key: &str) -> Self {
        let hash = blake3::hash(key.as_bytes());
        let mut priority = [0; 8];
        priority.copy_from_slice(&hash.as_bytes()[..8]);
        Self {
            priority: u64::from_be_bytes(priority),
            key: key.to_string(),
        }
    }

    pub fn row_key(&self, set_id: Ulid, range_id: Ulid) -> Vec<u8> {
        let mut out = sample_prefix(set_id, range_id);
        out.extend_from_slice(&self.priority.to_be_bytes());
        out.extend_from_slice(self.key.as_bytes());
        out
    }

    /// Reads an entry back from its row key.
    pub fn decode(row: &[u8]) -> Option<Self> {
        let rest = row.get(SAMPLE_PREFIX_LEN..)?;
        let priority = u64::from_be_bytes(rest.get(..8)?.try_into().ok()?);
        let key = String::from_utf8(rest[8..].to_vec()).ok()?;
        Some(Self { priority, key })
    }
}

/// Set id, range id and tag.
const SAMPLE_PREFIX_LEN: usize = 33;

pub fn sample_prefix(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut out = range_prefix(set_id, range_id);
    out.push(SLOT_TAG);
    out
}

/// The delta-merged row that counts a range's sample entries in [`RangeCounters::sampled`].
pub fn sample_count_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut out = range_prefix(set_id, range_id);
    out.push(COUNT_TAG);
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
pub fn needs_refill(occupied: usize, objects: i128) -> bool {
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
            identity: None,
            pending: false,
        }
    }

    #[test]
    fn counts_contributions() {
        let stored = RangeCounters::of_version(OriginalEpoch(2), &entry(false, 10, false));
        let linked = RangeCounters::of_version(OriginalEpoch(1), &entry(false, 4, true));
        let marker = RangeCounters::of_version(OriginalEpoch(3), &entry(true, 0, false));
        let total = stored.merge(linked).unwrap().merge(marker).unwrap();
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
        let removed = total.merge(stored.negate().unwrap()).unwrap();
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
    fn samples_by_priority() {
        let (set, range) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        let entries: Vec<_> = ["a/1", "a/2", "b"].map(SampleEntry::of_key).into();
        assert_eq!(
            SampleEntry::of_key("a/1"),
            entries[0],
            "a replay finds the same entry"
        );
        let mut rows: Vec<_> = entries
            .iter()
            .map(|entry| entry.row_key(set, range))
            .collect();
        rows.sort();
        let decoded: Vec<_> = rows
            .iter()
            .map(|row| SampleEntry::decode(row).unwrap())
            .collect();
        let mut sorted = entries.clone();
        sorted.sort_by_key(|entry| (entry.priority, entry.key.clone()));
        assert_eq!(decoded, sorted);
    }

    #[test]
    fn refills_below_half() {
        assert!(!needs_refill(0, 1));
        assert!(needs_refill(0, 2));
        assert!(!needs_refill(512, 1_000_000));
        assert!(needs_refill(511, 1_000_000));
    }
}
