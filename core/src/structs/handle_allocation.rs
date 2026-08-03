//! Fail-closed directory over replicated handle grants plus the node-local
//! durable allocation cursor. Overlapping grants never become allocatable.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{FIRST_GRANTABLE_HANDLE, HANDLE_BANDS, HANDLE_RANGE_SIZE, HandleRange};
use crate::structured_id::PlacementHandle;

/// The derived view over the replicated handle-range set. Overlapping grants —
/// or two distinct values re-using one `range_id` — are retained as conflicted
/// and excluded from allocation, mirroring [`crate::structs::BindingDirectory`].
#[derive(Debug, Default, Clone)]
pub struct HandleRangeDirectory {
    by_id: HashMap<Ulid, BTreeSet<HandleRange>>,
    conflicted: BTreeSet<Ulid>,
}

impl HandleRangeDirectory {
    pub fn from_ranges(ranges: &[HandleRange]) -> Self {
        let mut directory = Self::default();
        // Distinct value re-using a `range_id` is a same-key divergence.
        for range in ranges {
            match directory.by_id.get_mut(&range.range_id) {
                Some(existing) if existing.contains(range) => {}
                Some(existing) => {
                    existing.insert(*range);
                    directory.conflicted.insert(range.range_id);
                }
                None => {
                    directory
                        .by_id
                        .insert(range.range_id, BTreeSet::from([*range]));
                }
            }
        }
        // Any two ranges whose intervals intersect are both fail-closed.
        let ranges: Vec<(Ulid, HandleRange)> = directory
            .by_id
            .iter()
            .flat_map(|(range_id, ranges)| ranges.iter().map(|range| (*range_id, *range)))
            .collect();
        for (i, (left_id, left)) in ranges.iter().enumerate() {
            for (right_id, right) in &ranges[i + 1..] {
                if left_id == right_id {
                    continue;
                }
                if left.overlaps(right) {
                    directory.conflicted.insert(*left_id);
                    directory.conflicted.insert(*right_id);
                }
            }
        }
        directory
    }

    pub fn conflicts(&self) -> usize {
        self.conflicted.len()
    }

    /// Non-conflicted ranges owned by `owner`, sorted by `start`. These are the
    /// disjoint slices the owner may mint from.
    pub fn granted_to(&self, owner: &NodeId) -> Vec<HandleRange> {
        let mut ranges: Vec<HandleRange> = self
            .by_id
            .iter()
            .filter(|(range_id, _)| !self.conflicted.contains(range_id))
            .flat_map(|(_, ranges)| ranges)
            .filter(|range| range.owner == *owner)
            .copied()
            .collect();
        ranges.sort_by_key(|range| (range.start, range.range_id));
        ranges
    }

    pub fn owned_range(&self, range_id: &Ulid, owner: &NodeId) -> Option<HandleRange> {
        self.by_id
            .get(range_id)
            .filter(|_| !self.conflicted.contains(range_id))
            .and_then(|ranges| ranges.first().copied())
            .filter(|range| range.owner == *owner)
    }

    /// First free band at or after the band holding `preferred_start`, wrapping.
    /// A band is free when it intersects no stored grant; `None` means the whole
    /// assignable space is occupied.
    pub fn free_band_from(&self, preferred_start: u32) -> Option<(u32, u32)> {
        let preferred = preferred_start.saturating_sub(FIRST_GRANTABLE_HANDLE) / HANDLE_RANGE_SIZE;
        (0..HANDLE_BANDS).find_map(|offset| {
            let index = (preferred + offset) % HANDLE_BANDS;
            let start = FIRST_GRANTABLE_HANDLE + index * HANDLE_RANGE_SIZE;
            let end = start + HANDLE_RANGE_SIZE;
            (!self
                .by_id
                .values()
                .flatten()
                .any(|range| range.start < end && start < range.end))
            .then_some((start, end))
        })
    }
}

/// The durable, node-local next-unused handle. Persisted before an allocation is
/// acknowledged so a crash never lets a spent handle be drawn twice. Not
/// replicated: peers rely on non-overlapping grants, not on knowing progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandleAllocationCursor {
    /// Next handle value to try. Monotonic across all of this node's ranges.
    pub next: u32,
}

impl Default for HandleAllocationCursor {
    fn default() -> Self {
        Self {
            next: FIRST_GRANTABLE_HANDLE,
        }
    }
}

impl HandleAllocationCursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Draws the lowest unused handle at or after `next` that falls inside one of
    /// `ranges` (this node's disjoint granted slices, any order), advancing the
    /// cursor past it. `None` ⇒ every granted handle is spent.
    pub fn allocate(&mut self, ranges: &[HandleRange]) -> Option<(PlacementHandle, Ulid)> {
        let mut sorted: Vec<&HandleRange> =
            ranges.iter().filter(|range| !range.is_empty()).collect();
        sorted.sort_by_key(|range| (range.start, range.range_id));
        for range in sorted {
            if range.end <= self.next {
                continue;
            }
            let candidate = self.next.max(range.start);
            if let Ok(handle) = PlacementHandle::new(candidate) {
                self.next = candidate + 1;
                return Some((handle, range.range_id));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{HANDLE_RANGE_SIZE, HANDLE_SPACE_END};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn assigned_bands_survive() {
        let left = node(1);
        let right = node(7);
        let ls = FIRST_GRANTABLE_HANDLE;
        let le = ls + HANDLE_RANGE_SIZE;
        let rs = le;
        let re = rs + HANDLE_RANGE_SIZE;
        let ranges = [range(1, left, ls, le), range(2, right, rs, re)];
        let directory = HandleRangeDirectory::from_ranges(&ranges);
        assert_eq!(directory.conflicts(), 0);
        assert_eq!(directory.granted_to(&left).len(), 1);
        assert_eq!(directory.granted_to(&right).len(), 1);
        assert_eq!(
            directory.free_band_from(FIRST_GRANTABLE_HANDLE),
            Some((re, re + HANDLE_RANGE_SIZE))
        );
    }

    fn range(id: u8, owner: NodeId, start: u32, end: u32) -> HandleRange {
        HandleRange {
            range_id: Ulid::from_bytes([id; 16]),
            owner,
            start,
            end,
        }
    }

    #[test]
    fn disjoint_grants_work() {
        let owner = node(1);
        let ranges = [range(1, owner, 1, 1025), range(2, owner, 1025, 2049)];
        let directory = HandleRangeDirectory::from_ranges(&ranges);
        assert_eq!(directory.conflicts(), 0);
        assert_eq!(directory.granted_to(&owner).len(), 2);
        assert_eq!(
            directory.free_band_from(FIRST_GRANTABLE_HANDLE),
            Some((2051, 3075))
        );
    }

    #[test]
    fn overlap_fails_closed() {
        let owner = node(1);
        let ranges = [range(1, owner, 1, 1025), range(2, owner, 512, 2049)];
        let directory = HandleRangeDirectory::from_ranges(&ranges);
        assert_eq!(directory.conflicts(), 2);
        assert!(directory.granted_to(&owner).is_empty());
        // Order-independent: reversing the input yields the same verdict.
        let reversed = HandleRangeDirectory::from_ranges(&[ranges[1], ranges[0]]);
        assert_eq!(reversed.conflicts(), 2);
    }

    #[test]
    fn divergence_occupies_span() {
        let owner = node(1);
        let first = range(1, owner, 3, 1027);
        let divergent = range(1, owner, 1027, 2051);
        let overlap = range(2, owner, 1500, 2500);
        let directory = HandleRangeDirectory::from_ranges(&[first, divergent, overlap]);

        assert_eq!(directory.conflicts(), 2);
        assert!(directory.granted_to(&owner).is_empty());
        assert_eq!(
            directory.free_band_from(FIRST_GRANTABLE_HANDLE),
            Some((3075, 4099))
        );
    }

    #[test]
    fn cursor_skips_gaps() {
        let owner = node(1);
        let low = range(1, owner, 3, 5);
        let high = range(2, owner, 2049, 2051);
        let mut cursor = HandleAllocationCursor::new();
        let drawn: Vec<u32> = std::iter::from_fn(|| {
            cursor
                .allocate(&[low, high])
                .map(|(handle, _)| handle.get())
        })
        .collect();
        assert_eq!(drawn, vec![3, 4, 2049, 2050]);
        assert!(cursor.allocate(&[low, high]).is_none());
    }

    #[test]
    fn cursor_avoids_reuse() {
        let owner = node(1);
        let ranges = [range(1, owner, 1, HANDLE_RANGE_SIZE + 1)];
        let mut cursor = HandleAllocationCursor::new();
        let (first, range_id) = cursor.allocate(&ranges).unwrap();
        assert_eq!(range_id, ranges[0].range_id);
        // Persist and reload: a fresh cursor at the saved `next` never re-draws.
        let mut reloaded = HandleAllocationCursor { next: cursor.next };
        let (second, _) = reloaded.allocate(&ranges).unwrap();
        assert!(second.get() > first.get());
    }

    #[test]
    fn last_range_bounded() {
        let owner = node(1);
        let last = range(1, owner, HANDLE_SPACE_END - 1, HANDLE_SPACE_END);
        let mut cursor = HandleAllocationCursor { next: last.start };
        let (handle, _) = cursor.allocate(&[last]).unwrap();
        assert_eq!(handle.get(), HANDLE_SPACE_END - 1);
        assert!(cursor.allocate(&[last]).is_none());
    }
}
