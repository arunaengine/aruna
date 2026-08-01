//! Derived handle-range directory and the node-local allocation cursor.
//!
//! Coordinator grants ([`HandleRange`]) are replicated realm config, immutable
//! and append-only. The [`HandleRangeDirectory`] is each node's derived view over
//! that set: it fails closed when two grants overlap (both converge to
//! `Conflicted` on every node, independent of arrival order, because the verdict
//! is a pure function of the set). The [`HandleAllocationCursor`] is the opposite
//! kind of state — durable, node-local, never replicated — tracking the next
//! unused handle inside a node's own granted ranges so a restart never re-issues
//! a spent handle.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{FIRST_HANDLE, HandleRange};
use crate::structured_id::PlacementHandle;

/// The derived view over the replicated handle-range set. Overlapping grants —
/// or two distinct values re-using one `range_id` — are retained as conflicted
/// and excluded from allocation, mirroring [`crate::structs::BindingDirectory`].
#[derive(Debug, Default, Clone)]
pub struct HandleRangeDirectory {
    by_id: HashMap<Ulid, HandleRange>,
    conflicted: BTreeSet<Ulid>,
}

impl HandleRangeDirectory {
    pub fn from_ranges(ranges: &[HandleRange]) -> Self {
        let mut directory = Self::default();
        // Distinct value re-using a `range_id` is a same-key divergence.
        for range in ranges {
            match directory.by_id.get(&range.range_id) {
                Some(existing) if existing == range => {}
                Some(_) => {
                    directory.conflicted.insert(range.range_id);
                }
                None => {
                    directory.by_id.insert(range.range_id, *range);
                }
            }
        }
        // Any two ranges whose intervals intersect are both fail-closed.
        let ids: Vec<Ulid> = directory.by_id.keys().copied().collect();
        for (i, left_id) in ids.iter().enumerate() {
            for right_id in &ids[i + 1..] {
                let left = directory.by_id[left_id];
                let right = directory.by_id[right_id];
                if left.overlaps(&right) {
                    directory.conflicted.insert(*left_id);
                    directory.conflicted.insert(*right_id);
                }
            }
        }
        directory
    }

    pub fn is_conflicted(&self, range_id: &Ulid) -> bool {
        self.conflicted.contains(range_id)
    }

    pub fn conflicts(&self) -> usize {
        self.conflicted.len()
    }

    /// Non-conflicted ranges owned by `owner`, sorted by `start`. These are the
    /// disjoint slices the owner may mint from.
    pub fn granted_to(&self, owner: &NodeId) -> Vec<HandleRange> {
        let mut ranges: Vec<HandleRange> = self
            .by_id
            .values()
            .filter(|range| range.owner == *owner && !self.conflicted.contains(&range.range_id))
            .copied()
            .collect();
        ranges.sort_by_key(|range| (range.start, range.range_id));
        ranges
    }

    /// The start of the next range a coordinator should grant: one past the
    /// highest end already carved out (conflicted grants included, so a bad
    /// overlap is never re-granted), or [`FIRST_HANDLE`] when nothing is granted.
    pub fn next_grantable_start(&self) -> u32 {
        self.by_id
            .values()
            .map(|range| range.end)
            .max()
            .unwrap_or(FIRST_HANDLE)
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
        Self { next: FIRST_HANDLE }
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

    fn range(id: u8, owner: NodeId, start: u32, end: u32) -> HandleRange {
        HandleRange {
            range_id: Ulid::from_bytes([id; 16]),
            owner,
            start,
            end,
        }
    }

    #[test]
    fn disjoint_grants_are_usable() {
        let owner = node(1);
        let ranges = [range(1, owner, 1, 1025), range(2, owner, 1025, 2049)];
        let directory = HandleRangeDirectory::from_ranges(&ranges);
        assert_eq!(directory.conflicts(), 0);
        assert_eq!(directory.granted_to(&owner).len(), 2);
        assert_eq!(directory.next_grantable_start(), 2049);
    }

    #[test]
    fn overlapping_grants_fail_closed() {
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
    fn cursor_walks_ranges_and_skips_gaps() {
        let owner = node(1);
        let low = range(1, owner, 1, 3);
        let high = range(2, owner, 2049, 2051);
        let mut cursor = HandleAllocationCursor::new();
        let drawn: Vec<u32> = std::iter::from_fn(|| {
            cursor
                .allocate(&[low, high])
                .map(|(handle, _)| handle.get())
        })
        .collect();
        assert_eq!(drawn, vec![1, 2, 2049, 2050]);
        assert!(cursor.allocate(&[low, high]).is_none());
    }

    #[test]
    fn cursor_reload_does_not_reuse() {
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
    fn full_space_last_range_is_bounded() {
        let owner = node(1);
        let last = range(1, owner, HANDLE_SPACE_END - 1, HANDLE_SPACE_END);
        let mut cursor = HandleAllocationCursor { next: last.start };
        let (handle, _) = cursor.allocate(&[last]).unwrap();
        assert_eq!(handle.get(), HANDLE_SPACE_END - 1);
        assert!(cursor.allocate(&[last]).is_none());
    }
}
