//! Encodes the durable bootstrap of a catching-up member: per coordinated range one cursor over
//! the ordered inventory, the bounded page of jobs it waits for, and the catch-up watermark.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::inventory::range_interval;
use super::{DistributionMember, DistributionSnapshot, KeyRange, MemberState};
use crate::errors::ConversionError;

/// The walk of one range for one catching-up member on the range's coordinator.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BootstrapCursor {
    /// The member's intent; a cursor of another intent starts over.
    pub intent: [u8; 32],
    /// Milliseconds when the walk started. Live publications reach the member for every write
    /// after it, so the walk owes only what the inventory held before.
    pub watermark: u64,
    /// The last inventory position queued, inclusive; `None` before the first page.
    pub after: Option<Vec<u8>>,
    /// Job keys of the last page; the next page waits until all of them are gone.
    pub pending: Vec<Vec<u8>>,
    /// The walk reached the end of the range.
    pub walked: bool,
    /// The walk reached the end and its last page was delivered.
    pub done: bool,
}

impl BootstrapCursor {
    pub fn start(intent: [u8; 32], watermark: u64) -> Self {
        Self {
            intent,
            watermark,
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

impl BootstrapCursor {
    /// The cursor a split child of the walked range continues with: what the parent queued
    /// inside the child counts as walked, the parent's watermark and pending jobs stay.
    pub fn for_child(&self, set_id: Ulid, child: &KeyRange) -> Self {
        let (lower, upper) = range_interval(set_id, &child.lower, child.upper.as_deref());
        let mut cursor = Self {
            pending: self.pending.clone(),
            ..Self::start(self.intent, self.watermark)
        };
        let after = self.after.as_ref();
        if self.walked || after.is_some_and(|after| *after >= upper) {
            cursor.walked = true;
            cursor.done = self.done;
        } else if let Some(after) = after.filter(|after| **after >= lower) {
            cursor.after = Some(after.clone());
        }
        cursor
    }
}

/// The bootstrap keys of one set: set id, then member id and range id.
pub fn bootstrap_prefix(set_id: Ulid) -> Vec<u8> {
    set_id.to_bytes().to_vec()
}

pub fn bootstrap_key(set_id: Ulid, member_id: Ulid, range_id: Ulid) -> Vec<u8> {
    let mut key = bootstrap_prefix(set_id);
    key.extend_from_slice(&member_id.to_bytes());
    key.extend_from_slice(&range_id.to_bytes());
    key
}

impl DistributionSnapshot {
    /// Admitted members that still catch up.
    pub fn catching_up(&self) -> Vec<&DistributionMember> {
        let members = self.config.members.iter();
        members
            .filter(|member| {
                member.state == MemberState::CatchingUp && member.incarnation.is_some()
            })
            .collect()
    }

    /// The ranges `coordinator` walks for the catching-up `member`: those it publishes to it.
    pub fn catch_up_ranges(&self, coordinator: Ulid, member: Ulid) -> Vec<&KeyRange> {
        let ranges = self.config.ranges.iter();
        ranges
            .filter(|range| {
                self.range_deliveries(coordinator, range)
                    .iter()
                    .any(|delivery| delivery.member_id == member)
            })
            .collect()
    }

    /// The id and epoch of each range `coordinator` walks for `member`, as a catch-up names them.
    pub fn catch_up_cover(&self, coordinator: Ulid, member: Ulid) -> Vec<(Ulid, u64)> {
        let ranges = self.catch_up_ranges(coordinator, member).into_iter();
        ranges.map(|range| (range.range_id, range.epoch)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_continues_walk() {
        // A child keeps what the parent queued inside it and starts where the parent stood.
        let set = Ulid::from_bytes([1; 16]);
        let child = |lower: &str, upper: Option<&str>| KeyRange {
            range_id: Ulid::from_bytes([2; 16]),
            lower: lower.to_string(),
            upper: upper.map(str::to_string),
            lineage: Some(Ulid::from_bytes([3; 16])),
            coordinator: Ulid::from_bytes([4; 16]),
            epoch: 1,
            replicas: Vec::new(),
        };
        let mut parent = BootstrapCursor::start([5; 32], 6);
        parent.after =
            Some(crate::structs::storage::distribution::inventory::head_position(set, "k"));
        parent.pending = vec![vec![7]];
        let lower = parent.for_child(set, &child("", Some("g")));
        assert!(lower.walked && !lower.done);
        assert_eq!(lower.pending, parent.pending);
        let middle = parent.for_child(set, &child("g", Some("p")));
        assert_eq!(middle.after, parent.after);
        let upper = parent.for_child(set, &child("p", None));
        assert_eq!(
            (upper.after, upper.walked, upper.watermark),
            (None, false, 6)
        );
    }

    #[test]
    fn keys_group_sets() {
        let (set, other) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        let key = bootstrap_key(set, Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
        assert!(key.starts_with(&bootstrap_prefix(set)));
        assert!(!key.starts_with(&bootstrap_prefix(other)));
        let cursor = BootstrapCursor::start([5; 32], 6);
        assert_eq!(
            BootstrapCursor::from_bytes(&cursor.to_bytes().unwrap()).unwrap(),
            cursor
        );
    }
}
