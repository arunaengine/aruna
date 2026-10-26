//! Estimates the physical bytes a member needs for the ranges it accepts, and the reservation
//! rows that keep what a node admitted until the ranges leave it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::on_use::CACHE_CAPACITY_BYTES;
use super::{DataStrategy, KeyRange};
use crate::errors::ConversionError;

/// Estimated metadata bytes per object on a member: head and version rows, inventory, version,
/// content and search index entries and queue rows. An estimate until measured in the testbed.
pub const OBJECT_METADATA_BYTES: u64 = 4096;

/// The expected size of one range, from the counters of its holders.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeSize {
    pub range_id: Ulid,
    /// Bytes of every stored version.
    pub bytes: u64,
    /// Version rows and delete markers.
    pub objects: u64,
}

/// What one member of a set reserves on its node besides its ranges.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberReservation {
    /// The member bucket, whose stored bytes fill the reservation.
    pub bucket: String,
    /// Room for cache-on-use entries.
    pub cache_bytes: u64,
}

impl MemberReservation {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The bytes a member needs per range it opens, and its cache budget.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemberNeed {
    pub ranges: Vec<(Ulid, u64)>,
    pub cache_bytes: u64,
}

impl MemberNeed {
    pub fn total(&self) -> u64 {
        let ranges = self.ranges.iter().map(|(_, bytes)| *bytes);
        ranges.fold(self.cache_bytes, u64::saturating_add)
    }
}

/// Every object costs metadata; a coordinated range and a range copied by `copy` or
/// `copy_on_use` also cost its bytes. A range without a known size counts as empty, as a new
/// range is. Cache-on-use reserves its cache, at most the bytes it could hold.
pub fn member_need(
    member_id: Ulid,
    strategy: DataStrategy,
    ranges: &[KeyRange],
    sizes: &[RangeSize],
) -> MemberNeed {
    let mut need = MemberNeed::default();
    let mut cached = 0u64;
    for range in ranges {
        let size = (sizes.iter())
            .find(|size| size.range_id == range.range_id)
            .copied()
            .unwrap_or_default();
        let metadata = size.objects.saturating_mul(OBJECT_METADATA_BYTES);
        let replica = range.replicas.contains(&member_id);
        let bytes = match (range.coordinator == member_id, replica, strategy) {
            (true, _, _) | (_, true, DataStrategy::Copy | DataStrategy::CopyOnUse) => size.bytes,
            (_, true, DataStrategy::CacheOnUse) => {
                cached = cached.saturating_add(size.bytes);
                0
            }
            _ => 0,
        };
        need.ranges
            .push((range.range_id, metadata.saturating_add(bytes)));
    }
    need.cache_bytes = cached.min(CACHE_CAPACITY_BYTES);
    need
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(id: u8, coordinator: Ulid, replicas: Vec<Ulid>) -> KeyRange {
        KeyRange {
            range_id: Ulid::from_parts(id.into(), 0),
            lower: String::new(),
            upper: None,
            lineage: None,
            coordinator,
            epoch: 1,
            replicas,
        }
    }

    fn size(id: u8, bytes: u64, objects: u64) -> RangeSize {
        RangeSize {
            range_id: Ulid::from_parts(id.into(), 0),
            bytes,
            objects,
        }
    }

    /// Coordinated and copied ranges cost bytes and metadata, covered ones only metadata, a
    /// stream replica only metadata, and an unknown range nothing.
    #[test]
    fn need_by_role() {
        let (me, other) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let ranges = vec![
            range(1, me, vec![]),
            range(2, other, vec![me]),
            range(3, other, vec![]),
            range(4, other, vec![me]),
        ];
        let sizes = [size(1, 1000, 1), size(2, 2000, 2), size(3, 3000, 3)];
        let copy = member_need(me, DataStrategy::Copy, &ranges, &sizes);
        let meta = OBJECT_METADATA_BYTES;
        let expected = vec![
            (ranges[0].range_id, 1000 + meta),
            (ranges[1].range_id, 2000 + 2 * meta),
            (ranges[2].range_id, 3 * meta),
            (ranges[3].range_id, 0),
        ];
        assert_eq!(copy.ranges, expected);
        assert_eq!(copy.cache_bytes, 0);
        let stream = member_need(me, DataStrategy::Stream, &ranges, &sizes);
        assert_eq!(stream.ranges[1], (ranges[1].range_id, 2 * meta));
        let cache = member_need(me, DataStrategy::CacheOnUse, &ranges, &sizes);
        assert_eq!(cache.ranges[1], (ranges[1].range_id, 2 * meta));
        assert_eq!(cache.cache_bytes, 2000);
        assert_eq!(cache.total(), 1000 + meta + 2 * meta + 3 * meta + 2000);
    }

    /// The cache budget of a member never exceeds the node's cache.
    #[test]
    fn cache_is_capped() {
        let (me, other) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let ranges = vec![range(1, other, vec![me])];
        let sizes = [size(1, u64::MAX, 0)];
        let need = member_need(me, DataStrategy::CacheOnUse, &ranges, &sizes);
        assert_eq!(need.cache_bytes, CACHE_CAPACITY_BYTES);
    }
}
