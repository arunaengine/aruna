//! Estimates the physical bytes a member needs for the ranges it accepts, and the reservation
//! rows that keep what a node admitted until the ranges leave it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::on_use::CACHE_CAPACITY_BYTES;
use super::{DataStrategy, DistributionSnapshot, KeyRange};
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
/// `copy_on_use` also cost its bytes. Cache-on-use reserves its cache, at most the bytes it
/// could hold. Every range needs a size, a confirmed empty one included; else its id is the error.
pub fn member_need(
    member_id: Ulid,
    strategy: DataStrategy,
    ranges: &[KeyRange],
    sizes: &[RangeSize],
) -> Result<MemberNeed, Ulid> {
    let mut need = MemberNeed::default();
    let mut cached = 0u64;
    for range in ranges {
        let size = (sizes.iter())
            .find(|size| size.range_id == range.range_id)
            .ok_or(range.range_id)?;
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
    Ok(need)
}

/// `sizes` with a confirmed empty size for each range no admitted member holds, since only an
/// admission opens a range on a node.
pub fn with_unopened(snapshot: &DistributionSnapshot, sizes: &[RangeSize]) -> Vec<RangeSize> {
    let admitted = (snapshot.config.members.iter()).filter(|member| member.incarnation.is_some());
    let opened: Vec<Ulid> = admitted
        .flat_map(|member| snapshot.held_ranges(member.member_id))
        .map(|range| range.range_id)
        .collect();
    let mut sizes = sizes.to_vec();
    for range in &snapshot.config.ranges {
        let known = sizes.iter().any(|size| size.range_id == range.range_id);
        if !known && !opened.contains(&range.range_id) {
            sizes.push(RangeSize {
                range_id: range.range_id,
                ..RangeSize::default()
            });
        }
    }
    sizes
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
    /// stream replica only metadata, and a confirmed empty range nothing.
    #[test]
    fn need_by_role() {
        let (me, other) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let ranges = vec![
            range(1, me, vec![]),
            range(2, other, vec![me]),
            range(3, other, vec![]),
            range(4, other, vec![me]),
        ];
        let sizes = [
            size(1, 1000, 1),
            size(2, 2000, 2),
            size(3, 3000, 3),
            size(4, 0, 0),
        ];
        let copy = member_need(me, DataStrategy::Copy, &ranges, &sizes).unwrap();
        let meta = OBJECT_METADATA_BYTES;
        let expected = vec![
            (ranges[0].range_id, 1000 + meta),
            (ranges[1].range_id, 2000 + 2 * meta),
            (ranges[2].range_id, 3 * meta),
            (ranges[3].range_id, 0),
        ];
        assert_eq!(copy.ranges, expected);
        assert_eq!(copy.cache_bytes, 0);
        let stream = member_need(me, DataStrategy::Stream, &ranges, &sizes).unwrap();
        assert_eq!(stream.ranges[1], (ranges[1].range_id, 2 * meta));
        let cache = member_need(me, DataStrategy::CacheOnUse, &ranges, &sizes).unwrap();
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
        let need = member_need(me, DataStrategy::CacheOnUse, &ranges, &sizes).unwrap();
        assert_eq!(need.cache_bytes, CACHE_CAPACITY_BYTES);
    }

    /// A range without a size is unknown, never empty.
    #[test]
    fn unknown_size_refused() {
        let (me, other) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let ranges = vec![range(1, me, vec![]), range(2, other, vec![me])];
        let sizes = [size(1, 1000, 1)];
        let need = member_need(me, DataStrategy::Stream, &ranges, &sizes);
        assert_eq!(need, Err(ranges[1].range_id));
    }

    /// Only a range no admitted member holds counts as confirmed empty.
    #[test]
    fn unopened_ranges_empty() {
        use crate::structs::identity::realm::RealmId;
        use crate::structs::storage::distribution::presets::shard_layout;
        use crate::structs::storage::distribution::{
            DistributionConfig, DistributionRequest, DistributionSettings, SetRoot, Statement,
            next_snapshot,
        };

        let secret = |seed: u8| iroh::SecretKey::from_bytes(&[seed; 32]);
        let (realm, member) = (RealmId::from_bytes([9; 32]), Ulid::from_bytes([1; 16]));
        let root = SetRoot {
            publisher: secret(1).public(),
            owner_group: Ulid::from_bytes([90; 16]),
            nonce: [7; 16],
        };
        let nodes = [
            (member, secret(1).public()),
            (Ulid::from_bytes([2; 16]), secret(2).public()),
        ];
        let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], 0).unwrap();
        let create = DistributionRequest::Create {
            root: root.clone(),
            config: DistributionConfig {
                bucket: "spire".to_string(),
                owner_group: root.owner_group,
                node_set: None,
                members: layout.members,
                ranges: layout.ranges,
                settings: DistributionSettings::default(),
            },
        };
        let head = next_snapshot(realm, None, &create).unwrap();
        let ids: Vec<Ulid> = head.config.ranges.iter().map(|r| r.range_id).collect();
        let empty = |id: Ulid| RangeSize {
            range_id: id,
            ..RangeSize::default()
        };
        assert_eq!(
            with_unopened(&head, &[]),
            vec![empty(ids[0]), empty(ids[1])]
        );
        let intent = head.member(member).unwrap().intent;
        let statement = Statement::admit(realm, head.set_id(), member, 5, intent);
        let request = DistributionRequest::Statement(Box::new(statement.sign(&secret(1))));
        let head = next_snapshot(realm, Some(&head), &request).unwrap();
        assert_eq!(with_unopened(&head, &[]), vec![empty(ids[1])]);
        let known = [size(1, 10, 1)];
        let sizes = with_unopened(&head, &known);
        assert_eq!(sizes, vec![known[0], empty(ids[1])]);
    }
}
