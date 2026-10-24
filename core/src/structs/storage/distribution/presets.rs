//! Fills members and key ranges for the distribution presets: replica or mirror, sharding,
//! sharding with replicas, and copy-on-use, cache-on-use or stream edges. Members may mix.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::{
    DataStrategy, DistributionError, DistributionMember, FIRST_EPOCH, KeyRange, MemberState,
    MetadataCoverage, validate::validate_ranges,
};
use crate::NodeId;

const RANGE_ID_DOMAIN: &[u8] = b"aruna-distribution-range-v1";
const RANDOM_MASK: u128 = (1 << 80) - 1;

/// Members and ranges a preset produced; the caller adds them to a configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Layout {
    pub members: Vec<DistributionMember>,
    pub ranges: Vec<KeyRange>,
}

/// One range; the first node coordinates and every other node keeps a full `copy`.
/// Two nodes give the two-node replica.
pub fn replica_layout(set_id: Ulid, nodes: &[(Ulid, NodeId)]) -> Result<Layout, DistributionError> {
    let Some(((coordinator, _), others)) = nodes.split_first() else {
        return Err(DistributionError::MemberCount);
    };
    Ok(Layout {
        members: members(nodes, MetadataCoverage::All, DataStrategy::Copy),
        ranges: vec![KeyRange {
            range_id: range_id(set_id, 0),
            lower: String::new(),
            upper: None,
            lineage: None,
            coordinator: *coordinator,
            epoch: FIRST_EPOCH,
            replicas: others.iter().map(|(member_id, _)| *member_id).collect(),
        }],
    })
}

/// Ranges split at `boundaries`, coordinators in turn, each with `replicas` further copies on
/// the following nodes; `replicas` 0 is plain sharding. Members hold only their own ranges.
pub fn shard_layout(
    set_id: Ulid,
    nodes: &[(Ulid, NodeId)],
    boundaries: &[String],
    replicas: usize,
) -> Result<Layout, DistributionError> {
    if nodes.is_empty() {
        return Err(DistributionError::MemberCount);
    }
    if replicas >= nodes.len() {
        return Err(DistributionError::Malformed(
            "a range needs fewer replicas than members",
        ));
    }
    let lowers = std::iter::once(String::new()).chain(boundaries.iter().cloned());
    let uppers = boundaries.iter().cloned().map(Some).chain([None]);
    let ranges: Vec<KeyRange> = lowers
        .zip(uppers)
        .enumerate()
        .map(|(index, (lower, upper))| KeyRange {
            range_id: range_id(set_id, index as u64),
            lower,
            upper,
            lineage: None,
            coordinator: nodes[index % nodes.len()].0,
            epoch: FIRST_EPOCH,
            replicas: (1..=replicas)
                .map(|offset| nodes[(index + offset) % nodes.len()].0)
                .collect(),
        })
        .collect();
    validate_ranges(&ranges)?;
    Ok(Layout {
        members: members(nodes, MetadataCoverage::Assigned, DataStrategy::Copy),
        ranges,
    })
}

/// Adds edge members that hold every record and fetch bytes on use or stream them.
pub fn add_edges(
    layout: &mut Layout,
    edges: &[(Ulid, NodeId)],
    strategy: DataStrategy,
) -> Result<(), DistributionError> {
    if strategy == DataStrategy::Copy {
        return Err(DistributionError::Malformed(
            "edge members use copy-on-use, cache-on-use or stream",
        ));
    }
    layout
        .members
        .extend(members(edges, MetadataCoverage::All, strategy));
    Ok(())
}

/// The immutable id of a preset's range, derived from the set so planning stays pure.
pub fn range_id(set_id: Ulid, index: u64) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(RANGE_ID_DOMAIN);
    hasher.update(&set_id.to_bytes());
    hasher.update(&index.to_be_bytes());
    let mut random = [0; 16];
    random.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ulid::from_parts(
        set_id.timestamp_ms(),
        u128::from_be_bytes(random) & RANDOM_MASK,
    )
}

fn members(
    nodes: &[(Ulid, NodeId)],
    coverage: MetadataCoverage,
    strategy: DataStrategy,
) -> Vec<DistributionMember> {
    nodes
        .iter()
        .map(|(member_id, node_id)| DistributionMember {
            member_id: *member_id,
            node_id: *node_id,
            incarnation: None,
            state: MemberState::Created,
            coverage,
            strategy,
            range_behaviour: None,
            // The create request that adds the member fills in its intent.
            intent: [0; 32],
        })
        .collect()
}
