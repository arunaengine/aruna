//! Defines distributed buckets: the local binding, the replicated record, members, key ranges,
//! settings and the epochs that order versions and admit publications.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::placement::policy::PlacementPolicyRef;
use crate::types::GroupId;

pub mod change;
pub mod hint;
pub mod presets;
pub mod statement;
pub mod validate;

pub use change::{DistributionChange, DistributionOp, TransferRole};
pub use hint::{HintIndex, SetHint};
pub use statement::{SetRoot, SignedStatement, Statement, StatementKind};
pub use validate::DistributionError;

/// At most this many key ranges per set (scale rules).
pub const MAX_RANGES: usize = 4096;
/// At most this many members per set.
pub const MAX_MEMBERS: usize = 256;
/// A range bound is an S3 key, so it is at most one key long.
pub const MAX_BOUND_BYTES: usize = 1024;
/// The encoded record stays below this size, whatever its ranges and members.
pub const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;
/// The first coordinator epoch of a range; epoch 0 belongs to ordinary buckets.
pub const FIRST_EPOCH: u64 = 1;

const OWNER_DOMAIN: &[u8] = b"aruna-distribution-owner-v1";
const OWNER_BITS: u32 = 48;
const FREE_BITS: u32 = 80 - OWNER_BITS;

/// A set id whose random part commits to its owner group: 48 bits of BLAKE3 over the group
/// and 32 free bits. No other group can create a set under this id.
pub fn owned_set_id(owner_group: GroupId, time_ms: u64, free: u32) -> Ulid {
    Ulid::from_parts(
        time_ms,
        owner_tag(owner_group) << FREE_BITS | u128::from(free),
    )
}

/// Mints a new set id for `owner_group` now.
pub fn mint_set_id(owner_group: GroupId) -> Ulid {
    let fresh = Ulid::generate();
    owned_set_id(owner_group, fresh.timestamp_ms(), fresh.random() as u32)
}

/// Whether `set_id` commits to `owner_group`; the set's ownership never changes.
pub fn owned_by(set_id: Ulid, owner_group: GroupId) -> bool {
    set_id.random() >> FREE_BITS == owner_tag(owner_group)
}

fn owner_tag(owner_group: GroupId) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OWNER_DOMAIN);
    hasher.update(&owner_group.to_bytes());
    let mut tag = [0; 16];
    tag[10..].copy_from_slice(&hasher.finalize().as_bytes()[..6]);
    u128::from_be_bytes(tag)
}

/// The coordinator epoch a version was written under. Heads order by (original epoch,
/// generation); ordinary buckets always carry 0.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct OriginalEpoch(pub u64);

/// The sender's current coordinator epoch on a publication. It only admits the
/// publication and never orders the versions it carries.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct PublicationEpoch(pub u64);

/// Binds a node-local bucket to one member of a distributed set. Ordinary buckets have none.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct DistributionBinding {
    pub set_id: Ulid,
    pub member_id: Ulid,
    /// The local bucket incarnation this member admitted with.
    pub incarnation: u128,
    /// The record revision the binding was last aligned with, digest included, so a binding
    /// aligned with a losing branch is told apart from the winner.
    pub revision: DistributionRevision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum MemberState {
    Created,
    CatchingUp,
    Ready,
    Leaving,
    Failed,
}

/// Which records a member holds besides the ranges it coordinates or replicates.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum MetadataCoverage {
    All,
    Assigned,
}

/// How a member keeps the bytes of the ranges it holds records for.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DataStrategy {
    Copy,
    CopyOnUse,
    CacheOnUse,
    Stream,
}

/// How a member that does not hold the bytes answers a byte range request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum RangeBehaviour {
    Passthrough,
    FillFirst,
    ServeAndFill,
}

impl DataStrategy {
    /// The range behaviour a member uses when neither it nor the bucket sets one.
    pub fn default_behaviour(self) -> RangeBehaviour {
        match self {
            Self::Stream => RangeBehaviour::Passthrough,
            Self::Copy | Self::CopyOnUse | Self::CacheOnUse => RangeBehaviour::ServeAndFill,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionMember {
    pub member_id: Ulid,
    pub node_id: NodeId,
    /// The member's local bucket incarnation, filled when the member admits itself.
    pub incarnation: Option<u128>,
    pub state: MemberState,
    pub coverage: MetadataCoverage,
    pub strategy: DataStrategy,
    /// Overrides the bucket's range behaviour for this member.
    pub range_behaviour: Option<RangeBehaviour>,
}

/// One key range `[lower, upper)`; `upper` `None` is the end of the key space.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyRange {
    pub range_id: Ulid,
    pub lower: String,
    pub upper: Option<String>,
    /// The range this one was split from.
    pub lineage: Option<Ulid>,
    /// The member that is the only writer of the range.
    pub coordinator: Ulid,
    /// The coordinator epoch; a new coordinator always takes a fresh, higher one.
    pub epoch: u64,
    pub replicas: Vec<Ulid>,
}

impl KeyRange {
    pub fn contains(&self, key: &str) -> bool {
        key >= self.lower.as_str() && self.upper.as_deref().is_none_or(|upper| key < upper)
    }

    /// The epoch the coordinator stamps on the versions it writes now.
    pub fn original_epoch(&self) -> OriginalEpoch {
        OriginalEpoch(self.epoch)
    }

    /// The epoch the coordinator presents when it publishes to members now.
    pub fn publication_epoch(&self) -> PublicationEpoch {
        PublicationEpoch(self.epoch)
    }
}

/// Bucket-wide settings a group admin may change after creation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionSettings {
    /// Seconds between coordinated source refreshes; `None` refreshes only on request.
    pub refresh_secs: Option<u64>,
    /// `None` lets each member's data strategy decide.
    pub range_behaviour: Option<RangeBehaviour>,
    pub search_index: bool,
    pub public: bool,
}

/// The configuration a `create` carries in full.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionConfig {
    /// The one bucket name every member's local bucket carries.
    pub bucket: String,
    pub owner_group: GroupId,
    /// The pinned placement policy that chose the members; later changes never move them.
    pub node_set: Option<PlacementPolicyRef>,
    pub members: Vec<DistributionMember>,
    pub ranges: Vec<KeyRange>,
    pub settings: DistributionSettings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SetPhase {
    Pending,
    Active,
}

/// One reduced revision; the digest chains every applied change, so two branches differ.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct DistributionRevision {
    pub number: u64,
    pub digest: [u8; 32],
}

/// The replicated record of one distributed bucket. It carries configuration only:
/// no object records, credentials or metrics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistributionRecord {
    pub set_id: Ulid,
    pub revision: DistributionRevision,
    pub phase: SetPhase,
    pub config: DistributionConfig,
}

impl DistributionRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn member(&self, member_id: Ulid) -> Option<&DistributionMember> {
        self.config
            .members
            .iter()
            .find(|member| member.member_id == member_id)
    }

    /// The range holding `key`; ranges are complete, so a valid record always has one.
    pub fn range_for(&self, key: &str) -> Option<&KeyRange> {
        let index = self
            .config
            .ranges
            .partition_point(|range| range.lower.as_str() <= key);
        index
            .checked_sub(1)
            .and_then(|index| self.config.ranges.get(index))
            .filter(|range| range.contains(key))
    }

    /// The bucket serves once it is active and every coordinator is ready.
    pub fn serving(&self) -> bool {
        self.phase == SetPhase::Active
            && self.config.ranges.iter().all(|range| {
                self.member(range.coordinator)
                    .is_some_and(|member| member.state == MemberState::Ready)
            })
    }

    /// The range behaviour a member uses: its own, the bucket's, then its strategy default.
    pub fn range_behaviour(&self, member: &DistributionMember) -> RangeBehaviour {
        member
            .range_behaviour
            .or(self.config.settings.range_behaviour)
            .unwrap_or_else(|| member.strategy.default_behaviour())
    }
}

#[cfg(test)]
mod tests;
