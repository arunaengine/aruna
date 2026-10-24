//! Defines distributed buckets: the local binding, the replicated record, members, key ranges,
//! settings and the epochs that order versions and admit publications.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::placement::policy::PlacementPolicyRef;
use crate::types::GroupId;

pub mod accounting;
pub mod bootstrap;
pub mod delegation;
pub mod delivery;
pub mod hint;
pub mod inventory;
pub mod parking;
pub mod presets;
pub mod request;
pub mod route;
pub mod snapshot;
pub mod statement;
pub mod validate;

pub use delivery::{AdmissionError, Delivery, Publication};
pub use hint::{HintIndex, SetHint};
pub use request::{DistributionRequest, TransferRole, next_snapshot};
pub use route::{RouteError, WriteRoute};
pub use snapshot::{DistributionSnapshot, DistributionView, SignedSnapshot};
pub use statement::{SetRoot, SignedStatement, Statement, StatementClaim, StatementKind};
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
    /// The digest of the create or join that added the member; its admission names exactly
    /// this intent, whatever head it is published on.
    pub intent: [u8; 32],
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

/// At most this many sets one removed publisher's cutoffs name.
pub const MAX_CUTOFFS: usize = 1024;

/// The last revision of one set a removed publisher may still have relayed: its authority
/// to publish ended with the realm config change that carries this cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PublisherCutoff {
    pub set_id: Ulid,
    pub revision: DistributionRevision,
}

/// The publisher index key: publisher node id, then set id.
pub fn publisher_key(publisher: &NodeId, set_id: Ulid) -> Vec<u8> {
    let mut key = publisher.as_bytes().to_vec();
    key.extend_from_slice(&set_id.to_bytes());
    key
}

/// Reads one publisher index row back into the set's cutoff.
pub fn cutoff_from_row(key: &[u8], value: &[u8]) -> Option<PublisherCutoff> {
    let set_id = <[u8; 16]>::try_from(key.get(32..48)?).ok()?;
    Some(PublisherCutoff {
        set_id: Ulid::from_bytes(set_id),
        revision: postcard::from_bytes(value).ok()?,
    })
}

/// One revision of a set: its number and the digest of its canonical snapshot.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct DistributionRevision {
    pub number: u64,
    pub digest: [u8; 32],
}

impl DistributionConfig {
    /// The range behaviour a member uses: its own, the bucket's, then its strategy default.
    pub fn range_behaviour(&self, member: &DistributionMember) -> RangeBehaviour {
        member
            .range_behaviour
            .or(self.settings.range_behaviour)
            .unwrap_or_else(|| member.strategy.default_behaviour())
    }
}

#[cfg(test)]
mod tests;
