//! Names the status reads of a distributed bucket: each returns the operation that answers it
//! from this node's record and heartbeats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::heartbeat::Heard;
use aruna_core::structs::identity::realm::RealmId;
use ulid::Ulid;

pub use super::status_view::{
    AddProgressOperation, CopyStatus, DeletionStatus, DistributionStatus, FillProgress, FillStatus,
    LifecycleStatus, MemberStatus, RangeStatus, Reach, ReadStatusOperation, ReadViewOperation,
    StatusError, TransferStatus, heartbeat_reach, node_reach, summarize,
};

/// This node's record of the set, if it holds one.
pub fn read_view(set_id: Ulid) -> ReadViewOperation {
    ReadViewOperation { set_id }
}

/// The status of a set this node holds the record of.
pub fn read_status(heard: Heard, realm_id: RealmId, set_id: Ulid) -> ReadStatusOperation {
    ReadStatusOperation::new(realm_id, set_id, heard)
}

/// Adds this node's search index progress and, for the ranges it coordinates, what it still
/// owes each member. Both are hints, so a failed read leaves them out.
pub fn add_progress(
    local: Option<NodeId>,
    has_indexes: bool,
    status: &DistributionStatus,
) -> AddProgressOperation {
    AddProgressOperation::new(status.clone(), local, has_indexes)
}
