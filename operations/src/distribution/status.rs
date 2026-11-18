//! Names the status reads of a distributed bucket: each returns the operation that answers it
//! from this node's record and heartbeats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::realm::RealmId;
use ulid::Ulid;

pub use super::here::node_reach;
use super::here::{heard, here};
pub use super::status_view::{
    AddProgressOperation, CopyStatus, DeletionStatus, DistributionStatus, FillProgress, FillStatus,
    LifecycleStatus, MemberStatus, RangeStatus, Reach, ReadStatusOperation, ReadViewOperation,
    StatusError, TransferStatus, heartbeat_reach, summarize,
};
use crate::driver::DriverContext;

/// This node's record of the set, if it holds one.
pub fn read_view(set_id: Ulid) -> ReadViewOperation {
    ReadViewOperation { set_id }
}

/// The status of a set this node holds the record of.
pub fn read_status(
    context: &DriverContext,
    realm_id: RealmId,
    set_id: Ulid,
) -> ReadStatusOperation {
    ReadStatusOperation::new(realm_id, set_id, heard(context))
}

/// Adds this node's search index progress and, for the ranges it coordinates, what it still
/// owes each member. Both are hints, so a failed read leaves them out.
pub fn add_progress(context: &DriverContext, status: &DistributionStatus) -> AddProgressOperation {
    let local = here(context).ids.map(|(node_id, _)| node_id);
    let has_indexes = context.storage_handle.range_indexes().is_some();
    AddProgressOperation::new(status.clone(), local, has_indexes)
}
