//! Summarizes a distributed bucket from this node's record and heartbeats: members, ranges, a
//! degraded range per unreachable coordinator, and whether changes can reach the publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::DistributionView;
use ulid::Ulid;

use super::status_view::{AddProgressOperation, Heard, ReadStatusOperation, ReadViewOperation};
pub use super::status_view::{
    CopyStatus, DeletionStatus, DistributionStatus, FillProgress, FillStatus, LifecycleStatus,
    MemberStatus, RangeStatus, Reach, StatusError, TransferStatus, heartbeat_reach, summarize,
};
use crate::driver::{DriverContext, drive};

/// Who this node hears from now, from its heartbeats.
fn heard(context: &DriverContext) -> Heard {
    let nodes = context
        .held_heartbeats()
        .into_iter()
        .map(|(node_id, held)| (node_id, (held.age_ms, held.received_at_ms)))
        .collect();
    Heard {
        local: context.net_handle.as_ref().map(|net| net.node_id()),
        nodes,
    }
}

/// Whether this node hears from `node_id` now, from its heartbeats.
pub fn node_reach(context: &DriverContext, node_id: NodeId) -> Reach {
    let Heard { local, nodes } = heard(context);
    heartbeat_reach(local, nodes)(node_id)
}

/// This node's record of the set, if it holds one.
pub async fn read_view(
    context: &DriverContext,
    set_id: Ulid,
) -> Result<Option<DistributionView>, StatusError> {
    drive(ReadViewOperation { set_id }, context).await
}

/// The status of a set this node holds the record of.
pub async fn read_status(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    set_id: Ulid,
) -> Result<Option<DistributionStatus>, StatusError> {
    let read = ReadStatusOperation::new(realm_id, set_id, heard(context));
    drive(read, context.as_ref()).await
}

/// Adds this node's search index progress and, for the ranges it coordinates, what it still
/// owes each member. Both are hints, so a failed read leaves them out.
pub async fn add_progress(context: &Arc<DriverContext>, status: &mut DistributionStatus) {
    let local = context.net_handle.as_ref().map(|net| net.node_id());
    let has_indexes = context.storage_handle.range_indexes().is_some();
    let add = AddProgressOperation::new(status.clone(), local, has_indexes);
    if let Ok(progress) = drive(add, context.as_ref()).await {
        *status = progress;
    }
}
