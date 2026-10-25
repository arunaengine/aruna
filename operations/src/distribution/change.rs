//! Sends a group admin's membership and ownership changes of a distributed bucket to its
//! publisher: join, leave, transfer, settings and deletion. A change that needs a coordinator
//! waits while that coordinator is unreachable; nothing is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionError, DistributionMember, DistributionRequest, DistributionRevision,
    DistributionSettings, DistributionSnapshot, MemberState, MetadataCoverage, RangeBehaviour,
    TransferRole,
};
use ulid::Ulid;

use super::create::{CreateError, CreationReport, admit_pending, send_admin};
use super::status::{Reach, node_reach, read_view};
use crate::driver::DriverContext;
use crate::forward::authorize::is_sync_eligible;
use crate::metadata::protocol::AuthToken;
use crate::placement::process_placements::load_realm_config;

/// The caller of an admin change: its own token context and, for a remote publisher, the
/// bearer token forwarded there.
pub type Caller<'a> = (&'a AuthContext, Option<AuthToken>);

/// A member to add to a set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinInput {
    pub node_id: NodeId,
    pub coverage: MetadataCoverage,
    pub strategy: DataStrategy,
    pub range_behaviour: Option<RangeBehaviour>,
    /// Ranges the member keeps a copy of; `all` coverage members may name none.
    pub ranges: Vec<Ulid>,
}

/// This node, and the set's head while configuration changes may proceed.
async fn head(
    context: &Arc<DriverContext>,
    set_id: Ulid,
) -> Result<(NodeId, DistributionSnapshot), CreateError> {
    let local = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or_else(|| CreateError::Invalid("this node has no realm transport".to_string()))?;
    let view = read_view(context, set_id)
        .await?
        .ok_or(CreateError::UnknownSet(set_id))?;
    if view.conflicted() {
        return Err(CreateError::Conflicted);
    }
    Ok((local, view.head.snapshot))
}

async fn send(
    context: &Arc<DriverContext>,
    (local, snapshot): (NodeId, &DistributionSnapshot),
    caller: Caller<'_>,
    request: DistributionRequest,
) -> Result<DistributionRevision, CreateError> {
    let nodes = (snapshot.root.publisher, local);
    Ok(send_admin(context, nodes, snapshot.set_id(), caller, request).await?)
}

/// Refuses a change that needs the coordinator of `range_id` while it is unreachable.
fn needs_coordinator(
    context: &DriverContext,
    snapshot: &DistributionSnapshot,
    range_id: Ulid,
    change: &str,
) -> Result<(), CreateError> {
    let range = (snapshot.config.ranges.iter())
        .find(|range| range.range_id == range_id)
        .ok_or_else(|| CreateError::Invalid(format!("range {range_id} is not part of the set")))?;
    let Some(node_id) = snapshot.member(range.coordinator).map(|m| m.node_id) else {
        return Ok(());
    };
    match node_reach(context, node_id) {
        Reach::Reachable => Ok(()),
        reach => Err(CreateError::Blocked(format!(
            "range {range_id} of distributed bucket {} is degraded: its coordinator node \
             {node_id} is {}; {change} waits until it is reachable again",
            snapshot.set_id(),
            reach.hint()
        ))),
    }
}

/// Adds a member on `input.node_id`, then creates and admits its bucket there under the
/// caller's bearer token. The member catches up from the coordinators before it is ready.
pub async fn join_member(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    input: JoinInput,
) -> Result<CreationReport, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    let realm_id = caller.0.realm_id;
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or_else(|| CreateError::Invalid("the realm configuration is not available".into()))?;
    if !is_sync_eligible(&config, input.node_id) {
        return Err(CreateError::Invalid(format!(
            "node {} is not a realm infrastructure node and cannot hold a member",
            input.node_id
        )));
    }
    let member = DistributionMember {
        member_id: Ulid::generate(),
        node_id: input.node_id,
        incarnation: None,
        state: MemberState::Created,
        coverage: input.coverage,
        strategy: input.strategy,
        range_behaviour: input.range_behaviour,
        intent: [0; 32],
    };
    let request = DistributionRequest::Join {
        member,
        ranges: input.ranges,
    };
    send(context, (local, &snapshot), caller.clone(), request).await?;
    admit_pending(context, caller.0, caller.1, set_id).await
}

/// Marks a member leaving; a coordinator transfers its ranges first.
pub async fn leave_member(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    member_id: Ulid,
) -> Result<DistributionRevision, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    if snapshot
        .config
        .ranges
        .iter()
        .any(|range| range.coordinator == member_id)
    {
        return Err(DistributionError::CoordinatorMember(member_id).into());
    }
    let request = DistributionRequest::Leave { member_id };
    send(context, (local, &snapshot), caller, request).await
}

/// Opens a fenced transfer of one range to a ready copy replica of it; the former coordinator
/// stays a replica or leaves the range as `role` says.
pub async fn transfer_range(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    (range_id, to, role): (Ulid, Ulid, TransferRole),
) -> Result<DistributionRevision, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    needs_coordinator(context, &snapshot, range_id, "the transfer")?;
    let request = DistributionRequest::TransferPrepare {
        transfer_id: Ulid::generate(),
        range_id,
        to,
        // The publisher raises it above every epoch its head names.
        epoch: 0,
        role,
    };
    send(context, (local, &snapshot), caller, request).await
}

pub async fn change_settings(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    settings: DistributionSettings,
) -> Result<DistributionRevision, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    let request = DistributionRequest::ChangeSettings { settings };
    send(context, (local, &snapshot), caller, request).await
}

/// Prepares the deletion of the set with every admitted member as its identity. It commits
/// once every coordinator fenced its ranges; then every member purges its bucket.
pub async fn delete_set(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
) -> Result<DistributionRevision, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    let members = (snapshot.config.members.iter())
        .filter_map(|member| Some((member.member_id, member.incarnation?)))
        .collect();
    let request = DistributionRequest::DeletePrepare { members };
    send(context, (local, &snapshot), caller, request).await
}
