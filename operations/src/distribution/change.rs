//! Sends a group admin's membership and ownership changes of a distributed bucket to its
//! publisher: join, leave, new copies, splits, transfers, settings and deletion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A change that needs a coordinator waits while it is unreachable; nothing is queued.

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::bucket_metadata::BucketMetadata;
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::snapshot::RangeFill;
use aruna_core::structs::storage::distribution::split::split_ids;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionError, DistributionMember, DistributionRequest, DistributionRevision,
    DistributionSettings, DistributionSnapshot, MemberState, MetadataCoverage, RangeBehaviour,
    TransferRole, next_snapshot,
};
use ulid::Ulid;

use super::create::{CreateError, CreationReport, admit_pending, send_admin};
use super::sharding::{range_sample, split_point};
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
    /// Expected sizes of the set's ranges, for the member's capacity admission.
    pub sizes: Vec<RangeSize>,
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

/// Refuses a transfer to a member that cannot take the range, naming the members that can.
fn eligible_target(
    snapshot: &DistributionSnapshot,
    range_id: Ulid,
    to: Ulid,
) -> Result<(), CreateError> {
    let range = (snapshot.config.ranges.iter())
        .find(|range| range.range_id == range_id)
        .ok_or_else(|| CreateError::Invalid(format!("range {range_id} is not part of the set")))?;
    let targets = snapshot.transfer_targets(range);
    if targets.iter().any(|target| target.member_id == to) {
        return Ok(());
    }
    let eligible = match targets.is_empty() {
        true => "none; join a member with the copy strategy for this range first".to_string(),
        false => (targets.iter())
            .map(|m| format!("{} (node {})", m.member_id, m.node_id))
            .collect::<Vec<_>>()
            .join(", "),
    };
    Err(CreateError::Invalid(format!(
        "member {to} cannot take range {range_id}: only a ready copy replica of the range can \
         become its coordinator; eligible members: {eligible}"
    )))
}

/// Refuses a new copy on a member that cannot take one, naming the members that can.
fn copy_target(
    snapshot: &DistributionSnapshot,
    range_id: Ulid,
    member_id: Ulid,
) -> Result<(), CreateError> {
    let range = (snapshot.config.ranges.iter())
        .find(|range| range.range_id == range_id)
        .ok_or_else(|| CreateError::Invalid(format!("range {range_id} is not part of the set")))?;
    let candidates: Vec<_> = (snapshot.config.members.iter())
        .filter(|m| m.state == MemberState::Ready && m.strategy == DataStrategy::Copy)
        .filter(|m| m.member_id != range.coordinator && !range.replicas.contains(&m.member_id))
        .collect();
    if candidates.iter().any(|m| m.member_id == member_id) {
        return Ok(());
    }
    let eligible = match candidates.is_empty() {
        true => "none; add a member with the copy strategy first".to_string(),
        false => (candidates.iter())
            .map(|m| format!("{} (node {})", m.member_id, m.node_id))
            .collect::<Vec<_>>()
            .join(", "),
    };
    Err(CreateError::Invalid(format!(
        "member {member_id} cannot take a copy of range {range_id}: only a ready member with \
         the copy strategy that keeps no copy of the range yet can; eligible members: {eligible}"
    )))
}

/// A split of one range at a key, as the publisher applies it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitPlan {
    pub range_id: Ulid,
    pub split_key: String,
    pub lower_id: Ulid,
    pub upper_id: Ulid,
}

/// The split of `range_id` at `split_key`, or at the middle of this node's key sample of the
/// range by object count when no key is given.
pub async fn plan_split(
    context: &DriverContext,
    snapshot: &DistributionSnapshot,
    range_id: Ulid,
    split_key: Option<String>,
) -> Result<SplitPlan, CreateError> {
    let range = (snapshot.config.ranges.iter())
        .find(|range| range.range_id == range_id)
        .ok_or_else(|| CreateError::Invalid(format!("range {range_id} is not part of the set")))?;
    let split_key = match split_key {
        Some(key) => key,
        None => {
            let sample = range_sample(context, snapshot.set_id(), range_id).await;
            let node = snapshot.member(range.coordinator).map(|m| m.node_id);
            (sample.and_then(|keys| split_point(&keys, range))).ok_or_else(|| {
                let node = node.map(|node| node.to_string()).unwrap_or_default();
                CreateError::Invalid(format!(
                    "this node has no key sample of range {range_id} with keys on both sides of \
                     a middle key; give `split_key`, or ask the range's coordinator node {node}"
                ))
            })?
        }
    };
    let [_, lower_id, upper_id] = split_ids(snapshot.set_id(), range_id, range.epoch, &split_key);
    Ok(SplitPlan {
        range_id,
        split_key,
        lower_id,
        upper_id,
    })
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
    admit_pending(context, caller.0, caller.1, set_id, &input.sizes).await
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
    eligible_target(&snapshot, range_id, to)?;
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

/// Adds a copy of one range on a ready `copy` member, then admits the member again under the
/// caller's bearer token: its node opens the range against its capacity and the expected
/// `sizes`. The range's coordinator fills the copy.
pub async fn add_copy(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    (range_id, member_id, sizes): (Ulid, Ulid, &[RangeSize]),
) -> Result<CreationReport, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    copy_target(&snapshot, range_id, member_id)?;
    needs_coordinator(context, &snapshot, range_id, "the new copy")?;
    let fill = RangeFill {
        fill_id: Ulid::generate(),
        range_id,
        member_id,
        admitted: false,
    };
    let request = DistributionRequest::AddCopy { fill };
    send(context, (local, &snapshot), caller.clone(), request).await?;
    admit_pending(context, caller.0, caller.1, set_id, sizes).await
}

/// Splits one range by hand; `dry_run` only plans the split.
pub async fn split_range(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    (range_id, split_key, dry_run): (Ulid, Option<String>, bool),
) -> Result<(SplitPlan, Option<DistributionRevision>), CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    let plan = plan_split(context, &snapshot, range_id, split_key).await?;
    let request = checked_split(&snapshot, caller.0.realm_id, &plan)?;
    if dry_run {
        return Ok((plan, None));
    }
    needs_coordinator(context, &snapshot, range_id, "the split")?;
    let revision = send(context, (local, &snapshot), caller, request).await?;
    Ok((plan, Some(revision)))
}

/// The split request of `plan`, refused unless the head accepts it as the publisher would.
fn checked_split(
    snapshot: &DistributionSnapshot,
    realm_id: RealmId,
    plan: &SplitPlan,
) -> Result<DistributionRequest, CreateError> {
    let epoch = (snapshot.config.ranges.iter())
        .find(|range| range.range_id == plan.range_id)
        .map(|range| range.epoch)
        .unwrap_or_default();
    let request = DistributionRequest::Split {
        range_id: plan.range_id,
        epoch,
        split_key: plan.split_key.clone(),
    };
    next_snapshot(realm_id, Some(snapshot), &request)?;
    Ok(request)
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

/// Replaces the set's bucket metadata, if `expected` is set only while it is still that; the
/// publisher checks group admin of the set's owner.
pub async fn change_metadata(
    context: &Arc<DriverContext>,
    caller: Caller<'_>,
    set_id: Ulid,
    (metadata, expected): (Option<BucketMetadata>, Option<BucketMetadata>),
) -> Result<DistributionRevision, CreateError> {
    let (local, snapshot) = head(context, set_id).await?;
    let request = DistributionRequest::ChangeMetadata {
        metadata: metadata.map(Box::new),
        expected: expected.map(Box::new),
    };
    send(context, (local, &snapshot), caller, request).await
}

/// The bucket metadata of a set this node holds the record of.
pub async fn set_metadata(
    context: &DriverContext,
    set_id: Ulid,
) -> Result<Option<BucketMetadata>, String> {
    let view = read_view(context, set_id)
        .await
        .map_err(|error| error.to_string())?;
    let metadata = view.and_then(|view| view.snapshot().config.settings.metadata.clone());
    Ok(metadata.map(|metadata| *metadata))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{member, realm, record, replicated};

    #[test]
    fn refusal_names_eligible() {
        // Member 2 keeps a copy of member 1's range; member 1 itself cannot take it.
        let (snapshot, _) = replicated(true, 1);
        let range = &snapshot.config.ranges[0];
        let (range_id, coordinator) = (range.range_id, range.coordinator);
        let other = if coordinator == member(1) {
            member(2)
        } else {
            member(1)
        };
        assert!(eligible_target(&snapshot, range_id, other).is_ok());
        let Err(CreateError::Invalid(message)) = eligible_target(&snapshot, range_id, coordinator)
        else {
            panic!("a transfer to the coordinator itself is refused");
        };
        assert!(message.contains(&other.to_string()), "{message}");
        let (bare, _) = replicated(true, 0);
        let Err(CreateError::Invalid(message)) =
            eligible_target(&bare, bare.config.ranges[0].range_id, other)
        else {
            panic!("a range without copies has no eligible member");
        };
        assert!(message.contains("none"), "{message}");
    }

    #[test]
    fn split_checked_early() {
        // A dry run refuses what the publisher would refuse: a key outside the range.
        let (snapshot, _) = record(true);
        let lower = &snapshot.config.ranges[0];
        let plan = |key: &str| {
            let [_, lower_id, upper_id] = split_ids(snapshot.set_id(), lower.range_id, 0, key);
            SplitPlan {
                range_id: lower.range_id,
                split_key: key.to_string(),
                lower_id,
                upper_id,
            }
        };
        assert!(checked_split(&snapshot, realm(), &plan("f")).is_ok());
        let refused = checked_split(&snapshot, realm(), &plan("x"));
        assert!(
            matches!(refused, Err(CreateError::Distribution(_))),
            "{refused:?}"
        );
        let (pending, _) = record(false);
        let refused = checked_split(&pending, realm(), &plan("f"));
        assert!(
            matches!(refused, Err(CreateError::Distribution(_))),
            "{refused:?}"
        );
    }
}
