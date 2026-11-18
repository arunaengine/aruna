//! Sends a group admin's membership and ownership changes of a distributed bucket to its
//! publisher: join, leave, new copies, splits, transfers, settings and deletion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// A change that needs a coordinator waits while it is unreachable; nothing is queued.

use std::fmt::Debug;
use std::marker::PhantomData;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::bucket_metadata::BucketMetadata;
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::snapshot::RangeFill;
use aruna_core::structs::storage::distribution::split::split_ids;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionError, DistributionMember, DistributionRequest, DistributionRevision,
    DistributionSettings, DistributionSnapshot, DistributionView, MemberState, MetadataCoverage,
    RangeBehaviour, TransferRole, next_snapshot,
};
use ulid::Ulid;

use super::create::{AdmitPendingOperation, CreateError, CreationReport};
use super::forward::{PublishRefusal, config_of, realm_config};
use super::here::{Heard, Here, here};
use super::sharding::{RangeSampleOperation, split_point};
use super::status_view::{Reach, ReadViewOperation, StatusError};
use super::submit::SendAdminOperation;
use crate::driver::DriverContext;
use crate::realm::peer_trust::is_sync_eligible;

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

/// Refuses a change that needs the coordinator of `range_id` while it is unreachable.
fn needs_coordinator(
    heard: &Heard,
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
    match heard.reach(node_id) {
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

/// One configuration change of a set.
#[derive(Clone, Debug, PartialEq)]
enum Change {
    Join(JoinInput),
    Leave(Ulid),
    Transfer(Ulid, Ulid, TransferRole),
    AddCopy(Ulid, Ulid, Vec<RangeSize>),
    Split(Ulid, Option<String>, bool),
    Request(DistributionRequest),
    Delete,
}

/// What a finished change produced; each change's caller takes its part.
#[derive(Debug, Default)]
pub struct Changed {
    revision: Option<DistributionRevision>,
    report: Option<CreationReport>,
    plan: Option<SplitPlan>,
}

/// What a kind of change answers its caller.
pub trait ChangeAnswer: Sized + Send + Debug + PartialEq + 'static {
    fn of(changed: Changed) -> Option<Self>;
}

impl ChangeAnswer for DistributionRevision {
    fn of(changed: Changed) -> Option<Self> {
        changed.revision
    }
}

impl ChangeAnswer for CreationReport {
    fn of(changed: Changed) -> Option<Self> {
        changed.report
    }
}

impl ChangeAnswer for (SplitPlan, Option<DistributionRevision>) {
    fn of(changed: Changed) -> Option<Self> {
        Some((changed.plan?, changed.revision))
    }
}

/// What the pending call of a change answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChangeStep {
    Head,
    Config,
    Sample,
    Send,
    Admit,
}

/// Checks one change against this node's head of the set and sends it to the publisher.
#[derive(Debug, PartialEq)]
pub struct ChangeOperation<T> {
    here: Here,
    auth: AuthContext,
    bearer: Option<AuthToken>,
    set_id: Ulid,
    change: Change,
    step: ChangeStep,
    snapshot: Option<DistributionSnapshot>,
    plan: Option<SplitPlan>,
    answer: PhantomData<T>,
}

type Changing<T> = Result<Next<T>, CreateError>;

fn change<T>(
    context: &DriverContext,
    (auth, bearer): Caller<'_>,
    set_id: Ulid,
    change: Change,
) -> ChangeOperation<T> {
    ChangeOperation {
        here: here(context),
        auth: auth.clone(),
        bearer,
        set_id,
        change,
        step: ChangeStep::Head,
        snapshot: None,
        plan: None,
        answer: PhantomData,
    }
}

/// Adds a member on `input.node_id`, then creates and admits its bucket there under the
/// caller's bearer token. The member catches up from the coordinators before it is ready.
pub fn join_member(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    input: JoinInput,
) -> ChangeOperation<CreationReport> {
    change(context, caller, set_id, Change::Join(input))
}

/// Marks a member leaving; a coordinator transfers its ranges first.
pub fn leave_member(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    member_id: Ulid,
) -> ChangeOperation<DistributionRevision> {
    change(context, caller, set_id, Change::Leave(member_id))
}

/// Opens a fenced transfer of one range to a ready copy replica of it; the former coordinator
/// stays a replica or leaves the range as `role` says.
pub fn transfer_range(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    (range_id, to, role): (Ulid, Ulid, TransferRole),
) -> ChangeOperation<DistributionRevision> {
    change(context, caller, set_id, Change::Transfer(range_id, to, role))
}

/// Adds a copy of one range on a ready `copy` member, then admits the member again under the
/// caller's bearer token: its node opens the range against its capacity and the expected
/// `sizes`. The range's coordinator fills the copy.
pub fn add_copy(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    (range_id, member_id, sizes): (Ulid, Ulid, &[RangeSize]),
) -> ChangeOperation<CreationReport> {
    let add = Change::AddCopy(range_id, member_id, sizes.to_vec());
    change(context, caller, set_id, add)
}

/// Splits one range by hand; `dry_run` only plans the split.
pub fn split_range(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    (range_id, split_key, dry_run): (Ulid, Option<String>, bool),
) -> ChangeOperation<(SplitPlan, Option<DistributionRevision>)> {
    let split = Change::Split(range_id, split_key, dry_run);
    change(context, caller, set_id, split)
}

pub fn change_settings(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    settings: DistributionSettings,
) -> ChangeOperation<DistributionRevision> {
    let request = DistributionRequest::ChangeSettings { settings };
    change(context, caller, set_id, Change::Request(request))
}

/// Replaces the set's bucket metadata, if `expected` is set only while it is still that; the
/// publisher checks group admin of the set's owner.
pub fn change_metadata(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
    (metadata, expected): (Option<BucketMetadata>, Option<BucketMetadata>),
) -> ChangeOperation<DistributionRevision> {
    let request = DistributionRequest::ChangeMetadata {
        metadata: metadata.map(Box::new),
        expected: expected.map(Box::new),
    };
    change(context, caller, set_id, Change::Request(request))
}

/// Prepares the deletion of the set with every admitted member as its identity. It commits
/// once every coordinator fenced its ranges; then every member purges its bucket.
pub fn delete_set(
    context: &DriverContext,
    caller: Caller<'_>,
    set_id: Ulid,
) -> ChangeOperation<DistributionRevision> {
    change(context, caller, set_id, Change::Delete)
}

impl<T: ChangeAnswer> ChangeOperation<T> {
    fn head(&self) -> Result<&DistributionSnapshot, IoError> {
        self.snapshot.as_ref().ok_or(IoError::Unfinished)
    }

    fn send(&mut self, request: DistributionRequest) -> Changing<T> {
        let publisher = self.head()?.root.publisher;
        let send = SendAdminOperation::new(
            (self.here.ids, publisher),
            self.set_id,
            (self.auth.clone(), self.bearer.clone()),
            request,
        );
        self.step = ChangeStep::Send;
        Ok(Next::Call(IoCall::child(send)))
    }

    fn done(&mut self, changed: Changed) -> Changing<T> {
        let answer = T::of(changed).ok_or(IoError::Unfinished)?;
        Ok(Next::Done(answer))
    }

    /// Checks the change against the head; a change that needs more reads asks for them.
    fn decide(&mut self) -> Changing<T> {
        let snapshot = self.head()?;
        let heard = &self.here.heard;
        match self.change.clone() {
            Change::Join(_) => {
                self.step = ChangeStep::Config;
                Ok(Next::Call(realm_config(self.auth.realm_id)))
            }
            Change::Leave(member_id) => {
                let coordinates = snapshot.config.ranges.iter();
                if coordinates.into_iter().any(|r| r.coordinator == member_id) {
                    return Err(DistributionError::CoordinatorMember(member_id).into());
                }
                self.send(DistributionRequest::Leave { member_id })
            }
            Change::Transfer(range_id, to, role) => {
                eligible_target(snapshot, range_id, to)?;
                needs_coordinator(heard, snapshot, range_id, "the transfer")?;
                self.send(DistributionRequest::TransferPrepare {
                    transfer_id: Ulid::generate(),
                    range_id,
                    to,
                    // The publisher raises it above every epoch its head names.
                    epoch: 0,
                    role,
                })
            }
            Change::AddCopy(range_id, member_id, _) => {
                copy_target(snapshot, range_id, member_id)?;
                needs_coordinator(heard, snapshot, range_id, "the new copy")?;
                let fill = RangeFill {
                    fill_id: Ulid::generate(),
                    range_id,
                    member_id,
                    admitted: false,
                };
                self.send(DistributionRequest::AddCopy { fill })
            }
            Change::Split(range_id, Some(key), _) => {
                let range = known_range(snapshot, range_id)?;
                let plan = split_plan(snapshot, range.epoch, range_id, key);
                self.split(plan)
            }
            Change::Split(range_id, None, _) => {
                known_range(snapshot, range_id)?;
                let sample = RangeSampleOperation::new(self.set_id, range_id);
                self.step = ChangeStep::Sample;
                Ok(Next::Call(IoCall::child(sample)))
            }
            Change::Request(request) => self.send(request),
            Change::Delete => {
                let members = (snapshot.config.members.iter())
                    .filter_map(|member| Some((member.member_id, member.incarnation?)))
                    .collect();
                self.send(DistributionRequest::DeletePrepare { members })
            }
        }
    }

    fn join(&mut self, input: JoinInput, eligible: bool) -> Changing<T> {
        if !eligible {
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
        self.send(DistributionRequest::Join {
            member,
            ranges: input.ranges,
        })
    }

    /// The split at the middle of this node's key sample of the range by object count.
    fn sampled(&mut self, range_id: Ulid, keys: Option<Vec<String>>) -> Changing<T> {
        let snapshot = self.head()?;
        let range = known_range(snapshot, range_id)?;
        let point = keys.and_then(|keys| split_point(&keys, range));
        let Some(split_key) = point else {
            let node = snapshot.member(range.coordinator).map(|m| m.node_id);
            let node = node.map(|node| node.to_string()).unwrap_or_default();
            return Err(CreateError::Invalid(format!(
                "this node has no key sample of range {range_id} with keys on both sides of a \
                 middle key; give `split_key`, or ask the range's coordinator node {node}"
            )));
        };
        let plan = split_plan(snapshot, range.epoch, range_id, split_key);
        self.split(plan)
    }

    fn split(&mut self, plan: SplitPlan) -> Changing<T> {
        let snapshot = self.head()?;
        let request = checked_split(snapshot, self.auth.realm_id, &plan)?;
        let range_id = plan.range_id;
        let Change::Split(_, _, dry_run) = self.change else {
            return Err(IoError::Unfinished.into());
        };
        if dry_run {
            return self.done(Changed {
                plan: Some(plan),
                ..Changed::default()
            });
        }
        needs_coordinator(&self.here.heard, snapshot, range_id, "the split")?;
        self.plan = Some(plan);
        self.send(request)
    }

    /// A member change goes on with the member's admission; the others are done.
    fn sent(&mut self, revision: DistributionRevision) -> Changing<T> {
        let admission = match &self.change {
            Change::Join(input) => Some(((self.set_id, None), input.sizes.clone())),
            Change::AddCopy(_, _, sizes) => {
                Some(((self.set_id, Some(revision.clone())), sizes.clone()))
            }
            _ => None,
        };
        let Some((accepted, sizes)) = admission else {
            let plan = self.plan.take();
            return self.done(Changed {
                revision: Some(revision),
                plan,
                ..Changed::default()
            });
        };
        let caller = (self.auth.clone(), self.bearer.clone());
        let admit = AdmitPendingOperation::new(self.here.clone(), caller, accepted, sizes);
        self.step = ChangeStep::Admit;
        Ok(Next::Call(IoCall::child(admit)))
    }
}

impl<T: ChangeAnswer> Operation for ChangeOperation<T> {
    type Output = T;
    type Error = CreateError;

    fn first(&mut self) -> Changing<T> {
        if self.here.ids.is_none() {
            return Err(CreateError::Invalid(
                "this node has no realm transport".to_string(),
            ));
        }
        let read = ReadViewOperation {
            set_id: self.set_id,
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Changing<T> {
        match self.step {
            ChangeStep::Head => {
                let view = result.child::<Option<DistributionView>, StatusError>()??;
                let view = view.ok_or(CreateError::UnknownSet(self.set_id))?;
                if view.conflicted() {
                    return Err(CreateError::Conflicted);
                }
                self.snapshot = Some(view.head.snapshot);
                self.decide()
            }
            ChangeStep::Config => {
                let config = config_of(result)?.ok_or_else(|| {
                    CreateError::Invalid("the realm configuration is not available".into())
                })?;
                let Change::Join(input) = self.change.clone() else {
                    return Err(IoError::Unfinished.into());
                };
                let eligible = is_sync_eligible(&config, input.node_id);
                self.join(input, eligible)
            }
            ChangeStep::Sample => {
                let keys = result.child::<Option<Vec<String>>, IoError>()??;
                let Change::Split(range_id, ..) = self.change else {
                    return Err(IoError::Unfinished.into());
                };
                self.sampled(range_id, keys)
            }
            ChangeStep::Send => {
                let revision = result.child::<DistributionRevision, PublishRefusal>()??;
                self.sent(revision)
            }
            ChangeStep::Admit => {
                let report = result.child::<CreationReport, CreateError>()??;
                self.done(Changed {
                    report: Some(report),
                    ..Changed::default()
                })
            }
        }
    }
}

fn known_range(
    snapshot: &DistributionSnapshot,
    range_id: Ulid,
) -> Result<&aruna_core::structs::storage::distribution::KeyRange, CreateError> {
    (snapshot.config.ranges.iter())
        .find(|range| range.range_id == range_id)
        .ok_or_else(|| CreateError::Invalid(format!("range {range_id} is not part of the set")))
}

fn split_plan(
    snapshot: &DistributionSnapshot,
    epoch: u64,
    range_id: Ulid,
    split_key: String,
) -> SplitPlan {
    let [_, lower_id, upper_id] = split_ids(snapshot.set_id(), range_id, epoch, &split_key);
    SplitPlan {
        range_id,
        split_key,
        lower_id,
        upper_id,
    }
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
    next_snapshot(realm_id, Some(snapshot), &request)
        .map_err(|error| PublishRefusal::Rejected(error.to_string()))?;
    Ok(request)
}

#[cfg(test)]
#[path = "change_tests.rs"]
mod tests;
