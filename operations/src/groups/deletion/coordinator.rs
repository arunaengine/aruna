//! Runs a resumable group deletion decision across the current realm members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::metadata::AuthToken;
use aruna_core::onboarding::OnboardingTicket;
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::{
    GroupDeleteAction, GroupDeleteCertificate, GroupDeletePhase, GroupDeletePlan, GroupDeleteProof,
    GroupDeleteRecord, GroupDeletionError,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, admin_documents::AdminDocumentEvent};
use ulid::Ulid;

use super::access::{AuthorizeDeleteOperation, BlockedOperation, LoadGroupOperation, LoadedGroup};
use super::commit::CommitOperation;
use super::onboarding::InstallOperation;
use super::peer::{NodeReplies, NodeReply, RequestNodeOperation, RequestNodesOperation};
use super::prepare::PrepareOperation;
use crate::document_repository::{GetConfigError, GetConfigOperation};

pub(super) fn coordinator(config: &RealmConfigDocument) -> Result<NodeId, GroupDeletionError> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .map(|node| node.node_id.parse::<NodeId>())
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?
        .first()
        .copied()
        .ok_or_else(|| GroupDeletionError::Unavailable("realm has no group coordinator".into()))
}

pub(super) fn member_nodes(
    config: &RealmConfigDocument,
) -> Result<BTreeSet<NodeId>, GroupDeletionError> {
    (config.nodes.iter())
        .map(|node| node.node_id.parse::<NodeId>())
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))
}

pub(super) fn network_missing() -> GroupDeletionError {
    GroupDeletionError::Unavailable("network handle is missing".into())
}

pub(super) fn clock_time(now_ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(now_ms)
}

pub(super) fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

pub(super) fn read_config(result: IoResult) -> Result<RealmConfigDocument, GroupDeletionError> {
    result
        .child::<RealmConfigDocument, GetConfigError>()?
        .map_err(|_| GroupDeletionError::Unavailable("realm configuration is missing".into()))
}

/// The outcome of a child that returns nothing.
pub(super) fn finished(result: IoResult) -> Result<(), GroupDeletionError> {
    result.child::<(), GroupDeletionError>()?
}

pub(super) fn prepare_call(
    plan: &GroupDeletePlan,
    phase: GroupDeletePhase,
    node_id: NodeId,
    now_ms: u64,
) -> IoCall {
    let time = clock_time(now_ms);
    IoCall::child(PrepareOperation::new(plan.clone(), phase, node_id, time))
}

fn is_member(config: &RealmConfigDocument, node: NodeId) -> bool {
    let node = node.to_string();
    config.nodes.iter().any(|member| member.node_id == node)
}

/// Makes the deletion decision durable before anything acts on it.
#[derive(Debug, PartialEq)]
pub struct SyncDecisionOperation;

impl Operation for SyncDecisionOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        Ok(Next::Call(IoCall::Sync))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum DeleteStage {
    Config,
    Load,
    Authorize,
    Forward,
    Publish,
    Prepare,
    Requests,
    Blocked,
    Recheck,
    Commit,
    /// The plan is being cancelled; this error answers the caller afterwards.
    Cancel(GroupDeletionError),
    /// As `Cancel`, for a refusal that still has to be worded for the caller.
    CancelBlocked(NodeId, GroupDeletionError),
    Refuse,
}

type DeleteTurn = Result<Next<()>, GroupDeletionError>;

/// Deletes an empty group: all nodes confirm the plan, then the coordinator commits it.
#[derive(Debug, PartialEq)]
pub struct DeleteGroupOperation {
    local: Option<(NodeId, RealmId)>,
    auth: AuthContext,
    token: Option<AuthToken>,
    group_id: GroupId,
    request_id: Ulid,
    now_ms: u64,
    stage: DeleteStage,
    config: Option<RealmConfigDocument>,
    group: Option<Group>,
    record: Option<GroupDeleteRecord>,
    plan: Option<GroupDeletePlan>,
    /// Reversed, so the next reply pops off the end.
    replies: NodeReplies,
    proofs: Vec<GroupDeleteProof>,
    failure: Option<GroupDeletionError>,
}

impl DeleteGroupOperation {
    /// `request_id` names the plan when this call starts a new one.
    pub fn new(
        local: Option<(NodeId, RealmId)>,
        auth: AuthContext,
        token: Option<AuthToken>,
        group_id: GroupId,
        request_id: Ulid,
    ) -> Self {
        Self {
            local,
            auth,
            token,
            group_id,
            request_id,
            now_ms: 0,
            stage: DeleteStage::Config,
            config: None,
            group: None,
            record: None,
            plan: None,
            replies: Vec::new(),
            proofs: Vec::new(),
            failure: None,
        }
    }

    fn node_id(&self) -> Result<NodeId, GroupDeletionError> {
        Ok(self.local.ok_or_else(network_missing)?.0)
    }

    fn current_plan(&self) -> Result<&GroupDeletePlan, GroupDeletionError> {
        (self.plan.as_ref()).ok_or_else(|| GroupDeletionError::Unavailable("no plan".into()))
    }

    fn cancel(&mut self, stage: DeleteStage) -> DeleteTurn {
        let plan = self.current_plan()?.clone();
        self.stage = stage;
        let cancel = CancelPlanOperation::new(self.local, plan);
        Ok(Next::Call(IoCall::child(cancel)))
    }

    fn publish(&mut self, plan: GroupDeletePlan, event: AdminDocumentEvent) -> DeleteTurn {
        self.stage = DeleteStage::Publish;
        let publish = PublishDecisionOperation::new(self.local, plan, event);
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn route(&mut self) -> DeleteTurn {
        let node_id = self.node_id()?;
        let config = self.config.take().ok_or_else(network_missing)?;
        let record = self.record.take();
        let active = (record.as_ref()).filter(|record| record.phase != GroupDeletePhase::Cancelled);
        let target = match active {
            Some(record) => record.plan.coordinator,
            None => coordinator(&config)?,
        };
        if target != node_id {
            self.stage = DeleteStage::Forward;
            let start = GroupDeleteAction::Start {
                group_id: self.group_id,
            };
            let token = self.token.take();
            let forward = RequestNodeOperation::new(self.local, target, token, start);
            return Ok(Next::Call(IoCall::child(forward)));
        }
        if let Some(record) = (record.as_ref()).filter(|r| r.phase == GroupDeletePhase::Deleted) {
            let event = record.event.as_deref().ok_or_else(|| {
                GroupDeletionError::Unavailable("committed deletion event is missing".into())
            })?;
            return self.publish(record.plan.clone(), event.clone());
        }
        let group = self.group.take().ok_or(GroupDeletionError::NotFound)?;
        if group.realm_id != self.auth.realm_id || group.group_id != self.group_id {
            return Err(GroupDeletionError::Unauthorized);
        }
        let nodes = member_nodes(&config)?;
        let plan = active
            .map(|record| record.plan.clone())
            .unwrap_or_else(|| GroupDeletePlan {
                request_id: self.request_id,
                group_id: self.group_id,
                realm_id: self.auth.realm_id,
                owner: group.owner,
                requested_by: self.auth.user_id,
                coordinator: node_id,
                nodes,
            });
        let aborting = record.is_some_and(|record| record.phase == GroupDeletePhase::Aborting);
        let stale = !plan.matches_config(&config) || plan.requested_by != self.auth.user_id;
        self.plan = Some(plan);
        if aborting || stale {
            let retry =
                GroupDeletionError::Conflict("previous attempt cancelled; retry deletion".into());
            return self.cancel(DeleteStage::Cancel(retry));
        }
        self.stage = DeleteStage::Prepare;
        let phase = GroupDeletePhase::Preparing;
        let call = prepare_call(self.current_plan()?, phase, node_id, self.now_ms);
        Ok(Next::Call(call))
    }

    fn request_all(&mut self) -> DeleteTurn {
        let plan = self.current_plan()?.clone();
        let nodes = plan.nodes.iter().copied().collect();
        let prepare = GroupDeleteAction::Prepare {
            plan: Box::new(plan),
        };
        self.stage = DeleteStage::Requests;
        let ask = RequestNodesOperation::new(self.local, nodes, prepare);
        Ok(Next::Call(IoCall::child(ask)))
    }

    fn blocked_call(&self, node: NodeId, error: GroupDeletionError) -> IoCall {
        let blocked = BlockedOperation::new(&self.auth, self.group_id, node, error);
        IoCall::child(blocked)
    }

    fn take_reply(&mut self, node: NodeId, reply: NodeReply) -> Option<GroupDeletionError> {
        match reply {
            Ok(Some(proof)) if proof.node_id == node => self.proofs.push(proof),
            Ok(_) => {
                self.failure = Some(GroupDeletionError::Unavailable(format!(
                    "node {node} did not confirm emptiness"
                )));
            }
            Err(error) => return Some(error),
        }
        None
    }

    /// Reads the replies in turn; the last failure is the one the caller sees.
    fn walk_replies(&mut self) -> DeleteTurn {
        while let Some((node, reply)) = self.replies.pop() {
            if let Some(error) = self.take_reply(node, reply) {
                self.stage = DeleteStage::Blocked;
                return Ok(Next::Call(self.blocked_call(node, error)));
            }
        }
        if let Some(error) = self.failure.take() {
            return self.cancel(DeleteStage::Cancel(error));
        }
        self.stage = DeleteStage::Recheck;
        let check = AuthorizeDeleteOperation::new(&self.auth, self.group_id, None, true);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn commit(&mut self) -> DeleteTurn {
        let node_id = self.node_id()?;
        let certificate = GroupDeleteCertificate {
            plan: self.current_plan()?.clone(),
            proofs: std::mem::take(&mut self.proofs),
        };
        if !certificate.verify() {
            let invalid =
                GroupDeletionError::Unavailable("a node supplied an invalid confirmation".into());
            return self.cancel(DeleteStage::Cancel(invalid));
        }
        let actor = Actor {
            node_id,
            user_id: self.auth.user_id,
            realm_id: self.auth.realm_id,
        };
        self.stage = DeleteStage::Commit;
        let auth = Some(self.auth.clone());
        let commit = CommitOperation::new(certificate, actor, auth, None);
        Ok(Next::Call(IoCall::child(commit)))
    }
}

impl Operation for DeleteGroupOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> DeleteTurn {
        let (_, realm_id) = self.local.ok_or_else(network_missing)?;
        if self.auth.realm_id != realm_id {
            return Err(GroupDeletionError::Unauthorized);
        }
        Ok(Next::Call(config_call(realm_id)))
    }

    fn next(&mut self, result: IoResult) -> DeleteTurn {
        match std::mem::replace(&mut self.stage, DeleteStage::Refuse) {
            DeleteStage::Config => {
                self.config = Some(read_config(result)?);
                self.stage = DeleteStage::Load;
                let load = LoadGroupOperation::new(self.group_id);
                Ok(Next::Call(IoCall::child(load)))
            }
            DeleteStage::Load => {
                let (group, record) = result.child::<LoadedGroup, GroupDeletionError>()??;
                self.stage = DeleteStage::Authorize;
                let check = AuthorizeDeleteOperation::new(
                    &self.auth,
                    self.group_id,
                    record.as_ref(),
                    group.is_some(),
                );
                self.group = group;
                self.record = record;
                Ok(Next::Call(IoCall::child(check)))
            }
            DeleteStage::Authorize => {
                finished(result)?;
                self.route()
            }
            DeleteStage::Forward => {
                result.child::<Option<GroupDeleteProof>, GroupDeletionError>()??;
                Ok(Next::Done(()))
            }
            DeleteStage::Publish => {
                finished(result)?;
                Ok(Next::Done(()))
            }
            DeleteStage::Prepare => match finished(result) {
                Ok(()) => self.request_all(),
                Err(error) => {
                    let node_id = self.node_id()?;
                    self.cancel(DeleteStage::CancelBlocked(node_id, error))
                }
            },
            DeleteStage::Requests => {
                let mut replies = result.child::<NodeReplies, GroupDeletionError>()??;
                replies.reverse();
                self.replies = replies;
                self.walk_replies()
            }
            DeleteStage::Blocked => {
                let worded = result.child::<GroupDeletionError, GroupDeletionError>()??;
                self.failure = Some(worded);
                self.walk_replies()
            }
            DeleteStage::Recheck => match finished(result) {
                Ok(()) => self.commit(),
                Err(error) => self.cancel(DeleteStage::Cancel(error)),
            },
            DeleteStage::Commit => {
                match result.child::<AdminDocumentEvent, GroupDeletionError>()? {
                    Ok(event) => {
                        let plan = self.current_plan()?.clone();
                        self.publish(plan, event)
                    }
                    Err(
                        error @ (GroupDeletionError::Unauthorized
                        | GroupDeletionError::NotFound
                        | GroupDeletionError::Invalid(_)
                        | GroupDeletionError::Conflict(_)),
                    ) => self.cancel(DeleteStage::Cancel(error)),
                    Err(error) => Err(error),
                }
            }
            DeleteStage::Cancel(error) => {
                finished(result)?;
                Err(error)
            }
            DeleteStage::CancelBlocked(node, error) => {
                finished(result)?;
                self.stage = DeleteStage::Refuse;
                Ok(Next::Call(self.blocked_call(node, error)))
            }
            DeleteStage::Refuse => Err(result.child::<GroupDeletionError, GroupDeletionError>()??),
        }
    }
}

#[derive(Debug, PartialEq)]
enum CancelStage {
    Abort,
    Config,
    Notify,
    Cancelled,
}

/// Aborts a plan here, tells the other members, and records it as cancelled.
#[derive(Debug, PartialEq)]
struct CancelPlanOperation {
    local: Option<(NodeId, RealmId)>,
    plan: GroupDeletePlan,
    now_ms: u64,
    stage: CancelStage,
    /// Reversed, so the next member pops off the end.
    members: Vec<NodeId>,
    failure: Option<GroupDeletionError>,
}

impl CancelPlanOperation {
    fn new(local: Option<(NodeId, RealmId)>, plan: GroupDeletePlan) -> Self {
        Self {
            local,
            plan,
            now_ms: 0,
            stage: CancelStage::Abort,
            members: Vec::new(),
            failure: None,
        }
    }

    fn node_id(&self) -> Result<NodeId, GroupDeletionError> {
        Ok(self.local.ok_or_else(network_missing)?.0)
    }

    fn notify_next(&mut self) -> Result<Next<()>, GroupDeletionError> {
        if let Some(node) = self.members.pop() {
            self.stage = CancelStage::Notify;
            let abort = GroupDeleteAction::Abort {
                plan: Box::new(self.plan.clone()),
            };
            let notify = RequestNodeOperation::new(self.local, node, None, abort);
            return Ok(Next::Call(IoCall::child(notify)));
        }
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        self.stage = CancelStage::Cancelled;
        let phase = GroupDeletePhase::Cancelled;
        let call = prepare_call(&self.plan, phase, self.node_id()?, self.now_ms);
        Ok(Next::Call(call))
    }
}

impl Operation for CancelPlanOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let phase = GroupDeletePhase::Aborting;
        let call = prepare_call(&self.plan, phase, self.node_id()?, self.now_ms);
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        match self.stage {
            CancelStage::Abort => {
                finished(result)?;
                self.stage = CancelStage::Config;
                Ok(Next::Call(config_call(self.plan.realm_id)))
            }
            CancelStage::Config => {
                let config = read_config(result)?;
                let node_id = self.node_id()?;
                self.members = (self.plan.nodes.iter().rev().copied())
                    .filter(|node| *node != node_id && is_member(&config, *node))
                    .collect();
                self.notify_next()
            }
            CancelStage::Notify => {
                let reply = result.child::<Option<GroupDeleteProof>, GroupDeletionError>()?;
                if let Err(error) = reply {
                    self.failure = Some(error);
                }
                self.notify_next()
            }
            CancelStage::Cancelled => {
                finished(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PublishStage {
    Sync,
    Config,
    Notify,
}

/// Makes the decision durable, then hands it to every other member.
#[derive(Debug, PartialEq)]
struct PublishDecisionOperation {
    local: Option<(NodeId, RealmId)>,
    plan: GroupDeletePlan,
    event: AdminDocumentEvent,
    stage: PublishStage,
    /// Reversed, so the next member pops off the end.
    members: Vec<NodeId>,
}

impl PublishDecisionOperation {
    fn new(
        local: Option<(NodeId, RealmId)>,
        plan: GroupDeletePlan,
        event: AdminDocumentEvent,
    ) -> Self {
        Self {
            local,
            plan,
            event,
            stage: PublishStage::Sync,
            members: Vec::new(),
        }
    }

    fn notify_next(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let Some(node) = self.members.pop() else {
            return Ok(Next::Done(()));
        };
        self.stage = PublishStage::Notify;
        let commit = GroupDeleteAction::Commit {
            event: Box::new(self.event.clone()),
        };
        let notify = RequestNodeOperation::new(self.local, node, None, commit);
        Ok(Next::Call(IoCall::child(notify)))
    }
}

impl Operation for PublishDecisionOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        Ok(Next::Call(IoCall::Sync))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        match self.stage {
            PublishStage::Sync => {
                done(result)?;
                self.local.ok_or_else(network_missing)?;
                self.stage = PublishStage::Config;
                Ok(Next::Call(config_call(self.plan.realm_id)))
            }
            PublishStage::Config => {
                let config = read_config(result)?;
                let (node_id, _) = self.local.ok_or_else(network_missing)?;
                self.members = (self.plan.nodes.iter().rev().copied())
                    .filter(|node| *node != node_id && is_member(&config, *node))
                    .collect();
                self.notify_next()
            }
            PublishStage::Notify => {
                result.child::<Option<GroupDeleteProof>, GroupDeletionError>()??;
                self.notify_next()
            }
        }
    }
}

/// Installs the deletion records an onboarding ticket carries, durably.
#[derive(Debug, PartialEq)]
pub struct InstallOnboardingOperation {
    /// Reversed, so the next record pops off the end.
    records: Result<Vec<GroupDeleteRecord>, GroupDeletionError>,
    syncing: bool,
}

impl InstallOnboardingOperation {
    pub fn new(ticket: &OnboardingTicket, realm_id: RealmId, node_id: NodeId, now: u64) -> Self {
        Self {
            records: ticket_records(ticket, realm_id, node_id, now),
            syncing: false,
        }
    }

    fn install_next(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let records = self.records.as_mut().map_err(|error| error.clone())?;
        match records.pop() {
            Some(record) => Ok(Next::Call(IoCall::child(InstallOperation::new(record)))),
            None => {
                self.syncing = true;
                Ok(Next::Call(IoCall::Sync))
            }
        }
    }
}

fn ticket_records(
    ticket: &OnboardingTicket,
    realm_id: RealmId,
    node_id: NodeId,
    now: u64,
) -> Result<Vec<GroupDeleteRecord>, GroupDeletionError> {
    if ticket.payload.realm_id != realm_id.to_string() {
        return Err(GroupDeletionError::Invalid(
            "onboarding realm mismatch".into(),
        ));
    }
    let target = DocumentTarget::RealmAuthorization { realm_id };
    ticket
        .verify(node_id, &target, now)
        .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?;
    let records = &ticket.payload.group_deletions;
    if records
        .iter()
        .any(|record| record.plan.realm_id != realm_id)
    {
        return Err(GroupDeletionError::Invalid(
            "deletion realm mismatch".into(),
        ));
    }
    Ok(records.iter().rev().cloned().collect())
}

impl Operation for InstallOnboardingOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        self.install_next()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        if self.syncing {
            done(result)?;
            return Ok(Next::Done(()));
        }
        finished(result)?;
        self.install_next()
    }
}

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;
