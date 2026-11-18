//! Authenticates group deletion coordination, signs emptiness confirmations and asks other nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentEvent, AdminDocumentOperation};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::group_delete::{
    GroupDeleteAction, GroupDeletePhase, GroupDeletePlan, GroupDeleteProof, GroupDeletionError,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use ulid::Ulid;

use super::check::CheckNodeOperation;
use super::commit::CommitOperation;
use super::coordinator::{
    DeleteGroupOperation, config_call, coordinator, finished, network_missing, prepare_call,
    read_config,
};
use crate::forward::authorize::{ForwardAuthError, ForwardedCallerOperation};
use crate::forward::fetch::{FanoutOperation, FetchError, PeerAnswer, peer_call, peer_reply};
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};
use crate::realm::peer_trust::is_sync_eligible;

/// Each answer bounds itself in the transport; this only stops a request that never ends.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
/// Peers asked at once by one fan-out, as before the effect group.
const FANOUT_LIMIT: usize = 4;

/// What a node answers to a deletion request.
pub(super) type NodeReply = Result<Option<GroupDeleteProof>, GroupDeletionError>;
pub(super) type NodeReplies = Vec<(NodeId, NodeReply)>;

type ControlTurn = Result<Next<Option<GroupDeleteProof>>, GroupDeletionError>;

#[derive(Debug, PartialEq)]
enum RequestStage {
    Start,
    Caller,
    Run,
}

/// Runs a deletion request a peer sent. Only a start carries a user's authority.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyRequestOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    message: MetadataTransportMessage,
    request_id: Ulid,
    stage: RequestStage,
}

impl ApplyRequestOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
        request_id: Ulid,
    ) -> Self {
        Self {
            local,
            peer,
            message,
            request_id,
            stage: RequestStage::Start,
        }
    }

    fn start(&mut self, auth: AuthContext) -> ControlTurn {
        let MetadataTransportMessage::GroupDeletion {
            auth_token,
            action: GroupDeleteAction::Start { group_id },
        } = &self.message
        else {
            return Err(GroupDeletionError::Invalid("invalid group request".into()));
        };
        self.stage = RequestStage::Run;
        let token = auth_token.clone();
        let request_id = self.request_id;
        let delete = DeleteGroupOperation::new(self.local, auth, token, *group_id, request_id);
        Ok(Next::Call(IoCall::child(delete)))
    }
}

impl Operation for ApplyRequestOperation {
    type Output = Option<GroupDeleteProof>;
    type Error = GroupDeletionError;

    fn first(&mut self) -> ControlTurn {
        let (_, realm_id) = self.local.ok_or_else(network_missing)?;
        let MetadataTransportMessage::GroupDeletion { action, .. } = &self.message else {
            return Err(GroupDeletionError::Invalid("invalid group request".into()));
        };
        if matches!(action, GroupDeleteAction::Start { .. }) {
            self.stage = RequestStage::Caller;
            let caller = ForwardedCallerOperation::new(self.peer, realm_id, &self.message);
            return Ok(Next::Call(IoCall::child(caller)));
        }
        self.stage = RequestStage::Run;
        let control = ApplyControlOperation::new(self.local, self.peer, action.clone());
        Ok(Next::Call(IoCall::child(control)))
    }

    fn next(&mut self, result: IoResult) -> ControlTurn {
        match self.stage {
            RequestStage::Caller => {
                let caller = result.child::<AuthContext, ForwardAuthError>()?;
                self.start(caller.map_err(|error| match error {
                    ForwardAuthError::Unavailable(reason) => {
                        GroupDeletionError::Unavailable(reason)
                    }
                    _ => GroupDeletionError::Unauthorized,
                })?)
            }
            RequestStage::Run => match &self.message {
                MetadataTransportMessage::GroupDeletion {
                    action: GroupDeleteAction::Start { .. },
                    ..
                } => {
                    finished(result)?;
                    Ok(Next::Done(None))
                }
                _ => {
                    let reply =
                        result.child::<Option<GroupDeleteProof>, GroupDeletionError>()??;
                    Ok(Next::Done(reply))
                }
            },
            RequestStage::Start => Err(GroupDeletionError::Invalid(
                "group request was not started".into(),
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ControlStage {
    Config,
    Prepared(Box<GroupDeletePlan>),
    Signed,
    Applied,
    Committed,
}

/// Runs one coordinator step on this node; only an infrastructure peer may send one.
#[derive(Debug, PartialEq)]
pub(super) struct ApplyControlOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    action: Option<GroupDeleteAction>,
    now_ms: u64,
    stage: ControlStage,
}

impl ApplyControlOperation {
    pub(super) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        action: GroupDeleteAction,
    ) -> Self {
        Self {
            local,
            peer,
            action: Some(action),
            now_ms: 0,
            stage: ControlStage::Config,
        }
    }

    fn run(&mut self, config: &RealmConfigDocument) -> ControlTurn {
        let (node_id, realm_id) = self.local.ok_or_else(network_missing)?;
        let peer = self.peer;
        if !is_sync_eligible(config, peer) {
            return Err(GroupDeletionError::Unauthorized);
        }
        let action = self.action.take().ok_or_else(network_missing)?;
        match action {
            GroupDeleteAction::Prepare { plan } => {
                if peer != plan.coordinator
                    || !plan.matches_config(config)
                    || coordinator(config)? != peer
                {
                    return Err(GroupDeletionError::Unauthorized);
                }
                let phase = GroupDeletePhase::Preparing;
                let call = prepare_call(&plan, phase, node_id, self.now_ms);
                self.stage = ControlStage::Prepared(plan);
                Ok(Next::Call(call))
            }
            GroupDeleteAction::Abort { plan } => {
                if peer != plan.coordinator || plan.realm_id != realm_id {
                    return Err(GroupDeletionError::Unauthorized);
                }
                self.stage = ControlStage::Applied;
                let phase = GroupDeletePhase::Cancelled;
                Ok(Next::Call(prepare_call(&plan, phase, node_id, self.now_ms)))
            }
            GroupDeleteAction::Commit { event } => {
                let AdminDocumentOperation::GroupDeleted { certificate } = &event.op else {
                    return Err(GroupDeletionError::Invalid(
                        "missing deletion certificate".into(),
                    ));
                };
                if peer != event.origin_node_id
                    || peer != certificate.plan.coordinator
                    || event.actor.realm_id != realm_id
                {
                    return Err(GroupDeletionError::Unauthorized);
                }
                let commit = CommitOperation::new(
                    (**certificate).clone(),
                    event.actor.clone(),
                    None,
                    Some((*event).clone()),
                );
                self.stage = ControlStage::Committed;
                Ok(Next::Call(IoCall::child(commit)))
            }
            GroupDeleteAction::Check { group_id, caller } => {
                if caller.realm_id != realm_id {
                    return Err(GroupDeletionError::Unauthorized);
                }
                self.stage = ControlStage::Applied;
                let check = CheckNodeOperation::new(config, &caller, group_id, node_id);
                Ok(Next::Call(IoCall::child(check)))
            }
            GroupDeleteAction::Start { .. } => Err(GroupDeletionError::Invalid(
                "start requires caller authorization".into(),
            )),
        }
    }
}

impl Operation for ApplyControlOperation {
    type Output = Option<GroupDeleteProof>;
    type Error = GroupDeletionError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ControlTurn {
        let (_, realm_id) = self.local.ok_or_else(network_missing)?;
        Ok(Next::Call(config_call(realm_id)))
    }

    fn next(&mut self, result: IoResult) -> ControlTurn {
        match std::mem::replace(&mut self.stage, ControlStage::Applied) {
            ControlStage::Config => self.run(&read_config(result)?),
            ControlStage::Prepared(plan) => {
                finished(result)?;
                self.stage = ControlStage::Signed;
                let sign = Effect::Net(NetEffect::SignDeletePlan(plan));
                Ok(Next::Call(IoCall::Effect(Box::new(sign))))
            }
            ControlStage::Signed => match event(result)? {
                Event::Net(NetEvent::DeletePlanSigned(signed)) => Ok(Next::Done(Some(
                    signed.map_err(GroupDeletionError::Unavailable)?,
                ))),
                other => Err(GroupDeletionError::Unavailable(format!(
                    "unexpected signing answer {}",
                    other.label()
                ))),
            },
            ControlStage::Applied => {
                finished(result)?;
                Ok(Next::Done(None))
            }
            ControlStage::Committed => {
                result.child::<AdminDocumentEvent, GroupDeletionError>()??;
                Ok(Next::Done(None))
            }
        }
    }
}

fn deletion_reply(node: NodeId, answer: Result<MetadataTransportMessage, String>) -> NodeReply {
    match answer {
        Ok(MetadataTransportMessage::GroupDeletionResult { result }) => result,
        Ok(_) => Err(GroupDeletionError::Unavailable(format!(
            "node {node} refused the deletion request"
        ))),
        Err(error) => Err(GroupDeletionError::Unavailable(format!(
            "node {node}: {error}"
        ))),
    }
}

/// Sends `action` to one node, which is this node's own control step when it names this node.
#[derive(Debug, PartialEq)]
pub(super) struct RequestNodeOperation {
    local: Option<(NodeId, RealmId)>,
    node: NodeId,
    token: Option<AuthToken>,
    action: Option<GroupDeleteAction>,
    here: bool,
}

impl RequestNodeOperation {
    pub(super) fn new(
        local: Option<(NodeId, RealmId)>,
        node: NodeId,
        token: Option<AuthToken>,
        action: GroupDeleteAction,
    ) -> Self {
        Self {
            local,
            node,
            token,
            action: Some(action),
            here: local.is_some_and(|(local, _)| local == node),
        }
    }
}

impl Operation for RequestNodeOperation {
    type Output = Option<GroupDeleteProof>;
    type Error = GroupDeletionError;

    fn first(&mut self) -> ControlTurn {
        let action = self.action.take().ok_or_else(network_missing)?;
        if self.here {
            let control = ApplyControlOperation::new(self.local, self.node, action);
            return Ok(Next::Call(IoCall::child(control)));
        }
        let message = MetadataTransportMessage::GroupDeletion {
            auth_token: self.token.take(),
            action,
        };
        let frame = encode_message(&message).map_err(GroupDeletionError::Unavailable)?;
        Ok(Next::Call(peer_call(self.node, frame, REQUEST_TIMEOUT)))
    }

    fn next(&mut self, result: IoResult) -> ControlTurn {
        if self.here {
            let reply = result.child::<Option<GroupDeleteProof>, GroupDeletionError>()??;
            return Ok(Next::Done(reply));
        }
        let answer = peer_reply(self.node, result)?.map_err(|error| format!("{error:?}"));
        Ok(Next::Done(deletion_reply(self.node, answer)?))
    }
}

/// Sends `action` to every node: this node runs it itself, all others are asked at once.
#[derive(Debug, PartialEq)]
pub(super) struct RequestNodesOperation {
    local: Option<(NodeId, RealmId)>,
    nodes: Vec<NodeId>,
    action: GroupDeleteAction,
    asked_here: bool,
    replies: NodeReplies,
}

impl RequestNodesOperation {
    pub(super) fn new(
        local: Option<(NodeId, RealmId)>,
        nodes: Vec<NodeId>,
        action: GroupDeleteAction,
    ) -> Self {
        Self {
            local,
            nodes,
            action,
            asked_here: false,
            replies: Vec::new(),
        }
    }

    fn local_node(&self) -> Option<NodeId> {
        self.local.map(|(node_id, _)| node_id)
    }

    fn remote(&self) -> impl Iterator<Item = NodeId> + '_ {
        let local = self.local_node();
        (self.nodes.iter().copied()).filter(move |node| Some(*node) != local)
    }

    fn ask_remote(&mut self) -> Result<Next<NodeReplies>, GroupDeletionError> {
        let requests: Vec<_> = self
            .remote()
            .map(|node| {
                let message = MetadataTransportMessage::GroupDeletion {
                    auth_token: None,
                    action: self.action.clone(),
                };
                (node, message)
            })
            .collect();
        if requests.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.replies)));
        }
        let fanout = FanoutOperation::new(requests, FANOUT_LIMIT, REQUEST_TIMEOUT);
        Ok(Next::Call(IoCall::child(fanout)))
    }
}

impl Operation for RequestNodesOperation {
    type Output = NodeReplies;
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<NodeReplies>, GroupDeletionError> {
        match self.local_node().filter(|local| self.nodes.contains(local)) {
            Some(local) => {
                self.asked_here = true;
                let action = self.action.clone();
                let control = ApplyControlOperation::new(self.local, local, action);
                Ok(Next::Call(IoCall::child(control)))
            }
            None => self.ask_remote(),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeReplies>, GroupDeletionError> {
        if self.asked_here {
            self.asked_here = false;
            let reply = result.child::<Option<GroupDeleteProof>, GroupDeletionError>()?;
            let local = self.local_node().ok_or_else(network_missing)?;
            self.replies.push((local, reply));
            return self.ask_remote();
        }
        match result.child::<Vec<PeerAnswer>, FetchError>()? {
            Ok(answers) => {
                let answers = answers.into_iter();
                let replies = answers.map(|(node, answer)| (node, deletion_reply(node, answer)));
                self.replies.extend(replies);
            }
            Err(error) => {
                let unavailable = |node| {
                    let error = GroupDeletionError::Unavailable(error.to_string());
                    (node, Err(error))
                };
                let replies: Vec<_> = self.remote().map(unavailable).collect();
                self.replies.extend(replies);
            }
        }
        Ok(Next::Done(std::mem::take(&mut self.replies)))
    }
}

#[cfg(test)]
#[path = "peer_tests.rs"]
mod tests;
