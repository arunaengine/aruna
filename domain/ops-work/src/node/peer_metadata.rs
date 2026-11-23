//! Answers one request of an inbound metadata stream: picks the operation for its message kind
//! and turns that operation's outcome into the reply this node sends back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::offer::BackendLimits;
use aruna_core::heartbeat::{HeldHeartbeat, Here};
use aruna_core::id::NodeId;
use aruna_core::jobs::{ClosureAnswer, LaunchClosureAnswer, SubmissionAck, SubmissionRefusal};
use aruna_core::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::input::{CapturedInput, InputSelection};
use aruna_core::structs::execution::job::pages::InputPage;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;
use ulid::Ulid;

use crate::jobs::lifecycle::admit::forwarded::AdmitForwardedOperation;
use crate::jobs::lifecycle::closure::{
    AnswerClosureOperation, ClosureError, ServeClosureOperation,
};
use crate::jobs::lifecycle::serve_inputs::ServeInputsOperation;
use crate::jobs::node_inputs::{ingress_node, launch_node};
use crate::jobs::records::pages::{PageReadError, ServePageOperation};
use crate::jobs::records::transport::{ServeLaunchOperation, ServeRecordOperation};
use crate::plugins::peer::{AnswerPeer, PeerAnswer as PluginAnswer};
use aruna_ops_data::metadata::serve::forwards::needs_view;
use aruna_ops_data::metadata::serve::{ServeNode, ServeRequestOperation, ServedReply};

/// What this node offers a metadata request, read by the shell before the run; the costly parts
/// only when [`metadata_needs`] asks for them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataNode {
    /// Whether the graph store runs here.
    pub graph: bool,
    /// Whether an object search may use the range indexes.
    pub indexed: bool,
    /// The largest metadata document this node exports.
    pub metadata_bytes: u64,
    pub here: Option<Here>,
    pub routing: Option<NodeRouting>,
    pub limits: BackendLimits,
    pub heartbeats: BTreeMap<NodeId, HeldHeartbeat>,
    /// The execution id a launch offer starts with, drawn by the shell.
    pub execution_id: Ulid,
}

/// The costly parts of [`MetadataNode`] a request reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataNeeds {
    /// This node's view of the realm and its storage routing.
    pub view: bool,
    /// The compute limits and a new execution id, for a launch offer.
    pub launch: bool,
    /// The fresh heartbeats, for a forwarded job submission or its inputs.
    pub heartbeats: bool,
}

/// The parts of [`MetadataNode`] the shell reads for `message`.
pub fn metadata_needs(message: &MetadataTransportMessage) -> MetadataNeeds {
    use MetadataTransportMessage as Message;
    MetadataNeeds {
        view: needs_view(message),
        launch: matches!(message, Message::ForwardLaunchOffer { .. }),
        heartbeats: matches!(
            message,
            Message::ForwardJobSubmission { .. } | Message::ResolveJobInputs { .. }
        ),
    }
}

/// The child that answers the request, which names how its outcome becomes the reply.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Asked {
    Plugin(PluginAnswer),
    Record,
    Submission,
    /// A holder asks whether a launch's interval is closed.
    Closure,
    /// A holder asks this receiver about a task submission's claim.
    Claim,
    Inputs,
    Page,
    Serve,
}

type Turn = Result<Next<ServedReply>, IoError>;

/// One metadata peer request. An audit page past the inbound audit slots is answered
/// unavailable without a run.
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataPeerOperation {
    local: (NodeId, RealmId),
    peer: NodeId,
    now_ms: u64,
    message: Option<MetadataTransportMessage>,
    refused: bool,
    node: MetadataNode,
    asked: Option<Asked>,
}

impl MetadataPeerOperation {
    pub fn new(
        local: (NodeId, RealmId),
        peer: NodeId,
        now_ms: u64,
        message: MetadataTransportMessage,
        refused: bool,
        node: MetadataNode,
    ) -> Self {
        Self {
            local,
            peer,
            now_ms,
            message: Some(message),
            refused,
            node,
            asked: None,
        }
    }

    fn ask<O>(&mut self, asked: Asked, operation: O) -> Turn
    where
        O: Operation + 'static,
        O::Output: PartialEq + 'static,
        O::Error: PartialEq + 'static,
    {
        self.asked = Some(asked);
        Ok(Next::Call(IoCall::child(operation)))
    }

    /// Asks the job or metadata domain once no plugin operation took the message.
    fn ask_domain(&mut self, message: MetadataTransportMessage) -> Turn {
        use MetadataTransportMessage as Message;
        let (peer, now_ms, serving) = (self.peer, self.now_ms, Some(self.local));
        match message {
            record @ (Message::ForwardJobRecord { .. } | Message::ForwardRecordPage { .. }) => {
                let serve = ServeRecordOperation::new(peer, record, serving, now_ms);
                self.ask(Asked::Record, serve)
            }
            Message::ForwardLaunchOffer { launch } => {
                let id = self.node.execution_id;
                let node = launch_node(serving, &self.node.limits, &launch, now_ms, id);
                let serve = ServeLaunchOperation::new(peer, *launch, node);
                self.ask(Asked::Record, serve)
            }
            Message::ForwardJobSubmission {
                auth_token,
                submission_id,
                request,
            } => {
                let heartbeats = std::mem::take(&mut self.node.heartbeats);
                let node = ingress_node(serving, heartbeats, now_ms);
                let admit =
                    AdmitForwardedOperation::new(peer, auth_token, submission_id, *request, node);
                self.ask(Asked::Submission, admit)
            }
            Message::QueryLaunchClosure { launch } => {
                let answer = AnswerClosureOperation::new(*launch, serving, Some(peer));
                self.ask(Asked::Closure, answer)
            }
            Message::QuerySubmissionClaim {
                submission_id,
                admit_by,
            } => {
                let question = (submission_id, admit_by);
                let serve = ServeClosureOperation::new(self.local.1, peer, question);
                self.ask(Asked::Claim, serve)
            }
            Message::ResolveJobInputs {
                auth_token,
                request,
            } => {
                let heartbeats = std::mem::take(&mut self.node.heartbeats);
                let Some(node) = ingress_node(serving, heartbeats, now_ms) else {
                    let result = Err(SubmissionRefusal::Unavailable);
                    let reply = Message::ResolvedJobInputs { result };
                    return Ok(Next::Done((reply, None)));
                };
                let serve = ServeInputsOperation::new(peer, auth_token, &node, *request);
                self.ask(Asked::Inputs, serve)
            }
            Message::JobInputPage { submission, digest } => {
                let serve = ServePageOperation::new(self.local.1, peer, submission, digest);
                self.ask(Asked::Page, serve)
            }
            message => {
                let serving = ServeNode {
                    local: serving,
                    graph: self.node.graph,
                    indexed: self.node.indexed,
                    metadata_bytes: self.node.metadata_bytes,
                    here: self.node.here.take(),
                    routing: self.node.routing.take(),
                };
                let serve = ServeRequestOperation::new(peer, serving, message);
                self.ask(Asked::Serve, serve)
            }
        }
    }
}

impl Operation for MetadataPeerOperation {
    type Output = ServedReply;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        let Some(message) = self.message.take() else {
            return Err(IoError::Unfinished);
        };
        if self.refused {
            let result = Err(MetadataReadError::Unavailable);
            let reply = MetadataTransportMessage::ForwardedAuditPage { result };
            return Ok(Next::Done((reply, None)));
        }
        let (local, realm_id) = self.local;
        let node = Some((realm_id, local));
        match AnswerPeer::new(node, self.peer, message, self.now_ms) {
            Ok(answer) => self.ask(Asked::Plugin(answer.answer()), answer),
            Err(message) => self.ask_domain(message),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn {
        use MetadataTransportMessage as Message;
        let Some(asked) = self.asked.take() else {
            return Err(IoError::Unfinished);
        };
        let reply = match asked {
            Asked::Plugin(kind) => {
                let served = result.child::<Message, IoError>()?;
                served.unwrap_or_else(|_| kind.unavailable())
            }
            Asked::Record => {
                let served = result.child::<Message, IoError>()?;
                served.unwrap_or(Message::ForwardedWriteUnavailable)
            }
            Asked::Submission => Message::ForwardedJobSubmission {
                result: result.child::<SubmissionAck, SubmissionRefusal>()?,
            },
            Asked::Closure => match result.child::<LaunchClosureAnswer, ClosureError>()? {
                Ok(answer) => Message::LaunchClosureAnswered { answer },
                Err(ClosureError::Outsider) => Message::Reject(ClosureError::Outsider.to_string()),
                Err(ClosureError::Undecided(_)) => Message::ForwardedWriteUnavailable,
            },
            Asked::Claim => Message::SubmissionClaimAnswered {
                answer: (result.child::<ClosureAnswer, IoError>()?)
                    .unwrap_or(ClosureAnswer::Undecided),
            },
            Asked::Inputs => Message::ResolvedJobInputs {
                result: result
                    .child::<Vec<(InputSelection, CapturedInput)>, SubmissionRefusal>()?,
            },
            Asked::Page => {
                let page = result.child::<Option<Box<InputPage>>, PageReadError>()?;
                Message::InputPageResult {
                    page: page.ok().flatten(),
                }
            }
            Asked::Serve => {
                let served = result.child::<ServedReply, IoError>()?;
                let reject = |error: IoError| (Message::Reject(error.to_string()), None);
                return Ok(Next::Done(served.unwrap_or_else(reject)));
            }
        };
        Ok(Next::Done((reply, None)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::family::SubmissionId;

    fn local() -> (NodeId, RealmId) {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        (node, RealmId::from_bytes([2; 32]))
    }

    fn peer() -> NodeId {
        iroh::SecretKey::from_bytes(&[3; 32]).public()
    }

    fn operation(message: MetadataTransportMessage, refused: bool) -> MetadataPeerOperation {
        let node = MetadataNode::default();
        MetadataPeerOperation::new(local(), peer(), 5, message, refused, node)
    }

    // An audit page past the audit slots is answered unavailable without a run.
    #[test]
    fn refused_audit_unavailable() {
        let page = MetadataTransportMessage::JobInputPage {
            submission: SubmissionId([0; 32]),
            digest: [0; 32],
        };
        let mut refused = operation(page, true);
        let result = Err(MetadataReadError::Unavailable);
        let reply = MetadataTransportMessage::ForwardedAuditPage { result };
        assert_eq!(refused.first(), Ok(Next::Done((reply, None))));
    }

    // A job input page goes to the page operation, and a failed read answers no page.
    #[test]
    fn page_read_answered() {
        let (submission, digest) = (SubmissionId([1; 32]), [7; 32]);
        let page = MetadataTransportMessage::JobInputPage { submission, digest };
        let mut run = operation(page, false);
        let serve = ServePageOperation::new(local().1, peer(), submission, digest);
        assert_eq!(run.first(), Ok(Next::Call(IoCall::child(serve))));
        let failed = Err::<Option<Box<InputPage>>, _>(PageReadError::from(IoError::Unfinished));
        let failed = IoResult::Child(ChildOutcome::new(failed));
        let reply = MetadataTransportMessage::InputPageResult { page: None };
        assert_eq!(run.next(failed), Ok(Next::Done((reply, None))));
    }

    // A claim question goes to this receiver's closure answer; a run that broke off decides
    // nothing.
    #[test]
    fn claim_query_answered() {
        let (submission_id, admit_by) = (SubmissionId([4; 32]), 9);
        let query = MetadataTransportMessage::QuerySubmissionClaim {
            submission_id,
            admit_by,
        };
        let mut run = operation(query.clone(), false);
        let serve = ServeClosureOperation::new(local().1, peer(), (submission_id, admit_by));
        assert_eq!(run.first(), Ok(Next::Call(IoCall::child(serve))));
        let absent = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(ClosureAnswer::Absent)));
        let answer = ClosureAnswer::Absent;
        let reply = MetadataTransportMessage::SubmissionClaimAnswered { answer };
        assert_eq!(run.next(absent), Ok(Next::Done((reply, None))));
        let mut broken = operation(query, false);
        broken.first().unwrap();
        let failed = Err::<ClosureAnswer, _>(IoError::Unfinished);
        let failed = IoResult::Child(ChildOutcome::new(failed));
        let answer = ClosureAnswer::Undecided;
        let reply = MetadataTransportMessage::SubmissionClaimAnswered { answer };
        assert_eq!(broken.next(failed), Ok(Next::Done((reply, None))));
    }

    // A launch closure answer goes back as is; an outsider is rejected and an undecided answer
    // is unavailable, so the holder asks again.
    #[test]
    fn closure_answer_replied() {
        let reply = |answer: Result<LaunchClosureAnswer, ClosureError>| {
            let mut run = operation(MetadataTransportMessage::ForwardedWriteUnavailable, false);
            run.asked = Some(Asked::Closure);
            match run.next(IoResult::Child(ChildOutcome::new(answer))) {
                Ok(Next::Done((reply, None))) => reply,
                other => panic!("a closure answer is a reply: {other:?}"),
            }
        };
        let never = LaunchClosureAnswer::NeverAccepted;
        let answered = MetadataTransportMessage::LaunchClosureAnswered {
            answer: never.clone(),
        };
        assert_eq!(reply(Ok(never)), answered);
        let outsider = MetadataTransportMessage::Reject(ClosureError::Outsider.to_string());
        assert_eq!(reply(Err(ClosureError::Outsider)), outsider);
        let undecided = Err(ClosureError::Undecided("down".to_string()));
        assert_eq!(
            reply(undecided),
            MetadataTransportMessage::ForwardedWriteUnavailable
        );
    }

    // A request no plugin or job operation takes is served by the metadata domain, and a run
    // that broke off is answered with its refusal.
    #[test]
    fn served_request_rejects() {
        let mut run = operation(MetadataTransportMessage::ForwardedWriteUnavailable, false);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed = Err::<ServedReply, _>(IoError::Unfinished);
        let failed = IoResult::Child(ChildOutcome::new(failed));
        let reply = MetadataTransportMessage::Reject(IoError::Unfinished.to_string());
        assert_eq!(run.next(failed), Ok(Next::Done((reply, None))));
    }
}
