//! Forwards one submission to the holders of its family, one holder after another.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::{SubmissionAck, SubmissionRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::SubmissionId;
use tracing::{debug, warn};

use super::admit_here::AcceptedSubmission;
use super::ids::{RequestIdentity, SubmissionRequest};
use crate::jobs::submit::SubmitJobError;
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};

/// Upper bound of one forwarded request; the transport's own step limits end it earlier.
const FORWARD_DEADLINE: Duration = Duration::from_secs(60);

/// Forwards the complete request and its preassigned identity one hop. The holder revalidates
/// the caller and recomputes the identity itself, so this node never becomes the authority for
/// a job it does not hold. A holder that read the request and refused it answers for every
/// holder; an unreachable or unwilling one leaves the next to try.
#[derive(Debug, PartialEq)]
pub(super) struct ForwardSubmissionOperation {
    submission_id: SubmissionId,
    frame: Option<Vec<u8>>,
    has_token: bool,
    holders: VecDeque<NodeId>,
    asked: Option<NodeId>,
}

type ForwardNext = Result<Next<AcceptedSubmission>, SubmitJobError>;

impl ForwardSubmissionOperation {
    pub(super) fn new(
        request: &SubmissionRequest,
        identity: &RequestIdentity,
        holders: Vec<NodeId>,
        auth_token: Option<AuthToken>,
    ) -> Self {
        let has_token = auth_token.is_some();
        let frame = auth_token.and_then(|auth_token| {
            let message = MetadataTransportMessage::ForwardJobSubmission {
                auth_token,
                submission_id: identity.submission_id,
                request: Box::new(request.clone()),
            };
            encode_message(&message)
                .inspect_err(|error| warn!(error = %error, "Submission frame does not encode"))
                .ok()
        });
        Self {
            submission_id: identity.submission_id,
            frame,
            has_token,
            holders: holders.into(),
            asked: None,
        }
    }

    fn ask(&mut self) -> ForwardNext {
        let next = self.frame.as_ref().zip(self.holders.pop_front());
        let Some((frame, peer)) = next else {
            return Err(SubmitJobError::PlacementUnavailable(
                "no job family holder accepted the submission".to_string(),
            ));
        };
        let request = MetadataRequestEffect {
            peer,
            frame: frame.clone(),
            timeout: FORWARD_DEADLINE,
        };
        self.asked = Some(peer);
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn accepted(&self, ack: SubmissionAck) -> ForwardNext {
        Ok(Next::Done(AcceptedSubmission {
            job_id: ack.job_id,
            created: ack.created,
            submission_id: self.submission_id,
            state: ack.state,
        }))
    }
}

impl Operation for ForwardSubmissionOperation {
    type Output = AcceptedSubmission;
    type Error = SubmitJobError;

    fn first(&mut self) -> ForwardNext {
        if !self.has_token {
            return Err(SubmitJobError::PlacementUnavailable(
                "forwarding a submission needs the caller's token".to_string(),
            ));
        }
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> ForwardNext {
        let IoResult::Event(event) = result else {
            let got = result.label();
            let expected = "Event::Net(NetEvent::MetadataAnswer)";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let Event::Net(NetEvent::MetadataAnswer(answer)) = *event else {
            let expected = "Event::Net(NetEvent::MetadataAnswer)";
            let got = "another event";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let MetadataAnswer { peer, result, .. } = *answer;
        let reply = match result {
            Ok(bytes) => decode_message(&bytes).map_err(|error| error.to_string()),
            Err(error) => Err(format!("{error:?}")),
        };
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                debug!(peer = %peer, error = %error, "Submission forwarding failed");
                return self.ask();
            }
        };
        let MetadataTransportMessage::ForwardedJobSubmission { result } = reply else {
            if !matches!(reply, MetadataTransportMessage::ForwardedWriteUnavailable) {
                warn!(
                    peer = %peer,
                    reply = crate::metadata::transport_message_kind(&reply),
                    "Unexpected submission forwarding reply"
                );
            }
            return self.ask();
        };
        match result {
            Ok(ack) => self.accepted(ack),
            Err(SubmissionRefusal::Conflict { existing_job_id }) => {
                Err(SubmitJobError::JobPlanConflict { existing_job_id })
            }
            Err(SubmissionRefusal::Unauthorized) => Err(SubmitJobError::AuthorityDenied),
            Err(SubmissionRefusal::Invalid { reason }) => {
                Err(SubmitJobError::InvalidWorkspace(reason))
            }
            Err(reason) => {
                debug!(peer = %peer, reason = ?reason, "Family holder refused the submission");
                self.ask()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{node, payload, user};
    use aruna_core::events::PeerRequestError;
    use aruna_core::structs::execution::job::{JobId, LogicalJobState};

    use super::super::ids::SubmissionScope;

    fn request() -> SubmissionRequest {
        SubmissionRequest {
            created_by: user(),
            spec: payload(),
            scope: SubmissionScope::Keyed("forward".to_string()),
            retention_ms: 60_000,
            ingress_node_id: node(1),
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            input_pages: None,
        }
    }

    fn forward(token: Option<AuthToken>) -> ForwardSubmissionOperation {
        let request = request();
        let identity = request.identity().expect("identity derives");
        ForwardSubmissionOperation::new(&request, &identity, vec![node(2), node(3)], token)
    }

    fn token() -> Option<AuthToken> {
        Some(AuthToken::bearer("token").expect("bearer token"))
    }

    fn answer(peer: NodeId, result: Result<SubmissionAck, SubmissionRefusal>) -> IoResult {
        let message = MetadataTransportMessage::ForwardedJobSubmission { result };
        let result = Ok(encode_message(&message).expect("answer encodes"));
        event(peer, result)
    }

    fn event(peer: NodeId, result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
            MetadataAnswer {
                peer,
                elapsed: Duration::from_secs(1),
                result,
            },
        )))))
    }

    fn asked(next: ForwardNext) -> NodeId {
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("a holder is asked");
        };
        let Effect::Net(NetEffect::MetadataRequest(request)) = *effect else {
            panic!("the request goes over the metadata transport");
        };
        request.peer
    }

    // Without the caller's token nothing is sent.
    #[test]
    fn needs_caller_token() {
        assert!(matches!(
            forward(None).first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // An unreachable or unwilling holder leaves the next one to answer.
    #[test]
    fn tries_next_holder() {
        let mut operation = forward(token());
        assert_eq!(asked(operation.first()), node(2));
        let failed = event(node(2), Err(PeerRequestError::TimedOut));
        assert_eq!(asked(operation.next(failed)), node(3));
        let ack = SubmissionAck {
            job_id: JobId::from_bytes([7u8; 16]),
            created: true,
            state: LogicalJobState::Queued,
        };
        let Ok(Next::Done(accepted)) = operation.next(answer(node(3), Ok(ack))) else {
            panic!("the holder accepted");
        };
        assert!(accepted.created);

        let mut unwilling = forward(token());
        unwilling.first().expect("first holder");
        let refused = answer(node(2), Err(SubmissionRefusal::NotHolder));
        assert_eq!(asked(unwilling.next(refused)), node(3));
        let refused = answer(node(3), Err(SubmissionRefusal::Unavailable));
        assert!(matches!(
            unwilling.next(refused),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // A holder that read the request and refused it answers for every holder.
    #[test]
    fn refusal_ends_forwarding() {
        let mut operation = forward(token());
        operation.first().expect("first holder");
        let reason = "bad".to_string();
        let refused = answer(node(2), Err(SubmissionRefusal::Invalid { reason }));
        assert!(matches!(
            operation.next(refused),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }
}
