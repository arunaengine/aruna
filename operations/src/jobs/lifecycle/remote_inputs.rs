//! Asks another node to resolve the inputs of one submission that it holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::{InputResolveRequest, SubmissionRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{CapturedInput, InputSelection, SubmissionId};

use super::submission_rules::{batch_len, resolve_answer};
use crate::jobs::submit::SubmitJobError;
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};

/// Upper bound of one resolve request; the transport's own step limits end it earlier.
const RESOLVE_DEADLINE: Duration = Duration::from_secs(60);

/// Who asks another node to resolve inputs, and until when its grants may hold.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolveCaller {
    pub created_by: UserId,
    pub submission_id: SubmissionId,
    pub expires_at: u64,
}

type RemoteAnswers = Vec<(InputSelection, CapturedInput)>;
type RemoteNext = Result<Next<RemoteAnswers>, SubmitJobError>;

/// The inputs of another node, resolved and granted by that node for the caller's own bearer.
/// Every input must name its exact version, so nothing depends on a remote head. Each request
/// carries the inputs that fit one batch and the node answers a prefix of them, so the rest go
/// out again until every input is answered.
#[derive(Debug, PartialEq)]
pub struct RemoteInputsOperation {
    node: NodeId,
    inputs: Vec<InputSelection>,
    caller: ResolveCaller,
    token: Option<AuthToken>,
    batch: usize,
    answered: RemoteAnswers,
}

impl RemoteInputsOperation {
    pub fn new(
        node: NodeId,
        inputs: Vec<InputSelection>,
        caller: ResolveCaller,
        token: Option<AuthToken>,
    ) -> Self {
        Self {
            node,
            inputs,
            caller,
            token: token.filter(|token| matches!(token, AuthToken::Bearer(_))),
            batch: 0,
            answered: Vec::new(),
        }
    }

    fn ask(&mut self) -> RemoteNext {
        let rest = &self.inputs[self.answered.len()..];
        if rest.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.answered)));
        }
        self.batch = batch_len(rest);
        let batch = &rest[..self.batch];
        if batch.is_empty() {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "{}: the input exceeds one message",
                rest[0].dest_key
            )));
        }
        let Some(auth_token) = self.token.clone() else {
            return Err(SubmitJobError::PlacementUnavailable(
                "an input on another node needs the caller's bearer token".to_string(),
            ));
        };
        let message = MetadataTransportMessage::ResolveJobInputs {
            auth_token,
            request: Box::new(InputResolveRequest {
                created_by: self.caller.created_by,
                submission_id: self.caller.submission_id,
                expires_at: self.caller.expires_at,
                inputs: batch.to_vec(),
            }),
        };
        let frame = encode_message(&message).map_err(|error| {
            SubmitJobError::PlacementUnavailable(format!("input node {}: {error}", self.node))
        })?;
        let request = MetadataRequestEffect {
            peer: self.node,
            frame,
            timeout: RESOLVE_DEADLINE,
        };
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }
}

/// What a resolve request came back with, or why it did not.
fn resolve_reply(
    answer: MetadataAnswer,
) -> Result<Result<RemoteAnswers, SubmissionRefusal>, String> {
    let bytes = answer.result.map_err(|error| format!("{error:?}"))?;
    match decode_message(&bytes).map_err(|error| error.to_string())? {
        MetadataTransportMessage::ResolvedJobInputs { result } => Ok(result),
        other => Err(format!(
            "the node answered {}",
            crate::metadata::transport_message_kind(&other)
        )),
    }
}

impl Operation for RemoteInputsOperation {
    type Output = RemoteAnswers;
    type Error = SubmitJobError;

    fn first(&mut self) -> RemoteNext {
        if self.token.is_none() {
            return Err(SubmitJobError::PlacementUnavailable(
                "an input on another node needs the caller's bearer token".to_string(),
            ));
        }
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> RemoteNext {
        let expected = "Event::Net(NetEvent::MetadataAnswer)";
        let IoResult::Event(event) = result else {
            let got = result.label();
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let Event::Net(NetEvent::MetadataAnswer(answer)) = *event else {
            let got = "another event";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let start = self.answered.len();
        let batch = &self.inputs[start..start + self.batch];
        let answers = resolve_answer(resolve_reply(*answer), batch, self.node)?;
        self.answered.extend(answers);
        self.ask()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{node, user};
    use aruna_core::events::PeerRequestError;
    use aruna_core::structs::execution::job::{InputMode, InputSource};

    fn input() -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "b".to_string(),
                key: "a".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "a".to_string(),
            mode: InputMode::Snapshot,
            container_path: Some("/in/a".to_string()),
            name: None,
            description: None,
        }
    }

    fn remote(token: Option<AuthToken>) -> RemoteInputsOperation {
        let caller = ResolveCaller {
            created_by: user(),
            submission_id: SubmissionId([7u8; 32]),
            expires_at: 9,
        };
        RemoteInputsOperation::new(node(2), vec![input()], caller, token)
    }

    fn answer(result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
            MetadataAnswer {
                peer: node(2),
                elapsed: Duration::from_secs(1),
                result,
            },
        )))))
    }

    fn bearer() -> Option<AuthToken> {
        Some(AuthToken::bearer("token").expect("bearer token"))
    }

    // Another node resolves only for the caller's bearer; its refusal keeps its meaning.
    #[test]
    fn remote_needs_bearer() {
        assert!(matches!(
            remote(None).first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
        let mut asked = remote(bearer());
        assert!(matches!(asked.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let message = MetadataTransportMessage::ResolvedJobInputs {
            result: Err(SubmissionRefusal::Unauthorized),
        };
        let bytes = encode_message(&message).expect("answer encodes");
        assert!(matches!(
            asked.next(answer(Ok(bytes))),
            Err(SubmitJobError::InputDenied(_))
        ));

        let mut lost = remote(bearer());
        lost.first().expect("resolve request");
        assert!(matches!(
            lost.next(answer(Err(PeerRequestError::TimedOut))),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }
}
