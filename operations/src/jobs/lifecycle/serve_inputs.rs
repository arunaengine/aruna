//! Resolves inputs this node holds for an ingress, under a bearer validated here, so grants
//! vouch only for an identity this node checked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::PeerAuthRefusal;
use aruna_core::id::NodeId;
use aruna_core::jobs::{InputResolveRequest, SubmissionRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{CapturedInput, InputSelection, InputSource};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::replication::VersionedObjectArn;

use super::admit_here::placement_failure;
use super::input_capture::{CaptureCaller, CaptureInputsOperation};
use super::input_walk::{FollowLinkOperation, InputCaller};
use super::submission_rules::{fits, peer_refusal, refusal_of, resolver_admits};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::JOB_RETENTION_MS;
use crate::jobs::submit::SubmitJobError;
use crate::metadata::write_peer::WritePeerOperation;

#[derive(Debug, PartialEq)]
enum ServeStep {
    Token,
    Config(Box<AuthContext>),
    Follow,
    Capture(Box<InputSelection>, Option<VersionedObjectArn>),
}

/// Each answer is an input after its links with its capture here; the answers that fit one
/// message return, and the ingress asks again for the rest.
#[derive(Debug, PartialEq)]
pub(super) struct ServeInputsOperation {
    peer: NodeId,
    token: AuthToken,
    caller: InputCaller,
    request: InputResolveRequest,
    now_ms: u64,
    bearer: String,
    next_input: usize,
    answers: Vec<(InputSelection, CapturedInput)>,
    size: usize,
    step: ServeStep,
}

type ServeOutput = Vec<(InputSelection, CapturedInput)>;
type ServeNext = Result<Next<ServeOutput>, SubmissionRefusal>;
type Followed = (InputSelection, Option<VersionedObjectArn>);

impl ServeInputsOperation {
    /// `local` and the realm in `caller` are this node's; `now_ms` bounds the grants.
    pub(super) fn new(
        peer: NodeId,
        token: AuthToken,
        caller: InputCaller,
        request: InputResolveRequest,
        now_ms: u64,
    ) -> Self {
        Self {
            peer,
            token,
            caller,
            request,
            now_ms,
            bearer: String::new(),
            next_input: 0,
            answers: Vec::new(),
            size: 0,
            step: ServeStep::Token,
        }
    }

    fn expires_at(&self) -> u64 {
        let bound = self.now_ms.saturating_add(JOB_RETENTION_MS) / 1000;
        self.request.expires_at.min(bound)
    }

    fn admit(&mut self, auth: &AuthContext, config: &RealmConfigDocument) -> ServeNext {
        // Only a realm node resolves for an ingress; a device forwards its whole submission.
        resolver_admits(auth, self.request.created_by, config, self.peer)?;
        let local = self.caller.local;
        let elsewhere = |input: &InputSelection| input.source_node_id.is_some_and(|n| n != local);
        if self.request.inputs.iter().any(elsewhere) {
            return Err(SubmissionRefusal::Invalid {
                reason: "an input names another node".to_string(),
            });
        }
        self.follow_next()
    }

    fn follow_next(&mut self) -> ServeNext {
        let Some(input) = self.request.inputs.get(self.next_input) else {
            return Ok(Next::Done(std::mem::take(&mut self.answers)));
        };
        self.step = ServeStep::Follow;
        let follow = FollowLinkOperation::new(self.caller, input.clone());
        Ok(Next::Call(IoCall::child(follow)))
    }

    fn capture(&mut self, (mut input, link): Followed) -> ServeNext {
        let local = self.caller.local;
        if input.source_node_id.is_some_and(|node| node != local) {
            return Err(SubmissionRefusal::Invalid {
                reason: format!("{}: a link here names another node", input.dest_key),
            });
        }
        input.source_node_id = Some(local);
        let caller = CaptureCaller {
            input: self.caller,
            submission_id: self.request.submission_id,
            expires_at: self.expires_at(),
            bearer: Some(self.bearer.clone()),
        };
        let capture = CaptureInputsOperation::new(caller, vec![input.clone()]);
        self.step = ServeStep::Capture(Box::new(input), link);
        Ok(Next::Call(IoCall::child(capture)))
    }

    fn answer(
        &mut self,
        mut input: InputSelection,
        link: Option<VersionedObjectArn>,
        captured: Vec<CapturedInput>,
    ) -> ServeNext {
        let Some(mut captured) = captured.into_iter().next() else {
            return Err(SubmissionRefusal::Unavailable);
        };
        captured.link = link;
        // An input asked without a version is pinned to the one captured here.
        let InputSource::S3 { version_id, .. } = &mut input.source;
        version_id.get_or_insert_with(|| captured.version_id.to_string());
        let answer = (input, captured);
        if !fits(&mut self.size, &answer) {
            if self.answers.is_empty() {
                return Err(SubmissionRefusal::Invalid {
                    reason: format!("{}: the input exceeds one message", answer.0.dest_key),
                });
            }
            return Ok(Next::Done(std::mem::take(&mut self.answers)));
        }
        self.answers.push(answer);
        self.next_input += 1;
        self.follow_next()
    }
}

impl Operation for ServeInputsOperation {
    type Output = ServeOutput;
    type Error = SubmissionRefusal;

    fn first(&mut self) -> ServeNext {
        let AuthToken::Bearer(bearer) = &self.token else {
            return Err(SubmissionRefusal::Unauthorized);
        };
        // Kept to ask range coordinators about release membership for the same caller.
        self.bearer = bearer.as_str().to_string();
        let check = WritePeerOperation::new(self.peer, self.token.clone());
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> ServeNext {
        match std::mem::replace(&mut self.step, ServeStep::Follow) {
            ServeStep::Token => {
                let auth = result.child::<AuthContext, PeerAuthRefusal>()?;
                let auth = auth.map_err(peer_refusal)?;
                self.step = ServeStep::Config(Box::new(auth));
                let read = GetConfigOperation::new(self.caller.realm_id);
                Ok(Next::Call(IoCall::child(read)))
            }
            ServeStep::Config(auth) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| SubmissionRefusal::Unavailable)?;
                self.admit(&auth, &config)
            }
            ServeStep::Follow => {
                let followed = result.child::<Followed, SubmitJobError>()?;
                let followed = followed.map_err(|error| refusal_of(placement_failure(error)))?;
                self.capture(followed)
            }
            ServeStep::Capture(input, link) => {
                let captured = result.child::<Vec<CapturedInput>, SubmitJobError>()?;
                let captured = captured.map_err(|error| refusal_of(placement_failure(error)))?;
                self.answer(*input, link, captured)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, captured_input, node, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{InputMode, SubmissionId};
    use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};

    fn input(key: &str, holder: Option<NodeId>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "b".to_string(),
                key: key.to_string(),
                version_id: None,
            },
            source_node_id: holder,
            dest_key: key.to_string(),
            mode: InputMode::Snapshot,
            container_path: Some(format!("/in/{key}")),
            name: None,
            description: None,
        }
    }

    fn operation(token: AuthToken, inputs: Vec<InputSelection>) -> ServeInputsOperation {
        let caller = InputCaller {
            created_by: user(),
            realm_id: REALM,
            local: node(1),
        };
        let request = InputResolveRequest {
            created_by: user(),
            submission_id: SubmissionId([4; 32]),
            expires_at: u64::MAX,
            inputs,
        };
        ServeInputsOperation::new(node(2), token, caller, request, 0)
    }

    fn bearer() -> AuthToken {
        AuthToken::bearer("token").unwrap()
    }

    fn auth() -> AuthContext {
        AuthContext {
            user_id: user(),
            realm_id: REALM,
            path_restrictions: None,
            session: None,
        }
    }

    fn config() -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(REALM, Vec::new(), 1);
        config.nodes.push(RealmNode {
            node_id: node(2).to_string(),
            kind: RealmNodeKind::Server,
        });
        config
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    /// The operation after the token and the realm config admitted the peer.
    fn admitted(inputs: Vec<InputSelection>) -> (ServeInputsOperation, ServeNext) {
        let mut serve = operation(bearer(), inputs);
        serve.first().expect("the token is checked");
        let read = serve.next(child(Ok::<_, PeerAuthRefusal>(auth())));
        assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
        let next = serve.next(child(Ok::<_, GetConfigError>(config())));
        (serve, next)
    }

    // Only a bearer validated here can vouch for grants.
    #[test]
    fn needs_bearer_token() {
        let internal = AuthToken::Internal(auth());
        assert_eq!(
            operation(internal, Vec::new()).first(),
            Err(SubmissionRefusal::Unauthorized)
        );
    }

    // A refused token ends the request as the refusal the peer may act on.
    #[test]
    fn refused_token_ends() {
        let mut serve = operation(bearer(), Vec::new());
        serve.first().expect("the token is checked");
        let refused = Err::<AuthContext, _>(PeerAuthRefusal::Unauthorized);
        assert_eq!(
            serve.next(child(refused)),
            Err(SubmissionRefusal::Unauthorized)
        );
    }

    // Only inputs of this node are resolved here.
    #[test]
    fn refuses_other_node() {
        let (_, next) = admitted(vec![input("a", Some(node(3)))]);
        assert!(matches!(next, Err(SubmissionRefusal::Invalid { .. })));
    }

    // Each input follows its links, is captured here and is pinned to the captured version.
    #[test]
    fn answers_pinned_input() {
        let asked = input("a", None);
        let (mut serve, next) = admitted(vec![asked.clone()]);
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        let followed = Ok::<Followed, SubmitJobError>((asked.clone(), None));
        assert!(matches!(
            serve.next(child(followed)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let captured = captured_input(&asked, node(1));
        let version = captured.version_id.to_string();
        let done = serve.next(child(Ok::<_, SubmitJobError>(vec![captured])));
        let Ok(Next::Done(answers)) = done else {
            panic!("the input is answered: {done:?}");
        };
        let InputSource::S3 { version_id, .. } = &answers[0].0.source;
        assert_eq!(version_id.as_deref(), Some(version.as_str()));
        assert_eq!(answers[0].0.source_node_id, Some(node(1)));
    }

    // Grants never outlive what this node keeps for the job.
    #[test]
    fn grants_stay_bounded() {
        let serve = operation(bearer(), Vec::new());
        assert_eq!(serve.expires_at(), JOB_RETENTION_MS / 1000);
    }
}
