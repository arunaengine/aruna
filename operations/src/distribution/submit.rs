//! Brings a request or a member's statement to a set's publisher: accepted here when this node
//! publishes the set, otherwise sent to the publisher. Nothing is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::rows::{outside_answer, outside_call};
use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, StatementSignEvent};
use aruna_core::keyspaces::CATCH_UP_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::distribution::{
    DistributionRequest, DistributionRevision, SignedStatement, StatementClaim,
};
use aruna_core::types::Key;
use tracing::warn;
use ulid::Ulid;

use super::forward::PublishRefusal;
use aruna_core::structs::identity::realm::NodeRealm;
use crate::forward::transport::{peer_answer, peer_call};
use super::publish::{PublishConfig, PublishError, PublishOperation};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::transport_message_kind;
use crate::replication::queue::schedule_blob_drain;

type Revision = Result<DistributionRevision, PublishRefusal>;

fn no_transport() -> PublishRefusal {
    PublishRefusal::Unavailable("net transport unavailable".to_string())
}

/// The first row of the catch-up keyspace, read outside any transaction.
fn waiting_read() -> IoCall {
    let page = IoCall::Page {
        space: CATCH_UP_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after: None,
        limit: 1,
    };
    outside_call(page)
}

/// Accepts one request on this node, the set's publisher, retrying storage conflicts, and
/// wakes the replication drain when a set waits for its bootstrap.
#[derive(Debug, PartialEq)]
pub(crate) struct PublishLocalOperation {
    ids: NodeRealm,
    set_id: Ulid,
    request_id: Ulid,
    request: Option<DistributionRequest>,
    auth_context: Option<AuthContext>,
    revision: Option<DistributionRevision>,
}

impl PublishLocalOperation {
    pub(crate) fn new(
        ids: NodeRealm,
        (set_id, request_id): (Ulid, Ulid),
        request: DistributionRequest,
        auth_context: Option<AuthContext>,
    ) -> Self {
        Self {
            ids,
            set_id,
            request_id,
            request: Some(request),
            auth_context,
            revision: None,
        }
    }
}

impl Operation for PublishLocalOperation {
    type Output = DistributionRevision;
    type Error = PublishRefusal;

    fn first(&mut self) -> Result<Next<DistributionRevision>, PublishRefusal> {
        let (node_id, realm_id) = self.ids.ok_or_else(no_transport)?;
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        let config = PublishConfig {
            realm_id,
            node_id,
            set_id: self.set_id,
            request_id: self.request_id,
            request,
            auth_context: self.auth_context.take(),
        };
        let build = move || PublishOperation::new(config.clone());
        let seed = self.request_id.to_bytes();
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DistributionRevision>, PublishRefusal> {
        let Some(output) = self.revision.take() else {
            let revision = result.child::<DistributionRevision, PublishError>()??;
            self.revision = Some(revision);
            return Ok(Next::Call(waiting_read()));
        };
        // The change is committed, so a failed look only skips the wake-up.
        let waiting = matches!(
            outside_answer(result),
            Ok(IoResult::Page { rows, .. }) if !rows.is_empty()
        );
        Ok(match waiting {
            true => Next::Then {
                output,
                calls: vec![IoCall::from(schedule_blob_drain())],
            },
            false => Next::Done(output),
        })
    }
}

/// What a publisher's node answered a sent request.
fn sent_answer(publisher: NodeId, result: IoResult) -> Result<Revision, IoError> {
    Ok(match peer_answer(result)? {
        Ok(MetadataTransportMessage::DistributionAccepted { result }) => result,
        Ok(other) => Err(PublishRefusal::Unavailable(format!(
            "the publisher answered {}",
            transport_message_kind(&other)
        ))),
        Err(error) => Err(unreachable_publisher(publisher, &error)),
    })
}

fn unreachable_publisher(publisher: NodeId, error: &str) -> PublishRefusal {
    warn!(%publisher, %error, "The distributed bucket publisher is unreachable");
    PublishRefusal::Unavailable(format!("publisher node {publisher} is unreachable"))
}

/// The call that sends `message` to the publisher, or the refusal when it cannot be sent.
fn send_call(
    publisher: NodeId,
    message: &MetadataTransportMessage,
) -> Result<IoCall, PublishRefusal> {
    peer_call(publisher, message).map_err(|error| unreachable_publisher(publisher, &error))
}

/// Hands a member's statement to the publisher; one it already embedded counts as done.
#[derive(Debug, PartialEq)]
pub(crate) struct SubmitOperation {
    ids: NodeRealm,
    publisher: NodeId,
    statement: Option<SignedStatement>,
    sent: bool,
}

impl SubmitOperation {
    pub(crate) fn new(ids: NodeRealm, publisher: NodeId, statement: SignedStatement) -> Self {
        Self {
            ids,
            publisher,
            statement: Some(statement),
            sent: false,
        }
    }
}

impl Operation for SubmitOperation {
    type Output = ();
    type Error = PublishRefusal;

    fn first(&mut self) -> Result<Next<()>, PublishRefusal> {
        let (local, _) = self.ids.ok_or_else(no_transport)?;
        let statement = self.statement.take().ok_or(IoError::Unfinished)?;
        let (set_id, id) = (statement.statement.set_id, statement.statement.statement_id);
        let statement = Box::new(statement);
        if self.publisher == local {
            let request = DistributionRequest::Statement(statement);
            let publish = PublishLocalOperation::new(self.ids, (set_id, id), request, None);
            return Ok(Next::Call(IoCall::child(publish)));
        }
        self.sent = true;
        let message = MetadataTransportMessage::SubmitDistributionStatement { statement };
        Ok(Next::Call(send_call(self.publisher, &message)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PublishRefusal> {
        let outcome = match self.sent {
            true => sent_answer(self.publisher, result)?,
            false => result.child::<DistributionRevision, PublishRefusal>()?,
        };
        match outcome {
            Ok(_) | Err(PublishRefusal::Replayed) => Ok(Next::Done(())),
            Err(refusal) => Err(refusal),
        }
    }
}

/// Sends an admin request to the set's publisher: accepted here when this node publishes,
/// otherwise forwarded with the caller's bearer token, which the publisher checks again.
#[derive(Debug, PartialEq)]
pub(crate) struct SendAdminOperation {
    ids: NodeRealm,
    publisher: NodeId,
    set_id: Ulid,
    auth: AuthContext,
    bearer: Option<AuthToken>,
    request: Option<DistributionRequest>,
    sent: bool,
}

impl SendAdminOperation {
    pub(crate) fn new(
        (ids, publisher): (NodeRealm, NodeId),
        set_id: Ulid,
        (auth, bearer): (AuthContext, Option<AuthToken>),
        request: DistributionRequest,
    ) -> Self {
        Self {
            ids,
            publisher,
            set_id,
            auth,
            bearer,
            request: Some(request),
            sent: false,
        }
    }
}

impl Operation for SendAdminOperation {
    type Output = DistributionRevision;
    type Error = PublishRefusal;

    fn first(&mut self) -> Result<Next<DistributionRevision>, PublishRefusal> {
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        let request_id = Ulid::generate();
        if self.ids.is_some_and(|(local, _)| local == self.publisher) {
            let ids = (self.set_id, request_id);
            let auth = Some(self.auth.clone());
            let publish = PublishLocalOperation::new(self.ids, ids, request, auth);
            return Ok(Next::Call(IoCall::child(publish)));
        }
        let auth_token = self.bearer.take().ok_or(PublishRefusal::Unauthorized)?;
        self.sent = true;
        let message = MetadataTransportMessage::ForwardDistributionRequest {
            auth_token,
            set_id: self.set_id,
            request_id,
            request: Box::new(request),
        };
        Ok(Next::Call(send_call(self.publisher, &message)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DistributionRevision>, PublishRefusal> {
        let outcome = match self.sent {
            true => sent_answer(self.publisher, result)?,
            false => result.child::<DistributionRevision, PublishRefusal>()?,
        };
        Ok(Next::Done(outcome?))
    }
}

/// How signing a statement and handing it to the publisher ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Stated {
    Taken,
    /// This node could not sign it.
    Unsigned(String),
    Refused(PublishRefusal),
}

/// Signs a statement with this node's key and hands it to the publisher.
#[derive(Debug, PartialEq)]
pub(crate) struct StateOperation {
    ids: NodeRealm,
    publisher: NodeId,
    claim: Option<StatementClaim>,
}

impl StateOperation {
    pub(crate) fn new(ids: NodeRealm, publisher: NodeId, claim: StatementClaim) -> Self {
        Self {
            ids,
            publisher,
            claim: Some(claim),
        }
    }
}

impl Operation for StateOperation {
    type Output = Stated;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Stated>, IoError> {
        let claim = self.claim.as_ref().cloned().ok_or(IoError::Unfinished)?;
        let effect = Effect::Net(NetEffect::StatementSign(Box::new(claim)));
        Ok(Next::Call(IoCall::from(effect)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Stated>, IoError> {
        if self.claim.take().is_none() {
            return Ok(Next::Done(match result.child::<(), PublishRefusal>()? {
                Ok(()) => Stated::Taken,
                Err(refusal) => Stated::Refused(refusal),
            }));
        }
        let signed = match result.event()? {
            Event::Net(NetEvent::StatementSign(signed)) => signed,
            other => {
                return Err(IoError::Unexpected {
                    expected: "Event::Net(NetEvent::StatementSign)",
                    got: other.label(),
                });
            }
        };
        match signed {
            StatementSignEvent::Signed(signed) => {
                let submit = SubmitOperation::new(self.ids, self.publisher, *signed);
                Ok(Next::Call(IoCall::child(submit)))
            }
            StatementSignEvent::Unavailable(reason) => Ok(Next::Done(Stated::Unsigned(reason))),
        }
    }
}

#[cfg(test)]
#[path = "submit_tests.rs"]
mod tests;
