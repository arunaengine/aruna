//! Asks one member of a distributed bucket which key a version id names, from its version id
//! index, or which versions carry one content identity. Read-only; infrastructure peers only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use thiserror::Error;
use ulid::Ulid;

use crate::replication::protocol::VersionReplicationMessage;

#[derive(Debug, Error, PartialEq)]
pub enum VersionKeyError {
    #[error("the member did not answer: {0}")]
    Transport(String),
    #[error("the member sent an unexpected answer")]
    Unexpected,
}

impl From<IoError> for VersionKeyError {
    fn from(error: IoError) -> Self {
        Self::Transport(error.to_string())
    }
}

/// What a member answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemberAnswer {
    /// The key of the version, or `None` when its index has no such version.
    Key(Option<String>),
    /// Version ids and keys of a content identity; `more` when the member cut the page.
    Versions {
        versions: Vec<(Ulid, String)>,
        more: bool,
    },
}

/// One lookup at `node_id`. A run that fails with the connection open closes it on abort.
#[derive(Debug, PartialEq)]
pub struct VersionKeyOperation {
    node_id: NodeId,
    message: VersionReplicationMessage,
    /// The open connection, until its close is asked for.
    stream_id: Option<Ulid>,
    sent: bool,
    answer: Option<MemberAnswer>,
}

fn blob(effect: BlobEffect) -> Next<MemberAnswer> {
    Next::Call(IoCall::Effect(Box::new(Effect::Blob(effect))))
}

impl VersionKeyOperation {
    pub fn new(node_id: NodeId, realm_id: RealmId, set_id: Ulid, version_id: Ulid) -> Self {
        Self {
            node_id,
            message: VersionReplicationMessage::VersionKeyRequest {
                realm_id,
                set_id,
                version_id,
            },
            stream_id: None,
            sent: false,
            answer: None,
        }
    }

    /// Asks `node_id` for the versions of `identity` in `set_id`.
    pub fn content(node_id: NodeId, realm_id: RealmId, set_id: Ulid, identity: [u8; 32]) -> Self {
        Self {
            message: VersionReplicationMessage::ContentRequest {
                realm_id,
                set_id,
                identity,
            },
            ..Self::new(node_id, realm_id, set_id, Ulid::nil())
        }
    }

    /// The member's answer to this lookup's request, if it is one.
    fn answer(&self, payload: &[u8]) -> Option<MemberAnswer> {
        match (
            &self.message,
            VersionReplicationMessage::from_bytes(payload),
        ) {
            (
                VersionReplicationMessage::VersionKeyRequest { .. },
                Ok(VersionReplicationMessage::VersionKeyResponse(key)),
            ) => Some(MemberAnswer::Key(key)),
            (
                VersionReplicationMessage::ContentRequest { .. },
                Ok(VersionReplicationMessage::ContentResponse { versions, more }),
            ) => Some(MemberAnswer::Versions { versions, more }),
            _ => None,
        }
    }
}

impl Operation for VersionKeyOperation {
    type Output = MemberAnswer;
    type Error = VersionKeyError;

    fn first(&mut self) -> Result<Next<MemberAnswer>, VersionKeyError> {
        Ok(blob(BlobEffect::OpenConnection {
            node_id: self.node_id,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MemberAnswer>, VersionKeyError> {
        let event = match result.event()? {
            Event::Blob(BlobEvent::Error(error)) => {
                return Err(VersionKeyError::Transport(error.to_string()));
            }
            Event::Blob(event) => event,
            _ => return Err(VersionKeyError::Unexpected),
        };
        let ours = |stream_id: Ulid| Some(stream_id) == self.stream_id;
        match event {
            BlobEvent::ConnectionEstablished { stream_id } if self.stream_id.is_none() => {
                self.stream_id = Some(stream_id);
                let payload = (self.message.to_bytes()).map_err(|_| VersionKeyError::Unexpected)?;
                Ok(blob(BlobEffect::SendMessage { stream_id, payload }))
            }
            BlobEvent::MessageSent { stream_id } if ours(stream_id) && !self.sent => {
                self.sent = true;
                Ok(blob(BlobEffect::ReadMessage { stream_id }))
            }
            BlobEvent::MessageReceived { stream_id, payload } if ours(stream_id) && self.sent => {
                self.answer = Some(self.answer(&payload).ok_or(VersionKeyError::Unexpected)?);
                self.stream_id = None;
                Ok(blob(BlobEffect::CloseConnection { stream_id }))
            }
            BlobEvent::ConnectionClosed { .. } if self.stream_id.is_none() => self
                .answer
                .take()
                .map(Next::Done)
                .ok_or(VersionKeyError::Unexpected),
            _ => Err(VersionKeyError::Unexpected),
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        (self.stream_id.take())
            .map(|stream_id| {
                let close = BlobEffect::CloseConnection { stream_id };
                IoCall::Effect(Box::new(Effect::Blob(close)))
            })
            .into_iter()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM: Ulid = Ulid::from_bytes([5; 16]);

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn operation() -> VersionKeyOperation {
        let set = Ulid::from_bytes([2; 16]);
        VersionKeyOperation::new(
            node(),
            RealmId::from_bytes([3; 32]),
            set,
            Ulid::from_bytes([4; 16]),
        )
    }

    fn event(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    /// Runs the lookup through a member answering `payload`.
    fn ask(
        mut operation: VersionKeyOperation,
        payload: Vec<u8>,
    ) -> Result<MemberAnswer, VersionKeyError> {
        operation.first()?;
        operation.next(event(BlobEvent::ConnectionEstablished {
            stream_id: STREAM,
        }))?;
        operation.next(event(BlobEvent::MessageSent { stream_id: STREAM }))?;
        let received = BlobEvent::MessageReceived {
            stream_id: STREAM,
            payload,
        };
        let close = operation.next(event(received))?;
        assert_eq!(
            close,
            blob(BlobEffect::CloseConnection { stream_id: STREAM })
        );
        match operation.next(event(BlobEvent::ConnectionClosed { stream_id: STREAM }))? {
            Next::Done(answer) => Ok(answer),
            other => panic!("expected the answer, got {other:?}"),
        }
    }

    #[test]
    fn returns_member_key() {
        let payload = VersionReplicationMessage::VersionKeyResponse(Some("a/b".to_string()))
            .to_bytes()
            .unwrap();
        assert_eq!(
            ask(operation(), payload),
            Ok(MemberAnswer::Key(Some("a/b".to_string())))
        );
    }

    #[test]
    fn other_answer_fails() {
        let payload = VersionReplicationMessage::LocationSummaryDenied
            .to_bytes()
            .unwrap();
        let mut lookup = operation();
        lookup.first().unwrap();
        lookup
            .next(event(BlobEvent::ConnectionEstablished {
                stream_id: STREAM,
            }))
            .unwrap();
        lookup
            .next(event(BlobEvent::MessageSent { stream_id: STREAM }))
            .unwrap();
        let received = BlobEvent::MessageReceived {
            stream_id: STREAM,
            payload,
        };
        assert_eq!(
            lookup.next(event(received)),
            Err(VersionKeyError::Unexpected)
        );
        // The failed run closes the open connection on abort.
        let close = BlobEffect::CloseConnection { stream_id: STREAM };
        let close = IoCall::Effect(Box::new(Effect::Blob(close)));
        assert_eq!(lookup.abort(), vec![close]);
        assert!(lookup.abort().is_empty());
    }

    #[test]
    fn content_needs_answer() {
        let realm = RealmId::from_bytes([3; 32]);
        let content =
            || VersionKeyOperation::content(node(), realm, Ulid::from_bytes([2; 16]), [7; 32]);
        let versions = vec![(Ulid::from_bytes([4; 16]), "a".to_string())];
        let answer = VersionReplicationMessage::ContentResponse {
            versions: versions.clone(),
            more: false,
        };
        assert_eq!(
            ask(content(), answer.to_bytes().unwrap()),
            Ok(MemberAnswer::Versions {
                versions,
                more: false
            })
        );
        let key = VersionReplicationMessage::VersionKeyResponse(None);
        let mut lookup = content();
        lookup.first().unwrap();
        lookup
            .next(event(BlobEvent::ConnectionEstablished {
                stream_id: STREAM,
            }))
            .unwrap();
        lookup
            .next(event(BlobEvent::MessageSent { stream_id: STREAM }))
            .unwrap();
        let received = BlobEvent::MessageReceived {
            stream_id: STREAM,
            payload: key.to_bytes().unwrap(),
        };
        assert_eq!(
            lookup.next(event(received)),
            Err(VersionKeyError::Unexpected),
            "a key answer never stands for a content answer"
        );
    }

    #[test]
    fn foreign_stream_fails() {
        // An event of another stream is never taken for this lookup's answer.
        let mut lookup = operation();
        lookup.first().unwrap();
        lookup
            .next(event(BlobEvent::ConnectionEstablished {
                stream_id: STREAM,
            }))
            .unwrap();
        let foreign = BlobEvent::MessageSent {
            stream_id: Ulid::from_bytes([6; 16]),
        };
        assert_eq!(
            lookup.next(event(foreign)),
            Err(VersionKeyError::Unexpected)
        );
    }
}
