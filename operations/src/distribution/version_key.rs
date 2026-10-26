//! Asks one member of a distributed bucket which key a version id names, from its version id
//! index, or which versions carry one content identity. Read-only; infrastructure peers only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::replication::protocol::VersionReplicationMessage;

#[derive(Debug, Error, PartialEq)]
pub enum VersionKeyError {
    #[error("the member did not answer: {0}")]
    Transport(String),
    #[error("the member sent an unexpected answer")]
    Unexpected,
    #[error("the version key lookup was aborted")]
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyState {
    Open,
    Send,
    Read,
    Close,
    Finish,
    Error,
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

/// One lookup at `node_id`.
#[derive(Debug, PartialEq)]
pub struct VersionKeyOperation {
    node_id: NodeId,
    message: VersionReplicationMessage,
    stream_id: Option<Ulid>,
    state: KeyState,
    output: Option<Result<MemberAnswer, VersionKeyError>>,
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
            state: KeyState::Open,
            output: None,
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

    fn fail(&mut self, error: VersionKeyError) -> Effects {
        self.output = Some(Err(error));
        self.state = KeyState::Error;
        match self.stream_id.take() {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }
}

impl Operation for VersionKeyOperation {
    type Output = MemberAnswer;
    type Error = VersionKeyError;

    fn start(&mut self) -> Effects {
        self.state = KeyState::Open;
        smallvec![Effect::Blob(BlobEffect::OpenConnection {
            node_id: self.node_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::Error(error)) = &event {
            return self.fail(VersionKeyError::Transport(error.to_string()));
        }
        match (self.state, event) {
            (KeyState::Open, Event::Blob(BlobEvent::ConnectionEstablished { stream_id })) => {
                self.stream_id = Some(stream_id);
                let Ok(payload) = self.message.to_bytes() else {
                    return self.fail(VersionKeyError::Unexpected);
                };
                self.state = KeyState::Send;
                smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
            }
            (KeyState::Send, Event::Blob(BlobEvent::MessageSent { stream_id }))
                if Some(stream_id) == self.stream_id =>
            {
                self.state = KeyState::Read;
                smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
            }
            (KeyState::Read, Event::Blob(BlobEvent::MessageReceived { stream_id, payload }))
                if Some(stream_id) == self.stream_id =>
            {
                let answer = match (
                    &self.message,
                    VersionReplicationMessage::from_bytes(&payload),
                ) {
                    (
                        VersionReplicationMessage::VersionKeyRequest { .. },
                        Ok(VersionReplicationMessage::VersionKeyResponse(key)),
                    ) => MemberAnswer::Key(key),
                    (
                        VersionReplicationMessage::ContentRequest { .. },
                        Ok(VersionReplicationMessage::ContentResponse { versions, more }),
                    ) => MemberAnswer::Versions { versions, more },
                    _ => return self.fail(VersionKeyError::Unexpected),
                };
                self.output = Some(Ok(answer));
                self.state = KeyState::Close;
                smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
            }
            (KeyState::Close, Event::Blob(BlobEvent::ConnectionClosed { stream_id }))
                if Some(stream_id) == self.stream_id =>
            {
                self.state = KeyState::Finish;
                smallvec![]
            }
            (KeyState::Finish | KeyState::Error, _) => smallvec![],
            _ => self.fail(VersionKeyError::Unexpected),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, KeyState::Finish | KeyState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(VersionKeyError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        self.output.get_or_insert(Err(VersionKeyError::Aborted));
        self.state = KeyState::Error;
        match self.stream_id.take() {
            Some(stream_id) => smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation() -> VersionKeyOperation {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let set = Ulid::from_bytes([2; 16]);
        VersionKeyOperation::new(
            node,
            RealmId::from_bytes([3; 32]),
            set,
            Ulid::from_bytes([4; 16]),
        )
    }

    fn answered(payload: Vec<u8>) -> VersionKeyOperation {
        let stream_id = Ulid::from_bytes([5; 16]);
        let mut operation = operation();
        operation.start();
        operation.step(Event::Blob(BlobEvent::ConnectionEstablished { stream_id }));
        operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        operation.step(Event::Blob(BlobEvent::MessageReceived {
            stream_id,
            payload,
        }));
        operation.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
        operation
    }

    #[test]
    fn returns_member_key() {
        let payload = VersionReplicationMessage::VersionKeyResponse(Some("a/b".to_string()))
            .to_bytes()
            .unwrap();
        let operation = answered(payload);
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Ok(MemberAnswer::Key(Some("a/b".to_string())))
        );
    }

    #[test]
    fn other_answer_fails() {
        let payload = VersionReplicationMessage::LocationSummaryDenied
            .to_bytes()
            .unwrap();
        assert_eq!(
            answered(payload).finalize(),
            Err(VersionKeyError::Unexpected)
        );
    }

    #[test]
    fn content_needs_answer() {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let realm = RealmId::from_bytes([3; 32]);
        let stream_id = Ulid::from_bytes([5; 16]);
        let ask = |payload: Vec<u8>| {
            let mut operation =
                VersionKeyOperation::content(node, realm, Ulid::from_bytes([2; 16]), [7; 32]);
            operation.start();
            operation.step(Event::Blob(BlobEvent::ConnectionEstablished { stream_id }));
            operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
            operation.step(Event::Blob(BlobEvent::MessageReceived {
                stream_id,
                payload,
            }));
            operation.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
            operation.finalize()
        };
        let versions = vec![(Ulid::from_bytes([4; 16]), "a".to_string())];
        let answer = VersionReplicationMessage::ContentResponse {
            versions: versions.clone(),
            more: false,
        };
        assert_eq!(
            ask(answer.to_bytes().unwrap()),
            Ok(MemberAnswer::Versions {
                versions,
                more: false
            })
        );
        let key = VersionReplicationMessage::VersionKeyResponse(None);
        assert_eq!(
            ask(key.to_bytes().unwrap()),
            Err(VersionKeyError::Unexpected),
            "a key answer never stands for a content answer"
        );
    }

    #[test]
    fn foreign_stream_fails() {
        // An event of another stream is never taken for this lookup's answer.
        let mut operation = operation();
        operation.start();
        operation.step(Event::Blob(BlobEvent::ConnectionEstablished {
            stream_id: Ulid::from_bytes([5; 16]),
        }));
        operation.step(Event::Blob(BlobEvent::MessageSent {
            stream_id: Ulid::from_bytes([6; 16]),
        }));
        assert_eq!(operation.finalize(), Err(VersionKeyError::Unexpected));
    }
}
