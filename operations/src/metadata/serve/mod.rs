//! Answers the metadata requests peers send, as one operation the inbound stream handler drives.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod reads;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::realm_vault::RealmSecretRef;
use aruna_core::structs::identity::realm::RealmId;
use thiserror::Error;

use self::reads::{CallerReadOperation, ReadReply};
use crate::distribution::imports::{HandedImport, StagingSpecOperation};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::realm::promotion::{PromotionAnswer, PromotionRequest, ServePromotionOperation};
use crate::realm_vault::{SecretAnswer, ServeRequest, ServeSecretOperation};
use crate::replication::page::PageAnswer;
use crate::replication::page::deliver::ServePageOperation;
use crate::replication::queue::BlobQueueError;

/// What this node offers the requests it serves.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServeNode {
    /// This node and its realm, when it serves one.
    pub local: Option<(NodeId, RealmId)>,
    /// Whether the graph store runs here.
    pub graph: bool,
    /// Whether an object search may use the range indexes.
    pub indexed: bool,
}

/// A reply and, for an export, the body sent after it.
pub type ServedReply = (MetadataTransportMessage, Option<Vec<u8>>);

#[derive(Debug, Error, PartialEq)]
pub enum ServeError {
    /// The request is one this operation does not answer.
    #[error("the request is served elsewhere")]
    Unserved(Box<MetadataTransportMessage>),
    #[error(transparent)]
    Io(#[from] IoError),
}

/// The child a request waits for, which names the reply its outcome becomes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Waiting {
    Read,
    Spec,
    Page,
    Realm(RealmAsk),
}

/// A request only a node serving a realm answers.
#[derive(Clone, Copy, Debug, PartialEq)]
enum RealmAsk {
    KeyStored,
    Secret,
    Setting,
    Promotion,
}

impl RealmAsk {
    fn refused(self, error: MetadataReadError) -> MetadataTransportMessage {
        match self {
            Self::KeyStored => MetadataTransportMessage::RealmKeyStored { result: Err(error) },
            Self::Secret => MetadataTransportMessage::RealmSecretResult { result: Err(error) },
            Self::Setting => MetadataTransportMessage::OpenSettingResult { result: Err(error) },
            Self::Promotion => MetadataTransportMessage::PromotionReady { result: Err(error) },
        }
    }

    fn answer(self, result: IoResult) -> Result<MetadataTransportMessage, IoError> {
        Ok(match self {
            Self::KeyStored => MetadataTransportMessage::RealmKeyStored {
                result: (result.child::<PromotionAnswer, MetadataReadError>()?)
                    .and_then(|answer| answer)
                    .map(|_| ()),
            },
            Self::Secret => MetadataTransportMessage::RealmSecretResult {
                result: (result.child::<SecretAnswer, MetadataReadError>()?)
                    .and_then(|answer| answer),
            },
            Self::Setting => MetadataTransportMessage::OpenSettingResult {
                result: (result.child::<SecretAnswer, MetadataReadError>()?)
                    .and_then(|answer| answer),
            },
            Self::Promotion => MetadataTransportMessage::PromotionReady {
                result: (result.child::<PromotionAnswer, MetadataReadError>()?)
                    .and_then(|answer| answer),
            },
        })
    }
}

impl Waiting {
    fn answer(self, result: IoResult) -> Result<MetadataTransportMessage, ServeError> {
        Ok(match self {
            Self::Read => result.child::<MetadataTransportMessage, IoError>()??,
            Self::Spec => MetadataTransportMessage::StagingSpecResult {
                result: (result.child::<HandedImport, MetadataReadError>()?).map(Box::new),
            },
            Self::Page => MetadataTransportMessage::ReferencePageAnswer {
                answer: (result.child::<PageAnswer, BlobQueueError>()?).unwrap_or_else(|error| {
                    PageAnswer::Refused {
                        reason: error.to_string(),
                        retry: true,
                    }
                }),
            },
            Self::Realm(ask) => ask.answer(result)?,
        })
    }
}

/// Answers one metadata request `peer` sent to this node.
#[derive(Debug, PartialEq)]
pub struct ServeRequestOperation {
    peer: NodeId,
    node: ServeNode,
    message: Option<MetadataTransportMessage>,
    waiting: Option<Waiting>,
}

type ServeTurn = Result<Next<ServedReply>, ServeError>;

impl ServeRequestOperation {
    pub fn new(peer: NodeId, node: ServeNode, message: MetadataTransportMessage) -> Self {
        Self {
            peer,
            node,
            message: Some(message),
            waiting: None,
        }
    }

    fn call(&mut self, waiting: Waiting, call: IoCall) -> ServeTurn {
        self.waiting = Some(waiting);
        Ok(Next::Call(call))
    }

    // A realm request needs this node's realm; without one it answers unavailable.
    fn realm(&mut self, ask: RealmAsk, serve: impl FnOnce(RealmId, NodeId) -> IoCall) -> ServeTurn {
        match self.node.local {
            Some((node, realm_id)) => self.call(Waiting::Realm(ask), serve(realm_id, node)),
            None => Ok(Next::Done((
                ask.refused(MetadataReadError::Unavailable),
                None,
            ))),
        }
    }
}

impl Operation for ServeRequestOperation {
    type Output = ServedReply;
    type Error = ServeError;

    fn first(&mut self) -> ServeTurn {
        let peer = self.peer;
        let message = self.message.take().ok_or(IoError::Unexpected {
            expected: "a request",
            got: "none",
        })?;
        if let Some(reply) = ReadReply::of(&message) {
            let read = CallerReadOperation::new(peer, self.node, message, reply);
            return self.call(Waiting::Read, IoCall::child(read));
        }
        match message {
            MetadataTransportMessage::StagingSpec {
                transfer_id,
                job_id,
            } => {
                let spec = StagingSpecOperation::new(peer, (transfer_id, job_id));
                self.call(Waiting::Spec, IoCall::child(spec))
            }
            MetadataTransportMessage::ReferencePage { page } => {
                let serve = ServePageOperation::new(self.node.local, peer, *page);
                self.call(Waiting::Page, IoCall::child(serve))
            }
            MetadataTransportMessage::HandRealmKey { key } => {
                let request = PromotionRequest::HandKey(key);
                self.realm(RealmAsk::KeyStored, |realm_id, node| {
                    IoCall::child(ServePromotionOperation::new(realm_id, node, peer, request))
                })
            }
            MetadataTransportMessage::PromotionCheck => {
                self.realm(RealmAsk::Promotion, |realm_id, node| {
                    let request = PromotionRequest::Check;
                    IoCall::child(ServePromotionOperation::new(realm_id, node, peer, request))
                })
            }
            MetadataTransportMessage::RealmSecret { entry, revision } => {
                let request = ServeRequest::Secret(RealmSecretRef { entry, revision });
                self.realm(RealmAsk::Secret, |realm_id, node| {
                    IoCall::child(ServeSecretOperation::new(realm_id, node, peer, request))
                })
            }
            MetadataTransportMessage::OpenSetting { target, bytes } => {
                let bytes = bytes.expose().to_vec();
                let request = ServeRequest::Setting { target, bytes };
                self.realm(RealmAsk::Setting, |realm_id, node| {
                    IoCall::child(ServeSecretOperation::new(realm_id, node, peer, request))
                })
            }
            message => Err(ServeError::Unserved(Box::new(message))),
        }
    }

    fn next(&mut self, result: IoResult) -> ServeTurn {
        let waiting = self.waiting.take().ok_or(IoError::Unexpected {
            expected: "no answer",
            got: result.label(),
        })?;
        Ok(Next::Done((waiting.answer(result)?, None)))
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
