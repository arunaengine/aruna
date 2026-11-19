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

use self::reads::{CallerRead, CallerReadOperation};
use crate::distribution::imports::{HandedImport, StagingSpecOperation};
use crate::metadata::identifier_lookup::IdentifierQuery;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::realm::promotion::{PromotionAnswer, PromotionRequest, ServePromotionOperation};
use crate::realm_vault::{SecretAnswer, ServeRequest, ServeSecretOperation};
use crate::replication::page::PageAnswer;
use crate::replication::page::deliver::ServePageOperation;
use crate::replication::queue::BlobQueueError;

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

/// Answers one metadata request `peer` sent to this node; `local` names this node and its
/// realm when it serves one.
#[derive(Debug, PartialEq)]
pub struct ServeRequestOperation {
    peer: NodeId,
    local: Option<(NodeId, RealmId)>,
    message: Option<MetadataTransportMessage>,
    waiting: Option<Waiting>,
}

type ServeTurn = Result<Next<ServedReply>, ServeError>;

impl ServeRequestOperation {
    pub fn new(
        peer: NodeId,
        local: Option<(NodeId, RealmId)>,
        message: MetadataTransportMessage,
    ) -> Self {
        Self {
            peer,
            local,
            message: Some(message),
            waiting: None,
        }
    }

    fn call(&mut self, waiting: Waiting, call: IoCall) -> ServeTurn {
        self.waiting = Some(waiting);
        Ok(Next::Call(call))
    }

    fn read(
        &mut self,
        token: Option<aruna_core::metadata::AuthToken>,
        read: CallerRead,
    ) -> ServeTurn {
        let caller = (self.peer, self.local.map(|(_, realm_id)| realm_id));
        let read = CallerReadOperation::new(caller, token, read);
        self.call(Waiting::Read, IoCall::child(read))
    }

    // A realm request needs this node's realm; without one it answers unavailable.
    fn realm(&mut self, ask: RealmAsk, serve: impl FnOnce(RealmId, NodeId) -> IoCall) -> ServeTurn {
        match self.local {
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
        match message {
            MetadataTransportMessage::LookupIdentifier {
                auth_token,
                kind,
                value,
                endpoint,
            } => {
                let query = IdentifierQuery {
                    kind,
                    value,
                    endpoint,
                };
                self.read(auth_token, CallerRead::Identifier(query))
            }
            MetadataTransportMessage::DataLinks {
                auth_token,
                request,
            } => self.read(auth_token, CallerRead::Links(request)),
            MetadataTransportMessage::BucketOwner { auth_token, bucket } => {
                self.read(auth_token, CallerRead::Owner(bucket))
            }
            MetadataTransportMessage::SetPreview {
                auth_token,
                set_id,
                limit,
            } => self.read(auth_token, CallerRead::Preview { set_id, limit }),
            MetadataTransportMessage::StagingSpec {
                transfer_id,
                job_id,
            } => {
                let spec = StagingSpecOperation::new(peer, (transfer_id, job_id));
                self.call(Waiting::Spec, IoCall::child(spec))
            }
            MetadataTransportMessage::ReferencePage { page } => {
                let serve = ServePageOperation::new(self.local, peer, *page);
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
