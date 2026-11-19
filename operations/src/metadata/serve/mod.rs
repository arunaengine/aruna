//! Answers the metadata requests peers send, as one operation the inbound stream handler drives.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod exports;
mod forwards;
mod held;
mod mirror;
mod paths;
mod pulls;
mod reads;
mod writes;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::heartbeat::Here;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::realm_vault::RealmSecretRef;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;

use self::exports::{ExportAsk, ExportServeOperation};
use self::forwards::{Forward, ForwardReply, forward_call};
use self::held::{HeldAsk, HeldReadOperation};
use self::mirror::{MirrorRequest, MirrorServeOperation};
use self::paths::{PathAsk, PathReadOperation};
use self::pulls::{PullAsk, PullServeOperation};
use self::reads::{CallerReadOperation, ReadReply};
use self::writes::WriteServeOperation;
use crate::distribution::imports::{HandedImport, StagingSpecOperation};
use crate::forward::transport::reject;
use crate::metadata::api::export_types::ExportMetadataRequest;
use crate::metadata::device_pull::SetListing;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::realm::promotion::{PromotionAnswer, PromotionRequest, ServePromotionOperation};
use crate::realm_vault::{SecretAnswer, ServeRequest, ServeSecretOperation};
use crate::replication::page::PageAnswer;
use crate::replication::page::deliver::ServePageOperation;
use crate::replication::queue::BlobQueueError;
use aruna_core::metadata::AuthToken;

/// What this node offers the requests it serves.
#[derive(Clone, Debug, PartialEq)]
pub struct ServeNode {
    /// This node and its realm, when it serves one.
    pub local: Option<(NodeId, RealmId)>,
    /// Whether the graph store runs here.
    pub graph: bool,
    /// Whether an object search may use the range indexes.
    pub indexed: bool,
    /// The largest metadata document this node exports.
    pub metadata_bytes: u64,
    /// This node's view of the realm, given only when [`needs_view`] asks for it.
    pub here: Option<Here>,
    /// This node's storage routing, given only when [`needs_view`] asks for it.
    pub routing: Option<NodeRouting>,
}

/// A reply and, for an export, the body sent after it.
pub type ServedReply = (MetadataTransportMessage, Option<Vec<u8>>);

/// The child a request waits for, which names the reply its outcome becomes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Waiting {
    /// A child that answers the reply itself.
    Reply,
    Export,
    Forward(ForwardReply),
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
    fn answer(self, result: IoResult) -> Result<ServedReply, IoError> {
        let reply = match self {
            Self::Export => return Ok(result.child::<ServedReply, IoError>()??),
            Self::Reply => result.child::<MetadataTransportMessage, IoError>()??,
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
            Self::Forward(reply) => reply.answer(result)?,
        };
        Ok((reply, None))
    }
}

/// Answers one metadata request `peer` sent to this node.
#[derive(Debug, PartialEq)]
pub struct ServeRequestOperation {
    peer: NodeId,
    node: ServeNode,
    message: Option<MetadataTransportMessage>,
    waiting: Option<Waiting>,
    /// The runner's time and the fresh ids of this run, for the ids a request needs.
    now_ms: u64,
    ids: IdMint,
}

type ServeTurn = Result<Next<ServedReply>, IoError>;

impl ServeRequestOperation {
    pub fn new(peer: NodeId, node: ServeNode, message: MetadataTransportMessage) -> Self {
        Self {
            peer,
            node,
            message: Some(message),
            waiting: None,
            now_ms: 0,
            ids: IdMint::new(0),
        }
    }

    fn call(&mut self, waiting: Waiting, call: IoCall) -> ServeTurn {
        self.waiting = Some(waiting);
        Ok(Next::Call(call))
    }

    fn path(&mut self, token: Option<AuthToken>, digest: [u8; 32], ask: PathAsk) -> ServeTurn {
        let read = PathReadOperation::new((self.peer, self.node.clone()), token, digest, ask);
        self.call(Waiting::Reply, IoCall::child(read))
    }

    fn mirror(&mut self, request: MirrorRequest) -> ServeTurn {
        let mirror = MirrorServeOperation::new(self.peer, self.node.local, request);
        self.call(Waiting::Reply, IoCall::child(mirror))
    }

    fn export(&mut self, digest: [u8; 32], ask: ExportAsk) -> ServeTurn {
        let export = ExportServeOperation::new((self.peer, self.node.clone()), digest, ask);
        self.call(Waiting::Export, IoCall::child(export))
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
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> ServeTurn {
        let peer = self.peer;
        let message = self.message.take().ok_or(IoError::Unexpected {
            expected: "a request",
            got: "none",
        })?;
        if let Some(reply) = ReadReply::of(&message) {
            let read = CallerReadOperation::new(peer, self.node.clone(), message, reply);
            return self.call(Waiting::Reply, IoCall::child(read));
        }
        let fresh = self.ids.mint(self.now_ms);
        let local = (self.node.local, self.node.here.as_ref());
        let message = match forward_call(peer, local, message, fresh) {
            Ok(Forward::Call(call, reply)) => return self.call(Waiting::Forward(reply), call),
            Ok(Forward::Done(reply)) => return Ok(Next::Done((reply, None))),
            Err(message) => message,
        };
        match message {
            MetadataTransportMessage::ForwardSyncPull { auth_token, pull } => {
                let serve = (self.peer, self.node.clone());
                let pull = PullServeOperation::new(serve, auth_token, PullAsk::Pull(pull));
                self.call(Waiting::Reply, IoCall::child(pull))
            }
            MetadataTransportMessage::ForwardPullStatus { auth_token, pull } => {
                let serve = (self.peer, self.node.clone());
                let poll = PullServeOperation::new(serve, auth_token, PullAsk::Status(pull));
                self.call(Waiting::Reply, IoCall::child(poll))
            }
            MetadataTransportMessage::ForwardListVersions {
                auth_token,
                bucket,
                prefix,
                cursor,
                limit,
            } => {
                let listing = SetListing {
                    bucket,
                    prefix,
                    cursor,
                    limit,
                };
                let serve = (self.peer, self.node.clone());
                let list = PullServeOperation::new(serve, auth_token, PullAsk::List(listing));
                self.call(Waiting::Reply, IoCall::child(list))
            }
            MetadataTransportMessage::ForwardReadDocument {
                auth_token,
                config_digest,
                document_id,
            } => {
                let peer = (self.peer, self.node.clone());
                let read = (auth_token, config_digest);
                let held = HeldReadOperation::new(peer, read, document_id, HeldAsk::Record);
                self.call(Waiting::Reply, IoCall::child(held))
            }
            MetadataTransportMessage::ForwardValidationStatus {
                auth_token,
                config_digest,
                document_id,
                revalidate,
            } => {
                let peer = (self.peer, self.node.clone());
                let read = (auth_token, config_digest);
                let ask = HeldAsk::Status { revalidate };
                let held = HeldReadOperation::new(peer, read, document_id, ask);
                self.call(Waiting::Reply, IoCall::child(held))
            }
            write @ (MetadataTransportMessage::ForwardCreateDocument { config_digest, .. }
            | MetadataTransportMessage::ForwardUpdateDocument { config_digest, .. }
            | MetadataTransportMessage::ForwardDeleteDocument { config_digest, .. }) => {
                let write =
                    WriteServeOperation::new((self.peer, self.node.clone()), config_digest, write);
                self.call(Waiting::Reply, IoCall::child(write))
            }
            MetadataTransportMessage::CreateSyncMirror {
                auth_token,
                source_group_id,
                relationship,
                extras,
            } => self.mirror(MirrorRequest {
                token: auth_token,
                relationship: *relationship,
                source_group: Some(source_group_id),
                extras: Some(extras),
                delete: false,
                rollback: false,
            }),
            MetadataTransportMessage::DeleteSyncMirror {
                auth_token,
                relationship,
                extras,
                rollback,
            } => self.mirror(MirrorRequest {
                token: auth_token,
                relationship: *relationship,
                source_group: None,
                extras: Some(extras),
                delete: true,
                rollback,
            }),
            MetadataTransportMessage::QueryDocument {
                auth_token,
                config_digest,
                document_id,
                sparql,
            } => {
                let ask = PathAsk::Query {
                    document_id,
                    sparql,
                };
                self.path(auth_token, config_digest, ask)
            }
            MetadataTransportMessage::ForwardPathLookup {
                auth_token,
                group_id,
                document_path,
                config_digest,
            } => {
                let ask = PathAsk::Lookup {
                    group_id,
                    path: document_path,
                };
                self.path(auth_token, config_digest, ask)
            }
            MetadataTransportMessage::ForwardPathResolution {
                auth_token,
                group_id,
                document_path,
                config_digest,
            } => {
                let ask = PathAsk::Resolve {
                    group_id,
                    path: document_path,
                };
                self.path(auth_token, config_digest, ask)
            }
            MetadataTransportMessage::ForwardExportDocument {
                auth_token,
                config_digest,
                document_id,
                view,
                metadata_bytes,
                limit,
                offset,
                after,
            } => {
                let request = ExportMetadataRequest {
                    document_id,
                    auth: None,
                    view,
                    limit,
                    offset,
                    after,
                };
                let ask = ExportAsk::Document {
                    token: auth_token,
                    request: Some(request),
                    metadata_bytes,
                };
                self.export(config_digest, ask)
            }
            MetadataTransportMessage::ForwardExportProfile {
                config_digest,
                profile_id,
                expected_revision,
            } => {
                let ask = ExportAsk::Profile {
                    profile_id,
                    expected_revision,
                };
                self.export(config_digest, ask)
            }
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
            _ => {
                let unexpected = reject("unexpected metadata control message");
                Ok(Next::Done((unexpected, None)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> ServeTurn {
        let waiting = self.waiting.take().ok_or(IoError::Unexpected {
            expected: "no answer",
            got: result.label(),
        })?;
        Ok(Next::Done(waiting.answer(result)?))
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;

pub use self::forwards::needs_view;
