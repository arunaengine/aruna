//! Serves the record reads a peer forwards to a holder: the record itself and its Profile
//! validation status, each under a realm config that must match the sender's.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::{AuthToken, MetadataError, ProfileValidationStatus};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::ServeNode;
use super::paths::{config_call, digest_matches, read_config};
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::{read_error, reject};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};
use crate::metadata::profile::validation::{RevalidateOperation, ValidationStatusOperation};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage as Message};
use crate::metadata::read_peer::ReadPeerOperation;

/// A forwarded record read; a status read may ask for a fresh validation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum HeldAsk {
    Record,
    Status { revalidate: bool },
}

#[derive(Debug, PartialEq)]
enum HeldStage {
    Config,
    Caller(Box<RealmConfigDocument>),
    Live(Option<AuthContext>),
    Readable(Box<MetadataRegistryRecord>),
    Read,
}

/// Answers a forwarded record or status read for the caller the token names; this node must
/// hold the document.
#[derive(Debug, PartialEq)]
pub(crate) struct HeldReadOperation {
    peer: NodeId,
    node: ServeNode,
    token: Option<AuthToken>,
    digest: [u8; 32],
    document_id: Ulid,
    ask: HeldAsk,
    stage: HeldStage,
}

type HeldTurn = Result<Next<Message>, IoError>;

impl HeldReadOperation {
    pub(crate) fn new(
        (peer, node): (NodeId, ServeNode),
        (token, digest): (Option<AuthToken>, [u8; 32]),
        document_id: Ulid,
        ask: HeldAsk,
    ) -> Self {
        Self {
            peer,
            node,
            token,
            digest,
            document_id,
            ask,
            stage: HeldStage::Config,
        }
    }

    fn refused(&self, error: MetadataReadError) -> HeldTurn {
        Ok(Next::Done(match self.ask {
            HeldAsk::Record => Message::ForwardedRead { result: Err(error) },
            HeldAsk::Status { .. } => Message::ForwardedValidationStatus { result: Err(error) },
        }))
    }

    fn call(&mut self, stage: HeldStage, call: IoCall) -> HeldTurn {
        self.stage = stage;
        Ok(Next::Call(call))
    }
}

impl Operation for HeldReadOperation {
    type Output = Message;
    type Error = IoError;

    fn first(&mut self) -> HeldTurn {
        match self.node.local {
            Some((_, realm_id)) => self.call(HeldStage::Config, config_call(realm_id)),
            None => self.refused(MetadataReadError::Unavailable),
        }
    }

    fn next(&mut self, result: IoResult) -> HeldTurn {
        let (node, realm_id): (NodeId, RealmId) = self.node.local.ok_or(IoError::Unexpected {
            expected: "this node's realm",
            got: "none",
        })?;
        match std::mem::replace(&mut self.stage, HeldStage::Read) {
            HeldStage::Config => {
                let config = match read_config(result)? {
                    Some(config) if digest_matches(Some(&config), &self.digest) => config,
                    _ => return self.refused(MetadataReadError::Unavailable),
                };
                if !self.node.graph {
                    return match self.ask {
                        HeldAsk::Record => Ok(Next::Done(reject(
                            "forwarded metadata read needs a metadata handle",
                        ))),
                        HeldAsk::Status { .. } => self.refused(MetadataReadError::Unavailable),
                    };
                }
                let check = ReadPeerOperation::new((self.peer, realm_id), self.token.take(), false);
                self.call(HeldStage::Caller(Box::new(config)), IoCall::child(check))
            }
            HeldStage::Caller(config) => {
                let auth = match result.child::<Option<AuthContext>, MetadataReadError>()? {
                    Ok(auth) => auth,
                    Err(error) => return self.refused(error),
                };
                if !holds_metadata_id(&config, realm_id, node, self.document_id) {
                    return self.refused(MetadataReadError::Unavailable);
                }
                match self.ask {
                    HeldAsk::Record => {
                        let request = GetVisibleRequest {
                            document_id: self.document_id,
                            auth,
                        };
                        let record = VisibleRecordOperation::new(realm_id, request);
                        self.call(HeldStage::Read, IoCall::child(record))
                    }
                    HeldAsk::Status { .. } => {
                        let live = LiveRecordOperation::new(self.document_id);
                        self.call(HeldStage::Live(auth), IoCall::child(live))
                    }
                }
            }
            HeldStage::Live(auth) => {
                let live = result.child::<Option<MetadataRegistryRecord>, LiveRecordsError>()?;
                let record = match live {
                    Ok(Some(record)) => record,
                    Ok(None) => return self.refused(read_error(MetadataApiError::NotFound)),
                    Err(error) => return self.refused(read_error(error.into())),
                };
                let readable = RecordReadableOperation::new(realm_id, auth, record.clone());
                self.call(
                    HeldStage::Readable(Box::new(record)),
                    IoCall::child(readable),
                )
            }
            HeldStage::Readable(record) => {
                if let Err(error) = result.child::<(), MetadataApiError>()? {
                    return self.refused(read_error(error));
                }
                let call = match self.ask {
                    HeldAsk::Status { revalidate: true } => {
                        IoCall::child(RevalidateOperation::new(Some(node), *record))
                    }
                    _ => IoCall::child(ValidationStatusOperation::new(*record)),
                };
                self.call(HeldStage::Read, call)
            }
            HeldStage::Read => Ok(Next::Done(match self.ask {
                HeldAsk::Record => Message::ForwardedRead {
                    result: (result.child::<MetadataRegistryRecord, MetadataApiError>()?)
                        .map(Box::new)
                        .map_err(read_error),
                },
                HeldAsk::Status { .. } => Message::ForwardedValidationStatus {
                    result: (result.child::<ProfileValidationStatus, MetadataError>()?)
                        .map(Box::new)
                        .map_err(|_| MetadataReadError::Unavailable),
                },
            })),
        }
    }
}
