//! Serves the document query and the path reads a peer forwards, each under a realm config
//! that must match the sender's.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::metadata::{AuthToken, MetadataError, MetadataQueryResults};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::ServeNode;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::path::{LocalPathOperation, LookupPathOperation};
use crate::metadata::api::path_types::{MetadataLookupRequest, MetadataLookupResult};
use crate::metadata::forward::holders::holds_metadata_id;
use crate::metadata::forward::write::read_error;
use crate::metadata::local_query::LocalQueryOperation;
use crate::metadata::read_peer::ReadPeerOperation;
use crate::realm::load_config::config_unavailable;
use aruna_core::metadata::protocol::{
    MetadataPathCandidate, MetadataPathResolution, MetadataReadError, MetadataTransportMessage,
};

/// The realm config read; a failed read is logged and counts as none.
pub(super) fn read_config(result: IoResult) -> Result<Option<RealmConfigDocument>, IoError> {
    let config = result.child::<RealmConfigDocument, GetConfigError>()?;
    Ok(config.inspect_err(config_unavailable).ok())
}

/// Whether the read config matches the digest the sender saw.
pub(super) fn digest_matches(config: Option<&RealmConfigDocument>, digest: &[u8; 32]) -> bool {
    config.and_then(|config| config.digest().ok()).as_ref() == Some(digest)
}

pub(super) fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

/// A forwarded document query or path read.
#[derive(Debug, PartialEq)]
pub(crate) enum PathAsk {
    Query {
        document_id: Ulid,
        sparql: String,
    },
    /// The candidates for a path; the sender vouches for the caller.
    Lookup {
        group_id: GroupId,
        path: String,
    },
    Resolve {
        group_id: GroupId,
        path: String,
    },
}

#[derive(Debug, PartialEq)]
enum PathStage {
    Config,
    Caller,
    Record(Option<AuthContext>),
    Gate(Option<AuthContext>),
    Read,
    /// The config is read again after a path read, which only stands if it still matches.
    Recheck(Box<MetadataTransportMessage>),
}

/// Answers a forwarded document query or path read; a document query checks the config and
/// the holder first, a path read checks the caller first and the config around its read.
#[derive(Debug, PartialEq)]
pub(crate) struct PathReadOperation {
    peer: NodeId,
    node: ServeNode,
    token: Option<AuthToken>,
    digest: [u8; 32],
    ask: PathAsk,
    stage: PathStage,
}

type PathTurn = Result<Next<MetadataTransportMessage>, IoError>;

impl PathReadOperation {
    pub(crate) fn new(
        (peer, node): (NodeId, ServeNode),
        token: Option<AuthToken>,
        digest: [u8; 32],
        ask: PathAsk,
    ) -> Self {
        Self {
            peer,
            node,
            token,
            digest,
            ask,
            stage: PathStage::Config,
        }
    }

    fn refused(&self, error: MetadataReadError) -> PathTurn {
        Ok(Next::Done(match self.ask {
            PathAsk::Query { .. } => {
                MetadataTransportMessage::DocumentQueryResults { result: Err(error) }
            }
            PathAsk::Lookup { .. } => {
                MetadataTransportMessage::ForwardedPathLookup { result: Err(error) }
            }
            PathAsk::Resolve { .. } => {
                MetadataTransportMessage::ForwardedPathResolution { result: Err(error) }
            }
        }))
    }

    fn call(&mut self, stage: PathStage, call: IoCall) -> PathTurn {
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn caller(&mut self, realm_id: RealmId) -> PathTurn {
        let trusted = matches!(self.ask, PathAsk::Lookup { .. });
        let check = ReadPeerOperation::new((self.peer, realm_id), self.token.take(), trusted);
        self.call(PathStage::Caller, IoCall::child(check))
    }

    fn read(&mut self, (node, realm_id): (NodeId, RealmId), auth: Option<AuthContext>) -> PathTurn {
        let call = match &mut self.ask {
            PathAsk::Query { .. } => return self.refused(MetadataReadError::Unavailable),
            PathAsk::Lookup { group_id, path } => {
                let local =
                    LocalPathOperation::new(realm_id, (*group_id, path.as_str()), auth, Some(node));
                IoCall::child(local)
            }
            PathAsk::Resolve { group_id, path } => {
                let request = MetadataLookupRequest {
                    group_id: *group_id,
                    document_path: std::mem::take(path),
                    auth,
                };
                IoCall::child(LookupPathOperation::local(realm_id, request, Some(node)))
            }
        };
        self.call(PathStage::Read, call)
    }

    fn answer(&mut self, result: IoResult) -> Result<MetadataTransportMessage, IoError> {
        Ok(match &self.ask {
            PathAsk::Query { .. } => MetadataTransportMessage::DocumentQueryResults {
                result: (result.child::<MetadataQueryResults, MetadataError>()?)
                    .map_err(|_| MetadataReadError::Unavailable),
            },
            PathAsk::Lookup { .. } => MetadataTransportMessage::ForwardedPathLookup {
                result: (result.child::<Vec<MetadataPathCandidate>, MetadataApiError>()?)
                    .map_err(read_error),
            },
            PathAsk::Resolve { .. } => MetadataTransportMessage::ForwardedPathResolution {
                result: (result.child::<MetadataLookupResult, MetadataApiError>()?)
                    .map(|found| {
                        Box::new(MetadataPathResolution {
                            winner: found.winner,
                            conflicts: found.conflicts,
                        })
                    })
                    .map_err(read_error),
            },
        })
    }
}

impl Operation for PathReadOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> PathTurn {
        let Some((_, realm_id)) = self.node.local else {
            return self.refused(MetadataReadError::Unavailable);
        };
        match self.ask {
            PathAsk::Query { .. } => self.call(PathStage::Config, config_call(realm_id)),
            PathAsk::Lookup { .. } | PathAsk::Resolve { .. } => self.caller(realm_id),
        }
    }

    fn next(&mut self, result: IoResult) -> PathTurn {
        let (node, realm_id) = self.node.local.ok_or(IoError::Unexpected {
            expected: "this node's realm",
            got: "none",
        })?;
        match std::mem::replace(&mut self.stage, PathStage::Read) {
            PathStage::Config => {
                let config = read_config(result)?;
                let PathAsk::Query { document_id, .. } = self.ask else {
                    return self.refused(MetadataReadError::Unavailable);
                };
                let held = (config.as_ref())
                    .is_some_and(|config| holds_metadata_id(config, realm_id, node, document_id));
                if !digest_matches(config.as_ref(), &self.digest) || !held || !self.node.graph {
                    return self.refused(MetadataReadError::Unavailable);
                }
                self.caller(realm_id)
            }
            PathStage::Caller => match result.child::<Option<AuthContext>, MetadataReadError>()? {
                Err(error) => self.refused(error),
                Ok(auth) => match &self.ask {
                    PathAsk::Query { document_id, .. } => {
                        let request = GetVisibleRequest {
                            document_id: *document_id,
                            auth: auth.clone(),
                        };
                        let record = VisibleRecordOperation::new(realm_id, request);
                        self.call(PathStage::Record(auth), IoCall::child(record))
                    }
                    _ => self.call(PathStage::Gate(auth), config_call(realm_id)),
                },
            },
            PathStage::Record(auth) => {
                let record = result.child::<MetadataRegistryRecord, MetadataApiError>()?;
                let record = match record {
                    Ok(record) => record,
                    Err(error) => return self.refused(read_error(error)),
                };
                let PathAsk::Query { sparql, .. } = &mut self.ask else {
                    return self.refused(MetadataReadError::Unavailable);
                };
                let graphs = Some(vec![record.graph_iri]);
                let query =
                    LocalQueryOperation::new((realm_id, auth), graphs, std::mem::take(sparql));
                self.call(PathStage::Read, IoCall::child(query))
            }
            PathStage::Gate(auth) => {
                match digest_matches(read_config(result)?.as_ref(), &self.digest) {
                    true => self.read((node, realm_id), auth),
                    false => self.refused(MetadataReadError::Unavailable),
                }
            }
            PathStage::Read => {
                let reply = self.answer(result)?;
                match self.ask {
                    PathAsk::Query { .. } => Ok(Next::Done(reply)),
                    _ => self.call(PathStage::Recheck(Box::new(reply)), config_call(realm_id)),
                }
            }
            PathStage::Recheck(reply) => {
                match digest_matches(read_config(result)?.as_ref(), &self.digest) {
                    true => Ok(Next::Done(*reply)),
                    false => self.refused(MetadataReadError::Unavailable),
                }
            }
        }
    }
}
