//! Serves the document and Profile exports a peer forwards: the export answers with its length
//! and the encoded export follows as the body.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

use super::paths::{config_call, digest_matches, read_config};
use super::{ServeNode, ServedReply};
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::read_error;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::{ExportMetadataRequest, ExportMetadataResult};
use crate::metadata::forward::profile_export::ProfileLocalOperation;
use crate::metadata::forward::read::admits_profile_peer;
use crate::metadata::forward::routed::export::{ExportWithinOperation, ensure_export_limit};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, metadata_body_limit};
use crate::metadata::read_peer::ReadPeerOperation;

/// A forwarded export.
#[derive(Debug, PartialEq)]
pub(crate) enum ExportAsk {
    /// A document export for the caller the token names, within `request`'s bounds.
    Document {
        token: Option<AuthToken>,
        request: Option<ExportMetadataRequest>,
        metadata_bytes: u64,
    },
    /// A Profile document; the channel carries no caller, so only the peer is admitted.
    Profile {
        profile_id: Ulid,
        expected_revision: Ulid,
    },
}

#[derive(Debug, PartialEq)]
enum ExportStage {
    Config,
    Caller(Box<RealmConfigDocument>),
    /// The export runs within this many metadata bytes.
    Export(u64),
}

/// Answers a forwarded export; the realm config must match the sender's digest and this node
/// must hold the document.
#[derive(Debug, PartialEq)]
pub(crate) struct ExportServeOperation {
    peer: NodeId,
    node: ServeNode,
    digest: [u8; 32],
    ask: ExportAsk,
    stage: ExportStage,
}

type ExportTurn = Result<Next<ServedReply>, IoError>;

fn refused(error: MetadataReadError) -> ExportTurn {
    Ok(Next::Done((
        MetadataTransportMessage::ForwardedExport { result: Err(error) },
        None,
    )))
}

// The export's length as the reply and its encoding as the body, within the body limit.
fn exported(export: &ExportMetadataResult, metadata_bytes: u64) -> ExportTurn {
    match postcard::to_allocvec(export) {
        Ok(bytes) if bytes.len() as u64 <= metadata_body_limit(metadata_bytes) => {
            let result = Ok(bytes.len() as u64);
            let reply = MetadataTransportMessage::ForwardedExport { result };
            Ok(Next::Done((reply, Some(bytes))))
        }
        _ => refused(MetadataReadError::Unavailable),
    }
}

impl ExportServeOperation {
    pub(crate) fn new((peer, node): (NodeId, ServeNode), digest: [u8; 32], ask: ExportAsk) -> Self {
        Self {
            peer,
            node,
            digest,
            ask,
            stage: ExportStage::Config,
        }
    }

    fn call(&mut self, stage: ExportStage, call: IoCall) -> ExportTurn {
        self.stage = stage;
        Ok(Next::Call(call))
    }
}

impl Operation for ExportServeOperation {
    type Output = ServedReply;
    type Error = IoError;

    fn first(&mut self) -> ExportTurn {
        match self.node.local {
            Some((_, realm_id)) => self.call(ExportStage::Config, config_call(realm_id)),
            None => refused(MetadataReadError::Unavailable),
        }
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        let (node, realm_id) = self.node.local.ok_or(IoError::Unexpected {
            expected: "this node's realm",
            got: "none",
        })?;
        let local_bytes = self.node.metadata_bytes;
        match std::mem::replace(&mut self.stage, ExportStage::Export(local_bytes)) {
            ExportStage::Config => {
                let config = read_config(result)?;
                let config = match config {
                    Some(config) if digest_matches(Some(&config), &self.digest) => config,
                    _ => return refused(MetadataReadError::Unavailable),
                };
                match &mut self.ask {
                    ExportAsk::Document { token, .. } => {
                        if !self.node.graph {
                            return refused(MetadataReadError::Unavailable);
                        }
                        let check =
                            ReadPeerOperation::new((self.peer, realm_id), token.take(), false);
                        self.call(ExportStage::Caller(Box::new(config)), IoCall::child(check))
                    }
                    ExportAsk::Profile {
                        profile_id,
                        expected_revision,
                    } => {
                        if !admits_profile_peer(&config, self.peer, realm_id) {
                            return refused(MetadataReadError::Forbidden);
                        }
                        if !holds_metadata_id(&config, realm_id, node, *profile_id) {
                            return refused(MetadataReadError::Unavailable);
                        }
                        let local =
                            ProfileLocalOperation::new(realm_id, *profile_id, *expected_revision);
                        self.call(ExportStage::Export(local_bytes), IoCall::child(local))
                    }
                }
            }
            ExportStage::Caller(config) => {
                let auth = match result.child::<Option<AuthContext>, MetadataReadError>()? {
                    Ok(auth) => auth,
                    Err(error) => return refused(error),
                };
                let ExportAsk::Document {
                    request,
                    metadata_bytes,
                    ..
                } = &mut self.ask
                else {
                    return refused(MetadataReadError::Unavailable);
                };
                let Some(mut request) = request.take() else {
                    return refused(MetadataReadError::Unavailable);
                };
                if !holds_metadata_id(&config, realm_id, node, request.document_id) {
                    return refused(MetadataReadError::Unavailable);
                }
                let bytes = (*metadata_bytes).min(local_bytes);
                request.auth = auth;
                let export = ExportWithinOperation::new(realm_id, request, bytes);
                self.call(ExportStage::Export(bytes), IoCall::child(export))
            }
            ExportStage::Export(bytes) => match self.ask {
                ExportAsk::Document { .. } => {
                    match result.child::<ExportMetadataResult, MetadataApiError>()? {
                        Ok(export) => exported(&export, bytes),
                        Err(error) => refused(read_error(error)),
                    }
                }
                ExportAsk::Profile { .. } => {
                    match result.child::<ExportMetadataResult, MetadataReadError>()? {
                        Ok(export) => match ensure_export_limit(&export, bytes) {
                            Ok(()) => exported(&export, bytes),
                            Err(error) => refused(read_error(error)),
                        },
                        Err(error) => refused(error),
                    }
                }
            },
        }
    }
}
