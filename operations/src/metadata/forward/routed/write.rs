//! Routes metadata deletes and device edits to the nodes that hold the document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::{Event, PeerRequestError};
use aruna_core::metadata::{AuthToken, MetadataBatch, MetadataBatchSource, MetadataEffect};
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;
use ulid::Ulid;

use super::super::write::batch_refusal;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::forward::replay::routed_record_matches;
use crate::forward::routing::{
    ForwardHoldersOperation, HolderWriteError, distinct_holders, holder_intersection,
};
use crate::forward::transport::{MetadataWriteError, unexpected_response};
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::delete_document::{DeleteDocumentError, DeleteDocumentOperation};
use crate::metadata::persistent_id::forward::pid_authority_node;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};
use crate::realm::load_config::config_unavailable;

/// How long one holder may take to answer a forwarded device edit, which it merges and commits.
const BATCH_FORWARD_TIMEOUT: Duration = Duration::from_secs(120);

fn undeliverable(reason: impl ToString) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(reason.to_string())
}

/// The realm config a routed write needs to find the document's holders.
fn routing_config(result: IoResult) -> Result<RealmConfigDocument, MetadataWriteError> {
    (result.child::<RealmConfigDocument, GetConfigError>()?).map_err(|error| {
        config_unavailable(&error);
        undeliverable("realm placement config is unavailable")
    })
}

#[derive(Debug, PartialEq)]
enum DeleteStage {
    Config,
    Local,
    Forward,
}

/// Deletes one document: here when this node holds it and is the persistent id authority,
/// otherwise at its holders. A known record pins the holders to its frozen holder set.
#[derive(Debug, PartialEq)]
pub struct RouteDeleteOperation {
    actor: Actor,
    record: Option<MetadataRegistryRecord>,
    document_id: Ulid,
    auth_token: Option<AuthToken>,
    local_node: Option<NodeId>,
    stage: DeleteStage,
}

impl RouteDeleteOperation {
    /// `local_node` is this node on the network, if it has one.
    pub fn new(
        (actor, local_node): (Actor, Option<NodeId>),
        record: Option<MetadataRegistryRecord>,
        document_id: Ulid,
        auth_token: Option<AuthToken>,
    ) -> Self {
        Self {
            actor,
            record,
            document_id,
            auth_token,
            local_node,
            stage: DeleteStage::Config,
        }
    }

    fn configured(&mut self, config: RealmConfigDocument) -> Result<Next<()>, MetadataWriteError> {
        let realm_id = self.actor.realm_id;
        let document_id = self.document_id;
        let group_id = self.record.as_ref().map(|record| record.group_id);
        let placement =
            resolve_metadata_id(&config, realm_id, group_id, document_id).map_err(undeliverable)?;
        let config_digest = config.digest().map_err(undeliverable)?;
        if (self.record.as_ref()).is_some_and(|record| {
            !routed_record_matches(&config, realm_id, document_id, &placement, record)
        }) {
            return Err(undeliverable(
                "local metadata registry record does not match the routed document",
            ));
        }
        let current_holders = resolve_shard_holders(&config, &placement);
        let holders = self.record.as_ref().map_or_else(
            || current_holders.clone(),
            |record| holder_intersection(&current_holders, &record.holder_node_ids),
        );
        if self.record.is_some() && holders.is_empty() {
            return Err(undeliverable(
                "metadata document has no active frozen holder with history capacity",
            ));
        }
        let authority = pid_authority_node(&config, realm_id, document_id)
            .ok_or_else(|| undeliverable("persistent id authority is unavailable"))?;
        let local_node_id = self.actor.node_id;
        let local_holds = holders.contains(&local_node_id);
        if local_node_id == authority
            && local_holds
            && let Some(record) = self.record.as_ref()
        {
            let delete =
                DeleteDocumentOperation::new(self.actor.clone(), record.group_id, document_id);
            let retried = boxed_retry(
                move || delete.fresh_copy(),
                &document_id.to_bytes(),
                |result| Event::Child(ChildOutcome::new(result)),
            );
            self.stage = DeleteStage::Local;
            return Ok(Next::Call(IoCall::Child(retried)));
        }
        let message = MetadataTransportMessage::ForwardDeleteDocument {
            auth_token: self.auth_token.clone(),
            config_digest,
            document_id,
        };
        let local_miss = (local_holds && self.record.is_none()).then_some(local_node_id);
        let forward =
            ForwardHoldersOperation::new(self.local_node, &holders, &message, local_miss, false);
        self.stage = DeleteStage::Forward;
        Ok(Next::Call(IoCall::child(forward)))
    }
}

impl Operation for RouteDeleteOperation {
    type Output = ();
    type Error = MetadataWriteError;

    fn first(&mut self) -> Result<Next<()>, MetadataWriteError> {
        let config = GetConfigOperation::new(self.actor.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataWriteError> {
        match self.stage {
            DeleteStage::Config => self.configured(routing_config(result)?),
            DeleteStage::Local => {
                result.child::<(), DeleteDocumentError>()??;
                // The registry cache drops the row only after the delete committed.
                let uncache = MetadataEffect::UncacheRecord {
                    document_id: self.document_id,
                };
                Ok(Next::Then {
                    output: (),
                    calls: vec![IoCall::Effect(Box::new(Effect::Metadata(uncache)))],
                })
            }
            DeleteStage::Forward => {
                match result.child::<MetadataTransportMessage, HolderWriteError>()?? {
                    MetadataTransportMessage::ForwardedDelete => Ok(Next::Done(())),
                    other => Err(unexpected_response(other)),
                }
            }
        }
    }
}

/// Forwards one edit a device made on its replica to the document's holders, one after the
/// other. The batch is idempotent by its dot, but an ambiguous delivery stops the attempt:
/// replaying would append a second event for a merge that changes nothing.
#[derive(Debug, PartialEq)]
pub struct ApplyBatchOperation {
    realm_id: RealmId,
    document_id: Ulid,
    message: Option<MetadataTransportMessage>,
    frame: Vec<u8>,
    holders: VecDeque<NodeId>,
    asked: Option<NodeId>,
    detail: String,
}

impl ApplyBatchOperation {
    pub fn new(
        (realm_id, document_id): (RealmId, Ulid),
        (batch, authored): (Box<MetadataBatch>, MetadataBatchSource),
        auth_token: AuthToken,
    ) -> Self {
        let message = MetadataTransportMessage::ForwardApplyBatch {
            auth_token,
            config_digest: [0; 32],
            document_id,
            batch,
            authored,
        };
        Self {
            realm_id,
            document_id,
            message: Some(message),
            frame: Vec::new(),
            holders: VecDeque::new(),
            asked: None,
            detail: "the document's bucket has no reachable holder".to_string(),
        }
    }

    fn configured(
        &mut self,
        config: RealmConfigDocument,
    ) -> Result<Next<MetadataRegistryRecord>, MetadataWriteError> {
        let digest = config.digest().map_err(undeliverable)?;
        let placement = resolve_metadata_id(&config, self.realm_id, None, self.document_id)
            .map_err(undeliverable)?;
        let mut message = self
            .message
            .take()
            .ok_or_else(|| undeliverable("the edit was already sent"))?;
        if let MetadataTransportMessage::ForwardApplyBatch { config_digest, .. } = &mut message {
            *config_digest = digest;
        }
        self.frame = encode_message(&message).map_err(undeliverable)?;
        self.holders = distinct_holders(&resolve_shard_holders(&config, &placement)).into();
        self.ask()
    }

    fn ask(&mut self) -> Result<Next<MetadataRegistryRecord>, MetadataWriteError> {
        let Some(holder) = self.holders.pop_front() else {
            return Err(undeliverable(std::mem::take(&mut self.detail)));
        };
        self.asked = Some(holder);
        Ok(Next::Call(peer_call(
            holder,
            self.frame.clone(),
            BATCH_FORWARD_TIMEOUT,
        )))
    }

    fn answered(
        &mut self,
        holder: NodeId,
        result: IoResult,
    ) -> Result<Next<MetadataRegistryRecord>, MetadataWriteError> {
        match peer_reply(holder, result)? {
            Ok(MetadataTransportMessage::ForwardedApplyBatch { result: Ok(record) }) => {
                Ok(Next::Done(*record))
            }
            Ok(MetadataTransportMessage::ForwardedApplyBatch {
                result: Err(SyncRefusal::Unavailable),
            }) => {
                self.detail = format!("{holder}: holder could not apply the device edit");
                self.ask()
            }
            Ok(MetadataTransportMessage::ForwardedApplyBatch {
                result: Err(refusal),
            }) => Err(batch_refusal(refusal)),
            Ok(other) => Err(unexpected_response(other)),
            Err(PeerRequestError::NotSent(error)) => {
                warn!(holder = %holder, error = %error, "Failed to forward a device edit to holder");
                self.detail = format!("{holder}: {error}");
                self.ask()
            }
            Err(error) => {
                warn!(holder = %holder, error = ?error, "Failed to forward a device edit to holder");
                Err(undeliverable(format!(
                    "forward to holder `{holder}` may have applied the device edit before failing; refusing to replay it: {error:?}"
                )))
            }
        }
    }
}

impl Operation for ApplyBatchOperation {
    type Output = MetadataRegistryRecord;
    type Error = MetadataWriteError;

    fn first(&mut self) -> Result<Next<MetadataRegistryRecord>, MetadataWriteError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<MetadataRegistryRecord>, MetadataWriteError> {
        match self.asked {
            None => self.configured(routing_config(result)?),
            Some(holder) => self.answered(holder, result),
        }
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod call_tests;
