//! Updates a metadata document here when this node holds it, otherwise at another holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::replay::routed_record_matches;
use crate::forward::routing::{ForwardHoldersOperation, HolderWriteError, holder_intersection};
use crate::forward::transport::{MetadataWriteError, unexpected_response};
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};
use crate::metadata::update_entry::UpdateEntryOperation;
use crate::realm::load_config::config_unavailable;

/// One update of a document, as its caller asked for it.
#[derive(Clone, Debug, PartialEq)]
pub struct RouteUpdateInput {
    pub actor: Actor,
    /// The local record; it pins the holders to its frozen holder set.
    pub record: Option<MetadataRegistryRecord>,
    pub document_id: Ulid,
    /// `None` keeps the current visibility.
    pub public: Option<bool>,
    pub mutation: UpdateDocumentMutation,
    /// The holder refuses the update when the document moved past this revision.
    pub expected_revision: Option<Ulid>,
    pub auth_token: Option<AuthToken>,
    pub commit_message: Option<String>,
}

#[derive(Debug, PartialEq)]
enum UpdateStage {
    Config,
    Local,
    Forward,
}

/// Applies the update locally when this node is a holder with a record, and forwards it to the
/// other holders when it is not or its history is full.
#[derive(Debug, PartialEq)]
pub struct RouteUpdateOperation {
    input: RouteUpdateInput,
    local_node: Option<NodeId>,
    config: Option<RealmConfigDocument>,
    placement: Option<PlacementRef>,
    config_digest: [u8; 32],
    holders: Vec<NodeId>,
    stage: UpdateStage,
}

type UpdateTurn = Result<Next<MetadataRegistryRecord>, MetadataWriteError>;

fn undeliverable(reason: impl ToString) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(reason.to_string())
}

impl RouteUpdateOperation {
    /// `local_node` is this node on the network, if it has one.
    pub fn new(input: RouteUpdateInput, local_node: Option<NodeId>) -> Self {
        Self {
            input,
            local_node,
            config: None,
            placement: None,
            config_digest: [0; 32],
            holders: Vec::new(),
            stage: UpdateStage::Config,
        }
    }

    fn configured(&mut self, config: RealmConfigDocument) -> UpdateTurn {
        let input = &self.input;
        let realm_id = input.actor.realm_id;
        let document_id = input.document_id;
        let record = input.record.as_ref();
        let group_id = record.map(|record| record.group_id);
        let placement =
            resolve_metadata_id(&config, realm_id, group_id, document_id).map_err(undeliverable)?;
        self.config_digest = config.digest().map_err(undeliverable)?;
        if record.is_some_and(|record| {
            !routed_record_matches(&config, realm_id, document_id, &placement, record)
        }) {
            return Err(undeliverable(
                "local metadata registry record does not match the routed document",
            ));
        }
        let current_holders = resolve_shard_holders(&config, &placement);
        let holders = record.map_or_else(
            || current_holders.clone(),
            |record| holder_intersection(&current_holders, &record.holder_node_ids),
        );
        if record.is_some() && holders.is_empty() {
            return Err(undeliverable(
                "metadata document has no active frozen holder with history capacity",
            ));
        }
        let local = holders.contains(&input.actor.node_id).then_some(record);
        let update = local.flatten().map(|record| {
            let update = UpdateDocumentOperation::new(UpdateDocumentConfig {
                actor: input.actor.clone(),
                group_id: record.group_id,
                document_id,
                public: input.public.unwrap_or(record.public),
                mutation: input.mutation.clone(),
                expected_revision: input.expected_revision,
            });
            update.with_message(input.commit_message.clone())
        });
        self.holders = holders;
        self.placement = Some(placement);
        self.config = Some(config);
        match update {
            Some(update) => {
                self.stage = UpdateStage::Local;
                let entry = UpdateEntryOperation::new(update, self.local_node);
                Ok(Next::Call(IoCall::child(entry)))
            }
            None => self.forward(false),
        }
    }

    fn forward(&mut self, local_capacity: bool) -> UpdateTurn {
        let input = &self.input;
        let message = MetadataTransportMessage::ForwardUpdateDocument {
            auth_token: input.auth_token.clone(),
            config_digest: self.config_digest,
            document_id: input.document_id,
            public: input.public,
            mutation: input.mutation.clone(),
            expected_revision: input.expected_revision,
            commit_message: input.commit_message.clone(),
        };
        let own = input.actor.node_id;
        let local_miss = self.holders.contains(&own).then_some(own);
        let forward = ForwardHoldersOperation::new(
            self.local_node,
            &self.holders,
            &message,
            local_miss,
            local_capacity,
        );
        self.stage = UpdateStage::Forward;
        Ok(Next::Call(IoCall::child(forward)))
    }

    /// A forwarded record counts only for this document and the requested visibility.
    fn accepted(&self, answer: &MetadataRegistryRecord) -> bool {
        let input = &self.input;
        let routed = match (&self.config, &self.placement) {
            (Some(config), Some(placement)) => routed_record_matches(
                config,
                input.actor.realm_id,
                input.document_id,
                placement,
                answer,
            ),
            _ => false,
        };
        routed
            && (input.record.as_ref()).is_none_or(|record| record.same_identity(answer))
            && input.public.is_none_or(|public| answer.public == public)
    }

    fn answered(&self, response: MetadataTransportMessage) -> UpdateTurn {
        let error: UpdateDocumentError = match response {
            MetadataTransportMessage::ForwardedRecord { record } if self.accepted(&record) => {
                return Ok(Next::Done(*record));
            }
            MetadataTransportMessage::ForwardedRecord { .. } => {
                return Err(undeliverable(
                    "holder returned a metadata update record for another document",
                ));
            }
            MetadataTransportMessage::UpdateInvalidInput { message } => {
                UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message))
            }
            MetadataTransportMessage::ForwardedProfileValidation { findings } => {
                UpdateDocumentError::MetadataError(MetadataError::ProfileValidation(findings))
            }
            MetadataTransportMessage::ForwardedRevisionConflict { expected, current } => {
                UpdateDocumentError::RevisionConflict { expected, current }
            }
            MetadataTransportMessage::ForwardedGraphPending { expected } => {
                UpdateDocumentError::GraphPending { expected }
            }
            other => return Err(unexpected_response(other)),
        };
        Err(error.into())
    }
}

impl Operation for RouteUpdateOperation {
    type Output = MetadataRegistryRecord;
    type Error = MetadataWriteError;

    fn first(&mut self) -> UpdateTurn {
        let realm_id = self.input.actor.realm_id;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        match self.stage {
            UpdateStage::Config => {
                let config =
                    (result.child::<RealmConfigDocument, GetConfigError>()?).map_err(|error| {
                        config_unavailable(&error);
                        undeliverable("realm placement config is unavailable")
                    })?;
                self.configured(config)
            }
            UpdateStage::Local => {
                match result.child::<MetadataRegistryRecord, UpdateDocumentError>()? {
                    Ok(record) => Ok(Next::Done(record)),
                    Err(UpdateDocumentError::RawLimit) => self.forward(true),
                    Err(error) => Err(error.into()),
                }
            }
            UpdateStage::Forward => {
                let response = result.child::<MetadataTransportMessage, HolderWriteError>()?;
                self.answered(response?)
            }
        }
    }
}

#[cfg(test)]
#[path = "route_update_tests.rs"]
mod tests;
