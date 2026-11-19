//! Updates one metadata document: on this device's replica, here as a holder, or at a holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::write::device_edit_error;
use crate::device::edit::{DeviceEditError, accepts_edits, apply_local_edit};
use crate::device::publish_queue::token_issued_at;
use crate::device::replica::{ReplicaRecord, read_answer, replica_read};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::replay::routed_record_matches;
use crate::forward::routing::{ForwardHoldersOperation, HolderWriteError, holder_intersection};
use crate::forward::transport::{MetadataWriteError, unexpected_response};
use crate::metadata::call_results::effect_event;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};
use crate::metadata::update_entry::UpdateEntryOperation;
use crate::realm::load_config::config_unavailable;
use crate::realm::mutate_placement::node_kind;
use crate::realm::peer_trust::is_sync_eligible;

type UpdateTurn = Result<Next<MetadataRegistryRecord>, MetadataWriteError>;

fn undeliverable(reason: impl ToString) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(reason.to_string())
}

/// The change one update asks for.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentUpdate {
    /// A new visibility; only a holder may change it.
    pub public: Option<bool>,
    pub mutation: UpdateDocumentMutation,
    /// The revision the caller based the change on; only a holder can check it.
    pub expected_revision: Option<Ulid>,
    pub commit_message: Option<String>,
}

#[derive(Debug, PartialEq)]
enum UpdateStage {
    Config,
    Replica,
    Device,
    Local,
    Forward,
}

/// Updates one document. A device edits a selected or read document locally and queues it for
/// the intake drain; a holder updates its own copy, and hands the change to the other holders
/// when it holds none or its history is full. A known record pins the holders to its frozen set.
#[derive(Debug, PartialEq)]
pub struct RouteUpdateOperation {
    actor: Actor,
    local_node: Option<NodeId>,
    record: Option<MetadataRegistryRecord>,
    document_id: Ulid,
    update: DocumentUpdate,
    auth_token: Option<AuthToken>,
    stage: UpdateStage,
    config: Option<RealmConfigDocument>,
    placement: Option<PlacementRef>,
    holders: Vec<NodeId>,
}

impl RouteUpdateOperation {
    /// `local_node` is this node on the network, if it has one.
    pub fn new(
        (actor, local_node): (Actor, Option<NodeId>),
        (record, document_id): (Option<MetadataRegistryRecord>, Ulid),
        update: DocumentUpdate,
        auth_token: Option<AuthToken>,
    ) -> Self {
        Self {
            actor,
            local_node,
            record,
            document_id,
            update,
            auth_token,
            stage: UpdateStage::Config,
            config: None,
            placement: None,
            holders: Vec::new(),
        }
    }

    fn config(&self) -> Result<&RealmConfigDocument, MetadataWriteError> {
        self.config
            .as_ref()
            .ok_or_else(|| undeliverable("realm placement config is unavailable"))
    }

    fn configured(&mut self, config: RealmConfigDocument) -> UpdateTurn {
        // Only a holder can check an expected revision or change visibility, so those always go
        // to a holder and fail while none answers. A node that holds buckets is never a device.
        let device_edit = self.update.expected_revision.is_none()
            && self.update.public.is_none()
            && !is_sync_eligible(&config, self.actor.node_id);
        self.config = Some(config);
        if !device_edit {
            return self.route();
        }
        self.stage = UpdateStage::Replica;
        let read = replica_read(self.document_id);
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(read)))))
    }

    fn replica(&mut self, replica: Option<ReplicaRecord>) -> UpdateTurn {
        let config = self.config()?;
        let owner = node_kind(config, self.actor.node_id).and_then(|kind| kind.owner());
        let by_owner = owner == Some(self.actor.user_id);
        let Some(replica) = replica.filter(|replica| accepts_edits(replica, by_owner)) else {
            return self.route();
        };
        let issued_at = token_issued_at(self.auth_token.as_ref());
        let (owner, node_id) = (self.actor.user_id, self.actor.node_id);
        let mutation = self.update.mutation.clone();
        let edit = apply_local_edit(owner, node_id, &replica, mutation, issued_at);
        self.stage = UpdateStage::Device;
        Ok(Next::Call(IoCall::child(edit)))
    }

    fn route(&mut self) -> UpdateTurn {
        let config = self.config()?;
        let realm_id = self.actor.realm_id;
        let group_id = self.record.as_ref().map(|record| record.group_id);
        let placement = resolve_metadata_id(config, realm_id, group_id, self.document_id)
            .map_err(undeliverable)?;
        if (self.record.as_ref()).is_some_and(|record| {
            !routed_record_matches(config, realm_id, self.document_id, &placement, record)
        }) {
            return Err(undeliverable(
                "local metadata registry record does not match the routed document",
            ));
        }
        let current_holders = resolve_shard_holders(config, &placement);
        let holders = self.record.as_ref().map_or_else(
            || current_holders.clone(),
            |record| holder_intersection(&current_holders, &record.holder_node_ids),
        );
        if self.record.is_some() && holders.is_empty() {
            return Err(undeliverable(
                "metadata document has no active frozen holder with history capacity",
            ));
        }
        self.placement = Some(placement);
        self.holders = holders;
        let local_holds = self.holders.contains(&self.actor.node_id);
        let Some(record) = self.record.as_ref().filter(|_| local_holds) else {
            return self.forward(false);
        };
        let update = &self.update;
        let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
            actor: self.actor.clone(),
            group_id: record.group_id,
            document_id: self.document_id,
            public: update.public.unwrap_or(record.public),
            mutation: update.mutation.clone(),
            expected_revision: update.expected_revision,
        })
        .with_message(update.commit_message.clone());
        self.stage = UpdateStage::Local;
        let entry = UpdateEntryOperation::new(operation, self.local_node);
        Ok(Next::Call(IoCall::child(entry)))
    }

    /// Hands the change to the other holders; `local_capacity` says this node's history is full.
    fn forward(&mut self, local_capacity: bool) -> UpdateTurn {
        let config_digest = self.config()?.digest().map_err(undeliverable)?;
        let update = &self.update;
        let message = MetadataTransportMessage::ForwardUpdateDocument {
            auth_token: self.auth_token.clone(),
            config_digest,
            document_id: self.document_id,
            public: update.public,
            mutation: update.mutation.clone(),
            expected_revision: update.expected_revision,
            commit_message: update.commit_message.clone(),
        };
        let local_node_id = self.actor.node_id;
        let local_miss = self
            .holders
            .contains(&local_node_id)
            .then_some(local_node_id);
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

    fn answered(
        &self,
        response: MetadataTransportMessage,
    ) -> Result<MetadataRegistryRecord, MetadataWriteError> {
        let config = self.config()?;
        let placement =
            (self.placement.as_ref()).ok_or_else(|| undeliverable("the update was not routed"))?;
        let realm_id = self.actor.realm_id;
        let invalid = |error: UpdateDocumentError| MetadataWriteError::from(error);
        match response {
            MetadataTransportMessage::ForwardedRecord { record }
                if routed_record_matches(
                    config,
                    realm_id,
                    self.document_id,
                    placement,
                    &record,
                ) && (self.record.as_ref()).is_none_or(|known| known.same_identity(&record))
                    && (self.update.public).is_none_or(|public| record.public == public) =>
            {
                Ok(*record)
            }
            MetadataTransportMessage::ForwardedRecord { .. } => Err(undeliverable(
                "holder returned a metadata update record for another document",
            )),
            MetadataTransportMessage::UpdateInvalidInput { message } => Err(invalid(
                UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)),
            )),
            MetadataTransportMessage::ForwardedProfileValidation { findings } => Err(invalid(
                UpdateDocumentError::MetadataError(MetadataError::ProfileValidation(findings)),
            )),
            MetadataTransportMessage::ForwardedRevisionConflict { expected, current } => {
                Err(invalid(UpdateDocumentError::RevisionConflict {
                    expected,
                    current,
                }))
            }
            MetadataTransportMessage::ForwardedGraphPending { expected } => {
                Err(invalid(UpdateDocumentError::GraphPending { expected }))
            }
            other => Err(unexpected_response(other)),
        }
    }
}

impl Operation for RouteUpdateOperation {
    type Output = MetadataRegistryRecord;
    type Error = MetadataWriteError;

    fn first(&mut self) -> UpdateTurn {
        let config = GetConfigOperation::new(self.actor.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        match self.stage {
            UpdateStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| {
                    config_unavailable(&error);
                    undeliverable("realm placement config is unavailable")
                })?;
                self.configured(config)
            }
            UpdateStage::Replica => {
                let replica = read_answer(&effect_event(result)?);
                self.replica(replica.filter(ReplicaRecord::serves_reads))
            }
            UpdateStage::Device => {
                let edited = result.child::<MetadataRegistryRecord, DeviceEditError>()?;
                Ok(Next::Done(edited.map_err(device_edit_error)?))
            }
            UpdateStage::Local => {
                match result.child::<MetadataRegistryRecord, UpdateDocumentError>()? {
                    Ok(record) => Ok(Next::Done(record)),
                    Err(UpdateDocumentError::RawLimit) => self.forward(true),
                    Err(error) => Err(error.into()),
                }
            }
            UpdateStage::Forward => {
                let response = result.child::<MetadataTransportMessage, HolderWriteError>()??;
                Ok(Next::Done(self.answered(response)?))
            }
        }
    }
}

#[cfg(test)]
#[path = "routed_update_tests.rs"]
mod call_tests;
