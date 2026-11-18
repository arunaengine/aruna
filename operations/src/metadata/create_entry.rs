//! Runs a metadata document create: id minting, Profile validation, the retried operation and
//! the projection and materialization wakeups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::metadata::MetadataError;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structured_id::StructuredId;
use aruna_core::types::GroupId;
use aruna_core::{MetaResourceId, NodeId};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::create_document::{
    CreateDocumentError, CreateDocumentOperation, CreateDocumentPayload, CreateDocumentResult,
    mint_document_for,
};
use crate::metadata::profile::rules::not_profiled_status;
use crate::metadata::profile::validation::{
    MetadataProfilePreview, ProfileAssessOperation, submission_verdict,
};
use crate::metadata::repository::{schedule_materialization, schedule_projection};

/// Mints a metadata document id from the realm config: from a local held bucket and a
/// pre-provisioned binding, or with the blind placement a job-produced or forwarded document
/// uses. Missing holdership or bindings fail closed for routed handling.
#[derive(Debug, PartialEq)]
pub struct DocumentMintOperation {
    actor: Actor,
    group_id: GroupId,
    document_path: String,
    forward_blind: bool,
}

impl DocumentMintOperation {
    pub(crate) fn local(actor: Actor, group_id: GroupId, document_path: String) -> Self {
        Self {
            actor,
            group_id,
            document_path,
            forward_blind: false,
        }
    }

    /// A job-produced document id with the blind placement.
    pub fn job(actor: Actor, group_id: GroupId, document_path: String) -> Self {
        Self {
            actor,
            group_id,
            document_path,
            forward_blind: true,
        }
    }
}

impl Operation for DocumentMintOperation {
    type Output = MetaResourceId;
    type Error = CreateDocumentError;

    fn first(&mut self) -> Result<Next<MetaResourceId>, CreateDocumentError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.actor.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetaResourceId>, CreateDocumentError> {
        let realm_config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(config_error)?;
        let minted = mint_document_for(
            &realm_config,
            &self.actor,
            self.group_id,
            &self.document_path,
            self.forward_blind,
        )?;
        Ok(Next::Done(minted))
    }
}

/// Maps a realm config read failure into the create error; a missing config fails closed.
fn config_error(error: GetConfigError) -> CreateDocumentError {
    match error {
        GetConfigError::StorageError(error) => error.into(),
        GetConfigError::ConversionError(error) => error.into(),
        GetConfigError::DocumentNotFound => CreateDocumentError::PlacementBindingUnavailable(
            "realm config document missing".to_string(),
        ),
        other => CreateDocumentError::PlacementBindingUnavailable(format!(
            "unexpected storage event reading realm config: {other}"
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EntryStage {
    Mint,
    Validate,
    Create,
}

/// A metadata document create from its entry: only a local create with the nil id mints one
/// here, supplied and forwarded ids persist. The Profile verdict is enforced before the create
/// runs, retried after lost conflicts, and the projection and materialization drains wake after.
#[derive(Debug, PartialEq)]
pub struct CreateEntryOperation {
    template: Option<CreateDocumentOperation>,
    local_node: Option<NodeId>,
    stage: EntryStage,
}

type EntryTurn = Result<Next<CreateDocumentResult>, CreateDocumentError>;

impl CreateEntryOperation {
    /// `local_node` is this node on the network, which a Profile fetch needs.
    pub fn new(template: CreateDocumentOperation, local_node: Option<NodeId>) -> Self {
        Self {
            template: Some(template),
            local_node,
            stage: EntryStage::Mint,
        }
    }

    fn template(&mut self) -> Result<&mut CreateDocumentOperation, CreateDocumentError> {
        self.template
            .as_mut()
            .ok_or_else(|| MetadataError::Backend("the create was already started".into()).into())
    }

    fn minted(&mut self, result: IoResult) -> EntryTurn {
        let document_id = result.child::<MetaResourceId, CreateDocumentError>()??;
        self.template()?.config.document_id = document_id.as_ulid();
        self.validate()
    }

    fn validate(&mut self) -> EntryTurn {
        let local_node = self.local_node;
        let template = self.template()?;
        match &template.config.payload {
            CreateDocumentPayload::RoCrate { jsonld } => {
                let assess = ProfileAssessOperation::write(
                    local_node,
                    template.config.document_id,
                    template.config.group_id,
                    jsonld.clone(),
                );
                self.stage = EntryStage::Validate;
                Ok(Next::Call(IoCall::child(assess)))
            }
            CreateDocumentPayload::Scaffold { .. } => {
                template.route_profile_status =
                    Some(not_profiled_status(template.config.document_id));
                self.create()
            }
        }
    }

    fn validated(&mut self, result: IoResult) -> EntryTurn {
        let assessed = result.child::<MetadataProfilePreview, MetadataError>()?;
        let template = self.template()?;
        let jsonld = match &template.config.payload {
            CreateDocumentPayload::RoCrate { jsonld } => jsonld.as_str(),
            CreateDocumentPayload::Scaffold { .. } => "",
        };
        let status = submission_verdict(assessed, template.config.document_id, jsonld)?;
        template.route_profile_status = Some(status);
        self.create()
    }

    fn create(&mut self) -> EntryTurn {
        let template = self
            .template
            .take()
            .ok_or_else(|| MetadataError::Backend("the create was already started".into()))?;
        let seed = template.config.document_id.to_bytes();
        let create = boxed_retry(
            move || template.fresh_copy(),
            &seed,
            |result| Event::Child(ChildOutcome::new(result)),
        );
        self.stage = EntryStage::Create;
        Ok(Next::Call(IoCall::Child(create)))
    }
}

impl Operation for CreateEntryOperation {
    type Output = CreateDocumentResult;
    type Error = CreateDocumentError;

    fn first(&mut self) -> EntryTurn {
        let template = self.template()?;
        if template.forwarded || !template.config.document_id.is_nil() {
            return self.validate();
        }
        let mint = DocumentMintOperation::local(
            template.config.actor.clone(),
            template.config.group_id,
            template.config.document_path.clone(),
        );
        Ok(Next::Call(IoCall::child(mint)))
    }

    fn next(&mut self, result: IoResult) -> EntryTurn {
        match self.stage {
            EntryStage::Mint => self.minted(result),
            EntryStage::Validate => self.validated(result),
            EntryStage::Create => {
                let created = result.child::<CreateDocumentResult, CreateDocumentError>()??;
                Ok(Next::Then {
                    output: created,
                    calls: vec![
                        IoCall::Effect(Box::new(schedule_projection(Duration::ZERO))),
                        IoCall::Effect(Box::new(schedule_materialization())),
                    ],
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "create_entry_tests.rs"]
mod tests;
