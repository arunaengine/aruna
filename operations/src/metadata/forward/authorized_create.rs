//! Creates a metadata document for an authorized caller: mints or takes its id, checks the
//! caller's write permission, creates it here or at a holder, and emits the watch event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::StructuredId;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::pinned::PinnedCreate;
use super::route_create::RouteCreateOperation;
use super::write::CreateAuthorizedError;
use crate::auth::error::AuthorizeError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{AuthorizeCreateOperation, CreateScope};
use crate::forward::replay::create_record_matches;
use crate::forward::routing::{
    ForwardHoldersOperation, HolderWriteError, create_forward_holders, is_user_origin,
};
use crate::forward::transport::{MetadataWriteError, unexpected_response};
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentPayload,
    CreateDocumentResult, mint_forward_document, mint_local_document,
};
use crate::metadata::create_entry::CreateEntryOperation;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::notifications::watch::origin::EmitEventOperation;
use crate::realm::load_config::config_unavailable;

type CreateTurn = Result<Next<MetadataRegistryRecord>, CreateAuthorizedError>;

/// The realm config, or why the create cannot go on without it.
fn read_config(result: IoResult) -> Result<RealmConfigDocument, CreateAuthorizedError> {
    (result.child::<RealmConfigDocument, GetConfigError>()?).map_err(|error| {
        config_unavailable(&error);
        MetadataApiError::ServiceUnavailable.into()
    })
}

/// The created record, after the watch event of the create is sent; a failed emission only
/// warns and never affects the create.
fn emitted(
    realm_id: RealmId,
    user: aruna_core::UserId,
    created: CreateDocumentResult,
) -> CreateTurn {
    let record = created.record;
    let emit = EmitEventOperation::metadata_created(
        realm_id,
        user,
        record.group_id,
        record.document_id,
        &record.document_path,
        created.event_id,
    );
    Ok(Next::Then {
        output: record,
        calls: vec![IoCall::child(emit)],
    })
}

/// Sends the create of `document_id` to the holders of its bucket, or to the `pinned` one only,
/// and checks the record the answering holder returns.
#[derive(Debug, PartialEq)]
pub struct ForwardCreateOperation {
    local_node: Option<NodeId>,
    realm_config: RealmConfigDocument,
    config: CreateDocumentConfig,
    pinned: Option<NodeId>,
    auth_token: Option<AuthToken>,
    commit_message: Option<String>,
    placement: Option<aruna_core::structs::placement::record::PlacementRef>,
}

impl ForwardCreateOperation {
    pub fn new(
        (local_node, realm_config): (Option<NodeId>, RealmConfigDocument),
        (config, pinned): (CreateDocumentConfig, Option<NodeId>),
        (auth_token, commit_message): (Option<AuthToken>, Option<String>),
    ) -> Self {
        Self {
            local_node,
            realm_config,
            config,
            pinned,
            auth_token,
            commit_message,
            placement: None,
        }
    }
}

impl Operation for ForwardCreateOperation {
    type Output = CreateDocumentResult;
    type Error = MetadataWriteError;

    fn first(&mut self) -> Result<Next<CreateDocumentResult>, MetadataWriteError> {
        let document_id = self.config.document_id;
        let (placement, mut holders) =
            create_forward_holders(&self.realm_config, &self.config, document_id).ok_or_else(
                || {
                    MetadataWriteError::Undeliverable(
                        "document id has no resolvable metadata placement".to_string(),
                    )
                },
            )?;
        if let Some(pinned) = self.pinned {
            holders = vec![pinned];
        }
        let config_digest = (self.realm_config.digest())
            .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
        let message = MetadataTransportMessage::ForwardCreateDocument {
            auth_token: self.auth_token.clone(),
            config_digest,
            group_id: self.config.group_id,
            document_id,
            document_path: self.config.document_path.clone(),
            public: self.config.public,
            payload: self.config.payload.clone(),
            commit_message: self.commit_message.clone(),
        };
        self.placement = Some(placement);
        let forward =
            ForwardHoldersOperation::new(self.local_node, &holders, &message, None, false);
        Ok(Next::Call(IoCall::child(forward)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CreateDocumentResult>, MetadataWriteError> {
        let response = result.child::<MetadataTransportMessage, HolderWriteError>()??;
        let document_id = self.config.document_id;
        let placement = self.placement.as_ref().ok_or_else(|| {
            MetadataWriteError::Undeliverable("the create was not sent".to_string())
        })?;
        match response {
            MetadataTransportMessage::ForwardedRecord { record }
                if create_record_matches(&self.config, document_id, placement, &record) =>
            {
                Ok(Next::Done(CreateDocumentResult {
                    event_id: record.last_event_id,
                    record: *record,
                }))
            }
            MetadataTransportMessage::ForwardedRecord { .. } => {
                Err(MetadataWriteError::Undeliverable(
                    "holder returned a metadata create record for another document".to_string(),
                ))
            }
            MetadataTransportMessage::ForwardedProfileValidation { findings } => Err(
                CreateDocumentError::MetadataError(MetadataError::ProfileValidation(findings))
                    .into(),
            ),
            other => Err(unexpected_response(other)),
        }
    }
}

/// The document a caller asks to create in a group.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentCreate {
    pub group_id: GroupId,
    pub path: String,
    pub public: bool,
    pub payload: CreateDocumentPayload,
    pub commit_message: Option<String>,
}

#[derive(Debug, PartialEq)]
enum AuthorizedStage {
    Config,
    Authorize,
    Route,
}

/// The metadata create of REST and MCP: normalizes the path, mints the document id, checks a
/// caller that does not come from a user node on its group and document, routes the create to a
/// holder and emits the watch event. The caller owns bearer conversion and status mapping.
#[derive(Debug, PartialEq)]
pub struct AuthorizedCreateOperation {
    realm_id: RealmId,
    local_node: NodeId,
    auth: AuthContext,
    extras: PolicyRequestExtras,
    auth_token: Option<AuthToken>,
    create: DocumentCreate,
    stage: AuthorizedStage,
    config: Option<CreateDocumentConfig>,
    networked: bool,
}

impl AuthorizedCreateOperation {
    /// `networked` says whether this node has a network transport.
    pub fn new(
        (realm_id, local_node, networked): (RealmId, NodeId, bool),
        (auth, extras, auth_token): (AuthContext, PolicyRequestExtras, Option<AuthToken>),
        mut create: DocumentCreate,
    ) -> Self {
        create.path = MetadataRegistryRecord::normalize_document_path(&create.path);
        Self {
            realm_id,
            local_node,
            auth,
            extras,
            auth_token,
            create,
            stage: AuthorizedStage::Config,
            config: None,
            networked,
        }
    }

    fn minted(
        &self,
        realm_config: &RealmConfigDocument,
        user_origin: bool,
    ) -> Result<Ulid, CreateAuthorizedError> {
        let actor = self.actor();
        let (group_id, path) = (self.create.group_id, self.create.path.as_str());
        if user_origin {
            return Ok(mint_forward_document(realm_config, &actor, group_id, path)?.as_ulid());
        }
        match mint_local_document(realm_config, &actor, group_id, path) {
            Ok(document_id) => Ok(document_id.as_ulid()),
            Err(CreateDocumentError::HoldsNoBucket) => {
                Ok(mint_forward_document(realm_config, &actor, group_id, path)?.as_ulid())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn actor(&self) -> Actor {
        Actor {
            node_id: self.local_node,
            user_id: self.auth.user_id,
            realm_id: self.realm_id,
        }
    }

    fn configured(&mut self, realm_config: RealmConfigDocument) -> CreateTurn {
        let user_origin = is_user_origin(&realm_config, self.local_node)
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        let document_id = self.minted(&realm_config, user_origin)?;
        let create = &self.create;
        self.config = Some(CreateDocumentConfig {
            actor: self.actor(),
            group_id: create.group_id,
            document_id,
            document_path: create.path.clone(),
            public: create.public,
            payload: create.payload.clone(),
        });
        if user_origin {
            return self.route();
        }
        if self.auth.realm_id != self.realm_id {
            return Err(CreateAuthorizedError::Forbidden);
        }
        self.stage = AuthorizedStage::Authorize;
        let authorize = AuthorizeCreateOperation::new(
            self.realm_id,
            &self.auth,
            self.extras.clone(),
            create.group_id,
            &create.path,
            document_id,
            CreateScope::Group,
        );
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn route(&mut self) -> CreateTurn {
        let config = self
            .config
            .clone()
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        let operation = CreateDocumentOperation::new_generated_id(config)
            .with_message(self.create.commit_message.clone());
        let local_node = self.networked.then_some(self.local_node);
        self.stage = AuthorizedStage::Route;
        let route = RouteCreateOperation::new(operation, local_node, self.auth_token.clone());
        Ok(Next::Call(IoCall::child(route)))
    }
}

impl Operation for AuthorizedCreateOperation {
    type Output = MetadataRegistryRecord;
    type Error = CreateAuthorizedError;

    fn first(&mut self) -> CreateTurn {
        if self.create.path.is_empty() {
            return Err(CreateAuthorizedError::EmptyPath);
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        match self.stage {
            AuthorizedStage::Config => {
                let config = read_config(result)?;
                self.configured(config)
            }
            AuthorizedStage::Authorize => {
                result.child::<(), AuthorizeError>()??;
                self.route()
            }
            AuthorizedStage::Route => {
                let created = result.child::<CreateDocumentResult, MetadataWriteError>()??;
                emitted(self.realm_id, self.auth.user_id, created)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PinnedStage {
    Authorize,
    Config,
    Create,
}

/// Creates a pinned document on its target only, authorized on the document's own path, so a
/// retry meets the holder's replay of an accepted create. Emits the watch event like REST.
#[derive(Debug, PartialEq)]
pub struct CreateWithIdOperation {
    actor: Actor,
    auth: AuthContext,
    extras: PolicyRequestExtras,
    create: PinnedCreate,
    local_node: Option<NodeId>,
    stage: PinnedStage,
}

impl CreateWithIdOperation {
    /// `local_node` is this node on the network, if it has one.
    pub fn new(
        (actor, local_node): (Actor, Option<NodeId>),
        (auth, extras): (AuthContext, PolicyRequestExtras),
        create: PinnedCreate,
    ) -> Self {
        Self {
            actor,
            auth,
            extras,
            create,
            local_node,
            stage: PinnedStage::Authorize,
        }
    }

    fn config(&self) -> CreateDocumentConfig {
        CreateDocumentConfig {
            actor: self.actor.clone(),
            group_id: self.create.group_id,
            document_id: self.create.document_id,
            document_path: self.create.path.clone(),
            public: false,
            payload: self.create.payload.clone(),
        }
    }

    fn create(&mut self) -> CreateTurn {
        if self.create.target != self.actor.node_id {
            self.stage = PinnedStage::Config;
            let config = GetConfigOperation::new(self.actor.realm_id);
            return Ok(Next::Call(IoCall::child(config)));
        }
        let operation = CreateDocumentOperation::new_generated_id(self.config())
            .with_message(self.create.message.clone());
        self.stage = PinnedStage::Create;
        let entry = CreateEntryOperation::new(operation, self.local_node);
        Ok(Next::Call(IoCall::child(entry)))
    }
}

impl Operation for CreateWithIdOperation {
    type Output = MetadataRegistryRecord;
    type Error = CreateAuthorizedError;

    fn first(&mut self) -> CreateTurn {
        let realm_id = self.actor.realm_id;
        if self.auth.realm_id != realm_id {
            return Err(CreateAuthorizedError::Forbidden);
        }
        let create = &self.create;
        let authorize = AuthorizeCreateOperation::new(
            realm_id,
            &self.auth,
            self.extras.clone(),
            create.group_id,
            &create.path,
            create.document_id,
            CreateScope::Document,
        );
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        let realm_id = self.actor.realm_id;
        match self.stage {
            PinnedStage::Authorize => {
                result.child::<(), AuthorizeError>()??;
                self.create()
            }
            PinnedStage::Config => {
                let realm_config = read_config(result)?;
                let token = Some(AuthToken::Internal(self.auth.clone()));
                let forward = ForwardCreateOperation::new(
                    (self.local_node, realm_config),
                    (self.config(), Some(self.create.target)),
                    (token, self.create.message.clone()),
                );
                self.stage = PinnedStage::Create;
                Ok(Next::Call(IoCall::child(forward)))
            }
            PinnedStage::Create => {
                let created = match self.create.target == self.actor.node_id {
                    true => result.child::<CreateDocumentResult, CreateDocumentError>()??,
                    false => result.child::<CreateDocumentResult, MetadataWriteError>()??,
                };
                emitted(realm_id, self.auth.user_id, created)
            }
        }
    }
}

#[cfg(test)]
#[path = "authorized_create_tests.rs"]
mod call_tests;
