//! Mints the id of a new document and pins the node that creates it, so a retry sends the very
//! same create to the very same node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::StructuredId;
use aruna_core::types::GroupId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::authorized_create::CreateAuthorizedError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::routing::is_user_origin;
use crate::forward::transport::MetadataWriteError;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::{
    CreateDocumentError, CreateDocumentPayload, mint_forward_document, mint_local_document,
    resolve_metadata_id,
};

/// A create of a given document on one pinned node, kept so that a retry sends the very same
/// arguments to the very same node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PinnedCreate {
    pub group_id: GroupId,
    /// The normalized document path.
    pub path: String,
    pub document_id: Ulid,
    /// This node, or the holder of the document's bucket chosen when the id was minted.
    pub target: NodeId,
    pub payload: CreateDocumentPayload,
    pub message: Option<String>,
}

/// Mints the id of a new private document at `path` and pins the node that creates it: this
/// node when it holds the id's bucket, else the first other holder of that bucket.
#[derive(Debug, PartialEq)]
pub struct PinnedMintOperation {
    actor: Actor,
    group_id: GroupId,
    path: String,
    payload: Option<CreateDocumentPayload>,
    message: Option<String>,
}

impl PinnedMintOperation {
    pub fn new(
        actor: Actor,
        (group_id, path): (GroupId, &str),
        (payload, message): (CreateDocumentPayload, Option<String>),
    ) -> Self {
        Self {
            actor,
            group_id,
            path: MetadataRegistryRecord::normalize_document_path(path),
            payload: Some(payload),
            message,
        }
    }

    fn pin(&mut self, config: &RealmConfigDocument) -> Result<PinnedCreate, CreateAuthorizedError> {
        let actor = &self.actor;
        let user_origin =
            is_user_origin(config, actor.node_id).ok_or(MetadataApiError::ServiceUnavailable)?;
        let local = match user_origin {
            true => Err(CreateDocumentError::HoldsNoBucket),
            false => mint_local_document(config, actor, self.group_id, &self.path),
        };
        let (document_id, target) = match local {
            Ok(id) => (id.as_ulid(), actor.node_id),
            Err(CreateDocumentError::HoldsNoBucket) => {
                let id = mint_forward_document(config, actor, self.group_id, &self.path)?.as_ulid();
                let placement =
                    resolve_metadata_id(config, actor.realm_id, Some(self.group_id), id)?;
                let holders = resolve_shard_holders(config, &placement);
                let target = holders.into_iter().find(|holder| *holder != actor.node_id);
                let target = target.ok_or_else(|| {
                    MetadataWriteError::Undeliverable("the document has no holder".to_string())
                })?;
                (id, target)
            }
            Err(error) => return Err(error.into()),
        };
        let payload = self
            .payload
            .take()
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        Ok(PinnedCreate {
            group_id: self.group_id,
            path: std::mem::take(&mut self.path),
            document_id,
            target,
            payload,
            message: self.message.take(),
        })
    }
}

impl Operation for PinnedMintOperation {
    type Output = PinnedCreate;
    type Error = CreateAuthorizedError;

    fn first(&mut self) -> Result<Next<PinnedCreate>, CreateAuthorizedError> {
        if self.path.is_empty() {
            return Err(CreateAuthorizedError::EmptyPath);
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.actor.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PinnedCreate>, CreateAuthorizedError> {
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        Ok(Next::Done(self.pin(&config)?))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes([1; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        }
    }

    fn scaffold() -> CreateDocumentPayload {
        CreateDocumentPayload::Scaffold {
            name: "Pinned".to_string(),
            description: String::new(),
            date_published: "2026-01-01".to_string(),
            license: None,
        }
    }

    #[test]
    fn empty_path_refused() {
        let mut mint = PinnedMintOperation::new(actor(), (Ulid::nil(), " / "), (scaffold(), None));
        assert_eq!(mint.first(), Err(CreateAuthorizedError::EmptyPath));
    }

    #[test]
    fn missing_config_unavailable() {
        let mut mint = PinnedMintOperation::new(
            actor(),
            (Ulid::nil(), "datasets/pinned"),
            (scaffold(), None),
        );
        assert!(matches!(mint.first(), Ok(Next::Call(IoCall::Child(_)))));
        let missing = IoResult::Child(ChildOutcome::new(Err::<RealmConfigDocument, _>(
            GetConfigError::DocumentNotFound,
        )));
        assert_eq!(
            mint.next(missing),
            Err(CreateAuthorizedError::Api(
                MetadataApiError::ServiceUnavailable
            ))
        );
    }

    #[test]
    fn unlisted_node_unavailable() {
        let mut mint = PinnedMintOperation::new(
            actor(),
            (Ulid::nil(), "datasets/pinned"),
            (scaffold(), None),
        );
        mint.first().unwrap();
        let config = RealmConfigDocument::new(actor().realm_id, Vec::new(), 3);
        let answer = IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)));
        assert_eq!(
            mint.next(answer),
            Err(CreateAuthorizedError::Api(
                MetadataApiError::ServiceUnavailable
            ))
        );
    }
}
