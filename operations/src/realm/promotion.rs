//! Serves the node side of a promotion: stores the realm key a management node hands over, and
//! reports whether this node's config lists it as management while it holds that key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::{NODE_STATE_KEYSPACE, PROMOTED_KEY_ROW};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::Value;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;

use crate::document_repository::read_row;
use crate::metadata::protocol::{MetadataReadError, RealmKeyPem};
use crate::realm::mutate_placement::{is_management, node_kind};

/// What a management node asks of the node it promotes.
#[derive(Debug, PartialEq)]
pub enum PromotionRequest {
    /// Store the handed realm key for the next start of this node.
    HandKey(RealmKeyPem),
    /// Tell whether this node is listed as management and holds a handed key.
    Check,
}

/// The answer a peer gets: for a handed key `true` once it is stored, for a check whether the
/// promotion is ready; or why the request is refused.
pub type PromotionAnswer = Result<bool, MetadataReadError>;

/// Answers one promotion request of a peer, refusals included in the answer.
#[derive(Debug, PartialEq)]
pub struct ServePromotionOperation {
    realm_id: RealmId,
    local: NodeId,
    peer: NodeId,
    request: PromotionRequest,
    config_read: bool,
}

impl ServePromotionOperation {
    pub fn new(realm_id: RealmId, local: NodeId, peer: NodeId, request: PromotionRequest) -> Self {
        Self {
            realm_id,
            local,
            peer,
            request,
            config_read: false,
        }
    }

    fn config(&mut self, value: Option<Value>) -> Next<PromotionAnswer> {
        let config = value.and_then(|value| RealmConfigDocument::from_bytes(&value).ok());
        let Some(config) = config else {
            return Next::Done(Err(MetadataReadError::Unavailable));
        };
        match self.key_call(&config) {
            Ok(call) => {
                self.config_read = true;
                Next::Call(call)
            }
            Err(answer) => Next::Done(answer),
        }
    }

    /// The key row write or read the request needs, or the answer it gets without one.
    fn key_call(&self, config: &RealmConfigDocument) -> Result<IoCall, PromotionAnswer> {
        let key = ByteView::from(PROMOTED_KEY_ROW);
        let key_space = NODE_STATE_KEYSPACE.to_string();
        match &self.request {
            PromotionRequest::HandKey(handed) => {
                check_handoff(config, self.peer, self.local, self.realm_id, handed).map_err(Err)?;
                let value = ByteView::from(handed.0.as_bytes());
                Ok(IoCall::Write(vec![(key_space, key, value)]))
            }
            PromotionRequest::Check if !is_management(config, self.peer) => {
                Err(Err(MetadataReadError::Forbidden))
            }
            PromotionRequest::Check if !is_management(config, self.local) => Err(Ok(false)),
            PromotionRequest::Check => Ok(IoCall::Read(vec![(key_space, key)])),
        }
    }
}

impl Operation for ServePromotionOperation {
    type Output = PromotionAnswer;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<PromotionAnswer>, MetadataReadError> {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![read_row(&config)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PromotionAnswer>, MetadataReadError> {
        let hand = matches!(self.request, PromotionRequest::HandKey(_));
        match (self.config_read, hand, result) {
            (false, _, IoResult::Values(values)) => {
                Ok(self.config(values.into_iter().next().flatten()))
            }
            (true, true, IoResult::Done) => Ok(Next::Done(Ok(true))),
            (true, false, IoResult::Values(values)) => {
                let held = values.into_iter().next().flatten().is_some();
                Ok(Next::Done(Ok(held)))
            }
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    /// A runner failure is answered as unavailable, never retried as a conflict.
    fn is_conflict(_error: &MetadataReadError) -> bool {
        false
    }
}

/// Only a current management node may hand the key, never to a device, and only the realm's key.
fn check_handoff(
    document: &RealmConfigDocument,
    peer: NodeId,
    local: NodeId,
    realm_id: RealmId,
    key: &RealmKeyPem,
) -> Result<(), MetadataReadError> {
    if !is_management(document, peer)
        || !matches!(
            node_kind(document, local),
            Some(RealmNodeKind::Server | RealmNodeKind::Management)
        )
    {
        return Err(MetadataReadError::Forbidden);
    }
    let signing_key =
        SigningKey::from_pkcs8_pem(&key.0).map_err(|_| MetadataReadError::Unauthorized)?;
    if signing_key.verifying_key().to_bytes() != *realm_id.as_bytes() {
        return Err(MetadataReadError::Unauthorized);
    }
    Ok(())
}

#[cfg(test)]
#[path = "promotion_tests.rs"]
mod tests;
