//! Serves the node side of a promotion: stores the realm key a management node hands over, and
//! reports whether this node's config lists it as management while it holds that key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_STATE_KEYSPACE, PROMOTED_KEY_ROW};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::{Effects, Value};
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use smallvec::smallvec;

use crate::document_repository::read_effect;
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

#[derive(Debug, PartialEq)]
enum ServeState {
    Init,
    Config,
    Key,
    Done,
}

/// Answers one promotion request of a peer, refusals included in the answer.
#[derive(Debug, PartialEq)]
pub struct ServePromotionOperation {
    realm_id: RealmId,
    local: NodeId,
    peer: NodeId,
    request: PromotionRequest,
    state: ServeState,
    output: Option<Result<PromotionAnswer, MetadataReadError>>,
}

impl ServePromotionOperation {
    pub fn new(realm_id: RealmId, local: NodeId, peer: NodeId, request: PromotionRequest) -> Self {
        Self {
            realm_id,
            local,
            peer,
            request,
            state: ServeState::Init,
            output: None,
        }
    }

    fn answer(&mut self, answer: PromotionAnswer) -> Effects {
        self.state = ServeState::Done;
        self.output = Some(Ok(answer));
        smallvec![]
    }

    fn config(&mut self, value: Option<Value>) -> Effects {
        let config = value.and_then(|value| RealmConfigDocument::from_bytes(&value).ok());
        let Some(config) = config else {
            return self.answer(Err(MetadataReadError::Unavailable));
        };
        match self.key_effect(&config) {
            Ok(effect) => {
                self.state = ServeState::Key;
                smallvec![Effect::Storage(effect)]
            }
            Err(answer) => self.answer(answer),
        }
    }

    /// The key row write or read the request needs, or the answer it gets without one.
    fn key_effect(&self, config: &RealmConfigDocument) -> Result<StorageEffect, PromotionAnswer> {
        let key = ByteView::from(PROMOTED_KEY_ROW);
        let key_space = NODE_STATE_KEYSPACE.to_string();
        match &self.request {
            PromotionRequest::HandKey(handed) => {
                check_handoff(config, self.peer, self.local, self.realm_id, handed).map_err(Err)?;
                Ok(StorageEffect::Write {
                    key_space,
                    key,
                    value: ByteView::from(handed.0.as_bytes()),
                    txn_id: None,
                })
            }
            PromotionRequest::Check if !is_management(config, self.peer) => {
                Err(Err(MetadataReadError::Forbidden))
            }
            PromotionRequest::Check if !is_management(config, self.local) => Err(Ok(false)),
            PromotionRequest::Check => Ok(StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            }),
        }
    }
}

impl Operation for ServePromotionOperation {
    type Output = PromotionAnswer;
    type Error = MetadataReadError;

    fn start(&mut self) -> Effects {
        self.state = ServeState::Config;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![read_effect(&config, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, ServeState::Done);
        let hand = matches!(self.request, PromotionRequest::HandKey(_));
        match (state, hand, event) {
            (ServeState::Config, _, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.config(value)
            }
            (ServeState::Key, true, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.answer(Ok(true))
            }
            (ServeState::Key, false, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.answer(Ok(value.is_some()))
            }
            (
                ServeState::Config | ServeState::Key,
                _,
                Event::Storage(StorageEvent::Error { .. }),
            ) => self.answer(Err(MetadataReadError::Unavailable)),
            _ => {
                self.output = Some(Err(MetadataReadError::Unavailable));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<PromotionAnswer, MetadataReadError> {
        self.output.unwrap_or(Err(MetadataReadError::Unavailable))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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
