//! Serves one policy document to a peer that fetches it, after checking the peer and the hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, placement_policy_target,
};
use aruna_core::structs::placement::policy::{PlacementPolicyRef, VerifiedPolicy};
use aruna_core::types::{Effects, Value};
use smallvec::smallvec;
use tracing::warn;

use crate::document_repository::read_effect;
use crate::metadata::protocol::MetadataReadError;

/// The answer a peer gets: the stored document, none, or why it is refused.
pub type PolicyAnswer = Result<Option<Box<PlacementPolicyDocument>>, MetadataReadError>;

#[derive(Debug, PartialEq)]
enum ServeState {
    Init,
    Config,
    Policy,
    Done,
}

/// Serves one policy document to a peer. The peer must be a sync-eligible node of this realm,
/// and the answer must hash to the requested ref. The original publication travels with it:
/// this holder never becomes the author. A refused or unreadable request is part of the answer.
#[derive(Debug, PartialEq)]
pub struct ServePolicyOperation {
    realm_id: RealmId,
    peer: NodeId,
    policy_ref: PlacementPolicyRef,
    state: ServeState,
    output: Option<Result<PolicyAnswer, MetadataReadError>>,
}

impl ServePolicyOperation {
    pub fn new(realm_id: RealmId, peer: NodeId, policy_ref: PlacementPolicyRef) -> Self {
        Self {
            realm_id,
            peer,
            policy_ref,
            state: ServeState::Init,
            output: None,
        }
    }

    fn answer(&mut self, answer: PolicyAnswer) -> Effects {
        self.state = ServeState::Done;
        self.output = Some(Ok(answer));
        smallvec![]
    }

    fn config(&mut self, value: Option<Value>) -> Effects {
        let config = value.and_then(|value| RealmConfigDocument::from_bytes(&value).ok());
        let Some(config) = config else {
            return self.answer(Err(MetadataReadError::Unavailable));
        };
        if !sync_eligible_peer(&config, self.peer) {
            return self.answer(Err(MetadataReadError::Forbidden));
        }
        let target = placement_policy_target(self.policy_ref.policy_id);
        self.state = ServeState::Policy;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn policy(&mut self, value: Option<Value>) -> Effects {
        let answer = match value {
            None => Ok(None),
            Some(value) => stored_policy(self.realm_id, self.policy_ref, &value),
        };
        self.answer(answer.map(|document| document.map(Box::new)))
    }
}

impl Operation for ServePolicyOperation {
    type Output = PolicyAnswer;
    type Error = MetadataReadError;

    fn start(&mut self) -> Effects {
        self.state = ServeState::Config;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![read_effect(&config, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.state, ServeState::Done), event) {
            (ServeState::Config, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.config(value)
            }
            (ServeState::Policy, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.policy(value)
            }
            (
                ServeState::Config | ServeState::Policy,
                Event::Storage(StorageEvent::Error { .. }),
            ) => self.answer(Err(MetadataReadError::Unavailable)),
            (state, event) => {
                warn!(state = ?state, event = event.label(), "Unexpected event serving a policy");
                self.output = Some(Err(MetadataReadError::Unavailable));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<PolicyAnswer, MetadataReadError> {
        self.output.unwrap_or(Err(MetadataReadError::Unavailable))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn sync_eligible_peer(config: &RealmConfigDocument, peer: NodeId) -> bool {
    config
        .sync_eligible_nodes()
        .is_ok_and(|eligible| eligible.contains(&peer))
}

/// The stored row as the policy `policy_ref` names, or none when the row is another policy.
fn stored_policy(
    realm_id: RealmId,
    policy_ref: PlacementPolicyRef,
    value: &[u8],
) -> Result<Option<PlacementPolicyDocument>, MetadataReadError> {
    let document =
        PlacementPolicyDocument::from_bytes(value).map_err(|_| MetadataReadError::Unavailable)?;
    // A stored row that no longer matches the requested ref is not this policy:
    // answering with it would let one id resolve to two definitions.
    if document.realm_id != realm_id {
        return Ok(None);
    }
    match VerifiedPolicy::verify(document.policy.clone()) {
        Ok(verified) if verified.policy_ref() == policy_ref => {
            // Cheap pre-serve check: a stored row whose publication no longer
            // authenticates is not evidence, so another holder is asked instead.
            document.verify_publication().map_err(|error| {
                warn!(policy_id = %policy_ref.policy_id, error = %error, "Refusing to serve an unauthenticated policy row");
                MetadataReadError::Unavailable
            })?;
            Ok(Some(document))
        }
        Ok(_) => Ok(None),
        Err(_) => Err(MetadataReadError::Unavailable),
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
