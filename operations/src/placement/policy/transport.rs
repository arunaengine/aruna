//! Serves one policy document to a peer that fetches it, after checking the peer and the hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, placement_policy_target,
};
use aruna_core::structs::placement::policy::{PlacementPolicyRef, VerifiedPolicy};
use aruna_core::types::Value;
use tracing::warn;

use crate::document_repository::read_row;
use crate::metadata::protocol::MetadataReadError;

/// The answer a peer gets: the stored document, none, or why it is refused.
pub type PolicyAnswer = Result<Option<Box<PlacementPolicyDocument>>, MetadataReadError>;

/// Serves one policy document to a peer. The peer must be a sync-eligible node of this realm,
/// and the answer must hash to the requested ref. The original publication travels with it:
/// this holder never becomes the author. A refused or unreadable request is part of the answer.
#[derive(Debug, PartialEq)]
pub struct ServePolicyOperation {
    realm_id: RealmId,
    peer: NodeId,
    policy_ref: PlacementPolicyRef,
    config_read: bool,
}

impl ServePolicyOperation {
    pub fn new(realm_id: RealmId, peer: NodeId, policy_ref: PlacementPolicyRef) -> Self {
        Self {
            realm_id,
            peer,
            policy_ref,
            config_read: false,
        }
    }

    fn config(&mut self, value: Option<Value>) -> Next<PolicyAnswer> {
        let config = value.and_then(|value| RealmConfigDocument::from_bytes(&value).ok());
        let Some(config) = config else {
            return Next::Done(Err(MetadataReadError::Unavailable));
        };
        if !sync_eligible_peer(&config, self.peer) {
            return Next::Done(Err(MetadataReadError::Forbidden));
        }
        self.config_read = true;
        let target = placement_policy_target(self.policy_ref.policy_id);
        let row = (target.storage_keyspace().to_string(), target.storage_key());
        Next::Call(IoCall::Read(vec![row]))
    }

    fn policy(&self, value: Option<Value>) -> Next<PolicyAnswer> {
        let answer = match value {
            None => Ok(None),
            Some(value) => stored_policy(self.realm_id, self.policy_ref, &value),
        };
        Next::Done(answer.map(|document| document.map(Box::new)))
    }
}

impl Operation for ServePolicyOperation {
    type Output = PolicyAnswer;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<PolicyAnswer>, MetadataReadError> {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![read_row(&config)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PolicyAnswer>, MetadataReadError> {
        let IoResult::Values(values) = result else {
            warn!("Unexpected answer serving a policy");
            return Err(MetadataReadError::Unavailable);
        };
        let value = values.into_iter().next().flatten();
        Ok(match self.config_read {
            false => self.config(value),
            true => self.policy(value),
        })
    }

    /// A runner failure is answered as unavailable, never retried as a conflict.
    fn is_conflict(_error: &MetadataReadError) -> bool {
        false
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
