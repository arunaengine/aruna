//! The immutable placed document carrying one [`PlacementPolicy`] to its holders.
//! The holder set answers where the rule is obtained; the policy's own selectors
//! answer where governed data may live. Neither set is derived from the other.

use crate::NodeId;
use crate::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision, DocumentSyncTarget,
};
use crate::errors::ConversionError;
use crate::structs::{
    PlacementPolicy, PlacementPolicyError, PlacementPolicyRef, PlacementRef, RealmId,
    VerifiedPolicy,
};
use crate::types::UserId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// STATE-PLACEMENT-POLICY: one immutable residency rule replicated to the holders
/// its policy id resolves to. Identity is `(policy_id, digest)`, so a known id with
/// different bytes is refused as reuse; provenance fields stay outside the digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPolicyDocument {
    pub realm_id: RealmId,
    pub policy: PlacementPolicy,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub event_id: Ulid,
    /// Publisher of the original document; the only node whose sync actor may
    /// author it.
    pub actor: NodeId,
}

impl PlacementPolicyDocument {
    /// Only a verified policy is publishable, so a holder never stores bytes
    /// that could not be matched against a subject.
    pub fn new(
        realm_id: RealmId,
        policy: &VerifiedPolicy,
        created_by: UserId,
        actor: NodeId,
        event_id: Ulid,
        created_at_ms: u64,
    ) -> Self {
        Self {
            realm_id,
            policy: policy.policy().clone(),
            created_by,
            created_at_ms,
            event_id,
            actor,
        }
    }

    pub fn policy_id(&self) -> Ulid {
        self.policy.policy_id
    }

    /// Boundary re-check: decoded or replicated bytes are a candidate until they
    /// pass verification again.
    pub fn verified(&self) -> Result<VerifiedPolicy, PlacementPolicyError> {
        VerifiedPolicy::verify(self.policy.clone())
    }

    pub fn policy_ref(&self) -> Result<PlacementPolicyRef, PlacementPolicyError> {
        Ok(self.verified()?.policy_ref())
    }

    /// Whether both documents define the same policy id with the same canonical
    /// bytes. Provenance is deliberately excluded.
    pub fn same_definition(&self, other: &Self) -> bool {
        self.policy.policy_id == other.policy.policy_id
            && self.policy.canonical_bytes() == other.policy.canonical_bytes()
    }

    /// Folds a replicated publication into the local document. A different
    /// definition under a known id fails closed; byte-identical publications
    /// keep the earliest provenance, so every holder converges on one envelope.
    pub fn merge(&mut self, incoming: &Self) -> Result<bool, PlacementPolicyError> {
        if self.policy.policy_id != incoming.policy.policy_id {
            return Ok(false);
        }
        if !self.same_definition(incoming) {
            return Err(PlacementPolicyError::PolicyIdReuse {
                policy_id: self.policy.policy_id,
            });
        }
        if (incoming.created_at_ms, incoming.event_id) < (self.created_at_ms, self.event_id) {
            *self = incoming.clone();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Document key: the policy id alone, so a replay resolves the same row.
pub fn placement_policy_key(policy_id: Ulid) -> Vec<u8> {
    policy_id.to_bytes().to_vec()
}

pub fn placement_policy_target(policy_id: Ulid) -> DocumentSyncTarget {
    DocumentSyncTarget::PlacementPolicy { policy_id }
}

/// Sync change a policy row publishes and records. Derived purely from the row,
/// so two holders of one document write byte-identical manifest entries.
pub fn placement_policy_change(
    document: &PlacementPolicyDocument,
    placement: PlacementRef,
) -> DocumentSyncChange {
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: document.created_at_ms,
            event_id: document.event_id,
            actor: document.actor,
            updated_at_ms: document.created_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{LabelMatch, PlacementSelector};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([2; 16]), RealmId([3; 32]))
    }

    fn policy(seed: u8, location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "eu-only".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: vec![LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                }],
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn document(seed: u8, location: &str, created_at_ms: u64) -> PlacementPolicyDocument {
        PlacementPolicyDocument::new(
            RealmId([9; 32]),
            &policy(seed, location),
            user(),
            node(seed),
            Ulid::from_bytes([seed; 16]),
            created_at_ms,
        )
    }

    #[test]
    fn document_roundtrips() {
        let document = document(1, "eu-west", 10);
        assert_eq!(
            PlacementPolicyDocument::from_bytes(&document.to_bytes().unwrap()).unwrap(),
            document
        );
        assert_eq!(
            document.policy_ref().unwrap(),
            document.verified().unwrap().policy_ref()
        );
    }

    #[test]
    fn merge_rejects_reuse() {
        let mut local = document(1, "eu-west", 10);
        let reused = document(1, "us-east", 10);
        assert_eq!(
            local.merge(&reused),
            Err(PlacementPolicyError::PolicyIdReuse {
                policy_id: Ulid::from_bytes([1; 16])
            })
        );
        assert_eq!(local, document(1, "eu-west", 10));
    }

    #[test]
    fn merge_keeps_earliest() {
        let mut later = document(1, "eu-west", 20);
        let earlier = document(1, "eu-west", 5);
        assert!(later.merge(&earlier).unwrap());
        assert_eq!(later.created_at_ms, 5);

        let mut earliest = earlier.clone();
        assert!(!earliest.merge(&document(1, "eu-west", 20)).unwrap());
        assert_eq!(earliest, later);
    }

    #[test]
    fn merge_ignores_others() {
        let mut local = document(1, "eu-west", 10);
        assert!(!local.merge(&document(2, "us-east", 1)).unwrap());
        assert_eq!(local, document(1, "eu-west", 10));
    }

    #[test]
    fn change_follows_row() {
        let document = document(7, "eu-west", 42);
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([3; 16]),
            shard: 4,
        };
        let change = placement_policy_change(&document, placement);

        assert_eq!(change.current.generation, 42);
        assert_eq!(change.current.event_id, Ulid::from_bytes([7; 16]));
        assert_eq!(change.current.actor, node(7));
        assert_eq!(change.kind, DocumentSyncChangeKind::Upsert);
        assert_eq!(change.placement, placement);
    }
}
