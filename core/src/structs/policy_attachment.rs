//! Durable records behind explicit policy attachment: the idempotency row one
//! successor mint is keyed by, and the sealed bulk run plus its per-object
//! intents. Attaching a policy never rewrites a stored version, so every record
//! here describes a successor that is minted instead.

use crate::errors::ConversionError;
use crate::structs::blob::checked_refs;
use crate::structs::{BucketIdentity, CurrentVersionPointer, PlacementPolicyRef};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// One row per `mutation_id`, carrying the successor VersionId this node
/// durably assigned. A replay reads it instead of minting a second version.
pub const POLICY_MUTATION_KEYSPACE: &str = "policy_mutations";
/// One row per bulk run, sealing the bucket default every intent applies.
pub const POLICY_BULK_RUN_KEYSPACE: &str = "policy_bulk_runs";
/// One row per (run, object): observed head, preassigned successor, outcome.
pub const POLICY_BULK_INTENT_KEYSPACE: &str = "policy_bulk_intents";

/// How target refs combine with the refs already on the re-read head.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PolicyRefMode {
    /// Realm-admin exact replacement, which may tighten or relax.
    Replace,
    /// Bucket-default application, which unions and never removes a ref.
    Union,
}

/// The exact parameters one successor mint is authorized by. A replay must
/// present all of them; anything else is a different mutation under a reused id.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyMutationParams {
    pub bucket: String,
    pub key: String,
    pub expected_head: CurrentVersionPointer,
    pub bucket_identity: BucketIdentity,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub mode: PolicyRefMode,
}

/// Committed in the successor's own transaction, so a lost response or restart
/// resolves to the same version instead of minting another one.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyMutationRecord {
    pub mutation_id: Ulid,
    pub params: PolicyMutationParams,
    pub successor_version_id: Ulid,
    /// Effective refs sealed on the successor, canonically sorted.
    pub sealed_refs: Vec<PlacementPolicyRef>,
    /// False for a reference-only successor, which registers no local copy.
    pub materialized: bool,
}

impl PolicyMutationRecord {
    pub fn key(mutation_id: Ulid) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&mutation_id)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        checked_refs(&self.sealed_refs)?;
        checked_refs(&self.params.target_refs)?;
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = postcard::from_bytes(bytes)?;
        checked_refs(&record.sealed_refs)?;
        checked_refs(&record.params.target_refs)?;
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PolicyBulkStatus {
    Active,
    /// Every observed head carried the sealed refs at the end of a pass.
    Completed,
    /// The bucket default moved on; the run stops instead of mixing policies.
    Superseded,
}

/// The sealed target of one bulk run. Nothing in a pass re-reads the bucket
/// default into the intents: a changed default supersedes the run instead.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyBulkRun {
    pub operation_id: Ulid,
    pub bucket: String,
    pub bucket_identity: BucketIdentity,
    pub generation: u64,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub status: PolicyBulkStatus,
}

impl PolicyBulkRun {
    pub fn key(operation_id: Ulid) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&operation_id)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        checked_refs(&self.target_refs)?;
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let run: Self = postcard::from_bytes(bytes)?;
        checked_refs(&run.target_refs)?;
        Ok(run)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyBulkIntentKey {
    pub operation_id: Ulid,
    pub key: String,
}

impl PolicyBulkIntentKey {
    pub fn new(operation_id: Ulid, key: impl Into<String>) -> Self {
        Self {
            operation_id,
            key: key.into(),
        }
    }

    /// Scans every intent of one run.
    pub fn run_prefix(operation_id: Ulid) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&operation_id)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Why an intent could not be completed. A blocked intent stays in the run and
/// is retried by a later pass; it is never reported as completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PolicyBlockedReason {
    /// No serveable local copy to mint a materialized successor from.
    SourceUnavailable,
    /// The sealed refs do not admit this node as a destination.
    DestinationDenied,
    /// A referenced policy is not resolvable here right now.
    PolicyUnresolved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PolicyIntentOutcome {
    /// A successor VersionId is assigned and the head was observed.
    Planned,
    Completed {
        version_id: Ulid,
        materialized: bool,
    },
    Blocked(PolicyBlockedReason),
}

/// One object's durable place in a bulk run. `observed_head` is the only head a
/// mint may advance from, so a concurrent write is superseded and replanned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyBulkIntent {
    pub operation_id: Ulid,
    pub key: String,
    pub observed_head: CurrentVersionPointer,
    pub successor_version_id: Ulid,
    pub outcome: PolicyIntentOutcome,
}

impl PolicyBulkIntent {
    pub fn key(&self) -> PolicyBulkIntentKey {
        PolicyBulkIntentKey::new(self.operation_id, self.key.clone())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PolicyBulkIntent, PolicyBulkIntentKey, PolicyBulkRun, PolicyBulkStatus,
        PolicyIntentOutcome, PolicyMutationParams, PolicyMutationRecord, PolicyRefMode,
    };
    use crate::structs::{CurrentVersionPointer, PlacementPolicyRef};
    use crate::types::UserId;
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    fn policy_ref(byte: u8) -> PlacementPolicyRef {
        PlacementPolicyRef {
            policy_id: Ulid::from_bytes([byte; 16]),
            digest: [byte; 32],
        }
    }

    fn params(refs: Vec<PlacementPolicyRef>) -> PolicyMutationParams {
        PolicyMutationParams {
            bucket: "bucket".to_string(),
            key: "path/file.txt".to_string(),
            expected_head: CurrentVersionPointer::new_with_generation(
                Ulid::from_bytes([1u8; 16]),
                4,
            ),
            bucket_identity: (Ulid::from_bytes([2u8; 16]), UNIX_EPOCH, UserId::default()),
            target_refs: refs,
            mode: PolicyRefMode::Replace,
        }
    }

    fn mutation(refs: Vec<PlacementPolicyRef>) -> PolicyMutationRecord {
        PolicyMutationRecord {
            mutation_id: Ulid::from_bytes([3u8; 16]),
            params: params(refs.clone()),
            successor_version_id: Ulid::from_bytes([4u8; 16]),
            sealed_refs: refs,
            materialized: true,
        }
    }

    #[test]
    fn mutation_round_trips() {
        let record = mutation(vec![policy_ref(1), policy_ref(6)]);
        let bytes = record.to_bytes().expect("record encodes");
        assert_eq!(
            PolicyMutationRecord::from_bytes(&bytes).expect("record decodes"),
            record
        );
    }

    #[test]
    fn mutation_rejects_noncanonical() {
        // A reordered ref set must never reach or leave storage.
        let record = mutation(vec![policy_ref(6), policy_ref(1)]);
        assert!(record.to_bytes().is_err());
    }

    #[test]
    fn run_rejects_noncanonical() {
        let run = PolicyBulkRun {
            operation_id: Ulid::from_bytes([5u8; 16]),
            bucket: "bucket".to_string(),
            bucket_identity: (Ulid::from_bytes([2u8; 16]), UNIX_EPOCH, UserId::default()),
            generation: 3,
            target_refs: vec![policy_ref(6), policy_ref(1)],
            status: PolicyBulkStatus::Active,
        };
        assert!(run.to_bytes().is_err());
    }

    #[test]
    fn key_prefixes_run() {
        let operation_id = Ulid::from_bytes([7u8; 16]);
        let intent = PolicyBulkIntent {
            operation_id,
            key: "path/file.txt".to_string(),
            observed_head: CurrentVersionPointer::new(Ulid::from_bytes([8u8; 16])),
            successor_version_id: Ulid::from_bytes([9u8; 16]),
            outcome: PolicyIntentOutcome::Planned,
        };
        let bytes = intent.to_bytes().expect("intent encodes");
        assert_eq!(
            PolicyBulkIntent::from_bytes(&bytes).expect("intent decodes"),
            intent
        );

        let key = intent.key().to_bytes().expect("key encodes");
        let prefix = PolicyBulkIntentKey::run_prefix(operation_id).expect("prefix encodes");
        assert!(key.starts_with(&prefix));
        assert!(!key.starts_with(
            &PolicyBulkIntentKey::run_prefix(Ulid::from_bytes([6u8; 16])).expect("prefix encodes")
        ));
    }
}
