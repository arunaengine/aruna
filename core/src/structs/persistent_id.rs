use crate::errors::ConversionError;
use crate::structs::MetadataRegistryRecord;
use crate::types::UserId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// What a persistent identifier resolves to. Only `Conceptual` is built now; a
/// version-bound kind (pinned to a VersionCursor) can be added later without a
/// format change.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PersistentIdKind {
    /// Resolves to the document's current state, independent of version.
    Conceptual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PersistentIdStatus {
    Active,
    Withdrawn,
}

/// STATE-PERSISTENT-ID-MAPPING: binds a minted w3id PID to a document.
///
/// The PID IS the document graph IRI `https://w3id.org/aruna/{document_id}`; no
/// opaque scheme. Keyed by `document_id`, so minting is idempotent. Once minted,
/// the mapping is never removed: a deleted or tombstoned document flips it to
/// `Withdrawn` (a permanent 410), it never 404s or is reused.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistentIdMapping {
    pub pid: String,
    pub target: Ulid,
    pub kind: PersistentIdKind,
    pub status: PersistentIdStatus,
    pub minted_at_ms: u64,
    pub minted_by: UserId,
    pub withdrawn_at_ms: Option<u64>,
}

impl PersistentIdMapping {
    pub fn conceptual(document_id: Ulid, minted_at_ms: u64, minted_by: UserId) -> Self {
        Self {
            pid: MetadataRegistryRecord::graph_iri_for(document_id),
            target: document_id,
            kind: PersistentIdKind::Conceptual,
            status: PersistentIdStatus::Active,
            minted_at_ms,
            minted_by,
            withdrawn_at_ms: None,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, PersistentIdStatus::Active)
    }

    /// Flip to a permanent tombstone. A no-op when already withdrawn so the first
    /// withdrawal timestamp is preserved.
    pub fn withdraw(&mut self, withdrawn_at_ms: u64) {
        if self.is_active() {
            self.status = PersistentIdStatus::Withdrawn;
            self.withdrawn_at_ms = Some(withdrawn_at_ms);
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Mapping key: the document id alone, so a re-mint resolves the same row.
pub fn persistent_id_key(document_id: Ulid) -> Vec<u8> {
    document_id.to_bytes().to_vec()
}

/// Internal job payload for an idempotent PID registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintPersistentIdSpec {
    pub document_id: Ulid,
    pub minted_by: UserId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([2; 16]), RealmId([3; 32]))
    }

    #[test]
    fn conceptual_pid_is_the_graph_iri() {
        let id = Ulid::from_bytes([1; 16]);
        let mapping = PersistentIdMapping::conceptual(id, 5, user());
        assert_eq!(mapping.pid, MetadataRegistryRecord::graph_iri_for(id));
        assert!(mapping.is_active());
        assert_eq!(persistent_id_key(id), id.to_bytes().to_vec());
    }

    #[test]
    fn withdraw_is_permanent_and_keeps_first_timestamp() {
        let mut mapping = PersistentIdMapping::conceptual(Ulid::from_bytes([1; 16]), 5, user());
        mapping.withdraw(10);
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
        mapping.withdraw(20);
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
    }

    #[test]
    fn mapping_roundtrips() {
        let mapping = PersistentIdMapping::conceptual(Ulid::from_bytes([9; 16]), 7, user());
        assert_eq!(
            PersistentIdMapping::from_bytes(&mapping.to_bytes().unwrap()).unwrap(),
            mapping
        );
    }
}
