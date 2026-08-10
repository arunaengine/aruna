use crate::NodeId;
use crate::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision, DocumentSyncTarget,
};
use crate::errors::ConversionError;
use crate::structs::{MetadataRegistryRecord, PlacementRef};
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

impl PersistentIdStatus {
    /// Transition order. `Absent` (no row) is 0, so every accepted transition
    /// strictly increases the rank and `Withdrawn` is terminal.
    fn rank(self) -> u8 {
        match self {
            Self::Active => 1,
            Self::Withdrawn => 2,
        }
    }
}

/// Provenance of the transition that produced a mapping's current status. It
/// lives in the replicated row rather than being minted per holder so every
/// holder records byte-identical sync and shard-manifest revisions whatever the
/// order the transitions arrive in.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistentIdRevision {
    pub event_id: Ulid,
    pub actor: NodeId,
    pub occurred_at_ms: u64,
}

/// STATE-PERSISTENT-ID-MAPPING: binds a minted w3id PID to a document.
///
/// The PID IS the document graph IRI `https://w3id.org/aruna/{document_id}`; no
/// opaque scheme. Keyed by `document_id`, so minting is idempotent. Once written
/// the row is never removed: a deleted or tombstoned document flips it to
/// `Withdrawn` (a permanent 410), it never 404s or is reused. A withdrawal that
/// races or precedes a mint writes the tombstone with no mint fields at all, so
/// `Withdrawn` is reachable straight from `Absent` and can never be replaced.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistentIdMapping {
    pub pid: String,
    pub target: Ulid,
    pub kind: PersistentIdKind,
    pub status: PersistentIdStatus,
    pub minted_at_ms: Option<u64>,
    pub minted_by: Option<UserId>,
    pub withdrawn_at_ms: Option<u64>,
    pub revision: PersistentIdRevision,
}

impl PersistentIdMapping {
    /// `Absent -> Active`.
    pub fn conceptual(
        document_id: Ulid,
        minted_by: UserId,
        revision: PersistentIdRevision,
    ) -> Self {
        Self {
            pid: MetadataRegistryRecord::graph_iri_for(document_id),
            target: document_id,
            kind: PersistentIdKind::Conceptual,
            status: PersistentIdStatus::Active,
            minted_at_ms: Some(revision.occurred_at_ms),
            minted_by: Some(minted_by),
            withdrawn_at_ms: None,
            revision,
        }
    }

    /// `Absent -> Withdrawn`: a tombstone for a PID that was never minted, so a
    /// withdrawal racing an accepted mint job cannot be overwritten by it.
    pub fn tombstone(document_id: Ulid, revision: PersistentIdRevision) -> Self {
        Self {
            pid: MetadataRegistryRecord::graph_iri_for(document_id),
            target: document_id,
            kind: PersistentIdKind::Conceptual,
            status: PersistentIdStatus::Withdrawn,
            minted_at_ms: None,
            minted_by: None,
            withdrawn_at_ms: Some(revision.occurred_at_ms),
            revision,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, PersistentIdStatus::Active)
    }

    /// `Active -> Withdrawn`. Returns whether the mapping changed; a mapping that
    /// is already withdrawn keeps its first withdrawal timestamp and revision.
    pub fn withdraw(&mut self, revision: PersistentIdRevision) -> bool {
        if !self.is_active() {
            return false;
        }
        self.status = PersistentIdStatus::Withdrawn;
        self.withdrawn_at_ms = Some(revision.occurred_at_ms);
        self.revision = revision;
        true
    }

    /// Fold a replicated mapping into the local one, monotonically: the higher
    /// transition rank wins and carries its revision, mint provenance is retained
    /// once seen, and the earliest timestamp of each transition survives.
    /// Commutative, so a replayed or reordered replication frame can never
    /// resurrect a withdrawn PID. Returns whether the local mapping changed.
    pub fn merge(&mut self, incoming: &Self) -> bool {
        if incoming.target != self.target {
            return false;
        }
        let before = self.clone();
        let local_rank = self.status.rank();
        let incoming_rank = incoming.status.rank();
        if incoming_rank > local_rank
            || (incoming_rank == local_rank && incoming.revision.event_id < self.revision.event_id)
        {
            self.status = incoming.status;
            self.revision = incoming.revision;
        }
        if let Some(minted_at_ms) = incoming.minted_at_ms
            && self.minted_at_ms.is_none_or(|local| minted_at_ms < local)
        {
            self.minted_at_ms = Some(minted_at_ms);
            self.minted_by = incoming.minted_by;
        }
        if let Some(withdrawn_at_ms) = incoming.withdrawn_at_ms
            && self
                .withdrawn_at_ms
                .is_none_or(|local| withdrawn_at_ms < local)
        {
            self.withdrawn_at_ms = Some(withdrawn_at_ms);
        }
        *self != before
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

pub fn persistent_id_target(document_id: Ulid) -> DocumentSyncTarget {
    DocumentSyncTarget::PersistentIdMapping { document_id }
}

/// Sync change a mapping row publishes and records. Derived purely from the row,
/// so the manifest entry two holders write for the same mapping state is
/// identical and shard verification cannot report a phantom divergence.
pub fn persistent_id_change(
    mapping: &PersistentIdMapping,
    placement: PlacementRef,
) -> DocumentSyncChange {
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: mapping.revision.occurred_at_ms,
            event_id: mapping.revision.event_id,
            actor: mapping.revision.actor,
            updated_at_ms: mapping.revision.occurred_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    }
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

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn revision(seed: u8, occurred_at_ms: u64) -> PersistentIdRevision {
        PersistentIdRevision {
            event_id: Ulid::from_bytes([seed; 16]),
            actor: node(seed),
            occurred_at_ms,
        }
    }

    #[test]
    fn conceptual_pid_is_the_graph_iri() {
        let id = Ulid::from_bytes([1; 16]);
        let mapping = PersistentIdMapping::conceptual(id, user(), revision(1, 5));
        assert_eq!(mapping.pid, MetadataRegistryRecord::graph_iri_for(id));
        assert!(mapping.is_active());
        assert_eq!(mapping.minted_at_ms, Some(5));
        assert_eq!(persistent_id_key(id), id.to_bytes().to_vec());
    }

    #[test]
    fn withdraw_is_permanent_and_keeps_first_timestamp() {
        let id = Ulid::from_bytes([1; 16]);
        let mut mapping = PersistentIdMapping::conceptual(id, user(), revision(1, 5));
        assert!(mapping.withdraw(revision(2, 10)));
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
        assert!(!mapping.withdraw(revision(3, 20)));
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
    }

    #[test]
    fn tombstone_omits_mint() {
        let id = Ulid::from_bytes([4; 16]);
        let mapping = PersistentIdMapping::tombstone(id, revision(1, 12));
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
        assert_eq!(mapping.minted_at_ms, None);
        assert_eq!(mapping.minted_by, None);
        assert_eq!(mapping.withdrawn_at_ms, Some(12));
    }

    #[test]
    fn merge_converges_unordered() {
        let id = Ulid::from_bytes([1; 16]);
        let active = PersistentIdMapping::conceptual(id, user(), revision(1, 5));
        let mut withdrawn = active.clone();
        withdrawn.withdraw(revision(2, 9));

        let mut forward = active.clone();
        assert!(forward.merge(&withdrawn));
        let mut backward = withdrawn.clone();
        assert!(!backward.merge(&active));

        assert_eq!(forward, backward);
        assert_eq!(forward.status, PersistentIdStatus::Withdrawn);
        assert_eq!(forward.revision, revision(2, 9));
        assert_eq!(forward.minted_at_ms, Some(5));
    }

    #[test]
    fn merge_keeps_tombstone() {
        let id = Ulid::from_bytes([1; 16]);
        let mut tombstone = PersistentIdMapping::tombstone(id, revision(2, 3));
        let active = PersistentIdMapping::conceptual(id, user(), revision(1, 5));

        assert!(tombstone.merge(&active));
        assert_eq!(tombstone.status, PersistentIdStatus::Withdrawn);
        assert_eq!(tombstone.withdrawn_at_ms, Some(3));
        assert_eq!(tombstone.minted_at_ms, Some(5));
    }

    #[test]
    fn merge_ignores_others() {
        let mut mapping =
            PersistentIdMapping::conceptual(Ulid::from_bytes([1; 16]), user(), revision(1, 5));
        let foreign = PersistentIdMapping::tombstone(Ulid::from_bytes([2; 16]), revision(2, 1));
        assert!(!mapping.merge(&foreign));
        assert!(mapping.is_active());
    }

    #[test]
    fn change_follows_row() {
        let id = Ulid::from_bytes([9; 16]);
        let mapping = PersistentIdMapping::conceptual(id, user(), revision(7, 42));
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([3; 16]),
            epoch: 0,
            shard: 4,
        };
        let change = persistent_id_change(&mapping, placement);

        assert_eq!(change.current.generation, 42);
        assert_eq!(change.current.event_id, Ulid::from_bytes([7; 16]));
        assert_eq!(change.current.actor, node(7));
        assert_eq!(change.kind, DocumentSyncChangeKind::Upsert);
        assert_eq!(change.placement, placement);
    }

    #[test]
    fn mapping_roundtrips() {
        let mapping =
            PersistentIdMapping::conceptual(Ulid::from_bytes([9; 16]), user(), revision(1, 7));
        assert_eq!(
            PersistentIdMapping::from_bytes(&mapping.to_bytes().unwrap()).unwrap(),
            mapping
        );
    }
}
