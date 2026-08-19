use crate::NodeId;
use crate::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision, DocumentSyncTarget,
};
use crate::errors::ConversionError;
use crate::structs::{JobId, MetadataRegistryRecord, PlacementRef};
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

/// Authority that owns the identifier namespace. Only w3id is configured in
/// this release; the explicit field keeps the stored one-row model honest and
/// lets a later provider migration distinguish records without inspecting the
/// identifier string.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PersistentIdProvider {
    W3id,
}

impl PersistentIdProvider {
    pub const fn name(self) -> &'static str {
        match self {
            Self::W3id => "w3id",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PersistentIdStatus {
    /// Legacy pre-2A active row. Kept first so the v1 decoder remains explicit.
    Active,
    /// Legacy pre-2A retirement whose cause was not recorded.
    Withdrawn,
    Requested,
    Processing,
    Failed,
    AdminWithdrawn,
    Tombstoned,
}

impl PersistentIdStatus {
    pub fn is_retired(self) -> bool {
        matches!(
            self,
            Self::Withdrawn | Self::AdminWithdrawn | Self::Tombstoned
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistentIdFailure {
    pub message: String,
    pub retryable: bool,
    pub recorded_at_ms: u64,
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

/// STATE-PERSISTENT-ID-MAPPING: binds one typed w3id intent to a document.
///
/// Ordinary documents use `https://w3id.org/aruna/{document_id}` and Profiles
/// use `https://w3id.org/aruna/profile/{document_id}` as their sole primary PID.
/// The row is still keyed 1:1 by `document_id`, so one automatic intent and every
/// retry converge here. Once written it is never removed: normal deletion moves
/// it to `Tombstoned`, while exceptional administration moves it to
/// `AdminWithdrawn`; either retirement is a permanent 410 and can never be
/// replaced by an accepted-but-delayed mint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistentIdMapping {
    pub pid: String,
    pub target: Ulid,
    pub kind: PersistentIdKind,
    pub provider: PersistentIdProvider,
    pub status: PersistentIdStatus,
    pub requested_at_ms: Option<u64>,
    pub requested_by: Option<UserId>,
    pub job_id: Option<JobId>,
    /// Visibility frozen with the intent, so status remains permission-safe while
    /// the metadata projection is unreadable and after the registry row is gone.
    pub public: Option<bool>,
    pub permission_path: Option<String>,
    pub minted_at_ms: Option<u64>,
    pub minted_by: Option<UserId>,
    pub failure: Option<PersistentIdFailure>,
    pub withdrawn_at_ms: Option<u64>,
    pub withdrawn_by: Option<UserId>,
    pub withdrawal_reason: Option<String>,
    pub revision: PersistentIdRevision,
}

impl PersistentIdMapping {
    pub fn profile_pid(document_id: Ulid) -> String {
        format!("https://w3id.org/aruna/profile/{document_id}")
    }

    pub fn automatic_pid(document_id: Ulid, profile: bool) -> String {
        if profile {
            Self::profile_pid(document_id)
        } else {
            MetadataRegistryRecord::graph_iri_for(document_id)
        }
    }

    /// The create-transaction intent. The identity is already fixed here; the
    /// asynchronous job only activates this exact record.
    #[allow(clippy::too_many_arguments)]
    pub fn requested(
        document_id: Ulid,
        profile: bool,
        requested_by: UserId,
        job_id: JobId,
        public: bool,
        permission_path: String,
        revision: PersistentIdRevision,
    ) -> Self {
        Self {
            pid: Self::automatic_pid(document_id, profile),
            target: document_id,
            kind: PersistentIdKind::Conceptual,
            provider: PersistentIdProvider::W3id,
            status: PersistentIdStatus::Requested,
            requested_at_ms: Some(revision.occurred_at_ms),
            requested_by: Some(requested_by),
            job_id: Some(job_id),
            public: Some(public),
            permission_path: Some(permission_path),
            minted_at_ms: None,
            minted_by: None,
            failure: None,
            withdrawn_at_ms: None,
            withdrawn_by: None,
            withdrawal_reason: None,
            revision,
        }
    }

    /// Compatibility constructor for an already-active ordinary-document PID.
    pub fn conceptual(
        document_id: Ulid,
        minted_by: UserId,
        revision: PersistentIdRevision,
    ) -> Self {
        Self {
            pid: MetadataRegistryRecord::graph_iri_for(document_id),
            target: document_id,
            kind: PersistentIdKind::Conceptual,
            provider: PersistentIdProvider::W3id,
            status: PersistentIdStatus::Active,
            requested_at_ms: Some(revision.occurred_at_ms),
            requested_by: Some(minted_by),
            job_id: None,
            public: None,
            permission_path: None,
            minted_at_ms: Some(revision.occurred_at_ms),
            minted_by: Some(minted_by),
            failure: None,
            withdrawn_at_ms: None,
            withdrawn_by: None,
            withdrawal_reason: None,
            revision,
        }
    }

    /// Reconciliation constructor for a projected legacy document. Unlike the
    /// compatibility constructor, this preserves the document's exact automatic
    /// identity and visibility for later authenticated status checks.
    #[allow(clippy::too_many_arguments)]
    pub fn active(
        document_id: Ulid,
        profile: bool,
        minted_by: UserId,
        public: bool,
        permission_path: String,
        revision: PersistentIdRevision,
    ) -> Self {
        let mut mapping = Self::conceptual(document_id, minted_by, revision);
        mapping.pid = Self::automatic_pid(document_id, profile);
        mapping.public = Some(public);
        mapping.permission_path = Some(permission_path);
        mapping
    }

    /// `Absent -> Tombstoned`: deletion evidence for a legacy document whose
    /// automatic mapping had not yet been backfilled.
    pub fn tombstone(document_id: Ulid, revision: PersistentIdRevision) -> Self {
        Self {
            pid: MetadataRegistryRecord::graph_iri_for(document_id),
            target: document_id,
            kind: PersistentIdKind::Conceptual,
            provider: PersistentIdProvider::W3id,
            status: PersistentIdStatus::Tombstoned,
            requested_at_ms: None,
            requested_by: None,
            job_id: None,
            public: None,
            permission_path: None,
            minted_at_ms: None,
            minted_by: None,
            failure: None,
            withdrawn_at_ms: Some(revision.occurred_at_ms),
            withdrawn_by: None,
            withdrawal_reason: None,
            revision,
        }
    }

    pub fn tombstone_with_visibility(
        document_id: Ulid,
        public: bool,
        permission_path: String,
        revision: PersistentIdRevision,
    ) -> Self {
        let mut mapping = Self::tombstone(document_id, revision);
        mapping.public = Some(public);
        mapping.permission_path = Some(permission_path);
        mapping
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, PersistentIdStatus::Active)
    }

    pub fn is_retired(&self) -> bool {
        self.status.is_retired()
    }

    /// `Requested|Processing|Failed -> Processing` for one re-driven job.
    pub fn processing(
        &mut self,
        revision: PersistentIdRevision,
        failure: Option<PersistentIdFailure>,
    ) -> bool {
        if self.is_active() || self.is_retired() {
            return false;
        }
        self.status = PersistentIdStatus::Processing;
        self.failure = failure;
        self.revision = revision;
        true
    }

    /// Activate the already-selected identity. A retry observes `false` and the
    /// same row; a retirement can never be revived.
    pub fn activate(&mut self, minted_by: UserId, revision: PersistentIdRevision) -> bool {
        if self.is_active() || self.is_retired() {
            return false;
        }
        self.status = PersistentIdStatus::Active;
        self.minted_at_ms = Some(revision.occurred_at_ms);
        self.minted_by = Some(minted_by);
        self.failure = None;
        self.revision = revision;
        true
    }

    pub fn fail(&mut self, failure: PersistentIdFailure, revision: PersistentIdRevision) -> bool {
        if self.is_active() || self.is_retired() {
            return false;
        }
        self.status = PersistentIdStatus::Failed;
        self.failure = Some(failure);
        self.revision = revision;
        true
    }

    /// Exceptional terminal transition. The required actor and reason live in
    /// the replicated row in addition to the generic metadata audit trail.
    pub fn admin_withdraw(
        &mut self,
        withdrawn_by: UserId,
        reason: String,
        revision: PersistentIdRevision,
    ) -> bool {
        if self.is_retired() {
            return false;
        }
        self.status = PersistentIdStatus::AdminWithdrawn;
        self.withdrawn_at_ms = Some(revision.occurred_at_ms);
        self.withdrawn_by = Some(withdrawn_by);
        self.withdrawal_reason = Some(reason);
        self.failure = None;
        self.revision = revision;
        true
    }

    /// Normal deletion. Returns whether this call recorded the first terminal
    /// transition; an earlier administrative withdrawal remains authoritative.
    pub fn mark_tombstoned(&mut self, revision: PersistentIdRevision) -> bool {
        if self.is_retired() {
            return false;
        }
        self.status = PersistentIdStatus::Tombstoned;
        self.withdrawn_at_ms = Some(revision.occurred_at_ms);
        self.withdrawn_by = None;
        self.withdrawal_reason = None;
        self.failure = None;
        self.revision = revision;
        true
    }

    /// Fold a replicated mapping into the local one. Retirement always absorbs
    /// non-terminal work, Active absorbs stale requested/processing/failure rows,
    /// and otherwise the later transition revision wins. Provenance keeps its
    /// earliest timestamp. The identity tuple must match exactly, so a future
    /// provider can never overwrite this one-document intent accidentally.
    pub fn merge(&mut self, incoming: &Self) -> bool {
        if incoming.target != self.target
            || incoming.pid != self.pid
            || incoming.kind != self.kind
            || incoming.provider != self.provider
        {
            return false;
        }
        let before = self.clone();
        if status_supersedes(self, incoming) {
            self.status = incoming.status;
            self.revision = incoming.revision;
            self.failure = incoming.failure.clone();
            self.withdrawn_at_ms = incoming.withdrawn_at_ms;
            self.withdrawn_by = incoming.withdrawn_by;
            self.withdrawal_reason = incoming.withdrawal_reason.clone();
        }
        if let Some(requested_at_ms) = incoming.requested_at_ms
            && self
                .requested_at_ms
                .is_none_or(|local| requested_at_ms < local)
        {
            self.requested_at_ms = Some(requested_at_ms);
            self.requested_by = incoming.requested_by;
            self.job_id = incoming.job_id;
        }
        if self.job_id.is_none() {
            self.job_id = incoming.job_id;
        }
        if self.public.is_none() {
            self.public = incoming.public;
        }
        if self.permission_path.is_none() {
            self.permission_path = incoming.permission_path.clone();
        }
        if let Some(minted_at_ms) = incoming.minted_at_ms
            && self.minted_at_ms.is_none_or(|local| minted_at_ms < local)
        {
            self.minted_at_ms = Some(minted_at_ms);
            self.minted_by = incoming.minted_by;
        }
        *self != before
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&PersistentIdMappingWire::V2(
            self.clone(),
        ))?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        if let Ok(PersistentIdMappingWire::V2(mapping)) = postcard::from_bytes(bytes) {
            return Ok(mapping);
        }
        let legacy: PersistentIdMappingV1 = postcard::from_bytes(bytes)?;
        Ok(legacy.into())
    }
}

fn status_supersedes(local: &PersistentIdMapping, incoming: &PersistentIdMapping) -> bool {
    match (local.status.is_retired(), incoming.status.is_retired()) {
        (true, false) => false,
        (false, true) => true,
        // The first terminal transition wins and remains the durable cause.
        (true, true) => revision_key(incoming.revision) < revision_key(local.revision),
        (false, false) => match (local.status, incoming.status) {
            (PersistentIdStatus::Active, _) => false,
            (_, PersistentIdStatus::Active) => true,
            _ => revision_key(incoming.revision) > revision_key(local.revision),
        },
    }
}

fn revision_key(revision: PersistentIdRevision) -> (u64, Ulid) {
    (revision.occurred_at_ms, revision.event_id)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum PersistentIdMappingWire {
    V2(PersistentIdMapping),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PersistentIdMappingV1 {
    pid: String,
    target: Ulid,
    kind: PersistentIdKind,
    status: PersistentIdStatusV1,
    minted_at_ms: Option<u64>,
    minted_by: Option<UserId>,
    withdrawn_at_ms: Option<u64>,
    revision: PersistentIdRevision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum PersistentIdStatusV1 {
    Active,
    Withdrawn,
}

impl From<PersistentIdMappingV1> for PersistentIdMapping {
    fn from(legacy: PersistentIdMappingV1) -> Self {
        Self {
            pid: legacy.pid,
            target: legacy.target,
            kind: legacy.kind,
            provider: PersistentIdProvider::W3id,
            status: match legacy.status {
                PersistentIdStatusV1::Active => PersistentIdStatus::Active,
                PersistentIdStatusV1::Withdrawn => PersistentIdStatus::Withdrawn,
            },
            requested_at_ms: legacy.minted_at_ms,
            requested_by: legacy.minted_by,
            job_id: None,
            public: None,
            permission_path: None,
            minted_at_ms: legacy.minted_at_ms,
            minted_by: legacy.minted_by,
            failure: None,
            withdrawn_at_ms: legacy.withdrawn_at_ms,
            withdrawn_by: None,
            withdrawal_reason: None,
            revision: legacy.revision,
        }
    }
}

/// Mapping key: the document id alone, so a re-mint resolves the same row.
///
/// Migration contract for the first real second provider: introduce a v2 key as
/// `document_id || 0x00 || provider-name`, dual-read that key and this legacy
/// document-only key, copy each existing row under `(document, w3id)` without
/// deleting the legacy row, verify parity including terminal tombstones, then
/// flip writes and finally retire the legacy read after every holder converges.
/// The delimiter makes two provider names distinct while preserving the first
/// 16 bytes as a document scan prefix. Until that migration, changing this key
/// would break the deliberately retained 1:1 storage contract.
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

    // the conceptual PID is the graph IRI
    #[test]
    fn pid_is_iri() {
        let id = Ulid::from_bytes([1; 16]);
        let mapping = PersistentIdMapping::conceptual(id, user(), revision(1, 5));
        assert_eq!(mapping.pid, MetadataRegistryRecord::graph_iri_for(id));
        assert!(mapping.is_active());
        assert_eq!(mapping.minted_at_ms, Some(5));
        assert_eq!(persistent_id_key(id), id.to_bytes().to_vec());
    }

    // Administrative withdrawal is permanent and keeps its required evidence.
    #[test]
    fn admin_withdraw_is_permanent() {
        let id = Ulid::from_bytes([1; 16]);
        let mut mapping = PersistentIdMapping::conceptual(id, user(), revision(1, 5));
        assert!(mapping.admin_withdraw(user(), "invalid registration".into(), revision(2, 10)));
        assert_eq!(mapping.status, PersistentIdStatus::AdminWithdrawn);
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
        assert_eq!(mapping.withdrawn_by, Some(user()));
        assert_eq!(
            mapping.withdrawal_reason.as_deref(),
            Some("invalid registration")
        );
        assert!(!mapping.mark_tombstoned(revision(3, 20)));
        assert_eq!(mapping.withdrawn_at_ms, Some(10));
    }

    #[test]
    fn tombstone_omits_mint() {
        let id = Ulid::from_bytes([4; 16]);
        let mapping = PersistentIdMapping::tombstone(id, revision(1, 12));
        assert_eq!(mapping.status, PersistentIdStatus::Tombstoned);
        assert_eq!(mapping.minted_at_ms, None);
        assert_eq!(mapping.minted_by, None);
        assert_eq!(mapping.withdrawn_at_ms, Some(12));
    }

    #[test]
    fn merge_converges_unordered() {
        let id = Ulid::from_bytes([1; 16]);
        let active = PersistentIdMapping::conceptual(id, user(), revision(1, 5));
        let mut withdrawn = active.clone();
        withdrawn.admin_withdraw(user(), "retired".into(), revision(2, 9));

        let mut forward = active.clone();
        assert!(forward.merge(&withdrawn));
        let mut backward = withdrawn.clone();
        assert!(!backward.merge(&active));

        assert_eq!(forward, backward);
        assert_eq!(forward.status, PersistentIdStatus::AdminWithdrawn);
        assert_eq!(forward.revision, revision(2, 9));
        assert_eq!(forward.minted_at_ms, Some(5));
    }

    #[test]
    fn merge_keeps_tombstone() {
        let id = Ulid::from_bytes([1; 16]);
        let mut tombstone = PersistentIdMapping::tombstone(id, revision(2, 3));
        let active = PersistentIdMapping::conceptual(id, user(), revision(1, 5));

        assert!(tombstone.merge(&active));
        assert_eq!(tombstone.status, PersistentIdStatus::Tombstoned);
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

    #[test]
    fn requested_profile_has_one_typed_provider_identity() {
        let id = Ulid::from_bytes([8; 16]);
        let mapping = PersistentIdMapping::requested(
            id,
            true,
            user(),
            JobId::from_bytes([7; 16]),
            false,
            "/projects/private".into(),
            revision(1, 7),
        );

        assert_eq!(mapping.pid, PersistentIdMapping::profile_pid(id));
        assert_ne!(mapping.pid, MetadataRegistryRecord::graph_iri_for(id));
        assert_eq!(mapping.provider, PersistentIdProvider::W3id);
        assert_eq!(mapping.kind, PersistentIdKind::Conceptual);
        assert_eq!(mapping.status, PersistentIdStatus::Requested);
    }

    #[test]
    fn legacy_rows_decode_with_explicit_provider() {
        let id = Ulid::from_bytes([6; 16]);
        let legacy = PersistentIdMappingV1 {
            pid: MetadataRegistryRecord::graph_iri_for(id),
            target: id,
            kind: PersistentIdKind::Conceptual,
            status: PersistentIdStatusV1::Active,
            minted_at_ms: Some(5),
            minted_by: Some(user()),
            withdrawn_at_ms: None,
            revision: revision(1, 5),
        };

        let decoded =
            PersistentIdMapping::from_bytes(&postcard::to_allocvec(&legacy).unwrap()).unwrap();
        assert_eq!(decoded.provider, PersistentIdProvider::W3id);
        assert_eq!(decoded.status, PersistentIdStatus::Active);
        assert_eq!(decoded.target, id);
    }

    #[test]
    fn future_composite_key_migration_preserves_document_prefix() {
        fn future_key(document_id: Ulid, provider: &str) -> Vec<u8> {
            let mut key = persistent_id_key(document_id);
            key.push(0);
            key.extend_from_slice(provider.as_bytes());
            key
        }

        let id = Ulid::from_bytes([5; 16]);
        let current = persistent_id_key(id);
        let w3id = future_key(id, PersistentIdProvider::W3id.name());
        let second_provider = future_key(id, "example-second-provider");

        assert_eq!(current, id.to_bytes());
        assert!(w3id.starts_with(&current));
        assert!(second_provider.starts_with(&current));
        assert_ne!(w3id, second_provider);
    }
}
