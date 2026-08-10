use crate::NodeId;
use crate::admin_documents::AdminDocumentTarget;
use crate::document::{DocumentSyncEvent, DocumentSyncTarget};
use crate::errors::ConversionError;
use crate::keyspaces::{SYNC_QUARANTINE_KEYSPACE, SYNC_QUARANTINE_USAGE_KEYSPACE};
use crate::types::{Key, KeySpace, Value};
use byteview::ByteView;
use irokle::{ActorId, TopicId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Single row of the usage keyspace; the quarantine keyspace itself stays a pure
/// `topic || actor || actor_seq` prefix scan.
pub const SYNC_QUARANTINE_USAGE_KEY: &[u8] = b"usage";

/// Hard defaults for [`SyncQuarantineCapacity`].
pub const SYNC_QUARANTINE_MAX_RECORDS: u64 = 4_096;
pub const SYNC_QUARANTINE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Which `DocumentSyncEvent` variant the retained envelope carries, so listings
/// can group by family without decoding `event_bytes`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncQuarantineFamily {
    Upsert,
    Delete,
    AdminOperation,
}

impl SyncQuarantineFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
            Self::AdminOperation => "admin_operation",
        }
    }
}

/// Immutable transport identity of a rejected operation: the topic it arrived
/// on, the signed publisher actor, and that actor's sequence. No payload field
/// takes part, so two publishers reusing one `event_id` keep distinct rows and
/// a redelivery replaces exactly its own row.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SyncQuarantineIdentity {
    pub topic: TopicId,
    pub actor: ActorId,
    pub actor_seq: u64,
}

impl SyncQuarantineIdentity {
    pub fn from_parts(topic: [u8; 32], actor: [u8; 32], actor_seq: u64) -> Self {
        Self {
            topic: TopicId::from_bytes(topic),
            actor: ActorId::from_bytes(actor),
            actor_seq,
        }
    }

    pub fn storage_key(&self) -> Vec<u8> {
        sync_quarantine_key(self)
    }
}

/// What the rejected operation carried. A payload that cannot be decoded into a
/// `DocumentSyncEvent` at all is retained raw, so a poison op is still evidence
/// instead of an error that replays forever.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncQuarantineEvidence {
    Event {
        event_id: Ulid,
        family: SyncQuarantineFamily,
        target: DocumentSyncTarget,
        /// `Upsert`/`Delete` carry their actor in the revision, `AdminOperation` its origin.
        origin_node_id: NodeId,
        /// The postcard-encoded complete `DocumentSyncEvent`.
        bytes: Vec<u8>,
    },
    Raw {
        /// The undecodable transport payload exactly as it arrived.
        bytes: Vec<u8>,
    },
}

impl SyncQuarantineEvidence {
    pub fn from_event(event: &DocumentSyncEvent) -> Self {
        Self::Event {
            event_id: event.event_id(),
            family: event_family(event),
            target: event.target().clone(),
            origin_node_id: event_origin(event),
            bytes: postcard::to_allocvec(event).unwrap_or_default(),
        }
    }

    pub fn raw(bytes: Vec<u8>) -> Self {
        Self::Raw { bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Event { bytes, .. } | Self::Raw { bytes } => bytes,
        }
    }
}

/// A replicated sync event that failed permanent validation, retained for
/// inspection instead of being silently dropped (#338). Evidence is committed in
/// the same transaction as the cursor that advances past it, so a topic never
/// moves ahead of an unpersisted rejection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncQuarantineRecord {
    pub identity: SyncQuarantineIdentity,
    pub reason: String,
    pub quarantined_at_ms: u64,
    /// Set by an operator through the admin surface; only acknowledged rows are prunable.
    pub acknowledged: bool,
    pub evidence: SyncQuarantineEvidence,
}

impl SyncQuarantineRecord {
    pub fn new(
        identity: SyncQuarantineIdentity,
        evidence: SyncQuarantineEvidence,
        reason: &str,
        quarantined_at_ms: u64,
    ) -> Self {
        Self {
            identity,
            reason: reason.to_string(),
            quarantined_at_ms,
            acknowledged: false,
            evidence,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// `None` for raw evidence, which by definition has no decodable event.
    pub fn decoded_event(&self) -> Option<DocumentSyncEvent> {
        match &self.evidence {
            SyncQuarantineEvidence::Event { bytes, .. } => postcard::from_bytes(bytes).ok(),
            SyncQuarantineEvidence::Raw { .. } => None,
        }
    }

    pub fn event_id(&self) -> Option<Ulid> {
        match &self.evidence {
            SyncQuarantineEvidence::Event { event_id, .. } => Some(*event_id),
            SyncQuarantineEvidence::Raw { .. } => None,
        }
    }

    pub fn family(&self) -> Option<SyncQuarantineFamily> {
        match &self.evidence {
            SyncQuarantineEvidence::Event { family, .. } => Some(*family),
            SyncQuarantineEvidence::Raw { .. } => None,
        }
    }

    pub fn target(&self) -> Option<&DocumentSyncTarget> {
        match &self.evidence {
            SyncQuarantineEvidence::Event { target, .. } => Some(target),
            SyncQuarantineEvidence::Raw { .. } => None,
        }
    }

    pub fn origin(&self) -> Option<NodeId> {
        match &self.evidence {
            SyncQuarantineEvidence::Event { origin_node_id, .. } => Some(*origin_node_id),
            SyncQuarantineEvidence::Raw { .. } => None,
        }
    }

    pub fn storage_key(&self) -> Vec<u8> {
        self.identity.storage_key()
    }
}

/// Key: `topic || actor || actor_seq`, all immutable transport identity. The
/// sequence is big-endian so a topic's rows sort by publisher and delivery
/// order, and a redelivery of the same op overwrites exactly its own row.
pub fn sync_quarantine_key(identity: &SyncQuarantineIdentity) -> Vec<u8> {
    let mut key = Vec::with_capacity(TopicId::LEN + ActorId::LEN + 8);
    key.extend_from_slice(identity.topic.as_bytes());
    key.extend_from_slice(identity.actor.as_bytes());
    key.extend_from_slice(&identity.actor_seq.to_be_bytes());
    key
}

fn event_family(event: &DocumentSyncEvent) -> SyncQuarantineFamily {
    match event {
        DocumentSyncEvent::Upsert { .. } => SyncQuarantineFamily::Upsert,
        DocumentSyncEvent::Delete { .. } => SyncQuarantineFamily::Delete,
        DocumentSyncEvent::AdminOperation { .. } => SyncQuarantineFamily::AdminOperation,
    }
}

fn event_origin(event: &DocumentSyncEvent) -> NodeId {
    match event {
        DocumentSyncEvent::Upsert { change, .. } | DocumentSyncEvent::Delete { change, .. } => {
            change.current.actor
        }
        DocumentSyncEvent::AdminOperation { event, .. } => event.origin_node_id,
    }
}

/// The document-sync target an admin operation rides under, mirroring the arms
/// of the admin apply dispatch.
pub fn admin_sync_target(target: &AdminDocumentTarget) -> DocumentSyncTarget {
    match target {
        AdminDocumentTarget::Group { group_id } => DocumentSyncTarget::GroupAuthorization {
            group_id: *group_id,
        },
        AdminDocumentTarget::Realm { realm_id } => DocumentSyncTarget::RealmAuthorization {
            realm_id: *realm_id,
        },
        AdminDocumentTarget::RealmConfig { realm_id } => DocumentSyncTarget::RealmConfig {
            realm_id: *realm_id,
        },
        AdminDocumentTarget::User { user_id } => DocumentSyncTarget::User { user_id: *user_id },
    }
}

/// Durable accounting for the quarantine store, written in the same batch as
/// every row write and every prune delete so a crash cannot desynchronize it.
/// Acknowledged rows keep occupying capacity until they are pruned.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncQuarantineUsage {
    pub records: u64,
    pub bytes: u64,
}

impl SyncQuarantineUsage {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Saturating so a lost row can never underflow the counter into a huge value.
    pub fn release(&mut self, records: u64, bytes: u64) {
        self.records = self.records.saturating_sub(records);
        self.bytes = self.bytes.saturating_sub(bytes);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncQuarantineCapacity {
    pub max_records: u64,
    pub max_bytes: u64,
}

impl Default for SyncQuarantineCapacity {
    fn default() -> Self {
        Self {
            max_records: SYNC_QUARANTINE_MAX_RECORDS,
            max_bytes: SYNC_QUARANTINE_MAX_BYTES,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum SyncQuarantineError {
    #[error(
        "sync quarantine at capacity ({records}/{max_records} records, {bytes}/{max_bytes} bytes)"
    )]
    AtCapacity {
        records: u64,
        max_records: u64,
        bytes: u64,
        max_bytes: u64,
    },
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

pub struct SyncQuarantineInput<'a> {
    pub identity: SyncQuarantineIdentity,
    pub evidence: SyncQuarantineEvidence,
    pub reason: &'a str,
    pub quarantined_at_ms: u64,
    /// Encoded length of the value already held under this key, whether already
    /// stored or pending earlier in the same batch. A redelivery replaces its
    /// own row, so its bytes are swapped, not added.
    pub replaced_bytes: Option<u64>,
}

/// One evidence row plus the usage total it produces. Several rejects in one
/// batch chain `usage` into the next call, so the batch's last usage value is
/// its complete accounting.
#[derive(Debug)]
pub struct SyncQuarantineWrite {
    pub record: SyncQuarantineRecord,
    pub usage: SyncQuarantineUsage,
    pub row: (KeySpace, Key, Value),
}

/// Build the evidence row for one permanently rejected operation and the usage
/// total it leaves behind. Fails closed at capacity: the caller must leave the
/// topic cursor unpersisted rather than drop the evidence or grow the store.
pub fn build_quarantine_entries(
    input: SyncQuarantineInput<'_>,
    usage: SyncQuarantineUsage,
    capacity: SyncQuarantineCapacity,
) -> Result<SyncQuarantineWrite, SyncQuarantineError> {
    let record = SyncQuarantineRecord::new(
        input.identity,
        input.evidence,
        input.reason,
        input.quarantined_at_ms,
    );
    let value = record.to_bytes()?;
    let replaced = input.replaced_bytes.unwrap_or_default();
    let mut usage = usage;
    if input.replaced_bytes.is_none() {
        usage.records = usage.records.saturating_add(1);
    }
    usage.bytes = usage
        .bytes
        .saturating_sub(replaced)
        .saturating_add(value.len() as u64);
    check_quarantine_capacity(usage, capacity)?;

    Ok(SyncQuarantineWrite {
        row: (
            SYNC_QUARANTINE_KEYSPACE.to_string(),
            ByteView::from(record.storage_key()),
            ByteView::from(value),
        ),
        record,
        usage,
    })
}

pub fn check_quarantine_capacity(
    usage: SyncQuarantineUsage,
    capacity: SyncQuarantineCapacity,
) -> Result<(), SyncQuarantineError> {
    if usage.records > capacity.max_records || usage.bytes > capacity.max_bytes {
        return Err(SyncQuarantineError::AtCapacity {
            records: usage.records,
            max_records: capacity.max_records,
            bytes: usage.bytes,
            max_bytes: capacity.max_bytes,
        });
    }
    Ok(())
}

pub fn quarantine_usage_entry(
    usage: SyncQuarantineUsage,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        SYNC_QUARANTINE_USAGE_KEYSPACE.to_string(),
        ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
        ByteView::from(usage.to_bytes()?),
    ))
}

pub fn quarantine_row_entry(
    record: &SyncQuarantineRecord,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        SYNC_QUARANTINE_KEYSPACE.to_string(),
        ByteView::from(record.storage_key()),
        ByteView::from(record.to_bytes()?),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_documents::{AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation};
    use crate::document::{DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision};
    use crate::structs::{Actor, PlacementRef, RealmId};
    use crate::types::UserId;

    fn node() -> NodeId {
        NodeId::from_bytes(&[1u8; 32]).unwrap()
    }

    fn change(kind: DocumentSyncChangeKind) -> DocumentSyncChange {
        DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 3,
                event_id: Ulid::from_bytes([4; 16]),
                actor: node(),
                updated_at_ms: 11,
            },
            kind,
            placement: PlacementRef::NIL,
        }
    }

    fn admin_event() -> AdminDocumentEvent {
        let realm_id = RealmId([9; 32]);
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([5; 16]),
            target: AdminDocumentTarget::RealmConfig { realm_id },
            origin_node_id: node(),
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: node(),
                user_id: UserId::local(Ulid::from_bytes([6; 16]), realm_id),
                realm_id,
            },
            op: AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "quarantined".to_string(),
            },
        }
    }

    fn families() -> Vec<DocumentSyncEvent> {
        let realm_id = RealmId([9; 32]);
        vec![
            DocumentSyncEvent::Upsert {
                event_id: Ulid::from_bytes([4; 16]),
                target: DocumentSyncTarget::RealmConfig { realm_id },
                bytes: vec![1, 2, 3],
                change: change(DocumentSyncChangeKind::Upsert),
            },
            DocumentSyncEvent::Delete {
                event_id: Ulid::from_bytes([4; 16]),
                target: DocumentSyncTarget::User {
                    user_id: UserId::local(Ulid::from_bytes([7; 16]), realm_id),
                },
                change: change(DocumentSyncChangeKind::Delete),
            },
            DocumentSyncEvent::AdminOperation {
                target: DocumentSyncTarget::RealmConfig { realm_id },
                event: Box::new(admin_event()),
                placement: PlacementRef::NIL,
            },
        ]
    }

    fn identity(actor_seq: u64) -> SyncQuarantineIdentity {
        SyncQuarantineIdentity {
            topic: TopicId::from_bytes([7; 32]),
            actor: ActorId::from_bytes([8; 32]),
            actor_seq,
        }
    }

    #[test]
    fn record_roundtrips_families() {
        for event in families() {
            let record = SyncQuarantineRecord::new(
                identity(1),
                SyncQuarantineEvidence::from_event(&event),
                "invalid",
                42,
            );
            let decoded = SyncQuarantineRecord::from_bytes(&record.to_bytes().unwrap()).unwrap();
            assert_eq!(decoded, record);
            assert!(!decoded.acknowledged);
            assert_eq!(decoded.event_id(), Some(event.event_id()));
            assert_eq!(decoded.target(), Some(event.target()));
            assert_eq!(decoded.origin(), Some(node()));
            assert_eq!(decoded.decoded_event().unwrap(), event);
        }
    }

    #[test]
    fn raw_record_keeps_bytes() {
        let record = SyncQuarantineRecord::new(
            identity(3),
            SyncQuarantineEvidence::raw(vec![9, 9, 9]),
            "undecodable",
            42,
        );
        let decoded = SyncQuarantineRecord::from_bytes(&record.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded.evidence.bytes(), &[9, 9, 9]);
        assert_eq!(decoded.event_id(), None);
        assert_eq!(decoded.family(), None);
        assert_eq!(decoded.target(), None);
        assert!(decoded.decoded_event().is_none());
    }

    #[test]
    fn key_is_transport_identity() {
        let key = identity(5).storage_key();
        assert_eq!(key.len(), 72);
        assert!(key.starts_with(&[7u8; 32]));
        assert_eq!(&key[32..64], &[8u8; 32]);
        assert_eq!(&key[64..], &5u64.to_be_bytes());
        // The payload never enters the key, and the sequence orders the rows.
        assert_ne!(identity(5).storage_key(), identity(6).storage_key());
        assert!(identity(5).storage_key() < identity(6).storage_key());
    }

    #[test]
    fn build_emits_row() {
        let write = build_quarantine_entries(
            SyncQuarantineInput {
                identity: identity(1),
                evidence: SyncQuarantineEvidence::from_event(&families()[0]),
                reason: "invalid",
                quarantined_at_ms: 42,
                replaced_bytes: None,
            },
            SyncQuarantineUsage::default(),
            SyncQuarantineCapacity::default(),
        )
        .unwrap();
        assert_eq!(write.row.0, SYNC_QUARANTINE_KEYSPACE);
        assert_eq!(write.row.1.as_ref(), identity(1).storage_key());
        assert_eq!(write.usage.records, 1);
        assert_eq!(write.usage.bytes, write.row.2.len() as u64);
        assert_eq!(
            quarantine_usage_entry(write.usage).unwrap().0,
            SYNC_QUARANTINE_USAGE_KEYSPACE
        );
    }

    #[test]
    fn capacity_blocks_build() {
        let usage = SyncQuarantineUsage {
            records: 2,
            bytes: 10,
        };
        let error = build_quarantine_entries(
            SyncQuarantineInput {
                identity: identity(1),
                evidence: SyncQuarantineEvidence::from_event(&families()[0]),
                reason: "invalid",
                quarantined_at_ms: 42,
                replaced_bytes: None,
            },
            usage,
            SyncQuarantineCapacity {
                max_records: 2,
                max_bytes: SYNC_QUARANTINE_MAX_BYTES,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SyncQuarantineError::AtCapacity { records: 3, .. }
        ));
    }

    #[test]
    fn redelivery_reuses_usage() {
        let event = families().remove(2);
        let input = |replaced| SyncQuarantineInput {
            identity: identity(2),
            evidence: SyncQuarantineEvidence::from_event(&event),
            reason: "invalid",
            quarantined_at_ms: 42,
            replaced_bytes: replaced,
        };
        let capacity = SyncQuarantineCapacity {
            max_records: 1,
            max_bytes: SYNC_QUARANTINE_MAX_BYTES,
        };
        let first = build_quarantine_entries(input(None), SyncQuarantineUsage::default(), capacity)
            .unwrap();
        let again = build_quarantine_entries(input(Some(first.usage.bytes)), first.usage, capacity)
            .unwrap();
        assert_eq!(again.usage, first.usage);
    }

    /// One `event_id` from two publishers is two rows, and the same op from one
    /// publisher is one row whose bytes are swapped rather than added.
    #[test]
    fn identity_separates_publishers() {
        let event = families().remove(0);
        let other = SyncQuarantineIdentity {
            actor: ActorId::from_bytes([9; 32]),
            ..identity(1)
        };
        let build = |identity, usage, replaced| {
            build_quarantine_entries(
                SyncQuarantineInput {
                    identity,
                    evidence: SyncQuarantineEvidence::from_event(&event),
                    reason: "invalid",
                    quarantined_at_ms: 42,
                    replaced_bytes: replaced,
                },
                usage,
                SyncQuarantineCapacity::default(),
            )
            .unwrap()
        };
        let first = build(identity(1), SyncQuarantineUsage::default(), None);
        let second = build(other, first.usage, None);
        assert_ne!(first.row.1, second.row.1);
        assert_eq!(second.usage.records, 2);

        let replayed = build(identity(1), second.usage, Some(first.row.2.len() as u64));
        assert_eq!(replayed.row.1, first.row.1);
        assert_eq!(replayed.usage, second.usage);
    }
}
