use crate::NodeId;
use crate::admin_documents::AdminDocumentTarget;
use crate::document::{DocumentSyncEvent, DocumentSyncTarget};
use crate::errors::ConversionError;
use crate::keyspaces::{SYNC_QUARANTINE_KEYSPACE, SYNC_QUARANTINE_USAGE_KEYSPACE};
use crate::types::{Key, KeySpace, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Single row of the usage keyspace; the quarantine keyspace itself stays a pure
/// `topic || event_id` prefix scan.
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

/// A replicated sync event that failed permanent validation, retained for
/// inspection instead of being silently dropped (#338). Evidence is committed in
/// the same transaction as the cursor that advances past it, so a topic never
/// moves ahead of an unpersisted rejection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncQuarantineRecord {
    /// Raw sync-topic bytes the event arrived on.
    pub topic: Vec<u8>,
    pub event_id: Ulid,
    pub family: SyncQuarantineFamily,
    pub target: DocumentSyncTarget,
    /// `Upsert`/`Delete` carry their actor in the revision, `AdminOperation` its origin.
    pub origin_node_id: NodeId,
    pub reason: String,
    pub quarantined_at_ms: u64,
    /// Set by an operator through the admin surface; only acknowledged rows are prunable.
    pub acknowledged: bool,
    /// The postcard-encoded complete `DocumentSyncEvent`, for out-of-band inspection.
    pub event_bytes: Vec<u8>,
}

impl SyncQuarantineRecord {
    pub fn from_event(
        topic: &[u8],
        event: &DocumentSyncEvent,
        reason: &str,
        quarantined_at_ms: u64,
    ) -> Self {
        Self {
            topic: topic.to_vec(),
            event_id: event.event_id(),
            family: event_family(event),
            target: event.target().clone(),
            origin_node_id: event_origin(event),
            reason: reason.to_string(),
            quarantined_at_ms,
            acknowledged: false,
            event_bytes: postcard::to_allocvec(event).unwrap_or_default(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn decode_event(&self) -> Result<DocumentSyncEvent, ConversionError> {
        Ok(postcard::from_bytes(&self.event_bytes)?)
    }

    pub fn storage_key(&self) -> Vec<u8> {
        sync_quarantine_key(&self.topic, self.event_id)
    }
}

/// Key: `topic || event_id`. Re-delivery of the same poison event overwrites its
/// own row, so the store is bounded by the count of distinct invalid events.
pub fn sync_quarantine_key(topic: &[u8], event_id: Ulid) -> Vec<u8> {
    let mut key = Vec::with_capacity(topic.len() + 16);
    key.extend_from_slice(topic);
    key.extend_from_slice(&event_id.to_bytes());
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
    pub topic: &'a [u8],
    pub event: &'a DocumentSyncEvent,
    pub reason: &'a str,
    pub quarantined_at_ms: u64,
    /// Encoded length of the row already stored under this key. A re-delivered
    /// poison event replaces its own row, so its bytes are swapped, not added.
    pub replaced_bytes: Option<u64>,
}

/// `entries` is the evidence row followed by the usage row. Several rejects in
/// one batch chain `usage` into the next call and fold every entry into the same
/// transaction: the repeated usage key resolves to the last write, which is the
/// batch's final accounting.
#[derive(Debug)]
pub struct SyncQuarantineWrite {
    pub record: SyncQuarantineRecord,
    pub usage: SyncQuarantineUsage,
    pub entries: Vec<(KeySpace, Key, Value)>,
}

/// Build the evidence row and the matching usage row for one permanently
/// rejected event. Fails closed at capacity: the caller must leave the topic
/// cursor unpersisted rather than drop the evidence or grow the store.
pub fn build_quarantine_entries(
    input: SyncQuarantineInput<'_>,
    usage: SyncQuarantineUsage,
    capacity: SyncQuarantineCapacity,
) -> Result<SyncQuarantineWrite, SyncQuarantineError> {
    let record = SyncQuarantineRecord::from_event(
        input.topic,
        input.event,
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
        entries: vec![
            (
                SYNC_QUARANTINE_KEYSPACE.to_string(),
                ByteView::from(record.storage_key()),
                ByteView::from(value),
            ),
            quarantine_usage_entry(usage)?,
        ],
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

    #[test]
    fn record_roundtrips_families() {
        for event in families() {
            let record = SyncQuarantineRecord::from_event(&[7u8; 32], &event, "invalid", 42);
            let decoded = SyncQuarantineRecord::from_bytes(&record.to_bytes().unwrap()).unwrap();
            assert_eq!(decoded, record);
            assert!(!decoded.acknowledged);
            assert_eq!(decoded.event_id, event.event_id());
            assert_eq!(&decoded.target, event.target());
            assert_eq!(decoded.origin_node_id, node());
            assert_eq!(decoded.decode_event().unwrap(), event);
        }
    }

    #[test]
    fn key_prefixes_topic() {
        let record = SyncQuarantineRecord::from_event(&[7u8; 32], &families()[0], "invalid", 42);
        let key = record.storage_key();
        assert!(key.starts_with(&record.topic));
        assert_eq!(key.len(), record.topic.len() + 16);
    }

    #[test]
    fn build_emits_entries() {
        let write = build_quarantine_entries(
            SyncQuarantineInput {
                topic: &[7u8; 32],
                event: &families()[0],
                reason: "invalid",
                quarantined_at_ms: 42,
                replaced_bytes: None,
            },
            SyncQuarantineUsage::default(),
            SyncQuarantineCapacity::default(),
        )
        .unwrap();
        assert_eq!(write.entries.len(), 2);
        assert_eq!(write.entries[0].0, SYNC_QUARANTINE_KEYSPACE);
        assert_eq!(write.entries[1].0, SYNC_QUARANTINE_USAGE_KEYSPACE);
        assert_eq!(write.usage.records, 1);
        assert_eq!(write.usage.bytes, write.entries[0].2.len() as u64);
    }

    #[test]
    fn capacity_blocks_build() {
        let usage = SyncQuarantineUsage {
            records: 2,
            bytes: 10,
        };
        let error = build_quarantine_entries(
            SyncQuarantineInput {
                topic: &[7u8; 32],
                event: &families()[0],
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
            topic: &[7u8; 32],
            event: &event,
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
}
