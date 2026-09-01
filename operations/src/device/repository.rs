//! Storage shape of the device-local authoring intake.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
use aruna_core::metadata::{MetadataBatch, MetadataBatchSource};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key, TxnId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Entries one device may hold. A device authors for one person, so the queue
/// is a human-sized backlog rather than an unbounded spool.
pub const MAX_INTAKE_ENTRIES: usize = 256;

/// Entries one drain pass reads at a time.
pub const INTAKE_PAGE_SIZE: usize = 64;

/// Forward attempts before a still-retryable entry parks for the owner. An
/// entry that keeps failing must stay visible instead of retrying forever.
pub const MAX_INTAKE_ATTEMPTS: u32 = 8;

/// What the owner asked the device to publish once the realm is reachable: a
/// create of a new document, or an edit already applied to a local replica.
/// A delete of shared state still needs connectivity and is never queued.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntakeEntry {
    /// Stable local reference. It never becomes the realm document id.
    pub draft_id: Ulid,
    /// The owner the drain forwards as; the holder re-checks the binding.
    pub owner: UserId,
    pub group_id: GroupId,
    pub document_path: String,
    pub public: bool,
    /// RO-Crate JSON-LD exactly as the owner authored it. Empty for an edit,
    /// whose submission travels inside the kind.
    pub jsonld: String,
    pub created_at_ms: u64,
    pub state: IntakeState,
    pub kind: IntakeKind,
}

/// What one entry publishes. Appended after the create fields so a create
/// entry keeps describing itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntakeKind {
    Create,
    /// An OR-Set change set the owner already applied to this device's replica.
    /// A holder appends it as an `ApplyBatch` event, unchanged.
    Edit {
        document_id: Ulid,
        batch: Box<MetadataBatch>,
        authored: MetadataBatchSource,
    },
}

/// Lifecycle of one queued create.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntakeState {
    Pending {
        due_at_ms: u64,
        attempts: u32,
        last_error: Option<String>,
    },
    /// The realm document id is minted before the first forward, so a crash
    /// mid-publish re-forwards the same id and the holder's create-acceptance
    /// fence answers with the original record instead of a second document.
    Publishing {
        document_id: Ulid,
        due_at_ms: u64,
        attempts: u32,
    },
    Published {
        document_id: Ulid,
    },
    Failed {
        reason: String,
        retryable: bool,
        document_id: Option<Ulid>,
    },
}

impl IntakeEntry {
    pub fn new(
        draft_id: Ulid,
        owner: UserId,
        group_id: GroupId,
        document_path: String,
        public: bool,
        jsonld: String,
    ) -> Self {
        let now = unix_timestamp_millis();
        Self {
            draft_id,
            owner,
            group_id,
            document_path,
            public,
            jsonld,
            created_at_ms: now,
            state: IntakeState::Pending {
                due_at_ms: now,
                attempts: 0,
                last_error: None,
            },
            kind: IntakeKind::Create,
        }
    }

    /// One offline edit of a document this device holds a replica of. The batch
    /// is already merged locally; queueing only records that a holder has not
    /// seen it yet.
    pub fn edit(
        draft_id: Ulid,
        owner: UserId,
        record: &MetadataRegistryRecord,
        batch: MetadataBatch,
        authored: MetadataBatchSource,
    ) -> Self {
        let now = unix_timestamp_millis();
        Self {
            draft_id,
            owner,
            group_id: record.group_id,
            document_path: record.document_path.clone(),
            public: record.public,
            jsonld: String::new(),
            created_at_ms: now,
            state: IntakeState::Pending {
                due_at_ms: now,
                attempts: 0,
                last_error: None,
            },
            kind: IntakeKind::Edit {
                document_id: record.document_id,
                batch: Box::new(batch),
                authored,
            },
        }
    }

    /// The document this entry publishes onto, once it is known.
    pub fn document_id(&self) -> Option<Ulid> {
        match &self.kind {
            IntakeKind::Edit { document_id, .. } => Some(*document_id),
            IntakeKind::Create => match &self.state {
                IntakeState::Publishing { document_id, .. }
                | IntakeState::Published { document_id }
                | IntakeState::Failed {
                    document_id: Some(document_id),
                    ..
                } => Some(*document_id),
                IntakeState::Pending { .. }
                | IntakeState::Failed {
                    document_id: None, ..
                } => None,
            },
        }
    }

    /// Re-arms a retryable failure for an explicit owner-requested sync.
    pub fn retry_failed(&self, now_ms: u64) -> Option<IntakeState> {
        if !matches!(
            self.state,
            IntakeState::Failed {
                retryable: true,
                ..
            }
        ) {
            return None;
        }
        Some(match self.document_id() {
            Some(document_id) => IntakeState::Publishing {
                document_id,
                due_at_ms: now_ms,
                attempts: 0,
            },
            None => IntakeState::Pending {
                due_at_ms: now_ms,
                attempts: 0,
                last_error: None,
            },
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Whether the drain may pick this entry up now.
    pub fn is_due(&self, now_ms: u64) -> bool {
        match &self.state {
            IntakeState::Pending { due_at_ms, .. } | IntakeState::Publishing { due_at_ms, .. } => {
                *due_at_ms <= now_ms
            }
            IntakeState::Published { .. } | IntakeState::Failed { .. } => false,
        }
    }

    /// Attempts already spent, so a park decision does not have to match on
    /// the state twice.
    pub fn attempts(&self) -> u32 {
        match &self.state {
            IntakeState::Pending { attempts, .. } | IntakeState::Publishing { attempts, .. } => {
                *attempts
            }
            IntakeState::Published { .. } | IntakeState::Failed { .. } => 0,
        }
    }
}

/// The same entry under a new lifecycle state; everything the owner authored
/// stays frozen so a retry forwards the identical create.
pub fn entry_with_state(entry: &IntakeEntry, state: IntakeState) -> IntakeEntry {
    IntakeEntry {
        state,
        ..entry.clone()
    }
}

/// Draft ids are ULIDs, so the raw key orders the queue by creation time and a
/// forward scan is FIFO.
pub fn intake_key(draft_id: Ulid) -> Key {
    ByteView::from(draft_id.to_bytes().to_vec())
}

pub fn intake_entry(entry: &IntakeEntry) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        DEVICE_INTAKE_KEYSPACE.to_string(),
        intake_key(entry.draft_id),
        ByteView::from(entry.to_bytes()?),
    ))
}

pub fn scan_intake(start_after: Option<Key>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: DEVICE_INTAKE_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: INTAKE_PAGE_SIZE,
        txn_id,
    })
}

pub fn read_intake(draft_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: DEVICE_INTAKE_KEYSPACE.to_string(),
        key: intake_key(draft_id),
        txn_id,
    })
}

#[cfg(test)]
mod tests {
    use super::{IntakeEntry, IntakeState, intake_key};
    use aruna_core::metadata::{MetadataBatch, MetadataBatchSource, MetadataDot, MetadataQuadOp};
    use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
    use aruna_core::types::UserId;
    use craqle::VectorClock;
    use ulid::Ulid;

    fn record() -> MetadataRegistryRecord {
        let document_id = Ulid::from_bytes([2u8; 16]);
        MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([3u8; 32]),
            group_id: Ulid::from_bytes([1u8; 16]),
            document_id,
            document_path: "notes".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: "/notes".to_string(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
            establishing_event_id: Ulid::from_bytes([4u8; 16]),
            last_event_id: Ulid::from_bytes([5u8; 16]),
        }
    }

    fn batch() -> MetadataBatch {
        MetadataBatch {
            graph_iri: record().graph_iri,
            actor: [7u8; 32],
            counter: 1,
            base_clock: VectorClock::default(),
            ops: vec![MetadataQuadOp::Add {
                subject: "<https://example.org/s>".to_string(),
                predicate: "<https://example.org/p>".to_string(),
                object: "\"o\"".to_string(),
                dot: MetadataDot {
                    actor: [7u8; 32],
                    counter: 1,
                },
            }],
            timestamp_millis: 9,
        }
    }

    fn entry() -> IntakeEntry {
        IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([7u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            false,
            "{}".to_string(),
        )
    }

    #[test]
    fn keys_draft_id() {
        // The raw ULID is the key, so a forward scan is creation order.
        let entry = entry();
        assert_eq!(
            intake_key(entry.draft_id).as_ref(),
            entry.draft_id.to_bytes().as_slice()
        );
    }

    #[test]
    fn round_trips_entry() {
        let entry = entry();
        let bytes = entry.to_bytes().unwrap();
        assert_eq!(IntakeEntry::from_bytes(&bytes).unwrap(), entry);
    }

    #[test]
    fn round_trips_edit() {
        // The batch is the only copy of what the owner changed offline, so it
        // has to survive the store byte for byte.
        let entry = IntakeEntry::edit(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32])),
            &record(),
            batch(),
            MetadataBatchSource::UpsertDataEntity {
                jsonld: r#"{"@id":"data.csv"}"#.to_string(),
            },
        );
        let decoded = IntakeEntry::from_bytes(&entry.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(decoded.document_id(), Some(record().document_id));
    }

    #[test]
    fn holds_future_entries() {
        let mut entry = entry();
        entry.state = IntakeState::Pending {
            due_at_ms: 1_000,
            attempts: 1,
            last_error: Some("unreachable".to_string()),
        };
        assert!(!entry.is_due(999));
        assert!(entry.is_due(1_000));
    }

    #[test]
    fn settles_terminal_entries() {
        // A published or parked entry stays visible but is never re-forwarded.
        let mut entry = entry();
        entry.state = IntakeState::Published {
            document_id: Ulid::generate(),
        };
        assert!(!entry.is_due(u64::MAX));
        entry.state = IntakeState::Failed {
            reason: "denied".to_string(),
            retryable: false,
            document_id: None,
        };
        assert!(!entry.is_due(u64::MAX));
        assert!(entry.retry_failed(1_000).is_none());
        entry.state = IntakeState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
            document_id: None,
        };
        assert!(matches!(
            entry.retry_failed(1_000),
            Some(IntakeState::Pending {
                due_at_ms: 1_000,
                attempts: 0,
                ..
            })
        ));
        let document_id = Ulid::generate();
        entry.state = IntakeState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
            document_id: Some(document_id),
        };
        assert!(matches!(
            entry.retry_failed(1_000),
            Some(IntakeState::Publishing {
                document_id: retried,
                due_at_ms: 1_000,
                attempts: 0,
            }) if retried == document_id
        ));
    }
}
