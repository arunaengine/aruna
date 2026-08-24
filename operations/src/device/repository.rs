//! Storage shape of the device-local authoring intake.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::DEVICE_INTAKE_KEYSPACE;
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

/// What the owner asked the device to create once the realm is reachable.
/// Only creates queue: an update or delete of shared state must be refused
/// while the realm is unreachable rather than replayed later.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntakeEntry {
    /// Stable local reference. It never becomes the realm document id.
    pub draft_id: Ulid,
    /// The owner the drain forwards as; the holder re-checks the binding.
    pub owner: UserId,
    pub group_id: GroupId,
    pub document_path: String,
    pub public: bool,
    /// RO-Crate JSON-LD exactly as the owner authored it.
    pub jsonld: String,
    pub created_at_ms: u64,
    pub state: IntakeState,
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
        }
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
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use ulid::Ulid;

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
    fn keys_the_raw_draft_id() {
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
    fn holds_back_future_entries() {
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
        };
        assert!(!entry.is_due(u64::MAX));
    }
}
