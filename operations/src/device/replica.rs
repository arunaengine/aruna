//! The metadata documents this device keeps a local craqle replica of.
//!
//! The ledger is what makes a device usable offline: it names the documents the
//! owner selected, remembers how far each replica has been synced, and carries
//! the registry record and the last valid render so a read answers without a
//! holder. Nothing here is realm authority; a replica becomes realm state only
//! when the intake drain forwards the edits made on it.

use std::sync::Arc;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_REPLICA_KEYSPACE;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key, Value};
use byteview::ByteView;
use craqle::VectorClock;
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;

use super::repository::{IntakeEntry, IntakeState};

/// Documents one device may keep a replica of. A device serves one person, so
/// this is a working set rather than an archive.
pub const MAX_DEVICE_REPLICAS: usize = 512;

/// Rows one scan reads at a time.
pub const REPLICA_PAGE_SIZE: usize = 128;

/// How the document came to be on this device.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaOrigin {
    /// Created here, so the realm learned about it from this device.
    Device,
    /// Fetched from a holder because the owner selected or edited it.
    Realm,
}

/// What the last refresh or forward left the replica in. Pending and failed
/// edits are not stored here: they are the intake rows joined onto the record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Created on this device and not published yet.
    LocalOnly,
    /// The replica agrees with what the realm last served.
    Synced,
    /// The merged state fails profile validation; the last valid render is
    /// still what this device displays.
    Invalid,
    /// The last refresh did not complete. The replica keeps serving.
    Failed,
}

/// One selected document as this device holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRecord {
    pub document_id: Ulid,
    pub group_id: GroupId,
    pub document_path: String,
    pub graph_iri: String,
    pub origin: ReplicaOrigin,
    /// Whether the owner wants this document available offline. A deselected
    /// record is kept only while it still has edits the realm has not seen.
    pub selected: bool,
    /// The local graph's clock after the last local edit or install.
    pub local_clock: VectorClock,
    /// The holder's clock at the last snapshot this device installed.
    pub realm_clock: VectorClock,
    pub last_synced_ms: Option<u64>,
    pub pending_edits: u32,
    pub state: ReplicaState,
    pub last_error: Option<String>,
    /// The registry record the last refresh carried, so a local read answers
    /// the same shape a holder would.
    pub record: Option<Box<MetadataRegistryRecord>>,
    /// The last valid render of the merged graph, which is what this device
    /// displays and exports.
    pub displayed_jsonld: String,
    pub dataset_digest: Option<[u8; 32]>,
    /// Profile findings the last refresh reported for the merged state.
    pub findings: u32,
}

impl ReplicaRecord {
    /// A replica the owner selected but that has not been refreshed yet.
    pub fn new(
        document_id: Ulid,
        group_id: GroupId,
        document_path: String,
        origin: ReplicaOrigin,
    ) -> Self {
        Self {
            document_id,
            group_id,
            document_path,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            origin,
            selected: true,
            local_clock: VectorClock::default(),
            realm_clock: VectorClock::default(),
            last_synced_ms: None,
            pending_edits: 0,
            state: match origin {
                ReplicaOrigin::Device => ReplicaState::LocalOnly,
                ReplicaOrigin::Realm => ReplicaState::Synced,
            },
            last_error: None,
            record: None,
            displayed_jsonld: String::new(),
            dataset_digest: None,
            findings: 0,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Whether a local read may be answered from this replica alone.
    pub fn serves_reads(&self) -> bool {
        self.record.is_some() && !self.displayed_jsonld.is_empty()
    }
}

/// What the owner sees for one document in the Sync view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentState {
    LocalOnly,
    Pending,
    Publishing,
    Invalid,
    Failed,
    Synced,
}

impl DocumentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalOnly => "local_only",
            Self::Pending => "pending",
            Self::Publishing => "publishing",
            Self::Invalid => "invalid",
            Self::Failed => "failed",
            Self::Synced => "synced",
        }
    }
}

/// Joins one replica with the intake entries that publish onto it. A failure
/// the owner has to answer outranks work still in flight, which outranks the
/// state the last refresh left.
pub fn document_state(replica: &ReplicaRecord, intake: &[&IntakeEntry]) -> DocumentState {
    let mut queued = None;
    for entry in intake {
        match &entry.state {
            IntakeState::Failed { .. } => return DocumentState::Failed,
            IntakeState::Pending { .. } => queued = Some(DocumentState::Pending),
            IntakeState::Publishing { .. } => {
                queued.get_or_insert(DocumentState::Publishing);
            }
            IntakeState::Published { .. } => {}
        }
    }
    if let Some(queued) = queued {
        return queued;
    }
    match replica.state {
        ReplicaState::LocalOnly => DocumentState::LocalOnly,
        ReplicaState::Invalid => DocumentState::Invalid,
        ReplicaState::Failed => DocumentState::Failed,
        ReplicaState::Synced => DocumentState::Synced,
    }
}

pub fn replica_key(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub fn replica_entry(replica: &ReplicaRecord) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        DEVICE_REPLICA_KEYSPACE.to_string(),
        replica_key(replica.document_id),
        ByteView::from(replica.to_bytes()?),
    ))
}

pub fn scan_replicas(start_after: Option<Key>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: REPLICA_PAGE_SIZE,
        txn_id: None,
    })
}

/// One replica, or `None` for both an absent row and an unreadable one: every
/// caller treats an unreadable row as a document this device does not hold.
pub async fn read_replica(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Option<ReplicaRecord> {
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
            key: replica_key(document_id),
            txn_id: None,
        })
        .await
    else {
        return None;
    };
    ReplicaRecord::from_bytes(&bytes).ok()
}

pub async fn store_replica(context: &Arc<DriverContext>, replica: &ReplicaRecord) -> bool {
    let Ok((key_space, key, value)) = replica_entry(replica) else {
        warn!(document_id = %replica.document_id, "Failed to encode a device replica");
        return false;
    };
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    )
}

pub async fn delete_replica(context: &Arc<DriverContext>, document_id: Ulid) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
                key: replica_key(document_id),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    )
}

/// Every replica this device holds, in document order. `None` means the scan
/// itself failed, which a caller must not read as an empty ledger.
pub async fn list_replicas(context: &Arc<DriverContext>) -> Option<Vec<ReplicaRecord>> {
    let mut replicas = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let Effect::Storage(effect) = scan_replicas(cursor) else {
            return None;
        };
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = context.storage_handle.send_storage_effect(effect).await
        else {
            warn!("Failed to scan the device replica ledger");
            return None;
        };
        replicas.extend(
            values
                .into_iter()
                .filter_map(|(_, bytes)| ReplicaRecord::from_bytes(&bytes).ok()),
        );
        match next_start_after {
            Some(next) => cursor = Some(next),
            None => return Some(replicas),
        }
    }
}

/// Records that this device now holds an edit the realm has not seen.
pub fn mark_edited(replica: &mut ReplicaRecord, local_clock: VectorClock) {
    replica.local_clock = local_clock;
    replica.pending_edits = replica.pending_edits.saturating_add(1);
    replica.selected = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;

    fn replica() -> ReplicaRecord {
        ReplicaRecord::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            ReplicaOrigin::Realm,
        )
    }

    fn entry(state: IntakeState) -> IntakeEntry {
        let mut entry = IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([8u8; 32])),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            false,
            "{}".to_string(),
        );
        entry.state = state;
        entry
    }

    #[test]
    fn round_trips_replica() {
        let mut replica = replica();
        replica.displayed_jsonld = r#"{"@graph":[]}"#.to_string();
        replica.dataset_digest = Some([4u8; 32]);
        replica.last_synced_ms = Some(17);
        let decoded = ReplicaRecord::from_bytes(&replica.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, replica);
        assert_eq!(
            replica_key(replica.document_id).as_ref(),
            replica.document_id.to_bytes().as_slice()
        );
    }

    #[test]
    fn reports_queued_work() {
        // What the owner still has to act on outranks work in flight, which
        // outranks the state the last refresh left.
        let replica = replica();
        let pending = entry(IntakeState::Pending {
            due_at_ms: 0,
            attempts: 1,
            last_error: None,
        });
        let publishing = entry(IntakeState::Publishing {
            document_id: replica.document_id,
            due_at_ms: 0,
            attempts: 1,
        });
        let failed = entry(IntakeState::Failed {
            reason: "denied".to_string(),
            retryable: false,
        });

        assert_eq!(document_state(&replica, &[]), DocumentState::Synced);
        assert_eq!(
            document_state(&replica, &[&publishing]),
            DocumentState::Publishing
        );
        assert_eq!(
            document_state(&replica, &[&publishing, &pending]),
            DocumentState::Pending
        );
        assert_eq!(
            document_state(&replica, &[&pending, &failed]),
            DocumentState::Failed
        );
    }

    #[test]
    fn reports_replica_state() {
        // A published entry is history: the record's own state answers again.
        let mut replica = replica();
        let published = entry(IntakeState::Published {
            document_id: replica.document_id,
        });
        replica.state = ReplicaState::Invalid;
        assert_eq!(
            document_state(&replica, &[&published]),
            DocumentState::Invalid
        );
        replica.state = ReplicaState::LocalOnly;
        assert_eq!(document_state(&replica, &[]), DocumentState::LocalOnly);
    }

    #[test]
    fn counts_local_edits() {
        let mut replica = replica();
        replica.selected = false;
        mark_edited(&mut replica, VectorClock::default());
        mark_edited(&mut replica, VectorClock::default());
        assert_eq!(replica.pending_edits, 2);
        assert!(replica.selected, "an edited document is selected again");
    }
}
