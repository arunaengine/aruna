//! What this device still owes the realm, and how to make it run now.
//!
//! Everything here is derived from state the device already keeps: the replica
//! ledger, the authoring intake and the synced-folder rows. Nothing is asked of
//! the realm, so the Sync view answers while the realm is out of reach.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DEVICE_SYNC_STATE_KEYSPACE, SYNC_BASE_KEYSPACE};
use aruna_core::structs::{EntryState, FolderState, SyncBase, SyncedFolder};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{GroupId, Key};
use aruna_core::util::unix_timestamp_millis;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;

use super::replica::{DocumentState, ReplicaRecord, ReplicaState, document_state, list_replicas};
use super::repository::{IntakeEntry, IntakeState, scan_intake};
use super::sync::folders::{list_folders, list_transfers};
use super::sync::repository::{scan_folder, scan_page};

/// How recently the realm must have answered for the device to call it
/// reachable. Two folder beats, so one missed pass does not flip the view.
pub const REALM_CONTACT_WINDOW: Duration = Duration::from_secs(90);

/// How long a run stays active without finishing. A crashed run must not block
/// the next one for longer than this.
pub const SYNC_RUN_LIFETIME: Duration = Duration::from_secs(120);

/// The one row this device keeps about its exchange with the realm.
const SYNC_STATE_KEY: [u8; 1] = [0];

/// When the realm last answered, when the last pass finished, and whether one
/// is in flight.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceSyncState {
    pub last_contact_ms: Option<u64>,
    pub last_sync_ms: Option<u64>,
    pub run_started_ms: Option<u64>,
}

impl DeviceSyncState {
    pub fn realm_reachable(&self, now_ms: u64) -> bool {
        self.last_contact_ms.is_some_and(|contact| {
            now_ms.saturating_sub(contact) <= REALM_CONTACT_WINDOW.as_millis() as u64
        })
    }

    pub fn run_active(&self, now_ms: u64) -> bool {
        self.run_started_ms.is_some_and(|started| {
            now_ms.saturating_sub(started) <= SYNC_RUN_LIFETIME.as_millis() as u64
        })
    }
}

/// One selected document in the Sync view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentRow {
    pub document_id: Ulid,
    pub document_path: String,
    pub group_id: GroupId,
    pub state: DocumentState,
    pub pending_edits: u32,
    pub local_only: bool,
    pub validation_findings: u32,
    pub last_error: Option<String>,
    pub last_synced_ms: Option<u64>,
}

/// One synced folder in the Sync view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRow {
    pub folder_id: Ulid,
    pub label: String,
    pub state: String,
    pub pending_uploads: usize,
    pub unsynced_files: usize,
    pub conflicts: usize,
    pub last_error: Option<String>,
}

/// Everything the Sync view shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncStatus {
    pub realm_reachable: bool,
    pub last_sync_ms: Option<u64>,
    pub pending_total: usize,
    pub documents: Vec<DocumentRow>,
    pub datasets: Vec<DatasetRow>,
}

/// Joins the replica ledger with the intake queue. Both are already read; this
/// is the whole derivation, so it is testable without a device.
pub fn document_rows(replicas: Vec<ReplicaRecord>, intake: &[IntakeEntry]) -> Vec<DocumentRow> {
    let mut queued: BTreeMap<Ulid, Vec<&IntakeEntry>> = BTreeMap::new();
    let mut unminted: Vec<&IntakeEntry> = Vec::new();
    for entry in intake {
        match entry.document_id() {
            Some(document_id) => queued.entry(document_id).or_default().push(entry),
            None => unminted.push(entry),
        }
    }
    let mut rows: Vec<DocumentRow> = replicas
        .into_iter()
        .map(|replica| {
            let entries = queued.remove(&replica.document_id).unwrap_or_default();
            DocumentRow {
                state: document_state(&replica, &entries),
                last_error: replica.last_error.clone().or_else(|| entry_error(&entries)),
                document_id: replica.document_id,
                document_path: replica.document_path,
                group_id: replica.group_id,
                pending_edits: replica.pending_edits,
                local_only: replica.state == ReplicaState::LocalOnly,
                validation_findings: replica.findings,
                last_synced_ms: replica.last_synced_ms,
            }
        })
        .collect();
    // A create the realm has not accepted yet has no replica of its own: the
    // queue is all there is, keyed by the draft id until an id is minted.
    let orphans = unminted.into_iter().chain(queued.into_values().flatten());
    rows.extend(orphans.map(queued_row));
    rows
}

/// One queued create that no replica covers yet.
fn queued_row(entry: &IntakeEntry) -> DocumentRow {
    DocumentRow {
        document_id: entry.document_id().unwrap_or(entry.draft_id),
        document_path: entry.document_path.clone(),
        group_id: entry.group_id,
        state: match &entry.state {
            IntakeState::Pending { .. } => DocumentState::Pending,
            IntakeState::Publishing { .. } => DocumentState::Publishing,
            IntakeState::Published { .. } => DocumentState::Synced,
            IntakeState::Failed { .. } => DocumentState::Failed,
        },
        pending_edits: 0,
        local_only: !matches!(entry.state, IntakeState::Published { .. }),
        validation_findings: 0,
        last_error: entry_error(&[entry]),
        last_synced_ms: None,
    }
}

/// What the last attempt on this document said, if anything did.
fn entry_error(entries: &[&IntakeEntry]) -> Option<String> {
    entries.iter().rev().find_map(|entry| match &entry.state {
        IntakeState::Failed { reason, .. } => Some(reason.clone()),
        IntakeState::Pending { last_error, .. } => last_error.clone(),
        _ => None,
    })
}

/// One folder's row from the rows the device already holds for it.
pub fn dataset_row(
    folder: &SyncedFolder,
    bases: &[SyncBase],
    pending_uploads: usize,
) -> DatasetRow {
    let mut unsynced_files = 0usize;
    let mut in_sync = 0u64;
    let mut conflicts = 0usize;
    for base in bases {
        if base.synced.is_none() {
            unsynced_files += 1;
        } else {
            in_sync += 1;
        }
        if matches!(
            base.entry,
            EntryState::Conflict { .. } | EntryState::PendingReplace { .. }
        ) {
            conflicts += 1;
        }
    }
    if bases.is_empty() {
        unsynced_files =
            usize::try_from(folder.observed_files.saturating_sub(in_sync)).unwrap_or(usize::MAX);
    }
    DatasetRow {
        folder_id: folder.folder_id,
        label: folder.root.clone(),
        state: folder_state(&folder.state),
        pending_uploads,
        unsynced_files,
        conflicts,
        last_error: folder.last_error.clone(),
    }
}

fn folder_state(state: &FolderState) -> String {
    match state {
        FolderState::Active => "active".to_string(),
        FolderState::Paused => "paused".to_string(),
        FolderState::Deleting => "deleting".to_string(),
        FolderState::Error { .. } => "error".to_string(),
    }
}

/// What still needs the realm: queued documents plus the files a folder has not
/// exchanged yet.
pub fn pending_total(documents: &[DocumentRow], datasets: &[DatasetRow]) -> usize {
    let queued = documents
        .iter()
        .filter(|document| {
            matches!(
                document.state,
                DocumentState::Pending | DocumentState::Publishing | DocumentState::LocalOnly
            )
        })
        .count();
    datasets.iter().fold(queued, |total, dataset| {
        total + dataset.pending_uploads + dataset.unsynced_files
    })
}

/// Reads everything the Sync view shows. A part the device cannot read is
/// reported as empty rather than failing the whole view.
pub async fn sync_status(context: &Arc<DriverContext>) -> SyncStatus {
    let now = unix_timestamp_millis();
    let state = read_sync_state(context).await;
    let replicas = list_replicas(context).await.unwrap_or_default();
    let intake = read_intake_entries(context).await;
    let documents = document_rows(replicas, &intake);
    let datasets = read_datasets(context).await;
    SyncStatus {
        realm_reachable: state.realm_reachable(now),
        last_sync_ms: state.last_sync_ms,
        pending_total: pending_total(&documents, &datasets),
        documents,
        datasets,
    }
}

/// Starts one sync pass: drain the intake, drain the upload outbox, refresh
/// every selected replica. Idempotent while a pass is still in flight.
pub async fn start_sync_run(context: &Arc<DriverContext>) -> bool {
    let now = unix_timestamp_millis();
    let mut state = read_sync_state(context).await;
    if state.run_active(now) {
        return false;
    }
    state.run_started_ms = Some(now);
    write_sync_state(context, &state).await;
    for key in [
        TaskKey::DrainDeviceIntake,
        TaskKey::DrainSyncUploadOutbox,
        // The folder beat is also the beat the replicas refresh on.
        TaskKey::ReconcileSyncedFolders,
    ] {
        arm(context, key).await;
    }
    true
}

async fn arm(context: &Arc<DriverContext>, key: TaskKey) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    if let TaskEvent::Error { message, .. } = task_handle
        .schedule_timer_if_idle(key, Duration::ZERO)
        .await
    {
        warn!(message = %message, "Failed to arm a device sync timer");
    }
}

/// Records that the realm answered this device.
pub async fn note_contact(context: &Arc<DriverContext>) {
    let mut state = read_sync_state(context).await;
    state.last_contact_ms = Some(unix_timestamp_millis());
    write_sync_state(context, &state).await;
}

/// Records that a sync pass finished, which also ends the run it belonged to.
pub async fn note_sync(context: &Arc<DriverContext>) {
    let now = unix_timestamp_millis();
    let mut state = read_sync_state(context).await;
    state.last_contact_ms = Some(now);
    state.last_sync_ms = Some(now);
    state.run_started_ms = None;
    write_sync_state(context, &state).await;
}

pub async fn read_sync_state(context: &Arc<DriverContext>) -> DeviceSyncState {
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DEVICE_SYNC_STATE_KEYSPACE.to_string(),
            key: sync_state_key(),
            txn_id: None,
        })
        .await
    else {
        return DeviceSyncState::default();
    };
    postcard::from_bytes(&bytes).unwrap_or_default()
}

async fn write_sync_state(context: &Arc<DriverContext>, state: &DeviceSyncState) {
    let Ok(bytes) = postcard::to_allocvec(state) else {
        return;
    };
    if let Event::Storage(StorageEvent::Error { error }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: DEVICE_SYNC_STATE_KEYSPACE.to_string(),
            key: sync_state_key(),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        warn!(error = %error, "Failed to store the device sync state");
    }
}

fn sync_state_key() -> Key {
    ByteView::from(SYNC_STATE_KEY.to_vec())
}

pub(crate) async fn read_intake_entries(context: &Arc<DriverContext>) -> Vec<IntakeEntry> {
    let mut entries = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let Some((values, next)) = scan_page(context, scan_intake(cursor, None)).await else {
            return entries;
        };
        entries.extend(
            values
                .into_iter()
                .filter_map(|(_, bytes)| IntakeEntry::from_bytes(&bytes).ok()),
        );
        match next {
            Some(next) => cursor = Some(next),
            None => return entries,
        }
    }
}

async fn read_datasets(context: &Arc<DriverContext>) -> Vec<DatasetRow> {
    let Ok(folders) = list_folders(context).await else {
        return Vec::new();
    };
    let mut uploads: BTreeMap<Ulid, usize> = BTreeMap::new();
    for upload in list_transfers(context).await.unwrap_or_default() {
        *uploads.entry(upload.folder_id).or_default() += 1;
    }
    let mut datasets = Vec::with_capacity(folders.len());
    for folder in folders {
        let bases = read_bases(context, folder.folder_id).await;
        let pending = uploads.get(&folder.folder_id).copied().unwrap_or_default();
        datasets.push(dataset_row(&folder, &bases, pending));
    }
    datasets
}

async fn read_bases(context: &Arc<DriverContext>, folder_id: Ulid) -> Vec<SyncBase> {
    let mut bases = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let Some((values, next)) = scan_page(
            context,
            scan_folder(SYNC_BASE_KEYSPACE, folder_id, cursor, None),
        )
        .await
        else {
            return bases;
        };
        bases.extend(
            values
                .into_iter()
                .filter_map(|(_, bytes)| SyncBase::from_bytes(&bytes).ok()),
        );
        match next {
            Some(next) => cursor = Some(next),
            None => return bases,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::replica::ReplicaOrigin;
    use aruna_core::structs::{FolderMode, RealmId, RemoteBinding};
    use aruna_core::types::UserId;

    fn replica(state: ReplicaState) -> ReplicaRecord {
        let mut replica = ReplicaRecord::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            ReplicaOrigin::Realm,
        );
        replica.state = state;
        replica
    }

    fn draft(state: IntakeState) -> IntakeEntry {
        let mut entry = IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([5u8; 32])),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            false,
            "{}".to_string(),
        );
        entry.state = state;
        entry
    }

    fn folder(state: FolderState) -> SyncedFolder {
        SyncedFolder {
            folder_id: Ulid::from_bytes([3u8; 16]),
            root: "/home/ada/lab".to_string(),
            local_bucket: "folder-3".to_string(),
            group_id: Ulid::from_bytes([1u8; 16]),
            remote: RemoteBinding {
                node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                bucket: "lab".to_string(),
                prefix: "ada/".to_string(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: false,
            state,
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([5u8; 32])),
            created_at_ms: 1,
            last_reconcile_ms: None,
            last_error: None,
            last_error_at_ms: None,
            observed_files: 0,
            list_cursor: None,
        }
    }

    fn base(synced: bool, entry: EntryState) -> SyncBase {
        SyncBase {
            synced: synced.then(|| aruna_core::structs::SyncedBytes {
                fingerprint: "f".to_string(),
                blake3: [6u8; 32],
                size: 1,
                remote_version_id: Ulid::from_bytes([6u8; 16]),
            }),
            local_version_id: None,
            synced_at_ms: 0,
            entry,
            pending_at: None,
            local: None,
            remote: None,
        }
    }

    #[test]
    fn joins_queued_documents() {
        // A queued create is what the owner sees, not the ledger's own state.
        let replica = replica(ReplicaState::LocalOnly);
        let entry = draft(IntakeState::Publishing {
            document_id: replica.document_id,
            due_at_ms: 0,
            attempts: 1,
        });
        let rows = document_rows(vec![replica], std::slice::from_ref(&entry));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, DocumentState::Publishing);
        assert!(rows[0].local_only);
    }

    #[test]
    fn lists_unpublished_creates() {
        // A draft the realm has never seen has no replica, and must still be
        // what the owner sees as waiting.
        let entry = draft(IntakeState::Pending {
            due_at_ms: 0,
            attempts: 1,
            last_error: Some("unreachable".to_string()),
        });
        let rows = document_rows(Vec::new(), std::slice::from_ref(&entry));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].document_id, entry.draft_id);
        assert_eq!(rows[0].state, DocumentState::Pending);
        assert!(rows[0].local_only);
        assert_eq!(rows[0].last_error.as_deref(), Some("unreachable"));
    }

    #[test]
    fn reports_folder_backlog() {
        let mut folder = folder(FolderState::Active);
        folder.observed_files = 4;
        folder.last_error = Some("remote unavailable".to_string());
        let bases = vec![
            base(true, EntryState::InSync),
            base(false, EntryState::LocalNew),
            base(
                true,
                EntryState::Conflict {
                    remote_version: Ulid::from_bytes([7u8; 16]),
                    conflicted_copy: "notes.txt (conflict)".to_string(),
                },
            ),
        ];
        let row = dataset_row(&folder, &bases, 2);
        assert_eq!(row.unsynced_files, 1);
        assert_eq!(row.conflicts, 1);
        assert_eq!(row.pending_uploads, 2);
        assert_eq!(row.state, "active");
        assert_eq!(pending_total(&[], &[row]), 3);

        let empty = dataset_row(&folder, &[], 0);
        assert_eq!(empty.unsynced_files, 4);
        assert_eq!(empty.last_error.as_deref(), Some("remote unavailable"));
    }

    #[test]
    fn windows_realm_contact() {
        // A device that has not heard from its realm in two beats is offline,
        // and a run that never finished stops blocking the next one.
        let now = 1_000_000;
        let state = DeviceSyncState {
            last_contact_ms: Some(now - 1_000),
            last_sync_ms: Some(now - 1_000),
            run_started_ms: Some(now - 1_000),
        };
        assert!(state.realm_reachable(now));
        assert!(state.run_active(now));

        let stale = DeviceSyncState {
            last_contact_ms: Some(now - REALM_CONTACT_WINDOW.as_millis() as u64 - 1),
            run_started_ms: Some(now - SYNC_RUN_LIFETIME.as_millis() as u64 - 1),
            ..state
        };
        assert!(!stale.realm_reachable(now));
        assert!(!stale.run_active(now));
        assert!(!DeviceSyncState::default().realm_reachable(now));
    }
}
