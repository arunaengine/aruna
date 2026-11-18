//! Builds the Sync view of what this device still owes the realm, from local state alone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::rows::{outside_answer, outside_call};
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::{DEVICE_INTAKE_KEYSPACE, SYNC_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::{EntryState, FolderState, SyncBase, SyncedFolder};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Value};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::publish_queue::{
    ClaimError, ClaimStateOperation, PUBLISH_PAGE_SIZE, PublishEntry, PublishState,
};
use super::replica::{
    DocumentState, ReplicaRecord, ReplicaScanError, ReplicaState, document_state,
};
use super::replica_store::list_replicas;
use super::scan_rows::{ScanRowsError, ScanRowsOperation};
use super::sync::folders::FolderError;
use super::sync::folders::reads::{folder_bases, list_folders, list_transfers};
use super::sync::outbox::RequeueUploadsOperation;
use super::sync::repository::SyncUpload;
use super::sync_note::{DeviceSyncState, sync_state_key};

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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncStatus {
    pub realm_reachable: bool,
    pub last_sync_ms: Option<u64>,
    pub pending_total: usize,
    pub documents: Vec<DocumentRow>,
    pub datasets: Vec<DatasetRow>,
}

/// Joins the replica ledger with the publish queue. Both are already read; this
/// is the whole derivation, so it is testable without a device.
pub fn document_rows(replicas: Vec<ReplicaRecord>, entries: &[PublishEntry]) -> Vec<DocumentRow> {
    let mut queued: BTreeMap<Ulid, Vec<&PublishEntry>> = BTreeMap::new();
    let mut unminted: Vec<&PublishEntry> = Vec::new();
    for entry in entries {
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
fn queued_row(entry: &PublishEntry) -> DocumentRow {
    DocumentRow {
        document_id: entry.document_id().unwrap_or(entry.draft_id),
        document_path: entry.document_path.clone(),
        group_id: entry.group_id,
        state: match &entry.state {
            PublishState::Pending { .. } => DocumentState::Pending,
            PublishState::Publishing { .. } => DocumentState::Publishing,
            PublishState::Published { .. } => DocumentState::Synced,
            PublishState::Failed { .. } => DocumentState::Failed,
        },
        pending_edits: 0,
        local_only: !matches!(entry.state, PublishState::Published { .. }),
        validation_findings: 0,
        last_error: entry_error(&[entry]),
        last_synced_ms: None,
    }
}

/// What the last attempt on this document said, if anything did.
fn entry_error(entries: &[&PublishEntry]) -> Option<String> {
    entries.iter().rev().find_map(|entry| match &entry.state {
        PublishState::Failed { reason, .. } => Some(reason.clone()),
        PublishState::Pending { last_error, .. } => last_error.clone(),
        _ => None,
    })
}

/// One folder's row from the rows the device already holds for it.
fn dataset_row(folder: &SyncedFolder, bases: &[SyncBase], pending_uploads: usize) -> DatasetRow {
    let mut unsynced_files = 0usize;
    let mut conflicts = 0usize;
    for base in bases {
        if base.synced.is_none() {
            unsynced_files += 1;
        }
        if matches!(
            base.entry,
            EntryState::Conflict { .. } | EntryState::PendingReplace { .. }
        ) {
            conflicts += 1;
        }
    }
    if bases.is_empty() {
        unsynced_files = usize::try_from(folder.observed_files).unwrap_or(usize::MAX);
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
        FolderState::Binding { .. } => "binding".to_string(),
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

/// The sync state row; an absent or unreadable one reads as nothing known yet.
fn state_read() -> IoCall {
    let read = read_row(SYNC_STATE_KEYSPACE, sync_state_key().to_vec());
    outside_call(read)
}

fn state_of(result: IoResult) -> DeviceSyncState {
    let stored = outside_answer(result).and_then(|read| read.value());
    (stored.ok().flatten())
        .and_then(|bytes| postcard::from_bytes(&bytes).ok())
        .unwrap_or_default()
}

/// The device's sync state.
#[derive(Debug, Default, PartialEq)]
pub struct SyncStateOperation;

/// The read of the sync state; see [`SyncStateOperation`].
pub fn read_sync_state() -> SyncStateOperation {
    SyncStateOperation
}

impl Operation for SyncStateOperation {
    type Output = DeviceSyncState;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<DeviceSyncState>, IoError> {
        Ok(Next::Call(state_read()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DeviceSyncState>, IoError> {
        Ok(Next::Done(state_of(result)))
    }
}

/// Every readable entry of the publish queue; a failed scan reads as none.
#[derive(Debug, Default, PartialEq)]
pub struct PublishEntriesOperation;

/// The read of the publish queue; see [`PublishEntriesOperation`].
pub(crate) fn read_publish_entries() -> PublishEntriesOperation {
    PublishEntriesOperation
}

impl Operation for PublishEntriesOperation {
    type Output = Vec<PublishEntry>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<PublishEntry>>, IoError> {
        let scan = ScanRowsOperation::new(DEVICE_INTAKE_KEYSPACE, None, PUBLISH_PAGE_SIZE);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<PublishEntry>>, IoError> {
        let rows = result
            .child::<Vec<Value>, ScanRowsError>()?
            .unwrap_or_default();
        Ok(Next::Done(
            (rows.iter())
                .filter_map(|bytes| PublishEntry::from_bytes(bytes).ok())
                .collect(),
        ))
    }
}

/// Moves a timer to now. An idle arm would keep the beat a device always has pending.
#[derive(Debug, PartialEq)]
pub(crate) struct ArmTimerOperation {
    key: TaskKey,
}

impl ArmTimerOperation {
    pub(crate) fn new(key: TaskKey) -> Self {
        Self { key }
    }
}

impl Operation for ArmTimerOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let effect = TaskEffect::ShortenTimer {
            key: self.key.clone(),
            after: Duration::ZERO,
        };
        Ok(Next::Call(IoCall::from(Effect::Task(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if let Event::Task(TaskEvent::Error { message, .. }) = result.event()? {
            warn!(%message, "Failed to arm a device sync timer");
        }
        Ok(Next::Done(()))
    }
}

/// What the pending call of the Sync view answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ViewStep {
    State,
    Replicas,
    Entries,
    Folders,
    Transfers,
    Bases,
}

/// Reads everything the Sync view shows. A part the device cannot read is reported as empty
/// rather than failing the whole view.
#[derive(Debug, PartialEq)]
pub struct SyncStatusOperation {
    now_ms: u64,
    step: ViewStep,
    state: DeviceSyncState,
    replicas: Vec<ReplicaRecord>,
    entries: Vec<PublishEntry>,
    folders: VecDeque<SyncedFolder>,
    current: Option<SyncedFolder>,
    uploads: BTreeMap<Ulid, usize>,
    datasets: Vec<DatasetRow>,
}

/// The read of the Sync view; see [`SyncStatusOperation`].
pub fn sync_status() -> SyncStatusOperation {
    SyncStatusOperation {
        now_ms: unix_timestamp_millis(),
        step: ViewStep::State,
        state: DeviceSyncState::default(),
        replicas: Vec::new(),
        entries: Vec::new(),
        folders: VecDeque::new(),
        current: None,
        uploads: BTreeMap::new(),
        datasets: Vec::new(),
    }
}

impl SyncStatusOperation {
    fn next_folder(&mut self) -> Result<Next<SyncStatus>, IoError> {
        let Some(folder) = self.folders.pop_front() else {
            return Ok(Next::Done(self.finished()));
        };
        let bases = folder_bases(folder.folder_id);
        self.current = Some(folder);
        self.step = ViewStep::Bases;
        Ok(Next::Call(IoCall::child(bases)))
    }

    fn finished(&mut self) -> SyncStatus {
        let replicas = std::mem::take(&mut self.replicas);
        let documents = document_rows(replicas, &self.entries);
        let datasets = std::mem::take(&mut self.datasets);
        SyncStatus {
            realm_reachable: self.state.realm_reachable(self.now_ms),
            last_sync_ms: self.state.last_sync_ms,
            pending_total: pending_total(&documents, &datasets),
            documents,
            datasets,
        }
    }
}

impl Operation for SyncStatusOperation {
    type Output = SyncStatus;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<SyncStatus>, IoError> {
        Ok(Next::Call(state_read()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncStatus>, IoError> {
        match self.step {
            ViewStep::State => {
                self.state = state_of(result);
                self.step = ViewStep::Replicas;
                Ok(Next::Call(IoCall::child(list_replicas())))
            }
            ViewStep::Replicas => {
                let replicas = result.child::<Vec<ReplicaRecord>, ReplicaScanError>()?;
                self.replicas = replicas.unwrap_or_default();
                self.step = ViewStep::Entries;
                Ok(Next::Call(IoCall::child(read_publish_entries())))
            }
            ViewStep::Entries => {
                self.entries = result.child::<Vec<PublishEntry>, IoError>()??;
                self.step = ViewStep::Folders;
                Ok(Next::Call(IoCall::child(list_folders())))
            }
            ViewStep::Folders => match result.child::<Vec<SyncedFolder>, FolderError>()? {
                Ok(folders) => {
                    self.folders = folders.into();
                    self.step = ViewStep::Transfers;
                    Ok(Next::Call(IoCall::child(list_transfers())))
                }
                Err(_) => Ok(Next::Done(self.finished())),
            },
            ViewStep::Transfers => {
                let uploads = result.child::<Vec<SyncUpload>, FolderError>()?;
                for upload in uploads.unwrap_or_default() {
                    *self.uploads.entry(upload.folder_id).or_default() += 1;
                }
                self.next_folder()
            }
            ViewStep::Bases => {
                let bases = result
                    .child::<Vec<SyncBase>, FolderError>()?
                    .unwrap_or_default();
                let folder = self.current.take().ok_or(IoError::Unfinished)?;
                let pending = (self.uploads.get(&folder.folder_id).copied()).unwrap_or_default();
                self.datasets.push(dataset_row(&folder, &bases, pending));
                self.next_folder()
            }
        }
    }
}

/// What the pending call of starting a sync run answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunStep {
    State,
    Entries,
    Claim,
    Uploads,
    Store,
    Arm,
}

/// Starts one sync pass: drain the publish queue, drain the upload outbox, refresh every
/// selected replica. Idempotent while a pass is still in flight.
#[derive(Debug, PartialEq)]
pub struct SyncRunOperation {
    now_ms: u64,
    step: RunStep,
    state: DeviceSyncState,
    failed: VecDeque<(PublishEntry, PublishState)>,
    timers: VecDeque<TaskKey>,
}

/// The start of one sync pass; see [`SyncRunOperation`].
pub fn start_sync_run() -> SyncRunOperation {
    SyncRunOperation {
        now_ms: unix_timestamp_millis(),
        step: RunStep::State,
        state: DeviceSyncState::default(),
        failed: VecDeque::new(),
        // The folder beat is also the beat the replicas refresh on.
        timers: VecDeque::from([
            TaskKey::DrainDeviceIntake,
            TaskKey::DrainUploadOutbox,
            TaskKey::ReconcileSyncedFolders,
        ]),
    }
}

impl SyncRunOperation {
    fn next_claim(&mut self) -> Result<Next<bool>, IoError> {
        if let Some((entry, state)) = self.failed.pop_front() {
            self.step = RunStep::Claim;
            let claim = ClaimStateOperation::new(entry, state);
            return Ok(Next::Call(IoCall::child(claim)));
        }
        self.step = RunStep::Uploads;
        let requeue = RequeueUploadsOperation::new(self.now_ms);
        Ok(Next::Call(IoCall::child(requeue)))
    }

    fn store_state(&mut self) -> Result<Next<bool>, IoError> {
        self.state.run_started_ms = Some(self.now_ms);
        self.step = RunStep::Store;
        let Ok(bytes) = postcard::to_allocvec(&self.state) else {
            return self.next_timer();
        };
        let row = (
            SYNC_STATE_KEYSPACE.to_string(),
            sync_state_key(),
            ByteView::from(bytes),
        );
        Ok(Next::Call(outside_call(IoCall::Write(vec![row]))))
    }

    fn next_timer(&mut self) -> Result<Next<bool>, IoError> {
        let Some(key) = self.timers.pop_front() else {
            return Ok(Next::Done(true));
        };
        self.step = RunStep::Arm;
        Ok(Next::Call(IoCall::child(ArmTimerOperation::new(key))))
    }
}

impl Operation for SyncRunOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(state_read()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match self.step {
            RunStep::State => {
                self.state = state_of(result);
                if self.state.run_active(self.now_ms) {
                    return Ok(Next::Done(false));
                }
                self.step = RunStep::Entries;
                Ok(Next::Call(IoCall::child(read_publish_entries())))
            }
            RunStep::Entries => {
                let entries = result.child::<Vec<PublishEntry>, IoError>()??;
                self.failed = (entries.into_iter())
                    .filter_map(|entry| {
                        let state = entry.retry_failed(self.now_ms)?;
                        Some((entry, state))
                    })
                    .collect();
                self.next_claim()
            }
            RunStep::Claim => {
                if let Err(error) = result.child::<bool, ClaimError>()? {
                    warn!(%error, "Failed to requeue a failed publish entry");
                }
                self.next_claim()
            }
            RunStep::Uploads => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Failed to requeue the folder uploads");
                }
                self.store_state()
            }
            RunStep::Store => {
                if let Err(error) = outside_answer(result) {
                    warn!(%error, "Failed to store the device sync state");
                }
                self.next_timer()
            }
            RunStep::Arm => {
                result.child::<(), IoError>()?.ok();
                self.next_timer()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::publish_queue::publish_entry;
    use crate::device::replica::ReplicaOrigin;
    use crate::device::sync_note::{NoteSyncOperation, SyncNote};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::handle::Handle;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::{FolderMode, RemoteBinding};
    use std::sync::Arc;

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

    fn draft(state: PublishState) -> PublishEntry {
        let mut entry = PublishEntry::new(
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
            last_error_ms: None,
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
        let entry = draft(PublishState::Publishing {
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
        let entry = draft(PublishState::Pending {
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

    #[tokio::test]
    async fn runs_pending_beat() {
        // A device always has its folder beat pending, so an explicit run must move it to now.
        use crate::tests::incoming::RecordingTaskHandler;
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let (seen, mut fired) = tokio::sync::mpsc::channel(16);
        let task = aruna_tasks::TaskHandle::new();
        task.set_inbound_handler(Arc::new(RecordingTaskHandler { seen }))
            .await;
        let key = TaskKey::ReconcileSyncedFolders;
        let after = Duration::from_secs(3600);
        let later = Effect::Task(TaskEffect::ResetTimer { key, after });
        assert!(matches!(
            task.send_effect(later).await,
            Event::Task(TaskEvent::TimerScheduled { .. })
        ));
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(task),
            compute_handle: None,
            outbound_handle: None,
        });

        assert!(drive(start_sync_run(), &context).await.unwrap());
        // Detects a wakeup that never arrives, not a slow machine.
        let woke = tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(key) = fired.recv().await {
                if key == TaskKey::ReconcileSyncedFolders {
                    return true;
                }
            }
            false
        })
        .await;
        assert!(
            matches!(woke, Ok(true)),
            "the folder beat kept its later deadline"
        );
    }

    #[tokio::test]
    async fn drops_failed_contact() {
        // The first failed attempt shows the realm as unreachable; the next answer restores it.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let now = unix_timestamp_millis();
        let note = |note| crate::driver::drive(NoteSyncOperation::new(note, now), &context);
        note(SyncNote::Contact).await.unwrap();
        assert!(
            drive(read_sync_state(), &context)
                .await
                .unwrap()
                .realm_reachable(now)
        );
        note(SyncNote::Failure).await.unwrap();
        assert!(
            !drive(read_sync_state(), &context)
                .await
                .unwrap()
                .realm_reachable(now)
        );
        note(SyncNote::Contact).await.unwrap();
        assert!(
            drive(read_sync_state(), &context)
                .await
                .unwrap()
                .realm_reachable(now)
        );
    }

    #[tokio::test]
    async fn retries_explicit_sync() {
        // An owner-requested run re-arms retryable failures only.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let document_id = Ulid::generate();
        let retryable = draft(PublishState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
            document_id: Some(document_id),
        });
        let permanent = draft(PublishState::Failed {
            reason: "denied".to_string(),
            retryable: false,
            document_id: None,
        });
        for entry in [&retryable, &permanent] {
            let (key_space, key, value) = publish_entry(entry).unwrap();
            assert!(matches!(
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
            ));
        }

        assert!(drive(start_sync_run(), &context).await.unwrap());
        let entries = drive(read_publish_entries(), &context).await.unwrap();
        assert!(entries.iter().any(|entry| matches!(
            entry.state,
            PublishState::Publishing {
                document_id: retried,
                attempts: 0,
                ..
            } if retried == document_id
        )));
        assert!(entries.iter().any(|entry| entry.state == permanent.state));
    }
}
