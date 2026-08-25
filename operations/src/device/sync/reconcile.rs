//! Decides one page of a synced folder, without touching the network or the
//! filesystem beyond the strong hashes the decision itself needs.
//!
//! The operation only ever writes device-local rows. Bytes reach the disk
//! through the guarded local-file effects the plan it answers with names, and
//! reach the realm through the upload rows it queues.

use std::collections::BTreeMap;

use aruna_core::effects::{Effect, LocalFileEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, LocalFileEvent, StorageEvent};
use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    EntryState, Observed, PendingMark, RemoteHead, SyncAction, SyncBase, SyncedFolder, WriteGuard,
    decide,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::repository::{
    MAX_HASH_BATCH, SyncUpload, UploadState, base_entry, base_key, upload_entry,
};

/// One remote version the plan asks the driver to fetch and write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Download {
    pub relative: String,
    pub remote_version: Ulid,
    pub guard: WriteGuard,
    /// The bytes land beside the file instead of replacing it, and the local
    /// version is published as its own realm version.
    pub conflicted: bool,
}

/// What one reconciled page decided.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcilePlan {
    pub downloads: Vec<Download>,
    pub uploads: usize,
    pub pending: usize,
}

impl ReconcilePlan {
    pub fn absorb(&mut self, other: ReconcilePlan) {
        self.downloads.extend(other.downloads);
        self.uploads += other.uploads;
        self.pending += other.pending;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileInput {
    pub folder: SyncedFolder,
    pub local: BTreeMap<String, Observed>,
    pub remote: BTreeMap<String, RemoteHead>,
    pub now_ms: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReconcileError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("reconciling the folder page did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum ReconcileState {
    Init,
    ReadBases,
    HashEntry { index: usize },
    StartTransaction,
    WriteRows { txn_id: TxnId },
    DeleteRows { txn_id: TxnId },
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ReconcileFolderOperation {
    input: ReconcileInput,
    state: ReconcileState,
    /// Every path this page decides, in key order.
    paths: Vec<String>,
    bases: Vec<Option<SyncBase>>,
    /// Indices of `paths` whose strong hash the decision still needs.
    hashing: Vec<usize>,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    plan: ReconcilePlan,
    output: Option<Result<ReconcilePlan, ReconcileError>>,
}

impl ReconcileFolderOperation {
    pub fn new(input: ReconcileInput) -> Self {
        Self {
            input,
            state: ReconcileState::Init,
            paths: Vec::new(),
            bases: Vec::new(),
            hashing: Vec::new(),
            writes: Vec::new(),
            deletes: Vec::new(),
            plan: ReconcilePlan::default(),
            output: None,
        }
    }

    /// Emits the next strong hash, or moves on to the decisions when the batch
    /// is done.
    fn next_hash(&mut self, index: usize) -> Effects {
        let Some(position) = self.hashing.get(index) else {
            return self.settle();
        };
        let relative = self.paths[*position].clone();
        self.state = ReconcileState::HashEntry { index };
        smallvec![Effect::LocalFile(LocalFileEffect::Hash {
            root: self.input.folder.root.clone(),
            relative,
        })]
    }

    /// Turns every decision of the page into rows and downloads.
    fn settle(&mut self) -> Effects {
        let policy = self.input.folder.policy();
        let paths = self.paths.clone();
        for (index, relative) in paths.iter().enumerate() {
            let base = self.bases.get(index).cloned().flatten();
            let remote = self.input.remote.get(relative).cloned();
            let mut local = self.input.local.get(relative).cloned();
            // A file whose weak fingerprint still equals the base carries the
            // base's strong hash; the adapter re-verifies both before it writes.
            if let (Some(local), Some(base)) = (local.as_mut(), base.as_ref())
                && local.fingerprint == base.fingerprint
            {
                local.blake3 = Some(base.blake3);
            }
            if base
                .as_ref()
                .is_some_and(|base| base.holds_pending(local.as_ref(), remote.as_ref()))
            {
                self.plan.pending += 1;
                continue;
            }
            let action = decide(policy, local.as_ref(), base.as_ref(), remote.as_ref());
            if let Err(error) = self.record(relative, action, local.as_ref(), base, remote.as_ref())
            {
                return fail(self, error);
            }
        }
        if self.writes.is_empty() && self.deletes.is_empty() {
            self.state = ReconcileState::Finish;
            self.output = Some(Ok(std::mem::take(&mut self.plan)));
            return smallvec![];
        }
        self.state = ReconcileState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// One decision, as the rows and downloads it implies.
    fn record(
        &mut self,
        relative: &str,
        action: SyncAction,
        local: Option<&Observed>,
        base: Option<SyncBase>,
        remote: Option<&RemoteHead>,
    ) -> Result<(), ReconcileError> {
        let folder_id = self.input.folder.folder_id;
        match action {
            SyncAction::Nothing => {
                if let Some(base) = base.filter(|base| base.entry != EntryState::InSync) {
                    self.push_base(relative, settled(base, self.input.now_ms))?;
                }
            }
            SyncAction::AdoptBase => {
                let (Some(local), Some(remote)) = (local, remote) else {
                    return Ok(());
                };
                self.push_base(relative, adopted(local, remote, self.input.now_ms))?;
            }
            SyncAction::Upload { deleted } => {
                self.push_upload(relative, deleted, local, base.as_ref())?;
                let entry = upload_entry_state(deleted, base.is_some());
                if let Some(base) = base {
                    self.push_base(relative, SyncBase { entry, ..base })?;
                }
                self.plan.uploads += 1;
            }
            SyncAction::Materialize {
                remote_version,
                guard,
            } => self.plan.downloads.push(Download {
                relative: relative.to_string(),
                remote_version,
                guard,
                conflicted: false,
            }),
            SyncAction::ConflictCopy {
                remote_version,
                upload,
            } => {
                self.plan.downloads.push(Download {
                    relative: relative.to_string(),
                    remote_version,
                    guard: WriteGuard::MustNotExist,
                    conflicted: true,
                });
                if upload {
                    self.push_upload(relative, false, local, base.as_ref())?;
                    self.plan.uploads += 1;
                }
            }
            SyncAction::Report(entry) => {
                let mark = PendingMark {
                    fingerprint: local.map(|local| local.fingerprint.clone()),
                    remote_version: remote.map(|remote| remote.version_id),
                };
                let base = base.unwrap_or_else(|| unsynced(local, self.input.now_ms));
                self.plan.pending += usize::from(entry.is_pending());
                self.push_base(
                    relative,
                    SyncBase {
                        entry,
                        pending_at: Some(mark),
                        ..base
                    },
                )?;
            }
            SyncAction::Forget => self.deletes.push((
                SYNC_BASE_KEYSPACE.to_string(),
                base_key(folder_id, relative),
            )),
        }
        Ok(())
    }

    fn push_base(&mut self, relative: &str, base: SyncBase) -> Result<(), ReconcileError> {
        self.writes
            .push(base_entry(self.input.folder.folder_id, relative, &base)?);
        Ok(())
    }

    fn push_upload(
        &mut self,
        relative: &str,
        deleted: bool,
        local: Option<&Observed>,
        base: Option<&SyncBase>,
    ) -> Result<(), ReconcileError> {
        let upload = SyncUpload {
            folder_id: self.input.folder.folder_id,
            relative: relative.to_string(),
            deleted,
            fingerprint: local
                .map(|local| local.fingerprint.clone())
                .unwrap_or_default(),
            blake3: local.and_then(|local| local.blake3),
            size: local.map(|local| local.size).unwrap_or_default(),
            local_version: local
                .and_then(|local| local.version_id)
                .or_else(|| base.and_then(|base| base.local_version_id)),
            queued_at_ms: self.input.now_ms,
            state: UploadState::Pending {
                due_at_ms: self.input.now_ms,
                attempts: 0,
                last_error: None,
            },
        };
        self.writes.push(upload_entry(&upload)?);
        Ok(())
    }
}

/// The base a settled entry keeps: the synced bytes are unchanged, only the
/// reported state and the pending mark clear.
fn settled(base: SyncBase, now_ms: u64) -> SyncBase {
    SyncBase {
        entry: EntryState::InSync,
        pending_at: None,
        synced_at_ms: now_ms,
        ..base
    }
}

/// The base of a file whose local bytes provably equal the remote version.
fn adopted(local: &Observed, remote: &RemoteHead, now_ms: u64) -> SyncBase {
    SyncBase {
        fingerprint: local.fingerprint.clone(),
        blake3: local.blake3.unwrap_or_default(),
        size: local.size,
        local_version_id: local.version_id,
        remote_version_id: Some(remote.version_id),
        synced_at_ms: now_ms,
        entry: EntryState::InSync,
        pending_at: None,
    }
}

/// A base row for an entry that never synced. Its recorded hash is the local
/// one when it is known, so no later guard can be built from nothing.
fn unsynced(local: Option<&Observed>, now_ms: u64) -> SyncBase {
    SyncBase {
        fingerprint: local
            .map(|local| local.fingerprint.clone())
            .unwrap_or_default(),
        blake3: local.and_then(|local| local.blake3).unwrap_or_default(),
        size: local.map(|local| local.size).unwrap_or_default(),
        local_version_id: local.and_then(|local| local.version_id),
        remote_version_id: None,
        synced_at_ms: now_ms,
        entry: EntryState::InSync,
        pending_at: None,
    }
}

fn upload_entry_state(deleted: bool, had_base: bool) -> EntryState {
    match (deleted, had_base) {
        (true, _) => EntryState::LocalDeleted,
        (false, true) => EntryState::LocalChanged,
        (false, false) => EntryState::LocalNew,
    }
}

impl Operation for ReconcileFolderOperation {
    type Output = ReconcilePlan;
    type Error = ReconcileError;

    fn start(&mut self) -> Effects {
        let mut paths: Vec<String> = self
            .input
            .local
            .keys()
            .chain(self.input.remote.keys())
            .cloned()
            .collect();
        paths.sort();
        paths.dedup();
        if paths.is_empty() {
            self.state = ReconcileState::Finish;
            self.output = Some(Ok(ReconcilePlan::default()));
            return smallvec![];
        }
        let folder_id = self.input.folder.folder_id;
        let reads = paths
            .iter()
            .map(|relative| {
                (
                    SYNC_BASE_KEYSPACE.to_string(),
                    base_key(folder_id, relative),
                )
            })
            .collect();
        self.paths = paths;
        self.state = ReconcileState::ReadBases;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                return fail(self, ReconcileError::Storage(error));
            }
            other => other,
        };

        match self.state.clone() {
            ReconcileState::ReadBases => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return unexpected(self, "batch read result", got);
                };
                self.bases = values
                    .into_iter()
                    .map(|(_, value)| {
                        value.and_then(|bytes| SyncBase::from_bytes(bytes.as_ref()).ok())
                    })
                    .collect();
                self.hashing = self.hash_batch();
                self.next_hash(0)
            }
            ReconcileState::HashEntry { index } => {
                let Some(position) = self.hashing.get(index).copied() else {
                    return self.settle();
                };
                if let Event::LocalFile(LocalFileEvent::Hashed {
                    fingerprint,
                    blake3,
                    size,
                }) = event
                    && let Some(local) = self.input.local.get_mut(&self.paths[position])
                    && local.fingerprint == fingerprint
                {
                    local.blake3 = Some(blake3);
                    local.size = size;
                }
                self.next_hash(index + 1)
            }
            ReconcileState::StartTransaction => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return unexpected(self, "transaction started", got);
                };
                if self.writes.is_empty() {
                    return self.emit_deletes(txn_id);
                }
                self.state = ReconcileState::WriteRows { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.writes),
                    txn_id: Some(txn_id),
                })]
            }
            ReconcileState::WriteRows { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return unexpected(self, "batch write result", got);
                };
                self.emit_deletes(txn_id)
            }
            ReconcileState::DeleteRows { txn_id } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return unexpected(self, "batch delete result", got);
                };
                self.state = ReconcileState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            ReconcileState::Commit => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return unexpected(self, "transaction committed", got);
                };
                self.state = ReconcileState::Finish;
                self.output = Some(Ok(std::mem::take(&mut self.plan)));
                smallvec![]
            }
            ReconcileState::Init | ReconcileState::Finish | ReconcileState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReconcileState::Finish | ReconcileState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ReconcileError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            ReconcileState::WriteRows { txn_id } | ReconcileState::DeleteRows { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

impl ReconcileFolderOperation {
    /// Paths whose weak fingerprint already says the file moved. Only those
    /// need a strong hash; the batch is bounded so one pass stays short.
    fn hash_batch(&self) -> Vec<usize> {
        self.paths
            .iter()
            .enumerate()
            .filter(|(index, relative)| {
                let Some(local) = self.input.local.get(*relative) else {
                    return false;
                };
                self.bases
                    .get(*index)
                    .and_then(Option::as_ref)
                    .is_none_or(|base| base.fingerprint != local.fingerprint)
            })
            .map(|(index, _)| index)
            .take(MAX_HASH_BATCH)
            .collect()
    }

    fn emit_deletes(&mut self, txn_id: TxnId) -> Effects {
        if self.deletes.is_empty() {
            self.state = ReconcileState::Commit;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        self.state = ReconcileState::DeleteRows { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: Some(txn_id),
        })]
    }
}

fn unexpected(
    operation: &mut ReconcileFolderOperation,
    expected: &'static str,
    got: String,
) -> Effects {
    let state = format!("{:?}", operation.state);
    fail(
        operation,
        ReconcileError::UnexpectedEvent {
            state,
            expected,
            got,
        },
    )
}

fn fail(operation: &mut ReconcileFolderOperation, error: ReconcileError) -> Effects {
    let cleanup = operation.abort();
    operation.state = ReconcileState::Error;
    operation.output = Some(Err(error));
    cleanup
}
