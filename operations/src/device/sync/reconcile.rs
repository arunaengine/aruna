//! Decides one page of a synced folder, without touching the network or the
//! filesystem beyond the strong hashes the decision itself needs.
//!
//! The operation only ever writes device-local rows. Bytes reach the disk
//! through the guarded local-file effects the plan it answers with names, and
//! reach the realm through the upload rows it queues.

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::effects::{Effect, LocalFileEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, LocalFileEvent, StorageEvent};
use aruna_core::keyspaces::{SYNC_BASE_KEYSPACE, SYNC_UPLOAD_OUTBOX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    EntrySide, EntryState, Observed, PendingMark, RemoteHead, SyncAction, SyncBase, SyncedBytes,
    SyncedFolder, WriteGuard, decide, fingerprint_complete,
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
    /// The local side as this pass observed it, strong hash included, so the
    /// written row keeps the bytes an explicit action has to echo back.
    pub local: Option<EntrySide>,
    /// The synced bytes the entry already had, carried so a refused write
    /// cannot lose them.
    pub synced: Option<SyncedBytes>,
}

/// What one reconciled page decided.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcilePlan {
    pub downloads: Vec<Download>,
    /// Rows this pass newly queued for upload. A row that was already queued is
    /// not counted, so a waiting backoff never keeps the timer hot.
    pub uploads: usize,
    pub pending: usize,
    /// The realm listing was cut short; the next pass resumes where it stopped.
    pub truncated: bool,
}

impl ReconcilePlan {
    pub fn absorb(&mut self, other: ReconcilePlan) {
        self.downloads.extend(other.downloads);
        self.uploads += other.uploads;
        self.pending += other.pending;
        self.truncated |= other.truncated;
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
    /// Rows already waiting in the outbox, so a pass never re-queues an entry
    /// whose backoff has not elapsed.
    queued: Vec<Option<SyncUpload>>,
    /// Indices of `paths` whose strong hash the decision still needs.
    hashing: Vec<usize>,
    /// Indices this pass could not hash within its batch. They are left for the
    /// next pass rather than decided on bytes nobody read.
    deferred: BTreeSet<usize>,
    /// Whether this folder's filesystem timestamps below the second. Without
    /// that, a rewrite can land inside one timestamp and no stat can tell.
    fine_timestamps: bool,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    plan: ReconcilePlan,
    output: Option<Result<ReconcilePlan, ReconcileError>>,
}

impl ReconcileFolderOperation {
    pub fn new(input: ReconcileInput) -> Self {
        Self {
            fine_timestamps: input.local.values().any(fine_stat),
            input,
            state: ReconcileState::Init,
            paths: Vec::new(),
            bases: Vec::new(),
            queued: Vec::new(),
            hashing: Vec::new(),
            deferred: BTreeSet::new(),
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
            // A settled observation carries the base's strong hash; anything
            // else was read instead, so its own hash is already in place.
            if let Some(local) = local.as_mut()
                && self.reuses_hash(index, local)
                && let Some(synced) = base.as_ref().and_then(|base| base.synced.as_ref())
            {
                local.blake3 = Some(synced.blake3);
            }
            if base
                .as_ref()
                .is_some_and(|base| base.holds_pending(local.as_ref(), remote.as_ref()))
            {
                self.plan.pending += 1;
                continue;
            }
            if self.awaits_upload(index, local.as_ref(), base.as_ref())
                || self.deferred.contains(&index)
            {
                continue;
            }
            let action = decide(policy, local.as_ref(), base.as_ref(), remote.as_ref());
            if let Err(error) = self.record(
                index,
                relative,
                action,
                local.as_ref(),
                base,
                remote.as_ref(),
            ) {
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
        index: usize,
        relative: &str,
        action: SyncAction,
        local: Option<&Observed>,
        base: Option<SyncBase>,
        remote: Option<&RemoteHead>,
    ) -> Result<(), ReconcileError> {
        let folder_id = self.input.folder.folder_id;
        match action {
            SyncAction::Nothing => {
                // Neither side holds the entry, so there is nothing to record:
                // a delete marker must not churn a row into existence.
                if local.is_none() && base.is_none() {
                    return Ok(());
                }
                let current = base.unwrap_or_else(|| unsynced(local, self.input.now_ms));
                let next = SyncBase {
                    local: local.map(EntrySide::from_local),
                    remote: remote.map(EntrySide::from_remote),
                    ..settled(current.clone())
                };
                // An entry that already reads this way must not be rewritten on
                // every pass: a large folder would rewrite its whole base table.
                if next != current {
                    self.push_base(relative, next, local, remote)?;
                }
            }
            SyncAction::AdoptBase => {
                let (Some(local), Some(remote)) = (local, remote) else {
                    return Ok(());
                };
                self.push_base(
                    relative,
                    adopted(local, remote, self.input.now_ms),
                    Some(local),
                    Some(remote),
                )?;
            }
            SyncAction::Upload { deleted } => {
                if self.push_upload(index, relative, deleted, local, base.as_ref())? {
                    self.plan.uploads += 1;
                }
                let entry = upload_entry_state(deleted, base.is_some());
                let base = base.unwrap_or_else(|| unsynced(local, self.input.now_ms));
                self.push_base(relative, SyncBase { entry, ..base }, local, remote)?;
            }
            SyncAction::Materialize {
                remote_version,
                guard,
            } => {
                let entry = match base.is_some() {
                    true => EntryState::RemoteChanged,
                    false => EntryState::RemoteNew,
                };
                self.push_download(
                    relative,
                    Planned {
                        remote_version,
                        guard,
                        conflicted: false,
                        entry,
                    },
                    base,
                    local,
                    remote,
                )?;
            }
            SyncAction::ConflictCopy {
                remote_version,
                upload,
            } => {
                self.push_download(
                    relative,
                    Planned {
                        remote_version,
                        guard: WriteGuard::MustNotExist,
                        conflicted: true,
                        entry: EntryState::RemoteChanged,
                    },
                    base.clone(),
                    local,
                    remote,
                )?;
                if upload && self.push_upload(index, relative, false, local, base.as_ref())? {
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
                    local,
                    remote,
                )?;
            }
            SyncAction::Forget => self.deletes.push((
                SYNC_BASE_KEYSPACE.to_string(),
                base_key(folder_id, relative),
            )),
        }
        Ok(())
    }

    fn push_base(
        &mut self,
        relative: &str,
        base: SyncBase,
        local: Option<&Observed>,
        remote: Option<&RemoteHead>,
    ) -> Result<(), ReconcileError> {
        let base = SyncBase {
            local: local.map(EntrySide::from_local),
            remote: remote.map(EntrySide::from_remote),
            ..base
        };
        self.writes
            .push(base_entry(self.input.folder.folder_id, relative, &base)?);
        Ok(())
    }

    /// Records one planned download and the row that reports it as in flight.
    /// The row never adopts the remote version: a download that never lands
    /// must be retried, not mistaken for a synced state.
    fn push_download(
        &mut self,
        relative: &str,
        planned: Planned,
        base: Option<SyncBase>,
        local: Option<&Observed>,
        remote: Option<&RemoteHead>,
    ) -> Result<(), ReconcileError> {
        let base = base.unwrap_or_else(|| unsynced(local, self.input.now_ms));
        self.plan.downloads.push(Download {
            relative: relative.to_string(),
            remote_version: planned.remote_version,
            guard: planned.guard,
            conflicted: planned.conflicted,
            local: local.map(EntrySide::from_local),
            synced: base.synced.clone(),
        });
        self.push_base(
            relative,
            SyncBase {
                entry: planned.entry,
                ..base
            },
            local,
            remote,
        )
    }

    /// Queues one upload unless the same local version is already owed.
    /// Answers whether this pass added a row.
    fn push_upload(
        &mut self,
        index: usize,
        relative: &str,
        deleted: bool,
        local: Option<&Observed>,
        base: Option<&SyncBase>,
    ) -> Result<bool, ReconcileError> {
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
        // Rewriting the row would reset its backoff and its attempt count, so
        // an entry that is already owed is left exactly as the drain left it.
        if self
            .queued
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|queued| {
                queued.deleted == upload.deleted && queued.local_version == upload.local_version
            })
        {
            return Ok(false);
        }
        self.writes.push(upload_entry(&upload)?);
        Ok(true)
    }
}

/// How long a file must have been still before its stat may stand in for its
/// bytes. Below it a rewrite can share the timestamps of the observation that
/// preceded it, whatever resolution the filesystem keeps.
const SETTLE_WINDOW_MS: u64 = 2_000;

/// Whether one observation shows sub-second timestamps. A whole-second stat may
/// be a coarse filesystem or a file that happened to land on the second, so the
/// answer is taken over the whole page and never from one file alone.
fn fine_stat(local: &Observed) -> bool {
    local.stat.is_some_and(|stat| {
        [stat.modified_ns, stat.changed_ns]
            .into_iter()
            .flatten()
            .any(|nanos| nanos % 1_000_000_000 != 0)
    })
}

/// Whether the file has been still long enough that a rewrite could not have
/// happened inside the observation's own timestamp.
fn stat_settled(local: &Observed, now_ms: u64) -> bool {
    let Some(changed_ns) = local.stat.and_then(|stat| stat.changed_ns) else {
        return false;
    };
    let changed_ms = u64::try_from(changed_ns / 1_000_000).unwrap_or(u64::MAX);
    now_ms.saturating_sub(changed_ms) >= SETTLE_WINDOW_MS
}

/// The base a settled entry keeps: the synced bytes and their timestamp are
/// unchanged, only the reported state and the pending mark clear.
fn settled(base: SyncBase) -> SyncBase {
    SyncBase {
        entry: EntryState::InSync,
        pending_at: None,
        ..base
    }
}

/// The base of a file whose local bytes provably equal the remote version. An
/// unknown local hash records no synced bytes rather than a fabricated one.
fn adopted(local: &Observed, remote: &RemoteHead, now_ms: u64) -> SyncBase {
    SyncBase {
        synced: local.blake3.map(|blake3| SyncedBytes {
            fingerprint: local.fingerprint.clone(),
            blake3,
            size: local.size,
            remote_version_id: remote.version_id,
        }),
        local_version_id: local.version_id,
        synced_at_ms: now_ms,
        entry: EntryState::InSync,
        pending_at: None,
        local: None,
        remote: None,
    }
}

/// A base row for an entry the realm never acknowledged. It carries no synced
/// bytes at all, so no guard can ever be built from it.
fn unsynced(local: Option<&Observed>, now_ms: u64) -> SyncBase {
    SyncBase {
        synced: None,
        local_version_id: local.and_then(|local| local.version_id),
        synced_at_ms: now_ms,
        entry: EntryState::InSync,
        pending_at: None,
        local: None,
        remote: None,
    }
}

/// One decided download, so the recording call keeps a readable signature.
struct Planned {
    remote_version: Ulid,
    guard: WriteGuard,
    conflicted: bool,
    entry: EntryState,
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
        // Both rows of every path in one read: the base decides, and the outbox
        // row says whether an upload is already owed.
        let reads = paths
            .iter()
            .map(|relative| {
                (
                    SYNC_BASE_KEYSPACE.to_string(),
                    base_key(folder_id, relative),
                )
            })
            .chain(paths.iter().map(|relative| {
                (
                    SYNC_UPLOAD_OUTBOX_KEYSPACE.to_string(),
                    base_key(folder_id, relative),
                )
            }))
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
                let split = self.paths.len();
                if values.len() != split * 2 {
                    return unexpected(self, "one base and one outbox row per path", got);
                }
                let (bases, queued) = values.split_at(split);
                self.bases = bases
                    .iter()
                    .map(|(_, value)| {
                        value
                            .as_ref()
                            .and_then(|bytes| SyncBase::from_bytes(bytes.as_ref()).ok())
                    })
                    .collect();
                self.queued = queued
                    .iter()
                    .map(|(_, value)| {
                        value
                            .as_ref()
                            .and_then(|bytes| SyncUpload::from_bytes(bytes.as_ref()).ok())
                    })
                    .collect();
                let eligible = self.hash_batch();
                // A page that cannot hash every moved file decides only what it
                // read: the rest waits for the next pass, which is asked for
                // promptly instead of after the idle wait.
                self.plan.truncated |= eligible.len() > MAX_HASH_BATCH;
                self.deferred = eligible.iter().copied().skip(MAX_HASH_BATCH).collect();
                self.hashing = eligible.into_iter().take(MAX_HASH_BATCH).collect();
                self.next_hash(0)
            }
            ReconcileState::HashEntry { index } => {
                let Some(position) = self.hashing.get(index).copied() else {
                    return self.settle();
                };
                let got = format!("{event:?}");
                let Event::LocalFile(event) = event else {
                    return unexpected(self, "local file result", got);
                };
                // A refusal or an error simply leaves the strong hash unknown,
                // which every later decision already treats as changed bytes.
                if let LocalFileEvent::Hashed {
                    fingerprint,
                    blake3,
                    size,
                } = event
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
    /// Whether the realm is already owed exactly these local bytes. The queued
    /// row is the decision this pass would take again, and repeating it before
    /// the realm answers adds a second conflicted copy of the same file.
    fn awaits_upload(
        &self,
        index: usize,
        local: Option<&Observed>,
        base: Option<&SyncBase>,
    ) -> bool {
        if base.is_some_and(|base| base.synced.is_some()) {
            return false;
        }
        let Some(version) = local.and_then(|local| local.version_id) else {
            return false;
        };
        self.queued
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|queued| !queued.deleted && queued.local_version == Some(version))
    }

    /// Paths the recorded stat no longer vouches for: it moved, it was never
    /// recorded, or the filesystem could not answer for it. Only those need a
    /// strong hash, and only the first [`MAX_HASH_BATCH`] of them are read here.
    fn hash_batch(&self) -> Vec<usize> {
        self.paths
            .iter()
            .enumerate()
            .filter(|(index, relative)| {
                self.input
                    .local
                    .get(*relative)
                    .is_some_and(|local| !self.reuses_hash(*index, local))
            })
            .map(|(index, _)| index)
            .collect()
    }

    /// Whether the recorded hash may stand in for reading this file. Only a
    /// settled observation may: the same complete stat, a filesystem that
    /// timestamps finely, and a file still since before the window.
    fn reuses_hash(&self, index: usize, local: &Observed) -> bool {
        let Some(synced) = self
            .bases
            .get(index)
            .and_then(Option::as_ref)
            .and_then(|base| base.synced.as_ref())
        else {
            return false;
        };
        synced.fingerprint == local.fingerprint
            && fingerprint_complete(&local.fingerprint)
            && self.fine_timestamps
            && stat_settled(local, self.input.now_ms)
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{FileStat, FolderMode, FolderState, RealmId, RemoteBinding};
    use aruna_core::types::UserId;
    use byteview::ByteView;

    fn folder() -> SyncedFolder {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        SyncedFolder {
            folder_id: Ulid::from_bytes([1u8; 16]),
            root: "/home/ada/data".to_string(),
            local_bucket: "folder-x".to_string(),
            group_id: Ulid::from_bytes([2u8; 16]),
            remote: RemoteBinding {
                node_id: aruna_core::NodeId::from_bytes(&[3u8; 32]).expect("node id"),
                bucket: "lab".to_string(),
                prefix: String::new(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            state: FolderState::Active,
            created_by: UserId::new(Ulid::from_bytes([4u8; 16]), realm_id),
            created_at_ms: 1,
            last_reconcile_ms: None,
            list_cursor: None,
        }
    }

    fn base(fingerprint: &str, remote: Option<Ulid>) -> SyncBase {
        SyncBase {
            synced: remote.map(|remote_version_id| SyncedBytes {
                fingerprint: fingerprint.to_string(),
                blake3: [9u8; 32],
                size: 5,
                remote_version_id,
            }),
            local_version_id: None,
            synced_at_ms: 1,
            entry: EntryState::InSync,
            pending_at: None,
            local: None,
            remote: None,
        }
    }

    /// When every fixture pass observes, far enough past the fixture's stat
    /// timestamps that a file which has not changed reads as settled.
    const OBSERVED_AT_MS: u64 = 10_000;

    /// An observation of a file that has been still for a while, on a
    /// filesystem that timestamps below the second.
    fn observed(fingerprint: &str) -> Observed {
        Observed {
            fingerprint: fingerprint.to_string(),
            size: 5,
            blake3: None,
            modified_at_ms: Some(3),
            version_id: Some(Ulid::from_bytes([6u8; 16])),
            stat: Some(FileStat {
                size: 5,
                modified_ns: Some(1_500_000),
                changed_ns: Some(1_500_000),
                inode: Some(42),
            }),
        }
    }

    /// The same file as the sweep saw it moments ago: the write that made it
    /// could still be hiding inside these timestamps.
    fn observed_now(fingerprint: &str, now_ms: u64) -> Observed {
        Observed {
            stat: Some(FileStat {
                size: 5,
                modified_ns: Some(u128::from(now_ms) * 1_000_000 + 500_000),
                changed_ns: Some(u128::from(now_ms) * 1_000_000 + 500_000),
                inode: Some(42),
            }),
            ..observed(fingerprint)
        }
    }

    fn head(version: Ulid, deleted: bool) -> RemoteHead {
        RemoteHead {
            relative: "paper.txt".to_string(),
            version_id: version,
            size: 5,
            blake3: Some([8u8; 32]),
            modified_at_ms: Some(4),
            deleted,
        }
    }

    fn operation(
        local: Option<Observed>,
        base: Option<SyncBase>,
        remote: Option<RemoteHead>,
    ) -> (ReconcileFolderOperation, Option<SyncBase>) {
        let input = ReconcileInput {
            folder: folder(),
            local: local
                .map(|local| BTreeMap::from([("paper.txt".to_string(), local)]))
                .unwrap_or_default(),
            remote: remote
                .map(|remote| BTreeMap::from([("paper.txt".to_string(), remote)]))
                .unwrap_or_default(),
            now_ms: OBSERVED_AT_MS,
        };
        (ReconcileFolderOperation::new(input), base)
    }

    /// Feeds the stored base back and runs the operation to completion.
    fn run(operation: ReconcileFolderOperation, base: Option<SyncBase>) -> ReconcilePlan {
        run_queued(operation, base, None)
    }

    /// The same, with an upload row already waiting for this path.
    fn run_queued(
        mut operation: ReconcileFolderOperation,
        base: Option<SyncBase>,
        queued: Option<SyncUpload>,
    ) -> ReconcilePlan {
        let effects = operation.start();
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::BatchRead { .. }))
        ));
        let value = base.map(|base| ByteView::from(base.to_bytes().expect("base encodes")));
        let queued = queued.map(|row| ByteView::from(row.to_bytes().expect("row encodes")));
        let mut effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (ByteView::from(vec![0u8]), value),
                (ByteView::from(vec![1u8]), queued),
            ],
        }));
        while !operation.is_complete() {
            let effect = effects.first().expect("the operation must ask for more");
            let event = match effect {
                Effect::LocalFile(LocalFileEffect::Hash { .. }) => {
                    Event::LocalFile(LocalFileEvent::Hashed {
                        fingerprint: "5-2-2-2".to_string(),
                        blake3: [5u8; 32],
                        size: 5,
                    })
                }
                Effect::Storage(StorageEffect::StartTransaction { .. }) => {
                    Event::Storage(StorageEvent::TransactionStarted {
                        txn_id: Ulid::from_bytes([2u8; 16]),
                    })
                }
                Effect::Storage(StorageEffect::BatchWrite { .. }) => {
                    Event::Storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    })
                }
                Effect::Storage(StorageEffect::BatchDelete { .. }) => {
                    Event::Storage(StorageEvent::BatchDeleteResult {
                        entries: Vec::new(),
                    })
                }
                Effect::Storage(StorageEffect::CommitTransaction { txn_id }) => {
                    Event::Storage(StorageEvent::TransactionCommitted { txn_id: *txn_id })
                }
                other => panic!("unexpected effect {other:?}"),
            };
            effects = operation.step(event);
        }
        operation.finalize().expect("the page must decide")
    }

    #[test]
    fn rejects_wrong_event() {
        // An event the state cannot explain must fail loudly, not be ignored.
        let (mut operation, _) = operation(Some(observed("5-1-1-1")), None, None);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([2u8; 16]),
        }));
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(ReconcileError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn defers_unhashed_paths() {
        // More moved files than one pass may hash: the rest stay undecided, and
        // the folder asks to be passed again rather than waiting out the idle.
        let mut local = BTreeMap::new();
        let mut paths = Vec::new();
        for index in 0..(MAX_HASH_BATCH + 2) {
            let relative = format!("file-{index:04}.txt");
            local.insert(relative.clone(), observed("5-1-1-1"));
            paths.push(relative);
        }
        let mut operation = ReconcileFolderOperation::new(ReconcileInput {
            folder: folder(),
            local,
            remote: BTreeMap::new(),
            now_ms: 10,
        });
        operation.start();
        let values = (0..paths.len() * 2)
            .map(|index| (ByteView::from(vec![index as u8]), None))
            .collect();
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));

        assert_eq!(operation.deferred.len(), 2);
        assert!(operation.plan.truncated, "the folder must be passed again");
    }

    #[test]
    fn holds_queued_upload() {
        // The owner kept their copy and the realm has not answered yet. Until it
        // does, the entry must not be decided again: a second pass would write
        // another conflicted copy of a file that is already on its way.
        let version = Ulid::from_bytes([3u8; 16]);
        let queued = SyncUpload {
            folder_id: folder().folder_id,
            relative: "paper.txt".to_string(),
            deleted: false,
            fingerprint: "5-1-1-1".to_string(),
            blake3: Some([5u8; 32]),
            size: 5,
            local_version: observed("5-1-1-1").version_id,
            queued_at_ms: 1,
            state: UploadState::Pending {
                due_at_ms: 1,
                attempts: 3,
                last_error: Some("the realm is unreachable".to_string()),
            },
        };
        for _ in 0..2 {
            let (operation, base) = operation(
                Some(observed("5-1-1-1")),
                Some(base("5-1-1-1", None)),
                Some(head(version, false)),
            );
            let plan = run_queued(operation, base, Some(queued.clone()));
            assert!(plan.downloads.is_empty(), "no second conflicted copy");
            assert_eq!(plan.uploads, 0);
        }
    }

    #[test]
    fn plans_conflict_copy() {
        // Both sides changed: the local bytes are published and the remote
        // version is only ever added beside them.
        let version = Ulid::from_bytes([3u8; 16]);
        let (operation, base) = operation(
            Some(observed("5-2-2-2")),
            Some(base("5-1-1-1", Some(Ulid::from_bytes([4u8; 16])))),
            Some(head(version, false)),
        );
        let plan = run(operation, base);
        assert_eq!(plan.uploads, 1);
        assert_eq!(plan.downloads.len(), 1);
        assert!(plan.downloads[0].conflicted);
        assert_eq!(plan.downloads[0].guard, WriteGuard::MustNotExist);
    }

    #[test]
    fn keeps_remote_delete() {
        // A realm deletion never plans a write on the owner's disk.
        let version = Ulid::from_bytes([4u8; 16]);
        let (operation, base) = operation(
            Some(observed("5-1-1-1")),
            Some(base("5-1-1-1", Some(version))),
            Some(head(Ulid::from_bytes([5u8; 16]), true)),
        );
        let plan = run(operation, base);
        assert!(plan.downloads.is_empty());
        assert_eq!(plan.uploads, 0);
        assert_eq!(plan.pending, 1);
    }

    /// Whether the first thing this pass asks for is a hash of the file.
    fn hashes_first(local: Observed, base: Option<SyncBase>) -> bool {
        let (mut operation, base) = operation(Some(local), base, None);
        operation.start();
        let value = base.map(|base| ByteView::from(base.to_bytes().expect("base encodes")));
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (ByteView::from(vec![0u8]), value),
                (ByteView::from(vec![1u8]), None),
            ],
        }));
        matches!(
            effects.first(),
            Some(Effect::LocalFile(LocalFileEffect::Hash { .. }))
        )
    }

    #[test]
    fn hashes_young_file() {
        // A rewrite can restore the size, the inode and the modification time,
        // so a file that changed moments ago is read rather than trusted; one
        // that has been still keeps the hash the base recorded for it.
        let settled = base("5-1-1-1", Some(Ulid::from_bytes([4u8; 16])));
        assert!(hashes_first(
            observed_now("5-1-1-1", OBSERVED_AT_MS),
            Some(settled.clone())
        ));
        assert!(!hashes_first(observed("5-1-1-1"), Some(settled)));
    }

    #[test]
    fn hashes_coarse_stat() {
        // Whole-second timestamps cannot tell a rewrite from the write before
        // it, so the bytes are always read on such a filesystem.
        let coarse = Observed {
            stat: Some(FileStat {
                size: 5,
                modified_ns: Some(1_000_000_000),
                changed_ns: Some(1_000_000_000),
                inode: Some(42),
            }),
            ..observed("5-1-1-1")
        };
        assert!(hashes_first(
            coarse,
            Some(base("5-1-1-1", Some(Ulid::from_bytes([4u8; 16]))))
        ));
    }

    #[test]
    fn hashes_partial_fingerprint() {
        // A stat the filesystem could not fill in proves nothing, so the file is
        // read instead of being assumed to still carry the recorded bytes.
        let (mut operation, base) = operation(
            Some(observed("5-1")),
            Some(base("5-1", Some(Ulid::from_bytes([4u8; 16])))),
            Some(head(Ulid::from_bytes([5u8; 16]), false)),
        );
        operation.start();
        let value = base.map(|base| ByteView::from(base.to_bytes().expect("base encodes")));
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (ByteView::from(vec![0u8]), value),
                (ByteView::from(vec![1u8]), None),
            ],
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::LocalFile(LocalFileEffect::Hash { .. }))
        ));
    }

    #[test]
    fn guards_unchanged_replace() {
        // The one automatic overwrite carries the base it was decided on.
        let (operation, base) = operation(
            Some(observed("5-1-1-1")),
            Some(base("5-1-1-1", Some(Ulid::from_bytes([4u8; 16])))),
            Some(head(Ulid::from_bytes([5u8; 16]), false)),
        );
        let plan = run(operation, base);
        assert_eq!(plan.uploads, 0);
        assert_eq!(plan.downloads.len(), 1);
        assert!(!plan.downloads[0].conflicted);
        assert!(matches!(
            plan.downloads[0].guard,
            WriteGuard::MatchesBase { .. }
        ));
    }
}
