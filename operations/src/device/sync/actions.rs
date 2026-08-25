//! The explicit owner decisions that may change local bytes.
//!
//! The automatic sync never replaces divergent bytes and never removes a file.
//! Both happen only here, only for the exact bytes the owner was shown, and
//! every one of them leaves an audit row committed with the state it changed.

use std::sync::Arc;

use aruna_core::effects::{Effect, LocalFileEffect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, LocalFileEvent, StorageEvent};
use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    ActionKind, ActionOutcome, ActionScope, EntryState, SyncActionRecord, SyncBase, SyncedFolder,
    WriteGuard,
};
use aruna_core::types::{Effects, Key, TxnId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};

use super::folders::{FolderError, list_entries, read_bound};
use super::materialize::{
    MaterializeEntryOperation, MaterializeInput, MaterializeOutcome, fetch_remote,
};
use super::repository::{
    SyncUpload, UploadState, action_entry, base_entry, base_key, read_value, upload_entry,
    write_rows,
};

/// The identity of the bytes the owner acted on. A replace applies to exactly
/// these bytes or to nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpectedEntry {
    pub fingerprint: String,
    pub blake3: [u8; 32],
    pub remote_version: Option<Ulid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyActionInput {
    pub folder_id: Ulid,
    pub kind: ActionKind,
    pub scope: ActionScope,
    pub expected: Option<ExpectedEntry>,
    pub actor: UserId,
}

#[derive(Debug, Error, PartialEq)]
pub enum ActionError {
    #[error("no such entry in this folder")]
    NotFound,
    #[error("the entry names no remote version to replace it with")]
    NoRemoteVersion,
    #[error("replacing an entry needs the hashes the owner was shown")]
    ExpectedMissing,
    #[error("the remote version could not be read")]
    RemoteUnavailable,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the device store is unavailable")]
    Unavailable,
    #[error(transparent)]
    Folder(#[from] FolderError),
}

/// Runs one owner action and answers the audit row it wrote.
pub async fn apply_action(
    context: &Arc<DriverContext>,
    input: ApplyActionInput,
) -> Result<SyncActionRecord, ActionError> {
    let folder = read_bound(context, input.folder_id).await?;
    match input.scope.clone() {
        ActionScope::Entry { relative } => apply_entry(context, &folder, &input, &relative).await,
        ActionScope::AllPending => apply_pending(context, &folder, &input).await,
    }
}

fn record_for(input: &ApplyActionInput, scope: ActionScope) -> SyncActionRecord {
    SyncActionRecord {
        action_id: Ulid::generate(),
        folder_id: input.folder_id,
        kind: input.kind,
        scope,
        actor: input.actor,
        at_ms: unix_timestamp_millis(),
        before: input.expected.as_ref().map(|expected| expected.blake3),
        after: None,
        outcome: ActionOutcome::Applied,
        entries: 1,
    }
}

async fn apply_entry(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    input: &ApplyActionInput,
    relative: &str,
) -> Result<SyncActionRecord, ActionError> {
    let base = read_base(context, folder.folder_id, relative)
        .await
        .ok_or(ActionError::NotFound)?;
    let record = record_for(
        input,
        ActionScope::Entry {
            relative: relative.to_string(),
        },
    );
    match input.kind {
        ActionKind::Replace => replace_entry(context, folder, input, relative, &base, record).await,
        ActionKind::KeepLocal => keep_local(context, folder, relative, &base, record).await,
        ActionKind::RemoveLocal => remove_local(context, folder, relative, record).await,
        ActionKind::Resolve => settle_entry(context, folder, relative, &base, record).await,
    }
}

/// Replaces the local bytes with the remote version, under a guard built from
/// the hashes the owner was shown. Anything else keeps the file and is audited
/// as stale.
async fn replace_entry(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    input: &ApplyActionInput,
    relative: &str,
    base: &SyncBase,
    record: SyncActionRecord,
) -> Result<SyncActionRecord, ActionError> {
    let expected = input
        .expected
        .as_ref()
        .ok_or(ActionError::ExpectedMissing)?;
    let version = expected
        .remote_version
        .or_else(|| pending_version(&base.entry))
        .or(base.remote_version_id)
        .ok_or(ActionError::NoRemoteVersion)?;
    let blob = fetch_remote(context, folder, relative, version)
        .await
        .ok_or(ActionError::RemoteUnavailable)?;
    let outcome = drive(
        MaterializeEntryOperation::new(MaterializeInput {
            folder: folder.clone(),
            relative: relative.to_string(),
            remote_version: version,
            guard: WriteGuard::MatchesBase {
                fingerprint: expected.fingerprint.clone(),
                blake3: expected.blake3,
            },
            conflicted: false,
            local_fingerprint: Some(expected.fingerprint.clone()),
            local: base.local.clone(),
            remote: base.remote.clone(),
            audit: Some(record.clone()),
            blob,
        }),
        context,
    )
    .await
    .map_err(|_| ActionError::Unavailable)?;
    Ok(SyncActionRecord {
        outcome: match outcome {
            MaterializeOutcome::Refused { .. } => ActionOutcome::Stale,
            _ => ActionOutcome::Applied,
        },
        ..record
    })
}

/// The bytes the last observation saw, which a bulk replace is guarded by. An
/// entry whose file changed since then is refused rather than overwritten.
fn observed_expected(base: &SyncBase) -> Option<ExpectedEntry> {
    let local = base.local.as_ref()?;
    Some(ExpectedEntry {
        fingerprint: local.fingerprint.clone()?,
        blake3: local.blake3?,
        remote_version: pending_version(&base.entry),
    })
}

/// The remote version a pending entry was reported against.
fn pending_version(entry: &EntryState) -> Option<Ulid> {
    match entry {
        EntryState::Conflict { remote_version, .. }
        | EntryState::PendingReplace { remote_version, .. }
        | EntryState::RemoteDeleted { remote_version } => Some(*remote_version),
        _ => None,
    }
}

/// Publishes the local bytes as the entry's next realm version and clears the
/// pending mark, so the automatic sync takes the entry back over.
async fn keep_local(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    relative: &str,
    base: &SyncBase,
    record: SyncActionRecord,
) -> Result<SyncActionRecord, ActionError> {
    let now = unix_timestamp_millis();
    let upload = SyncUpload {
        folder_id: folder.folder_id,
        relative: relative.to_string(),
        deleted: false,
        fingerprint: base.fingerprint.clone(),
        blake3: Some(base.blake3),
        size: base.size,
        local_version: base.local_version_id,
        queued_at_ms: now,
        state: UploadState::Pending {
            due_at_ms: now,
            attempts: 0,
            last_error: None,
        },
    };
    let cleared = SyncBase {
        entry: EntryState::LocalChanged,
        pending_at: None,
        ..base.clone()
    };
    let rows = vec![
        base_entry(folder.folder_id, relative, &cleared).map_err(|_| ActionError::Unavailable)?,
        upload_entry(&upload).map_err(|_| ActionError::Unavailable)?,
        action_entry(&record).map_err(|_| ActionError::Unavailable)?,
    ];
    match write_rows(context, rows, None).await {
        true => Ok(record),
        false => Err(ActionError::Unavailable),
    }
}

/// Accepts the current state: the entry stops being reported and the automatic
/// sync decides it again on the next pass.
async fn settle_entry(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    relative: &str,
    base: &SyncBase,
    record: SyncActionRecord,
) -> Result<SyncActionRecord, ActionError> {
    let cleared = SyncBase {
        entry: EntryState::InSync,
        pending_at: None,
        ..base.clone()
    };
    let rows = vec![
        base_entry(folder.folder_id, relative, &cleared).map_err(|_| ActionError::Unavailable)?,
        action_entry(&record).map_err(|_| ActionError::Unavailable)?,
    ];
    match write_rows(context, rows, None).await {
        true => Ok(record),
        false => Err(ActionError::Unavailable),
    }
}

async fn remove_local(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    relative: &str,
    record: SyncActionRecord,
) -> Result<SyncActionRecord, ActionError> {
    drive(
        RemoveEntryOperation::new(folder.clone(), relative.to_string(), record),
        context,
    )
    .await
    .map_err(|_| ActionError::Unavailable)
}

/// Applies one kind to every entry of the folder that is currently pending.
async fn apply_pending(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    input: &ApplyActionInput,
) -> Result<SyncActionRecord, ActionError> {
    let mut cursor: Option<Key> = None;
    let mut applied = 0usize;
    let mut stale = 0usize;
    loop {
        let (entries, next) = list_entries(context, folder.folder_id, None, cursor).await?;
        // A removal is per entry: only the entries that name a remote version
        // to take are replaced in bulk.
        for (relative, base) in entries.into_iter().filter(|(_, base)| {
            matches!(
                base.entry,
                EntryState::Conflict { .. } | EntryState::PendingReplace { .. }
            )
        }) {
            let Some(expected) = observed_expected(&base) else {
                stale += 1;
                continue;
            };
            let step = ApplyActionInput {
                scope: ActionScope::Entry {
                    relative: relative.clone(),
                },
                expected: Some(expected),
                ..input.clone()
            };
            match apply_entry(context, folder, &step, &relative).await {
                Ok(record) if record.outcome == ActionOutcome::Applied => applied += 1,
                Ok(_) | Err(_) => stale += 1,
            }
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let record = SyncActionRecord {
        entries: applied,
        outcome: match stale {
            0 => ActionOutcome::Applied,
            _ => ActionOutcome::Failed {
                reason: format!("{stale} entries changed since the owner saw them"),
            },
        },
        ..record_for(input, ActionScope::AllPending)
    };
    let row = action_entry(&record).map_err(|_| ActionError::Unavailable)?;
    match write_rows(context, vec![row], None).await {
        true => Ok(record),
        false => Err(ActionError::Unavailable),
    }
}

async fn read_base(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    relative: &str,
) -> Option<SyncBase> {
    read_value(
        context,
        SYNC_BASE_KEYSPACE,
        base_key(folder_id, relative),
        None,
    )
    .await
    .and_then(|bytes| SyncBase::from_bytes(&bytes).ok())
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RemoveState {
    Init,
    MoveFile,
    StartTransaction,
    WriteRows { txn_id: TxnId },
    DeleteBase { txn_id: TxnId },
    Commit,
    Finish,
    Error,
}

/// Moves one file into the folder's trash and drops its base row, with the
/// audit row committed alongside. The file is never unlinked.
#[derive(Debug, PartialEq)]
pub struct RemoveEntryOperation {
    folder: SyncedFolder,
    relative: String,
    record: SyncActionRecord,
    state: RemoveState,
    rows: Vec<(String, Key, Value)>,
    output: Option<Result<SyncActionRecord, ActionError>>,
}

impl RemoveEntryOperation {
    pub fn new(folder: SyncedFolder, relative: String, record: SyncActionRecord) -> Self {
        Self {
            folder,
            relative,
            record,
            state: RemoveState::Init,
            rows: Vec::new(),
            output: None,
        }
    }
}

impl Operation for RemoveEntryOperation {
    type Output = SyncActionRecord;
    type Error = ActionError;

    fn start(&mut self) -> Effects {
        self.state = RemoveState::MoveFile;
        smallvec![Effect::LocalFile(LocalFileEffect::MoveAside {
            root: self.folder.root.clone(),
            relative: self.relative.clone(),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return remove_failed(self, ActionError::Storage(error));
        }
        match (self.state, event) {
            (RemoveState::MoveFile, Event::LocalFile(event)) => {
                self.record = match event {
                    LocalFileEvent::Moved { to } => SyncActionRecord {
                        scope: ActionScope::Entry { relative: to },
                        // The trash path is the evidence the bytes still exist.
                        outcome: ActionOutcome::Applied,
                        ..self.record.clone()
                    },
                    LocalFileEvent::Refused { reason } => SyncActionRecord {
                        outcome: ActionOutcome::Failed {
                            reason: format!("{reason:?}"),
                        },
                        ..self.record.clone()
                    },
                    other => {
                        return remove_failed(
                            self,
                            ActionError::Storage(StorageError::WriteError(format!("{other:?}"))),
                        );
                    }
                };
                let Ok(row) = action_entry(&self.record) else {
                    return remove_failed(self, ActionError::Unavailable);
                };
                self.rows = vec![row];
                self.state = RemoveState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            (
                RemoveState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.state = RemoveState::WriteRows { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.rows),
                    txn_id: Some(txn_id),
                })]
            }
            (
                RemoveState::WriteRows { txn_id },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                self.state = RemoveState::DeleteBase { txn_id };
                smallvec![Effect::Storage(StorageEffect::Delete {
                    key_space: SYNC_BASE_KEYSPACE.to_string(),
                    key: base_key(self.folder.folder_id, &self.relative),
                    txn_id: Some(txn_id),
                })]
            }
            (
                RemoveState::DeleteBase { txn_id },
                Event::Storage(StorageEvent::DeleteResult { .. }),
            ) => {
                self.state = RemoveState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (RemoveState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = RemoveState::Finish;
                self.output = Some(Ok(self.record.clone()));
                smallvec![]
            }
            (_, _) => remove_failed(self, ActionError::Unavailable),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RemoveState::Finish | RemoveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ActionError::Unavailable)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveState::WriteRows { txn_id } | RemoveState::DeleteBase { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn remove_failed(operation: &mut RemoveEntryOperation, error: ActionError) -> Effects {
    let cleanup = operation.abort();
    operation.state = RemoveState::Error;
    operation.output = Some(Err(error));
    cleanup
}
