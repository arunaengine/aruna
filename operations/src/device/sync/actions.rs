//! Runs the owner's explicit sync actions that may replace or remove local bytes.
//! Automatic sync never does this, and every action writes an audit row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{done, outside_answer, outside_call, read_row, value};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{
    ActionKind, ActionOutcome, ActionScope, EntryState, FolderMode, FolderState, SyncActionRecord,
    SyncBase, SyncedFolder, WriteGuard,
};
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

use crate::device::sync_status::ArmTimerOperation;
use crate::driver::DriverContext;

use super::download::{Blob, FetchError, FetchRemoteOperation};
use super::folders::{FolderError, list_entries, read_bound};
use super::materialize::{
    MaterializeEntryOperation, MaterializeError, MaterializeInput, MaterializeOutcome,
};
use super::remove::RemoveEntryOperation;
use super::repository::{
    SyncUpload, UploadState, action_entry, base_entry, base_key, upload_entry,
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

impl From<IoError> for ActionError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Runs one owner action; the realm comes from this node's network identity.
pub fn apply_action(context: &DriverContext, input: ApplyActionInput) -> ApplyActionOperation {
    ApplyActionOperation {
        realm_id: context.net_handle.as_ref().map(|net| *net.realm_id()),
        input,
        step: ActionStep::Folder,
        folder: None,
        cursor: None,
        pending: VecDeque::new(),
        applied: 0,
        stale: 0,
        record: None,
    }
}

/// What the pending call of an owner action answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActionStep {
    Folder,
    Entry,
    Listing,
    Pending,
    Audit,
}

/// Runs one owner action and answers the audit row it wrote.
#[derive(Debug, PartialEq)]
pub struct ApplyActionOperation {
    realm_id: Option<RealmId>,
    input: ApplyActionInput,
    step: ActionStep,
    folder: Option<SyncedFolder>,
    cursor: Option<Key>,
    pending: VecDeque<(String, SyncBase)>,
    applied: usize,
    stale: usize,
    record: Option<SyncActionRecord>,
}

type ActionTurn = Result<Next<SyncActionRecord>, ActionError>;

impl ApplyActionOperation {
    fn folder(&self) -> Result<SyncedFolder, IoError> {
        self.folder.clone().ok_or(IoError::Unfinished)
    }

    fn entry(&self, input: ApplyActionInput, relative: String) -> Result<IoCall, ActionError> {
        let entry = EntryActionOperation::new(self.realm_id, self.folder()?, input, relative);
        Ok(IoCall::child(entry))
    }

    fn bound(&mut self, folder: SyncedFolder) -> ActionTurn {
        if folder.state == FolderState::Deleting {
            return Err(FolderError::NotFound.into());
        }
        self.folder = Some(folder);
        match self.input.scope.clone() {
            ActionScope::Entry { relative } => {
                self.step = ActionStep::Entry;
                Ok(Next::Call(self.entry(self.input.clone(), relative)?))
            }
            ActionScope::AllPending => self.list_page(),
        }
    }

    fn list_page(&mut self) -> ActionTurn {
        self.step = ActionStep::Listing;
        let list = list_entries(self.input.folder_id, None, self.cursor.take());
        Ok(Next::Call(IoCall::child(list)))
    }

    /// Takes the next pending entry; a removal is per entry, so only the entries that name a
    /// remote version to take are replaced in bulk.
    fn next_pending(&mut self) -> ActionTurn {
        while let Some((relative, base)) = self.pending.pop_front() {
            let Some(expected) = observed_expected(&base) else {
                self.stale += 1;
                continue;
            };
            let step = ApplyActionInput {
                scope: ActionScope::Entry {
                    relative: relative.clone(),
                },
                expected: Some(expected),
                ..self.input.clone()
            };
            self.step = ActionStep::Pending;
            return Ok(Next::Call(self.entry(step, relative)?));
        }
        match self.cursor {
            Some(_) => self.list_page(),
            None => self.audit(),
        }
    }

    fn audit(&mut self) -> ActionTurn {
        let record = SyncActionRecord {
            entries: self.applied,
            outcome: match self.stale {
                0 => ActionOutcome::Applied,
                stale => ActionOutcome::Failed {
                    reason: format!("{stale} entries changed since the owner saw them"),
                },
            },
            ..record_for(&self.input, ActionScope::AllPending)
        };
        let row = action_entry(&record).map_err(|_| ActionError::Unavailable)?;
        self.record = Some(record);
        self.step = ActionStep::Audit;
        Ok(Next::Call(write_call(vec![row])))
    }
}

impl Operation for ApplyActionOperation {
    type Output = SyncActionRecord;
    type Error = ActionError;

    fn first(&mut self) -> ActionTurn {
        Ok(Next::Call(IoCall::child(read_bound(self.input.folder_id))))
    }

    fn next(&mut self, result: IoResult) -> ActionTurn {
        match self.step {
            ActionStep::Folder => self.bound(result.child::<SyncedFolder, FolderError>()??),
            ActionStep::Entry => {
                let record = result.child::<SyncActionRecord, ActionError>()??;
                Ok(Next::Done(record))
            }
            ActionStep::Listing => {
                let (entries, next) =
                    result.child::<(Vec<(String, SyncBase)>, Option<Key>), FolderError>()??;
                self.pending = (entries.into_iter())
                    .filter(|(_, base)| {
                        matches!(
                            base.entry,
                            EntryState::Conflict { .. } | EntryState::PendingReplace { .. }
                        )
                    })
                    .collect();
                self.cursor = next;
                self.next_pending()
            }
            ActionStep::Pending => {
                match result.child::<SyncActionRecord, ActionError>()? {
                    Ok(record) if record.outcome == ActionOutcome::Applied => self.applied += 1,
                    Ok(_) | Err(_) => self.stale += 1,
                }
                self.next_pending()
            }
            ActionStep::Audit => {
                row_written(result)?;
                let record = self.record.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(record))
            }
        }
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
        trashed_to: None,
        entries: 1,
    }
}

/// `rows` as a call whose failure reaches the action as an answer.
fn write_call(rows: Vec<(String, Key, aruna_core::types::Value)>) -> IoCall {
    outside_call(IoCall::Write(rows))
}

fn row_written(result: IoResult) -> Result<(), ActionError> {
    (outside_answer(result).and_then(done)).map_err(|_| ActionError::Unavailable)
}

/// What the pending call of an action on one entry answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryStep {
    Base,
    Fetch,
    Write,
    Kept,
    Armed,
    Settled,
    Removed,
}

/// Applies the owner's action to one entry.
#[derive(Debug, PartialEq)]
struct EntryActionOperation {
    realm_id: Option<RealmId>,
    folder: SyncedFolder,
    input: ApplyActionInput,
    relative: String,
    step: EntryStep,
    base: Option<SyncBase>,
    version: Option<Ulid>,
    record: Option<SyncActionRecord>,
}

type EntryTurn = Result<Next<SyncActionRecord>, ActionError>;

impl EntryActionOperation {
    fn new(
        realm_id: Option<RealmId>,
        folder: SyncedFolder,
        input: ApplyActionInput,
        relative: String,
    ) -> Self {
        Self {
            realm_id,
            folder,
            input,
            relative,
            step: EntryStep::Base,
            base: None,
            version: None,
            record: None,
        }
    }

    fn take_record(&mut self) -> Result<SyncActionRecord, IoError> {
        self.record.take().ok_or(IoError::Unfinished)
    }

    fn decide(&mut self, base: SyncBase) -> EntryTurn {
        let scope = ActionScope::Entry {
            relative: self.relative.clone(),
        };
        self.record = Some(record_for(&self.input, scope));
        match self.input.kind {
            ActionKind::Replace => self.replace(base),
            ActionKind::KeepLocal => self.keep_local(&base),
            ActionKind::RemoveLocal => self.remove_local(&base),
            ActionKind::Resolve => self.settle(&base),
        }
    }

    /// Asks for the remote version the owner chose to replace the local bytes with.
    fn replace(&mut self, base: SyncBase) -> EntryTurn {
        let expected = (self.input.expected.as_ref()).ok_or(ActionError::ExpectedMissing)?;
        let version = (expected.remote_version)
            .or_else(|| pending_version(&base.entry))
            .or_else(|| base.synced.as_ref().map(|synced| synced.remote_version_id))
            .ok_or(ActionError::NoRemoteVersion)?;
        let realm_id = self.realm_id.ok_or(ActionError::RemoteUnavailable)?;
        let fetch = FetchRemoteOperation::new(realm_id, &self.folder, &self.relative, version);
        self.base = Some(base);
        self.version = Some(version);
        self.step = EntryStep::Fetch;
        Ok(Next::Call(IoCall::child(fetch)))
    }

    /// Writes the fetched bytes under a guard built from the hashes the owner was shown.
    fn write(&mut self, blob: Blob) -> EntryTurn {
        let expected = (self.input.expected.as_ref()).ok_or(ActionError::ExpectedMissing)?;
        let base = self.base.take().ok_or(IoError::Unfinished)?;
        let write = MaterializeEntryOperation::new(MaterializeInput {
            folder: self.folder.clone(),
            relative: self.relative.clone(),
            remote_version: self.version.ok_or(IoError::Unfinished)?,
            guard: WriteGuard::MatchesBase {
                fingerprint: expected.fingerprint.clone(),
                blake3: expected.blake3,
            },
            conflicted: false,
            local_fingerprint: Some(expected.fingerprint.clone()),
            local: base.local,
            remote: base.remote,
            synced: base.synced,
            audit: self.record.clone(),
            blob,
        });
        self.step = EntryStep::Write;
        Ok(Next::Call(IoCall::child(write)))
    }

    /// Keeps the local bytes and publishes them: the automatic sync never makes an
    /// unknown-base copy the realm head, so this turns the file into the next realm version.
    fn keep_local(&mut self, base: &SyncBase) -> EntryTurn {
        let cleared = SyncBase {
            entry: EntryState::LocalChanged,
            pending_at: None,
            ..base.clone()
        };
        let mut rows = self.cleared_rows(&cleared)?;
        // A download-only folder keeps the owner's bytes without publishing them.
        if self.folder.mode != FolderMode::DownloadOnly
            && let Some(upload) = queued_upload(self.folder.folder_id, &self.relative, base)
        {
            rows.push(upload_entry(&upload).map_err(|_| ActionError::Unavailable)?);
        }
        self.step = EntryStep::Kept;
        Ok(Next::Call(write_call(rows)))
    }

    /// Accepts the current state: the entry stops being reported and the automatic sync
    /// decides it again on the next pass.
    fn settle(&mut self, base: &SyncBase) -> EntryTurn {
        let cleared = SyncBase {
            entry: EntryState::InSync,
            pending_at: None,
            ..base.clone()
        };
        let rows = self.cleared_rows(&cleared)?;
        self.step = EntryStep::Settled;
        Ok(Next::Call(write_call(rows)))
    }

    fn cleared_rows(
        &self,
        cleared: &SyncBase,
    ) -> Result<Vec<(String, Key, aruna_core::types::Value)>, ActionError> {
        let record = self.record.as_ref().ok_or(IoError::Unfinished)?;
        let unavailable = |_| ActionError::Unavailable;
        Ok(vec![
            base_entry(self.folder.folder_id, &self.relative, cleared).map_err(unavailable)?,
            action_entry(record).map_err(unavailable)?,
        ])
    }

    /// Moves the file the owner named into the folder's trash, under a guard built from the
    /// bytes they were shown when they gave one.
    fn remove_local(&mut self, base: &SyncBase) -> EntryTurn {
        let expected = (self.input.expected.clone())
            .or_else(|| observed_expected(base))
            .ok_or(ActionError::ExpectedMissing)?;
        let guard = WriteGuard::MatchesBase {
            fingerprint: expected.fingerprint,
            blake3: expected.blake3,
        };
        let record = self.take_record()?;
        let remove =
            RemoveEntryOperation::new(self.folder.clone(), self.relative.clone(), guard, record);
        self.step = EntryStep::Removed;
        Ok(Next::Call(IoCall::child(remove)))
    }
}

impl Operation for EntryActionOperation {
    type Output = SyncActionRecord;
    type Error = ActionError;

    fn first(&mut self) -> EntryTurn {
        let key = base_key(self.folder.folder_id, &self.relative);
        Ok(Next::Call(outside_call(read_row(SYNC_BASE_KEYSPACE, key))))
    }

    fn next(&mut self, result: IoResult) -> EntryTurn {
        match self.step {
            EntryStep::Base => {
                let row = outside_answer(result).and_then(value).ok().flatten();
                let base = row.and_then(|bytes| SyncBase::from_bytes(&bytes).ok());
                self.decide(base.ok_or(ActionError::NotFound)?)
            }
            EntryStep::Fetch => {
                let fetched = result.child::<Blob, FetchError>()?;
                self.write(fetched.map_err(|_| ActionError::RemoteUnavailable)?)
            }
            // Anything but the guarded write keeps the file and is audited as stale.
            EntryStep::Write => {
                let outcome = result.child::<MaterializeOutcome, MaterializeError>()?;
                let outcome = outcome.map_err(|_| ActionError::Unavailable)?;
                let record = self.take_record()?;
                Ok(Next::Done(SyncActionRecord {
                    outcome: match outcome {
                        MaterializeOutcome::Refused { .. } => ActionOutcome::Stale,
                        _ => ActionOutcome::Applied,
                    },
                    ..record
                }))
            }
            EntryStep::Kept => {
                row_written(result)?;
                self.step = EntryStep::Armed;
                let arm = ArmTimerOperation::new(TaskKey::DrainUploadOutbox);
                Ok(Next::Call(IoCall::child(arm)))
            }
            EntryStep::Armed => {
                result.child::<(), IoError>()?.ok();
                Ok(Next::Done(self.take_record()?))
            }
            EntryStep::Settled => {
                row_written(result)?;
                Ok(Next::Done(self.take_record()?))
            }
            EntryStep::Removed => {
                let removed = result.child::<SyncActionRecord, ActionError>()?;
                Ok(Next::Done(removed.map_err(|_| ActionError::Unavailable)?))
            }
        }
    }
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

/// The outbox row that publishes the bytes the last observation saw. An entry
/// whose local side was never observed has nothing to publish yet.
fn queued_upload(folder_id: Ulid, relative: &str, base: &SyncBase) -> Option<SyncUpload> {
    let local = base.local.as_ref()?;
    let now = unix_timestamp_millis();
    Some(SyncUpload {
        folder_id,
        relative: relative.to_string(),
        deleted: false,
        fingerprint: local.fingerprint.clone()?,
        blake3: local.blake3,
        size: local.size,
        local_version: local.version_id.or(base.local_version_id),
        queued_at_ms: now,
        state: UploadState::Pending {
            due_at_ms: now,
            attempts: 0,
            last_error: None,
        },
    })
}

#[cfg(test)]
#[path = "actions_tests.rs"]
mod operation_tests;

#[cfg(test)]
mod pure_tests {
    use super::queued_upload;
    use aruna_core::structs::{EntrySide, EntryState, SyncBase};
    use ulid::Ulid;

    fn base(local: Option<EntrySide>) -> SyncBase {
        SyncBase {
            synced: None,
            local_version_id: Some(Ulid::from_bytes([2u8; 16])),
            synced_at_ms: 1,
            entry: EntryState::LocalChanged,
            pending_at: None,
            local,
            remote: None,
        }
    }

    #[test]
    fn publishes_observed_bytes() {
        // Keeping the local copy is what publishes an unknown-base conflict, so
        // it must queue exactly the bytes the owner was shown.
        let local = EntrySide {
            size: 4,
            modified_at_ms: None,
            fingerprint: Some("4-1".to_string()),
            blake3: Some([7u8; 32]),
            version_id: Some(Ulid::from_bytes([3u8; 16])),
        };
        let folder_id = Ulid::from_bytes([1u8; 16]);
        let upload = queued_upload(folder_id, "note.txt", &base(Some(local))).expect("a row");
        assert_eq!(upload.fingerprint, "4-1");
        assert_eq!(upload.blake3, Some([7u8; 32]));
        assert_eq!(upload.local_version, Some(Ulid::from_bytes([3u8; 16])));
        assert!(!upload.deleted);
        assert!(queued_upload(folder_id, "note.txt", &base(None)).is_none());
    }
}
