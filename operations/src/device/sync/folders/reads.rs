//! Reads the synced folder rows: the bound folders, their entries, actions and transfers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::rows::{outside_answer, outside_call};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    SYNC_BASE_KEYSPACE, SYNC_LOG_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::{EntryState, SyncActionRecord, SyncBase, SyncedFolder};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::super::repository::{
    MAX_SYNCED_FOLDERS, SYNC_PAGE_SIZE, SyncUpload, base_key, folder_key, folder_prefix, key_path,
};
use super::{FolderCounters, FolderError};

/// A synced folder row as its reader wants it.
pub trait FolderRow: Sized + Send + Debug + PartialEq + 'static {
    fn of(key: &[u8], value: &[u8]) -> Option<Self>;
}

impl FolderRow for SyncedFolder {
    fn of(_: &[u8], value: &[u8]) -> Option<Self> {
        Self::from_bytes(value).ok()
    }
}

impl FolderRow for SyncUpload {
    fn of(_: &[u8], value: &[u8]) -> Option<Self> {
        Self::from_bytes(value).ok()
    }
}

impl FolderRow for SyncBase {
    fn of(_: &[u8], value: &[u8]) -> Option<Self> {
        Self::from_bytes(value).ok()
    }
}

/// Every readable row below a prefix, page by page; a row that does not decode is skipped.
#[derive(Debug, PartialEq)]
pub struct FolderRowsOperation<T> {
    pages: PrefixPages,
    rows: Vec<T>,
    row: PhantomData<T>,
}

impl<T> FolderRowsOperation<T> {
    fn new(space: &str, prefix: Key, limit: usize) -> Self {
        Self {
            pages: PrefixPages::new(space, prefix, limit),
            rows: Vec::new(),
            row: PhantomData,
        }
    }
}

impl<T: FolderRow> Operation for FolderRowsOperation<T> {
    type Output = Vec<T>;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<Vec<T>>, FolderError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<T>>, FolderError> {
        let rows = self.pages.take(result)?;
        (self.rows).extend(rows.iter().filter_map(|(key, value)| T::of(key, value)));
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.rows)),
            false => Next::Call(self.pages.call()),
        })
    }
}

/// Every folder this device binds.
pub fn list_folders() -> FolderRowsOperation<SyncedFolder> {
    let prefix = Key::from(Vec::new());
    FolderRowsOperation::new(SYNCED_FOLDER_KEYSPACE, prefix, MAX_SYNCED_FOLDERS)
}

/// The base rows of one folder.
pub fn folder_bases(folder_id: Ulid) -> FolderRowsOperation<SyncBase> {
    FolderRowsOperation::new(SYNC_BASE_KEYSPACE, folder_prefix(folder_id), SYNC_PAGE_SIZE)
}

fn folder_uploads(folder_id: Ulid) -> FolderRowsOperation<SyncUpload> {
    let prefix = folder_prefix(folder_id);
    FolderRowsOperation::new(SYNC_UPLOAD_KEYSPACE, prefix, SYNC_PAGE_SIZE)
}

/// One row that must decode; absent or unreadable it is not found.
#[derive(Debug, PartialEq)]
pub struct FolderRowOperation<T> {
    space: &'static str,
    key: Key,
    row: PhantomData<T>,
}

/// One bound folder.
pub fn read_bound(folder_id: Ulid) -> FolderRowOperation<SyncedFolder> {
    FolderRowOperation {
        space: SYNCED_FOLDER_KEYSPACE,
        key: folder_key(folder_id),
        row: PhantomData,
    }
}

/// One entry of a folder.
pub fn read_entry(folder_id: Ulid, relative: &str) -> FolderRowOperation<SyncBase> {
    FolderRowOperation {
        space: SYNC_BASE_KEYSPACE,
        key: base_key(folder_id, relative),
        row: PhantomData,
    }
}

impl<T: FolderRow> Operation for FolderRowOperation<T> {
    type Output = T;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<T>, FolderError> {
        let read = read_row(self.space, self.key.to_vec());
        Ok(Next::Call(outside_call(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, FolderError> {
        let stored = outside_answer(result).and_then(|read| read.value());
        let row = (stored.ok().flatten()).and_then(|value| T::of(&self.key, &value));
        Ok(Next::Done(row.ok_or(FolderError::NotFound)?))
    }
}

fn folder_page(space: &str, folder_id: Ulid, cursor: Option<Key>) -> IoCall {
    IoCall::Page {
        space: space.to_string(),
        prefix: folder_prefix(folder_id),
        after: cursor,
        limit: SYNC_PAGE_SIZE,
    }
}

type RowPage<T> = (Vec<T>, Option<Key>);

/// One page of a folder's entries, optionally in one state, with the next cursor.
#[derive(Debug, PartialEq)]
pub struct ListEntriesOperation {
    folder_id: Ulid,
    state: Option<String>,
    cursor: Option<Key>,
}

/// The entries of a folder from `cursor`; see [`ListEntriesOperation`].
pub fn list_entries(
    folder_id: Ulid,
    state: Option<&str>,
    cursor: Option<Key>,
) -> ListEntriesOperation {
    ListEntriesOperation {
        folder_id,
        state: state.map(str::to_string),
        cursor,
    }
}

impl Operation for ListEntriesOperation {
    type Output = RowPage<(String, SyncBase)>;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<Self::Output>, FolderError> {
        let cursor = self.cursor.take();
        Ok(Next::Call(folder_page(
            SYNC_BASE_KEYSPACE,
            self.folder_id,
            cursor,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, FolderError> {
        let (rows, next) = result.page()?;
        let entries = (rows.into_iter())
            .filter_map(|(key, bytes)| {
                let base = SyncBase::from_bytes(&bytes).ok()?;
                Some((key_path(key.as_ref())?, base))
            })
            .filter(|(_, base)| {
                (self.state.as_deref()).is_none_or(|state| base.entry.name() == state)
            })
            .collect();
        Ok(Next::Done((entries, next)))
    }
}

/// One page of a folder's action log, with the next cursor.
#[derive(Debug, PartialEq)]
pub struct ListActionsOperation {
    folder_id: Ulid,
    cursor: Option<Key>,
}

/// The actions of a folder from `cursor`; see [`ListActionsOperation`].
pub fn list_actions(folder_id: Ulid, cursor: Option<Key>) -> ListActionsOperation {
    ListActionsOperation { folder_id, cursor }
}

impl Operation for ListActionsOperation {
    type Output = RowPage<SyncActionRecord>;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<Self::Output>, FolderError> {
        let cursor = self.cursor.take();
        Ok(Next::Call(folder_page(
            SYNC_LOG_KEYSPACE,
            self.folder_id,
            cursor,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, FolderError> {
        let (rows, next) = result.page()?;
        let actions = (rows.into_iter())
            .filter_map(|(_, bytes)| SyncActionRecord::from_bytes(&bytes).ok())
            .collect();
        Ok(Next::Done((actions, next)))
    }
}

/// How a folder's entries stand, counted from its base and upload rows.
#[derive(Debug, PartialEq)]
pub struct FolderCountersOperation {
    bases: PrefixPages,
    uploads: PrefixPages,
    counting_uploads: bool,
    counters: FolderCounters,
}

/// The counters of one folder; see [`FolderCountersOperation`].
pub fn folder_counters(folder_id: Ulid) -> FolderCountersOperation {
    let prefix = folder_prefix(folder_id);
    FolderCountersOperation {
        bases: PrefixPages::new(SYNC_BASE_KEYSPACE, prefix.clone(), SYNC_PAGE_SIZE),
        uploads: PrefixPages::new(SYNC_UPLOAD_KEYSPACE, prefix, SYNC_PAGE_SIZE),
        counting_uploads: false,
        counters: FolderCounters::default(),
    }
}

impl FolderCountersOperation {
    fn count_bases(&mut self, rows: &[(Key, Value)]) {
        for (_, bytes) in rows {
            let Ok(base) = SyncBase::from_bytes(bytes) else {
                continue;
            };
            match base.entry {
                EntryState::InSync => self.counters.in_sync += 1,
                EntryState::Conflict { .. } => self.counters.conflicts += 1,
                EntryState::PendingReplace { .. } => self.counters.pending_replacements += 1,
                EntryState::RemoteDeleted { .. } => self.counters.remote_deleted += 1,
                EntryState::Error { .. } => self.counters.errors += 1,
                _ => {}
            }
        }
    }
}

impl Operation for FolderCountersOperation {
    type Output = FolderCounters;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<FolderCounters>, FolderError> {
        Ok(Next::Call(self.bases.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FolderCounters>, FolderError> {
        if self.counting_uploads {
            self.counters.uploading += self.uploads.take(result)?.len();
            return Ok(match self.uploads.done() {
                true => Next::Done(self.counters),
                false => Next::Call(self.uploads.call()),
            });
        }
        let rows = self.bases.take(result)?;
        self.count_bases(&rows);
        if !self.bases.done() {
            return Ok(Next::Call(self.bases.call()));
        }
        self.counting_uploads = true;
        Ok(Next::Call(self.uploads.call()))
    }
}

/// Every upload still queued, over all folders.
#[derive(Debug, Default, PartialEq)]
pub struct ListTransfersOperation {
    folders: Option<VecDeque<Ulid>>,
    uploads: Vec<SyncUpload>,
}

/// The queued uploads of this device; see [`ListTransfersOperation`].
pub fn list_transfers() -> ListTransfersOperation {
    ListTransfersOperation::default()
}

impl ListTransfersOperation {
    fn next_folder(&mut self) -> Result<Next<Vec<SyncUpload>>, FolderError> {
        let folders = self.folders.as_mut().ok_or(IoError::Unfinished)?;
        Ok(match folders.pop_front() {
            Some(folder_id) => Next::Call(IoCall::child(folder_uploads(folder_id))),
            None => Next::Done(std::mem::take(&mut self.uploads)),
        })
    }
}

impl Operation for ListTransfersOperation {
    type Output = Vec<SyncUpload>;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<Vec<SyncUpload>>, FolderError> {
        Ok(Next::Call(IoCall::child(list_folders())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<SyncUpload>>, FolderError> {
        if self.folders.is_none() {
            let folders = result.child::<Vec<SyncedFolder>, FolderError>()??;
            self.folders = Some(folders.iter().map(|folder| folder.folder_id).collect());
        } else {
            let uploads = result.child::<Vec<SyncUpload>, FolderError>()??;
            self.uploads.extend(uploads);
        }
        self.next_folder()
    }
}

#[cfg(test)]
#[path = "folder_reads_tests.rs"]
mod tests;
