//! Holds the storage shape and row encodings of the device-local synced folder state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    SYNC_BASE_KEYSPACE, SYNC_LOG_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::structs::{SyncActionRecord, SyncBase, SyncedFolder};
use aruna_core::types::{Key, TxnId, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::device::retry::RetryView;

/// Folders one device may bind. A device serves one person's machine, so this
/// is a human-sized list rather than an inventory.
pub const MAX_SYNCED_FOLDERS: usize = 64;

/// Rows one scan reads at a time.
pub const SYNC_PAGE_SIZE: usize = 256;

/// Files one reconcile pass hashes. The strong hash is only needed where the
/// weak fingerprint already says the file moved, so a large first bind
/// converges over several passes instead of blocking one.
pub const MAX_HASH_BATCH: usize = 256;

/// Pull attempts before a still-retryable upload parks for the owner. A refusal, a failed
/// request or a stalled pull spends one; a pull that keeps receiving bytes never does.
pub const MAX_UPLOAD_ATTEMPTS: u32 = 8;

/// One local version waiting for its realm node to pull it. The row is keyed by
/// path, so a path that changes twice before the drain runs is pulled once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncUpload {
    pub folder_id: Ulid,
    pub relative: String,
    /// A local delete asks the realm for a delete marker.
    pub deleted: bool,
    pub fingerprint: String,
    pub blake3: Option<[u8; 32]>,
    pub size: u64,
    /// Device-local version the realm node reads. Absent for a delete.
    pub local_version: Option<Ulid>,
    pub queued_at_ms: u64,
    pub state: UploadState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UploadState {
    Pending {
        due_at_ms: u64,
        attempts: u32,
        last_error: Option<String>,
    },
    Failed {
        reason: String,
        retryable: bool,
    },
    /// The realm node accepted the pull and the device polls it until it ends. A row left in
    /// this state by a stopped drain is due at once and asks the node again.
    Transferring {
        attempts: u32,
        bytes_done: u64,
    },
}

impl SyncUpload {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn is_due(&self, now_ms: u64) -> bool {
        self.state.is_due(now_ms)
    }

    pub fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

pub fn folder_key(folder_id: Ulid) -> Key {
    ByteView::from(folder_id.to_bytes().to_vec())
}

pub fn folder_entry(folder: &SyncedFolder) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        SYNCED_FOLDER_KEYSPACE.to_string(),
        folder_key(folder.folder_id),
        ByteView::from(folder.to_bytes()?),
    ))
}

/// Base rows are keyed by folder and path, so one folder's rows are one
/// contiguous prefix scan and a path is read without scanning at all.
pub fn base_key(folder_id: Ulid, relative: &str) -> Key {
    let mut key = folder_id.to_bytes().to_vec();
    key.extend_from_slice(relative.as_bytes());
    ByteView::from(key)
}

pub(crate) fn folder_prefix(folder_id: Ulid) -> Key {
    ByteView::from(folder_id.to_bytes().to_vec())
}

/// The path a base or upload key carries, or `None` when the key is not one.
pub fn key_path(key: &[u8]) -> Option<String> {
    key.len()
        .checked_sub(16)
        .and_then(|_| std::str::from_utf8(&key[16..]).ok())
        .map(ToOwned::to_owned)
}

pub fn base_entry(
    folder_id: Ulid,
    relative: &str,
    base: &SyncBase,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        SYNC_BASE_KEYSPACE.to_string(),
        base_key(folder_id, relative),
        ByteView::from(base.to_bytes()?),
    ))
}

pub fn upload_entry(upload: &SyncUpload) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        SYNC_UPLOAD_KEYSPACE.to_string(),
        base_key(upload.folder_id, &upload.relative),
        ByteView::from(upload.to_bytes()?),
    ))
}

/// Action rows are keyed by folder and a ULID, so a folder's audit reads in
/// the order the owner acted.
pub fn action_entry(record: &SyncActionRecord) -> Result<(String, Key, Value), ConversionError> {
    let mut key = record.folder_id.to_bytes().to_vec();
    key.extend_from_slice(&record.action_id.to_bytes());
    Ok((
        SYNC_LOG_KEYSPACE.to_string(),
        ByteView::from(key),
        ByteView::from(record.to_bytes()?),
    ))
}

pub fn scan_folders(start_after: Option<Key>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: SYNCED_FOLDER_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: MAX_SYNCED_FOLDERS,
        txn_id,
    })
}

/// One page of a keyspace restricted to one folder.
pub fn scan_folder(
    key_space: &str,
    folder_id: Ulid,
    start_after: Option<Key>,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: Some(folder_prefix(folder_id)),
        start: start_after.map(IterStart::After),
        limit: SYNC_PAGE_SIZE,
        txn_id,
    })
}

/// The device-local bucket a folder is observed as. The owner never names it:
/// a derived name cannot collide with a bucket they already use.
pub fn folder_bucket(folder_id: Ulid) -> String {
    format!("folder-{}", folder_id.to_string().to_lowercase())
}

/// The folder whose device-local bucket `bucket` is, if it is one.
pub fn bucket_folder(bucket: &str) -> Option<Ulid> {
    let folder_id = Ulid::from_string(bucket.strip_prefix("folder-")?).ok()?;
    (folder_bucket(folder_id) == bucket).then_some(folder_id)
}

/// Whether two rows publish the same local version of one file.
pub(crate) fn same_version(stored: &SyncUpload, upload: &SyncUpload) -> bool {
    stored.local_version == upload.local_version && stored.deleted == upload.deleted
}

/// The state an explicit sync gives one upload: a backed-off one is due now with its
/// attempts kept, a retryable failure starts over, and anything else stays.
pub(crate) fn requeued(state: &UploadState, now_ms: u64) -> Option<UploadState> {
    match state {
        UploadState::Pending {
            due_at_ms,
            attempts,
            last_error,
        } if *due_at_ms > now_ms => Some(UploadState::Pending {
            due_at_ms: now_ms,
            attempts: *attempts,
            last_error: last_error.clone(),
        }),
        UploadState::Failed {
            reason,
            retryable: true,
        } => Some(UploadState::Pending {
            due_at_ms: now_ms,
            attempts: 0,
            last_error: Some(reason.clone()),
        }),
        _ => None,
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum StoreUploadError {
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Writes one upload row under a new state in one transaction and outputs whether it was
/// written. `guarded` refuses the write when the stored row no longer carries the state and
/// version this pass read.
#[derive(Debug, PartialEq)]
pub struct StoreUploadOperation {
    upload: SyncUpload,
    next: UploadState,
    guarded: bool,
    written: bool,
}

impl StoreUploadOperation {
    pub fn new(upload: SyncUpload, next: UploadState, guarded: bool) -> Self {
        Self {
            upload,
            next,
            guarded,
            written: false,
        }
    }

    fn write(&mut self) -> Next<bool> {
        let updated = SyncUpload {
            state: self.next.clone(),
            ..self.upload.clone()
        };
        let Ok(row) = upload_entry(&updated) else {
            return Next::Done(false);
        };
        self.written = true;
        Next::Call(IoCall::Write(vec![row]))
    }
}

impl Operation for StoreUploadOperation {
    type Output = bool;
    type Error = StoreUploadError;

    fn first(&mut self) -> Result<Next<bool>, StoreUploadError> {
        if !self.guarded {
            return Ok(self.write());
        }
        let key = base_key(self.upload.folder_id, &self.upload.relative);
        let row = (SYNC_UPLOAD_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, StoreUploadError> {
        if self.written {
            done(result)?;
            return Ok(Next::Done(true));
        }
        let stored = result.values()?.into_iter().next().flatten();
        let stored = stored.and_then(|bytes| SyncUpload::from_bytes(&bytes).ok());
        let holds = stored.is_some_and(|stored| {
            stored.state == self.upload.state && same_version(&stored, &self.upload)
        });
        Ok(match holds {
            true => self.write(),
            false => Next::Done(false),
        })
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        StoreUploadOperation, SyncUpload, UploadState, base_key, key_path, requeued, upload_entry,
    };
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::keyspaces::SYNC_UPLOAD_KEYSPACE;
    use aruna_core::operation::Operation;
    use ulid::Ulid;

    fn upload(state: UploadState) -> SyncUpload {
        SyncUpload {
            folder_id: Ulid::from_bytes([4; 16]),
            relative: "a.txt".to_string(),
            deleted: false,
            fingerprint: "1-1".to_string(),
            blake3: None,
            size: 1,
            local_version: Some(Ulid::from_bytes([5; 16])),
            queued_at_ms: 1,
            state,
        }
    }

    fn failed() -> UploadState {
        UploadState::Failed {
            reason: "down".to_string(),
            retryable: true,
        }
    }

    fn due() -> UploadState {
        UploadState::Pending {
            due_at_ms: 7,
            attempts: 0,
            last_error: Some("down".to_string()),
        }
    }

    fn stored(upload: &SyncUpload) -> IoResult {
        IoResult::Values(vec![Some(upload.to_bytes().unwrap().into())])
    }

    // A row still in the read state is written under the new one in the transaction.
    #[test]
    fn stores_unchanged_row() {
        let row = upload(failed());
        let mut store = StoreUploadOperation::new(row.clone(), due(), true);
        assert!(store.transaction());
        let read = (
            SYNC_UPLOAD_KEYSPACE.to_string(),
            base_key(row.folder_id, &row.relative),
        );
        assert_eq!(store.first(), Ok(Next::Call(IoCall::Read(vec![read]))));
        let written = upload_entry(&upload(due())).unwrap();
        let write = Next::Call(IoCall::Write(vec![written.clone()]));
        assert_eq!(store.next(stored(&row)), Ok(write));
        assert_eq!(store.next(IoResult::Done), Ok(Next::Done(true)));
        let mut unguarded = StoreUploadOperation::new(row, due(), false);
        assert_eq!(
            unguarded.first(),
            Ok(Next::Call(IoCall::Write(vec![written])))
        );
    }

    // A row that moved on is left alone; a result that answers no call fails.
    #[test]
    fn moved_row_kept() {
        let mut store = StoreUploadOperation::new(upload(failed()), due(), true);
        store.first().unwrap();
        assert_eq!(store.next(stored(&upload(due()))), Ok(Next::Done(false)));
        let mut store = StoreUploadOperation::new(upload(failed()), due(), true);
        store.first().unwrap();
        assert!(store.next(IoResult::Woke).is_err());
        assert_eq!(requeued(&failed(), 7), Some(due()));
    }

    #[test]
    fn keys_folder_paths() {
        // One folder's rows must be a single prefix, and the path must come
        // back out of the key so a scan never needs a second lookup.
        let folder_id = Ulid::from_bytes([4u8; 16]);
        let key = base_key(folder_id, "a/b.txt");
        assert!(key.as_ref().starts_with(&folder_id.to_bytes()));
        assert_eq!(key_path(key.as_ref()).as_deref(), Some("a/b.txt"));
        assert_eq!(key_path(&[0u8; 4]), None);
    }
}
