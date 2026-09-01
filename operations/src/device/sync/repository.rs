//! Storage shape of the device-local synced folders.

use std::sync::Arc;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    SYNC_ACTION_LOG_KEYSPACE, SYNC_BASE_KEYSPACE, SYNC_UPLOAD_OUTBOX_KEYSPACE,
    SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::structs::{SyncActionRecord, SyncBase, SyncedFolder};
use aruna_core::types::{Key, TxnId, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Folders one device may bind. A device serves one person's machine, so this
/// is a human-sized list rather than an inventory.
pub const MAX_SYNCED_FOLDERS: usize = 64;

/// Rows one scan reads at a time.
pub const SYNC_PAGE_SIZE: usize = 256;

/// Files one reconcile pass hashes. The strong hash is only needed where the
/// weak fingerprint already says the file moved, so a large first bind
/// converges over several passes instead of blocking one.
pub const MAX_HASH_BATCH: usize = 256;

/// Pull attempts before a still-retryable upload parks for the owner.
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
}

impl SyncUpload {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn is_due(&self, now_ms: u64) -> bool {
        match &self.state {
            UploadState::Pending { due_at_ms, .. } => *due_at_ms <= now_ms,
            UploadState::Failed { .. } => false,
        }
    }

    pub fn attempts(&self) -> u32 {
        match &self.state {
            UploadState::Pending { attempts, .. } => *attempts,
            UploadState::Failed { .. } => 0,
        }
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

pub fn folder_prefix(folder_id: Ulid) -> Key {
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
        SYNC_UPLOAD_OUTBOX_KEYSPACE.to_string(),
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
        SYNC_ACTION_LOG_KEYSPACE.to_string(),
        ByteView::from(key),
        ByteView::from(record.to_bytes()?),
    ))
}

pub fn read_folder(folder_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: SYNCED_FOLDER_KEYSPACE.to_string(),
        key: folder_key(folder_id),
        txn_id,
    })
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

/// Reads one row, answering `None` for both an absent row and a failed read:
/// every caller here treats an unreadable row as work for the next pass.
pub(crate) async fn read_value(
    context: &Arc<crate::driver::DriverContext>,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Option<Value> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        _ => None,
    }
}

pub(crate) async fn write_rows(
    context: &Arc<crate::driver::DriverContext>,
    writes: Vec<(String, Key, Value)>,
    txn_id: Option<TxnId>,
) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite { writes, txn_id })
            .await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    )
}

pub(crate) async fn delete_rows(
    context: &Arc<crate::driver::DriverContext>,
    deletes: Vec<(String, Key)>,
    txn_id: Option<TxnId>,
) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchDelete { deletes, txn_id })
            .await,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    )
}

/// One page of a keyspace, with the cursor of the next page. `None` means the
/// scan itself failed.
pub(crate) async fn scan_page(
    context: &Arc<crate::driver::DriverContext>,
    effect: Effect,
) -> Option<(Vec<(Key, Value)>, Option<Key>)> {
    let Effect::Storage(effect) = effect else {
        return None;
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Some((values, next_start_after)),
        _ => None,
    }
}

pub(crate) async fn start_txn(context: &Arc<crate::driver::DriverContext>) -> Option<TxnId> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Some(txn_id),
        _ => None,
    }
}

pub(crate) async fn commit_txn(context: &Arc<crate::driver::DriverContext>, txn_id: TxnId) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    )
}

pub(crate) async fn abort_txn(context: &Arc<crate::driver::DriverContext>, txn_id: TxnId) {
    context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

#[cfg(test)]
mod tests {
    use super::{base_key, key_path};
    use ulid::Ulid;

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
