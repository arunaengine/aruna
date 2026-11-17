//! Holds the storage shape and row encodings of the device-local synced folder state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect, abort_effects};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    SYNC_BASE_KEYSPACE, SYNC_LOG_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncActionRecord, SyncBase, SyncedFolder};
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::device::retry::RetryView;
use crate::device::scan_rows::{ScanRowsError, ScanRowsOperation};

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
    #[error("unexpected {0} while storing an upload row")]
    UnexpectedEvent(&'static str),
    #[error("storing an upload row did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreStep {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Finish,
}

/// Writes one upload row under a new state in one transaction and outputs whether it committed.
/// `guarded` refuses the write when the stored row no longer carries the state this pass read.
#[derive(Debug, PartialEq)]
pub struct StoreUploadOperation {
    upload: SyncUpload,
    next: UploadState,
    guarded: bool,
    step: StoreStep,
    txn_id: Option<TxnId>,
    output: Option<Result<bool, StoreUploadError>>,
}

impl StoreUploadOperation {
    pub fn new(upload: SyncUpload, next: UploadState, guarded: bool) -> Self {
        Self {
            upload,
            next,
            guarded,
            step: StoreStep::Init,
            txn_id: None,
            output: None,
        }
    }

    /// Ends with `output`, aborting the transaction first when one is open.
    fn end(&mut self, output: Result<bool, StoreUploadError>) -> Effects {
        self.output = Some(output);
        let abort = abort_effects(&mut self.txn_id);
        self.step = match abort.is_empty() {
            true => StoreStep::Finish,
            false => StoreStep::Abort,
        };
        abort
    }

    fn write(&mut self) -> Effects {
        let updated = SyncUpload {
            state: self.next.clone(),
            ..self.upload.clone()
        };
        let Ok(row) = upload_entry(&updated) else {
            return self.end(Ok(false));
        };
        self.step = StoreStep::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![row],
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for StoreUploadOperation {
    type Output = bool;
    type Error = StoreUploadError;

    fn start(&mut self) -> Effects {
        self.step = StoreStep::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (StoreStep::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                if !self.guarded {
                    return self.write();
                }
                self.step = StoreStep::Read;
                let key = base_key(self.upload.folder_id, &self.upload.relative);
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: SYNC_UPLOAD_KEYSPACE.to_string(),
                    key,
                    txn_id: Some(txn_id),
                })]
            }
            (StoreStep::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let stored = value.and_then(|bytes| SyncUpload::from_bytes(&bytes).ok());
                let holds = stored.is_some_and(|stored| {
                    stored.state == self.upload.state && same_version(&stored, &self.upload)
                });
                match holds {
                    true => self.write(),
                    false => self.end(Ok(false)),
                }
            }
            (StoreStep::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id.take() else {
                    return self.end(Ok(false));
                };
                self.step = StoreStep::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (StoreStep::Commit, event) => {
                let committed = matches!(
                    event,
                    Event::Storage(StorageEvent::TransactionCommitted { .. })
                );
                self.end(Ok(committed))
            }
            (StoreStep::Abort, _) => {
                self.step = StoreStep::Finish;
                smallvec![]
            }
            (
                StoreStep::Start | StoreStep::Read | StoreStep::Write,
                Event::Storage(StorageEvent::Error { .. }),
            ) => self.end(Ok(false)),
            (_, event) => self.end(Err(StoreUploadError::UnexpectedEvent(event.label()))),
        }
    }

    fn is_complete(&self) -> bool {
        self.step == StoreStep::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(StoreUploadError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

/// Makes every backed-off upload due now and re-arms retryable failures at `now_ms`, each only
/// while its row still carries the state the scan read. Outputs how many moved.
#[derive(Debug, PartialEq)]
pub struct RequeueUploadsOperation {
    now_ms: u64,
    scan: Option<ScanRowsOperation>,
    /// Uploads still to move with their next state, the next one last.
    pending: Vec<(SyncUpload, UploadState)>,
    store: Option<StoreUploadOperation>,
    requeued: usize,
    output: Option<Result<usize, ScanRowsError>>,
}

impl RequeueUploadsOperation {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            scan: None,
            pending: Vec::new(),
            store: None,
            requeued: 0,
            output: None,
        }
    }

    fn next_store(&mut self) -> Effects {
        let Some((upload, state)) = self.pending.pop() else {
            self.output = Some(Ok(self.requeued));
            return smallvec![];
        };
        let mut store = StoreUploadOperation::new(upload, state, true);
        let effects = store.start();
        self.store = Some(store);
        effects
    }
}

impl Operation for RequeueUploadsOperation {
    type Output = usize;
    type Error = ScanRowsError;

    fn start(&mut self) -> Effects {
        let mut scan = ScanRowsOperation::new(SYNC_UPLOAD_KEYSPACE, None, SYNC_PAGE_SIZE);
        let effects = scan.start();
        self.scan = Some(scan);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Some(mut store) = self.store.take() {
            let effects = store.step(event);
            if !store.is_complete() {
                self.store = Some(store);
                return effects;
            }
            if store.finalize() == Ok(true) {
                self.requeued += 1;
            }
            return self.next_store();
        }
        let Some(mut scan) = self.scan.take() else {
            self.output = Some(Err(ScanRowsError::UnexpectedEvent(event.label())));
            return smallvec![];
        };
        let effects = scan.step(event);
        if !scan.is_complete() {
            self.scan = Some(scan);
            return effects;
        }
        let rows = match scan.finalize() {
            Ok(rows) => rows,
            Err(error) => {
                self.output = Some(Err(error));
                return smallvec![];
            }
        };
        let uploads = rows
            .iter()
            .filter_map(|bytes| SyncUpload::from_bytes(bytes).ok());
        let now_ms = self.now_ms;
        self.pending = uploads
            .filter_map(|upload| Some((requeued(&upload.state, now_ms)?, upload)))
            .map(|(state, upload)| (upload, state))
            .collect();
        self.pending.reverse();
        self.next_store()
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ScanRowsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.store {
            Some(store) => store.abort(),
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        RequeueUploadsOperation, SYNC_PAGE_SIZE, StoreUploadError, StoreUploadOperation,
        SyncUpload, UploadState, base_key, key_path, upload_entry,
    };
    use crate::device::scan_rows::ScanRowsOperation;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::SYNC_UPLOAD_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

    const TXN: Ulid = Ulid::from_bytes([9; 16]);

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

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn stored(upload: &SyncUpload) -> Event {
        storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(upload.to_bytes().unwrap().into()),
        })
    }

    // A row still in the read state is written under the new one in one transaction.
    #[test]
    fn stores_unchanged_row() {
        let row = upload(failed());
        let written = upload_entry(&upload(due())).unwrap();
        let mut store = StoreUploadOperation::new(row.clone(), due(), true);
        check_trace(
            &mut store,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![Effect::Storage(StorageEffect::Read {
                        key_space: SYNC_UPLOAD_KEYSPACE.to_string(),
                        key: base_key(row.folder_id, &row.relative),
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    stored(&row),
                    vec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: vec![written],
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(store.finalize(), Ok(true));
    }

    // A row that moved on is left alone and the transaction aborted; other events are refused.
    #[test]
    fn moved_row_kept() {
        let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN });
        let mut store = StoreUploadOperation::new(upload(failed()), due(), true);
        store.start();
        store.step(storage(StorageEvent::TransactionStarted { txn_id: TXN }));
        assert_eq!(
            store.step(stored(&upload(due()))).into_vec(),
            vec![abort.clone()]
        );
        store.step(storage(StorageEvent::TransactionAborted { txn_id: TXN }));
        assert_eq!(store.finalize(), Ok(false));
        let mut open = StoreUploadOperation::new(upload(failed()), due(), true);
        open.start();
        open.step(storage(StorageEvent::TransactionStarted { txn_id: TXN }));
        check_abort(&mut open, &[abort]);

        let fresh = || StoreUploadOperation::new(upload(failed()), due(), true);
        let refused = rejects_events(fresh, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        assert!(matches!(refused[0].2, StoreUploadError::UnexpectedEvent(_)));
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }

    // A retryable failure is stored again as due; a node without uploads moves nothing.
    #[test]
    fn requeues_failed_uploads() {
        let (key, value) = {
            let (_, key, value) = upload_entry(&upload(failed())).unwrap();
            (key, value)
        };
        let page = |values| {
            storage(StorageEvent::IterResult {
                values,
                next_start_after: None,
            })
        };
        let scan = ScanRowsOperation::new(SYNC_UPLOAD_KEYSPACE, None, SYNC_PAGE_SIZE).start();
        let store = StoreUploadOperation::new(upload(failed()), due(), true).start();
        let mut requeue = RequeueUploadsOperation::new(7);
        check_trace(
            &mut requeue,
            &scan,
            vec![(page(vec![(key, value)]), store.into_vec())],
        );
        assert!(!requeue.is_complete());

        let mut requeue = RequeueUploadsOperation::new(7);
        requeue.start();
        assert!(requeue.step(page(Vec::new())).is_empty());
        assert_eq!(requeue.finalize(), Ok(0));
        let fresh = || RequeueUploadsOperation::new(7);
        let refused = rejects_events(fresh, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        rejects_finalize(fresh());
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
