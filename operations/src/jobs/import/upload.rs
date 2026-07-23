use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{HiddenBlobKey, JobId, RoCrateMediaType, RoCrateUploadRecord};
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_storage::StorageHandle;
use bytes::Bytes;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::jobs::JOB_MUTATE_MAX_ATTEMPTS;

#[derive(Debug, PartialEq)]
pub struct CreateRoCrateUploadConfig {
    pub upload_id: Ulid,
    pub owner: UserId,
    pub media_type: RoCrateMediaType,
    pub expires_at_ms: u64,
    pub max_bytes: u64,
    pub blob: BackendStream<Result<Bytes, StreamError>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateRoCrateUploadError {
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("upload record is invalid: {0}")]
    Invalid(String),
    #[error("RO-Crate upload creation was aborted")]
    Aborted,
    #[error("RO-Crate upload creation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: &'static str,
        expected: &'static str,
        got: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateUploadState {
    Init,
    Spool,
    Write,
    Cleanup,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct CreateRoCrateUploadOperation {
    upload_id: Ulid,
    owner: UserId,
    media_type: RoCrateMediaType,
    expires_at_ms: u64,
    max_bytes: u64,
    blob: Option<BackendStream<Result<Bytes, StreamError>>>,
    record: Option<RoCrateUploadRecord>,
    hidden_key: Option<HiddenBlobKey>,
    pending_error: Option<CreateRoCrateUploadError>,
    output: Option<Result<RoCrateUploadRecord, CreateRoCrateUploadError>>,
    state: CreateUploadState,
}

impl CreateRoCrateUploadOperation {
    pub fn new(config: CreateRoCrateUploadConfig) -> Self {
        Self {
            upload_id: config.upload_id,
            owner: config.owner,
            media_type: config.media_type,
            expires_at_ms: config.expires_at_ms,
            max_bytes: config.max_bytes,
            blob: Some(config.blob),
            record: None,
            hidden_key: None,
            pending_error: None,
            output: None,
            state: CreateUploadState::Init,
        }
    }

    fn fail(&mut self, error: CreateRoCrateUploadError) -> Effects {
        self.output = Some(Err(error));
        self.state = CreateUploadState::Error;
        smallvec![]
    }

    fn cleanup_effect(&self) -> Effects {
        self.hidden_key
            .clone()
            .map(|key| smallvec![Effect::Blob(BlobEffect::DeleteHidden { key })])
            .unwrap_or_default()
    }

    fn fail_with_cleanup(&mut self, error: CreateRoCrateUploadError) -> Effects {
        let effects = self.cleanup_effect();
        if effects.is_empty() {
            return self.fail(error);
        }
        self.pending_error = Some(error);
        self.state = CreateUploadState::Cleanup;
        effects
    }

    fn unexpected_event(&mut self, expected: &'static str, event: Event) -> Effects {
        let error = CreateRoCrateUploadError::UnexpectedEvent {
            state: self.state_name(),
            expected,
            got: format!("{event:?}"),
        };
        if self.hidden_key.is_some() {
            self.fail_with_cleanup(error)
        } else {
            self.fail(error)
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            CreateUploadState::Init => "init",
            CreateUploadState::Spool => "spool",
            CreateUploadState::Write => "write",
            CreateUploadState::Cleanup => "cleanup",
            CreateUploadState::Finish => "finish",
            CreateUploadState::Error => "error",
        }
    }

    fn handle_spool(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }) => {
                let hidden_key = match HiddenBlobKey::try_from(&location) {
                    Ok(key) => key,
                    Err(error) => return self.fail(BlobError::from(error).into()),
                };
                let record = RoCrateUploadRecord {
                    upload_id: self.upload_id,
                    owner: self.owner,
                    location,
                    blake3,
                    size,
                    media_type: self.media_type,
                    expires_at_ms: self.expires_at_ms,
                    claimed_by: None,
                };
                let value = match postcard::to_allocvec(&record) {
                    Ok(value) => ByteView::from(value),
                    Err(error) => {
                        self.hidden_key = Some(hidden_key);
                        return self.fail_with_cleanup(CreateRoCrateUploadError::Invalid(
                            error.to_string(),
                        ));
                    }
                };
                self.record = Some(record);
                self.hidden_key = Some(hidden_key);
                self.state = CreateUploadState::Write;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
                    key: upload_key(self.upload_id),
                    value,
                    txn_id: None,
                })]
            }
            Event::Blob(BlobEvent::Error(error)) => self.fail(error.into()),
            event => self.unexpected_event(
                "Event::Blob(BlobEvent::HiddenSpooled | BlobEvent::Error)",
                event,
            ),
        }
    }

    fn handle_write(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                let Some(record) = self.record.take() else {
                    return self.fail_with_cleanup(CreateRoCrateUploadError::NotFinished);
                };
                self.hidden_key = None;
                self.output = Some(Ok(record));
                self.state = CreateUploadState::Finish;
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail_with_cleanup(error.into()),
            event => self.unexpected_event(
                "Event::Storage(StorageEvent::WriteResult | StorageEvent::Error)",
                event,
            ),
        }
    }

    fn handle_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::HiddenDeleted | BlobEvent::Error(_)) => {
                self.emit_pending_error()
            }
            event => {
                self.hidden_key = None;
                self.unexpected_event(
                    "Event::Blob(BlobEvent::HiddenDeleted | BlobEvent::Error)",
                    event,
                )
            }
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let error = self
            .pending_error
            .take()
            .unwrap_or(CreateRoCrateUploadError::Aborted);
        self.hidden_key = None;
        self.fail(error)
    }
}

impl Operation for CreateRoCrateUploadOperation {
    type Output = RoCrateUploadRecord;
    type Error = CreateRoCrateUploadError;

    fn start(&mut self) -> Effects {
        let Some(blob) = self.blob.take() else {
            return self.fail(CreateRoCrateUploadError::NotFinished);
        };
        self.state = CreateUploadState::Spool;
        smallvec![Effect::Blob(BlobEffect::SpoolHidden {
            namespace: self.upload_id,
            name: "input".to_string(),
            created_by: self.owner,
            max_bytes: Some(self.max_bytes),
            blob,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateUploadState::Spool => self.handle_spool(event),
            CreateUploadState::Write => self.handle_write(event),
            CreateUploadState::Cleanup => self.handle_cleanup(event),
            CreateUploadState::Init | CreateUploadState::Finish | CreateUploadState::Error => {
                self.unexpected_event("no event", event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateUploadState::Finish | CreateUploadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateRoCrateUploadError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        if self.state == CreateUploadState::Cleanup {
            return self.cleanup_effect();
        }
        if self.is_complete() {
            return smallvec![];
        }
        if self.hidden_key.is_some() {
            self.fail_with_cleanup(CreateRoCrateUploadError::Aborted)
        } else {
            self.fail(CreateRoCrateUploadError::Aborted)
        }
    }
}

#[derive(Debug, Error)]
pub enum UploadClaimError {
    #[error("upload not found")]
    NotFound,
    #[error("upload is owned by another user")]
    WrongOwner,
    #[error("upload expired")]
    Expired,
    #[error("upload is already claimed")]
    AlreadyClaimed,
    #[error("upload record is invalid: {0}")]
    Invalid(String),
    #[error("upload storage failed: {0}")]
    Storage(String),
}

pub async fn read_rocrate_upload(
    storage: &StorageHandle,
    upload_id: Ulid,
) -> Result<Option<RoCrateUploadRecord>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: upload_key(upload_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload read event: {other:?}")),
    }
}

pub async fn load_rocrate_upload(
    context: &DriverContext,
    upload_id: Ulid,
) -> Result<Option<RoCrateUploadRecord>, String> {
    read_rocrate_upload(&context.storage_handle, upload_id).await
}

pub async fn write_rocrate_upload(
    storage: &StorageHandle,
    record: &RoCrateUploadRecord,
) -> Result<(), String> {
    let value = postcard::to_allocvec(record).map_err(|error| error.to_string())?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: upload_key(record.upload_id),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload write event: {other:?}")),
    }
}

pub async fn claim_rocrate_upload(
    storage: &StorageHandle,
    upload_id: Ulid,
    owner: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<RoCrateUploadRecord, UploadClaimError> {
    for attempt in 0..JOB_MUTATE_MAX_ATTEMPTS {
        let txn_id = start_txn(storage)
            .await
            .map_err(UploadClaimError::Storage)?;
        let result = claim_in_txn(storage, txn_id, upload_id, owner, job_id, now_ms).await;
        let record = match result {
            Ok(record) => record,
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(error);
            }
        };
        match commit_txn(storage, txn_id).await {
            CommitResult::Committed => return Ok(record),
            CommitResult::Conflict if attempt + 1 < JOB_MUTATE_MAX_ATTEMPTS => continue,
            CommitResult::Conflict => {
                return Err(UploadClaimError::Storage(
                    "upload claim exhausted conflict retries".to_string(),
                ));
            }
            CommitResult::Failed(error) => return Err(UploadClaimError::Storage(error)),
        }
    }
    Err(UploadClaimError::Storage(
        "upload claim exhausted conflict retries".to_string(),
    ))
}

pub async fn delete_rocrate_upload(
    storage: &StorageHandle,
    upload_id: Ulid,
    claimed_by: JobId,
) -> Result<(), String> {
    let Some(record) = read_rocrate_upload(storage, upload_id).await? else {
        return Ok(());
    };
    if record.claimed_by != Some(claimed_by) {
        return Err("upload cleanup claim does not match the job".to_string());
    }
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: upload_key(upload_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload delete event: {other:?}")),
    }
}

async fn claim_in_txn(
    storage: &StorageHandle,
    txn_id: TxnId,
    upload_id: Ulid,
    owner: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<RoCrateUploadRecord, UploadClaimError> {
    let value = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: upload_key(upload_id),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => value,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            return Err(UploadClaimError::NotFound);
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(UploadClaimError::Storage(error.to_string()));
        }
        other => {
            return Err(UploadClaimError::Storage(format!(
                "unexpected upload claim read event: {other:?}"
            )));
        }
    };
    let mut record: RoCrateUploadRecord = postcard::from_bytes(value.as_ref())
        .map_err(|error| UploadClaimError::Invalid(error.to_string()))?;
    if record.owner != owner {
        return Err(UploadClaimError::WrongOwner);
    }
    match record.claimed_by {
        Some(existing) if existing != job_id => return Err(UploadClaimError::AlreadyClaimed),
        Some(_) => return Ok(record),
        None if record.expires_at_ms <= now_ms => return Err(UploadClaimError::Expired),
        None => record.claimed_by = Some(job_id),
    }
    let value = postcard::to_allocvec(&record)
        .map(ByteView::from)
        .map_err(|error| UploadClaimError::Invalid(error.to_string()))?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: upload_key(upload_id),
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(record),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(UploadClaimError::Storage(error.to_string()))
        }
        other => Err(UploadClaimError::Storage(format!(
            "unexpected upload claim write event: {other:?}"
        ))),
    }
}

async fn start_txn(storage: &StorageHandle) -> Result<TxnId, String> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload transaction event: {other:?}")),
    }
}

enum CommitResult {
    Committed,
    Conflict,
    Failed(String),
}

async fn commit_txn(storage: &StorageHandle, txn_id: TxnId) -> CommitResult {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => CommitResult::Committed,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => CommitResult::Conflict,
        Event::Storage(StorageEvent::Error { error }) => CommitResult::Failed(error.to_string()),
        other => CommitResult::Failed(format!("unexpected upload commit event: {other:?}")),
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

fn upload_key(upload_id: Ulid) -> ByteView {
    ByteView::from(upload_id.to_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{BackendLocation, RealmId, RoCrateMediaType};
    use aruna_storage::FjallStorage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn upload_operation() -> (CreateRoCrateUploadOperation, BackendLocation) {
        let owner = UserId::nil(RealmId::from_bytes([1u8; 32]));
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let location = BackendLocation {
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: format!("_jobs/{upload_id}/input"),
            ulid: Ulid::from_bytes([3u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: owner,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 7,
            hashes: HashMap::new(),
        };
        let operation = CreateRoCrateUploadOperation::new(CreateRoCrateUploadConfig {
            upload_id,
            owner,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms: 20,
            max_bytes: 10,
            blob: BackendStream::new(
                futures_util::stream::empty::<Result<Bytes, std::io::Error>>(),
            ),
        });
        (operation, location)
    }

    #[test]
    fn creates_upload() {
        let (mut operation, location) = upload_operation();
        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Blob(BlobEffect::SpoolHidden { .. })]
        ));

        let effects = operation.step(Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3: [4u8; 32],
            size: 7,
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected upload record write")
        };
        let record: RoCrateUploadRecord = postcard::from_bytes(value.as_ref()).unwrap();

        assert!(
            operation
                .step(Event::Storage(StorageEvent::WriteResult {
                    key: upload_key(record.upload_id),
                }))
                .is_empty()
        );
        assert_eq!(operation.finalize().unwrap(), record);
    }

    #[test]
    fn cleans_storage_error() {
        let (mut operation, location) = upload_operation();
        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Blob(BlobEffect::SpoolHidden { .. })]
        ));
        assert!(matches!(
            operation
                .step(Event::Blob(BlobEvent::HiddenSpooled {
                    location: location.clone(),
                    blake3: [4u8; 32],
                    size: 7,
                }))
                .as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DeleteHidden { key })]
                if key == &HiddenBlobKey::try_from(&location).unwrap()
        ));
        assert!(
            operation
                .step(Event::Blob(BlobEvent::HiddenDeleted))
                .is_empty()
        );
        assert_eq!(
            operation.finalize(),
            Err(CreateRoCrateUploadError::Storage(StorageError::WriteError))
        );
    }

    #[tokio::test]
    async fn reclaims_expired_upload() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let owner = UserId::nil(realm_id);
        let upload_id = Ulid::from_bytes([2u8; 16]);
        let job_id = JobId::from_bytes([3u8; 16]);
        let record = RoCrateUploadRecord {
            upload_id,
            owner,
            location: BackendLocation {
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: "_jobs/input".to_string(),
                ulid: Ulid::from_bytes([4u8; 16]),
                compressed: false,
                encrypted: false,
                created_by: owner,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: HashMap::new(),
            },
            blake3: [5u8; 32],
            size: 1,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms: 10,
            claimed_by: None,
        };
        write_rocrate_upload(&storage, &record).await.unwrap();
        claim_rocrate_upload(&storage, upload_id, owner, job_id, 9)
            .await
            .unwrap();

        let reclaimed = claim_rocrate_upload(&storage, upload_id, owner, job_id, 11)
            .await
            .unwrap();

        assert_eq!(reclaimed.claimed_by, Some(job_id));
        assert!(matches!(
            claim_rocrate_upload(&storage, upload_id, owner, JobId::from_bytes([6u8; 16]), 11)
                .await,
            Err(UploadClaimError::AlreadyClaimed)
        ));
    }
}
