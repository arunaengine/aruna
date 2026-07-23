use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE;
use aruna_core::structs::{JobId, RoCrateUploadRecord};
use aruna_core::types::{TxnId, UserId};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::jobs::JOB_MUTATE_MAX_ATTEMPTS;

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
    if record.expires_at_ms <= now_ms {
        return Err(UploadClaimError::Expired);
    }
    match record.claimed_by {
        Some(existing) if existing != job_id => return Err(UploadClaimError::AlreadyClaimed),
        Some(_) => return Ok(record),
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
