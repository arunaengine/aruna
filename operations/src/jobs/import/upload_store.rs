//! Reads, writes, claims and deletes the stored RO-Crate upload records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE;
use aruna_core::structs::execution::job::{JobId, RoCrateUploadRecord};
use aruna_core::types::TxnId;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use ulid::Ulid;

use super::upload::{UploadClaimError, claim_upload, upload_key};
use crate::driver::DriverContext;
use crate::jobs::MUTATE_MAX_ATTEMPTS;
use crate::jobs::store::query::{CommitStep, commit_write};

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
        other => Err(format!("unexpected upload read event: {}", other.label())),
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
        other => Err(format!("unexpected upload write event: {}", other.label())),
    }
}

pub async fn claim_rocrate_upload(
    storage: &StorageHandle,
    upload_id: Ulid,
    owner: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<RoCrateUploadRecord, UploadClaimError> {
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
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
        match commit_write(
            storage,
            txn_id,
            attempt,
            "upload claim exhausted conflict retries",
        )
        .await
        .map_err(|error| UploadClaimError::Storage(error.to_string()))?
        {
            CommitStep::Committed => return Ok(record),
            CommitStep::Retry => continue,
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
        other => Err(format!("unexpected upload delete event: {}", other.label())),
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
                "unexpected upload claim read event: {}",
                other.label()
            )));
        }
    };
    let mut record: RoCrateUploadRecord = postcard::from_bytes(value.as_ref())
        .map_err(|error| UploadClaimError::Invalid(error.to_string()))?;
    if !claim_upload(&mut record, owner, job_id, now_ms)? {
        return Ok(record);
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
            "unexpected upload claim write event: {}",
            other.label()
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
        other => Err(format!(
            "unexpected upload transaction event: {}",
            other.label()
        )),
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}
