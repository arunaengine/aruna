//! Reads, writes, claims and deletes the stored RO-Crate upload records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ROCRATE_UPLOAD_KEYSPACE;
use aruna_core::structs::execution::job::{JobId, RoCrateUploadRecord};
use byteview::ByteView;
use ulid::Ulid;

use super::upload::{ClaimUploadOperation, UploadClaimError};
use crate::driver::{DriverContext, drive_retry};
use aruna_core::structs::execution::job::upload_key;

pub async fn read_rocrate_upload(
    context: &DriverContext,
    upload_id: Ulid,
) -> Result<Option<RoCrateUploadRecord>, String> {
    match context
        .storage_handle
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
    read_rocrate_upload(context, upload_id).await
}

pub async fn write_rocrate_upload(
    context: &DriverContext,
    record: &RoCrateUploadRecord,
) -> Result<(), String> {
    let value = postcard::to_allocvec(record).map_err(|error| error.to_string())?;
    match context
        .storage_handle
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
    context: &DriverContext,
    upload_id: Ulid,
    owner: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<RoCrateUploadRecord, UploadClaimError> {
    let build = || ClaimUploadOperation::new(upload_id, owner, job_id, now_ms);
    drive_retry(build, context, &upload_id.to_bytes()).await
}

pub async fn delete_rocrate_upload(
    context: &DriverContext,
    upload_id: Ulid,
    claimed_by: JobId,
) -> Result<(), String> {
    let Some(record) = read_rocrate_upload(context, upload_id).await? else {
        return Ok(());
    };
    if record.claimed_by != Some(claimed_by) {
        return Err("upload cleanup claim does not match the job".to_string());
    }
    match context
        .storage_handle
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
