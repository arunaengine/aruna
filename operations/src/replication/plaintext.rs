//! Plaintext copy requests of encrypted sources and the key holder check they need. A request
//! counts only while its requester is a current key holder of the source bucket (D30).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::jobs::key_wake::read_row;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::bucket::key::rows::{authority_read, parse_authority};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, PLAINTEXT_COPY_KEYSPACE,
};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketHolder, HolderOrigin};
use aruna_core::types::Key;
use ulid::Ulid;

/// Row of the plaintext request of an explicit copy job.
pub fn job_consent(job_key: &[u8]) -> Vec<u8> {
    [b"j", job_key].concat()
}

/// Row of the plaintext request of a sync relationship.
pub fn relationship_consent(id: Ulid) -> Vec<u8> {
    [&b"r"[..], &id.to_bytes()].concat()
}

/// The write of a plaintext request by `requester`.
pub fn consent_write(row: Vec<u8>, requester: UserId) -> Result<Effect, String> {
    let value = postcard::to_allocvec(&requester).map_err(|error| error.to_string())?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: PLAINTEXT_COPY_KEYSPACE.to_string(),
        key: Key::from(row),
        value: value.into(),
        txn_id: None,
    }))
}

pub fn consent_delete(row: Vec<u8>) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: PLAINTEXT_COPY_KEYSPACE.to_string(),
        key: Key::from(row),
        txn_id: None,
    })
}

/// Runs a consent write or delete.
pub async fn store_consent(context: &DriverContext, effect: Effect) -> Result<(), String> {
    let Effect::Storage(effect) = effect else {
        return Err("not a storage effect".to_string());
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. }) => {
            Ok(())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

/// Who asked for a plaintext copy in `row`, if anyone did.
pub async fn read_consent(context: &DriverContext, row: Vec<u8>) -> Result<Option<UserId>, String> {
    let value = read_row(&context.storage_handle, PLAINTEXT_COPY_KEYSPACE, row).await?;
    value
        .map(|value| postcard::from_bytes(&value).map_err(|error| error.to_string()))
        .transpose()
}

/// Whether `bucket` on this node encrypts its objects.
pub async fn source_encrypted(context: &DriverContext, bucket: &str) -> Result<bool, String> {
    let row = bucket.as_bytes().to_vec();
    let settings = read_row(&context.storage_handle, BUCKET_ENCRYPTION_KEYSPACE, row).await?;
    BucketEncryption::from_row(settings.as_deref())
        .map(|settings| settings.is_encrypted())
        .map_err(|error| error.to_string())
}

/// Whether `user` holds the key of `bucket` now: its creator, a current group admin, or an
/// explicit holder. Losing the admin role ends it at once (D30).
pub async fn is_holder(
    context: &DriverContext,
    bucket: &str,
    user: UserId,
) -> Result<bool, String> {
    let info = drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| error.to_string())?;
    let (realm_id, group_id) = (user.realm_id, info.group_id);
    let read = authority_read(bucket, realm_id, group_id, None);
    let Effect::Storage(read) = read else {
        return Err("not a storage read".to_string());
    };
    let values = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected storage event: {other:?}")),
    };
    let state = parse_authority(values, realm_id, group_id).map_err(|error| error.to_string())?;
    if state.info.created_by == user || state.admins.contains(&user) {
        return Ok(true);
    }
    let Some(bucket_id) = state.settings.bucket_id else {
        return Ok(false);
    };
    let row = [&bucket_id.to_bytes()[..], &user.to_storage_key()].concat();
    let grant = read_row(&context.storage_handle, BUCKET_HOLDER_KEYSPACE, row).await?;
    let grant = grant
        .map(|value| BucketHolder::from_bytes(&value))
        .transpose();
    let grant = grant.map_err(|error| error.to_string())?;
    Ok(grant.is_some_and(|grant| grant.origin == HolderOrigin::Explicit))
}

/// Whether a job of `requester` may copy encrypted items of `bucket` as plaintext now: the
/// request in `row` must be theirs, and they must still hold the bucket key.
pub async fn plaintext_allowed(
    context: &DriverContext,
    bucket: &str,
    requester: UserId,
    row: Vec<u8>,
) -> Result<bool, String> {
    if read_consent(context, row).await? != Some(requester) {
        return Ok(false);
    }
    is_holder(context, bucket, requester).await
}
