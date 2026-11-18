//! Promotes pending archives after a key unlock and drops received archives whose hash was rejected.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_QUARANTINE_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PENDING_CLAIM_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobQuarantineRecord, BlobVersion, BucketInfo,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::Value;

use super::promote::{
    PROMOTE_PAGE, PromoteError, PromotePendingOperation, Promotion, RejectOwnersOperation,
};

/// Promotes every pending archive sealed with `key`, one page of pending rows at a time.
/// Call after `key` is installed; archives of other keys are skipped.
pub async fn promote_unlocked(
    context: &crate::driver::DriverContext,
    key: BucketKeyRef,
    origin: (RealmId, NodeId),
    limits: &RoCrateLimits,
) -> Result<usize, PromoteError> {
    let mut start_after = None;
    let mut promoted = 0;
    loop {
        let (rows, next) = crate::storage_read::iter_prefix_page(
            &context.storage_handle,
            PENDING_LOCATION_KEYSPACE,
            None,
            start_after,
            PROMOTE_PAGE,
            None,
        )
        .await
        .map_err(|error| PromoteError::Storage(StorageError::ReadError(error)))?;
        for (row, value) in &rows {
            let location = BackendLocation::from_bytes(value)?;
            if location.format.bucket_key() != Some(key) {
                continue;
            }
            let archive = ArchiveKey::from_bytes(row)?;
            let operation =
                PromotePendingOperation::new(archive.clone(), origin.0, origin.1, limits.clone());
            let outcome = match crate::driver::drive(operation, context).await {
                Err(PromoteError::Blob(BlobError::IntegrityCheckFailed(reason))) => {
                    reject_archive(context, &archive, origin, reason).await?;
                    continue;
                }
                result => result?,
            };
            match outcome {
                Promotion::Promoted { .. } => promoted += 1,
                // A lock in between leaves the rest pending until the next unlock.
                Promotion::AwaitingKey(_) => return Ok(promoted),
                Promotion::Gone => {}
                Promotion::Mismatch { claimed } => {
                    drop_mismatch(context, &archive, claimed, origin).await?;
                }
            }
        }
        match next {
            Some(next) if !rows.is_empty() => start_after = Some(next),
            _ => return Ok(promoted),
        }
    }
}

/// Quarantines an archive whose content differs from the hash its sender claimed and deletes the
/// versions that use it; none of them was registered. Returns how many versions were deleted.
pub async fn drop_mismatch(
    context: &crate::driver::DriverContext,
    archive: &ArchiveKey,
    claimed: [u8; 32],
    origin: (RealmId, NodeId),
) -> Result<usize, PromoteError> {
    let reason = "replicated content hash differs from the claimed hash".to_string();
    drop_received(context, archive, claimed, origin, reason).await
}

pub(crate) async fn reject_archive(
    context: &crate::driver::DriverContext,
    archive: &ArchiveKey,
    origin: (RealmId, NodeId),
    reason: String,
) -> Result<usize, PromoteError> {
    let claim = read_value(
        &context.storage_handle,
        PENDING_CLAIM_KEYSPACE,
        archive.to_bytes(),
    )
    .await?;
    let Some(claimed) = claim
        .as_ref()
        .and_then(|claim| <[u8; 32]>::try_from(claim.as_ref()).ok())
    else {
        return Err(BlobError::IntegrityCheckFailed(reason).into());
    };
    drop_received(context, archive, claimed, origin, reason).await
}

async fn drop_received(
    context: &crate::driver::DriverContext,
    archive: &ArchiveKey,
    claimed: [u8; 32],
    (realm_id, node_id): (RealmId, NodeId),
    reason: String,
) -> Result<usize, PromoteError> {
    use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
    let storage = &context.storage_handle;
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let record = BlobQuarantineRecord::new(claimed, archive.backend.clone(), reason, now_ms);
    let write = StorageEffect::Write {
        key_space: BLOB_QUARANTINE_KEYSPACE.to_string(),
        key: record.key().into(),
        value: record.to_bytes()?.into(),
        txn_id: None,
    };
    if let Event::Storage(StorageEvent::Error { error }) = storage.send_storage_effect(write).await
    {
        return Err(error.into());
    }
    let mut dropped = 0;
    loop {
        let fence = RejectOwnersOperation::new(archive.clone(), &record)?;
        let seed = archive.to_bytes();
        let owners = crate::driver::drive_retry(|| fence.clone(), context, &seed).await?;
        if owners.is_empty() {
            return Ok(dropped);
        }
        let before = dropped;
        for version in owners {
            let row = version.to_bytes()?;
            let Some(value) = read_value(storage, BLOB_VERSIONS_KEYSPACE, row).await? else {
                continue;
            };
            let stored = BlobVersion::from_bytes(&value)?;
            let bucket = version.bucket.as_bytes().to_vec();
            let info = read_value(storage, S3_BUCKET_KEYSPACE, bucket).await?;
            let Some(info) = info else {
                continue;
            };
            if stored.state.pending_archive() != Some(archive) {
                continue;
            }
            let input = DeleteObjectInput {
                bucket: version.bucket.clone(),
                key: version.key.clone(),
                version_id: Some(version.version_id),
                group_id: BucketInfo::from_bytes(&info)?.group_id,
                realm_id,
                node_id,
                deleted_by: stored.created_by,
            };
            match crate::driver::drive(DeleteObjectOperation::new(input), context).await {
                Ok(_) => dropped += 1,
                Err(error) => return Err(PromoteError::Dropped(error.to_string())),
            }
        }
        if dropped == before {
            return Err(PromoteError::Dropped(
                "pending owners could not be removed".into(),
            ));
        }
    }
}

pub(super) async fn read_value(
    storage: &aruna_storage::StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, PromoteError> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(PromoteError::InvalidStateEvent {
            state: "Drop",
            expected: "ReadResult",
            received: (other).label(),
        }),
    }
}
