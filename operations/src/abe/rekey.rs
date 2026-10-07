//! Gives every version under a key prefix a new object key and envelope, one page per call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::KeyError;
use crate::blob::migration::queue::quota_origin;
use crate::blob::migration::rewrite::{RewriteOutcome, RewriteVersionOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::store::iter_prefix_page;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_DUE_KEYSPACE, ABE_EPOCH_KEYSPACE, ABE_REKEY_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::storage::abe::AbeError;
use aruna_core::structs::storage::blob::{BucketInfo, VersionKey};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRecord, SealPlan};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionTarget,
};
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::StorageHandle;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Version rows one page scans for the prefix.
const SCAN: usize = 1024;

/// A bucket's unfinished re-key pass, keyed by bucket id in `abe_rekeys`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RekeyProgress {
    pub prefix: String,
    /// The epoch of the new envelopes; a raise since then starts the pass again.
    pub epoch: u64,
    /// The last version row handled.
    pub cursor: Vec<u8>,
    pub rekeyed: u64,
}

async fn read_rows(
    storage: &StorageHandle,
    reads: Vec<(String, Key)>,
    txn_id: Option<TxnId>,
) -> Result<Vec<Option<Value>>, KeyError> {
    match storage
        .send_storage_effect(StorageEffect::BatchRead { reads, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        _ => Err(KeyError::Storage),
    }
}

fn epoch_of(row: Option<&Value>) -> Option<u64> {
    Some(u64::from_be_bytes(row?.as_ref().try_into().ok()?))
}

/// Re-keys up to `page` versions under `prefix` after the saved cursor through the rotation
/// unit, then saves the cursor. Returns the progress and whether the pass is done.
pub async fn rekey_page(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
    page: usize,
) -> Result<(RekeyProgress, bool), KeyError> {
    let storage = &context.storage_handle;
    let name: Key = bucket.as_bytes().to_vec().into();
    let reads = vec![
        (BUCKET_ENCRYPTION_KEYSPACE.to_string(), name.clone()),
        (S3_BUCKET_KEYSPACE.to_string(), name),
    ];
    let [settings, info] = <[_; 2]>::try_from(read_rows(storage, reads, None).await?)
        .map_err(|_| KeyError::Storage)?;
    let settings =
        BucketEncryption::from_row(settings.as_deref()).map_err(|_| KeyError::Missing)?;
    let info = info.as_deref().map(BucketInfo::from_bytes);
    let (Some(key), Some(Ok(info))) = (settings.active_key(), info) else {
        return Err(KeyError::Missing);
    };
    let id: Key = key.bucket_id.to_bytes().to_vec().into();
    let reads = vec![
        (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
        (ABE_EPOCH_KEYSPACE.to_string(), id.clone()),
        (ABE_REKEY_KEYSPACE.to_string(), id.clone()),
    ];
    let [record, epoch, seen] = <[_; 3]>::try_from(read_rows(storage, reads, None).await?)
        .map_err(|_| KeyError::Storage)?;
    let record = record.as_deref().map(BucketKeyRecord::from_bytes);
    let plan = match record {
        Some(Ok(record)) => SealPlan::capture(&settings, &record).ok().flatten(),
        _ => None,
    };
    let (Some(plan), Some(epoch)) = (plan, epoch_of(epoch.as_ref())) else {
        return Err(KeyError::Missing);
    };
    let fresh = RekeyProgress {
        prefix: prefix.to_string(),
        epoch,
        cursor: Vec::new(),
        rekeyed: 0,
    };
    let mut progress = match seen.as_deref().map(postcard::from_bytes::<RekeyProgress>) {
        None => fresh,
        Some(Ok(saved)) if saved.prefix != prefix => return Err(KeyError::Busy),
        Some(Ok(saved)) if saved.epoch != epoch => fresh,
        Some(Ok(saved)) => saved,
        Some(Err(_)) => return Err(AbeError::Context.into()),
    };
    let target = TransitionTarget {
        compression: info.compression,
        plan: Some(plan),
    };
    let now = aruna_core::time::unix_timestamp_millis();
    let generation = settings.storage_generation;
    let unit =
        EncryptionTransition::new(TransitionKind::Rotate, Some(key), target, generation, now);
    let quota = quota_origin(context).await.map_err(|_| KeyError::Storage)?;
    let versions = VersionKey::bucket_prefix(bucket).map_err(|_| KeyError::Storage)?;
    let after = (!progress.cursor.is_empty()).then(|| progress.cursor.clone().into());
    let scan = iter_prefix_page(
        storage,
        BLOB_VERSIONS_KEYSPACE,
        Some(versions.into()),
        after,
        SCAN,
        None,
    );
    let (rows, next) = scan.await.map_err(|_| KeyError::Storage)?;
    let (mut moved, mut handled, mut stopped) = (0, 0, None);
    for (row, _) in &rows {
        if moved >= page.max(1) {
            break;
        }
        let version = VersionKey::from_bytes(row).map_err(|_| KeyError::Storage)?;
        if version.key.starts_with(prefix) {
            let mut operation =
                RewriteVersionOperation::new(version, unit.clone(), SystemTime::now()).rekey();
            if let Some((quota, realm, node)) = &quota {
                operation = operation.with_quota(quota.clone(), *realm, *node);
            }
            match drive(operation, context).await {
                Ok(RewriteOutcome::Moved) => {
                    progress.rekeyed += 1;
                    moved += 1;
                }
                Ok(RewriteOutcome::Skipped) => {}
                Ok(RewriteOutcome::AwaitingKey) => {
                    stopped = Some(KeyError::Locked);
                    break;
                }
                Err(error) => {
                    tracing::warn!(event = "abe.rekey.failed", bucket, error = %error);
                    stopped = Some(KeyError::Storage);
                    break;
                }
            }
        }
        progress.cursor = row.to_vec();
        handled += 1;
    }
    let more = stopped.is_some() || handled < rows.len() || next.is_some();
    let done = save(storage, id, seen, &mut progress, more).await?;
    match stopped {
        Some(error) => Err(error),
        None => Ok((progress, done)),
    }
}

/// Saves the cursor while the row is still the one this page read. The last page finishes the
/// pass only without a raise or due removal since it began; otherwise the pass starts again.
async fn save(
    storage: &StorageHandle,
    id: Key,
    seen: Option<Value>,
    progress: &mut RekeyProgress,
    more: bool,
) -> Result<bool, KeyError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        _ => return Err(KeyError::Storage),
    };
    let staged = stage(storage, txn_id, id, seen, progress, more).await;
    let commit = match staged {
        Ok(Some(_)) => StorageEffect::CommitTransaction { txn_id },
        _ => StorageEffect::AbortTransaction { txn_id },
    };
    let event = storage.send_storage_effect(commit).await;
    match (staged, event) {
        (Ok(Some(done)), Event::Storage(StorageEvent::TransactionCommitted { .. })) => Ok(done),
        (Ok(Some(_)), _) => Err(KeyError::Storage),
        // Another call moved the pass meanwhile; its own save stands.
        (Ok(None), _) => Ok(false),
        (Err(error), _) => Err(error),
    }
}

async fn stage(
    storage: &StorageHandle,
    txn_id: TxnId,
    id: Key,
    seen: Option<Value>,
    progress: &mut RekeyProgress,
    more: bool,
) -> Result<Option<bool>, KeyError> {
    let reads = vec![
        (ABE_REKEY_KEYSPACE.to_string(), id.clone()),
        (ABE_EPOCH_KEYSPACE.to_string(), id.clone()),
        (ABE_DUE_KEYSPACE.to_string(), id.clone()),
    ];
    let [row, epoch, due] = <[_; 3]>::try_from(read_rows(storage, reads, Some(txn_id)).await?)
        .map_err(|_| KeyError::Storage)?;
    if row != seen {
        return Ok(None);
    }
    // A removal during the walk is not covered by it, so the subtree is walked again.
    let restart = !more && (epoch_of(epoch.as_ref()) != Some(progress.epoch) || due.is_some());
    if restart {
        progress.cursor.clear();
        progress.rekeyed = 0;
    }
    let done = !more && !restart;
    let effect = match done {
        true => StorageEffect::BatchDelete {
            deletes: vec![(ABE_REKEY_KEYSPACE.to_string(), id)],
            txn_id: Some(txn_id),
        },
        false => StorageEffect::BatchWrite {
            writes: vec![(
                ABE_REKEY_KEYSPACE.to_string(),
                id,
                postcard::to_allocvec(progress)
                    .map_err(|_| KeyError::Storage)?
                    .into(),
            )],
            txn_id: Some(txn_id),
        },
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(
            StorageEvent::BatchDeleteResult { .. } | StorageEvent::BatchWriteResult { .. },
        ) => Ok(Some(done)),
        _ => Err(KeyError::Storage),
    }
}
