//! Advances queued encryption transitions: one page of versions per bucket and run, then the
//! wait for old copies to be removed. Only then the source key retires and its node copy goes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::migration::MIGRATION_CONTINUE;
use crate::blob::migration_rewrite::{RewriteOutcome, RewriteVersionOperation};
use crate::driver::DriverContext;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_KEY_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, TRANSITION_CLEANUP_KEYSPACE, TRANSITION_KEYSPACE,
    TRANSITION_QUEUE_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::structs::storage::blob::{BackendLocation, BlobCleanupWork, VersionKey};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, KeyState, KeyTicket, SealPlan, UnlockStatus,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionState, TransitionTarget, cleanup_prefix,
};
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::StorageHandle;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};

/// Versions, or old copies, one run checks per bucket.
const PAGE: usize = 64;
/// Queued buckets read per queue page.
const BUCKET_PAGE: usize = 16;
/// Wait before a transition looks again for an unlocked key or removed old copies.
pub const RECHECK: Duration = Duration::from_secs(60);
/// Passes with failures started again before the transition reports itself blocked.
const RETRIES: u32 = 10;

/// Advances every queued transition by one page. Returns when the task must run again.
pub async fn process_transitions(context: &DriverContext) -> Result<Option<Duration>, String> {
    let storage = &context.storage_handle;
    let now = crate::effect_adapters::routing::now_ms();
    let mut next = None::<Duration>;
    let mut soon = |after: Duration| next = Some(next.map_or(after, |next| next.min(after)));
    let mut after = None;
    loop {
        let (rows, cursor) = crate::jobs::store::iter_prefix_page(
            storage,
            TRANSITION_QUEUE_KEYSPACE,
            None,
            after,
            BUCKET_PAGE,
            None,
        )
        .await?;
        for (key, _) in rows {
            let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
            let Some(record) = read_record(storage, &bucket).await? else {
                continue;
            };
            if record.finished_at_ms.is_some() {
                continue;
            }
            if let Some(at) = record.retry_at_ms.filter(|at| *at > now) {
                soon(Duration::from_millis(at - now));
                continue;
            }
            if let Some(wait) = advance(context, &bucket, record).await? {
                soon(wait);
            }
        }
        match cursor {
            Some(cursor) => after = Some(cursor),
            None => return Ok(next),
        }
    }
}

/// The rows that start converting a bucket's plain copies after encryption is enabled. The
/// enabling transaction writes them with its settings, then wakes `TaskKey::MigrateCompression`.
pub fn encrypt_rows(
    bucket: &str,
    settings: &BucketEncryption,
    record: &BucketKeyRecord,
    compression: Compression,
    now_ms: u64,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let plan = SealPlan::capture(settings, record).map_err(ConversionError::from)?;
    let target = TransitionTarget { compression, plan };
    let generation = settings.storage_generation;
    let kind = TransitionKind::Encrypt;
    let transition = EncryptionTransition::new(kind, None, target, generation, now_ms);
    let key: Key = bucket.as_bytes().to_vec().into();
    Ok(vec![
        (
            TRANSITION_KEYSPACE.to_string(),
            key.clone(),
            transition.to_bytes()?.into(),
        ),
        (
            TRANSITION_QUEUE_KEYSPACE.to_string(),
            key,
            Vec::new().into(),
        ),
    ])
}

async fn read_record(
    storage: &StorageHandle,
    bucket: &str,
) -> Result<Option<EncryptionTransition>, String> {
    let read = StorageEffect::Read {
        key_space: TRANSITION_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| EncryptionTransition::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read transition progress: {other:?}")),
    }
}

/// One step of a bucket's transition: a page of versions, or a cleanup check after the pass.
async fn advance(
    context: &DriverContext,
    bucket: &str,
    mut record: EncryptionTransition,
) -> Result<Option<Duration>, String> {
    let now = crate::effect_adapters::routing::now_ms();
    record.retry_at_ms = None;
    if record.state == TransitionState::Cleanup {
        return settle(context, bucket, record).await;
    }
    if record.cursor.is_none() {
        // A new pass counts its own waits and failures.
        record.remaining = 0;
        record.failed = 0;
        record.state = TransitionState::Running;
    }
    let prefix = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (versions, next) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        BLOB_VERSIONS_KEYSPACE,
        Some(prefix.into()),
        record.cursor.clone().map(Into::into),
        PAGE,
        None,
    )
    .await?;
    for (key, _) in &versions {
        let version_key = VersionKey::from_bytes(key.as_ref()).map_err(|e| e.to_string())?;
        let operation =
            RewriteVersionOperation::new(version_key, record.clone(), SystemTime::now());
        match crate::driver::drive(operation, context).await {
            Ok(RewriteOutcome::Moved) => record.done += 1,
            Ok(RewriteOutcome::Skipped) => {}
            Ok(RewriteOutcome::AwaitingKey) => record.remaining += 1,
            Err(error) => {
                record.failed += 1;
                tracing::warn!(bucket, %error, "Failed to move a version to the new encryption");
            }
        }
        record.cursor = Some(key.to_vec());
    }
    if next.is_some() {
        store(&context.storage_handle, bucket, &record).await?;
        return Ok(Some(MIGRATION_CONTINUE));
    }
    record.cursor = None;
    let wait = match (record.failed > 0, record.remaining > 0) {
        (true, _) if record.retries < RETRIES => {
            record.retries += 1;
            Some(RECHECK.saturating_mul(1 << record.retries.min(6)))
        }
        (true, _) => {
            record.state = TransitionState::Blocked;
            record.blocked_reason = Some("versions failed to move in every retry".to_string());
            Some(RECHECK.saturating_mul(60))
        }
        (false, true) => {
            record.state = TransitionState::AwaitingKey;
            Some(RECHECK)
        }
        (false, false) => {
            record.state = TransitionState::Cleanup;
            return settle(context, bucket, record).await;
        }
    };
    record.retry_at_ms = wait.map(|wait| now.saturating_add(wait.as_millis() as u64));
    store(&context.storage_handle, bucket, &record).await?;
    Ok(wait)
}

/// Counts old copies whose location rows still exist and forgets removed ones. With none
/// left the transition finishes.
async fn settle(
    context: &DriverContext,
    bucket: &str,
    mut record: EncryptionTransition,
) -> Result<Option<Duration>, String> {
    let storage = &context.storage_handle;
    let prefix: Key = cleanup_prefix(bucket).into();
    let mut after = None;
    let mut left = 0u64;
    loop {
        let (rows, cursor) = crate::jobs::store::iter_prefix_page(
            storage,
            TRANSITION_CLEANUP_KEYSPACE,
            Some(prefix.clone()),
            after,
            PAGE,
            None,
        )
        .await?;
        let keys = rows.into_iter().map(|(key, _)| key).collect();
        left += forget_removed(storage, prefix.len(), keys).await?;
        match cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    record.cleanup_remaining = left;
    let now = crate::effect_adapters::routing::now_ms();
    if left > 0 {
        record.retry_at_ms = Some(now.saturating_add(RECHECK.as_millis() as u64));
        store(storage, bucket, &record).await?;
        return Ok(Some(RECHECK));
    }
    let pending = pending_on_source(storage, &record).await?;
    if pending > 0 {
        // Pending archives move only after promotion; until then the source key must stay.
        record.remaining = pending;
        record.state = TransitionState::Blocked;
        record.blocked_reason = Some(PENDING_REASON.to_string());
        record.retry_at_ms = Some(now.saturating_add(RECHECK.as_millis() as u64));
        store(storage, bucket, &record).await?;
        return Ok(Some(RECHECK));
    }
    record.remaining = 0;
    record.blocked_reason = None;
    record.state = TransitionState::Finished;
    record.finished_at_ms = Some(now);
    if store(storage, bucket, &record).await? {
        forget_retired(context, &record).await;
    }
    Ok(None)
}

/// The sessions of a retired source generation; admitted leases keep their own key handle.
fn retired_sessions(generations: &[UnlockStatus], record: &EncryptionTransition) -> Vec<KeyTicket> {
    let Some(source) = record.source else {
        return Vec::new();
    };
    if record.target.plan.map(|plan| plan.key) == Some(source) {
        return Vec::new();
    }
    let sessions = generations.iter().filter(|status| status.key == source);
    sessions
        .map(|status| KeyTicket {
            key: status.key,
            session_id: status.session_id,
        })
        .collect()
}

/// Drops a retired generation from the unlock registry, so it no longer takes a generation
/// slot. A failure only leaves the key until the next lock or restart.
async fn forget_retired(context: &DriverContext, record: &EncryptionTransition) {
    let (Some(blob), Some(source)) = (context.blob_handle.as_ref(), record.source) else {
        return;
    };
    let status = BlobEffect::ReadKeyStatus {
        bucket_id: source.bucket_id,
    };
    let Event::Blob(BlobEvent::KeyStatus { generations }) = blob.send_blob_effect(status).await
    else {
        tracing::warn!("Could not read the unlock state of a retired key generation");
        return;
    };
    for ticket in retired_sessions(&generations, record) {
        blob.send_blob_effect(BlobEffect::DiscardKey { ticket })
            .await;
    }
}

/// Why a transition waits after every other copy moved.
const PENDING_REASON: &str = "archives with pending content still use the source key";

/// Pending archives sealed to the transition's source generation, on every backend.
async fn pending_on_source(
    storage: &StorageHandle,
    record: &EncryptionTransition,
) -> Result<u64, String> {
    let Some(source) = record.source else {
        return Ok(0);
    };
    let (mut after, mut count) = (None, 0u64);
    loop {
        let (rows, cursor) = crate::jobs::store::iter_prefix_page(
            storage,
            PENDING_LOCATION_KEYSPACE,
            None,
            after,
            PAGE,
            None,
        )
        .await?;
        let locations = rows
            .iter()
            .map(|(_, value)| BackendLocation::from_bytes(value.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        count += uses_key(&locations, source);
        match cursor {
            Some(cursor) => after = Some(cursor),
            None => return Ok(count),
        }
    }
}

/// How many of `locations` are sealed to `source`.
fn uses_key(locations: &[BackendLocation], source: BucketKeyRef) -> u64 {
    let sealed = locations
        .iter()
        .filter(|location| location.format.bucket_key() == Some(source));
    sealed.count() as u64
}

/// Location keys of copies whose physical deletion is still queued or failed and waits for
/// a retry.
/// Forgets the old copies of one page that are gone, in one transaction: a reclaim committing
/// meanwhile is either in its snapshot or fails the commit. Returns the copies still present.
async fn forget_removed(
    storage: &StorageHandle,
    prefix_len: usize,
    keys: Vec<Key>,
) -> Result<u64, String> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("could not start a cleanup transaction: {other:?}")),
    };
    let staged = stage_forget(storage, txn_id, prefix_len, keys).await;
    let Ok(left) = staged else {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return staged;
    };
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(left),
        other => Err(format!("removed copies were not forgotten: {other:?}")),
    }
}

/// Checks the location row and the queued delete work of each old copy in `txn_id`. Reclaim
/// drops the row before the backend delete, so only finished delete work proves removal.
async fn stage_forget(
    storage: &StorageHandle,
    txn_id: TxnId,
    prefix_len: usize,
    keys: Vec<Key>,
) -> Result<u64, String> {
    let queued = queued_deletes(storage, txn_id).await?;
    let mut left = 0;
    for key in keys {
        let location = key[prefix_len..].to_vec();
        if queued.contains(&location)
            || exists(storage, BLOB_LOCATIONS_KEYSPACE, location, Some(txn_id)).await?
        {
            left += 1;
            continue;
        }
        let delete = StorageEffect::Delete {
            key_space: TRANSITION_CLEANUP_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        };
        match storage.send_storage_effect(delete).await {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            other => return Err(format!("could not forget a removed copy: {other:?}")),
        }
    }
    Ok(left)
}

/// Location keys of copies whose physical deletion is still queued or failed and waits for
/// a retry.
async fn queued_deletes(
    storage: &StorageHandle,
    txn_id: TxnId,
) -> Result<HashSet<Vec<u8>>, String> {
    let (mut after, mut queued) = (None, HashSet::new());
    loop {
        let (rows, cursor) = crate::jobs::store::iter_prefix_page(
            storage,
            BLOB_CLEANUP_KEYSPACE,
            None,
            after,
            PAGE,
            Some(txn_id),
        )
        .await?;
        for (_, value) in rows {
            let work = BlobCleanupWork::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
            if let Some(Ok(key)) = work.location().map(BackendLocation::location_key) {
                queued.insert(key.to_bytes());
            }
        }
        match cursor {
            Some(cursor) => after = Some(cursor),
            None => return Ok(queued),
        }
    }
}

async fn exists(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
    txn_id: Option<TxnId>,
) -> Result<bool, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
        other => Err(format!("could not read {key_space}: {other:?}")),
    }
}

/// Stores the progress unless a newer change replaced the transition. A finished one leaves
/// the queue, retires its source key and removes that key's node copy in the same commit.
async fn store(
    storage: &StorageHandle,
    bucket: &str,
    record: &EncryptionTransition,
) -> Result<bool, String> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("could not start a progress transaction: {other:?}")),
    };
    let staged = stage(storage, txn_id, bucket, record).await;
    if !matches!(staged, Ok(true)) {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return staged;
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(true),
        other => Err(format!("transition progress was not stored: {other:?}")),
    }
}

async fn stage(
    storage: &StorageHandle,
    txn_id: TxnId,
    bucket: &str,
    record: &EncryptionTransition,
) -> Result<bool, String> {
    let key: Key = bucket.as_bytes().to_vec().into();
    let read = StorageEffect::Read {
        key_space: TRANSITION_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let stored = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => EncryptionTransition::from_bytes(value.as_ref()).map_err(|e| e.to_string())?,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(false),
        other => return Err(format!("could not read transition progress: {other:?}")),
    };
    if stored.started_at_ms != record.started_at_ms || stored.kind != record.kind {
        return Ok(false);
    }
    let value = record.to_bytes().map_err(|error| error.to_string())?;
    let mut effects = vec![StorageEffect::Write {
        key_space: TRANSITION_KEYSPACE.to_string(),
        key: key.clone(),
        value: value.into(),
        txn_id: Some(txn_id),
    }];
    if record.finished_at_ms.is_some() {
        effects.push(StorageEffect::Delete {
            key_space: TRANSITION_QUEUE_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        });
        effects.extend(retire_source(storage, txn_id, record).await?);
    }
    for effect in effects {
        match storage.send_storage_effect(effect).await {
            Event::Storage(
                StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
            ) => {}
            other => return Err(format!("could not store transition progress: {other:?}")),
        }
    }
    Ok(true)
}

/// No copy needs the source generation any more: it retires and its node copy is removed.
async fn retire_source(
    storage: &StorageHandle,
    txn_id: TxnId,
    record: &EncryptionTransition,
) -> Result<Vec<StorageEffect>, String> {
    let Some(source) = record.source else {
        return Ok(Vec::new());
    };
    let current = record.target.plan.map(|plan| plan.key);
    if current == Some(source) {
        return Ok(Vec::new());
    }
    let read = StorageEffect::Read {
        key_space: BUCKET_KEY_KEYSPACE.to_string(),
        key: source.key().into(),
        txn_id: Some(txn_id),
    };
    let mut key = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => BucketKeyRecord::from_bytes(value.as_ref()).map_err(|e| e.to_string())?,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(Vec::new()),
        other => return Err(format!("could not read the source key: {other:?}")),
    };
    let vault = key.vault_entry.take().or(record.source_vault);
    key.state = KeyState::Retired;
    let mut effects = vec![StorageEffect::Write {
        key_space: BUCKET_KEY_KEYSPACE.to_string(),
        key: source.key().into(),
        value: key.to_bytes().map_err(|e| e.to_string())?.into(),
        txn_id: Some(txn_id),
    }];
    if let Some(id) = vault {
        effects.push(StorageEffect::VaultDelete {
            entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            txn_id: Some(txn_id),
        });
    }
    Ok(effects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn pending(key: Option<BucketKeyRef>) -> BackendLocation {
        let layout = PithosLayout {
            stored_size: 9,
            metadata_digest: [1; 32],
        };
        let format = key.map_or_else(StoredFormat::default, |key| {
            StoredFormat::pithos(layout, key)
        });
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: String::new(),
            ulid: Ulid::from_bytes([2; 16]),
            format,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 9,
            hashes: Default::default(),
        }
    }

    #[test]
    fn pending_keeps_source() {
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let newer = BucketKeyRef::new(source.bucket_id, 2);
        let other = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1);
        let locations = [
            pending(Some(source)),
            pending(Some(newer)),
            pending(Some(other)),
            pending(None),
            pending(Some(source)),
        ];
        assert_eq!(uses_key(&locations, source), 2);
        assert_eq!(uses_key(&locations[1..4], source), 0);
    }

    fn context(root: &std::path::Path) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root.to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        let event = context.storage_handle.send_storage_effect(write).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn failed_delete_keeps_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let mut record = EncryptionTransition::new(TransitionKind::Decrypt, None, target, 2, 1);
        record.state = TransitionState::Cleanup;
        put(
            &context,
            TRANSITION_KEYSPACE,
            b"b".to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;
        let mut old = pending(None);
        let hash = aruna_core::structs::checksum::HASH_BLAKE3.to_string();
        old.hashes.insert(hash, vec![7; 32]);
        let old_key = old.location_key().unwrap().to_bytes();
        let cleanup = aruna_core::structs::storage::transition::cleanup_key("b", &old_key);
        put(&context, TRANSITION_CLEANUP_KEYSPACE, cleanup, Vec::new()).await;
        // Reclaim removed the location row, but the backend delete is still queued or failed.
        let work = BlobCleanupWork::DeleteBlob { location: old }
            .to_bytes()
            .unwrap();
        let work_key = Ulid::generate().to_bytes().to_vec();
        put(&context, BLOB_CLEANUP_KEYSPACE, work_key.clone(), work).await;

        let wait = settle(&context, "b", record.clone()).await.unwrap();

        assert_eq!(wait, Some(RECHECK));
        let stored = read_record(&context.storage_handle, "b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.cleanup_remaining, 1);
        assert_eq!(stored.finished_at_ms, None);

        // Only a finished delete removes the work row; then the transition may finish.
        let delete = StorageEffect::Delete {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: work_key.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(delete).await;
        assert_eq!(settle(&context, "b", stored).await.unwrap(), None);
        let stored = read_record(&context.storage_handle, "b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.reported_state(), TransitionState::Finished);
    }

    #[test]
    fn retired_sessions_only() {
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let newer = BucketKeyRef::new(source.bucket_id, 2);
        let status = |key, seed| UnlockStatus {
            key,
            session_id: Ulid::from_bytes([seed; 16]),
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining: None,
            max_remaining: None,
        };
        let generations = [status(source, 5), status(newer, 6)];
        let plan = |key| SealPlan {
            key,
            public_key: [2; 32],
            cipher: Default::default(),
            block_keys: Default::default(),
            storage_generation: 3,
        };
        let rotate = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(newer)),
        };
        let kind = TransitionKind::Rotate;
        let rotation = EncryptionTransition::new(kind, Some(source), rotate, 3, 1);
        let retired = retired_sessions(&generations, &rotation);
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].key, source);

        // A re-encode keeps its key, so nothing retires.
        let same = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(source)),
        };
        let kind = TransitionKind::Reencode;
        let reencode = EncryptionTransition::new(kind, Some(source), same, 3, 1);
        assert!(retired_sessions(&generations, &reencode).is_empty());
    }

    #[tokio::test]
    async fn reclaim_between_reads() {
        // A reclaim commits after the cleanup check began: the check must still see the copy.
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let storage = &context.storage_handle;
        let mut old = pending(None);
        let hash = aruna_core::structs::checksum::HASH_BLAKE3.to_string();
        old.hashes.insert(hash, vec![7; 32]);
        let old_key = old.location_key().unwrap().to_bytes();
        let cleanup = aruna_core::structs::storage::transition::cleanup_key("b", &old_key);
        put(
            &context,
            TRANSITION_CLEANUP_KEYSPACE,
            cleanup.clone(),
            Vec::new(),
        )
        .await;
        let row = old.to_bytes().unwrap();
        put(&context, BLOB_LOCATIONS_KEYSPACE, old_key.clone(), row).await;
        let started = storage
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await;
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = started else {
            panic!("no transaction: {started:?}")
        };

        let delete = StorageEffect::Delete {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: old_key.into(),
            txn_id: None,
        };
        storage.send_storage_effect(delete).await;
        let work = BlobCleanupWork::DeleteBlob { location: old }
            .to_bytes()
            .unwrap();
        put(
            &context,
            BLOB_CLEANUP_KEYSPACE,
            Ulid::generate().to_bytes().to_vec(),
            work,
        )
        .await;
        let prefix = aruna_core::structs::storage::transition::cleanup_prefix("b").len();
        let left = stage_forget(storage, txn_id, prefix, vec![cleanup.clone().into()]).await;

        assert_eq!(left, Ok(1));
        storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
        assert!(
            exists(storage, TRANSITION_CLEANUP_KEYSPACE, cleanup.clone(), None)
                .await
                .unwrap()
        );
        // A later check sees the queued delete instead and still keeps the copy.
        let left = forget_removed(storage, prefix, vec![cleanup.into()]).await;
        assert_eq!(left, Ok(1));
    }
}
