//! Purges what a member bucket holds of one range of a distributed bucket: first the open
//! uploads from the upload inventory, then every key from the ordered inventory, whole keys
//! with all their versions and kept copies. Pages are bounded, a durable cursor resumes them
//! and nothing is counted first. Every page checks that the bucket still carries the recorded
//! set, member and incarnation; an absent or replaced bucket means the purge is done.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ATTACHMENT_KEYSPACE, BOOTSTRAP_KEYSPACE, CACHE_ENTRY_KEYSPACE, CACHE_ORDER_KEYSPACE,
    DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_PURGE_KEYSPACE, INVENTORY_KEYSPACE,
    KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, RANGE_INDEX_KEYSPACE,
    SOURCE_ROOT_KEYSPACE, UPLOAD_INVENTORY_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::structs::storage::distribution::accounting::account_key;
use aruna_core::structs::storage::distribution::fence::{ClearCursor, PurgeCursor};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, Position, VersionEntry, index_key, range_interval,
};
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CACHE_TOTALS_KEY, CacheEntry, CacheTotals, attachment_key,
};
use aruna_core::types::{GroupId, Key, Value};
use ulid::Ulid;

use super::pull::drop_copy;
use crate::driver::{DriverContext, drive, now_ms};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::multipart::abort::{AbortUploadError, AbortUploadInput, AbortUploadOperation};
use crate::s3::object::delete::{DeleteObjectError, DeleteObjectInput, DeleteObjectOperation};
use crate::s3::object::versions::{ListVersionsInput, ListVersionsItem, ListVersionsOperation};

/// Inventory rows one page purges at most.
const PAGE_ROWS: usize = 64;
const PAGE_BYTES: usize = 1 << 20;

/// One range of one member bucket to purge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeTarget {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: String,
    pub group_id: GroupId,
    pub binding: DistributionBinding,
    pub range_id: Ulid,
    pub lower: String,
    pub upper: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Purged {
    /// The page budget ran out; the next pass continues at the cursor.
    More,
    Done,
}

/// Purges at most `pages` pages of the range. `Done` once the range holds nothing of the set
/// anymore, or the bucket is no longer the bound member.
pub async fn purge_range(
    context: &DriverContext,
    target: &PurgeTarget,
    pages: &mut usize,
) -> Result<Purged, String> {
    let set_id = target.binding.set_id;
    let key = PurgeCursor::key(set_id, target.range_id);
    let stored = read(context, DISTRIBUTION_PURGE_KEYSPACE, key.clone()).await?;
    let mut cursor = PurgeCursor::decode(stored.as_deref()).map_err(|e| e.to_string())?;
    while !cursor.done {
        if *pages == 0 {
            return Ok(Purged::More);
        }
        *pages -= 1;
        if !still_bound(context, &target.bucket, &target.binding).await? {
            cursor.done = true;
        } else {
            let space = match cursor.uploads_done {
                true => INVENTORY_KEYSPACE,
                false => UPLOAD_INVENTORY_KEYSPACE,
            };
            let rows = page(context, space, target, cursor.after.clone()).await?;
            match (rows.last().map(|(key, _)| key.clone()), cursor.uploads_done) {
                (None, false) => (cursor.uploads_done, cursor.after) = (true, None),
                (None, true) => {
                    clear_range_rows(context, set_id, target.range_id).await?;
                    cursor.done = true;
                }
                (Some(last), false) => {
                    abort_uploads(context, target, &rows).await?;
                    cursor.after = Some(last);
                }
                (Some(last), true) => {
                    purge_keys(context, target, rows).await?;
                    cursor.after = Some(last);
                }
            }
        }
        let value = cursor.to_bytes().map_err(|error| error.to_string())?;
        write(context, DISTRIBUTION_PURGE_KEYSPACE, key.clone(), value).await?;
    }
    Ok(Purged::Done)
}

/// Whether `bucket` is still the member bucket of `binding`: same set, member and incarnation.
pub async fn still_bound(
    context: &DriverContext,
    bucket: &str,
    binding: &DistributionBinding,
) -> Result<bool, String> {
    match drive(GetBucketOperation::new(bucket.to_string()), context).await {
        Ok(info) => Ok(info.distribution.is_some_and(|bound| {
            (bound.set_id, bound.member_id, bound.incarnation)
                == (binding.set_id, binding.member_id, binding.incarnation)
        })),
        Err(GetBucketError::NotFound) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

async fn page(
    context: &DriverContext,
    key_space: &str,
    target: &PurgeTarget,
    after: Option<Vec<u8>>,
) -> Result<Vec<(Vec<u8>, Value)>, String> {
    let set_id = target.binding.set_id;
    let (lower, upper) = range_interval(set_id, &target.lower, target.upper.as_deref());
    let read = StorageEffect::IterRange {
        key_space: key_space.to_string(),
        start: match after {
            Some(after) => IterStart::After(after.into()),
            None => IterStart::At(lower.into()),
        },
        end: IterEnd::Before(upper.into()),
        limit: PAGE_ROWS,
        max_bytes: PAGE_BYTES,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value))
            .collect()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected purge page answer {other:?}")),
    }
}

/// Whether the range still has an open upload.
pub async fn has_uploads(context: &DriverContext, target: &PurgeTarget) -> Result<bool, String> {
    let rows = page(context, UPLOAD_INVENTORY_KEYSPACE, target, None).await?;
    Ok(!rows.is_empty())
}

/// Aborts one page of the range's open uploads; `true` while it found any, so the caller
/// looks again until none is left.
pub async fn abort_range_uploads(
    context: &DriverContext,
    target: &PurgeTarget,
) -> Result<bool, String> {
    let rows = page(context, UPLOAD_INVENTORY_KEYSPACE, target, None).await?;
    abort_uploads(context, target, &rows).await?;
    Ok(!rows.is_empty())
}

/// Deletes one page of versions the inventory does not name, from the bucket's own listing,
/// so the member bucket ends up empty. The listing restarts at the first key each time.
pub async fn purge_rest(context: &DriverContext, target: &PurgeTarget) -> Result<(), String> {
    if !still_bound(context, &target.bucket, &target.binding).await? {
        return Ok(());
    }
    let list = ListVersionsOperation::new(ListVersionsInput {
        bucket: target.bucket.clone(),
        prefix: None,
        delimiter: None,
        key_marker: None,
        version_id_marker: None,
        max_keys: Some(PAGE_ROWS),
    });
    let listed = drive(list, context)
        .await
        .map_err(|error| error.to_string())?;
    for item in listed.items {
        let (ListVersionsItem::Version {
            key, version_id, ..
        }
        | ListVersionsItem::DeleteMarker {
            key, version_id, ..
        }) = item;
        delete_version(context, target, &key, version_id).await?;
    }
    Ok(())
}

/// The keyspaces a set's local rows are cleared from, in step order after the attachments.
const CLEARED: [&str; 10] = [
    INVENTORY_KEYSPACE,
    UPLOAD_INVENTORY_KEYSPACE,
    VERSION_INDEX_KEYSPACE,
    RANGE_INDEX_KEYSPACE,
    RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE,
    KEY_SAMPLE_KEYSPACE,
    BOOTSTRAP_KEYSPACE,
    SOURCE_ROOT_KEYSPACE,
    DISTRIBUTION_PURGE_KEYSPACE,
];

/// Deletes what this node keeps of a set besides its bucket: copy-on-use attachments with
/// their bytes, inventory, accounting, bootstrap and purge cursors, at most `pages` pages per
/// call from a durable cursor. The fence row stays with `keep_fence`, so nothing writes into a
/// deleted set again.
pub async fn clear_set_rows(
    context: &DriverContext,
    set_id: Ulid,
    keep_fence: bool,
    pages: &mut usize,
) -> Result<Purged, String> {
    let key = ClearCursor::key(set_id);
    let stored = read(context, DISTRIBUTION_PURGE_KEYSPACE, key.clone()).await?;
    let mut cursor = ClearCursor::decode(stored.as_deref()).map_err(|e| e.to_string())?;
    let prefix = set_id.to_bytes().to_vec();
    while usize::from(cursor.step) <= CLEARED.len() {
        if *pages == 0 {
            return Ok(Purged::More);
        }
        *pages -= 1;
        let space = match cursor.step {
            0 => ATTACHMENT_KEYSPACE,
            step => CLEARED[usize::from(step) - 1],
        };
        // The cursor row itself sorts first among the purge cursors and is skipped.
        let start = match (cursor.after.clone(), space == DISTRIBUTION_PURGE_KEYSPACE) {
            (Some(after), _) => Some(after),
            (None, true) => Some(key.clone()),
            (None, false) => None,
        };
        let rows = clear_page(context, space, &prefix, start).await?;
        match rows.last() {
            None => (cursor.step, cursor.after) = (cursor.step + 1, None),
            Some((last, _)) => cursor.after = Some(last.to_vec()),
        }
        let mut deletes = Vec::with_capacity(rows.len());
        for (row, value) in rows {
            if space == ATTACHMENT_KEYSPACE
                && let Ok(attachment) = Attachment::from_bytes(&value)
            {
                drop_copy(context, &attachment.location).await;
            }
            deletes.push((space.to_string(), row));
        }
        batch_delete(context, deletes).await?;
        let value = cursor.to_bytes().map_err(|error| error.to_string())?;
        write(context, DISTRIBUTION_PURGE_KEYSPACE, key.clone(), value).await?;
    }
    let mut deletes = vec![(DISTRIBUTION_PURGE_KEYSPACE.to_string(), key.into())];
    if !keep_fence {
        deletes.push((DISTRIBUTION_FENCE_KEYSPACE.to_string(), prefix.into()));
    }
    batch_delete(context, deletes).await?;
    Ok(Purged::Done)
}

/// One page of a set's rows in `key_space`, after `start` when given.
async fn clear_page(
    context: &DriverContext,
    key_space: &str,
    prefix: &[u8],
    start: Option<Vec<u8>>,
) -> Result<Vec<(Key, Value)>, String> {
    let limit = match key_space == ATTACHMENT_KEYSPACE {
        true => PAGE_ROWS,
        false => PAGE_ROWS * 16,
    };
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: Some(prefix.to_vec().into()),
        start: start.map(|key| IterStart::After(key.into())),
        limit,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected scan answer {other:?}")),
    }
}

async fn abort_uploads(
    context: &DriverContext,
    target: &PurgeTarget,
    rows: &[(Vec<u8>, Value)],
) -> Result<(), String> {
    for (position, _) in rows {
        let position = Position::decode(position).map_err(|error| error.to_string())?;
        let Entry::Upload(upload_id) = position.entry else {
            continue;
        };
        let input = AbortUploadInput {
            bucket: target.bucket.clone(),
            key: position.key,
            upload_id,
            now_ms: now_ms(),
        };
        let abort = AbortUploadOperation::new(input).including_in_progress();
        match drive(abort, context).await {
            Ok(()) | Err(AbortUploadError::NoSuchUpload) => {}
            Err(error) => return Err(format!("an upload was not aborted: {error}")),
        }
    }
    Ok(())
}

/// Deletes every version of the page's keys with their kept copies, then the page's
/// inventory and index rows, which the version deletes do not maintain.
async fn purge_keys(
    context: &DriverContext,
    target: &PurgeTarget,
    rows: Vec<(Vec<u8>, Value)>,
) -> Result<(), String> {
    let set_id = target.binding.set_id;
    let mut deletes = Vec::with_capacity(rows.len() * 2);
    for (row, value) in rows {
        let position = Position::decode(&row).map_err(|error| error.to_string())?;
        if let Entry::Version { version_id, .. } = position.entry {
            delete_version(context, target, &position.key, version_id).await?;
            drop_attachment(context, set_id, version_id).await?;
            let entry = VersionEntry::from_bytes(&value).map_err(|error| error.to_string())?;
            if let Some(identity) = entry.identity {
                drop_cached(context, identity).await?;
            }
            let index = index_key(set_id, version_id).into();
            deletes.push((VERSION_INDEX_KEYSPACE.to_string(), index));
        }
        deletes.push((INVENTORY_KEYSPACE.to_string(), Key::from(row)));
    }
    batch_delete(context, deletes).await
}

async fn delete_version(
    context: &DriverContext,
    target: &PurgeTarget,
    key: &str,
    version_id: Ulid,
) -> Result<(), String> {
    let input = DeleteObjectInput {
        bucket: target.bucket.clone(),
        key: key.to_string(),
        version_id: Some(version_id),
        group_id: target.group_id,
        realm_id: target.realm_id,
        node_id: target.node_id,
        deleted_by: UserId::nil(target.realm_id),
    };
    let delete = DeleteObjectOperation::new(input).purging(target.binding.clone());
    match drive(delete, context).await {
        Ok(_) | Err(DeleteObjectError::NoSuchVersion) => Ok(()),
        Err(error) => Err(format!("a purged version was not deleted: {error}")),
    }
}

/// Removes a copy-on-use attachment of the version and its bytes.
async fn drop_attachment(
    context: &DriverContext,
    set_id: Ulid,
    version_id: Ulid,
) -> Result<(), String> {
    let key = attachment_key(set_id, version_id);
    let Some(value) = read(context, ATTACHMENT_KEYSPACE, key.clone()).await? else {
        return Ok(());
    };
    let attachment = Attachment::from_bytes(&value).map_err(|error| error.to_string())?;
    batch_delete(context, vec![(ATTACHMENT_KEYSPACE.to_string(), key.into())]).await?;
    drop_copy(context, &attachment.location).await;
    Ok(())
}

/// Removes the cache entry of a logical identity with its order row, and its bytes. The cache
/// is shared by identity, so another set reading the same bytes fills it again on use.
async fn drop_cached(context: &DriverContext, identity: [u8; 32]) -> Result<(), String> {
    let storage = &context.storage_handle;
    let started = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = started else {
        return Err(format!("a cache entry was not dropped: {started:?}"));
    };
    let abort = || StorageEffect::AbortTransaction { txn_id };
    let read = StorageEffect::BatchRead {
        reads: vec![
            (CACHE_ENTRY_KEYSPACE.to_string(), identity.to_vec().into()),
            (
                CACHE_ENTRY_KEYSPACE.to_string(),
                CACHE_TOTALS_KEY.to_vec().into(),
            ),
        ],
        txn_id: Some(txn_id),
    };
    let values = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == 2 => values,
        other => {
            storage.send_storage_effect(abort()).await;
            return Err(format!("a cache entry was not read: {other:?}"));
        }
    };
    let mut values = values.into_iter().map(|(_, value)| value);
    let (entry, totals) = (values.next().flatten(), values.next().flatten());
    let Some(entry) = entry
        .map(|value| CacheEntry::from_bytes(&value))
        .transpose()
        .ok()
        .flatten()
    else {
        storage.send_storage_effect(abort()).await;
        return Ok(());
    };
    let mut totals = (totals.map(|value| CacheTotals::from_bytes(&value)))
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    totals.bytes = totals.bytes.saturating_sub(entry.size);
    totals.entries = totals.entries.saturating_sub(1);
    let totals = totals.to_bytes().map_err(|error| error.to_string())?;
    let effects = [
        StorageEffect::BatchDelete {
            deletes: vec![
                (CACHE_ENTRY_KEYSPACE.to_string(), identity.to_vec().into()),
                (
                    CACHE_ORDER_KEYSPACE.to_string(),
                    entry.order.to_bytes().to_vec().into(),
                ),
            ],
            txn_id: Some(txn_id),
        },
        StorageEffect::Write {
            key_space: CACHE_ENTRY_KEYSPACE.to_string(),
            key: CACHE_TOTALS_KEY.to_vec().into(),
            value: totals.into(),
            txn_id: Some(txn_id),
        },
        StorageEffect::CommitTransaction { txn_id },
    ];
    for effect in effects {
        match storage.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::Error { error }) => {
                storage.send_storage_effect(abort()).await;
                return Err(format!("a cache entry was not dropped: {error}"));
            }
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                drop_copy(context, &entry.location).await;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Deletes the accounting rows this node keeps for the range: its account, counters and
/// key sample, in bounded batches.
async fn clear_range_rows(
    context: &DriverContext,
    set_id: Ulid,
    range_id: Ulid,
) -> Result<(), String> {
    let prefix = account_key(set_id, range_id);
    for key_space in [
        RANGE_COUNTER_KEYSPACE,
        KEY_SAMPLE_KEYSPACE,
        RANGE_ACCOUNT_KEYSPACE,
    ] {
        clear_prefix(context, key_space, prefix.clone()).await?;
    }
    Ok(())
}

/// Deletes every row under `prefix` in bounded batches.
pub(super) async fn clear_prefix(
    context: &DriverContext,
    key_space: &str,
    prefix: Vec<u8>,
) -> Result<(), String> {
    loop {
        let iter = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.clone().into()),
            start: None,
            limit: PAGE_ROWS * 16,
            txn_id: None,
        };
        let keys: Vec<Key> = match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult { values, .. }) => {
                values.into_iter().map(|(key, _)| key).collect()
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected scan answer {other:?}")),
        };
        if keys.is_empty() {
            return Ok(());
        }
        let deletes = keys
            .into_iter()
            .map(|key| (key_space.to_string(), key))
            .collect();
        batch_delete(context, deletes).await?;
    }
}

pub(super) async fn read(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected read answer {other:?}")),
    }
}

async fn write(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), String> {
    let write = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected write answer {other:?}")),
    }
}

pub(super) async fn batch_delete(
    context: &DriverContext,
    deletes: Vec<(String, Key)>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    let effect = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected delete answer {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::open;

    fn context(storage_handle: aruna_storage::storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn row(set_id: Ulid, seed: u8) -> Vec<u8> {
        let mut key = set_id.to_bytes().to_vec();
        key.push(seed);
        key
    }

    #[tokio::test]
    async fn clearing_resumes_in_bounded_passes() {
        // Each pass spends at most its pages; the next resumes at the cursor until the set is gone.
        let (_dir, handle) = open();
        let context = context(handle);
        let (set_id, other) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        for seed in 0..4 {
            for id in [set_id, other] {
                write(&context, INVENTORY_KEYSPACE, row(id, seed), vec![seed])
                    .await
                    .unwrap();
            }
        }
        let range_cursor = PurgeCursor::key(set_id, Ulid::from_bytes([3; 16]));
        write(
            &context,
            DISTRIBUTION_PURGE_KEYSPACE,
            range_cursor.clone(),
            vec![],
        )
        .await
        .unwrap();
        let fence = set_id.to_bytes().to_vec();
        write(&context, DISTRIBUTION_FENCE_KEYSPACE, fence.clone(), vec![])
            .await
            .unwrap();
        let mut passes = 0;
        loop {
            passes += 1;
            let mut pages = 1;
            let cleared = clear_set_rows(&context, set_id, true, &mut pages).await;
            if cleared.unwrap() == Purged::Done {
                break;
            }
            assert_eq!(pages, 0, "a waiting pass spent its whole budget");
            assert!(passes < 32, "clearing must finish");
        }
        assert!(
            passes > CLEARED.len(),
            "every keyspace takes at least one page"
        );
        for seed in 0..4 {
            let gone = read(&context, INVENTORY_KEYSPACE, row(set_id, seed)).await;
            assert_eq!(gone.unwrap(), None);
            let kept = read(&context, INVENTORY_KEYSPACE, row(other, seed)).await;
            assert!(kept.unwrap().is_some(), "another set's rows stay");
        }
        let purged = read(&context, DISTRIBUTION_PURGE_KEYSPACE, range_cursor).await;
        assert_eq!(purged.unwrap(), None);
        let cursor = read(
            &context,
            DISTRIBUTION_PURGE_KEYSPACE,
            ClearCursor::key(set_id),
        )
        .await;
        assert_eq!(cursor.unwrap(), None, "a finished clear leaves no cursor");
        let kept = read(&context, DISTRIBUTION_FENCE_KEYSPACE, fence).await;
        assert!(kept.unwrap().is_some(), "a deleted set keeps its fence");
    }
}
