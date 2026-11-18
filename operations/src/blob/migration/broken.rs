//! Runs the queued storage checks of buckets, lists the broken versions found and cleans them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BROKEN_VERSION_KEYSPACE, CHECK_QUEUE_KEYSPACE, STORAGE_CHECK_KEYSPACE,
};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, VersionKey};
use aruna_core::structs::storage::broken::{BrokenVersion, StorageCheck};
use aruna_core::types::Key;

use super::check::{CheckOutcome, CheckVersionOperation};
use super::drain::{BUCKET_PAGE, MIGRATION_CONTINUE, MIGRATION_PAGE};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::driver::DriverContext;
use crate::driver::drive;

/// Advances the unfinished checks at the head of the queue by one page each. Returns when
/// the task must run again.
pub async fn process_checks(context: &DriverContext) -> Result<Option<Duration>, String> {
    let (rows, more) = crate::storage_read::iter_prefix_page(
        &context.storage_handle,
        CHECK_QUEUE_KEYSPACE,
        None,
        None,
        BUCKET_PAGE,
        None,
    )
    .await?;
    let mut next = more.map(|_| MIGRATION_CONTINUE);
    for (key, _) in rows {
        let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
        let Some(record) = read_check(context, &bucket).await? else {
            continue;
        };
        if record.finished_at_ms.is_none() && check_page(context, &bucket, record).await? {
            next = Some(MIGRATION_CONTINUE);
        }
    }
    Ok(next)
}

pub async fn read_check(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<StorageCheck>, String> {
    let read = StorageEffect::Read {
        key_space: STORAGE_CHECK_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| StorageCheck::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read check progress: {}", other.label())),
    }
}

/// Checks one page of current versions and stores the progress. Returns whether more remain.
async fn check_page(
    context: &DriverContext,
    bucket: &str,
    mut record: StorageCheck,
) -> Result<bool, String> {
    let prefix = BlobHeadKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (heads, next) = crate::storage_read::iter_prefix_page(
        &context.storage_handle,
        BLOB_HEAD_KEYSPACE,
        Some(prefix.into()),
        record.cursor.clone().map(Into::into),
        MIGRATION_PAGE,
        None,
    )
    .await?;
    let now_ms = aruna_core::time::unix_timestamp_millis();
    for (key, value) in &heads {
        record.cursor = Some(key.to_vec());
        let head = BlobHeadKey::from_bytes(key.as_ref()).map_err(|error| error.to_string())?;
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref());
        let pointer = pointer.map_err(|error| error.to_string())?;
        let version_key = VersionKey::new(head.bucket, head.key, pointer.version_id);
        let operation = CheckVersionOperation::new(version_key, now_ms);
        match crate::driver::drive(operation, context).await {
            Ok(CheckOutcome::Broken(_)) => record.broken += 1,
            Ok(_) => {}
            Err(error) => {
                record.failed += 1;
                tracing::warn!(bucket, %error, "Failed to check a version");
            }
        }
        record.checked += 1;
    }
    if next.is_none() {
        record.finished_at_ms = Some(now_ms);
    }
    store_check(context, bucket, &record).await?;
    Ok(next.is_some())
}

/// Stores the progress unless a newer check replaced it; a finished check leaves the queue.
async fn store_check(
    context: &DriverContext,
    bucket: &str,
    record: &StorageCheck,
) -> Result<(), String> {
    let storage = &context.storage_handle;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => {
            return Err(format!(
                "could not start a progress transaction: {}",
                other.label()
            ));
        }
    };
    let key: Key = bucket.as_bytes().to_vec().into();
    let read = StorageEffect::Read {
        key_space: STORAGE_CHECK_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let current = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| StorageCheck::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read check progress: {}", other.label())),
    };
    let same = matches!(&current, Ok(Some(stored)) if stored.started_at_ms == record.started_at_ms);
    let mut writes = Vec::new();
    if same {
        let value = record.to_bytes().map_err(|error| error.to_string())?;
        writes.push((
            STORAGE_CHECK_KEYSPACE.to_string(),
            key.clone(),
            value.into(),
        ));
    }
    let batch = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    let written = match (current, storage.send_storage_effect(batch).await) {
        (Err(error), _) => Err(error),
        (_, Event::Storage(StorageEvent::BatchWriteResult { .. })) => Ok(()),
        (_, other) => Err(format!("could not write check progress: {}", other.label())),
    };
    let finished = written.is_ok() && same && record.finished_at_ms.is_some();
    let left = match finished {
        true => {
            let delete = StorageEffect::Delete {
                key_space: CHECK_QUEUE_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            };
            match storage.send_storage_effect(delete).await {
                Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
                other => Err(format!(
                    "could not leave the check queue: {}",
                    other.label()
                )),
            }
        }
        false => written,
    };
    if let Err(error) = left {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Err(error);
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(format!("check progress was not stored: {}", other.label())),
    }
}

/// One page of a bucket's broken versions after `after`, and the key the next page starts after.
pub async fn list_broken(
    context: &DriverContext,
    bucket: &str,
    after: Option<Key>,
    limit: usize,
) -> Result<(Vec<(VersionKey, BrokenVersion)>, Option<Key>), String> {
    let prefix = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (rows, next) = crate::storage_read::iter_prefix_page(
        &context.storage_handle,
        BROKEN_VERSION_KEYSPACE,
        Some(prefix.into()),
        after,
        limit,
        None,
    )
    .await?;
    let mut broken = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let key = VersionKey::from_bytes(key.as_ref()).map_err(|error| error.to_string())?;
        let row = BrokenVersion::from_bytes(value.as_ref()).map_err(|error| error.to_string())?;
        broken.push((key, row));
    }
    Ok((broken, next))
}

/// What one cleanup request did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Cleaned {
    pub deleted: u64,
    /// Recorded versions that changed since the check, or whose delete failed; their rows stay.
    pub kept: u64,
    /// Recorded versions the caller may not delete; their rows stay.
    pub denied: u64,
    /// The key the next request starts after; `None` after the last page.
    pub next: Option<Key>,
    /// The scan budget ran out before one row the caller may delete was found.
    pub exhausted: bool,
}

/// Deletes up to `limit` recorded broken versions after `after`, authorized for `auth` per path,
/// each only while its record is still the recorded one. At most `scan` rows are read;
/// denied rows do not count toward `limit` and never name `next`.
pub async fn clean_broken(
    context: &DriverContext,
    template: &crate::s3::object::delete::DeleteObjectInput,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    after: Option<Key>,
    limit: usize,
    scan: usize,
) -> Result<Cleaned, String> {
    let (mut after, mut handled, mut examined) = (after, 0, 0);
    let mut last = None;
    let mut cleaned = Cleaned::default();
    loop {
        if examined == scan {
            cleaned.exhausted = handled == 0;
            cleaned.next = last;
            return Ok(cleaned);
        }
        let page = limit.min(scan - examined);
        let (rows, next) = list_broken(context, &template.bucket, after.take(), page).await?;
        for (key, row) in rows {
            if handled == limit {
                cleaned.next = last;
                return Ok(cleaned);
            }
            examined += 1;
            if clean_one(context, template, auth, &key, row, &mut cleaned).await? {
                handled += 1;
                last = Some(key.to_bytes().map_err(|error| error.to_string())?.into());
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(cleaned),
        }
    }
}

/// Cleans one recorded version; `false` when the caller may not delete it.
async fn clean_one(
    context: &DriverContext,
    template: &crate::s3::object::delete::DeleteObjectInput,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    key: &VersionKey,
    row: BrokenVersion,
    cleaned: &mut Cleaned,
) -> Result<bool, String> {
    use crate::auth::error::AuthorizeError;
    use crate::auth::request_policy::PolicyRequestExtras;
    use crate::s3::object::delete::{DeleteObjectError, DeleteObjectInput, DeleteObjectOperation};
    use aruna_core::structs::identity::auth::Permission;
    let path = aruna_core::structs::storage::blob::object_permission_path(
        template.realm_id,
        template.group_id,
        template.node_id,
        &key.bucket,
        &key.key,
    );
    let extras = PolicyRequestExtras::rest();
    match drive(
        AuthorizeOperation::new(template.realm_id, auth, &path, &Permission::WRITE, extras),
        context,
    )
    .await
    {
        Ok(()) => {}
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            cleaned.denied += 1;
            return Ok(false);
        }
        Err(error) => return Err(error.to_string()),
    }
    let input = DeleteObjectInput {
        bucket: key.bucket.clone(),
        key: key.key.clone(),
        version_id: Some(key.version_id),
        group_id: template.group_id,
        realm_id: template.realm_id,
        node_id: template.node_id,
        deleted_by: template.deleted_by,
    };
    let operation = DeleteObjectOperation::new(input)
        .with_restrictions(auth.path_restrictions.clone())
        .with_expected(row.version);
    // A version deleted meanwhile only leaves its row behind.
    let deleted = match crate::driver::drive(operation, context).await {
        Ok(_) => true,
        Err(DeleteObjectError::NoSuchVersion) => false,
        Err(error) => {
            if error != DeleteObjectError::VersionChanged {
                tracing::warn!(bucket = %key.bucket, %error, "Failed to delete a broken version");
            }
            cleaned.kept += 1;
            return Ok(true);
        }
    };
    let delete = StorageEffect::Delete {
        key_space: BROKEN_VERSION_KEYSPACE.to_string(),
        key: key.to_bytes().map_err(|error| error.to_string())?.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::DeleteResult { .. }) => cleaned.deleted += u64::from(deleted),
        other => {
            return Err(format!(
                "could not clear a broken version: {}",
                other.label()
            ));
        }
    }
    Ok(true)
}

/// Whether `key` names the current version of its object.
pub async fn is_current(context: &DriverContext, key: &VersionKey) -> Result<bool, String> {
    let head = BlobHeadKey::new(key.bucket.clone(), key.key.clone());
    let read = StorageEffect::Read {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: head.to_bytes().map_err(|error| error.to_string())?.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            Some(value) => CurrentVersionPointer::from_bytes(value.as_ref())
                .map(|pointer| pointer.version_id == key.version_id)
                .map_err(|error| error.to_string()),
            None => Ok(false),
        },
        other => Err(format!("could not read a head: {}", other.label())),
    }
}
