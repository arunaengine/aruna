//! Runs the queued storage checks of buckets, lists the broken versions found and cleans them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BROKEN_VERSION_KEYSPACE, CHECK_QUEUE_KEYSPACE, STORAGE_CHECK_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, CurrentVersionPointer, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::broken::{BrokenVersion, StorageCheck};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};

use super::check::{CheckOutcome, CheckVersionError, CheckVersionOperation};
use super::drain::{BUCKET_PAGE, MIGRATION_CONTINUE, MIGRATION_PAGE};
use super::shared::MigrationError;
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};

type Turn<T> = Result<Next<T>, MigrationError>;

fn empty_prefix() -> Key {
    Key::from(Vec::new())
}

fn decode_check(value: Option<Value>) -> Result<Option<StorageCheck>, MigrationError> {
    Ok(value
        .map(|value| StorageCheck::from_bytes(value.as_ref()))
        .transpose()?)
}

#[derive(Debug, PartialEq)]
enum ChecksStep {
    Queue,
    Record(String),
    Page,
}

/// Advances the unfinished checks at the head of the queue by one page each. Outputs when the
/// task must run again.
#[derive(Debug, PartialEq)]
pub struct ProcessChecksOperation {
    step: ChecksStep,
    buckets: VecDeque<Key>,
    next: Option<Duration>,
}

impl ProcessChecksOperation {
    pub fn new() -> Self {
        Self {
            step: ChecksStep::Queue,
            buckets: VecDeque::new(),
            next: None,
        }
    }

    fn next_bucket(&mut self) -> Turn<Option<Duration>> {
        let Some(key) = self.buckets.pop_front() else {
            return Ok(Next::Done(self.next));
        };
        let bucket = String::from_utf8(key.to_vec()).map_err(MigrationError::failed)?;
        let read = read_row(STORAGE_CHECK_KEYSPACE, bucket.as_bytes().to_vec());
        self.step = ChecksStep::Record(bucket);
        Ok(Next::Call(read))
    }
}

impl Default for ProcessChecksOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for ProcessChecksOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Option<Duration>> {
        Ok(Next::Call(IoCall::Page {
            space: CHECK_QUEUE_KEYSPACE.to_string(),
            prefix: empty_prefix(),
            after: None,
            limit: BUCKET_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match std::mem::replace(&mut self.step, ChecksStep::Page) {
            ChecksStep::Queue => {
                let (rows, more) = result.page()?;
                self.next = more.map(|_| MIGRATION_CONTINUE);
                self.buckets = rows.into_iter().map(|(key, _)| key).collect();
                self.next_bucket()
            }
            ChecksStep::Record(bucket) => match decode_check(result.value()?)? {
                Some(record) if record.finished_at_ms.is_none() => {
                    let page = CheckPageOperation::new(bucket, record);
                    Ok(Next::Call(IoCall::child(page)))
                }
                _ => self.next_bucket(),
            },
            ChecksStep::Page => {
                if result.child::<bool, MigrationError>()?? {
                    self.next = Some(MIGRATION_CONTINUE);
                }
                self.next_bucket()
            }
        }
    }
}

/// Reads the progress of the check of `bucket`.
#[derive(Debug, PartialEq)]
pub struct ReadCheckOperation {
    bucket: String,
}

impl ReadCheckOperation {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
        }
    }
}

impl Operation for ReadCheckOperation {
    type Output = Option<StorageCheck>;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Option<StorageCheck>> {
        let key = self.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(STORAGE_CHECK_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<StorageCheck>> {
        Ok(Next::Done(decode_check(result.value()?)?))
    }
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Heads,
    Version,
    Store,
}

/// Checks one page of current versions and stores the progress. Outputs whether more remain.
#[derive(Debug, PartialEq)]
struct CheckPageOperation {
    bucket: String,
    record: StorageCheck,
    now_ms: u64,
    step: PageStep,
    heads: VecDeque<(Key, Value)>,
    more: bool,
}

impl CheckPageOperation {
    fn new(bucket: String, record: StorageCheck) -> Self {
        Self {
            bucket,
            record,
            now_ms: 0,
            step: PageStep::Heads,
            heads: VecDeque::new(),
            more: false,
        }
    }

    fn next_head(&mut self) -> Turn<bool> {
        let Some((key, value)) = self.heads.pop_front() else {
            if !self.more {
                self.record.finished_at_ms = Some(self.now_ms);
            }
            let store = StoreCheckOperation::new(&self.bucket, self.record.clone());
            self.step = PageStep::Store;
            return Ok(Next::Call(IoCall::child(store)));
        };
        self.record.cursor = Some(key.to_vec());
        let head = BlobHeadKey::from_bytes(key.as_ref())?;
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
        let version = VersionKey::new(head.bucket, head.key, pointer.version_id);
        self.step = PageStep::Version;
        let check = CheckVersionOperation::new(version, self.now_ms);
        Ok(Next::Call(IoCall::child(check)))
    }
}

impl Operation for CheckPageOperation {
    type Output = bool;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<bool> {
        let prefix = BlobHeadKey::bucket_prefix(&self.bucket)?.into();
        Ok(Next::Call(IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix,
            after: self.record.cursor.clone().map(Into::into),
            limit: MIGRATION_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match self.step {
            PageStep::Heads => {
                let (heads, next) = result.page()?;
                self.heads = heads.into();
                self.more = next.is_some();
                self.next_head()
            }
            PageStep::Version => {
                match result.child::<CheckOutcome, CheckVersionError>()? {
                    Ok(CheckOutcome::Broken(_)) => self.record.broken += 1,
                    Ok(_) => {}
                    Err(error) => {
                        self.record.failed += 1;
                        tracing::warn!(bucket = %self.bucket, %error, "Failed to check a version");
                    }
                }
                self.record.checked += 1;
                self.next_head()
            }
            PageStep::Store => {
                result.child::<(), MigrationError>()??;
                Ok(Next::Done(self.more))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StoreStep {
    Current,
    Written,
    Left,
}

/// Stores the progress unless a newer check replaced it; a finished check leaves the queue.
#[derive(Debug, PartialEq)]
struct StoreCheckOperation {
    key: Key,
    record: StorageCheck,
    step: StoreStep,
}

impl StoreCheckOperation {
    fn new(bucket: &str, record: StorageCheck) -> Self {
        Self {
            key: bucket.as_bytes().to_vec().into(),
            record,
            step: StoreStep::Current,
        }
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
    use crate::auth::request_authorization::authorize;
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
    match authorize(
        context,
        template.realm_id,
        auth,
        &path,
        &Permission::WRITE,
        extras,
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
