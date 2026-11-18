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

impl Operation for StoreCheckOperation {
    type Output = ();
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(read_row(
            STORAGE_CHECK_KEYSPACE,
            self.key.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            StoreStep::Current => {
                let stored = decode_check(result.value()?)?;
                let started = self.record.started_at_ms;
                if stored.is_none_or(|stored| stored.started_at_ms != started) {
                    return Ok(Next::Done(()));
                }
                self.step = StoreStep::Written;
                let row = (
                    STORAGE_CHECK_KEYSPACE.to_string(),
                    self.key.clone(),
                    self.record.to_bytes()?.into(),
                );
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            StoreStep::Written => {
                result.done()?;
                if self.record.finished_at_ms.is_none() {
                    return Ok(Next::Done(()));
                }
                self.step = StoreStep::Left;
                let row = (CHECK_QUEUE_KEYSPACE.to_string(), self.key.clone());
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
            StoreStep::Left => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

fn broken_rows(
    rows: Vec<(Key, Value)>,
) -> Result<VecDeque<(VersionKey, BrokenVersion)>, MigrationError> {
    let decode = |(key, value): (Key, Value)| -> Result<_, MigrationError> {
        let key = VersionKey::from_bytes(key.as_ref())?;
        Ok((key, BrokenVersion::from_bytes(value.as_ref())?))
    };
    rows.into_iter().map(decode).collect()
}

fn broken_page(bucket: &str, after: Option<Key>, limit: usize) -> Result<IoCall, MigrationError> {
    Ok(IoCall::Page {
        space: BROKEN_VERSION_KEYSPACE.to_string(),
        prefix: VersionKey::bucket_prefix(bucket)?.into(),
        after,
        limit,
    })
}

/// One page of a bucket's broken versions and the key the next page starts after.
pub type BrokenPage = (Vec<(VersionKey, BrokenVersion)>, Option<Key>);

/// Lists one page of a bucket's broken versions after `after`.
#[derive(Debug, PartialEq)]
pub struct ListBrokenOperation {
    bucket: String,
    after: Option<Key>,
    limit: usize,
}

impl ListBrokenOperation {
    pub fn new(bucket: &str, after: Option<Key>, limit: usize) -> Self {
        Self {
            bucket: bucket.to_string(),
            after,
            limit,
        }
    }
}

impl Operation for ListBrokenOperation {
    type Output = BrokenPage;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<BrokenPage> {
        let page = broken_page(&self.bucket, self.after.take(), self.limit)?;
        Ok(Next::Call(page))
    }

    fn next(&mut self, result: IoResult) -> Turn<BrokenPage> {
        let (rows, next) = result.page()?;
        Ok(Next::Done((broken_rows(rows)?.into(), next)))
    }
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

#[derive(Debug, PartialEq)]
enum CleanStep {
    Page,
    Authorize(VersionKey, BrokenVersion),
    Delete(VersionKey),
    Cleared(VersionKey, bool),
}

/// The bucket and the actor a cleanup deletes for.
#[derive(Debug, PartialEq)]
struct CleanScope {
    bucket: String,
    group_id: GroupId,
    realm_id: RealmId,
    node_id: NodeId,
    deleted_by: UserId,
    restrictions: Option<Vec<PathRestriction>>,
}

/// Deletes up to `limit` recorded broken versions after `after`, authorized for `auth` per path,
/// each only while its record is still the recorded one. At most `scan` rows are read;
/// denied rows do not count toward `limit` and never name `next`.
#[derive(Debug, PartialEq)]
pub struct CleanBrokenOperation {
    scope: CleanScope,
    auth: AuthContext,
    after: Option<Key>,
    limit: usize,
    scan: usize,
    step: CleanStep,
    rows: VecDeque<(VersionKey, BrokenVersion)>,
    more: Option<Key>,
    handled: usize,
    examined: usize,
    last: Option<Key>,
    cleaned: Cleaned,
}

impl CleanBrokenOperation {
    pub fn new(
        template: &DeleteObjectInput,
        auth: &AuthContext,
        after: Option<Key>,
        (limit, scan): (usize, usize),
    ) -> Self {
        Self {
            scope: CleanScope {
                bucket: template.bucket.clone(),
                group_id: template.group_id,
                realm_id: template.realm_id,
                node_id: template.node_id,
                deleted_by: template.deleted_by,
                restrictions: auth.path_restrictions.clone(),
            },
            auth: auth.clone(),
            after,
            limit,
            scan,
            step: CleanStep::Page,
            rows: VecDeque::new(),
            more: None,
            handled: 0,
            examined: 0,
            last: None,
            cleaned: Cleaned::default(),
        }
    }

    fn finish(&mut self) -> Turn<Cleaned> {
        Ok(Next::Done(std::mem::take(&mut self.cleaned)))
    }

    fn read_page(&mut self) -> Turn<Cleaned> {
        if self.examined == self.scan {
            self.cleaned.exhausted = self.handled == 0;
            self.cleaned.next = self.last.take();
            return self.finish();
        }
        let page = self.limit.min(self.scan - self.examined);
        self.step = CleanStep::Page;
        let call = broken_page(&self.scope.bucket, self.after.take(), page)?;
        Ok(Next::Call(call))
    }

    fn next_row(&mut self) -> Turn<Cleaned> {
        let Some((key, row)) = self.rows.pop_front() else {
            return match self.more.take() {
                Some(next) => {
                    self.after = Some(next);
                    self.read_page()
                }
                None => self.finish(),
            };
        };
        if self.handled == self.limit {
            self.cleaned.next = self.last.take();
            return self.finish();
        }
        self.examined += 1;
        let scope = &self.scope;
        let path = object_permission_path(
            scope.realm_id,
            scope.group_id,
            scope.node_id,
            &key.bucket,
            &key.key,
        );
        let authorize = AuthorizeOperation::new(
            scope.realm_id,
            &self.auth,
            &path,
            &Permission::WRITE,
            PolicyRequestExtras::rest(),
        );
        self.step = CleanStep::Authorize(key, row);
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn delete(&mut self, key: VersionKey, row: BrokenVersion) -> Turn<Cleaned> {
        let scope = &self.scope;
        let input = DeleteObjectInput {
            bucket: key.bucket.clone(),
            key: key.key.clone(),
            version_id: Some(key.version_id),
            group_id: scope.group_id,
            realm_id: scope.realm_id,
            node_id: scope.node_id,
            deleted_by: scope.deleted_by,
        };
        let delete = DeleteObjectOperation::new(input)
            .with_restrictions(scope.restrictions.clone())
            .with_expected(row.version);
        self.step = CleanStep::Delete(key);
        Ok(Next::Call(IoCall::child(delete)))
    }

    fn handled_row(&mut self, key: &VersionKey) -> Turn<Cleaned> {
        self.handled += 1;
        self.last = Some(key.to_bytes()?.into());
        self.next_row()
    }

    /// A version deleted meanwhile only leaves its row behind.
    fn deleted(
        &mut self,
        key: VersionKey,
        outcome: Result<DeleteObjectResult, DeleteObjectError>,
    ) -> Turn<Cleaned> {
        let deleted = match outcome {
            Ok(_) => true,
            Err(DeleteObjectError::NoSuchVersion) => false,
            Err(error) => {
                if error != DeleteObjectError::VersionChanged {
                    tracing::warn!(bucket = %key.bucket, %error, "Failed to delete a broken version");
                }
                self.cleaned.kept += 1;
                return self.handled_row(&key);
            }
        };
        let row = (BROKEN_VERSION_KEYSPACE.to_string(), key.to_bytes()?.into());
        self.step = CleanStep::Cleared(key, deleted);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for CleanBrokenOperation {
    type Output = Cleaned;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Cleaned> {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn<Cleaned> {
        match std::mem::replace(&mut self.step, CleanStep::Page) {
            CleanStep::Page => {
                let (rows, more) = result.page()?;
                self.rows = broken_rows(rows)?;
                self.more = more;
                self.next_row()
            }
            CleanStep::Authorize(key, row) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.delete(key, row),
                Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
                    self.cleaned.denied += 1;
                    self.next_row()
                }
                Err(error) => Err(MigrationError::failed(error)),
            },
            CleanStep::Delete(key) => self.deleted(key, result.child()?),
            CleanStep::Cleared(key, deleted) => {
                result.done()?;
                self.cleaned.deleted += u64::from(deleted);
                self.handled_row(&key)
            }
        }
    }
}

/// Whether `key` names the current version of its object.
#[derive(Debug, PartialEq)]
pub struct IsCurrentOperation {
    key: VersionKey,
}

impl IsCurrentOperation {
    pub fn new(key: &VersionKey) -> Self {
        Self { key: key.clone() }
    }
}

impl Operation for IsCurrentOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        let head = BlobHeadKey::new(self.key.bucket.clone(), self.key.key.clone());
        Ok(Next::Call(read_row(BLOB_HEAD_KEYSPACE, head.to_bytes()?)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        let current = match result.value()? {
            Some(value) => {
                CurrentVersionPointer::from_bytes(value.as_ref())?.version_id == self.key.version_id
            }
            None => false,
        };
        Ok(Next::Done(current))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use ulid::Ulid;

    fn empty_page() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    fn cleaner(limit: usize, scan: usize) -> CleanBrokenOperation {
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let template = DeleteObjectInput {
            bucket: "bucket".to_string(),
            key: String::new(),
            version_id: None,
            group_id: Ulid::from_bytes([5; 16]),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[6; 32]).public(),
            deleted_by: user_id,
        };
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        CleanBrokenOperation::new(&template, &auth, None, (limit, scan))
    }

    #[test]
    fn empty_queue_ends() {
        let mut checks = ProcessChecksOperation::new();
        assert!(matches!(
            checks.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        assert_eq!(checks.next(empty_page()), Ok(Next::Done(None)));
    }

    #[test]
    fn finished_check_skipped() {
        let mut checks = ProcessChecksOperation::new();
        checks.first().unwrap();
        let row = (Key::from(b"bucket".to_vec()), Value::from(Vec::new()));
        let page = IoResult::Page {
            rows: vec![row],
            next: None,
        };
        assert!(matches!(checks.next(page), Ok(Next::Call(IoCall::Read(_)))));
        let mut record = StorageCheck::new(1);
        record.finished_at_ms = Some(2);
        let value = Value::from(record.to_bytes().unwrap());
        let answer = IoResult::Values(vec![Some(value)]);
        assert_eq!(checks.next(answer), Ok(Next::Done(None)));
    }

    #[test]
    fn last_page_finishes() {
        let mut page = CheckPageOperation::new("bucket".to_string(), StorageCheck::new(1));
        page.stamp(9);
        page.first().unwrap();
        let turn = page.next(empty_page());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(page.record.finished_at_ms, Some(9));
        let stored: Result<(), MigrationError> = Ok(());
        let answer = IoResult::Child(ChildOutcome::new(stored));
        assert_eq!(page.next(answer), Ok(Next::Done(false)));
    }

    #[test]
    fn replaced_check_kept() {
        let mut store = StoreCheckOperation::new("bucket", StorageCheck::new(1));
        store.first().unwrap();
        let newer = Value::from(StorageCheck::new(2).to_bytes().unwrap());
        let answer = IoResult::Values(vec![Some(newer)]);
        assert_eq!(store.next(answer), Ok(Next::Done(())));
    }

    #[test]
    fn finished_check_leaves() {
        let mut record = StorageCheck::new(1);
        record.finished_at_ms = Some(2);
        let stored = Value::from(record.to_bytes().unwrap());
        let mut store = StoreCheckOperation::new("bucket", record);
        store.first().unwrap();
        let turn = store.next(IoResult::Values(vec![Some(stored)]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        let turn = store.next(IoResult::Done);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert_eq!(store.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn spent_scan_exhausted() {
        let done = cleaner(10, 0).first();
        let expected = Cleaned {
            exhausted: true,
            ..Cleaned::default()
        };
        assert_eq!(done, Ok(Next::Done(expected)));
    }

    #[test]
    fn empty_page_cleans_nothing() {
        let mut clean = cleaner(10, 5);
        assert!(matches!(clean.first(), Ok(Next::Call(IoCall::Page { .. }))));
        assert_eq!(clean.next(empty_page()), Ok(Next::Done(Cleaned::default())));
    }

    #[test]
    fn missing_head_not_current() {
        let key = VersionKey::new("bucket", "key", Ulid::from_bytes([7; 16]));
        let mut current = IsCurrentOperation::new(&key);
        assert!(matches!(current.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = IoResult::Values(vec![None]);
        assert_eq!(current.next(answer), Ok(Next::Done(false)));
    }
}
