//! The steps of purging what a member bucket holds of a range, and of clearing a set's rows, as
//! operations: bounded pages from durable cursors, each page checking the bucket is still bound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::key_index::{IndexEffect, IndexError, IndexEvent};
use aruna_core::keyspaces::{
    ATTACHMENT_KEYSPACE, BOOTSTRAP_KEYSPACE, CACHE_ENTRY_KEYSPACE, CACHE_ORDER_KEYSPACE,
    CAPACITY_RESERVATION_KEYSPACE, CONTENT_INDEX_KEYSPACE, DISTRIBUTION_FENCE_KEYSPACE,
    DISTRIBUTION_PURGE_KEYSPACE, INVENTORY_KEYSPACE, KEY_SAMPLE_KEYSPACE, NODE_STATE_KEYSPACE,
    RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE, RANGE_INDEX_KEYSPACE, SEARCH_FEED_KEYSPACE,
    SEARCH_INDEX_KEYSPACE, SOURCE_ROOT_KEYSPACE, UPLOAD_INVENTORY_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, read_row, value, values};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BackendLocation, BucketInfo, HiddenBlobKey};
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, account_key,
};
use aruna_core::structs::storage::distribution::fence::{ClearCursor, LocalFence, PurgeCursor};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, Position, VersionEntry, content_key, head_position, index_key, range_interval,
};
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CACHE_TOTALS_KEY, CacheEntry, CacheTotals, attachment_key,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::feed_walks::split_key;
use super::imports::{jobs_key, resume_prefix};
use super::inventory::{Walk, route};
use crate::metadata::call_results::page_rows;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::multipart::abort::{AbortUploadError, AbortUploadInput, AbortUploadOperation};
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use crate::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
    ListVersionsResult,
};
use crate::s3::usage::{MarkDirtyError, MarkDirtyOperation, dirty_rows};

/// Inventory rows one page purges at most.
const PAGE_ROWS: usize = 64;
const PAGE_BYTES: usize = 1 << 20;

/// The keyspaces a set's local rows are cleared from, in step order after the attachments.
pub(super) const CLEARED: [&str; 14] = [
    INVENTORY_KEYSPACE,
    UPLOAD_INVENTORY_KEYSPACE,
    VERSION_INDEX_KEYSPACE,
    RANGE_INDEX_KEYSPACE,
    RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE,
    KEY_SAMPLE_KEYSPACE,
    BOOTSTRAP_KEYSPACE,
    SOURCE_ROOT_KEYSPACE,
    CONTENT_INDEX_KEYSPACE,
    SEARCH_FEED_KEYSPACE,
    SEARCH_INDEX_KEYSPACE,
    CAPACITY_RESERVATION_KEYSPACE,
    DISTRIBUTION_PURGE_KEYSPACE,
];

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

/// Why a purge step stopped.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{0}")]
pub(crate) struct PurgeError(pub(crate) String);

impl From<IoError> for PurgeError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn fault(error: impl std::fmt::Display) -> PurgeError {
    PurgeError(error.to_string())
}

fn delete_call(rows: Vec<(String, Key)>) -> IoCall {
    IoCall::Delete(rows)
}

fn write_call(key_space: &str, key: Vec<u8>, value: Vec<u8>) -> IoCall {
    IoCall::Write(vec![(key_space.to_string(), key.into(), value.into())])
}

/// The read of one page of the range's rows in `key_space`.
fn range_page(target: &PurgeTarget, key_space: &str, after: Option<Vec<u8>>) -> IoCall {
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
    IoCall::Effect(Box::new(Effect::Storage(read)))
}

fn bucket_call(bucket: &str) -> IoCall {
    IoCall::child(GetBucketOperation::new(bucket.to_string()))
}

/// The bucket's binding, `None` for an absent bucket.
fn bucket_binding(result: IoResult) -> Result<Option<DistributionBinding>, PurgeError> {
    match result.child::<BucketInfo, GetBucketError>()? {
        Ok(info) => Ok(info.distribution),
        Err(GetBucketError::NotFound) => Ok(None),
        Err(error) => Err(fault(error)),
    }
}

/// Whether the read bucket is still the member bucket of `binding`.
fn still_bound(result: IoResult, binding: &DistributionBinding) -> Result<bool, PurgeError> {
    Ok(bucket_binding(result)?.is_some_and(|bound| {
        (bound.set_id, bound.member_id, bound.incarnation)
            == (binding.set_id, binding.member_id, binding.incarnation)
    }))
}

fn step_done(result: IoResult) -> Result<(), PurgeError> {
    result.child::<(), PurgeError>()?
}

fn delete_version(target: &PurgeTarget, key: String, version_id: Ulid, inventory: bool) -> IoCall {
    let input = DeleteObjectInput {
        bucket: target.bucket.clone(),
        key,
        version_id: Some(version_id),
        group_id: target.group_id,
        realm_id: target.realm_id,
        node_id: target.node_id,
        deleted_by: UserId::nil(target.realm_id),
    };
    let mut delete = DeleteObjectOperation::new(input).purging(target.binding.clone());
    if inventory {
        delete = delete.in_inventory();
    }
    IoCall::child(delete)
}

fn version_deleted(result: IoResult) -> Result<(), PurgeError> {
    match result.child::<DeleteObjectResult, DeleteObjectError>()? {
        Ok(_) | Err(DeleteObjectError::NoSuchVersion) => Ok(()),
        Err(error) => Err(PurgeError(format!(
            "a purged version was not deleted: {error}"
        ))),
    }
}

fn prune_index(set_id: Ulid, range_id: Ulid) -> IoCall {
    let prune = IndexEffect::Prune {
        set_id,
        range_id,
        keep: None,
    };
    IoCall::Effect(Box::new(Effect::Index(Box::new(prune))))
}

/// A node without range indexes has none to remove.
fn index_pruned(result: IoResult) -> Result<(), PurgeError> {
    match result.event()? {
        Event::Index(IndexEvent::Pruned | IndexEvent::Error(IndexError::Unavailable)) => Ok(()),
        Event::Index(IndexEvent::Error(error)) => Err(fault(error)),
        other => Err(PurgeError(format!(
            "unexpected index answer {}",
            other.label()
        ))),
    }
}

/// One row, or `None` when absent.
#[derive(Debug, PartialEq)]
pub(super) struct ReadRowOperation {
    pub(super) key_space: String,
    pub(super) key: Vec<u8>,
}

impl Operation for ReadRowOperation {
    type Output = Option<Value>;
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<Option<Value>>, PurgeError> {
        let key = std::mem::take(&mut self.key);
        Ok(Next::Call(read_row(&self.key_space, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Value>>, PurgeError> {
        Ok(Next::Done(value(result)?))
    }
}

/// Deletes the given rows together.
#[derive(Debug, PartialEq)]
pub(super) struct DeleteRowsOperation {
    pub(super) deletes: Vec<(String, Key)>,
}

impl Operation for DeleteRowsOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        Ok(Next::Call(delete_call(std::mem::take(&mut self.deletes))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

/// Deletes every row under `prefix` in bounded batches.
#[derive(Debug, PartialEq)]
pub(super) struct ClearPrefixOperation {
    key_space: String,
    prefix: Vec<u8>,
    deleting: bool,
}

impl ClearPrefixOperation {
    pub(super) fn new(key_space: &str, prefix: Vec<u8>) -> Self {
        Self {
            key_space: key_space.to_string(),
            prefix,
            deleting: false,
        }
    }

    fn page(&mut self) -> Next<()> {
        self.deleting = false;
        Next::Call(IoCall::Page {
            space: self.key_space.clone(),
            prefix: self.prefix.clone().into(),
            after: None,
            limit: PAGE_ROWS * 16,
        })
    }
}

impl Operation for ClearPrefixOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        if self.deleting {
            done(result)?;
            return Ok(self.page());
        }
        let (rows, _) = result.page()?;
        if rows.is_empty() {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        let deletes = (rows.into_iter())
            .map(|(key, _)| (self.key_space.clone(), key))
            .collect();
        Ok(Next::Call(delete_call(deletes)))
    }
}

/// Deletes a hidden kept copy; a copy that stays is left to the hidden sweep.
#[derive(Debug, PartialEq)]
pub(super) struct DropCopyOperation {
    location: BackendLocation,
}

impl DropCopyOperation {
    pub(super) fn new(location: BackendLocation) -> Self {
        Self { location }
    }
}

impl Operation for DropCopyOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        let Ok(key) = HiddenBlobKey::try_from(&self.location) else {
            return Ok(Next::Done(()));
        };
        let delete = Effect::Blob(BlobEffect::DeleteHidden { key });
        Ok(Next::Call(IoCall::Effect(Box::new(delete))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        if let Event::Blob(BlobEvent::Error(error)) = result.event()? {
            warn!(%error, "A dropped kept copy is left to the hidden sweep");
        }
        Ok(Next::Done(()))
    }
}

/// Removes a copy-on-use attachment of the version and its bytes.
#[derive(Debug, PartialEq)]
struct DropAttachmentOperation {
    key: Vec<u8>,
    location: Option<BackendLocation>,
    deleted: bool,
}

impl DropAttachmentOperation {
    fn new(set_id: Ulid, version_id: Ulid) -> Self {
        Self {
            key: attachment_key(set_id, version_id),
            location: None,
            deleted: false,
        }
    }
}

impl Operation for DropAttachmentOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        Ok(Next::Call(read_row(ATTACHMENT_KEYSPACE, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        if self.deleted {
            step_done(result)?;
            return Ok(Next::Done(()));
        }
        if let Some(location) = self.location.take() {
            done(result)?;
            self.deleted = true;
            return Ok(Next::Call(IoCall::child(DropCopyOperation::new(location))));
        }
        let Some(stored) = value(result)? else {
            return Ok(Next::Done(()));
        };
        let attachment = Attachment::from_bytes(&stored).map_err(fault)?;
        self.location = Some(attachment.location);
        let row = (ATTACHMENT_KEYSPACE.to_string(), self.key.clone().into());
        Ok(Next::Call(delete_call(vec![row])))
    }
}

/// Removes the cache entry of a logical identity with its order row, and its bytes after the
/// commit. The cache is shared by identity, so another set fills it again on use.
#[derive(Debug, PartialEq)]
struct DropCachedOperation {
    identity: [u8; 32],
    state: CachedState,
}

#[derive(Debug, PartialEq)]
enum CachedState {
    Read,
    Delete { totals: Vec<u8>, entry: CacheEntry },
    Write { location: BackendLocation },
}

impl DropCachedOperation {
    fn new(identity: [u8; 32]) -> Self {
        Self {
            identity,
            state: CachedState::Read,
        }
    }

    fn entry_key(&self) -> (String, Key) {
        let key = self.identity.to_vec().into();
        (CACHE_ENTRY_KEYSPACE.to_string(), key)
    }

    fn plan(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        let [entry, totals] = values::<2>(result)?;
        let entry = entry.map(|value| CacheEntry::from_bytes(&value));
        let Some(entry) = entry.transpose().ok().flatten() else {
            return Ok(Next::Done(()));
        };
        let mut totals = (totals.map(|value| CacheTotals::from_bytes(&value)))
            .transpose()
            .map_err(fault)?
            .unwrap_or_default();
        totals.bytes = totals.bytes.saturating_sub(entry.size);
        totals.entries = totals.entries.saturating_sub(1);
        let order = (
            CACHE_ORDER_KEYSPACE.to_string(),
            entry.order.to_bytes().to_vec().into(),
        );
        let deletes = vec![self.entry_key(), order];
        self.state = CachedState::Delete {
            totals: totals.to_bytes().map_err(fault)?,
            entry,
        };
        Ok(Next::Call(delete_call(deletes)))
    }
}

impl Operation for DropCachedOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        let totals = (
            CACHE_ENTRY_KEYSPACE.to_string(),
            CACHE_TOTALS_KEY.to_vec().into(),
        );
        Ok(Next::Call(IoCall::Read(vec![self.entry_key(), totals])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        match std::mem::replace(&mut self.state, CachedState::Read) {
            CachedState::Read => self.plan(result),
            CachedState::Delete { totals, entry } => {
                done(result)?;
                self.state = CachedState::Write {
                    location: entry.location,
                };
                let write = write_call(CACHE_ENTRY_KEYSPACE, CACHE_TOTALS_KEY.to_vec(), totals);
                Ok(Next::Call(write))
            }
            CachedState::Write { location } => {
                done(result)?;
                Ok(Next::Then {
                    output: (),
                    calls: vec![IoCall::child(DropCopyOperation::new(location))],
                })
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Deletes a page's rows and takes the inventory rows its version deletes left off the range's
/// counter rows, in one transaction.
#[derive(Debug, PartialEq)]
struct DebitRowsOperation {
    set_id: Ulid,
    range_id: Ulid,
    group_id: GroupId,
    deletes: Vec<(String, Key)>,
    left: Vec<Vec<u8>>,
    deltas: Vec<(Key, RangeCounters)>,
    state: DebitState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DebitState {
    Read,
    Delete,
    Count,
    Mark,
}

impl DebitRowsOperation {
    fn new(target: &PurgeTarget, deletes: Vec<(String, Key)>, left: Vec<Vec<u8>>) -> Self {
        Self {
            set_id: target.binding.set_id,
            range_id: target.range_id,
            group_id: target.group_id,
            deletes,
            left,
            deltas: Vec::new(),
            state: DebitState::Read,
        }
    }

    /// Finds what the rows still present count for and deletes the page.
    fn plan(&mut self, stored: Vec<Option<Value>>) -> Result<IoCall, PurgeError> {
        let mut stored = stored.into_iter();
        let account = (stored.next().flatten().as_deref())
            .map(RangeAccount::from_bytes)
            .transpose()
            .map_err(fault)?;
        let mut removed = Vec::new();
        for (row, value) in std::mem::take(&mut self.left).into_iter().zip(stored) {
            // A row a version delete already took is gone with its counters.
            let Some(value) = value else {
                continue;
            };
            let position = Position::decode(&row).map_err(fault)?;
            let counted = match position.entry {
                Entry::Head => {
                    HeadEntry::from_bytes(&value).map(|head| RangeCounters::of_head(&head))
                }
                Entry::Version { epoch, .. } => VersionEntry::from_bytes(&value)
                    .map(|entry| RangeCounters::of_version(epoch, &entry)),
                Entry::Upload(_) => continue,
            };
            let counted = counted.map_err(fault)?.negate().map_err(fault)?;
            removed.push((Walk::Versions, row, counted));
        }
        // A cleared or retired account has no rows left to keep in step.
        let account = account.filter(|a| !matches!(a.charge, Charge::Retired { .. }));
        if let Some(account) = account {
            self.deltas = route(self.set_id, self.range_id, &account, removed).map_err(fault)?;
        }
        self.state = DebitState::Delete;
        Ok(delete_call(std::mem::take(&mut self.deletes)))
    }
}

impl Operation for DebitRowsOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        let account = account_key(self.set_id, self.range_id).into();
        let mut reads = vec![(RANGE_ACCOUNT_KEYSPACE.to_string(), account)];
        let rows = self.left.iter().cloned().map(Key::from);
        reads.extend(rows.map(|row| (INVENTORY_KEYSPACE.to_string(), row)));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        match self.state {
            DebitState::Read => Ok(Next::Call(self.plan(result.values()?)?)),
            DebitState::Delete => {
                done(result)?;
                if self.deltas.is_empty() {
                    return Ok(Next::Done(()));
                }
                self.state = DebitState::Count;
                Ok(Next::Call(IoCall::AddCounters {
                    space: RANGE_COUNTER_KEYSPACE.to_string(),
                    deltas: std::mem::take(&mut self.deltas),
                }))
            }
            DebitState::Count => {
                done(result)?;
                self.state = DebitState::Mark;
                // The share moves without the group counters, so the group is published again.
                let writes = dirty_rows([self.group_id], Ulid::generate());
                Ok(Next::Call(IoCall::Write(writes)))
            }
            DebitState::Mark => {
                done(result)?;
                // Scheduling the publish is best effort; the committed markers wait for it.
                Ok(Next::Then {
                    output: (),
                    calls: vec![IoCall::child(MarkDirtyOperation::wake())],
                })
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// What a page of keys still has to do, in order.
#[derive(Debug, PartialEq)]
enum KeyStep {
    Version { key: String, version_id: Ulid },
    Attachment(Ulid),
    Cached([u8; 32]),
    Debit,
}

/// Deletes every version of the page's keys with their kept copies, each with its inventory row
/// and range counters, then the page's other inventory and index rows.
#[derive(Debug, PartialEq)]
pub(super) struct PurgeKeysOperation {
    target: PurgeTarget,
    rows: Vec<(Vec<u8>, Value)>,
    steps: VecDeque<KeyStep>,
    current: Option<KeyStep>,
    deletes: Vec<(String, Key)>,
    left: Vec<Vec<u8>>,
}

impl PurgeKeysOperation {
    pub(super) fn new(target: PurgeTarget, rows: Vec<(Vec<u8>, Value)>) -> Self {
        Self {
            target,
            rows,
            steps: VecDeque::new(),
            current: None,
            deletes: Vec::new(),
            left: Vec::new(),
        }
    }

    fn plan(&mut self) -> Result<(), PurgeError> {
        let set_id = self.target.binding.set_id;
        for (row, value) in std::mem::take(&mut self.rows) {
            let position = Position::decode(&row).map_err(fault)?;
            if let Entry::Head = position.entry {
                let feed = head_position(set_id, &position.key).into();
                self.deletes.push((SEARCH_FEED_KEYSPACE.to_string(), feed));
            }
            if let Entry::Version { version_id, .. } = position.entry {
                self.steps.push_back(KeyStep::Version {
                    key: position.key,
                    version_id,
                });
                self.steps.push_back(KeyStep::Attachment(version_id));
                let entry = VersionEntry::from_bytes(&value).map_err(fault)?;
                if let Some(identity) = entry.identity {
                    self.steps.push_back(KeyStep::Cached(identity));
                    let content = content_key(set_id, &identity, version_id).into();
                    self.deletes
                        .push((CONTENT_INDEX_KEYSPACE.to_string(), content));
                }
                let index = index_key(set_id, version_id).into();
                self.deletes
                    .push((VERSION_INDEX_KEYSPACE.to_string(), index));
            }
            let inventory = Key::from(row.clone());
            self.deletes
                .push((INVENTORY_KEYSPACE.to_string(), inventory));
            self.left.push(row);
        }
        self.steps.push_back(KeyStep::Debit);
        Ok(())
    }

    fn advance(&mut self) -> Next<()> {
        let Some(step) = self.steps.pop_front() else {
            return Next::Done(());
        };
        let target = &self.target;
        let call = match &step {
            KeyStep::Version { key, version_id } => {
                delete_version(target, key.clone(), *version_id, true)
            }
            KeyStep::Attachment(version_id) => IoCall::child(DropAttachmentOperation::new(
                target.binding.set_id,
                *version_id,
            )),
            KeyStep::Cached(identity) => IoCall::child(DropCachedOperation::new(*identity)),
            KeyStep::Debit => {
                let deletes = std::mem::take(&mut self.deletes);
                let left = std::mem::take(&mut self.left);
                IoCall::child(DebitRowsOperation::new(target, deletes, left))
            }
        };
        self.current = Some(step);
        Next::Call(call)
    }
}

impl Operation for PurgeKeysOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        self.plan()?;
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        match self.current.take() {
            Some(KeyStep::Version { .. }) => version_deleted(result)?,
            _ => step_done(result)?,
        }
        Ok(self.advance())
    }
}

/// Aborts the uploads of a page of upload inventory rows.
#[derive(Debug, PartialEq)]
struct AbortUploadsOperation {
    bucket: String,
    rows: Vec<Vec<u8>>,
    uploads: VecDeque<(String, Ulid)>,
}

impl AbortUploadsOperation {
    fn new(bucket: String, rows: Vec<Vec<u8>>) -> Self {
        Self {
            bucket,
            rows,
            uploads: VecDeque::new(),
        }
    }

    fn next_upload(&mut self) -> Next<()> {
        let Some((key, upload_id)) = self.uploads.pop_front() else {
            return Next::Done(());
        };
        let input = AbortUploadInput {
            bucket: self.bucket.clone(),
            key,
            upload_id,
            now_ms: unix_timestamp_millis(),
        };
        Next::Call(IoCall::child(
            AbortUploadOperation::new(input).including_in_progress(),
        ))
    }
}

impl Operation for AbortUploadsOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        for row in std::mem::take(&mut self.rows) {
            let position = Position::decode(&row).map_err(fault)?;
            if let Entry::Upload(upload_id) = position.entry {
                self.uploads.push_back((position.key, upload_id));
            }
        }
        Ok(self.next_upload())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        match result.child::<(), AbortUploadError>()? {
            Ok(()) | Err(AbortUploadError::NoSuchUpload) => Ok(self.next_upload()),
            Err(error) => Err(PurgeError(format!("an upload was not aborted: {error}"))),
        }
    }
}

/// What a range's accounting rows still need.
#[derive(Debug, PartialEq)]
enum ClearStep {
    Prefix(&'static str, Vec<u8>),
    Index(Ulid),
}

/// Deletes the accounting rows this node keeps for the range: its account, counters, key
/// sample, search index and capacity reservation, in bounded batches.
#[derive(Debug, PartialEq)]
pub(super) struct ClearRangeOperation {
    set_id: Ulid,
    range_id: Ulid,
    /// The split parent read about, while the sibling's account is read.
    parent: Option<Ulid>,
    planned: bool,
    steps: VecDeque<ClearStep>,
    current: Option<ClearStep>,
}

impl ClearRangeOperation {
    pub(super) fn new(set_id: Ulid, range_id: Ulid) -> Self {
        Self {
            set_id,
            range_id,
            parent: None,
            planned: false,
            steps: VecDeque::new(),
            current: None,
        }
    }

    fn plan(&mut self, parent: Option<Ulid>) -> Next<()> {
        self.planned = true;
        // A split parent's retired rows go first, with the last of its children held here.
        for range_id in parent.into_iter().chain([self.range_id]) {
            let prefix = account_key(self.set_id, range_id);
            for key_space in [
                RANGE_COUNTER_KEYSPACE,
                KEY_SAMPLE_KEYSPACE,
                RANGE_ACCOUNT_KEYSPACE,
                SEARCH_INDEX_KEYSPACE,
                CAPACITY_RESERVATION_KEYSPACE,
            ] {
                let step = ClearStep::Prefix(key_space, prefix.clone());
                self.steps.push_back(step);
            }
            self.steps.push_back(ClearStep::Index(range_id));
        }
        self.advance()
    }

    fn advance(&mut self) -> Next<()> {
        let Some(step) = self.steps.pop_front() else {
            return Next::Done(());
        };
        let call = match &step {
            ClearStep::Prefix(key_space, prefix) => {
                IoCall::child(ClearPrefixOperation::new(key_space, prefix.clone()))
            }
            ClearStep::Index(range_id) => prune_index(self.set_id, *range_id),
        };
        self.current = Some(step);
        Next::Call(call)
    }
}

impl Operation for ClearRangeOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        let own = account_key(self.set_id, self.range_id);
        Ok(Next::Call(read_row(RANGE_ACCOUNT_KEYSPACE, own)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        if self.planned {
            match self.current.take() {
                Some(ClearStep::Index(_)) => index_pruned(result)?,
                _ => step_done(result)?,
            }
            return Ok(self.advance());
        }
        let stored = value(result)?;
        if let Some(parent) = self.parent.take() {
            // The sibling's account decides whether this is the last child held here.
            return Ok(self.plan(stored.is_none().then_some(parent)));
        }
        let account = stored.as_deref().map(RangeAccount::from_bytes);
        let charge = account.transpose().map_err(fault)?.map(|a| a.charge);
        let Some(Charge::Child {
            parent, sibling, ..
        }) = charge
        else {
            return Ok(self.plan(None));
        };
        self.parent = Some(parent);
        let other = account_key(self.set_id, sibling);
        Ok(Next::Call(read_row(RANGE_ACCOUNT_KEYSPACE, other)))
    }
}

/// What the pending call of a range purge answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeState {
    Cursor,
    Bound,
    Page,
    Aborted,
    KeysPurged,
    Cleared,
    Marked,
    Written,
}

/// Purges at most `pages` pages of the range and outputs what is left of the budget. `Done`
/// once the range holds nothing of the set anymore, or the bucket is no longer the bound member.
#[derive(Debug, PartialEq)]
pub(super) struct PurgeRangeOperation {
    target: PurgeTarget,
    pages: usize,
    key: Vec<u8>,
    cursor: PurgeCursor,
    state: RangeState,
    last: Option<Vec<u8>>,
}

impl PurgeRangeOperation {
    pub(super) fn new(target: PurgeTarget, pages: usize) -> Self {
        let key = PurgeCursor::key(target.binding.set_id, target.range_id);
        Self {
            target,
            pages,
            key,
            cursor: PurgeCursor::default(),
            state: RangeState::Cursor,
            last: None,
        }
    }

    fn call(&mut self, state: RangeState, call: IoCall) -> RangeNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    /// Starts the next page while the purge is open and the budget lasts.
    fn turn(&mut self) -> RangeNext {
        if self.cursor.done {
            return Ok(Next::Done((Purged::Done, self.pages)));
        }
        if self.pages == 0 {
            return Ok(Next::Done((Purged::More, 0)));
        }
        self.pages -= 1;
        let bucket = bucket_call(&self.target.bucket);
        self.call(RangeState::Bound, bucket)
    }

    fn write_cursor(&mut self) -> RangeNext {
        let value = self.cursor.to_bytes().map_err(fault)?;
        let write = write_call(DISTRIBUTION_PURGE_KEYSPACE, self.key.clone(), value);
        self.call(RangeState::Written, write)
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>) -> RangeNext {
        let last = rows.last().map(|(key, _)| key.to_vec());
        let rows: Vec<(Vec<u8>, Value)> = (rows.into_iter())
            .map(|(key, value)| (key.to_vec(), value))
            .collect();
        let set_id = self.target.binding.set_id;
        match (last, self.cursor.uploads_done) {
            (None, false) => {
                (self.cursor.uploads_done, self.cursor.after) = (true, None);
                self.write_cursor()
            }
            (None, true) => {
                let clear = ClearRangeOperation::new(set_id, self.target.range_id);
                self.call(RangeState::Cleared, IoCall::child(clear))
            }
            (Some(last), false) => {
                self.last = Some(last);
                let rows = rows.into_iter().map(|(key, _)| key).collect();
                let abort = AbortUploadsOperation::new(self.target.bucket.clone(), rows);
                self.call(RangeState::Aborted, IoCall::child(abort))
            }
            (Some(last), true) => {
                self.last = Some(last);
                let purge = PurgeKeysOperation::new(self.target.clone(), rows);
                self.call(RangeState::KeysPurged, IoCall::child(purge))
            }
        }
    }
}

type RangeNext = Result<Next<(Purged, usize)>, PurgeError>;

impl Operation for PurgeRangeOperation {
    type Output = (Purged, usize);
    type Error = PurgeError;

    fn first(&mut self) -> RangeNext {
        Ok(Next::Call(read_row(
            DISTRIBUTION_PURGE_KEYSPACE,
            self.key.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> RangeNext {
        match self.state {
            RangeState::Cursor => {
                let stored = value(result)?;
                self.cursor = PurgeCursor::decode(stored.as_deref()).map_err(fault)?;
                self.turn()
            }
            RangeState::Bound => {
                if !still_bound(result, &self.target.binding)? {
                    self.cursor.done = true;
                    return self.write_cursor();
                }
                let space = match self.cursor.uploads_done {
                    true => INVENTORY_KEYSPACE,
                    false => UPLOAD_INVENTORY_KEYSPACE,
                };
                let page = range_page(&self.target, space, self.cursor.after.clone());
                self.call(RangeState::Page, page)
            }
            RangeState::Page => {
                let (rows, _) = page_rows(result)?;
                self.page_read(rows)
            }
            RangeState::Aborted | RangeState::KeysPurged => {
                step_done(result)?;
                self.cursor.after = self.last.take();
                self.write_cursor()
            }
            RangeState::Cleared => {
                step_done(result)?;
                // The range leaves this node's replica share with its counter rows.
                let mark = MarkDirtyOperation::new([self.target.group_id], Ulid::generate());
                self.call(RangeState::Marked, IoCall::child(mark))
            }
            RangeState::Marked => {
                result.child::<(), MarkDirtyError>()?.map_err(fault)?;
                self.cursor.done = true;
                self.write_cursor()
            }
            RangeState::Written => {
                done(result)?;
                self.turn()
            }
        }
    }
}

/// Reads the first page of the range's open uploads, and with `abort` aborts them. Outputs
/// whether it found any, so the caller looks again until none is left.
#[derive(Debug, PartialEq)]
pub(super) struct RangeUploadsOperation {
    target: PurgeTarget,
    abort: bool,
    found: Option<bool>,
}

impl RangeUploadsOperation {
    pub(super) fn new(target: PurgeTarget, abort: bool) -> Self {
        Self {
            target,
            abort,
            found: None,
        }
    }
}

impl Operation for RangeUploadsOperation {
    type Output = bool;
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<bool>, PurgeError> {
        let page = range_page(&self.target, UPLOAD_INVENTORY_KEYSPACE, None);
        Ok(Next::Call(page))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, PurgeError> {
        if let Some(found) = self.found {
            step_done(result)?;
            return Ok(Next::Done(found));
        }
        let (rows, _) = page_rows(result)?;
        let found = !rows.is_empty();
        if !self.abort {
            return Ok(Next::Done(found));
        }
        self.found = Some(found);
        let rows = rows.into_iter().map(|(key, _)| key.to_vec()).collect();
        let abort = AbortUploadsOperation::new(self.target.bucket.clone(), rows);
        Ok(Next::Call(IoCall::child(abort)))
    }
}

/// What the pending call of [`PurgeRestOperation`] answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestState {
    Bound,
    Listed,
    Deleted,
}

/// Deletes one page of versions the inventory does not name, from the bucket's own listing, so
/// the member bucket ends up empty. The listing restarts at the first key each time.
#[derive(Debug, PartialEq)]
pub(super) struct PurgeRestOperation {
    target: PurgeTarget,
    state: RestState,
    versions: VecDeque<(String, Ulid)>,
}

impl PurgeRestOperation {
    pub(super) fn new(target: PurgeTarget) -> Self {
        Self {
            target,
            state: RestState::Bound,
            versions: VecDeque::new(),
        }
    }

    fn next_version(&mut self) -> Next<()> {
        self.state = RestState::Deleted;
        match self.versions.pop_front() {
            Some((key, version_id)) => {
                Next::Call(delete_version(&self.target, key, version_id, false))
            }
            None => Next::Done(()),
        }
    }
}

impl Operation for PurgeRestOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        Ok(Next::Call(bucket_call(&self.target.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        match self.state {
            RestState::Bound => {
                if !still_bound(result, &self.target.binding)? {
                    return Ok(Next::Done(()));
                }
                self.state = RestState::Listed;
                let list = ListVersionsOperation::new(ListVersionsInput {
                    bucket: self.target.bucket.clone(),
                    prefix: None,
                    delimiter: None,
                    key_marker: None,
                    version_id_marker: None,
                    max_keys: Some(PAGE_ROWS),
                });
                Ok(Next::Call(IoCall::child(list)))
            }
            RestState::Listed => {
                let listed = result.child::<ListVersionsResult, ListVersionsError>()?;
                for item in listed.map_err(fault)?.items {
                    let (ListVersionsItem::Version {
                        key, version_id, ..
                    }
                    | ListVersionsItem::DeleteMarker {
                        key, version_id, ..
                    }) = item;
                    self.versions.push_back((key, version_id));
                }
                Ok(self.next_version())
            }
            RestState::Deleted => {
                version_deleted(result)?;
                Ok(self.next_version())
            }
        }
    }
}

/// Removes what this node kept to resume the imports of a transfer.
#[derive(Debug, PartialEq)]
pub(super) struct DropResumeOperation {
    transfer_id: Ulid,
    state: ResumeState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResumeState {
    Read,
    Cleared,
    Deleted,
}

impl DropResumeOperation {
    pub(super) fn new(transfer_id: Ulid) -> Self {
        Self {
            transfer_id,
            state: ResumeState::Read,
        }
    }
}

impl Operation for DropResumeOperation {
    type Output = ();
    type Error = PurgeError;

    fn first(&mut self) -> Result<Next<()>, PurgeError> {
        let key = jobs_key(self.transfer_id);
        Ok(Next::Call(read_row(NODE_STATE_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeError> {
        match self.state {
            ResumeState::Read => {
                if value(result)?.is_none() {
                    return Ok(Next::Done(()));
                }
                self.state = ResumeState::Cleared;
                let prefix = resume_prefix(self.transfer_id);
                let clear = ClearPrefixOperation::new(NODE_STATE_KEYSPACE, prefix);
                Ok(Next::Call(IoCall::child(clear)))
            }
            ResumeState::Cleared => {
                step_done(result)?;
                self.state = ResumeState::Deleted;
                let key = jobs_key(self.transfer_id).into();
                let row = (NODE_STATE_KEYSPACE.to_string(), key);
                Ok(Next::Call(delete_call(vec![row])))
            }
            ResumeState::Deleted => {
                done(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// What a cleared page still has to drop before its rows go.
#[derive(Debug, PartialEq)]
enum PageDrop {
    Copy(BackendLocation),
    Index(Ulid),
}

/// What the pending call of a set clear answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClearState {
    Cursor,
    Bound,
    Page,
    Drop,
    Deleted,
    Written,
    Fence,
    Resume,
    Finished,
}

type ClearNext = Result<Next<(Purged, usize)>, PurgeError>;

/// Deletes what this node keeps of a set besides its bucket, at most `pages` pages per run from
/// a durable cursor, and outputs what is left of the budget; the fence row stays with
/// `keep_fence`. Once `bucket` is bound to the set by another binding than `clearing`, the rows
/// are that member's and the clear ends.
#[derive(Debug, PartialEq)]
pub(super) struct ClearSetOperation {
    set_id: Ulid,
    bucket: String,
    clearing: Option<DistributionBinding>,
    keep_fence: bool,
    pages: usize,
    key: Vec<u8>,
    cursor: ClearCursor,
    state: ClearState,
    space: &'static str,
    drops: VecDeque<PageDrop>,
    dropping: Option<PageDrop>,
    deletes: Vec<(String, Key)>,
    transfers: VecDeque<Ulid>,
}

impl ClearSetOperation {
    pub(super) fn new(
        (set_id, bucket, clearing): (Ulid, String, Option<DistributionBinding>),
        keep_fence: bool,
        pages: usize,
    ) -> Self {
        Self {
            set_id,
            bucket,
            clearing,
            keep_fence,
            pages,
            key: ClearCursor::key(set_id),
            cursor: ClearCursor::default(),
            state: ClearState::Cursor,
            space: ATTACHMENT_KEYSPACE,
            drops: VecDeque::new(),
            dropping: None,
            deletes: Vec::new(),
            transfers: VecDeque::new(),
        }
    }

    fn call(&mut self, state: ClearState, call: IoCall) -> ClearNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn prefix(&self) -> Vec<u8> {
        self.set_id.to_bytes().to_vec()
    }

    fn cursor_row(&self) -> (String, Key) {
        let key = self.key.clone().into();
        (DISTRIBUTION_PURGE_KEYSPACE.to_string(), key)
    }

    fn turn(&mut self) -> ClearNext {
        if usize::from(self.cursor.step) > CLEARED.len() {
            let fence = read_row(DISTRIBUTION_FENCE_KEYSPACE, self.prefix());
            return self.call(ClearState::Fence, fence);
        }
        if self.pages == 0 {
            return Ok(Next::Done((Purged::More, 0)));
        }
        self.pages -= 1;
        let bucket = bucket_call(&self.bucket);
        self.call(ClearState::Bound, bucket)
    }

    /// Whether the read bucket is bound to the set by another member or incarnation.
    fn bound_elsewhere(&self, result: IoResult) -> Result<bool, PurgeError> {
        let same = |bound: &DistributionBinding| {
            (self.clearing.as_ref()).is_some_and(|own| {
                (own.member_id, own.incarnation) == (bound.member_id, bound.incarnation)
            })
        };
        Ok(bucket_binding(result)?
            .is_some_and(|bound| bound.set_id == self.set_id && !same(&bound)))
    }

    fn read_page(&mut self) -> ClearNext {
        self.space = match self.cursor.step {
            0 => ATTACHMENT_KEYSPACE,
            step => CLEARED[usize::from(step) - 1],
        };
        // The cursor row itself sorts first among the purge cursors and is skipped.
        let start = match (
            self.cursor.after.clone(),
            self.space == DISTRIBUTION_PURGE_KEYSPACE,
        ) {
            (Some(after), _) => Some(after),
            (None, true) => Some(self.key.clone()),
            (None, false) => None,
        };
        let limit = match self.space == ATTACHMENT_KEYSPACE {
            true => PAGE_ROWS,
            false => PAGE_ROWS * 16,
        };
        let page = IoCall::Page {
            space: self.space.to_string(),
            prefix: self.prefix().into(),
            after: start.map(Key::from),
            limit,
        };
        self.call(ClearState::Page, page)
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>) -> ClearNext {
        match rows.last() {
            None => (self.cursor.step, self.cursor.after) = (self.cursor.step + 1, None),
            Some((last, _)) => self.cursor.after = Some(last.to_vec()),
        }
        for (row, value) in rows {
            if self.space == ATTACHMENT_KEYSPACE
                && let Ok(attachment) = Attachment::from_bytes(&value)
            {
                self.drops.push_back(PageDrop::Copy(attachment.location));
            }
            if self.space == SEARCH_INDEX_KEYSPACE {
                let (_, range_id) = split_key(&row).map_err(fault)?;
                self.drops.push_back(PageDrop::Index(range_id));
            }
            self.deletes.push((self.space.to_string(), row));
        }
        self.next_drop()
    }

    fn next_drop(&mut self) -> ClearNext {
        let Some(drop) = self.drops.pop_front() else {
            let deletes = std::mem::take(&mut self.deletes);
            return self.call(ClearState::Deleted, delete_call(deletes));
        };
        let call = match &drop {
            PageDrop::Copy(location) => IoCall::child(DropCopyOperation::new(location.clone())),
            PageDrop::Index(range_id) => prune_index(self.set_id, *range_id),
        };
        self.dropping = Some(drop);
        self.call(ClearState::Drop, call)
    }

    fn next_transfer(&mut self) -> ClearNext {
        if let Some(transfer_id) = self.transfers.pop_front() {
            let drop = DropResumeOperation::new(transfer_id);
            return self.call(ClearState::Resume, IoCall::child(drop));
        }
        let mut deletes = vec![self.cursor_row()];
        if !self.keep_fence {
            let fence = self.prefix().into();
            deletes.push((DISTRIBUTION_FENCE_KEYSPACE.to_string(), fence));
        }
        self.call(ClearState::Finished, delete_call(deletes))
    }
}

impl Operation for ClearSetOperation {
    type Output = (Purged, usize);
    type Error = PurgeError;

    fn first(&mut self) -> ClearNext {
        Ok(Next::Call(read_row(
            DISTRIBUTION_PURGE_KEYSPACE,
            self.key.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> ClearNext {
        match self.state {
            ClearState::Cursor => {
                let stored = value(result)?;
                self.cursor = ClearCursor::decode(stored.as_deref()).map_err(fault)?;
                self.turn()
            }
            ClearState::Bound => {
                if self.bound_elsewhere(result)? {
                    let cursor = self.cursor_row();
                    return self.call(ClearState::Finished, delete_call(vec![cursor]));
                }
                self.read_page()
            }
            ClearState::Page => {
                let (rows, _) = result.page()?;
                self.page_read(rows)
            }
            ClearState::Drop => {
                match self.dropping.take() {
                    Some(PageDrop::Index(_)) => index_pruned(result)?,
                    _ => step_done(result)?,
                }
                self.next_drop()
            }
            ClearState::Deleted => {
                done(result)?;
                let value = self.cursor.to_bytes().map_err(fault)?;
                let write = write_call(DISTRIBUTION_PURGE_KEYSPACE, self.key.clone(), value);
                self.call(ClearState::Written, write)
            }
            ClearState::Written => {
                done(result)?;
                self.turn()
            }
            ClearState::Fence => {
                let stored = value(result)?;
                let fence = LocalFence::decode(stored.as_deref()).map_err(fault)?;
                self.transfers = fence.ranges.iter().map(|f| f.transfer_id).collect();
                self.next_transfer()
            }
            ClearState::Resume => {
                step_done(result)?;
                self.next_transfer()
            }
            ClearState::Finished => {
                done(result)?;
                Ok(Next::Done((Purged::Done, self.pages)))
            }
        }
    }
}

#[cfg(test)]
#[path = "purge_steps_tests.rs"]
mod tests;
