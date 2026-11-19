//! Rebuilds the node's usage counters by a paged walk over one storage snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Pages add their counts to the rebuild rows while storage adds every later delta there too,
// so the rows are exact when the walk ends and replace the live counter rows.

// Each page commit saves the position after it. Storage keeps the rows as the rebuild began,
// so a restarted rebuild reads them unchanged and resumes there, rereading only the buckets.

use std::collections::{BTreeSet, HashMap, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE, USAGE_STATS_KEYSPACE,
    prefix_upper_bound,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    VersionKey,
};
use aruna_core::structs::storage::usage::{
    RebuildProgress, USAGE_PROGRESS_KEY, UsageCounterError, UsageCounters,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::node::rebuild_counts::{
    CounterDeltas, Joined, RebuildCounts, is_pithos, named_size, usable,
};

/// Rows read per walk page; every page commits before the next one is read.
const PAGE_LIMIT: usize = 256;
/// Copies of one hash read per page when a version's own location row is unusable.
const COPY_LIMIT: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebuildStatsState {
    Init,
    ReadProgress,
    ReadConfig,
    Page,
    Joins,
    Fallback,
    /// A page books its counts as a child in its own transaction.
    Book,
    /// The parent swaps the rebuild rows in once the walk ended.
    Finish,
}

#[derive(Debug, Error, PartialEq)]
pub enum RebuildStatsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    CounterError(#[from] UsageCounterError),
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received}]")]
    InvalidStateEvent {
        state: RebuildStatsState,
        expected: &'static str,
        received: &'static str,
    },
    #[error("RebuildUsageStats failed")]
    RebuildFailed,
    #[error("another usage rebuild replaced this one")]
    Superseded,
    #[error(transparent)]
    Io(IoError),
}

/// A refused storage call keeps its storage error, as the rebuild reported it before.
impl From<IoError> for RebuildStatsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

type Walking = Result<Next<UsageCounters>, RebuildStatsError>;

/// The call that hands `effect` to storage.
fn storage(effect: StorageEffect) -> Walking {
    Ok(Next::Call(IoCall::from(Effect::Storage(effect))))
}

/// A walk position: walk index, quota bucket and the start of the next page.
type Position = (usize, usize, Option<IterStart>);

/// The walks in their order. Buckets come first for the bucket groups; the versions of
/// buckets whose group has a quota come before all other versions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Walk {
    Buckets,
    QuotaVersions,
    QuotaEnvelopes,
    Versions,
    Envelopes,
    Copies,
    Blobs,
    Pending,
    Uploads,
}

/// The walks in their order; a saved position names one by its index.
const WALKS: [Walk; 9] = [
    Walk::Buckets,
    Walk::QuotaVersions,
    Walk::QuotaEnvelopes,
    Walk::Versions,
    Walk::Envelopes,
    Walk::Copies,
    Walk::Blobs,
    Walk::Pending,
    Walk::Uploads,
];

impl Walk {
    fn index(self) -> usize {
        WALKS
            .iter()
            .position(|walk| *walk == self)
            .unwrap_or(WALKS.len())
    }

    fn key_space(self) -> &'static str {
        match self {
            Walk::Buckets => S3_BUCKET_KEYSPACE,
            Walk::QuotaVersions | Walk::Versions => BLOB_VERSIONS_KEYSPACE,
            Walk::QuotaEnvelopes | Walk::Envelopes => ABE_VERSION_KEYSPACE,
            Walk::Copies => ABE_COPY_KEYSPACE,
            Walk::Blobs => BLOB_LOCATIONS_KEYSPACE,
            Walk::Pending => PENDING_LOCATION_KEYSPACE,
            Walk::Uploads => UPLOAD_KEYSPACE,
        }
    }

    fn next(self) -> Option<Walk> {
        match self {
            Walk::Buckets => Some(Walk::QuotaVersions),
            Walk::QuotaVersions => Some(Walk::QuotaEnvelopes),
            Walk::QuotaEnvelopes => Some(Walk::Versions),
            Walk::Versions => Some(Walk::Envelopes),
            Walk::Envelopes => Some(Walk::Copies),
            Walk::Copies => Some(Walk::Blobs),
            Walk::Blobs => Some(Walk::Pending),
            Walk::Pending => Some(Walk::Uploads),
            Walk::Uploads => None,
        }
    }

    /// Walks one quota bucket at a time through its version key prefix.
    fn scoped(self) -> bool {
        matches!(self, Walk::QuotaVersions | Walk::QuotaEnvelopes)
    }

    /// Node-wide version walks that seek past the quota buckets already walked.
    fn skips_quota(self) -> bool {
        matches!(self, Walk::Versions | Walk::Envelopes)
    }

    fn versions(self) -> bool {
        matches!(self, Walk::QuotaVersions | Walk::Versions)
    }
}

/// Recomputes all usage counters without stopping writers: storage begins the rebuild and
/// answers a snapshot, every page adds its counts from that snapshot to the rebuild rows, and
/// storage swaps them in at the end. Memory grows with buckets and one page, never objects.
#[derive(Debug, Default, PartialEq)]
pub struct RebuildStatsOperation {
    realm_id: Option<RealmId>,
    /// The walk's totals while storage swaps the rebuild rows in.
    walked: Option<UsageCounters>,
}

impl RebuildStatsOperation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Walks the buckets of groups with a quota in this realm first.
    pub fn with_realm(mut self, realm_id: RealmId) -> Self {
        self.realm_id = Some(realm_id);
        self
    }
}

impl Operation for RebuildStatsOperation {
    type Output = UsageCounters;
    type Error = RebuildStatsError;

    fn first(&mut self) -> Walking {
        let walk = UsageWalkOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(walk)))
    }

    /// The walk's snapshot is closed when its child run ends, before the rows are swapped in.
    fn next(&mut self, result: IoResult) -> Walking {
        let Some(counters) = self.walked.take() else {
            self.walked = Some(result.child::<UsageCounters, RebuildStatsError>()??);
            return storage(StorageEffect::FinishUsageRebuild {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
            });
        };
        match result.event()? {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(Next::Done(counters)),
            other => Err(unexpected(
                RebuildStatsState::Finish,
                "BatchWriteResult",
                &other,
            )),
        }
    }
}

/// A refusal of `received` where `state` expects `expected`.
fn unexpected(
    state: RebuildStatsState,
    expected: &'static str,
    received: &Event,
) -> RebuildStatsError {
    RebuildStatsError::InvalidStateEvent {
        state,
        expected,
        received: received.label(),
    }
}

/// The paged walk over the rebuild's snapshot. Each page books its counts as a child, so the
/// snapshot stays open across the page commits.
#[derive(Debug, PartialEq)]
struct UsageWalkOperation {
    state: RebuildStatsState,
    realm_id: Option<RealmId>,
    walk: Walk,
    /// Start of the current page; `None` starts the walk.
    cursor: Option<IterStart>,
    /// Start of the following page; `None` once the current page ends the walk.
    next: Option<IterStart>,
    /// Quota bucket a scoped walk is in.
    scope: usize,
    rows: Vec<(Key, Value)>,
    reads: Vec<(&'static str, Vec<u8>)>,
    joined: Joined,
    /// Hashes whose size comes from any usable copy, as their named row was unusable.
    copies: VecDeque<[u8; 32]>,
    copy_cursor: Option<IterStart>,
    sizes: HashMap<[u8; 32], Option<u64>>,
    counts: RebuildCounts,
    /// The rebuild this walk belongs to, from the saved progress.
    generation: Option<Ulid>,
    /// The saved position to resume at after the bucket walk recounted the bucket map.
    resume: Option<Position>,
}

impl UsageWalkOperation {
    fn new(realm_id: Option<RealmId>) -> Self {
        Self {
            state: RebuildStatsState::Init,
            realm_id,
            walk: Walk::Buckets,
            cursor: None,
            next: None,
            scope: 0,
            rows: Vec::new(),
            reads: Vec::new(),
            joined: HashMap::new(),
            copies: VecDeque::new(),
            copy_cursor: None,
            sizes: HashMap::new(),
            counts: RebuildCounts::default(),
            generation: None,
            resume: None,
        }
    }

    fn unexpected(&self, expected: &'static str, received: Event) -> Walking {
        Err(unexpected(self.state.clone(), expected, &received))
    }

    fn scope_bucket(&self) -> Option<&String> {
        self.counts.quota_buckets.iter().nth(self.scope)
    }

    /// Takes the saved position and totals; the walk restarts at the buckets either way.
    fn handle_progress(&mut self, event: Event) -> Walking {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("ReadResult", event);
        };
        let progress = marker_progress(value)?;
        self.generation = Some(progress.generation);
        self.counts.total = progress.total;
        self.counts.orphan_versions = progress.orphan_versions;
        let start = (progress.start).map(|(key, inclusive)| match inclusive {
            true => IterStart::At(key.into()),
            false => IterStart::After(key.into()),
        });
        if progress.walk > 0 || start.is_some() {
            self.resume = Some((progress.walk.into(), progress.scope as usize, start));
        }
        let Some(realm_id) = self.realm_id else {
            return self.start_walk(Walk::Buckets);
        };
        self.state = RebuildStatsState::ReadConfig;
        storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: DocumentTarget::RealmConfig { realm_id }.storage_key(),
            txn_id: None,
        })
    }

    fn handle_config(&mut self, event: Event) -> Walking {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("ReadResult", event);
        };
        if let Some(bytes) = value {
            self.counts.quota = Some(RealmConfigDocument::from_bytes(bytes.as_ref())?.quota);
        }
        self.start_walk(Walk::Buckets)
    }

    fn start_walk(&mut self, walk: Walk) -> Walking {
        self.walk = walk;
        self.cursor = None;
        self.scope = 0;
        if walk.scoped() && self.counts.quota_buckets.is_empty() {
            return self.walk_done();
        }
        self.page_effect()
    }

    fn page_effect(&mut self) -> Walking {
        let prefix = match (self.walk.scoped(), self.scope_bucket()) {
            (true, Some(bucket)) => Some(Key::from(VersionKey::bucket_prefix(bucket)?)),
            _ => None,
        };
        self.state = RebuildStatsState::Page;
        storage(StorageEffect::Iter {
            key_space: self.walk.key_space().to_string(),
            prefix,
            start: self.cursor.clone(),
            limit: PAGE_LIMIT,
            txn_id: None,
        })
    }

    fn handle_page(&mut self, event: Event) -> Walking {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected("IterResult", event);
        };
        self.rows = values;
        self.next = next_start_after.map(IterStart::After);
        if self.walk.skips_quota() && !self.counts.quota_buckets.is_empty() {
            self.skip_quota()?;
        }
        let reads = self.page_reads()?;
        if reads.is_empty() {
            return self.book_page();
        }
        let effect = StorageEffect::BatchRead {
            reads: (reads.iter())
                .map(|(key_space, key)| (key_space.to_string(), Key::from(key.clone())))
                .collect(),
            txn_id: None,
        };
        self.reads = reads;
        self.state = RebuildStatsState::Joins;
        storage(effect)
    }

    /// Ends the page at the first row of a quota bucket and seeks past that bucket.
    fn skip_quota(&mut self) -> Result<(), RebuildStatsError> {
        for (index, (key, _)) in self.rows.iter().enumerate() {
            let version = VersionKey::from_bytes(key.as_ref())?;
            if !self.counts.quota_buckets.contains(&version.bucket) {
                continue;
            }
            let prefix = VersionKey::bucket_prefix(&version.bucket)?;
            self.next = prefix_upper_bound(&prefix).map(|end| IterStart::At(end.into()));
            self.rows.truncate(index);
            return Ok(());
        }
        Ok(())
    }

    /// Point reads in the snapshot that the page's rows join.
    fn page_reads(&self) -> Result<Vec<(&'static str, Vec<u8>)>, RebuildStatsError> {
        let mut wanted: BTreeSet<(&'static str, Vec<u8>)> = BTreeSet::new();
        match self.walk {
            Walk::QuotaVersions | Walk::Versions => {
                for (key, value) in &self.rows {
                    let version_key = VersionKey::from_bytes(key.as_ref())?;
                    let version = BlobVersion::from_bytes(value.as_ref())?;
                    let object = (version_key.bucket.clone(), version_key.key.clone());
                    let cached = self
                        .counts
                        .last_head
                        .as_ref()
                        .is_some_and(|(last, _)| *last == object);
                    if !cached {
                        let head = BlobHeadKey::new(object.0, object.1).to_bytes()?;
                        wanted.insert((BLOB_HEAD_KEYSPACE, head));
                    }
                    match version.state {
                        BlobVersionState::Materialized {
                            blob_hash,
                            backend,
                            encoding,
                            ..
                        } => {
                            let location = BlobLocationKey::new(blob_hash, encoding, backend);
                            wanted.insert((BLOB_LOCATIONS_KEYSPACE, location.to_bytes()));
                        }
                        BlobVersionState::PendingContent { archive, .. } => {
                            wanted.insert((PENDING_LOCATION_KEYSPACE, archive.to_bytes()));
                        }
                        BlobVersionState::Reference { .. } | BlobVersionState::Deleted => {}
                    }
                }
            }
            Walk::QuotaEnvelopes | Walk::Envelopes => {
                for (_, id) in &self.rows {
                    wanted.insert((ABE_ENVELOPE_KEYSPACE, id.to_vec()));
                    wanted.insert((ABE_ARCHIVE_KEYSPACE, id.to_vec()));
                }
            }
            Walk::Blobs => {
                for (_, value) in &self.rows {
                    let location = BackendLocation::from_bytes(value.as_ref())?;
                    if usable(&location) && is_pithos(&location) {
                        let archive = ArchiveKey::of(&location).to_bytes();
                        wanted.insert((PENDING_LOCATION_KEYSPACE, archive));
                    }
                }
            }
            Walk::Buckets | Walk::Copies | Walk::Pending | Walk::Uploads => {}
        }
        Ok(wanted.into_iter().collect())
    }

    fn handle_joins(&mut self, event: Event) -> Walking {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("BatchReadResult", event);
        };
        let reads = std::mem::take(&mut self.reads);
        if reads.len() != values.len() {
            return Err(RebuildStatsError::RebuildFailed);
        }
        for (read, (_, value)) in reads.into_iter().zip(values) {
            self.joined.insert(read, value);
        }
        if self.walk.versions() {
            self.queue_copies()?;
        }
        self.next_copy()
    }

    /// Queues the hashes whose named location row is missing or not a finished copy.
    fn queue_copies(&mut self) -> Result<(), RebuildStatsError> {
        for (_, value) in &self.rows {
            let version = BlobVersion::from_bytes(value.as_ref())?;
            let BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                ..
            } = version.state
            else {
                continue;
            };
            let key = BlobLocationKey::new(blob_hash, encoding, backend).to_bytes();
            if named_size(self.joined.get(&(BLOB_LOCATIONS_KEYSPACE, key)))?.is_none()
                && !self.copies.contains(&blob_hash)
            {
                self.copies.push_back(blob_hash);
            }
        }
        Ok(())
    }

    fn next_copy(&mut self) -> Walking {
        let Some(hash) = self.copies.front().copied() else {
            return self.book_page();
        };
        self.state = RebuildStatsState::Fallback;
        storage(StorageEffect::Iter {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            prefix: Some(Key::from(hash.to_vec())),
            start: self.copy_cursor.clone(),
            limit: COPY_LIMIT,
            txn_id: None,
        })
    }

    fn handle_copy(&mut self, event: Event) -> Walking {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected("IterResult", event);
        };
        let Some(hash) = self.copies.front().copied() else {
            return Err(RebuildStatsError::RebuildFailed);
        };
        let mut size = None;
        for (_, value) in &values {
            let location = BackendLocation::from_bytes(value.as_ref())?;
            if usable(&location) {
                size = Some(location.blob_size);
                break;
            }
        }
        match (size, next_start_after) {
            (None, Some(start)) => self.copy_cursor = Some(IterStart::After(start)),
            (size, _) => {
                self.sizes.insert(hash, size);
                self.copies.pop_front();
                self.copy_cursor = None;
            }
        }
        self.next_copy()
    }

    fn book_page(&mut self) -> Walking {
        self.book_rows()?;
        self.joined.clear();
        self.sizes.clear();
        if self.counts.is_empty() {
            return self.advance();
        }
        let generation = self.generation.ok_or(RebuildStatsError::RebuildFailed)?;
        let progress = self.progress()?;
        let page = BookPageOperation::new(generation, self.counts.take_deltas(), progress);
        self.state = RebuildStatsState::Book;
        Ok(Next::Call(IoCall::child(page)))
    }

    fn book_rows(&mut self) -> Result<(), RebuildStatsError> {
        for (key, value) in std::mem::take(&mut self.rows) {
            let replayed = self.walk == Walk::Buckets && self.replayed(&key);
            let (joined, sizes) = (&self.joined, &self.sizes);
            let counts = &mut self.counts;
            match self.walk {
                Walk::Buckets if replayed => {
                    counts.remember(&key, &value)?;
                }
                Walk::Buckets => counts.bucket(&key, &value)?,
                Walk::QuotaVersions | Walk::Versions => {
                    counts.version(&key, &value, joined, sizes)?
                }
                Walk::QuotaEnvelopes | Walk::Envelopes => counts.envelope(&key, &value, joined)?,
                Walk::Copies => counts.copy(&key, &value)?,
                Walk::Blobs => counts.location(&key, &value, false, joined)?,
                Walk::Pending => counts.location(&key, &value, true, joined)?,
                Walk::Uploads => counts.upload(&value),
            }
        }
        Ok(())
    }

    /// Reads the next page of the walk, or moves to the next quota bucket or walk.
    fn advance(&mut self) -> Walking {
        if let Some(next) = self.next.take() {
            self.cursor = Some(next);
            return self.page_effect();
        }
        if self.walk.scoped() && self.scope + 1 < self.counts.quota_buckets.len() {
            self.scope += 1;
            self.cursor = None;
            self.counts.last_head = None;
            return self.page_effect();
        }
        self.walk_done()
    }

    fn walk_done(&mut self) -> Walking {
        self.counts.last_head = None;
        if self.walk == Walk::Buckets
            && let Some((walk, scope, start)) = self.resume.take()
            && walk > 0
        {
            return self.resume_at(walk, scope, start);
        }
        if let Some(walk) = self.walk.next() {
            return self.start_walk(walk);
        }
        self.close_walks()
    }

    /// Continues at a saved position; past the last walk the rebuild only finishes.
    fn resume_at(&mut self, walk: usize, scope: usize, start: Option<IterStart>) -> Walking {
        let Some(walk) = WALKS.get(walk).copied() else {
            return self.close_walks();
        };
        self.walk = walk;
        self.scope = scope;
        self.cursor = start;
        if walk.scoped() && scope >= self.counts.quota_buckets.len() {
            return self.walk_done();
        }
        self.page_effect()
    }

    /// Bucket rows the saved position is past were counted before; they only fill the map.
    fn replayed(&self, key: &Key) -> bool {
        match &self.resume {
            Some((walk, _, _)) if *walk > 0 => true,
            Some((_, _, Some(IterStart::After(last)))) => key <= last,
            Some((_, _, Some(IterStart::At(first)))) => key < first,
            _ => false,
        }
    }

    /// The position after the booked page, as `advance` will take it.
    fn following(&self) -> Position {
        let walk = self.walk.index();
        if let Some(next) = &self.next {
            return (walk, self.scope, Some(next.clone()));
        }
        if self.walk.scoped() && self.scope + 1 < self.counts.quota_buckets.len() {
            return (walk, self.scope + 1, None);
        }
        (walk + 1, 0, None)
    }

    fn progress(&self) -> Result<Vec<u8>, RebuildStatsError> {
        let generation = self.generation.ok_or(RebuildStatsError::RebuildFailed)?;
        let (walk, scope, start) = self.following();
        let start = start.map(|start| match start {
            IterStart::At(key) => (key.to_vec(), true),
            IterStart::After(key) => (key.to_vec(), false),
        });
        let progress = RebuildProgress {
            generation,
            walk: u8::try_from(walk).map_err(|_| RebuildStatsError::RebuildFailed)?,
            scope: u32::try_from(scope).map_err(|_| RebuildStatsError::RebuildFailed)?,
            start,
            total: self.counts.total,
            orphan_versions: self.counts.orphan_versions,
        };
        Ok(progress.to_bytes()?)
    }

    fn close_walks(&mut self) -> Walking {
        if self.counts.orphan_versions > 0 {
            warn!(
                versions = self.counts.orphan_versions,
                "rebuilt usage omits versions without a blob location row"
            );
        }
        Ok(Next::Done(self.counts.total))
    }
}

/// The saved progress; a begun rebuild always has one.
fn marker_progress(value: Option<Value>) -> Result<RebuildProgress, RebuildStatsError> {
    let value = value.ok_or(RebuildStatsError::RebuildFailed)?;
    Ok(RebuildProgress::from_bytes(value.as_ref())?)
}

impl Operation for UsageWalkOperation {
    type Output = UsageCounters;
    type Error = RebuildStatsError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::UsageRebuild
    }

    fn first(&mut self) -> Walking {
        self.state = RebuildStatsState::ReadProgress;
        storage(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: Key::from(USAGE_PROGRESS_KEY.to_vec()),
            txn_id: None,
        })
    }

    fn next(&mut self, result: IoResult) -> Walking {
        if self.state == RebuildStatsState::Book {
            result.child::<(), RebuildStatsError>()??;
            return self.advance();
        }
        let event = result.event()?;
        match self.state {
            RebuildStatsState::ReadProgress => self.handle_progress(event),
            RebuildStatsState::ReadConfig => self.handle_config(event),
            RebuildStatsState::Page => self.handle_page(event),
            RebuildStatsState::Joins => self.handle_joins(event),
            RebuildStatsState::Fallback => self.handle_copy(event),
            _ => self.unexpected("no event", event),
        }
    }
}

/// What a page booking waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BookStep {
    Marker,
    Added,
    Saved,
}

/// Books one page in its own write transaction: only into the rebuild that the saved progress
/// still names, then its counts and the position after it, committed together.
#[derive(Debug, PartialEq)]
struct BookPageOperation {
    generation: Ulid,
    deltas: Option<CounterDeltas>,
    progress: Vec<u8>,
    step: BookStep,
}

impl BookPageOperation {
    fn new(generation: Ulid, deltas: CounterDeltas, progress: Vec<u8>) -> Self {
        Self {
            generation,
            deltas: Some(deltas),
            progress,
            step: BookStep::Marker,
        }
    }
}

impl Operation for BookPageOperation {
    type Output = ();
    type Error = RebuildStatsError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, RebuildStatsError> {
        let marker = (
            USAGE_STATS_KEYSPACE.to_string(),
            Key::from(USAGE_PROGRESS_KEY.to_vec()),
        );
        Ok(Next::Call(IoCall::Read(vec![marker])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RebuildStatsError> {
        match self.step {
            BookStep::Marker => {
                let marker = result.value()?;
                if marker_progress(marker)?.generation != self.generation {
                    return Err(RebuildStatsError::Superseded);
                }
                let (deltas, buckets) =
                    self.deltas.take().ok_or(RebuildStatsError::RebuildFailed)?;
                self.step = BookStep::Added;
                Ok(Next::Call(IoCall::AddUsage {
                    space: USAGE_STATS_KEYSPACE.to_string(),
                    deltas,
                    buckets,
                }))
            }
            BookStep::Added => {
                result.done()?;
                self.step = BookStep::Saved;
                let progress = std::mem::take(&mut self.progress);
                let key = Key::from(USAGE_PROGRESS_KEY.to_vec());
                let row = (USAGE_STATS_KEYSPACE.to_string(), key, Value::from(progress));
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            BookStep::Saved => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "rebuild_tests.rs"]
mod tests;
