//! Rebuilds the node's usage counters by a paged walk over one storage snapshot. Pages add
//! their counts to the rebuild rows while storage adds every later delta there too, so the
//! rows are exact when the walk ends and replace the live counter rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap, VecDeque};

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
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    VersionKey,
};
use aruna_core::structs::storage::usage::{UsageCounterError, UsageCounters};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::node::rebuild_counts::{Joined, RebuildCounts, is_pithos, named_size, usable};

/// Rows read per walk page; every page commits before the next one is read.
const PAGE_LIMIT: usize = 256;
/// Copies of one hash read per page when a version's own location row is unusable.
const COPY_LIMIT: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebuildStatsState {
    Init,
    Begin,
    ReadConfig,
    Page,
    Joins,
    Fallback,
    StartPage,
    AddPage,
    CommitPage,
    CloseSnapshot,
    Finish,
    Done,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RebuildStatsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    CounterError(#[from] UsageCounterError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: RebuildStatsState,
        expected: &'static str,
        received: Event,
    },
    #[error("RebuildUsageStats failed")]
    RebuildFailed,
}

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

impl Walk {
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
#[derive(Debug, PartialEq)]
pub struct RebuildStatsOperation {
    state: RebuildStatsState,
    realm_id: Option<RealmId>,
    snapshot: Option<TxnId>,
    page_txn: Option<TxnId>,
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
    output: Option<Result<UsageCounters, RebuildStatsError>>,
}

impl Default for RebuildStatsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl RebuildStatsOperation {
    pub fn new() -> Self {
        Self {
            state: RebuildStatsState::Init,
            realm_id: None,
            snapshot: None,
            page_txn: None,
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
            output: None,
        }
    }

    /// Walks the buckets of groups with a quota in this realm first.
    pub fn with_realm(mut self, realm_id: RealmId) -> Self {
        self.realm_id = Some(realm_id);
        self
    }

    /// Open transactions are left to `abort` and the runner's transaction cleanup.
    fn fail(&mut self, error: RebuildStatsError) -> Effects {
        self.state = RebuildStatsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, received: Event) -> Effects {
        self.fail(RebuildStatsError::InvalidStateEvent {
            state: self.state.clone(),
            expected,
            received,
        })
    }

    fn snapshot_id(&self) -> Result<TxnId, RebuildStatsError> {
        self.snapshot.ok_or(RebuildStatsError::RebuildFailed)
    }

    fn scope_bucket(&self) -> Option<&String> {
        self.counts.quota_buckets.iter().nth(self.scope)
    }

    fn handle_begin(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("TransactionStarted", event);
        };
        self.snapshot = Some(txn_id);
        let Some(realm_id) = self.realm_id else {
            return self.start_walk(Walk::Buckets);
        };
        self.state = RebuildStatsState::ReadConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: DocumentTarget::RealmConfig { realm_id }.storage_key(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_config(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("ReadResult", event);
        };
        if let Some(bytes) = value {
            match RealmConfigDocument::from_bytes(bytes.as_ref()) {
                Ok(document) => self.counts.quota = Some(document.quota),
                Err(error) => return self.fail(error.into()),
            }
        }
        self.start_walk(Walk::Buckets)
    }

    fn start_walk(&mut self, walk: Walk) -> Effects {
        self.walk = walk;
        self.cursor = None;
        self.scope = 0;
        if walk.scoped() && self.counts.quota_buckets.is_empty() {
            return self.walk_done();
        }
        self.page_effect()
    }

    fn page_effect(&mut self) -> Effects {
        let snapshot = match self.snapshot_id() {
            Ok(snapshot) => snapshot,
            Err(error) => return self.fail(error),
        };
        let prefix = match (self.walk.scoped(), self.scope_bucket()) {
            (true, Some(bucket)) => match VersionKey::bucket_prefix(bucket) {
                Ok(prefix) => Some(Key::from(prefix)),
                Err(error) => return self.fail(error.into()),
            },
            _ => None,
        };
        self.state = RebuildStatsState::Page;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: self.walk.key_space().to_string(),
            prefix,
            start: self.cursor.clone(),
            limit: PAGE_LIMIT,
            txn_id: Some(snapshot),
        })]
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected("IterResult", event);
        };
        self.rows = values;
        self.next = next_start_after.map(IterStart::After);
        if self.walk.skips_quota()
            && !self.counts.quota_buckets.is_empty()
            && let Err(error) = self.skip_quota()
        {
            return self.fail(error);
        }
        match self.page_reads() {
            Ok(reads) if reads.is_empty() => self.book_page(),
            Ok(reads) => {
                let snapshot = match self.snapshot_id() {
                    Ok(snapshot) => snapshot,
                    Err(error) => return self.fail(error),
                };
                let effect = StorageEffect::BatchRead {
                    reads: reads
                        .iter()
                        .map(|(key_space, key)| (key_space.to_string(), Key::from(key.clone())))
                        .collect(),
                    txn_id: Some(snapshot),
                };
                self.reads = reads;
                self.state = RebuildStatsState::Joins;
                smallvec![Effect::Storage(effect)]
            }
            Err(error) => self.fail(error),
        }
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

    fn handle_joins(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("BatchReadResult", event);
        };
        let reads = std::mem::take(&mut self.reads);
        if reads.len() != values.len() {
            return self.fail(RebuildStatsError::RebuildFailed);
        }
        for (read, (_, value)) in reads.into_iter().zip(values) {
            self.joined.insert(read, value);
        }
        if self.walk.versions()
            && let Err(error) = self.queue_copies()
        {
            return self.fail(error);
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

    fn next_copy(&mut self) -> Effects {
        let Some(hash) = self.copies.front().copied() else {
            return self.book_page();
        };
        let snapshot = match self.snapshot_id() {
            Ok(snapshot) => snapshot,
            Err(error) => return self.fail(error),
        };
        self.state = RebuildStatsState::Fallback;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            prefix: Some(Key::from(hash.to_vec())),
            start: self.copy_cursor.clone(),
            limit: COPY_LIMIT,
            txn_id: Some(snapshot),
        })]
    }

    fn handle_copy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected("IterResult", event);
        };
        let Some(hash) = self.copies.front().copied() else {
            return self.fail(RebuildStatsError::RebuildFailed);
        };
        let mut size = None;
        for (_, value) in &values {
            match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) if usable(&location) => {
                    size = Some(location.blob_size);
                    break;
                }
                Ok(_) => {}
                Err(error) => return self.fail(error.into()),
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

    fn book_page(&mut self) -> Effects {
        if let Err(error) = self.book_rows() {
            return self.fail(error);
        }
        self.joined.clear();
        self.sizes.clear();
        if self.counts.is_empty() {
            return self.advance();
        }
        self.state = RebuildStatsState::StartPage;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn book_rows(&mut self) -> Result<(), RebuildStatsError> {
        self.counts.seed_shards();
        let (joined, sizes) = (&self.joined, &self.sizes);
        for (key, value) in std::mem::take(&mut self.rows) {
            let counts = &mut self.counts;
            match self.walk {
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

    fn handle_page_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("TransactionStarted", event);
        };
        self.page_txn = Some(txn_id);
        let (deltas, buckets) = self.counts.take_deltas();
        self.state = RebuildStatsState::AddPage;
        smallvec![Effect::Storage(StorageEffect::AddUsage {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            deltas,
            buckets,
            txn_id,
        })]
    }

    fn handle_page_added(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected("BatchWriteResult", event);
        };
        let Some(txn_id) = self.page_txn else {
            return self.fail(RebuildStatsError::RebuildFailed);
        };
        self.state = RebuildStatsState::CommitPage;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_page_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("TransactionCommitted", event);
        };
        self.page_txn = None;
        self.advance()
    }

    /// Reads the next page of the walk, or moves to the next quota bucket or walk.
    fn advance(&mut self) -> Effects {
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

    fn walk_done(&mut self) -> Effects {
        self.counts.last_head = None;
        if let Some(walk) = self.walk.next() {
            return self.start_walk(walk);
        }
        if self.counts.orphan_versions > 0 {
            warn!(
                versions = self.counts.orphan_versions,
                "rebuilt usage omits versions without a blob location row"
            );
        }
        let snapshot = match self.snapshot_id() {
            Ok(snapshot) => snapshot,
            Err(error) => return self.fail(error),
        };
        self.state = RebuildStatsState::CloseSnapshot;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: snapshot
        })]
    }

    fn handle_closed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("TransactionCommitted", event);
        };
        self.snapshot = None;
        self.state = RebuildStatsState::Finish;
        smallvec![Effect::Storage(StorageEffect::FinishUsageRebuild {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
        })]
    }

    fn handle_finished(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected("BatchWriteResult", event);
        };
        self.state = RebuildStatsState::Done;
        self.output = Some(Ok(self.counts.total));
        smallvec![]
    }
}

impl Operation for RebuildStatsOperation {
    type Output = UsageCounters;
    type Error = RebuildStatsError;

    fn start(&mut self) -> Effects {
        self.state = RebuildStatsState::Begin;
        smallvec![Effect::Storage(StorageEffect::BeginUsageRebuild {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(RebuildStatsError::StorageError(error));
        }
        match self.state {
            RebuildStatsState::Init => self.start(),
            RebuildStatsState::Begin => self.handle_begin(event),
            RebuildStatsState::ReadConfig => self.handle_config(event),
            RebuildStatsState::Page => self.handle_page(event),
            RebuildStatsState::Joins => self.handle_joins(event),
            RebuildStatsState::Fallback => self.handle_copy(event),
            RebuildStatsState::StartPage => self.handle_page_started(event),
            RebuildStatsState::AddPage => self.handle_page_added(event),
            RebuildStatsState::CommitPage => self.handle_page_committed(event),
            RebuildStatsState::CloseSnapshot => self.handle_closed(event),
            RebuildStatsState::Finish => self.handle_finished(event),
            RebuildStatsState::Done | RebuildStatsState::Error => {
                self.unexpected("no event", event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RebuildStatsState::Done | RebuildStatsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(counters)) => Ok(counters),
            Some(Err(error)) => Err(error),
            None => Err(RebuildStatsError::RebuildFailed),
        }
    }

    /// Closes the open page transaction and the snapshot. The marker stays, so the counters
    /// stay incomplete until a later rebuild finishes.
    fn abort(&mut self) -> Effects {
        let mut effects = Effects::new();
        for txn_id in [self.page_txn.take(), self.snapshot.take()]
            .into_iter()
            .flatten()
        {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects
    }
}
