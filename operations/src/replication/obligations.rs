//! Turns live copy obligations into replication jobs and retires the ones that are done.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    NODE_STATE_KEYSPACE, RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE,
    REPLICATION_JOB_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{
    ReferenceHandling, SyncMode, SyncRelationship, sync_relationship_key, sync_relationship_prefix,
};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use tracing::warn;

use super::members::{BoundRecord, BoundRecordOperation, RecordError, member_jobs};
use super::page::PAGE_BYTES;
use super::queue::BlobQueueError;
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, LiveObligationRecord, LiveReplicationContinuation, RelationshipJobTarget,
    blob_job_entry, blob_job_key, blob_job_preferred, continuation_newer, live_obligation_entry,
    relationship_job, same_endpoint, validate_sync_key,
};

pub(super) const REPLICATION_PAGE_SIZE: usize = 512;

/// Obligations one pass reads; those without sync work are retired together.
pub(super) const OBLIGATION_BATCH_SIZE: usize = super::page::PAGE_RECORDS;

/// Obligations one pass turns into replication jobs.
const OBLIGATION_WORK_LIMIT: usize = 64;

pub(super) const REPLICATION_JOB_LIMIT: usize = 64;

pub(super) const REPLICATION_RELATIONSHIP_LIMIT: usize = 1024;

/// The obligation the next obligation pass continues after, so later ones get their turn.
const OBLIGATION_CURSOR_KEY: &[u8] = b"live_obligation_cursor";

type Turn<T> = Result<Next<T>, BlobQueueError>;
type Obligation = (Vec<u8>, LiveObligationRecord);
type Retired = (Vec<u8>, Option<LiveReplicationContinuation>);

fn unexpected(event: &Event) -> BlobQueueError {
    BlobQueueError::UnexpectedEvent(event.label().to_string())
}

fn cursor_key() -> Key {
    ByteView::from(OBLIGATION_CURSOR_KEY.to_vec())
}

#[derive(Debug, PartialEq)]
pub(super) struct RelationshipPage {
    pub(super) values: Vec<(Vec<u8>, SyncRelationship)>,
    pub(super) next: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub(super) struct LiveRepairWrite {
    pub(super) queued: usize,
    continuation: Option<LiveReplicationContinuation>,
    progressed: bool,
}

#[derive(Debug, Default, PartialEq)]
pub(super) struct LiveRepairResult {
    pub(super) processed: usize,
    pub(super) queued: usize,
    pub(super) has_more: bool,
    /// When the oldest obligation read was written.
    pub(super) oldest_ms: Option<u64>,
}

fn hop_exhausted(obligation: &LiveObligationRecord) -> bool {
    (obligation.origin.as_ref()).is_some_and(|origin| origin.hop_count >= 4)
}

/// The relationship each bucket's obligations start reading at: the earliest any of them needs.
fn relationship_starts(obligations: &[Obligation]) -> HashMap<String, Option<Vec<u8>>> {
    let mut starts = HashMap::<String, Option<Vec<u8>>>::new();
    for (_, obligation) in obligations {
        let done =
            (obligation.continuation.as_ref()).is_some_and(|cursor| cursor.relationships_complete);
        if hop_exhausted(obligation) || done {
            continue;
        }
        let start =
            (obligation.continuation.as_ref()).and_then(|cursor| cursor.relationship_after.clone());
        starts
            .entry(obligation.bucket.clone())
            .and_modify(|current| {
                let replace = match (current.as_ref(), start.as_ref()) {
                    (Some(current), Some(start)) => start < current,
                    (Some(_), None) => true,
                    _ => false,
                };
                if replace {
                    *current = start.clone();
                }
            })
            .or_insert(start);
    }
    starts
}

#[derive(Debug, PartialEq)]
enum PassStep {
    Cursor,
    Page,
    Malformed,
    Record(String),
    Relationships(String),
    Jobs(Box<Obligation>),
    Stored(usize),
    Retired,
}

/// One pass over the live obligations: those without sync work are retired together with their
/// member jobs, and a bounded number of the others becomes replication jobs.
#[derive(Debug, PartialEq)]
pub(super) struct ProcessObligationsOperation {
    now_ms: u64,
    step: PassStep,
    after: Option<Vec<u8>>,
    next: Option<Vec<u8>>,
    malformed: VecDeque<Vec<u8>>,
    obligations: VecDeque<Obligation>,
    buckets: VecDeque<String>,
    records: HashMap<String, Option<BoundRecord>>,
    starts: VecDeque<(String, Option<Vec<u8>>)>,
    relationships: HashMap<String, RelationshipPage>,
    relationship_work: usize,
    retired: Vec<Retired>,
    member_work: Vec<BlobJobRecord>,
    worked: usize,
    result: LiveRepairResult,
}

impl ProcessObligationsOperation {
    pub(super) fn new() -> Self {
        Self {
            now_ms: 0,
            step: PassStep::Cursor,
            after: None,
            next: None,
            malformed: VecDeque::new(),
            obligations: VecDeque::new(),
            buckets: VecDeque::new(),
            records: HashMap::new(),
            starts: VecDeque::new(),
            relationships: HashMap::new(),
            relationship_work: 0,
            retired: Vec::new(),
            member_work: Vec::new(),
            worked: 0,
            result: LiveRepairResult::default(),
        }
    }

    fn call(&mut self, step: PassStep, call: IoCall) -> Turn<LiveRepairResult> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn read_page(&mut self) -> Turn<LiveRepairResult> {
        let page = IoCall::Page {
            space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone().map(Into::into),
            limit: OBLIGATION_BATCH_SIZE,
        };
        self.call(PassStep::Page, page)
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> Turn<LiveRepairResult> {
        self.next = next.map(|key| key.to_vec());
        for (key, value) in rows {
            match LiveObligationRecord::from_bytes(&value) {
                Ok(record) => self.obligations.push_back((key.to_vec(), record)),
                Err(error) => {
                    let key = key.to_vec();
                    warn!(error = %error, key = ?key, "Deleting malformed live replication obligation");
                    self.malformed.push_back(key);
                }
            }
        }
        self.next_malformed()
    }

    fn next_malformed(&mut self) -> Turn<LiveRepairResult> {
        match self.malformed.pop_front() {
            Some(key) => {
                let delete = DeleteObligationOperation::new(key, None);
                self.call(PassStep::Malformed, IoCall::child(delete))
            }
            None => self.plan_reads(),
        }
    }

    fn plan_reads(&mut self) -> Turn<LiveRepairResult> {
        let obligations = self.obligations.make_contiguous();
        // A pass that started past the first obligation wraps to the start next.
        self.result.has_more = self.next.is_some() || self.after.is_some();
        self.result.oldest_ms = (obligations.iter())
            .map(|(_, obligation)| obligation.version_id.timestamp_ms())
            .min();
        self.starts = relationship_starts(obligations).into_iter().collect();
        let mut buckets: Vec<String> = Vec::new();
        for (_, obligation) in obligations.iter() {
            if !buckets.contains(&obligation.bucket) {
                buckets.push(obligation.bucket.clone());
            }
        }
        self.buckets = buckets.into();
        self.next_record()
    }

    fn next_record(&mut self) -> Turn<LiveRepairResult> {
        match self.buckets.pop_front() {
            Some(bucket) => {
                let read = BoundRecordOperation::new(&bucket);
                self.call(PassStep::Record(bucket), IoCall::child(read))
            }
            None => self.next_relationships(),
        }
    }

    fn next_relationships(&mut self) -> Turn<LiveRepairResult> {
        let remaining = REPLICATION_RELATIONSHIP_LIMIT.saturating_sub(self.relationship_work);
        match self.starts.pop_front().filter(|_| remaining > 0) {
            Some((bucket, start)) => {
                let read = ReadRelationshipsOperation::new(&bucket, start, remaining);
                self.call(PassStep::Relationships(bucket), IoCall::child(read))
            }
            None => self.next_obligation(),
        }
    }

    fn next_obligation(&mut self) -> Turn<LiveRepairResult> {
        while let Some((key, obligation)) = self.obligations.pop_front() {
            let cursor = obligation.continuation.clone().unwrap_or_default();
            // A hop-exhausted obligation is never scanned for relationships, so it must reach
            // the job write to be retired instead of blocking the scan.
            let exhausted = hop_exhausted(&obligation);
            let relationships = match cursor.relationships_complete {
                true => None,
                false => self.relationships.get(&obligation.bucket),
            };
            if !exhausted && !cursor.relationships_complete && relationships.is_none() {
                self.result.has_more = true;
                continue;
            }
            let members = match self.records.get(&obligation.bucket) {
                Some(Some((binding, snapshot))) if !cursor.members_done => {
                    member_jobs(binding, snapshot, &obligation, self.now_ms)
                }
                _ => Vec::new(),
            };
            // A version of a bucket nothing replicates to owes no job: such obligations, one per
            // imported file, are retired in one transaction instead of one each.
            let idle =
                relationships.is_none_or(|page| page.values.is_empty() && page.next.is_none());
            // Without sync work an obligation owes only its member jobs, queued in one batch.
            if idle && !exhausted {
                self.member_work.extend(members);
                self.retired.push((key, obligation.continuation));
                continue;
            }
            if self.worked == OBLIGATION_WORK_LIMIT {
                self.result.has_more = true;
                continue;
            }
            self.worked += 1;
            let write = WriteLiveJobsOperation::new(&obligation, relationships, members);
            let step = PassStep::Jobs(Box::new((key, obligation)));
            return self.call(step, IoCall::child(write));
        }
        self.result.processed = self.result.processed.saturating_add(self.retired.len());
        self.result.queued = self.result.queued.saturating_add(self.member_work.len());
        let retire = RetireObligationsOperation::new(
            std::mem::take(&mut self.retired),
            std::mem::take(&mut self.member_work),
            (self.after.take(), self.next.take()),
        );
        self.call(PassStep::Retired, IoCall::child(retire))
    }

    fn jobs_written(
        &mut self,
        (key, obligation): Obligation,
        write: LiveRepairWrite,
    ) -> Turn<LiveRepairResult> {
        if !write.progressed {
            self.result.has_more = true;
            return self.next_obligation();
        }
        let step = PassStep::Stored(write.queued);
        match write.continuation {
            Some(continuation) => {
                let mut next = obligation;
                next.continuation = Some(continuation);
                self.result.has_more = true;
                self.call(step, IoCall::child(WriteObligationOperation::new(next)))
            }
            None => {
                let expected = obligation.continuation;
                let delete = DeleteObligationOperation::new(key, expected);
                self.call(step, IoCall::child(delete))
            }
        }
    }
}

impl Operation for ProcessObligationsOperation {
    type Output = LiveRepairResult;
    type Error = BlobQueueError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<LiveRepairResult> {
        Ok(Next::Call(read_row(NODE_STATE_KEYSPACE, cursor_key())))
    }

    fn next(&mut self, result: IoResult) -> Turn<LiveRepairResult> {
        match std::mem::replace(&mut self.step, PassStep::Retired) {
            PassStep::Cursor => {
                self.after = (result.value()?)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_vec());
                self.read_page()
            }
            PassStep::Page => {
                let (rows, next) = result.page()?;
                self.page_read(rows, next)
            }
            PassStep::Malformed => {
                result.child::<(), BlobQueueError>()??;
                self.next_malformed()
            }
            PassStep::Record(bucket) => {
                let record = (result.child::<Option<BoundRecord>, RecordError>()?)
                    .map_err(|error| BlobQueueError::UnexpectedEvent(error.to_string()))?;
                self.records.insert(bucket, record);
                self.next_record()
            }
            PassStep::Relationships(bucket) => {
                let page = result.child::<RelationshipPage, BlobQueueError>()??;
                self.relationship_work = self.relationship_work.saturating_add(page.values.len());
                self.relationships.insert(bucket, page);
                self.next_relationships()
            }
            PassStep::Jobs(obligation) => {
                let write = result.child::<LiveRepairWrite, BlobQueueError>()??;
                self.jobs_written(*obligation, write)
            }
            PassStep::Stored(queued) => {
                result.child::<(), BlobQueueError>()??;
                self.result.processed = self.result.processed.saturating_add(1);
                self.result.queued = self.result.queued.saturating_add(queued);
                self.next_obligation()
            }
            PassStep::Retired => {
                result.child::<(), BlobQueueError>()??;
                Ok(Next::Done(std::mem::take(&mut self.result)))
            }
        }
    }
}

/// The cursor row; an empty value starts the next pass at the first obligation.
fn cursor_entry(after: Option<Vec<u8>>) -> (String, Key, Value) {
    (
        NODE_STATE_KEYSPACE.to_string(),
        cursor_key(),
        ByteView::from(after.unwrap_or_default()),
    )
}

#[derive(Debug, PartialEq)]
enum RetireStep {
    Existing,
    Cursor,
    Transaction,
}

/// Queues the jobs and deletes the obligations, a continued one only while it still holds the
/// continuation read, with the obligation cursor moved along, in one transaction.
#[derive(Debug, PartialEq)]
pub(super) struct RetireObligationsOperation {
    obligations: Vec<Retired>,
    jobs: Vec<BlobJobRecord>,
    cursor: (Option<Vec<u8>>, Option<Vec<u8>>),
    step: RetireStep,
}

impl RetireObligationsOperation {
    pub(super) fn new(
        obligations: Vec<Retired>,
        jobs: Vec<BlobJobRecord>,
        cursor: (Option<Vec<u8>>, Option<Vec<u8>>),
    ) -> Self {
        Self {
            obligations,
            jobs,
            cursor,
            step: RetireStep::Existing,
        }
    }
}

impl Operation for RetireObligationsOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<()> {
        if self.obligations.is_empty() && self.jobs.is_empty() {
            if self.cursor.0 == self.cursor.1 {
                return Ok(Next::Done(()));
            }
            self.step = RetireStep::Cursor;
            let row = cursor_entry(self.cursor.1.take());
            return Ok(Next::Call(IoCall::Write(vec![row])));
        }
        // One drain at a time creates or parks member jobs, so this read needs no transaction.
        let existing = ExistingJobsOperation::new(&self.jobs)?;
        Ok(Next::Call(IoCall::child(existing)))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            RetireStep::Existing => {
                let existing = result.child::<Vec<Option<Value>>, BlobQueueError>()??;
                self.step = RetireStep::Transaction;
                let retire = RetireRowsOperation::new(
                    std::mem::take(&mut self.obligations),
                    std::mem::take(&mut self.jobs),
                    existing,
                    self.cursor.1.take(),
                );
                Ok(Next::Call(IoCall::child(retire)))
            }
            RetireStep::Cursor => {
                result.done()?;
                Ok(Next::Done(()))
            }
            RetireStep::Transaction => {
                result.child::<(), BlobQueueError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RowsStep {
    Read,
    Written,
    Deleted,
}

/// The transaction of a retirement.
#[derive(Debug, PartialEq)]
struct RetireRowsOperation {
    /// Continued obligations, deleted only while they hold the continuation read.
    continued: Vec<Retired>,
    /// Obligations any rewrite of which owes just the jobs queued here.
    fresh: Vec<Vec<u8>>,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    step: RowsStep,
    failure: Option<ConversionError>,
}

impl RetireRowsOperation {
    fn new(
        obligations: Vec<Retired>,
        jobs: Vec<BlobJobRecord>,
        existing: Vec<Option<Value>>,
        cursor: Option<Vec<u8>>,
    ) -> Self {
        // Only a drain sets continuations; any other rewrite owes just the jobs queued here.
        let (fresh, continued): (Vec<_>, Vec<_>) =
            (obligations.into_iter()).partition(|(_, expected)| expected.is_none());
        let mut writes = Vec::with_capacity(jobs.len() + 1);
        let mut failure = None;
        for (job, value) in jobs.iter().zip(existing) {
            let kept = value
                .as_deref()
                .and_then(|value| BlobJobRecord::from_bytes(value).ok())
                .is_some_and(|existing| blob_job_preferred(&existing, job));
            if kept {
                continue;
            }
            match blob_job_entry(job) {
                Ok(entry) => writes.push(entry),
                Err(error) => failure = Some(error),
            }
        }
        writes.push(cursor_entry(cursor));
        Self {
            continued,
            fresh: fresh.into_iter().map(|(key, _)| key).collect(),
            writes,
            deletes: Vec::new(),
            step: RowsStep::Read,
            failure,
        }
    }

    fn unchanged(&self, values: Vec<Option<Value>>) -> Vec<(String, Key)> {
        let still = |(expected, value): (&Option<LiveReplicationContinuation>, Option<Value>)| {
            value
                .as_deref()
                .is_some_and(|value| match LiveObligationRecord::from_bytes(value) {
                    Ok(record) => record.continuation == *expected,
                    Err(_) => expected.is_none(),
                })
        };
        (self.continued.iter())
            .zip(values)
            .filter(|((_, expected), value)| still((expected, value.clone())))
            .map(|((key, _), _)| key.clone())
            .chain(self.fresh.iter().cloned())
            .map(|key| (REPLICATION_OBLIGATION_KEYSPACE.to_string(), key.into()))
            .collect()
    }
}

impl Operation for RetireRowsOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<()> {
        if let Some(error) = self.failure.take() {
            return Err(error.into());
        }
        let reads = (self.continued.iter())
            .map(|(key, _)| {
                let key = ByteView::from(key.clone());
                (REPLICATION_OBLIGATION_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            RowsStep::Read => {
                self.deletes = self.unchanged(result.values()?);
                self.step = RowsStep::Written;
                let writes = std::mem::take(&mut self.writes);
                Ok(Next::Call(IoCall::Write(writes)))
            }
            RowsStep::Written => {
                result.done()?;
                self.step = RowsStep::Deleted;
                let deletes = std::mem::take(&mut self.deletes);
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            RowsStep::Deleted => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// The stored row of each job, from one range read while the range holds no other rows, as
/// lookups key by key cost far more than a range in these churned keyspaces.
#[derive(Debug, PartialEq)]
struct ExistingJobsOperation {
    keys: Vec<Key>,
    by_key: bool,
}

impl ExistingJobsOperation {
    fn new(jobs: &[BlobJobRecord]) -> Result<Self, BlobQueueError> {
        let keys = jobs.iter().map(blob_job_key).collect::<Result<_, _>>()?;
        Ok(Self {
            keys,
            by_key: false,
        })
    }
}

impl Operation for ExistingJobsOperation {
    type Output = Vec<Option<Value>>;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<Vec<Option<Value>>> {
        let (Some(first), Some(last)) = (self.keys.iter().min(), self.keys.iter().max()) else {
            return Ok(Next::Done(Vec::new()));
        };
        let range = StorageEffect::IterRange {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            start: IterStart::At(first.clone()),
            end: IterEnd::Through(last.clone()),
            limit: self.keys.len(),
            max_bytes: PAGE_BYTES,
            txn_id: None,
        };
        Ok(Next::Call(Effect::Storage(range).into()))
    }

    fn next(&mut self, result: IoResult) -> Turn<Vec<Option<Value>>> {
        if self.by_key {
            return Ok(Next::Done(result.values()?));
        }
        match result.event()? {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after: None,
            }) => {
                let rows: HashMap<_, _> = values.into_iter().collect();
                let found = self.keys.iter().map(|key| rows.get(key).cloned());
                Ok(Next::Done(found.collect()))
            }
            // A range holding more rows than the jobs is read key by key.
            Event::Storage(StorageEvent::IterResult { .. }) => {
                self.by_key = true;
                let reads = (self.keys.iter())
                    .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), key.clone()))
                    .collect();
                Ok(Next::Call(IoCall::Read(reads)))
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(unexpected(&other)),
        }
    }
}

/// Stores an obligation, a continued one only when its continuation is newer than the stored.
#[derive(Debug, PartialEq)]
pub(super) struct WriteObligationOperation {
    record: LiveObligationRecord,
    writing: bool,
}

impl WriteObligationOperation {
    pub(super) fn new(record: LiveObligationRecord) -> Self {
        Self {
            record,
            writing: false,
        }
    }
}

impl Operation for WriteObligationOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<()> {
        let (key_space, key, _) = live_obligation_entry(&self.record)?;
        Ok(Next::Call(read_row(&key_space, key)))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let current = result.value()?;
        let stored = current.as_deref().map(LiveObligationRecord::from_bytes);
        let write = match stored {
            None | Some(Err(_)) => true,
            Some(Ok(current)) => {
                match (
                    self.record.continuation.as_ref(),
                    current.continuation.as_ref(),
                ) {
                    (Some(candidate), Some(current)) => continuation_newer(candidate, current),
                    (Some(_), None) => true,
                    (None, _) => false,
                }
            }
        };
        if !write {
            return Ok(Next::Done(()));
        }
        self.writing = true;
        let entry = live_obligation_entry(&self.record)?;
        Ok(Next::Call(IoCall::Write(vec![entry])))
    }
}

/// Deletes an obligation while it still holds the expected continuation.
#[derive(Debug, PartialEq)]
pub(super) struct DeleteObligationOperation {
    key: Key,
    expected: Option<LiveReplicationContinuation>,
    deleting: bool,
}

impl DeleteObligationOperation {
    pub(super) fn new(key: Vec<u8>, expected: Option<LiveReplicationContinuation>) -> Self {
        Self {
            key: key.into(),
            expected,
            deleting: false,
        }
    }
}

impl Operation for DeleteObligationOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<()> {
        let read = read_row(REPLICATION_OBLIGATION_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let delete =
            result
                .value()?
                .is_some_and(|value| match LiveObligationRecord::from_bytes(&value) {
                    Ok(record) => record.continuation == self.expected,
                    Err(_) => self.expected.is_none(),
                });
        if !delete {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        let row = (
            REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            self.key.clone(),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

/// Reads a bucket's outgoing relationships after `start`, at most `limit`.
#[derive(Debug, PartialEq)]
pub(super) struct ReadRelationshipsOperation {
    bucket: String,
    after: Option<Vec<u8>>,
    limit: usize,
    values: Vec<(Vec<u8>, SyncRelationship)>,
}

impl ReadRelationshipsOperation {
    pub(super) fn new(bucket: &str, start: Option<Vec<u8>>, limit: usize) -> Self {
        Self {
            bucket: bucket.to_string(),
            after: start,
            limit,
            values: Vec::with_capacity(limit.min(REPLICATION_PAGE_SIZE)),
        }
    }

    fn read_page(&mut self) -> Turn<RelationshipPage> {
        let limit = (self.limit - self.values.len()).min(REPLICATION_PAGE_SIZE);
        Ok(Next::Call(IoCall::Page {
            space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
            prefix: sync_relationship_prefix(&self.bucket).into(),
            after: self.after.take().map(Into::into),
            limit,
        }))
    }

    fn finish(&mut self, next: Option<Vec<u8>>) -> Turn<RelationshipPage> {
        Ok(Next::Done(RelationshipPage {
            values: std::mem::take(&mut self.values),
            next,
        }))
    }
}

impl Operation for ReadRelationshipsOperation {
    type Output = RelationshipPage;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<RelationshipPage> {
        if self.limit == 0 {
            return self.finish(None);
        }
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn<RelationshipPage> {
        let (rows, next) = result.page()?;
        for (key, value) in rows {
            let relationship = SyncRelationship::from_bytes(&value)?;
            validate_sync_key(&self.bucket, &key, &relationship)?;
            self.values.push((key.to_vec(), relationship));
        }
        let next = next.map(|key| key.to_vec());
        if self.values.len() == self.limit {
            return self.finish(next);
        }
        match next {
            Some(next) => {
                self.after = Some(next);
                self.read_page()
            }
            None => self.finish(None),
        }
    }
}

/// The relationship jobs one obligation owes now, and how far they take its cursor.
fn relationship_jobs(
    obligation: &LiveObligationRecord,
    upstream_sources: &[ArunaArn],
    page: &RelationshipPage,
    cursor: &mut LiveReplicationContinuation,
) -> Option<Vec<BlobJobRecord>> {
    let mut jobs = Vec::with_capacity(REPLICATION_JOB_LIMIT);
    let mut found = false;
    let mut complete = page.next.is_none();
    for (index, (key, relationship)) in page.values.iter().enumerate() {
        if (cursor.relationship_after.as_ref()).is_some_and(|after| key <= after) {
            continue;
        }
        found = true;
        cursor.relationship_after = Some(key.clone());
        if obligation.reference_advance.is_some()
            && relationship.mode != SyncMode::Reference
            && relationship.reference_handling != ReferenceHandling::Preserve
        {
            continue;
        }
        let job = relationship_job(
            obligation.local_node_id,
            RelationshipJobTarget {
                bucket: &obligation.bucket,
                key: &obligation.key,
                version_id: obligation.version_id,
                delete_marker: obligation.delete_marker,
            },
            relationship.clone(),
            obligation.origin.as_ref(),
            upstream_sources,
        );
        let job = job.map(|mut job| match obligation.reference_advance {
            Some(advance) => {
                job.input.auth_context = obligation.auth_context.clone();
                job.writer_auth_context = None;
                job.with_reference_advance(advance)
            }
            None => job.with_writer_auth(obligation.auth_context.clone()),
        });
        if let Some(job) = job {
            if jobs.len() == REPLICATION_JOB_LIMIT {
                complete = false;
                break;
            }
            jobs.push(job);
            if jobs.len() == REPLICATION_JOB_LIMIT {
                complete = index + 1 == page.values.len() && page.next.is_none();
                break;
            }
        }
    }
    if !found {
        if page.next.is_some() {
            return None;
        }
        cursor.relationship_after = None;
        cursor.relationships_complete = true;
    } else if complete {
        cursor.relationship_after = None;
        cursor.relationships_complete = true;
    } else {
        cursor.relationships_complete = false;
    }
    Some(jobs)
}

#[derive(Debug, PartialEq)]
enum LiveStep {
    Inbound,
    Members,
    Relationships,
}

/// Queues the member and relationship jobs one obligation owes now.
#[derive(Debug, PartialEq)]
pub(super) struct WriteLiveJobsOperation {
    obligation: LiveObligationRecord,
    page: Option<RelationshipPage>,
    members: Vec<BlobJobRecord>,
    upstream_sources: Vec<ArunaArn>,
    cursor: LiveReplicationContinuation,
    queued: usize,
    step: LiveStep,
}

fn no_progress() -> LiveRepairWrite {
    LiveRepairWrite {
        queued: 0,
        continuation: None,
        progressed: false,
    }
}

impl WriteLiveJobsOperation {
    pub(super) fn new(
        obligation: &LiveObligationRecord,
        page: Option<&RelationshipPage>,
        members: Vec<BlobJobRecord>,
    ) -> Self {
        Self {
            obligation: obligation.clone(),
            page: page.map(|page| RelationshipPage {
                values: page.values.clone(),
                next: page.next.clone(),
            }),
            members,
            upstream_sources: obligation.upstream_sources.clone(),
            cursor: obligation.continuation.clone().unwrap_or_default(),
            queued: 0,
            step: LiveStep::Inbound,
        }
    }

    fn inbound_read(&mut self, value: Option<Value>) -> Result<(), BlobQueueError> {
        let (Some(value), Some(origin)) = (value, self.obligation.origin.as_ref()) else {
            return Ok(());
        };
        let bucket = &self.obligation.bucket;
        let relationship = SyncRelationship::from_bytes(&value)?;
        if relationship.id != origin.relationship_id
            || relationship.target.bucket() != Some(bucket.as_str())
            || sync_relationship_key(bucket, relationship.id)
                != sync_relationship_key(bucket, origin.relationship_id)
        {
            return Err(ConversionError::FromStrError(
                "incoming sync relationship key does not match payload".to_string(),
            )
            .into());
        }
        let source = relationship.source;
        let known = (self.upstream_sources.iter()).any(|existing| same_endpoint(existing, &source));
        if !known {
            self.upstream_sources.push(source);
        }
        Ok(())
    }

    /// Publications to set members are bounded by the member count and queued once.
    fn queue_members(&mut self) -> Turn<LiveRepairWrite> {
        let members = std::mem::take(&mut self.members);
        if self.cursor.members_done || members.is_empty() {
            return self.queue_relationships();
        }
        self.step = LiveStep::Members;
        let persist = PersistJobsOperation::new(members, None);
        Ok(Next::Call(IoCall::child(persist)))
    }

    fn queue_relationships(&mut self) -> Turn<LiveRepairWrite> {
        self.cursor.members_done = true;
        let mut jobs = Vec::new();
        if !self.cursor.relationships_complete {
            let Some(page) = self.page.as_ref() else {
                return Ok(Next::Done(no_progress()));
            };
            let planned = relationship_jobs(
                &self.obligation,
                &self.upstream_sources,
                page,
                &mut self.cursor,
            );
            let Some(planned) = planned else {
                return Ok(Next::Done(no_progress()));
            };
            jobs = planned;
        }
        let continuation = (!self.cursor.relationships_complete).then(|| self.cursor.clone());
        self.step = LiveStep::Relationships;
        let persist = PersistJobsOperation::new(jobs, continuation);
        Ok(Next::Call(IoCall::child(persist)))
    }
}

impl Operation for WriteLiveJobsOperation {
    type Output = LiveRepairWrite;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<LiveRepairWrite> {
        let obligation = &self.obligation;
        if hop_exhausted(obligation) {
            warn!(
                bucket = %obligation.bucket,
                key = %obligation.key,
                version_id = %obligation.version_id,
                "Retiring live replication obligation at the hop limit"
            );
            return Ok(Next::Done(LiveRepairWrite {
                queued: 0,
                continuation: None,
                progressed: true,
            }));
        }
        match obligation.origin.as_ref() {
            Some(origin) => {
                let key = sync_relationship_key(&obligation.bucket, origin.relationship_id);
                Ok(Next::Call(read_row(RELATIONSHIP_IN_KEYSPACE, key)))
            }
            None => self.queue_members(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<LiveRepairWrite> {
        match self.step {
            LiveStep::Inbound => {
                self.inbound_read(result.value()?)?;
                self.queue_members()
            }
            LiveStep::Members => {
                let write = result.child::<LiveRepairWrite, BlobQueueError>()??;
                self.queued = write.queued;
                self.queue_relationships()
            }
            LiveStep::Relationships => {
                let mut write = result.child::<LiveRepairWrite, BlobQueueError>()??;
                write.queued = write.queued.saturating_add(self.queued);
                Ok(Next::Done(write))
            }
        }
    }
}

/// Queues the jobs a stored job does not already cover.
#[derive(Debug, PartialEq)]
struct PersistJobsOperation {
    jobs: Vec<BlobJobRecord>,
    continuation: Option<LiveReplicationContinuation>,
    writing: Option<usize>,
}

impl PersistJobsOperation {
    fn new(jobs: Vec<BlobJobRecord>, continuation: Option<LiveReplicationContinuation>) -> Self {
        Self {
            jobs,
            continuation,
            writing: None,
        }
    }

    fn finish(&mut self, queued: usize) -> Turn<LiveRepairWrite> {
        Ok(Next::Done(LiveRepairWrite {
            queued,
            continuation: self.continuation.take(),
            progressed: true,
        }))
    }
}

impl Operation for PersistJobsOperation {
    type Output = LiveRepairWrite;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<LiveRepairWrite> {
        if self.jobs.is_empty() {
            return self.finish(0);
        }
        let mut reads = Vec::with_capacity(self.jobs.len());
        for job in &self.jobs {
            reads.push((REPLICATION_JOB_KEYSPACE.to_string(), blob_job_key(job)?));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Turn<LiveRepairWrite> {
        if let Some(queued) = self.writing {
            result.done()?;
            return self.finish(queued);
        }
        let values = result.values()?;
        if values.len() != self.jobs.len() {
            return Err(BlobQueueError::UnexpectedEvent(
                "blob replication existing job read count mismatch".to_string(),
            ));
        }
        let mut writes = Vec::with_capacity(self.jobs.len());
        for (job, value) in self.jobs.iter().zip(values) {
            let kept = (value.as_deref())
                .and_then(|value| BlobJobRecord::from_bytes(value).ok())
                .is_some_and(|existing| blob_job_preferred(&existing, job));
            if !kept {
                writes.push(blob_job_entry(job)?);
            }
        }
        if writes.is_empty() {
            return self.finish(0);
        }
        self.writing = Some(writes.len());
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

#[cfg(test)]
mod step_tests {
    use super::*;

    fn empty_page() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    #[test]
    fn nothing_to_retire_ends() {
        let mut retire = RetireObligationsOperation::new(Vec::new(), Vec::new(), (None, None));
        assert_eq!(retire.first(), Ok(Next::Done(())));
    }

    #[test]
    fn moved_cursor_written() {
        let cursor = (None, Some(vec![1]));
        let mut retire = RetireObligationsOperation::new(Vec::new(), Vec::new(), cursor);
        assert!(matches!(retire.first(), Ok(Next::Call(IoCall::Write(_)))));
        assert_eq!(retire.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn zero_limit_reads_nothing() {
        let mut read = ReadRelationshipsOperation::new("b", None, 0);
        let expected = RelationshipPage {
            values: Vec::new(),
            next: None,
        };
        assert_eq!(read.first(), Ok(Next::Done(expected)));
    }

    #[test]
    fn missing_obligation_kept() {
        let mut delete = DeleteObligationOperation::new(vec![1], None);
        assert!(matches!(delete.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = IoResult::Values(vec![None]);
        assert_eq!(delete.next(answer), Ok(Next::Done(())));
    }

    #[test]
    fn empty_pass_moves_on() {
        let mut pass = ProcessObligationsOperation::new();
        assert!(matches!(pass.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = pass.next(IoResult::Values(vec![None]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let turn = pass.next(empty_page());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    }
}
