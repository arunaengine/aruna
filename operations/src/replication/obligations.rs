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
) -> Result<(), BlobQueueError> {
    if obligations.is_empty() && jobs.is_empty() {
        if cursor.0 != cursor.1 {
            write_obligation_cursor(storage, cursor.1).await?;
        }
        return Ok(());
    }
    let unexpected = |other: Event| BlobQueueError::UnexpectedEvent(other.label().to_string());
    // One drain at a time creates or parks member jobs, so this read needs no transaction.
    let existing = read_existing_jobs(storage, &jobs).await?;
    // Only a drain sets continuations; any other rewrite owes just the jobs queued here.
    let (fresh, obligations): (Vec<_>, Vec<_>) =
        (obligations.into_iter()).partition(|(_, expected)| expected.is_none());
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(unexpected(other)),
    };
    let reads: Vec<_> = (obligations.iter())
        .map(|(key, _)| {
            let key = ByteView::from(key.clone());
            (REPLICATION_OBLIGATION_KEYSPACE.to_string(), key)
        })
        .collect();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    };
    let values = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(match other {
                Event::Storage(StorageEvent::Error { error }) => error.into(),
                other => unexpected(other),
            });
        }
    };
    let mut writes = Vec::with_capacity(jobs.len() + 1);
    for (job, value) in jobs.iter().zip(existing) {
        let kept = value
            .as_deref()
            .and_then(|value| BlobJobRecord::from_bytes(value).ok())
            .is_some_and(|existing| blob_job_preferred(&existing, job));
        if !kept {
            writes.push(blob_job_entry(job)?);
        }
    }
    writes.push(obligation_cursor_entry(cursor.1)?);
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(match other {
                Event::Storage(StorageEvent::Error { error }) => error.into(),
                other => unexpected(other),
            });
        }
    }
    let deletes = (obligations.into_iter().zip(values))
        .filter(|((_, expected), (_, value))| {
            value
                .as_deref()
                .is_some_and(|value| match LiveObligationRecord::from_bytes(value) {
                    Ok(record) => record.continuation == *expected,
                    Err(_) => expected.is_none(),
                })
        })
        .map(|((key, _), _)| key)
        .chain(fresh.into_iter().map(|(key, _)| key))
        .map(|key| (REPLICATION_OBLIGATION_KEYSPACE.to_string(), key.into()))
        .collect();
    let delete = StorageEffect::BatchDelete {
        deletes,
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(match other {
                Event::Storage(StorageEvent::Error { error }) => error.into(),
                other => unexpected(other),
            });
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(unexpected(other)),
    }
}

/// The stored row of each job, from one range read while the range holds no other rows, as
/// lookups key by key cost far more than a range in these churned keyspaces.
async fn read_existing_jobs(
    storage: &StorageHandle,
    jobs: &[BlobJobRecord],
) -> Result<Vec<Option<ByteView>>, BlobQueueError> {
    let keys = jobs
        .iter()
        .map(blob_job_key)
        .collect::<Result<Vec<_>, _>>()?;
    let (Some(first), Some(last)) = (keys.iter().min(), keys.iter().max()) else {
        return Ok(Vec::new());
    };
    let range = StorageEffect::IterRange {
        key_space: REPLICATION_JOB_KEYSPACE.to_string(),
        start: IterStart::At(first.clone()),
        end: IterEnd::Through(last.clone()),
        limit: keys.len(),
        max_bytes: PAGE_BYTES,
        txn_id: None,
    };
    match storage.send_storage_effect(range).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }) => {
            let rows: HashMap<_, _> = values.into_iter().collect();
            return Ok(keys.iter().map(|key| rows.get(key).cloned()).collect());
        }
        // A range holding more rows than `jobs` is read key by key.
        Event::Storage(StorageEvent::IterResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
    let reads = (keys.into_iter())
        .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), key))
        .collect();
    match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

/// Obligations after `after`, and the key the next pass continues after; `None` once the
/// read reached the end.
async fn read_live_obligations(
    storage: &StorageHandle,
    after: Option<Vec<u8>>,
) -> Result<(Vec<(Vec<u8>, LiveObligationRecord)>, Option<Vec<u8>>), BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: after.map(|key| IterStart::After(key.into())),
            limit: OBLIGATION_BATCH_SIZE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let mut obligations = Vec::with_capacity(values.len());
            for (key, value) in values {
                match LiveObligationRecord::from_bytes(&value) {
                    Ok(record) => obligations.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed live replication obligation");
                        delete_live_obligation(storage, key, None).await?;
                    }
                }
            }
            Ok((obligations, next_start_after.map(|key| key.to_vec())))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn write_live_obligation(
    storage: &StorageHandle,
    record: &LiveObligationRecord,
) -> Result<(), BlobQueueError> {
    let (key_space, key, value) = live_obligation_entry(record)?;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let (current, valid) = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.clone(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => (None, true),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match LiveObligationRecord::from_bytes(&value) {
            Ok(record) => (Some(record), true),
            Err(_) => (None, false),
        },
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let should_write = !valid
        || match current.as_ref() {
            None => true,
            Some(current) => match (record.continuation.as_ref(), current.continuation.as_ref()) {
                (Some(candidate), Some(current)) => continuation_newer(candidate, current),
                (Some(_), None) => true,
                (None, None) | (None, Some(_)) => false,
            },
        };
    if !should_write {
        abort_cursor(storage, txn_id).await;
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn read_relationships_limit(
    storage: &StorageHandle,
    bucket: &str,
    start: Option<Vec<u8>>,
    limit: usize,
) -> Result<RelationshipPage, BlobQueueError> {
    let mut start_after = start.clone();
    let mut relationships = Vec::with_capacity(limit.min(REPLICATION_PAGE_SIZE));
    if limit == 0 {
        return Ok(RelationshipPage {
            values: relationships,
            next: None,
        });
    }
    loop {
        let page_limit = (limit - relationships.len()).min(REPLICATION_PAGE_SIZE);
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(sync_relationship_prefix(bucket).into()),
                start: start_after.take().map(|key| IterStart::After(key.into())),
                limit: page_limit,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
            }
        };
        for (key, value) in values {
            let relationship = SyncRelationship::from_bytes(&value)?;
            validate_sync_key(bucket, &key, &relationship)?;
            relationships.push((key.to_vec(), relationship));
        }
        if relationships.len() == limit {
            return Ok(RelationshipPage {
                values: relationships,
                next: next_start_after.map(|key| key.to_vec()),
            });
        }
        match next_start_after {
            Some(next) => start_after = Some(next.to_vec()),
            None => {
                return Ok(RelationshipPage {
                    values: relationships,
                    next: None,
                });
            }
        }
    }
}

pub(super) async fn write_live_jobs(
    storage: &StorageHandle,
    obligation: &LiveObligationRecord,
    relationships: Option<&RelationshipPage>,
    members: Vec<BlobJobRecord>,
) -> Result<LiveRepairWrite, BlobQueueError> {
    if obligation
        .origin
        .as_ref()
        .is_some_and(|origin| origin.hop_count >= 4)
    {
        warn!(
            bucket = %obligation.bucket,
            key = %obligation.key,
            version_id = %obligation.version_id,
            "Retiring live replication obligation at the hop limit"
        );
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation: None,
            progressed: true,
        });
    }
    let inbound_source = match obligation.origin.as_ref() {
        Some(origin) => read_inbound_source(storage, &obligation.bucket, origin).await?,
        None => None,
    };
    let mut upstream_sources = obligation.upstream_sources.clone();
    if let Some(source) = inbound_source
        && !upstream_sources
            .iter()
            .any(|existing| same_endpoint(existing, &source))
    {
        upstream_sources.push(source);
    }
    let mut cursor = obligation.continuation.clone().unwrap_or_default();
    // Publications to set members are bounded by the member count and queued once.
    let mut queued = 0;
    if !cursor.members_done && !members.is_empty() {
        queued = persist_live_jobs(storage, members, None).await?.queued;
    }
    cursor.members_done = true;
    let mut relationship_jobs = Vec::with_capacity(REPLICATION_JOB_LIMIT);
    if !cursor.relationships_complete {
        let Some(page) = relationships else {
            return Ok(LiveRepairWrite {
                queued: 0,
                continuation: None,
                progressed: false,
            });
        };
        let mut found = false;
        let mut complete = page.next.is_none();
        for (index, (key, relationship)) in page.values.iter().enumerate() {
            if cursor
                .relationship_after
                .as_ref()
                .is_some_and(|after| key <= after)
            {
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
            if let Some(job) = relationship_job(
                obligation.local_node_id,
                RelationshipJobTarget {
                    bucket: &obligation.bucket,
                    key: &obligation.key,
                    version_id: obligation.version_id,
                    delete_marker: obligation.delete_marker,
                },
                relationship.clone(),
                obligation.origin.as_ref(),
                &upstream_sources,
            )
            .map(|mut job| {
                if let Some(advance) = obligation.reference_advance {
                    job.input.auth_context = obligation.auth_context.clone();
                    job.writer_auth_context = None;
                    job.with_reference_advance(advance)
                } else {
                    job.with_writer_auth(obligation.auth_context.clone())
                }
            }) {
                if relationship_jobs.len() == REPLICATION_JOB_LIMIT {
                    complete = false;
                    break;
                }
                relationship_jobs.push(job);
                if relationship_jobs.len() == REPLICATION_JOB_LIMIT {
                    complete = index + 1 == page.values.len() && page.next.is_none();
                    break;
                }
            }
        }
        if !found {
            if page.next.is_none() {
                cursor.relationship_after = None;
                cursor.relationships_complete = true;
            } else {
                return Ok(LiveRepairWrite {
                    queued: 0,
                    continuation: None,
                    progressed: false,
                });
            }
        } else if complete {
            cursor.relationship_after = None;
            cursor.relationships_complete = true;
        } else {
            cursor.relationships_complete = false;
        }
    }
    let continuation = (!cursor.relationships_complete).then_some(cursor);
    let mut write = persist_live_jobs(storage, relationship_jobs, continuation).await?;
    write.queued = write.queued.saturating_add(queued);
    Ok(write)
}

async fn persist_live_jobs(
    storage: &StorageHandle,
    jobs: Vec<BlobJobRecord>,
    continuation: Option<LiveReplicationContinuation>,
) -> Result<LiveRepairWrite, BlobQueueError> {
    if jobs.is_empty() {
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation,
            progressed: true,
        });
    }

    let reads = jobs
        .iter()
        .map(|job| Ok((REPLICATION_JOB_KEYSPACE.to_string(), blob_job_key(job)?)))
        .collect::<Result<Vec<_>, ConversionError>>()?;
    let values = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    if values.len() != jobs.len() {
        return Err(BlobQueueError::UnexpectedEvent(
            "blob replication existing job read count mismatch".to_string(),
        ));
    }

    let mut writes = Vec::with_capacity(jobs.len());
    for (job, (_, value)) in jobs.iter().zip(values) {
        match value {
            Some(value) => match BlobJobRecord::from_bytes(&value) {
                Ok(existing) if blob_job_preferred(&existing, job) => {}
                Ok(_) | Err(_) => writes.push(blob_job_entry(job)?),
            },
            None => writes.push(blob_job_entry(job)?),
        }
    }
    if writes.is_empty() {
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation,
            progressed: true,
        });
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { entries }) => Ok(LiveRepairWrite {
            queued: entries.len(),
            continuation,
            progressed: true,
        }),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

async fn read_inbound_source(
    storage: &StorageHandle,
    bucket: &str,
    origin: &SyncOrigin,
) -> Result<Option<ArunaArn>, BlobQueueError> {
    let key = sync_relationship_key(bucket, origin.relationship_id);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: RELATIONSHIP_IN_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let relationship = SyncRelationship::from_bytes(&value)?;
            if relationship.id != origin.relationship_id
                || relationship.target.bucket() != Some(bucket)
                || sync_relationship_key(bucket, relationship.id) != key
            {
                return Err(ConversionError::FromStrError(
                    "incoming sync relationship key does not match payload".to_string(),
                )
                .into());
            }
            Ok(Some(relationship.source))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn delete_live_obligation(
    storage: &StorageHandle,
    key: Vec<u8>,
    expected: Option<&LiveReplicationContinuation>,
) -> Result<(), BlobQueueError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let current = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    };
    let should_delete = match current {
        None => false,
        Some(value) => match LiveObligationRecord::from_bytes(&value) {
            Ok(record) => record.continuation.as_ref() == expected,
            Err(_) => expected.is_none(),
        },
    };
    if !should_delete {
        abort_cursor(storage, txn_id).await;
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobQueueError::UnexpectedEvent(other.label().to_string()));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

async fn read_obligation_cursor(
    storage: &StorageHandle,
) -> Result<Option<Vec<u8>>, BlobQueueError> {
    let read = StorageEffect::Read {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: ByteView::from(OBLIGATION_CURSOR_KEY.to_vec()),
        txn_id: None,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .filter(|value| !value.is_empty())
            .map(|value| value.to_vec())),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

/// The cursor row; an empty value starts the next pass at the first obligation.
fn obligation_cursor_entry(
    after: Option<Vec<u8>>,
) -> Result<(String, Key, ByteView), BlobQueueError> {
    Ok((
        NODE_STATE_KEYSPACE.to_string(),
        ByteView::from(OBLIGATION_CURSOR_KEY.to_vec()),
        ByteView::from(after.unwrap_or_default()),
    ))
}

async fn write_obligation_cursor(
    storage: &StorageHandle,
    after: Option<Vec<u8>>,
) -> Result<(), BlobQueueError> {
    let (key_space, key, value) = obligation_cursor_entry(after)?;
    let write = StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id: None,
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}
