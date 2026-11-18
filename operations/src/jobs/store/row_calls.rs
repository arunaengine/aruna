//! The call forms of the job store's plain row reads, row writes and page walks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, page};
use aruna_core::structs::execution::job::{
    KeyWait, LocalCrate, RunLink, parse_schedule_key, parse_wait_key, run_crate_key,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;

use super::park_wake::{AwaitOutcome, ParkJobOperation, SatisfyWaitOperation};
use super::store_call::{RowBatch, RowRead, StoreCall};
use super::*;

/// Wake rows handled per transaction page.
pub const WAKE_PAGE: usize = 64;

fn storage(error: impl std::fmt::Display) -> JobMutationError {
    JobMutationError::Storage(error.to_string())
}

fn encoded(bytes: Result<Vec<u8>, impl std::fmt::Display>) -> Result<Value, JobMutationError> {
    bytes.map(ByteView::from).map_err(storage)
}

fn decoded<T, E: std::fmt::Display>(
    row: Option<Value>,
    decode: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<Option<T>, JobMutationError> {
    row.map(|row| decode(row.as_ref()).map_err(storage))
        .transpose()
}

/// Reads a job record; a malformed row is retained for offline recovery and reads as absent.
pub fn read_job_record(job_id: JobId) -> RowRead<Option<JobRecord>> {
    RowRead::new(JOB_KEYSPACE, job_record_key(job_id), move |row| {
        Ok(row.and_then(|row| match JobRecord::from_bytes(&row) {
            Ok(record) => Some(record),
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Malformed job record retained");
                None
            }
        }))
    })
}

/// Reads and decodes a state row, keeping a missing row distinct from a malformed one.
pub fn read_state<T: DeserializeOwned>(key_space: &str, key: Key) -> RowRead<Option<T>> {
    RowRead::new(key_space, key, |row| {
        decoded(row, |bytes| postcard::from_bytes::<T>(bytes))
    })
}

pub fn read_attempt_control(job_id: JobId, attempt_epoch: u64) -> RowRead<Option<AttemptControl>> {
    let key = ByteView::from(attempt_control_key(job_id, attempt_epoch));
    RowRead::new(ATTEMPT_CONTROL_KEYSPACE, key, |row| {
        decoded(row, AttemptControl::from_bytes)
    })
}

/// The executed launch record of the record's final attempt, if any.
pub fn read_launch_record(record: &JobRecord) -> RowRead<Option<LaunchRecord>> {
    let decode = |row: Option<Value>| {
        let launch = decoded(row, LaunchRecord::from_bytes)?;
        Ok(launch.filter(|launch| launch.started_at_ms.is_some()))
    };
    match &record.attempt_intent {
        Some(intent) => {
            let key = attempt_control_key(record.job_id, intent.attempt_epoch);
            RowRead::new(LAUNCH_RECORD_KEYSPACE, ByteView::from(key), decode)
        }
        None => RowRead::absent(decode),
    }
}

/// The signed output record the attempt already stored, if any. It shares the attempt-control
/// key, so pruning the job removes both.
pub fn read_output_record(job_id: JobId, attempt_epoch: u64) -> RowRead<Option<JobRecordEnvelope>> {
    let key = ByteView::from(attempt_control_key(job_id, attempt_epoch));
    RowRead::new(OUTPUT_RECORD_KEYSPACE, key, |row| {
        decoded(row, |bytes| postcard::from_bytes(bytes))
    })
}

pub fn read_purge_checkpoint(job_id: JobId) -> RowRead<Option<StoragePurgeCheckpoint>> {
    let key = ByteView::from(job_id.to_bytes().to_vec());
    RowRead::new(PURGE_CHECKPOINT_KEYSPACE, key, |row| {
        decoded(row, StoragePurgeCheckpoint::from_bytes)
    })
}

/// The crate of a local run and its owner, while this device keeps one.
pub fn read_local_crate(job_id: JobId) -> RowRead<Option<LocalCrate>> {
    let space = aruna_core::keyspaces::LOCAL_CRATE_KEYSPACE;
    RowRead::new(space, run_crate_key(job_id), |row| {
        decoded(row, LocalCrate::from_bytes)
    })
}

/// The run link of a physical job id, if recorded.
pub fn read_run_link(job_id: JobId) -> RowRead<Option<RunLink>> {
    let space = aruna_core::keyspaces::RUN_LINK_KEYSPACE;
    RowRead::new(space, run_crate_key(job_id), |row| {
        decoded(row, RunLink::from_bytes)
    })
}

/// The run-crate obligation status of `job_id`, if recorded.
pub fn read_crate_status(job_id: JobId) -> RowRead<Option<RunCrateStatus>> {
    RowRead::new(RUN_CRATE_KEYSPACE, run_crate_key(job_id), |row| {
        decoded(row, RunCrateStatus::from_bytes)
    })
}

pub(crate) fn artifact_tombstone_key(job_id: JobId) -> Key {
    ByteView::from(job_id.to_bytes().to_vec())
}

/// The owner a deleted job's artifacts stay reserved for, until the tombstone expires.
pub fn read_artifact_tombstone(job_id: JobId, now_ms: u64) -> RowRead<Option<UserId>> {
    let key = artifact_tombstone_key(job_id);
    RowRead::new(ARTIFACT_TOMBSTONE_KEYSPACE, key, move |row| {
        let tombstone = decoded(row, |bytes| {
            postcard::from_bytes::<ArtifactTombstone>(bytes)
        })?;
        Ok(tombstone
            .filter(|tombstone| tombstone.expires_at_ms > now_ms)
            .map(|tombstone| tombstone.owner))
    })
}

pub fn preserve_artifact_tombstone(job_id: JobId, owner: UserId, expires_at_ms: u64) -> RowBatch {
    let tombstone = ArtifactTombstone {
        owner,
        expires_at_ms,
    };
    RowBatch::write(encoded(postcard::to_allocvec(&tombstone)).map(|value| {
        vec![
            (
                ARTIFACT_TOMBSTONE_KEYSPACE.to_string(),
                artifact_tombstone_key(job_id),
                value,
            ),
            (
                SCHEDULE_INDEX_KEYSPACE.to_string(),
                job_prune_key(expires_at_ms, job_id),
                Value::from(Vec::<u8>::new()),
            ),
        ]
    }))
}

/// The run-crate obligation status, in its side keyspace; the terminal parent stays untouched.
pub fn put_crate_status(job_id: JobId, status: &RunCrateStatus) -> RowBatch {
    RowBatch::write(
        encoded(status.to_bytes())
            .map(|value| vec![(RUN_CRATE_KEYSPACE.to_string(), run_crate_key(job_id), value)]),
    )
}

/// The crate outcome and the run link in one batch, so a final status always has its link.
pub fn put_crate_outcome(job_id: JobId, status: &RunCrateStatus, link: &RunLink) -> RowBatch {
    let key = run_crate_key(job_id);
    let rows = encoded(status.to_bytes()).and_then(|status| {
        let link = encoded(link.to_bytes())?;
        Ok(vec![
            (RUN_CRATE_KEYSPACE.to_string(), key.clone(), status),
            (
                aruna_core::keyspaces::RUN_LINK_KEYSPACE.to_string(),
                key,
                link,
            ),
        ])
    });
    RowBatch::write(rows)
}

/// The crate of an unpublished local run with its `Local` status in one batch.
pub fn put_local_crate(job_id: JobId, local: &LocalCrate) -> RowBatch {
    let key = run_crate_key(job_id);
    let rows = encoded(RunCrateStatus::Local.to_bytes()).and_then(|status| {
        let local = encoded(local.to_bytes())?;
        Ok(vec![
            (RUN_CRATE_KEYSPACE.to_string(), key.clone(), status),
            (
                aruna_core::keyspaces::LOCAL_CRATE_KEYSPACE.to_string(),
                key,
                local,
            ),
        ])
    });
    RowBatch::write(rows)
}

pub fn insert_job(record: &JobRecord) -> RowBatch {
    RowBatch::write(job_insert_entries(record).map_err(storage))
}

pub fn delete_rows(deletes: JobDeletes) -> RowBatch {
    RowBatch::delete(deletes)
}

/// The job and plan digest a dedup key names.
pub fn find_dedup_plan(created_by: UserId, dedup_key: &[u8]) -> RowRead<Option<(JobId, [u8; 32])>> {
    let key = dedup_index_key(created_by, dedup_key);
    RowRead::new(DEDUP_INDEX_KEYSPACE, key, |row| {
        decoded(row, parse_dedup_value)
    })
}

pub fn find_dedup_job(created_by: UserId, dedup_key: &[u8]) -> RowRead<Option<JobId>> {
    let key = dedup_index_key(created_by, dedup_key);
    RowRead::new(DEDUP_INDEX_KEYSPACE, key, |row| {
        let plan = decoded(row, parse_dedup_value)?;
        Ok(plan.map(|(job_id, _)| job_id))
    })
}

/// The keys a parked job still waits for; empty when it is not parked.
pub fn read_key_waits(job_id: JobId) -> RowRead<Vec<KeyWait>> {
    RowRead::new(KEY_WAIT_KEYSPACE, job_wait_key(job_id), |row| {
        let waits = decoded(row, |bytes| postcard::from_bytes::<Vec<KeyWait>>(bytes))?;
        Ok(waits.unwrap_or_default())
    })
}

type PageRows = Vec<(Key, Value)>;
type DecodePage<T> = Box<dyn FnMut(PageRows, Option<Key>) -> Result<T, JobMutationError> + Send>;

/// Reads one page of rows under a prefix and decodes it.
pub struct PageRead<T> {
    space: &'static str,
    prefix: Key,
    after: Option<Key>,
    limit: usize,
    decode: DecodePage<T>,
}

impl<T> std::fmt::Debug for PageRead<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRead")
            .field("space", &self.space)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl<T> PartialEq for PageRead<T> {
    fn eq(&self, other: &Self) -> bool {
        self.space == other.space && self.prefix == other.prefix && self.after == other.after
    }
}

impl<T: Send + std::fmt::Debug> Operation for PageRead<T> {
    type Output = T;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<T>, JobMutationError> {
        Ok(Next::Call(IoCall::Page {
            space: self.space.to_string(),
            prefix: self.prefix.clone(),
            after: self.after.clone(),
            limit: self.limit,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, JobMutationError> {
        let (rows, next) = page(result)?;
        (self.decode)(rows, next).map(Next::Done)
    }
}

/// One page of a job's entries with their plain keys, and the entry to continue after.
pub type EntryPage = (Vec<(Vec<u8>, Value)>, Option<Vec<u8>>);

pub fn list_job_entries(
    job_id: JobId,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> PageRead<EntryPage> {
    PageRead {
        space: JOB_ENTRY_KEYSPACE,
        prefix: job_entry_prefix(job_id),
        after: last_key.map(|key| job_entry_key(job_id, &key)),
        limit,
        decode: Box::new(move |rows, next| {
            let entries = rows
                .into_iter()
                .map(|(key, value)| Ok((parse_entry_key(job_id, key.as_ref())?, value)))
                .collect::<Result<Vec<_>, ConversionError>>()
                .map_err(storage)?;
            let next = next
                .map(|key| parse_entry_key(job_id, key.as_ref()))
                .transpose()
                .map_err(storage)?;
            Ok((entries, next))
        }),
    }
}

/// One bounded page of a job's entry rows to delete; `true` while more remain.
pub fn job_entry_deletes(job_id: JobId, limit: usize) -> PageRead<(JobDeletes, bool)> {
    PageRead {
        space: JOB_ENTRY_KEYSPACE,
        prefix: job_entry_prefix(job_id),
        after: None,
        limit,
        decode: Box::new(|rows, next| {
            let deletes = rows
                .into_iter()
                .map(|(key, _)| (JOB_ENTRY_KEYSPACE.to_string(), key))
                .collect();
            Ok((deletes, next.is_some()))
        }),
    }
}

const STAGING_SPACES: [&str; 3] = [
    aruna_core::keyspaces::STAGING_QUEUE_KEYSPACE,
    aruna_core::keyspaces::STAGING_MANIFEST_KEYSPACE,
    aruna_core::keyspaces::STAGING_STATE_KEYSPACE,
];

/// One bounded page of a staging job's queue, manifest and lane rows; `true` while more remain.
#[derive(Debug, PartialEq)]
pub struct StagingDeletesOperation {
    prefix: Key,
    limit: usize,
    space: usize,
    deletes: JobDeletes,
    more: bool,
}

type StagingNext = Result<Next<(JobDeletes, bool)>, JobMutationError>;

/// See [`StagingDeletesOperation`].
pub fn staging_row_deletes(job_id: JobId, limit: usize) -> StagingDeletesOperation {
    StagingDeletesOperation {
        prefix: ByteView::from(job_id.to_bytes().to_vec()),
        limit,
        space: 0,
        deletes: Vec::new(),
        more: false,
    }
}

impl StagingDeletesOperation {
    fn read_next(&mut self) -> StagingNext {
        let room = self.limit.saturating_sub(self.deletes.len());
        let Some(space) = STAGING_SPACES.get(self.space) else {
            return Ok(Next::Done((std::mem::take(&mut self.deletes), self.more)));
        };
        if room == 0 {
            return Ok(Next::Done((std::mem::take(&mut self.deletes), true)));
        }
        Ok(Next::Call(IoCall::Page {
            space: space.to_string(),
            prefix: self.prefix.clone(),
            after: None,
            limit: room,
        }))
    }
}

impl Operation for StagingDeletesOperation {
    type Output = (JobDeletes, bool);
    type Error = JobMutationError;

    fn first(&mut self) -> StagingNext {
        self.read_next()
    }

    fn next(&mut self, result: IoResult) -> StagingNext {
        let (rows, next) = page(result)?;
        self.more |= next.is_some();
        let space = STAGING_SPACES[self.space];
        let deletes = rows.into_iter().map(|(key, _)| (space.to_string(), key));
        self.deletes.extend(deletes);
        self.space += 1;
        self.read_next()
    }
}

/// Earliest `(timestamp, job_id)` under a schedule-index prefix. A malformed row is deleted and
/// reads as no entry.
#[derive(Debug, PartialEq)]
pub struct FirstScheduleOperation {
    prefix: Key,
    deleting: bool,
}

/// See [`FirstScheduleOperation`].
pub fn first_schedule_entry(prefix: &[u8]) -> FirstScheduleOperation {
    FirstScheduleOperation {
        prefix: ByteView::from(prefix.to_vec()),
        deleting: false,
    }
}

impl Operation for FirstScheduleOperation {
    type Output = Option<(u64, JobId)>;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<Self::Output>, JobMutationError> {
        Ok(Next::Call(IoCall::Page {
            space: SCHEDULE_INDEX_KEYSPACE.to_string(),
            prefix: self.prefix.clone(),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, JobMutationError> {
        if self.deleting {
            done(result)?;
            return Ok(Next::Done(None));
        }
        let (rows, _) = page(result)?;
        let Some((key, _)) = rows.into_iter().next() else {
            return Ok(Next::Done(None));
        };
        match parse_schedule_key(key.as_ref()) {
            Ok(parsed) => Ok(Next::Done(Some(parsed))),
            Err(error) => {
                warn!(error = %error, "Deleting malformed job schedule index row");
                self.deleting = true;
                let row = (SCHEDULE_INDEX_KEYSPACE.to_string(), key);
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
        }
    }
}

/// Parks the claimed job unless it settled or a cancel was requested meanwhile.
pub(super) fn park_mutation(
    token: Ulid,
    now_ms: u64,
) -> impl FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Clone + Send + 'static {
    move |record: &mut JobRecord| {
        guard_token(record, token)?;
        if record.is_settled() || record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        // An attempt intent may already have reached an executor; it cannot wait here.
        if record.attempt_intent.is_some() {
            return Err(JobMutationError::IntentConflict);
        }
        record.state = JobState::AwaitingKey;
        record.claim = None;
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    }
}

/// Parks a claimed job until every key in `waits` is unlocked; see [`ParkJobOperation`].
pub fn park_job(
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    waits: Vec<KeyWait>,
) -> StoreCall<AwaitOutcome> {
    if waits.is_empty() {
        return StoreCall::refused(JobMutationError::Storage("park without key waits".into()));
    }
    let mutate = park_mutation(token, now_ms);
    let build = move || ParkJobOperation::new(job_id, mutate.clone(), waits.clone());
    StoreCall::of(build, Ok, "job park exhausted retries")
}

/// Marks `key` available for one job; see [`SatisfyWaitOperation`].
pub fn satisfy_key_wait(job_id: JobId, key: BucketKeyRef, now_ms: u64) -> StoreCall<bool> {
    let build = move || SatisfyWaitOperation::new(job_id, key, now_ms);
    StoreCall::of(build, Ok, "key wake exhausted retries")
}

#[derive(Debug, PartialEq)]
enum WakeStep {
    Page,
    Delete,
    Satisfy,
}

/// Wakes one page of jobs waiting for `key` and answers how many woke and whether a further
/// page may follow. Handled rows are deleted, so callers repeat while one may follow.
#[derive(Debug, PartialEq)]
pub struct WakePageOperation {
    key: BucketKeyRef,
    now_ms: u64,
    rows: Vec<Key>,
    full: bool,
    woken: usize,
    step: WakeStep,
}

type WakeNext = Result<Next<(usize, bool)>, JobMutationError>;

/// See [`WakePageOperation`].
pub fn wake_key_page(key: BucketKeyRef, now_ms: u64) -> WakePageOperation {
    WakePageOperation {
        key,
        now_ms,
        rows: Vec::new(),
        full: false,
        woken: 0,
        step: WakeStep::Page,
    }
}

impl WakePageOperation {
    fn wake_next(&mut self) -> WakeNext {
        let Some(row) = self.rows.pop() else {
            return Ok(Next::Done((self.woken, self.full)));
        };
        let Some((_, job_id)) = parse_wait_key(row.as_ref()) else {
            warn!("Deleting malformed key wait row");
            self.step = WakeStep::Delete;
            let row = (KEY_WAIT_KEYSPACE.to_string(), row);
            return Ok(Next::Call(IoCall::Delete(vec![row])));
        };
        self.step = WakeStep::Satisfy;
        let satisfy = satisfy_key_wait(job_id, self.key, self.now_ms);
        Ok(Next::Call(IoCall::child(satisfy)))
    }
}

impl Operation for WakePageOperation {
    type Output = (usize, bool);
    type Error = JobMutationError;

    fn first(&mut self) -> WakeNext {
        Ok(Next::Call(IoCall::Page {
            space: KEY_WAIT_KEYSPACE.to_string(),
            prefix: ByteView::from([&b"k"[..], &self.key.key()].concat()),
            after: None,
            limit: WAKE_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> WakeNext {
        match self.step {
            WakeStep::Page => {
                let (rows, _) = page(result)?;
                self.full = rows.len() == WAKE_PAGE;
                self.rows = rows.into_iter().rev().map(|(key, _)| key).collect();
            }
            WakeStep::Delete => done(result)?,
            WakeStep::Satisfy => {
                if result.child::<bool, JobMutationError>()?? {
                    self.woken += 1;
                }
            }
        }
        self.wake_next()
    }
}

/// Wakes every job waiting for `key` in bounded pages; run it after the key is installed.
#[derive(Debug, PartialEq)]
pub struct WakeKeysOperation {
    key: BucketKeyRef,
    now_ms: u64,
    total: usize,
}

/// See [`WakeKeysOperation`].
pub fn wake_key_waits(key: BucketKeyRef, now_ms: u64) -> WakeKeysOperation {
    WakeKeysOperation {
        key,
        now_ms,
        total: 0,
    }
}

impl Operation for WakeKeysOperation {
    type Output = usize;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<usize>, JobMutationError> {
        let page = wake_key_page(self.key, self.now_ms);
        Ok(Next::Call(IoCall::child(page)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, JobMutationError> {
        let (woken, more) = result.child::<(usize, bool), JobMutationError>()??;
        self.total += woken;
        match more {
            true => self.first(),
            false => Ok(Next::Done(self.total)),
        }
    }
}
