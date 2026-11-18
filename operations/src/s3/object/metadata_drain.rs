//! Drains the reference metadata refresh queue: refreshes due references and retries failed ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, REFRESH_DUE_KEYSPACE, REFRESH_JOB_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::source_access::{ObservationMatch, SourceMetadata};
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, VersionKey};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;
use tracing::{info, warn};

use super::metadata::{
    ReferenceRefresh, ReferenceRefreshError, ReferenceRefreshRecord, due_key, job_entries,
    split_due,
};
use crate::replication::queue::schedule_blob_drain;
use crate::s3::object::refresh_mint::{MintError, MintSuccessorOperation};
use crate::s3::object::refresh_observe::{ObserveError, ObserveSourceOperation};
use crate::s3::usage::schedule_snapshot_publish;
use crate::tasks::queue_backoff::{due_after, min_due_at, retry_delay_ms};

const REFRESH_SCAN_PAGE: usize = 512;

const REFRESH_BATCH_SIZE: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceDrainResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error, PartialEq)]
pub enum RefreshRunError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Mint(#[from] MintError),
    #[error(transparent)]
    Observe(#[from] ObserveError),
    #[error("the refresh carries no observation")]
    NoObservation,
}

impl From<IoError> for RefreshRunError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

#[derive(Debug, PartialEq)]
enum ApplyState {
    ReadVersion,
    Mint(Box<MintSuccessorOperation>),
    WriteVersion,
}

/// Applies one observation in one transaction: a confirmed change becomes a successor, and a
/// confirmed match moves the version's refresh time. Outputs whether a successor was written.
#[derive(Debug, PartialEq)]
struct RefreshApply {
    refresh: ReferenceRefresh,
    state: ApplyState,
    version_key: Vec<u8>,
    refreshed: Option<Vec<u8>>,
    minted: bool,
}

type ApplyTurn = Result<Next<bool>, RefreshRunError>;

impl RefreshApply {
    fn new(refresh: ReferenceRefresh) -> Self {
        Self {
            refresh,
            state: ApplyState::ReadVersion,
            version_key: Vec::new(),
            refreshed: None,
            minted: false,
        }
    }

    fn version_read(&mut self, value: Option<Value>) -> ApplyTurn {
        let Some(value) = value else {
            return Ok(Next::Done(false));
        };
        let version = BlobVersion::from_bytes(value.as_ref())?;
        let BlobVersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = &version.state
        else {
            return Ok(Next::Done(false));
        };
        // Only a confirmed observation counts as a refresh; fields under the version id never
        // change, so a confirmed change becomes a successor and an unknown one is dropped.
        let observation = (self.refresh.metadata.clone()).ok_or(RefreshRunError::NoObservation)?;
        let comparison = cached_metadata.compare(&observation);
        // A version whose recorded digests bind its bytes gets no successor without them.
        let bound = !cached_metadata.enforced_checksums().is_empty();
        let newer = self.refresh.refreshed_at > *last_refresh;
        let mint = newer && comparison == ObservationMatch::Changed && !bound;
        if newer && comparison == ObservationMatch::Same {
            self.refreshed = Some(self.with_refresh_time(&version)?);
        }
        if !mint {
            return self.write_version();
        }
        let refresh = self.refresh.clone();
        let mut mint = Box::new(MintSuccessorOperation::new(refresh, observation, version));
        let turn = mint.first();
        self.state = ApplyState::Mint(mint);
        self.mint_turn(turn)
    }

    /// The stored version with only its refresh time moved.
    fn with_refresh_time(&self, version: &BlobVersion) -> Result<Vec<u8>, ConversionError> {
        let mut version = version.clone();
        if let BlobVersionState::Reference { last_refresh, .. } = &mut version.state {
            *last_refresh = self.refresh.refreshed_at;
        }
        version.to_bytes()
    }

    fn mint_turn(&mut self, turn: Result<Next<bool>, MintError>) -> ApplyTurn {
        match turn? {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Done(written) => {
                self.minted = written;
                self.write_version()
            }
            Next::Wait | Next::Then { .. } => Err(IoError::Unfinished.into()),
        }
    }

    fn write_version(&mut self) -> ApplyTurn {
        let Some(value) = self.refreshed.take() else {
            return Ok(Next::Done(self.minted));
        };
        self.state = ApplyState::WriteVersion;
        Ok(Next::Call(IoCall::Write(vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            self.version_key.clone().into(),
            value.into(),
        )])))
    }
}

impl Operation for RefreshApply {
    type Output = bool;
    type Error = RefreshRunError;

    fn first(&mut self) -> ApplyTurn {
        let refresh = &self.refresh;
        let key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id);
        self.version_key = key.to_bytes()?;
        let read = read_row(BLOB_VERSIONS_KEYSPACE, self.version_key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        match &mut self.state {
            ApplyState::ReadVersion => self.version_read(result.value()?),
            ApplyState::Mint(mint) => {
                let turn = mint.next(result);
                self.mint_turn(turn)
            }
            ApplyState::WriteVersion => {
                result.done()?;
                Ok(Next::Done(self.minted))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
enum RefreshStep {
    Observe,
    Apply,
}

/// Refreshes one reference version. Without an observation it observes the source first; a
/// failed observation fails the refresh, which the queue retries later.
#[derive(Debug, PartialEq)]
pub struct RefreshReferenceOperation {
    refresh: ReferenceRefresh,
    step: RefreshStep,
}

type RefreshTurn = Result<Next<()>, RefreshRunError>;

impl RefreshReferenceOperation {
    pub fn new(refresh: ReferenceRefresh) -> Self {
        Self {
            refresh,
            step: RefreshStep::Apply,
        }
    }

    fn apply(&mut self) -> RefreshTurn {
        self.step = RefreshStep::Apply;
        Ok(Next::Call(IoCall::child(RefreshApply::new(
            self.refresh.clone(),
        ))))
    }

    fn observed(&mut self, observed: Result<SourceMetadata, ObserveError>) -> RefreshTurn {
        match observed {
            Ok(metadata) => {
                self.refresh.metadata = Some(metadata);
                self.refresh.refreshed_at = SystemTime::now().max(self.refresh.refreshed_at);
                self.apply()
            }
            // Work for a version that is gone or no longer resolvable is finished, not retried.
            Err(error) if error.is_obsolete() => Ok(Next::Done(())),
            Err(error) => Err(error.into()),
        }
    }

    fn applied(&self, applied: Result<bool, RefreshRunError>) -> RefreshTurn {
        match applied {
            // The dirty markers and the obligation committed above are the durable work.
            Ok(true) => Ok(Next::Then {
                output: (),
                calls: vec![
                    schedule_snapshot_publish().into(),
                    schedule_blob_drain().into(),
                ],
            }),
            Ok(false) => Ok(Next::Done(())),
            // The key's range moved to another coordinator, which refreshes it there.
            Err(RefreshRunError::Mint(MintError::Distribution(error))) if error.moved_away() => {
                Ok(Next::Done(()))
            }
            Err(error) => Err(error),
        }
    }
}

impl Operation for RefreshReferenceOperation {
    type Output = ();
    type Error = RefreshRunError;

    fn first(&mut self) -> RefreshTurn {
        if self.refresh.metadata.is_some() {
            return self.apply();
        }
        let refresh = &self.refresh;
        let key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id);
        self.step = RefreshStep::Observe;
        Ok(Next::Call(IoCall::child(ObserveSourceOperation::new(key))))
    }

    fn next(&mut self, result: IoResult) -> RefreshTurn {
        match self.step {
            RefreshStep::Observe => self.observed(result.child::<SourceMetadata, ObserveError>()?),
            RefreshStep::Apply => self.applied(result.child::<bool, RefreshRunError>()?),
        }
    }
}

fn due_page(limit: usize) -> IoCall {
    IoCall::Page {
        space: REFRESH_DUE_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after: None,
        limit,
    }
}

/// Sets the drain timer for the earliest queued refresh, if any.
#[derive(Debug, Default, PartialEq)]
pub struct RestoreRefreshTimer {
    setting: bool,
}

impl RestoreRefreshTimer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Operation for RestoreRefreshTimer {
    type Output = ();
    type Error = ReferenceRefreshError;

    fn first(&mut self) -> Result<Next<()>, ReferenceRefreshError> {
        Ok(Next::Call(due_page(1)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ReferenceRefreshError> {
        if self.setting {
            if let Event::Task(TaskEvent::Error { message, .. }) = result.event()? {
                warn!(message = %message, "Failed to restore reference metadata refresh timer");
            }
            return Ok(Next::Done(()));
        }
        let (rows, _) = result.page()?;
        let due = rows.first().and_then(|(row, _)| split_due(row));
        let Some((due_at_ms, _)) = due else {
            return Ok(Next::Done(()));
        };
        self.setting = true;
        let reset = Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainRefreshQueue,
            after: due_after(unix_timestamp_millis(), due_at_ms),
        });
        Ok(Next::Call(reset.into()))
    }
}

/// Removes a finished job, or replaces it with `next`, unless a newer request replaced it.
#[derive(Debug, PartialEq)]
pub(super) struct SettleJob {
    key: Vec<u8>,
    job: ReferenceRefreshRecord,
    next: Option<ReferenceRefreshRecord>,
    state: SettleState,
}

#[derive(Debug, PartialEq)]
enum SettleState {
    ReadJob,
    DeleteRows,
    WriteNext,
}

type SettleTurn = Result<Next<()>, ReferenceRefreshError>;

impl SettleJob {
    pub(super) fn new(
        key: Vec<u8>,
        job: ReferenceRefreshRecord,
        next: Option<ReferenceRefreshRecord>,
    ) -> Self {
        Self {
            key,
            job,
            next,
            state: SettleState::ReadJob,
        }
    }

    fn job_read(&mut self, value: Option<Value>) -> SettleTurn {
        let stored = value.map(|value| postcard::from_bytes::<ReferenceRefreshRecord>(&value));
        if !matches!(stored, Some(Ok(stored)) if stored == self.job) {
            return Ok(Next::Done(()));
        }
        let due = due_key(self.job.due_at_ms, &self.key);
        let mut deletes = vec![(REFRESH_DUE_KEYSPACE.to_string(), due)];
        if self.next.is_none() {
            let key = ByteView::from(self.key.clone());
            deletes.push((REFRESH_JOB_KEYSPACE.to_string(), key));
        }
        self.state = SettleState::DeleteRows;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn rows_deleted(&mut self) -> SettleTurn {
        let Some(next) = self.next.as_ref() else {
            return Ok(Next::Done(()));
        };
        let writes = job_entries(next)?;
        self.state = SettleState::WriteNext;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for SettleJob {
    type Output = ();
    type Error = ReferenceRefreshError;

    fn first(&mut self) -> SettleTurn {
        let read = read_row(REFRESH_JOB_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> SettleTurn {
        match self.state {
            SettleState::ReadJob => self.job_read(result.value()?),
            SettleState::DeleteRows => {
                result.done()?;
                self.rows_deleted()
            }
            SettleState::WriteNext => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
enum DrainState {
    ReadDue,
    ReadJob(Key),
    DeleteStale,
    RunJob,
    Settle { failed: bool },
}

/// Runs one batch of due refreshes: walks a page of the due index in due order with a point read
/// per row, refreshes each job and settles it, rescheduling the ones that failed.
#[derive(Debug, PartialEq)]
pub struct DrainRefreshOperation {
    state: DrainState,
    started: Instant,
    now_ms: u64,
    rows: VecDeque<Key>,
    more_rows: bool,
    jobs: VecDeque<(Vec<u8>, ReferenceRefreshRecord)>,
    current: Option<(Vec<u8>, ReferenceRefreshRecord)>,
    job_count: usize,
    oldest_lag_ms: u64,
    scan_ms: u64,
    succeeded: usize,
    failed: usize,
    has_more_due: bool,
    next_due_ms: Option<u64>,
}

type DrainTurn = Result<Next<ReferenceDrainResult>, ReferenceRefreshError>;

impl Default for DrainRefreshOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl DrainRefreshOperation {
    pub fn new() -> Self {
        Self {
            state: DrainState::ReadDue,
            started: Instant::now(),
            now_ms: 0,
            rows: VecDeque::new(),
            more_rows: false,
            jobs: VecDeque::new(),
            current: None,
            job_count: 0,
            oldest_lag_ms: 0,
            scan_ms: 0,
            succeeded: 0,
            failed: 0,
            has_more_due: false,
            next_due_ms: None,
        }
    }

    fn call(&mut self, state: DrainState, call: IoCall) -> DrainTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    /// Takes the next due row: a job to read, a broken row to drop, or the end of the scan.
    fn walk_rows(&mut self) -> DrainTurn {
        let Some(row) = self.rows.pop_front() else {
            self.has_more_due = self.more_rows;
            return self.start_jobs();
        };
        let Some((due_at_ms, key)) = split_due(&row) else {
            let delete = vec![(REFRESH_DUE_KEYSPACE.to_string(), row)];
            return self.call(DrainState::DeleteStale, IoCall::Delete(delete));
        };
        if due_at_ms > self.now_ms {
            self.next_due_ms = Some(due_at_ms);
            return self.start_jobs();
        }
        if self.jobs.len() >= REFRESH_BATCH_SIZE {
            self.has_more_due = true;
            return self.start_jobs();
        }
        let read = read_row(REFRESH_JOB_KEYSPACE, key.to_vec());
        self.call(DrainState::ReadJob(row), read)
    }

    fn job_read(&mut self, row: Key, value: Option<Value>) -> DrainTurn {
        let Some((due_at_ms, key)) = split_due(&row) else {
            return self.walk_rows();
        };
        let key = key.to_vec();
        let mut stale = vec![(REFRESH_DUE_KEYSPACE.to_string(), row.clone())];
        match value.map(|value| postcard::from_bytes::<ReferenceRefreshRecord>(&value)) {
            Some(Ok(job)) if job.due_at_ms == due_at_ms => {
                self.jobs.push_back((key, job));
                return self.walk_rows();
            }
            Some(Ok(_)) | None => {}
            Some(Err(error)) => {
                warn!(error = %error, key = ?key, "Deleting malformed reference metadata refresh job");
                stale.push((REFRESH_JOB_KEYSPACE.to_string(), ByteView::from(key)));
            }
        }
        self.call(DrainState::DeleteStale, IoCall::Delete(stale))
    }

    fn start_jobs(&mut self) -> DrainTurn {
        self.scan_ms = duration_ms(self.started.elapsed());
        self.job_count = self.jobs.len();
        self.oldest_lag_ms = (self.jobs.iter())
            .map(|(_, job)| self.now_ms.saturating_sub(job.due_at_ms))
            .max()
            .unwrap_or(0);
        self.next_job()
    }

    fn next_job(&mut self) -> DrainTurn {
        let Some((key, job)) = self.jobs.pop_front() else {
            return Ok(Next::Done(self.summary()));
        };
        let refresh = RefreshReferenceOperation::new(job.refresh.clone());
        self.current = Some((key, job));
        self.call(DrainState::RunJob, IoCall::child(refresh))
    }

    fn job_ran(&mut self, outcome: Result<(), RefreshRunError>) -> DrainTurn {
        let (key, job) = self.current.take().ok_or(IoError::Unfinished)?;
        let Err(error) = outcome else {
            let settle = SettleJob::new(key, job, None);
            let state = DrainState::Settle { failed: false };
            return self.call(state, IoCall::child(settle));
        };
        let attempts = job.attempts.saturating_add(1);
        let due_at_ms = unix_timestamp_millis().saturating_add(retry_delay_ms(attempts));
        let next = ReferenceRefreshRecord {
            refresh: job.refresh.clone(),
            due_at_ms,
            attempts,
            last_error: Some(error.to_string()),
        };
        self.next_due_ms = min_due_at(self.next_due_ms, due_at_ms);
        let settle = SettleJob::new(key, job, Some(next));
        let state = DrainState::Settle { failed: true };
        self.call(state, IoCall::child(settle))
    }

    fn summary(&self) -> ReferenceDrainResult {
        if self.job_count > 0 {
            info!(
                event = "pipeline.reference_metadata_refresh.summary",
                jobs = self.job_count,
                succeeded = self.succeeded,
                failed = self.failed,
                scan_ms = self.scan_ms,
                total_ms = duration_ms(self.started.elapsed()),
                oldest_lag_ms = self.oldest_lag_ms,
                has_more_due = self.has_more_due,
                "Reference metadata refresh batch summary"
            );
        }
        let next_due_after = (self.next_due_ms.filter(|_| !self.has_more_due))
            .map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms));
        ReferenceDrainResult {
            processed: self.job_count,
            succeeded: self.succeeded,
            failed: self.failed,
            has_more_due: self.has_more_due,
            next_due_after,
        }
    }
}

impl Operation for DrainRefreshOperation {
    type Output = ReferenceDrainResult;
    type Error = ReferenceRefreshError;

    fn first(&mut self) -> DrainTurn {
        self.started = Instant::now();
        self.now_ms = unix_timestamp_millis();
        Ok(Next::Call(due_page(REFRESH_SCAN_PAGE)))
    }

    fn next(&mut self, result: IoResult) -> DrainTurn {
        match std::mem::replace(&mut self.state, DrainState::DeleteStale) {
            DrainState::ReadDue => {
                let (rows, next) = result.page()?;
                self.rows = rows.into_iter().map(|(row, _)| row).collect();
                self.more_rows = next.is_some();
                self.walk_rows()
            }
            DrainState::ReadJob(row) => self.job_read(row, result.value()?),
            DrainState::DeleteStale => {
                result.done()?;
                self.walk_rows()
            }
            DrainState::RunJob => self.job_ran(result.child::<(), RefreshRunError>()?),
            DrainState::Settle { failed } => {
                result.child::<(), ReferenceRefreshError>()??;
                match failed {
                    true => self.failed = self.failed.saturating_add(1),
                    false => self.succeeded = self.succeeded.saturating_add(1),
                }
                self.next_job()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use std::collections::HashMap;
    use ulid::Ulid;

    fn observation(content_length: u64) -> SourceMetadata {
        SourceMetadata {
            content_length,
            content_type: None,
            etag: Some(format!("etag-{content_length}")),
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }
    }

    if job_count > 0 {
        info!(
            event = "pipeline.reference_metadata_refresh.summary",
            jobs = job_count,
            succeeded,
            failed,
            scan_ms = duration_ms(scan_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Reference metadata refresh batch summary"
        );
    }

    Ok(ReferenceDrainResult {
        processed: job_count,
        succeeded,
        failed,
        has_more_due,
        next_due_after: if has_more_due {
            None
        } else {
            next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

/// The first `limit` due index rows and whether more follow.
async fn first_due(
    storage: &StorageHandle,
    limit: usize,
) -> Result<(Vec<(Key, ByteView)>, bool), ReferenceRefreshError> {
    let event = send(
        storage,
        StorageEffect::Iter {
            key_space: REFRESH_DUE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit,
            txn_id: None,
        },
    )
    .await?;
    match event {
        StorageEvent::IterResult {
            values,
            next_start_after,
        } => Ok((values, next_start_after.is_some())),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

/// Walks one page of the due index in due order, with a point read per due row.
async fn scan_due_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<ReferenceRefreshScan, ReferenceRefreshError> {
    let (rows, more_rows) = first_due(storage, REFRESH_SCAN_PAGE).await?;
    let mut jobs = Vec::new();
    for (row, _) in rows {
        let Some((due_at_ms, key)) = split_due(&row) else {
            delete_rows(storage, vec![(REFRESH_DUE_KEYSPACE.to_string(), row)], None).await?;
            continue;
        };
        if due_at_ms > now_ms {
            return Ok(ReferenceRefreshScan {
                jobs,
                has_more_due: false,
                next_due_ms: Some(due_at_ms),
            });
        }
        if jobs.len() >= limit {
            return Ok(ReferenceRefreshScan {
                jobs,
                has_more_due: true,
                next_due_ms: None,
            });
        }
        let mut stale = vec![(REFRESH_DUE_KEYSPACE.to_string(), row.clone())];
        match read_job(storage, key, None).await? {
            Some(Ok(job)) if job.due_at_ms == due_at_ms => {
                jobs.push((key.to_vec(), job));
                continue;
            }
            Some(Ok(_)) | None => {}
            Some(Err(error)) => {
                warn!(error = %error, key = ?key, "Deleting malformed reference metadata refresh job");
                let key = ByteView::from(key.to_vec());
                stale.push((REFRESH_JOB_KEYSPACE.to_string(), key));
            }
        }
        delete_rows(storage, stale, None).await?;
    }
    Ok(ReferenceRefreshScan {
        jobs,
        has_more_due: more_rows,
        next_due_ms: None,
    })
}

async fn send(
    storage: &StorageHandle,
    effect: StorageEffect,
) -> Result<StorageEvent, ReferenceRefreshError> {
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        Event::Storage(event) => Ok(event),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            other.label().to_string(),
        )),
    }
}

async fn read_job(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<Ulid>,
) -> Result<Option<Result<ReferenceRefreshRecord, postcard::Error>>, ReferenceRefreshError> {
    let effect = StorageEffect::Read {
        key_space: REFRESH_JOB_KEYSPACE.to_string(),
        key: ByteView::from(key.to_vec()),
        txn_id,
    };
    match send(storage, effect).await? {
        StorageEvent::ReadResult { value, .. } => {
            Ok(value.map(|value| postcard::from_bytes(&value)))
        }
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

async fn delete_rows(
    storage: &StorageHandle,
    deletes: Vec<(String, Key)>,
    txn_id: Option<Ulid>,
) -> Result<(), ReferenceRefreshError> {
    match send(storage, StorageEffect::BatchDelete { deletes, txn_id }).await? {
        StorageEvent::BatchDeleteResult { .. } => Ok(()),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

/// Removes a finished job, or replaces it with `next`, unless a newer request replaced it.
pub(super) async fn settle_job(
    storage: &StorageHandle,
    key: &[u8],
    job: &ReferenceRefreshRecord,
    next: Option<&ReferenceRefreshRecord>,
) -> Result<(), ReferenceRefreshError> {
    let txn_id = match send(storage, StorageEffect::StartTransaction { read: false }).await? {
        StorageEvent::TransactionStarted { txn_id } => txn_id,
        other => {
            return Err(ReferenceRefreshError::UnexpectedEvent(
                Event::Storage(other).label().to_string(),
            ));
        }
    };
    let result = async {
        if !matches!(read_job(storage, key, Some(txn_id)).await?, Some(Ok(stored)) if stored == *job)
        {
            return Ok(());
        }
        let mut deletes = vec![(
            REFRESH_DUE_KEYSPACE.to_string(),
            due_key(job.due_at_ms, key),
        )];
        let Some(next) = next else {
            let key = ByteView::from(key.to_vec());
            deletes.push((REFRESH_JOB_KEYSPACE.to_string(), key));
            return delete_rows(storage, deletes, Some(txn_id)).await;
        };
        delete_rows(storage, deletes, Some(txn_id)).await?;
        let writes = job_entries(next)?;
        match send(storage, StorageEffect::BatchWrite { writes, txn_id: Some(txn_id) }).await? {
            StorageEvent::BatchWriteResult { .. } => Ok(()),
            other => Err(ReferenceRefreshError::UnexpectedEvent(Event::Storage(other).label().to_string())),
        }
    }
    .await;
    let effect = match result {
        Ok(()) => StorageEffect::CommitTransaction { txn_id },
        Err(error) => {
            let _ = send(storage, StorageEffect::AbortTransaction { txn_id }).await;
            return Err(error);
        }
    };
    match send(storage, effect).await? {
        StorageEvent::TransactionCommitted { .. } => Ok(()),
        other => Err(ReferenceRefreshError::UnexpectedEvent(
            Event::Storage(other).label().to_string(),
        )),
    }
}

async fn reschedule_job(
    storage: &StorageHandle,
    key: &[u8],
    job: &ReferenceRefreshRecord,
    error: String,
) -> Result<u64, ReferenceRefreshError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = unix_timestamp_millis().saturating_add(retry_delay_ms(attempts));
    let next_job = ReferenceRefreshRecord {
        refresh: job.refresh.clone(),
        due_at_ms,
        attempts,
        last_error: Some(error),
    };
    settle_job(storage, key, job, Some(&next_job)).await?;
    Ok(due_at_ms)
}
