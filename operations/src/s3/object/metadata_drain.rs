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

    fn refresh(metadata: Option<SourceMetadata>, seconds: u64) -> ReferenceRefresh {
        ReferenceRefresh {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::from_parts(1, 1),
            metadata,
            refreshed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            auth_context: AuthContext {
                user_id: Default::default(),
                realm_id: RealmId([9; 32]),
                path_restrictions: None,
                session: None,
            },
        }
    }

    fn reference(cached: SourceMetadata, seconds: u64) -> BlobVersion {
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "a".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        BlobVersion::reference(
            source,
            cached,
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
        )
    }

    fn row(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn page(rows: Vec<Key>, next: Option<Key>) -> IoResult {
        IoResult::Page {
            rows: rows
                .into_iter()
                .map(|row| (row, Value::from(Vec::new())))
                .collect(),
            next,
        }
    }

    #[test]
    fn matching_observation_moves() {
        let version = reference(observation(1), 10);
        let mut apply = RefreshApply::new(refresh(Some(observation(1)), 20));
        assert_eq!(apply.transaction(), TransactionMode::Write);
        assert!(matches!(apply.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = apply.next(row(Some(version.to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Write(writes))) = &turn else {
            panic!("expected the version write, got {turn:?}");
        };
        let written = BlobVersion::from_bytes(writes[0].2.as_ref()).unwrap();
        let BlobVersionState::Reference { last_refresh, .. } = written.state else {
            panic!("the version stays a reference");
        };
        assert_eq!(
            last_refresh,
            SystemTime::UNIX_EPOCH + Duration::from_secs(20)
        );
        assert_eq!(apply.next(IoResult::Done), Ok(Next::Done(false)));
    }

    #[test]
    fn stale_observation_skipped() {
        for (metadata, seconds) in [(observation(1), 10), (observation(2), 5)] {
            let mut apply = RefreshApply::new(refresh(Some(metadata), seconds));
            apply.first().unwrap();
            let version = reference(observation(1), 10);
            let turn = apply.next(row(Some(version.to_bytes().unwrap())));
            assert_eq!(turn, Ok(Next::Done(false)));
        }
    }

    #[test]
    fn changed_source_mints() {
        let mut apply = RefreshApply::new(refresh(Some(observation(2)), 20));
        apply.first().unwrap();
        let version = reference(observation(1), 10);
        let turn = apply.next(row(Some(version.to_bytes().unwrap())));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        assert!(matches!(apply.state, ApplyState::Mint(_)));
    }

    #[test]
    fn missing_observation_fails() {
        let mut apply = RefreshApply::new(refresh(None, 20));
        apply.first().unwrap();
        let version = reference(observation(1), 10);
        assert_eq!(
            apply.next(row(Some(version.to_bytes().unwrap()))),
            Err(RefreshRunError::NoObservation)
        );
    }

    #[test]
    fn minted_refresh_schedules() {
        let mut operation = RefreshReferenceOperation::new(refresh(Some(observation(1)), 20));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let applied = IoResult::Child(ChildOutcome::new(Ok::<_, RefreshRunError>(true)));
        let Ok(Next::Then { calls, .. }) = operation.next(applied) else {
            panic!("a minted successor wakes the publisher and the drain");
        };
        assert_eq!(calls.len(), 2);
    }

    #[test]
    fn observes_when_missing() {
        // Without an observation the source is asked first, then the result is applied.
        let mut operation = RefreshReferenceOperation::new(refresh(None, 20));
        operation.first().unwrap();
        assert_eq!(operation.step, RefreshStep::Observe);
        let observed = ChildOutcome::new(Ok::<_, ObserveError>(observation(3)));
        let turn = operation.next(IoResult::Child(observed));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.refresh.metadata, Some(observation(3)));
        assert_eq!(operation.step, RefreshStep::Apply);
    }

    fn job(due_at_ms: u64) -> ReferenceRefreshRecord {
        ReferenceRefreshRecord::new(refresh(Some(observation(1)), 20), due_at_ms)
    }

    #[test]
    fn settle_keeps_newer() {
        // A job a newer request replaced stays queued.
        let mut settle = SettleJob::new(b"key".to_vec(), job(5), None);
        assert_eq!(settle.transaction(), TransactionMode::Write);
        settle.first().unwrap();
        let newer = postcard::to_allocvec(&job(9)).unwrap();
        assert_eq!(settle.next(row(Some(newer))), Ok(Next::Done(())));
    }

    #[test]
    fn settle_replaces_job() {
        let next = job(9);
        let mut settle = SettleJob::new(b"key".to_vec(), job(5), Some(next.clone()));
        settle.first().unwrap();
        let stored = postcard::to_allocvec(&job(5)).unwrap();
        let turn = settle.next(row(Some(stored)));
        let Ok(Next::Call(IoCall::Delete(deletes))) = &turn else {
            panic!("expected the due row delete, got {turn:?}");
        };
        assert_eq!(deletes.len(), 1);
        assert_eq!(
            settle.next(IoResult::Done),
            Ok(Next::Call(IoCall::Write(job_entries(&next).unwrap())))
        );
        assert_eq!(settle.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn drain_drops_broken() {
        // A due row too short to name a job is deleted, and the scan goes on.
        let mut drain = DrainRefreshOperation::new();
        drain.first().unwrap();
        let turn = drain.next(page(vec![Key::from(vec![1])], None));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        let Ok(Next::Done(result)) = drain.next(IoResult::Done) else {
            panic!("an empty batch ends the drain");
        };
        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
    }

    #[test]
    fn drain_stops_future() {
        let mut drain = DrainRefreshOperation::new();
        drain.first().unwrap();
        let later = u64::MAX;
        let Ok(Next::Done(result)) = drain.next(page(vec![due_key(later, b"key")], None)) else {
            panic!("a row due later ends the scan");
        };
        assert_eq!(result.processed, 0);
        assert!(result.next_due_after.is_some());
    }

    #[test]
    fn failed_job_reschedules() {
        let mut drain = DrainRefreshOperation::new();
        drain.first().unwrap();
        let queued = job(0);
        let row_key = due_key(0, b"key");
        let turn = drain.next(page(vec![row_key], None));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        let stored = postcard::to_allocvec(&queued).unwrap();
        let turn = drain.next(row(Some(stored)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let failed = Err::<(), _>(RefreshRunError::NoObservation);
        let turn = drain.next(IoResult::Child(ChildOutcome::new(failed)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let settled = Ok::<(), ReferenceRefreshError>(());
        let Ok(Next::Done(result)) = drain.next(IoResult::Child(ChildOutcome::new(settled))) else {
            panic!("the batch ends after its only job");
        };
        assert_eq!((result.processed, result.failed), (1, 1));
        assert!(result.next_due_after.is_some());
    }

    #[test]
    fn restores_earliest_timer() {
        let mut restore = RestoreRefreshTimer::new();
        restore.first().unwrap();
        let turn = restore.next(page(vec![due_key(0, b"key")], None));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
        let set = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainRefreshQueue,
            after: Duration::ZERO,
        });
        let answer = IoResult::Event(Box::new(set));
        assert_eq!(restore.next(answer), Ok(Next::Done(())));
        let mut empty = RestoreRefreshTimer::new();
        empty.first().unwrap();
        assert_eq!(empty.next(page(Vec::new(), None)), Ok(Next::Done(())));
    }
}
