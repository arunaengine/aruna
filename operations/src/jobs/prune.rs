//! Prunes due job records and their artifacts, page by page, and restores the prune timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{ARTIFACT_TOMBSTONE_KEYSPACE, SCHEDULE_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::execution::job::{
    JobDeletes, JobId, JobPayload, JobRecord, JobResultPayload, PRUNE_INDEX_PREFIX, cleanup_job_id,
    parse_schedule_key, prune_delete_entries,
};
use aruna_core::structs::storage::blob::HiddenBlobKey;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use super::store::JobMutationError;
use super::store::row_calls::{
    self, SchedulePage, artifact_tombstone_key, job_entry_deletes, preserve_artifact_tombstone,
    read_job_record, schedule_page, staging_row_deletes,
};
use super::store::store_call::RowBatch;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct JobPruneOutcome {
    pub pruned: usize,
    pub has_more: bool,
    pub next_due_after: Option<Duration>,
}

/// A prune step failed; the rows stay for the next pass.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct PruneError(pub String);

impl From<IoError> for PruneError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl From<JobMutationError> for PruneError {
    fn from(error: JobMutationError) -> Self {
        Self(error.to_string())
    }
}

/// The due jobs of the next prune pass, for the payload handlers that may keep one for now.
#[derive(Debug, PartialEq)]
pub struct PruneDueOperation {
    now_ms: u64,
    page_size: usize,
    rows: Vec<(Key, Value)>,
    next_page: Option<Key>,
    due: Vec<(JobId, JobPayload)>,
    reading: Option<JobId>,
}

type DueNext = Result<Next<Vec<(JobId, JobPayload)>>, PruneError>;

impl PruneDueOperation {
    pub fn new(now_ms: u64, page_size: usize) -> Self {
        Self {
            now_ms,
            page_size,
            rows: Vec::new(),
            next_page: None,
            due: Vec::new(),
            reading: None,
        }
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::child(schedule_page(PRUNE_INDEX_PREFIX, after, self.page_size))
    }

    fn next_row(&mut self) -> DueNext {
        while let Some((key, _)) = self.rows.pop() {
            let Ok((expiry_ms, job_id)) = parse_schedule_key(key.as_ref()) else {
                continue;
            };
            if expiry_ms > self.now_ms || self.due.len() >= self.page_size.saturating_mul(4) {
                return Ok(Next::Done(std::mem::take(&mut self.due)));
            }
            self.reading = Some(job_id);
            return Ok(Next::Call(IoCall::child(read_job_record(job_id))));
        }
        match self.next_page.take() {
            Some(next) => Ok(Next::Call(self.page(Some(next)))),
            None => Ok(Next::Done(std::mem::take(&mut self.due))),
        }
    }
}

impl Operation for PruneDueOperation {
    type Output = Vec<(JobId, JobPayload)>;
    type Error = PruneError;

    fn first(&mut self) -> DueNext {
        Ok(Next::Call(self.page(None)))
    }

    fn next(&mut self, result: IoResult) -> DueNext {
        if let Some(job_id) = self.reading.take() {
            let record = result.child::<Option<JobRecord>, JobMutationError>()??;
            if let Some(record) = record {
                self.due.push((job_id, record.payload));
            }
            return self.next_row();
        }
        let (mut rows, next) = result.child::<SchedulePage, JobMutationError>()??;
        rows.reverse();
        self.rows = rows;
        self.next_page = next;
        self.next_row()
    }
}

#[derive(Debug, PartialEq)]
enum PruneStep {
    Page,
    Record(Key),
    Cleanup(Box<JobRecord>),
    Entries(Box<JobRecord>),
    Staging(Box<JobRecord>),
    Tombstone(Box<JobRecord>),
    Artifact(Box<JobRecord>),
    Deletes,
}

/// Deletes one bounded batch of expired jobs and their rows. Jobs in `kept` stay for now, as
/// does the fence state a queued cleanup still needs after the retention ended.
#[derive(Debug, PartialEq)]
pub struct PruneJobsOperation {
    now_ms: u64,
    page_size: usize,
    deletion_cap: usize,
    kept: BTreeSet<JobId>,
    deletes: Vec<(KeySpace, Key)>,
    outcome: JobPruneOutcome,
    processed: usize,
    rows: Vec<(Key, Value)>,
    next_page: Option<Key>,
    step: PruneStep,
}

type PruneNext = Result<Next<JobPruneOutcome>, PruneError>;

/// The hidden blob of an export job's artifact, if it has one.
fn artifact_key(record: &JobRecord) -> Result<Option<HiddenBlobKey>, PruneError> {
    let Some(JobResultPayload::ExportRoCrate(result)) = &record.result else {
        return Ok(None);
    };
    let Some(artifact) = &result.artifact else {
        return Ok(None);
    };
    HiddenBlobKey::try_from(&artifact.location)
        .map(Some)
        .map_err(|error| PruneError(error.to_string()))
}

impl PruneJobsOperation {
    pub fn new(now_ms: u64, page_size: usize, kept: BTreeSet<JobId>) -> Self {
        Self {
            now_ms,
            page_size,
            deletion_cap: page_size.saturating_mul(4),
            kept,
            deletes: Vec::new(),
            outcome: JobPruneOutcome::default(),
            processed: 0,
            rows: Vec::new(),
            next_page: None,
            step: PruneStep::Page,
        }
    }

    fn call(&mut self, step: PruneStep, call: IoCall) -> PruneNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn page(&mut self, after: Option<Key>) -> PruneNext {
        let page = schedule_page(PRUNE_INDEX_PREFIX, after, self.page_size);
        self.call(PruneStep::Page, IoCall::child(page))
    }

    fn remaining(&self) -> usize {
        self.deletion_cap.saturating_sub(self.deletes.len()).max(1)
    }

    fn finish(&mut self) -> PruneNext {
        let deletes = std::mem::take(&mut self.deletes);
        self.call(PruneStep::Deletes, IoCall::child(RowBatch::delete(deletes)))
    }

    fn more_left(&mut self) -> PruneNext {
        self.outcome.has_more = true;
        self.finish()
    }

    fn next_row(&mut self) -> PruneNext {
        while let Some((key, _)) = self.rows.pop() {
            if self.deletes.len() >= self.deletion_cap {
                return self.more_left();
            }
            let (expiry_ms, job_id) = match parse_schedule_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(error = %error, "Deleting malformed job prune index row");
                    self.deletes
                        .push((SCHEDULE_INDEX_KEYSPACE.to_string(), key));
                    continue;
                }
            };
            if expiry_ms > self.now_ms {
                let after = expiry_ms.saturating_sub(self.now_ms);
                self.outcome.next_due_after = Some(Duration::from_millis(after));
                return self.finish();
            }
            let read = IoCall::child(read_job_record(job_id));
            return self.call(PruneStep::Record(key), read);
        }
        match self.next_page.take() {
            Some(next) => self.page(Some(next)),
            None => self.finish(),
        }
    }

    fn counted(&mut self) -> PruneNext {
        self.outcome.pruned = self.outcome.pruned.saturating_add(1);
        self.processed = self.processed.saturating_add(1);
        if self.processed >= self.deletion_cap {
            return self.more_left();
        }
        self.next_row()
    }

    fn record_read(&mut self, key: Key, record: Option<JobRecord>) -> PruneNext {
        let Some(record) = record else {
            let job_id = parse_schedule_key(key.as_ref())
                .map(|(_, job_id)| job_id)
                .map_err(|error| PruneError(error.to_string()))?;
            self.deletes
                .push((SCHEDULE_INDEX_KEYSPACE.to_string(), key));
            self.deletes.push((
                ARTIFACT_TOMBSTONE_KEYSPACE.to_string(),
                artifact_tombstone_key(job_id),
            ));
            return self.counted();
        };
        if !matches!(&record.payload, JobPayload::Execution(_)) {
            return self.cleanup_clear(Box::new(record));
        }
        let cleanup = read_job_record(cleanup_job_id(record.job_id));
        self.call(PruneStep::Cleanup(Box::new(record)), IoCall::child(cleanup))
    }

    /// No cleanup holds the job back any more, so only a payload handler still can.
    fn cleanup_clear(&mut self, record: Box<JobRecord>) -> PruneNext {
        if self.kept.contains(&record.job_id) {
            return self.next_row();
        }
        let entries = job_entry_deletes(record.job_id, self.remaining());
        self.call(PruneStep::Entries(record), IoCall::child(entries))
    }

    /// A staging job's rows go before its record, so a cleanup the job could not finish is
    /// still completed here.
    fn entries_gone(&mut self, record: Box<JobRecord>) -> PruneNext {
        let staging = staging_row_deletes(record.job_id, self.remaining());
        self.call(PruneStep::Staging(record), IoCall::child(staging))
    }

    fn staging_gone(&mut self, record: Box<JobRecord>) -> PruneNext {
        if let Some(JobResultPayload::ExportRoCrate(result)) = &record.result
            && let Some(artifact) = &result.artifact
        {
            let expires_at_ms = artifact.expires_at_ms.saturating_add(record.retention_ms);
            let keep = preserve_artifact_tombstone(record.job_id, record.created_by, expires_at_ms);
            return self.call(PruneStep::Tombstone(record), IoCall::child(keep));
        }
        self.delete_artifact(record)
    }

    fn delete_artifact(&mut self, record: Box<JobRecord>) -> PruneNext {
        let Some(key) = artifact_key(&record)? else {
            return self.record_gone(record);
        };
        let delete = Effect::Blob(BlobEffect::DeleteHidden { key });
        self.call(
            PruneStep::Artifact(record),
            IoCall::Effect(Box::new(delete)),
        )
    }

    fn record_gone(&mut self, record: Box<JobRecord>) -> PruneNext {
        self.deletes.extend(prune_delete_entries(&record));
        self.counted()
    }

    fn deletes_read(
        &mut self,
        (deletes, more): (JobDeletes, bool),
        record: Box<JobRecord>,
        then: fn(&mut Self, Box<JobRecord>) -> PruneNext,
    ) -> PruneNext {
        self.deletes.extend(deletes);
        if more {
            return self.more_left();
        }
        then(self, record)
    }
}

impl Operation for PruneJobsOperation {
    type Output = JobPruneOutcome;
    type Error = PruneError;

    fn first(&mut self) -> PruneNext {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> PruneNext {
        match std::mem::replace(&mut self.step, PruneStep::Page) {
            PruneStep::Page => {
                let (mut rows, next) = result.child::<SchedulePage, JobMutationError>()??;
                if rows.is_empty() {
                    return self.finish();
                }
                rows.reverse();
                self.rows = rows;
                self.next_page = next;
                self.next_row()
            }
            PruneStep::Record(key) => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()??;
                self.record_read(key, record)
            }
            PruneStep::Cleanup(record) => {
                let cleanup = result.child::<Option<JobRecord>, JobMutationError>()??;
                if cleanup.is_some_and(|cleanup| !cleanup.state.is_terminal()) {
                    return self.next_row();
                }
                self.cleanup_clear(record)
            }
            PruneStep::Entries(record) => {
                let deletes = result.child::<(JobDeletes, bool), JobMutationError>()??;
                self.deletes_read(deletes, record, Self::entries_gone)
            }
            PruneStep::Staging(record) => {
                let deletes = result.child::<(JobDeletes, bool), JobMutationError>()??;
                self.deletes_read(deletes, record, Self::staging_gone)
            }
            PruneStep::Tombstone(record) => {
                result.child::<(), JobMutationError>()??;
                self.delete_artifact(record)
            }
            PruneStep::Artifact(record) => match event(result)? {
                Event::Blob(BlobEvent::HiddenDeleted) => self.record_gone(record),
                Event::Blob(BlobEvent::Error(error)) => Err(PruneError(error.to_string())),
                other => Err(PruneError(format!(
                    "unexpected hidden artifact delete event: {}",
                    other.label()
                ))),
            },
            PruneStep::Deletes => {
                result.child::<(), JobMutationError>()??;
                Ok(Next::Done(std::mem::take(&mut self.outcome)))
            }
        }
    }
}

/// Moves the prune timer up to the earliest `prune/` entry. An unreadable index leaves the timer
/// as it is, and the timer call itself is best effort.
#[derive(Debug, PartialEq)]
pub struct PruneTimerOperation {
    now_ms: u64,
}

impl PruneTimerOperation {
    pub fn new(now_ms: u64) -> Self {
        Self { now_ms }
    }
}

impl Operation for PruneTimerOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let first = row_calls::first_schedule_entry(PRUNE_INDEX_PREFIX);
        Ok(Next::Call(IoCall::child(first)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let expiry_ms = match result.child::<Option<(u64, JobId)>, JobMutationError>()? {
            Ok(Some((expiry_ms, _))) => expiry_ms,
            Ok(None) => return Ok(Next::Done(())),
            Err(error) => {
                warn!(error = %error, "Failed to scan job prune index");
                return Ok(Next::Done(()));
            }
        };
        let timer = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PruneJobs,
            after: Duration::from_millis(expiry_ms.saturating_sub(self.now_ms)),
        });
        Ok(Next::Then {
            output: (),
            calls: vec![IoCall::Effect(Box::new(timer))],
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::JOB_PRUNE_PAGE;
    use crate::jobs::JOB_RETENTION_MS;
    use crate::jobs::executor::PayloadHandler;
    use crate::jobs::store::test_calls::put_job_entry;
    use crate::jobs::store::test_calls::insert_job;
    use crate::jobs::store::test_calls::read_job_record;
    use crate::jobs::store::state::ClaimOutcome;
    use crate::jobs::store::test_calls::{claim_job, complete_job, transition_to_running};
    use crate::storage_read::iter_prefix_page;
    use crate::testing::test_context;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::handle::Handle;
    use aruna_core::id::NodeId;
    use aruna_core::keyspaces::{
        ARTIFACT_TOMBSTONE_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE,
        STAGING_MANIFEST_KEYSPACE, STAGING_QUEUE_KEYSPACE,
    };
    use aruna_core::structs::execution::job::{
        ComputeResources, ExecutionSpec, ImportMetadataTarget, ImportRoCrateResult,
        ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobId, JobPayload,
        JobProgress, JobRecord, JobResultPayload, JobState, RoCrateLimits,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::task::TaskEvent;
    use aruna_core::time::unix_timestamp_millis;
    use aruna_storage::FjallStorage;
    use byteview::ByteView;
    use std::sync::Arc;
    use tempfile::tempdir;
    use ulid::Ulid;

    /// One prune pass with the keep decisions of `handlers`, as the prune task runs it.
    pub(crate) async fn prune_job_batch(
        context: &DriverContext,
        handlers: &[Arc<dyn PayloadHandler>],
    ) -> Result<JobPruneOutcome, String> {
        let now_ms = unix_timestamp_millis();
        let mut kept = BTreeSet::new();
        if !handlers.is_empty() {
            let due = drive(PruneDueOperation::new(now_ms, JOB_PRUNE_PAGE), context)
                .await
                .map_err(|error| error.0)?;
            for (job_id, payload) in due {
                for handler in handlers {
                    if handler.keeps_job(context, &payload).await? {
                        kept.insert(job_id);
                    }
                }
            }
        }
        let prune = PruneJobsOperation::new(now_ms, JOB_PRUNE_PAGE, kept);
        drive(prune, context).await.map_err(|error| error.0)
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn page_of(keys: Vec<Key>) -> IoResult {
        let rows = keys
            .into_iter()
            .map(|key| (key, Value::from(Vec::new())))
            .collect::<Vec<_>>();
        child(Ok::<SchedulePage, JobMutationError>((rows, None)))
    }

    fn prune_key(expiry_ms: u64, job_id: JobId) -> Key {
        aruna_core::structs::execution::job::job_prune_key(expiry_ms, job_id)
    }

    fn is_child(next: &PruneNext) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    // A row still ahead ends the batch and names when to look again.
    #[test]
    fn future_row_ends() {
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::new());
        assert!(is_child(&prune.first()));
        let page = page_of(vec![prune_key(25, JobId::from_bytes([1u8; 16]))]);
        assert!(is_child(&prune.next(page)));
        assert_eq!(prune.step, PruneStep::Deletes);
        let deleted = child(Ok::<_, JobMutationError>(()));
        let Next::Done(outcome) = prune.next(deleted).unwrap() else {
            panic!("expected the outcome");
        };
        assert_eq!(outcome.next_due_after, Some(Duration::from_millis(15)));
        assert_eq!(outcome.pruned, 0);
    }

    // A job its handler keeps stays, with its rows, for a later pass.
    #[test]
    fn kept_job_skipped() {
        let job_id = JobId::from_bytes([2u8; 16]);
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::from([job_id]));
        let record = Box::new(JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([1u8; 32])),
            iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            1,
            1,
            None,
        ));
        assert!(is_child(&prune.cleanup_clear(record)));
        assert_eq!(prune.step, PruneStep::Deletes);
        assert_eq!(prune.outcome.pruned, 0);
    }

    // A missing record drops its index row and tombstone, and counts as pruned.
    #[test]
    fn orphan_row_dropped() {
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::new());
        let key = prune_key(5, job_id);
        assert!(is_child(&prune.record_read(key, None)));
        assert_eq!(prune.deletes.len(), 2);
        assert_eq!(prune.outcome.pruned, 1);
    }

    // The due list stops at the first row still ahead.
    #[test]
    fn due_list_stops() {
        let (due, later) = (JobId::from_bytes([6u8; 16]), JobId::from_bytes([7u8; 16]));
        let mut list = PruneDueOperation::new(10, 4);
        assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
        let page = page_of(vec![prune_key(5, due), prune_key(20, later)]);
        assert!(matches!(list.next(page), Ok(Next::Call(IoCall::Child(_)))));
        let missing = child(Ok::<Option<JobRecord>, JobMutationError>(None));
        assert!(matches!(list.next(missing), Ok(Next::Done(found)) if found.is_empty()));
    }

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn terminal_record(job_id: JobId, finished_at_ms: u64) -> JobRecord {
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(finished_at_ms);
        record
    }

    fn execution_record(job_id: JobId, finished_at_ms: u64) -> JobRecord {
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Execution(ExecutionSpec {
                group_id: Ulid::from_bytes([3u8; 16]),
                name: None,
                description: None,
                tags: Default::default(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: Vec::new(),
                workdir: None,
                env: Default::default(),
                resources: ComputeResources::default(),
                executor_constraint: None,
                inputs: Vec::new(),
                file_outputs: Vec::new(),
                workspace_outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
            }),
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(finished_at_ms);
        record
    }

    fn cleanup_record(for_job: JobId) -> JobRecord {
        JobRecord::new(
            cleanup_job_id(for_job),
            JobPayload::TerminalCleanup {
                for_job,
                attempt: None,
                access_key: "workspace-access".to_string(),
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        )
    }

    async fn count(context: &DriverContext, key_space: &str) -> usize {
        iter_prefix_page(&context.storage_handle, key_space, None, None, 64, None)
            .await
            .unwrap()
            .0
            .len()
    }

    // The timer moves to the earliest entry, counted from now.
    #[test]
    fn timer_follows_entry() {
        let mut timer = PruneTimerOperation::new(10);
        assert!(matches!(timer.first(), Ok(Next::Call(IoCall::Child(_)))));
        let entry = Some((25, JobId::from_bytes([1u8; 16])));
        let found = IoResult::Child(ChildOutcome::new(Ok::<_, JobMutationError>(entry)));
        let Next::Then { calls, .. } = timer.next(found).unwrap() else {
            panic!("expected the timer call");
        };
        let [IoCall::Effect(effect)] = calls.as_slice() else {
            panic!("expected one effect");
        };
        assert_eq!(
            **effect,
            Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::PruneJobs,
                after: Duration::from_millis(15),
            })
        );
    }

    // Without an entry, or with an unreadable index, the timer stays as it is.
    #[test]
    fn timer_needs_entry() {
        let outcomes = [
            Ok::<Option<(u64, JobId)>, _>(None),
            Err(JobMutationError::Storage("down".to_string())),
        ];
        for outcome in outcomes {
            let found = IoResult::Child(ChildOutcome::new(outcome));
            let next = PruneTimerOperation::new(10).next(found).unwrap();
            assert!(matches!(next, Next::Done(())));
        }
    }

    #[tokio::test]
    async fn expired_job_pruned() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(
            &context,
            &terminal_record(JobId::from_bytes([1u8; 16]), finished),
        )
        .await
        .unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();
        assert_eq!(outcome.pruned, 1);
        assert_eq!(count(&context, JOB_KEYSPACE).await, 0);
        assert_eq!(count(&context, JOB_INDEX_KEYSPACE).await, 0);
        assert_eq!(count(&context, SCHEDULE_INDEX_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn staging_rows_pruned() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([3u8; 16]);
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(&context, &terminal_record(job_id, finished))
            .await
            .unwrap();
        let mut row = job_id.to_bytes().to_vec();
        row.push(1);
        let writes = [STAGING_QUEUE_KEYSPACE, STAGING_MANIFEST_KEYSPACE]
            .into_iter()
            .map(|key_space| {
                (
                    key_space.to_string(),
                    ByteView::from(row.clone()),
                    ByteView::from(Vec::new()),
                )
            })
            .collect();
        storage
            .send_storage_effect(aruna_core::effects::StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();

        assert_eq!(outcome.pruned, 1);
        assert_eq!(count(&context, STAGING_QUEUE_KEYSPACE).await, 0);
        assert_eq!(count(&context, STAGING_MANIFEST_KEYSPACE).await, 0);
        assert_eq!(count(&context, JOB_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn unexpired_job_retained() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        insert_job(
            &context,
            &terminal_record(JobId::from_bytes([2u8; 16]), unix_timestamp_millis()),
        )
        .await
        .unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();
        assert_eq!(outcome.pruned, 0);
        assert!(outcome.next_due_after.is_some());
        assert_eq!(count(&context, JOB_KEYSPACE).await, 1);
    }

    #[tokio::test]
    async fn expired_tombstone_pruned() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([8u8; 16]);
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        drive(
            preserve_artifact_tombstone(job_id, owner, unix_timestamp_millis().saturating_sub(1)),
            &context,
        )
        .await
        .unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();

        assert_eq!(outcome.pruned, 1);
        assert_eq!(count(&context, ARTIFACT_TOMBSTONE_KEYSPACE).await, 0);
        assert_eq!(count(&context, SCHEDULE_INDEX_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn rocrate_retention_prunes() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([6u8; 16]);
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let limits = RoCrateLimits {
            artifact_retention_ms: 1,
            ..RoCrateLimits::default()
        };
        let retention_ms = limits.artifact_retention_ms;
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: AuthContext {
                    user_id: owner,
                    realm_id: RealmId([1u8; 32]),
                    path_restrictions: None,
                    session: None,
                },
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([3u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: "crate".to_string(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([4u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits,
                document_id: Ulid::from_bytes([5u8; 16]),
            }),
            owner,
            node_id(7),
            1,
            1,
            None,
        );
        record.retention_ms = retention_ms;
        insert_job(&context, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(&context, job_id, node_id(7), 2).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let token = claimed.claim.unwrap().claim_token;
        put_job_entry(&context, job_id, token, b"entry", &"row")
            .await
            .unwrap();
        transition_to_running(&context, job_id, token, 3)
            .await
            .unwrap();
        complete_job(
            &context,
            job_id,
            token,
            JobResultPayload::ImportRoCrate(ImportRoCrateResult {
                document_id: None,
                entries_total: 1,
                imported: 1,
                unlisted: 0,
                failed: 0,
                report_digest: [0u8; 32],
            }),
            JobProgress {
                current: 1,
                total: Some(1),
                unit: "entries".to_string(),
                paused_until: None,
            },
            unix_timestamp_millis().saturating_sub(2),
        )
        .await
        .unwrap();
        assert_eq!(count(&context, JOB_ENTRY_KEYSPACE).await, 1);

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();
        assert_eq!(outcome.pruned, 1);
        assert_eq!(count(&context, JOB_KEYSPACE).await, 0);
        assert_eq!(count(&context, JOB_ENTRY_KEYSPACE).await, 0);
    }

    #[tokio::test]
    async fn cleanup_holds_prune() {
        // Cleanup retries are uncapped, so a queued cleanup can outlive retention; the
        // parent carries the fence state `authorize_cleanup` reads, so it is kept.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([4u8; 16]);
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(&context, &execution_record(job_id, finished))
            .await
            .unwrap();
        insert_job(&context, &cleanup_record(job_id)).await.unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();

        assert_eq!(outcome.pruned, 0);
        assert!(!outcome.has_more, "a retained row must not re-arm at zero");
        assert!(
            read_job_record(&context, job_id, None)
                .await
                .unwrap()
                .is_some(),
            "the cleanup still needs its parent"
        );
    }

    #[tokio::test]
    async fn finished_cleanup_prunes() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([5u8; 16]);
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(&context, &execution_record(job_id, finished))
            .await
            .unwrap();
        let mut cleanup = cleanup_record(job_id);
        cleanup.state = JobState::Succeeded;
        cleanup.finished_at_ms = Some(finished);
        insert_job(&context, &cleanup).await.unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()), &[])
            .await
            .unwrap();

        assert_eq!(outcome.pruned, 2, "parent and cleanup both expired");
        assert!(
            read_job_record(&context, job_id, None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn restore_arms_timer() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        insert_job(
            &context,
            &terminal_record(JobId::from_bytes([3u8; 16]), unix_timestamp_millis()),
        )
        .await
        .unwrap();
        let task_handle = aruna_tasks::TaskHandle::new();
        let mut context = context;
        context.task_handle = Some(task_handle.clone());

        drive(PruneTimerOperation::new(unix_timestamp_millis()), &context)
            .await
            .expect("timer scan");

        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::PruneJobs,
                after: Duration::from_secs(u32::MAX as u64),
            }))
            .await
        else {
            panic!("expected an armed prune timer to shorten");
        };
    }
}
