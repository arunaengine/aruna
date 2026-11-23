//! Prunes due job records and their artifacts, page by page, and restores the prune timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::BlobEvent;
#[cfg(test)]
use aruna_core::events::Event;
use aruna_core::keyspaces::{ARTIFACT_TOMBSTONE_KEYSPACE, SCHEDULE_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::{JobId, PRUNE_INDEX_PREFIX};
use aruna_core::structs::execution::job::input::JobPayload;
use aruna_core::structs::execution::job::local::{
    JobRecord, JobResultPayload, cleanup_job_id, parse_schedule_key,
};
use aruna_core::structs::execution::job::rows::{JobDeletes, prune_delete_entries};
use aruna_core::structs::storage::blob::HiddenBlobKey;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use super::lifecycle::LifecycleError;
use super::lifecycle::held::HeldReservations;
use super::store::mutation_error::JobMutationError;
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
    /// Rows the pass deleted, also those of a job whose record stays for a later pass.
    pub deleted_rows: usize,
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

/// The due jobs of one window, and the index row to continue after when the window was full.
#[derive(Debug, Default, PartialEq)]
pub struct DueJobs {
    pub jobs: Vec<(JobId, JobPayload)>,
    pub after: Option<Key>,
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
    last: Option<Key>,
}

type DueNext = Result<Next<DueJobs>, PruneError>;

impl PruneDueOperation {
    /// Starts after the index row `after`, or at the first row.
    pub fn new(now_ms: u64, page_size: usize, after: Option<Key>) -> Self {
        Self {
            now_ms,
            page_size,
            rows: Vec::new(),
            next_page: None,
            due: Vec::new(),
            reading: None,
            last: after,
        }
    }

    fn done(&mut self, full: bool) -> DueNext {
        let jobs = std::mem::take(&mut self.due);
        let after = if full && !jobs.is_empty() {
            self.last.clone()
        } else {
            None
        };
        Ok(Next::Done(DueJobs { jobs, after }))
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::child(schedule_page(PRUNE_INDEX_PREFIX, after, self.page_size))
    }

    fn next_row(&mut self) -> DueNext {
        while let Some((key, _)) = self.rows.pop() {
            let Ok((expiry_ms, job_id)) = parse_schedule_key(key.as_ref()) else {
                continue;
            };
            if expiry_ms > self.now_ms {
                return self.done(false);
            }
            if self.due.len() >= self.page_size.saturating_mul(4) {
                return self.done(true);
            }
            self.last = Some(key);
            self.reading = Some(job_id);
            return Ok(Next::Call(IoCall::child(read_job_record(job_id))));
        }
        match self.next_page.take() {
            Some(next) => Ok(Next::Call(self.page(Some(next)))),
            None => self.done(false),
        }
    }
}

impl Operation for PruneDueOperation {
    type Output = DueJobs;
    type Error = PruneError;

    fn first(&mut self) -> DueNext {
        Ok(Next::Call(self.page(self.last.clone())))
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
    /// A job whose reservation is not released yet stays, so its terminal settlement can finish.
    Held,
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
    checked: Option<BTreeSet<JobId>>,
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
            checked: None,
            deletes: Vec::new(),
            outcome: JobPruneOutcome::default(),
            processed: 0,
            rows: Vec::new(),
            next_page: None,
            step: PruneStep::Held,
        }
    }

    /// Limits the pass to the jobs a due window checked, read after the index row `after`; the
    /// first other job ends the pass.
    pub fn within(mut self, checked: BTreeSet<JobId>, after: Option<Key>) -> Self {
        self.checked = Some(checked);
        self.next_page = after;
        self
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
        self.outcome.deleted_rows = deletes.len();
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
        if let Some(checked) = &self.checked
            && !checked.contains(&record.job_id)
        {
            return self.more_left();
        }
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
        let delete = Effect::blob(BlobEffect::DeleteHidden { key });
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
        self.call(PruneStep::Held, IoCall::child(HeldReservations::all()))
    }

    fn next(&mut self, result: IoResult) -> PruneNext {
        match std::mem::replace(&mut self.step, PruneStep::Page) {
            PruneStep::Held => {
                let held = result.child::<Vec<JobReservationRecord>, LifecycleError>()?;
                let held = held.map_err(|error| PruneError(error.to_string()))?;
                self.kept
                    .extend(held.iter().map(|reservation| reservation.job_id));
                let after = self.next_page.take();
                self.page(after)
            }
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
            PruneStep::Artifact(record) => match result.blob()? {
                BlobEvent::HiddenDeleted => self.record_gone(record),
                BlobEvent::Error(error) => Err(PruneError(error.to_string())),
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
    use crate::jobs::JOB_PRUNE_PAGE;
    use crate::jobs::JOB_RETENTION_MS;
    use crate::jobs::store::job_calls::ClaimOutcome;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::handle::Handle;
    use aruna_core::id::NodeId;
    use aruna_core::keyspaces::{
        ARTIFACT_TOMBSTONE_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE,
        STAGING_MANIFEST_KEYSPACE, STAGING_QUEUE_KEYSPACE,
    };
    use aruna_core::structs::execution::job::identity::{JobId, JobState};
    use aruna_core::structs::execution::job::input::{
        ComputeResources, ExecutionSpec, ImportMetadataTarget, ImportRoCrateResult,
        ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobPayload, RoCrateLimits,
    };
    use aruna_core::structs::execution::job::local::{JobProgress, JobRecord, JobResultPayload};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::task::TaskEvent;
    use aruna_core::time::unix_timestamp_millis;
    use aruna_core::user::id::UserId;
    use aruna_runner::driver::{DriverContext, drive};
    use aruna_runner::testing::test_context;
    use aruna_storage::scan::iter_prefix_page;
    use aruna_storage::storage::worker::FjallStorage;
    use aruna_test_harness::jobs::insert_job;
    use aruna_test_harness::jobs::read_job_record;
    use byteview::ByteView;
    use tempfile::tempdir;
    use ulid::Ulid;

    /// One prune pass with nothing kept back.
    async fn prune_job_batch(context: &DriverContext) -> Result<JobPruneOutcome, String> {
        let (now_ms, kept) = (unix_timestamp_millis(), BTreeSet::new());
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
        aruna_core::structs::execution::job::local::job_prune_key(expiry_ms, job_id)
    }

    fn is_child<T>(next: &Result<Next<T>, PruneError>) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    // A row still ahead ends the batch and names when to look again.
    #[test]
    fn future_row_ends() {
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::new());
        assert!(is_child(&prune.first()));
        let held = child(Ok::<_, LifecycleError>(Vec::<JobReservationRecord>::new()));
        assert!(is_child(&prune.next(held)));
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

    // A finished job whose reservation is not released yet stays like a kept one.
    #[test]
    fn reserved_job_kept() {
        use aruna_core::structs::execution::job::family::EffectiveResources;
        let job_id = JobId::from_bytes([4u8; 16]);
        let reservation = JobReservationRecord {
            execution_id: Ulid::from_bytes([5u8; 16]),
            job_id,
            logical_job_id: job_id,
            resources: EffectiveResources {
                cpu_cores: 1,
                ram_bytes: 512,
                disk_bytes: 0,
                max_walltime_ms: 1_000,
                preemptible: false,
            },
            created_at_ms: 5,
            subject_generation: 1,
            subject_digest: [0u8; 32],
            config_clock: Default::default(),
            backend: "default".to_string(),
            config_generation: 0,
            config_digest: [0; 32],
        };
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::new());
        prune.first().unwrap();
        let held = child(Ok::<_, LifecycleError>(vec![reservation]));
        assert!(is_child(&prune.next(held)));
        assert!(prune.kept.contains(&job_id));
    }

    // A missing record drops its index row and tombstone, and counts as pruned.
    #[test]
    fn orphan_row_dropped() {
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::new());
        let key = prune_key(5, job_id);
        // A further due row keeps the batch open, so its deletes are still collected here.
        let next = prune_key(6, JobId::from_bytes([6u8; 16]));
        prune.rows.push((next, Value::from(Vec::new())));
        assert!(is_child(&prune.record_read(key, None)));
        assert_eq!(prune.deletes.len(), 2);
        assert_eq!(prune.outcome.pruned, 1);
    }

    // The due list stops at the first row still ahead.
    #[test]
    fn due_list_stops() {
        let (due, later) = (JobId::from_bytes([6u8; 16]), JobId::from_bytes([7u8; 16]));
        let mut list = PruneDueOperation::new(10, 4, None);
        assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
        let page = page_of(vec![prune_key(5, due), prune_key(20, later)]);
        assert!(matches!(list.next(page), Ok(Next::Call(IoCall::Child(_)))));
        let missing = child(Ok::<Option<JobRecord>, JobMutationError>(None));
        let found = list.next(missing);
        assert!(matches!(found, Ok(Next::Done(found)) if found == DueJobs::default()));
    }

    // A full window names the last row it read, so the next window starts after it.
    #[test]
    fn full_window_continues() {
        let mut list = PruneDueOperation::new(10, 1, None);
        list.first().unwrap();
        let keys = (1..=5u8).map(|seed| prune_key(5, JobId::from_bytes([seed; 16])));
        assert!(is_child(&list.next(page_of(keys.collect()))));
        for seed in 1..=4u8 {
            let record = terminal_record(JobId::from_bytes([seed; 16]), 1);
            let read = list.next(child(Ok::<_, JobMutationError>(Some(record))));
            if seed < 4 {
                assert!(is_child(&read));
                continue;
            }
            let Ok(Next::Done(window)) = read else {
                panic!("a full window ends");
            };
            assert_eq!(window.jobs.len(), 4);
            let last = prune_key(5, JobId::from_bytes([4u8; 16]));
            assert_eq!(window.after, Some(last));
        }
    }

    // A due job outside the checked window waits for the next pass and is not deleted.
    #[test]
    fn unchecked_job_waits() {
        let (checked, later) = (JobId::from_bytes([8u8; 16]), JobId::from_bytes([9u8; 16]));
        let only = BTreeSet::from([checked]);
        let mut prune = PruneJobsOperation::new(10, 4, BTreeSet::new()).within(only, None);
        let read = prune.record_read(prune_key(5, later), Some(terminal_record(later, 1)));
        assert!(is_child(&read));
        assert_eq!(prune.step, PruneStep::Deletes);
        assert!(prune.deletes.is_empty());
        assert!(prune.outcome.has_more);
        assert_eq!(prune.outcome.pruned, 0);
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
                backend: None,
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
        let entry = Some((25u64, JobId::from_bytes([1u8; 16])));
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
        let context = aruna_runner::testing::test_context(storage.clone());
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(
            &context,
            &terminal_record(JobId::from_bytes([1u8; 16]), finished),
        )
        .await
        .unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
        let context = aruna_runner::testing::test_context(storage.clone());
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

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
        let context = aruna_runner::testing::test_context(storage.clone());
        insert_job(
            &context,
            &terminal_record(JobId::from_bytes([2u8; 16]), unix_timestamp_millis()),
        )
        .await
        .unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
        let context = aruna_runner::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([8u8; 16]);
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        drive(
            preserve_artifact_tombstone(job_id, owner, unix_timestamp_millis().saturating_sub(1)),
            &context,
        )
        .await
        .unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
        let context = aruna_runner::testing::test_context(storage.clone());
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
        let ClaimOutcome::Claimed(claimed) = aruna_runner::driver::drive(
            crate::jobs::store::job_calls::claim_job(job_id, node_id(7), 2),
            &context,
        )
        .await
        .unwrap() else {
            panic!("job must be claimed")
        };
        let token = claimed.claim.unwrap().claim_token;
        aruna_runner::driver::drive(
            crate::jobs::store::job_calls::put_job_entry(job_id, token, b"entry", &"row"),
            &context,
        )
        .await
        .unwrap();
        aruna_runner::driver::drive(
            crate::jobs::store::job_calls::transition_to_running(job_id, token, 3),
            &context,
        )
        .await
        .unwrap();
        aruna_runner::driver::drive(
            crate::jobs::store::job_calls::complete_job(
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
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(count(&context, JOB_ENTRY_KEYSPACE).await, 1);

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
        let context = aruna_runner::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([4u8; 16]);
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(&context, &execution_record(job_id, finished))
            .await
            .unwrap();
        insert_job(&context, &cleanup_record(job_id)).await.unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
    async fn held_window_moves() {
        // A full window of jobs held by their cleanups does not stop a later due job.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = aruna_runner::testing::test_context(storage.clone());
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 10);
        for seed in 1..=4u8 {
            let job_id = JobId::from_bytes([seed; 16]);
            insert_job(&context, &execution_record(job_id, finished))
                .await
                .unwrap();
            insert_job(&context, &cleanup_record(job_id)).await.unwrap();
        }
        let eligible = JobId::from_bytes([9u8; 16]);
        insert_job(&context, &terminal_record(eligible, finished + 5))
            .await
            .unwrap();

        let prune = crate::plugins::work::PruneKeepingWork::new(1);
        let outcome = drive(prune, &context).await.unwrap();

        assert_eq!(outcome.pruned, 1, "the job after the held window is pruned");
        let gone = read_job_record(&context, eligible, None).await.unwrap();
        assert!(gone.is_none());
        let held = read_job_record(&context, JobId::from_bytes([1u8; 16]), None);
        assert!(held.await.unwrap().is_some(), "held jobs stay");
    }

    #[tokio::test]
    async fn partial_prune_continues() {
        // A job whose staging rows fill the pass keeps the run going, also when held jobs follow.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = aruna_runner::testing::test_context(storage.clone());
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 20);
        let large = JobId::from_bytes([9u8; 16]);
        insert_job(&context, &terminal_record(large, finished))
            .await
            .unwrap();
        let writes = (1..=6u8)
            .map(|seed| {
                let mut row = large.to_bytes().to_vec();
                row.push(seed);
                let row = ByteView::from(row);
                (
                    STAGING_QUEUE_KEYSPACE.to_string(),
                    row,
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
        for seed in 1..=4u8 {
            let job_id = JobId::from_bytes([seed; 16]);
            insert_job(&context, &execution_record(job_id, finished + 10))
                .await
                .unwrap();
            insert_job(&context, &cleanup_record(job_id)).await.unwrap();
        }

        let prune = crate::plugins::work::PruneKeepingWork::new(1);
        let outcome = drive(prune, &context).await.unwrap();

        assert_eq!((outcome.pruned, outcome.deleted_rows), (0, 4));
        assert!(outcome.has_more, "the rest of the large job is next");
        assert_eq!(count(&context, STAGING_QUEUE_KEYSPACE).await, 2);
    }

    #[tokio::test]
    async fn finished_cleanup_prunes() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = aruna_runner::testing::test_context(storage.clone());
        let job_id = JobId::from_bytes([5u8; 16]);
        let finished = unix_timestamp_millis().saturating_sub(JOB_RETENTION_MS + 1);
        insert_job(&context, &execution_record(job_id, finished))
            .await
            .unwrap();
        let mut cleanup = cleanup_record(job_id);
        cleanup.state = JobState::Succeeded;
        cleanup.finished_at_ms = Some(finished);
        insert_job(&context, &cleanup).await.unwrap();

        let outcome = prune_job_batch(&test_context(storage.clone()))
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
        let context = aruna_runner::testing::test_context(storage.clone());
        insert_job(
            &context,
            &terminal_record(JobId::from_bytes([3u8; 16]), unix_timestamp_millis()),
        )
        .await
        .unwrap();
        let task_handle = aruna_scheduler::TaskHandle::new();
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
