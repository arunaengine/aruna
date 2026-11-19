//! Drains the materialization queue: applies pending events, retries and dead-letters.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::keyspaces::{
    DEAD_LETTER_KEYSPACE, DOCUMENT_JOB_KEYSPACE, EVENT_LOG_KEYSPACE,
    MATERIALIZATION_CAPTURE_KEYSPACE, MATERIALIZATION_JOB_KEYSPACE,
    MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    ApplyRoCrateRequest, DeadLetterRecord, MaterializationState, MaterializationStatusRecord,
    MetadataCrateRequest, MetadataEffect, MetadataError, MetadataEvent, MetadataEventPayload,
    MetadataEventRecord, MetadataGraphPolicy, MetadataMaterializationRecord, MetadataRawRevision,
    MetadataRequestDurability, ProfileValidationStatus, deterministic_materialization_actor,
};
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::rows::{all_values, event, values};
use aruna_core::storage_entries::{
    dead_letter_entry, dead_letter_key, document_job_entry, document_job_key, document_job_prefix,
    event_log_key, materialization_job_entry, materialization_job_key,
    materialization_status_entry, materialization_status_key, profile_validation_entry,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::git::snapshot::capture;
use crate::git::{GitError, GitNode};
use crate::jobs::repository::link_queue::{LinkTimerOperation, QueueRowsOperation};
use crate::jobs::repository::links::LinkError;
use crate::metadata::call_results::page_rows;
use crate::metadata::checkpoint::{CheckpointError, CheckpointOperation};
use crate::metadata::graph_deleted::{GraphDeletedError, GraphDeletedOperation};
use crate::tasks::queue_backoff::{RETRY_BASE_MS, due_after, retry_delay_ms};

use super::data_links::{DataLinkError, LinkRows};
use super::iri_index::{MetadataIriError, ProjectRowsOperation};
use super::profile::validation::{
    MetadataProfilePreview, ProfileAssessOperation, render_verdict, violation_count,
};
use super::raw_events::RawReadError;
use super::raw_revision::{
    PrepareMergedOperation, PrepareRawOperation, RawEventPlan, RawStateCache,
};

const MATERIALIZATION_PAGE_SIZE: usize = 512;
const MATERIALIZATION_BATCH_SIZE: usize = 512;
// Only application failures count here: infrastructure errors retry forever with
// backoff, so an overloaded node never parks work it could still finish.
const MATERIALIZATION_MAX_FAILURES: u32 = 10;
// Parked jobs return to the queue on this backoff, doubling per park up to the
// cap, so a node recovers on its own once the cause clears.
const REQUEUE_BASE_MS: u64 = 60_000;
const REQUEUE_MAX_MS: u64 = 3_600_000;
const REQUEUE_PAGE_SIZE: usize = 256;
// Jobs per finish transaction. Small enough that a failed commit costs little,
// large enough that a full batch commits in a handful of transactions.
const MATERIALIZATION_FINISH_CHUNK: usize = 256;

pub const MATERIALIZATION_POLL_AFTER: Duration = Duration::from_secs(5);
pub const MATERIALIZATION_RETRY_AFTER: Duration = Duration::from_secs(1);
// The bulk storage lane enforces fairness when immediate timer refires bypass this gap.
pub const NEXT_BATCH_AFTER: Duration = Duration::from_millis(25);

#[derive(Debug)]
pub struct MetadataDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, PartialEq)]
struct CompletedMaterializationJob {
    job_key: Vec<u8>,
    document_job_key: Option<Vec<u8>>,
    status: Option<MaterializationStatusRecord>,
    iri_index_writes: Vec<(String, ByteView, ByteView)>,
    /// Older IRI index and data link rows of the document that this materialization replaces.
    link_deletes: Vec<(String, ByteView)>,
    raw_state_write: Option<(String, ByteView, ByteView)>,
    validation_write: Option<(String, ByteView, ByteView)>,
    sync: Option<CompletedMaterializationSync>,
    /// A merge renders the whole merged graph, so its raw state stays valid under a newer status.
    merge: bool,
}

impl CompletedMaterializationJob {
    /// A job that ends without graph rows, with the status it leaves, if any.
    fn without_rows(job_key: Vec<u8>, document_job_key: Vec<u8>, status: Option<Status>) -> Self {
        Self {
            job_key,
            document_job_key: Some(document_job_key),
            status,
            iri_index_writes: Vec::new(),
            link_deletes: Vec::new(),
            raw_state_write: None,
            validation_write: None,
            sync: None,
            merge: false,
        }
    }
}

type Status = MaterializationStatusRecord;

#[derive(Debug, Clone, PartialEq)]
struct CompletedMaterializationSync {
    graph_iri: String,
    peers: Vec<NodeId>,
}

/// The outcome of one job attempt, resolved together in the per-batch finish
/// transaction so a batch of failures costs one transaction, not one each.
#[derive(Debug, PartialEq)]
enum FinishedMaterializationJob {
    Completed(CompletedMaterializationJob),
    Rescheduled {
        job_key: Vec<u8>,
        job: MetadataMaterializationRecord,
        status: MaterializationStatusRecord,
        /// Waits for causal dependencies still in transit, so it retries soon.
        waiting: bool,
        /// A merge that later events never make obsolete.
        merge: bool,
    },
    Parked {
        job_key: Vec<u8>,
        job: MetadataMaterializationRecord,
        status: MaterializationStatusRecord,
    },
}

#[derive(Debug, Default, PartialEq)]
struct MaterializationGroupOutcome {
    finished: Vec<FinishedMaterializationJob>,
    processed: usize,
    craqle_elapsed: Duration,
    error: Option<MetadataMaterializationError>,
}

#[derive(Debug, Default, PartialEq)]
struct MaterializationBatchTimings {
    processed: usize,
    groups: usize,
    craqle_elapsed: Duration,
    finish_elapsed: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationJobObsolescence {
    Live,
    Final,
    RetryAdvanced,
}

#[derive(Debug, Error, PartialEq)]
pub enum MetadataMaterializationError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("metadata create event log record not found for {document_id}/{event_id}")]
    CreateEventMissing { document_id: Ulid, event_id: Ulid },
    #[error("unexpected event while processing metadata materialization queue: {0}")]
    UnexpectedEvent(String),
    #[error("inconsistent metadata raw event log: {0}")]
    InconsistentLog(String),
}

impl From<IoError> for MetadataMaterializationError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            error => Self::UnexpectedEvent(error.to_string()),
        }
    }
}

impl From<RawReadError> for MetadataMaterializationError {
    fn from(error: RawReadError) -> Self {
        match error {
            RawReadError::Storage(error) => Self::Storage(error),
            RawReadError::Conversion(error) => Self::Conversion(error),
            RawReadError::Metadata(error) => Self::Metadata(error),
            RawReadError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
            RawReadError::LimitExceeded(message) => Self::UnexpectedEvent(message),
            RawReadError::InconsistentLog(message) => Self::InconsistentLog(message),
        }
    }
}

impl From<MetadataIriError> for MetadataMaterializationError {
    fn from(error: MetadataIriError) -> Self {
        match error {
            MetadataIriError::Storage(error) => Self::Storage(error),
            MetadataIriError::Conversion(error) => Self::Conversion(error),
            MetadataIriError::DataLink(DataLinkError::Storage(error)) => Self::Storage(error),
            MetadataIriError::DataLink(DataLinkError::Conversion(error)) => Self::Conversion(error),
            MetadataIriError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
            error @ MetadataIriError::DataLink(DataLinkError::UnexpectedEvent(_)) => {
                Self::UnexpectedEvent(error.to_string())
            }
        }
    }
}

type MaterializationTurn<T> = Result<Next<T>, MetadataMaterializationError>;

/// When the materialization drain should run next: now while a job is due, at the earliest
/// due time otherwise, and never with an empty queue.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct NextTimerOperation {
    now_ms: u64,
}

impl NextTimerOperation {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl Operation for NextTimerOperation {
    type Output = Option<Duration>;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<Option<Duration>> {
        Ok(Next::Call(IoCall::child(DueJobsOperation::new(
            self.now_ms,
            1,
        ))))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<Option<Duration>> {
        let (jobs, has_more_due, next_due_ms) =
            result.child::<DueJobs, MetadataMaterializationError>()??;
        if !jobs.is_empty() || has_more_due {
            return Ok(Next::Done(Some(Duration::ZERO)));
        }
        let after = next_due_ms.map(|due_at_ms| due_after(self.now_ms, due_at_ms));
        Ok(Next::Done(after))
    }
}

// Half-core concurrency reserves capacity for foreground create and validation traffic.
pub(crate) fn materialization_group_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|cores| cores.get())
        .unwrap_or(4)
        .div_ceil(2)
        .max(1)
}

#[derive(Debug, PartialEq)]
enum BatchStage {
    Scan,
    Groups,
    Plan,
    Commit,
    Sync(String),
    Timer,
}

/// One drain pass: materializes the due jobs one document group at a time, `concurrency`
/// groups at once, then settles them in finish transactions of a bounded size.
#[derive(Debug, PartialEq)]
pub(crate) struct MaterializationBatchOperation {
    node: GitNode,
    concurrency: usize,
    link_timer: bool,
    now_ms: u64,
    started_ms: u64,
    scan_ms: u64,
    finish_started_ms: u64,
    jobs: usize,
    oldest_lag_ms: u64,
    has_more_due: bool,
    next_due_ms: Option<u64>,
    timings: MaterializationBatchTimings,
    chunks: VecDeque<Vec<FinishedMaterializationJob>>,
    syncs: VecDeque<CompletedMaterializationSync>,
    first_error: Option<MetadataMaterializationError>,
    stage: BatchStage,
}

impl MaterializationBatchOperation {
    /// `link_timer` arms the repository link drain after finished jobs, on a node with tasks.
    pub(crate) fn new(node: GitNode, concurrency: usize, link_timer: bool) -> Self {
        Self {
            node,
            concurrency,
            link_timer,
            now_ms: 0,
            started_ms: 0,
            scan_ms: 0,
            finish_started_ms: 0,
            jobs: 0,
            oldest_lag_ms: 0,
            has_more_due: false,
            next_due_ms: None,
            timings: MaterializationBatchTimings::default(),
            chunks: VecDeque::new(),
            syncs: VecDeque::new(),
            first_error: None,
            stage: BatchStage::Scan,
        }
    }

    fn groups(&mut self, jobs: ScannedJobs) -> MaterializationTurn<MetadataDrainResult> {
        self.jobs = jobs.len();
        self.oldest_lag_ms = (jobs.iter())
            .map(|(_, job)| self.started_ms.saturating_sub(job.due_at_ms))
            .max()
            .unwrap_or(0);
        let mut groups: BTreeMap<Ulid, ScannedJobs> = BTreeMap::new();
        for (job_key, job) in jobs {
            groups
                .entry(job.document_id)
                .or_default()
                .push((job_key, job));
        }
        self.timings.groups = groups.len();
        if groups.is_empty() {
            return self.summary();
        }
        let members = (groups.into_values())
            .map(|mut jobs| {
                jobs.sort_by_key(|(_, job)| job.event_id);
                let group = JobGroupOperation::new(jobs, self.node.clone());
                Effect::SubOperation(boxed_suboperation(group, |result| {
                    Event::Child(ChildOutcome::new(result))
                }))
            })
            .collect();
        self.stage = BatchStage::Groups;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: self.concurrency.max(1),
            form: GroupForm::All { deadline: None },
        })))
    }

    fn collect(&mut self, result: IoResult) -> Vec<FinishedMaterializationJob> {
        let results = match result {
            IoResult::Group(GroupEvent::Finished { results }) => results,
            other => {
                let error = MetadataMaterializationError::UnexpectedEvent(other.label().into());
                self.first_error.get_or_insert(error);
                return Vec::new();
            }
        };
        let mut finished = Vec::new();
        for member in results {
            let outcome = match member {
                GroupResult::Done(event) => match *event {
                    Event::Child(outcome) => {
                        outcome.take::<MaterializationGroupOutcome, MetadataMaterializationError>()
                    }
                    _ => None,
                },
                _ => None,
            };
            match outcome {
                Some(Ok(outcome)) => {
                    self.timings.processed =
                        self.timings.processed.saturating_add(outcome.processed);
                    self.timings.craqle_elapsed =
                        (self.timings.craqle_elapsed).saturating_add(outcome.craqle_elapsed);
                    finished.extend(outcome.finished);
                    if let Some(error) = outcome.error {
                        self.first_error.get_or_insert(error);
                    }
                }
                Some(Err(error)) => {
                    self.first_error.get_or_insert(error);
                }
                None => {
                    let error = MetadataMaterializationError::UnexpectedEvent(
                        "materialization group did not finish".to_string(),
                    );
                    self.first_error.get_or_insert(error);
                }
            }
        }
        finished
    }

    // Chunked so a failure costs one chunk instead of the whole batch, and the craqle work
    // behind the committed chunks survives.
    fn finish(&mut self) -> MaterializationTurn<MetadataDrainResult> {
        let Some(chunk) = self.chunks.pop_front() else {
            self.timings.finish_elapsed = elapsed(self.finish_started_ms, self.now_ms);
            return self.sync();
        };
        self.stage = BatchStage::Plan;
        Ok(Next::Call(IoCall::child(PlanFinishOperation::new(chunk))))
    }

    fn finish_failed(
        &mut self,
        error: MetadataMaterializationError,
    ) -> MaterializationTurn<MetadataDrainResult> {
        self.chunks.clear();
        self.first_error.get_or_insert(error);
        self.finish()
    }

    // Schedule syncs for committed chunks because their deleted job rows cannot retry them.
    fn sync(&mut self) -> MaterializationTurn<MetadataDrainResult> {
        let Some(sync) = self.syncs.pop_front() else {
            return self.timer();
        };
        self.stage = BatchStage::Sync(sync.graph_iri.clone());
        let effect = MetadataEffect::SyncBestEffort {
            graph_iri: sync.graph_iri,
            peers: sync.peers,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            effect,
        )))))
    }

    // Finished jobs may queue link pushes, and the link drain stops once its queue is empty.
    fn timer(&mut self) -> MaterializationTurn<MetadataDrainResult> {
        if let Some(error) = self.first_error.take() {
            return Err(error);
        }
        if self.timings.processed > 0 && self.link_timer {
            self.stage = BatchStage::Timer;
            return Ok(Next::Call(IoCall::child(LinkTimerOperation::new())));
        }
        self.summary()
    }

    fn summary(&mut self) -> MaterializationTurn<MetadataDrainResult> {
        let timings = &self.timings;
        if self.jobs > 0 {
            info!(
                event = "pipeline.materialization.summary",
                jobs = self.jobs,
                processed = timings.processed,
                groups = timings.groups,
                scan_ms = self.scan_ms,
                craqle_apply_ms = duration_ms(timings.craqle_elapsed),
                finish_ms = duration_ms(timings.finish_elapsed),
                total_ms = self.now_ms.saturating_sub(self.started_ms),
                oldest_lag_ms = self.oldest_lag_ms,
                has_more_due = self.has_more_due,
                "Metadata materialization batch summary"
            );
        }
        let next_due_after = match self.has_more_due {
            true => Some(Duration::ZERO),
            false => (self.next_due_ms).map(|due_at_ms| due_after(self.now_ms, due_at_ms)),
        };
        Ok(Next::Done(MetadataDrainResult {
            processed: timings.processed,
            has_more_due: self.has_more_due,
            next_due_after,
        }))
    }
}

impl Operation for MaterializationBatchOperation {
    type Output = MetadataDrainResult;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<MetadataDrainResult> {
        self.started_ms = self.now_ms;
        let scan = DueJobsOperation::new(self.now_ms, MATERIALIZATION_BATCH_SIZE);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<MetadataDrainResult> {
        match std::mem::replace(&mut self.stage, BatchStage::Scan) {
            BatchStage::Scan => {
                let (jobs, has_more_due, next_due_ms) =
                    result.child::<DueJobs, MetadataMaterializationError>()??;
                self.scan_ms = self.now_ms.saturating_sub(self.started_ms);
                self.has_more_due = has_more_due;
                self.next_due_ms = next_due_ms;
                self.groups(jobs)
            }
            BatchStage::Groups => {
                let finished = self.collect(result);
                self.finish_started_ms = self.now_ms;
                self.syncs = dedupe_graph_syncs(&finished).into();
                let mut finished = finished.into_iter().peekable();
                while finished.peek().is_some() {
                    let chunk = finished.by_ref().take(MATERIALIZATION_FINISH_CHUNK);
                    self.chunks.push_back(chunk.collect());
                }
                self.finish()
            }
            BatchStage::Plan => match result.child::<FinishPlan, MetadataMaterializationError>() {
                Ok(Ok(plan)) if plan.writes.is_empty() && plan.deletes.is_empty() => self.finish(),
                Ok(Ok(plan)) => {
                    self.stage = BatchStage::Commit;
                    Ok(Next::Call(IoCall::child(CommitFinishOperation::new(plan))))
                }
                Ok(Err(error)) => self.finish_failed(error),
                Err(error) => self.finish_failed(error.into()),
            },
            BatchStage::Commit => match result.child::<(), MetadataMaterializationError>() {
                Ok(Ok(())) => self.finish(),
                Ok(Err(error)) => self.finish_failed(error),
                Err(error) => self.finish_failed(error.into()),
            },
            BatchStage::Sync(graph_iri) => {
                match event(result) {
                    Ok(Event::Metadata(MetadataEvent::GraphSyncScheduled { .. })) => {}
                    Ok(Event::Metadata(MetadataEvent::Error { error, .. })) => {
                        warn!(error = ?error, graph_iri = %graph_iri, "Failed to schedule metadata graph sync after materialization");
                    }
                    other => {
                        warn!(event = ?other, graph_iri = %graph_iri, "Unexpected metadata graph sync result after materialization");
                    }
                }
                self.sync()
            }
            BatchStage::Timer => {
                match result.child::<(), LinkError>() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => warn!(%error, "Failed to arm the repository link queue"),
                    Err(error) => warn!(%error, "Failed to arm the repository link queue"),
                }
                self.summary()
            }
        }
    }
}

fn elapsed(started_ms: u64, now_ms: u64) -> Duration {
    Duration::from_millis(now_ms.saturating_sub(started_ms))
}

// One SyncGraphBestEffort per graph, keeping the last peers seen, so a batch
// carrying many events for one document schedules a single sync.
fn dedupe_graph_syncs(
    finished: &[FinishedMaterializationJob],
) -> Vec<CompletedMaterializationSync> {
    let mut by_graph: BTreeMap<String, CompletedMaterializationSync> = BTreeMap::new();
    for job in finished {
        if let FinishedMaterializationJob::Completed(job) = job
            && let Some(sync) = &job.sync
        {
            by_graph.insert(sync.graph_iri.clone(), sync.clone());
        }
    }
    by_graph.into_values().collect()
}

/// Row changes for one finish chunk, resolved before the transaction opens.
#[derive(Debug, Default, PartialEq)]
struct FinishPlan {
    writes: Vec<(String, ByteView, ByteView)>,
    deletes: Vec<(String, ByteView)>,
}

#[derive(Debug, PartialEq)]
enum PlanStage {
    Read,
    Queue(FinishPlan),
}

// Guard reads precede the transaction so concurrent changes enter its read set.
#[derive(Debug, PartialEq)]
struct PlanFinishOperation {
    finished: Vec<FinishedMaterializationJob>,
    documents: BTreeSet<Ulid>,
    parked: BTreeSet<(Ulid, Ulid)>,
    now_ms: u64,
    stage: PlanStage,
}

impl PlanFinishOperation {
    fn new(finished: Vec<FinishedMaterializationJob>) -> Self {
        let documents = finished.iter().map(finished_document_id).collect();
        let parked = (finished.iter())
            .filter_map(|finished| match finished {
                FinishedMaterializationJob::Parked { job, .. } => {
                    Some((job.document_id, job.event_id))
                }
                _ => None,
            })
            .collect();
        Self {
            finished,
            documents,
            parked,
            now_ms: 0,
            stage: PlanStage::Read,
        }
    }

    fn plan(&mut self, values: Vec<Option<Value>>) -> MaterializationTurn<FinishPlan> {
        let mut values = values.into_iter();
        let mut snapshot = HashMap::new();
        for (document_id, value) in self.documents.iter().zip(values.by_ref()) {
            if let Some(status) = decode_status(value)? {
                snapshot.insert(*document_id, status);
            }
        }
        let mut parked = HashMap::new();
        for (target, value) in self.parked.iter().zip(values) {
            if let Some(record) = value.and_then(|value| postcard::from_bytes(&value).ok()) {
                parked.insert(*target, record);
            }
        }
        let finished = std::mem::take(&mut self.finished);
        let (plan, superseding) = plan_rows(finished, &snapshot, &parked, self.now_ms)?;
        self.stage = PlanStage::Queue(plan);
        // Linked repositories learn of the change in the same commit as the new status.
        let queue_rows = QueueRowsOperation::new(superseding);
        Ok(Next::Call(IoCall::child(queue_rows)))
    }
}

impl Operation for PlanFinishOperation {
    type Output = FinishPlan;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<FinishPlan> {
        let statuses = self
            .documents
            .iter()
            .map(|document_id| status_read(*document_id));
        let dead_letters = (self.parked.iter()).map(|(document_id, event_id)| {
            let key = dead_letter_key(*document_id, *event_id);
            (DEAD_LETTER_KEYSPACE.to_string(), key)
        });
        let reads: Vec<_> = statuses.chain(dead_letters).collect();
        if reads.is_empty() {
            return self.plan(Vec::new());
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<FinishPlan> {
        match std::mem::replace(&mut self.stage, PlanStage::Read) {
            PlanStage::Read => self.plan(all_values(result)?),
            PlanStage::Queue(mut plan) => {
                match result.child::<Vec<(String, Key, Value)>, LinkError>() {
                    Ok(Ok(rows)) => plan.writes.extend(rows),
                    Ok(Err(error)) => {
                        warn!(%error, "Failed to queue repository link pushes after materialization")
                    }
                    Err(error) => {
                        warn!(%error, "Failed to queue repository link pushes after materialization")
                    }
                }
                Ok(Next::Done(plan))
            }
        }
    }
}

/// The rows a finish chunk writes and deletes, judged against the statuses read before it,
/// and the documents whose new final status supersedes the stored one.
fn plan_rows(
    finished: Vec<FinishedMaterializationJob>,
    snapshot: &HashMap<Ulid, Status>,
    parked: &HashMap<(Ulid, Ulid), DeadLetterRecord>,
    now_ms: u64,
) -> Result<(FinishPlan, Vec<Ulid>), MetadataMaterializationError> {
    let mut plan = FinishPlan {
        deletes: Vec::with_capacity(finished.len().saturating_mul(2)),
        ..FinishPlan::default()
    };
    let mut planned: HashMap<Ulid, MaterializationStatusRecord> = HashMap::new();
    let mut superseding: HashMap<Ulid, Ulid> = HashMap::new();
    for finished in finished {
        match finished {
            FinishedMaterializationJob::Completed(job) => {
                if let Some(status) = job.status {
                    let current = guard_status(snapshot, &planned, status.document_id);
                    if should_write_final(current, &status) {
                        superseding.insert(status.document_id, status.event_id);
                        plan.writes.extend(job.iri_index_writes);
                        plan.deletes.extend(job.link_deletes);
                        if let Some(raw_state_write) = job.raw_state_write {
                            plan.writes.push(raw_state_write);
                        }
                        if let Some(validation_write) = job.validation_write {
                            plan.writes.push(validation_write);
                        }
                        plan.writes.push(materialization_status_entry(&status)?);
                        planned.insert(status.document_id, status);
                    } else if job.merge
                        && current.is_some_and(|current| current.event_id > status.event_id)
                        && let Some(raw_state_write) = job.raw_state_write
                    {
                        // A late merge keeps the newer status but stores the graph it published.
                        plan.writes.push(raw_state_write);
                    }
                }
                plan.deletes.push((
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(job.job_key),
                ));
                if let Some(document_job_key) = job.document_job_key {
                    plan.deletes.push((
                        MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
                        ByteView::from(document_job_key.clone()),
                    ));
                    plan.deletes.push((
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        ByteView::from(document_job_key),
                    ));
                }
            }
            FinishedMaterializationJob::Rescheduled {
                job_key,
                job,
                status,
                waiting,
                merge,
            } => {
                let old_index_delete = (
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(job_key),
                );
                let current = guard_status(snapshot, &planned, job.document_id);
                if current.is_some_and(|current| job_superseded(current, &job, merge)) {
                    plan.deletes.push(old_index_delete);
                    plan.deletes.push((
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        document_job_key(job.document_id, job.event_id),
                    ));
                    continue;
                }
                let attempts = job.attempts.saturating_add(1);
                // A dependency that stays missing is checked rarely, not every two seconds.
                let delay = if waiting {
                    waiting_delay_ms(attempts)
                } else {
                    retry_delay_ms(attempts)
                };
                let next_job = MetadataMaterializationRecord {
                    document_id: job.document_id,
                    event_id: job.event_id,
                    due_at_ms: now_ms.saturating_add(delay),
                    attempts,
                    failures: status.failures,
                    parks: job.parks,
                };
                if should_write_retry(current, &status) {
                    plan.writes.push(materialization_status_entry(&status)?);
                    planned.insert(status.document_id, status);
                }
                plan.writes.push(materialization_job_entry(&next_job)?);
                plan.writes.push(document_job_entry(&next_job)?);
                plan.deletes.push(old_index_delete);
            }
            FinishedMaterializationJob::Parked {
                job_key,
                job,
                status,
            } => {
                let current = guard_status(snapshot, &planned, job.document_id);
                let job_deletes = [
                    (
                        MATERIALIZATION_JOB_KEYSPACE.to_string(),
                        ByteView::from(job_key),
                    ),
                    (
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        document_job_key(job.document_id, job.event_id),
                    ),
                    // A parked job no longer holds refreshes to its graph.
                    (
                        MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
                        document_job_key(job.document_id, job.event_id),
                    ),
                ];
                // An already-superseded job must not leave a dead letter behind:
                // requeueing it later would resurrect an obsolete event.
                if current.is_some_and(|current| retry_already_advanced(current, &job)) {
                    plan.deletes.extend(job_deletes);
                    continue;
                }
                if should_write_final(current, &status) {
                    plan.writes.push(materialization_status_entry(&status)?);
                }
                let previous = parked.get(&(job.document_id, job.event_id));
                let dead_letter = parked_dead_letter(&job, &status, previous, now_ms);
                plan.writes.push(dead_letter_entry(&dead_letter)?);
                plan.deletes.extend(job_deletes);
                warn!(
                    event = "materialization.job.parked",
                    document_id = %job.document_id,
                    event_id = %job.event_id,
                    attempts = status.attempts,
                    failures = status.failures,
                    requeue_at_ms = dead_letter.requeue_at_ms,
                    error = status.last_error.as_deref().unwrap_or_default(),
                    "Parked metadata materialization job as dead letter"
                );
                planned.insert(status.document_id, status);
            }
        }
    }
    Ok((plan, superseding.keys().copied().collect()))
}

#[derive(Debug, PartialEq)]
enum CommitStage {
    Written,
    Deleted,
}

/// Writes and deletes one finish chunk's rows in one transaction.
#[derive(Debug, PartialEq)]
struct CommitFinishOperation {
    plan: FinishPlan,
    stage: CommitStage,
}

impl CommitFinishOperation {
    fn new(plan: FinishPlan) -> Self {
        Self {
            plan,
            stage: CommitStage::Written,
        }
    }

    fn delete(&mut self) -> MaterializationTurn<()> {
        let deletes = std::mem::take(&mut self.plan.deletes);
        if deletes.is_empty() {
            return Ok(Next::Done(()));
        }
        self.stage = CommitStage::Deleted;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for CommitFinishOperation {
    type Output = ();
    type Error = MetadataMaterializationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> MaterializationTurn<()> {
        let writes = std::mem::take(&mut self.plan.writes);
        if writes.is_empty() {
            return self.delete();
        }
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<()> {
        result.done()?;
        match self.stage {
            CommitStage::Written => self.delete(),
            CommitStage::Deleted => Ok(Next::Done(())),
        }
    }
}

// A status planned earlier in this chunk is newer than the snapshot, so it is
// what later jobs of the same document must be judged against.
fn guard_status<'a>(
    snapshot: &'a HashMap<Ulid, MaterializationStatusRecord>,
    planned: &'a HashMap<Ulid, MaterializationStatusRecord>,
    document_id: Ulid,
) -> Option<&'a MaterializationStatusRecord> {
    planned
        .get(&document_id)
        .or_else(|| snapshot.get(&document_id))
}

fn finished_document_id(finished: &FinishedMaterializationJob) -> Ulid {
    match finished {
        FinishedMaterializationJob::Completed(job) => job
            .status
            .as_ref()
            .map(|status| status.document_id)
            .unwrap_or_else(|| {
                job_key_target(&job.job_key)
                    .map(|(document_id, _)| document_id)
                    .unwrap_or_else(Ulid::nil)
            }),
        FinishedMaterializationJob::Rescheduled { job, .. }
        | FinishedMaterializationJob::Parked { job, .. } => job.document_id,
    }
}

// A re-parked job keeps its park count so its requeue backoff keeps growing instead of
// restarting at the base delay.
fn parked_dead_letter(
    job: &MetadataMaterializationRecord,
    status: &MaterializationStatusRecord,
    previous: Option<&DeadLetterRecord>,
    now_ms: u64,
) -> DeadLetterRecord {
    let parks = previous
        .map_or(job.parks, |previous| previous.parks.max(job.parks))
        .saturating_add(1);
    DeadLetterRecord {
        job: job.clone(),
        last_error: status.last_error.clone().unwrap_or_default(),
        parked_at_ms: now_ms,
        parks,
        requeue_at_ms: now_ms.saturating_add(requeue_after_ms(parks)),
    }
}

/// Delay before a job that waits on a missing dependency is checked again; after 30 attempts the
/// wait grows to five minutes.
fn waiting_delay_ms(attempts: u32) -> u64 {
    let cap = if attempts > 30 { 300_000 } else { 2_000 };
    let base = Duration::from_millis(RETRY_BASE_MS);
    let delay = capped_backoff(base, Duration::from_millis(cap), attempts, 0);
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

fn requeue_after_ms(parks: u32) -> u64 {
    let base = Duration::from_millis(REQUEUE_BASE_MS);
    let cap = Duration::from_millis(REQUEUE_MAX_MS);
    let delay = capped_backoff(base, cap, parks.saturating_sub(1), 0);
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Debug, PartialEq)]
enum RequeueStage {
    Page,
    Malformed,
    Requeue(Ulid, Ulid),
}

/// Returns due dead letters to the queue and clears their terminal status, so a
/// node converges once the cause clears. Dead letters the document has moved
/// past are dropped instead. Answers the number of jobs requeued.
#[derive(Debug, PartialEq)]
pub(crate) struct RequeueDeadLettersOperation {
    now_ms: u64,
    due_ms: u64,
    rows: VecDeque<(Key, Value)>,
    next: Option<Key>,
    requeued: usize,
    stage: RequeueStage,
}

impl RequeueDeadLettersOperation {
    pub(crate) fn new() -> Self {
        Self {
            now_ms: 0,
            due_ms: 0,
            rows: VecDeque::new(),
            next: None,
            requeued: 0,
            stage: RequeueStage::Page,
        }
    }

    fn page(&mut self, after: Option<Key>) -> MaterializationTurn<usize> {
        self.stage = RequeueStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: DEAD_LETTER_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: REQUEUE_PAGE_SIZE,
        }))
    }

    fn sweep(&mut self) -> MaterializationTurn<usize> {
        while let Some((key, value)) = self.rows.pop_front() {
            let Ok(dead_letter) = postcard::from_bytes::<DeadLetterRecord>(&value) else {
                warn!(key = ?key.to_vec(), "Deleting malformed metadata materialization dead letter");
                self.stage = RequeueStage::Malformed;
                let delete = (DEAD_LETTER_KEYSPACE.to_string(), key);
                return Ok(Next::Call(IoCall::Delete(vec![delete])));
            };
            if dead_letter.requeue_at_ms > self.due_ms {
                continue;
            }
            let job = &dead_letter.job;
            self.stage = RequeueStage::Requeue(job.document_id, job.event_id);
            let requeue = RequeueDeadLetterOperation::new(dead_letter);
            return Ok(Next::Call(IoCall::child(requeue)));
        }
        if let Some(after) = self.next.take() {
            return self.page(Some(after));
        }
        if self.requeued > 0 {
            info!(
                event = "materialization.deadletter.requeued",
                jobs = self.requeued,
                "Requeued parked metadata materialization jobs"
            );
        }
        Ok(Next::Done(self.requeued))
    }
}

impl Operation for RequeueDeadLettersOperation {
    type Output = usize;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<usize> {
        self.due_ms = self.now_ms;
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<usize> {
        match std::mem::replace(&mut self.stage, RequeueStage::Page) {
            RequeueStage::Page => {
                let (rows, next) = page_rows(result)?;
                self.rows = rows.into();
                self.next = next;
            }
            RequeueStage::Malformed => result.done()?,
            RequeueStage::Requeue(document_id, event_id) => {
                match result.child::<bool, MetadataMaterializationError>()? {
                    Ok(true) => self.requeued = self.requeued.saturating_add(1),
                    Ok(false) => {}
                    // A racing finish aborts this requeue but must not stop the remaining sweep.
                    Err(MetadataMaterializationError::Storage(
                        StorageError::TransactionConflict,
                    )) => {
                        debug!(
                            event = "materialization.deadletter.contended",
                            document_id = %document_id,
                            event_id = %event_id,
                            "Deferring contended metadata materialization dead letter"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        self.sweep()
    }
}

// The parked job's own status is Failed at its own event, so only a final status
// beyond that means the document moved on and requeueing would regress it.
fn dead_letter_superseded(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status_obsoletes_job(status, job)
        && (status.event_id > job.event_id || status.state == MaterializationState::Materialized)
}

#[derive(Debug, PartialEq)]
enum ParkedStage {
    Read,
    Dropped,
    Written,
    Requeued,
}

/// The parked status is terminal for this event, so it must be cleared with the job rows or
/// the requeued job is pruned as obsolete on the next scan. The transactional status guard
/// conflicts with a newer event instead of restoring stale work.
#[derive(Debug, PartialEq)]
struct RequeueDeadLetterOperation {
    dead_letter: DeadLetterRecord,
    now_ms: u64,
    stage: ParkedStage,
}

impl RequeueDeadLetterOperation {
    fn new(dead_letter: DeadLetterRecord) -> Self {
        Self {
            dead_letter,
            now_ms: 0,
            stage: ParkedStage::Read,
        }
    }

    fn job(&self) -> MetadataMaterializationRecord {
        MetadataMaterializationRecord {
            document_id: self.dead_letter.job.document_id,
            event_id: self.dead_letter.job.event_id,
            due_at_ms: self.now_ms,
            attempts: 0,
            failures: MATERIALIZATION_MAX_FAILURES.saturating_sub(1),
            parks: self.dead_letter.parks,
        }
    }

    fn drop_letter(&mut self, job: &MetadataMaterializationRecord) -> MaterializationTurn<bool> {
        self.stage = ParkedStage::Dropped;
        let key = dead_letter_key(job.document_id, job.event_id);
        Ok(Next::Call(IoCall::Delete(vec![(
            DEAD_LETTER_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn requeue(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        let job = self.job();
        let [event, status] = values::<2>(result)?;
        let event = match logged_event(job.document_id, job.event_id, event) {
            Ok(event) => event,
            Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
                return self.drop_letter(&job);
            }
            Err(error) => return Err(error),
        };
        if let Some(current) = decode_status(status)?
            && dead_letter_superseded(&current, &job)
        {
            info!(
                event = "materialization.deadletter.superseded",
                document_id = %job.document_id,
                event_id = %job.event_id,
                status_event_id = %current.event_id,
                "Dropping superseded metadata materialization dead letter"
            );
            return self.drop_letter(&job);
        }
        let status = MaterializationStatusRecord {
            failures: job.failures,
            ..MaterializationStatusRecord::pending(&event, self.now_ms)
        };
        self.stage = ParkedStage::Written;
        Ok(Next::Call(IoCall::Write(vec![
            materialization_status_entry(&status)?,
            materialization_job_entry(&job)?,
            document_job_entry(&job)?,
        ])))
    }
}

impl Operation for RequeueDeadLetterOperation {
    type Output = bool;
    type Error = MetadataMaterializationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<bool> {
        let job = &self.dead_letter.job;
        Ok(Next::Call(IoCall::Read(vec![
            event_read(job.document_id, job.event_id),
            status_read(job.document_id),
        ])))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        match std::mem::replace(&mut self.stage, ParkedStage::Requeued) {
            ParkedStage::Read => self.requeue(result),
            ParkedStage::Dropped => {
                result.done()?;
                Ok(Next::Done(false))
            }
            ParkedStage::Written => {
                result.done()?;
                let job = self.job();
                self.stage = ParkedStage::Requeued;
                let key = dead_letter_key(job.document_id, job.event_id);
                Ok(Next::Call(IoCall::Delete(vec![(
                    DEAD_LETTER_KEYSPACE.to_string(),
                    key,
                )])))
            }
            ParkedStage::Requeued => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}

/// Due jobs, whether more are due beyond the limit, and when the next one falls due.
type DueJobs = (ScannedJobs, bool, Option<u64>);

#[derive(Debug, PartialEq)]
enum ScanStage {
    Page,
    Sidecars(Vec<DueCandidate>),
    Live(ScannedJobs),
    Prune(bool),
}

// The due index is due-ordered and a row is valid only when the sidecar row matches its due
// time.
#[derive(Debug, PartialEq)]
struct DueJobsOperation {
    now_ms: u64,
    limit: usize,
    jobs: ScannedJobs,
    candidates: VecDeque<DueCandidate>,
    stale: Vec<StaleJob>,
    next_due_ms: Option<u64>,
    next: Option<Key>,
    stage: ScanStage,
}

impl DueJobsOperation {
    fn new(now_ms: u64, limit: usize) -> Self {
        Self {
            now_ms,
            limit,
            jobs: Vec::new(),
            candidates: VecDeque::new(),
            stale: Vec::new(),
            next_due_ms: None,
            next: None,
            stage: ScanStage::Page,
        }
    }

    fn page(&mut self, after: Option<Key>) -> MaterializationTurn<DueJobs> {
        self.stage = ScanStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: MATERIALIZATION_PAGE_SIZE,
        }))
    }

    fn scanned(&mut self, rows: Vec<(Key, Value)>) -> MaterializationTurn<DueJobs> {
        for (key, _value) in rows {
            let key = key.to_vec();
            let Some((due_at_ms, document_id, event_id)) = job_key_parts(&key) else {
                warn!(key = ?key, "Deleting malformed metadata materialization index row");
                self.stale.push(StaleJob {
                    index_key: key,
                    seen: None,
                    drop_sidecar: false,
                });
                continue;
            };
            if due_at_ms > self.now_ms {
                self.next_due_ms = Some(due_at_ms);
                break;
            }
            (self.candidates).push_back((key, due_at_ms, document_id, event_id));
        }
        self.resolve()
    }

    // Resolved in slices of the outstanding limit so a probe for a single
    // due job does not read a whole page of sidecars, events and statuses.
    fn resolve(&mut self) -> MaterializationTurn<DueJobs> {
        if self.candidates.is_empty() {
            return self.prune(false);
        }
        let remaining = self.limit.saturating_sub(self.jobs.len()).max(1);
        let end = self.candidates.len().min(remaining);
        let slice: Vec<DueCandidate> = self.candidates.drain(..end).collect();
        let reads = (slice.iter())
            .map(|(_, _, document_id, event_id)| sidecar_read(*document_id, *event_id))
            .collect();
        self.stage = ScanStage::Sidecars(slice);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn sidecars(
        &mut self,
        slice: Vec<DueCandidate>,
        sidecars: Vec<Option<Value>>,
    ) -> MaterializationTurn<DueJobs> {
        let mut due = Vec::with_capacity(slice.len());
        for ((key, due_at_ms, _, _), sidecar) in slice.into_iter().zip(sidecars) {
            let sidecar = sidecar.and_then(|value| {
                postcard::from_bytes::<MetadataMaterializationRecord>(&value).ok()
            });
            match sidecar {
                Some(job) if job.due_at_ms == due_at_ms => due.push((key, job)),
                // The sidecar is authoritative: an index row it does not match is
                // stale. A missing or malformed sidecar is dropped with it.
                sidecar => self.stale.push(StaleJob {
                    index_key: key,
                    drop_sidecar: sidecar.is_none(),
                    seen: sidecar,
                }),
            }
        }
        if due.is_empty() {
            return self.resolve();
        }
        let documents: BTreeSet<Ulid> = due.iter().map(|(_, job)| job.document_id).collect();
        let statuses = documents
            .iter()
            .map(|document_id| status_read(*document_id));
        let events = (due.iter()).map(|(_, job)| event_read(job.document_id, job.event_id));
        let reads = statuses.chain(events).collect();
        self.stage = ScanStage::Live(due);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    // Live means the create event still exists and no status has advanced past the
    // job. Dead jobs are pruned with both of their rows.
    fn live(
        &mut self,
        due: ScannedJobs,
        values: Vec<Option<Value>>,
    ) -> MaterializationTurn<DueJobs> {
        let documents: BTreeSet<Ulid> = due.iter().map(|(_, job)| job.document_id).collect();
        let mut values = values.into_iter();
        let mut statuses = HashMap::new();
        for (document_id, value) in documents.into_iter().zip(values.by_ref()) {
            if let Some(status) = decode_status(value)? {
                statuses.insert(document_id, status);
            }
        }
        let mut live = Vec::with_capacity(due.len());
        for ((key, job), event) in due.into_iter().zip(values) {
            let merge = (event.as_ref())
                .and_then(|event| postcard::from_bytes::<MetadataEventRecord>(event).ok())
                .is_some_and(|event| merges(&event));
            let advanced = (statuses.get(&job.document_id))
                .is_some_and(|status| job_superseded(status, &job, merge));
            if event.is_none() || advanced {
                self.stale.push(StaleJob {
                    index_key: key,
                    seen: Some(job),
                    drop_sidecar: true,
                });
            } else {
                live.push(job);
            }
        }
        for job in live {
            self.jobs
                .push((materialization_job_key(&job).to_vec(), job));
            if self.jobs.len() >= self.limit {
                return self.prune(true);
            }
        }
        self.resolve()
    }

    fn prune(&mut self, full: bool) -> MaterializationTurn<DueJobs> {
        let stale = std::mem::take(&mut self.stale);
        if stale.is_empty() {
            return self.after_prune(full);
        }
        self.stage = ScanStage::Prune(full);
        Ok(Next::Call(IoCall::child(PruneJobsOperation::new(stale))))
    }

    fn after_prune(&mut self, full: bool) -> MaterializationTurn<DueJobs> {
        if full {
            return Ok(Next::Done((std::mem::take(&mut self.jobs), true, None)));
        }
        if let Some(due_at_ms) = self.next_due_ms {
            let jobs = std::mem::take(&mut self.jobs);
            return Ok(Next::Done((jobs, false, Some(due_at_ms))));
        }
        match self.next.take() {
            Some(after) => self.page(Some(after)),
            None => Ok(Next::Done((std::mem::take(&mut self.jobs), false, None))),
        }
    }
}

impl Operation for DueJobsOperation {
    type Output = DueJobs;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<DueJobs> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<DueJobs> {
        match std::mem::replace(&mut self.stage, ScanStage::Page) {
            ScanStage::Page => {
                let (rows, next) = page_rows(result)?;
                self.next = next;
                self.scanned(rows)
            }
            ScanStage::Sidecars(slice) => self.sidecars(slice, all_values(result)?),
            ScanStage::Live(due) => self.live(due, all_values(result)?),
            ScanStage::Prune(full) => {
                pruned(result)?;
                self.after_prune(full)
            }
        }
    }
}

/// One index row seen by the scan: its key, the due time encoded in it and the
/// sidecar it points at.
type DueCandidate = (Vec<u8>, u64, Ulid, Ulid);

type ScannedJobs = Vec<(Vec<u8>, MetadataMaterializationRecord)>;

#[derive(Debug, Default, PartialEq)]
struct ProcessedMaterializationJob {
    finished: Option<FinishedMaterializationJob>,
    craqle_elapsed: Duration,
    attempted: bool,
    stop_group: bool,
    /// The document's raw state cache, advanced by a materialized event.
    cache: RawStateCache,
}

impl ProcessedMaterializationJob {
    fn completed(job: CompletedMaterializationJob, craqle_elapsed: Duration) -> Self {
        Self {
            finished: Some(FinishedMaterializationJob::Completed(job)),
            craqle_elapsed,
            attempted: true,
            ..Self::default()
        }
    }

    // Rescheduled and parked jobs both stop the group so later events for the
    // same document do not apply out of order this batch.
    fn deferred(finished: FinishedMaterializationJob, craqle_elapsed: Duration) -> Self {
        Self {
            finished: Some(finished),
            craqle_elapsed,
            attempted: true,
            stop_group: true,
            ..Self::default()
        }
    }
}

// Jobs that exhaust the failure budget park as a dead letter; others reschedule with backoff.
// The resulting row writes and deletes are folded into the per-batch finish txn.
fn defer_materialization_job(
    job_key: &[u8],
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    error: &MetadataMaterializationError,
    now_ms: u64,
) -> FinishedMaterializationJob {
    let application_failure = matches!(
        materialization_failure_kind(error),
        MaterializationFailureKind::Application
    );
    let failures = job.failures.saturating_add(u32::from(application_failure));
    let message = error.to_string();
    if failures >= MATERIALIZATION_MAX_FAILURES {
        FinishedMaterializationJob::Parked {
            job_key: job_key.to_vec(),
            job: job.clone(),
            status: materialization_failure_status(job, event, (message, failures), true, now_ms),
        }
    } else {
        FinishedMaterializationJob::Rescheduled {
            job_key: job_key.to_vec(),
            job: job.clone(),
            status: materialization_failure_status(job, event, (message, failures), false, now_ms),
            waiting: matches!(
                error,
                MetadataMaterializationError::Metadata(MetadataError::MissingDependencies(_))
            ),
            merge: merges(event),
        }
    }
}

#[derive(Debug, PartialEq)]
enum GroupStage {
    Load,
    Older(Vec<u8>, MetadataMaterializationRecord),
    Job(Ulid),
}

/// Materializes one document's due jobs in event order. It stops at the first job that must
/// wait, and keeps a failure as the group's error beside the jobs it already finished.
#[derive(Debug, PartialEq)]
struct JobGroupOperation {
    jobs: VecDeque<(Vec<u8>, MetadataMaterializationRecord)>,
    node: GitNode,
    group: GroupJobs,
    advanced: BTreeSet<Ulid>,
    cache: RawStateCache,
    outcome: MaterializationGroupOutcome,
    stage: GroupStage,
}

impl JobGroupOperation {
    fn new(jobs: ScannedJobs, node: GitNode) -> Self {
        Self {
            jobs: jobs.into(),
            node,
            group: GroupJobs::default(),
            advanced: BTreeSet::new(),
            cache: RawStateCache::default(),
            outcome: MaterializationGroupOutcome::default(),
            stage: GroupStage::Load,
        }
    }

    fn done(&mut self) -> MaterializationTurn<MaterializationGroupOutcome> {
        Ok(Next::Done(std::mem::take(&mut self.outcome)))
    }

    fn job(&mut self) -> MaterializationTurn<MaterializationGroupOutcome> {
        let Some((job_key, job)) = self.jobs.pop_front() else {
            return self.done();
        };
        let pending = older_candidates(&self.group, &job, &self.advanced);
        if pending.is_empty() {
            return self.run(job_key, job);
        }
        self.stage = GroupStage::Older(job_key, job);
        Ok(Next::Call(IoCall::child(OlderJobOperation::new(pending))))
    }

    fn run(
        &mut self,
        job_key: Vec<u8>,
        job: MetadataMaterializationRecord,
    ) -> MaterializationTurn<MaterializationGroupOutcome> {
        self.stage = GroupStage::Job(job.event_id);
        let status = self.group.status.clone();
        let cache = std::mem::take(&mut self.cache);
        let run =
            MaterializationJobOperation::new((job_key, job), status, cache, self.node.clone());
        Ok(Next::Call(IoCall::child(run)))
    }

    fn step(&mut self, result: IoResult) -> MaterializationTurn<MaterializationGroupOutcome> {
        match std::mem::replace(&mut self.stage, GroupStage::Load) {
            GroupStage::Load => {
                self.group = result.child::<GroupJobs, MetadataMaterializationError>()??;
                self.job()
            }
            GroupStage::Older(job_key, job) => {
                if result.child::<bool, MetadataMaterializationError>()?? {
                    return self.done();
                }
                self.run(job_key, job)
            }
            GroupStage::Job(event_id) => {
                let processed = result
                    .child::<ProcessedMaterializationJob, MetadataMaterializationError>()??;
                self.cache = processed.cache;
                let outcome = &mut self.outcome;
                outcome.craqle_elapsed =
                    (outcome.craqle_elapsed).saturating_add(processed.craqle_elapsed);
                if processed.attempted {
                    outcome.processed = outcome.processed.saturating_add(1);
                }
                if let Some(finished) = processed.finished {
                    if matches!(finished, FinishedMaterializationJob::Completed(_)) {
                        self.advanced.insert(event_id);
                    }
                    outcome.finished.push(finished);
                }
                if processed.stop_group {
                    return self.done();
                }
                self.job()
            }
        }
    }
}

impl Operation for JobGroupOperation {
    type Output = MaterializationGroupOutcome;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<MaterializationGroupOutcome> {
        let Some(document_id) = self.jobs.front().map(|(_, job)| job.document_id) else {
            return self.done();
        };
        Ok(Next::Call(IoCall::child(GroupJobsOperation::new(
            document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<MaterializationGroupOutcome> {
        match self.step(result) {
            Err(error) => {
                self.outcome.error = Some(error);
                self.done()
            }
            turn => turn,
        }
    }
}

/// Everything a document's group needs to order its own events: its queued jobs
/// and its status, loaded once instead of once per event.
#[derive(Debug, Default, PartialEq)]
struct GroupJobs {
    pending: Vec<MetadataMaterializationRecord>,
    status: Option<MaterializationStatusRecord>,
}

#[derive(Debug, PartialEq)]
enum LoadStage {
    Status,
    Page,
    Delete,
}

// The sidecar keyspace is per-document and event-ordered, so this costs one
// prefix scan plus one status read for the whole group.
#[derive(Debug, PartialEq)]
struct GroupJobsOperation {
    document_id: Ulid,
    group: GroupJobs,
    malformed: Vec<(String, ByteView)>,
    stage: LoadStage,
}

impl GroupJobsOperation {
    fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            group: GroupJobs::default(),
            malformed: Vec::new(),
            stage: LoadStage::Status,
        }
    }

    fn page(&mut self, after: Option<Key>) -> MaterializationTurn<GroupJobs> {
        self.stage = LoadStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: DOCUMENT_JOB_KEYSPACE.to_string(),
            prefix: Key::from(document_job_prefix(self.document_id)),
            after,
            limit: MATERIALIZATION_PAGE_SIZE,
        }))
    }

    fn loaded(&mut self) -> MaterializationTurn<GroupJobs> {
        let mut group = std::mem::take(&mut self.group);
        group.pending.sort_by_key(|job| job.event_id);
        Ok(Next::Done(group))
    }
}

impl Operation for GroupJobsOperation {
    type Output = GroupJobs;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<GroupJobs> {
        Ok(Next::Call(IoCall::Read(vec![status_read(
            self.document_id,
        )])))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<GroupJobs> {
        match self.stage {
            LoadStage::Status => {
                self.group.status = decode_status(result.value()?)?;
                self.page(None)
            }
            LoadStage::Page => {
                let (rows, next) = page_rows(result)?;
                for (key, value) in rows {
                    match postcard::from_bytes::<MetadataMaterializationRecord>(&value) {
                        Ok(job) if job.document_id == self.document_id => {
                            self.group.pending.push(job)
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(error = %error, key = ?key.to_vec(), "Deleting malformed metadata materialization document job");
                            self.malformed
                                .push((DOCUMENT_JOB_KEYSPACE.to_string(), key));
                        }
                    }
                }
                if let Some(after) = next {
                    return self.page(Some(after));
                }
                if self.malformed.is_empty() {
                    return self.loaded();
                }
                self.stage = LoadStage::Delete;
                let malformed = std::mem::take(&mut self.malformed);
                Ok(Next::Call(IoCall::Delete(malformed)))
            }
            LoadStage::Delete => {
                result.done()?;
                self.loaded()
            }
        }
    }
}

/// The queued jobs for earlier events of the document that `job` may have to wait for: none
/// that this batch advanced, and none a final status already covers.
fn older_candidates(
    group: &GroupJobs,
    job: &MetadataMaterializationRecord,
    advanced: &BTreeSet<Ulid>,
) -> Vec<MetadataMaterializationRecord> {
    (group.pending.iter())
        .filter(|pending| {
            pending.event_id < job.event_id
                && !advanced.contains(&pending.event_id)
                && !(group.status.as_ref())
                    .is_some_and(|status| status_obsoletes_job(status, pending))
        })
        .cloned()
        .collect()
}

#[derive(Debug, PartialEq)]
enum OlderStage {
    Event(MetadataMaterializationRecord),
    Prune,
}

// An unadvanced, live job for an earlier event of the same document must run
// first, so this one waits for the next batch. Orphan jobs met on the way are deleted.
#[derive(Debug, PartialEq)]
struct OlderJobOperation {
    pending: VecDeque<MetadataMaterializationRecord>,
    stage: OlderStage,
}

impl OlderJobOperation {
    fn new(pending: Vec<MetadataMaterializationRecord>) -> Self {
        Self {
            pending: pending.into(),
            stage: OlderStage::Prune,
        }
    }

    fn check(&mut self) -> MaterializationTurn<bool> {
        let Some(pending) = self.pending.pop_front() else {
            return Ok(Next::Done(false));
        };
        let read = event_read(pending.document_id, pending.event_id);
        self.stage = OlderStage::Event(pending);
        Ok(Next::Call(IoCall::Read(vec![read])))
    }
}

impl Operation for OlderJobOperation {
    type Output = bool;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<bool> {
        self.check()
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        let pending = match std::mem::replace(&mut self.stage, OlderStage::Prune) {
            OlderStage::Event(pending) => pending,
            OlderStage::Prune => {
                pruned(result)?;
                return self.check();
            }
        };
        match logged_event(pending.document_id, pending.event_id, result.value()?) {
            // Merges commute and wait for their own dependencies, so an earlier merge never
            // holds back later work; blocking on it could wait for a dot a later event carries.
            Ok(event) if merges(&event) => self.check(),
            Ok(_) => Ok(Next::Done(true)),
            Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
                warn!(document_id = %pending.document_id, event_id = %pending.event_id, "Deleting orphan metadata materialization job");
                let stale = StaleJob {
                    index_key: materialization_job_key(&pending).to_vec(),
                    seen: Some(pending),
                    drop_sidecar: true,
                };
                Ok(Next::Call(IoCall::child(PruneJobsOperation::new(vec![
                    stale,
                ]))))
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, PartialEq)]
struct MaterializedCreateEvent {
    raw_revision: Option<MetadataRawRevision>,
    raw_state_write: (String, ByteView, ByteView),
    validation_write: Option<(String, ByteView, ByteView)>,
}

#[derive(Debug, PartialEq)]
enum JobStage {
    Event,
    Deleted,
    Removed,
    Policy,
    Change,
    Export,
    Assess(String),
    Merged(Box<ProfileValidationStatus>),
    Raw,
    Apply(Box<RawEventPlan>),
    Mark(Box<MaterializedCreateEvent>),
    Capture(Box<MaterializedCreateEvent>),
    Checkpoint(Box<MaterializedCreateEvent>),
    Project(Box<MaterializedCreateEvent>),
}

/// Applies one job's event to the graph store, publishes its Git snapshot and projects its
/// IRI and data link rows, all left for the finish transaction to store. An event that
/// fails is rescheduled or parked; a terminal failure finishes with a failed status.
#[derive(Debug, PartialEq)]
struct MaterializationJobOperation {
    job_key: Vec<u8>,
    job: MetadataMaterializationRecord,
    status: Option<MaterializationStatusRecord>,
    cache: RawStateCache,
    node: GitNode,
    event: Option<MetadataEventRecord>,
    now_ms: u64,
    apply_started_ms: u64,
    craqle_elapsed: Duration,
    stage: JobStage,
}

impl MaterializationJobOperation {
    fn new(
        (job_key, job): (Vec<u8>, MetadataMaterializationRecord),
        status: Option<MaterializationStatusRecord>,
        cache: RawStateCache,
        node: GitNode,
    ) -> Self {
        Self {
            job_key,
            job,
            status,
            cache,
            node,
            event: None,
            now_ms: 0,
            apply_started_ms: 0,
            craqle_elapsed: Duration::ZERO,
            stage: JobStage::Event,
        }
    }

    fn event(&self) -> Result<&MetadataEventRecord, MetadataMaterializationError> {
        (self.event.as_ref()).ok_or_else(|| {
            MetadataMaterializationError::UnexpectedEvent("materialization event not read".into())
        })
    }

    fn document_job_key(&self) -> Vec<u8> {
        document_job_key(self.job.document_id, self.job.event_id).to_vec()
    }

    fn finish(
        &mut self,
        mut processed: ProcessedMaterializationJob,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        processed.cache = std::mem::take(&mut self.cache);
        Ok(Next::Done(processed))
    }

    fn settled(
        &mut self,
        status: Option<MaterializationStatusRecord>,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let job_key = std::mem::take(&mut self.job_key);
        let completed =
            CompletedMaterializationJob::without_rows(job_key, self.document_job_key(), status);
        let elapsed = self.craqle_elapsed;
        self.finish(ProcessedMaterializationJob::completed(completed, elapsed))
    }

    fn read(&mut self, result: IoResult) -> MaterializationTurn<ProcessedMaterializationJob> {
        let (document_id, event_id) = (self.job.document_id, self.job.event_id);
        let event = (result.value().map_err(MetadataMaterializationError::from))
            .and_then(|value| logged_event(document_id, event_id, value));
        // A merge commutes, so a later materialized event never makes an earlier one obsolete;
        // skipping it would drop a concurrent edit that arrived late.
        let obsolescence = match job_obsolescence(self.status.as_ref(), &self.job) {
            MaterializationJobObsolescence::Final if event.as_ref().is_ok_and(merges) => {
                MaterializationJobObsolescence::Live
            }
            other => other,
        };
        match obsolescence {
            MaterializationJobObsolescence::Live => {}
            MaterializationJobObsolescence::Final => return self.settled(None),
            MaterializationJobObsolescence::RetryAdvanced => {
                self.stage = JobStage::Removed;
                let index = (
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(std::mem::take(&mut self.job_key)),
                );
                return Ok(Next::Call(IoCall::Delete(vec![index])));
            }
        }
        let event = match event {
            Ok(event) => event,
            Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
                return self.settled(None);
            }
            Err(error) => return Err(error),
        };
        let deleted = GraphDeletedOperation::new(&event.record.graph_iri);
        self.event = Some(event);
        self.stage = JobStage::Deleted;
        Ok(Next::Call(IoCall::child(deleted)))
    }

    fn apply(&mut self) -> MaterializationTurn<ProcessedMaterializationJob> {
        self.apply_started_ms = self.now_ms;
        let event = self.event()?;
        if !merges(event) {
            let prepare = PrepareRawOperation::new(event.clone(), self.cache.clone());
            self.stage = JobStage::Raw;
            return Ok(Next::Call(IoCall::child(prepare)));
        }
        // A batch carries quads only, so the event's visibility is applied here the
        // way a crate replacement used to carry it.
        let effect = MetadataEffect::SetGraphPolicy {
            graph_iri: event.record.graph_iri.clone(),
            policy: event_policy(event),
        };
        self.stage = JobStage::Policy;
        Ok(graph_call(Effect::Metadata(effect)))
    }

    /// Merges the origin's batch or joins a checkpoint's graph state, then re-renders and
    /// re-validates the graph. Both are order independent and idempotent by dot, so every
    /// holder converges whatever order events arrive in.
    fn applied(
        &mut self,
        stage: JobStage,
        result: IoResult,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let Some(event) = self.event.take() else {
            return Err(MetadataMaterializationError::UnexpectedEvent(
                "materialization event not read".into(),
            ));
        };
        let turn = self.apply_step(&event, stage, result);
        self.event = Some(event);
        turn
    }

    fn apply_step(
        &mut self,
        event: &MetadataEventRecord,
        stage: JobStage,
        result: IoResult,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let graph_iri = event.record.graph_iri.clone();
        match stage {
            JobStage::Policy => match graph_event(result)? {
                MetadataEvent::GraphPolicySet { .. } => {
                    self.stage = JobStage::Change;
                    Ok(graph_call(graph_materialization_effect(event, None, false)))
                }
                other => Err(unexpected(other)),
            },
            JobStage::Change => match graph_event(result)? {
                MetadataEvent::BatchMerged { .. } | MetadataEvent::SnapshotInstalled { .. } => {
                    self.stage = JobStage::Export;
                    let export = MetadataEffect::ExportRoCrate { graph_iri };
                    Ok(graph_call(Effect::Metadata(export)))
                }
                other => Err(unexpected(other)),
            },
            JobStage::Export => match graph_event(result)? {
                MetadataEvent::RoCrateExportResult { jsonld, .. } => {
                    let record = &event.record;
                    let assess = ProfileAssessOperation::render(
                        self.node.node(),
                        record.document_id,
                        record.group_id,
                        jsonld.clone(),
                    );
                    self.stage = JobStage::Assess(jsonld);
                    Ok(Next::Call(IoCall::child(assess)))
                }
                other => Err(unexpected(other)),
            },
            JobStage::Assess(render) => {
                let assessed = result.child::<MetadataProfilePreview, MetadataError>()?;
                let status = render_verdict(assessed, event.record.document_id);
                let findings = violation_count(&status);
                let event = event.clone();
                let prepare =
                    PrepareMergedOperation::new(event, (render, findings), self.cache.clone());
                self.stage = JobStage::Merged(Box::new(status));
                Ok(Next::Call(IoCall::child(prepare)))
            }
            JobStage::Merged(mut status) => {
                let plan = result.child::<RawEventPlan, RawReadError>()??;
                plan.cache(&mut self.cache);
                status.dataset_revision = event.event_id;
                status.dataset_digest =
                    (plan.revision.as_ref()).and_then(|revision| revision.dataset_digest);
                let validation_write = profile_validation_entry(&status)?;
                self.mark(MaterializedCreateEvent {
                    raw_revision: plan.revision,
                    raw_state_write: plan.state_write,
                    validation_write: Some(validation_write),
                })
            }
            JobStage::Raw => {
                let plan = result.child::<RawEventPlan, RawReadError>()??;
                let effect =
                    graph_materialization_effect(event, plan.revision.as_ref(), plan.rebuild);
                self.stage = JobStage::Apply(Box::new(plan));
                Ok(graph_call(effect))
            }
            JobStage::Apply(plan) => match graph_event(result)? {
                MetadataEvent::CreateCrateResult { .. }
                | MetadataEvent::ApplyRoCrateResult { .. }
                | MetadataEvent::EntityUpsertResult { .. } => {
                    plan.cache(&mut self.cache);
                    self.mark(MaterializedCreateEvent {
                        raw_revision: plan.revision,
                        raw_state_write: plan.state_write,
                        validation_write: None,
                    })
                }
                other => Err(unexpected(other)),
            },
            other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
                "materialization step {other:?} is not an apply step"
            ))),
        }
    }

    fn apply_failed(
        &mut self,
        error: MetadataMaterializationError,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        self.craqle_elapsed = elapsed(self.apply_started_ms, self.now_ms);
        if !is_terminal_error(&error) {
            return self.defer(&error);
        }
        let event = self.event()?;
        let status = materialization_failure_status(
            &self.job,
            event,
            (error.to_string(), self.job.failures),
            true,
            self.now_ms,
        );
        self.settled(Some(status))
    }

    fn defer(
        &mut self,
        error: &MetadataMaterializationError,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let event = self.event()?;
        let finished =
            defer_materialization_job(&self.job_key, &self.job, event, error, self.now_ms);
        let elapsed = self.craqle_elapsed;
        self.finish(ProcessedMaterializationJob::deferred(finished, elapsed))
    }

    // Refreshes keep off the raw view from here until the finish batch stores this graph.
    fn mark(
        &mut self,
        materialized: MaterializedCreateEvent,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        self.craqle_elapsed = elapsed(self.apply_started_ms, self.now_ms);
        let mark = (
            MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            document_job_key(self.job.document_id, self.job.event_id),
            ByteView::from(Vec::new()),
        );
        self.stage = JobStage::Mark(Box::new(materialized));
        Ok(Next::Call(IoCall::Write(vec![mark])))
    }

    fn projected(
        &mut self,
        materialized: MaterializedCreateEvent,
        rows: LinkRows,
    ) -> MaterializationTurn<ProcessedMaterializationJob> {
        let job_key = std::mem::take(&mut self.job_key);
        let event = self.event()?;
        let status = materialization_success_status(
            &self.job,
            event,
            materialized.raw_revision.as_ref(),
            self.now_ms,
        );
        let completed = CompletedMaterializationJob {
            job_key,
            document_job_key: Some(self.document_job_key()),
            status: Some(status),
            iri_index_writes: rows.writes,
            link_deletes: rows.deletes,
            raw_state_write: Some(materialized.raw_state_write),
            validation_write: materialized.validation_write,
            sync: Some(CompletedMaterializationSync {
                graph_iri: event.record.graph_iri.clone(),
                peers: event.record.holder_node_ids.clone(),
            }),
            merge: merges(event),
        };
        let elapsed = self.craqle_elapsed;
        self.finish(ProcessedMaterializationJob::completed(completed, elapsed))
    }
}

impl Operation for MaterializationJobOperation {
    type Output = ProcessedMaterializationJob;
    type Error = MetadataMaterializationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> MaterializationTurn<ProcessedMaterializationJob> {
        let read = event_read(self.job.document_id, self.job.event_id);
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<ProcessedMaterializationJob> {
        match std::mem::replace(&mut self.stage, JobStage::Removed) {
            JobStage::Event => self.read(result),
            JobStage::Removed => {
                result.done()?;
                self.finish(ProcessedMaterializationJob::default())
            }
            JobStage::Deleted => {
                let deleted = result.child::<bool, GraphDeletedError>()?;
                if !deleted
                    .map_err(GraphDeletedError::into_error::<MetadataMaterializationError>)?
                {
                    return self.apply();
                }
                let message = "metadata graph was deleted before materialization".to_string();
                let event = self.event()?;
                let status = materialization_failure_status(
                    &self.job,
                    event,
                    (message, self.job.failures),
                    true,
                    self.now_ms,
                );
                self.settled(Some(status))
            }
            JobStage::Mark(materialized) => {
                if let Err(error) = result.done() {
                    return self.defer(&MetadataMaterializationError::from(error));
                }
                let event = self.event()?;
                let snapshot = capture(
                    &self.node,
                    &event.record,
                    materialized.raw_revision.as_ref(),
                );
                self.stage = JobStage::Capture(materialized);
                Ok(Next::Call(IoCall::child(snapshot)))
            }
            JobStage::Capture(materialized) => {
                let failed = match result.child::<(), GitError>() {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error.to_string()),
                    Err(error) => Some(error.to_string()),
                };
                if let Some(message) = failed {
                    return self.defer(&MetadataMaterializationError::UnexpectedEvent(message));
                }
                let event = self.event()?;
                let checkpoint = CheckpointOperation::new(self.node.node(), event.record.clone());
                self.stage = JobStage::Checkpoint(materialized);
                Ok(Next::Call(IoCall::child(checkpoint)))
            }
            JobStage::Checkpoint(materialized) => {
                let event = self.event()?;
                let document_id = event.record.document_id;
                // The next materialization of this document checks the window again.
                match result.child::<(), CheckpointError>() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(document_id = %document_id, %error, "Metadata checkpoint failed")
                    }
                    Err(error) => {
                        warn!(document_id = %document_id, %error, "Metadata checkpoint failed")
                    }
                }
                let document = (document_id, event.event_id, event.record.graph_iri.clone());
                self.stage = JobStage::Project(materialized);
                Ok(Next::Call(IoCall::child(ProjectRowsOperation::new(
                    document,
                ))))
            }
            JobStage::Project(materialized) => {
                match result.child::<(LinkRows, usize), MetadataIriError>() {
                    Ok(Ok((rows, _))) => self.projected(*materialized, rows),
                    Ok(Err(error)) => self.defer(&MetadataMaterializationError::from(error)),
                    Err(error) => self.defer(&MetadataMaterializationError::from(error)),
                }
            }
            stage => match self.applied(stage, result) {
                Err(error) => self.apply_failed(error),
                turn => turn,
            },
        }
    }
}

fn graph_call(effect: Effect) -> Next<ProcessedMaterializationJob> {
    Next::Call(IoCall::Effect(Box::new(effect)))
}

/// The graph store's answer to one metadata effect; an error answer is the effect's failure.
fn graph_event(result: IoResult) -> Result<MetadataEvent, MetadataMaterializationError> {
    match event(result)? {
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
        Event::Metadata(answer) => Ok(answer),
        other => Err(MetadataMaterializationError::UnexpectedEvent(
            other.label().to_string(),
        )),
    }
}

fn unexpected(answer: MetadataEvent) -> MetadataMaterializationError {
    MetadataMaterializationError::UnexpectedEvent(Event::Metadata(answer).label().to_string())
}

fn event_policy(event: &MetadataEventRecord) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: event.record.public,
        permission_paths: vec![event.record.permission_path.clone()],
    }
    .normalized()
}

fn status_read(document_id: Ulid) -> (String, Key) {
    let key = materialization_status_key(document_id);
    (MATERIALIZATION_STATUS_KEYSPACE.to_string(), key)
}

fn event_read(document_id: Ulid, event_id: Ulid) -> (String, Key) {
    let key = event_log_key(document_id, event_id);
    (EVENT_LOG_KEYSPACE.to_string(), key)
}

fn sidecar_read(document_id: Ulid, event_id: Ulid) -> (String, Key) {
    let key = document_job_key(document_id, event_id);
    (DOCUMENT_JOB_KEYSPACE.to_string(), key)
}

fn decode_status(value: Option<Value>) -> Result<Option<Status>, MetadataMaterializationError> {
    let status = value.map(|value| postcard::from_bytes(&value)).transpose();
    Ok(status.map_err(ConversionError::from)?)
}

/// The create event a job materializes, read from the event log row for its key.
fn logged_event(
    document_id: Ulid,
    event_id: Ulid,
    value: Option<Value>,
) -> Result<MetadataEventRecord, MetadataMaterializationError> {
    let Some(value) = value else {
        return Err(MetadataMaterializationError::CreateEventMissing {
            document_id,
            event_id,
        });
    };
    let event: MetadataEventRecord = postcard::from_bytes(&value).map_err(ConversionError::from)?;
    if event.record.document_id != document_id || event.event_id != event_id {
        return Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "metadata event log key mismatch for {document_id}/{event_id}"
        )));
    }
    Ok(event)
}

fn job_obsolescence(
    status: Option<&MaterializationStatusRecord>,
    job: &MetadataMaterializationRecord,
) -> MaterializationJobObsolescence {
    let Some(status) = status else {
        return MaterializationJobObsolescence::Live;
    };
    if status_obsoletes_job(status, job) {
        return MaterializationJobObsolescence::Final;
    }
    if status.event_id == job.event_id && status.attempts > job.attempts {
        return MaterializationJobObsolescence::RetryAdvanced;
    }
    MaterializationJobObsolescence::Live
}

fn status_is_final(status: &MaterializationStatusRecord) -> bool {
    matches!(
        status.state,
        MaterializationState::Materialized | MaterializationState::Failed
    )
}

/// Batches and checkpoints merge by dot, in any order and more than once.
fn merges(event: &MetadataEventRecord) -> bool {
    matches!(
        event.payload,
        MetadataEventPayload::ApplyBatch { .. } | MetadataEventPayload::Checkpoint { .. }
    )
}

fn status_obsoletes_job(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status.event_id >= job.event_id && status_is_final(status)
}

fn retry_already_advanced(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status_obsoletes_job(status, job)
        || (status.event_id == job.event_id && status.attempts > job.attempts)
}

/// Whether the status moved past the job; only a retry of the same event does for a merge.
fn job_superseded(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
    merge: bool,
) -> bool {
    if merge {
        status.event_id == job.event_id && status.attempts > job.attempts
    } else {
        retry_already_advanced(status, job)
    }
}

fn should_write_final(
    current: Option<&MaterializationStatusRecord>,
    next: &MaterializationStatusRecord,
) -> bool {
    !current.is_some_and(|current| {
        current.event_id > next.event_id
            || (current.event_id == next.event_id && current.attempts >= next.attempts)
            || (current.event_id == next.event_id && status_is_final(current))
    })
}

fn should_write_retry(
    current: Option<&MaterializationStatusRecord>,
    next: &MaterializationStatusRecord,
) -> bool {
    !current.is_some_and(|current| {
        current.event_id > next.event_id
            || retry_already_advanced(
                current,
                &MetadataMaterializationRecord {
                    document_id: next.document_id,
                    event_id: next.event_id,
                    due_at_ms: 0,
                    attempts: next.attempts,
                    failures: next.failures,
                    parks: 0,
                },
            )
    })
}

#[derive(Debug, PartialEq)]
enum ExistStage {
    Page,
    Sidecar(Vec<u8>, u64),
    Live(Vec<u8>, MetadataMaterializationRecord),
    Pruned,
    Removed,
}

/// Whether a live job is queued. Dead and stale rows met on the way are deleted, so a queue
/// of only dead jobs answers `false` and is empty afterwards.
#[derive(Debug, PartialEq)]
pub(crate) struct JobsExistOperation {
    next: Option<Key>,
    stage: ExistStage,
}

impl JobsExistOperation {
    pub(crate) fn new() -> Self {
        Self {
            next: None,
            stage: ExistStage::Page,
        }
    }

    fn page(&mut self, after: Option<Key>) -> MaterializationTurn<bool> {
        self.stage = ExistStage::Page;
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: 1,
        }))
    }

    fn advance(&mut self) -> MaterializationTurn<bool> {
        match self.next.take() {
            Some(after) => self.page(Some(after)),
            None => Ok(Next::Done(false)),
        }
    }

    fn remove(&mut self, deletes: Vec<(String, ByteView)>) -> MaterializationTurn<bool> {
        self.stage = ExistStage::Removed;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for JobsExistOperation {
    type Output = bool;
    type Error = MetadataMaterializationError;

    fn first(&mut self) -> MaterializationTurn<bool> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<bool> {
        match std::mem::replace(&mut self.stage, ExistStage::Removed) {
            ExistStage::Page => {
                let (rows, next) = page_rows(result)?;
                self.next = next;
                let Some((key, _value)) = rows.into_iter().next() else {
                    return Ok(Next::Done(false));
                };
                let key = key.to_vec();
                let Some((due_at_ms, document_id, event_id)) = job_key_parts(&key) else {
                    return self.remove(vec![global_row(key)]);
                };
                self.stage = ExistStage::Sidecar(key, due_at_ms);
                Ok(Next::Call(IoCall::Read(vec![sidecar_read(
                    document_id,
                    event_id,
                )])))
            }
            ExistStage::Sidecar(key, due_at_ms) => {
                let value = result.value()?;
                let job = value
                    .map(|value| postcard::from_bytes::<MetadataMaterializationRecord>(&value));
                match job {
                    Some(Ok(job)) if job.due_at_ms == due_at_ms => {
                        let reads = vec![
                            event_read(job.document_id, job.event_id),
                            status_read(job.document_id),
                        ];
                        self.stage = ExistStage::Live(key, job);
                        Ok(Next::Call(IoCall::Read(reads)))
                    }
                    Some(Err(error)) => {
                        let sidecar = job_key_target(&key).map(|(document_id, event_id)| {
                            warn!(error = %error, document_id = %document_id, event_id = %event_id, "Deleting malformed metadata materialization document job");
                            sidecar_read(document_id, event_id)
                        });
                        let deletes = sidecar.into_iter().chain([global_row(key)]).collect();
                        self.remove(deletes)
                    }
                    _ => self.remove(vec![global_row(key)]),
                }
            }
            ExistStage::Live(key, job) => {
                let [event, status] = values::<2>(result)?;
                let live = match logged_event(job.document_id, job.event_id, event) {
                    Ok(event) => !(decode_status(status)?)
                        .is_some_and(|status| job_superseded(&status, &job, merges(&event))),
                    Err(MetadataMaterializationError::CreateEventMissing { .. }) => false,
                    Err(error) => return Err(error),
                };
                if live {
                    return Ok(Next::Done(true));
                }
                let stale = StaleJob {
                    index_key: key,
                    seen: Some(job),
                    drop_sidecar: true,
                };
                self.stage = ExistStage::Pruned;
                Ok(Next::Call(IoCall::child(PruneJobsOperation::new(vec![
                    stale,
                ]))))
            }
            ExistStage::Pruned => {
                pruned(result)?;
                self.advance()
            }
            ExistStage::Removed => {
                result.done()?;
                self.advance()
            }
        }
    }
}

fn global_row(key: Vec<u8>) -> (String, ByteView) {
    (
        MATERIALIZATION_JOB_KEYSPACE.to_string(),
        ByteView::from(key),
    )
}

/// An index row a scan found stale or dead, the sidecar it read for it and whether that
/// sidecar, with its capture mark, goes too.
#[derive(Debug, PartialEq)]
struct StaleJob {
    index_key: Vec<u8>,
    seen: Option<MetadataMaterializationRecord>,
    drop_sidecar: bool,
}

#[derive(Debug, PartialEq)]
enum PruneStage {
    Read,
    Delete,
}

/// Deletes stale job rows after reading their sidecars again in one transaction. A sidecar a
/// newer retry rewrote keeps its rows and capture mark; a conflict leaves all to the next scan.
#[derive(Debug, PartialEq)]
struct PruneJobsOperation {
    stale: Vec<StaleJob>,
    stage: PruneStage,
}

impl PruneJobsOperation {
    fn new(stale: Vec<StaleJob>) -> Self {
        Self {
            stale,
            stage: PruneStage::Read,
        }
    }

    fn delete(&mut self, sidecars: Vec<Option<Value>>) -> MaterializationTurn<()> {
        let mut sidecars = sidecars.into_iter();
        let mut deletes = Vec::new();
        for row in std::mem::take(&mut self.stale) {
            let target = job_key_parts(&row.index_key);
            let index = global_row(row.index_key);
            let Some((due_at_ms, document_id, event_id)) = target else {
                deletes.push(index);
                continue;
            };
            let current = (sidecars.next().flatten()).and_then(|value| {
                postcard::from_bytes::<MetadataMaterializationRecord>(&value).ok()
            });
            let changed = current != row.seen;
            // A retry that rewrote the sidecar onto this very index row keeps it.
            if !(changed && current.is_some_and(|job| job.due_at_ms == due_at_ms)) {
                deletes.push(index);
            }
            if row.drop_sidecar && !changed {
                let key = document_job_key(document_id, event_id);
                deletes.push((DOCUMENT_JOB_KEYSPACE.to_string(), key.clone()));
                deletes.push((MATERIALIZATION_CAPTURE_KEYSPACE.to_string(), key));
            }
        }
        if deletes.is_empty() {
            return Ok(Next::Done(()));
        }
        self.stage = PruneStage::Delete;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for PruneJobsOperation {
    type Output = ();
    type Error = MetadataMaterializationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> MaterializationTurn<()> {
        let reads: Vec<_> = (self.stale.iter())
            .filter_map(|row| job_key_parts(&row.index_key))
            .map(|(_, document_id, event_id)| sidecar_read(document_id, event_id))
            .collect();
        if reads.is_empty() {
            return self.delete(Vec::new());
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> MaterializationTurn<()> {
        match self.stage {
            PruneStage::Read => self.delete(all_values(result)?),
            PruneStage::Delete => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// A prune's answer; one that lost its transaction leaves the rows to the next scan.
fn pruned(result: IoResult) -> Result<(), MetadataMaterializationError> {
    match result.child::<(), MetadataMaterializationError>()? {
        Ok(()) | Err(MetadataMaterializationError::Storage(StorageError::TransactionConflict)) => {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn job_key_target(key: &[u8]) -> Option<(Ulid, Ulid)> {
    job_key_parts(key).map(|(_, document_id, event_id)| (document_id, event_id))
}

fn job_key_parts(key: &[u8]) -> Option<(u64, Ulid, Ulid)> {
    if key.len() != 40 {
        return None;
    }
    let mut due_at_ms = [0u8; 8];
    due_at_ms.copy_from_slice(&key[..8]);
    let mut document_id = [0u8; 16];
    document_id.copy_from_slice(&key[8..24]);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&key[24..40]);
    Some((
        u64::from_be_bytes(due_at_ms),
        Ulid::from_bytes(document_id),
        Ulid::from_bytes(event_id),
    ))
}

fn graph_materialization_effect(
    event: &MetadataEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
    rebuild: bool,
) -> Effect {
    let policy = event_policy(event);
    let deterministic_actor = Some(deterministic_materialization_actor(event.event_id));
    if rebuild && let Some(raw_revision) = raw_revision {
        return Effect::Metadata(MetadataEffect::ApplyRoCrate {
            request: ApplyRoCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                jsonld: raw_revision.jsonld.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        });
    }
    match &event.payload {
        MetadataEventPayload::Scaffold {
            name,
            description,
            date_published,
            license,
        } => Effect::Metadata(MetadataEffect::CreateCrate {
            request: MetadataCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        }),
        MetadataEventPayload::RoCrate { jsonld }
        | MetadataEventPayload::ReplaceRoCrate { jsonld } => {
            Effect::Metadata(MetadataEffect::ApplyRoCrate {
                request: ApplyRoCrateRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    policy,
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataEventPayload::UpsertDataEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertDataEntity {
                request: aruna_core::metadata::UpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataEventPayload::UpsertContextualEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertContextualEntity {
                request: aruna_core::metadata::UpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataEventPayload::ApplyBatch { batch, .. } => {
            Effect::Metadata(MetadataEffect::MergeBatch {
                graph_iri: event.record.graph_iri.clone(),
                batch: batch.clone(),
            })
        }
        // A checkpoint without graph state joins an empty snapshot, which changes nothing.
        MetadataEventPayload::Checkpoint { snapshot } => {
            Effect::Metadata(MetadataEffect::InstallSnapshot {
                graph_iri: event.record.graph_iri.clone(),
                snapshot: snapshot.clone().unwrap_or_else(|| {
                    Box::new(craqle::GraphReplicaSnapshot {
                        graph: craqle::GraphId::new(&event.record.graph_iri),
                        clock: craqle::VectorClock::default(),
                        quads: Vec::new(),
                    })
                }),
            })
        }
    }
}

fn materialization_success_status(
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
    now_ms: u64,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: raw_revision.map(|revision| revision.context_digest),
        dataset_digest: raw_revision.and_then(|revision| revision.dataset_digest),
        state: MaterializationState::Materialized,
        attempts: job.attempts.saturating_add(1),
        failures: job.failures,
        last_error: None,
        updated_at_ms: now_ms,
    }
}

fn materialization_failure_status(
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    (error, failures): (String, u32),
    terminal: bool,
    now_ms: u64,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: None,
        dataset_digest: None,
        state: if terminal {
            MaterializationState::Failed
        } else {
            MaterializationState::Pending
        },
        attempts: job.attempts.saturating_add(1),
        failures,
        last_error: Some(error),
        updated_at_ms: now_ms,
    }
}

/// How a failed materialization attempt is charged. Only [`Application`]
/// failures spend the budget that eventually parks a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationFailureKind {
    Terminal,
    Transient,
    Application,
}

fn materialization_failure_kind(
    error: &MetadataMaterializationError,
) -> MaterializationFailureKind {
    match error {
        MetadataMaterializationError::Metadata(
            MetadataError::InvalidInput(_) | MetadataError::Validation(_),
        ) => MaterializationFailureKind::Terminal,
        // A storage failure or an off-contract adapter event is never the
        // document's fault, so it must not spend the budget that parks a job.
        MetadataMaterializationError::Storage(_)
        | MetadataMaterializationError::UnexpectedEvent(_)
        | MetadataMaterializationError::Metadata(
            MetadataError::ChannelClosed
            | MetadataError::MissingDependencies(_)
            | MetadataError::TaskJoin(_)
            | MetadataError::HandleMissing
            | MetadataError::Persist(_)
            | MetadataError::Storage(_),
        ) => MaterializationFailureKind::Transient,
        _ => MaterializationFailureKind::Application,
    }
}

fn is_terminal_error(error: &MetadataMaterializationError) -> bool {
    matches!(
        materialization_failure_kind(error),
        MaterializationFailureKind::Terminal
    )
}

#[cfg(test)]
#[path = "materialization_queue_tests.rs"]
mod tests;
