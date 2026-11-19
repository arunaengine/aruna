//! Drains due blob replication jobs, retries failed ones and keeps the scan cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    NODE_STATE_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE, REPLICATION_JOB_KEYSPACE,
    REPLICATION_OBLIGATION_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::replication::ReplicationFailure;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::structs::{
    ReferenceHandling, SyncMode, SyncRelationship, SyncState, sync_relationship_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, Value};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use ulid::Ulid;

use super::catch_up::{BOOTSTRAP_POLL, BootstrapWaitingOperation, ProcessBootstrapOperation};
use super::members::{BoundRecord, BoundRecordOperation, RecordError, member_publication};
use super::obligations::{LiveRepairResult, ProcessObligationsOperation};
use super::page::PAGE_RECORDS;
use super::page::deliver::{DeliverPagesOperation, PageReport, PageTransport, pageable};
use super::parking::{PARKED_DUE, ParkJobOperation};
use super::plaintext::{HolderCheckOperation, PlaintextError, job_consent, relationship_consent};
use super::queue::BlobQueueError;
use super::retire::{BlobJobOutcome, RetireJobOperation};
use super::source_rules::SourceAuthOperation;
use super::version_replication::{
    ReplicateScopeError, ReplicateScopeOperation, ReplicateScopeResult, SourceAuthorization,
    SourceAuthorizationError,
};
use crate::groups::routing::{QuotaOperation, RoutingInputsError};
use crate::notifications::watch::origin::EmitEventOperation;
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::sync::mirror_repair::{KickRepairOperation, StoreStatusOperation, SyncMirrorError};
use crate::queue_backoff::{due_after, min_due_at, retry_delay_ms};
use aruna_core::structs::storage::replication_queue::MemberTarget;
use aruna_core::structs::storage::replication_queue::ReplicateScopeTarget;
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, blob_job_entry, blob_job_key, blob_job_preferred, merge_due_job,
    validate_sync_key,
};

const REPLICATION_BATCH_SIZE: usize = 64;

pub(super) const REPLICATION_CURSOR_KEY: &[u8] = b"blob_replication_cursor";

pub const REPLICATION_RETRY_AFTER: Duration = Duration::from_secs(1);

type Turn<T> = Result<Next<T>, BlobQueueError>;
type Job = (Vec<u8>, BlobJobRecord);
/// This node and its realm, where it serves one.
type Serving = Option<(NodeId, RealmId)>;
type Relationships = HashMap<(String, Ulid), SyncRelationship>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDrainResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub(super) struct BlobJobScan {
    pub(super) jobs: Vec<Job>,
    /// The generation the scan started in.
    generation: u64,
    pub(super) has_more_due: bool,
    next_due_ms: Option<u64>,
    pub(super) next_cursor: ReplicationScanCursor,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ReplicationScanCursor {
    pub(super) generation: u64,
    pub(super) after: Option<Vec<u8>>,
    #[serde(rename = "next_due_at_ms")]
    pub(super) next_due_ms: Option<u64>,
}

fn cursor_key() -> Key {
    ByteView::from(REPLICATION_CURSOR_KEY.to_vec())
}

fn cursor_read() -> IoCall {
    read_row(NODE_STATE_KEYSPACE, cursor_key())
}

/// Sets the drain's timer from the queue when the node starts.
#[derive(Debug, Default, PartialEq)]
pub struct RestoreDrainOperation {
    setting: bool,
}

impl Operation for RestoreDrainOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::child(NextTimerOperation::new())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.setting {
            if let Event::Task(TaskEvent::Error { message, .. }) = result.event()? {
                warn!(message = %message, "Failed to restore blob replication timer");
            }
            return Ok(Next::Done(()));
        }
        match result.child::<Option<Duration>, BlobQueueError>()? {
            Ok(None) => Ok(Next::Done(())),
            Ok(Some(after)) => {
                self.setting = true;
                let timer = Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainReplicationQueue,
                    after,
                });
                Ok(Next::Call(timer.into()))
            }
            Err(error) => {
                warn!(error = ?error, "Failed to scan blob replication jobs");
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum TimerStep {
    Obligations,
    Bootstrap,
    Scan,
    Cursor(Option<Duration>),
}

/// When the drain must run next, from the obligations, bootstraps and due jobs.
#[derive(Debug, PartialEq)]
pub struct NextTimerOperation {
    now_ms: u64,
    step: TimerStep,
}

impl NextTimerOperation {
    pub fn new() -> Self {
        Self {
            now_ms: 0,
            step: TimerStep::Obligations,
        }
    }

    fn advance(
        &mut self,
        cursor: ReplicationScanCursor,
        after: Option<Duration>,
    ) -> Turn<Option<Duration>> {
        self.step = TimerStep::Cursor(after);
        let advance = AdvanceCursorOperation::new(cursor);
        Ok(Next::Call(IoCall::child(advance)))
    }
}

impl Default for NextTimerOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for NextTimerOperation {
    type Output = Option<Duration>;
    type Error = BlobQueueError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        Ok(Next::Call(IoCall::Page {
            space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match std::mem::replace(&mut self.step, TimerStep::Scan) {
            TimerStep::Obligations => {
                if !result.page()?.0.is_empty() {
                    return Ok(Next::Done(Some(Duration::ZERO)));
                }
                self.step = TimerStep::Bootstrap;
                Ok(Next::Call(IoCall::child(BootstrapWaitingOperation)))
            }
            TimerStep::Bootstrap => {
                if result.child::<bool, IoError>()?.unwrap_or(false) {
                    return Ok(Next::Done(Some(Duration::ZERO)));
                }
                let scan = ScanJobsOperation::new(self.now_ms, 1);
                Ok(Next::Call(IoCall::child(scan)))
            }
            TimerStep::Scan => {
                let scan = result.child::<BlobJobScan, BlobQueueError>()??;
                if !scan.jobs.is_empty() {
                    return Ok(Next::Done(Some(Duration::ZERO)));
                }
                let after = match scan.has_more_due {
                    true => Some(Duration::ZERO),
                    false => (scan.next_due_ms).map(|due| due_after(self.now_ms, due)),
                };
                self.advance(scan.next_cursor, after)
            }
            TimerStep::Cursor(after) => {
                result.child::<(), BlobQueueError>()??;
                Ok(Next::Done(after))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum BatchStep {
    Repair,
    Bootstrap,
    Scan,
    Deliver,
    Relationships,
    Job(Box<Job>),
    Retired,
    Rescheduled,
    Parked,
    Cursor,
}

/// One drain: obligations become jobs, member jobs of reference records go out in pages, and
/// a bounded number of other jobs is delivered one by one.
#[derive(Debug, PartialEq)]
pub struct ProcessBatchOperation<T> {
    transport: T,
    routing: NodeRouting,
    serving: Serving,
    started_ms: u64,
    scan_ms: u64,
    step: BatchStep,
    repair: LiveRepairResult,
    bootstrap: bool,
    generation: u64,
    next_cursor: ReplicationScanCursor,
    next_due_ms: Option<u64>,
    has_more_due: bool,
    job_count: usize,
    oldest_lag_ms: u64,
    singles: VecDeque<Job>,
    relationships: Relationships,
    succeeded: usize,
    failed: usize,
}

type BatchTurn = Turn<BlobDrainResult>;

impl<T: PageTransport> ProcessBatchOperation<T> {
    pub fn new(transport: T, routing: NodeRouting, serving: Serving) -> Self {
        Self {
            transport,
            routing,
            serving,
            started_ms: 0,
            scan_ms: 0,
            step: BatchStep::Repair,
            repair: LiveRepairResult::default(),
            bootstrap: false,
            generation: 0,
            next_cursor: ReplicationScanCursor::default(),
            next_due_ms: None,
            has_more_due: false,
            job_count: 0,
            oldest_lag_ms: 0,
            singles: VecDeque::new(),
            relationships: HashMap::new(),
            succeeded: 0,
            failed: 0,
        }
    }

    fn call(&mut self, step: BatchStep, call: IoCall) -> BatchTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scanned(&mut self, scan: BlobJobScan) -> BatchTurn {
        let now_ms = self.started_ms.max(unix_timestamp_millis());
        self.scan_ms = now_ms.saturating_sub(self.started_ms);
        self.next_due_ms = scan.next_due_ms;
        self.has_more_due = self.repair.has_more || scan.has_more_due;
        self.generation = scan.generation;
        self.next_cursor = scan.next_cursor;
        self.job_count = scan.jobs.len();
        // Lag counts from the obligation, which a version's id dates, not from a retry's due
        // time.
        self.oldest_lag_ms = (scan.jobs.iter())
            .map(|(_, job)| now_ms.saturating_sub(job.owed_since_ms()))
            .chain(
                self.repair
                    .oldest_ms
                    .map(|owed| now_ms.saturating_sub(owed)),
            )
            .max()
            .unwrap_or(0);
        let (paged, singles): (Vec<_>, Vec<_>) =
            (scan.jobs.into_iter()).partition(|(_, job)| pageable(job));
        self.singles = singles.into();
        let deliver = DeliverPagesOperation::new(self.transport.clone(), paged);
        self.call(BatchStep::Deliver, IoCall::child(deliver))
    }

    fn delivered(&mut self, pages: PageReport) -> BatchTurn {
        if let Some(due_at_ms) = pages.next_due_ms {
            self.next_due_ms = min_due_at(self.next_due_ms, due_at_ms);
        }
        // A record held back for a later page is still due.
        self.has_more_due |= pages.later > 0;
        self.singles.extend(pages.single);
        let singles = self.singles.make_contiguous();
        singles.sort_by(|left, right| left.0.cmp(&right.0));
        // Jobs past the single bound stay due; the next drain continues after the last one sent.
        if self.singles.len() > REPLICATION_BATCH_SIZE {
            self.singles.truncate(REPLICATION_BATCH_SIZE);
            self.next_cursor.generation = self.generation;
            self.next_cursor.after = self.singles.back().map(|(key, _)| key.clone());
            self.has_more_due = true;
        }
        self.succeeded = pages.delivered;
        self.failed = pages.failed;
        let read = JobRelationshipsOperation::new(self.singles.make_contiguous());
        self.call(BatchStep::Relationships, IoCall::child(read))
    }

    fn next_job(&mut self) -> BatchTurn {
        let Some((key, job)) = self.singles.pop_front() else {
            let advance = AdvanceCursorOperation::new(std::mem::take(&mut self.next_cursor));
            return self.call(BatchStep::Cursor, IoCall::child(advance));
        };
        let relationship = job.relationship_id.and_then(|relationship_id| {
            let id = (job.input.bucket.clone(), relationship_id);
            self.relationships.get(&id).cloned()
        });
        let run = ProcessJobOperation::new(
            job.clone(),
            relationship,
            self.routing.clone(),
            self.serving,
        );
        self.call(BatchStep::Job(Box::new((key, job))), IoCall::child(run))
    }

    fn retire(&mut self, (key, job): Job, outcome: &BlobJobOutcome) -> BatchTurn {
        let retire = RetireJobOperation::new(key, job, outcome, unix_timestamp_millis());
        self.call(BatchStep::Retired, IoCall::child(retire))
    }

    fn reschedule(&mut self, (key, job): Job, error: String) -> BatchTurn {
        let (write, due) = reschedule_call(key, &job, error, unix_timestamp_millis())?;
        self.next_due_ms = min_due_at(self.next_due_ms, due);
        self.failed = self.failed.saturating_add(1);
        self.call(BatchStep::Rescheduled, write)
    }

    fn job_done(&mut self, job: Job, run: JobRun) -> BatchTurn {
        if let (Some(id), Some((relationship, stored))) = (job.1.relationship_id, run.cached) {
            let key = (job.1.input.bucket.clone(), id);
            match stored {
                true => self.relationships.insert(key, relationship),
                false => self.relationships.remove(&key),
            };
        }
        match run.result {
            Ok(outcome @ (BlobJobOutcome::Completed | BlobJobOutcome::Retired)) => {
                self.succeeded = self.succeeded.saturating_add(1);
                self.retire(job, &outcome)
            }
            // A member job ends only once delivered or no longer owed: a bootstrap counts a
            // deleted job as delivered, so a refusal retries instead.
            Ok(BlobJobOutcome::Failed) if job.1.distribution.is_some() => {
                self.reschedule(job, "the member did not take the version".to_string())
            }
            Ok(outcome @ BlobJobOutcome::Failed) => {
                self.failed = self.failed.saturating_add(1);
                self.retire(job, &outcome)
            }
            // No attempt, no error: the next unlock of a key makes the job due again.
            Ok(BlobJobOutcome::AwaitingKey(keys)) => {
                let park = ParkJobOperation::new(job.0, job.1, keys, unix_timestamp_millis());
                self.call(BatchStep::Parked, IoCall::child(park))
            }
            Err(error) => self.reschedule(job, error),
        }
    }

    fn finish(&mut self) -> BatchTurn {
        let now_ms = unix_timestamp_millis();
        if self.job_count > 0 || self.repair.processed > 0 {
            info!(
                event = "pipeline.blob_replication.summary",
                jobs = self.job_count,
                repaired_obligations = self.repair.processed,
                repaired_jobs = self.repair.queued,
                succeeded = self.succeeded,
                failed = self.failed,
                scan_ms = self.scan_ms,
                total_ms = now_ms.saturating_sub(self.started_ms),
                oldest_lag_ms = self.oldest_lag_ms,
                has_more_due = self.has_more_due,
                "Blob replication batch summary"
            );
        }
        let next = (self.next_due_ms).map(|due_at_ms| due_after(now_ms, due_at_ms));
        // A bootstrap waiting for its jobs or the publisher is looked at again soon.
        let next = match self.bootstrap {
            true => Some(next.map_or(BOOTSTRAP_POLL, |after| after.min(BOOTSTRAP_POLL))),
            false => next,
        };
        Ok(Next::Done(BlobDrainResult {
            processed: self.job_count,
            succeeded: self.succeeded,
            failed: self.failed,
            has_more_due: self.has_more_due,
            next_due_after: next.filter(|_| !self.has_more_due),
        }))
    }
}

impl<T: PageTransport> Operation for ProcessBatchOperation<T> {
    type Output = BlobDrainResult;
    type Error = BlobQueueError;

    fn first(&mut self) -> BatchTurn {
        self.started_ms = unix_timestamp_millis();
        Ok(Next::Call(
            IoCall::child(ProcessObligationsOperation::new()),
        ))
    }

    fn next(&mut self, result: IoResult) -> BatchTurn {
        match std::mem::replace(&mut self.step, BatchStep::Cursor) {
            BatchStep::Repair => {
                self.repair = result.child::<LiveRepairResult, BlobQueueError>()??;
                let bootstrap = ProcessBootstrapOperation::new(self.serving);
                self.call(BatchStep::Bootstrap, IoCall::child(bootstrap))
            }
            BatchStep::Bootstrap => {
                self.bootstrap = match result.child::<bool, BlobQueueError>()? {
                    Ok(waiting) => waiting,
                    Err(error) => {
                        warn!(%error, "Distributed bucket bootstrap failed; retrying");
                        true
                    }
                };
                let scan = ScanJobsOperation::new(unix_timestamp_millis(), PAGE_RECORDS);
                self.call(BatchStep::Scan, IoCall::child(scan))
            }
            BatchStep::Scan => {
                let scan = result.child::<BlobJobScan, BlobQueueError>()??;
                self.scanned(scan)
            }
            BatchStep::Deliver => {
                let pages = result.child::<PageReport, BlobQueueError>()??;
                self.delivered(pages)
            }
            BatchStep::Relationships => {
                self.relationships = result.child::<Relationships, BlobQueueError>()??;
                self.next_job()
            }
            BatchStep::Job(job) => {
                let run = result.child::<JobRun, BlobQueueError>()?;
                let run = run.unwrap_or_else(|error| JobRun {
                    result: Err(error.to_string()),
                    cached: None,
                });
                self.job_done(*job, run)
            }
            BatchStep::Retired => {
                result.child::<(), BlobQueueError>()??;
                self.next_job()
            }
            BatchStep::Rescheduled => {
                result.done()?;
                self.next_job()
            }
            BatchStep::Parked => {
                if let Some(due_at_ms) = result.child::<Option<u64>, BlobQueueError>()?? {
                    self.next_due_ms = min_due_at(self.next_due_ms, due_at_ms);
                }
                self.next_job()
            }
            BatchStep::Cursor => {
                result.child::<(), BlobQueueError>()??;
                self.finish()
            }
        }
    }
}

/// A source check that ended without a usable authorization, with the bucket's group if known.
pub(super) type SourceRefusal = (SourceAuthorizationError, Option<GroupId>);

/// Loads what the source node allows a job's caller, for the bucket's group.
#[derive(Debug, PartialEq)]
pub(super) struct LoadAuthOperation {
    auth_context: Option<AuthContext>,
    source_node_id: NodeId,
    bucket: String,
    group_id: Option<GroupId>,
}

impl LoadAuthOperation {
    pub(super) fn new(auth_context: AuthContext, source_node_id: NodeId, bucket: &str) -> Self {
        Self {
            auth_context: Some(auth_context),
            source_node_id,
            bucket: bucket.to_string(),
            group_id: None,
        }
    }
}

impl Operation for LoadAuthOperation {
    type Output = Result<SourceAuthorization, SourceRefusal>;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<Self::Output> {
        let read = GetBucketOperation::new(self.bucket.clone());
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        if let Some(group_id) = self.group_id {
            let loaded = result.child::<SourceAuthorization, SourceAuthorizationError>()?;
            return Ok(Next::Done(loaded.map_err(|error| (error, Some(group_id)))));
        }
        let info = match result.child::<BucketInfo, GetBucketError>()? {
            Ok(info) => info,
            Err(error) => {
                let error = SourceAuthorizationError::Unavailable(error.to_string());
                return Ok(Next::Done(Err((error, None))));
            }
        };
        self.group_id = Some(info.group_id);
        let auth = self.auth_context.take().ok_or(IoError::Unfinished)?;
        let load = SourceAuthOperation::new(auth, info.group_id, self.source_node_id);
        Ok(Next::Call(IoCall::child(load)))
    }
}

pub(super) fn mark_failure(relationship: &mut SyncRelationship, error: &str) {
    relationship.status.last_error = Some(error.to_string());
    relationship.status.counters.failures = relationship.status.counters.failures.saturating_add(1);
    relationship.status.counters.consecutive_failures = relationship
        .status
        .counters
        .consecutive_failures
        .saturating_add(1);
}

pub(super) fn mark_progress(relationship: &mut SyncRelationship, replicated: u64, bytes: u64) {
    relationship.status.last_synced_at = Some(SystemTime::now());
    relationship.status.counters.versions_synced = relationship
        .status
        .counters
        .versions_synced
        .saturating_add(replicated);
    relationship.status.counters.bytes_synced = relationship
        .status
        .counters
        .bytes_synced
        .saturating_add(bytes);
}

pub(super) fn mark_success(relationship: &mut SyncRelationship, replicated: u64, bytes: u64) {
    mark_progress(relationship, replicated, bytes);
    relationship.status.last_error = None;
    relationship.status.counters.consecutive_failures = 0;
}

/// The stable failure category of a scope-level error. Only a peer rejection
/// carries one through; everything else is retryable.
fn scope_failure(error: &ReplicateScopeError) -> ReplicationFailure {
    error.failure()
}

/// Stores a relationship's status and wakes the mirror repair. Outputs whether the status was
/// stored, or why it could not be.
#[derive(Debug, PartialEq)]
pub(super) struct StoreRelationshipOperation {
    relationship: Option<SyncRelationship>,
    stored: bool,
}

impl StoreRelationshipOperation {
    pub(super) fn new(relationship: SyncRelationship) -> Self {
        Self {
            relationship: Some(relationship),
            stored: false,
        }
    }
}

impl Operation for StoreRelationshipOperation {
    type Output = Result<bool, String>;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<Self::Output> {
        let relationship = self.relationship.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(StoreStatusOperation::new(
            relationship,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        if self.stored {
            // The repair also runs on its timer, so a failed kick costs only time.
            result.child::<(), SyncMirrorError>()?.ok();
            return Ok(Next::Done(Ok(true)));
        }
        match result.child::<bool, SyncMirrorError>()? {
            Ok(true) => {
                self.stored = true;
                Ok(Next::Call(IoCall::child(KickRepairOperation::new())))
            }
            Ok(false) => Ok(Next::Done(Ok(false))),
            Err(error) => Ok(Next::Done(Err(error.to_string()))),
        }
    }
}

/// The event of a finished or failed sync of `relationship`; `None` without a source bucket.
fn sync_event(
    relationship: &SyncRelationship,
    group_id: GroupId,
    versions_synced: u64,
    error: Option<&str>,
    now_ms: u64,
) -> Option<WatchEvent> {
    let bucket = relationship.source.bucket()?;
    let node_id = relationship.source.node_id;
    let (kind, detail) = match error {
        Some(error) => (
            WatchEventKind::SyncFailed,
            WatchEventDetail::SyncFailed {
                group_id,
                node_id,
                bucket: bucket.to_string(),
                relationship_id: relationship.id,
                error: error.to_string(),
            },
        ),
        None => (
            WatchEventKind::SyncCompleted,
            WatchEventDetail::SyncCompleted {
                group_id,
                node_id,
                bucket: bucket.to_string(),
                relationship_id: relationship.id,
                versions_synced,
            },
        ),
    };
    Some(WatchEvent {
        event_id: Ulid::generate(),
        realm_id: relationship.source.realm_id,
        kind,
        path: watch_resource_path(
            group_id,
            node_id,
            bucket,
            relationship.source.key_prefix().unwrap_or_default(),
        ),
        actor: relationship.created_by,
        occurred_at_ms: now_ms,
        detail,
    })
}

pub(super) fn job_source_auth<'a>(
    job: &'a BlobJobRecord,
    creator: &'a AuthContext,
) -> &'a AuthContext {
    if job.reference_advance.is_some()
        && job
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count == 0)
    {
        &job.input.auth_context
    } else {
        creator
    }
}

fn relationship_bucket<'a>(
    job: &BlobJobRecord,
    relationship: &'a SyncRelationship,
    relationship_id: Ulid,
) -> Result<&'a str, BlobJobOutcome> {
    if relationship.state != SyncState::Enabled {
        info!(
            %relationship_id,
            state = ?relationship.state,
            "Skipping replication job for disabled sync relationship"
        );
        return Err(BlobJobOutcome::Retired);
    }
    if job.reference_advance.is_some()
        && relationship.mode != SyncMode::Reference
        && relationship.reference_handling != ReferenceHandling::Preserve
    {
        return Err(BlobJobOutcome::Failed);
    }
    relationship.source.bucket().ok_or(BlobJobOutcome::Failed)
}

type JobResult = Result<BlobJobOutcome, String>;

/// How one job's run ended, and the relationship status it stored, with whether it was taken.
#[derive(Debug, PartialEq)]
struct JobRun {
    result: JobResult,
    cached: Option<(SyncRelationship, bool)>,
}

/// What follows a stored relationship status.
#[derive(Debug, PartialEq)]
enum AfterStore {
    Finish(JobResult),
    /// The failure path goes on after a refused plaintext copy failed the relationship.
    Failure(Option<ReplicationFailure>, String),
}

#[derive(Debug, PartialEq)]
enum JobStep {
    Quota,
    Gate,
    Member,
    RelationAuth,
    WriterAuth,
    Plaintext(Box<SourceAuthorization>),
    Scope,
    Store(Option<(u64, Option<String>)>, Box<AfterStore>),
    Emit(Box<AfterStore>),
}

/// Runs one replication job and keeps its relationship's status.
#[derive(Debug, PartialEq)]
struct ProcessJobOperation {
    job: BlobJobRecord,
    relationship: Option<SyncRelationship>,
    routing: NodeRouting,
    serving: Serving,
    now_ms: u64,
    step: JobStep,
    node_routing: Option<NodeRouting>,
    operation: Option<ReplicateScopeOperation>,
    watch_group: Option<GroupId>,
    creator: Option<AuthContext>,
    cached: Option<(SyncRelationship, bool)>,
}

type JobTurn = Turn<JobRun>;

impl ProcessJobOperation {
    fn new(
        job: BlobJobRecord,
        relationship: Option<SyncRelationship>,
        routing: NodeRouting,
        serving: Serving,
    ) -> Self {
        Self {
            job,
            relationship,
            routing,
            serving,
            now_ms: 0,
            step: JobStep::Quota,
            node_routing: None,
            operation: None,
            watch_group: None,
            creator: None,
            cached: None,
        }
    }

    fn call(&mut self, step: JobStep, call: IoCall) -> JobTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn finish(&mut self, result: JobResult) -> JobTurn {
        Ok(Next::Done(JobRun {
            result,
            cached: self.cached.take(),
        }))
    }

    fn local_node(&self) -> Result<NodeId, String> {
        (self.serving.map(|(node, _)| node))
            .ok_or_else(|| "source node identity unavailable".to_string())
    }

    fn gated(&mut self, gate: Option<GateContext>) -> JobTurn {
        let routing = self.node_routing.take().ok_or(IoError::Unfinished)?;
        let mut operation =
            ReplicateScopeOperation::new(self.job.input.clone()).with_routing(routing);
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        self.operation = Some(operation);
        if self.job.distribution.is_some() {
            // A member publication keeps no relationship status.
            self.relationship = None;
            if !matches!(self.job.input.target, ReplicateScopeTarget::Version { .. }) {
                return self.finish(Ok(BlobJobOutcome::Failed));
            }
            if let Err(error) = self.local_node() {
                return self.finish(Err(error));
            }
            let read = BoundRecordOperation::new(&self.job.input.bucket);
            return self.call(JobStep::Member, IoCall::child(read));
        }
        match self.job.relationship_id {
            Some(relationship_id) => self.load_relationship_auth(relationship_id),
            None => self.load_writer_auth(),
        }
    }

    /// Sends a distributed bucket's version to one member under the publication this node's
    /// record gives now. A job whose publication ended is done: the current coordinator owes it.
    fn record_read(&mut self, record: Option<BoundRecord>) -> JobTurn {
        let job = &self.job;
        let publication = (|| {
            let ReplicateScopeTarget::Version { key, .. } = &job.input.target else {
                return None;
            };
            let target: &MemberTarget = job.distribution.as_ref()?;
            let (binding, snapshot) = record.as_ref()?;
            member_publication(binding, snapshot, key, target)
                .filter(|(_, node_id)| *node_id == job.input.target_node_id)
        })();
        let Some((publication, _)) = publication else {
            info!(
                bucket = %job.input.bucket,
                "Dropping a publication this node no longer owes"
            );
            return self.finish(Ok(BlobJobOutcome::Retired));
        };
        let local = match self.local_node() {
            Ok(local) => local,
            Err(error) => return self.finish(Err(error)),
        };
        let operation = self.operation.take().ok_or(IoError::Unfinished)?;
        self.operation = Some(operation.with_distribution(publication, local));
        self.run_scope()
    }

    fn load_relationship_auth(&mut self, relationship_id: Ulid) -> JobTurn {
        let Some(relationship) = self.relationship.as_ref() else {
            info!(
                relationship_id = %relationship_id,
                "Skipping replication job for missing sync relationship"
            );
            return self.finish(Ok(BlobJobOutcome::Retired));
        };
        let bucket = match relationship_bucket(&self.job, relationship, relationship_id) {
            Ok(bucket) => bucket.to_string(),
            Err(outcome) => return self.finish(Ok(outcome)),
        };
        let creator = AuthContext {
            user_id: relationship.created_by,
            realm_id: relationship.source.realm_id,
            path_restrictions: None,
            session: None,
        };
        let source_auth = job_source_auth(&self.job, &creator).clone();
        let source_node = relationship.source.node_id;
        self.creator = Some(creator);
        let load = LoadAuthOperation::new(source_auth, source_node, &bucket);
        self.call(JobStep::RelationAuth, IoCall::child(load))
    }

    fn load_writer_auth(&mut self) -> JobTurn {
        let Some(writer) = self.job.writer_auth_context.clone() else {
            error!(
                bucket = %self.job.input.bucket,
                "Dropping replication job without durable source writer"
            );
            return self.finish(Ok(BlobJobOutcome::Failed));
        };
        let local = match self.local_node() {
            Ok(local) => local,
            Err(error) => return self.finish(Err(error)),
        };
        let load = LoadAuthOperation::new(writer, local, &self.job.input.bucket);
        self.call(JobStep::WriterAuth, IoCall::child(load))
    }

    fn relation_auth(&mut self, loaded: Result<SourceAuthorization, SourceRefusal>) -> JobTurn {
        let authorization = match loaded {
            Ok(authorization) => authorization,
            Err((SourceAuthorizationError::Denied, group_id)) => {
                if self.job.reference_advance.is_some() {
                    return self.finish(Ok(BlobJobOutcome::Failed));
                }
                self.watch_group = group_id;
                return self.access_denied();
            }
            Err((SourceAuthorizationError::Unavailable(error), group_id)) => {
                self.watch_group = group_id;
                if let Some(relationship) = self.relationship.as_mut() {
                    mark_failure(relationship, &error);
                }
                let watch = Some((0, Some(error.clone())));
                return self.store(watch, AfterStore::Finish(Err(error)));
            }
        };
        self.watch_group = Some(authorization.group_id());
        let Some(relationship) = self.relationship.as_ref() else {
            return self.finish(Ok(BlobJobOutcome::Retired));
        };
        let bucket = relationship.source.bucket().unwrap_or_default();
        let realm_id = self.job.input.auth_context.realm_id;
        let check = HolderCheckOperation::new(realm_id, bucket, relationship.created_by)
            .with_consent(relationship_consent(relationship.id));
        let step = JobStep::Plaintext(Box::new(authorization));
        self.call(step, IoCall::child(check))
    }

    fn writer_auth(&mut self, loaded: Result<SourceAuthorization, SourceRefusal>) -> JobTurn {
        let authorization = match loaded {
            Ok(authorization) => authorization,
            Err((SourceAuthorizationError::Denied, _)) => {
                return self.finish(Ok(BlobJobOutcome::Failed));
            }
            Err((SourceAuthorizationError::Unavailable(error), _)) => {
                return self.finish(Err(error));
            }
        };
        let Some(writer) = self.job.writer_auth_context.as_ref() else {
            return self.finish(Ok(BlobJobOutcome::Failed));
        };
        let consent = match blob_job_key(&self.job) {
            Ok(key) => job_consent(&key),
            Err(error) => return self.finish(Err(error.to_string())),
        };
        let realm_id = self.job.input.auth_context.realm_id;
        let check = HolderCheckOperation::new(realm_id, &self.job.input.bucket, writer.user_id)
            .with_consent(consent);
        let step = JobStep::Plaintext(Box::new(authorization));
        self.call(step, IoCall::child(check))
    }

    fn plaintext_known(&mut self, authorization: SourceAuthorization, plaintext: bool) -> JobTurn {
        let operation = self.operation.take().ok_or(IoError::Unfinished)?;
        let job = &self.job;
        let operation = match self
            .relationship
            .as_ref()
            .filter(|_| job.relationship_id.is_some())
        {
            Some(relationship) => {
                let writer = match job.reference_advance {
                    Some(_) => None,
                    None => job.writer_auth_context.clone().or(self.creator.take()),
                };
                let operation = operation
                    .with_plaintext(plaintext)
                    .with_relationship(
                        relationship.clone(),
                        job.origin.clone(),
                        job.upstream_sources.clone(),
                        writer,
                    )
                    .with_source_authorization(authorization);
                match job.reference_advance {
                    Some(advance) => operation.with_reference_advance(advance),
                    None => operation,
                }
            }
            None => {
                let writer = job.writer_auth_context.clone().ok_or(IoError::Unfinished)?;
                operation
                    .with_source_authorization(authorization)
                    .with_writer_auth(writer)
                    .with_plaintext(plaintext)
            }
        };
        self.operation = Some(operation);
        self.run_scope()
    }

    fn run_scope(&mut self) -> JobTurn {
        let operation = self.operation.take().ok_or(IoError::Unfinished)?;
        self.call(JobStep::Scope, IoCall::child(operation))
    }

    fn tracked(&self) -> bool {
        self.relationship.is_some() && self.job.relationship_id.is_some()
    }

    fn store(&mut self, watch: Option<(u64, Option<String>)>, after: AfterStore) -> JobTurn {
        let relationship = self.relationship.clone().filter(|_| self.tracked());
        let Some(relationship) = relationship else {
            return self.after_store(after);
        };
        let store = StoreRelationshipOperation::new(relationship);
        self.call(JobStep::Store(watch, Box::new(after)), IoCall::child(store))
    }

    fn stored(
        &mut self,
        watch: Option<(u64, Option<String>)>,
        after: AfterStore,
        stored: Result<bool, String>,
    ) -> JobTurn {
        let stored = match stored {
            Ok(stored) => stored,
            Err(error) => return self.finish(Err(error)),
        };
        let Some(relationship) = self.relationship.as_ref() else {
            return self.after_store(after);
        };
        self.cached = Some((relationship.clone(), stored));
        let event = watch.filter(|_| stored).zip(self.watch_group).and_then(
            |((versions, error), group_id)| {
                sync_event(
                    relationship,
                    group_id,
                    versions,
                    error.as_deref(),
                    self.now_ms,
                )
            },
        );
        match event {
            // Failures only warn inside the operation.
            Some(event) => {
                let emit = EmitEventOperation::new(event);
                self.call(JobStep::Emit(Box::new(after)), IoCall::child(emit))
            }
            None => self.after_store(after),
        }
    }

    fn after_store(&mut self, after: AfterStore) -> JobTurn {
        match after {
            AfterStore::Finish(result) => self.finish(result),
            AfterStore::Failure(failure, error) => self.after_failure(failure, error),
        }
    }

    fn access_denied(&mut self) -> JobTurn {
        if let Some(relationship) = self.relationship.as_mut() {
            relationship.state = SyncState::Failed {
                reason: "access_denied".to_string(),
            };
            mark_failure(relationship, "access_denied");
        }
        let watch = Some((0, Some("access_denied".to_string())));
        self.store(watch, AfterStore::Finish(Ok(BlobJobOutcome::Failed)))
    }

    fn scope_done(
        &mut self,
        outcome: Result<ReplicateScopeResult, ReplicateScopeError>,
    ) -> JobTurn {
        let result = match outcome {
            Ok(result) => result,
            Err(error) => return self.failure(Some(scope_failure(&error)), error.to_string()),
        };
        if result.failed == 0 && !result.awaiting.is_empty() {
            let outcome = Ok(BlobJobOutcome::AwaitingKey(result.awaiting));
            let progressed = (self.relationship.as_mut()).filter(|_| result.replicated > 0);
            let Some(relationship) = progressed else {
                return self.finish(outcome);
            };
            mark_progress(relationship, result.replicated, result.replicated_bytes);
            return self.store(None, AfterStore::Finish(outcome));
        }
        if result.failed == 0 {
            let outcome = AfterStore::Finish(Ok(BlobJobOutcome::Completed));
            if let Some(relationship) = self.relationship.as_mut() {
                mark_success(relationship, result.replicated, result.replicated_bytes);
            }
            return self.store(Some((result.replicated, None)), outcome);
        }
        let failure = result.failure;
        if failure.is_some_and(ReplicationFailure::is_writer_denied) {
            return self.finish(Ok(BlobJobOutcome::Failed));
        }
        let denied = failure.is_some_and(ReplicationFailure::is_access_denied);
        if self.job.reference_advance.is_some() && denied {
            return self.finish(Ok(BlobJobOutcome::Failed));
        }
        if result.replicated > 0
            && let Some(relationship) = self.relationship.as_mut()
        {
            mark_progress(relationship, result.replicated, result.replicated_bytes);
        }
        let counts = format!(
            "replication completed with {} replicated, {} skipped, {} failed",
            result.replicated, result.skipped, result.failed
        );
        let error = match result.last_error.as_deref() {
            Some(last_error) => format!("{counts}: {last_error}"),
            None => counts,
        };
        if denied && self.relationship.is_some() {
            return self.access_denied();
        }
        self.failure(failure, error)
    }

    /// A refused plaintext copy fails the relationship until it is changed and run again.
    fn failure(&mut self, failure: Option<ReplicationFailure>, error: String) -> JobTurn {
        if failure == Some(ReplicationFailure::PlaintextRefused)
            && let Some(relationship) = self.relationship.as_mut()
        {
            relationship.state = SyncState::Failed {
                reason: "plaintext_required".to_string(),
            };
            mark_failure(relationship, &error);
            let watch = Some((0, Some(error.clone())));
            return self.store(watch, AfterStore::Failure(failure, error));
        }
        self.after_failure(failure, error)
    }

    fn after_failure(&mut self, failure: Option<ReplicationFailure>, error: String) -> JobTurn {
        if failure.is_some_and(ReplicationFailure::is_denied) {
            return self.finish(Ok(BlobJobOutcome::Failed));
        }
        let Some(relationship) = self.relationship.as_mut() else {
            return self.finish(Err(error));
        };
        mark_failure(relationship, &error);
        let watch = Some((0, Some(error.clone())));
        self.store(watch, AfterStore::Finish(Err(error)))
    }
}

impl Operation for ProcessJobOperation {
    type Output = JobRun;
    type Error = BlobQueueError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> JobTurn {
        if self.job.reference_advance.is_some() && self.job.relationship_id.is_none() {
            return self.finish(Ok(BlobJobOutcome::Failed));
        }
        let quota = QuotaOperation::new(self.routing.clone());
        Ok(Next::Call(IoCall::child(quota)))
    }

    fn next(&mut self, result: IoResult) -> JobTurn {
        match std::mem::replace(&mut self.step, JobStep::Scope) {
            JobStep::Quota => match result.child::<NodeRouting, RoutingInputsError>()? {
                Ok(routing) => {
                    self.node_routing = Some(routing);
                    // Reference materialization writes real bytes on this node, so the job
                    // carries this node's destination details; a node mid-transition refuses.
                    let realm_id = self.job.input.auth_context.realm_id;
                    let gate = GateOperation::new(realm_id, self.now_ms);
                    self.call(JobStep::Gate, IoCall::child(gate))
                }
                // Retrying a record that will never decode only pins the queue at its backoff
                // ceiling; the drain repair re-enqueues once it is repaired.
                Err(error) if error.storage().is_none() => {
                    error!(error = %error, "Dropping blob replication job with undecodable routing inputs");
                    self.finish(Ok(BlobJobOutcome::Failed))
                }
                Err(error) => self.finish(Err(error.to_string())),
            },
            JobStep::Gate => match result.child::<Option<GateContext>, GateContextError>()? {
                Ok(gate) => self.gated(gate),
                Err(error) => self.finish(Err(error.to_string())),
            },
            JobStep::Member => match result.child::<Option<BoundRecord>, RecordError>()? {
                Ok(record) => self.record_read(record),
                Err(error) => self.finish(Err(error.to_string())),
            },
            JobStep::RelationAuth => {
                let loaded = result
                    .child::<Result<SourceAuthorization, SourceRefusal>, BlobQueueError>()??;
                self.relation_auth(loaded)
            }
            JobStep::WriterAuth => {
                let loaded = result
                    .child::<Result<SourceAuthorization, SourceRefusal>, BlobQueueError>()??;
                self.writer_auth(loaded)
            }
            JobStep::Plaintext(authorization) => match result.child::<bool, PlaintextError>()? {
                Ok(plaintext) => self.plaintext_known(*authorization, plaintext),
                Err(error) => self.finish(Err(error.to_string())),
            },
            JobStep::Scope => self.scope_done(result.child()?),
            JobStep::Store(watch, after) => {
                let stored = result.child::<Result<bool, String>, BlobQueueError>()??;
                self.stored(watch, *after, stored)
            }
            JobStep::Emit(after) => {
                result.child::<(), IoError>()?.ok();
                self.after_store(*after)
            }
        }
    }
}

/// Reads the relationships the jobs name, each once.
#[derive(Debug, PartialEq)]
pub(super) struct JobRelationshipsOperation {
    keys: Vec<(String, Ulid)>,
}

impl JobRelationshipsOperation {
    pub(super) fn new(jobs: &[Job]) -> Self {
        let mut requested = HashSet::new();
        let mut keys = Vec::new();
        for (_, job) in jobs {
            let Some(relationship_id) = job.relationship_id else {
                continue;
            };
            let identity = (job.input.bucket.clone(), relationship_id);
            if requested.insert(identity.clone()) {
                keys.push(identity);
            }
        }
        Self { keys }
    }
}

impl Operation for JobRelationshipsOperation {
    type Output = Relationships;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<Relationships> {
        if self.keys.is_empty() {
            return Ok(Next::Done(HashMap::new()));
        }
        let reads = (self.keys.iter())
            .map(|(bucket, relationship_id)| {
                let key = sync_relationship_key(bucket, *relationship_id).into();
                (RELATIONSHIP_OUT_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Relationships> {
        let values = result.values()?;
        if values.len() != self.keys.len() {
            return Err(BlobQueueError::UnexpectedEvent(
                "blob replication relationship read count mismatch".to_string(),
            ));
        }
        let mut relationships = HashMap::with_capacity(self.keys.len());
        for ((bucket, relationship_id), value) in
            std::mem::take(&mut self.keys).into_iter().zip(values)
        {
            let Some(value) = value else {
                continue;
            };
            let key = sync_relationship_key(&bucket, relationship_id);
            let relationship = SyncRelationship::from_bytes(&value)?;
            validate_sync_key(&bucket, &ByteView::from(key), &relationship)?;
            if relationship.id != relationship_id {
                return Err(ConversionError::FromStrError(
                    "sync relationship id does not match requested id".to_string(),
                )
                .into());
            }
            relationships.insert((bucket, relationship_id), relationship);
        }
        Ok(Next::Done(relationships))
    }
}

fn cursor_newer(candidate: &ReplicationScanCursor, current: &ReplicationScanCursor) -> bool {
    if candidate.generation != current.generation {
        return candidate.generation > current.generation;
    }
    match (&candidate.after, &current.after) {
        (Some(candidate), Some(current)) => candidate > current,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

fn cursor_merge(
    candidate: ReplicationScanCursor,
    current: ReplicationScanCursor,
) -> ReplicationScanCursor {
    if cursor_newer(&candidate, &current) {
        candidate
    } else {
        ReplicationScanCursor {
            next_due_ms: match (candidate.next_due_ms, current.next_due_ms) {
                (Some(candidate), Some(current)) => Some(candidate.min(current)),
                (Some(candidate), None) => Some(candidate),
                (None, Some(current)) => Some(current),
                (None, None) => None,
            },
            ..current
        }
    }
}

/// Moves the stored scan cursor forward; an older candidate only lowers its next due time.
#[derive(Debug, PartialEq)]
pub(super) struct AdvanceCursorOperation {
    candidate: ReplicationScanCursor,
    writing: bool,
}

impl AdvanceCursorOperation {
    pub(super) fn new(candidate: ReplicationScanCursor) -> Self {
        Self {
            candidate,
            writing: false,
        }
    }
}

impl Operation for AdvanceCursorOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(cursor_read()))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let (current, valid) = match result.value()? {
            None => (ReplicationScanCursor::default(), true),
            Some(value) => match postcard::from_bytes(value.as_ref()) {
                Ok(cursor) => (cursor, true),
                Err(_) => (ReplicationScanCursor::default(), false),
            },
        };
        let merged = cursor_merge(std::mem::take(&mut self.candidate), current.clone());
        if valid && merged == current {
            return Ok(Next::Done(()));
        }
        let value = postcard::to_allocvec(&merged).map_err(ConversionError::from)?;
        self.writing = true;
        let row = (NODE_STATE_KEYSPACE.to_string(), cursor_key(), value.into());
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// A job found under a key that is not its own, while its canonical row is looked at.
#[derive(Debug, PartialEq)]
struct RepairRow {
    key: Vec<u8>,
    job: BlobJobRecord,
    canonical: Vec<u8>,
}

#[derive(Debug, PartialEq)]
enum ScanStep {
    Cursor,
    Page,
    Dropped,
    Canonical(Box<RepairRow>),
    /// The repaired row is written; the old key goes next.
    Written(Box<RepairRow>),
    /// The old key is gone; the job goes on under its canonical key.
    Moved(Box<(Vec<u8>, BlobJobRecord)>),
    Refreshed(Box<(Vec<u8>, BlobJobRecord)>),
}

/// Reads one page of the job queue after the stored cursor, repairs rows under foreign keys and
/// collects the jobs due now.
#[derive(Debug, PartialEq)]
pub(super) struct ScanJobsOperation {
    now_ms: u64,
    limit: usize,
    step: ScanStep,
    cursor: ReplicationScanCursor,
    rows: VecDeque<(Key, Value)>,
    next_start: Option<Key>,
    page_empty: bool,
    last_key: Option<Vec<u8>>,
    jobs: Vec<Job>,
    next_due_ms: Option<u64>,
    canonical_changed: bool,
    /// The scan meets each key once; only after a key repair may a job already be taken.
    repaired: bool,
}

impl ScanJobsOperation {
    pub(super) fn new(now_ms: u64, limit: usize) -> Self {
        Self {
            now_ms,
            limit,
            step: ScanStep::Cursor,
            cursor: ReplicationScanCursor::default(),
            rows: VecDeque::new(),
            next_start: None,
            page_empty: false,
            last_key: None,
            jobs: Vec::new(),
            next_due_ms: None,
            canonical_changed: false,
            repaired: false,
        }
    }

    fn call(&mut self, step: ScanStep, call: IoCall) -> Turn<BlobJobScan> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn delete_call(key: Vec<u8>) -> IoCall {
        let row = (REPLICATION_JOB_KEYSPACE.to_string(), ByteView::from(key));
        IoCall::Delete(vec![row])
    }

    fn scan(
        &mut self,
        has_more_due: bool,
        next_cursor: ReplicationScanCursor,
    ) -> Turn<BlobJobScan> {
        Ok(Next::Done(BlobJobScan {
            next_cursor,
            generation: self.cursor.generation,
            jobs: std::mem::take(&mut self.jobs),
            has_more_due,
            next_due_ms: self.next_due_ms,
        }))
    }

    fn next_row(&mut self) -> Turn<BlobJobScan> {
        while let Some((key, value)) = self.rows.pop_front() {
            let key = key.to_vec();
            self.last_key = Some(key.clone());
            let job = match BlobJobRecord::from_bytes(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Deleting malformed blob replication job");
                    return self.call(ScanStep::Dropped, Self::delete_call(key));
                }
            };
            let canonical = blob_job_key(&job)?.to_vec();
            if canonical != key {
                self.repaired = true;
                warn!(key = ?key, "Repairing blob replication job stored under non-canonical key");
                let read = read_row(REPLICATION_JOB_KEYSPACE, canonical.clone());
                let row = RepairRow {
                    key,
                    job,
                    canonical,
                };
                return self.call(ScanStep::Canonical(Box::new(row)), read);
            }
            if self.canonical_changed {
                let read = read_row(REPLICATION_JOB_KEYSPACE, canonical);
                return self.call(ScanStep::Refreshed(Box::new((key, job))), read);
            }
            if let Some(done) = self.take_job(key, job) {
                return done;
            }
        }
        if let Some(next) = self.next_start.take() {
            let cursor = ReplicationScanCursor {
                generation: self.cursor.generation,
                after: Some(next.to_vec()),
                next_due_ms: self.next_due_ms,
            };
            return self.scan(true, cursor);
        }
        let generation = match self.page_empty && self.cursor.after.is_none() {
            true => self.cursor.generation,
            false => self.cursor.generation.saturating_add(1),
        };
        let cursor = ReplicationScanCursor {
            generation,
            after: None,
            next_due_ms: self.next_due_ms,
        };
        self.scan(false, cursor)
    }

    /// Notes a job that is not due, or takes a due one; a full batch ends the scan.
    fn take_job(&mut self, key: Vec<u8>, job: BlobJobRecord) -> Option<Turn<BlobJobScan>> {
        if job.due_at_ms == PARKED_DUE {
            return None;
        }
        if job.due_at_ms > self.now_ms {
            self.next_due_ms = min_due_at(self.next_due_ms, job.due_at_ms);
            return None;
        }
        if !merge_due_job(&mut self.jobs, key, job, (self.repaired, self.limit)) {
            return None;
        }
        let cursor = ReplicationScanCursor {
            generation: self.cursor.generation,
            after: self.last_key.take(),
            next_due_ms: self.next_due_ms,
        };
        Some(self.scan(true, cursor))
    }

    fn continue_with(&mut self, key: Vec<u8>, job: BlobJobRecord) -> Turn<BlobJobScan> {
        match self.take_job(key, job) {
            Some(done) => done,
            None => self.next_row(),
        }
    }

    fn canonical_read(&mut self, row: RepairRow, value: Option<Value>) -> Turn<BlobJobScan> {
        let existing = (value.as_deref())
            .map(BlobJobRecord::from_bytes)
            .transpose()?;
        if let Some(existing) = existing.filter(|existing| blob_job_preferred(existing, &row.job)) {
            let delete = Self::delete_call(row.key);
            return self.call(ScanStep::Moved(Box::new((row.canonical, existing))), delete);
        }
        let write = blob_job_entry(&row.job)?;
        self.call(ScanStep::Written(Box::new(row)), IoCall::Write(vec![write]))
    }
}

impl Operation for ScanJobsOperation {
    type Output = BlobJobScan;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<BlobJobScan> {
        Ok(Next::Call(cursor_read()))
    }

    fn next(&mut self, result: IoResult) -> Turn<BlobJobScan> {
        match std::mem::replace(&mut self.step, ScanStep::Dropped) {
            ScanStep::Cursor => {
                self.cursor = (result.value()?)
                    .and_then(|value| postcard::from_bytes(value.as_ref()).ok())
                    .unwrap_or_default();
                // A pass from the start drops the last pass's due time, which may be past.
                let from_start = self.cursor.after.is_none();
                self.next_due_ms = self.cursor.next_due_ms.filter(|_| !from_start);
                let page = IoCall::Page {
                    space: REPLICATION_JOB_KEYSPACE.to_string(),
                    prefix: Key::from(Vec::new()),
                    after: self.cursor.after.clone().map(ByteView::from),
                    limit: PAGE_RECORDS,
                };
                self.call(ScanStep::Page, page)
            }
            ScanStep::Page => {
                let (rows, next) = result.page()?;
                self.page_empty = rows.is_empty();
                self.rows = rows.into();
                self.next_start = next;
                self.next_row()
            }
            ScanStep::Dropped => {
                result.done()?;
                self.next_row()
            }
            ScanStep::Canonical(row) => self.canonical_read(*row, result.value()?),
            ScanStep::Written(row) => {
                result.done()?;
                let RepairRow {
                    key,
                    job,
                    canonical,
                } = *row;
                self.canonical_changed = true;
                self.jobs.retain(|(existing, _)| *existing != canonical);
                let delete = Self::delete_call(key);
                self.call(ScanStep::Moved(Box::new((canonical, job))), delete)
            }
            ScanStep::Moved(moved) => {
                result.done()?;
                let (key, job) = *moved;
                self.continue_with(key, job)
            }
            ScanStep::Refreshed(row) => {
                let (key, job) = *row;
                let job = match result.value()? {
                    Some(value) => BlobJobRecord::from_bytes(&value)?,
                    None => job,
                };
                self.continue_with(key, job)
            }
        }
    }
}

/// The write that puts a failed job back with a backoff, and when it is due.
pub(super) fn reschedule_call(
    key: Vec<u8>,
    job: &BlobJobRecord,
    error: String,
    now_ms: u64,
) -> Result<(IoCall, u64), BlobQueueError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = now_ms.saturating_add(retry_delay_ms(attempts));
    let next_job = BlobJobRecord {
        input: job.input.clone(),
        source_delete_marker: job.source_delete_marker,
        due_at_ms,
        attempts,
        last_error: Some(error),
        relationship_id: job.relationship_id,
        enqueued_at_ms: job.enqueued_at_ms,
        origin: job.origin.clone(),
        upstream_sources: job.upstream_sources.clone(),
        writer_auth_context: job.writer_auth_context.clone(),
        reference_advance: job.reference_advance,
        distribution: job.distribution.clone(),
        job_id: job.job_id,
    };
    let row = (
        REPLICATION_JOB_KEYSPACE.to_string(),
        ByteView::from(key),
        ByteView::from(next_job.to_bytes()?),
    );
    Ok((IoCall::Write(vec![row]), due_at_ms))
}

#[cfg(test)]
mod step_tests {
    use super::super::page::deliver::MetadataPages;
    use super::*;

    fn empty_page() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    #[test]
    fn empty_queue_ends() {
        let mut scan = ScanJobsOperation::new(5, 1);
        assert!(matches!(scan.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = scan.next(IoResult::Values(vec![None]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let Ok(Next::Done(found)) = scan.next(empty_page()) else {
            panic!("an empty queue ends the scan");
        };
        assert!(found.jobs.is_empty() && !found.has_more_due);
        assert_eq!(found.next_cursor.generation, 0);
    }

    #[test]
    fn unchanged_cursor_skipped() {
        let mut advance = AdvanceCursorOperation::new(ReplicationScanCursor::default());
        assert!(matches!(advance.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = IoResult::Values(vec![None]);
        assert_eq!(advance.next(answer), Ok(Next::Done(())));
    }

    #[test]
    fn newer_cursor_written() {
        let candidate = ReplicationScanCursor {
            generation: 1,
            ..ReplicationScanCursor::default()
        };
        let mut advance = AdvanceCursorOperation::new(candidate);
        advance.first().unwrap();
        let turn = advance.next(IoResult::Values(vec![None]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        assert_eq!(advance.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn jobless_reads_nothing() {
        let mut read = JobRelationshipsOperation::new(&[]);
        assert_eq!(read.first(), Ok(Next::Done(HashMap::new())));
    }

    #[test]
    fn batch_starts_obligations() {
        let routing = NodeRouting::default();
        let mut batch = ProcessBatchOperation::new(MetadataPages, routing, None);
        assert!(matches!(batch.first(), Ok(Next::Call(IoCall::Child(_)))));
    }
}
