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
use crate::tasks::queue_backoff::{due_after, min_due_at, retry_delay_ms};
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

fn failed(error: impl std::fmt::Display) -> BlobQueueError {
    BlobQueueError::UnexpectedEvent(error.to_string())
}

/// Sets the drain's timer from the queue when the node starts.
#[derive(Debug, Default, PartialEq)]
pub struct RestoreBlobTimerOperation {
    setting: bool,
}

impl Operation for RestoreBlobTimerOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::child(NextBlobTimerOperation::new())))
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
pub struct NextBlobTimerOperation {
    now_ms: u64,
    step: TimerStep,
}

impl NextBlobTimerOperation {
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

impl Default for NextBlobTimerOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for NextBlobTimerOperation {
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
pub struct ProcessBlobBatchOperation<T> {
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

impl<T: PageTransport> ProcessBlobBatchOperation<T> {
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
                let park = ParkJobOperation::new(job_key, job, keys, unix_timestamp_millis());
                if let Some(due_at_ms) = drive(park, context).await? {
                    next_due_ms = min_due_at(next_due_ms, due_at_ms);
                }
            }
            Err(error) => {
                let retry_due_at =
                    reschedule_blob_job(&context.storage_handle, job_key, &job, error).await?;
                next_due_ms = min_due_at(next_due_ms, retry_due_at);
                failed = failed.saturating_add(1);
            }
        }
    }

    advance_replication_cursor(&context.storage_handle, next_cursor).await?;

    if job_count > 0 || repair.processed > 0 {
        info!(
            event = "pipeline.blob_replication.summary",
            jobs = job_count,
            repaired_obligations = repair.processed,
            repaired_jobs = repair.queued,
            succeeded,
            failed,
            scan_ms = duration_ms(scan_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Blob replication batch summary"
        );
    }

    Ok(BlobDrainResult {
        processed: job_count,
        succeeded,
        failed,
        has_more_due,
        next_due_after: if has_more_due {
            None
        } else {
            let next = next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms));
            // A bootstrap waiting for its jobs or the publisher is looked at again soon.
            match bootstrap {
                true => Some(next.map_or(BOOTSTRAP_POLL, |after| after.min(BOOTSTRAP_POLL))),
                false => next,
            }
        },
    })
}

pub(super) async fn load_source_authorization(
    context: &DriverContext,
    auth_context: AuthContext,
    source_node_id: NodeId,
    bucket: &str,
) -> Result<SourceAuthorization, (SourceAuthorizationError, Option<GroupId>)> {
    let bucket_info = match drive(GetBucketOperation::new(bucket.to_string()), context).await {
        Ok(bucket_info) => bucket_info,
        Err(error) => {
            return Err((
                SourceAuthorizationError::Unavailable(error.to_string()),
                None,
            ));
        }
    };
    let group_id = bucket_info.group_id;
    drive(
        crate::replication::source_rules::SourceAuthOperation::new(
            auth_context,
            group_id,
            source_node_id,
        ),
        context,
    )
    .await
    .map_err(|error| (error, Some(group_id)))
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

pub(super) async fn store_relationship(
    context: &DriverContext,
    relationship: SyncRelationship,
) -> Result<bool, String> {
    let stored = drive(StoreStatusOperation::new(relationship.clone()), context)
        .await
        .map_err(|error| error.to_string())?;
    if stored {
        let _ = drive(KickRepairOperation::new(), context).await;
    }
    Ok(stored)
}

async fn emit_sync_watch(
    context: &DriverContext,
    relationship: &SyncRelationship,
    group_id: GroupId,
    versions_synced: u64,
    error: Option<&str>,
) {
    let Some(bucket) = relationship.source.bucket() else {
        return;
    };
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
    let emit = EmitEventOperation::new(WatchEvent {
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
        occurred_at_ms: unix_timestamp_millis(),
        detail,
    });
    // Failures only warn inside the operation.
    let _ = drive(emit, context).await;
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

async fn process_blob_job(
    context: &DriverContext,
    job: &BlobJobRecord,
    stored_relationship: Option<SyncRelationship>,
    relationships: &mut HashMap<(String, Ulid), SyncRelationship>,
) -> Result<BlobJobOutcome, String> {
    if job.reference_advance.is_some() && job.relationship_id.is_none() {
        return Ok(BlobJobOutcome::Failed);
    }
    let routing = match crate::driver::drive(
        crate::groups::routing::QuotaOperation::new(crate::driver::node_routing(context)),
        context,
    )
    .await
    {
        Ok(routing) => routing,
        // Retrying a record that will never decode only pins the queue at its
        // backoff ceiling; the drain repair re-enqueues once it is repaired.
        Err(error) if error.storage().is_none() => {
            error!(error = %error, "Dropping blob replication job with undecodable routing inputs");
            return Ok(BlobJobOutcome::Failed);
        }
        Err(error) => return Err(error.to_string()),
    };
    // Reference materialization writes real bytes on this node, so the job
    // carries this node's destination details; a node mid-transition refuses.
    let gate = match crate::driver::drive(
        crate::placement::policy::GateOperation::new(
            job.input.auth_context.realm_id,
            aruna_core::time::unix_timestamp_millis(),
        ),
        context,
    )
    .await
    {
        Ok(gate) => gate,
        Err(error) => return Err(error.to_string()),
    };
    let mut operation = ReplicateScopeOperation::new(job.input.clone()).with_routing(routing);
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    if let Some(target) = job.distribution.as_ref() {
        return publish_member(context, job, target, operation, relationships).await;
    }
    let mut watch_group_id = None;
    let relationship = if let Some(relationship_id) = job.relationship_id {
        let Some(relationship) = stored_relationship else {
            info!(
                relationship_id = %relationship_id,
                "Skipping replication job for missing sync relationship"
            );
            return Ok(BlobJobOutcome::Retired);
        };
        let bucket = match relationship_bucket(job, &relationship, relationship_id) {
            Ok(bucket) => bucket,
            Err(outcome) => return Ok(outcome),
        };
        let creator = AuthContext {
            user_id: relationship.created_by,
            realm_id: relationship.source.realm_id,
            path_restrictions: None,
            session: None,
        };
        let source_auth_context = job_source_auth(job, &creator).clone();
        let source_authorization = match load_source_authorization(
            context,
            source_auth_context,
            relationship.source.node_id,
            bucket,
        )
        .await
        {
            Ok(authorization) => {
                watch_group_id = Some(authorization.group_id());
                authorization
            }
            Err((SourceAuthorizationError::Denied, group_id)) => {
                if job.reference_advance.is_some() {
                    return Ok(BlobJobOutcome::Failed);
                }
                let mut relationship = relationship;
                relationship.state = SyncState::Failed {
                    reason: "access_denied".to_string(),
                };
                mark_failure(&mut relationship, "access_denied");
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, &relationship, stored);
                if stored && let Some(group_id) = group_id {
                    emit_sync_watch(context, &relationship, group_id, 0, Some("access_denied"))
                        .await;
                }
                return Ok(BlobJobOutcome::Failed);
            }
            Err((SourceAuthorizationError::Unavailable(error), group_id)) => {
                let mut relationship = relationship;
                mark_failure(&mut relationship, &error);
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, &relationship, stored);
                if stored && let Some(group_id) = group_id {
                    emit_sync_watch(context, &relationship, group_id, 0, Some(&error)).await;
                }
                return Err(error);
            }
        };
        let writer_auth_context = if job.reference_advance.is_some() {
            None
        } else {
            job.writer_auth_context.clone().or(Some(creator))
        };
        let consent = super::plaintext::relationship_consent(relationship.id);
        let realm_id = job.input.auth_context.realm_id;
        let plaintext = HolderCheckOperation::new(realm_id, bucket, relationship.created_by)
            .with_consent(consent);
        let plaintext = drive(plaintext, context)
            .await
            .map_err(|error| error.to_string())?;
        operation = operation
            .with_plaintext(plaintext)
            .with_relationship(
                relationship.clone(),
                job.origin.clone(),
                job.upstream_sources.clone(),
                writer_auth_context,
            )
            .with_source_authorization(source_authorization);
        if let Some(advance) = job.reference_advance {
            operation = operation.with_reference_advance(advance);
        }
        Some(relationship)
    } else {
        let Some(writer) = job.writer_auth_context.as_ref() else {
            error!(
                bucket = %job.input.bucket,
                "Dropping replication job without durable source writer"
            );
            return Ok(BlobJobOutcome::Failed);
        };
        let Some(source_node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
            return Err("source node identity unavailable".to_string());
        };
        let source_authorization = match load_source_authorization(
            context,
            writer.clone(),
            source_node_id,
            &job.input.bucket,
        )
        .await
        {
            Ok(authorization) => authorization,
            Err((SourceAuthorizationError::Denied, _)) => {
                return Ok(BlobJobOutcome::Failed);
            }
            Err((SourceAuthorizationError::Unavailable(error), _)) => return Err(error),
        };
        let consent = job_consent(&blob_job_key(job).map_err(|error| error.to_string())?);
        let (realm_id, bucket) = (job.input.auth_context.realm_id, &job.input.bucket);
        let plaintext =
            HolderCheckOperation::new(realm_id, bucket, writer.user_id).with_consent(consent);
        let plaintext = drive(plaintext, context)
            .await
            .map_err(|error| error.to_string())?;
        operation = operation
            .with_source_authorization(source_authorization)
            .with_writer_auth(writer.clone())
            .with_plaintext(plaintext);
        None
    };

    finish_blob_job(
        context,
        job,
        operation,
        relationship,
        watch_group_id,
        relationships,
    )
    .await
}

/// Sends a distributed bucket's version to one member under the publication this node's
/// record gives now. A job whose publication ended is done: the current coordinator owes it.
async fn publish_member(
    context: &DriverContext,
    job: &BlobJobRecord,
    target: &MemberTarget,
    operation: ReplicateScopeOperation,
    relationships: &mut HashMap<(String, Ulid), SyncRelationship>,
) -> Result<BlobJobOutcome, String> {
    let ReplicateScopeTarget::Version { key, .. } = &job.input.target else {
        return Ok(BlobJobOutcome::Failed);
    };
    let Some(local) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return Err("source node identity unavailable".to_string());
    };
    let publication = drive(BoundRecordOperation::new(&job.input.bucket), context)
        .await
        .map_err(|error| error.to_string())?
        .and_then(|(binding, snapshot)| member_publication(&binding, &snapshot, key, target))
        .filter(|(_, node_id)| *node_id == job.input.target_node_id);
    let Some((publication, _)) = publication else {
        info!(
            bucket = %job.input.bucket,
            member_id = %target.member_id,
            "Dropping a publication this node no longer owes"
        );
        return Ok(BlobJobOutcome::Retired);
    };
    let operation = operation.with_distribution(publication, local);
    finish_blob_job(context, job, operation, None, None, relationships).await
}

async fn finish_blob_job(
    context: &DriverContext,
    job: &BlobJobRecord,
    operation: ReplicateScopeOperation,
    mut relationship: Option<SyncRelationship>,
    watch_group_id: Option<GroupId>,
    relationships: &mut HashMap<(String, Ulid), SyncRelationship>,
) -> Result<BlobJobOutcome, String> {
    let failure: Option<ReplicationFailure>;
    let error = match drive(operation, context).await {
        Ok(result) if result.failed == 0 && !result.awaiting.is_empty() => {
            if result.replicated > 0
                && let Some(relationship) = relationship.as_mut()
            {
                mark_progress(relationship, result.replicated, result.replicated_bytes);
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, relationship, stored);
            }
            return Ok(BlobJobOutcome::AwaitingKey(result.awaiting));
        }
        Ok(result) if result.failed == 0 => {
            if let Some(relationship) = relationship.as_mut() {
                mark_success(relationship, result.replicated, result.replicated_bytes);
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, relationship, stored);
                if stored && let Some(group_id) = watch_group_id {
                    emit_sync_watch(context, relationship, group_id, result.replicated, None).await;
                }
            }
            return Ok(BlobJobOutcome::Completed);
        }
        Ok(result) => {
            failure = result.failure;
            if failure.is_some_and(ReplicationFailure::is_writer_denied) {
                return Ok(BlobJobOutcome::Failed);
            }
            if job.reference_advance.is_some()
                && failure.is_some_and(ReplicationFailure::is_access_denied)
            {
                return Ok(BlobJobOutcome::Failed);
            }
            if result.replicated > 0
                && let Some(relationship) = relationship.as_mut()
            {
                mark_progress(relationship, result.replicated, result.replicated_bytes);
            }
            let error = match result.last_error.as_deref() {
                Some(last_error) => format!(
                    "replication completed with {} replicated, {} skipped, {} failed: {}",
                    result.replicated, result.skipped, result.failed, last_error
                ),
                None => format!(
                    "replication completed with {} replicated, {} skipped, {} failed",
                    result.replicated, result.skipped, result.failed
                ),
            };
            if failure.is_some_and(ReplicationFailure::is_access_denied)
                && let Some(relationship) = relationship.as_mut()
            {
                relationship.state = SyncState::Failed {
                    reason: "access_denied".to_string(),
                };
                mark_failure(relationship, "access_denied");
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, relationship, stored);
                if stored && let Some(group_id) = watch_group_id {
                    emit_sync_watch(context, relationship, group_id, 0, Some("access_denied"))
                        .await;
                }
                return Ok(BlobJobOutcome::Failed);
            }
            error
        }
        Err(error) => {
            failure = Some(scope_failure(&error));
            error.to_string()
        }
    };
    // A refused plaintext copy fails the relationship until it is changed and run again.
    if failure == Some(ReplicationFailure::PlaintextRefused)
        && let Some(relationship) = relationship.as_mut()
    {
        relationship.state = SyncState::Failed {
            reason: "plaintext_required".to_string(),
        };
        mark_failure(relationship, &error);
        let stored = store_relationship(context, relationship.clone()).await?;
        cache_relationship(relationships, job, relationship, stored);
        if stored && let Some(group_id) = watch_group_id {
            emit_sync_watch(context, relationship, group_id, 0, Some(&error)).await;
        }
    }
    if failure.is_some_and(ReplicationFailure::is_denied) {
        return Ok(BlobJobOutcome::Failed);
    }
    if let Some(relationship) = relationship.as_mut() {
        mark_failure(relationship, &error);
        let stored = store_relationship(context, relationship.clone()).await?;
        cache_relationship(relationships, job, relationship, stored);
        if stored && let Some(group_id) = watch_group_id {
            emit_sync_watch(context, relationship, group_id, 0, Some(&error)).await;
        }
    }
    Err(error)
}

fn cache_relationship(
    relationships: &mut HashMap<(String, Ulid), SyncRelationship>,
    job: &BlobJobRecord,
    relationship: &SyncRelationship,
    stored: bool,
) {
    let Some(relationship_id) = job.relationship_id else {
        return;
    };
    let key = (job.input.bucket.clone(), relationship_id);
    if stored {
        relationships.insert(key, relationship.clone());
    } else {
        relationships.remove(&key);
    }
}

pub(super) async fn read_job_relationships(
    storage: &StorageHandle,
    jobs: &[(Vec<u8>, BlobJobRecord)],
) -> Result<HashMap<(String, Ulid), SyncRelationship>, BlobQueueError> {
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
    if keys.is_empty() {
        return Ok(HashMap::new());
    }

    let reads = keys
        .iter()
        .map(|(bucket, relationship_id)| {
            (
                RELATIONSHIP_OUT_KEYSPACE.to_string(),
                sync_relationship_key(bucket, *relationship_id).into(),
            )
        })
        .collect();
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
    if values.len() != keys.len() {
        return Err(BlobQueueError::UnexpectedEvent(
            "blob replication relationship read count mismatch".to_string(),
        ));
    }

    let mut relationships = HashMap::with_capacity(keys.len());
    for ((bucket, relationship_id), (key, value)) in keys.into_iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        let relationship = SyncRelationship::from_bytes(&value)?;
        validate_sync_key(&bucket, &key, &relationship)?;
        if relationship.id != relationship_id {
            return Err(ConversionError::FromStrError(
                "sync relationship id does not match requested id".to_string(),
            )
            .into());
        }
        relationships.insert((bucket, relationship_id), relationship);
    }
    Ok(relationships)
}

pub(super) async fn read_replication_cursor(
    storage: &StorageHandle,
) -> Result<ReplicationScanCursor, BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(REPLICATION_CURSOR_KEY.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Ok(ReplicationScanCursor::default())
        }
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(postcard::from_bytes(value.as_ref()).unwrap_or_default()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
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

pub(super) async fn abort_cursor(storage: &StorageHandle, txn_id: Ulid) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

pub(super) async fn advance_replication_cursor(
    storage: &StorageHandle,
    candidate: ReplicationScanCursor,
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
    let (current, valid) = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(REPLICATION_CURSOR_KEY.to_vec()),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            (ReplicationScanCursor::default(), true)
        }
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match postcard::from_bytes(value.as_ref()) {
            Ok(cursor) => (cursor, true),
            Err(_) => (ReplicationScanCursor::default(), false),
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
    let merged = cursor_merge(candidate, current.clone());
    if valid && merged == current {
        match storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
        }
    } else {
        let value = postcard::to_allocvec(&merged).map_err(ConversionError::from)?;
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NODE_STATE_KEYSPACE.to_string(),
                key: ByteView::from(REPLICATION_CURSOR_KEY.to_vec()),
                value: ByteView::from(value),
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
}

pub(super) async fn scan_due_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<BlobJobScan, BlobQueueError> {
    let cursor = read_replication_cursor(storage).await?;
    let start_after = cursor.after.clone();
    let mut jobs = Vec::new();
    // A pass from the start drops the last pass's due time, which may already be past.
    let mut next_due_ms = cursor.next_due_ms.filter(|_| start_after.is_some());
    let mut canonical_changed = false;
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(|key| IterStart::After(ByteView::from(key))),
            limit: PAGE_RECORDS,
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
    let mut last_key;
    let page_empty = values.is_empty();
    // The scan meets each key once; only after a key repair may a job already be taken.
    let mut repaired = false;

    for (key, value) in values {
        let mut key = key.to_vec();
        last_key = Some(key.clone());
        let mut job = match BlobJobRecord::from_bytes(&value) {
            Ok(job) => job,
            Err(error) => {
                warn!(error = %error, key = ?key, "Deleting malformed blob replication job");
                delete_blob_job(storage, key).await?;
                continue;
            }
        };
        let canonical_key = blob_job_key(&job)?.to_vec();
        if canonical_key.as_slice() != key.as_slice() {
            repaired = true;
            warn!(key = ?key, "Repairing blob replication job stored under non-canonical key");
            if let Some(existing) = read_blob_job(storage, &canonical_key).await?
                && blob_job_preferred(&existing, &job)
            {
                delete_blob_job(storage, key).await?;
                job = existing;
            } else {
                write_blob_job(storage, &job).await?;
                delete_blob_job(storage, key).await?;
                canonical_changed = true;
                jobs.retain(|(existing_key, _)| existing_key != &canonical_key);
            }
            key = canonical_key;
        } else if canonical_changed
            && let Some(existing) = read_blob_job(storage, &canonical_key).await?
        {
            job = existing;
        }
        if job.due_at_ms == PARKED_DUE {
            continue;
        }
        if job.due_at_ms > now_ms {
            next_due_ms = min_due_at(next_due_ms, job.due_at_ms);
            continue;
        }
        if merge_due_job(&mut jobs, key, job, (repaired, limit)) {
            return Ok(BlobJobScan {
                next_cursor: ReplicationScanCursor {
                    generation: cursor.generation,
                    after: last_key,
                    next_due_ms,
                },
                generation: cursor.generation,
                jobs,
                has_more_due: true,
                next_due_ms,
            });
        }
    }

    if let Some(next) = next_start_after {
        return Ok(BlobJobScan {
            next_cursor: ReplicationScanCursor {
                generation: cursor.generation,
                after: Some(next.to_vec()),
                next_due_ms,
            },
            generation: cursor.generation,
            jobs,
            has_more_due: true,
            next_due_ms,
        });
    }

    Ok(BlobJobScan {
        next_cursor: ReplicationScanCursor {
            generation: if page_empty && cursor.after.is_none() {
                cursor.generation
            } else {
                cursor.generation.saturating_add(1)
            },
            after: None,
            next_due_ms,
        },
        generation: cursor.generation,
        jobs,
        has_more_due: false,
        next_due_ms,
    })
}

async fn read_blob_job(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<BlobJobRecord>, BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(BlobJobRecord::from_bytes(&value)?)),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn write_blob_job(
    storage: &StorageHandle,
    job: &BlobJobRecord,
) -> Result<(), BlobQueueError> {
    let (key_space, key, value) = blob_job_entry(job)?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

async fn delete_blob_job(storage: &StorageHandle, key: Vec<u8>) -> Result<(), BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

pub(super) async fn reschedule_blob_job(
    storage: &StorageHandle,
    key: Vec<u8>,
    job: &BlobJobRecord,
    error: String,
) -> Result<u64, BlobQueueError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = unix_timestamp_millis().saturating_add(retry_delay_ms(attempts));
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
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(next_job.to_bytes()?),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(due_at_ms),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
    }
}

#[cfg(test)]
pub(super) async fn read_relationships(
    storage: &StorageHandle,
    bucket: &str,
) -> Result<Vec<SyncRelationship>, BlobQueueError> {
    let mut start_after = None;
    let mut relationships = Vec::new();
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(aruna_core::structs::sync_relationship_prefix(bucket).into()),
                start: start_after.take().map(IterStart::After),
                limit: super::obligations::REPLICATION_PAGE_SIZE,
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
            relationships.push(relationship);
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(relationships),
        }
    }
}
