//! Works off the blob cleanup backlog and sweeps multipart uploads that went stale.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use crate::blob::holders::dht_registration_effect;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, CLEANUP_CURSOR_KEYSPACE, COPY_OWNER_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::execution::job::{JobId, JobRecord, RoCrateLimits};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, CopyOwner,
    WriteOwner,
};
use aruna_core::structs::storage::multipart::{
    COMPLETION_DEADLINE_MS, MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::types::{Key, Value};
use tracing::{error, warn};
use ulid::Ulid;

use crate::blob::migration::drain::SettleMoveOperation;
use crate::blob::migration::shared::MigrationError;
use crate::blob::release::{DropClaimsOperation, ReleaseFile, ReleaseRenamedOperation};
use crate::blob::trail::{DropTrailOperation, TrailError};
use crate::groups::backends::backend_key;
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls::read_job_record;
use crate::s3::multipart::abort::{AbortUploadError, AbortUploadInput, AbortUploadOperation};
use aruna_core::structs::storage::routing::NodeRouting;

pub const BLOB_CLEANUP_AFTER: Duration = Duration::from_secs(300);
pub const BLOB_CLEANUP_RETRY: Duration = Duration::from_secs(30);
const CLEANUP_PAGE_SIZE: usize = 128;
/// An `Open` upload nobody added to or aborted is abandoned; S3 lifecycle rules
/// use the same order of magnitude.
const OPEN_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// A `Completing` upload past the completion deadline lost the request that
/// owned it; the margin keeps the sweep off a completion still running.
const COMPLETING_TTL_MS: u64 = COMPLETION_DEADLINE_MS + BLOB_CLEANUP_AFTER.as_millis() as u64;
/// Aborts are transactional, so one run reclaims a bounded slice of the backlog.
const UPLOAD_SWEEP_BATCH: usize = 32;

/// How long a recorded copy upload may go without a copied part before the cleanup takes it
/// for a leftover; each part renews it.
const SETTLE_GRACE_MS: u64 = 60 * 60 * 1000;

/// Rows one drain pass looks at, at most; the next pass continues after the last one.
const PASS_ROWS: usize = 4096;
/// Where the next drain pass starts.
const CURSOR_KEY: &[u8] = b"cursor";

type Turn<T> = Result<Next<T>, MigrationError>;

/// What a drain needs to know about this node.
#[derive(Clone, Debug, PartialEq)]
pub struct CleanupNode {
    pub routing: NodeRouting,
    /// Whether this node has blob storage to delete files with.
    pub has_blob: bool,
}

#[derive(Debug, Default, PartialEq)]
pub struct BlobCleanupOutcome {
    pub processed: usize,
    pub failed: usize,
    pub dropped: usize,
    /// Rows that must stay queued without being failures, such as a live upload's target.
    pub kept: usize,
    /// The pass stopped at its row limit; the next one continues after it.
    pub more: bool,
}

/// What one drain pass did with a row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowOutcome {
    Done,
    Failed,
    /// Not finished, but nothing went wrong: the row waits for a later pass.
    Kept,
}

impl From<bool> for RowOutcome {
    fn from(done: bool) -> Self {
        match done {
            true => Self::Done,
            false => Self::Failed,
        }
    }
}

#[derive(Debug, PartialEq)]
enum DrainStep {
    Cursor,
    Page,
    Backend(Key, Box<BlobCleanupWork>),
    Work(Key),
    Deleted,
    Stored,
}

/// Pages through the whole queue so persistently failing rows cannot starve the rows behind
/// them, deleting each page's completed rows before the next fetch.
#[derive(Debug, PartialEq)]
pub struct ProcessCleanupOperation {
    node: CleanupNode,
    step: DrainStep,
    outcome: BlobCleanupOutcome,
    rows: VecDeque<(Key, Value)>,
    done: Vec<(String, Key)>,
    last: Option<Key>,
    more: Option<Key>,
    seen: usize,
}

type DrainTurn = Turn<BlobCleanupOutcome>;

impl ProcessCleanupOperation {
    pub fn new(node: CleanupNode) -> Self {
        Self {
            node,
            step: DrainStep::Cursor,
            outcome: BlobCleanupOutcome::default(),
            rows: VecDeque::new(),
            done: Vec::new(),
            last: None,
            more: None,
            seen: 0,
        }
    }

    fn call(&mut self, step: DrainStep, call: IoCall) -> DrainTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn read_page(&mut self, after: Option<Key>) -> DrainTurn {
        let page = IoCall::Page {
            space: BLOB_CLEANUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: CLEANUP_PAGE_SIZE,
        };
        self.call(DrainStep::Page, page)
    }

    fn row_done(&mut self, key: Key) {
        self.done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
    }

    fn row_dropped(&mut self, key: Key) {
        self.row_done(key);
        self.outcome.dropped = self.outcome.dropped.saturating_add(1);
    }

    fn next_row(&mut self) -> DrainTurn {
        while let Some((key, value)) = self.rows.pop_front() {
            // A row nobody can decode would wedge the drain forever.
            let work = match BlobCleanupWork::from_bytes(&value) {
                Ok(work) => work,
                Err(error) => {
                    warn!(error = %error, "Dropping undecodable blob cleanup row");
                    self.row_dropped(key);
                    continue;
                }
            };
            // A tenant backend that is gone can never resolve credentials, so retrying this
            // row forever would wedge the drain.
            if let Some(BackendRef::Group(id)) = work_backend(&work) {
                let check = RemovedBackendOperation { backend_id: *id };
                let step = DrainStep::Backend(key, Box::new(work));
                return self.call(step, IoCall::child(check));
            }
            return self.run_work(key, work);
        }
        self.page_done()
    }

    fn run_work(&mut self, key: Key, work: BlobCleanupWork) -> DrainTurn {
        let run = CleanupWorkOperation::new(work, self.node.clone());
        self.call(DrainStep::Work(key), IoCall::child(run))
    }

    fn worked(&mut self, key: Key, outcome: RowOutcome) {
        let totals = &mut self.outcome;
        match outcome {
            RowOutcome::Done => {
                totals.processed = totals.processed.saturating_add(1);
                self.row_done(key);
            }
            RowOutcome::Failed => totals.failed = totals.failed.saturating_add(1),
            RowOutcome::Kept => totals.kept = totals.kept.saturating_add(1),
        }
    }

    fn page_done(&mut self) -> DrainTurn {
        if self.done.is_empty() {
            return self.after_deletes();
        }
        let deletes = std::mem::take(&mut self.done);
        self.call(DrainStep::Deleted, IoCall::Delete(deletes))
    }

    fn after_deletes(&mut self) -> DrainTurn {
        let Some(next) = self.more.take() else {
            return self.store_cursor(None);
        };
        // A bounded pass keeps its place, so every row gets its turn over the passes.
        if self.seen >= PASS_ROWS {
            self.outcome.more = true;
            let last = self.last.take();
            return self.store_cursor(last);
        }
        self.read_page(Some(next))
    }

    fn store_cursor(&mut self, cursor: Option<Key>) -> DrainTurn {
        let key: Key = CURSOR_KEY.to_vec().into();
        let space = CLEANUP_CURSOR_KEYSPACE.to_string();
        let call = match cursor {
            Some(value) => IoCall::Write(vec![(space, key, value)]),
            None => IoCall::Delete(vec![(space, key)]),
        };
        self.call(DrainStep::Stored, call)
    }
}

impl Operation for ProcessCleanupOperation {
    type Output = BlobCleanupOutcome;
    type Error = MigrationError;

    fn first(&mut self) -> DrainTurn {
        Ok(Next::Call(read_row(
            CLEANUP_CURSOR_KEYSPACE,
            CURSOR_KEY.to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> DrainTurn {
        match std::mem::replace(&mut self.step, DrainStep::Stored) {
            DrainStep::Cursor => self.read_page(result.value()?),
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                self.last = rows.last().map(|(key, _)| key.clone());
                self.seen += rows.len();
                self.rows = rows.into();
                self.more = next;
                self.next_row()
            }
            DrainStep::Backend(key, work) => {
                // Only a record that is provably absent counts as removed; an unreadable one
                // leaves the row queued.
                if result.child::<bool, IoError>()?.unwrap_or(false) {
                    if let Some(backend) = work_backend(&work) {
                        error!(backend = %backend, "Dropping blob cleanup for a removed backend");
                    }
                    self.row_dropped(key);
                    return self.next_row();
                }
                self.run_work(key, *work)
            }
            DrainStep::Work(key) => {
                let outcome = match result.child::<RowOutcome, MigrationError>()? {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        warn!(%error, "A blob cleanup row failed");
                        RowOutcome::Failed
                    }
                };
                self.worked(key, outcome);
                self.next_row()
            }
            DrainStep::Deleted => {
                result.done()?;
                self.after_deletes()
            }
            DrainStep::Stored => {
                result.done()?;
                Ok(Next::Done(std::mem::take(&mut self.outcome)))
            }
        }
    }
}

/// Whether the record of a tenant backend is absent.
#[derive(Debug, PartialEq)]
struct RemovedBackendOperation {
    backend_id: Ulid,
}

impl Operation for RemovedBackendOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(self.backend_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(result.value()?.is_none()))
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct UploadSweepOutcome {
    pub aborted: usize,
    pub failed: usize,
}

/// An upload no client will finish: an `Open` record past its TTL, a `Completing` one whose
/// lease lapsed long enough ago that its request is gone, or an abort that stopped midway.
fn stale_upload(record: &MultipartUpload, now_ms: u64) -> bool {
    let age = record
        .created_at
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| now_ms.saturating_sub(since.as_millis() as u64))
        .unwrap_or_default();
    match record.status {
        MultipartUploadStatus::Open => age >= OPEN_TTL_MS,
        MultipartUploadStatus::Completing => record
            .completing_since_ms
            .map(|since| now_ms.saturating_sub(since) >= COMPLETING_TTL_MS)
            .unwrap_or(age >= COMPLETING_TTL_MS),
        MultipartUploadStatus::Aborting => true,
    }
}

/// Reclaims stale multipart uploads, their part rows and their part blobs. The abort operation
/// owns the transaction, so a refused record stays for the next run instead of leaving
/// half-deleted state behind.
#[derive(Debug, PartialEq)]
pub struct SweepUploadsOperation {
    now_ms: u64,
    pages: PrefixPages,
    stale: VecDeque<MultipartUpload>,
    /// The upload being aborted, with the status the scan saw.
    aborting: Option<(Ulid, MultipartUploadStatus)>,
    outcome: UploadSweepOutcome,
}

impl SweepUploadsOperation {
    pub fn new(now_ms: u64) -> Self {
        let prefix = Key::from(Vec::new());
        Self {
            now_ms,
            pages: PrefixPages::new(UPLOAD_KEYSPACE, prefix, CLEANUP_PAGE_SIZE),
            stale: VecDeque::new(),
            aborting: None,
            outcome: UploadSweepOutcome::default(),
        }
    }

    fn next_abort(&mut self) -> Turn<UploadSweepOutcome> {
        let Some(record) = self.stale.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        };
        let upload_id = record.upload_id;
        let mut abort = AbortUploadOperation::new(AbortUploadInput {
            bucket: record.bucket,
            key: record.key,
            upload_id,
            now_ms: self.now_ms,
        });
        // An abort is transactional, so finishing one that stopped midway is safe to repeat.
        // The status is checked again in that transaction: this scan may be stale.
        if record.status == MultipartUploadStatus::Aborting {
            abort = abort.resuming_abort();
        }
        self.aborting = Some((upload_id, record.status));
        Ok(Next::Call(IoCall::child(abort)))
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>) -> Turn<UploadSweepOutcome> {
        for (_, value) in rows {
            match MultipartUpload::from_bytes(&value) {
                Ok(record) if stale_upload(&record, self.now_ms) => self.stale.push_back(record),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "Skipping an undecodable multipart upload"),
            }
            if self.stale.len() >= UPLOAD_SWEEP_BATCH {
                return self.next_abort();
            }
        }
        if self.pages.done() {
            return self.next_abort();
        }
        Ok(Next::Call(self.pages.call()))
    }
}

impl Operation for SweepUploadsOperation {
    type Output = UploadSweepOutcome;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<UploadSweepOutcome> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<UploadSweepOutcome> {
        let Some((upload_id, status)) = self.aborting.take() else {
            let rows = self.pages.take(result)?;
            return self.page_read(rows);
        };
        match result.child::<(), AbortUploadError>()? {
            Ok(()) => {
                self.outcome.aborted = self.outcome.aborted.saturating_add(1);
                warn!(%upload_id, ?status, "Reclaimed a stale multipart upload");
            }
            Err(error) => {
                self.outcome.failed = self.outcome.failed.saturating_add(1);
                warn!(%upload_id, error = %error, "Failed to reclaim a stale multipart upload");
            }
        }
        self.next_abort()
    }
}

/// The backend a row needs credentials for, if it needs any.
fn work_backend(work: &BlobCleanupWork) -> Option<&BackendRef> {
    work.location().map(|location| &location.backend)
}

/// Drops a page of `key_space` rows under `prefix`. Outputs `true` once none is left.
#[derive(Debug, PartialEq)]
struct DropRowsOperation {
    key_space: String,
    prefix: Vec<u8>,
    deleting: bool,
}

impl Operation for DropRowsOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        Ok(Next::Call(IoCall::Page {
            space: self.key_space.clone(),
            prefix: self.prefix.clone().into(),
            after: None,
            limit: CLEANUP_PAGE_SIZE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(false));
        }
        let (rows, _) = result.page()?;
        if rows.is_empty() {
            return Ok(Next::Done(true));
        }
        self.deleting = true;
        let deletes = (rows.into_iter())
            .map(|(key, _)| (self.key_space.clone(), key))
            .collect();
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

/// How the result of a finished step decides the row.
#[derive(Debug, PartialEq)]
enum WorkStep {
    Start,
    /// A child that answers with the row's outcome.
    Outcome,
    /// A child that answers whether the row is done.
    Finished,
    /// A child that answers done or not yet, and the message of its failure.
    Pass(&'static str),
    Trail,
    Owner(WriteOwner, Box<BackendLocation>),
    Reservation(WriteOwner),
    Delete,
    Reconcile,
    Dht,
    Planned(Box<ReleaseFile>),
    Settle,
}

/// Runs one queued cleanup row. Execution is best effort: a failed row stays queued and retries
/// in a later drain.
#[derive(Debug, PartialEq)]
struct CleanupWorkOperation {
    work: Option<BlobCleanupWork>,
    node: CleanupNode,
    now_ms: u64,
    step: WorkStep,
}

type WorkTurn = Turn<RowOutcome>;

impl CleanupWorkOperation {
    fn new(work: BlobCleanupWork, node: CleanupNode) -> Self {
        Self {
            work: Some(work),
            node,
            now_ms: 0,
            step: WorkStep::Start,
        }
    }

    fn call(&mut self, step: WorkStep, call: IoCall) -> WorkTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn release(&mut self, file: ReleaseFile) -> WorkTurn {
        let release = ReleaseRenamedOperation::new(file);
        self.call(WorkStep::Finished, IoCall::child(release))
    }

    fn delete_blob(&mut self, location: BackendLocation) -> WorkTurn {
        // An imported object is never Aruna's to delete; a part still ends its provider part
        // claim.
        if location.is_external() && !location.partial {
            return Ok(Next::Done(RowOutcome::Done));
        }
        let delete = Effect::Blob(BlobEffect::Delete { location });
        self.call(WorkStep::Delete, delete.into())
    }

    fn register_dht(&mut self, blake3: [u8; 32], realm_id: RealmId, ttl_ms: u64) -> WorkTurn {
        let limits = RoCrateLimits {
            holder_ttl_ms: ttl_ms,
            ..RoCrateLimits::default()
        };
        let Ok(effect) = dht_registration_effect(&blake3, realm_id, &limits) else {
            return Ok(Next::Done(RowOutcome::Failed));
        };
        self.call(WorkStep::Dht, effect.into())
    }

    fn start(&mut self, work: BlobCleanupWork) -> WorkTurn {
        match work {
            BlobCleanupWork::DeleteBlob { location } => self.delete_blob(location),
            BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Upload { upload_id },
            } => {
                let target = ReconcileTargetOperation::new(upload_id, None, location);
                self.call(WorkStep::Outcome, IoCall::child(target))
            }
            BlobCleanupWork::ReconcileWrite {
                location,
                owner:
                    WriteOwner::CompletedUpload {
                        upload_id,
                        blake3,
                        realm_id,
                        ttl_ms,
                        announce,
                    },
            } => {
                let holder = Some((blake3, realm_id, ttl_ms, announce));
                let target = ReconcileTargetOperation::new(upload_id, holder, location);
                self.call(WorkStep::Outcome, IoCall::child(target))
            }
            // The committed metadata decides. An unreadable owner is not a proof of either
            // outcome, so the row waits for a drain that can read it.
            BlobCleanupWork::ReconcileWrite { location, owner } => {
                let owns = OwnsWriteOperation::new(&owner, &location);
                let step = WorkStep::Owner(owner, Box::new(location));
                self.call(step, IoCall::child(owns))
            }
            BlobCleanupWork::ReconcileReservation { location } => {
                let reconcile = Effect::Blob(BlobEffect::ReconcileReservation { location });
                self.call(WorkStep::Reconcile, reconcile.into())
            }
            BlobCleanupWork::RegisterDht {
                blake3,
                realm_id,
                ttl_ms,
            } => self.register_dht(blake3, realm_id, ttl_ms),
            BlobCleanupWork::ReleaseRenamed {
                location,
                import_id,
                version_id,
                version,
            } => self.release(ReleaseFile {
                location,
                import_id,
                version_id,
                version,
                has_blob: self.node.has_blob,
            }),
            BlobCleanupWork::ReleasePlanned {
                job_id,
                location,
                import_id,
                version_id,
                version,
            } => {
                let file = ReleaseFile {
                    location,
                    import_id,
                    version_id,
                    version,
                    has_blob: self.node.has_blob,
                };
                // A purge job id that cannot exist has no purge left to wait for.
                let Ok(job_id) = JobId::try_from_bytes(job_id.to_bytes()) else {
                    return self.release(file);
                };
                let read = read_job_record(job_id);
                self.call(WorkStep::Planned(Box::new(file)), IoCall::child(read))
            }
            BlobCleanupWork::SettleUpload {
                attempt,
                location,
                upload_id,
                since_ms,
            } => {
                // A copy may still be running; only an upload past the grace is its leftover.
                if self.now_ms < since_ms.saturating_add(SETTLE_GRACE_MS) {
                    return Ok(Next::Done(RowOutcome::Kept));
                }
                // Exactly the decoded upload; a newer try of the attempt keeps its own.
                let upload = Some((Box::new(location), upload_id));
                let settle = Effect::Blob(BlobEffect::SettleCopy { attempt, upload });
                self.call(WorkStep::Settle, settle.into())
            }
            BlobCleanupWork::SettleMove { version, intents } => {
                let routing = self.node.routing.clone();
                let settle = SettleMoveOperation::new(routing, &version, intents);
                let step = WorkStep::Pass("A rename's move could not be settled");
                self.call(step, IoCall::child(settle))
            }
            BlobCleanupWork::DropRows { key_space, prefix } => {
                let drop = DropRowsOperation {
                    key_space,
                    prefix,
                    deleting: false,
                };
                let step = WorkStep::Pass("Rows of a removed rule could not be dropped");
                self.call(step, IoCall::child(drop))
            }
            BlobCleanupWork::DropClaims { import_id } => {
                let drop = DropClaimsOperation::new(import_id);
                let step = WorkStep::Pass("Claims of a removed rule could not be dropped");
                self.call(step, IoCall::child(drop))
            }
            BlobCleanupWork::DropTrail {
                group_id,
                bucket,
                incarnation,
            } => {
                let drop = DropTrailOperation::new(group_id, bucket.as_deref(), incarnation);
                self.call(WorkStep::Trail, IoCall::child(drop))
            }
        }
    }

    fn owner_known(
        &mut self,
        owner: WriteOwner,
        location: BackendLocation,
        owned: Option<bool>,
    ) -> WorkTurn {
        match owned {
            None => Ok(Next::Done(RowOutcome::Failed)),
            Some(true) => {
                let clear = Effect::Blob(BlobEffect::ReleaseReservation { id: location.ulid });
                self.call(WorkStep::Reservation(owner), clear.into())
            }
            Some(false) => self.delete_blob(location),
        }
    }

    fn reservation_cleared(&mut self, owner: WriteOwner) -> WorkTurn {
        match owner {
            WriteOwner::Blob {
                blake3,
                realm_id,
                ttl_ms,
                announce: true,
            } => self.register_dht(blake3, realm_id, ttl_ms),
            // A copy of a distributed bucket is found through its content index.
            WriteOwner::Blob {
                announce: false, ..
            }
            | WriteOwner::UploadPart { .. }
            | WriteOwner::Upload { .. }
            | WriteOwner::CompletedUpload { .. }
            | WriteOwner::Pending => Ok(Next::Done(RowOutcome::Done)),
        }
    }

    fn planned(
        &mut self,
        file: ReleaseFile,
        record: Result<Option<JobRecord>, JobMutationError>,
    ) -> WorkTurn {
        match record {
            Ok(record) if record.as_ref().is_none_or(|record| record.is_settled()) => {
                self.release(file)
            }
            Ok(_) => Ok(Next::Done(RowOutcome::Kept)),
            Err(error) => {
                warn!(%error, "A purge job could not be read");
                Ok(Next::Done(RowOutcome::Failed))
            }
        }
    }
}

fn deleted(event: Event) -> RowOutcome {
    match event {
        Event::Blob(BlobEvent::DeleteFinished) => RowOutcome::Done,
        event => {
            warn!(event = event.label(), "Deferred blob delete failed");
            RowOutcome::Failed
        }
    }
}

fn registered(event: Event) -> RowOutcome {
    match event {
        Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. })) => RowOutcome::Done,
        event => {
            warn!(
                event = event.label(),
                "Deferred blob DHT registration failed"
            );
            RowOutcome::Failed
        }
    }
}

impl Operation for CleanupWorkOperation {
    type Output = RowOutcome;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> WorkTurn {
        match self.work.take() {
            Some(work) => self.start(work),
            None => Err(MigrationError::Failed("no cleanup work".to_string())),
        }
    }

    fn next(&mut self, result: IoResult) -> WorkTurn {
        match std::mem::replace(&mut self.step, WorkStep::Start) {
            WorkStep::Start => Err(MigrationError::Failed("no cleanup call".to_string())),
            WorkStep::Outcome => Ok(Next::Done(result.child::<RowOutcome, MigrationError>()??)),
            WorkStep::Finished => {
                let done = result.child::<bool, MigrationError>()?;
                let done = done.unwrap_or_else(|error| {
                    warn!(%error, "A renamed file could not be released");
                    false
                });
                Ok(Next::Done(done.into()))
            }
            WorkStep::Pass(failure) => Ok(Next::Done(
                match result.child::<bool, MigrationError>()? {
                    Ok(true) => RowOutcome::Done,
                    Ok(false) => RowOutcome::Kept,
                    Err(error) => {
                        warn!(%error, "{failure}");
                        RowOutcome::Failed
                    }
                },
            )),
            WorkStep::Trail => Ok(Next::Done(match result.child::<(), TrailError>()? {
                Ok(()) => RowOutcome::Done,
                Err(error) => {
                    warn!(%error, "Delete trail of a removed bucket could not be dropped");
                    RowOutcome::Failed
                }
            })),
            WorkStep::Owner(owner, location) => {
                let owned = result.child::<Option<bool>, IoError>()?.unwrap_or(None);
                self.owner_known(owner, *location, owned)
            }
            WorkStep::Reservation(owner) => {
                result.event()?;
                self.reservation_cleared(owner)
            }
            WorkStep::Delete => Ok(Next::Done(deleted(result.event()?))),
            WorkStep::Reconcile => Ok(Next::Done(match result.event()? {
                Event::Blob(BlobEvent::ReservationReconciled { done }) => done.into(),
                event => {
                    let event = event.label();
                    warn!(event, "Deferred bucket reservation reconciliation failed");
                    RowOutcome::Failed
                }
            })),
            WorkStep::Dht => Ok(Next::Done(registered(result.event()?))),
            WorkStep::Planned(file) => self.planned(*file, result.child()?),
            // The settlement removes the record itself, and only while it is unchanged.
            WorkStep::Settle => Ok(Next::Done(match result.event()? {
                Event::Blob(BlobEvent::UploadAborted) => RowOutcome::Kept,
                event => {
                    warn!(event = event.label(), "A copy upload could not be settled");
                    RowOutcome::Failed
                }
            })),
        }
    }
}

/// The DHT announcement a committed upload still owes.
type Holder = ([u8; 32], RealmId, u64, bool);

#[derive(Debug, PartialEq)]
enum TargetStep {
    Upload,
    Holder,
    Reservation(bool),
    Dht,
    Row,
    Settled,
}

/// A live upload's target row stays queued: it is also the bucket reservation abort or
/// completion settles. A committed version takes it over; otherwise, after the grace a running
/// create needs, the target goes with its provider uploads.
#[derive(Debug, PartialEq)]
struct ReconcileTargetOperation {
    upload_id: Ulid,
    holder: Option<Holder>,
    location: BackendLocation,
    now_ms: u64,
    step: TargetStep,
}

impl ReconcileTargetOperation {
    fn new(upload_id: Ulid, holder: Option<Holder>, location: BackendLocation) -> Self {
        Self {
            upload_id,
            holder,
            location,
            now_ms: 0,
            step: TargetStep::Upload,
        }
    }

    fn call(&mut self, step: TargetStep, call: IoCall) -> WorkTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn check_holder(&mut self) -> WorkTurn {
        let Some((blake3, realm_id, ttl_ms, announce)) = self.holder else {
            return self.past_grace();
        };
        let owner = WriteOwner::Blob {
            blake3,
            realm_id,
            ttl_ms,
            announce,
        };
        let owns = OwnsWriteOperation::new(&owner, &self.location);
        self.call(TargetStep::Holder, IoCall::child(owns))
    }

    fn past_grace(&mut self) -> WorkTurn {
        let opened_ms = self.location.ulid.timestamp_ms();
        let age = self.now_ms.saturating_sub(opened_ms);
        if age < BLOB_CLEANUP_AFTER.as_millis() as u64 {
            return Ok(Next::Done(RowOutcome::Kept));
        }
        // Read after the upload: a completion replaces this row in the commit that removes it.
        let key = self.location.ulid.to_bytes().to_vec();
        self.call(TargetStep::Row, read_row(BLOB_CLEANUP_KEYSPACE, key))
    }

    /// Whether the target's own row still names an upload. A committed version replaced it
    /// with its own row, which settles the object instead.
    fn row_read(&mut self, value: Option<Value>) -> WorkTurn {
        let names_upload = |value: &Value| {
            matches!(
                BlobCleanupWork::from_bytes(value),
                Ok(BlobCleanupWork::ReconcileWrite {
                    location: row,
                    owner: WriteOwner::Upload { .. } | WriteOwner::CompletedUpload { .. },
                }) if row.same_object(&self.location)
            )
        };
        let own_row = value.is_some();
        if value.as_ref().is_some_and(|value| !names_upload(value)) {
            return Ok(Next::Done(RowOutcome::Kept));
        }
        // Only the target's own row may delete it; a later abort's row may outlive a completion.
        let location = self.location.clone();
        let settle = match own_row {
            true => BlobEffect::DiscardTarget { location },
            false => BlobEffect::AbortTarget { location },
        };
        self.call(TargetStep::Settled, Effect::Blob(settle).into())
    }
}

impl Operation for ReconcileTargetOperation {
    type Output = RowOutcome;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> WorkTurn {
        let owner = WriteOwner::Upload {
            upload_id: self.upload_id,
        };
        let owns = OwnsWriteOperation::new(&owner, &self.location);
        Ok(Next::Call(IoCall::child(owns)))
    }

    fn next(&mut self, result: IoResult) -> WorkTurn {
        match self.step {
            TargetStep::Upload => match result.child::<Option<bool>, IoError>()?.unwrap_or(None) {
                None => Ok(Next::Done(RowOutcome::Failed)),
                Some(true) => Ok(Next::Done(RowOutcome::Kept)),
                Some(false) => self.check_holder(),
            },
            TargetStep::Holder => match result.child::<Option<bool>, IoError>()?.unwrap_or(None) {
                None => Ok(Next::Done(RowOutcome::Failed)),
                Some(true) => {
                    let announce = self.holder.is_some_and(|(.., announce)| announce);
                    let clear = BlobEffect::ReleaseReservation {
                        id: self.location.ulid,
                    };
                    let step = TargetStep::Reservation(announce);
                    self.call(step, Effect::Blob(clear).into())
                }
                Some(false) => self.past_grace(),
            },
            TargetStep::Reservation(announce) => {
                result.event()?;
                let Some((blake3, realm_id, ttl_ms, _)) = self.holder.filter(|_| announce) else {
                    return Ok(Next::Done(RowOutcome::Done));
                };
                let limits = RoCrateLimits {
                    holder_ttl_ms: ttl_ms,
                    ..RoCrateLimits::default()
                };
                let Ok(effect) = dht_registration_effect(&blake3, realm_id, &limits) else {
                    return Ok(Next::Done(RowOutcome::Failed));
                };
                self.call(TargetStep::Dht, effect.into())
            }
            TargetStep::Dht => Ok(Next::Done(registered(result.event()?))),
            TargetStep::Row => self.row_read(result.value()?),
            TargetStep::Settled => Ok(Next::Done(match result.event()? {
                Event::Blob(BlobEvent::DeleteFinished | BlobEvent::UploadAborted) => {
                    RowOutcome::Done
                }
                event => {
                    let event = event.label();
                    warn!(event, "Discarding an in-place upload target failed");
                    RowOutcome::Failed
                }
            })),
        }
    }
}

/// Whether committed metadata still names this exact physical copy. `None` means the record
/// could not be decoded, so nothing is proven; a failed read ends as an error.
#[derive(Debug, PartialEq)]
struct OwnsWriteOperation {
    owner: WriteOwner,
    location: BackendLocation,
    scanning: bool,
}

impl OwnsWriteOperation {
    fn new(owner: &WriteOwner, location: &BackendLocation) -> Self {
        Self {
            owner: owner.clone(),
            location: location.clone(),
            scanning: false,
        }
    }

    fn owner_row(&self) -> Option<(&'static str, Key)> {
        let location = &self.location;
        Some(match &self.owner {
            WriteOwner::Blob { blake3, .. } => (
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(
                    *blake3,
                    location.format.encoding(),
                    location.backend.clone(),
                )
                .to_bytes()
                .into(),
            ),
            WriteOwner::UploadPart {
                upload_id,
                part_number,
            } => (
                UPLOAD_PART_KEYSPACE,
                MultipartPartKey::new(*upload_id, *part_number)
                    .to_bytes()
                    .ok()?
                    .into(),
            ),
            WriteOwner::Upload { upload_id } | WriteOwner::CompletedUpload { upload_id, .. } => {
                (UPLOAD_KEYSPACE, upload_id.to_bytes().to_vec().into())
            }
            WriteOwner::Pending => (
                PENDING_LOCATION_KEYSPACE,
                ArchiveKey::of(location).to_bytes().into(),
            ),
        })
    }

    fn owned(&self, value: &[u8]) -> Option<bool> {
        let owned = match &self.owner {
            WriteOwner::Blob { .. } | WriteOwner::Pending => {
                BackendLocation::from_bytes(value).ok()?
            }
            WriteOwner::UploadPart { .. } => MultipartPart::from_bytes(value).ok()?.location,
            WriteOwner::Upload { .. } | WriteOwner::CompletedUpload { .. } => {
                // An aborting upload still owns its target: a failed abort reopens it for a
                // completion, and the abort itself, also when resumed, removes the target.
                match MultipartUpload::from_bytes(value).ok()?.backend_upload {
                    Some(upload) => upload.location,
                    None => return Some(false),
                }
            }
        };
        Some(owned.same_object(&self.location))
    }
}

impl Operation for OwnsWriteOperation {
    type Output = Option<bool>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<bool>>, IoError> {
        match self.owner_row() {
            Some((space, key)) => Ok(Next::Call(read_row(space, key))),
            None => Ok(Next::Done(None)),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<bool>>, IoError> {
        if self.scanning {
            return Ok(Next::Done(Some(!result.page()?.0.is_empty())));
        }
        if let Some(value) = result.value()? {
            return Ok(Next::Done(self.owned(&value)));
        }
        // A promoted archive has no pending row; its versions still own it.
        if !matches!(self.owner, WriteOwner::Pending) {
            return Ok(Next::Done(Some(false)));
        }
        self.scanning = true;
        Ok(Next::Call(IoCall::Page {
            space: COPY_OWNER_KEYSPACE.to_string(),
            prefix: CopyOwner::prefix(&ArchiveKey::of(&self.location)).into(),
            after: None,
            limit: 1,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlobCleanupOutcome, CLEANUP_PAGE_SIZE, CleanupNode, CleanupWorkOperation,
        ProcessCleanupOperation, ReconcileTargetOperation, RowOutcome,
    };
    use crate::blob::migration::shared::MigrationError;
    use crate::driver::{DriverContext, drive, node_routing};
    use crate::storage_read::iter_prefix_page;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, UPLOAD_KEYSPACE};
    use aruna_core::structs::execution::job::RoCrateLimits;
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, WriteOwner,
    };
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::multipart::{
        BackendUpload, MultipartUpload, MultipartUploadStatus,
    };
    use aruna_storage::storage::{FjallStorage, StorageHandle};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    fn node(context: &DriverContext) -> CleanupNode {
        CleanupNode {
            routing: node_routing(context),
            has_blob: context.blob_handle.is_some(),
        }
    }

    async fn process_cleanup_batch(
        context: &DriverContext,
    ) -> Result<BlobCleanupOutcome, MigrationError> {
        drive(ProcessCleanupOperation::new(node(context)), context).await
    }

    async fn run_cleanup_work(context: &DriverContext, work: BlobCleanupWork) -> RowOutcome {
        let run = CleanupWorkOperation::new(work, node(context));
        drive(run, context).await.unwrap_or(RowOutcome::Failed)
    }

    #[test]
    fn empty_queue_clears() {
        use aruna_core::calls::{IoCall, IoResult, Next};
        use aruna_core::operation::Operation;
        let node = CleanupNode {
            routing: Default::default(),
            has_blob: false,
        };
        let mut drain = ProcessCleanupOperation::new(node);
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = drain.next(IoResult::Values(vec![None]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(
            drain.next(page),
            Ok(Next::Call(IoCall::Delete(_)))
        ));
        let done = drain.next(IoResult::Done);
        assert_eq!(done, Ok(Next::Done(BlobCleanupOutcome::default())));
    }

    fn setup_context() -> (TempDir, StorageHandle, DriverContext) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (dir, storage, context)
    }

    fn delete_work() -> Vec<u8> {
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([3u8; 32]);
        BlobCleanupWork::DeleteBlob {
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "root".to_string(),
                storage_bucket: "bucket".to_string(),
                backend_path: "bucket/object".to_string(),
                ulid: Ulid::generate(),
                format: StoredFormat::default(),
                created_by: UserId::local(Ulid::generate(), realm_id),
                created_at: SystemTime::now(),
                staging: false,
                partial: false,
                blob_size: 0,
                hashes: HashMap::new(),
            },
        }
        .to_bytes()
        .unwrap()
    }

    fn group_delete_work() -> Vec<u8> {
        let mut work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        if let BlobCleanupWork::DeleteBlob { location } = &mut work {
            location.backend = BackendRef::Group(Ulid::generate());
        }
        work.to_bytes().unwrap()
    }

    async fn write_rows(storage: &StorageHandle, rows: Vec<Vec<u8>>) {
        let writes = rows
            .into_iter()
            .map(|value| {
                (
                    BLOB_CLEANUP_KEYSPACE.to_string(),
                    Ulid::generate().to_bytes().to_vec().into(),
                    value.into(),
                )
            })
            .collect();
        let event = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn remaining_rows(storage: &StorageHandle) -> usize {
        let (values, _) = iter_prefix_page(storage, BLOB_CLEANUP_KEYSPACE, None, None, 4096, None)
            .await
            .unwrap();
        values.len()
    }

    #[tokio::test]
    async fn releases_renamed_once() {
        use aruna_core::keyspaces::IMPORTED_OBJECT_KEYSPACE;
        use aruna_core::structs::storage::backend_import::{ImportedObject, object_key};
        let (_dir, storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { mut location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            unreachable!()
        };
        (location.storage_bucket, location.backend_path) = (String::new(), "raw/a".to_string());
        let (import_id, version_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let work = |version_id| BlobCleanupWork::ReleaseRenamed {
            location: location.clone(),
            import_id,
            version_id,
            version: "v1".to_string(),
        };
        let row = ImportedObject::written(version_id, "in/a".to_string(), 0, None);
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
                key: object_key(import_id, "raw/a"),
                value: row.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        // The file of this version waits for a blob handle; its row stays until the delete.
        write_rows(&storage, vec![work(version_id).to_bytes().unwrap()]).await;
        let outcome = process_cleanup_batch(&context).await.unwrap();
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);

        // A path that another version took keeps its file, and the release ends.
        let (_dir, storage, context) = setup_context();
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
                key: object_key(import_id, "raw/a"),
                value: row.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let other = work(Ulid::from_parts(3, 3)).to_bytes().unwrap();
        write_rows(&storage, vec![other]).await;
        let outcome = process_cleanup_batch(&context).await.unwrap();
        assert_eq!(outcome.failed, 0);
        assert_eq!(remaining_rows(&storage).await, 0);
    }

    #[tokio::test]
    async fn drops_poison_rows() {
        // An undecodable row must be deleted, and the rows after it still run.
        let (_dir, storage, context) = setup_context();
        write_rows(
            &storage,
            vec![b"not-postcard-at-all".to_vec(), delete_work()],
        )
        .await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, 1);
        // The delete row fails because this context has no blob handle.
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);
    }

    #[tokio::test]
    async fn drops_removed_backend() {
        // A tenant backend whose record is gone can never resolve credentials,
        // so its delete must be dropped instead of retried forever.
        let (_dir, storage, context) = setup_context();
        write_rows(&storage, vec![group_delete_work()]).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, 1);
        assert_eq!(outcome.failed, 0);
        assert_eq!(remaining_rows(&storage).await, 0);
    }

    fn reconcile_work(location: &BackendLocation, announce: bool) -> Vec<u8> {
        BlobCleanupWork::ReconcileWrite {
            location: location.clone(),
            owner: WriteOwner::Blob {
                blake3: [7u8; 32],
                realm_id: location.created_by.realm_id,
                ttl_ms: RoCrateLimits::default().holder_ttl_ms,
                announce,
            },
        }
        .to_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn owned_write_waits() {
        for announce in [true, false] {
            owned_write(announce).await;
        }
    }

    /// A committed copy stays queued until its DHT holder is registered; a copy of a distributed
    /// bucket is never announced and settles at once.
    async fn owned_write(announce: bool) {
        let (_dir, storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            panic!("expected a delete row")
        };
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::new([7u8; 32], EncodingClass::Raw, location.backend.clone())
                    .to_bytes()
                    .into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        write_rows(&storage, vec![reconcile_work(&location, announce)]).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        let waiting = usize::from(announce);
        assert_eq!(outcome.processed, 1 - waiting);
        assert_eq!(outcome.failed, waiting);
        assert_eq!(remaining_rows(&storage).await, waiting);
    }

    #[tokio::test]
    async fn upload_keeps_target() {
        // An upload keeps its in-place target, also while aborting: a failed abort reopens it.
        for status in [MultipartUploadStatus::Open, MultipartUploadStatus::Aborting] {
            let (_dir, storage, context) = setup_context();
            let BlobCleanupWork::DeleteBlob { mut location } =
                BlobCleanupWork::from_bytes(&delete_work()).unwrap()
            else {
                panic!("expected a delete row")
            };
            // Opened long ago, so no create can still be running.
            location.ulid = Ulid::from_parts(1, 3);
            let upload_id = Ulid::generate();
            let record = MultipartUpload {
                upload_id,
                backend: location.backend.clone(),
                storage_class: None,
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                group_id: Ulid::generate(),
                created_by: location.created_by,
                created_at: SystemTime::now(),
                status,
                checksum_hint: None,
                metadata: HashMap::new(),
                placement_policies: Vec::new(),
                subject_generation: 0,
                completing_since_ms: None,
                backend_upload: Some(BackendUpload {
                    location: location.clone(),
                    upload_id: "provider".to_string(),
                    record_id: upload_id,
                }),
                encryption: None,
            };
            let event = storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: UPLOAD_KEYSPACE.to_string(),
                    key: upload_id.to_bytes().to_vec().into(),
                    value: record.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                event,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
            let row = BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Upload { upload_id },
            };
            write_rows(&storage, vec![row.to_bytes().unwrap()]).await;

            let outcome = process_cleanup_batch(&context).await.unwrap();

            // A kept row is the target's reservation and stays.
            assert_eq!(outcome.processed, 0);
            assert_eq!(outcome.kept, 1);
            assert_eq!(outcome.failed, 0);
            assert_eq!(remaining_rows(&storage).await, 1);
        }
    }

    #[tokio::test]
    async fn bounds_each_pass() {
        // A long queue is worked in bounded passes that continue where the last one stopped.
        let (_dir, storage, context) = setup_context();
        let writes = (0..super::PASS_ROWS + 10)
            .map(|index| {
                let key = (index as u32).to_be_bytes().to_vec().into();
                (BLOB_CLEANUP_KEYSPACE.to_string(), key, vec![0xff].into())
            })
            .collect();
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
        let first = process_cleanup_batch(&context).await.unwrap();
        assert!(first.more);
        assert!(first.dropped >= super::PASS_ROWS);
        let second = process_cleanup_batch(&context).await.unwrap();
        assert!(!second.more);
        assert_eq!(first.dropped + second.dropped, super::PASS_ROWS + 10);
    }

    #[tokio::test]
    async fn waits_before_settling() {
        // A recorded copy upload is left alone during its grace, so a copy that still runs
        // keeps it; one past the grace is aborted through the blob storage.
        let (_dir, _storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            panic!("expected a delete row")
        };
        let settle = |since_ms| BlobCleanupWork::SettleUpload {
            attempt: Ulid::from_parts(4, 4),
            location: location.clone(),
            upload_id: "upload".to_string(),
            since_ms,
        };
        let now = aruna_core::time::unix_timestamp_millis();
        let fresh = run_cleanup_work(&context, settle(now)).await;
        assert!(matches!(fresh, RowOutcome::Kept));
        // This context has no blob storage, so the abort cannot run and the row stays.
        let stale = run_cleanup_work(&context, settle(0)).await;
        assert!(matches!(stale, RowOutcome::Failed));
    }

    #[tokio::test]
    async fn drops_prefix_rows() {
        // A removed rule's rows go page by page; rows of other prefixes stay.
        use aruna_core::keyspaces::RENAMED_ROW_KEYSPACE;
        let (_dir, storage, context) = setup_context();
        for key in [
            b"rule-a/1".to_vec(),
            b"rule-a/2".to_vec(),
            b"rule-b/1".to_vec(),
        ] {
            let write = StorageEffect::Write {
                key_space: RENAMED_ROW_KEYSPACE.to_string(),
                key: key.into(),
                value: Vec::new().into(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        let work = || BlobCleanupWork::DropRows {
            key_space: RENAMED_ROW_KEYSPACE.to_string(),
            prefix: b"rule-a/".to_vec(),
        };
        assert!(matches!(
            run_cleanup_work(&context, work()).await,
            RowOutcome::Kept
        ));
        assert!(matches!(
            run_cleanup_work(&context, work()).await,
            RowOutcome::Done
        ));
        let (left, _) = iter_prefix_page(&storage, RENAMED_ROW_KEYSPACE, None, None, 10, None)
            .await
            .unwrap();
        assert_eq!(left.len(), 1);
    }

    #[tokio::test]
    async fn replaced_target_kept() {
        // A completion removed the upload and replaced its target row after this row was
        // scanned: the committed object is left to the new row.
        let (_dir, storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { mut location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            panic!("expected a delete row")
        };
        location.ulid = Ulid::from_parts(1, 3);
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key: location.ulid.to_bytes().to_vec().into(),
                value: reconcile_work(&location, true).into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let target = ReconcileTargetOperation::new(Ulid::generate(), None, location);
        let outcome = drive(target, &context).await.unwrap();

        // Without the recheck the discard runs and fails here for want of a blob handle.
        assert!(matches!(outcome, RowOutcome::Kept));
    }

    #[tokio::test]
    async fn unowned_write_deletes() {
        // Without a location row the commit never landed, so the bytes must
        // go; no blob handle here, so the delete fails and the row waits.
        let (_dir, storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            panic!("expected a delete row")
        };
        write_rows(&storage, vec![reconcile_work(&location, true)]).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);
    }

    #[tokio::test]
    async fn pending_write_owned() {
        // The pending location row is the commit of an archive without a hash.
        use aruna_core::keyspaces::COPY_OWNER_KEYSPACE;
        use aruna_core::keyspaces::PENDING_LOCATION_KEYSPACE;
        use aruna_core::structs::storage::blob::ArchiveKey;
        use aruna_core::structs::storage::blob::{CopyOwner, VersionKey};
        use ulid::Ulid;
        for (owned, promoted) in [(true, false), (false, false), (true, true)] {
            let (_dir, storage, context) = setup_context();
            let BlobCleanupWork::DeleteBlob { location } =
                BlobCleanupWork::from_bytes(&delete_work()).unwrap()
            else {
                panic!("expected a delete row")
            };
            // A promoted archive lost its pending row, but a version still owns it.
            if promoted {
                let version = VersionKey::new("bucket", "key", Ulid::from_bytes([6; 16]));
                let owner = CopyOwner::new(ArchiveKey::of(&location), version);
                let event = storage
                    .send_storage_effect(StorageEffect::Write {
                        key_space: COPY_OWNER_KEYSPACE.to_string(),
                        key: owner.key().unwrap().into(),
                        value: Vec::new().into(),
                        txn_id: None,
                    })
                    .await;
                assert!(matches!(
                    event,
                    Event::Storage(StorageEvent::WriteResult { .. })
                ));
            } else if owned {
                let event = storage
                    .send_storage_effect(StorageEffect::Write {
                        key_space: PENDING_LOCATION_KEYSPACE.to_string(),
                        key: ArchiveKey::of(&location).to_bytes().into(),
                        value: location.to_bytes().unwrap().into(),
                        txn_id: None,
                    })
                    .await;
                assert!(matches!(
                    event,
                    Event::Storage(StorageEvent::WriteResult { .. })
                ));
            }
            let row = BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Pending,
            };
            write_rows(&storage, vec![row.to_bytes().unwrap()]).await;

            let outcome = process_cleanup_batch(&context).await.unwrap();

            // An unowned archive must be deleted, which needs the blob handle this context lacks.
            assert_eq!(outcome.processed, usize::from(owned));
            assert_eq!(outcome.failed, usize::from(!owned));
            assert_eq!(remaining_rows(&storage).await, usize::from(!owned));
        }
    }

    #[tokio::test]
    async fn pages_past_failures() {
        // Rows behind a full page of poison must still be reached in one drain.
        let (_dir, storage, context) = setup_context();
        let rows = std::iter::repeat_n(b"broken".to_vec(), CLEANUP_PAGE_SIZE + 5).collect();
        write_rows(&storage, rows).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, CLEANUP_PAGE_SIZE + 5);
        assert_eq!(remaining_rows(&storage).await, 0);
    }
}
