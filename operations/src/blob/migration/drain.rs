//! Advances queued bucket migrations page by page, hands moved versions over and settles them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::NodeId;
use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, EffectGroup, GroupForm};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, GroupEvent, GroupResult};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE, IMPORT_INTENT_KEYSPACE,
    MIGRATION_AHEAD_KEYSPACE, MOVE_HANDOVER_KEYSPACE, RENAME_MOVE_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::backend_import::{RenameRequest, WriteIntent, object_prefix};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobVersion, ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::format::{
    Compression, CompressionMigration, MigrationAhead, StoredLayout,
};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::batch::{Batch, Stop};
use super::broken::ProcessChecksOperation;
use super::check::{CheckOutcome, CheckVersionError, CheckVersionOperation};
use super::queue::ProcessTransitionsOperation;
use super::shared::{
    EarlierPatternsOperation, MigrationError, encryption_read, is_encrypted, routing_call,
    routing_result,
};
use super::{
    Earlier, MigrateOutcome, MigrateVersionError, MigrateVersionOperation, RULE_PAGE, place,
    same_bytes,
};
use crate::auth::bearer_token::GrantValidOperation;
use crate::auth::error::{ArunaBearerError, AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::{BucketAccess, BucketAccessOperation};
use crate::s3::bucket::job_pages::{ProcessLinkedOperation, StillAdminOperation};

/// Versions one task run handles per bucket before it yields to other timers.
pub(super) const MIGRATION_PAGE: usize = 64;

/// Versions with work one migration run handles per bucket before it stores its progress;
/// the next run follows at once, so this only sets how often progress is saved.
pub(super) const MIGRATION_WORK: usize = 256;

/// Version rows one migration run passes over before it stores its progress, likewise.
const MIGRATION_ROWS: usize = 4096;

/// How often a migration run stores its progress: a run that worked this long starts no more
/// versions, and the next run follows at once.
pub(super) const MIGRATION_TIME: Duration = Duration::from_secs(5);

/// Versions a rename moves at once; the versions of one key move one after another.
const RENAME_PARALLEL: usize = 8;

/// Queued buckets one task run advances; the next run continues after the last one.
pub(super) const BUCKET_PAGE: usize = 16;

/// Delay between task runs while hand-overs, storage checks or encryption transitions go on.
pub const MIGRATION_CONTINUE: Duration = Duration::from_secs(1);

/// Passes started again after a pass with failures, before the migration ends with them.
const MIGRATION_RETRIES: u32 = 10;

/// Wait before the first retry pass; it doubles per pass up to `RETRY_MAX`.
pub(super) const RETRY_BASE: Duration = Duration::from_secs(60);

pub(super) const RETRY_MAX: Duration = Duration::from_secs(3600);

/// Intent pages one hand-over run writes at most before it stores its place.
const HANDOVER_PAGES: usize = 16;

type Turn<T> = Result<Next<T>, MigrationError>;
/// This node and its realm, where it serves one.
type Serving = Option<(NodeId, RealmId)>;

fn bucket_key(bucket: &str) -> Key {
    bucket.as_bytes().to_vec().into()
}

fn soonest(next: &mut Option<Duration>, after: Duration) {
    *next = Some(next.map_or(after, |next| next.min(after)));
}

/// The rename request `value` holds, if the move `record` was started for it.
pub(super) fn decode_request(
    value: Option<Value>,
    record: (bool, u64),
) -> Result<Option<RenameRequest>, MigrationError> {
    let request = value
        .map(|value| RenameRequest::from_bytes(&value))
        .transpose()
        .map_err(MigrationError::failed)?;
    let (moving, started_at_ms) = record;
    Ok(request.filter(|request| moving && request.started_at_ms == started_at_ms))
}

/// A denial ends the rename; policy state that cannot be read is an error to retry.
pub(super) fn granted<T>(loaded: Result<T, AuthorizeError>) -> Result<Option<T>, String> {
    match loaded {
        Ok(access) => Ok(Some(access)),
        Err(AuthorizeError::PermissionDenied)
        | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum AccessStep {
    Grant,
    Admin,
    Bucket,
}

/// The caller's WRITE on the bucket's keys, loaded once for a page of a rename; `None` when
/// the caller's token no longer holds or the caller may not use the bucket any more. State
/// that cannot be read is an error, so the page is retried rather than passed over.
#[derive(Debug, PartialEq)]
pub(super) struct RenameAccessOperation {
    request: RenameRequest,
    group_id: ulid::Ulid,
    bucket: String,
    step: AccessStep,
}

impl RenameAccessOperation {
    pub(super) fn new(request: &RenameRequest, group_id: ulid::Ulid, bucket: &str) -> Self {
        Self {
            request: request.clone(),
            group_id,
            bucket: bucket.to_string(),
            step: AccessStep::Grant,
        }
    }
}

impl Operation for RenameAccessOperation {
    type Output = Option<BucketAccess>;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Option<BucketAccess>> {
        let request = &self.request;
        let valid = GrantValidOperation::new(&request.grant, &request.auth.user_id);
        Ok(Next::Call(IoCall::child(valid)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<BucketAccess>> {
        let request = &self.request;
        match self.step {
            AccessStep::Grant => {
                let valid =
                    (result.child::<bool, ArunaBearerError>()?).map_err(MigrationError::failed)?;
                if !valid {
                    return Ok(Next::Done(None));
                }
                let admin = StillAdminOperation::new(&request.auth, self.group_id);
                self.step = AccessStep::Admin;
                Ok(Next::Call(IoCall::child(admin)))
            }
            AccessStep::Admin => {
                if !result.child::<bool, MigrationError>()?? {
                    return Ok(Next::Done(None));
                }
                let scope = (request.auth.realm_id, self.group_id, request.node_id);
                let load = BucketAccessOperation::new(&request.auth, scope, &self.bucket);
                self.step = AccessStep::Bucket;
                Ok(Next::Call(IoCall::child(load)))
            }
            AccessStep::Bucket => {
                let loaded = result.child::<BucketAccess, AuthorizeError>()?;
                let access = granted(loaded).map_err(MigrationError::Failed)?;
                let write = Permission::WRITE;
                let access = access.filter(|access| access.allows(None, &write));
                Ok(Next::Done(access))
            }
        }
    }
}

/// What one task run left behind.
#[derive(Debug, PartialEq)]
pub struct MigrationRun {
    /// When the task must run again; `None` once no queued migration has work left.
    pub next: Option<Duration>,
    /// Queue key the next run resumes after; `None` starts at the head.
    pub cursor: Option<Key>,
}

#[derive(Debug, PartialEq)]
enum RunStep {
    Queue,
    Progress(Key),
    Encryption(String, CompressionMigration),
    Handover(String, CompressionMigration),
    Retired,
    Page,
    Transitions,
    Checks,
    Linked,
}

/// Advances the unfinished migrations of one page of queued buckets by one page each.
#[derive(Debug, PartialEq)]
pub struct ProcessMigrationsOperation {
    routing: NodeRouting,
    serving: Serving,
    after: Option<Key>,
    resumed: bool,
    now_ms: u64,
    step: RunStep,
    rows: VecDeque<Key>,
    cursor: Option<Key>,
    next: Option<Duration>,
}

type RunTurn = Turn<MigrationRun>;

impl ProcessMigrationsOperation {
    pub fn new(routing: NodeRouting, serving: Serving, after: Option<Key>) -> Self {
        Self {
            routing,
            serving,
            resumed: after.is_some(),
            after,
            now_ms: 0,
            step: RunStep::Queue,
            rows: VecDeque::new(),
            cursor: None,
            next: None,
        }
    }

    fn call(&mut self, step: RunStep, call: IoCall) -> RunTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn next_bucket(&mut self) -> RunTurn {
        match self.rows.pop_front() {
            Some(key) => {
                let read = read_row(COMPRESSION_MIGRATION_KEYSPACE, key.clone());
                self.call(RunStep::Progress(key), read)
            }
            None => self.shared_work(),
        }
    }

    fn progress_read(&mut self, key: Key, value: Option<Value>) -> RunTurn {
        let Some(record) = decode_progress(value)? else {
            return self.next_bucket();
        };
        if record.finished_at_ms.is_some() {
            return self.next_bucket();
        }
        if let Some(at) = record.retry_at_ms.filter(|at| *at > self.now_ms) {
            soonest(&mut self.next, Duration::from_millis(at - self.now_ms));
            return self.next_bucket();
        }
        let bucket = String::from_utf8(key.to_vec()).map_err(MigrationError::failed)?;
        let read = encryption_read(&bucket);
        self.call(RunStep::Encryption(bucket, record), read)
    }

    /// Encryption replaced this work: its transition converts every version.
    fn retire(&mut self, bucket: String, mut record: CompressionMigration) -> RunTurn {
        record.retry_at_ms = None;
        record.finished_at_ms = Some(self.now_ms);
        let store = StoreMigrationOperation::new(&bucket, record, None);
        self.call(RunStep::Retired, IoCall::child(store))
    }

    /// Encryption transitions, storage checks and the jobs of buckets share this task; a run at
    /// the queue head advances them all.
    fn shared_work(&mut self) -> RunTurn {
        if self.resumed {
            return self.finish();
        }
        let transitions = ProcessTransitionsOperation::new(self.serving);
        self.call(RunStep::Transitions, IoCall::child(transitions))
    }

    fn take_wait(&mut self, result: IoResult) -> Result<(), MigrationError> {
        if let Some(after) = result.child::<Option<Duration>, MigrationError>()?? {
            soonest(&mut self.next, after);
        }
        Ok(())
    }

    fn finish(&mut self) -> RunTurn {
        // Queued buckets after this page, or before it when the run resumed mid-queue.
        if self.cursor.is_some() || self.resumed {
            soonest(&mut self.next, MIGRATION_CONTINUE);
        }
        Ok(Next::Done(MigrationRun {
            next: self.next,
            cursor: self.cursor.take(),
        }))
    }
}

impl Operation for ProcessMigrationsOperation {
    type Output = MigrationRun;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RunTurn {
        Ok(Next::Call(IoCall::Page {
            space: COMPRESSION_QUEUE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.take(),
            limit: BUCKET_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> RunTurn {
        match std::mem::replace(&mut self.step, RunStep::Page) {
            RunStep::Queue => {
                let (rows, cursor) = result.page()?;
                self.cursor = cursor;
                self.rows = rows.into_iter().map(|(key, _)| key).collect();
                self.next_bucket()
            }
            RunStep::Progress(key) => self.progress_read(key, result.value()?),
            RunStep::Encryption(bucket, record) => {
                if !is_encrypted(result.value()?)? {
                    let budget = MIGRATION_TIME;
                    let page =
                        MigratePageOperation::new(self.routing.clone(), &bucket, record, budget);
                    return self.call(RunStep::Page, IoCall::child(page));
                }
                if !record.moving {
                    return self.retire(bucket, record);
                }
                let started = record.started_at_ms;
                let hand_over = HandOverOperation::new(self.routing.clone(), &bucket, started);
                self.call(RunStep::Handover(bucket, record), IoCall::child(hand_over))
            }
            RunStep::Handover(bucket, record) => {
                if result.child::<bool, MigrationError>()?? {
                    return self.retire(bucket, record);
                }
                soonest(&mut self.next, MIGRATION_CONTINUE);
                self.next_bucket()
            }
            RunStep::Retired => {
                result.child::<(), MigrationError>()??;
                self.next_bucket()
            }
            RunStep::Page => {
                self.take_wait(result)?;
                self.next_bucket()
            }
            RunStep::Transitions => {
                self.take_wait(result)?;
                let checks = ProcessChecksOperation::new();
                self.call(RunStep::Checks, IoCall::child(checks))
            }
            RunStep::Checks => {
                self.take_wait(result)?;
                let linked = ProcessLinkedOperation::new(self.routing.clone());
                self.call(RunStep::Linked, IoCall::child(linked))
            }
            RunStep::Linked => {
                self.take_wait(result)?;
                self.finish()
            }
        }
    }
}

fn decode_progress(value: Option<Value>) -> Result<Option<CompressionMigration>, MigrationError> {
    value
        .map(|value| CompressionMigration::from_bytes(value.as_ref()))
        .transpose()
        .map_err(MigrationError::failed)
}

/// How one version of a run ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowEnd {
    /// The rename was paused or aborted before the version started.
    Halted,
    Migrated,
    Skipped,
    /// A move found a copy it cannot read, which a storage check recorded.
    Broken,
    Failed,
}

/// What a version of a run moves with, when it moves.
#[derive(Debug, PartialEq)]
struct RowMove {
    routing: RoutingSnapshot,
    earlier: Earlier,
    started_ms: u64,
}

#[derive(Debug, PartialEq)]
enum RowStep {
    Rename,
    Migrate,
    Broken(String),
}

/// Checks that a rename still runs, migrates one version, and checks a copy a move could not
/// read.
#[derive(Debug, PartialEq)]
struct MigrateRowOperation {
    bucket: String,
    version: VersionKey,
    target: Compression,
    moved: Option<RowMove>,
    moving: bool,
    /// The moving flag and start of a move whose rename must still run.
    rename: Option<(bool, u64)>,
    now_ms: u64,
    step: RowStep,
}

impl MigrateRowOperation {
    fn migrate(&mut self) -> Turn<RowEnd> {
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
        let mut operation = MigrateVersionOperation::new(self.version.clone(), self.target, now);
        if let Some(moved) = self.moved.take() {
            self.moving = true;
            operation = (operation.with_routing(moved.routing))
                .with_earlier(moved.earlier)
                .with_mover(moved.started_ms);
        }
        self.step = RowStep::Migrate;
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn migrated(&mut self, moved: Result<MigrateOutcome, MigrateVersionError>) -> Turn<RowEnd> {
        let error = match moved {
            Ok(MigrateOutcome::Migrated) => return Ok(Next::Done(RowEnd::Migrated)),
            Ok(MigrateOutcome::Skipped) => return Ok(Next::Done(RowEnd::Skipped)),
            Err(error) => error,
        };
        // A taken name says nothing about the old copy, so the provider is not asked about it.
        let taken = matches!(error, MigrateVersionError::Blob(BlobError::PathTaken));
        if !self.moving || taken {
            return self.failed(&error.to_string());
        }
        self.step = RowStep::Broken(error.to_string());
        let check = CheckVersionOperation::new(self.version.clone(), self.now_ms);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn failed(&self, error: &str) -> Turn<RowEnd> {
        tracing::warn!(bucket = %self.bucket, %error, "Failed to re-encode a version");
        Ok(Next::Done(RowEnd::Failed))
    }
}

impl Operation for MigrateRowOperation {
    type Output = RowEnd;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<RowEnd> {
        if self.rename.is_some() {
            return Ok(Next::Call(read_row(
                RENAME_MOVE_KEYSPACE,
                bucket_key(&self.bucket),
            )));
        }
        self.migrate()
    }

    fn next(&mut self, result: IoResult) -> Turn<RowEnd> {
        match std::mem::replace(&mut self.step, RowStep::Migrate) {
            RowStep::Rename => {
                let record = self.rename.unwrap_or((false, 0));
                let request = decode_request(result.value()?, record)?;
                if request.is_none_or(|request| request.paused || request.aborted_at_ms.is_some()) {
                    return Ok(Next::Done(RowEnd::Halted));
                }
                self.migrate()
            }
            RowStep::Migrate => self.migrated(result.child()?),
            RowStep::Broken(error) => {
                let checked = result.child::<CheckOutcome, CheckVersionError>()?;
                match checked {
                    Ok(CheckOutcome::Broken(_)) => Ok(Next::Done(RowEnd::Broken)),
                    _ => self.failed(&error),
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Handover,
    HandedOver,
    Routing,
    Request,
    Access,
    Earlier,
    Versions,
    Ahead,
    Window,
    StopRequest,
    FinalHandOver,
    /// The child that decides the answer of this operation.
    Tail,
    Store(Option<Duration>),
}

/// Migrates one page of a bucket's versions and stores the progress, unless a newer setting
/// replaced the record meanwhile. A run that worked for `budget` starts no more versions.
/// Outputs when the bucket needs the next run.
#[derive(Debug, PartialEq)]
pub(super) struct MigratePageOperation {
    routing: NodeRouting,
    bucket: String,
    record: CompressionMigration,
    budget: Duration,
    begun_ms: u64,
    now_ms: u64,
    step: PageStep,
    snapshot: Option<RoutingSnapshot>,
    request: Option<RenameRequest>,
    access: Option<Arc<BucketAccess>>,
    earlier: Earlier,
    versions: Vec<Key>,
    keys: Vec<VersionKey>,
    more: bool,
    batch: Batch,
    limit: usize,
    /// The row of each window member, in the order they started.
    members: Vec<usize>,
    started: usize,
    worked: usize,
    denied: usize,
    stop: Option<Stop>,
    failure: Option<MigrationError>,
    pending_after: Option<MigrationAhead>,
}

type PageTurn = Turn<Option<Duration>>;

impl MigratePageOperation {
    pub(super) fn new(
        routing: NodeRouting,
        bucket: &str,
        record: CompressionMigration,
        budget: Duration,
    ) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            record,
            budget,
            begun_ms: 0,
            now_ms: 0,
            step: PageStep::Handover,
            snapshot: None,
            request: None,
            access: None,
            earlier: Earlier::new(),
            versions: Vec::new(),
            keys: Vec::new(),
            more: false,
            batch: Batch::new(Vec::new()),
            limit: 1,
            members: Vec::new(),
            started: 0,
            worked: 0,
            denied: 0,
            stop: None,
            failure: None,
            pending_after: None,
        }
    }

    fn call(&mut self, step: PageStep, call: IoCall) -> PageTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn store(&mut self, ahead: Option<MigrationAhead>, after: Option<Duration>) -> PageTurn {
        let store = StoreMigrationOperation::new(&self.bucket, self.record.clone(), ahead);
        self.call(PageStep::Store(after), IoCall::child(store))
    }

    fn end_now(&mut self) -> PageTurn {
        self.record.finished_at_ms = Some(self.now_ms);
        self.store(None, None)
    }

    fn end_aborted(&mut self) -> PageTurn {
        let end = EndAbortedOperation::new(self.routing.clone(), &self.bucket, self.record.clone());
        self.call(PageStep::Tail, IoCall::child(end))
    }

    fn hand_over(&self) -> IoCall {
        let started = self.record.started_at_ms;
        IoCall::child(HandOverOperation::new(
            self.routing.clone(),
            &self.bucket,
            started,
        ))
    }

    /// A due retry pass counts its own skips and failures.
    fn after_handover(&mut self) -> PageTurn {
        if self.record.retry_at_ms.take().is_some() {
            self.record.skipped = 0;
            self.record.failed = 0;
        }
        if !self.record.moving {
            return self.read_versions();
        }
        let call = routing_call(self.routing.clone(), &self.bucket);
        self.call(PageStep::Routing, call)
    }

    /// Without earlier patterns no file is renamed, so only an asked rename reads them; each
    /// file is renamed only while the caller who asked may still write its key.
    fn request_read(&mut self, value: Option<Value>) -> PageTurn {
        let record = (self.record.moving, self.record.started_at_ms);
        self.request = decode_request(value, record)?;
        let Some(request) = self.request.as_ref() else {
            return self.read_versions();
        };
        // An aborted rename ends here with its counts, once no version of it is in flight.
        if request.aborted_at_ms.is_some() {
            return self.end_aborted();
        }
        // A paused rename waits for its resume, which wakes this task again.
        if request.paused {
            return Ok(Next::Done(None));
        }
        let Some(routing) = self.snapshot.as_ref() else {
            return self.read_versions();
        };
        let access = RenameAccessOperation::new(request, routing.group_id, &self.bucket);
        self.call(PageStep::Access, IoCall::child(access))
    }

    fn read_versions(&mut self) -> PageTurn {
        let page = IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: VersionKey::bucket_prefix(&self.bucket)?.into(),
            after: self.record.cursor.clone().map(Into::into),
            limit: MIGRATION_ROWS,
        };
        self.call(PageStep::Versions, page)
    }

    /// The routing a key moves with; keys outside a move's prefix are only re-encoded.
    fn moves(&self, key: &VersionKey) -> Option<&RoutingSnapshot> {
        (self.snapshot.as_ref()).filter(|_| key.key.starts_with(&self.record.prefix))
    }

    fn versions_read(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> PageTurn {
        self.more = next.is_some();
        self.versions = rows.into_iter().map(|(key, _)| key).collect();
        self.keys = (self.versions.iter())
            .map(|key| VersionKey::from_bytes(key.as_ref()))
            .collect::<Result<_, _>>()?;
        // A rename moves several keys at once, but never two versions of one key or one name.
        self.limit = match self.request {
            Some(_) => RENAME_PARALLEL,
            None => 1,
        };
        let name = |key: &VersionKey| {
            let routing = self.moves(key).filter(|_| self.limit > 1)?;
            Some(place(routing, key).ok()?.1?.path)
        };
        let rows = self.keys.iter().map(|key| (key.key.clone(), name(key)));
        let batch = Batch::new(rows.collect());
        self.batch = batch;
        let read = read_row(MIGRATION_AHEAD_KEYSPACE, bucket_key(&self.bucket));
        self.call(PageStep::Ahead, read)
    }

    /// Rows the last run of this pass finished after its cursor already count, so they pass.
    fn ahead_read(&mut self, value: Option<Value>) -> PageTurn {
        let ahead = value
            .map(|value| MigrationAhead::from_bytes(&value))
            .transpose()?;
        let ahead: HashSet<Vec<u8>> = ahead
            .map(|ahead| ahead.rows_for(&self.record))
            .unwrap_or_default()
            .into_iter()
            .collect();
        for (row, key) in self.versions.iter().enumerate() {
            if ahead.contains(key.as_ref()) {
                self.batch.passed(row);
            }
        }
        self.begun_ms = self.now_ms;
        let mut members = Vec::new();
        while let Some(row) = self.ready_row() {
            members.push(self.row_member(row));
        }
        if members.is_empty() {
            return self.after_window();
        }
        let group = EffectGroup {
            members,
            limit: self.limit,
            form: GroupForm::Window,
        };
        self.call(PageStep::Window, IoCall::Group(group))
    }

    /// The next row to start. A run with enough work or time starts no more, and stores its
    /// progress; a pause or an abort is seen by the row itself.
    fn ready_row(&mut self) -> Option<usize> {
        if self.stop.is_some() || self.failure.is_some() || self.batch.running() >= self.limit {
            return None;
        }
        let row = self.batch.ready()?;
        let elapsed = aruna_core::time::unix_timestamp_millis().saturating_sub(self.begun_ms);
        let spent = Duration::from_millis(elapsed) >= self.budget || self.worked >= MIGRATION_WORK;
        if self.started > 0 && spent {
            self.stop = Some(Stop::More);
            return None;
        }
        self.started += 1;
        self.batch.start(row);
        self.members.push(row);
        Some(row)
    }

    fn row_operation(&mut self, row: usize) -> MigrateRowOperation {
        let version = self.keys[row].clone();
        let moved = self.moves(&version).cloned().map(|routing| {
            let write = Permission::WRITE;
            let access = self.access.as_ref();
            let allowed = access.is_some_and(|access| access.allows(Some(&version.key), &write));
            let denied = !self.earlier.is_empty() && !allowed;
            let earlier = match allowed {
                true => self.earlier.clone(),
                false => Earlier::new(),
            };
            let started_ms = self.record.started_at_ms;
            let moved = RowMove {
                routing,
                earlier,
                started_ms,
            };
            (moved, denied)
        });
        let moved = moved.map(|(moved, denied)| {
            self.denied += usize::from(denied);
            moved
        });
        let rename =
            (self.request.is_some()).then_some((self.record.moving, self.record.started_at_ms));
        MigrateRowOperation {
            bucket: self.bucket.clone(),
            version,
            target: self.record.target,
            moved,
            moving: false,
            step: match rename {
                Some(_) => RowStep::Rename,
                None => RowStep::Migrate,
            },
            rename,
            now_ms: 0,
        }
    }

    fn row_member(&mut self, row: usize) -> Effect {
        let operation = self.row_operation(row);
        Effect::SubOperation(boxed_suboperation(operation, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }

    fn row_ended(&mut self, row: usize, end: Result<RowEnd, MigrationError>) {
        let end = match end {
            Ok(end) => end,
            Err(error) => {
                self.batch.abandon(row);
                self.failure.get_or_insert(error);
                return;
            }
        };
        if end == RowEnd::Halted {
            self.batch.abandon(row);
            self.stop = Some(Stop::Halted);
            return;
        }
        self.batch.finish(row);
        self.worked += usize::from(end != RowEnd::Skipped);
        match end {
            RowEnd::Migrated => self.record.migrated += 1,
            // A move records a copy that cannot be read as broken instead of retrying it.
            RowEnd::Skipped | RowEnd::Broken => self.record.skipped += 1,
            RowEnd::Failed => self.record.failed += 1,
            RowEnd::Halted => {}
        }
    }

    fn window_event(&mut self, event: GroupEvent) -> PageTurn {
        let (index, result) = match event {
            GroupEvent::Member { index, result } => (index, result),
            GroupEvent::Closed => return self.after_window(),
            GroupEvent::Finished { .. } => {
                return Err(MigrationError::Failed(
                    "unexpected group answer".to_string(),
                ));
            }
        };
        let row = (self.members.get(index).copied())
            .ok_or_else(|| MigrationError::Failed("unknown window member".to_string()))?;
        let end = match result {
            GroupResult::Done(event) => match *event {
                Event::Child(outcome) => {
                    IoResult::Child(outcome).child::<RowEnd, MigrationError>()?
                }
                other => Err(MigrationError::Failed(other.label().to_string())),
            },
            other => Err(MigrationError::Failed(format!("{other:?}"))),
        };
        self.row_ended(row, end);
        self.step = PageStep::Window;
        match self.ready_row() {
            Some(row) => {
                let operation = self.row_operation(row);
                Ok(Next::Call(IoCall::child(operation)))
            }
            None => Ok(Next::Wait),
        }
    }

    /// Each row this run finished counts once; the next run starts after the rows finished in
    /// storage order and passes over those finished after them.
    fn after_window(&mut self) -> PageTurn {
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        let done = self.batch.done();
        let rows = (self.batch.finished_after()).map(|row| self.versions[row].to_vec());
        let rows = rows.collect();
        if let Some(last) = done.checked_sub(1) {
            self.record.cursor = Some(self.versions[last].to_vec());
        }
        let ahead = MigrationAhead {
            started_at_ms: self.record.started_at_ms,
            retries: self.record.retries,
            cursor: self.record.cursor.clone(),
            rows,
        };
        let more = self.more || done < self.versions.len();
        self.pending_after = Some(ahead);
        if self.stop == Some(Stop::Halted) {
            let read = read_row(RENAME_MOVE_KEYSPACE, bucket_key(&self.bucket));
            return self.call(PageStep::StopRequest, read);
        }
        if self.denied > 0 {
            tracing::warn!(
                bucket = %self.bucket,
                denied = self.denied,
                "Files the rename's caller may not write keep their name"
            );
        }
        let record = &mut self.record;
        let after = match (more, record.failed > 0) {
            // This run moved the cursor, so the rows left can go on at once.
            (true, _) => Some(Duration::ZERO),
            // Successful versions are already in the target, so a retry pass reads no blob for
            // them.
            (false, true) if record.retries < MIGRATION_RETRIES => {
                let wait = capped_backoff(RETRY_BASE, RETRY_MAX, record.retries, 0);
                record.retries += 1;
                record.cursor = None;
                record.retry_at_ms = Some(self.now_ms.saturating_add(wait.as_millis() as u64));
                Some(wait)
            }
            // A move that ends hands every attempt it still owns to the cleanup queue first;
            // a long hand-over goes on in the next runs.
            (false, _) if record.moving => {
                let call = self.hand_over();
                return self.call(PageStep::FinalHandOver, call);
            }
            (false, _) => {
                record.finished_at_ms = Some(self.now_ms);
                None
            }
        };
        self.store_ahead(after)
    }

    fn store_ahead(&mut self, after: Option<Duration>) -> PageTurn {
        let ahead = self.pending_after.take();
        self.store(ahead, after)
    }
}

impl Operation for MigratePageOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PageTurn {
        // A hand-over under way finishes before anything else of the move.
        if self.record.moving {
            let read = read_row(MOVE_HANDOVER_KEYSPACE, bucket_key(&self.bucket));
            return Ok(Next::Call(read));
        }
        self.after_handover()
    }

    fn next(&mut self, result: IoResult) -> PageTurn {
        match std::mem::replace(&mut self.step, PageStep::Tail) {
            PageStep::Handover => {
                let handover = decode_handover(result.value()?)?;
                let started = self.record.started_at_ms;
                if handover.is_some_and(|handover| handover.started_ms == started) {
                    let call = self.hand_over();
                    return self.call(PageStep::HandedOver, call);
                }
                self.after_handover()
            }
            PageStep::HandedOver => match result.child::<bool, MigrationError>()?? {
                true => self.end_now(),
                false => Ok(Next::Done(Some(MIGRATION_CONTINUE))),
            },
            PageStep::Routing => {
                self.snapshot = routing_result(result)?;
                if self.snapshot.is_none() {
                    return self.end_now();
                }
                let read = read_row(RENAME_MOVE_KEYSPACE, bucket_key(&self.bucket));
                self.call(PageStep::Request, read)
            }
            PageStep::Request => self.request_read(result.value()?),
            PageStep::Access => {
                let access = result.child::<Option<BucketAccess>, MigrationError>()??;
                let Some(access) = access else {
                    return self.read_versions();
                };
                self.access = Some(Arc::new(access));
                let Some(routing) = self.snapshot.as_ref() else {
                    return self.read_versions();
                };
                let earlier = EarlierPatternsOperation::new(routing);
                self.call(PageStep::Earlier, IoCall::child(earlier))
            }
            PageStep::Earlier => {
                self.earlier = result.child::<Earlier, MigrationError>()??;
                self.read_versions()
            }
            PageStep::Versions => {
                let (rows, next) = result.page()?;
                self.versions_read(rows, next)
            }
            PageStep::Ahead => self.ahead_read(result.value()?),
            PageStep::Window => match result {
                IoResult::Group(event) => self.window_event(event),
                other => Err(MigrationError::Failed(other.label().to_string())),
            },
            PageStep::StopRequest => {
                let record = (self.record.moving, self.record.started_at_ms);
                let request = decode_request(result.value()?, record)?;
                if request.is_some_and(|request| request.aborted_at_ms.is_some()) {
                    return self.end_aborted();
                }
                self.store_ahead(None)
            }
            PageStep::FinalHandOver => match result.child::<bool, MigrationError>()?? {
                true => {
                    self.record.finished_at_ms = Some(self.now_ms);
                    self.store_ahead(None)
                }
                false => self.store_ahead(Some(MIGRATION_CONTINUE)),
            },
            PageStep::Tail => {
                let after = result.child::<Option<Duration>, MigrationError>()??;
                Ok(Next::Done(after))
            }
            PageStep::Store(after) => {
                result.child::<(), MigrationError>()??;
                Ok(Next::Done(after))
            }
        }
    }
}

/// Ends an aborted rename's move once its intents are handed to the cleanup queue, which
/// settles them apart from this move; its request stays to show the abort.
#[derive(Debug, PartialEq)]
pub(super) struct EndAbortedOperation {
    routing: NodeRouting,
    bucket: String,
    record: CompressionMigration,
    now_ms: u64,
    storing: bool,
}

impl EndAbortedOperation {
    pub(super) fn new(routing: NodeRouting, bucket: &str, record: CompressionMigration) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            record,
            now_ms: 0,
            storing: false,
        }
    }
}

impl Operation for EndAbortedOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PageTurn {
        let started = self.record.started_at_ms;
        let hand_over = HandOverOperation::new(self.routing.clone(), &self.bucket, started);
        Ok(Next::Call(IoCall::child(hand_over)))
    }

    fn next(&mut self, result: IoResult) -> PageTurn {
        if self.storing {
            result.child::<(), MigrationError>()??;
            return Ok(Next::Done(None));
        }
        if !result.child::<bool, MigrationError>()?? {
            return Ok(Next::Done(Some(MIGRATION_CONTINUE)));
        }
        self.record.retry_at_ms = None;
        self.record.finished_at_ms = Some(self.now_ms);
        self.storing = true;
        let store = StoreMigrationOperation::new(&self.bucket, self.record.clone(), None);
        Ok(Next::Call(IoCall::child(store)))
    }
}

/// Where a move's hand-over of its intents stands: the move, the rule and the last intent.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct Handover {
    pub(super) started_ms: u64,
    pub(super) import_id: Option<Ulid>,
    pub(super) after: Option<Vec<u8>>,
}

pub(super) fn decode_handover(value: Option<Value>) -> Result<Option<Handover>, MigrationError> {
    value
        .map(|value| postcard::from_bytes(&value))
        .transpose()
        .map_err(MigrationError::failed)
}

/// The row that stores `handover`, or the delete that ends it.
pub(super) fn handover_call(
    bucket: &str,
    handover: Option<&Handover>,
) -> Result<IoCall, MigrationError> {
    let key = bucket_key(bucket);
    let Some(handover) = handover else {
        return Ok(IoCall::Delete(vec![(
            MOVE_HANDOVER_KEYSPACE.to_string(),
            key,
        )]));
    };
    let value = postcard::to_allocvec(handover).map_err(MigrationError::failed)?;
    let row = (MOVE_HANDOVER_KEYSPACE.to_string(), key, value.into());
    Ok(IoCall::Write(vec![row]))
}

#[derive(Debug, PartialEq)]
enum HandStep {
    Routing,
    Place,
    Page,
    Written,
    Stored(bool),
}

/// Writes one `SettleMove` work per intent this move still owns, with the intent itself, a
/// bounded number of pages per run from a stored place. Outputs `true` once all are handed over.
#[derive(Debug, PartialEq)]
pub(super) struct HandOverOperation {
    routing: NodeRouting,
    bucket: String,
    started_ms: u64,
    step: HandStep,
    rules: Vec<Ulid>,
    place: Option<Handover>,
    pages: usize,
    more: Option<Key>,
}

impl HandOverOperation {
    pub(super) fn new(routing: NodeRouting, bucket: &str, started_ms: u64) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            started_ms,
            step: HandStep::Routing,
            rules: Vec::new(),
            place: None,
            pages: 0,
            more: None,
        }
    }

    fn store(&mut self, place: Option<&Handover>, finished: bool) -> Turn<bool> {
        let call = handover_call(&self.bucket, place)?;
        self.step = HandStep::Stored(finished);
        Ok(Next::Call(call))
    }

    fn next_page(&mut self) -> Turn<bool> {
        if self.pages == HANDOVER_PAGES {
            let place = self.place.take();
            return self.store(place.as_ref(), false);
        }
        let Some(place) = self.place.as_mut() else {
            return self.store(None, true);
        };
        let Some(import_id) = place.import_id else {
            return self.store(None, true);
        };
        self.pages += 1;
        let after = place.after.take().map(Into::into);
        self.step = HandStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: IMPORT_INTENT_KEYSPACE.to_string(),
            prefix: object_prefix(import_id),
            after,
            limit: RULE_PAGE,
        }))
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> Turn<bool> {
        let mut writes = Vec::new();
        for (key, value) in rows {
            let intent = WriteIntent::from_bytes(&value)?;
            // Only this move's own intents; an upload's or another move's stay.
            if intent.mover != Some(self.started_ms) {
                continue;
            }
            let version = VersionKey::new(&self.bucket, &intent.key, intent.version_id);
            let work = BlobCleanupWork::SettleMove {
                version,
                intents: vec![(key.to_vec(), intent)],
            };
            let value = work.to_bytes()?.into();
            writes.push((BLOB_CLEANUP_KEYSPACE.to_string(), key, value));
        }
        self.more = next;
        if writes.is_empty() {
            return self.advance_place();
        }
        self.step = HandStep::Written;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn advance_place(&mut self) -> Turn<bool> {
        let next = self.more.take();
        if let Some(place) = self.place.as_mut() {
            match next {
                Some(next) => place.after = Some(next.to_vec()),
                None => {
                    // Rules are sorted; a removed rule still has its place among them.
                    let current = place.import_id;
                    place.import_id = (self.rules.iter())
                        .find(|rule| Some(**rule) > current)
                        .copied();
                }
            }
        }
        self.next_page()
    }
}

impl Operation for HandOverOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        Ok(Next::Call(routing_call(self.routing.clone(), &self.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match std::mem::replace(&mut self.step, HandStep::Page) {
            HandStep::Routing => {
                let Some(routing) = routing_result(result)? else {
                    return self.store(None, true);
                };
                self.rules = (routing.imports.iter())
                    .filter(|rule| rule.bucket == self.bucket)
                    .map(|rule| rule.import_id)
                    .collect();
                self.rules.sort();
                self.step = HandStep::Place;
                let read = read_row(MOVE_HANDOVER_KEYSPACE, bucket_key(&self.bucket));
                Ok(Next::Call(read))
            }
            HandStep::Place => {
                let stored = decode_handover(result.value()?)?;
                let started_ms = self.started_ms;
                let stored = stored.filter(|handover| handover.started_ms == started_ms);
                self.place = Some(stored.unwrap_or(Handover {
                    started_ms,
                    import_id: self.rules.first().copied(),
                    after: None,
                }));
                self.next_page()
            }
            HandStep::Page => {
                let (rows, next) = result.page()?;
                self.page_read(rows, next)
            }
            HandStep::Written => {
                result.done()?;
                self.advance_place()
            }
            HandStep::Stored(finished) => {
                result.done()?;
                Ok(Next::Done(finished))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StoreStep {
    Current,
    Progress,
    Ahead,
    Request,
    Left,
}

/// Stores the progress, with the rows a run finished ahead of its cursor where given, unless a
/// newer setting replaced the record. A finished migration leaves the queue and its rename
/// request with its last progress; the request of an aborted rename stays to show the abort.
#[derive(Debug, PartialEq)]
pub(super) struct StoreMigrationOperation {
    key: Key,
    record: CompressionMigration,
    ahead: Option<MigrationAhead>,
    step: StoreStep,
}

impl StoreMigrationOperation {
    pub(super) fn new(
        bucket: &str,
        record: CompressionMigration,
        ahead: Option<MigrationAhead>,
    ) -> Self {
        Self {
            key: bucket_key(bucket),
            record,
            ahead,
            step: StoreStep::Current,
        }
    }

    /// The rows go in the transaction of the progress, so they count exactly when it does;
    /// without any the row goes.
    fn store_ahead(&mut self) -> Turn<()> {
        let Some(ahead) = self.ahead.take() else {
            return self.leave();
        };
        self.step = StoreStep::Ahead;
        let space = MIGRATION_AHEAD_KEYSPACE.to_string();
        let key = self.key.clone();
        if ahead.rows.is_empty() {
            return Ok(Next::Call(IoCall::Delete(vec![(space, key)])));
        }
        Ok(Next::Call(IoCall::Write(vec![(
            space,
            key,
            ahead.to_bytes()?.into(),
        )])))
    }

    fn leave(&mut self) -> Turn<()> {
        if self.record.finished_at_ms.is_none() {
            return Ok(Next::Done(()));
        }
        self.step = StoreStep::Request;
        Ok(Next::Call(read_row(RENAME_MOVE_KEYSPACE, self.key.clone())))
    }
}

impl Operation for StoreMigrationOperation {
    type Output = ();
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<()> {
        let read = read_row(COMPRESSION_MIGRATION_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            StoreStep::Current => {
                let Some(stored) = decode_progress(result.value()?)? else {
                    return Ok(Next::Done(()));
                };
                // An aborted move keeps the end its abort wrote.
                let record = &self.record;
                if stored.started_at_ms != record.started_at_ms
                    || stored.target != record.target
                    || stored.finished_at_ms.is_some()
                {
                    return Ok(Next::Done(()));
                }
                self.step = StoreStep::Progress;
                let row = (
                    COMPRESSION_MIGRATION_KEYSPACE.to_string(),
                    self.key.clone(),
                    record.to_bytes().map_err(MigrationError::failed)?.into(),
                );
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            StoreStep::Progress => {
                result.done()?;
                self.store_ahead()
            }
            StoreStep::Ahead => {
                result.done()?;
                self.leave()
            }
            StoreStep::Request => {
                let request = result.value()?;
                let request = request.as_deref().map(RenameRequest::from_bytes);
                let aborted = request.is_some_and(|request| {
                    request.is_ok_and(|request| request.aborted_at_ms.is_some())
                });
                let mut deletes = vec![(COMPRESSION_QUEUE_KEYSPACE.to_string(), self.key.clone())];
                if !aborted {
                    deletes.push((RENAME_MOVE_KEYSPACE.to_string(), self.key.clone()));
                }
                self.step = StoreStep::Left;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            StoreStep::Left => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettleStep {
    Version,
    Routing,
    Earlier(Box<RoutingSnapshot>),
    Migrate,
    Check(String),
    ReadVersion,
    ReadLocation,
    Abandon,
    Dropped,
}

/// Runs a version once more as a rename, which adopts a copy that reached its new name, then
/// retires its intents; a copy no version reads goes to the release first. Outputs `true` once
/// settled, `false` while the version cannot run yet.
#[derive(Debug, PartialEq)]
pub struct SettleMoveOperation {
    routing: NodeRouting,
    version: VersionKey,
    intents: VecDeque<(Vec<u8>, WriteIntent)>,
    keys: Vec<Vec<u8>>,
    read_path: Option<String>,
    now_ms: u64,
    step: SettleStep,
}

type SettleTurn = Turn<bool>;

impl SettleMoveOperation {
    pub fn new(
        routing: NodeRouting,
        version: &VersionKey,
        intents: Vec<(Vec<u8>, WriteIntent)>,
    ) -> Self {
        Self {
            routing,
            version: version.clone(),
            keys: intents.iter().map(|(key, _)| key.clone()).collect(),
            intents: intents.into(),
            read_path: None,
            now_ms: 0,
            step: SettleStep::Version,
        }
    }

    fn call(&mut self, step: SettleStep, call: IoCall) -> SettleTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn version_row(&self) -> Result<IoCall, MigrationError> {
        Ok(read_row(BLOB_VERSIONS_KEYSPACE, self.version.to_bytes()?))
    }

    /// Retires the intents of a settled move, starting from the file the version reads now.
    fn retire(&mut self) -> SettleTurn {
        let read = self.version_row()?;
        self.call(SettleStep::ReadVersion, read)
    }

    fn version_read(&mut self, value: Option<Value>) -> SettleTurn {
        let Some(value) = value else {
            return self.next_intent();
        };
        let Some(location) = BlobVersion::from_bytes(&value)?.location_key() else {
            return self.next_intent();
        };
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, location.to_bytes());
        self.call(SettleStep::ReadLocation, read)
    }

    /// The next intent whose file no version reads goes to the release.
    fn next_intent(&mut self) -> SettleTurn {
        while let Some((key, intent)) = self.intents.pop_front() {
            // The key is the rule, the path, a zero byte and the version id.
            let end = key.len().saturating_sub(17);
            let (Some(import), Some(path)) = (key.get(..16), key.get(16..end)) else {
                continue;
            };
            let path = String::from_utf8_lossy(path).into_owned();
            if self.read_path.as_deref() == Some(path.as_str()) {
                continue;
            }
            let import = <[u8; 16]>::try_from(import).map_err(MigrationError::failed)?;
            let abandon = AbandonOperation::new(key, intent, Ulid::from_bytes(import), path);
            return self.call(SettleStep::Abandon, IoCall::child(abandon));
        }
        let deletes = (self.keys.iter())
            .map(|key| (IMPORT_INTENT_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        self.call(SettleStep::Dropped, IoCall::Delete(deletes))
    }

    fn migrated(&mut self, moved: Result<MigrateOutcome, MigrateVersionError>) -> SettleTurn {
        match moved {
            // A run that ends on its own decided the version's file; any intent it leaves is
            // stale.
            Ok(_) => self.retire(),
            Err(error) => {
                let check = CheckVersionOperation::new(self.version.clone(), self.now_ms);
                self.call(SettleStep::Check(error.to_string()), IoCall::child(check))
            }
        }
    }
}

impl Operation for SettleMoveOperation {
    type Output = bool;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SettleTurn {
        Ok(Next::Call(self.version_row()?))
    }

    fn next(&mut self, result: IoResult) -> SettleTurn {
        match std::mem::replace(&mut self.step, SettleStep::Migrate) {
            SettleStep::Version => {
                if result.value()?.is_none() {
                    return self.retire();
                }
                let call = routing_call(self.routing.clone(), &self.version.bucket);
                self.call(SettleStep::Routing, call)
            }
            SettleStep::Routing => {
                let Some(routing) = routing_result(result)? else {
                    return self.retire();
                };
                let earlier = EarlierPatternsOperation::new(&routing);
                self.call(
                    SettleStep::Earlier(Box::new(routing)),
                    IoCall::child(earlier),
                )
            }
            SettleStep::Earlier(routing) => {
                let earlier = result.child::<Earlier, MigrationError>()??;
                let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
                let version = self.version.clone();
                let migrate = MigrateVersionOperation::new(version, Compression::Off, now)
                    .with_routing(*routing)
                    .with_earlier(earlier);
                self.call(SettleStep::Migrate, IoCall::child(migrate))
            }
            SettleStep::Migrate => self.migrated(result.child()?),
            SettleStep::Check(error) => {
                // A copy the storage check records as broken never moves, so its intents are
                // stale too.
                let checked = result.child::<CheckOutcome, CheckVersionError>()?;
                if matches!(checked, Ok(CheckOutcome::Broken(_))) {
                    let (bucket, version_id) = (&self.version.bucket, self.version.version_id);
                    tracing::warn!(%bucket, %version_id, "A moved version has no stored copy");
                    return self.retire();
                }
                tracing::warn!(%error, "A move did not settle yet");
                Ok(Next::Done(false))
            }
            SettleStep::ReadVersion => self.version_read(result.value()?),
            SettleStep::ReadLocation => {
                self.read_path = result
                    .value()?
                    .map(|value| BackendLocation::from_bytes(&value))
                    .transpose()?
                    .map(|found| found.backend_path);
                self.next_intent()
            }
            SettleStep::Abandon => match result.child::<bool, MigrationError>()?? {
                true => self.next_intent(),
                false => Ok(Next::Done(false)),
            },
            SettleStep::Dropped => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}

/// The cleanup key of the release of an abandoned copy: its intent, a tag and its version.
pub(super) fn release_key(intent_row: &[u8], version: &str) -> Vec<u8> {
    [intent_row, &[1], version.as_bytes()].concat()
}

#[derive(Debug, PartialEq)]
enum AbandonStep {
    Stat,
    Dropped,
    Copies(Box<BackendLocation>, String, Option<String>, PrefixPages),
    Queued,
}

/// Queues the release of the file at `path`, once per intent and version, if it has a backend
/// version and its size and ETag prove the bytes the intent recorded; others stay. Outputs
/// `false` when the storage cannot be asked yet.
#[derive(Debug, PartialEq)]
struct AbandonOperation {
    intent_row: Vec<u8>,
    intent: WriteIntent,
    import_id: Ulid,
    path: String,
    step: AbandonStep,
}

impl AbandonOperation {
    fn new(intent_row: Vec<u8>, intent: WriteIntent, import_id: Ulid, path: String) -> Self {
        Self {
            intent_row,
            intent,
            import_id,
            path,
            step: AbandonStep::Stat,
        }
    }

    /// Nothing there releases nothing but this write's own reservation, which a stat does not
    /// show as a file; without a backend version nothing names the bytes.
    fn stat_read(&mut self, event: Event) -> Turn<bool> {
        let (location, version, etag) = match event {
            Event::Blob(BlobEvent::ExternalStat { location: None, .. }) => {
                let drop = Effect::Blob(BlobEffect::DropReservation {
                    backend: self.intent.backend.clone(),
                    path: self.path.clone(),
                    version_id: self.intent.version_id,
                });
                self.step = AbandonStep::Dropped;
                return Ok(Next::Call(drop.into()));
            }
            Event::Blob(BlobEvent::ExternalStat {
                location: Some(location),
                version: Some(version),
                etag,
            }) => (location, version, etag),
            Event::Blob(BlobEvent::ExternalStat { .. }) => return Ok(Next::Done(true)),
            _ => return Ok(Next::Done(false)),
        };
        // A stored copy of the bytes that records their MD5 proves what the file holds.
        let Some(hash) = self.intent.written else {
            return Ok(Next::Done(true));
        };
        let pages = PrefixPages::new(BLOB_LOCATIONS_KEYSPACE, hash.to_vec(), RULE_PAGE);
        let page = pages.call();
        self.step = AbandonStep::Copies(Box::new(location), version, etag, pages);
        Ok(Next::Call(page))
    }

    fn queue_release(
        &mut self,
        mut location: BackendLocation,
        version: String,
        etag: Option<String>,
        source: BackendLocation,
    ) -> Turn<bool> {
        if !same_bytes(&source, location.blob_size, etag.as_deref()) {
            return Ok(Next::Done(true));
        }
        location.hashes = source.hashes;
        location.format.layout = StoredLayout::Plain(location.ulid);
        let key = release_key(&self.intent_row, &version);
        let work = BlobCleanupWork::ReleaseRenamed {
            location,
            import_id: self.import_id,
            version_id: self.intent.version_id,
            version,
        };
        self.step = AbandonStep::Queued;
        let row = (
            BLOB_CLEANUP_KEYSPACE.to_string(),
            key.into(),
            work.to_bytes()?.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for AbandonOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        let stat = Effect::Blob(BlobEffect::StatExternal {
            resolved: ResolvedBackend::new(self.intent.backend.clone(), None),
            path: self.path.clone(),
            created_by: self.intent.created_by,
        });
        Ok(Next::Call(stat.into()))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match std::mem::replace(&mut self.step, AbandonStep::Queued) {
            AbandonStep::Stat => self.stat_read(result.event()?),
            AbandonStep::Dropped => Ok(Next::Done(matches!(
                result.event()?,
                Event::Blob(BlobEvent::DeleteFinished)
            ))),
            AbandonStep::Copies(location, version, etag, mut pages) => {
                for (_, value) in pages.take(result)? {
                    let source = BackendLocation::from_bytes(&value)?;
                    if source.hashes.contains_key(HASH_MD5) {
                        return self.queue_release(*location, version, etag, source);
                    }
                }
                if pages.done() {
                    return Ok(Next::Done(true));
                }
                let page = pages.call();
                self.step = AbandonStep::Copies(location, version, etag, pages);
                Ok(Next::Call(page))
            }
            AbandonStep::Queued => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}

#[cfg(test)]
mod step_tests {
    use super::*;

    fn record() -> CompressionMigration {
        CompressionMigration::new(Compression::Off, 5)
    }

    fn empty_page() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    #[test]
    fn replaced_record_kept() {
        let mut store = StoreMigrationOperation::new("b", record(), None);
        store.first().unwrap();
        let mut newer = record();
        newer.started_at_ms = 6;
        let value = Value::from(newer.to_bytes().unwrap());
        assert_eq!(
            store.next(IoResult::Values(vec![Some(value)])),
            Ok(Next::Done(()))
        );
    }

    #[test]
    fn finished_record_leaves_queue() {
        let mut finished = record();
        finished.finished_at_ms = Some(9);
        let stored = Value::from(record().to_bytes().unwrap());
        let mut store = StoreMigrationOperation::new("b", finished, None);
        store.first().unwrap();
        let turn = store.next(IoResult::Values(vec![Some(stored)]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        let turn = store.next(IoResult::Done);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        let turn = store.next(IoResult::Values(vec![None]));
        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("a finished migration leaves the queue");
        };
        assert_eq!(deletes.len(), 2);
    }

    #[test]
    fn empty_bucket_finishes_pass() {
        let mut page =
            MigratePageOperation::new(NodeRouting::default(), "b", record(), MIGRATION_TIME);
        page.stamp(7);
        assert!(matches!(page.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let turn = page.next(empty_page());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        let turn = page.next(IoResult::Values(vec![None]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(page.record.finished_at_ms, Some(7));
        assert_eq!(
            page.next(child::<(), MigrationError>(Ok(()))),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn gone_bucket_hands_over() {
        let mut hand = HandOverOperation::new(NodeRouting::default(), "b", 5);
        assert!(matches!(hand.first(), Ok(Next::Call(IoCall::Child(_)))));
        let routed: Result<Option<RoutingSnapshot>, crate::blob::import_rows::ImportRowsError> =
            Ok(None);
        let turn = hand.next(child(routed));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert_eq!(hand.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn empty_queue_ends_run() {
        let routing = NodeRouting::default();
        let after = Some(Key::from(b"a".to_vec()));
        let mut run = ProcessMigrationsOperation::new(routing, None, after);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let expected = MigrationRun {
            next: Some(MIGRATION_CONTINUE),
            cursor: None,
        };
        assert_eq!(run.next(empty_page()), Ok(Next::Done(expected)));
    }

    #[test]
    fn locked_storage_waits() {
        let intent = WriteIntent {
            key: "k".to_string(),
            version_id: Ulid::from_bytes([1; 16]),
            backend: aruna_core::structs::storage::blob::BackendRef::node_default(),
            created_by: Default::default(),
            written: None,
            mover: None,
        };
        let import = Ulid::from_bytes([2; 16]);
        let mut abandon = AbandonOperation::new(vec![1], intent, import, "p".to_string());
        assert!(matches!(abandon.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let missing = Event::Blob(BlobEvent::Error(BlobError::HandleMissing));
        let answer = IoResult::Event(Box::new(missing));
        assert_eq!(abandon.next(answer), Ok(Next::Done(false)));
    }
}
