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

/// Writes the rows a run finished after its cursor in `txn_id`, the transaction of its progress,
/// so they count exactly when the progress does; without any the row goes.
async fn stage_ahead(
    storage: &aruna_storage::StorageHandle,
    txn_id: TxnId,
    bucket: &str,
    ahead: &MigrationAhead,
) -> Result<(), String> {
    let key: Key = bucket.as_bytes().to_vec().into();
    let space = MIGRATION_AHEAD_KEYSPACE.to_string();
    let effect = match ahead.rows.is_empty() {
        true => StorageEffect::Delete {
            key_space: space,
            key,
            txn_id: Some(txn_id),
        },
        false => StorageEffect::Write {
            key_space: space,
            key,
            value: ahead.to_bytes().map_err(|error| error.to_string())?.into(),
            txn_id: Some(txn_id),
        },
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. }) => {
            Ok(())
        }
        other => Err(format!("could not store the rows ahead: {}", other.label())),
    }
}

/// Ends an aborted rename's move once its intents are handed to the cleanup queue, which
/// settles them apart from this move; its request stays to show the abort.
pub(super) async fn end_aborted(
    context: &crate::driver::DriverContext,
    bucket: &str,
    mut record: CompressionMigration,
) -> Result<Option<Duration>, String> {
    if !hand_over_moves(context, bucket, record.started_at_ms).await? {
        return Ok(Some(MIGRATION_CONTINUE));
    }
    record.retry_at_ms = None;
    record.finished_at_ms = Some(aruna_core::time::unix_timestamp_millis());
    store_progress(context, bucket, &record, None).await?;
    Ok(None)
}

/// Where a move's hand-over of its intents stands: the move, the rule and the last intent.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct Handover {
    pub(super) started_ms: u64,
    pub(super) import_id: Option<Ulid>,
    pub(super) after: Option<Vec<u8>>,
}

/// Intent pages one hand-over run writes at most before it stores its place.
const HANDOVER_PAGES: usize = 16;

/// Whether the move started at `started_ms` has a hand-over under way.
async fn handing_over(
    context: &crate::driver::DriverContext,
    bucket: &str,
    started_ms: u64,
) -> Result<bool, String> {
    Ok(read_handover(context, bucket)
        .await?
        .is_some_and(|handover| handover.started_ms == started_ms))
}

pub(super) async fn read_handover(
    context: &crate::driver::DriverContext,
    bucket: &str,
) -> Result<Option<Handover>, String> {
    let read = StorageEffect::Read {
        key_space: MOVE_HANDOVER_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| postcard::from_bytes(&value))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read the hand-over: {}", other.label())),
    }
}

pub(super) async fn store_handover(
    context: &crate::driver::DriverContext,
    bucket: &str,
    handover: Option<&Handover>,
) -> Result<(), String> {
    let key: Key = bucket.as_bytes().to_vec().into();
    let effect = match handover {
        Some(handover) => StorageEffect::Write {
            key_space: MOVE_HANDOVER_KEYSPACE.to_string(),
            key,
            value: postcard::to_allocvec(handover)
                .map_err(|error| error.to_string())?
                .into(),
            txn_id: None,
        },
        None => StorageEffect::Delete {
            key_space: MOVE_HANDOVER_KEYSPACE.to_string(),
            key,
            txn_id: None,
        },
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. }) => {
            Ok(())
        }
        other => Err(format!("could not store the hand-over: {}", other.label())),
    }
}

/// Writes one `SettleMove` work per intent this move still owns, with the intent itself, a
/// bounded number of pages per run from a stored place; `true` once all are handed over.
async fn hand_over_moves(
    context: &crate::driver::DriverContext,
    bucket: &str,
    started_ms: u64,
) -> Result<bool, String> {
    let Some(routing) = move_routing(context, bucket).await? else {
        store_handover(context, bucket, None).await?;
        return Ok(true);
    };
    let mut rules: Vec<Ulid> = (routing.imports.iter())
        .filter(|rule| rule.bucket == bucket)
        .map(|rule| rule.import_id)
        .collect();
    rules.sort();
    let stored = read_handover(context, bucket).await?;
    let mut place = match stored.filter(|handover| handover.started_ms == started_ms) {
        Some(handover) => handover,
        None => Handover {
            started_ms,
            import_id: rules.first().copied(),
            after: None,
        },
    };
    for _ in 0..HANDOVER_PAGES {
        let Some(import_id) = place.import_id else {
            store_handover(context, bucket, None).await?;
            return Ok(true);
        };
        let (rows, next) = crate::storage_read::iter_prefix_page(
            &context.storage_handle,
            IMPORT_INTENT_KEYSPACE,
            Some(aruna_core::structs::storage::backend_import::object_prefix(
                import_id,
            )),
            place.after.take().map(Into::into),
            RULE_PAGE,
            None,
        )
        .await?;
        let mut writes = Vec::new();
        for (key, value) in rows {
            let intent = WriteIntent::from_bytes(&value).map_err(|error| error.to_string())?;
            // Only this move's own intents; an upload's or another move's stay.
            if intent.mover != Some(started_ms) {
                continue;
            }
            let version = VersionKey::new(bucket, &intent.key, intent.version_id);
            let work = BlobCleanupWork::SettleMove {
                version,
                intents: vec![(key.to_vec(), intent)],
            };
            let value = work.to_bytes().map_err(|error| error.to_string())?;
            writes.push((BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into()));
        }
        if !writes.is_empty() {
            let write = StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            };
            match context.storage_handle.send_storage_effect(write).await {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
                other => return Err(format!("could not hand over the moves: {}", other.label())),
            }
        }
        match next {
            Some(next) => place.after = Some(next.to_vec()),
            None => {
                // Rules are sorted; a removed rule still has its place among them.
                place.import_id = rules.iter().find(|rule| **rule > import_id).copied();
            }
        }
    }
    store_handover(context, bucket, Some(&place)).await?;
    Ok(false)
}

/// Runs `version` once more as a rename, which adopts a copy that reached its new name, then
/// retires `intents`; a copy no version reads goes to the release first. `true` once settled,
/// `false` while the version cannot run yet.
pub async fn settle_move(
    context: &crate::driver::DriverContext,
    version: &VersionKey,
    intents: &[(Vec<u8>, WriteIntent)],
) -> Result<bool, String> {
    let storage = &context.storage_handle;
    let key: Key = version
        .to_bytes()
        .map_err(|error| error.to_string())?
        .into();
    let read = StorageEffect::Read {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: None,
    };
    let present = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
        other => {
            return Err(format!(
                "could not read the moved version: {}",
                other.label()
            ));
        }
    };
    let routing = match present {
        true => move_routing(context, &version.bucket).await?,
        false => None,
    };
    let Some(routing) = routing else {
        return retire_intents(context, version, intents).await;
    };
    let earlier = read_earlier(context, &routing).await?;
    let operation =
        MigrateVersionOperation::new(version.clone(), Compression::Off, SystemTime::now())
            .with_routing(routing)
            .with_earlier(earlier);
    // A run that ends on its own decided the version's file; any intent it leaves is stale.
    match crate::driver::drive(operation, context).await {
        Ok(_) => retire_intents(context, version, intents).await,
        // A copy the storage check records as broken never moves, so its intents are stale too.
        Err(_) if is_broken(context, &key).await => {
            let (bucket, version_id) = (version.bucket.as_str(), version.version_id);
            tracing::warn!(bucket, %version_id, "A moved version has no stored copy");
            retire_intents(context, version, intents).await
        }
        Err(error) => {
            tracing::warn!(%error, "A move did not settle yet");
            Ok(false)
        }
    }
}

/// Retires the intents of a settled move; a copy one of them proves that no version reads is
/// handed to the release of renamed files at its exact backend version first.
async fn retire_intents(
    context: &crate::driver::DriverContext,
    version: &VersionKey,
    intents: &[(Vec<u8>, WriteIntent)],
) -> Result<bool, String> {
    let read_path = read_path(context, version).await?;
    for (key, intent) in intents {
        // The key is the rule, the path, a zero byte and the version id.
        let (Some(import), Some(path)) = (key.get(..16), key.get(16..key.len().saturating_sub(17)))
        else {
            continue;
        };
        let path = String::from_utf8_lossy(path).into_owned();
        if read_path.as_deref() == Some(path.as_str()) {
            continue;
        }
        let import = Ulid::from_bytes(import.try_into().map_err(|_| "bad intent key")?);
        if !abandon(context, (key, intent, import), path).await? {
            return Ok(false);
        }
    }
    let keys: Vec<Vec<u8>> = intents.iter().map(|(key, _)| key.clone()).collect();
    drop_intents(context, &keys).await
}

/// The path of the linked file `version` reads now, if any.
async fn read_path(
    context: &crate::driver::DriverContext,
    version: &VersionKey,
) -> Result<Option<String>, String> {
    let key = version.to_bytes().map_err(|error| error.to_string())?;
    let read = StorageEffect::Read {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: key.into(),
        txn_id: None,
    };
    let value = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => return Err(format!("could not read a version: {}", other.label())),
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let blob = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
    let Some(location) = blob.location_key() else {
        return Ok(None);
    };
    let Effect::Storage(read) = blob_location_read(&location, None) else {
        return Err("unexpected location read".to_string());
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| BackendLocation::from_bytes(&value).map(|found| found.backend_path))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read a location: {}", other.label())),
    }
}

/// The cleanup key of the release of an abandoned copy: its intent, a tag and its version.
pub(super) fn release_key(intent_row: &[u8], version: &str) -> Vec<u8> {
    [intent_row, &[1], version.as_bytes()].concat()
}

/// Queues the release of the file at `path`, once per intent and version, if it has a backend
/// version and its size and ETag prove the bytes `intent` recorded; others stay. `false` when
/// the storage cannot be asked yet.
async fn abandon(
    context: &crate::driver::DriverContext,
    (intent_row, intent, import_id): (&[u8], &WriteIntent, Ulid),
    path: String,
) -> Result<bool, String> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return Ok(false);
    };
    let resolved = ResolvedBackend::new(intent.backend.clone(), None);
    let stat = BlobEffect::StatExternal {
        resolved,
        path: path.clone(),
        created_by: intent.created_by,
    };
    // Nothing there releases nothing but this write's own reservation, which a stat does not
    // show as a file; without a backend version nothing names the bytes.
    let (mut location, version, etag) = match blob_handle.send_blob_effect(stat).await {
        Event::Blob(BlobEvent::ExternalStat { location: None, .. }) => {
            let drop = BlobEffect::DropReservation {
                backend: intent.backend.clone(),
                path,
                version_id: intent.version_id,
            };
            let dropped = blob_handle.send_blob_effect(drop).await;
            return Ok(matches!(dropped, Event::Blob(BlobEvent::DeleteFinished)));
        }
        Event::Blob(BlobEvent::ExternalStat {
            location: Some(location),
            version: Some(version),
            etag,
        }) => (location, version, etag),
        Event::Blob(BlobEvent::ExternalStat { .. }) => return Ok(true),
        _ => return Ok(false),
    };
    let Some(source) = stored_copy(context, intent.written).await? else {
        return Ok(true);
    };
    if !same_bytes(&source, location.blob_size, etag.as_deref()) {
        return Ok(true);
    }
    location.hashes = source.hashes;
    location.format.layout = StoredLayout::Plain(location.ulid);
    let key = release_key(intent_row, &version);
    let work = BlobCleanupWork::ReleaseRenamed {
        location,
        import_id,
        version_id: intent.version_id,
        version,
    };
    let value = work.to_bytes().map_err(|error| error.to_string())?;
    let write = StorageEffect::Write {
        key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(true),
        other => Err(format!(
            "could not queue the abandoned copy: {}",
            other.label()
        )),
    }
}

/// A stored copy of the bytes with the BLAKE3 `hash` that records their MD5.
async fn stored_copy(
    context: &crate::driver::DriverContext,
    hash: Option<[u8; 32]>,
) -> Result<Option<BackendLocation>, String> {
    let Some(hash) = hash else {
        return Ok(None);
    };
    let mut after = None;
    loop {
        let (rows, next) = crate::storage_read::iter_prefix_page(
            &context.storage_handle,
            BLOB_LOCATIONS_KEYSPACE,
            Some(hash.to_vec().into()),
            after,
            RULE_PAGE,
            None,
        )
        .await?;
        for (_, value) in rows {
            let location =
                BackendLocation::from_bytes(&value).map_err(|error| error.to_string())?;
            if location.hashes.contains_key(HASH_MD5) {
                return Ok(Some(location));
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(None),
        }
    }
}

async fn drop_intents(
    context: &crate::driver::DriverContext,
    intents: &[Vec<u8>],
) -> Result<bool, String> {
    let deletes = intents
        .iter()
        .map(|key| (IMPORT_INTENT_KEYSPACE.to_string(), key.clone().into()))
        .collect();
    let delete = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(true),
        other => Err(format!(
            "could not drop the move's intents: {}",
            other.label()
        )),
    }
}

/// Whether the caller paused or aborted the rename of the move `record`.
async fn rename_stopped(
    context: &crate::driver::DriverContext,
    bucket: &str,
    record: &CompressionMigration,
) -> Result<bool, String> {
    let request = rename_request(context, bucket, record).await?;
    Ok(request.is_none_or(|request| request.paused || request.aborted_at_ms.is_some()))
}

/// Whether a version whose move failed is broken; a check records it.
async fn is_broken(context: &crate::driver::DriverContext, key: &Key) -> bool {
    let Ok(version_key) = VersionKey::from_bytes(key.as_ref()) else {
        return false;
    };
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let operation = check::CheckVersionOperation::new(version_key, now_ms);
    let outcome = crate::driver::drive(operation, context).await;
    matches!(outcome, Ok(check::CheckOutcome::Broken(_)))
}

/// The routing a move resolves against, read once per page; `None` once the bucket is gone.
pub async fn move_routing(
    context: &crate::driver::DriverContext,
    bucket: &str,
) -> Result<Option<RoutingSnapshot>, String> {
    let routing = crate::driver::node_routing(context);
    let operation = crate::blob::import_rows::BucketRoutingOperation::new(routing, bucket);
    let snapshot = crate::driver::drive(operation, context).await;
    snapshot.map_err(|error| error.to_string())
}

/// Stores the progress, with the rows a run finished `ahead` of its cursor where given.
async fn store_progress(
    context: &crate::driver::DriverContext,
    bucket: &str,
    record: &CompressionMigration,
    ahead: Option<&MigrationAhead>,
) -> Result<(), String> {
    let storage = &context.storage_handle;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => {
            return Err(format!(
                "could not start a progress transaction: {}",
                other.label()
            ));
        }
    };
    let staged = stage_progress(storage, txn_id, bucket, record, ahead).await;
    if !matches!(staged, Ok(true)) {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return staged.map(|_| ());
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(format!(
            "migration progress was not stored: {}",
            other.label()
        )),
    }
}

/// Writes the progress, and the rows `ahead` where given, in `txn_id`. Returns `false` when a
/// newer setting replaced the record.
async fn stage_progress(
    storage: &aruna_storage::StorageHandle,
    txn_id: TxnId,
    bucket: &str,
    record: &CompressionMigration,
    ahead: Option<&MigrationAhead>,
) -> Result<bool, String> {
    let key: Key = bucket.as_bytes().to_vec().into();
    let current = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await;
    let stored = match current {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            CompressionMigration::from_bytes(value.as_ref()).map_err(|error| error.to_string())?
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(false),
        other => {
            return Err(format!(
                "could not read migration progress: {}",
                other.label()
            ));
        }
    };
    // An aborted move keeps the end its abort wrote.
    let ended = stored.finished_at_ms.is_some();
    if stored.started_at_ms != record.started_at_ms || stored.target != record.target || ended {
        return Ok(false);
    }
    let value = record.to_bytes().map_err(|error| error.to_string())?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => {
            return Err(format!(
                "could not write migration progress: {}",
                other.label()
            ));
        }
    }
    if let Some(ahead) = ahead {
        stage_ahead(storage, txn_id, bucket, ahead).await?;
    }
    // A finished migration leaves the queue and its rename request with its last progress;
    // the request of an aborted rename stays to show the abort.
    if record.finished_at_ms.is_some() {
        let read = StorageEffect::Read {
            key_space: RENAME_MOVE_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        };
        let request = match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => {
                return Err(format!(
                    "could not read the rename request: {}",
                    other.label()
                ));
            }
        };
        let request = request.as_deref().map(RenameRequest::from_bytes);
        let aborted = request
            .is_some_and(|request| request.is_ok_and(|request| request.aborted_at_ms.is_some()));
        let spaces = match aborted {
            true => &[COMPRESSION_QUEUE_KEYSPACE][..],
            false => &[COMPRESSION_QUEUE_KEYSPACE, RENAME_MOVE_KEYSPACE][..],
        };
        for space in spaces {
            let delete = StorageEffect::Delete {
                key_space: space.to_string(),
                key: key.clone(),
                txn_id: Some(txn_id),
            };
            match storage.send_storage_effect(delete).await {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {}
                other => {
                    return Err(format!(
                        "could not leave the migration queue: {}",
                        other.label()
                    ));
                }
            }
        }
    }
    Ok(true)
}
