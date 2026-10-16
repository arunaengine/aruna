//! Runs one backend import: lists the rule's folder and registers each object in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, JOB_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobError, JobId, JobResultPayload};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportCheckpoint, BackendImportSpec, ExternalObject, ImportedObject,
    object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer,
    ResolvedBackend, VersionKey, bucket_permission_path, ensure_confined_path,
};
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::{RoutingSnapshot, RoutingTarget, RuleSource};
use aruna_core::types::{Effects, Key, Value};
use byteview::ByteView;
use smallvec::smallvec;
use std::path::Path;
use std::time::{Duration, Instant};
use ulid::Ulid;

use super::executor::{JobContext, JobRunOutcome};
use super::store::{put_state, read_state};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive, gate_context, now_ms, routing_snapshot};
use crate::groups::backends::query::GetBackendOperation;
use crate::groups::imports::read_import;
use crate::placement::policy::{GateContext, PolicyGateError};
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::complete_put;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use crate::s3::purge_fence::PurgeFenceError;

const LIST_PAGE: usize = 500;

/// What a run should do with one listed object.
#[derive(Debug, PartialEq)]
enum Decision {
    Skip,
    Import,
}

/// Writes and deletes in Aruna win: a changed object (or `changed`, a hash unlike the committed
/// one) gets a new version only while the key shows the imported one or nothing.
/// A purged but unchanged object is imported again.
fn decide(
    object: &ExternalObject,
    row: Option<&ImportedObject>,
    head: Option<Ulid>,
    version_exists: bool,
    changed: bool,
) -> Decision {
    match row {
        None if head.is_some() => Decision::Skip,
        None => Decision::Import,
        Some(row) if !changed && row.matches(object) => match head {
            None if !version_exists => Decision::Import,
            _ => Decision::Skip,
        },
        Some(row) if head.is_none_or(|head| head == row.version_id) => Decision::Import,
        Some(_) => Decision::Skip,
    }
}

enum Outcome {
    Imported,
    Skipped,
    /// Outside the rule's folder or inside Aruna's own; not counted at all.
    Ignored,
}

/// `Item` fails one object and the run goes on; `System` ends the run for a retry.
enum ItemError {
    Item(String),
    System(String),
}

/// What every object of one listing page is checked against.
struct ImportRun {
    rule: BackendImport,
    backend: GroupStorage,
    bucket: BucketInfo,
    routing: RoutingSnapshot,
    quota: Option<u64>,
    gate: Option<GateContext>,
}

fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message))
}

fn retryable(message: impl ToString) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.to_string()))
}

fn checkpoint_key(job_id: JobId) -> ByteView {
    ByteView::from(job_id.to_bytes().to_vec())
}

pub async fn read_checkpoint(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<BackendImportCheckpoint>, String> {
    read_state(
        &context.storage_handle,
        JOB_STATE_KEYSPACE,
        checkpoint_key(job_id),
        "backend import checkpoint read",
    )
    .await
}

async fn persist(ctx: &JobContext, checkpoint: &BackendImportCheckpoint) -> Result<(), String> {
    ctx.progress
        .set_current(checkpoint.imported + checkpoint.skipped + checkpoint.failed);
    put_state(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        JOB_STATE_KEYSPACE,
        checkpoint_key(ctx.job_id),
        checkpoint,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Longest time a run of skips goes without a persisted checkpoint.
const SKIP_PERSIST: Duration = Duration::from_secs(2);

pub async fn run_backend_import(ctx: &JobContext, spec: &BackendImportSpec) -> JobRunOutcome {
    let mut checkpoint = match read_checkpoint(&ctx.driver, ctx.job_id).await {
        Ok(checkpoint) => checkpoint.unwrap_or_default(),
        Err(error) => return retryable(error),
    };
    let mut last = Instant::now();
    loop {
        let run = match prepare(ctx, spec).await {
            Ok(run) => run,
            Err(outcome) => return outcome,
        };
        let (objects, next) = match list_page(&ctx.driver, &run, checkpoint.cursor.clone()).await {
            Ok(page) => page,
            Err(error) => return retryable(error),
        };
        for object in objects {
            if ctx.cancel.is_cancelled() {
                return JobRunOutcome::Cancelled;
            }
            if ctx.shutdown.is_cancelled() {
                return JobRunOutcome::Interrupted;
            }
            let result = import_object(ctx, spec, &run, &object).await;
            let counted = match tally(&mut checkpoint, &object.key, result) {
                Ok(counted) => counted,
                Err(error) => return retryable(error),
            };
            // Real work is persisted at once; skips only every few seconds, to show progress.
            if counted || last.elapsed() >= SKIP_PERSIST {
                if let Err(error) = persist(ctx, &checkpoint).await {
                    return retryable(error);
                }
                last = Instant::now();
            }
        }
        let done = next.is_none();
        if next.is_some() {
            checkpoint.cursor = next;
        }
        if let Err(error) = persist(ctx, &checkpoint).await {
            return retryable(error);
        }
        last = Instant::now();
        if done {
            return JobRunOutcome::Succeeded(JobResultPayload::BackendImport {
                imported: checkpoint.imported,
                skipped: checkpoint.skipped,
                failed: checkpoint.failed,
            });
        }
    }
}

/// Counts one object and moves the cursor past it. Answers whether the count is worth
/// persisting at once; a system error leaves the cursor so a retry decides the object.
fn tally(
    checkpoint: &mut BackendImportCheckpoint,
    key: &str,
    result: Result<Outcome, ItemError>,
) -> Result<bool, String> {
    let counted = match result {
        Ok(Outcome::Imported) => {
            checkpoint.imported += 1;
            true
        }
        Ok(Outcome::Skipped) => {
            checkpoint.skipped += 1;
            false
        }
        Ok(Outcome::Ignored) => false,
        Err(ItemError::Item(error)) => {
            checkpoint.fail(key, error);
            true
        }
        Err(ItemError::System(error)) => return Err(error),
    };
    checkpoint.cursor = Some(key.to_string());
    Ok(counted)
}

/// Rereads the rule and everything it depends on before each page, so a removed rule,
/// a disabled backend or a lost permission stops the run.
async fn prepare(ctx: &JobContext, spec: &BackendImportSpec) -> Result<ImportRun, JobRunOutcome> {
    let rule = match read_import(&ctx.driver, spec.group_id, spec.import_id).await {
        Ok(Some(rule)) => rule,
        Ok(None) => return Err(permanent("the import rule was removed")),
        Err(error) => return Err(retryable(error)),
    };
    let backend = match drive(GetBackendOperation::new(rule.backend_id), &ctx.driver).await {
        Ok(Some(backend)) if backend.group_id == rule.group_id && !backend.disabled => backend,
        Ok(_) => return Err(permanent("the storage backend is missing or disabled")),
        Err(error) => return Err(retryable(error)),
    };
    let bucket = match drive(GetBucketOperation::new(rule.bucket.clone()), &ctx.driver).await {
        Ok(bucket) if bucket.group_id == rule.group_id => bucket,
        Ok(_) | Err(GetBucketError::NotFound) => {
            return Err(permanent("the bucket no longer exists in this group"));
        }
        Err(error) => return Err(retryable(error)),
    };
    let realm_id = spec.auth_context.realm_id;
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: spec.auth_context.clone(),
            path: bucket_permission_path(realm_id, rule.group_id, spec.node_id, &rule.bucket),
            required_permission: Permission::WRITE,
        }),
        &ctx.driver,
    )
    .await
    .map_err(retryable)?;
    if !allowed {
        return Err(permanent("the importing user may not write to the bucket"));
    }
    let routing = routing_snapshot(&ctx.driver, rule.group_id, &rule.bucket)
        .await
        .map_err(retryable)?;
    let quota = drive(GetConfigOperation::new(realm_id), &ctx.driver)
        .await
        .map_err(retryable)?
        .quota
        .effective_group_ceiling(&rule.group_id);
    let gate = gate_context(&ctx.driver, realm_id, now_ms())
        .await
        .map_err(retryable)?;
    Ok(ImportRun {
        rule,
        backend,
        bucket,
        routing,
        quota,
        gate,
    })
}

async fn list_page(
    context: &DriverContext,
    run: &ImportRun,
    start_after: Option<String>,
) -> Result<(Vec<ExternalObject>, Option<String>), BlobError> {
    let effect = BlobEffect::ListExternal {
        backend: BackendRef::Group(run.rule.backend_id),
        prefix: run.rule.backend_prefix.clone(),
        start_after,
        limit: LIST_PAGE,
    };
    drive(BlobRequest::new(effect), context)
        .await
        .and_then(|event| match event {
            BlobEvent::ExternalListed { objects, next } => Ok((objects, next)),
            other => Err(BlobError::ListError(format!("unexpected answer {other:?}"))),
        })
}

async fn import_object(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    object: &ExternalObject,
) -> Result<Outcome, ItemError> {
    let rule = &run.rule;
    if rule.aruna_key(&object.key).is_none() || run.backend.is_reserved(&object.key) {
        return Ok(Outcome::Ignored);
    }
    let row_key = object_key(rule.import_id, &object.key);
    let [stored] = read_values(&ctx.driver, [(IMPORTED_OBJECT_KEYSPACE, row_key.clone())]).await?;
    let row = parse(stored.clone(), ImportedObject::from_bytes)?;
    // The bytes stay on the rule's backend, wherever new writes to the key are routed.
    let target = RoutingTarget::Backend(BackendRef::Group(rule.backend_id));
    let resolved = match run
        .routing
        .catalog
        .resolve_target(&target, RuleSource::Tenant)
    {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return Err(ItemError::Item("the backend is not usable".to_string())),
        Err(error) => return Err(ItemError::Item(error.to_string())),
    };
    let key = match row.as_ref().and_then(|row| row.key.clone()) {
        Some(key) => key,
        None => rule.aruna_key(&object.key).unwrap_or_default(),
    };
    if key.is_empty() {
        return Err(ItemError::Item(
            "the object maps to an empty key".to_string(),
        ));
    }
    ensure_confined_path(Path::new(&key)).map_err(|error| ItemError::Item(error.to_string()))?;
    // An equal size is no proof for the first listing of Aruna's own write (which only notes
    // ETag and time) or a listing without both, so the bytes must hash to the committed version.
    let listed = row.as_ref().and_then(|row| row.first_listing(object));
    let unmarked = object.etag.is_none() && object.modified_ms.is_none();
    let blind = row
        .clone()
        .filter(|row| unmarked && row.size == object.size);
    let (mut hashed, mut changed) = (None, false);
    if let (Some(listed), Some(seen)) = (listed.or(blind), stored) {
        let found = hash_object(ctx, spec, resolved.clone(), &object.key).await?;
        let committed = committed_hash(&ctx.driver, &rule.bucket, &key, listed.version_id).await?;
        if committed.is_some_and(|hash| Some(&hash[..]) == found.0.get_blake3()) {
            note_listing(&ctx.driver, rule, row_key, &seen, encode(&listed)?).await?;
            return Ok(Outcome::Skipped);
        }
        changed = committed.is_some();
        hashed = Some(found);
    }
    let head_key = BlobHeadKey::new(&rule.bucket, &key)
        .to_bytes()
        .map_err(|error| ItemError::Item(error.to_string()))?;
    let [head] = read_values(&ctx.driver, [(BLOB_HEAD_KEYSPACE, head_key.into())]).await?;
    let head = parse(head, CurrentVersionPointer::from_bytes)?.map(|head| head.version_id);
    let version_exists = match (&row, head) {
        (Some(row), None) => {
            let version_key = VersionKey::new(&rule.bucket, &key, row.version_id)
                .to_bytes()
                .map_err(|error| ItemError::Item(error.to_string()))?;
            let [version] =
                read_values(&ctx.driver, [(BLOB_VERSIONS_KEYSPACE, version_key.into())]).await?;
            version.is_some()
        }
        _ => false,
    };
    if decide(object, row.as_ref(), head, version_exists, changed) == Decision::Skip {
        return Ok(Outcome::Skipped);
    }
    // The committed row stays until the new version replaces it in one transaction.
    let version_id = Ulid::generate();
    let committed = ImportedObject {
        key: row.as_ref().and_then(|row| row.key.clone()),
        ..ImportedObject::new(version_id, object)
    };
    let (location, version) = match hashed {
        Some(hashed) => hashed,
        None => hash_object(ctx, spec, resolved, &object.key).await?,
    };
    if location.blob_size != object.size {
        return Err(ItemError::Item(
            "the object changed while it was read".to_string(),
        ));
    }
    let size = location.blob_size;
    let put = put_version(
        ctx,
        spec,
        run,
        key.clone(),
        (location, version),
        committed,
        head,
    );
    match put.await {
        Ok(()) => {}
        // An Aruna write or delete marker came first and keeps the key.
        Err(PutObjectError::HeadChanged) => return Ok(Outcome::Skipped),
        Err(error) => return Err(put_failure(error)),
    }
    // As for any put, a failed queue write after the commit leaves the obligation for repair.
    let auth = spec.auth_context.clone();
    let (realm_id, group_id, bucket) = (auth.realm_id, rule.group_id, rule.bucket.clone());
    complete_put(
        &ctx.driver,
        realm_id,
        spec.node_id,
        auth,
        group_id,
        bucket,
        key,
        version_id,
        size,
    )
    .await;
    Ok(Outcome::Imported)
}

/// The BLAKE3 hash of the committed version of `key` in `bucket`, if it still exists.
async fn committed_hash(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<Option<[u8; 32]>, ItemError> {
    let version_key = VersionKey::new(bucket, key, version_id)
        .to_bytes()
        .map_err(|error| ItemError::Item(error.to_string()))?;
    let [version] = read_values(context, [(BLOB_VERSIONS_KEYSPACE, version_key.into())]).await?;
    let version = parse(version, BlobVersion::from_bytes)?;
    Ok(version.and_then(|version| version.blob_hash().copied()))
}

async fn hash_object(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    resolved: ResolvedBackend,
    path: &str,
) -> Result<(BackendLocation, Option<String>), ItemError> {
    let effect = BlobEffect::HashExternal {
        resolved,
        path: path.to_string(),
        created_by: spec.auth_context.user_id,
    };
    match drive(BlobRequest::new(effect), &ctx.driver).await {
        Ok(BlobEvent::ExternalHashed { location, version }) => Ok((location, version)),
        Ok(other) => Err(ItemError::System(format!("unexpected answer {other:?}"))),
        Err(error) => Err(hash_failure(error)),
    }
}

/// Only an object that is gone or unreadable by its key fails alone; a backend outage
/// ends the run for a retry.
fn hash_failure(error: BlobError) -> ItemError {
    match error {
        BlobError::Unreadable(_) => ItemError::Item(error.to_string()),
        error => ItemError::System(error.to_string()),
    }
}

/// A discovery of a whole-bucket provider link is its version's own copy; one of an ordinary
/// rule may share the raw copy of its content on the backend.
fn discovered_copy(rule: &BackendImport, mut location: BackendLocation) -> BackendLocation {
    if rule.mapped_bucket().is_some() {
        location.format.layout = StoredLayout::Plain(location.ulid);
    }
    location
}

async fn put_version(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    key: String,
    (location, version): (BackendLocation, Option<String>),
    row: ImportedObject,
    head: Option<Ulid>,
) -> Result<(), PutObjectError> {
    let row_key = object_key(run.rule.import_id, &location.backend_path);
    let mut rows = vec![(
        IMPORTED_OBJECT_KEYSPACE.to_string(),
        row_key,
        row.to_bytes()?.into(),
    )];
    // A later same-name overwrite must not change what this version reads.
    if let Some(version) = version {
        let value = version.into_bytes().into();
        rows.push((
            BACKEND_VERSION_KEYSPACE.to_string(),
            version_key(location.ulid),
            value,
        ));
    }
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: spec.auth_context.user_id,
        group_id: run.rule.group_id,
        realm_id: spec.auth_context.realm_id,
        node_id: spec.node_id,
        request: PutObjectInput {
            bucket: run.rule.bucket.clone(),
            key,
            content_length: Some(location.blob_size),
            body: None,
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: Some(row.version_id),
        quota_ceiling: run.quota,
        routing: run.routing.clone(),
    })
    .without_envelope()
    .with_adopted(discovered_copy(&run.rule, location))
    .with_bucket_guard(run.bucket.clone())
    .with_restrictions(spec.auth_context.path_restrictions.clone())
    .with_expected_head(head)
    .with_import_rows(run.rule.import_id, rows);
    if let Some(gate) = run.gate.clone() {
        operation = operation.with_gate(gate);
    }
    drive(operation, &ctx.driver).await.map(|_| ())
}

/// Only a refusal of this one object fails it and lets the run go on; storage and other
/// system errors end the run for a retry, so the checkpoint never passes an undecided object.
fn put_failure(error: PutObjectError) -> ItemError {
    use PolicyGateError::{Denied, ForeignPolicy, Invalid, NoSubject, Required};
    match error {
        PutObjectError::QuotaExceeded { .. }
        | PutObjectError::ObjectExists
        | PutObjectError::MissingHash(_)
        | PutObjectError::RoutingFailed(_)
        | PutObjectError::PolicyError(_)
        | PutObjectError::PurgeFence(PurgeFenceError::Suspended)
        | PutObjectError::PolicyGate(
            Denied { .. } | ForeignPolicy { .. } | Required { .. } | Invalid | NoSubject,
        ) => ItemError::Item(error.to_string()),
        error => ItemError::System(error.to_string()),
    }
}

fn parse<T>(
    value: Option<Value>,
    decode: impl Fn(&[u8]) -> Result<T, aruna_core::errors::ConversionError>,
) -> Result<Option<T>, ItemError> {
    value
        .map(|value| decode(value.as_ref()))
        .transpose()
        .map_err(|error| ItemError::System(error.to_string()))
}

async fn read_values<const N: usize>(
    context: &DriverContext,
    reads: [(&str, Key); N],
) -> Result<[Option<Value>; N], ItemError> {
    let reads = reads
        .into_iter()
        .map(|(key_space, key)| (key_space.to_string(), key))
        .collect();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| ItemError::System("short batch read".to_string())),
        Event::Storage(StorageEvent::Error { error }) => Err(ItemError::System(error.to_string())),
        other => Err(ItemError::System(format!("unexpected answer {other:?}"))),
    }
}

fn encode(row: &ImportedObject) -> Result<Vec<u8>, ItemError> {
    row.to_bytes()
        .map_err(|error| ItemError::System(error.to_string()))
}

#[cfg(test)]
async fn write_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    value: Vec<u8>,
) -> Result<(), ItemError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(ItemError::System(error.to_string())),
        other => Err(ItemError::System(format!("unexpected answer {other:?}"))),
    }
}

/// Replaces the row only while the rule exists and the row still holds `seen`, in one
/// transaction. Otherwise nothing is written and the next run looks at the object again.
async fn note_listing(
    context: &DriverContext,
    rule: &BackendImport,
    row_key: Key,
    seen: &Value,
    row: Vec<u8>,
) -> Result<(), ItemError> {
    let storage = &context.storage_handle;
    let unexpected = |event: Event| ItemError::System(format!("unexpected answer {event:?}"));
    let started = StorageEffect::StartTransaction { read: false };
    let txn_id = match storage.send_storage_effect(started).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(unexpected(other)),
    };
    let abort = StorageEffect::AbortTransaction { txn_id };
    let reads = vec![
        (BACKEND_IMPORT_KEYSPACE.to_string(), rule.key()),
        (IMPORTED_OBJECT_KEYSPACE.to_string(), row_key.clone()),
    ];
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    };
    let values = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            storage.send_storage_effect(abort).await;
            return Err(unexpected(other));
        }
    };
    if !matches!(values.as_slice(), [(_, Some(_)), (_, Some(value))] if value == seen) {
        storage.send_storage_effect(abort).await;
        return Ok(());
    }
    let write = StorageEffect::Write {
        key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
        key: row_key,
        value: row.into(),
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => {
            storage.send_storage_effect(abort).await;
            return Err(unexpected(other));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Ok(()),
        other => Err(unexpected(other)),
    }
}

/// Emits one listing or hashing effect and hands back the blob adapter's answer.
#[derive(Debug, PartialEq)]
struct BlobRequest {
    state: RequestState,
    output: Option<Result<BlobEvent, BlobError>>,
}

#[derive(Debug, PartialEq)]
enum RequestState {
    Ready(BlobEffect),
    Sent(Answer),
    Done,
}

/// The one event kind that answers a sent request, besides an error.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Answer {
    Listed,
    Hashed,
}

impl BlobRequest {
    fn new(effect: BlobEffect) -> Self {
        Self {
            state: RequestState::Ready(effect),
            output: None,
        }
    }

    fn finish(&mut self, output: Result<BlobEvent, BlobError>) -> Effects {
        self.state = RequestState::Done;
        self.output = Some(output);
        smallvec![]
    }
}

impl Operation for BlobRequest {
    type Output = BlobEvent;
    type Error = BlobError;

    fn start(&mut self) -> Effects {
        let RequestState::Ready(effect) = std::mem::replace(&mut self.state, RequestState::Done)
        else {
            return self.finish(Err(BlobError::ReadError("request already sent".into())));
        };
        let answer = match &effect {
            BlobEffect::ListExternal { .. } => Answer::Listed,
            BlobEffect::HashExternal { .. } => Answer::Hashed,
            other => {
                let error = format!("not a listing or hashing request: {other:?}");
                return self.finish(Err(BlobError::ReadError(error)));
            }
        };
        self.state = RequestState::Sent(answer);
        smallvec![Effect::Blob(effect)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let RequestState::Sent(answer) = self.state else {
            let error = format!("event outside a sent request: {event:?}");
            return self.finish(Err(BlobError::ReadError(error)));
        };
        match (answer, event) {
            (_, Event::Blob(BlobEvent::Error(error))) => self.finish(Err(error)),
            (Answer::Listed, Event::Blob(event @ BlobEvent::ExternalListed { .. }))
            | (Answer::Hashed, Event::Blob(event @ BlobEvent::ExternalHashed { .. })) => {
                self.finish(Ok(event))
            }
            (_, other) => {
                let error = format!("unexpected event {other:?}");
                self.finish(Err(BlobError::ReadError(error)))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == RequestState::Done
    }

    fn finalize(self) -> Result<BlobEvent, BlobError> {
        self.output
            .unwrap_or_else(|| Err(BlobError::ReadError("no answer".into())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapped_discovery_owned() {
        use aruna_core::structs::storage::backend_import::PLAIN_PATTERN;
        use aruna_core::structs::storage::format::EncodingClass;
        let mut rule = BackendImport {
            import_id: Ulid::nil(),
            group_id: Ulid::nil(),
            backend_id: Ulid::nil(),
            backend_prefix: "team-a-x/".to_string(),
            bucket: "x".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: PLAIN_PATTERN.to_string(),
        };
        let location = BackendLocation {
            backend: BackendRef::Group(Ulid::nil()),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "team-a-x/a.csv".to_string(),
            ulid: Ulid::from_parts(7, 7),
            format: Default::default(),
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: Default::default(),
        };
        let owned = discovered_copy(&rule, location.clone()).format.encoding();
        assert_eq!(
            owned,
            EncodingClass::Plain {
                location: location.ulid
            }
        );
        // An ordinary rule keeps sharing identical bytes on its backend.
        rule.write_pattern = "{dir}{id}_{name}".to_string();
        let shared = discovered_copy(&rule, location).format.encoding();
        assert_eq!(shared, EncodingClass::Raw);
    }

    fn object(etag: &str) -> ExternalObject {
        ExternalObject {
            key: "raw/a.csv".to_string(),
            size: 4,
            etag: Some(etag.to_string()),
            modified_ms: None,
        }
    }

    fn row(version_id: Ulid, etag: &str) -> ImportedObject {
        ImportedObject::new(version_id, &object(etag))
    }

    fn listing() -> BlobEffect {
        BlobEffect::ListExternal {
            backend: BackendRef::Group(Ulid::nil()),
            prefix: "raw/".to_string(),
            start_after: None,
            limit: 1,
        }
    }

    fn listed() -> Event {
        Event::Blob(BlobEvent::ExternalListed {
            objects: Vec::new(),
            next: None,
        })
    }

    #[test]
    fn checks_blob_answers() {
        let mut request = BlobRequest::new(listing());
        assert_eq!(request.start().len(), 1);
        request.step(listed());
        assert!(request.is_complete());
        assert!(request.finalize().is_ok());

        // Another kind of answer does not end a listing.
        let mut request = BlobRequest::new(listing());
        request.start();
        request.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(request.is_complete());
        assert!(request.finalize().is_err());

        // Events before the request is sent or after its answer are refused.
        let mut request = BlobRequest::new(listing());
        request.step(listed());
        assert!(request.finalize().is_err());
        let mut request = BlobRequest::new(listing());
        request.start();
        request.step(listed());
        request.step(listed());
        assert!(request.finalize().is_err());
    }

    #[test]
    fn counts_outcomes() {
        let mut checkpoint = BackendImportCheckpoint::default();
        assert_eq!(tally(&mut checkpoint, "a", Ok(Outcome::Skipped)), Ok(false));
        assert_eq!(tally(&mut checkpoint, "b", Ok(Outcome::Imported)), Ok(true));
        let failed = Err(ItemError::Item("broken".to_string()));
        assert_eq!(tally(&mut checkpoint, "c", failed), Ok(true));
        assert_eq!(tally(&mut checkpoint, "d", Ok(Outcome::Ignored)), Ok(false));
        assert_eq!(
            (checkpoint.skipped, checkpoint.imported, checkpoint.failed),
            (1, 1, 1)
        );
        assert_eq!(checkpoint.cursor.as_deref(), Some("d"));

        // A system error ends the run before the cursor passes the object.
        let error = Err(ItemError::System("storage down".to_string()));
        assert!(tally(&mut checkpoint, "e", error).is_err());
        assert_eq!(checkpoint.cursor.as_deref(), Some("d"));
    }

    #[test]
    fn classifies_put_errors() {
        let item = |error| matches!(put_failure(error), ItemError::Item(_));
        assert!(item(PutObjectError::QuotaExceeded { limit: 1, usage: 2 }));
        assert!(item(PutObjectError::PolicyGate(PolicyGateError::Invalid)));
        assert!(!item(PutObjectError::StorageError(
            aruna_core::errors::StorageError::TransactionConflict
        )));
        assert!(!item(PutObjectError::ImportRemoved));
        assert!(!item(PutObjectError::PolicyGate(PolicyGateError::Drift)));
        let unavailable = PolicyGateError::Unavailable {
            policy_ids: Vec::new(),
        };
        assert!(!item(PutObjectError::PolicyGate(unavailable)));

        // A backend outage while hashing ends the run; a vanished object fails alone.
        let gone = BlobError::Unreadable("gone".to_string());
        assert!(matches!(hash_failure(gone), ItemError::Item(_)));
        let down = BlobError::ReadError("connection refused".to_string());
        assert!(matches!(hash_failure(down), ItemError::System(_)));
    }

    #[tokio::test]
    async fn notes_listing_guarded() {
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let rule = BackendImport {
            import_id: Ulid::from_parts(4, 4),
            group_id: Ulid::from_parts(3, 3),
            backend_id: Ulid::from_parts(5, 5),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "in/".to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{name}".to_string(),
        };
        let row_key = object_key(rule.import_id, "raw/a.csv");
        let written = ImportedObject::written(Ulid::nil(), "in/a.csv".to_string(), 4);
        let seen = Value::from(written.to_bytes().unwrap());
        let listed = written.first_listing(&object("e1")).unwrap();
        let stored = || async {
            let read = read_values(&context, [(IMPORTED_OBJECT_KEYSPACE, row_key.clone())]).await;
            let Ok([Some(value)]) = read else {
                panic!("the row is missing")
            };
            ImportedObject::from_bytes(&value).unwrap()
        };
        let update = encode(&listed).ok().unwrap();
        let row = write_value(
            &context,
            IMPORTED_OBJECT_KEYSPACE,
            row_key.clone(),
            seen.to_vec(),
        );
        assert!(row.await.is_ok());

        // A removed rule gets no row update.
        let note = note_listing(&context, &rule, row_key.clone(), &seen, update.clone());
        assert!(note.await.is_ok());
        assert_eq!(stored().await, written);

        let value = rule.to_bytes().unwrap();
        assert!(
            write_value(&context, BACKEND_IMPORT_KEYSPACE, rule.key(), value)
                .await
                .is_ok()
        );
        // A row another write replaced meanwhile stays as that write left it.
        let other = Value::from(listed.to_bytes().unwrap());
        let note = note_listing(&context, &rule, row_key.clone(), &other, update.clone());
        assert!(note.await.is_ok());
        assert_eq!(stored().await, written);

        let note = note_listing(&context, &rule, row_key.clone(), &seen, update);
        assert!(note.await.is_ok());
        assert_eq!(stored().await, listed);
    }

    #[tokio::test]
    async fn reads_committed_hash() {
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let row = ImportedObject::written(Ulid::from_parts(6, 6), "in/a.csv".to_string(), 4);
        assert_eq!(
            committed_hash(&context, "bucket", "in/a.csv", row.version_id)
                .await
                .ok(),
            Some(None)
        );

        let version = BlobVersion::materialized(
            [7; 32],
            BackendRef::Group(Ulid::from_parts(5, 5)),
            aruna_core::structs::storage::format::EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let key = VersionKey::new("bucket", "in/a.csv", row.version_id);
        let key = Key::from(key.to_bytes().unwrap());
        let value = version.to_bytes().unwrap();
        let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value);
        assert!(write.await.is_ok());
        assert_eq!(
            committed_hash(&context, "bucket", "in/a.csv", row.version_id)
                .await
                .ok(),
            Some(Some([7; 32]))
        );
    }

    #[test]
    fn imports_new_objects() {
        assert_eq!(
            decide(&object("e1"), None, None, false, false),
            Decision::Import
        );
        // A key the bucket already holds is never taken over.
        let other = Some(Ulid::generate());
        assert_eq!(
            decide(&object("e1"), None, other, false, false),
            Decision::Skip
        );
    }

    #[test]
    fn skips_unchanged_objects() {
        let version = Ulid::generate();
        let row = row(version, "e1");
        assert_eq!(
            decide(&object("e1"), Some(&row), Some(version), true, false),
            Decision::Skip
        );
        // A purged version is imported again.
        assert_eq!(
            decide(&object("e1"), Some(&row), None, false, false),
            Decision::Import
        );
        assert_eq!(
            decide(&object("e1"), Some(&row), None, true, false),
            Decision::Skip
        );
    }

    #[test]
    fn aruna_side_wins() {
        let version = Ulid::generate();
        let row = row(version, "e1");
        let changed = object("e2");
        let fresh = decide(&changed, Some(&row), Some(version), true, false);
        assert_eq!(fresh, Decision::Import);
        assert_eq!(
            decide(&changed, Some(&row), None, true, false),
            Decision::Import
        );
        // A later write or delete marker in Aruna keeps the key as Aruna has it.
        let newer = Some(Ulid::generate());
        assert_eq!(
            decide(&changed, Some(&row), newer, true, false),
            Decision::Skip
        );
        assert_eq!(
            decide(&object("e1"), Some(&row), newer, true, false),
            Decision::Skip
        );
    }

    #[test]
    fn imports_hash_mismatch() {
        let version = Ulid::generate();
        let row = row(version, "e1");
        // Same size and ETag, yet the bytes hash differently.
        let ours = decide(&object("e1"), Some(&row), Some(version), true, true);
        assert_eq!(ours, Decision::Import);
        let newer = Some(Ulid::generate());
        let later = decide(&object("e1"), Some(&row), newer, true, true);
        assert_eq!(later, Decision::Skip);
    }
}
