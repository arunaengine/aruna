//! Runs one backend import: lists the rule's folder and registers each object in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BUCKET_CREATION_KEYSPACE, DELETE_CLAIM_KEYSPACE, DELETE_TRAIL_KEYSPACE,
    IMPORT_INTENT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, JOB_STATE_KEYSPACE, PURGE_FENCE_KEYSPACE,
    RENAMED_ROW_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::execution::job::{JobError, JobId, JobResultPayload};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportCheckpoint, BackendImportSpec, ExternalObject, ImportedObject,
    PathIdentity, WriteIntent, object_key, object_prefix, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo,
    CurrentVersionPointer, ResolvedBackend, VersionKey, bucket_permission_path,
    ensure_confined_path, incarnation,
};
use aruna_core::structs::storage::delete_audit::BlobAuditRecord;
use aruna_core::structs::storage::discovery::{
    DiscoveryRule, FileIdentity, IdentityError, file_identity,
};
use aruna_core::structs::storage::distribution::hint::mint_version;
use aruna_core::structs::storage::format::{EncodingClass, StoredLayout};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::{RoutingSnapshot, RoutingTarget, RuleSource};
use aruna_core::types::{Effects, Key, Value};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};
use ulid::Ulid;

use super::executor::{JobContext, JobRunOutcome};
use super::store::{put_state, read_state};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive, gate_context, now_ms, routing_snapshot};
use crate::groups::backends::query::GetBackendOperation;
use crate::groups::imports::discovery::read_discovery;
use crate::groups::imports::shared::{self, SharedRule, claim_file, read_shared, taken_paths};
use crate::groups::imports::{read_earlier, read_import};
use crate::placement::policy::{GateContext, PolicyGateError};
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::complete_put;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::delete::{DeleteObjectError, DeleteObjectInput, DeleteObjectOperation};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, RuleScan, Trail,
    intent_keys, trail_ranges,
};
use crate::s3::purge_fence::PurgeFenceError;

const LIST_PAGE: usize = 500;

/// What a run should do with one listed object.
#[derive(Debug, PartialEq)]
enum Decision {
    Skip,
    Import,
    /// An unmapped file whose key the bucket already holds: a conflict, never a takeover.
    Taken,
}

/// Writes and deletes in Aruna win: a changed object gets a new version only while the key
/// shows the imported one or nothing.
/// A purged but unchanged object is imported again.
fn decide(
    object: &ExternalObject,
    row: Option<&ImportedObject>,
    head: Option<Ulid>,
    version_exists: bool,
) -> Decision {
    match row {
        None if head.is_some() => Decision::Taken,
        None => Decision::Import,
        Some(row) if row.matches(object) => match head {
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
    /// The file changed between its listing and its read; the next run looks again.
    Changed,
}

impl ItemError {
    fn reason(self) -> String {
        match self {
            Self::Item(error) | Self::System(error) => error,
            Self::Changed => "the file changed while it was read".to_string(),
        }
    }
}

/// What every object of one listing page is checked against.
struct ImportRun {
    rule: BackendImport,
    /// Patterns the rule wrote with before, read for files its pattern does not match.
    earlier: Vec<String>,
    backend: GroupStorage,
    bucket: BucketInfo,
    /// The bucket incarnation the run started on, whose delete trail it reads.
    incarnation: u128,
    routing: RoutingSnapshot,
    quota: Option<u64>,
    gate: Option<GateContext>,
    /// The write intents of the listed page by path, read in one range.
    intents: BTreeMap<String, Vec<(Key, WriteIntent)>>,
    /// File name rules for objects without a recorded mapping.
    discovery: Vec<DiscoveryRule>,
    /// Other rules that read an overlapping folder; they may own a listed file.
    shared: Vec<SharedRule>,
    /// The page's rows, heads and committed versions, read in two batches.
    page: PageReads,
}

/// Values read once per listing page. Only the skip of an unchanged file trusts them; every
/// other path reads again, and every write checks again in its transaction.
#[derive(Default)]
struct PageReads {
    rows: HashMap<String, Option<Value>>,
    heads: HashMap<String, Option<Ulid>>,
    /// The version each row names, by listed path.
    versions: HashMap<String, Option<Value>>,
    /// Listed paths that another rule of a shared folder recorded or writes.
    taken: HashSet<String>,
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
        let mut run = match prepare(ctx, spec).await {
            Ok(run) => run,
            Err(outcome) => return outcome,
        };
        let (objects, next) = match list_page(&ctx.driver, &run, checkpoint.cursor.clone()).await {
            Ok(page) => page,
            Err(error) => return retryable(error),
        };
        run.intents = match page_intents(&ctx.driver, &run.rule, &objects).await {
            Ok(intents) => intents,
            Err(error) => return retryable(error.reason()),
        };
        run.page = match page_reads(&ctx.driver, &run.rule, &objects).await {
            Ok(page) => page,
            Err(error) => return retryable(error.reason()),
        };
        let paths: Vec<&str> = objects.iter().map(|object| object.key.as_str()).collect();
        run.page.taken = match taken_paths(&ctx.driver, &run.shared, &paths).await {
            Ok(taken) => taken,
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
        Err(ItemError::Changed) => {
            checkpoint.skipped += 1;
            false
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
    let earlier = read_earlier(&ctx.driver, rule.import_id)
        .await
        .map_err(retryable)?;
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
    let creation = [(
        BUCKET_CREATION_KEYSPACE,
        rule.bucket.as_bytes().to_vec().into(),
    )];
    let [creation] = read_values(&ctx.driver, creation)
        .await
        .map_err(|error| retryable(error.reason()))?;
    let incarnation = incarnation(creation.as_ref().map(|value| value.as_ref()));
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
    let discovery = read_discovery(&ctx.driver, rule.group_id, rule.import_id)
        .await
        .map_err(retryable)?;
    let shared = read_shared(&ctx.driver, &rule).await.map_err(retryable)?;
    Ok(ImportRun {
        rule,
        earlier,
        backend,
        bucket,
        incarnation,
        routing,
        quota,
        gate,
        intents: BTreeMap::new(),
        discovery,
        shared,
        page: PageReads::default(),
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
    // An empty file a write's intent names is that write's reservation, in progress.
    if object.size == 0 && reserved(rule, run.intents.get(&object.key)) {
        return Ok(Outcome::Skipped);
    }
    if page_skip(
        (rule, &run.earlier),
        &run.page,
        run.intents.contains_key(&object.key),
        object,
    ) {
        return Ok(Outcome::Skipped);
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
    let recovery = recover(
        ctx,
        spec,
        run,
        object,
        row.as_ref(),
        stored.as_ref(),
        &resolved,
    );
    if let Some(outcome) = recovery.await? {
        return Ok(outcome);
    }
    let names = (rule, run.earlier.as_slice(), run.discovery.as_slice());
    let key = bucket_key(names, row.as_ref(), &object.key)?;
    if row.is_none()
        && let Some(outcome) = claimed(run, &object.key)?
    {
        return Ok(outcome);
    }
    if key.is_empty() {
        return Err(ItemError::Item(
            "the object maps to an empty key".to_string(),
        ));
    }
    ensure_confined_path(Path::new(&key)).map_err(|error| ItemError::Item(error.to_string()))?;
    // Only new or differing objects are read; a trusted listing needs its committed version
    // and, when listed with a single-part ETag, the MD5 of that version.
    if let (Some(listed), Some(seen)) = (trusted_listing(row.as_ref(), object), stored)
        && let Some(md5) = committed_md5(&ctx.driver, &rule.bucket, &key, &listed, object).await?
        && !etag_differs(object.etag.as_deref(), md5.as_deref())
    {
        let listed = encode(&listed)?;
        if listed.as_slice() != seen.as_ref() {
            let note = Note::default();
            note_listing(&ctx.driver, rule, row_key, Some(&seen), listed, note).await?;
        }
        return Ok(Outcome::Skipped);
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
    match decide(object, row.as_ref(), head, version_exists) {
        Decision::Skip => return Ok(Outcome::Skipped),
        Decision::Taken => {
            return register_twin(ctx, spec, run, object, (&key, head, resolved)).await;
        }
        Decision::Import => {}
    }
    // The committed row stays until the new version replaces it in one transaction. A new
    // row records its key, so later rule changes never move the object. A new file is dated
    // by its modification time, as a second file of its key would be.
    let version_id = match &row {
        None => import_version(run, object.modified_ms),
        Some(_) => import_version(run, None),
    };
    let committed = ImportedObject {
        key: match row.as_ref() {
            Some(row) => row.key.clone(),
            None => Some(key.clone()),
        },
        ..ImportedObject::new(version_id, object)
    };
    // Aruna's own write that lost its head returns only if no delete removed it on purpose.
    let keyed = row
        .as_ref()
        .filter(|row| row.key.is_some() && head.is_none());
    if let Some(row) = keyed
        && deleted_on_purpose(&ctx.driver, run, &key, row.version_id).await?
    {
        return Ok(Outcome::Skipped);
    }
    let (location, version, verified) = hash_object(ctx, spec, resolved, object).await?;
    if location.blob_size != object.size {
        return Err(ItemError::Item(
            "the object changed while it was read".to_string(),
        ));
    }
    let committed = ImportedObject {
        etag: observed(object, verified).etag,
        ..committed
    };
    let size = location.blob_size;
    let fence = Fence {
        head,
        keep_head: false,
        guards: match row {
            Some(_) => Vec::new(),
            None => shared_guards(run, &object.key),
        },
        retired: Vec::new(),
        trail: keyed.map(|row| row.version_id),
        keep_row: false,
        copy_index: copy_index(&ctx.driver, rule, row.as_ref(), &object.key).await?,
    };
    let put = put_version(
        ctx,
        spec,
        run,
        key.clone(),
        (location, version),
        committed,
        fence,
    );
    match put.await {
        Ok(()) => {}
        // An Aruna write or delete marker came first and keeps the key.
        Err(PutObjectError::HeadChanged) if row.is_none() => return Err(taken(&key)),
        Err(PutObjectError::HeadChanged | PutObjectError::HistoryDeleted) => {
            return Ok(Outcome::Skipped);
        }
        Err(error) if moved_away(&error) => return Ok(Outcome::Ignored),
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

/// The key and version a file was written as, from its write intent or its path.
#[derive(Debug, PartialEq)]
struct Identity {
    key: String,
    version_id: Ulid,
    /// The intent that names it, and who wrote the file.
    intent: Option<(Key, UserId)>,
}

/// One stored intent of a write to the file's path, and whether its version committed.
type Claim = (Key, WriteIntent, bool);

/// Whether a write to the rule's backend holds an intent for the path; its empty file there
/// is the reservation of a write still in progress, not a file to register.
fn reserved(rule: &BackendImport, intents: Option<&Vec<(Key, WriteIntent)>>) -> bool {
    let backend = BackendRef::Group(rule.backend_id);
    intents.is_some_and(|intents| intents.iter().any(|(_, intent)| intent.backend == backend))
}

/// Prefers the one write whose version never committed; else the path itself. Several open
/// writes or a path that does not tell the folder of its key are reported, never guessed.
fn choose_identity(
    (rule, earlier, discovery): (&BackendImport, &[String], &[DiscoveryRule]),
    path: &str,
    claims: Vec<Claim>,
    hash: Option<&[u8; 32]>,
) -> Result<Option<Identity>, ItemError> {
    let backend = BackendRef::Group(rule.backend_id);
    let claims = claims
        .into_iter()
        .filter(|(_, intent, _)| intent.backend == backend);
    let claims: Vec<Claim> = claims.collect();
    // A write names the file only if it recorded these very bytes, committed or not.
    let proven = |(_, intent, _): &&Claim| {
        let written = intent.written.as_ref();
        written.is_some() && written == hash
    };
    let open: Vec<&Claim> = claims.iter().filter(|(.., committed)| !committed).collect();
    let open_proven: Vec<&Claim> = open.iter().copied().filter(proven).collect();
    let done = claims.iter().filter(|(.., committed)| *committed);
    let done: Vec<&Claim> = done.filter(proven).collect();
    let chosen = match (open.as_slice(), open_proven.as_slice(), done.as_slice()) {
        (_, [one], _) => Some(*one),
        ([], _, [one]) => Some(*one),
        ([], _, []) => None,
        (_, [], _) if !open.is_empty() => {
            return Err(item(
                "an unfinished write claims this file, but no record shows it wrote these bytes",
            ));
        }
        _ => return Err(item("several writes claim this file")),
    };
    if let Some((key, intent, _)) = chosen {
        return Ok(Some(Identity {
            key: intent.key.clone(),
            version_id: intent.version_id,
            intent: Some((key.clone(), intent.created_by)),
        }));
    }
    // A name alone gives the version id; the discovery rules place it like any found file.
    match file_identity(rule, earlier, discovery, path) {
        None
        | Some(Ok(FileIdentity {
            version_id: None, ..
        })) => Ok(None),
        Some(Err(error)) => Err(identity_error(error)),
        Some(Ok(FileIdentity {
            key,
            version_id: Some(version_id),
            ..
        })) => Ok(Some(Identity {
            key,
            version_id,
            intent: None,
        })),
    }
}

fn identity_error(error: IdentityError) -> ItemError {
    match error {
        IdentityError::Ambiguous => item("the file name fits more than one key or version"),
        IdentityError::Conflict(patterns) => {
            let patterns = patterns.join("`, `");
            item(format!(
                "the file matches several discovery patterns: `{patterns}`"
            ))
        }
    }
}

/// The free path of a file at `path`, without a trailing `~<id>` tag of any id.
fn free_path(path: &str) -> &str {
    let Some((free, tag)) = path.rsplit_once('~') else {
        return path;
    };
    match Ulid::from_string(tag) {
        Ok(_) if !free.is_empty() => free,
        _ => path,
    }
}

/// Every intent key the write `id` may have stored for the file at `path`: the free path and
/// its tagged path, whichever of the two the bytes reached.
fn write_intents(import_id: Ulid, path: &str, id: Ulid) -> Vec<Key> {
    let free = path.strip_suffix(&format!("~{id}")).unwrap_or(path);
    intent_keys(import_id, free, id, true)
}

/// `write_intents` and the intents of `id` at any tagged path of the file's path or of its
/// free path, as a move tags with its attempt id; a suffix pattern's name already ends in one.
async fn stored_intents(
    context: &DriverContext,
    import_id: Ulid,
    path: &str,
    id: Ulid,
) -> Result<Vec<Key>, ItemError> {
    let mut keys = Vec::new();
    for base in [path, free_path(path)] {
        for key in write_intents(import_id, base, id) {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        let prefix = object_key(import_id, &format!("{base}~"));
        for (key, _) in iterate(context, IMPORT_INTENT_KEYSPACE, prefix).await? {
            let tagged = intent_path(&key).is_some_and(|tagged| free_path(tagged) == base);
            if tagged && key.ends_with(&id.to_bytes()) && !keys.contains(&key) {
                keys.push(key);
            }
        }
    }
    Ok(keys)
}

/// Reads the listed object's bytes; a size other than the listed one means it changed meanwhile.
async fn hash_listed(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    resolved: &ResolvedBackend,
    object: &ExternalObject,
) -> Result<Hashed, ItemError> {
    let hashed = hash_object(ctx, spec, resolved.clone(), object).await?;
    if hashed.0.blob_size != object.size {
        return Err(item("the object changed while it was read"));
    }
    Ok(hashed)
}

fn item(message: impl Into<String>) -> ItemError {
    ItemError::Item(message.into())
}

/// Reads the rows of a page in one batch, then the heads and named versions of their keys.
async fn page_reads(
    context: &DriverContext,
    rule: &BackendImport,
    objects: &[ExternalObject],
) -> Result<PageReads, ItemError> {
    let mut page = PageReads::default();
    let space = IMPORTED_OBJECT_KEYSPACE.to_string();
    let reads = objects
        .iter()
        .map(|object| (space.clone(), object_key(rule.import_id, &object.key)));
    let rows = batch_read(context, reads.collect()).await?;
    let mut reads = Vec::new();
    let mut named = Vec::new();
    for (object, row) in objects.iter().zip(rows) {
        let parsed = parse(row.clone(), ImportedObject::from_bytes)?;
        page.rows.insert(object.key.clone(), row);
        let Some(parsed) = parsed else { continue };
        let Some(key) = parsed.key.clone().or_else(|| rule.aruna_key(&object.key)) else {
            continue;
        };
        let head = BlobHeadKey::new(&rule.bucket, &key).to_bytes();
        let version = VersionKey::new(&rule.bucket, &key, parsed.version_id).to_bytes();
        let (Ok(head), Ok(version)) = (head, version) else {
            continue;
        };
        reads.push((BLOB_HEAD_KEYSPACE.to_string(), head.into()));
        reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
        named.push((object.key.clone(), key));
    }
    let mut values = batch_read(context, reads).await?.into_iter();
    for (path, key) in named {
        let (Some(head), Some(version)) = (values.next(), values.next()) else {
            return Err(ItemError::System("short batch read".to_string()));
        };
        let head = parse(head, CurrentVersionPointer::from_bytes)?;
        page.heads.insert(key, head.map(|head| head.version_id));
        page.versions.insert(path, version);
    }
    Ok(page)
}

/// Whether the page reads alone prove a known file unchanged, as the full checks would find.
fn page_skip(
    (rule, earlier): (&BackendImport, &[String]),
    page: &PageReads,
    intents: bool,
    object: &ExternalObject,
) -> bool {
    let Some(Some(stored)) = page.rows.get(&object.key) else {
        return false;
    };
    let Ok(row) = ImportedObject::from_bytes(stored) else {
        return false;
    };
    if needs_lookup((rule, earlier), object, Some(&row), intents) {
        return false;
    }
    let Some(key) = row.key.clone().or_else(|| rule.aruna_key(&object.key)) else {
        return false;
    };
    if key.is_empty() || ensure_confined_path(Path::new(&key)).is_err() {
        return false;
    }
    let (Some(head), Some(version)) = (page.heads.get(&key), page.versions.get(&object.key)) else {
        return false;
    };
    if let Some(listed) = trusted_listing(Some(&row), object) {
        // A listed ETag is checked against the MD5 of the copy, which the page does not read.
        let version = version.as_ref().map(|value| BlobVersion::from_bytes(value));
        let located = matches!(version, Some(Ok(version)) if version.location_key().is_some());
        let unchanged = listed
            .to_bytes()
            .is_ok_and(|listed| listed == stored.as_ref());
        return located && object.etag.is_none() && unchanged;
    }
    decide(object, Some(&row), *head, version.is_some()) == Decision::Skip
}

/// Whether a file may be an uncommitted write, so its intents are read. A new file, a literal
/// import whose name encodes a version and a changed file are; an unchanged known file is not.
fn needs_lookup(
    (rule, earlier): (&BackendImport, &[String]),
    object: &ExternalObject,
    row: Option<&ImportedObject>,
    intents: bool,
) -> bool {
    let Some(row) = row else {
        return true;
    };
    let decoded = rule.decode_any(&object.key, earlier) != PathIdentity::Unmatched;
    intents || (row.key.is_none() && decoded)
}

/// A row of Aruna's own write names the file while every intent at the path is that very
/// committed write. Any other write, also a committed move of another version, is checked by
/// the file's hash.
fn row_covers(row: Option<&ImportedObject>, claims: &[Claim]) -> bool {
    let Some(row) = row.filter(|row| row.key.is_some()) else {
        return false;
    };
    claims.iter().all(|(_, intent, committed)| {
        *committed
            && row.key.as_deref() == Some(intent.key.as_str())
            && row.version_id == intent.version_id
    })
}

/// Registers a file whose version never committed as the key and version its write intent or
/// path names. `None` leaves the file to the ordinary import, as for any row Aruna wrote.
async fn recover(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    object: &ExternalObject,
    row: Option<&ImportedObject>,
    stored: Option<&Value>,
    resolved: &ResolvedBackend,
) -> Result<Option<Outcome>, ItemError> {
    let rule = &run.rule;
    let intents = run.intents.get(&object.key).map_or(&[][..], Vec::as_slice);
    if !needs_lookup((rule, &run.earlier), object, row, !intents.is_empty()) {
        return Ok(None);
    }
    let claims = read_claims(&ctx.driver, rule, intents).await?;
    if row_covers(row, &claims) {
        return Ok(None);
    }
    // Only the hash of the file can prove which write left it, so it is read first.
    let mut hashed = None;
    if !claims.is_empty() {
        hashed = Some(hash_listed(ctx, spec, resolved, object).await?);
    }
    let hash = hashed
        .as_ref()
        .and_then(|(location, ..)| location.get_blake3());
    let hash = hash.and_then(|hash| <[u8; 32]>::try_from(hash).ok());
    let names = (rule, run.earlier.as_slice(), run.discovery.as_slice());
    let Some(identity) = choose_identity(names, &object.key, claims, hash.as_ref())? else {
        return Ok(None);
    };
    let Identity {
        key,
        version_id,
        intent,
    } = identity;
    ensure_confined_path(Path::new(&key)).map_err(|error| item(error.to_string()))?;
    // A name alone proves no owner where several rules read the folder.
    if intent.is_none()
        && row.is_none()
        && let Some(outcome) = claimed(run, &object.key)?
    {
        return Ok(Some(outcome));
    }
    let (mut location, version, verified) = match hashed {
        Some(hashed) => hashed,
        None => hash_listed(ctx, spec, resolved, object).await?,
    };
    let object = &observed(object, verified);
    if let Some((_, writer)) = &intent {
        location.created_by = *writer;
    }
    let committed = ImportedObject {
        key: Some(key.clone()),
        ..ImportedObject::new(version_id, object)
    };
    let row_key = object_key(rule.import_id, &object.key);
    // An existing version only gets its missing row, and only for the same bytes.
    let (version_key, exact) = exact_version(&ctx.driver, &rule.bucket, &key, version_id).await?;
    match &exact {
        Exact::Incompatible => {
            return Err(item(format!(
                "version {version_id} of {key} exists in a state that cannot hold the file"
            )));
        }
        Exact::Materialized(hash, _) if location.get_blake3() != Some(&hash[..]) => {
            return Err(item(format!(
                "the file differs from version {version_id} of {key}"
            )));
        }
        Exact::Absent | Exact::Materialized(..) => {}
    }
    // A literal-name import of the file stops being current only once its replacement exists.
    let literal = rule
        .aruna_key(&object.key)
        .filter(|literal| *literal != key);
    let alias = row.filter(|row| row.key.is_none()).zip(literal);
    let (version_key, exact) = match exact {
        Exact::Absent => {
            // The version transaction reads the delete trail again, so a delete meanwhile
            // conflicts. An alias keeps its row until the alias is hidden.
            let head = read_head(&ctx.driver, &rule.bucket, &key).await?;
            let mut guards = vec![
                rule_guard(rule)?,
                (
                    IMPORTED_OBJECT_KEYSPACE.to_string(),
                    row_key.clone(),
                    stored.cloned(),
                ),
            ];
            if row.is_none() {
                guards.extend(shared_guards(run, &object.key));
            }
            let retired = match (&intent, &alias) {
                (Some(_), None) => {
                    stored_intents(&ctx.driver, rule.import_id, &object.key, version_id).await?
                }
                _ => Vec::new(),
            };
            // Older history joins below a head; a newer file becomes the head.
            let keep_head = head.is_some_and(|head| head > version_id);
            let fence = Fence {
                head,
                keep_head,
                guards,
                retired,
                trail: Some(version_id),
                keep_row: alias.is_some(),
                copy_index: None,
            };
            let size = location.blob_size;
            let file = (location, version);
            let put = put_version(ctx, spec, run, key.clone(), file, committed.clone(), fence);
            match put.await {
                Ok(()) => {}
                // Another write or recovery came first; the next run looks again.
                Err(PutObjectError::HeadChanged | PutObjectError::VersionExists) => {
                    return Ok(Some(Outcome::Skipped));
                }
                Err(error) if moved_away(&error) => return Ok(Some(Outcome::Ignored)),
                Err(error) => return Err(put_failure(error)),
            }
            if !keep_head {
                let auth = spec.auth_context.clone();
                let (realm_id, group_id) = (auth.realm_id, rule.group_id);
                let (bucket, node_id) = (rule.bucket.clone(), spec.node_id);
                let key = key.clone();
                complete_put(
                    &ctx.driver,
                    realm_id,
                    node_id,
                    auth,
                    group_id,
                    bucket,
                    key,
                    version_id,
                    size,
                )
                .await;
            }
            if alias.is_none() {
                return Ok(Some(Outcome::Imported));
            }
            exact_version(&ctx.driver, &rule.bucket, &key, version_id).await?
        }
        exact => (version_key, exact),
    };
    if let Some((alias, literal)) = alias {
        hide_alias(&ctx.driver, spec, rule, &literal, alias.version_id).await?;
    }
    // An existing version only gets its missing row, and only for the same bytes.
    let Exact::Materialized(_, value) = exact else {
        return Ok(Some(Outcome::Skipped));
    };
    let guards = [
        rule_guard(rule)?,
        (BLOB_VERSIONS_KEYSPACE.to_string(), version_key, Some(value)),
    ];
    let retired = match intent {
        Some(_) => stored_intents(&ctx.driver, rule.import_id, &object.key, version_id).await?,
        None => Vec::new(),
    };
    let repaired = encode(&committed)?;
    let note = Note {
        guards: &guards,
        retired,
        writes: Vec::new(),
        sharing: None,
        fenced: None,
    };
    if !note_listing(&ctx.driver, rule, row_key, stored, repaired, note).await? {
        return Ok(Some(Outcome::Skipped));
    }
    Ok(Some(Outcome::Imported))
}

/// The rule must still be defined as this run read it.
fn rule_guard(rule: &BackendImport) -> Result<(String, Key, Option<Value>), ItemError> {
    let bytes = rule.to_bytes();
    let value = bytes.map_err(|error| ItemError::System(error.to_string()))?;
    let space = BACKEND_IMPORT_KEYSPACE.to_string();
    Ok((space, rule.key(), Some(value.into())))
}

async fn read_head(
    context: &DriverContext,
    bucket: &str,
    key: &str,
) -> Result<Option<Ulid>, ItemError> {
    let head_key = BlobHeadKey::new(bucket, key)
        .to_bytes()
        .map_err(|error| ItemError::Item(error.to_string()))?;
    let [head] = read_values(context, [(BLOB_HEAD_KEYSPACE, head_key.into())]).await?;
    Ok(parse(head, CurrentVersionPointer::from_bytes)?.map(|head| head.version_id))
}

/// The intents stored for a path, each with whether its version committed.
async fn read_claims(
    context: &DriverContext,
    rule: &BackendImport,
    intents: &[(Key, WriteIntent)],
) -> Result<Vec<Claim>, ItemError> {
    let mut claims = Vec::new();
    for (key, intent) in intents {
        let found = exact_version(context, &rule.bucket, &intent.key, intent.version_id).await?;
        claims.push((key.clone(), intent.clone(), found.1 != Exact::Absent));
    }
    Ok(claims)
}

/// The write intents of every path from the first to the last object of a listing page, read
/// as one range, so a page of known files costs one read however many objects it has.
async fn page_intents(
    context: &DriverContext,
    rule: &BackendImport,
    objects: &[ExternalObject],
) -> Result<BTreeMap<String, Vec<(Key, WriteIntent)>>, ItemError> {
    let mut intents: BTreeMap<String, Vec<(Key, WriteIntent)>> = BTreeMap::new();
    let paths = objects.iter().map(|object| object.key.as_str());
    let (Some(first), Some(last)) = (paths.clone().min(), paths.max()) else {
        return Ok(intents);
    };
    let mut start = IterStart::At(object_key(rule.import_id, first));
    loop {
        let effect = StorageEffect::Iter {
            key_space: IMPORT_INTENT_KEYSPACE.to_string(),
            prefix: Some(object_prefix(rule.import_id)),
            start: Some(start),
            limit: LIST_PAGE,
            txn_id: None,
        };
        let (values, next) = match context.storage_handle.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(ItemError::System(error.to_string()));
            }
            other => return Err(ItemError::System(format!("unexpected answer {other:?}"))),
        };
        for (key, value) in values {
            let Some(path) = intent_path(&key) else {
                return Err(ItemError::System(
                    "a malformed write intent key".to_string(),
                ));
            };
            if path > last {
                return Ok(intents);
            }
            let intent = WriteIntent::from_bytes(&value);
            let intent = intent.map_err(|error| ItemError::System(error.to_string()))?;
            intents
                .entry(path.to_string())
                .or_default()
                .push((key, intent));
        }
        match next {
            Some(next) => start = IterStart::After(next),
            None => return Ok(intents),
        }
    }
}

/// `page_intents` for a caller outside the run, such as a preview.
pub(crate) async fn listed_intents(
    context: &DriverContext,
    rule: &BackendImport,
    objects: &[ExternalObject],
) -> Result<BTreeMap<String, Vec<(Key, WriteIntent)>>, String> {
    page_intents(context, rule, objects)
        .await
        .map_err(ItemError::reason)
}

/// The path of an intent key: the import id, the path, a zero byte and the version id.
fn intent_path(key: &[u8]) -> Option<&str> {
    let end = key.len().checked_sub(17)?;
    let path = key.get(16..end)?;
    (key[end] == 0).then_some(())?;
    std::str::from_utf8(path).ok()
}

/// Covers the literal-name head `version_id` of `literal` with a delete marker. The normal delete
/// path adjusts the live charges, keeps the version for pinned references and keeps the bytes.
async fn hide_alias(
    context: &DriverContext,
    spec: &BackendImportSpec,
    rule: &BackendImport,
    literal: &str,
    version_id: Ulid,
) -> Result<(), ItemError> {
    if read_head(context, &rule.bucket, literal).await? != Some(version_id) {
        return Ok(());
    }
    let input = DeleteObjectInput {
        bucket: rule.bucket.clone(),
        key: literal.to_string(),
        version_id: None,
        group_id: rule.group_id,
        realm_id: spec.auth_context.realm_id,
        node_id: spec.node_id,
        deleted_by: spec.auth_context.user_id,
    };
    let restrictions = spec.auth_context.path_restrictions.clone();
    let delete = DeleteObjectOperation::new(input).with_head(version_id);
    match drive(delete.with_restrictions(restrictions), context).await {
        // A write to the literal key meanwhile is the tenant's own and stays current.
        Ok(_) | Err(DeleteObjectError::VersionChanged) => Ok(()),
        Err(error @ DeleteObjectError::StorageError(_)) => {
            Err(ItemError::System(error.to_string()))
        }
        Err(error) => Err(item(error.to_string())),
    }
}

/// Whether the delete trail removed version `id` of `key`, or the key without a head. Only the
/// deletes of this key and the purges of this incarnation of the bucket are read.
async fn deleted_on_purpose(
    context: &DriverContext,
    run: &ImportRun,
    key: &str,
    id: Ulid,
) -> Result<bool, ItemError> {
    trail_of(context, &run.rule, run.incarnation, key, id).await
}

async fn trail_of(
    context: &DriverContext,
    rule: &BackendImport,
    incarnation: u128,
    key: &str,
    id: Ulid,
) -> Result<bool, ItemError> {
    let (group_id, bucket) = (rule.group_id, &rule.bucket);
    let mut trail = Trail::Clean;
    for prefix in trail_ranges(group_id, (bucket, incarnation), key) {
        for (_, value) in iterate(context, DELETE_TRAIL_KEYSPACE, prefix).await? {
            let record = BlobAuditRecord::from_bytes(&value);
            let record = record.map_err(|error| ItemError::System(error.to_string()))?;
            trail = trail.add(&record, bucket, key, id);
        }
    }
    Ok(trail != Trail::Clean)
}

/// Every entry under `prefix`, read in pages.
async fn iterate(
    context: &DriverContext,
    key_space: &str,
    prefix: Key,
) -> Result<Vec<(Key, Value)>, ItemError> {
    let mut entries = Vec::new();
    let mut start = None;
    loop {
        let effect = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.clone()),
            start: start.take().map(IterStart::After),
            limit: LIST_PAGE,
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                entries.extend(values);
                match next_start_after {
                    Some(next) => start = Some(next),
                    None => return Ok(entries),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(ItemError::System(error.to_string()));
            }
            other => return Err(ItemError::System(format!("unexpected answer {other:?}"))),
        }
    }
}

/// The ids of the rules this run saw sharing its folder.
fn sharing(run: &ImportRun) -> std::collections::BTreeSet<Ulid> {
    run.shared
        .iter()
        .map(|other| other.rule.import_id)
        .collect()
}

/// `None` when this rule imports an unmapped file of a shared folder; else the outcome.
fn claimed(run: &ImportRun, path: &str) -> Result<Option<Outcome>, ItemError> {
    let own = (&run.rule, run.earlier.as_slice(), run.discovery.as_slice());
    let taken = run.page.taken.contains(path);
    match claim_file(own, &run.shared, taken, path) {
        shared::Claim::Own => Ok(None),
        shared::Claim::Other => Ok(Some(Outcome::Ignored)),
        shared::Claim::Conflict => Err(item(SHARED_CONFLICT)),
    }
}

/// The rows other rules of a shared folder would record for `path`, which the commit must
/// still find absent; a rule importing the file at the same time then conflicts.
fn shared_guards(run: &ImportRun, path: &str) -> Vec<(String, Key, Option<Value>)> {
    let readers = run.shared.iter().map(|other| &other.rule);
    readers
        .filter(|other| path.starts_with(&other.backend_prefix))
        .map(|other| {
            let key = object_key(other.import_id, path);
            (IMPORTED_OBJECT_KEYSPACE.to_string(), key, None)
        })
        .collect()
}

const SHARED_CONFLICT: &str =
    "several rules read this folder and none or more than one names the file";

/// An unmapped file whose key holds a version of the same content is a second copy of that
/// version: its row names the version, so the copy cleanup can let the key keep one file.
/// Other content fails as a conflict.
async fn register_twin(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    object: &ExternalObject,
    (key, head, resolved): (&str, Option<Ulid>, ResolvedBackend),
) -> Result<Outcome, ItemError> {
    let rule = &run.rule;
    let candidates = candidates(&ctx.driver, rule, key, object).await?;
    let (mut location, version, verified) = hash_object(ctx, spec, resolved, object).await?;
    if location.blob_size != object.size {
        return Err(item("the object changed while it was read"));
    }
    let object = &observed(object, verified);
    let same = (candidates.into_iter()).find(|(.., hash)| location.get_blake3() == Some(&hash[..]));
    let Some((stored, value, version_id, _)) = same else {
        return add_version(ctx, spec, run, object, (key, head), (location, version)).await;
    };
    location.format.layout = StoredLayout::Plain(location.ulid);
    let row = ImportedObject {
        key: Some(key.to_string()),
        ..ImportedObject::new(version_id, object)
    };
    let mut guards = shared_guards(run, &object.key);
    guards.push(rule_guard(rule)?);
    guards.push((BLOB_VERSIONS_KEYSPACE.to_string(), stored, Some(value)));
    // A file a release is deleting is no copy to keep.
    let deleting = version.as_deref().unwrap_or_default();
    let claim = aruna_core::structs::storage::backend_import::claim_key(&location, deleting)
        .map_err(|error| ItemError::System(error.to_string()))?;
    guards.push((DELETE_CLAIM_KEYSPACE.to_string(), claim, None));
    // The copy is found by its key like a renamed row, and keeps its file and version there.
    let prefix = VersionKey::object_prefix(&rule.bucket, key)
        .map_err(|error| ItemError::Item(error.to_string()))?;
    let index = [
        &rule.import_id.to_bytes()[..],
        &prefix[..],
        object.key.as_bytes(),
    ]
    .concat();
    let file = location
        .to_bytes()
        .map_err(|error| ItemError::System(error.to_string()))?;
    let mut writes = vec![(RENAMED_ROW_KEYSPACE.to_string(), index.into(), file.into())];
    if let Some(version) = version {
        let provider = version_key(location.ulid);
        writes.push((
            BACKEND_VERSION_KEYSPACE.to_string(),
            provider,
            version.into_bytes().into(),
        ));
    }
    let note = Note {
        guards: &guards,
        retired: Vec::new(),
        writes,
        sharing: Some(sharing(run)),
        fenced: Some(key),
    };
    let row_key = object_key(rule.import_id, &object.key);
    // A refused copy is looked at again by the next run.
    match note_listing(&ctx.driver, rule, row_key, None, encode(&row)?, note).await? {
        true => Ok(Outcome::Imported),
        false => Ok(Outcome::Skipped),
    }
}

/// Version rows one page of the search for the same content reads.
const TWIN_SCAN: usize = 1024;

/// A version id of the time `at`.
fn dated(at: u64) -> Ulid {
    Ulid::from_parts(at, Ulid::generate().random())
}

/// A version id dated `at`, or now; a distributed bucket always dates by mint time and adds
/// its set hint, and the source time stays in the import row.
fn import_version(run: &ImportRun, at: Option<u64>) -> Ulid {
    match (run.bucket.distribution.as_ref(), at) {
        (Some(binding), _) => mint_version(Some(binding.hint())),
        (None, Some(at)) => dated(at),
        (None, None) => Ulid::generate(),
    }
}

/// A file with other bytes than every version of its key is another version of it, dated by
/// its modification time: the newest version becomes current, an older one joins the history.
async fn add_version(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    object: &ExternalObject,
    (key, head): (&str, Option<Ulid>),
    file: (BackendLocation, Option<String>),
) -> Result<Outcome, ItemError> {
    let rule = &run.rule;
    let version_id = import_version(run, Some(object.modified_ms.unwrap_or_else(now_ms)));
    let committed = ImportedObject {
        key: Some(key.to_string()),
        ..ImportedObject::new(version_id, object)
    };
    let keep_head = head.is_some_and(|head| head > version_id);
    let fence = Fence {
        head,
        keep_head,
        guards: shared_guards(run, &object.key),
        retired: Vec::new(),
        trail: None,
        keep_row: false,
        copy_index: None,
    };
    let size = file.0.blob_size;
    let put = put_version(ctx, spec, run, key.to_string(), file, committed, fence);
    match put.await {
        Ok(()) => {}
        // Another write came first; the next run looks again.
        Err(
            PutObjectError::HeadChanged
            | PutObjectError::VersionExists
            | PutObjectError::HistoryDeleted,
        ) => return Ok(Outcome::Skipped),
        Err(error) if moved_away(&error) => return Ok(Outcome::Ignored),
        Err(error) => return Err(put_failure(error)),
    }
    if !keep_head {
        let auth = spec.auth_context.clone();
        let (realm_id, group_id) = (auth.realm_id, rule.group_id);
        let (bucket, node_id) = (rule.bucket.clone(), spec.node_id);
        let key = key.to_string();
        complete_put(
            &ctx.driver,
            realm_id,
            node_id,
            auth,
            group_id,
            bucket,
            key,
            version_id,
            size,
        )
        .await;
    }
    Ok(Outcome::Imported)
}

/// The key index row of `path` when its row records a second copy, which a new version of
/// the file retires.
async fn copy_index(
    context: &DriverContext,
    rule: &BackendImport,
    row: Option<&ImportedObject>,
    path: &str,
) -> Result<Option<Key>, ItemError> {
    let Some(key) = row.and_then(|row| row.key.as_deref()) else {
        return Ok(None);
    };
    let prefix = VersionKey::object_prefix(&rule.bucket, key)
        .map_err(|error| ItemError::Item(error.to_string()))?;
    let index: Key = [&rule.import_id.to_bytes()[..], &prefix[..], path.as_bytes()]
        .concat()
        .into();
    let [value] = read_values(context, [(RENAMED_ROW_KEYSPACE, index.clone())]).await?;
    Ok(value.filter(|value| !value.is_empty()).map(|_| index))
}

/// The versions of `key` whose bytes `object` may hold: read from a file of the rule's
/// storage, of its size, and not refuted by its single-part ETag. Each comes with its key,
/// stored row, id and content hash.
async fn candidates(
    context: &DriverContext,
    rule: &BackendImport,
    key: &str,
    object: &ExternalObject,
) -> Result<Vec<(Key, Value, Ulid, [u8; 32])>, ItemError> {
    let prefix = VersionKey::object_prefix(&rule.bucket, key)
        .map_err(|error| ItemError::Item(error.to_string()))?;
    // Every version is compared, so a miss proves other bytes.
    let mut rows = Vec::new();
    let mut after = None;
    loop {
        let page = super::store::iter_prefix_page(
            &context.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            Some(prefix.clone().into()),
            after.take(),
            TWIN_SCAN,
            None,
        );
        let (page, next) = page.await.map_err(ItemError::System)?;
        rows.extend(page);
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let backend = BackendRef::Group(rule.backend_id);
    let mut found = Vec::new();
    for (stored, value) in rows {
        let version = BlobVersion::from_bytes(&value);
        let version = version.map_err(|error| ItemError::System(error.to_string()))?;
        let BlobVersionState::Materialized {
            blob_hash,
            backend: on,
            encoding: EncodingClass::Plain { .. } | EncodingClass::Raw,
            ..
        } = &version.state
        else {
            continue;
        };
        let Some(location_key) = version.location_key().filter(|_| *on == backend) else {
            continue;
        };
        let [location] = read_values(
            context,
            [(BLOB_LOCATIONS_KEYSPACE, location_key.to_bytes().into())],
        )
        .await?;
        let Some(mut location) = parse(location, BackendLocation::from_bytes)? else {
            continue;
        };
        let md5 = location.hashes.remove(HASH_MD5);
        let refuted = etag_differs(object.etag.as_deref(), md5.as_deref());
        if location.blob_size != object.size || refuted {
            continue;
        }
        let id = VersionKey::from_bytes(&stored)
            .map_err(|error| ItemError::System(error.to_string()))?
            .version_id;
        found.push((stored, value, id, *blob_hash));
    }
    Ok(found)
}

/// An unmapped object whose key the bucket holds fails, even when the other write came first.
fn taken(key: &str) -> ItemError {
    ItemError::Item(format!("the key `{key}` already holds another object"))
}

/// A recorded mapping is kept; only an unmapped object is placed by the discovery rules.
fn bucket_key(
    (rule, earlier, discovery): (&BackendImport, &[String], &[DiscoveryRule]),
    row: Option<&ImportedObject>,
    path: &str,
) -> Result<String, ItemError> {
    let key = match row {
        Some(row) => row.key.clone().or_else(|| rule.aruna_key(path)),
        None => match file_identity(rule, earlier, discovery, path) {
            Some(Err(error)) => return Err(identity_error(error)),
            placed => placed.and_then(Result::ok).map(|placed| placed.key),
        },
    };
    Ok(key.unwrap_or_default())
}

/// The row an equal size updates without reading the bytes: the first listing of Aruna's own
/// write, or any listing without ETag and time.
fn trusted_listing(
    row: Option<&ImportedObject>,
    object: &ExternalObject,
) -> Option<ImportedObject> {
    let row = row?;
    let unmarked = object.etag.is_none() && object.modified_ms.is_none();
    row.first_listing(object)
        .or_else(|| (unmarked && row.size == object.size).then(|| row.clone()))
}

/// `None` when the committed version is gone; else the MD5 of its stored copy, read only
/// for a listing with an ETag.
async fn committed_md5(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    row: &ImportedObject,
    object: &ExternalObject,
) -> Result<Option<Option<Vec<u8>>>, ItemError> {
    let version_key = VersionKey::new(bucket, key, row.version_id)
        .to_bytes()
        .map_err(|error| ItemError::Item(error.to_string()))?;
    let [version] = read_values(context, [(BLOB_VERSIONS_KEYSPACE, version_key.into())]).await?;
    let Some(version) = parse(version, BlobVersion::from_bytes)? else {
        return Ok(None);
    };
    let Some(location_key) = version.location_key() else {
        return Ok(None);
    };
    if object.etag.is_none() {
        return Ok(Some(None));
    }
    let [location] = read_values(
        context,
        [(BLOB_LOCATIONS_KEYSPACE, location_key.to_bytes().into())],
    )
    .await?;
    let location = parse(location, BackendLocation::from_bytes)?;
    Ok(Some(
        location.and_then(|mut location| location.hashes.remove(HASH_MD5)),
    ))
}

/// A single-part ETag is the MD5 of the bytes; a multipart or other ETag proves nothing.
fn etag_differs(etag: Option<&str>, md5: Option<&[u8]>) -> bool {
    let (Some(etag), Some(md5)) = (etag.map(|etag| etag.trim_matches('"')), md5) else {
        return false;
    };
    etag.len() == 32
        && etag.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !etag.eq_ignore_ascii_case(&hex::encode(md5))
}

/// What one exact version id holds for a file that names it.
#[derive(Debug, PartialEq)]
enum Exact {
    Absent,
    /// The content hash, and the stored record a reconciling transaction compares again.
    Materialized([u8; 32], Value),
    /// A reference, pending content or deleted version can never be the file.
    Incompatible,
}

/// The key and state of version `version_id` of `key` in `bucket`.
async fn exact_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<(Key, Exact), ItemError> {
    let version_key: Key = VersionKey::new(bucket, key, version_id)
        .to_bytes()
        .map_err(|error| ItemError::Item(error.to_string()))?
        .into();
    let [value] = read_values(context, [(BLOB_VERSIONS_KEYSPACE, version_key.clone())]).await?;
    let Some(value) = value else {
        return Ok((version_key, Exact::Absent));
    };
    let version = BlobVersion::from_bytes(&value);
    let version = version.map_err(|error| ItemError::System(error.to_string()))?;
    let exact = match version.blob_hash() {
        Some(hash) => Exact::Materialized(*hash, value),
        None => Exact::Incompatible,
    };
    Ok((version_key, exact))
}

/// A hashed file, its backend version, and whether its read showed the listing's ETag.
type Hashed = (BackendLocation, Option<String>, bool);

/// Hashes the listed `object`; bytes with another ETag than the listing's are left for the
/// next run, so a row never pairs one file's hash with another's ETag.
async fn hash_object(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    resolved: ResolvedBackend,
    object: &ExternalObject,
) -> Result<Hashed, ItemError> {
    let effect = BlobEffect::HashExternal {
        resolved,
        path: object.key.clone(),
        created_by: spec.auth_context.user_id,
    };
    match drive(BlobRequest::new(effect), &ctx.driver).await {
        Ok(BlobEvent::ExternalHashed {
            location,
            version,
            etag,
        }) => match read_listed(object.etag.as_deref(), etag.as_deref()) {
            Some(verified) => Ok((location, version, verified)),
            None => Err(ItemError::Changed),
        },
        Ok(other) => Err(ItemError::System(format!("unexpected answer {other:?}"))),
        Err(error) => Err(hash_failure(error)),
    }
}

/// `None` when the hashed read answered with another ETag than the listing; else whether it
/// showed the listing's ETag, which only then may be recorded for the hashed bytes.
fn read_listed(listed: Option<&str>, read: Option<&str>) -> Option<bool> {
    let unquoted = |etag: &str| etag.trim_matches('"').to_string();
    match (listed, read) {
        (Some(listed), Some(read)) => (unquoted(listed) == unquoted(read)).then_some(true),
        (Some(_), None) => Some(false),
        (None, _) => Some(true),
    }
}

/// The listed object as its rows record it: without an ETag its read did not show.
fn observed(object: &ExternalObject, verified: bool) -> ExternalObject {
    ExternalObject {
        etag: object.etag.clone().filter(|_| verified),
        ..object.clone()
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

/// What the version transaction must still find, and whether the version only joins history.
struct Fence {
    head: Option<Ulid>,
    keep_head: bool,
    guards: Vec<(String, Key, Option<Value>)>,
    /// The write intents the version makes obsolete, deleted in its transaction.
    retired: Vec<Key>,
    /// The version the delete trail must not show as removed.
    trail: Option<Ulid>,
    /// The reverse row stays as it was read; a later step rewrites it.
    keep_row: bool,
    /// The key index row of a second copy the new version replaces, retired with it.
    copy_index: Option<Key>,
}

/// The rows a version commits with: its reverse row, unless kept as read, and the backend
/// version, so a later same-name overwrite does not change what the version reads.
fn import_rows(
    rule: &BackendImport,
    location: &BackendLocation,
    version: Option<String>,
    row: &ImportedObject,
    keep_row: bool,
) -> Result<Vec<(String, Key, Value)>, aruna_core::errors::ConversionError> {
    let mut rows = Vec::new();
    if !keep_row {
        let row_key = object_key(rule.import_id, &location.backend_path);
        rows.push((
            IMPORTED_OBJECT_KEYSPACE.to_string(),
            row_key,
            row.to_bytes()?.into(),
        ));
    }
    if let Some(version) = version {
        let value = version.into_bytes().into();
        let key = version_key(location.ulid);
        rows.push((BACKEND_VERSION_KEYSPACE.to_string(), key, value));
    }
    Ok(rows)
}

async fn put_version(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    key: String,
    (location, version): (BackendLocation, Option<String>),
    row: ImportedObject,
    fence: Fence,
) -> Result<(), PutObjectError> {
    let mut guards = fence.guards;
    // A file a release is deleting, at its backend version or without one, is not registered
    // again.
    let deleting = version.as_deref().unwrap_or_default();
    let claim = aruna_core::structs::storage::backend_import::claim_key(&location, deleting)?;
    guards.push((DELETE_CLAIM_KEYSPACE.to_string(), claim, None));
    let mut rows = import_rows(&run.rule, &location, version, &row, fence.keep_row)?;
    // The file is the new version's own now: its index row names no second copy any more.
    if let Some(index) = fence.copy_index {
        rows.push((
            RENAMED_ROW_KEYSPACE.to_string(),
            index,
            Value::from(Vec::new()),
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
    .with_version_origin(Some(
        aruna_core::structs::storage::origin::VersionOrigin::Storage {
            backend_id: run.rule.backend_id,
            key: location.backend_path.clone(),
        },
    ))
    .with_adopted(discovered_copy(&run.rule, location))
    .with_bucket_guard(run.bucket.clone())
    .with_restrictions(spec.auth_context.path_restrictions.clone())
    .with_expected_head(fence.head)
    .with_import_rows(run.rule.import_id, rows)
    .with_import_guards(guards)
    .with_rule_scan(RuleScan::new(run.rule.clone(), sharing(run)));
    if fence.keep_head {
        operation = operation.keep_head();
    }
    for key in fence.retired {
        operation = operation.with_retired_intent(key);
    }
    if let Some(version_id) = fence.trail {
        operation = operation.with_trail_check(version_id);
    }
    if let Some(gate) = run.gate.clone() {
        operation = operation.with_gate(gate);
    }
    drive(operation, &ctx.driver).await.map(|_| ())
}

/// Whether the key's range of a distributed bucket is another coordinator's, or moves there;
/// that coordinator's run imports it.
fn moved_away(error: &PutObjectError) -> bool {
    match error {
        PutObjectError::Distribution(error)
        | PutObjectError::PurgeFence(PurgeFenceError::Distribution(error)) => error.moved_away(),
        _ => false,
    }
}

/// Only a refusal of this one object fails it and lets the run go on; storage and other
/// system errors end the run for a retry, so the checkpoint never passes an undecided object.
fn put_failure(error: PutObjectError) -> ItemError {
    use PolicyGateError::{Denied, ForeignPolicy, Invalid, NoSubject, Required};
    match error {
        PutObjectError::QuotaExceeded { .. }
        | PutObjectError::HistoryDeleted
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
    batch_read(context, reads)
        .await?
        .try_into()
        .map_err(|_| ItemError::System("short batch read".to_string()))
}

async fn batch_read(
    context: &DriverContext,
    reads: Vec<(String, Key)>,
) -> Result<Vec<Option<Value>>, ItemError> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let wanted = reads.len();
    let effect = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == wanted => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::BatchReadResult { .. }) => {
            Err(ItemError::System("short batch read".to_string()))
        }
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

/// What else a row update reads and deletes in its transaction.
#[derive(Default)]
struct Note<'a> {
    /// Values that must still be stored as they were read.
    guards: &'a [(String, Key, Option<Value>)],
    /// The write intents the committed row makes obsolete.
    retired: Vec<Key>,
    /// Rows the committed row comes with.
    writes: Vec<(String, Key, Value)>,
    /// The rules that must share the folder at commit, read again in the transaction.
    sharing: Option<std::collections::BTreeSet<Ulid>>,
    /// The bucket key a permanent purge must not hold at commit.
    fenced: Option<&'a str>,
}

/// Replaces the row only while the rule exists and the row still holds `seen` (`None`: no row),
/// in one transaction, and answers whether it committed. Otherwise nothing is written and the
/// next run looks at the object again.
async fn note_listing(
    context: &DriverContext,
    rule: &BackendImport,
    row_key: Key,
    seen: Option<&Value>,
    row: Vec<u8>,
    note: Note<'_>,
) -> Result<bool, ItemError> {
    let storage = &context.storage_handle;
    let unexpected = |event: Event| ItemError::System(format!("unexpected answer {event:?}"));
    let started = StorageEffect::StartTransaction { read: false };
    let txn_id = match storage.send_storage_effect(started).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(unexpected(other)),
    };
    let abort = StorageEffect::AbortTransaction { txn_id };
    let mut reads = vec![
        (BACKEND_IMPORT_KEYSPACE.to_string(), rule.key()),
        (IMPORTED_OBJECT_KEYSPACE.to_string(), row_key.clone()),
    ];
    let guards = note.guards.iter();
    reads.extend(guards.map(|(space, key, _)| (space.clone(), key.clone())));
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
    let expected = note.guards.iter().map(|(.., value)| value.as_ref());
    let held = match values.as_slice() {
        [(_, Some(_)), (_, value), rest @ ..] if value.as_ref() == seen => {
            rest.len() == note.guards.len() && rest.iter().map(|(_, v)| v.as_ref()).eq(expected)
        }
        _ => false,
    };
    let shared = match &note.sharing {
        Some(expected) if held => match sharing_now(context, rule, txn_id).await {
            Ok(found) => found == *expected,
            Err(error) => {
                storage.send_storage_effect(abort).await;
                return Err(error);
            }
        },
        _ => true,
    };
    let open = match note.fenced.filter(|_| held && shared) {
        Some(key) => {
            let read = StorageEffect::Read {
                key_space: PURGE_FENCE_KEYSPACE.to_string(),
                key: crate::s3::purge_fence::fence_key(&rule.bucket),
                txn_id: Some(txn_id),
            };
            let event = storage.send_storage_effect(read).await;
            match crate::s3::purge_fence::check_write_fence(event, &rule.bucket, key) {
                Ok(()) => true,
                Err(crate::s3::purge_fence::PurgeFenceError::Suspended) => false,
                Err(error) => {
                    storage.send_storage_effect(abort).await;
                    return Err(ItemError::System(error.to_string()));
                }
            }
        }
        None => true,
    };
    if !held || !shared || !open {
        storage.send_storage_effect(abort).await;
        return Ok(false);
    }
    let write = StorageEffect::Write {
        key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
        key: row_key,
        value: row.into(),
        txn_id: Some(txn_id),
    };
    let retire = note.retired.into_iter().map(|key| StorageEffect::Delete {
        key_space: IMPORT_INTENT_KEYSPACE.to_string(),
        key,
        txn_id: Some(txn_id),
    });
    let rows = note
        .writes
        .into_iter()
        .map(|(key_space, key, value)| StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        });
    for effect in std::iter::once(write).chain(rows).chain(retire) {
        match storage.send_storage_effect(effect).await {
            Event::Storage(
                StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
            ) => {}
            other => {
                storage.send_storage_effect(abort).await;
                return Err(unexpected(other));
            }
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(true),
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Ok(false),
        other => Err(unexpected(other)),
    }
}

/// The rules that share the folder of `rule` as `txn_id` reads them.
async fn sharing_now(
    context: &DriverContext,
    rule: &BackendImport,
    txn_id: Ulid,
) -> Result<std::collections::BTreeSet<Ulid>, ItemError> {
    let mut found = std::collections::BTreeSet::new();
    let mut after = None;
    loop {
        let page = super::store::iter_prefix_page(
            &context.storage_handle,
            BACKEND_IMPORT_KEYSPACE,
            Some(rule.group_id.to_bytes().to_vec().into()),
            after.take(),
            128,
            Some(txn_id),
        );
        let (rows, next) = page.await.map_err(ItemError::System)?;
        for (_, value) in rows {
            let other = BackendImport::from_bytes(&value);
            let other = other.map_err(|error| ItemError::System(error.to_string()))?;
            if crate::groups::imports::shared::shares_folder(rule, &other) {
                found.insert(other.import_id);
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(found),
        }
    }
}

/// Emits one listing or hashing effect and hands back the blob adapter's answer.
#[derive(Debug, PartialEq)]
pub(crate) struct BlobRequest {
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
    pub(crate) fn new(effect: BlobEffect) -> Self {
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
    use aruna_core::structs::storage::backend_import::intent_key;

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
    fn checks_read_etags() {
        assert_eq!(read_listed(None, None), Some(true));
        assert_eq!(read_listed(Some("\"a\""), Some("a")), Some(true));
        assert_eq!(read_listed(Some("a"), Some("b")), None);
        // A read without an ETag imports the file, which then records none.
        assert_eq!(read_listed(Some("a"), None), Some(false));
        let listed = object("\"a\"");
        assert_eq!(observed(&listed, false).etag, None);
        assert_eq!(observed(&listed, true), listed);
    }

    #[test]
    fn counts_outcomes() {
        let mut checkpoint = BackendImportCheckpoint::default();
        assert_eq!(tally(&mut checkpoint, "a", Ok(Outcome::Skipped)), Ok(false));
        assert_eq!(tally(&mut checkpoint, "b", Ok(Outcome::Imported)), Ok(true));
        let failed = Err(ItemError::Item("broken".to_string()));
        assert_eq!(tally(&mut checkpoint, "c", failed), Ok(true));
        assert_eq!(tally(&mut checkpoint, "d", Ok(Outcome::Ignored)), Ok(false));
        // A file changed since its listing is skipped and read again by the next run.
        assert_eq!(
            tally(&mut checkpoint, "d", Err(ItemError::Changed)),
            Ok(false)
        );
        assert_eq!(
            (checkpoint.skipped, checkpoint.imported, checkpoint.failed),
            (2, 1, 1)
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
        // Deleted history is reported for the file; the run goes on.
        assert!(item(PutObjectError::HistoryDeleted));
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
        let written = ImportedObject::written(Ulid::nil(), "in/a.csv".to_string(), 4, None);
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
        let note = note_listing(
            &context,
            &rule,
            row_key.clone(),
            Some(&seen),
            update.clone(),
            Note::default(),
        );
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
        let note = note_listing(
            &context,
            &rule,
            row_key.clone(),
            Some(&other),
            update.clone(),
            Note::default(),
        );
        assert!(note.await.is_ok());
        assert_eq!(stored().await, written);

        // A version deleted since it was compared keeps the row and the intent as they are.
        let intent = intent_key(rule.import_id, "raw/a.csv", Ulid::nil());
        let write = write_value(&context, IMPORT_INTENT_KEYSPACE, intent.clone(), vec![1]);
        assert!(write.await.is_ok());
        let version = (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            Key::from(vec![7u8]),
            Some(Value::from(vec![8u8])),
        );
        let guards = [version];
        let note = |retired| Note {
            guards: &guards,
            retired,
            writes: Vec::new(),
            sharing: None,
            fenced: None,
        };
        let held = note_listing(
            &context,
            &rule,
            row_key.clone(),
            Some(&seen),
            update.clone(),
            note(vec![intent.clone()]),
        );
        assert!(held.await.is_ok());
        assert_eq!(stored().await, written);
        let intent_left = || async {
            let read = read_values(&context, [(IMPORT_INTENT_KEYSPACE, intent.clone())]).await;
            matches!(read, Ok([Some(_)]))
        };
        assert!(intent_left().await);

        // The committed row retires the intent in the same transaction.
        let write = write_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            guards[0].1.clone(),
            vec![8],
        );
        assert!(write.await.is_ok());
        let retired = vec![intent.clone()];
        let done = note_listing(
            &context,
            &rule,
            row_key.clone(),
            Some(&seen),
            update,
            note(retired),
        );
        assert!(done.await.is_ok());
        assert_eq!(stored().await, listed);
        assert!(!intent_left().await);
    }

    #[tokio::test]
    async fn retires_move_intents() {
        // A move tags with its attempt id; its intents of the version go wherever the file is.
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let (import_id, id, attempt) = (
            Ulid::from_parts(4, 4),
            Ulid::from_parts(5, 5),
            Ulid::from_parts(6, 6),
        );
        let tagged = format!("raw/a~{attempt}");
        let moved = intent_key(import_id, &tagged, id);
        let other = intent_key(import_id, &tagged, Ulid::from_parts(7, 7));
        for key in [moved.clone(), other] {
            assert!(
                write_value(&context, IMPORT_INTENT_KEYSPACE, key, vec![1])
                    .await
                    .is_ok()
            );
        }
        let mut expected = intent_keys(import_id, "raw/a", id, true);
        expected.push(moved);
        for path in ["raw/a", tagged.as_str()] {
            let found = stored_intents(&context, import_id, path, id).await;
            let found = found.ok().unwrap();
            assert!(expected.iter().all(|key| found.contains(key)), "{path}");
        }
        // A suffix name ends in the version id; its move tags it once more.
        let named = format!("raw/b~{id}");
        let sibling = intent_key(import_id, &format!("{named}~{attempt}"), id);
        let write = write_value(&context, IMPORT_INTENT_KEYSPACE, sibling.clone(), vec![1]);
        assert!(write.await.is_ok());
        let found = stored_intents(&context, import_id, &named, id).await;
        let found = found.ok().unwrap();
        assert!(found.contains(&sibling));
        assert!(found.contains(&intent_key(import_id, &named, id)));
    }

    #[test]
    fn retires_both_destinations() {
        let (import_id, id) = (Ulid::from_parts(4, 4), Ulid::from_parts(5, 5));
        let tagged = format!("raw/a~{id}");
        let expected = intent_keys(import_id, "raw/a", id, true);
        // Whichever path the bytes reached, both intents of the write go.
        assert_eq!(write_intents(import_id, "raw/a", id), expected);
        assert_eq!(write_intents(import_id, &tagged, id), expected);
    }

    #[test]
    fn keeps_alias_row() {
        let rule = recovery_rule("{dir}{id}_{name}");
        let location = BackendLocation {
            backend: BackendRef::Group(rule.backend_id),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "raw/a.csv".to_string(),
            ulid: Ulid::from_parts(7, 7),
            format: Default::default(),
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: Default::default(),
        };
        let row = ImportedObject::new(Ulid::nil(), &object("e1"));
        let spaces = |keep_row| {
            let rows = import_rows(&rule, &location, Some("v1".to_string()), &row, keep_row);
            let rows = rows.ok().unwrap();
            rows.into_iter()
                .map(|(space, ..)| space)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            spaces(false),
            [IMPORTED_OBJECT_KEYSPACE, BACKEND_VERSION_KEYSPACE]
        );
        // An alias keeps its literal row until the alias is hidden after the commit.
        assert_eq!(spaces(true), [BACKEND_VERSION_KEYSPACE]);
    }

    #[tokio::test]
    async fn hides_literal_alias() {
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let rule = recovery_rule("{dir}{id}_{name}");
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        let spec = BackendImportSpec {
            auth_context: aruna_core::structs::identity::auth::AuthContext {
                user_id: UserId::local(Ulid::from_parts(2, 2), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            group_id: rule.group_id,
            import_id: rule.import_id,
        };
        let (alias, other) = (Ulid::from_parts(7, 7), Ulid::from_parts(8, 8));
        let literal = "in/x_a.csv";
        let version = BlobVersion::materialized(
            [7; 32],
            BackendRef::Group(rule.backend_id),
            aruna_core::structs::storage::format::EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        for id in [alias, other] {
            let key = Key::from(VersionKey::new("bucket", literal, id).to_bytes().unwrap());
            let value = version.to_bytes().unwrap();
            let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value);
            assert!(write.await.is_ok());
        }
        let head_key = Key::from(BlobHeadKey::new("bucket", literal).to_bytes().unwrap());
        let set_head = |id| {
            let value = CurrentVersionPointer::new(id).to_bytes().unwrap();
            write_value(&context, BLOB_HEAD_KEYSPACE, head_key.clone(), value)
        };
        let head = || read_head(&context, "bucket", literal);

        // A write to the literal key meanwhile stays current.
        assert!(set_head(other).await.is_ok());
        let hidden = hide_alias(&context, &spec, &rule, literal, alias);
        assert!(hidden.await.is_ok());
        assert_eq!(head().await.ok().flatten(), Some(other));

        // The alias head gets a delete marker; its version stays for pinned references.
        assert!(set_head(alias).await.is_ok());
        let hidden = hide_alias(&context, &spec, &rule, literal, alias);
        assert!(hidden.await.is_ok());
        let marker = head().await.ok().flatten().unwrap();
        assert_ne!(marker, alias);
        let kept = exact_version(&context, "bucket", literal, alias).await;
        assert!(matches!(kept, Ok((_, Exact::Materialized(..)))));
        let (_, marker) = exact_version(&context, "bucket", literal, marker)
            .await
            .ok()
            .unwrap();
        assert_eq!(marker, Exact::Incompatible);
    }

    #[tokio::test]
    async fn reads_key_trail() {
        use aruna_core::structs::storage::delete_audit::{BlobAuditKind, trail_key};
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let rule = recovery_rule("{dir}{name}");
        let id = Ulid::from_parts(1_000, 1);
        let deleted = || trail_of(&context, &rule, 5, "in/a.csv", id);
        assert!(!deleted().await.ok().unwrap());
        let record = |key: &str| aruna_core::structs::storage::delete_audit::BlobAuditRecord {
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
            group_id: rule.group_id,
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            user_id: Default::default(),
            kind: BlobAuditKind::DeleteMarker,
            bucket: "bucket".to_string(),
            key: key.to_string(),
            version_id: Some(Ulid::nil()),
            occurred_at_ms: 2_000,
        };
        // A delete of another key is not read at all.
        for key in ["in/a.csv2", "in/a.csv"] {
            let record = record(key);
            let trail = Key::from(trail_key(&record, 5, Ulid::generate()));
            let value = record.to_bytes().unwrap();
            let write = write_value(&context, DELETE_TRAIL_KEYSPACE, trail, value);
            assert!(write.await.is_ok());
            assert_eq!(deleted().await.ok().unwrap(), key == "in/a.csv");
        }
        // A bucket created again under the name starts with a clean trail.
        let fresh = trail_of(&context, &rule, 6, "in/a.csv", id).await;
        assert!(!fresh.ok().unwrap());
    }

    #[tokio::test]
    async fn classifies_exact_versions() {
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let id = Ulid::from_parts(9, 9);
        let exact = || async {
            let found = exact_version(&context, "bucket", "in/a.csv", id).await;
            found.ok().unwrap().1
        };
        assert_eq!(exact().await, Exact::Absent);
        // A deleted version holds no bytes, so a file naming it is a conflict, not new.
        let deleted = BlobVersion::deleted(std::time::SystemTime::UNIX_EPOCH, Default::default());
        let key = Key::from(
            VersionKey::new("bucket", "in/a.csv", id)
                .to_bytes()
                .unwrap(),
        );
        let write = write_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            key.clone(),
            deleted.to_bytes().unwrap(),
        );
        assert!(write.await.is_ok());
        assert_eq!(exact().await, Exact::Incompatible);
        let version = BlobVersion::materialized(
            [7; 32],
            BackendRef::node_default(),
            aruna_core::structs::storage::format::EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let value = version.to_bytes().unwrap();
        let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value.clone());
        assert!(write.await.is_ok());
        assert_eq!(exact().await, Exact::Materialized([7; 32], value.into()));
    }

    #[tokio::test]
    async fn reads_committed_md5() {
        use aruna_core::structs::storage::blob::BlobLocationKey;
        use aruna_core::structs::storage::format::EncodingClass;
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let row = ImportedObject::written(Ulid::from_parts(6, 6), "in/a.csv".to_string(), 4, None);
        let listed = object("e1");
        let md5 = || committed_md5(&context, "bucket", "in/a.csv", &row, &listed);
        assert_eq!(md5().await.ok(), Some(None));

        let backend = BackendRef::Group(Ulid::from_parts(5, 5));
        let version = BlobVersion::materialized(
            [7; 32],
            backend.clone(),
            EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let key = VersionKey::new("bucket", "in/a.csv", row.version_id);
        let key = Key::from(key.to_bytes().unwrap());
        let value = version.to_bytes().unwrap();
        let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value);
        assert!(write.await.is_ok());
        // A version without a location row has no MD5 to compare.
        assert_eq!(md5().await.ok(), Some(Some(None)));

        let location = BackendLocation {
            backend: backend.clone(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: "in/a.csv".to_string(),
            ulid: Ulid::from_parts(7, 7),
            format: Default::default(),
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: [(HASH_MD5.to_string(), vec![1; 16])].into(),
        };
        let key = BlobLocationKey::new([7; 32], EncodingClass::Raw, backend).to_bytes();
        let value = location.to_bytes().unwrap();
        let write = write_value(&context, BLOB_LOCATIONS_KEYSPACE, key.into(), value);
        assert!(write.await.is_ok());
        assert_eq!(md5().await.ok(), Some(Some(Some(vec![1; 16]))));
        // A listing without an ETag never reads the location.
        let mut unmarked = object("e1");
        unmarked.etag = None;
        let blind = committed_md5(&context, "bucket", "in/a.csv", &row, &unmarked);
        assert_eq!(blind.await.ok(), Some(Some(None)));
    }

    #[test]
    fn compares_single_etags() {
        let md5 = [0xab; 16];
        let etag = format!("\"{}\"", hex::encode(md5));
        assert!(!etag_differs(Some(&etag), Some(&md5)));
        assert!(!etag_differs(Some(&etag.to_uppercase()), Some(&md5)));
        assert!(etag_differs(Some(&etag), Some(&[0xcd; 16])));
        // Multipart ETags, unknown MD5s and missing ETags fall back to the size.
        let multipart = format!("{}-3", hex::encode([0xcd; 16]));
        assert!(!etag_differs(Some(&multipart), Some(&md5)));
        assert!(!etag_differs(Some(&etag), None));
        assert!(!etag_differs(None, Some(&md5)));
    }

    fn recovery_rule(pattern: &str) -> BackendImport {
        BackendImport {
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
            write_pattern: pattern.to_string(),
        }
    }

    /// The hash every test claim recorded for its written bytes.
    const HASH: [u8; 32] = [7; 32];

    #[tokio::test]
    async fn finds_copy_candidates() {
        // Only versions a file of this size and ETag could hold are hashed against: on the
        // rule's storage, of a linked encoding, of the size and not refuted by the ETag.
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let rule = recovery_rule(aruna_core::structs::storage::backend_import::DEFAULT_PATTERN);
        let ours = BackendRef::Group(rule.backend_id);
        let other = BackendRef::Group(Ulid::from_parts(6, 6));
        let raw = EncodingClass::Raw;
        let plain = |at: u64| EncodingClass::Plain {
            location: Ulid::from_parts(at, 1),
        };
        let md5 = |byte: u8| Some(vec![byte; 16]);
        let seeds = [
            (1, ours.clone(), raw, 4, None),
            (2, other, plain(2), 4, None),
            (3, ours.clone(), raw, 9, None),
            (4, ours.clone(), plain(4), 4, md5(0)),
            (5, ours, plain(5), 4, md5(1)),
        ];
        for (at, backend, encoding, size, md5) in seeds {
            let id = Ulid::from_parts(at, 9);
            let hash = [at as u8; 32];
            let version = BlobVersion::materialized(
                hash,
                backend.clone(),
                encoding,
                std::time::SystemTime::UNIX_EPOCH,
                Default::default(),
                None,
            );
            let location_key = version.location_key().unwrap();
            let mut hashes = std::collections::HashMap::from([(
                aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
                hash.to_vec(),
            )]);
            if let Some(md5) = md5 {
                hashes.insert(HASH_MD5.to_string(), md5);
            }
            let location = BackendLocation {
                backend,
                storage_class: None,
                root: String::new(),
                storage_bucket: String::new(),
                backend_path: format!("raw/{at}"),
                ulid: Ulid::from_parts(at, 1),
                format: Default::default(),
                created_by: Default::default(),
                created_at: std::time::SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: size,
                hashes,
            };
            let version_key = VersionKey::new("bucket", "in/a", id).to_bytes().unwrap();
            let rows = [
                (
                    BLOB_VERSIONS_KEYSPACE,
                    version_key,
                    version.to_bytes().unwrap(),
                ),
                (
                    BLOB_LOCATIONS_KEYSPACE,
                    location_key.to_bytes(),
                    location.to_bytes().unwrap(),
                ),
            ];
            for (space, key, value) in rows {
                assert!(
                    write_value(&context, space, key.into(), value)
                        .await
                        .is_ok()
                );
            }
        }
        // A long history before them is read to its end.
        let filler = BlobVersion::materialized(
            [9; 32],
            BackendRef::Group(Ulid::from_parts(6, 6)),
            EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let writes = (0..1100)
            .map(|at| {
                let key = VersionKey::new("bucket", "in/a", Ulid::from_parts(0, at));
                let value = filler.to_bytes().unwrap().into();
                let key = key.to_bytes().unwrap().into();
                (BLOB_VERSIONS_KEYSPACE.to_string(), key, value)
            })
            .collect();
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
        let object = ExternalObject {
            key: "raw/a".to_string(),
            size: 4,
            etag: Some(hex::encode([0u8; 16])),
            modified_ms: None,
        };
        let found = candidates(&context, &rule, "in/a", &object)
            .await
            .ok()
            .unwrap();
        let ids: Vec<_> = found
            .iter()
            .map(|(_, _, id, _)| id.timestamp_ms())
            .collect();
        assert_eq!(ids, [1, 4]);
        // Without one, a run refuses the file at once and never reads it.
        let object = ExternalObject { size: 5, ..object };
        let found = candidates(&context, &rule, "in/a", &object)
            .await
            .ok()
            .unwrap();
        assert!(found.is_empty());
    }

    fn claim(rule: &BackendImport, key: &str, committed: bool) -> Claim {
        let intent = WriteIntent {
            key: key.to_string(),
            version_id: Ulid::generate(),
            backend: BackendRef::Group(rule.backend_id),
            created_by: Default::default(),
            written: Some(HASH),
            mover: None,
        };
        let stored = intent_key(rule.import_id, "raw/a.csv", intent.version_id);
        (stored, intent, committed)
    }

    #[test]
    fn covers_own_write() {
        let rule = recovery_rule("{dir}{name}");
        let id = Ulid::from_parts(5, 5);
        let row = ImportedObject::written(id, "in/a.csv".to_string(), 4, None);
        let committed = |key: &str, version_id: Ulid| {
            let mut claim = claim(&rule, key, true);
            claim.1.version_id = version_id;
            claim
        };
        // The row's own committed write needs no hash.
        assert!(row_covers(Some(&row), &[committed("in/a.csv", id)]));
        // A committed move of another version onto the same name is checked by its hash.
        let moved = committed("in/a.csv", Ulid::from_parts(6, 6));
        assert!(!row_covers(Some(&row), &[moved]));
        assert!(!row_covers(Some(&row), &[committed("in/b.csv", id)]));
        assert!(!row_covers(Some(&row), &[claim(&rule, "in/a.csv", false)]));
        assert!(!row_covers(None, &[]));
    }

    #[test]
    fn looks_up_intents() {
        let rule = recovery_rule("{dir}{id}_{name}");
        let listed = object("e1");
        assert!(needs_lookup((&rule, &[]), &listed, None, false));
        // A known file without intents takes the fast path, changed or not.
        let literal = row(Ulid::nil(), "e1");
        assert!(!needs_lookup((&rule, &[]), &listed, Some(&literal), false));
        let keyed = ImportedObject {
            key: Some("in/a.csv".to_string()),
            ..literal.clone()
        };
        assert!(!needs_lookup(
            (&rule, &[]),
            &object("e2"),
            Some(&keyed),
            false
        ));
        // An intent at the path is checked before any skip, also for an unchanged file.
        assert!(needs_lookup((&rule, &[]), &listed, Some(&keyed), true));
        assert!(needs_lookup((&rule, &[]), &listed, Some(&literal), true));
        // A literal import whose name encodes a version is checked as well.
        let id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let named = ExternalObject {
            key: format!("raw/{id}_a.csv"),
            ..object("e1")
        };
        assert!(needs_lookup((&rule, &[]), &named, Some(&literal), false));
        assert!(!needs_lookup((&rule, &[]), &named, Some(&keyed), false));
        // So is one an earlier pattern of the rule named.
        let suffix = recovery_rule("{dir}{name}~{id}");
        assert!(!needs_lookup((&suffix, &[]), &named, Some(&literal), false));
        let earlier = ["{dir}{id}_{name}".to_string()];
        assert!(needs_lookup(
            (&suffix, &earlier),
            &named,
            Some(&literal),
            false
        ));
    }

    #[test]
    fn chooses_identity() {
        let rule = recovery_rule("{dir}{id}_{name}");
        let id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let path = format!("raw/x/{id}_a.csv");
        let decoded = choose_identity((&rule, &[], &[]), &path, Vec::new(), None)
            .ok()
            .flatten();
        // A name alone names the version, not the stripped folder `in/`: found files go to the
        // bucket root or where the discovery rules place them.
        let expected = Identity {
            key: "x/a.csv".to_string(),
            version_id: id,
            intent: None,
        };
        assert_eq!(decoded, Some(expected));
        // After a switch to suffix names the replaced pattern still reads the file.
        let suffix = recovery_rule("{dir}{name}~{id}");
        assert!(matches!(
            choose_identity((&suffix, &[], &[]), &path, Vec::new(), None),
            Ok(None)
        ));
        let earlier = ["{dir}{id}_{name}".to_string()];
        let found = choose_identity((&suffix, &earlier, &[]), &path, Vec::new(), None)
            .ok()
            .flatten();
        assert_eq!(
            found.map(|found| (found.key, found.version_id)),
            Some(("x/a.csv".to_string(), id))
        );
        let sorted = [DiscoveryRule {
            pattern: "*.csv".to_string(),
            folder: "tables/".to_string(),
        }];
        let placed = choose_identity((&rule, &[], &sorted), &path, Vec::new(), None);
        let placed = placed
            .ok()
            .flatten()
            .map(|found| (found.key, found.version_id));
        assert_eq!(placed, Some(("tables/x/a.csv".to_string(), id)));
        assert!(matches!(
            choose_identity((&rule, &[], &[]), "raw/x/a.csv", Vec::new(), None),
            Ok(None)
        ));
        // A pattern without the folder gives the name and version only.
        let stripped = recovery_rule("{id}_{name}");
        let path = format!("raw/{id}_a.csv");
        let found = choose_identity((&stripped, &[], &[]), &path, Vec::new(), None);
        let found = found
            .ok()
            .flatten()
            .map(|found| (found.key, found.version_id));
        assert_eq!(found, Some(("a.csv".to_string(), id)));
        // The intent of the write that never committed names the full key.
        let open = claim(&stripped, "in/deep/a.csv", false);
        let done = claim(&stripped, "in/other/a.csv", true);
        let chosen = choose_identity(
            (&stripped, &[], &[]),
            &path,
            vec![done.clone(), open.clone()],
            Some(&HASH),
        );
        let chosen = chosen.ok().flatten().unwrap();
        assert_eq!(
            (chosen.key.as_str(), chosen.version_id),
            ("in/deep/a.csv", open.1.version_id)
        );
        assert_eq!(chosen.intent.map(|(key, _)| key), Some(open.0.clone()));
        let only = choose_identity(
            (&stripped, &[], &[]),
            &path,
            vec![done.clone()],
            Some(&HASH),
        )
        .ok()
        .flatten();
        assert_eq!(
            only.map(|identity| identity.version_id),
            Some(done.1.version_id)
        );
        let second = claim(&stripped, "in/b.csv", false);
        assert!(
            choose_identity(
                (&stripped, &[], &[]),
                &path,
                vec![open.clone(), second],
                Some(&HASH)
            )
            .is_err()
        );
        // An intent for another backend says nothing about this file; its name still does.
        let mut foreign = open;
        foreign.1.backend = BackendRef::Group(Ulid::from_parts(9, 9));
        let found = choose_identity((&stripped, &[], &[]), &path, vec![foreign], Some(&HASH));
        let found = found
            .ok()
            .flatten()
            .map(|found| (found.key, found.intent.is_none()));
        assert_eq!(found, Some(("a.csv".to_string(), true)));
        // An open write without a record of these bytes proves nothing; it is reported.
        let mut unproven = claim(&stripped, "in/deep/a.csv", false);
        let other = [8u8; 32];
        assert!(
            choose_identity(
                (&stripped, &[], &[]),
                &path,
                vec![unproven.clone()],
                Some(&other)
            )
            .is_err()
        );
        unproven.1.written = None;
        let claims = vec![unproven, done.clone()];
        assert!(choose_identity((&stripped, &[], &[]), &path, claims, Some(&HASH)).is_err());
        // A committed write without a record of these bytes never takes over the file.
        let mut sibling = done;
        sibling.1.written = None;
        let named = recovery_rule("{dir}{name}");
        let chosen = choose_identity((&named, &[], &[]), "raw/a", vec![sibling], Some(&HASH));
        assert!(matches!(chosen, Ok(None)));
    }

    #[tokio::test]
    async fn reads_write_claims() {
        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let rule = recovery_rule("{dir}{id}_{name}");
        let (open, done) = (
            claim(&rule, "in/a.csv", false),
            claim(&rule, "in/a.csv", true),
        );
        // Another path that only starts like this one holds no claim on it.
        let longer = intent_key(rule.import_id, "raw/a.csv2", Ulid::generate());
        for (key, intent) in [(&open.0, &open.1), (&done.0, &done.1), (&longer, &open.1)] {
            let value = intent.to_bytes().unwrap();
            let write = write_value(&context, IMPORT_INTENT_KEYSPACE, key.clone(), value);
            assert!(write.await.is_ok());
        }
        let version = BlobVersion::materialized(
            [7; 32],
            BackendRef::Group(rule.backend_id),
            aruna_core::structs::storage::format::EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let key = VersionKey::new("bucket", "in/a.csv", done.1.version_id);
        let key = Key::from(key.to_bytes().unwrap());
        let write = write_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            key,
            version.to_bytes().unwrap(),
        );
        assert!(write.await.is_ok());

        // One range read covers the page; a path past its last object is not read.
        let far = intent_key(rule.import_id, "raw/z.csv", Ulid::generate());
        let value = open.1.to_bytes().unwrap();
        assert!(
            write_value(&context, IMPORT_INTENT_KEYSPACE, far, value)
                .await
                .is_ok()
        );
        let page = [
            object("e1"),
            ExternalObject {
                key: "raw/b.csv".to_string(),
                ..object("e1")
            },
        ];
        let intents = page_intents(&context, &rule, &page).await.ok().unwrap();
        let paths: Vec<&str> = intents.keys().map(String::as_str).collect();
        assert_eq!(paths, ["raw/a.csv", "raw/a.csv2"]);
        let mut claims = read_claims(&context, &rule, &intents["raw/a.csv"])
            .await
            .ok()
            .unwrap();
        claims.sort_by_key(|(_, intent, _)| intent.version_id);
        let mut expected = vec![open.clone(), done.clone()];
        expected.sort_by_key(|(_, intent, _)| intent.version_id);
        assert_eq!(claims, expected);
    }

    #[test]
    fn keeps_recorded_keys() {
        let mut rule = rule_at("");
        rule.write_pattern = "{name}".to_string();
        let discovery = [DiscoveryRule {
            pattern: "*.txt".to_string(),
            folder: "texts/".to_string(),
        }];
        let key = |row: Option<&ImportedObject>, path: &str| {
            bucket_key((&rule, &[], &discovery), row, path).ok()
        };
        // Uploads of `foo/bar.txt` and `bar/foo.txt` flattened into one folder keep their keys.
        let bar = ImportedObject::written(Ulid::generate(), "foo/bar.txt".to_string(), 4, None);
        let foo = ImportedObject::written(Ulid::generate(), "bar/foo.txt".to_string(), 4, None);
        assert_eq!(
            key(Some(&bar), "raw/bar.txt").as_deref(),
            Some("foo/bar.txt")
        );
        assert_eq!(
            key(Some(&foo), "raw/foo.txt").as_deref(),
            Some("bar/foo.txt")
        );
        // An older row without a key keeps the rule's own mapping, whatever the patterns say.
        let older = row(Ulid::generate(), "e1");
        assert_eq!(key(Some(&older), "raw/a.txt").as_deref(), Some("a.txt"));
        // A new file goes to the root unless one pattern places it.
        assert_eq!(key(None, "raw/baz").as_deref(), Some("baz"));
        assert_eq!(
            key(None, "raw/x/new.txt").as_deref(),
            Some("texts/x/new.txt")
        );
        let overlapping = [
            discovery[0].clone(),
            DiscoveryRule {
                pattern: "new.*".to_string(),
                folder: "other/".to_string(),
            },
        ];
        let clash = bucket_key((&rule, &[], &overlapping), None, "raw/new.txt");
        assert!(matches!(clash, Err(ItemError::Item(_))));
        // The rule's own folder is where its uploads come from, not where found files go.
        let foo = rule_at("foo/");
        let found = |path| bucket_key((&foo, &[], &discovery), None, path).ok();
        assert_eq!(found("raw/baz").as_deref(), Some("baz"));
        assert_eq!(found("raw/x/new.txt").as_deref(), Some("texts/x/new.txt"));
    }

    fn rule_at(key_prefix: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::nil(),
            group_id: Ulid::nil(),
            backend_id: Ulid::nil(),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: key_prefix.to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{name}".to_string(),
        }
    }

    #[test]
    fn imports_new_objects() {
        assert_eq!(decide(&object("e1"), None, None, false), Decision::Import);
        // A key the bucket already holds is a conflict, never taken over.
        let other = Some(Ulid::generate());
        assert_eq!(decide(&object("e1"), None, other, false), Decision::Taken);
    }

    #[test]
    fn skips_unchanged_objects() {
        let version = Ulid::generate();
        let row = row(version, "e1");
        assert_eq!(
            decide(&object("e1"), Some(&row), Some(version), true),
            Decision::Skip
        );
        // A purged version is imported again.
        assert_eq!(
            decide(&object("e1"), Some(&row), None, false),
            Decision::Import
        );
        assert_eq!(
            decide(&object("e1"), Some(&row), None, true),
            Decision::Skip
        );
    }

    #[test]
    fn aruna_side_wins() {
        let version = Ulid::generate();
        let row = row(version, "e1");
        let changed = object("e2");
        let fresh = decide(&changed, Some(&row), Some(version), true);
        assert_eq!(fresh, Decision::Import);
        assert_eq!(decide(&changed, Some(&row), None, true), Decision::Import);
        // A later write or delete marker in Aruna keeps the key as Aruna has it.
        let newer = Some(Ulid::generate());
        assert_eq!(decide(&changed, Some(&row), newer, true), Decision::Skip);
        assert_eq!(
            decide(&object("e1"), Some(&row), newer, true),
            Decision::Skip
        );
    }

    #[test]
    fn skips_known_pages() {
        let rule = rule_at("");
        let id = Ulid::generate();
        let key = rule.aruna_key("raw/a.csv").unwrap();
        let version = BlobVersion::materialized(
            [1u8; 32],
            BackendRef::Group(Ulid::nil()),
            aruna_core::structs::storage::format::EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let mut page = PageReads::default();
        let stored = row(id, "e1").to_bytes().unwrap().into();
        page.rows.insert("raw/a.csv".to_string(), Some(stored));
        page.heads.insert(key, Some(id));
        let version = version.to_bytes().unwrap().into();
        page.versions.insert("raw/a.csv".to_string(), Some(version));
        assert!(page_skip((&rule, &[]), &page, false, &object("e1")));
        // A changed file, an open intent or an unread page takes the full path.
        assert!(!page_skip((&rule, &[]), &page, false, &object("e2")));
        assert!(!page_skip((&rule, &[]), &page, true, &object("e1")));
        assert!(!page_skip(
            (&rule, &[]),
            &PageReads::default(),
            false,
            &object("e1")
        ));
        // Without ETag and time an equal size skips only when the row would stay the same.
        let mut unmarked = object("e1");
        unmarked.etag = None;
        assert!(page_skip((&rule, &[]), &page, false, &unmarked));
        page.versions.insert("raw/a.csv".to_string(), None);
        assert!(!page_skip((&rule, &[]), &page, false, &unmarked));
    }

    #[test]
    fn trusts_equal_sizes() {
        // Aruna's own write is not read again; its first listing only adds ETag and time.
        let written = ImportedObject::written(Ulid::generate(), "in/a.csv".to_string(), 4, None);
        let listed = trusted_listing(Some(&written), &object("e1")).unwrap();
        assert_eq!(listed.etag.as_deref(), Some("e1"));
        let mut grown = object("e1");
        grown.size = 5;
        assert_eq!(trusted_listing(Some(&written), &grown), None);
        // An imported row is compared by ETag later, never trusted here.
        let imported = row(Ulid::generate(), "e1");
        assert_eq!(trusted_listing(Some(&imported), &object("e2")), None);
        let mut unmarked = object("e1");
        (unmarked.etag, unmarked.modified_ms) = (None, None);
        let blind = trusted_listing(Some(&imported), &unmarked);
        assert_eq!(blind, Some(imported.clone()));
        unmarked.size = 5;
        assert_eq!(trusted_listing(Some(&imported), &unmarked), None);
        assert_eq!(trusted_listing(None, &unmarked), None);
    }
}
