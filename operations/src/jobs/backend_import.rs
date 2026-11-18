//! Runs one backend import: lists the rule's folder and registers each object in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod job;
mod object;
mod prepare;
mod reads;
mod recover;
mod twin;

pub use job::BackendImportOperation;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError};
use aruna_core::errors::BlobError;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, DELETE_CLAIM_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, RENAMED_ROW_KEYSPACE,
};
use aruna_core::structs::execution::job::{JobError, JobId};
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportCheckpoint, BackendImportSpec, ExternalObject, ImportedObject,
    PathIdentity, WriteIntent, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobVersion, BucketInfo, ensure_confined_path,
};
use aruna_core::structs::storage::discovery::{
    DiscoveryRule, FileIdentity, IdentityError, file_identity,
};
use aruna_core::structs::storage::distribution::hint::with_hint;
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use ulid::Ulid;

use super::executor::JobRunOutcome;
use super::page_intents::PathIntents;
use crate::placement::policy::{GateContext, PolicyGateError};
use crate::replication::live_queue::{CompletePutInput, CompletePutOperation};
use crate::s3::imports::shared::{self, SharedRule, claim_file};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, RuleScan, intent_keys,
};
use crate::s3::purge_fence::PurgeFenceError;

/// What every operation of one import job knows about its run.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportJob {
    pub spec: Arc<BackendImportSpec>,
    pub job_id: JobId,
    pub claim_token: Ulid,
    pub routing: NodeRouting,
    /// Makes the version ids this run mints distinct.
    pub seed: u64,
}

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

#[derive(Debug, PartialEq)]
enum Outcome {
    Imported,
    Skipped,
    /// Outside the rule's folder or inside Aruna's own; not counted at all.
    Ignored,
}

/// `Item` fails one object and the run goes on; `System` ends the run for a retry.
#[derive(Debug, PartialEq)]
enum ItemError {
    Item(String),
    System(String),
    /// The file changed between its listing and its read; the next run looks again.
    Changed,
}

impl From<IoError> for ItemError {
    fn from(error: IoError) -> Self {
        Self::System(error.to_string())
    }
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
#[derive(Debug, PartialEq)]
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
    intents: PathIntents,
    /// File name rules for objects without a recorded mapping.
    discovery: Vec<DiscoveryRule>,
    /// Other rules that read an overlapping folder; they may own a listed file.
    shared: Vec<SharedRule>,
    /// The page's rows, heads and committed versions, read in two batches.
    page: PageReads,
}

/// Values read once per listing page. Only the skip of an unchanged file trusts them; every
/// other path reads again, and every write checks again in its transaction.
#[derive(Debug, Default, PartialEq)]
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

fn item(message: impl Into<String>) -> ItemError {
    ItemError::Item(message.into())
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

/// The rule must still be defined as this run read it.
fn rule_guard(rule: &BackendImport) -> Result<(String, Key, Option<Value>), ItemError> {
    let bytes = rule.to_bytes();
    let value = bytes.map_err(|error| ItemError::System(error.to_string()))?;
    let space = BACKEND_IMPORT_KEYSPACE.to_string();
    Ok((space, rule.key(), Some(value.into())))
}

/// The ids of the rules this run saw sharing its folder.
fn sharing(run: &ImportRun) -> BTreeSet<Ulid> {
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

/// Version rows one page of the search for the same content reads.
const TWIN_SCAN: usize = 1024;

/// A version id for object `index` of this run, dated `at` or `now_ms`; a distributed bucket
/// always dates by mint time and adds its set hint, and the source time stays in the import row.
fn import_version(
    (job, run): (&ImportJob, &ImportRun),
    (index, now_ms): (u64, u64),
    at: Option<u64>,
) -> Ulid {
    let random = (u128::from(job.seed) << 32) | u128::from(index);
    match (run.bucket.distribution.as_ref(), at) {
        (Some(binding), _) => with_hint(Ulid::from_parts(now_ms, random), binding.hint()),
        (None, Some(at)) => Ulid::from_parts(at, random),
        (None, None) => Ulid::from_parts(now_ms, random),
    }
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

/// A hashed file, its backend version, and whether its read showed the listing's ETag.
type Hashed = (BackendLocation, Option<String>, bool);

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
#[derive(Debug, PartialEq)]
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

fn encode(row: &ImportedObject) -> Result<Vec<u8>, ItemError> {
    row.to_bytes()
        .map_err(|error| ItemError::System(error.to_string()))
}

/// The put that commits one version of a file already on the rule's backend, with its import
/// rows, in one transaction.
fn put_operation(
    (job, run): (&ImportJob, &ImportRun),
    key: String,
    (location, version): (BackendLocation, Option<String>),
    row: ImportedObject,
    fence: Fence,
) -> Result<PutObjectOperation, PutObjectError> {
    let spec = &job.spec;
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
            job_id: Some(job.job_id.as_ulid()),
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
    Ok(operation)
}

/// The best-effort work after a committed version: queues its replication and tells watchers.
/// A failure leaves the obligation for repair, as for any put.
fn complete_call(
    (job, run): (&ImportJob, &ImportRun),
    key: String,
    version_id: Ulid,
    size_bytes: u64,
) -> IoCall {
    let auth = job.spec.auth_context.clone();
    IoCall::child(CompletePutOperation::new(CompletePutInput {
        realm_id: auth.realm_id,
        node_id: job.spec.node_id,
        auth,
        group_id: run.rule.group_id,
        bucket: run.rule.bucket.clone(),
        key,
        version_id,
        size_bytes,
    }))
}

#[cfg(test)]
#[path = "backend_import/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "backend_import/op_tests.rs"]
mod op_tests;
