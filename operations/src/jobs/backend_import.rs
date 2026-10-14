//! Runs one backend import: lists the rule's folder and registers each object in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, JOB_STATE_KEYSPACE,
    PLAIN_NAME_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobError, JobId, JobResultPayload};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportCheckpoint, BackendImportSpec, ExternalObject, ImportedObject,
    object_key, plain_name_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BucketInfo, CurrentVersionPointer, ResolvedBackend,
    VersionKey, bucket_permission_path, ensure_confined_path,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::{RoutingSnapshot, RoutingTarget, RuleSource};
use aruna_core::types::{Effects, Key, Value};
use byteview::ByteView;
use smallvec::smallvec;
use std::path::Path;
use ulid::Ulid;

use super::executor::{JobContext, JobRunOutcome};
use super::store::{put_state, read_state};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive, gate_context, now_ms, routing_snapshot};
use crate::groups::backends::query::GetBackendOperation;
use crate::groups::imports::read_import;
use crate::placement::policy::GateContext;
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::{LiveVersionInput, LiveVersionOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};

const LIST_PAGE: usize = 500;

/// What a run should do with one listed object.
#[derive(Debug, PartialEq)]
enum Decision {
    Skip,
    Import(Ulid),
}

/// Objects a bucket already holds win, and so do later writes and deletes in Aruna:
/// a changed object gets a new version only while the key still shows the imported
/// one or nothing. A recorded attempt without a version retries the same version id.
fn decide(
    object: &ExternalObject,
    row: Option<&ImportedObject>,
    head: Option<Ulid>,
    version_exists: bool,
) -> Decision {
    match row {
        None if head.is_some() => Decision::Skip,
        None => Decision::Import(Ulid::generate()),
        Some(row) if row.matches(object) => match head {
            None if !version_exists => Decision::Import(row.version_id),
            _ => Decision::Skip,
        },
        Some(row) if head.is_none_or(|head| head == row.version_id) => {
            Decision::Import(Ulid::generate())
        }
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

pub async fn run_backend_import(ctx: &JobContext, spec: &BackendImportSpec) -> JobRunOutcome {
    let mut checkpoint = match read_checkpoint(&ctx.driver, ctx.job_id).await {
        Ok(checkpoint) => checkpoint.unwrap_or_default(),
        Err(error) => return retryable(error),
    };
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
            let counted = match import_object(ctx, spec, &run, &object).await {
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
                    checkpoint.fail(&object.key, error);
                    true
                }
                Err(ItemError::System(error)) => return retryable(error),
            };
            checkpoint.cursor = Some(object.key);
            // Skips are cheap to repeat after a restart, so only real work is persisted at once.
            if counted && let Err(error) = persist(ctx, &checkpoint).await {
                return retryable(error);
            }
        }
        let done = next.is_none();
        if next.is_some() {
            checkpoint.cursor = next;
        }
        if let Err(error) = persist(ctx, &checkpoint).await {
            return retryable(error);
        }
        if done {
            return JobRunOutcome::Succeeded(JobResultPayload::BackendImport {
                imported: checkpoint.imported,
                skipped: checkpoint.skipped,
                failed: checkpoint.failed,
            });
        }
    }
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
    let [row] = read_values(&ctx.driver, [(IMPORTED_OBJECT_KEYSPACE, row_key.clone())]).await?;
    let row = parse(row, ImportedObject::from_bytes)?;
    // Aruna's own write is recorded with its key; its first listing only notes ETag and time.
    if let Some(listed) = row.as_ref().and_then(|row| row.first_listing(object)) {
        write_value(
            &ctx.driver,
            IMPORTED_OBJECT_KEYSPACE,
            row_key,
            encode(&listed)?,
        )
        .await?;
        return Ok(Outcome::Skipped);
    }
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
    let version_id = match decide(object, row.as_ref(), head, version_exists) {
        Decision::Skip => return Ok(Outcome::Skipped),
        Decision::Import(version_id) => version_id,
    };
    let intent = ImportedObject {
        key: row.as_ref().and_then(|row| row.key.clone()),
        ..ImportedObject::new(version_id, object)
    };
    if row.as_ref() != Some(&intent) {
        write_value(
            &ctx.driver,
            IMPORTED_OBJECT_KEYSPACE,
            row_key,
            encode(&intent)?,
        )
        .await?;
    }
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
    let location = hash_object(ctx, spec, resolved, &object.key).await?;
    if location.blob_size != object.size {
        return Err(ItemError::Item(
            "the object changed while it was read".to_string(),
        ));
    }
    let hint = location.location_key().ok();
    put_version(ctx, spec, run, key.clone(), version_id, location).await?;
    // A same-name write to this path first moves the imported bytes aside.
    if let Some(hint) = hint.filter(|_| rule.same_name() && rule.write_backend() == rule.backend_id)
    {
        let name = plain_name_key(&BackendRef::Group(rule.backend_id), &object.key);
        write_value(&ctx.driver, PLAIN_NAME_KEYSPACE, name, hint.to_bytes()).await?;
    }
    // Without the replication seed, peers never learn about the version.
    drive(
        LiveVersionOperation::new(LiveVersionInput {
            local_node_id: spec.node_id,
            auth_context: spec.auth_context.clone(),
            bucket: rule.bucket.clone(),
            key,
            version_id,
            delete_marker: false,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| ItemError::Item(format!("replication queue failed: {error}")))?;
    Ok(Outcome::Imported)
}

async fn hash_object(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    resolved: ResolvedBackend,
    path: &str,
) -> Result<BackendLocation, ItemError> {
    let effect = BlobEffect::HashExternal {
        resolved,
        path: path.to_string(),
        created_by: spec.auth_context.user_id,
    };
    match drive(BlobRequest::new(effect), &ctx.driver).await {
        Ok(BlobEvent::ExternalHashed { location }) => Ok(location),
        Ok(other) => Err(ItemError::System(format!("unexpected answer {other:?}"))),
        Err(error) => Err(ItemError::Item(error.to_string())),
    }
}

async fn put_version(
    ctx: &JobContext,
    spec: &BackendImportSpec,
    run: &ImportRun,
    key: String,
    version_id: Ulid,
    location: BackendLocation,
) -> Result<(), ItemError> {
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
        preassigned_version_id: Some(version_id),
        quota_ceiling: run.quota,
        routing: run.routing.clone(),
    })
    .without_envelope()
    .with_adopted(location)
    .with_bucket_guard(run.bucket.clone())
    .with_restrictions(spec.auth_context.path_restrictions.clone());
    if let Some(gate) = run.gate.clone() {
        operation = operation.with_gate(gate);
    }
    drive(operation, &ctx.driver)
        .await
        .map(|_| ())
        .map_err(|error| ItemError::Item(error.to_string()))
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

/// Emits one listing or hashing effect and hands back the blob adapter's answer.
#[derive(Debug, PartialEq)]
struct BlobRequest {
    effect: Option<BlobEffect>,
    output: Option<Result<BlobEvent, BlobError>>,
}

impl BlobRequest {
    fn new(effect: BlobEffect) -> Self {
        Self {
            effect: Some(effect),
            output: None,
        }
    }
}

impl Operation for BlobRequest {
    type Output = BlobEvent;
    type Error = BlobError;

    fn start(&mut self) -> Effects {
        match self.effect.take() {
            Some(effect) => smallvec![Effect::Blob(effect)],
            None => {
                self.output = Some(Err(BlobError::ReadError("request already sent".into())));
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match event {
            Event::Blob(BlobEvent::Error(error)) => Err(error),
            Event::Blob(event) => Ok(event),
            other => Err(BlobError::ReadError(format!("unexpected event {other:?}"))),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
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

    #[test]
    fn imports_new_objects() {
        assert!(matches!(
            decide(&object("e1"), None, None, false),
            Decision::Import(_)
        ));
        // A key the bucket already holds is never taken over.
        let other = Some(Ulid::generate());
        assert_eq!(decide(&object("e1"), None, other, false), Decision::Skip);
    }

    #[test]
    fn skips_unchanged_objects() {
        let version = Ulid::generate();
        let row = row(version, "e1");
        assert_eq!(
            decide(&object("e1"), Some(&row), Some(version), true),
            Decision::Skip
        );
        // An attempt that never wrote its version retries the same id.
        assert_eq!(
            decide(&object("e1"), Some(&row), None, false),
            Decision::Import(version)
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
        assert!(matches!(fresh, Decision::Import(id) if id != version));
        assert!(
            matches!(decide(&changed, Some(&row), None, true), Decision::Import(id) if id != version)
        );
        // A later write or delete marker in Aruna keeps the key as Aruna has it.
        let newer = Some(Ulid::generate());
        assert_eq!(decide(&changed, Some(&row), newer, true), Decision::Skip);
        assert_eq!(
            decide(&object("e1"), Some(&row), newer, true),
            Decision::Skip
        );
    }
}
