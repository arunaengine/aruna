//! Advances queued bucket migrations page by page, hands moved versions over and settles them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use aruna_core::backoff::capped_backoff;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE,
    IMPORT_INTENT_KEYSPACE, MIGRATION_AHEAD_KEYSPACE, MOVE_HANDOVER_KEYSPACE, RENAME_MOVE_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::backend_import::{RenameRequest, WriteIntent, version_key};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobVersion, BucketInfo, ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::{
    Compression, CompressionMigration, MigrationAhead, StoredLayout,
};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::{Key, TxnId};
use ulid::Ulid;

use super::batch::Stop;
use super::{
    Earlier, MigrateOutcome, MigrateVersionError, MigrateVersionOperation, RULE_PAGE, place,
    same_bytes, stays,
};
use super::{batch, broken, check};
use crate::blob::records::blob_location_read;

/// The rename request of the move `record` of `bucket`, if it was started to rename files.
pub(super) async fn rename_request(
    context: &crate::driver::DriverContext,
    bucket: &str,
    record: &CompressionMigration,
) -> Result<Option<RenameRequest>, String> {
    let read = StorageEffect::Read {
        key_space: RENAME_MOVE_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    let value = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => {
            return Err(format!(
                "could not read the rename request: {}",
                other.label()
            ));
        }
    };
    let request = value
        .map(|value| RenameRequest::from_bytes(&value))
        .transpose();
    let request = request.map_err(|error| error.to_string())?;
    Ok(request.filter(|request| record.moving && request.started_at_ms == record.started_at_ms))
}

/// The caller's WRITE on the bucket's keys, loaded once for a page of a rename; `None` when
/// the caller's token no longer holds or the caller may not use the bucket any more. State
/// that cannot be read is an error, so the page is retried rather than passed over.
pub(super) async fn rename_access(
    context: &crate::driver::DriverContext,
    request: &RenameRequest,
    routing: &RoutingSnapshot,
    bucket: &str,
) -> Result<Option<crate::auth::request_authorization::BucketAccess>, String> {
    use crate::auth::request_authorization::BucketAccess;
    let valid = crate::auth::bearer_token::grant_valid(
        &context.storage_handle,
        &request.grant,
        &request.auth.user_id,
    );
    if !valid.await.map_err(|error| error.to_string())? {
        return Ok(None);
    }
    let admin = crate::s3::bucket::job_pages::still_admin(context, &request.auth, routing.group_id);
    if !admin.await? {
        return Ok(None);
    }
    let scope = (request.auth.realm_id, routing.group_id, request.node_id);
    let loaded = BucketAccess::load(context, &request.auth, scope, bucket).await;
    let Some(access) = granted(loaded)? else {
        return Ok(None);
    };
    let write = aruna_core::structs::identity::auth::Permission::WRITE;
    Ok(access.allows(None, &write).then_some(access))
}

/// A denial ends the rename; policy state that cannot be read is an error to retry.
pub(super) fn granted<T>(
    loaded: Result<T, crate::auth::request_authorization::AuthorizeError>,
) -> Result<Option<T>, String> {
    use crate::auth::error::AuthorizeError;
    use crate::auth::error::PolicyEnforcementError;
    match loaded {
        Ok(access) => Ok(Some(access)),
        Err(AuthorizeError::PermissionDenied)
        | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Reads the earlier patterns of the rules a move resolves against.
pub async fn read_earlier(
    context: &crate::driver::DriverContext,
    routing: &RoutingSnapshot,
) -> Result<Earlier, String> {
    let mut earlier = Earlier::new();
    for rule in &routing.imports {
        let patterns = crate::s3::imports::read_earlier(context, rule.import_id).await;
        let patterns = patterns.map_err(|error| error.to_string())?;
        if !patterns.is_empty() {
            earlier.insert(rule.import_id, patterns);
        }
    }
    Ok(earlier)
}

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

/// What one task run left behind.
#[derive(Debug, PartialEq)]
pub struct MigrationRun {
    /// When the task must run again; `None` once no queued migration has work left.
    pub next: Option<Duration>,
    /// Queue key the next run resumes after; `None` starts at the head.
    pub cursor: Option<Key>,
}

/// Advances the unfinished migrations of one page of queued buckets by one page each.
pub async fn process_migrations(
    context: &crate::driver::DriverContext,
    after: Option<Key>,
) -> Result<MigrationRun, String> {
    let resumed = after.is_some();
    let (rows, cursor) = crate::storage_read::iter_prefix_page(
        &context.storage_handle,
        COMPRESSION_QUEUE_KEYSPACE,
        None,
        after,
        BUCKET_PAGE,
        None,
    )
    .await?;
    let now = aruna_core::time::unix_timestamp_millis();
    let mut next = None::<Duration>;
    let mut soon = |after: Duration| next = Some(next.map_or(after, |next| next.min(after)));
    for (key, _) in rows {
        let Some(record) = read_progress(context, &key).await? else {
            continue;
        };
        if record.finished_at_ms.is_some() {
            continue;
        }
        if let Some(at) = record.retry_at_ms.filter(|at| *at > now) {
            soon(Duration::from_millis(at - now));
            continue;
        }
        let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
        // Encryption replaced this work: its transition converts every version.
        if encrypting(context, &key).await? {
            if record.moving && !hand_over_moves(context, &bucket, record.started_at_ms).await? {
                soon(MIGRATION_CONTINUE);
                continue;
            }
            let mut retired = record;
            retired.retry_at_ms = None;
            retired.finished_at_ms = Some(now);
            store_progress(context, &bucket, &retired, None).await?;
            continue;
        }
        if let Some(after) = migrate_page(context, &bucket, record, MIGRATION_TIME).await? {
            soon(after);
        }
    }
    // Encryption transitions share this task; a run at the queue head advances them all.
    if !resumed
        && let Some(after) = crate::blob::migration::queue::process_transitions(context).await?
    {
        soon(after);
    }
    // Storage checks share this task too.
    if !resumed && let Some(after) = broken::process_checks(context).await? {
        soon(after);
    }
    // So do the re-mapping and duplicate cleanup jobs of buckets.
    if !resumed && let Some(after) = crate::s3::bucket::job_pages::process_linked(context).await? {
        soon(after);
    }
    // Queued buckets after this page, or before it when the run resumed mid-queue.
    if cursor.is_some() || resumed {
        soon(MIGRATION_CONTINUE);
    }
    Ok(MigrationRun { next, cursor })
}

pub(super) async fn encrypting(
    context: &crate::driver::DriverContext,
    bucket: &Key,
) -> Result<bool, String> {
    let read = StorageEffect::Read {
        key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
        key: bucket.clone(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            BucketEncryption::from_row(value.as_deref())
                .map(|settings| settings.is_encrypted())
                .map_err(|error| error.to_string())
        }
        other => Err(format!(
            "could not read encryption settings: {}",
            other.label()
        )),
    }
}

pub(super) async fn read_progress(
    context: &crate::driver::DriverContext,
    key: &Key,
) -> Result<Option<CompressionMigration>, String> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| CompressionMigration::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!(
            "could not read migration progress: {}",
            other.label()
        )),
    }
}

/// Migrates one page of a bucket's versions and stores the progress, unless a newer
/// setting replaced the record meanwhile. A run that worked for `budget` starts no more
/// versions. Returns when the bucket needs the next run.
pub(super) async fn migrate_page(
    context: &crate::driver::DriverContext,
    bucket: &str,
    mut record: CompressionMigration,
    budget: Duration,
) -> Result<Option<Duration>, String> {
    let begun = std::time::Instant::now();
    // A hand-over under way finishes before anything else of the move.
    if record.moving && handing_over(context, bucket, record.started_at_ms).await? {
        if !hand_over_moves(context, bucket, record.started_at_ms).await? {
            return Ok(Some(MIGRATION_CONTINUE));
        }
        record.finished_at_ms = Some(aruna_core::time::unix_timestamp_millis());
        store_progress(context, bucket, &record, None).await?;
        return Ok(None);
    }
    // A due retry pass counts its own skips and failures.
    if record.retry_at_ms.take().is_some() {
        record.skipped = 0;
        record.failed = 0;
    }
    let routing = match record.moving {
        true => match move_routing(context, bucket).await? {
            Some(routing) => Some(routing),
            None => {
                record.finished_at_ms = Some(aruna_core::time::unix_timestamp_millis());
                store_progress(context, bucket, &record, None).await?;
                return Ok(None);
            }
        },
        false => None,
    };
    // Without earlier patterns no file is renamed, so only an asked rename reads them; each
    // file is renamed only while the caller who asked may still write its key.
    let request = match routing.as_ref() {
        Some(_) => rename_request(context, bucket, &record).await?,
        None => None,
    };
    // An aborted rename ends here with its counts, once no version of it is in flight.
    if request
        .as_ref()
        .is_some_and(|request| request.aborted_at_ms.is_some())
    {
        return end_aborted(context, bucket, record).await;
    }
    // A paused rename waits for its resume, which wakes this task again.
    if request.as_ref().is_some_and(|request| request.paused) {
        return Ok(None);
    }
    let (earlier, access) = match routing.as_ref() {
        Some(routing) => match request.clone() {
            Some(request) => match rename_access(context, &request, routing, bucket).await? {
                Some(access) => (read_earlier(context, routing).await?, Some(access)),
                None => (Earlier::new(), None),
            },
            None => (Earlier::new(), None),
        },
        None => (Earlier::new(), None),
    };
    let write = aruna_core::structs::identity::auth::Permission::WRITE;
    let mut denied = 0usize;
    let prefix = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (versions, next) = crate::storage_read::iter_prefix_page(
        &context.storage_handle,
        BLOB_VERSIONS_KEYSPACE,
        Some(prefix.into()),
        record.cursor.clone().map(Into::into),
        MIGRATION_ROWS,
        None,
    )
    .await?;
    let keys = versions
        .iter()
        .map(|(key, _)| VersionKey::from_bytes(key.as_ref()));
    let keys: Vec<VersionKey> = keys.collect::<Result<_, _>>().map_err(|e| e.to_string())?;
    // Keys outside a move's prefix are only re-encoded.
    let moves =
        |key: &VersionKey| (routing.as_ref()).filter(|_| key.key.starts_with(&record.prefix));
    // A rename moves several keys at once, but never two versions of one key or one new name.
    let limit = match request {
        Some(_) => RENAME_PARALLEL,
        None => 1,
    };
    let name = |key: &VersionKey| {
        let routing = moves(key).filter(|_| limit > 1)?;
        Some(place(routing, key).ok()?.1?.path)
    };
    let rows = keys.iter().map(|key| (key.key.clone(), name(key)));
    let mut batch = batch::Batch::new(rows.collect());
    // Rows the last run of this pass finished after its cursor already count, so they pass.
    let ahead = read_ahead(context, bucket).await?;
    let ahead: std::collections::HashSet<Vec<u8>> = ahead
        .map(|ahead| ahead.rows_for(&record))
        .unwrap_or_default()
        .into_iter()
        .collect();
    for (row, (key, _)) in versions.iter().enumerate() {
        if ahead.contains(key.as_ref()) {
            batch.passed(row);
        }
    }
    let worked = AtomicUsize::new(0);
    let (target, started_ms) = (record.target, record.started_at_ms);
    let start = |row: usize| {
        let version_key = keys[row].clone();
        let routing = moves(&version_key).cloned();
        let moving = routing.is_some();
        let object = version_key.key.clone();
        let mut operation = MigrateVersionOperation::new(version_key, target, SystemTime::now());
        if let Some(routing) = routing {
            let allowed =
                (access.as_ref()).is_some_and(|access| access.allows(Some(&object), &write));
            denied += usize::from(!earlier.is_empty() && !allowed);
            let earlier = match allowed {
                true => earlier.clone(),
                false => Earlier::new(),
            };
            operation = (operation.with_routing(routing))
                .with_earlier(earlier)
                .with_mover(started_ms);
        }
        let (key, worked) = (&versions[row].0, &worked);
        async move {
            let moved = crate::driver::drive(operation, context).await;
            // A taken name says nothing about the old copy, so the provider is not asked about it.
            let taken = matches!(moved, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
            let broken = moved.is_err() && moving && !taken && is_broken(context, key).await;
            let skipped = matches!(moved, Ok(MigrateOutcome::Skipped));
            worked.fetch_add(usize::from(!skipped), Ordering::Relaxed);
            if let (Err(error), false) = (&moved, broken) {
                tracing::warn!(bucket, %error, "Failed to re-encode a version");
            }
            (moved, broken)
        }
    };
    // A run with enough work or time stores its progress, and a pause or an abort of the rename
    // stops it, before the next version starts.
    let mut started = 0usize;
    let proceed = || {
        let spent = begun.elapsed() >= budget || worked.load(Ordering::Relaxed) >= MIGRATION_WORK;
        let full = started > 0 && spent;
        started += 1;
        let (renaming, record) = (request.is_some(), &record);
        async move {
            if full {
                return Ok(Some(Stop::More));
            }
            let stopped = renaming && rename_stopped(context, bucket, record).await?;
            Ok(stopped.then_some(Stop::Halted))
        }
    };
    let (results, stop) = batch::run_rows(&mut batch, limit, start, proceed).await;
    let stopped = stop? == Some(Stop::Halted);
    // Each row this run finished counts once; the next run starts after the rows finished in
    // storage order and passes over those finished after them.
    let done = batch.done();
    let rows = batch.finished_after().map(|row| versions[row].0.to_vec());
    let ahead = MigrationAhead {
        rows: rows.collect(),
        ..MigrationAhead::default()
    };
    for (moved, broken) in results.into_iter().flatten() {
        match moved {
            Ok(MigrateOutcome::Migrated) => record.migrated += 1,
            Ok(MigrateOutcome::Skipped) => record.skipped += 1,
            // A move records a copy that cannot be read as broken instead of retrying it.
            Err(_) if broken => record.skipped += 1,
            Err(_) => record.failed += 1,
        }
    }
    if let Some(last) = done.checked_sub(1) {
        record.cursor = Some(versions[last].0.to_vec());
    }
    let ahead = MigrationAhead {
        started_at_ms: record.started_at_ms,
        retries: record.retries,
        cursor: record.cursor.clone(),
        ..ahead
    };
    let more = next.is_some() || done < versions.len();
    if stopped {
        let aborted = rename_request(context, bucket, &record).await?;
        if aborted.is_some_and(|request| request.aborted_at_ms.is_some()) {
            return end_aborted(context, bucket, record).await;
        }
        store_progress(context, bucket, &record, Some(&ahead)).await?;
        return Ok(None);
    }
    if denied > 0 {
        tracing::warn!(
            bucket,
            denied,
            "Files the rename's caller may not write keep their name"
        );
    }
    let now = aruna_core::time::unix_timestamp_millis();
    let after = match (more, record.failed > 0) {
        // This run moved the cursor, so the rows left can go on at once.
        (true, _) => Some(Duration::ZERO),
        // Successful versions are already in the target, so a retry pass reads no blob for them.
        (false, true) if record.retries < MIGRATION_RETRIES => {
            let wait = capped_backoff(RETRY_BASE, RETRY_MAX, record.retries, 0);
            record.retries += 1;
            record.cursor = None;
            record.retry_at_ms = Some(now.saturating_add(wait.as_millis() as u64));
            Some(wait)
        }
        (false, _) => {
            // A move that ends hands every attempt it still owns to the cleanup queue first;
            // a long hand-over goes on in the next runs.
            match record.moving && !hand_over_moves(context, bucket, record.started_at_ms).await? {
                true => Some(MIGRATION_CONTINUE),
                false => {
                    record.finished_at_ms = Some(now);
                    None
                }
            }
        }
    };
    store_progress(context, bucket, &record, Some(&ahead)).await?;
    Ok(after)
}

/// The rows the last run of a bucket's migration finished after its cursor.
pub(super) async fn read_ahead(
    context: &crate::driver::DriverContext,
    bucket: &str,
) -> Result<Option<MigrationAhead>, String> {
    let read = StorageEffect::Read {
        key_space: MIGRATION_AHEAD_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| MigrationAhead::from_bytes(&value))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read the rows ahead: {}", other.label())),
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
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(read).await;
    let info = crate::groups::backends::parse_read(event, BucketInfo::from_bytes);
    let Some(info) = info.map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    let snapshot = crate::driver::drive(
        crate::groups::routing::SnapshotOperation::new(
            &crate::driver::node_routing(context),
            info.group_id,
            crate::groups::routing::BucketRules::of(&info),
        ),
        context,
    )
    .await;
    snapshot.map(Some).map_err(|error| error.to_string())
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
