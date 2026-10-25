//! Works off the blob cleanup backlog and sweeps multipart uploads that went stale.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use crate::replication::dht_registration::dht_registration_effect;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, CLEANUP_CURSOR_KEYSPACE, COPY_OWNER_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, CopyOwner,
    WriteOwner,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::multipart::{
    COMPLETION_DEADLINE_MS, MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use tracing::{error, warn};
use ulid::Ulid;

use crate::blob::release::release_renamed;
use crate::driver::{DriverContext, drive, now_ms};
use crate::groups::backends::{backend_key, parse_read};
use crate::jobs::store::iter_prefix_page;
use crate::s3::multipart::abort::{AbortUploadInput, AbortUploadOperation};

pub const BLOB_CLEANUP_AFTER: Duration = Duration::from_secs(300);
pub const BLOB_CLEANUP_RETRY: Duration = Duration::from_secs(30);
const CLEANUP_PAGE_SIZE: usize = 128;
const MAX_CLEANUP_RETRIES: u8 = 3;
/// An `Open` upload nobody added to or aborted is abandoned; S3 lifecycle rules
/// use the same order of magnitude.
const OPEN_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// A `Completing` upload past the completion deadline lost the request that
/// owned it; the margin keeps the sweep off a completion still running.
const COMPLETING_TTL_MS: u64 = COMPLETION_DEADLINE_MS + BLOB_CLEANUP_AFTER.as_millis() as u64;
/// Aborts are transactional, so one run reclaims a bounded slice of the backlog.
const UPLOAD_SWEEP_BATCH: usize = 32;

/// A cleanup row an operation has emitted but storage has not yet accepted.
/// The row is the only durable record that the written object exists, so the
/// work is held until the write succeeds.
#[derive(Debug, Default, PartialEq)]
pub struct PendingCleanup {
    work: Option<BlobCleanupWork>,
    key: Option<Key>,
    channel_closed: bool,
    attempts: u8,
}

impl PendingCleanup {
    /// The write effect for one row. `None` means the row will never encode,
    /// leaving the caller with the failure it already carries.
    pub fn queue(&mut self, work: BlobCleanupWork) -> Option<Effect> {
        let key = cleanup_row_key(&work);
        let effect = cleanup_row_write(&work, &key)?;
        self.work = Some(work);
        self.key = Some(key);
        self.channel_closed = false;
        self.attempts = 0;
        Some(effect)
    }

    pub fn accepted(&mut self) {
        self.work = None;
        self.key = None;
    }

    /// Re-emit one write per temporary rejection; the caller remains queued.
    /// The retries are bounded: an endless loop would pin the blob reservation
    /// and never let the request finish.
    pub fn retry(&mut self, error: &StorageError) -> Option<Effect> {
        if self.channel_closed {
            return None;
        }
        if matches!(error, StorageError::ChannelClosed) {
            self.channel_closed = true;
            return None;
        }
        self.attempts = self.attempts.saturating_add(1);
        if self.attempts > MAX_CLEANUP_RETRIES {
            error!(error = %error, "Giving up on a rejected blob cleanup row");
            return None;
        }
        cleanup_row_write(self.work.as_ref()?, self.key.as_ref()?)
    }
}

/// How long a recorded copy upload may go without a copied part before the cleanup takes it
/// for a leftover; each part renews it.
const SETTLE_GRACE_MS: u64 = 60 * 60 * 1000;

/// Drops a page of `key_space` rows under `prefix`; `true` once none is left.
async fn drop_rows(
    context: &DriverContext,
    key_space: &str,
    prefix: Vec<u8>,
) -> Result<bool, String> {
    let page = iter_prefix_page(
        &context.storage_handle,
        key_space,
        Some(prefix.into()),
        None,
        CLEANUP_PAGE_SIZE,
        None,
    );
    let (rows, _) = page.await?;
    if rows.is_empty() {
        return Ok(true);
    }
    let deletes = rows
        .into_iter()
        .map(|(key, _)| (key_space.to_string(), key))
        .collect();
    let effect = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(false),
        other => Err(format!("could not drop rows: {other:?}")),
    }
}

/// Whether the purge job `job_id` has ended, or is gone; `None` while unreadable.
async fn purge_ended(context: &DriverContext, job_id: Ulid) -> Option<bool> {
    let job_id = aruna_core::structs::execution::job::JobId::try_from_bytes(job_id.to_bytes());
    let Ok(job_id) = job_id else {
        return Some(true);
    };
    let record = crate::jobs::store::read_job_record(&context.storage_handle, job_id, None);
    match record.await {
        Ok(record) => Some(record.is_none_or(|record| record.is_settled())),
        Err(error) => {
            warn!(%error, "A purge job could not be read");
            None
        }
    }
}

fn cleanup_row_key(work: &BlobCleanupWork) -> Key {
    match work {
        BlobCleanupWork::ReconcileWrite { location, .. } => {
            location.ulid.to_bytes().to_vec().into()
        }
        BlobCleanupWork::DeleteBlob { .. }
        | BlobCleanupWork::RegisterDht { .. }
        | BlobCleanupWork::ReconcileReservation { .. }
        | BlobCleanupWork::ReleaseRenamed { .. }
        | BlobCleanupWork::DropTrail { .. }
        | BlobCleanupWork::DropClaims { .. }
        | BlobCleanupWork::ReleasePlanned { .. }
        | BlobCleanupWork::SettleUpload { .. }
        | BlobCleanupWork::SettleMove { .. }
        | BlobCleanupWork::DropRows { .. } => Ulid::generate().to_bytes().to_vec().into(),
    }
}

fn cleanup_row_write(work: &BlobCleanupWork, key: &Key) -> Option<Effect> {
    match work.to_bytes() {
        Ok(bytes) => Some(Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: key.clone(),
            value: bytes.into(),
            txn_id: None,
        })),
        Err(error) => {
            warn!(error = %error, "Blob cleanup row could not be encoded");
            None
        }
    }
}

pub fn schedule_cleanup_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainCleanupQueue,
        after: Duration::ZERO,
    })
}

#[derive(Debug, Default)]
pub struct BlobCleanupOutcome {
    pub processed: usize,
    pub failed: usize,
    pub dropped: usize,
    /// Rows that must stay queued without being failures, such as a live upload's target.
    pub kept: usize,
    /// The pass stopped at its row limit; the next one continues after it.
    pub more: bool,
}

/// Rows one drain pass looks at, at most; the next pass continues after the last one.
const PASS_ROWS: usize = 4096;
/// Where the next drain pass starts.
const CURSOR_KEY: &[u8] = b"cursor";

/// What one drain pass did with a row.
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

// Pages through the whole queue so persistently failing rows cannot starve the
// rows behind them, deleting each page's completed rows before the next fetch.
pub async fn process_cleanup_batch(context: &DriverContext) -> Result<BlobCleanupOutcome, String> {
    let mut outcome = BlobCleanupOutcome::default();
    let mut start_after = read_cursor(context).await?;
    let mut seen = 0usize;

    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            start_after,
            CLEANUP_PAGE_SIZE,
            None,
        )
        .await?;

        let mut done: Vec<(String, Key)> = Vec::new();
        let last = values.last().map(|(key, _)| key.clone());
        seen += values.len();
        for (key, value) in values {
            // A row nobody can decode would wedge the drain forever.
            let work = match BlobCleanupWork::from_bytes(&value) {
                Ok(work) => work,
                Err(error) => {
                    warn!(error = %error, "Dropping undecodable blob cleanup row");
                    done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                    outcome.dropped = outcome.dropped.saturating_add(1);
                    continue;
                }
            };
            // A tenant backend that is gone can never resolve credentials, so
            // retrying this row forever would wedge the drain.
            if let Some(backend) = work_backend(&work)
                && is_removed_backend(context, backend).await
            {
                error!(backend = %backend, "Dropping blob cleanup for a removed backend");
                done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            }
            match run_cleanup_work(context, work).await {
                RowOutcome::Done => {
                    done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                    outcome.processed = outcome.processed.saturating_add(1);
                }
                RowOutcome::Failed => outcome.failed = outcome.failed.saturating_add(1),
                RowOutcome::Kept => outcome.kept = outcome.kept.saturating_add(1),
            }
        }

        delete_cleanup_rows(context, done).await?;

        let Some(next) = next else {
            store_cursor(context, None).await?;
            return Ok(outcome);
        };
        // A bounded pass keeps its place, so every row gets its turn over the passes.
        if seen >= PASS_ROWS {
            store_cursor(context, last).await?;
            outcome.more = true;
            return Ok(outcome);
        }
        start_after = Some(next);
    }
}

async fn read_cursor(context: &DriverContext) -> Result<Option<Key>, String> {
    let read = StorageEffect::Read {
        key_space: CLEANUP_CURSOR_KEYSPACE.to_string(),
        key: CURSOR_KEY.to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        other => Err(format!("could not read the cleanup cursor: {other:?}")),
    }
}

async fn store_cursor(context: &DriverContext, cursor: Option<Key>) -> Result<(), String> {
    let key: Key = CURSOR_KEY.to_vec().into();
    let effect = match cursor {
        Some(value) => StorageEffect::Write {
            key_space: CLEANUP_CURSOR_KEYSPACE.to_string(),
            key,
            value,
            txn_id: None,
        },
        None => StorageEffect::Delete {
            key_space: CLEANUP_CURSOR_KEYSPACE.to_string(),
            key,
            txn_id: None,
        },
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. }) => {
            Ok(())
        }
        other => Err(format!("could not store the cleanup cursor: {other:?}")),
    }
}

#[derive(Debug, Default)]
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

/// Reclaims stale multipart uploads, their part rows and their part blobs. The
/// abort operation owns the transaction, so a refused record stays for the next
/// run instead of leaving half-deleted state behind.
pub async fn sweep_stale_uploads(
    context: &DriverContext,
    now_ms: u64,
) -> Result<UploadSweepOutcome, String> {
    let mut outcome = UploadSweepOutcome::default();
    let mut start_after = None;
    let mut stale: Vec<MultipartUpload> = Vec::new();

    while stale.len() < UPLOAD_SWEEP_BATCH {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            UPLOAD_KEYSPACE,
            None,
            start_after,
            CLEANUP_PAGE_SIZE,
            None,
        )
        .await?;
        for (_, value) in values {
            match MultipartUpload::from_bytes(&value) {
                Ok(record) if stale_upload(&record, now_ms) => stale.push(record),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "Skipping an undecodable multipart upload"),
            }
            if stale.len() >= UPLOAD_SWEEP_BATCH {
                break;
            }
        }
        let Some(next) = next else {
            break;
        };
        start_after = Some(next);
    }

    for record in stale {
        let upload_id = record.upload_id;
        let mut operation = AbortUploadOperation::new(AbortUploadInput {
            bucket: record.bucket,
            key: record.key,
            upload_id,
            now_ms,
        });
        // An abort is transactional, so finishing one that stopped midway is safe to repeat.
        // The status is checked again in that transaction: this scan may be stale.
        if record.status == MultipartUploadStatus::Aborting {
            operation = operation.resuming_abort();
        }
        match drive(operation, context).await {
            Ok(_) => {
                outcome.aborted = outcome.aborted.saturating_add(1);
                warn!(%upload_id, status = ?record.status, "Reclaimed a stale multipart upload");
            }
            Err(error) => {
                outcome.failed = outcome.failed.saturating_add(1);
                warn!(%upload_id, error = %error, "Failed to reclaim a stale multipart upload");
            }
        }
    }

    Ok(outcome)
}

/// The backend a row needs credentials for, if it needs any.
fn work_backend(work: &BlobCleanupWork) -> Option<&BackendRef> {
    work.location().map(|location| &location.backend)
}

/// Only a tenant backend whose record is provably absent counts as removed; an
/// unreadable record leaves the row queued.
async fn is_removed_backend(context: &DriverContext, backend: &BackendRef) -> bool {
    let BackendRef::Group(backend_id) = backend else {
        return false;
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(*backend_id),
            txn_id: None,
        })
        .await;
    matches!(parse_read(event, GroupStorage::from_bytes), Ok(None))
}

async fn delete_cleanup_rows(
    context: &DriverContext,
    deletes: Vec<(String, Key)>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected cleanup delete event: {other:?}")),
    }
}

// Best-effort execution: a failed entry stays queued and retries next drain.
async fn run_cleanup_work(context: &DriverContext, work: BlobCleanupWork) -> RowOutcome {
    match work {
        BlobCleanupWork::DeleteBlob { location } => delete_blob(context, location).await.into(),
        BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::Upload { upload_id },
        } => reconcile_target(context, upload_id, None, location).await,
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
            let holder = (blake3, realm_id, ttl_ms, announce);
            reconcile_target(context, upload_id, Some(holder), location).await
        }
        // The committed metadata decides. An unreadable owner is not a proof of
        // either outcome, so the row waits for a drain that can read it.
        BlobCleanupWork::ReconcileWrite { location, owner } => {
            let done = match owns_write(context, &owner, &location).await {
                None => false,
                Some(true) => {
                    if let Some(blob_handle) = context.blob_handle.as_ref() {
                        blob_handle.clear_reservation(location.ulid);
                    }
                    match owner {
                        WriteOwner::Blob {
                            blake3,
                            realm_id,
                            ttl_ms,
                            announce: true,
                        } => register_dht(context, blake3, realm_id, ttl_ms).await,
                        // A copy of a distributed bucket is found through its content index.
                        WriteOwner::Blob {
                            announce: false, ..
                        } => true,
                        WriteOwner::UploadPart { .. }
                        | WriteOwner::Upload { .. }
                        | WriteOwner::CompletedUpload { .. }
                        | WriteOwner::Pending => true,
                    }
                }
                Some(false) => delete_blob(context, location).await,
            };
            done.into()
        }
        BlobCleanupWork::ReconcileReservation { location } => {
            reconcile_reservation(context, location).await.into()
        }
        BlobCleanupWork::RegisterDht {
            blake3,
            realm_id,
            ttl_ms,
        } => register_dht(context, blake3, realm_id, ttl_ms).await.into(),
        BlobCleanupWork::ReleaseRenamed {
            location,
            import_id,
            version_id,
            version,
        } => release_renamed(context, location, import_id, version_id, version)
            .await
            .into(),
        BlobCleanupWork::ReleasePlanned {
            job_id,
            location,
            import_id,
            version_id,
            version,
        } => match purge_ended(context, job_id).await {
            Some(true) => release_renamed(context, location, import_id, version_id, version)
                .await
                .into(),
            Some(false) => RowOutcome::Kept,
            None => RowOutcome::Failed,
        },
        BlobCleanupWork::SettleUpload {
            attempt,
            location,
            upload_id,
            since_ms,
        } => {
            let now = crate::effect_adapters::routing::now_ms();
            // A copy may still be running; only an upload past the grace is its leftover.
            if now < since_ms.saturating_add(SETTLE_GRACE_MS) {
                return RowOutcome::Kept;
            }
            let Some(blob_handle) = context.blob_handle.as_ref() else {
                return RowOutcome::Failed;
            };
            // Exactly the decoded upload; a newer try of the attempt keeps its own.
            let upload = Some((Box::new(location), upload_id));
            let effect = BlobEffect::SettleCopy { attempt, upload };
            // The settlement removes the record itself, and only while it is unchanged.
            match blob_handle.send_blob_effect(effect).await {
                Event::Blob(BlobEvent::UploadAborted) => RowOutcome::Kept,
                event => {
                    warn!(?event, "A copy upload could not be settled");
                    RowOutcome::Failed
                }
            }
        }
        BlobCleanupWork::SettleMove { version, intents } => {
            match crate::blob::migration::settle_move(context, &version, &intents).await {
                Ok(true) => RowOutcome::Done,
                Ok(false) => RowOutcome::Kept,
                Err(error) => {
                    warn!(%error, "A rename's move could not be settled");
                    RowOutcome::Failed
                }
            }
        }
        BlobCleanupWork::DropRows { key_space, prefix } => {
            match drop_rows(context, &key_space, prefix).await {
                Ok(true) => RowOutcome::Done,
                Ok(false) => RowOutcome::Kept,
                Err(error) => {
                    warn!(%error, "Rows of a removed rule could not be dropped");
                    RowOutcome::Failed
                }
            }
        }
        BlobCleanupWork::DropClaims { import_id } => {
            match crate::blob::release::drop_claims(context, import_id).await {
                Some(true) => RowOutcome::Done,
                Some(false) => RowOutcome::Kept,
                None => RowOutcome::Failed,
            }
        }
        BlobCleanupWork::DropTrail {
            group_id,
            bucket,
            incarnation,
        } => {
            match crate::blob::trail::drop_trail(context, group_id, bucket.as_deref(), incarnation)
                .await
            {
                Ok(()) => RowOutcome::Done,
                Err(error) => {
                    warn!(%error, "Delete trail of a removed bucket could not be dropped");
                    RowOutcome::Failed
                }
            }
        }
    }
}

/// A live upload's target row stays queued: it is also the bucket reservation abort or
/// completion settles. A committed version takes it over; otherwise, after the grace a running
/// create needs, the target goes with its provider uploads.
async fn reconcile_target(
    context: &DriverContext,
    upload_id: Ulid,
    holder: Option<([u8; 32], RealmId, u64, bool)>,
    location: BackendLocation,
) -> RowOutcome {
    match owns_write(context, &WriteOwner::Upload { upload_id }, &location).await {
        None => return RowOutcome::Failed,
        Some(true) => return RowOutcome::Kept,
        Some(false) => {}
    }
    if let Some((blake3, realm_id, ttl_ms, announce)) = holder {
        let owner = WriteOwner::Blob {
            blake3,
            realm_id,
            ttl_ms,
            announce,
        };
        match owns_write(context, &owner, &location).await {
            None => return RowOutcome::Failed,
            Some(true) => {
                if let Some(blob_handle) = context.blob_handle.as_ref() {
                    blob_handle.clear_reservation(location.ulid);
                }
                if !announce {
                    return RowOutcome::Done;
                }
                return register_dht(context, blake3, realm_id, ttl_ms).await.into();
            }
            Some(false) => {}
        }
    }
    let opened_ms = location.ulid.timestamp_ms();
    if now_ms().saturating_sub(opened_ms) < BLOB_CLEANUP_AFTER.as_millis() as u64 {
        return RowOutcome::Kept;
    }
    // Read after the upload: a completion replaces this row in the commit that removes it.
    let own_row = match target_current(context, &location).await {
        None => return RowOutcome::Failed,
        Some(Some(false)) => return RowOutcome::Kept,
        Some(row) => row.is_some(),
    };
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return RowOutcome::Failed;
    };
    // Only the target's own row may delete it; a later abort's row may outlive a completion.
    let settled = match own_row {
        true => blob_handle.discard_target(location).await,
        false => blob_handle.abort_target(location).await,
    };
    match settled {
        Ok(()) => RowOutcome::Done,
        Err(error) => {
            warn!(%error, "Discarding an in-place upload target failed");
            RowOutcome::Failed
        }
    }
}

/// Whether the target's own row still names an upload; `Some(None)` when it is gone. A
/// committed version replaced it with its own row, which settles the object instead.
async fn target_current(
    context: &DriverContext,
    location: &BackendLocation,
) -> Option<Option<bool>> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: location.ulid.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        warn!(?event, "Upload target row could not be read");
        return None;
    };
    Some(value.map(|value| {
        matches!(
            BlobCleanupWork::from_bytes(&value),
            Ok(BlobCleanupWork::ReconcileWrite {
                location: row,
                owner: WriteOwner::Upload { .. } | WriteOwner::CompletedUpload { .. },
            }) if row.same_object(location)
        )
    }))
}

async fn register_dht(
    context: &DriverContext,
    blake3: [u8; 32],
    realm_id: RealmId,
    ttl_ms: u64,
) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return false;
    };
    let limits = RoCrateLimits {
        holder_ttl_ms: ttl_ms,
        ..RoCrateLimits::default()
    };
    let Ok(effect) = dht_registration_effect(&blake3, realm_id, &limits) else {
        return false;
    };
    match net_handle.send_effect(effect).await {
        Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. })) => true,
        event => {
            warn!(?event, "Deferred blob DHT registration failed");
            false
        }
    }
}

async fn delete_blob(context: &DriverContext, location: BackendLocation) -> bool {
    // An imported object is never Aruna's to delete; a part still ends its provider part claim.
    if location.is_external() && !location.partial {
        return true;
    }
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return false;
    };
    match blob_handle
        .send_blob_effect(BlobEffect::Delete { location })
        .await
    {
        Event::Blob(BlobEvent::DeleteFinished) => true,
        event => {
            warn!(?event, "Deferred blob delete failed");
            false
        }
    }
}

async fn reconcile_reservation(context: &DriverContext, location: BackendLocation) -> bool {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return false;
    };
    match blob_handle.reconcile_reservation(location).await {
        Ok(done) => done,
        Err(error) => {
            warn!(%error, "Deferred bucket reservation reconciliation failed");
            false
        }
    }
}

/// Whether committed metadata still names this exact physical copy. `None`
/// means the record could not be read or decoded, so nothing is proven.
async fn owns_write(
    context: &DriverContext,
    owner: &WriteOwner,
    location: &BackendLocation,
) -> Option<bool> {
    let (key_space, key): (&str, Key) = match owner {
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
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        warn!(?event, "Ambiguous write owner could not be read");
        return None;
    };
    let Some(value) = value else {
        // A promoted archive has no pending row; its versions still own it.
        if matches!(owner, WriteOwner::Pending) {
            return archive_owned(context, location).await;
        }
        return Some(false);
    };
    let owned = match owner {
        WriteOwner::Blob { .. } | WriteOwner::Pending => {
            BackendLocation::from_bytes(&value).ok()?
        }
        WriteOwner::UploadPart { .. } => MultipartPart::from_bytes(&value).ok()?.location,
        WriteOwner::Upload { .. } | WriteOwner::CompletedUpload { .. } => {
            // An aborting upload still owns its target: a failed abort reopens it for a
            // completion, and the abort itself, also when resumed, removes the target.
            let record = MultipartUpload::from_bytes(&value).ok()?;
            match record.backend_upload {
                Some(upload) => upload.location,
                None => return Some(false),
            }
        }
    };
    Some(owned.same_object(location))
}

/// Whether any committed version owns the Pithos archive of `location`.
async fn archive_owned(context: &DriverContext, location: &BackendLocation) -> Option<bool> {
    let scan = StorageEffect::Iter {
        key_space: COPY_OWNER_KEYSPACE.to_string(),
        prefix: Some(CopyOwner::prefix(&ArchiveKey::of(location)).into()),
        start: None,
        limit: 1,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(scan).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Some(!values.is_empty()),
        event => {
            warn!(?event, "Archive owners could not be read");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CLEANUP_PAGE_SIZE, MAX_CLEANUP_RETRIES, PendingCleanup, process_cleanup_batch};
    use crate::driver::DriverContext;
    use crate::jobs::store::iter_prefix_page;
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

    fn setup_context() -> (TempDir, StorageHandle, DriverContext) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
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

    #[test]
    fn retains_refused_row() {
        // A temporary storage rejection must not discard the only location.
        let work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        let error = aruna_core::errors::StorageError::QueueFull;
        let mut pending = PendingCleanup::default();

        let first = pending.queue(work).unwrap();
        assert_eq!(pending.retry(&error), Some(first));
        let first = pending.retry(&error).unwrap();
        assert_eq!(pending.retry(&error), Some(first));

        let mut pending = PendingCleanup::default();
        assert!(
            pending
                .queue(BlobCleanupWork::from_bytes(&delete_work()).unwrap())
                .is_some()
        );
        pending.accepted();
        assert!(pending.retry(&error).is_none());
    }

    #[test]
    fn bounds_retry_attempts() {
        // Endless rejection must surrender the row so the request can finish and
        // release its blob reservation.
        let work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        let error = aruna_core::errors::StorageError::QueueFull;
        let mut pending = PendingCleanup::default();
        assert!(pending.queue(work).is_some());

        for _ in 0..MAX_CLEANUP_RETRIES {
            assert!(pending.retry(&error).is_some());
        }

        assert!(pending.retry(&error).is_none());
        assert!(pending.retry(&error).is_none());
    }

    #[test]
    fn closed_retry_stops() {
        let work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        let mut pending = PendingCleanup::default();
        assert!(pending.queue(work).is_some());

        assert_eq!(
            pending.retry(&aruna_core::errors::StorageError::ChannelClosed),
            None
        );
        assert_eq!(
            pending.retry(&aruna_core::errors::StorageError::Timeout),
            None
        );
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
        let now = crate::effect_adapters::routing::now_ms();
        let fresh = super::run_cleanup_work(&context, settle(now)).await;
        assert!(matches!(fresh, super::RowOutcome::Kept));
        // This context has no blob storage, so the abort cannot run and the row stays.
        let stale = super::run_cleanup_work(&context, settle(0)).await;
        assert!(matches!(stale, super::RowOutcome::Failed));
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
            super::run_cleanup_work(&context, work()).await,
            super::RowOutcome::Kept
        ));
        assert!(matches!(
            super::run_cleanup_work(&context, work()).await,
            super::RowOutcome::Done
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

        let outcome = super::reconcile_target(&context, Ulid::generate(), None, location).await;

        // Without the recheck the discard runs and fails here for want of a blob handle.
        assert!(matches!(outcome, super::RowOutcome::Kept));
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
