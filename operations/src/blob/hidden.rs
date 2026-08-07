use std::time::{Duration, Instant, UNIX_EPOCH};

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    JOB_KEYSPACE, NODE_STATE_KEYSPACE, ROCRATE_UPLOAD_CLEANUP_KEYSPACE, ROCRATE_UPLOAD_KEYSPACE,
};
use aruna_core::structs::{
    BackendLocation, HiddenBlobEntry, HiddenBlobKey, JobId, JobRecord, JobResultPayload,
    RoCrateUploadCleanup, RoCrateUploadRecord, job_record_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, TxnId};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;
use crate::task_persistence::persist_task_effect;

const SWEEP_PAGE_SIZE: usize = 128;
const SWEEP_IO_TIMEOUT: Duration = Duration::from_secs(5);
const SWEEP_BUDGET: Duration = Duration::from_secs(30);
const MAX_JOB_BYTES: usize = 1024 * 1024;
pub const HIDDEN_SWEEP_AFTER: Duration = Duration::from_secs(60 * 60);
pub const HIDDEN_SWEEP_RETRY: Duration = Duration::from_secs(30);
const ORPHAN_GRACE: Duration = Duration::from_secs(60 * 60);
const HIDDEN_CURSOR_KEY: &[u8] = b"hidden-sweep";

#[derive(Debug, Deserialize, Serialize)]
struct SweepCursor {
    phase: SweepPhase,
    start_after: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum SweepPhase {
    Cleanup,
    Uploads,
    Hidden,
}

impl Default for SweepCursor {
    fn default() -> Self {
        Self {
            phase: SweepPhase::Cleanup,
            start_after: None,
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct HiddenSweepOutcome {
    pub cleanup_pending: bool,
    pub uploads_deleted: usize,
    pub orphans_deleted: usize,
}

pub async fn delete_hidden(
    context: &DriverContext,
    location: &BackendLocation,
) -> Result<(), String> {
    let key = HiddenBlobKey::try_from(location).map_err(|error| error.to_string())?;
    delete_key(context, key, Instant::now() + SWEEP_BUDGET).await
}

pub async fn process_hidden_sweep(context: &DriverContext) -> Result<HiddenSweepOutcome, String> {
    sweep_at(context, unix_timestamp_millis()).await
}

pub async fn restore_hidden_sweep(storage: &StorageHandle, task_handle: &TaskHandle) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::SweepHiddenBlobs,
        after: Duration::ZERO,
    };
    if let Err(message) = persist_task_effect(storage, &effect).await {
        warn!(message = %message, "Failed to persist hidden blob sweep timer");
        return;
    }
    match task_handle.send_effect(Effect::Task(effect)).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => {}
        Event::Task(TaskEvent::Error { message, .. }) => {
            warn!(message = %message, "Failed to schedule hidden blob sweep");
        }
        other => warn!(event = ?other, "Unexpected hidden blob sweep timer result"),
    }
}

fn time_left(deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(Instant::now())
        .min(SWEEP_IO_TIMEOUT)
}

async fn load_cursor(storage: &StorageHandle, deadline: Instant) -> Result<SweepCursor, String> {
    let event = tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(HIDDEN_CURSOR_KEY.to_vec()),
            txn_id: None,
        }),
    )
    .await
    .map_err(|_| "timed out reading hidden sweep cursor".to_string())?;
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref()).map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(SweepCursor::default()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected hidden sweep cursor event: {other:?}")),
    }
}

async fn save_cursor(
    storage: &StorageHandle,
    cursor: &SweepCursor,
    deadline: Instant,
) -> Result<(), String> {
    let value = postcard::to_allocvec(cursor).map_err(|error| error.to_string())?;
    let event = tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(HIDDEN_CURSOR_KEY.to_vec()),
            value: ByteView::from(value),
            txn_id: None,
        }),
    )
    .await
    .map_err(|_| "timed out writing hidden sweep cursor".to_string())?;
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected hidden sweep cursor event: {other:?}")),
    }
}

async fn sweep_at(context: &DriverContext, now_ms: u64) -> Result<HiddenSweepOutcome, String> {
    let deadline = Instant::now() + SWEEP_BUDGET;
    let mut cursor = load_cursor(&context.storage_handle, deadline).await?;
    let mut cleanup_pending = false;
    let mut uploads_deleted = 0;
    let mut orphans_deleted = 0;
    let more = match cursor.phase {
        SweepPhase::Cleanup => {
            let (pending, more) =
                sweep_upload_cleanups(context, &mut cursor.start_after, deadline).await?;
            cleanup_pending = pending;
            if !more {
                cursor.phase = SweepPhase::Uploads;
                cursor.start_after = None;
            }
            !matches!(cursor.phase, SweepPhase::Cleanup) || more
        }
        SweepPhase::Uploads => {
            let (deleted, more, pending) =
                sweep_uploads(context, now_ms, &mut cursor.start_after, deadline).await?;
            uploads_deleted = deleted;
            cleanup_pending = pending;
            if !more {
                cursor.phase = SweepPhase::Hidden;
                cursor.start_after = None;
            }
            !matches!(cursor.phase, SweepPhase::Uploads) || more
        }
        SweepPhase::Hidden => {
            let (deleted, more, pending) =
                sweep_hidden(context, now_ms, &mut cursor.start_after, deadline).await?;
            orphans_deleted = deleted;
            cleanup_pending = pending;
            if !more {
                cursor.phase = SweepPhase::Cleanup;
                cursor.start_after = None;
            }
            more
        }
    };
    save_cursor(&context.storage_handle, &cursor, deadline).await?;
    Ok(HiddenSweepOutcome {
        cleanup_pending: cleanup_pending || more,
        uploads_deleted,
        orphans_deleted,
    })
}

async fn sweep_upload_cleanups(
    context: &DriverContext,
    cursor: &mut Option<Vec<u8>>,
    deadline: Instant,
) -> Result<(bool, bool), String> {
    let mut pending = false;
    let start_after = cursor.take().map(ByteView::from);
    let (values, next) = tokio::time::timeout(
        time_left(deadline),
        iter_prefix_page(
            &context.storage_handle,
            ROCRATE_UPLOAD_CLEANUP_KEYSPACE,
            None,
            start_after,
            SWEEP_PAGE_SIZE,
            None,
        ),
    )
    .await
    .map_err(|_| "timed out scanning upload cleanup rows".to_string())??;
    for (storage_key, value) in values {
        if time_left(deadline).is_zero() {
            return Err("hidden sweep budget exhausted".to_string());
        }
        let cleanup = match RoCrateUploadCleanup::from_bytes(&value) {
            Ok(cleanup) => cleanup,
            Err(error) => {
                warn!(error = %error, "Failed to decode RO-Crate upload cleanup");
                continue;
            }
        };
        if storage_key.as_ref() != cleanup.upload_id.to_bytes().as_slice() {
            warn!("RO-Crate upload cleanup key does not match its upload id");
            pending = true;
            continue;
        }
        let record_cleanup = match delete_unclaimed(
            &context.storage_handle,
            cleanup.upload_id,
            unix_timestamp_millis(),
            deadline,
        )
        .await
        {
            Ok(cleanup) => cleanup,
            Err(error) => {
                warn!(error = %error, "Failed to compensate RO-Crate upload record");
                pending = true;
                continue;
            }
        };
        match record_cleanup {
            RecordCleanup::Protected => {
                pending = true;
            }
            RecordCleanup::Missing | RecordCleanup::Deleted => {
                if let Err(error) = delete_key(context, cleanup.hidden_key, deadline).await {
                    warn!(error = %error, "Failed to delete upload cleanup blob");
                    pending = true;
                    continue;
                }
                if let Err(error) = delete_cleanup(context, storage_key, deadline).await {
                    warn!(error = %error, "Failed to delete upload cleanup row");
                    pending = true;
                }
            }
        }
    }
    let more = next.is_some();
    *cursor = next.map(|key| key.to_vec());
    Ok((pending, more))
}

async fn sweep_uploads(
    context: &DriverContext,
    now_ms: u64,
    cursor: &mut Option<Vec<u8>>,
    deadline: Instant,
) -> Result<(usize, bool, bool), String> {
    let mut deleted = 0usize;
    let mut pending = false;
    let start_after = cursor.take().map(ByteView::from);
    let (values, next) = tokio::time::timeout(
        time_left(deadline),
        iter_prefix_page(
            &context.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            None,
            start_after,
            SWEEP_PAGE_SIZE,
            None,
        ),
    )
    .await
    .map_err(|_| "timed out scanning RO-Crate uploads".to_string())??;
    for (storage_key, value) in values {
        if time_left(deadline).is_zero() {
            return Err("hidden sweep budget exhausted".to_string());
        }
        let record: RoCrateUploadRecord = match postcard::from_bytes(&value) {
            Ok(record) => record,
            Err(error) => {
                warn!(error = %error, "Failed to decode RO-Crate upload");
                continue;
            }
        };
        if storage_key.as_ref() != record.upload_id.to_bytes().as_slice() {
            warn!("RO-Crate upload key does not match its upload id");
            pending = true;
            continue;
        }
        let key = match HiddenBlobKey::try_from(&record.location) {
            Ok(key) => key,
            Err(error) => {
                warn!(error = %error, "Invalid RO-Crate upload location");
                continue;
            }
        };
        let record_cleanup =
            match delete_unclaimed(&context.storage_handle, record.upload_id, now_ms, deadline)
                .await
            {
                Ok(cleanup) => cleanup,
                Err(error) => {
                    warn!(error = %error, "Failed to claim stale RO-Crate upload cleanup");
                    pending = true;
                    continue;
                }
            };
        if matches!(record_cleanup, RecordCleanup::Protected) {
            continue;
        }
        if let Err(error) = delete_key(context, key, deadline).await {
            warn!(error = %error, "Failed to delete stale upload blob");
            pending = true;
            continue;
        }
        deleted = deleted.saturating_add(1);
    }
    let more = next.is_some();
    *cursor = next.map(|key| key.to_vec());
    Ok((deleted, more, pending))
}

async fn upload_live(
    context: &DriverContext,
    record: &RoCrateUploadRecord,
    now_ms: u64,
    deadline: Instant,
) -> Result<bool, String> {
    let claimed_live = match record.claimed_by {
        Some(job_id) => job_live(&context.storage_handle, job_id, None, deadline).await?,
        None => false,
    };
    Ok(upload_is_live(record, claimed_live, now_ms))
}

fn upload_is_live(record: &RoCrateUploadRecord, claimed_live: bool, now_ms: u64) -> bool {
    match record.claimed_by {
        Some(_) => claimed_live,
        None => record.expires_at_ms > now_ms,
    }
}

fn hidden_is_old(entry: &HiddenBlobEntry, cutoff_ms: u64) -> bool {
    entry.modified_at.is_some_and(|modified| {
        modified
            .duration_since(UNIX_EPOCH)
            .ok()
            .is_some_and(|time| time.as_millis() as u64 <= cutoff_ms)
    })
}

async fn sweep_hidden(
    context: &DriverContext,
    now_ms: u64,
    cursor: &mut Option<Vec<u8>>,
    deadline: Instant,
) -> Result<(usize, bool, bool), String> {
    let (entries, next) = list_hidden(context, cursor.take(), deadline).await?;
    let cutoff_ms = now_ms.saturating_sub(ORPHAN_GRACE.as_millis() as u64);
    let mut deleted = 0usize;
    let mut pending = false;
    for entry in entries {
        if time_left(deadline).is_zero() {
            return Err("hidden sweep budget exhausted".to_string());
        }
        if !hidden_is_old(&entry, cutoff_ms) {
            continue;
        }
        match is_protected(context, &entry.key, now_ms, deadline).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                warn!(error = %error, "Failed to validate hidden blob ownership");
                pending = true;
                continue;
            }
        }
        if let Err(error) = delete_key(context, entry.key, deadline).await {
            warn!(error = %error, "Failed to delete hidden orphan");
            pending = true;
            continue;
        }
        deleted = deleted.saturating_add(1);
    }
    let more = next.is_some();
    *cursor = next;
    Ok((deleted, more, pending))
}

async fn is_protected(
    context: &DriverContext,
    key: &HiddenBlobKey,
    now_ms: u64,
    deadline: Instant,
) -> Result<bool, String> {
    let namespace = key.namespace().map_err(|error| error.to_string())?;
    if let Some(value) = read_value(
        &context.storage_handle,
        ROCRATE_UPLOAD_KEYSPACE,
        ByteView::from(namespace.to_bytes().to_vec()),
        None,
        deadline,
    )
    .await?
    {
        if value.len() > MAX_JOB_BYTES {
            return Err("RO-Crate upload record exceeds limit".to_string());
        }
        let record: RoCrateUploadRecord =
            postcard::from_bytes(&value).map_err(|error| error.to_string())?;
        let record_key =
            HiddenBlobKey::try_from(&record.location).map_err(|error| error.to_string())?;
        if record_key == *key && upload_live(context, &record, now_ms, deadline).await? {
            return Ok(true);
        }
    }
    let Ok(job_id) = JobId::try_from_bytes(namespace.to_bytes()) else {
        return Ok(false);
    };
    let Some(value) = read_value(
        &context.storage_handle,
        JOB_KEYSPACE,
        job_record_key(job_id),
        None,
        deadline,
    )
    .await?
    else {
        return Ok(false);
    };
    if value.len() > MAX_JOB_BYTES {
        return Err("job record exceeds limit".to_string());
    }
    let record = JobRecord::from_bytes(&value).map_err(|error| error.to_string())?;
    if !record.state.is_terminal() && record.payload.is_rocrate() {
        return Ok(true);
    }
    if let Some(JobResultPayload::ExportRoCrate(result)) = &record.result
        && let Some(artifact) = &result.artifact
    {
        let artifact_key =
            HiddenBlobKey::try_from(&artifact.location).map_err(|error| error.to_string())?;
        if artifact_key == *key {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn read_value(
    storage: &StorageHandle,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
    deadline: Instant,
) -> Result<Option<ByteView>, String> {
    let event = tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id,
        }),
    )
    .await
    .map_err(|_| "timed out reading sweep metadata".to_string())?;
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(value)),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected sweep metadata event: {other:?}")),
    }
}

async fn job_live(
    storage: &StorageHandle,
    job_id: JobId,
    txn_id: Option<TxnId>,
    deadline: Instant,
) -> Result<bool, String> {
    let Some(value) = read_value(
        storage,
        JOB_KEYSPACE,
        job_record_key(job_id),
        txn_id,
        deadline,
    )
    .await?
    else {
        return Ok(false);
    };
    if value.len() > MAX_JOB_BYTES {
        return Err("job record exceeds limit".to_string());
    }
    let record = JobRecord::from_bytes(&value).map_err(|error| error.to_string())?;
    Ok(!record.state.is_terminal() && record.payload.is_rocrate())
}

async fn list_hidden(
    context: &DriverContext,
    cursor: Option<Vec<u8>>,
    deadline: Instant,
) -> Result<(Vec<HiddenBlobEntry>, Option<Vec<u8>>), String> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return Err("blob handle unavailable".to_string());
    };
    let event = tokio::time::timeout(
        time_left(deadline),
        blob_handle.send_blob_effect(BlobEffect::ListHidden {
            namespace: None,
            cursor,
        }),
    )
    .await
    .map_err(|_| "timed out listing hidden blobs".to_string())?;
    match event {
        Event::Blob(BlobEvent::HiddenListed {
            entries,
            next_cursor,
        }) => Ok((entries, next_cursor)),
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        other => Err(format!("unexpected hidden blob list event: {other:?}")),
    }
}

async fn delete_key(
    context: &DriverContext,
    key: HiddenBlobKey,
    deadline: Instant,
) -> Result<(), String> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return Err("blob handle unavailable".to_string());
    };
    let event = tokio::time::timeout(
        time_left(deadline),
        blob_handle.send_blob_effect(BlobEffect::DeleteHidden { key }),
    )
    .await
    .map_err(|_| "timed out deleting hidden blob".to_string())?;
    match event {
        Event::Blob(BlobEvent::HiddenDeleted) => Ok(()),
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        other => Err(format!("unexpected hidden blob delete event: {other:?}")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordCleanup {
    Missing,
    Protected,
    Deleted,
}

async fn delete_unclaimed(
    storage: &StorageHandle,
    upload_id: Ulid,
    now_ms: u64,
    deadline: Instant,
) -> Result<RecordCleanup, String> {
    let txn_id = start_txn(storage, deadline).await?;
    let event = match tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: ByteView::from(upload_id.to_bytes().to_vec()),
            txn_id: Some(txn_id),
        }),
    )
    .await
    {
        Ok(event) => event,
        Err(_) => {
            let abort = abort_txn(storage, txn_id).await;
            return Err(match abort {
                Ok(()) => "timed out reading upload cleanup record".to_string(),
                Err(error) => {
                    format!("timed out reading upload cleanup record; abort failed: {error}")
                }
            });
        }
    };
    let value = match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => value,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_txn(storage, txn_id).await?;
            return Ok(RecordCleanup::Missing);
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_txn(storage, txn_id).await?;
            return Err(error.to_string());
        }
        other => {
            abort_txn(storage, txn_id).await?;
            return Err(format!("unexpected upload cleanup read event: {other:?}"));
        }
    };
    let record: RoCrateUploadRecord = match postcard::from_bytes(value.as_ref()) {
        Ok(record) => record,
        Err(error) => {
            abort_txn(storage, txn_id).await?;
            return Err(error.to_string());
        }
    };
    let claimed_live = match record.claimed_by {
        Some(job_id) => match job_live(storage, job_id, Some(txn_id), deadline).await {
            Ok(live) => live,
            Err(error) => {
                abort_txn(storage, txn_id).await?;
                return Err(error);
            }
        },
        None => false,
    };
    if upload_is_live(&record, claimed_live, now_ms) {
        abort_txn(storage, txn_id).await?;
        return Ok(RecordCleanup::Protected);
    }
    let event = match tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::Delete {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: ByteView::from(upload_id.to_bytes().to_vec()),
            txn_id: Some(txn_id),
        }),
    )
    .await
    {
        Ok(event) => event,
        Err(_) => {
            let abort = abort_txn(storage, txn_id).await;
            return Err(match abort {
                Ok(()) => "timed out deleting upload cleanup record".to_string(),
                Err(error) => {
                    format!("timed out deleting upload cleanup record; abort failed: {error}")
                }
            });
        }
    };
    if !matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })) {
        abort_txn(storage, txn_id).await?;
        return match event {
            Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
            other => Err(format!("unexpected upload cleanup delete event: {other:?}")),
        };
    }
    match commit_txn(storage, txn_id, deadline).await {
        Ok(()) => Ok(RecordCleanup::Deleted),
        Err(error) if error.proves_no_commit() => match abort_txn(storage, txn_id).await {
            Ok(()) => Err(error.to_string()),
            Err(abort_error) => Err(format!("{error}; abort failed: {abort_error}")),
        },
        Err(error) => Err(error.to_string()),
    }
}

async fn delete_cleanup(
    storage: &StorageHandle,
    key: Key,
    deadline: Instant,
) -> Result<(), String> {
    let event = tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::Delete {
            key_space: ROCRATE_UPLOAD_CLEANUP_KEYSPACE.to_string(),
            key,
            txn_id: None,
        }),
    )
    .await
    .map_err(|_| "timed out deleting upload cleanup row".to_string())?;
    match event {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected upload cleanup row delete event: {other:?}"
        )),
    }
}

async fn start_txn(storage: &StorageHandle, deadline: Instant) -> Result<TxnId, String> {
    let event = tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::StartTransaction { read: false }),
    )
    .await
    .map_err(|_| "timed out starting upload cleanup transaction".to_string())?;
    match event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected upload cleanup transaction event: {other:?}"
        )),
    }
}

async fn commit_txn(
    storage: &StorageHandle,
    txn_id: TxnId,
    deadline: Instant,
) -> Result<(), StorageError> {
    let event = tokio::time::timeout(
        time_left(deadline),
        storage.send_storage_effect(StorageEffect::CommitTransaction { txn_id }),
    )
    .await
    .map_err(|_| StorageError::Timeout)?;
    match event {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        _ => Err(StorageError::InvalidEffect),
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) -> Result<(), String> {
    let event = tokio::time::timeout(
        SWEEP_IO_TIMEOUT,
        storage.send_storage_effect(StorageEffect::AbortTransaction { txn_id }),
    )
    .await
    .map_err(|_| "timed out aborting upload cleanup transaction".to_string())?;
    match event {
        Event::Storage(StorageEvent::TransactionAborted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload cleanup abort event: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{BackendRef, RealmId, RoCrateCheckpointRefs, RoCrateMediaType};
    use aruna_core::types::UserId;
    use serde::Serialize;
    use std::collections::HashMap;

    fn upload_record(
        upload_id: Ulid,
        expires_at_ms: u64,
        claimed_by: Option<JobId>,
    ) -> RoCrateUploadRecord {
        RoCrateUploadRecord {
            upload_id,
            owner: UserId::nil(RealmId::from_bytes([1u8; 32])),
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: format!("_jobs/{upload_id}/input_01"),
                ulid: Ulid::from_bytes([2u8; 16]),
                compressed: false,
                encrypted: false,
                created_by: UserId::nil(RealmId::from_bytes([1u8; 32])),
                created_at: UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: HashMap::new(),
            },
            blake3: [3u8; 32],
            size: 1,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms,
            claimed_by,
        }
    }

    #[test]
    fn upload_liveness() {
        let upload_id = Ulid::from_bytes([4u8; 16]);
        let job_id = JobId::from_bytes([5u8; 16]);

        assert!(upload_is_live(
            &upload_record(upload_id, 11, None),
            false,
            10
        ));
        assert!(!upload_is_live(
            &upload_record(upload_id, 10, None),
            false,
            10
        ));
        assert!(upload_is_live(
            &upload_record(upload_id, 0, Some(job_id)),
            true,
            10
        ));
        assert!(!upload_is_live(
            &upload_record(upload_id, 0, Some(job_id)),
            false,
            10
        ));
    }

    #[test]
    fn orphan_selection() {
        let namespace = Ulid::from_bytes([7u8; 16]);
        let key = HiddenBlobKey::new(
            BackendRef::node_default(),
            "/data".to_string(),
            "storage".to_string(),
            format!("_jobs/{namespace}/artifact_01"),
        )
        .unwrap();
        let entry = HiddenBlobEntry {
            key,
            modified_at: Some(UNIX_EPOCH),
        };

        assert!(hidden_is_old(&entry, 0));
        assert!(!hidden_is_old(
            &HiddenBlobEntry {
                key: entry.key.clone(),
                modified_at: Some(UNIX_EPOCH + Duration::from_millis(1)),
            },
            0
        ));
        assert!(!hidden_is_old(
            &HiddenBlobEntry {
                key: entry.key,
                modified_at: None,
            },
            0
        ));
    }

    #[test]
    fn checkpoint_prefix_decodes() {
        #[derive(Serialize)]
        struct Checkpoint {
            refs: RoCrateCheckpointRefs,
            phase: u8,
        }

        let location = upload_record(Ulid::from_bytes([8u8; 16]), 10, None).location;
        let encoded = postcard::to_allocvec(&Checkpoint {
            refs: RoCrateCheckpointRefs {
                hidden_locations: vec![location.clone()],
            },
            phase: 3,
        })
        .unwrap();
        let (refs, remainder) =
            postcard::take_from_bytes::<RoCrateCheckpointRefs>(&encoded).unwrap();

        assert_eq!(refs.hidden_locations, vec![location]);
        assert!(!remainder.is_empty());
    }
}
