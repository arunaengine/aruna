use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    JOB_KEYSPACE, ROCRATE_JOB_STATE_KEYSPACE, ROCRATE_UPLOAD_CLEANUP_KEYSPACE,
    ROCRATE_UPLOAD_KEYSPACE,
};
use aruna_core::structs::{
    BackendLocation, HiddenBlobEntry, HiddenBlobKey, JOB_RECORD_KEY_PREFIX, JobId, JobRecord,
    JobResultPayload, RoCrateCheckpointRefs, RoCrateUploadCleanup, RoCrateUploadRecord,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, TxnId};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;
use crate::task_persistence::persist_task_effect;

const SWEEP_PAGE_SIZE: usize = 512;
pub const HIDDEN_SWEEP_AFTER: Duration = Duration::from_secs(60 * 60);
pub const HIDDEN_SWEEP_RETRY: Duration = Duration::from_secs(30);
const ORPHAN_GRACE: Duration = Duration::from_secs(60 * 60);

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
    delete_key(context, key).await
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

async fn sweep_at(context: &DriverContext, now_ms: u64) -> Result<HiddenSweepOutcome, String> {
    let (active_jobs, active_rocrate, mut referenced) = scan_jobs(&context.storage_handle).await?;
    let cleanup_pending = sweep_upload_cleanups(context).await?;
    let uploads_deleted = sweep_uploads(context, &active_jobs, now_ms, &mut referenced).await?;
    let entries = list_hidden(context).await?;
    let cutoff = UNIX_EPOCH
        .checked_add(Duration::from_millis(
            now_ms.saturating_sub(ORPHAN_GRACE.as_millis() as u64),
        ))
        .unwrap_or(UNIX_EPOCH);
    let mut orphans_deleted = 0usize;
    for entry in entries {
        if is_orphaned(&entry, &referenced, &active_rocrate, cutoff) {
            delete_key(context, entry.key).await?;
            orphans_deleted = orphans_deleted.saturating_add(1);
        }
    }
    Ok(HiddenSweepOutcome {
        cleanup_pending,
        uploads_deleted,
        orphans_deleted,
    })
}

async fn sweep_upload_cleanups(context: &DriverContext) -> Result<bool, String> {
    let mut pending = false;
    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            ROCRATE_UPLOAD_CLEANUP_KEYSPACE,
            None,
            start_after.take(),
            SWEEP_PAGE_SIZE,
            None,
        )
        .await?;
        for (storage_key, value) in values {
            let cleanup = match RoCrateUploadCleanup::from_bytes(&value) {
                Ok(cleanup) => cleanup,
                Err(error) => {
                    warn!(error = %error, "Failed to decode RO-Crate upload cleanup");
                    pending = true;
                    continue;
                }
            };
            let record_cleanup =
                match delete_unclaimed(&context.storage_handle, cleanup.upload_id).await {
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
                    if delete_key(context, cleanup.hidden_key).await.is_err() {
                        pending = true;
                        continue;
                    }
                    if delete_cleanup(context, storage_key).await.is_err() {
                        pending = true;
                    }
                }
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    Ok(pending)
}

async fn scan_jobs(
    storage: &StorageHandle,
) -> Result<(HashSet<JobId>, HashSet<JobId>, HashSet<HiddenBlobKey>), String> {
    let mut active = HashSet::new();
    let mut active_rocrate = HashSet::new();
    let mut referenced = HashSet::new();
    let mut start_after = None;
    loop {
        // Scan the versioned record prefix only.
        let (values, next) = iter_prefix_page(
            storage,
            JOB_KEYSPACE,
            Some(ByteView::from(JOB_RECORD_KEY_PREFIX.to_vec())),
            start_after.take(),
            SWEEP_PAGE_SIZE,
            None,
        )
        .await?;
        for (_, value) in values {
            let record = JobRecord::from_bytes(&value).map_err(|error| error.to_string())?;
            if !record.state.is_terminal() {
                active.insert(record.job_id);
                if record.payload.is_rocrate() {
                    active_rocrate.insert(record.job_id);
                    for location in checkpoint_refs(storage, record.job_id).await? {
                        referenced.insert(
                            HiddenBlobKey::try_from(&location)
                                .map_err(|error| error.to_string())?,
                        );
                    }
                }
            }
            if let Some(JobResultPayload::ExportRoCrate(result)) = record.result
                && let Some(artifact) = result.artifact
            {
                referenced.insert(
                    HiddenBlobKey::try_from(&artifact.location)
                        .map_err(|error| error.to_string())?,
                );
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    Ok((active, active_rocrate, referenced))
}

async fn checkpoint_refs(
    storage: &StorageHandle,
    job_id: JobId,
) -> Result<Vec<BackendLocation>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_JOB_STATE_KEYSPACE.to_string(),
            key: job_id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::take_from_bytes::<RoCrateCheckpointRefs>(value.as_ref())
            .map(|(refs, _)| refs.hidden_locations)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(Vec::new()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected RO-Crate checkpoint read event: {other:?}"
        )),
    }
}

async fn sweep_uploads(
    context: &DriverContext,
    active_jobs: &HashSet<JobId>,
    now_ms: u64,
    referenced: &mut HashSet<HiddenBlobKey>,
) -> Result<usize, String> {
    let mut deleted = 0usize;
    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            None,
            start_after.take(),
            SWEEP_PAGE_SIZE,
            None,
        )
        .await?;
        for (storage_key, value) in values {
            let record: RoCrateUploadRecord =
                postcard::from_bytes(&value).map_err(|error| error.to_string())?;
            let key =
                HiddenBlobKey::try_from(&record.location).map_err(|error| error.to_string())?;
            if upload_is_live(&record, active_jobs, now_ms) {
                referenced.insert(key);
                continue;
            }
            delete_key(context, key).await?;
            delete_record(&context.storage_handle, storage_key).await?;
            deleted = deleted.saturating_add(1);
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    Ok(deleted)
}

fn upload_is_live(record: &RoCrateUploadRecord, active_jobs: &HashSet<JobId>, now_ms: u64) -> bool {
    match record.claimed_by {
        Some(job_id) => active_jobs.contains(&job_id),
        None => record.expires_at_ms > now_ms,
    }
}

fn is_orphaned(
    entry: &HiddenBlobEntry,
    referenced: &HashSet<HiddenBlobKey>,
    active_rocrate: &HashSet<JobId>,
    cutoff: SystemTime,
) -> bool {
    if referenced.contains(&entry.key) {
        return false;
    }
    if entry.key.namespace().is_ok_and(|namespace| {
        JobId::try_from_bytes(namespace.to_bytes())
            .is_ok_and(|job_id| active_rocrate.contains(&job_id))
    }) {
        return false;
    }
    entry.modified_at.is_some_and(|modified| modified <= cutoff)
}

async fn list_hidden(context: &DriverContext) -> Result<Vec<HiddenBlobEntry>, String> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return Err("blob handle unavailable".to_string());
    };
    match blob_handle
        .send_blob_effect(BlobEffect::ListHidden { namespace: None })
        .await
    {
        Event::Blob(BlobEvent::HiddenListed { entries }) => Ok(entries),
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        other => Err(format!("unexpected hidden blob list event: {other:?}")),
    }
}

async fn delete_key(context: &DriverContext, key: HiddenBlobKey) -> Result<(), String> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return Err("blob handle unavailable".to_string());
    };
    match blob_handle
        .send_blob_effect(BlobEffect::DeleteHidden { key })
        .await
    {
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
) -> Result<RecordCleanup, String> {
    let txn_id = start_txn(storage).await?;
    let event = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: ByteView::from(upload_id.to_bytes().to_vec()),
            txn_id: Some(txn_id),
        })
        .await;
    let value = match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => value,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_txn(storage, txn_id).await;
            return Ok(RecordCleanup::Missing);
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_txn(storage, txn_id).await;
            return Err(error.to_string());
        }
        other => {
            abort_txn(storage, txn_id).await;
            return Err(format!("unexpected upload cleanup read event: {other:?}"));
        }
    };
    let record: RoCrateUploadRecord = match postcard::from_bytes(value.as_ref()) {
        Ok(record) => record,
        Err(error) => {
            abort_txn(storage, txn_id).await;
            return Err(error.to_string());
        }
    };
    if record.claimed_by.is_some() {
        abort_txn(storage, txn_id).await;
        return Ok(RecordCleanup::Protected);
    }
    let event = storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: ByteView::from(upload_id.to_bytes().to_vec()),
            txn_id: Some(txn_id),
        })
        .await;
    if !matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })) {
        abort_txn(storage, txn_id).await;
        return match event {
            Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
            other => Err(format!("unexpected upload cleanup delete event: {other:?}")),
        };
    }
    match commit_txn(storage, txn_id).await {
        Ok(()) => Ok(RecordCleanup::Deleted),
        Err(error) => Err(error),
    }
}

async fn delete_cleanup(storage: &StorageHandle, key: Key) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: ROCRATE_UPLOAD_CLEANUP_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected upload cleanup row delete event: {other:?}"
        )),
    }
}

async fn start_txn(storage: &StorageHandle) -> Result<TxnId, String> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected upload cleanup transaction event: {other:?}"
        )),
    }
}

async fn commit_txn(storage: &StorageHandle, txn_id: TxnId) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Err("upload cleanup transaction conflicted".to_string()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload cleanup commit event: {other:?}")),
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn delete_record(storage: &StorageHandle, key: Key) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected upload delete event: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{BackendLocation, BackendRef, RealmId, RoCrateMediaType};
    use aruna_core::types::UserId;
    use serde::Serialize;
    use std::collections::HashMap;
    use ulid::Ulid;

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
        let active = HashSet::from([job_id]);

        assert!(upload_is_live(
            &upload_record(upload_id, 11, None),
            &active,
            10
        ));
        assert!(!upload_is_live(
            &upload_record(upload_id, 10, None),
            &active,
            10
        ));
        assert!(upload_is_live(
            &upload_record(upload_id, 0, Some(job_id)),
            &active,
            10
        ));
        assert!(!upload_is_live(
            &upload_record(upload_id, 0, Some(JobId::from_bytes([6u8; 16]))),
            &active,
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
            key: key.clone(),
            modified_at: Some(UNIX_EPOCH),
        };

        assert!(is_orphaned(
            &entry,
            &HashSet::new(),
            &HashSet::new(),
            UNIX_EPOCH
        ));
        assert!(!is_orphaned(
            &entry,
            &HashSet::from([key]),
            &HashSet::new(),
            UNIX_EPOCH
        ));
        assert!(!is_orphaned(
            &entry,
            &HashSet::new(),
            &HashSet::from([JobId::from_bytes(namespace.to_bytes())]),
            UNIX_EPOCH
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
