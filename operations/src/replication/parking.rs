//! Parks replication jobs whose source key is locked and wakes them when that key unlocks. A
//! parked job keeps its record, attempts and error; only its due time changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::queue::{BlobJobRecord, BlobQueueError, schedule_blob_drain};
use crate::driver::DriverContext;
use crate::jobs::runtime::key_unlocked;
use crate::jobs::store::iter_prefix_page;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{COPY_WAIT_KEYSPACE, REPLICATION_JOB_KEYSPACE};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::StorageHandle;
use std::collections::BTreeSet;
use tracing::warn;
use ulid::Ulid;

/// Due time of a parked job: the drain never runs it and never times a wake for it.
pub(crate) const PARKED_DUE: u64 = u64::MAX;
const WAIT_PAGE: usize = 256;
/// Commit conflicts a wake retries before it reports an error.
const WAKE_ATTEMPTS: usize = 3;
/// Length of a key reference at the start of each wait row.
const REF_LEN: usize = 24;

fn wait_row(key: BucketKeyRef, job_key: &[u8]) -> Vec<u8> {
    [&key.key()[..], job_key].concat()
}

/// Parks `job` until one of `keys` unlocks: the job and its wait rows commit together, with no
/// attempt counted and no error recorded. A key unlocked meanwhile makes it due at once, and
/// that due time is returned.
pub(crate) async fn park_job(
    context: &DriverContext,
    job_key: Vec<u8>,
    job: &BlobJobRecord,
    keys: &[BucketKeyRef],
) -> Result<Option<u64>, BlobQueueError> {
    let storage = &context.storage_handle;
    let mut parked = job.clone();
    parked.due_at_ms = PARKED_DUE;
    let relationship = postcard::to_allocvec(&job.relationship_id)
        .map_err(aruna_core::errors::ConversionError::from)?;
    let mut writes: Vec<(String, Key, Value)> = vec![(
        REPLICATION_JOB_KEYSPACE.to_string(),
        job_key.clone().into(),
        parked.to_bytes()?.into(),
    )];
    for key in keys {
        writes.push((
            COPY_WAIT_KEYSPACE.to_string(),
            wait_row(*key, &job_key).into(),
            relationship.clone().into(),
        ));
    }
    let txn_id = start(storage).await?;
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    if let Err(error) = run(storage, write).await {
        abort(storage, txn_id).await;
        return Err(error);
    }
    commit(storage, txn_id).await?;
    // A key unlocked while the job parked wakes it here; no unlock call would.
    for key in keys {
        if key_unlocked(context, *key).await {
            let now_ms = aruna_core::time::unix_timestamp_millis();
            wake_job(storage, wait_row(*key, &job_key), now_ms).await?;
            return Ok(Some(now_ms));
        }
    }
    Ok(None)
}

/// Makes every job parked for `key` due now and schedules a drain. Returns how many woke.
pub async fn wake_parked(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<usize, BlobQueueError> {
    let storage = &context.storage_handle;
    let prefix: Key = key.key().into();
    let mut start_after = None;
    let mut woken = 0;
    loop {
        let page = iter_prefix_page(
            storage,
            COPY_WAIT_KEYSPACE,
            Some(prefix.clone()),
            start_after.clone(),
            WAIT_PAGE,
            None,
        )
        .await
        .map_err(|error| BlobQueueError::Storage(StorageError::ReadError(error)))?;
        let (rows, _) = page;
        for (row, _) in &rows {
            woken += usize::from(wake_job(storage, row.to_vec(), now_ms).await?);
        }
        match rows.last() {
            Some((row, _)) if rows.len() == WAIT_PAGE => start_after = Some(row.clone()),
            _ => break,
        }
    }
    if woken > 0
        && let Some(tasks) = context.task_handle.as_ref()
    {
        let event = tasks.send_effect(schedule_blob_drain()).await;
        if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
            warn!(message = %message, "Failed to schedule the drain of woken copy jobs");
        }
    }
    Ok(woken)
}

/// Copy jobs of a relationship that wait for a source key now.
pub async fn awaiting_jobs(
    context: &DriverContext,
    relationship_id: Ulid,
) -> Result<usize, BlobQueueError> {
    let storage = &context.storage_handle;
    let mut start_after = None;
    let mut jobs = BTreeSet::new();
    loop {
        let page = iter_prefix_page(
            storage,
            COPY_WAIT_KEYSPACE,
            None,
            start_after.clone(),
            WAIT_PAGE,
            None,
        )
        .await
        .map_err(|error| BlobQueueError::Storage(StorageError::ReadError(error)))?;
        let (rows, _) = page;
        for (row, value) in &rows {
            let owner: Option<Ulid> =
                postcard::from_bytes(value).map_err(aruna_core::errors::ConversionError::from)?;
            if owner == Some(relationship_id) && row.len() > REF_LEN {
                jobs.insert(row[REF_LEN..].to_vec());
            }
        }
        match rows.last() {
            Some((row, _)) if rows.len() == WAIT_PAGE => start_after = Some(row.clone()),
            _ => break,
        }
    }
    let mut parked = 0;
    for job_key in jobs {
        let job = read_job(storage, &job_key, None).await?;
        parked += usize::from(job.is_some_and(|job| job.due_at_ms == PARKED_DUE));
    }
    Ok(parked)
}

/// Makes the job of one wait row due at `now_ms` and drops the row, in one transaction. A job
/// that is gone or already due only loses the row. True when a parked job woke.
async fn wake_job(
    storage: &StorageHandle,
    wait: Vec<u8>,
    now_ms: u64,
) -> Result<bool, BlobQueueError> {
    let Some(job_key) = wait.get(REF_LEN..).map(<[u8]>::to_vec) else {
        return Ok(false);
    };
    let mut attempt = 0;
    loop {
        attempt += 1;
        match wake_once(storage, &wait, &job_key, now_ms).await {
            Err(BlobQueueError::Storage(StorageError::TransactionConflict))
                if attempt < WAKE_ATTEMPTS => {}
            result => return result,
        }
    }
}

async fn wake_once(
    storage: &StorageHandle,
    wait: &[u8],
    job_key: &[u8],
    now_ms: u64,
) -> Result<bool, BlobQueueError> {
    let txn_id = start(storage).await?;
    let result = async {
        let job = read_job(storage, job_key, Some(txn_id)).await?;
        let woke = job.as_ref().is_some_and(|job| job.due_at_ms == PARKED_DUE);
        if let Some(mut job) = job.filter(|_| woke) {
            job.due_at_ms = now_ms;
            let write = StorageEffect::Write {
                key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                key: job_key.to_vec().into(),
                value: job.to_bytes()?.into(),
                txn_id: Some(txn_id),
            };
            run(storage, write).await?;
        }
        let delete = StorageEffect::Delete {
            key_space: COPY_WAIT_KEYSPACE.to_string(),
            key: wait.to_vec().into(),
            txn_id: Some(txn_id),
        };
        run(storage, delete).await?;
        Ok(woke)
    }
    .await;
    match result {
        Ok(woke) => commit(storage, txn_id).await.map(|()| woke),
        Err(error) => {
            abort(storage, txn_id).await;
            Err(error)
        }
    }
}

async fn read_job(
    storage: &StorageHandle,
    job_key: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Option<BlobJobRecord>, BlobQueueError> {
    let read = StorageEffect::Read {
        key_space: REPLICATION_JOB_KEYSPACE.to_string(),
        key: job_key.to_vec().into(),
        txn_id,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| BlobJobRecord::from_bytes(&value))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn start(storage: &StorageHandle) -> Result<TxnId, BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn run(storage: &StorageHandle, effect: StorageEffect) -> Result<(), BlobQueueError> {
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        Event::Storage(_) => Ok(()),
        other => Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn commit(storage: &StorageHandle, txn_id: TxnId) -> Result<(), BlobQueueError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn abort(storage: &StorageHandle, txn_id: TxnId) {
    let effect = StorageEffect::AbortTransaction { txn_id };
    if let Event::Storage(StorageEvent::Error { error }) = storage.send_storage_effect(effect).await
    {
        warn!(%error, "Failed to abort a copy job wait transaction");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::protocol::ReplicationMode;
    use crate::replication::queue::{blob_job_key, next_blob_timer};
    use crate::replication::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
    use aruna_core::UserId;
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::events::BlobEvent;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::public_key_of;
    use tempfile::TempDir;

    fn context(storage: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    /// A node with a blob adapter, so key states are real.
    async fn keyed_context() -> (TempDir, DriverContext) {
        use aruna_core::structs::storage::blob::{Backend, BackendConfig};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let blob_root = format!("{root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        let net = aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage.clone())
            .await
            .unwrap();
        let config = BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_root,
            service_config: std::collections::HashMap::new(),
            timeouts: Default::default(),
        };
        let blob = aruna_blob::blob::BlobHandler::new(config, storage.clone(), net.clone())
            .await
            .unwrap();
        let mut context = context(storage);
        context.net_handle = Some(net);
        context.blob_handle = Some(blob);
        (dir, context)
    }

    async fn unlock(context: &DriverContext, key: BucketKeyRef) {
        let private = [9u8; 32];
        let public_key = public_key_of(&SecretBytes::new(private.to_vec())).unwrap();
        let blob = context.blob_handle.as_ref().unwrap();
        let prepare = BlobEffect::PrepareKey {
            key,
            public_key,
            private_key: SharedSecret::new(SecretBytes::new(private.to_vec())),
            duration: None,
            max: None,
        };
        let Event::Blob(BlobEvent::KeyPrepared { ticket }) = blob.send_blob_effect(prepare).await
        else {
            panic!("the key is prepared")
        };
        let activated = blob
            .send_blob_effect(BlobEffect::ActivateKey { ticket })
            .await;
        assert!(matches!(
            activated,
            Event::Blob(BlobEvent::KeyActivated { .. })
        ));
    }

    fn job(relationship_id: Ulid) -> BlobJobRecord {
        let realm_id = RealmId::from_bytes([2; 32]);
        let input = ReplicateScopeInput {
            bucket: "source".to_string(),
            target: ReplicateScopeTarget::Bucket,
            target_node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            replicate_delete_markers: false,
            mode: ReplicationMode::OnDemand,
        };
        BlobJobRecord::new_relationship(input, None, relationship_id, 1_000)
    }

    async fn store(storage: &StorageHandle, job: &BlobJobRecord) -> Vec<u8> {
        let key = blob_job_key(job).unwrap().to_vec();
        let write = StorageEffect::Write {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: key.clone().into(),
            value: job.to_bytes().unwrap().into(),
            txn_id: None,
        };
        run(storage, write).await.unwrap();
        key
    }

    #[tokio::test]
    async fn parked_job_wakes() {
        // A locked source parks the job without an attempt; the unlock makes it due again.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = context(storage.clone());
        let relationship = Ulid::from_parts(5, 5);
        let mut record = job(relationship);
        record.attempts = 2;
        let key = store(&storage, &record).await;
        let locked = BucketKeyRef::new(Ulid::from_parts(6, 6), 1);

        let due = park_job(&context, key.clone(), &record, &[locked])
            .await
            .unwrap();
        assert_eq!(due, None);
        let parked = read_job(&storage, &key, None).await.unwrap().unwrap();
        assert_eq!(parked.due_at_ms, PARKED_DUE);
        assert_eq!(parked.attempts, 2);
        assert_eq!(parked.last_error, None);
        assert_eq!(awaiting_jobs(&context, relationship).await.unwrap(), 1);
        // The drain neither runs a parked job nor times a wake for it.
        assert_eq!(next_blob_timer(&storage).await.unwrap(), None);

        // Another key's unlock leaves it parked.
        let other = BucketKeyRef::new(locked.bucket_id, 2);
        assert_eq!(wake_parked(&context, other, 5_000).await.unwrap(), 0);
        assert_eq!(wake_parked(&context, locked, 5_000).await.unwrap(), 1);
        let woken = read_job(&storage, &key, None).await.unwrap().unwrap();
        assert_eq!(woken.due_at_ms, 5_000);
        assert_eq!(woken.attempts, 2);
        assert_eq!(awaiting_jobs(&context, relationship).await.unwrap(), 0);
        assert_eq!(wake_parked(&context, locked, 6_000).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn unlock_before_park() {
        // The unlock ran before the wait row existed: the recheck after parking wakes the job.
        let (_dir, context) = keyed_context().await;
        let storage = context.storage_handle.clone();
        let record = job(Ulid::from_parts(7, 7));
        let key = store(&storage, &record).await;
        let unlocked = BucketKeyRef::new(Ulid::from_parts(8, 8), 1);
        unlock(&context, unlocked).await;
        assert_eq!(wake_parked(&context, unlocked, 1).await.unwrap(), 0);

        let due = park_job(&context, key.clone(), &record, &[unlocked])
            .await
            .unwrap();

        let woken = read_job(&storage, &key, None).await.unwrap().unwrap();
        assert_eq!(due, Some(woken.due_at_ms));
        assert_ne!(woken.due_at_ms, PARKED_DUE);
        assert_eq!(
            awaiting_jobs(&context, Ulid::from_parts(7, 7))
                .await
                .unwrap(),
            0
        );
    }
}
