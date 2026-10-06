//! Parks replication jobs whose source key is locked and wakes them when that key unlocks. A
//! parked job keeps its record, attempts and error; only its due time changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::queue::{BlobJobRecord, BlobQueueError, REPLICATION_POLL_AFTER, schedule_blob_drain};
use super::version_replication::ReplicateScopeTarget;
use crate::driver::DriverContext;
use crate::jobs::runtime::key_unlocked;
use crate::jobs::store::iter_prefix_page;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    BUCKET_KEY_KEYSPACE, COPY_WAIT_KEYSPACE, PENDING_LOCATION_KEYSPACE, REPLICATION_JOB_KEYSPACE,
};
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, KeyState,
};
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
    let txn_id = start(storage).await?;
    let current = match wait_current(storage, job, keys, Some(txn_id)).await {
        Ok(current) => current,
        Err(error) => {
            abort(storage, txn_id).await;
            return Err(error);
        }
    };
    let mut parked = job.clone();
    parked.due_at_ms = if current {
        PARKED_DUE
    } else {
        aruna_core::time::unix_timestamp_millis()
            .saturating_add(REPLICATION_POLL_AFTER.as_millis() as u64)
    };
    let relationship = postcard::to_allocvec(&job.relationship_id)
        .map_err(aruna_core::errors::ConversionError::from)?;
    let mut writes: Vec<(String, Key, Value)> = vec![(
        REPLICATION_JOB_KEYSPACE.to_string(),
        job_key.clone().into(),
        parked.to_bytes()?.into(),
    )];
    for key in keys.iter().filter(|_| current) {
        writes.push((
            COPY_WAIT_KEYSPACE.to_string(),
            wait_row(*key, &job_key).into(),
            relationship.clone().into(),
        ));
    }
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    if let Err(error) = run(storage, write).await {
        abort(storage, txn_id).await;
        return Err(error);
    }
    commit(storage, txn_id).await?;
    if !current {
        return Ok(Some(parked.due_at_ms));
    }
    let current = wait_current(storage, job, keys, None).await?;
    // A key unlocked while the job parked wakes it here; no unlock call would.
    for key in keys {
        if !current || key_unlocked(context, *key).await {
            let now_ms = aruna_core::time::unix_timestamp_millis();
            wake_job(storage, wait_row(*key, &job_key), now_ms).await?;
            return Ok(Some(now_ms));
        }
    }
    Ok(None)
}

async fn wait_current(
    storage: &StorageHandle,
    job: &BlobJobRecord,
    keys: &[BucketKeyRef],
    txn_id: Option<TxnId>,
) -> Result<bool, BlobQueueError> {
    let mut reads = vec![(
        BUCKET_ENCRYPTION_KEYSPACE.to_string(),
        job.input.bucket.as_bytes().into(),
    )];
    reads.extend(
        keys.iter()
            .map(|key| (BUCKET_KEY_KEYSPACE.to_string(), key.key().into())),
    );
    let Event::Storage(event) = storage
        .send_storage_effect(StorageEffect::BatchRead { reads, txn_id })
        .await
    else {
        return Err(BlobQueueError::UnexpectedEvent(
            "not a storage event".to_string(),
        ));
    };
    let mut values = match event {
        StorageEvent::BatchReadResult { values } if values.len() == keys.len() + 1 => {
            values.into_iter()
        }
        StorageEvent::Error { error } => return Err(error.into()),
        other => return Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
    };
    let settings = values.next().and_then(|(_, value)| value);
    let settings = BucketEncryption::from_row(settings.as_deref())?;
    for (key, (_, value)) in keys.iter().zip(values) {
        let record = value
            .map(|value| BucketKeyRecord::from_bytes(&value))
            .transpose()?;
        if !record.is_some_and(|record| {
            record.key == *key
                && match record.state {
                    KeyState::Active => settings.active_key() == Some(*key),
                    KeyState::Retiring => settings.bucket_id == Some(key.bucket_id),
                    KeyState::Retired => false,
                }
        }) {
            return Ok(false);
        }
    }
    let prefix = match &job.input.target {
        ReplicateScopeTarget::Version { key, version_id } => {
            VersionKey::new(&job.input.bucket, key, *version_id).to_bytes()?
        }
        ReplicateScopeTarget::Object { key } => VersionKey::object_prefix(&job.input.bucket, key)?,
        ReplicateScopeTarget::Bucket | ReplicateScopeTarget::Prefix(_) => {
            VersionKey::bucket_prefix(&job.input.bucket)?
        }
    };
    let mut start_after = None;
    let mut found = BTreeSet::new();
    loop {
        let (rows, next) = iter_prefix_page(
            storage,
            BLOB_VERSIONS_KEYSPACE,
            Some(prefix.clone().into()),
            start_after,
            WAIT_PAGE,
            txn_id,
        )
        .await
        .map_err(StorageError::ReadError)?;
        for (row, value) in rows {
            let version_key = VersionKey::from_bytes(&row)?;
            if matches!(&job.input.target, ReplicateScopeTarget::Prefix(prefix) if !version_key.key.starts_with(prefix))
            {
                continue;
            }
            let version = BlobVersion::from_bytes(&value)?;
            let (space, row) = match (version.location_key(), version.state.pending_archive()) {
                (Some(location), _) => (BLOB_LOCATIONS_KEYSPACE, location.to_bytes()),
                (_, Some(archive)) => (PENDING_LOCATION_KEYSPACE, archive.to_bytes()),
                _ => continue,
            };
            match storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: space.to_string(),
                    key: row.into(),
                    txn_id,
                })
                .await
            {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => {
                    if let Some(key) = BackendLocation::from_bytes(&value)?
                        .format
                        .bucket_key()
                        .or(settings.active_key())
                    {
                        found.insert(key);
                    }
                }
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
                Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
                other => return Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
            }
        }
        if keys.iter().all(|key| found.contains(key)) {
            return Ok(true);
        }
        match next {
            Some(next) => start_after = Some(next),
            None => return Ok(false),
        }
    }
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
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::encryption::{EncryptionMode, public_key_of};
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
    use std::time::SystemTime;
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

    async fn source(context: &DriverContext, key: BucketKeyRef) -> BackendLocation {
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(key.bucket_id),
            key_generation: key.generation,
            ..Default::default()
        };
        let record =
            BucketKeyRecord::new(key, Ulid::from_parts(1, key.generation.into()), [9; 32], 0);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "source".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::from_parts(2, key.generation.into()),
            format: StoredFormat::pithos(
                PithosLayout {
                    stored_size: 99,
                    metadata_digest: [key.generation as u8; 32],
                    storage_generation: key.generation,
                },
                key,
            ),
            created_by: job(Ulid::nil()).input.auth_context.user_id,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: std::collections::HashMap::from([("blake3".to_string(), vec![4; 32])]),
        };
        let version = BlobVersion::materialized(
            [4; 32],
            location.backend.clone(),
            location.format.encoding(),
            location.created_at,
            location.created_by,
            None,
        );
        for (space, row, value) in [
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                b"source".to_vec(),
                settings.to_bytes().unwrap(),
            ),
            (
                BUCKET_KEY_KEYSPACE,
                key.key().to_vec(),
                record.to_bytes().unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("source", "object", Ulid::from_parts(3, 3))
                    .to_bytes()
                    .unwrap(),
                version.to_bytes().unwrap(),
            ),
        ] {
            run(
                &context.storage_handle,
                StorageEffect::Write {
                    key_space: space.to_string(),
                    key: row.into(),
                    value: value.into(),
                    txn_id: None,
                },
            )
            .await
            .unwrap();
        }
        location
    }

    #[tokio::test]
    async fn rotated_copy_retries() {
        for state in [KeyState::Retiring, KeyState::Retired] {
            let (_dir, context) = keyed_context().await;
            let storage = &context.storage_handle;
            let old = BucketKeyRef::new(Ulid::from_parts(9, 9), 1);
            let location = source(&context, old).await;
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(captured),
                ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: location.location_key().unwrap().to_bytes().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("source location not read");
            };
            let captured = BackendLocation::from_bytes(&captured).unwrap();
            source(&context, BucketKeyRef::new(old.bucket_id, 2)).await;
            let mut old_record = BucketKeyRecord::new(old, Ulid::from_parts(1, 1), [9; 32], 0);
            old_record.state = state;
            run(
                storage,
                StorageEffect::Write {
                    key_space: BUCKET_KEY_KEYSPACE.to_string(),
                    key: old.key().into(),
                    value: old_record.to_bytes().unwrap().into(),
                    txn_id: None,
                },
            )
            .await
            .unwrap();
            let admission = context
                .blob_handle
                .as_ref()
                .unwrap()
                .send_blob_effect(BlobEffect::AdmitRead {
                    key: old,
                    archive: aruna_core::structs::storage::blob::ArchiveKey::of(&captured),
                })
                .await;
            assert!(matches!(
                admission,
                Event::Blob(BlobEvent::Error(aruna_core::errors::BlobError::BucketKey(
                    aruna_core::structs::storage::encryption::BucketKeyError::Locked(_)
                )))
            ));
            let mut record = job(Ulid::from_parts(10, 10));
            record.input.target = ReplicateScopeTarget::Version {
                key: "object".to_string(),
                version_id: Ulid::from_parts(3, 3),
            };
            record.attempts = 2;
            let row = store(storage, &record).await;
            let due = park_job(&context, row.clone(), &record, &[old])
                .await
                .unwrap();
            let stored = read_job(storage, &row, None).await.unwrap().unwrap();
            assert_eq!(due, Some(stored.due_at_ms));
            assert_ne!(stored.due_at_ms, PARKED_DUE);
            assert_eq!(stored.attempts, record.attempts);
            assert_eq!(stored.last_error, record.last_error);
            assert_eq!(
                awaiting_jobs(&context, Ulid::from_parts(10, 10))
                    .await
                    .unwrap(),
                0
            );
            assert!(next_blob_timer(storage).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn parking_fences_rotation() {
        let (_dir, storage) = crate::tests::s3::test_storage();
        let context = context(storage.clone());
        let old = BucketKeyRef::new(Ulid::from_parts(11, 11), 1);
        source(&context, old).await;
        let record = job(Ulid::from_parts(12, 12));
        let row = store(&storage, &record).await;
        let txn_id = start(&storage).await.unwrap();
        assert!(
            wait_current(&storage, &record, &[old], Some(txn_id))
                .await
                .unwrap()
        );
        source(&context, BucketKeyRef::new(old.bucket_id, 2)).await;
        let mut parked = record;
        parked.due_at_ms = PARKED_DUE;
        run(
            &storage,
            StorageEffect::Write {
                key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                key: row.clone().into(),
                value: parked.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            commit(&storage, txn_id).await,
            Err(BlobQueueError::Storage(StorageError::TransactionConflict))
        );
        assert_ne!(
            read_job(&storage, &row, None)
                .await
                .unwrap()
                .unwrap()
                .due_at_ms,
            PARKED_DUE
        );
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
        source(&context, locked).await;

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
    async fn plain_archive_waits() {
        let (_dir, storage) = crate::tests::s3::test_storage();
        let context = context(storage.clone());
        let key = BucketKeyRef::new(Ulid::from_parts(13, 13), 1);
        let mut location = source(&context, key).await;
        location.format = StoredFormat::default();
        let version = BlobVersion::materialized(
            [4; 32],
            location.backend.clone(),
            location.format.encoding(),
            location.created_at,
            location.created_by,
            None,
        );
        for (space, row, value) in [
            (
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("source", "object", Ulid::from_parts(3, 3))
                    .to_bytes()
                    .unwrap(),
                version.to_bytes().unwrap(),
            ),
        ] {
            run(
                &storage,
                StorageEffect::Write {
                    key_space: space.to_string(),
                    key: row.into(),
                    value: value.into(),
                    txn_id: None,
                },
            )
            .await
            .unwrap();
        }
        let record = job(Ulid::from_parts(14, 14));
        let row = store(&storage, &record).await;
        assert_eq!(
            park_job(&context, row.clone(), &record, &[key])
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            read_job(&storage, &row, None)
                .await
                .unwrap()
                .unwrap()
                .due_at_ms,
            PARKED_DUE
        );
    }

    #[tokio::test]
    async fn unlock_before_park() {
        // The unlock ran before the wait row existed: the recheck after parking wakes the job.
        let (_dir, context) = keyed_context().await;
        let storage = context.storage_handle.clone();
        let record = job(Ulid::from_parts(7, 7));
        let key = store(&storage, &record).await;
        let unlocked = BucketKeyRef::new(Ulid::from_parts(8, 8), 1);
        source(&context, unlocked).await;
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

    async fn retiring_wait(mode: EncryptionMode) {
        let (_dir, context) = keyed_context().await;
        let storage = &context.storage_handle;
        let old = BucketKeyRef::new(Ulid::from_parts(15, 15), 1);
        let location = source(&context, old).await;
        let settings = BucketEncryption {
            mode,
            bucket_id: Some(old.bucket_id),
            key_generation: 2,
            ..Default::default()
        };
        let mut retiring = BucketKeyRecord::new(old, Ulid::from_parts(1, 1), [9; 32], 0);
        retiring.state = KeyState::Retiring;
        run(
            storage,
            StorageEffect::BatchWrite {
                writes: vec![
                    (
                        BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                        b"source".to_vec().into(),
                        settings.to_bytes().unwrap().into(),
                    ),
                    (
                        BUCKET_KEY_KEYSPACE.to_string(),
                        old.key().into(),
                        retiring.to_bytes().unwrap().into(),
                    ),
                ],
                txn_id: None,
            },
        )
        .await
        .unwrap();
        let admission = context
            .blob_handle
            .as_ref()
            .unwrap()
            .send_blob_effect(BlobEffect::AdmitRead {
                key: old,
                archive: aruna_core::structs::storage::blob::ArchiveKey::of(&location),
            })
            .await;
        assert!(matches!(
            admission,
            Event::Blob(BlobEvent::Error(aruna_core::errors::BlobError::BucketKey(
                aruna_core::structs::storage::encryption::BucketKeyError::Locked(_)
            )))
        ));
        let mut record = job(Ulid::from_parts(16, 16));
        record.input.target = ReplicateScopeTarget::Version {
            key: "object".to_string(),
            version_id: Ulid::from_parts(3, 3),
        };
        record.attempts = 2;
        record.last_error = Some("previous copy failure".to_string());
        let row = store(storage, &record).await;
        assert_eq!(
            park_job(&context, row.clone(), &record, &[old])
                .await
                .unwrap(),
            None
        );
        let parked = read_job(storage, &row, None).await.unwrap().unwrap();
        assert_eq!(parked.due_at_ms, PARKED_DUE);
        assert_eq!(parked.attempts, record.attempts);
        assert_eq!(parked.last_error, record.last_error);
        let wait = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: COPY_WAIT_KEYSPACE.to_string(),
                key: wait_row(old, &row).into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            wait,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ));
        assert_eq!(
            awaiting_jobs(&context, record.relationship_id.unwrap())
                .await
                .unwrap(),
            1
        );
        assert_eq!(next_blob_timer(storage).await.unwrap(), None);
        assert_eq!(
            wake_parked(&context, BucketKeyRef::new(old.bucket_id, 2), 5_000)
                .await
                .unwrap(),
            0
        );
        unlock(&context, old).await;
        assert_eq!(wake_parked(&context, old, 5_000).await.unwrap(), 1);
        let woken = read_job(storage, &row, None).await.unwrap().unwrap();
        assert_eq!(woken.due_at_ms, 5_000);
        assert_eq!(woken.attempts, record.attempts);
        assert_eq!(woken.last_error, record.last_error);
        assert_eq!(
            awaiting_jobs(&context, record.relationship_id.unwrap())
                .await
                .unwrap(),
            0
        );
        assert!(next_blob_timer(storage).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rotation_key_waits() {
        retiring_wait(EncryptionMode::VaultLocked).await;
    }

    #[tokio::test]
    async fn decrypt_key_waits() {
        retiring_wait(EncryptionMode::Off).await;
    }

    #[tokio::test]
    async fn retirement_wakes_copy() {
        use aruna_core::keyspaces::{TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE};
        use aruna_core::structs::storage::encryption::SealPlan;
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::transition::{
            EncryptionTransition, TransitionKind, TransitionState, TransitionTarget,
        };

        let (_dir, context) = keyed_context().await;
        let storage = &context.storage_handle;
        let old = BucketKeyRef::new(Ulid::from_parts(17, 17), 1);
        source(&context, old).await;
        unlock(&context, old).await;
        assert_eq!(wake_parked(&context, old, 1).await.unwrap(), 0);
        let record = job(Ulid::from_parts(18, 18));
        let row = store(storage, &record).await;
        let (proxy, receivers) = StorageHandle::new();
        let (read_tx, read_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let direct = storage.clone();
        let actor = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut read_tx = Some(read_tx);
            while let Ok((effect, response, _span, _queued, _in_flight)) =
                receivers.foreground.recv()
            {
                let gated = read_tx.is_some()
                    && matches!(&effect, StorageEffect::Read { key_space, txn_id: None, .. }
                        if key_space == BLOB_LOCATIONS_KEYSPACE);
                let Event::Storage(event) = runtime.block_on(direct.send_storage_effect(effect))
                else {
                    panic!("expected a storage event");
                };
                if gated {
                    assert!(matches!(
                        &event,
                        StorageEvent::ReadResult { value: Some(_), .. }
                    ));
                    read_tx.take().unwrap().send(()).unwrap();
                    release_rx.recv().unwrap();
                }
                assert!(response.send(event));
            }
        });
        let mut parking = context.clone();
        parking.storage_handle = proxy;
        let parked_row = row.clone();
        let parked_record = record.clone();
        let task =
            tokio::spawn(
                async move { park_job(&parking, parked_row, &parked_record, &[old]).await },
            );
        read_rx.await.unwrap();
        assert_eq!(
            read_job(storage, &row, None)
                .await
                .unwrap()
                .unwrap()
                .due_at_ms,
            PARKED_DUE
        );

        // The source read saw generation 1; retirement finishes before the registry recheck.
        let new = BucketKeyRef::new(old.bucket_id, 2);
        source(&context, new).await;
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: Some(SealPlan {
                key: new,
                public_key: [9; 32],
                cipher: Default::default(),
                block_keys: Default::default(),
                storage_generation: 2,
            }),
        };
        let mut transition =
            EncryptionTransition::new(TransitionKind::Rotate, Some(old), target, 2, 1);
        transition.state = TransitionState::Cleanup;
        run(
            storage,
            StorageEffect::BatchWrite {
                writes: vec![
                    (
                        TRANSITION_KEYSPACE.to_string(),
                        b"source".to_vec().into(),
                        transition.to_bytes().unwrap().into(),
                    ),
                    (
                        TRANSITION_QUEUE_KEYSPACE.to_string(),
                        b"source".to_vec().into(),
                        Vec::new().into(),
                    ),
                ],
                txn_id: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            crate::blob::migration::queue::process_transitions(&context)
                .await
                .unwrap(),
            None
        );
        assert!(!key_unlocked(&context, old).await);
        release_tx.send(()).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), None);
        actor.join().unwrap();
        let woken = read_job(storage, &row, None).await.unwrap().unwrap();
        assert_ne!(woken.due_at_ms, PARKED_DUE);
        assert_eq!(woken.attempts, record.attempts);
        assert_eq!(woken.last_error, record.last_error);
        assert_eq!(
            awaiting_jobs(&context, record.relationship_id.unwrap())
                .await
                .unwrap(),
            0
        );
        assert!(next_blob_timer(storage).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn retirement_wake_recovers() {
        use aruna_core::keyspaces::{TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE};
        use aruna_core::structs::storage::encryption::SealPlan;
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::transition::{
            EncryptionTransition, TransitionKind, TransitionState, TransitionTarget,
        };

        for failed_page in [1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let context =
                context(aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap());
            let storage = &context.storage_handle;
            let old = BucketKeyRef::new(Ulid::from_parts(19, 19), 1);
            source(&context, old).await;
            let mut jobs = Vec::new();
            let mut writes = Vec::new();
            for index in 0..=WAIT_PAGE {
                let mut record = job(Ulid::from_parts(20, index as u128));
                record.due_at_ms = PARKED_DUE;
                record.attempts = 2;
                record.last_error = Some("previous copy failure".to_string());
                let row = blob_job_key(&record).unwrap().to_vec();
                writes.push((
                    REPLICATION_JOB_KEYSPACE.to_string(),
                    row.clone().into(),
                    record.to_bytes().unwrap().into(),
                ));
                writes.push((
                    COPY_WAIT_KEYSPACE.to_string(),
                    wait_row(old, &row).into(),
                    postcard::to_allocvec(&record.relationship_id)
                        .unwrap()
                        .into(),
                ));
                jobs.push((row, record));
            }
            let new = BucketKeyRef::new(old.bucket_id, 2);
            source(&context, new).await;
            let target = TransitionTarget {
                compression: Compression::Off,
                plan: Some(SealPlan {
                    key: new,
                    public_key: [9; 32],
                    cipher: Default::default(),
                    block_keys: Default::default(),
                    storage_generation: 2,
                }),
            };
            let mut transition =
                EncryptionTransition::new(TransitionKind::Rotate, Some(old), target, 2, 1);
            transition.state = TransitionState::Cleanup;
            writes.extend([
                (
                    TRANSITION_KEYSPACE.to_string(),
                    b"source".to_vec().into(),
                    transition.to_bytes().unwrap().into(),
                ),
                (
                    TRANSITION_QUEUE_KEYSPACE.to_string(),
                    b"source".to_vec().into(),
                    Vec::new().into(),
                ),
            ]);
            run(
                storage,
                StorageEffect::BatchWrite {
                    writes,
                    txn_id: None,
                },
            )
            .await
            .unwrap();

            let (proxy, receivers) = StorageHandle::new();
            let direct = storage.clone();
            let actor = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let mut page = 0;
                while let Ok((effect, response, _span, _queued, _in_flight)) =
                    receivers.foreground.recv()
                {
                    let wake_scan = matches!(&effect, StorageEffect::Iter { key_space, .. }
                        if key_space == COPY_WAIT_KEYSPACE);
                    if wake_scan {
                        page += 1;
                    }
                    let event = if wake_scan && page == failed_page {
                        StorageEvent::Error {
                            error: StorageError::ReadError("injected retirement wake".to_string()),
                        }
                    } else {
                        let Event::Storage(event) =
                            runtime.block_on(direct.send_storage_effect(effect))
                        else {
                            panic!("expected a storage event")
                        };
                        event
                    };
                    assert!(response.send(event));
                }
            });
            let mut interrupted = context.clone();
            interrupted.storage_handle = proxy;
            let error = crate::blob::migration::queue::process_transitions(&interrupted)
                .await
                .unwrap_err();
            assert!(error.contains("injected retirement wake"));
            drop(interrupted);
            actor.join().unwrap();

            let mut parked = 0;
            for (row, original) in &jobs {
                let record = read_job(storage, row, None).await.unwrap().unwrap();
                parked += usize::from(record.due_at_ms == PARKED_DUE);
                assert_eq!(record.attempts, original.attempts);
                assert_eq!(record.last_error, original.last_error);
            }
            assert_eq!(parked, WAIT_PAGE + 1 - (failed_page - 1) * WAIT_PAGE);
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: BUCKET_KEY_KEYSPACE.to_string(),
                    key: old.key().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("source key missing")
            };
            assert_eq!(
                BucketKeyRecord::from_bytes(&value).unwrap().state,
                KeyState::Retired
            );
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("transition missing")
            };
            let pending = EncryptionTransition::from_bytes(&value).unwrap();
            assert_eq!(pending.state, TransitionState::Finished);
            assert_eq!(pending.finished_at_ms, None);
            let queued = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_QUEUE_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                queued,
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
            ));
            assert!(!key_unlocked(&context, old).await);

            assert_eq!(
                crate::blob::migration::queue::process_transitions(&context)
                    .await
                    .unwrap(),
                None
            );
            for (row, original) in &jobs {
                let record = read_job(storage, row, None).await.unwrap().unwrap();
                assert_ne!(record.due_at_ms, PARKED_DUE);
                assert_eq!(record.attempts, original.attempts);
                assert_eq!(record.last_error, original.last_error);
            }
            let (waits, _) = iter_prefix_page(
                storage,
                COPY_WAIT_KEYSPACE,
                Some(old.key().into()),
                None,
                WAIT_PAGE,
                None,
            )
            .await
            .unwrap();
            assert!(waits.is_empty());
            let queued = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_QUEUE_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                queued,
                Event::Storage(StorageEvent::ReadResult { value: None, .. })
            ));
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("transition missing")
            };
            assert!(
                EncryptionTransition::from_bytes(&value)
                    .unwrap()
                    .finished_at_ms
                    .is_some()
            );
            assert!(next_blob_timer(storage).await.unwrap().is_some());
        }
    }
}
