//! Runs the work that waits for a bucket key once that key is installed on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use thiserror::Error;

use crate::blob::promote::{PromoteError, promote_unlocked};
use crate::driver::DriverContext;
use crate::jobs::runtime::key_unlocked;
use crate::jobs::store::{
    AwaitOutcome, JobMutationError, iter_prefix_page, park_job, satisfy_key_wait, wake_key_waits,
};
use crate::jobs::workflow::workspace::LOCKED_INPUT;
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE};
use aruna_core::structs::execution::job::{CapturedInput, JobError, JobRecord, KeyWait};
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::time::unix_timestamp_millis;
use byteview::ByteView;
use std::collections::{BTreeSet, HashMap};
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum KeyWakeError {
    #[error(transparent)]
    Jobs(#[from] JobMutationError),
    #[error(transparent)]
    Promote(#[from] PromoteError),
}

/// Wakes parked jobs of `key`, then promotes its pending archives. Call after unlock and after
/// node-managed keys open at startup. Answers the woken jobs and the promoted archives.
pub async fn wake_unlocked(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
    origin: (RealmId, NodeId),
    limits: &RoCrateLimits,
) -> Result<(usize, usize), KeyWakeError> {
    let woken = wake_key_waits(&context.storage_handle, key, now_ms).await?;
    let promoted = promote_unlocked(context, key, origin, limits).await?;
    // Remote waiters get owed wakes; the delivery timer retries them until acknowledged.
    super::remote_key::queue_wakes(context, key)
        .await
        .map_err(|error| KeyWakeError::Jobs(JobMutationError::Storage(error)))?;
    super::remote_key::arm_delivery(context).await;
    Ok((woken, promoted))
}

/// Local inputs whose every local copy is sealed with a key generation that is locked here.
/// Remote inputs are left to the source node. Missing copies are left to staging.
pub(crate) async fn locked_inputs(
    context: &DriverContext,
    inputs: &[CapturedInput],
    node_id: NodeId,
) -> Result<Vec<KeyWait>, String> {
    let local = inputs
        .iter()
        .filter(|input| input.source_node_id == node_id)
        .map(|input| input.blake3);
    locked_contents(context, local, node_id).await
}

/// Locked keys of contents stored here: a content waits only when every local copy is sealed
/// with a key generation that is locked here. Contents without a copy are left out.
pub(crate) async fn locked_contents(
    context: &DriverContext,
    contents: impl Iterator<Item = [u8; 32]>,
    node_id: NodeId,
) -> Result<Vec<KeyWait>, String> {
    let storage = &context.storage_handle;
    let mut locked = BTreeSet::new();
    for content in contents {
        let prefix = ByteView::from(content.to_vec());
        let (rows, _) = iter_prefix_page(
            storage,
            BLOB_LOCATIONS_KEYSPACE,
            Some(prefix),
            None,
            16,
            None,
        )
        .await?;
        let mut keys = BTreeSet::new();
        let mut readable = rows.is_empty();
        for (_, value) in &rows {
            let location = BackendLocation::from_bytes(value).map_err(|error| error.to_string())?;
            match location.format.bucket_key() {
                Some(key) if !key_unlocked(context, key).await => {
                    keys.insert(key);
                }
                _ => readable = true,
            }
        }
        if !readable {
            locked.extend(keys);
        }
    }
    if locked.is_empty() {
        return Ok(Vec::new());
    }
    let names = bucket_names(storage).await?;
    Ok(locked
        .into_iter()
        .map(|key| KeyWait {
            node_id,
            bucket: names.get(&key.bucket_id).cloned().unwrap_or_default(),
            group_id: None,
            key,
        })
        .collect())
}

/// Parks a job whose preparation hit a key locked after the precheck, before any attempt
/// intent exists. True when the job no longer runs here; any other error is left to the caller.
pub(crate) async fn park_on_lock(
    context: &DriverContext,
    record: &JobRecord,
    token: Ulid,
    node_id: NodeId,
    error: &JobError,
) -> bool {
    error.message.starts_with(LOCKED_INPUT)
        && Box::pin(park_locked(context, record, token, node_id)).await
}

/// Parks a claimed job whose local inputs need locked keys. True when the job no longer runs
/// here: it is parked, or its claim is gone. A cancel request or a failed check continues.
pub(crate) async fn park_locked(
    context: &DriverContext,
    record: &JobRecord,
    token: Ulid,
    node_id: NodeId,
) -> bool {
    let job_id = record.job_id;
    let waits = match locked_inputs(context, &record.captured_inputs, node_id).await {
        Ok(mut waits) => {
            waits.extend(super::remote_key::remote_waits(context, record, node_id).await);
            if waits.is_empty() {
                return false;
            }
            waits
        }
        Err(error) => {
            warn!(job_id = %job_id, %error, "Input key check failed; staging decides");
            return false;
        }
    };
    let keys: Vec<_> = waits.iter().map(|wait| wait.key).collect();
    let storage = &context.storage_handle;
    match park_job(storage, job_id, token, unix_timestamp_millis(), waits).await {
        Ok(AwaitOutcome::Parked(_)) => {}
        Ok(AwaitOutcome::Skipped) => return false,
        Err(JobMutationError::TokenMismatch) => return true,
        Err(error) => {
            warn!(job_id = %job_id, %error, "Failed to park job for a bucket key");
            return false;
        }
    }
    // A key unlocked while the job parked wakes it here; no unlock call would.
    for key in keys {
        if key_unlocked(context, key).await
            && let Err(error) =
                satisfy_key_wait(storage, job_id, key, unix_timestamp_millis()).await
        {
            warn!(job_id = %job_id, %error, "Failed to wake parked job");
        }
    }
    true
}

/// Bucket names by the stable id their keys bind to.
async fn bucket_names(
    storage: &aruna_storage::StorageHandle,
) -> Result<HashMap<Ulid, String>, String> {
    let mut names = HashMap::new();
    let mut start_after = None;
    loop {
        let (rows, next) = iter_prefix_page(
            storage,
            BUCKET_ENCRYPTION_KEYSPACE,
            None,
            start_after,
            256,
            None,
        )
        .await?;
        for (key, value) in &rows {
            let settings =
                BucketEncryption::from_bytes(value).map_err(|error| error.to_string())?;
            if let Some(bucket_id) = settings.bucket_id {
                names.insert(bucket_id, String::from_utf8_lossy(key).into_owned());
            }
        }
        match next {
            Some(next) if !rows.is_empty() => start_after = Some(next),
            _ => return Ok(names),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, park_job, read_job_record};
    use aruna_core::UserId;
    use aruna_core::structs::execution::job::{JobId, JobPayload, JobState};

    #[tokio::test]
    async fn wake_queues_parked() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let storage = &context.storage_handle;
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let record = JobRecord::new(job_id, payload, owner, node, 1_000, 1_000, None);
        insert_job(storage, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) = claim_job(storage, job_id, node, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let token = claimed.claim.unwrap().claim_token;
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let wait = KeyWait {
            node_id: node,
            bucket: "b".to_string(),
            group_id: None,
            key,
        };
        park_job(storage, job_id, token, 3_000, vec![wait])
            .await
            .unwrap();

        let origin = (RealmId([1; 32]), node);
        let limits = RoCrateLimits::default();
        let woken = wake_unlocked(&context, key, 4_000, origin, &limits)
            .await
            .unwrap();

        assert_eq!(woken, (1, 0));
        let record = read_job_record(storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Queued);
    }

    /// A node holding one copy of content `[9; 32]`, sealed with a locked key of bucket "sealed".
    async fn sealed_node() -> (tempfile::TempDir, DriverContext, NodeId, BucketKeyRef) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::structs::storage::blob::{BackendRef, BlobLocationKey};
        use aruna_core::structs::storage::encryption::EncryptionMode;
        use aruna_core::structs::storage::format::{EncodingClass, PithosLayout, StoredFormat};

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(key.bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let layout = PithosLayout {
            stored_size: 130,
            metadata_digest: [4; 32],
        };
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            format: StoredFormat::pithos(layout, key),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: HashMap::new(),
        };
        let row = BlobLocationKey::new([9; 32], EncodingClass::Raw, BackendRef::node_default());
        let writes = vec![
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                ByteView::from(b"sealed".to_vec()),
                ByteView::from(settings.to_bytes().unwrap()),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                ByteView::from(row.to_bytes()),
                ByteView::from(location.to_bytes().unwrap()),
            ),
        ];
        let effect = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        (dir, context, node, key)
    }

    #[tokio::test]
    async fn locked_input_waits() {
        let (_dir, context, node, key) = sealed_node().await;
        let input = |blake3: [u8; 32], source_node_id| CapturedInput {
            destination_key: "in".to_string(),
            source_node_id,
            version_id: Ulid::from_bytes([6; 16]),
            blake3,
            bytes: 100,
            policies: Vec::new(),
        };
        let remote = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let inputs = [
            input([9; 32], node),
            input([9; 32], remote),
            input([7; 32], node),
        ];

        let waits = locked_inputs(&context, &inputs, node).await.unwrap();

        assert_eq!(
            waits,
            vec![KeyWait {
                node_id: node,
                bucket: "sealed".to_string(),
                group_id: None,
                key,
            }]
        );
    }

    #[tokio::test]
    async fn lock_after_precheck() {
        use crate::jobs::store::read_key_waits;
        use aruna_core::structs::execution::job::JobError;

        let (_dir, context, node, key) = sealed_node().await;
        let storage = &context.storage_handle;
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let mut record = JobRecord::new(job_id, payload, owner, node, 1_000, 1_000, None);
        record.captured_inputs.push(CapturedInput {
            destination_key: "in".to_string(),
            source_node_id: node,
            version_id: Ulid::from_bytes([6; 16]),
            blake3: [9; 32],
            bytes: 100,
            policies: Vec::new(),
        });
        insert_job(storage, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) = claim_job(storage, job_id, node, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let token = claimed.claim.as_ref().unwrap().claim_token;

        // Any other preparation failure stays with the caller.
        let other = JobError::retryable("input read failed: offline");
        assert!(!park_on_lock(&context, &claimed, token, node, &other).await);
        // The precheck passed, then the key locked before the input was admitted.
        let locked = JobError::retryable(format!("{LOCKED_INPUT}: bucket is locked"));
        assert!(park_on_lock(&context, &claimed, token, node, &locked).await);

        let parked = read_job_record(storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parked.state, JobState::AwaitingKey);
        assert_eq!(parked.attempts, 0);
        let waits = read_key_waits(storage, job_id).await.unwrap();
        assert_eq!(
            waits.iter().map(|wait| wait.key).collect::<Vec<_>>(),
            vec![key]
        );
    }
}
