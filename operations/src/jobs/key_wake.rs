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
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    PATHS_INDEX_KEYSPACE, PENDING_LOCATION_KEYSPACE,
};
use aruna_core::structs::execution::job::{CapturedInput, JobError, JobRecord, KeyWait};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BlobVersionState, HashIndex, VersionKey,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::time::unix_timestamp_millis;
use tracing::warn;
use ulid::Ulid;

/// Aliases of one content checked on this node; a content beyond them stays unknown.
pub(crate) const ALIAS_PAGE: usize = 64;

#[derive(Debug, Error)]
pub enum KeyWakeError {
    #[error(transparent)]
    Jobs(#[from] JobMutationError),
    #[error(transparent)]
    Promote(#[from] PromoteError),
}

/// Wakes parked jobs of `key`, queues remote wakes, then promotes its pending archives. Call after unlock and after
/// node-managed keys open at startup. Answers the woken jobs and the promoted archives.
pub async fn wake_unlocked(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
    origin: (RealmId, NodeId),
    limits: &RoCrateLimits,
) -> Result<(usize, usize), KeyWakeError> {
    let woken = wake_key_waits(&context.storage_handle, key, now_ms).await;
    // Remote waiters get owed wakes before promotion, so a corrupt archive never holds them back.
    let queued = super::remote_key::queue_wakes(context, key).await;
    super::remote_key::arm_delivery(context).await;
    let promoted = promote_unlocked(context, key, origin, limits).await;
    queued.map_err(|error| KeyWakeError::Jobs(JobMutationError::Storage(error)))?;
    Ok((woken?, promoted?))
}

/// Local inputs whose exact source version needs a key that is locked here. Remote inputs are
/// left to their source node; a version without a local alias is left to staging.
pub(crate) async fn locked_inputs(
    context: &DriverContext,
    inputs: &[CapturedInput],
    node_id: NodeId,
) -> Result<Vec<KeyWait>, String> {
    let mut waits = Vec::new();
    for input in inputs
        .iter()
        .filter(|input| input.source_node_id == node_id)
    {
        let alias = find_alias(context, &input.blake3, input.version_id, node_id).await?;
        if let Some(alias) = alias
            && let Some(wait) = alias_wait(context, &alias).await?
            && !waits.contains(&wait)
        {
            waits.push(wait);
        }
    }
    Ok(waits)
}

/// The alias of exactly `version_id` among a content's aliases on this node, paging through
/// every alias page until it is found or the aliases end.
pub(crate) async fn find_alias(
    context: &DriverContext,
    content: &[u8; 32],
    version_id: Ulid,
    node_id: NodeId,
) -> Result<Option<HashIndex>, String> {
    let mut start_after = None;
    loop {
        let (aliases, next) = alias_page(context, content, node_id, start_after).await?;
        if let Some(alias) = aliases
            .into_iter()
            .find(|alias| alias.version_id == version_id)
        {
            return Ok(Some(alias));
        }
        match next {
            Some(next) => start_after = Some(next),
            None => return Ok(None),
        }
    }
}

/// One page of a content's aliases on this node, and the cursor of the next page.
pub(crate) async fn alias_page(
    context: &DriverContext,
    content: &[u8; 32],
    node_id: NodeId,
    start_after: Option<aruna_core::types::Key>,
) -> Result<(Vec<HashIndex>, Option<aruna_core::types::Key>), String> {
    let prefix = HashIndex::hash_prefix(content).map_err(|error| error.to_string())?;
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
        PATHS_INDEX_KEYSPACE,
        Some(prefix.into()),
        start_after,
        ALIAS_PAGE,
        None,
    )
    .await?;
    let next = (rows.len() == ALIAS_PAGE)
        .then(|| rows.last().map(|(row, _)| row.clone()))
        .flatten();
    let aliases = rows
        .iter()
        .filter_map(|(row, _)| HashIndex::from_bytes(row).ok())
        .filter(|alias| alias.node_id == node_id)
        .collect();
    Ok((aliases, next))
}

/// The locked key a read of the version an alias names would need.
pub(crate) async fn alias_wait(
    context: &DriverContext,
    alias: &HashIndex,
) -> Result<Option<KeyWait>, String> {
    let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
    version_wait(context, (alias.node_id, alias.group_id), &version).await
}

/// The locked key a read of exactly this version would need, following read admission: a
/// sealed copy needs its own key; a plain copy or a reference of an encrypting bucket needs
/// the bucket's active key.
pub(crate) async fn version_wait(
    context: &DriverContext,
    (node_id, group_id): (NodeId, Ulid),
    version: &VersionKey,
) -> Result<Option<KeyWait>, String> {
    let storage = &context.storage_handle;
    let row_key = version.to_bytes().map_err(|error| error.to_string())?;
    let Some(row) = read_row(storage, BLOB_VERSIONS_KEYSPACE, row_key).await? else {
        return Ok(None);
    };
    let state = BlobVersion::from_bytes(&row)
        .map_err(|error| error.to_string())?
        .state;
    let location = match (&state.location_key(), state.pending_archive(), &state) {
        (Some(key), _, _) => read_row(storage, BLOB_LOCATIONS_KEYSPACE, key.to_bytes()).await?,
        (None, Some(archive), _) => {
            read_row(storage, PENDING_LOCATION_KEYSPACE, archive.to_bytes()).await?
        }
        (None, None, BlobVersionState::Reference { .. }) => None,
        (None, None, _) => return Ok(None),
    };
    let sealed = match location {
        Some(location) => BackendLocation::from_bytes(&location)
            .map_err(|error| error.to_string())?
            .format
            .bucket_key(),
        None => None,
    };
    let key = match sealed {
        Some(key) => Some(key),
        None => {
            let bucket = version.bucket.as_bytes().to_vec();
            let settings = read_row(storage, BUCKET_ENCRYPTION_KEYSPACE, bucket).await?;
            BucketEncryption::from_row(settings.as_deref())
                .map_err(|error| error.to_string())?
                .active_key()
        }
    };
    let Some(key) = key else {
        return Ok(None);
    };
    if key_unlocked(context, key).await {
        return Ok(None);
    }
    Ok(Some(KeyWait {
        node_id,
        bucket: version.bucket.clone(),
        group_id: Some(group_id),
        key,
    }))
}

pub(crate) async fn read_row(
    storage: &aruna_storage::StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, String> {
    let effect = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|value| value.to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, park_job, read_job_record};
    use aruna_core::UserId;
    use aruna_core::structs::execution::job::{JobId, JobPayload, JobState};
    use byteview::ByteView;

    #[tokio::test]
    async fn corrupt_archive_still_wakes() {
        use crate::jobs::store::{owed_wakes, register_remote_wait};
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::PENDING_LOCATION_KEYSPACE;
        use aruna_core::structs::identity::auth::AuthContext;

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
        let waiter = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        };
        let job_id = JobId::from_bytes([5; 16]);
        register_remote_wait(storage, key, waiter, job_id, &auth)
            .await
            .unwrap();
        // A pending row that cannot be decoded makes promotion fail.
        let corrupt = StorageEffect::Write {
            key_space: PENDING_LOCATION_KEYSPACE.to_string(),
            key: ByteView::from(b"archive".to_vec()),
            value: ByteView::from(b"corrupt".to_vec()),
            txn_id: None,
        };
        storage.send_storage_effect(corrupt).await;

        let origin = (RealmId([1; 32]), node);
        let limits = RoCrateLimits::default();
        let result = wake_unlocked(&context, key, 4_000, origin, &limits).await;

        assert!(matches!(result, Err(KeyWakeError::Promote(_))));
        let (owed, _) = owed_wakes(storage, None).await.unwrap();
        assert_eq!(owed.len(), 1);
        assert_eq!((owed[0].waiter, owed[0].job_id), (waiter, job_id));
    }

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
    async fn sealed_node(sealed: bool) -> (tempfile::TempDir, DriverContext, NodeId, BucketKeyRef) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::structs::storage::blob::{BackendRef, BlobLocationKey};
        use aruna_core::structs::storage::encryption::EncryptionMode;
        use aruna_core::structs::storage::format::{EncodingClass, PithosLayout, StoredFormat};
        use byteview::ByteView;
        use std::collections::HashMap;

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
            // An unconverted copy is plain, yet its encrypting bucket still gates it.
            format: match sealed {
                true => StoredFormat::pithos(layout, key),
                false => StoredFormat::default(),
            },
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: HashMap::new(),
        };
        let row = BlobLocationKey::new([9; 32], EncodingClass::Raw, BackendRef::node_default());
        let version_id = Ulid::from_bytes([6; 16]);
        let version = BlobVersion::materialized(
            [9; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let version_key = VersionKey::new("sealed", "data.bin", version_id);
        let group_id = Ulid::from_bytes([4; 16]);
        let realm_id = RealmId([1; 32]);
        let alias = HashIndex::new(
            [9; 32], version_id, realm_id, group_id, node, "sealed", "data.bin",
        );
        let writes = vec![
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                ByteView::from(version_key.to_bytes().unwrap()),
                ByteView::from(version.to_bytes().unwrap()),
            ),
            (
                PATHS_INDEX_KEYSPACE.to_string(),
                ByteView::from(alias.to_bytes().unwrap()),
                ByteView::from(Vec::new()),
            ),
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
    async fn alias_beyond_page() {
        use aruna_core::effects::StorageEffect;

        let (_dir, context, node, key) = sealed_node(true).await;
        // Seventy older versions of the same content sort before the captured one.
        for version in 1..=70u128 {
            let decoy = HashIndex::new(
                [9; 32],
                Ulid::from(version),
                RealmId([1; 32]),
                Ulid::from_bytes([4; 16]),
                node,
                "sealed",
                format!("old-{version}"),
            );
            let effect = StorageEffect::Write {
                key_space: PATHS_INDEX_KEYSPACE.to_string(),
                key: decoy.to_bytes().unwrap().into(),
                value: Vec::new().into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(effect).await;
        }
        let input = CapturedInput {
            destination_key: "in".to_string(),
            source_node_id: node,
            version_id: Ulid::from_bytes([6; 16]),
            blake3: [9; 32],
            bytes: 100,
            policies: Vec::new(),
        };

        let waits = locked_inputs(&context, &[input], node).await.unwrap();

        assert_eq!(
            waits.iter().map(|wait| wait.key).collect::<Vec<_>>(),
            vec![key]
        );
    }

    #[tokio::test]
    async fn locked_input_waits() {
        let (_dir, context, node, key) = sealed_node(true).await;
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
                group_id: Some(Ulid::from_bytes([4; 16])),
                key,
            }]
        );
    }

    #[tokio::test]
    async fn lock_after_precheck() {
        use crate::jobs::store::read_key_waits;
        use aruna_core::structs::execution::job::JobError;

        // The input copy is not converted yet; the locked bucket key still parks the job.
        let (_dir, context, node, key) = sealed_node(false).await;
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
