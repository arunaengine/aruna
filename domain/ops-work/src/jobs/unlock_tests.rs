//! Checks that a key install or unlock wakes the parked jobs and rewrites that waited for it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::compute::SecretBytes;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::input::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{UnlockStatus, public_key_of};
use aruna_core::user::id::UserId;
use aruna_ops_data::s3::bucket::key::install::{InstallInput, InstallKeyOperation};
use aruna_runner::driver::{DriverContext, drive};
use ulid::Ulid;

use super::WakeUnlockedOperation;

/// Activates a key as the routes do, then resumes the work that waited for it.
async fn activate_and_wake<O>(
    context: &DriverContext,
    operation: O,
    origin: (RealmId, NodeId),
) -> Result<UnlockStatus, O::Error>
where
    O: Operation<Output = UnlockStatus>,
{
    let status = drive(operation, context).await?;
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let limits = RoCrateLimits::default();
    let wake = WakeUnlockedOperation::new(status.key, now_ms, origin, &limits);
    drive(wake, context).await.unwrap();
    Ok(status)
}
/// The realm and group authorization documents in which `admin` holds the group admin role.
fn authority_documents(
    info: &aruna_core::structs::storage::blob::BucketInfo,
    admin: UserId,
) -> [Vec<u8>; 2] {
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::structs::placement::policy::document::group_admin_path;
    let realm_id = info.created_by.realm_id;
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: info.created_by,
        realm_id,
    };
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: Default::default(),
        operation_restrictions: Default::default(),
    };
    let role = Role {
        role_id: Ulid::from_bytes([1; 16]),
        name: "keepers".to_string(),
        permissions: [(group_admin_path(realm_id, info.group_id), Permission::WRITE)].into(),
        assigned_users: [admin].into(),
    };
    let group = GroupAuthorizationDocument {
        group_id: info.group_id,
        roles: [(role.role_id, role)].into(),
        policies: Vec::new(),
    };
    [
        realm.to_bytes(&actor).unwrap(),
        group.to_bytes(&actor).unwrap(),
    ]
}

async fn enabled_context() -> (
    tempfile::TempDir,
    DriverContext,
    aruna_ops_data::s3::bucket::encryption::EnableResult,
) {
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::node::vault::NodeVaultKey;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
    use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys, EncryptionMode};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::holders::KeyLookup;
    use aruna_core::user::id::UserId;
    use aruna_ops_data::s3::bucket::create::CreateBucketOperation;
    use aruna_ops_data::s3::bucket::encryption::{EnableEncryptionOperation, EnableInput};
    use aruna_runner::driver::drive;
    use std::collections::{BTreeMap, HashMap};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let storage = aruna_storage::storage::worker::FjallStorage::open(root).unwrap();
    storage.open_vault(NodeVaultKey::random());
    let net = aruna_p2p::NetHandle::new(aruna_runner::testing::local_net(), storage.clone())
        .await
        .unwrap();
    let config = BackendConfig {
        backend_type: Backend::FileSystem,
        bucket_prefix: Some("aruna_".to_string()),
        max_bucket_size: Some(100_000),
        multipart_bucket: Some("multipart".to_string()),
        root: format!("{root}/blobs"),
        service_config: HashMap::new(),
        timeouts: Default::default(),
    };
    let blob = BlobHandler::new(config, storage.clone(), net.clone())
        .await
        .unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: Some(blob.clone()),
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId::from_bytes([1; 32]);
    let creator = UserId::new(Ulid::from_bytes([5; 16]), realm_id);
    let group_id = Ulid::from_bytes([3; 16]);
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: creator,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    // The authorization documents the enable reads its admins from.
    let documents = [realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec()];
    for (key, value) in documents
        .into_iter()
        .zip(authority_documents(&info, creator))
    {
        let key_space = aruna_core::keyspaces::AUTH_KEYSPACE.to_string();
        let write = StorageEffect::Write {
            key_space,
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
    }
    drive(
        CreateBucketOperation::new("sealed".to_string(), info),
        &context,
    )
    .await
    .unwrap();
    let holder = SecretBytes::new(vec![6; 32]);
    let record = aruna_core::structs::identity::user::vault::UserKeyRecord {
        realm_id: creator.realm_id,
        user_id: creator,
        record_id: Ulid::from_bytes([7; 16]),
        key_id: "slot".to_string(),
        public_key: public_key_of(&holder).unwrap(),
        fingerprint: aruna_core::vault_format::key_fingerprint(&public_key_of(&holder).unwrap()),
        has_recovery: true,
        node_id: net.node_id(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        created_at_ms: 1,
        signature: None,
    };
    let enabled = drive(
        EnableEncryptionOperation::new(EnableInput {
            bucket: "sealed".to_string(),
            group_id,
            realm_id,
            node_id: net.node_id(),
            caller: creator,
            mode: EncryptionMode::NodeManaged,
            cipher: BlockCipher::ChaCha20Poly1305,
            block_keys: BlockKeys::ContentDerived,
            max_unlock_ms: None,
            expected_generation: 0,
            lookups: BTreeMap::from([(creator, KeyLookup::Keys(vec![record]))]),
            now_ms: 1,
        }),
        &context,
    )
    .await
    .unwrap();
    (dir, context, enabled)
}

#[tokio::test]
async fn install_wakes_parked() {
    use crate::jobs::store::job_calls::ClaimOutcome;
    use aruna_core::keyspaces::KEY_COPY_KEYSPACE;
    use aruna_core::node::vault::{VaultEntry, VaultPurpose};
    use aruna_core::structs::execution::job::identity::{JobId, JobState};
    use aruna_core::structs::execution::job::input::JobPayload;
    use aruna_core::structs::execution::job::key_wait::KeyWait;
    use aruna_core::structs::execution::job::local::{JobRecord, due_index_key};
    use aruna_test_harness::jobs::{insert_job, read_job_record};

    let (_dir, context, enabled) = enabled_context().await;
    let storage = &context.storage_handle;
    let node = context.net_handle.as_ref().unwrap().node_id();
    let realm = RealmId::from_bytes([1; 32]);
    let creator = UserId::new(Ulid::from_bytes([5; 16]), realm);
    let key = enabled.key.clone();
    let job_id = JobId::from_bytes([8; 16]);
    let payload = JobPayload::Probe {
        steps: 1,
        step_sleep_ms: 0,
        fail_at: None,
        panic_at: None,
        cleanup_marker: None,
    };
    let job = JobRecord::new(job_id, payload, creator, node, 1_000, 1_000, None);
    insert_job(&context, &job).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = aruna_runner::driver::drive(
        crate::jobs::store::job_calls::claim_job(job_id, node, 2_000),
        &context,
    )
    .await
    .unwrap() else {
        panic!("job must be claimed")
    };
    let wait = KeyWait {
        node_id: node,
        bucket: "sealed".to_string(),
        group_id: None,
        key: key.key,
    };
    aruna_runner::driver::drive(
        crate::jobs::store::row_calls::park_job(
            job_id,
            claimed.claim.unwrap().claim_token,
            3_000,
            vec![wait],
        ),
        &context,
    )
    .await
    .unwrap();
    assert!(
        !aruna_runner::driver::drive(
            aruna_ops_data::blob::unlocked::KeyUnlockedOperation::new(key.key),
            &context
        )
        .await
        .unwrap()
    );
    assert_eq!(
        read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap()
            .state,
        JobState::AwaitingKey
    );
    let install = InstallKeyOperation::new(InstallInput {
        key: key.key,
        public_key: key.public_key,
        private_key: enabled.private_key,
        duration: None,
        max: None,
    });
    let status = activate_and_wake(&context, install, (realm, node))
        .await
        .unwrap();
    assert!(status.active && status.remaining.is_none());
    let job = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, JobState::Queued);
    let scheduled = StorageEffect::Read {
        key_space: aruna_core::keyspaces::SCHEDULE_INDEX_KEYSPACE.to_string(),
        key: due_index_key(job.due_at_ms, job_id),
        txn_id: None,
    };
    assert!(matches!(
        storage.send_storage_effect(scheduled).await,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));

    // The node copy opens to the installed key; the creator's copy is stored.
    let entry = VaultEntry::new(VaultPurpose::BucketKey, key.vault_entry.unwrap());
    let read = StorageEffect::VaultRead {
        entry,
        txn_id: None,
    };
    let Event::Storage(StorageEvent::VaultResult {
        secret: Some(secret),
        ..
    }) = storage.send_storage_effect(read).await
    else {
        panic!("no node copy");
    };
    assert_eq!(public_key_of(&secret), Some(key.public_key));
    let copy = enabled.holders.holder(creator).unwrap();
    assert_eq!(
        copy.state,
        aruna_core::structs::storage::holders::HolderState::Ready
    );
    let rows = StorageEffect::Iter {
        key_space: KEY_COPY_KEYSPACE.to_string(),
        prefix: Some(key.key.key().into()),
        start: None,
        limit: 8,
        txn_id: None,
    };
    let Event::Storage(StorageEvent::IterResult { values, .. }) =
        storage.send_storage_effect(rows).await
    else {
        panic!("no copies");
    };
    assert_eq!(values.len(), 1);
}

#[tokio::test]
async fn unlock_resumes_rewrite() {
    use crate::tests::records::read_row;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, TRANSITION_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::storage::blob::{BlobVersion, ResolvedBackend, VersionKey};
    use aruna_core::structs::storage::encryption::SealPlan;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::transition::{
        EncryptionTransition, TransitionKind, TransitionState, TransitionTarget,
    };
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_ops_data::blob::migration::queue::{ProcessTransitionsOperation, RECHECK};
    use aruna_ops_data::s3::bucket::key::unlock::{UnlockBucketOperation, UnlockInput};
    use aruna_runner::testing::RecordingTaskHandler;
    use aruna_runner::testing::freeze_clock;

    let (_dir, mut context, enabled) = enabled_context().await;
    let key = enabled.key.key;
    let plan = SealPlan::capture(&enabled.settings, &enabled.key)
        .unwrap()
        .unwrap();
    let blob = context.blob_handle.as_ref().unwrap();
    let stream = BackendStream(Box::pin(futures_util::stream::once(async {
        Ok(bytes::Bytes::from_static(b"waiting encrypted bytes"))
    })));
    let write = BlobEffect::Write {
        bucket: "sealed".to_string(),
        key: "data".to_string(),
        resolved: ResolvedBackend::node_default().with_encryption(Some(SealPlan {
            storage_generation: 0,
            ..plan
        })),
        created_by: UserId::default(),
        blob: stream,
        size: Some(23),
    };
    let Ok(BlobEvent::WriteFinished { location }) = blob.send_blob_effect(write).await.into_blob()
    else {
        panic!("archive must be written")
    };
    let version_key = VersionKey::new("sealed", "data", Ulid::from_bytes([8; 16]));
    let version = BlobVersion::materialized(
        location.get_blake3().unwrap().try_into().unwrap(),
        location.backend.clone(),
        location.format.encoding(),
        SystemTime::UNIX_EPOCH,
        UserId::default(),
        None,
    );
    let transition = EncryptionTransition::new(
        TransitionKind::Reencode,
        Some(key),
        TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan),
        },
        enabled.settings.storage_generation,
        1,
    );
    let writes = vec![
        (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            location.location_key().unwrap().to_bytes().into(),
            location.to_bytes().unwrap().into(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        ),
        (
            TRANSITION_KEYSPACE.to_string(),
            b"sealed".to_vec().into(),
            transition.to_bytes().unwrap().into(),
        ),
    ];
    context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert_eq!(
        aruna_runner::driver::drive(
            ProcessTransitionsOperation::new(context.node_and_realm()),
            &context
        )
        .await
        .unwrap(),
        Some(RECHECK)
    );
    let stored = read_row(&context, TRANSITION_KEYSPACE, b"sealed".to_vec())
        .await
        .unwrap()
        .unwrap();
    let parked = EncryptionTransition::from_bytes(&stored).unwrap();
    assert_eq!(
        (parked.state, parked.remaining),
        (TransitionState::AwaitingKey, 1)
    );
    assert!(parked.retry_at_ms.is_some());

    tokio::time::pause();
    let _clock = freeze_clock();
    let tasks = aruna_scheduler::TaskHandle::new();
    let (sender, mut fired) = tokio::sync::mpsc::channel(8);
    tasks
        .set_inbound_handler(std::sync::Arc::new(RecordingTaskHandler { seen: sender }))
        .await;
    tasks
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::MigrateCompression,
            after: RECHECK,
        }))
        .await;
    context.task_handle = Some(tasks.clone());
    let start = tokio::time::Instant::now();
    tokio::time::advance(Duration::from_secs(1)).await;
    let duration = Duration::from_secs(30);
    let node = context.net_handle.as_ref().unwrap().node_id();
    let realm = RealmId::from_bytes([1; 32]);
    let input = UnlockInput {
        bucket: "sealed".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: realm,
        node_id: node,
        caller: UserId::new(Ulid::from_bytes([5; 16]), realm),
        key,
        duration: Some(duration),
        now_ms: aruna_core::time::unix_timestamp_millis(),
    };
    let unlock = UnlockBucketOperation::new(input, enabled.private_key);
    let status = activate_and_wake(&context, unlock, (realm, node))
        .await
        .unwrap();
    assert!(
        status
            .remaining
            .is_some_and(|remaining| remaining <= duration)
    );
    while fired.recv().await.unwrap() != TaskKey::MigrateCompression {}
    let resumed = read_row(&context, TRANSITION_KEYSPACE, b"sealed".to_vec())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        EncryptionTransition::from_bytes(&resumed)
            .unwrap()
            .retry_at_ms,
        None
    );
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(1));
    assert!(duration < RECHECK);
    aruna_runner::driver::drive(
        ProcessTransitionsOperation::new(context.node_and_realm()),
        &context,
    )
    .await
    .unwrap();
    let stored = read_row(&context, TRANSITION_KEYSPACE, b"sealed".to_vec())
        .await
        .unwrap()
        .unwrap();
    let progressed = EncryptionTransition::from_bytes(&stored).unwrap();
    assert_eq!(
        (progressed.done, progressed.remaining, progressed.failed),
        (1, 0, 0)
    );
    assert_eq!(progressed.state, TransitionState::Cleanup);
    tasks.shutdown(Duration::ZERO).await;
    tokio::time::resume();
}
