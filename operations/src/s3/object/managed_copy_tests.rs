//! Tests that an interrupted, rolled back or deleted copy is never serveable, and that a governed
//! read fails closed without its registration.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use aruna_core::managed_copy::{
    CopyRegistration, ManagedCopyError, register_effect, scan_effect, version_scope,
};
use crate::driver::{DriverContext, drive};
use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation,
};
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE, NODE_SUBJECT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PlacementSubject};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendRef, BlobVersion, ManagedCopyKey, ManagedCopyQuarantine,
    ManagedCopyRecord, ManagedCopyState, VersionKey,
};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::GroupId;
use aruna_net::{NetConfig, NetHandle};
use aruna_storage::storage;
use std::collections::{HashMap, VecDeque};
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

const BUCKET: &str = "mybucket";
const OBJECT: &str = "governed.txt";
const BODY: &[u8] = b"payload";

async fn full_context() -> (TempDir, DriverContext) {
    let temp_handle = tempdir().expect("temp dir");
    let temp_root = temp_handle.path().to_str().expect("utf8 path");
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).expect("blob root");
    let storage_handle = storage::FjallStorage::open(temp_root).expect("storage opens");
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .expect("net handle");
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_root,
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await
    .expect("blob handle");
    let context = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (temp_handle, context)
}

fn put_config(
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    user_id: UserId,
    preassigned: Option<Ulid>,
    quota_ceiling: Option<u64>,
) -> PutObjectConfig {
    PutObjectConfig {
        user_id,
        group_id,
        realm_id,
        node_id,
        request: PutObjectInput {
            bucket: BUCKET.to_string(),
            key: OBJECT.to_string(),
            content_length: Some(BODY.len() as u64),
            body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(BODY))),
        },
        expected_checksums: vec![],
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: preassigned,
        quota_ceiling,
        routing: RoutingSnapshot::single(group_id),
    }
}

fn version_key(version_id: Ulid) -> VersionKey {
    VersionKey::new(BUCKET, OBJECT, version_id)
}

async fn read_copy(context: &DriverContext, version_id: Ulid) -> Option<ManagedCopyRecord> {
    let key = ManagedCopyKey::new(version_key(version_id), BackendRef::node_default());
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: MANAGED_COPY_KEYSPACE.to_string(),
            key: key.to_bytes().expect("key encodes").into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read result");
    };
    value.map(|value| ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes"))
}

async fn count_copies(context: &DriverContext, version_id: Ulid) -> usize {
    let scope = version_scope(&version_key(version_id)).expect("scope builds");
    let Event::Storage(StorageEvent::IterResult { values, .. }) = context
        .storage_handle
        .send_storage_effect(match scan_effect(Some(scope), None, 64, None) {
            Effect::Storage(effect) => effect,
            other => panic!("expected a storage effect, got {other:?}"),
        })
        .await
    else {
        panic!("unexpected storage iteration result");
    };
    values.len()
}

async fn read_version(context: &DriverContext, version_id: Ulid) -> Option<BlobVersion> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key(version_id)
                .to_bytes()
                .expect("key encodes")
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read result");
    };
    value.map(|value| BlobVersion::from_bytes(value.as_ref()).expect("version decodes"))
}

async fn write_copy(context: &DriverContext, record: &ManagedCopyRecord) {
    let Effect::Storage(effect) = register_effect(
        CopyRegistration {
            version: record.version.clone(),
            node_id: record.node_id,
            location: &record.location,
            policies: &record.policies,
            origin: record.origin,
            subject_generation: record.subject_generation,
            registered_at_ms: record.registered_at_ms,
        },
        None,
    )
    .expect("effect builds") else {
        panic!("expected a storage effect");
    };
    let _ = context.storage_handle.send_storage_effect(effect).await;
}

/// Runs a put until the version transaction would start, which is exactly a
/// crash after the blob bytes landed and before anything was registered.
async fn drive_until_txn(operation: &mut PutObjectOperation, context: &DriverContext) {
    let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
    while let Some(effect) = queue.pop_front() {
        let event = match effect {
            Effect::Storage(StorageEffect::StartTransaction { .. }) => return,
            Effect::Storage(effect) => context.storage_handle.send_storage_effect(effect).await,
            Effect::Blob(effect) => {
                context
                    .blob_handle
                    .as_ref()
                    .expect("blob handle")
                    .send_blob_effect(effect)
                    .await
            }
            other => panic!("unexpected pre-transaction effect {other:?}"),
        };
        queue.extend(operation.step(event));
    }
}

struct Fixture {
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    user_id: UserId,
}

fn fixture(context: &DriverContext) -> Fixture {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    Fixture {
        realm_id,
        group_id: Ulid::generate(),
        node_id: context.net_handle.as_ref().expect("net handle").node_id(),
        user_id: UserId::local(Ulid::generate(), realm_id),
    }
}

async fn put_object(context: &DriverContext, fixture: &Fixture) -> Ulid {
    drive(
        PutObjectOperation::new(put_config(
            fixture.realm_id,
            fixture.group_id,
            fixture.node_id,
            fixture.user_id,
            None,
            None,
        )),
        context,
    )
    .await
    .expect("put drives")
    .version_id
}

async fn head_object(context: &DriverContext) -> Result<(), HeadObjectError> {
    drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: BUCKET.to_string(),
            key: OBJECT.to_string(),
            version_id: None,
        }),
        context,
    )
    .await
    .map(|_| ())
}

async fn get_object(context: &DriverContext, fixture: &Fixture) -> Result<(), GetObjectError> {
    drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: BUCKET.to_string(),
            key: OBJECT.to_string(),
            version_id: None,
            range: None,
            group_id: fixture.group_id,
            user_identity: fixture.user_id,
            realm_id: fixture.user_id.realm_id,
            node_id: fixture.node_id,
        }),
        context,
    )
    .await
    .map(|_| ())
}

/// Advertises a subject for this node, without which nothing governed may
/// be served at all.
async fn seed_subject(context: &DriverContext, node_id: NodeId, blocked: bool) {
    let mut record = NodeSubjectRecord::seed(PlacementSubject {
        node_id,
        generation: 1,
        location: "eu-west".to_string(),
        labels: Default::default(),
        executor_kind: None,
        local_to_controller: true,
    })
    .expect("subject is valid");
    record.serving_blocked = blocked;
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: NODE_SUBJECT_KEY.to_vec().into(),
            value: record.to_bytes().expect("record encodes").into(),
            txn_id: None,
        })
        .await;
}

/// Puts refs on the stored version, the state step 6 mints directly.
async fn govern_version(context: &DriverContext, version_id: Ulid) {
    let version = read_version(context, version_id)
        .await
        .expect("version exists")
        .with_policies(vec![PlacementPolicyRef {
            policy_id: Ulid::from_bytes([3u8; 16]),
            digest: [4u8; 32],
        }])
        .expect("refs stored");
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key(version_id)
                .to_bytes()
                .expect("key encodes")
                .into(),
            value: version.to_bytes().expect("version encodes").into(),
            txn_id: None,
        })
        .await;
    // A gated write stores the same refs on the registration, so the
    // fixture must too or the row describes a different copy.
    if let Some(mut record) = read_copy(context, version_id).await {
        record.policies = version.placement_policies.clone();
        record.subject_generation = 1;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: record.key().to_bytes().expect("key encodes").into(),
                value: record.to_bytes().expect("record encodes").into(),
                txn_id: None,
            })
            .await;
    }
}

async fn delete_copy(context: &DriverContext, version_id: Ulid) {
    let key = ManagedCopyKey::new(version_key(version_id), BackendRef::node_default());
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: MANAGED_COPY_KEYSPACE.to_string(),
            key: key.to_bytes().expect("key encodes").into(),
            txn_id: None,
        })
        .await;
}

#[tokio::test]
async fn put_registers_copy() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;

    let record = read_copy(&context, version_id).await.expect("copy row");
    assert_eq!(record.state, ManagedCopyState::Registered);
    assert_eq!(record.node_id, fixture.node_id);
    assert_eq!(record.version, version_key(version_id));
    assert!(!record.location.staging && !record.location.partial);
}

#[tokio::test]
async fn crash_skips_registration() {
    // Bytes written before the version transaction never become serveable.
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = Ulid::generate();
    let mut operation = PutObjectOperation::new(put_config(
        fixture.realm_id,
        fixture.group_id,
        fixture.node_id,
        fixture.user_id,
        Some(version_id),
        None,
    ));

    drive_until_txn(&mut operation, &context).await;
    drop(operation);

    assert!(read_version(&context, version_id).await.is_none());
    assert!(read_copy(&context, version_id).await.is_none());
    assert!(matches!(
        get_object(&context, &fixture).await,
        Err(GetObjectError::NoSuchKey)
    ));
}

#[tokio::test]
async fn replay_reuses_registration() {
    // The retry after an interrupted write registers the copy exactly once.
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = Ulid::generate();
    let mut interrupted = PutObjectOperation::new(put_config(
        fixture.realm_id,
        fixture.group_id,
        fixture.node_id,
        fixture.user_id,
        Some(version_id),
        None,
    ));
    drive_until_txn(&mut interrupted, &context).await;
    drop(interrupted);

    for _ in 0..2 {
        drive(
            PutObjectOperation::new(put_config(
                fixture.realm_id,
                fixture.group_id,
                fixture.node_id,
                fixture.user_id,
                Some(version_id),
                None,
            )),
            &context,
        )
        .await
        .expect("put drives");
    }

    assert_eq!(count_copies(&context, version_id).await, 1);
    assert!(
        read_copy(&context, version_id)
            .await
            .expect("copy row")
            .state
            .is_serveable()
    );
}

#[tokio::test]
async fn rollback_removes_registration() {
    // The quota gate aborts the shared transaction, so neither the version
    // nor its registration may survive.
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = Ulid::generate();

    let error = drive(
        PutObjectOperation::new(put_config(
            fixture.realm_id,
            fixture.group_id,
            fixture.node_id,
            fixture.user_id,
            Some(version_id),
            Some(1),
        )),
        &context,
    )
    .await;

    assert!(matches!(error, Err(PutObjectError::QuotaExceeded { .. })));
    assert!(read_version(&context, version_id).await.is_none());
    assert!(read_copy(&context, version_id).await.is_none());
}

#[tokio::test]
async fn delete_removes_registration() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    assert!(read_copy(&context, version_id).await.is_some());

    drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: BUCKET.to_string(),
            key: OBJECT.to_string(),
            version_id: Some(version_id),
            group_id: fixture.group_id,
            realm_id: fixture.realm_id,
            node_id: fixture.node_id,
            deleted_by: fixture.user_id,
        }),
        &context,
    )
    .await
    .expect("delete drives");

    assert!(read_version(&context, version_id).await.is_none());
    assert_eq!(count_copies(&context, version_id).await, 0);
}

#[tokio::test]
async fn read_needs_registration() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    govern_version(&context, version_id).await;
    seed_subject(&context, fixture.node_id, false).await;
    let record = read_copy(&context, version_id).await.expect("copy row");
    delete_copy(&context, version_id).await;

    assert_eq!(
        get_object(&context, &fixture).await,
        Err(GetObjectError::ManagedCopyError(
            ManagedCopyError::Unregistered
        ))
    );

    write_copy(&context, &record).await;
    assert!(get_object(&context, &fixture).await.is_ok());
}

#[tokio::test]
async fn quarantine_blocks_read() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    govern_version(&context, version_id).await;
    seed_subject(&context, fixture.node_id, false).await;
    let mut record = read_copy(&context, version_id).await.expect("copy row");
    record.state = ManagedCopyState::Quarantined(ManagedCopyQuarantine::SubjectTransition);
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: MANAGED_COPY_KEYSPACE.to_string(),
            key: record.key().to_bytes().expect("key encodes").into(),
            value: record.to_bytes().expect("record encodes").into(),
            txn_id: None,
        })
        .await;

    assert_eq!(
        get_object(&context, &fixture).await,
        Err(GetObjectError::ManagedCopyError(
            ManagedCopyError::NotServeable(record.state)
        ))
    );
}

#[tokio::test]
async fn head_needs_registration() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    govern_version(&context, version_id).await;
    seed_subject(&context, fixture.node_id, false).await;
    delete_copy(&context, version_id).await;

    assert_eq!(
        head_object(&context).await,
        Err(HeadObjectError::ManagedCopyError(
            ManagedCopyError::Unregistered
        ))
    );
}

#[tokio::test]
async fn read_needs_subject() {
    // A node that never advertised a subject serves nothing governed.
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    govern_version(&context, version_id).await;

    assert_eq!(
        get_object(&context, &fixture).await,
        Err(GetObjectError::ManagedCopyError(
            ManagedCopyError::NoSubject
        ))
    );
}

#[tokio::test]
async fn rejoin_blocks_read() {
    // A rejoining node serves nothing until its inventory is revalidated,
    // even though the registration itself still reads as registered.
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    govern_version(&context, version_id).await;
    seed_subject(&context, fixture.node_id, true).await;

    assert_eq!(
        get_object(&context, &fixture).await,
        Err(GetObjectError::ManagedCopyError(
            ManagedCopyError::ServingBlocked
        ))
    );
    seed_subject(&context, fixture.node_id, false).await;
    assert!(get_object(&context, &fixture).await.is_ok());
}

#[tokio::test]
async fn ungoverned_read_unchanged() {
    // An object with no refs must read exactly as before, registration or not.
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context);
    let version_id = put_object(&context, &fixture).await;
    delete_copy(&context, version_id).await;

    assert!(get_object(&context, &fixture).await.is_ok());
    assert!(head_object(&context).await.is_ok());
}
