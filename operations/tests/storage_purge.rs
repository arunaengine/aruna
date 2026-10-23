//! Tests that a storage purge fences its scope, checks live objects and resumes cleanly.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE, PURGE_CHECKPOINT_KEYSPACE,
    PURGE_FENCE_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord, JobResultPayload};
use aruna_core::structs::identity::auth::{Actor, AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::storage::backend_import::version_key;
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BackendRef, BlobCleanupWork, BlobHeadKey, BlobVersion,
    BucketInfo, CurrentVersionPointer, VersionKey, bucket_permission_path,
};
use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};
use aruna_core::structs::storage::multipart::{
    MultipartChecksumType, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::structs::storage::storage_purge::{
    StoragePurgeCheckpoint, StoragePurgeScope, StoragePurgeSpec,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::store::{
    ClaimOutcome, claim_job, complete_job, insert_job, put_purge_checkpoint, transition_to_running,
};
use aruna_operations::jobs::workflow::purge::run_storage_purge;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::multipart::complete::{
    CompleteUploadError, CompleteUploadInput, CompleteUploadOperation,
};
use aruna_operations::s3::multipart::create::{
    CreateMultipartError, CreateMultipartInput, CreateMultipartOperation,
};
use aruna_operations::s3::multipart::part_copy::{PartCopyError, PartCopyInput, upload_part_copy};
use aruna_operations::s3::multipart::part_upload::{
    UploadPartError, UploadPartInput, UploadPartOperation,
};
use aruna_operations::s3::object::copy::{
    CopyObjectError, CopyObjectInput, CopyReferences, CopySourceConditions, copy_object,
};
use aruna_operations::s3::object::delete::bulk::{
    BulkDeleteEntry, BulkDeleteInput, delete_objects,
};
use aruna_operations::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation,
};
use aruna_operations::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation,
};
use aruna_operations::s3::purge_fence::{PurgeFenceError, acquire_purge_fence, fence_key};
use aruna_storage::{StorageHandle, storage};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

struct TestContext {
    _temp_dir: TempDir,
    driver: Arc<DriverContext>,
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    user_id: UserId,
}

async fn setup_context() -> TestContext {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    // Creating an upload asks the blob adapter whether the backend has a provider upload.
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root,
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: None,
            multipart_bucket: Some("uploaded-parts".to_string()),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await
    .unwrap();
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let group_id = Ulid::generate();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let driver = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(node_id, RealmNodeKind::Server);
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::default_group_doc(user_id, realm_id, group_id);
    let group = Group {
        display_name: "purge".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: user_id,
    };
    write_value(
        &driver.storage_handle,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor).unwrap(),
    )
    .await;
    write_value(
        &driver.storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_value(
        &driver.storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_value(
        &driver.storage_handle,
        GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
    drive(
        CreateBucketOperation::new(
            "bucket".to_string(),
            BucketInfo {
                group_id,
                created_at: UNIX_EPOCH,
                created_by: user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        &driver,
    )
    .await
    .unwrap();

    TestContext {
        _temp_dir: temp_dir,
        driver,
        realm_id,
        group_id,
        node_id,
        user_id,
    }
}

fn stream(bytes: &[u8]) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

fn auth(context: &TestContext) -> AuthContext {
    AuthContext {
        user_id: context.user_id,
        realm_id: context.realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn put_operation(context: &TestContext, key: &str, bytes: &[u8]) -> PutObjectOperation {
    PutObjectOperation::new(PutObjectConfig {
        user_id: context.user_id,
        group_id: context.group_id,
        realm_id: context.realm_id,
        node_id: context.node_id,
        request: PutObjectInput {
            bucket: "bucket".to_string(),
            key: key.to_string(),
            content_length: Some(bytes.len() as u64),
            body: Some(stream(bytes)),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: None,
        routing: RoutingSnapshot::single(context.group_id),
    })
}

#[tokio::test]
async fn scoped_fence_isolates() {
    let context = setup_context().await;
    let upload = seed_upload(
        &context.driver.storage_handle,
        "bucket",
        "blocked/multipart.bin",
        context.group_id,
        context.user_id,
        MultipartUploadStatus::Open,
    )
    .await;
    let scope = StoragePurgeScope::Prefix {
        bucket: "bucket".to_string(),
        prefix: "blocked/".to_string(),
    };
    acquire_purge_fence(
        &context.driver.storage_handle,
        JobId::from_bytes([0xC3; 16]),
        &scope,
    )
    .await
    .unwrap();

    assert!(matches!(
        drive(
            put_operation(&context, "blocked/put.txt", b"blocked"),
            &context.driver,
        )
        .await,
        Err(PutObjectError::PurgeFence(PurgeFenceError::Suspended))
    ));

    assert!(matches!(
        copy_object(
            &context.driver,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: context.group_id,
                source_auth_context: auth(&context),
                dest_bucket: "bucket".to_string(),
                dest_key: "blocked/copy.txt".to_string(),
                user_id: context.user_id,
                group_id: context.group_id,
                realm_id: context.realm_id,
                node_id: context.node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
                metadata: None,
                restrictions: None,
                references: CopyReferences::Materialize,
            },
        )
        .await,
        Err(CopyObjectError::Put(PutObjectError::PurgeFence(
            PurgeFenceError::Suspended
        )))
    ));
    assert!(matches!(
        upload_part_copy(
            &context.driver,
            PartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: context.group_id,
                dest_bucket: upload.bucket.clone(),
                dest_key: upload.key.clone(),
                upload_id: upload.upload_id,
                part_number: 2,
                range: None,
                user_id: context.user_id,
                node_id: context.node_id,
                source_auth_context: auth(&context),
                conditions: CopySourceConditions::default(),
            },
        )
        .await,
        Err(PartCopyError::UploadPart(UploadPartError::PurgeFence(
            PurgeFenceError::Suspended
        )))
    ));

    assert!(matches!(
        drive(
            CreateMultipartOperation::new(CreateMultipartInput {
                bucket: "bucket".to_string(),
                key: "blocked/new-upload.bin".to_string(),
                group_id: context.group_id,
                created_by: context.user_id,
                checksum_hint: None,
                routing: RoutingSnapshot::single(context.group_id),
            }),
            &context.driver,
        )
        .await,
        Err(CreateMultipartError::PurgeFence(PurgeFenceError::Suspended))
    ));

    assert!(matches!(
        drive(
            UploadPartOperation::new(UploadPartInput {
                bucket: upload.bucket.clone(),
                key: upload.key.clone(),
                upload_id: upload.upload_id,
                part_number: 2,
                content_length: Some(8),
                body: Some(stream(b"part-two")),
                created_by: context.user_id,
                expected_checksums: Vec::new(),
            }),
            &context.driver,
        )
        .await,
        Err(UploadPartError::PurgeFence(PurgeFenceError::Suspended))
    ));

    assert!(matches!(
        drive(
            CompleteUploadOperation::new(CompleteUploadInput {
                bucket: upload.bucket.clone(),
                key: upload.key.clone(),
                upload_id: upload.upload_id,
                realm_id: context.realm_id,
                node_id: context.node_id,
                completed_parts: Vec::new(),
                expected_checksums: Vec::new(),
                checksum_algorithm: None,
                checksum_type: MultipartChecksumType::FullObject,
                checksum_type_explicit: false,
                object_size: None,
                created_by: context.user_id,
                quota_ceiling: None,
                now_ms: aruna_operations::driver::now_ms(),
            }),
            &context.driver,
        )
        .await,
        Err(CompleteUploadError::PurgeFence(PurgeFenceError::Suspended))
    ));

    assert!(matches!(
        drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "bucket".to_string(),
                key: "blocked/delete.txt".to_string(),
                version_id: None,
                group_id: context.group_id,
                realm_id: context.realm_id,
                node_id: context.node_id,
                deleted_by: context.user_id,
            }),
            &context.driver,
        )
        .await,
        Err(DeleteObjectError::PurgeFence(PurgeFenceError::Suspended))
    ));
    let batch = delete_objects(
        &context.driver,
        BulkDeleteInput {
            bucket: "bucket".to_string(),
            entries: vec![BulkDeleteEntry {
                key: "blocked/batch-delete.txt".to_string(),
                version_id: None,
            }],
            group_id: context.group_id,
            realm_id: context.realm_id,
            node_id: context.node_id,
            deleted_by: context.user_id,
            restrictions: None,
        },
    )
    .await;
    assert!(matches!(
        batch[0].result,
        Err(DeleteObjectError::PurgeFence(PurgeFenceError::Suspended))
    ));

    let outside_upload = drive(
        CreateMultipartOperation::new(CreateMultipartInput {
            bucket: "bucket".to_string(),
            key: "allowed/outside.bin".to_string(),
            group_id: context.group_id,
            created_by: context.user_id,
            checksum_hint: None,
            routing: RoutingSnapshot::single(context.group_id),
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(outside_upload.key, "allowed/outside.bin");
    let stored_upload = read_value(
        &context.driver.storage_handle,
        UPLOAD_KEYSPACE,
        upload.upload_id.to_bytes().to_vec(),
    )
    .await
    .expect("rejected completion must leave an abortable upload");
    assert_eq!(
        MultipartUpload::from_bytes(stored_upload.as_ref())
            .unwrap()
            .status,
        MultipartUploadStatus::Open
    );
}

#[tokio::test]
async fn purge_checks_objects() {
    let context = setup_context().await;
    let version_id = Ulid::generate();
    seed_deleted_version(
        &context.driver.storage_handle,
        "bucket",
        "denied/file.txt",
        version_id,
        true,
        context.user_id,
    )
    .await;
    let mut auth_context = auth(&context);
    auth_context.path_restrictions = Some(vec![PathRestriction {
        pattern: bucket_permission_path(
            context.realm_id,
            context.group_id,
            context.node_id,
            "bucket",
        ),
        permission: Permission::WRITE,
    }]);
    let spec = StoragePurgeSpec {
        scope: StoragePurgeScope::Prefix {
            bucket: "bucket".to_string(),
            prefix: "denied/".to_string(),
        },
        group_id: context.group_id,
        auth_context,
        node_id: context.node_id,
    };
    let job = claimed_job(
        context.driver.clone(),
        JobId::from_bytes([0xD4; 16]),
        spec.clone(),
    )
    .await;

    let outcome = run_storage_purge(&job, &spec, false).await;

    assert!(matches!(
        outcome,
        JobRunOutcome::Failed(error) if error.message.contains("authorization failed")
    ));
    assert!(
        read_value(
            &context.driver.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket", "denied/file.txt", version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn purge_resumes_cleanly() {
    let context = setup_context().await;
    let target_old = Ulid::generate();
    let target_current = Ulid::generate();
    let outside = Ulid::generate();
    seed_deleted_version(
        &context.driver.storage_handle,
        "bucket",
        "purge/file.txt",
        target_old,
        false,
        context.user_id,
    )
    .await;
    seed_deleted_version(
        &context.driver.storage_handle,
        "bucket",
        "purge/file.txt",
        target_current,
        true,
        context.user_id,
    )
    .await;
    seed_deleted_version(
        &context.driver.storage_handle,
        "bucket",
        "keep/file.txt",
        outside,
        true,
        context.user_id,
    )
    .await;
    let upload = seed_upload(
        &context.driver.storage_handle,
        "bucket",
        "purge/incomplete.bin",
        context.group_id,
        context.user_id,
        MultipartUploadStatus::Completing,
    )
    .await;

    let prefix_scope = StoragePurgeScope::Prefix {
        bucket: "bucket".to_string(),
        prefix: "purge/".to_string(),
    };
    let prefix_spec = StoragePurgeSpec {
        scope: prefix_scope.clone(),
        group_id: context.group_id,
        auth_context: auth(&context),
        node_id: context.node_id,
    };
    let prefix_job_id = JobId::from_bytes([0xA1; 16]);
    let prefix_job = claimed_job(context.driver.clone(), prefix_job_id, prefix_spec.clone()).await;
    acquire_purge_fence(&context.driver.storage_handle, prefix_job_id, &prefix_scope)
        .await
        .unwrap();
    put_purge_checkpoint(
        &context.driver.storage_handle,
        prefix_job_id,
        prefix_job.claim_token,
        &StoragePurgeCheckpoint {
            initial_versions: 2,
            initial_multipart_uploads: 1,
            batches_completed: 1,
        },
    )
    .await
    .unwrap();
    drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "bucket".to_string(),
            key: "purge/file.txt".to_string(),
            version_id: Some(target_old),
            group_id: context.group_id,
            realm_id: context.realm_id,
            node_id: context.node_id,
            deleted_by: context.user_id,
        }),
        &context.driver,
    )
    .await
    .unwrap();

    let first = purge_result(run_storage_purge(&prefix_job, &prefix_spec, false).await);
    assert!(first.emptiness_proven);
    assert!(!first.bucket_deleted);
    assert_eq!(first.versions_removed, 2);
    assert_eq!(first.multipart_uploads_removed, 1);
    assert!(
        read_value(
            &context.driver.storage_handle,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket", "purge/file.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver.storage_handle,
            UPLOAD_KEYSPACE,
            upload.upload_id.to_bytes().to_vec(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket", "keep/file.txt", outside)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_some(),
        "a prefix purge must not delete neighboring keys"
    );

    let resumed = purge_result(run_storage_purge(&prefix_job, &prefix_spec, false).await);
    assert_eq!(
        resumed, first,
        "retry after a lost terminal write is idempotent"
    );
    complete_job(
        &context.driver.storage_handle,
        prefix_job_id,
        prefix_job.claim_token,
        JobResultPayload::StoragePurge(resumed),
        prefix_job.progress.snapshot(),
        unix_timestamp_millis(),
    )
    .await
    .unwrap();
    assert!(
        read_value(
            &context.driver.storage_handle,
            PURGE_FENCE_KEYSPACE,
            fence_key("bucket").as_ref().to_vec(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver.storage_handle,
            PURGE_CHECKPOINT_KEYSPACE,
            prefix_job_id.to_bytes().to_vec(),
        )
        .await
        .is_none()
    );

    let bucket_spec = StoragePurgeSpec {
        scope: StoragePurgeScope::Bucket {
            bucket: "bucket".to_string(),
        },
        group_id: context.group_id,
        auth_context: auth(&context),
        node_id: context.node_id,
    };
    let bucket_job_id = JobId::from_bytes([0xB2; 16]);
    let bucket_job = claimed_job(context.driver.clone(), bucket_job_id, bucket_spec.clone()).await;
    let bucket_result = purge_result(run_storage_purge(&bucket_job, &bucket_spec, false).await);
    assert!(bucket_result.emptiness_proven);
    assert!(bucket_result.bucket_deleted);
    complete_job(
        &context.driver.storage_handle,
        bucket_job_id,
        bucket_job.claim_token,
        JobResultPayload::StoragePurge(bucket_result),
        bucket_job.progress.snapshot(),
        unix_timestamp_millis(),
    )
    .await
    .unwrap();
    assert!(
        read_value(
            &context.driver.storage_handle,
            S3_BUCKET_KEYSPACE,
            b"bucket".to_vec(),
        )
        .await
        .is_none(),
        "bucket-scope success must prove the bucket record is gone"
    );
}

async fn claimed_job(
    driver: Arc<DriverContext>,
    job_id: JobId,
    spec: StoragePurgeSpec,
) -> JobContext {
    let owner_node_id = spec.node_id;
    let created_by = spec.auth_context.user_id;
    insert_job(
        &driver.storage_handle,
        &JobRecord::new(
            job_id,
            JobPayload::StoragePurge(spec),
            created_by,
            owner_node_id,
            1,
            1,
            None,
        ),
    )
    .await
    .unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(
        &driver.storage_handle,
        job_id,
        owner_node_id,
        unix_timestamp_millis(),
    )
    .await
    .unwrap() else {
        panic!("new purge job was not claimed")
    };
    let claim_token = claimed.claim.as_ref().unwrap().claim_token;
    let running = transition_to_running(
        &driver.storage_handle,
        job_id,
        claim_token,
        unix_timestamp_millis(),
    )
    .await
    .unwrap();
    JobContext {
        driver,
        job_id,
        owner_node_id,
        claim_token,
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&running.progress),
    }
}

fn purge_result(
    outcome: JobRunOutcome,
) -> aruna_core::structs::storage::storage_purge::StoragePurgeResult {
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::StoragePurge(result)) => result,
        JobRunOutcome::Succeeded(_) => panic!("purge returned the wrong result kind"),
        JobRunOutcome::Failed(error) => panic!("purge failed: {}", error.message),
        JobRunOutcome::Deferred(error) => panic!("purge was deferred: {}", error.message),
        JobRunOutcome::Cancelled => panic!("purge was cancelled"),
        JobRunOutcome::Interrupted => panic!("purge was interrupted"),
        JobRunOutcome::AwaitingKey(_) => panic!("purge was awaiting a key"),
    }
}

async fn seed_deleted_version(
    storage: &StorageHandle,
    bucket: &str,
    key: &str,
    version_id: Ulid,
    current: bool,
    created_by: UserId,
) {
    write_value(
        storage,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
        BlobVersion::deleted(SystemTime::now(), created_by)
            .to_bytes()
            .unwrap(),
    )
    .await;
    if current {
        write_value(
            storage,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        )
        .await;
    }
}

async fn seed_upload(
    storage: &StorageHandle,
    bucket: &str,
    key: &str,
    group_id: GroupId,
    created_by: UserId,
    status: MultipartUploadStatus,
) -> MultipartUpload {
    let upload = MultipartUpload {
        backend: BackendRef::node_default(),
        storage_class: None,
        upload_id: Ulid::generate(),
        bucket: bucket.to_string(),
        key: key.to_string(),
        group_id,
        created_by,
        created_at: SystemTime::now(),
        status,
        checksum_hint: None,
        metadata: HashMap::new(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
        backend_upload: None,
        encryption: None,
    };
    write_value(
        storage,
        UPLOAD_KEYSPACE,
        upload.upload_id.to_bytes().to_vec(),
        upload.to_bytes().unwrap(),
    )
    .await;
    upload
}

async fn write_value(storage: &StorageHandle, keyspace: &str, key: Vec<u8>, value: Vec<u8>) {
    assert!(matches!(
        storage
            .send_storage_effect(StorageEffect::Write {
                key_space: keyspace.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn read_value(
    storage: &StorageHandle,
    keyspace: &str,
    key: Vec<u8>,
) -> Option<byteview::ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read event")
    };
    value
}

/// Stores a current version of `key` that reads a linked file written as backend version `p1`.
async fn seed_linked(context: &TestContext, key: &str, version_id: Ulid) -> BackendLocation {
    let storage = &context.driver.storage_handle;
    let ulid = Ulid::generate();
    let location = BackendLocation {
        backend: BackendRef::Group(Ulid::from_parts(3, 3)),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: format!("raw/{key}"),
        ulid,
        format: StoredFormat {
            layout: StoredLayout::Plain(ulid),
            ..StoredFormat::default()
        },
        created_by: context.user_id,
        created_at: UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: [(HASH_BLAKE3.to_string(), vec![5; 32])].into(),
    };
    let version = BlobVersion::materialized(
        [5; 32],
        location.backend.clone(),
        location.format.encoding(),
        SystemTime::now(),
        context.user_id,
        None,
    );
    let rows = [
        (
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket", key, version_id)
                .to_bytes()
                .unwrap(),
            version.to_bytes().unwrap(),
        ),
        (
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket", key).to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        ),
        (
            BLOB_LOCATIONS_KEYSPACE,
            location.location_key().unwrap().to_bytes(),
            location.to_bytes().unwrap(),
        ),
        (
            BACKEND_VERSION_KEYSPACE,
            version_key(ulid).to_vec(),
            b"p1".to_vec(),
        ),
        (
            aruna_core::keyspaces::PATHS_INDEX_KEYSPACE,
            aruna_core::structs::storage::blob::HashIndex::new(
                [5; 32],
                version_id,
                context.realm_id,
                context.group_id,
                context.node_id,
                "bucket",
                key,
            )
            .to_bytes()
            .unwrap(),
            Vec::new(),
        ),
    ];
    for (space, key, value) in rows {
        write_value(storage, space, key, value).await;
    }
    location
}

/// The file releases the cleanup queue holds.
async fn releases(storage: &StorageHandle) -> Vec<BackendLocation> {
    let Event::Storage(StorageEvent::IterResult { values, .. }) = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 100,
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected cleanup scan")
    };
    values
        .into_iter()
        .filter_map(
            |(_, value)| match BlobCleanupWork::from_bytes(&value).unwrap() {
                BlobCleanupWork::ReleasePlanned { location, .. } => Some(location),
                _ => None,
            },
        )
        .collect()
}

fn files_spec(context: &TestContext) -> StoragePurgeSpec {
    StoragePurgeSpec {
        scope: StoragePurgeScope::Prefix {
            bucket: "bucket".to_string(),
            prefix: "linked/".to_string(),
        },
        group_id: context.group_id,
        auth_context: auth(context),
        node_id: context.node_id,
    }
}

#[tokio::test]
async fn purge_releases_files() {
    let context = setup_context().await;
    let version_id = Ulid::generate();
    let location = seed_linked(&context, "linked/a.txt", version_id).await;
    let spec = files_spec(&context);
    let job = claimed_job(
        context.driver.clone(),
        JobId::from_bytes([0xE1; 16]),
        spec.clone(),
    )
    .await;

    purge_result(run_storage_purge(&job, &spec, true).await);

    let storage = &context.driver.storage_handle;
    let version = VersionKey::new("bucket", "linked/a.txt", version_id);
    let version = version.to_bytes().unwrap();
    assert!(
        read_value(storage, BLOB_VERSIONS_KEYSPACE, version)
            .await
            .is_none()
    );
    // The file waits in the cleanup queue for its release, which deletes only its exact
    // backend version once the purge has ended.
    assert_eq!(releases(storage).await, [location]);
}

#[tokio::test]
async fn aborted_purge_releases() {
    let context = setup_context().await;
    let location = seed_linked(&context, "linked/a.txt", Ulid::generate()).await;
    let spec = files_spec(&context);
    let job = claimed_job(
        context.driver.clone(),
        JobId::from_bytes([0xE2; 16]),
        spec.clone(),
    )
    .await;
    // A page planned before the abort stays in the cleanup queue, released once it ended.
    let versions: Vec<_> = list_linked(&context).await;
    let found = aruna_operations::blob::purge::storage_files(
        &context.driver,
        ("bucket", context.group_id),
        &versions,
        None,
    )
    .await
    .unwrap();
    aruna_operations::blob::purge::plan_files(&context.driver, job.job_id.as_ulid(), &found)
        .await
        .unwrap();
    aruna_operations::blob::purge::set_paused(&context.driver, job.job_id.as_ulid(), true)
        .await
        .unwrap();
    job.cancel.cancel();

    let outcome = run_storage_purge(&job, &spec, true).await;

    assert!(matches!(outcome, JobRunOutcome::Cancelled));
    let storage = &context.driver.storage_handle;
    assert_eq!(releases(storage).await, [location]);
    let paused = aruna_operations::blob::purge::is_paused(&context.driver, job.job_id.as_ulid());
    assert!(!paused.await.unwrap());
}

/// The version ids of the seeded linked key.
async fn list_linked(context: &TestContext) -> Vec<(String, Ulid)> {
    let prefix = VersionKey::object_prefix("bucket", "linked/a.txt").unwrap();
    let Event::Storage(StorageEvent::IterResult { values, .. }) = context
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected version scan")
    };
    values
        .into_iter()
        .map(|(key, _)| {
            let key = VersionKey::from_bytes(&key).unwrap();
            (key.key, key.version_id)
        })
        .collect()
}

#[tokio::test]
async fn cancelled_queue_releases() {
    // A purge cancelled while queued ends at once; the files it planned before are released
    // by the cleanup queue, because the job ended.
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::service::cancel_owned_job;
    let context = setup_context().await;
    seed_linked(&context, "linked/a.txt", Ulid::generate()).await;
    let spec = files_spec(&context);
    let job_id = JobId::from_bytes([0xE3; 16]);
    let record = JobRecord::new(
        job_id,
        JobPayload::StoragePurgeFiles(spec),
        context.user_id,
        context.node_id,
        1,
        1,
        None,
    );
    insert_job(&context.driver.storage_handle, &record)
        .await
        .unwrap();
    let versions = list_linked(&context).await;
    let found = aruna_operations::blob::purge::storage_files(
        &context.driver,
        ("bucket", context.group_id),
        &versions,
        None,
    )
    .await
    .unwrap();
    aruna_operations::blob::purge::plan_files(&context.driver, job_id.as_ulid(), &found)
        .await
        .unwrap();
    let storage = &context.driver.storage_handle;
    assert_eq!(releases(storage).await.len(), 1);

    let runtime = JobsRuntime::new_paused();
    cancel_owned_job(&context.driver, &runtime, context.user_id, job_id)
        .await
        .unwrap();
    let stored = aruna_operations::jobs::store::read_job_record(storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.state.is_terminal());
    // The version still reads its file, so the release keeps it and the plan is settled.
    let backend = aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id: Ulid::from_parts(3, 3),
        group_id: context.group_id,
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: HashMap::new(),
        created_at: UNIX_EPOCH,
        updated_at: UNIX_EPOCH,
        created_by: context.user_id,
        disabled: false,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    };
    write_value(
        storage,
        aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE,
        aruna_operations::groups::backends::backend_key(backend.backend_id).to_vec(),
        backend.to_bytes().unwrap(),
    )
    .await;
    let outcome = aruna_operations::blob::cleanup::process_cleanup_batch(&context.driver)
        .await
        .unwrap();
    assert_eq!((outcome.processed, outcome.dropped), (1, 0), "{outcome:?}");
    assert!(releases(storage).await.is_empty());
}

/// Records a second copy of the version `version_id` of `key` at `path`, as an import does
/// for a plain file with the version's bytes, with the rule that links the folder.
async fn seed_copy(
    context: &TestContext,
    (key, version_id): (&str, Ulid),
    linked: &BackendLocation,
) -> BackendLocation {
    use aruna_core::keyspaces::{
        BACKEND_IMPORT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, RENAMED_ROW_KEYSPACE,
    };
    use aruna_core::structs::storage::backend_import::{
        BackendImport, DEFAULT_PATTERN, ImportedObject, object_key,
    };
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    let storage = &context.driver.storage_handle;
    let BackendRef::Group(backend_id) = linked.backend else {
        panic!("not a group storage")
    };
    let backend = GroupStorage {
        backend_id,
        group_id: context.group_id,
        name: "tenant".to_string(),
        kind: GroupBackendKind::S3,
        public_config: Default::default(),
        created_at: UNIX_EPOCH,
        updated_at: UNIX_EPOCH,
        created_by: context.user_id,
        disabled: false,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    };
    for (space, key, value) in aruna_operations::groups::backends::record_writes(&backend).unwrap()
    {
        write_value(storage, &space, key.to_vec(), value.to_vec()).await;
    }
    let rule = BackendImport {
        import_id: Ulid::from_parts(4, 4),
        group_id: context.group_id,
        backend_id,
        backend_prefix: "raw/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: context.user_id,
        created_at: UNIX_EPOCH,
        updated_at: UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: DEFAULT_PATTERN.to_string(),
    };
    write_value(
        storage,
        BACKEND_IMPORT_KEYSPACE,
        rule.key().to_vec(),
        rule.to_bytes().unwrap(),
    )
    .await;
    let ulid = Ulid::generate();
    let copy = BackendLocation {
        backend_path: format!("raw/{key}.copy"),
        ulid,
        format: StoredFormat {
            layout: StoredLayout::Plain(ulid),
            ..StoredFormat::default()
        },
        ..linked.clone()
    };
    let row = ImportedObject::written(version_id, key.to_string(), copy.blob_size, None);
    let prefix = VersionKey::object_prefix("bucket", key).unwrap();
    let index = [
        &rule.import_id.to_bytes()[..],
        &prefix[..],
        copy.backend_path.as_bytes(),
    ]
    .concat();
    let rows = [
        (
            IMPORTED_OBJECT_KEYSPACE,
            object_key(rule.import_id, &copy.backend_path).to_vec(),
            row.to_bytes().unwrap(),
        ),
        (RENAMED_ROW_KEYSPACE, index, copy.to_bytes().unwrap()),
        (
            BACKEND_VERSION_KEYSPACE,
            version_key(ulid).to_vec(),
            b"p2".to_vec(),
        ),
    ];
    for (space, key, value) in rows {
        write_value(storage, space, key, value).await;
    }
    copy
}

#[tokio::test]
async fn purge_releases_copies() {
    // A second copy of a purged version is counted, planned for release and unindexed.
    let context = setup_context().await;
    let version_id = Ulid::generate();
    let location = seed_linked(&context, "linked/a.txt", version_id).await;
    let copy = seed_copy(&context, ("linked/a.txt", version_id), &location).await;
    let spec = files_spec(&context);
    let readable = |_: &str| true;
    let totals =
        aruna_operations::blob::purge::scope_totals(&context.driver, &spec.scope, &readable)
            .await
            .unwrap();
    assert_eq!((totals.storage_files, totals.storage_bytes), (2, 8));
    let job = claimed_job(
        context.driver.clone(),
        JobId::from_bytes([0xE6; 16]),
        spec.clone(),
    )
    .await;

    purge_result(run_storage_purge(&job, &spec, true).await);

    let storage = &context.driver.storage_handle;
    let mut released = releases(storage).await;
    released.sort_by_key(|file| file.backend_path.clone());
    assert_eq!(released, [location, copy]);
    let (left, _) = aruna_operations::jobs::store::iter_prefix_page(
        storage,
        aruna_core::keyspaces::RENAMED_ROW_KEYSPACE,
        None,
        None,
        10,
        None,
    )
    .await
    .unwrap();
    assert!(left.is_empty());
}
