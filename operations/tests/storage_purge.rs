#![recursion_limit = "256"]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_PURGE_CHECKPOINT_KEYSPACE, S3_PURGE_FENCE_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AuthContext, BackendRef, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, JobId,
    JobPayload, JobRecord, JobResultPayload, MultipartChecksumType, MultipartUpload,
    MultipartUploadStatus, RealmId, RoutingSnapshot, StoragePurgeCheckpoint, StoragePurgeScope,
    StoragePurgeSpec, VersionKey,
};
use aruna_core::types::{GroupId, NodeId, UserId};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::store::{
    ClaimOutcome, claim_job, complete_job, insert_job, put_purge_checkpoint, transition_to_running,
};
use aruna_operations::jobs::workflow::purge::run_storage_purge;
use aruna_operations::s3::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadInput, CompleteMultipartUploadOperation,
};
use aruna_operations::s3::copy_object::{
    CopyObjectError, CopyObjectInput, CopySourceConditions, copy_object,
};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::create_multipart_upload::{
    CreateMultipartUploadError, CreateMultipartUploadInput, CreateMultipartUploadOperation,
};
use aruna_operations::s3::delete_object::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation,
};
use aruna_operations::s3::delete_objects::{
    DeleteObjectsEntry, DeleteObjectsInput, delete_objects,
};
use aruna_operations::s3::purge_fence::{PurgeFenceError, acquire_purge_fence, fence_key};
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation,
};
use aruna_operations::s3::upload_part::{UploadPartError, UploadPartInput, UploadPartOperation};
use aruna_operations::s3::upload_part_copy::{
    UploadPartCopyError, UploadPartCopyInput, upload_part_copy,
};
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
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let group_id = Ulid::generate();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let driver = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
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
            },
        ),
        &driver,
    )
    .await
    .unwrap()
    .unwrap()
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
async fn scoped_fence_rejects_racing_writes_without_freezing_other_prefixes() {
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
            UploadPartCopyInput {
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
        Err(UploadPartCopyError::UploadPart(
            UploadPartError::PurgeFence(PurgeFenceError::Suspended)
        ))
    ));

    assert!(matches!(
        drive(
            CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
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
        Err(CreateMultipartUploadError::PurgeFence(
            PurgeFenceError::Suspended
        ))
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
                compressed: false,
                encrypted: false,
                expected_checksums: Vec::new(),
            }),
            &context.driver,
        )
        .await,
        Err(UploadPartError::PurgeFence(PurgeFenceError::Suspended))
    ));

    assert!(matches!(
        drive(
            CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
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
            }),
            &context.driver,
        )
        .await,
        Err(CompleteMultipartUploadError::PurgeFence(
            PurgeFenceError::Suspended
        ))
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
        DeleteObjectsInput {
            bucket: "bucket".to_string(),
            entries: vec![DeleteObjectsEntry {
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
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
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
    .unwrap()
    .unwrap()
    .record;
    assert_eq!(outside_upload.key, "allowed/outside.bin");
    let stored_upload = read_value(
        &context.driver.storage_handle,
        S3_MULTIPART_UPLOAD_KEYSPACE,
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
async fn purge_resumes_aborts_uploads_preserves_prefix_neighbors_and_deletes_bucket() {
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
    .unwrap()
    .unwrap()
    .unwrap();

    let first = purge_result(run_storage_purge(&prefix_job, &prefix_spec).await);
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
            S3_MULTIPART_UPLOAD_KEYSPACE,
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

    let resumed = purge_result(run_storage_purge(&prefix_job, &prefix_spec).await);
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
            S3_PURGE_FENCE_KEYSPACE,
            fence_key("bucket").as_ref().to_vec(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver.storage_handle,
            S3_PURGE_CHECKPOINT_KEYSPACE,
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
    let bucket_result = purge_result(run_storage_purge(&bucket_job, &bucket_spec).await);
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

fn purge_result(outcome: JobRunOutcome) -> aruna_core::structs::StoragePurgeResult {
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::StoragePurge(result)) => result,
        JobRunOutcome::Succeeded(_) => panic!("purge returned the wrong result kind"),
        JobRunOutcome::Failed(error) => panic!("purge failed: {}", error.message),
        JobRunOutcome::Cancelled => panic!("purge was cancelled"),
        JobRunOutcome::Interrupted => panic!("purge was interrupted"),
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
    };
    write_value(
        storage,
        S3_MULTIPART_UPLOAD_KEYSPACE,
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
