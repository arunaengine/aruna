// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
use std::collections::HashMap;

use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    AttemptControl, AttemptIntent, Backend, BackendConfig, BucketInfo, ExecutionSpec,
    FIRST_GRANTABLE_HANDLE, JobClaim, JobId, JobPayload, JobRecord, JobState, RealmId,
    RoutingSnapshot,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::{GroupId, NodeId};
use aruna_core::util::unix_timestamp_millis;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::store::{insert_job, record_attempt_intent, reserve_output_commits};
use aruna_operations::jobs::submit::mint_job_id;
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::head_object::{HeadObjectInput, HeadObjectOperation, HeadObjectResult};
use aruna_operations::s3::list_object_versions::{
    ListObjectVersionsInput, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use aruna_storage::storage;
use tempfile::TempDir;
use ulid::Ulid;

const BUCKET: &str = "outputs";
const KEY: &str = "run/result.txt";

struct Harness {
    _temp_dir: TempDir,
    driver: DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    created_by: UserId,
    group_id: GroupId,
}

async fn setup() -> Harness {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();

    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root,
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("uploaded-parts".to_string()),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await
    .unwrap();

    let realm_id = RealmId::from_bytes([7u8; 32]);
    let node_id = net_handle.node_id();
    let harness = Harness {
        _temp_dir: temp_dir,
        driver: DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        },
        realm_id,
        node_id,
        created_by: UserId::local(Ulid::generate(), realm_id),
        group_id: Ulid::generate(),
    };
    create_bucket(&harness).await;
    harness
}

async fn create_bucket(harness: &Harness) {
    drive(
        CreateBucketOperation::new(
            BUCKET.to_string(),
            BucketInfo {
                group_id: harness.group_id,
                created_at: std::time::SystemTime::now(),
                created_by: harness.created_by,
                cors_configuration: None,
                replication: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            },
        ),
        &harness.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
}

fn execution_spec(group_id: GroupId) -> ExecutionSpec {
    ExecutionSpec {
        group_id,
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    }
}

/// One claimed job whose attempt intent and control row are already write-ahead,
/// which is the state every output capture starts from.
async fn seed_execution(harness: &Harness) -> (JobId, AttemptControl) {
    let job_id = mint_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap();
    let token = Ulid::generate();
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(execution_spec(harness.group_id)),
        harness.created_by,
        harness.node_id,
        1,
        1,
        None,
    );
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: harness.node_id,
        claim_token: token,
        lease_expires_at_ms: unix_timestamp_millis() + 60_000,
    });
    insert_job(&harness.driver.storage_handle, &record)
        .await
        .unwrap();
    let commit = record_attempt_intent(
        &harness.driver.storage_handle,
        job_id,
        token,
        AttemptIntent {
            attempt_no: 1,
            external_name: job_id.to_string().to_lowercase(),
            executor_kind: "docker".to_string(),
            pinned_image: "alpine@sha256:digest".to_string(),
            attempt_epoch: 0,
        },
        unix_timestamp_millis(),
    )
    .await
    .unwrap();
    (job_id, commit.control)
}

fn body(bytes: &[u8]) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

async fn put_version(
    harness: &Harness,
    content: &[u8],
    version_id: Option<Ulid>,
) -> PutObjectResult {
    drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: harness.created_by,
            group_id: harness.group_id,
            realm_id: harness.realm_id,
            node_id: harness.node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: KEY.to_string(),
                content_length: Some(content.len() as u64),
                body: Some(body(content)),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: version_id,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(harness.group_id),
        }),
        &harness.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

async fn head_object(harness: &Harness, version_id: Option<Ulid>) -> HeadObjectResult {
    drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: BUCKET.to_string(),
            key: KEY.to_string(),
            version_id,
        }),
        &harness.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

async fn list_versions(harness: &Harness) -> Vec<Ulid> {
    let result = drive(
        ListObjectVersionsOperation::new(ListObjectVersionsInput {
            bucket: BUCKET.to_string(),
            prefix: Some(KEY.to_string()),
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: None,
        }),
        &harness.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    result
        .items
        .into_iter()
        .filter_map(|item| match item {
            ListObjectVersionsItem::Version { version_id, .. } => Some(version_id),
            ListObjectVersionsItem::DeleteMarker { .. } => None,
        })
        .collect()
}

#[tokio::test]
async fn replay_reuses_version() {
    // A capture interrupted after its reservation must replay into the same version.
    let harness = setup().await;
    let (job_id, control) = seed_execution(&harness).await;
    let destinations = vec![(BUCKET.to_string(), KEY.to_string())];

    let reserved = reserve_output_commits(&harness.driver.storage_handle, job_id, &destinations)
        .await
        .unwrap();
    assert!(!reserved.execution_id.is_nil());
    assert_eq!(reserved.execution_id, control.execution_id);
    let version_id = reserved.output_commits[0].version_id;
    assert!(!version_id.is_nil());

    let first = put_version(&harness, b"result", Some(version_id)).await;

    // Replay after a crash: the reservation is re-read, never minted again.
    let replayed = reserve_output_commits(&harness.driver.storage_handle, job_id, &destinations)
        .await
        .unwrap();
    assert_eq!(replayed.output_commits, reserved.output_commits);
    let second = put_version(&harness, b"result", Some(version_id)).await;

    assert_eq!(first.version_id, version_id);
    assert_eq!(second.version_id, version_id);
    assert_eq!(list_versions(&harness).await, vec![version_id]);
}

#[tokio::test]
async fn executions_keep_versions() {
    // Two physical executions writing one key keep two independent exact versions.
    let harness = setup().await;
    let destinations = vec![(BUCKET.to_string(), KEY.to_string())];
    let (first_job, _) = seed_execution(&harness).await;
    let (second_job, _) = seed_execution(&harness).await;

    let first = reserve_output_commits(&harness.driver.storage_handle, first_job, &destinations)
        .await
        .unwrap();
    let second = reserve_output_commits(&harness.driver.storage_handle, second_job, &destinations)
        .await
        .unwrap();
    assert_ne!(first.execution_id, second.execution_id);
    let first_version = first.output_commits[0].version_id;
    let second_version = second.output_commits[0].version_id;
    assert_ne!(first_version, second_version);

    put_version(&harness, b"from-first", Some(first_version)).await;
    put_version(&harness, b"from-second-execution", Some(second_version)).await;

    let mut versions = list_versions(&harness).await;
    versions.sort();
    let mut expected = vec![first_version, second_version];
    expected.sort();
    assert_eq!(versions, expected);
    assert_eq!(
        head_object(&harness, Some(first_version))
            .await
            .location
            .unwrap()
            .blob_size,
        b"from-first".len() as u64
    );
    assert_eq!(
        head_object(&harness, Some(second_version))
            .await
            .location
            .unwrap()
            .blob_size,
        b"from-second-execution".len() as u64
    );
}

#[tokio::test]
async fn exact_version_survives() {
    // A later unrelated write may take S3 latest; the output stays exact-retrievable.
    let harness = setup().await;
    let (job_id, _) = seed_execution(&harness).await;
    let destinations = vec![(BUCKET.to_string(), KEY.to_string())];

    let reserved = reserve_output_commits(&harness.driver.storage_handle, job_id, &destinations)
        .await
        .unwrap();
    let version_id = reserved.output_commits[0].version_id;
    put_version(&harness, b"job-output", Some(version_id)).await;

    let unrelated = put_version(&harness, b"someone-elses-write", None).await;
    assert_ne!(unrelated.version_id, version_id);

    assert_eq!(
        head_object(&harness, None).await.version_id,
        Some(unrelated.version_id)
    );
    let exact = head_object(&harness, Some(version_id)).await;
    assert_eq!(exact.version_id, Some(version_id));
    assert_eq!(
        exact.location.unwrap().blob_size,
        b"job-output".len() as u64
    );
}
