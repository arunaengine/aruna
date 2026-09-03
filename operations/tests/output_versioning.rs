// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
use std::collections::HashMap;
use std::sync::Arc;

use aruna_blob::blob::BlobHandler;
use aruna_compute::ExecutorBackend;
use aruna_core::UserId;
use aruna_core::compute::{
    AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits,
    LogTails, NOBODY, ReconcileEvidence, TaskInput, TaskOutput, TaskSpec, UserSpec,
};
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    Actor, AttemptControl, AttemptIntent, Backend, BackendConfig, BucketInfo, ExecutionSpec,
    FIRST_GRANTABLE_HANDLE, Group, GroupAuthorizationDocument, InputMode, InputSelection,
    InputSource, JobClaim, JobId, JobInputFact, JobPayload, JobRecord, JobState, OutputDestination,
    OutputSelection, RealmAuthorizationDocument, RealmConfigDocument, RealmId, RoutingSnapshot,
    WorkspaceMode, checksum::HASH_BLAKE3,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::{GroupId, NodeId};
use aruna_core::util::unix_timestamp_millis;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::store::{insert_job, record_attempt_intent, reserve_output_commits};
use aruna_operations::jobs::submit::mint_job_id;
use aruna_operations::jobs::workflow::workspace::{capture_outputs, load_direct_inputs};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::head_object::{HeadObjectInput, HeadObjectOperation, HeadObjectResult};
use aruna_operations::s3::list_buckets::{ListBucketsInput, ListBucketsOperation};
use aruna_operations::s3::list_object_versions::{
    ListObjectVersionsInput, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use aruna_storage::storage;
use futures_util::StreamExt;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const BUCKET: &str = "outputs";
const KEY: &str = "run/result.txt";
const WORKSPACE: &str = "run-workspace";
const INPUT_PATH: &str = "/in/data.csv";
const RESULTS: &str = "results";
const OUTPUT_PATH: &str = "/out/report.txt";
const OUTPUT_BYTES: &[u8] = b"captured-report";

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
    create_bucket(&harness, BUCKET).await;
    harness
}

async fn create_bucket(harness: &Harness, bucket: &str) {
    drive(
        CreateBucketOperation::new(
            bucket.to_string(),
            BucketInfo {
                group_id: harness.group_id,
                created_at: std::time::SystemTime::now(),
                created_by: harness.created_by,
                cors_configuration: None,
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
async fn seed_execution(harness: &Harness, spec: ExecutionSpec) -> (JobRecord, AttemptControl) {
    let job_id = mint_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap();
    let token = Ulid::generate();
    let input_facts = seal_facts(harness, &spec).await;
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(spec),
        harness.created_by,
        harness.node_id,
        1,
        1,
        None,
    );
    record.input_facts = input_facts;
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
        None,
        unix_timestamp_millis(),
    )
    .await
    .unwrap();
    (commit.record, commit.control)
}

/// The facts admission seals for a run. A capture inherits its output refs
/// from them, so a seeded record carries the same ones.
async fn seal_facts(harness: &Harness, spec: &ExecutionSpec) -> Vec<JobInputFact> {
    let mut facts = Vec::with_capacity(spec.inputs.len());
    for input in &spec.inputs {
        let InputSource::S3 { version_id, .. } = &input.source;
        let version = version_id
            .as_deref()
            .map(|version| Ulid::from_string(version).unwrap());
        let head = head_object(harness, version).await;
        let location = head.location.as_ref().expect("input is materialized");
        facts.push(JobInputFact {
            destination_key: input.dest_key.clone(),
            source_node_id: harness.node_id,
            version_id: head
                .resolved_version_id
                .or(head.version_id)
                .expect("input has a version"),
            blake3: <[u8; 32]>::try_from(
                location
                    .hashes
                    .get(HASH_BLAKE3)
                    .expect("blake3 is stored")
                    .as_slice(),
            )
            .expect("blake3 is 32 bytes"),
            bytes: location.blob_size,
            policies: head.source_policies.clone(),
        });
    }
    facts
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

/// The realm documents an input read authorizes against.
async fn seed_auth(harness: &Harness) {
    let actor = Actor {
        node_id: harness.node_id,
        user_id: harness.created_by,
        realm_id: harness.realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(harness.realm_id);
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(
        harness.created_by,
        harness.realm_id,
        harness.group_id,
    );
    let group = Group {
        display_name: "inputs".to_string(),
        group_id: harness.group_id,
        realm_id: harness.realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: harness.created_by,
    };
    let realm_config = RealmConfigDocument::default_for_realm(harness.realm_id, Vec::new());
    for (key_space, key, value) in [
        (
            AUTH_KEYSPACE,
            harness.realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            harness.group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        ),
        (
            REALM_CONFIG_KEYSPACE,
            harness.realm_id.as_bytes().to_vec(),
            realm_config.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            harness.group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
    ] {
        harness
            .driver
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }
}

/// One execution reading the object at exactly the version the launch sealed.
fn pinned_spec(harness: &Harness, version: Ulid, mode: InputMode) -> ExecutionSpec {
    let mut spec = execution_spec(harness.group_id);
    spec.inputs.push(InputSelection {
        source: InputSource::S3 {
            bucket: BUCKET.to_string(),
            key: KEY.to_string(),
            version_id: Some(version.to_string()),
        },
        source_node_id: None,
        dest_key: INPUT_PATH[1..].to_string(),
        mode,
        container_path: Some(INPUT_PATH.to_string()),
        name: None,
        description: None,
    });
    spec
}

fn job_record(harness: &Harness, spec: &ExecutionSpec) -> JobRecord {
    JobRecord::new(
        JobId::from_bytes([4u8; 16]),
        JobPayload::Execution(spec.clone()),
        harness.created_by,
        harness.node_id,
        1,
        1,
        None,
    )
}

async fn read_input(input: TaskInput) -> Vec<u8> {
    let mut stream = input.take_stream().unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    bytes
}

/// Open the pinned input exactly as a launch does, so the bytes are the ones
/// the container would receive.
async fn staged_bytes(harness: &Harness, version: Ulid) -> Vec<u8> {
    let spec = pinned_spec(harness, version, InputMode::Snapshot);
    let record = job_record(harness, &spec);
    let mut inputs = load_direct_inputs(&harness.driver, &spec, &record, harness.node_id)
        .await
        .unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].path, INPUT_PATH);
    read_input(inputs.remove(0)).await
}

#[tokio::test]
async fn stages_pinned_version() {
    // A later write to the same key must not reach a launch sealed on the first.
    let harness = setup().await;
    seed_auth(&harness).await;
    let first = put_version(&harness, b"first-version", None).await;
    put_version(&harness, b"second-version", None).await;

    assert_eq!(
        staged_bytes(&harness, first.version_id).await,
        b"first-version"
    );
}

#[tokio::test]
async fn stages_later_version() {
    let harness = setup().await;
    seed_auth(&harness).await;
    put_version(&harness, b"first-version", None).await;
    let second = put_version(&harness, b"second-version", None).await;

    assert_eq!(
        staged_bytes(&harness, second.version_id).await,
        b"second-version"
    );
}

#[tokio::test]
async fn mounts_stage_pinned() {
    // A mounted input the launch pinned is staged read-only instead of mounted:
    // a mount serves the current head and could never serve this version.
    let harness = setup().await;
    seed_auth(&harness).await;
    let first = put_version(&harness, b"first-version", None).await;
    put_version(&harness, b"second-version", None).await;
    let spec = pinned_spec(&harness, first.version_id, InputMode::Mount);
    let record = job_record(&harness, &spec);

    let mut inputs = load_direct_inputs(&harness.driver, &spec, &record, harness.node_id)
        .await
        .unwrap();

    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].path, INPUT_PATH);
    assert_eq!(read_input(inputs.remove(0)).await, b"first-version");
}

#[tokio::test]
async fn replay_reuses_version() {
    // A capture interrupted after its reservation must replay into the same version.
    let harness = setup().await;
    let (record, control) = seed_execution(&harness, execution_spec(harness.group_id)).await;
    let job_id = record.job_id;
    let destinations = vec![(harness.node_id, BUCKET.to_string(), KEY.to_string())];

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
    let destinations = vec![(harness.node_id, BUCKET.to_string(), KEY.to_string())];
    let (first, _) = seed_execution(&harness, execution_spec(harness.group_id)).await;
    let (second, _) = seed_execution(&harness, execution_spec(harness.group_id)).await;
    let (first_job, second_job) = (first.job_id, second.job_id);

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
    let (record, _) = seed_execution(&harness, execution_spec(harness.group_id)).await;
    let job_id = record.job_id;
    let destinations = vec![(harness.node_id, BUCKET.to_string(), KEY.to_string())];

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

/// A terminal attempt whose declared output is one fixed byte string.
struct StubBackend;

#[async_trait::async_trait]
impl ExecutorBackend for StubBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Docker
    }
    fn run_identity(&self) -> UserSpec {
        NOBODY
    }
    async fn health(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn resolve_image(
        &self,
        image: &str,
        _cancel: &CancellationToken,
    ) -> Result<String, BackendError> {
        Ok(image.to_string())
    }
    async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }
    async fn submit(
        &self,
        _context: &FenceContext,
        _spec: &TaskSpec,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::Unavailable("stub submit".to_string()))
    }
    async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::Unavailable("stub status".to_string()))
    }
    async fn cancel(&self, _context: &FenceContext) -> Result<CancelEvidence, BackendError> {
        Ok(CancelEvidence::AlreadyGone)
    }
    async fn fetch_logs(
        &self,
        _context: &FenceContext,
        _limits: &LogLimits,
    ) -> Result<LogTails, BackendError> {
        Ok(LogTails::default())
    }
    async fn fetch_output(
        &self,
        _context: &FenceContext,
        _path: &str,
    ) -> Result<TaskOutput, BackendError> {
        Ok(TaskOutput {
            size: OUTPUT_BYTES.len() as u64,
            chunks: Box::pin(futures_util::stream::iter(vec![Ok(
                bytes::Bytes::from_static(OUTPUT_BYTES),
            )])),
        })
    }
    async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
        ReconcileEvidence::Absent
    }
    async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }
}

/// Every bucket this group owns after a run.
async fn list_buckets(harness: &Harness) -> Vec<String> {
    drive(
        ListBucketsOperation::new(ListBucketsInput {
            group_id: harness.group_id,
            prefix: None,
            continuation_token: None,
            max_buckets: None,
        }),
        &harness.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
    .buckets
    .into_iter()
    .map(|(name, _)| name)
    .collect()
}

/// One run that stages its pinned input and captures its declared output, with
/// the bucket the record names (empty for a run that owns none).
async fn run_capture(
    harness: &Harness,
    mode: InputMode,
    key: &str,
    workspace: Option<&str>,
) -> Vec<aruna_core::structs::OutputObject> {
    let pinned = put_version(harness, b"input-bytes", None).await;
    let mut spec = pinned_spec(harness, pinned.version_id, mode);
    spec.file_outputs.push(OutputSelection {
        container_path: OUTPUT_PATH.to_string(),
        path_prefix: None,
        destination_node_id: Some(harness.node_id),
        destination: OutputDestination::S3 {
            bucket: RESULTS.to_string(),
            key: key.to_string(),
        },
        name: None,
        description: None,
    });
    let (mut record, control) = seed_execution(harness, spec.clone()).await;
    if let Some(bucket) = workspace {
        record.workspace_mode = WorkspaceMode::Existing;
        record.workspace_bucket = Some(bucket.to_string());
    }
    let fence = FenceContext {
        attempt: AttemptRef::new(record.job_id.to_string().to_lowercase(), 1),
        attempt_epoch: control.attempt_epoch,
        controller_generation: 1,
    };
    let backend: Arc<dyn ExecutorBackend> = Arc::new(StubBackend);

    let mut inputs = load_direct_inputs(&harness.driver, &spec, &record, harness.node_id)
        .await
        .unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].path, INPUT_PATH);
    assert_eq!(read_input(inputs.remove(0)).await, b"input-bytes");
    capture_outputs(
        &harness.driver,
        &backend,
        &fence,
        &spec,
        &record,
        harness.node_id,
    )
    .await
    .unwrap()
}

/// Whether the object exists at all, at any version.
async fn object_exists(harness: &Harness, bucket: &str, key: &str) -> bool {
    matches!(
        drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
            }),
            &harness.driver,
        )
        .await,
        Ok(Some(Ok(_)))
    )
}

#[tokio::test]
async fn captures_without_workspace() {
    // A none-mode run reads its pinned input through file staging whatever the
    // input mode, and writes the output into the bucket it named.
    let harness = setup().await;
    seed_auth(&harness).await;
    create_bucket(&harness, RESULTS).await;

    for (mode, key) in [
        (InputMode::Mount, "mounted.txt"),
        (InputMode::Snapshot, "snapshot.txt"),
    ] {
        let outputs = run_capture(&harness, mode, key, None).await;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].bucket, RESULTS);
        assert_eq!(outputs[0].size, OUTPUT_BYTES.len() as u64);
    }

    let buckets = list_buckets(&harness).await;
    assert!(buckets.contains(&RESULTS.to_string()), "{buckets:?}");
    assert!(
        buckets.iter().all(|bucket| !bucket.starts_with("ws-")),
        "{buckets:?}"
    );
}

#[tokio::test]
async fn existing_keeps_clean() {
    // An existing-mode run reads its input from the source bucket; the bucket it
    // works inside never receives a staged copy.
    let harness = setup().await;
    seed_auth(&harness).await;
    create_bucket(&harness, RESULTS).await;
    create_bucket(&harness, WORKSPACE).await;

    let outputs = run_capture(&harness, InputMode::Snapshot, "report.txt", Some(WORKSPACE)).await;

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].bucket, RESULTS);
    assert!(
        !object_exists(&harness, WORKSPACE, &INPUT_PATH[1..]).await,
        "the workspace bucket must hold no copy of the input"
    );
}
