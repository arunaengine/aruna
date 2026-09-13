use super::*;
use std::time::Duration;

use axum::body::to_bytes;

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_ACCESS_KEYSPACE,
};
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, JobError, NodeCapabilities, OutputObject,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, UserAccess,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::store::insert_job;
use aruna_storage::FjallStorage;
use tempfile::TempDir;

fn realm() -> RealmId {
    RealmId([1u8; 32])
}

fn node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[7u8; 32]).public()
}

fn user(byte: u8) -> UserId {
    UserId::new(Ulid::from_bytes([byte; 16]), realm())
}

fn auth_for(user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    })
}

const TES_SECRET: &str = "tes-secret";

fn credential(group_id: Ulid) -> UserAccess {
    let user_identity = user(2);
    UserAccess {
        access_key: UserAccess::build_access_key("tes").unwrap(),
        user_identity,
        group_id,
        secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
        expiry: SystemTime::now() + Duration::from_secs(60),
        path_restrictions: None,
        issued_by: *node_id().as_bytes(),
        revoked_at: None,
    }
}

/// A credential whose secret is encrypted with the node's issuer-local key,
/// as the create-credential path would have produced.
fn issued_access(state: &ServerState, group_id: Ulid) -> UserAccess {
    let mut access = credential(group_id);
    access
        .encrypt_secret(state.credential_encryption_key(), TES_SECRET)
        .unwrap();
    access
}

fn basic_headers(access: &UserAccess, secret: &str) -> HeaderMap {
    let encoded = STANDARD.encode(format!("{}:{secret}", access.access_key));
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Basic {encoded}").parse().unwrap());
    headers
}

async fn write_credential(state: &ServerState, access: &UserAccess) {
    let _ = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: USER_ACCESS_KEYSPACE.to_string(),
            key: access.access_key.as_bytes().into(),
            value: access.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
}

async fn write_auth(state: &ServerState, group_id: Ulid, owner: UserId) {
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: owner,
        realm_id: realm(),
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm());
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm(), group_id);
    let group = Group {
        display_name: "tes-group".to_string(),
        group_id,
        realm_id: realm(),
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    // Request-policy loading fails closed without the realm config, the group
    // record, and the group auth document.
    for (key_space, key, value) in [
        (
            REALM_CONFIG_KEYSPACE,
            realm().as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm(), Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            realm().as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
    ] {
        let _ = state
            .get_ctx()
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

async fn build_state() -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let state = ServerState::new(
        ctx,
        realm(),
        node_id(),
        NodeCapabilities::user_node(realm()).unwrap(),
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state))
}

fn sample_task(group: Ulid) -> TesTask {
    TesTask {
        name: Some("align reads".to_string()),
        description: Some("sample task".to_string()),
        executors: vec![TesExecutor {
            image: "alpine:3".to_string(),
            command: vec!["echo".to_string(), "hi".to_string()],
            workdir: Some("/work".to_string()),
            env: BTreeMap::from([("K".to_string(), "V".to_string())]),
            ..Default::default()
        }],
        inputs: vec![TesInput {
            name: Some("reads".to_string()),
            description: Some("input reads".to_string()),
            url: Some("s3://src/data.csv".to_string()),
            path: "/in/data.csv".to_string(),
            kind: TesFileType::File,
            ..Default::default()
        }],
        outputs: vec![TesOutput {
            name: Some("report".to_string()),
            description: Some("output report".to_string()),
            url: Some("s3://dest/out/report.txt".to_string()),
            path: "/out/report.txt".to_string(),
            ..Default::default()
        }],
        resources: Some(TesResources {
            cpu_cores: Some(2),
            ram_gb: Some(4.0),
            disk_gb: Some(8.0),
            preemptible: Some(true),
            ..Default::default()
        }),
        tags: BTreeMap::from([
            (GROUP_TAG_KEY.to_string(), group.to_string()),
            ("project".to_string(), "alpha".to_string()),
        ]),
        ..Default::default()
    }
}

fn execution_record(job_id: JobId, owner: UserId, spec: ExecutionSpec) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::Execution(spec),
        owner,
        node_id(),
        1_000,
        1_000,
        None,
    )
}

#[test]
fn emits_required_logs() {
    // TES 1.1 requires taskLog.logs and taskLog.outputs to be present;
    // executor logs appear only once the task is terminal.
    let group = Ulid::from_bytes([5u8; 16]);
    let (spec, _) = map_execution_spec(&sample_task(group), None, ExecutionTarget::Realm).unwrap();
    let mut record = execution_record(JobId::from_bytes([9u8; 16]), user(2), spec);

    let running = build_task_log(&record, "");
    assert!(running.logs.is_empty());
    let json = serde_json::to_value(&running).unwrap();
    assert_eq!(json["outputs"], serde_json::json!([]));
    assert_eq!(json["logs"], serde_json::json!([]));

    record.state = JobState::Succeeded;
    record.result = Some(JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: Some("ws".to_string()),
        outputs: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    });
    let terminal = build_task_log(&record, "");
    assert_eq!(terminal.logs.len(), 1);
    assert_eq!(terminal.logs[0].exit_code, Some(0));
}

#[tokio::test]
async fn redacts_internal_detail() {
    // Raw server error text must never reach a TES client on 500.
    let response = TesError::internal("secret backend detail").into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["msg"], "Internal server error");

    let response = TesError::bad_request("visible reason").into_response();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["msg"], "visible reason");
}

#[test]
fn maps_submit_errors() {
    // A TES client retries 500, so every non-retryable admission refusal
    // must keep the status the native submit surface answers with.
    use aruna_core::ClockHealthError;
    use aruna_core::compute_quota::{QuotaDenied, QuotaDimension, QuotaScope};
    use aruna_core::structs::CompositionError;
    use aruna_operations::jobs::submit::SubmitJobError;

    let cases = [
        (
            SubmitJobError::JobPlanConflict {
                existing_job_id: JobId::from_bytes([7u8; 16]),
            },
            StatusCode::CONFLICT,
        ),
        (
            SubmitJobError::QuotaDenied(QuotaDenied {
                scope: QuotaScope::Group,
                dimension: QuotaDimension::CpuCores,
                observed: 30,
                requested: 8,
                limit: 32,
            }),
            StatusCode::CONFLICT,
        ),
        (
            SubmitJobError::ActiveJobLimit { limit: 4 },
            StatusCode::CONFLICT,
        ),
        (
            SubmitJobError::Composition(CompositionError::KeyConflict("reads".to_string())),
            StatusCode::CONFLICT,
        ),
        (
            SubmitJobError::Composition(CompositionError::MissingVersion("reads".to_string())),
            StatusCode::BAD_REQUEST,
        ),
        (
            SubmitJobError::TooManyOutputs { limit: 1024 },
            StatusCode::BAD_REQUEST,
        ),
        (
            SubmitJobError::InvalidWorkspace("no bucket".to_string()),
            StatusCode::BAD_REQUEST,
        ),
        (SubmitJobError::AuthorityDenied, StatusCode::FORBIDDEN),
        (
            SubmitJobError::ClockHealth(ClockHealthError::TimestampOverflow {
                timestamp_ms: u64::MAX,
            }),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(TesError::from_submit(error).status, expected);
    }

    // The 503 body carries the fixed reason, never a holder identity.
    let unavailable = TesError::from_submit(SubmitJobError::PlacementUnavailable(
        "node 7 idle".to_string(),
    ));
    assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.message, "job_placement_unavailable");
}

#[test]
fn resolves_task_group() {
    // Pre-authorization group resolution must mirror the mapping rules.
    let group = Ulid::from_bytes([5u8; 16]);
    let other = Ulid::from_bytes([6u8; 16]);
    let task = sample_task(group);
    assert_eq!(resolve_task_group(&task, None).unwrap(), group);
    assert_eq!(resolve_task_group(&task, Some(group)).unwrap(), group);
    assert_eq!(
        resolve_task_group(&task, Some(other)).unwrap_err().status,
        StatusCode::FORBIDDEN
    );

    let mut untagged = task.clone();
    untagged.tags.remove(GROUP_TAG_KEY);
    assert_eq!(resolve_task_group(&untagged, Some(other)).unwrap(), other);
    assert_eq!(
        resolve_task_group(&untagged, None).unwrap_err().status,
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn caps_task_io() {
    // Input and output counts are bounded before quadratic validation.
    let group = Ulid::from_bytes([5u8; 16]);
    let mut task = sample_task(group);
    task.inputs = vec![task.inputs[0].clone(); MAX_TASK_IO + 1];
    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);

    let mut task = sample_task(group);
    task.outputs = vec![task.outputs[0].clone(); MAX_EXECUTION_OUTPUTS + 1];
    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn maps_task() {
    let group = Ulid::from_bytes([5u8; 16]);
    let (spec, dedup) =
        map_execution_spec(&sample_task(group), None, ExecutionTarget::Realm).unwrap();
    assert_eq!(spec.group_id, group);
    assert_eq!(spec.name.as_deref(), Some("align reads"));
    assert_eq!(spec.description.as_deref(), Some("sample task"));
    assert_eq!(spec.tags.get("project").map(String::as_str), Some("alpha"));
    assert_eq!(spec.image, "alpine:3");
    // TES command becomes the entrypoint override; image CMD stays empty.
    assert_eq!(spec.entrypoint, Some(vec!["echo".into(), "hi".into()]));
    assert!(spec.command.is_empty());
    assert_eq!(spec.workdir.as_deref(), Some("/work"));
    assert_eq!(spec.env.get("K").map(String::as_str), Some("V"));
    assert_eq!(spec.resources.cpu_cores, Some(2));
    assert_eq!(spec.resources.ram_bytes, Some(4_000_000_000));
    assert_eq!(spec.resources.disk_bytes, Some(8_000_000_000));
    assert!(spec.resources.preemptible);
    assert_eq!(spec.inputs.len(), 1);
    assert_eq!(spec.inputs[0].mode, InputMode::Mount);
    assert_eq!(spec.inputs[0].dest_key, "in/data.csv");
    assert_eq!(
        spec.inputs[0].container_path.as_deref(),
        Some("/in/data.csv")
    );
    assert_eq!(spec.inputs[0].name.as_deref(), Some("reads"));
    assert_eq!(spec.inputs[0].description.as_deref(), Some("input reads"));
    assert_eq!(spec.file_outputs.len(), 1);
    assert_eq!(spec.file_outputs[0].container_path, "/out/report.txt");
    assert_eq!(spec.file_outputs[0].name.as_deref(), Some("report"));
    assert_eq!(
        spec.file_outputs[0].description.as_deref(),
        Some("output report")
    );
    assert_eq!(
        spec.file_outputs[0].destination,
        OutputDestination::S3 {
            bucket: "dest".to_string(),
            key: "out/report.txt".to_string(),
        }
    );
    assert!(spec.output_prefixes.is_empty());
    assert!(dedup.is_none());
}

#[test]
fn filters_tasks() {
    let group = Ulid::from_bytes([5u8; 16]);
    let (spec, _) = map_execution_spec(&sample_task(group), None, ExecutionTarget::Realm).unwrap();
    let mut record = execution_record(JobId::from_bytes([6u8; 16]), user(2), spec);
    record.state = JobState::Running;
    let uri: axum::http::Uri = "/ga4gh/tes/v1/tasks?state=RUNNING&name_prefix=align&tag_key=project&tag_key=aruna-engine.org%2Fgroup&tag_value=alpha"
            .parse()
            .unwrap();
    let Query(query) = Query::<ListTasksQuery>::try_from_uri(&uri).unwrap();
    let filters = TaskFilters::from_query(&query, uri.query()).unwrap();
    assert!(filters.matches(&record));

    let derived = TaskFilters::from_query(
            &ListTasksQuery::default(),
            Some(&format!(
                "tag_key=aruna-engine.org%2Fjob-id&tag_value={}&tag_key=aruna-engine.org%2Flogical-state&tag_value=running&tag_key=aruna-engine.org%2Fexecutor-kind&tag_value=docker&tag_key=aruna-engine.org%2Festimated-transfer-bytes&tag_value=4096",
                record.job_id
            )),
        )
        .unwrap();
    let details = TaskDetails {
        logical_state: Some("running".to_string()),
        executor_kind: Some("docker".to_string()),
        transfer_bytes: Some(4_096),
    };
    assert!(derived.has_derived());
    assert!(derived.matches(&record));
    assert!(derived.matches_details(&record, &details));

    let wrong_name = ListTasksQuery {
        name_prefix: Some("other".to_string()),
        ..Default::default()
    };
    assert!(
        !TaskFilters::from_query(&wrong_name, None)
            .unwrap()
            .matches(&record)
    );
    assert!(
        !TaskFilters::from_query(
            &ListTasksQuery::default(),
            Some("tag_key=project&tag_value=beta"),
        )
        .unwrap()
        .matches(&record)
    );
    assert!(
        !TaskFilters::from_query(&ListTasksQuery::default(), Some("tag_key=missing"),)
            .unwrap()
            .matches(&record)
    );
    assert!(
        TaskFilters::from_query(
            &ListTasksQuery {
                state: Some("INVALID".to_string()),
                ..Default::default()
            },
            None,
        )
        .is_err()
    );

    let probe = JobRecord::new(
        JobId::from_bytes([7u8; 16]),
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        user(2),
        node_id(),
        1_000,
        1_000,
        None,
    );
    assert!(
        !TaskFilters::from_query(&ListTasksQuery::default(), None)
            .unwrap()
            .matches(&probe)
    );
}

#[test]
fn rejects_duplicate_inputs() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    let mut input = task.inputs[0].clone();
    input.url = Some("s3://src/other.csv".to_string());
    task.inputs.push(input);
    assert_eq!(
        map_execution_spec(&task, None, ExecutionTarget::Realm)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn rejects_invalid_size() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    for size_gb in [-1.0, 0.0, f64::NAN, 1e-10, f64::MAX] {
        task.resources.as_mut().unwrap().ram_gb = Some(size_gb);
        assert_eq!(
            map_execution_spec(&task, None, ExecutionTarget::Realm)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        task.resources.as_mut().unwrap().ram_gb = Some(4.0);
        task.resources.as_mut().unwrap().disk_gb = Some(size_gb);
        assert_eq!(
            map_execution_spec(&task, None, ExecutionTarget::Realm)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        task.resources.as_mut().unwrap().disk_gb = Some(8.0);
    }
}

#[test]
fn rejects_multi_executor() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.executors.push(task.executors[0].clone());
    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("single executor"));
}

#[test]
fn rejects_missing_group() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.tags.clear();
    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains(GROUP_TAG_KEY));
}

#[test]
fn defaults_group() {
    let group = Ulid::from_bytes([5u8; 16]);
    let mut task = sample_task(group);
    task.tags.remove(GROUP_TAG_KEY);
    let (spec, _) = map_execution_spec(&task, Some(group), ExecutionTarget::Realm).unwrap();
    assert_eq!(spec.group_id, group);
}

#[test]
fn rejects_group_override() {
    let group = Ulid::from_bytes([5u8; 16]);
    let credential_group = Ulid::from_bytes([6u8; 16]);
    let error = map_execution_spec(
        &sample_task(group),
        Some(credential_group),
        ExecutionTarget::Realm,
    )
    .unwrap_err();
    assert_eq!(error.status, StatusCode::FORBIDDEN);
}

#[test]
fn rejects_invalid_paths() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.executors[0].workdir = Some("work".to_string());
    assert_eq!(
        map_execution_spec(&task, None, ExecutionTarget::Realm)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
    task.executors[0].workdir = Some("/work".to_string());
    task.inputs[0].path = "/in/../data.csv".to_string();
    assert_eq!(
        map_execution_spec(&task, None, ExecutionTarget::Realm)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
    task.inputs[0].path = "/in/data.csv".to_string();
    task.outputs[0].path = "/out//report.txt".to_string();
    assert_eq!(
        map_execution_spec(&task, None, ExecutionTarget::Realm)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn rejects_unsupported_fields() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.id = Some("server-owned".to_string());
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
    task.id = None;
    task.inputs[0].kind = TesFileType::Directory;
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
    task.inputs[0].kind = TesFileType::File;
    task.outputs[0].kind = TesFileType::Directory;
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
    task.outputs[0].kind = TesFileType::File;
    task.executors[0].stdout = Some("/logs/out".to_string());
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
    task.executors[0].stdout = None;
    task.volumes.push("/data".to_string());
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
    task.volumes.clear();
    task.resources
        .as_mut()
        .unwrap()
        .zones
        .push("zone-a".to_string());
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
}

#[test]
fn maps_wildcard_output() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.outputs[0].path = "/out/*.txt".to_string();
    task.outputs[0].path_prefix = Some("/out".to_string());
    task.outputs[0].url = Some("s3://dest/results".to_string());

    let (spec, _) = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap();

    assert_eq!(spec.file_outputs[0].container_path, "/out/*.txt");
    assert_eq!(spec.file_outputs[0].path_prefix.as_deref(), Some("/out"));
}

#[test]
fn rejects_input_match() {
    // An input the pattern would select must not be captured as an output.
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.inputs[0].path = "/in/data.csv".to_string();
    task.outputs[0].path = "/in/*.csv".to_string();
    task.outputs[0].path_prefix = Some("/in".to_string());

    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn rejects_missing_prefix() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.outputs[0].path = "/out/*.txt".to_string();

    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("/out/*.txt"), "{}", error.message);
}

#[test]
fn rejects_foreign_prefix() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.outputs[0].path = "/out/sub/*.txt".to_string();
    for prefix in ["/other", "/out/s", "/out/*", "out"] {
        task.outputs[0].path_prefix = Some(prefix.to_string());
        let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST, "{prefix}");
    }
    // The pattern itself must still compile.
    task.outputs[0].path = "/out/[a.txt".to_string();
    task.outputs[0].path_prefix = Some("/out".to_string());
    assert_eq!(
        map_execution_spec(&task, None, ExecutionTarget::Realm)
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn ignores_unused_prefix() {
    // TES 1.1 ignores path_prefix unless the path carries wildcards.
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.outputs[0].path_prefix = Some("/out".to_string());

    let (spec, _) = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap();

    assert!(spec.file_outputs[0].path_prefix.is_none());
}

#[test]
fn rejects_wildcard_input() {
    // TES 1.1 defines wildcards for outputs only.
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.inputs[0].path = "/in/*.csv".to_string();

    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn rejects_duplicate_outputs() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    let mut output = task.outputs[0].clone();
    output.url = Some("s3://dest/out/other.txt".to_string());
    task.outputs.push(output);
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());

    task.outputs[1].path = "/out/other.txt".to_string();
    task.outputs[1].url = task.outputs[0].url.clone();
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());

    task.outputs.truncate(1);
    task.outputs[0].path = task.inputs[0].path.clone();
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_err());
}

#[test]
fn allows_shared_workdir() {
    let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
    task.inputs[0].path = "/work/.command.sh".to_string();
    task.outputs[0].path = "/work/out.txt".to_string();
    assert!(map_execution_spec(&task, None, ExecutionTarget::Realm).is_ok());
}

#[test]
fn maps_states() {
    let spec = || ExecutionSpec {
        group_id: Ulid::from_bytes([5u8; 16]),
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: "img".to_string(),
        entrypoint: None,
        command: vec!["run".to_string()],
        workdir: None,
        env: BTreeMap::new(),
        resources: ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };
    let mut record = execution_record(JobId::from_bytes([1u8; 16]), user(2), spec());
    let cases = [
        (JobState::Queued, TesState::Queued),
        (JobState::Claimed, TesState::Queued),
        (JobState::Preparing, TesState::Initializing),
        (JobState::Ready, TesState::Initializing),
        (JobState::Running, TesState::Running),
        (JobState::Cancelling, TesState::Canceling),
        (JobState::Indeterminate, TesState::Unknown),
        (JobState::Succeeded, TesState::Complete),
        (JobState::Cancelled, TesState::Canceled),
    ];
    for (job_state, expected) in cases {
        record.state = job_state;
        assert_eq!(tes_state(&record), expected, "{job_state:?}");
    }
    record.state = JobState::Queued;
    record.cancel_requested = true;
    assert_eq!(tes_state(&record), TesState::Canceling);
    record.cancel_requested = false;
    // Failed splits on evidence.
    record.state = JobState::Failed;
    record.result = None;
    assert_eq!(tes_state(&record), TesState::SystemError);
    record.result = Some(JobResultPayload::Execution {
        exit_code: Some(1),
        workspace_bucket: Some("ws".to_string()),
        outputs: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    });
    assert_eq!(tes_state(&record), TesState::ExecutorError);
    if let Some(JobResultPayload::Execution { exit_code, .. }) = &mut record.result {
        *exit_code = Some(0);
    }
    assert_eq!(tes_state(&record), TesState::SystemError);
}

#[test]
fn view_projections() {
    let (spec, _) = map_execution_spec(
        &sample_task(Ulid::from_bytes([5u8; 16])),
        None,
        ExecutionTarget::Realm,
    )
    .unwrap();
    let mut record = execution_record(JobId::from_bytes([2u8; 16]), user(2), spec);
    let queued = project_task(&record, &TaskDetails::default(), TesView::Full, "http://x");
    assert!(queued.logs[0].start_time.is_none());
    assert!(queued.logs[0].logs.is_empty());
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(2_000);
    record.workspace_bucket = Some("ws-x".to_string());
    let JobPayload::Execution(spec) = &mut record.payload else {
        unreachable!();
    };
    spec.inputs.push(InputSelection {
        source: InputSource::S3 {
            bucket: "native".to_string(),
            key: "workspace-only".to_string(),
            version_id: None,
        },
        source_node_id: None,
        dest_key: "native/input".to_string(),
        mode: InputMode::Snapshot,
        container_path: None,
        name: None,
        description: None,
    });
    spec.output_prefixes.push("native/".to_string());
    record.last_error = Some(JobError::permanent("prior failure"));
    record.result = Some(JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: Some("ws-x".to_string()),
        outputs: vec![OutputObject {
            node_id: record.owner_node_id,
            bucket: "dest".to_string(),
            key: "out/r.txt".to_string(),
            version_id: Ulid::from_bytes([21u8; 16]),
            execution_id: Ulid::from_bytes([22u8; 16]),
            container_path: "/out/report.txt".to_string(),
            size: 12,
            digest: None,
        }],
        stdout: "hello".to_string(),
        stderr: "error".to_string(),
        output_digest: None,
    });

    let minimal = project_task(
        &record,
        &TaskDetails::default(),
        TesView::Minimal,
        "http://x",
    );
    assert!(minimal.executors.is_empty());
    assert!(minimal.logs.is_empty());
    assert_eq!(minimal.state, Some(TesState::Complete));

    let basic = project_task(&record, &TaskDetails::default(), TesView::Basic, "http://x");
    assert_eq!(basic.name.as_deref(), Some("align reads"));
    assert_eq!(basic.description.as_deref(), Some("sample task"));
    assert_eq!(basic.tags.get("project").map(String::as_str), Some("alpha"));
    assert_eq!(basic.executors.len(), 1);
    assert_eq!(basic.executors[0].command, vec!["echo", "hi"]);
    assert_eq!(basic.executors[0].workdir.as_deref(), Some("/work"));
    assert_eq!(basic.logs.len(), 1);
    assert!(basic.logs[0].system_logs.is_empty());
    assert!(basic.logs[0].logs[0].stdout.is_none());
    assert!(basic.logs[0].logs[0].stderr.is_none());
    assert_eq!(basic.inputs.len(), 1);
    assert_eq!(basic.inputs[0].path, "/in/data.csv");
    assert_eq!(basic.inputs[0].name.as_deref(), Some("reads"));
    assert_eq!(basic.outputs.len(), 1);
    assert_eq!(basic.outputs[0].path, "/out/report.txt");
    assert_eq!(
        basic.outputs[0].url.as_deref(),
        Some("s3://dest/out/report.txt")
    );
    assert_eq!(basic.resources.as_ref().unwrap().disk_gb, Some(8.0));
    assert_eq!(basic.resources.as_ref().unwrap().preemptible, Some(true));

    let full = project_task(&record, &TaskDetails::default(), TesView::Full, "http://x");
    assert_eq!(full.logs.len(), 1);
    assert_eq!(full.logs[0].logs[0].exit_code, Some(0));
    assert_eq!(full.logs[0].logs[0].stdout.as_deref(), Some("hello"));
    assert_eq!(full.logs[0].logs[0].stderr.as_deref(), Some("error"));
    assert_eq!(full.logs[0].system_logs, vec!["prior failure"]);
    assert_eq!(full.logs[0].outputs.len(), 1);
    assert_eq!(
        full.logs[0].outputs[0].url,
        format!(
            "s3://dest/out/r.txt?versionId={}",
            Ulid::from_bytes([21u8; 16])
        )
    );
    assert_eq!(full.logs[0].outputs[0].path, "/out/report.txt");
}

/// The replicated family behind one succeeded distributed task.
fn family_fixture() -> aruna_operations::jobs::lifecycle::FamilyReport {
    use aruna_core::jobs::{JobKind, JobStatusView};
    use aruna_core::structs::{
        EffectiveResources, JobAdmissionRecord, JobProgress, JobRetryPolicy, LogicalJobSpec,
        LogicalJobState, OutputObject, PlacementRef, RealmId, SubmissionId, WorkspaceMode,
    };
    use aruna_operations::jobs::lifecycle::FamilyReport;

    let realm_id = RealmId([1u8; 32]);
    let created_by = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
    let job_id = JobId::from_bytes([3u8; 16]);
    let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
    let submission_id = SubmissionId([5u8; 32]);
    let resources = EffectiveResources {
        cpu_cores: 1,
        ram_bytes: 1,
        disk_bytes: 0,
        max_walltime_ms: 1_000,
        preemptible: false,
    };
    let mut payload = ExecutionSpec {
        group_id: Ulid::from_bytes([6u8; 16]),
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: "img".to_string(),
        entrypoint: None,
        command: vec!["true".to_string()],
        workdir: None,
        env: BTreeMap::new(),
        resources: ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };
    payload.name = Some("family task".to_string());
    let spec = LogicalJobSpec {
        submission_id,
        job_id,
        origin_node_id: node_id,
        ingress_node_id: node_id,
        realm_id,
        group_id: payload.group_id,
        created_by,
        created_at_ms: 10,
        retention_ms: aruna_core::structs::DEFAULT_JOB_RETENTION_MS,
        payload,
        request_digest: [7u8; 32],
        spec_digest: [8u8; 32],
        resources,
        retry: JobRetryPolicy {
            max_launches_per_witness: 3,
        },
        admission: JobAdmissionRecord {
            submission_id,
            request_digest: [7u8; 32],
            job_id,
            group_id: Ulid::from_bytes([6u8; 16]),
            admitting_node_id: node_id,
            membership_generation: 0,
            resources,
            admitted_at_ms: 10,
        },
        captured_inputs: Vec::new(),
        output_policies: Vec::new(),
        placement: PlacementRef::NIL,
    };
    let version_id = Ulid::from_bytes([9u8; 16]);
    let execution_id = Ulid::from_bytes([10u8; 16]);
    FamilyReport {
        job: JobStatusView {
            job_id,
            created_by,
            kind: JobKind::Execution,
            state: JobState::Succeeded,
            attempts: 2,
            cancel_requested: false,
            created_at_ms: 10,
            updated_at_ms: 20,
            finished_at_ms: Some(20),
            progress: JobProgress::new("phases"),
            last_error: None,
            result: None,
            workspace_bucket: Some("ws".to_string()),
            workspace_mode: WorkspaceMode::None,
            locally_exhausted: false,
            session_runtime: None,
        },
        spec,
        submission_id,
        request_digest: [7u8; 32],
        canonical_job_id: job_id,
        aliases: vec![job_id],
        conflicts: 0,
        state: LogicalJobState::Succeeded,
        canonical_execution_id: Some(execution_id),
        canonical_result: Some(PhysicalExecutionResult {
            exit_code: Some(0),
            output_digest: Some([12u8; 32]),
            message: None,
            stdout: ResultMessage::tail("out tail"),
            stderr: ResultMessage::tail("err tail"),
        }),
        executions: 2,
        execution_list: Vec::new(),
        started_at_ms: Some(15),
        duplicate_successes: 1,
        outputs: vec![OutputObject {
            node_id,
            bucket: "dest".to_string(),
            key: "out/r.txt".to_string(),
            version_id,
            execution_id,
            container_path: "/out/report.txt".to_string(),
            size: 12,
            digest: None,
        }],
        output_endpoints: std::collections::BTreeMap::new(),
        revision: 3,
        digest: [11u8; 32],
        cancel_requested: false,
        responder: Some(node_id),
        partial: false,
        locally_exhausted: false,
        plan: None,
    }
}

#[test]
fn family_preserves_versions() {
    // TES preserves canonical output versions and reports none before canonical success.
    use aruna_core::structs::LogicalJobState;

    let report = family_fixture();
    let version_id = Ulid::from_bytes([9u8; 16]);

    let task = project_task(
        &family_record(&report),
        &TaskDetails::from_report(&report),
        TesView::Full,
        "http://x",
    );

    assert_eq!(task.state, Some(TesState::Complete));
    assert_eq!(
        task.logs[0].outputs[0].url,
        format!("s3://dest/out/r.txt?versionId={version_id}")
    );
    // The tails ride the replicated result, so any node answers with them.
    assert_eq!(task.logs[0].logs[0].stdout.as_deref(), Some("out tail"));
    assert_eq!(task.logs[0].logs[0].stderr.as_deref(), Some("err tail"));

    let mut running = report.clone();
    running.job.state = JobState::Running;
    running.state = LogicalJobState::Running;
    let pending = project_task(
        &family_record(&running),
        &TaskDetails::from_report(&running),
        TesView::Full,
        "http://x",
    );
    assert_eq!(pending.state, Some(TesState::Running));
    assert!(pending.logs[0].outputs.is_empty());
}

#[test]
fn derives_family_tags() {
    // Placement details are stamped at read time, so a task that was never
    // placed carries the job id and logical state and nothing more.
    use aruna_core::compute::ExecutionTargetId;
    use aruna_operations::jobs::lifecycle::PlanEstimate;

    let mut report = family_fixture();
    let unplaced = project_task(
        &family_record(&report),
        &TaskDetails::from_report(&report),
        TesView::Basic,
        "http://x",
    );
    assert_eq!(
        unplaced.tags.get(JOB_ID_TAG_KEY).map(String::as_str),
        Some(report.job.job_id.to_string().as_str())
    );
    assert_eq!(
        unplaced.tags.get(LOGICAL_STATE_TAG_KEY).map(String::as_str),
        Some("succeeded")
    );
    assert!(!unplaced.tags.contains_key(EXECUTOR_KIND_TAG_KEY));
    assert!(!unplaced.tags.contains_key(TRANSFER_BYTES_TAG_KEY));

    report.plan = Some(PlanEstimate {
        target: Some(ExecutionTargetId {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            executor_kind: "docker".to_string(),
        }),
        scheduler_node_id: None,
        estimated_transfer_bytes: 4_096,
        estimated_transfer_ms: 12,
        alternatives: 2,
        rejected: 1,
        omitted: 0,
        stored_at_ms: 15,
        inputs: Vec::new(),
        candidates: Vec::new(),
    });
    let details = TaskDetails::from_report(&report);
    let record = family_record(&report);
    for view in [TesView::Basic, TesView::Full] {
        let task = project_task(&record, &details, view, "http://x");
        assert_eq!(
            task.tags.get(EXECUTOR_KIND_TAG_KEY).map(String::as_str),
            Some("docker")
        );
        assert_eq!(
            task.tags.get(TRANSFER_BYTES_TAG_KEY).map(String::as_str),
            Some("4096")
        );
    }
    let minimal = project_task(&record, &details, TesView::Minimal, "http://x");
    assert!(minimal.tags.is_empty());
}

async fn management_state() -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let capabilities =
        NodeCapabilities::management_node(aruna_core::keys::generate_signing_key()).unwrap();
    let state = ServerState::new(
        ctx,
        realm(),
        node_id(),
        capabilities,
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state))
}

async fn enroll_device(state: &ServerState, owner: UserId) {
    use aruna_core::structs::{Actor, RealmConfigDocument, RealmNodeKind};
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    config.ensure_node(node_id(), RealmNodeKind::User { owner });
    let actor = Actor {
        node_id: node_id(),
        user_id: UserId::nil(realm()),
        realm_id: realm(),
    };
    let bytes = config.to_bytes(&actor).expect("config serializes");
    let _ = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            key: realm().as_bytes().to_vec().into(),
            value: bytes.into(),
            txn_id: None,
        })
        .await;
}

fn local_task(group: Ulid) -> TesTask {
    let mut task = sample_task(group);
    task.tags
        .insert(TARGET_TAG_KEY.to_string(), "local".to_string());
    task
}

fn bearer_auth(user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    })
}

#[tokio::test]
async fn local_needs_device() {
    // A node that serves no device plane refuses the target, not the caller.
    let (_dir, state) = management_state().await;
    let group = Ulid::from_bytes([5u8; 16]);

    let response = create_task(
        State(state),
        Extension(bearer_auth(user(2))),
        Extension(None),
        HeaderMap::new(),
        Json(local_task(group)),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn local_refuses_stranger() {
    let (_dir, state) = build_state().await;
    enroll_device(&state, user(2)).await;
    let group = Ulid::from_bytes([5u8; 16]);

    let response = create_task(
        State(state),
        Extension(bearer_auth(user(3))),
        Extension(None),
        HeaderMap::new(),
        Json(local_task(group)),
    )
    .await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[test]
fn maps_target_tag() {
    // The tag is the only way a TES caller asks for the local machine.
    let mut tags = BTreeMap::new();
    assert_eq!(task_target(&tags).unwrap(), ExecutionTarget::Realm);

    tags.insert(TARGET_TAG_KEY.to_string(), "realm".to_string());
    assert_eq!(task_target(&tags).unwrap(), ExecutionTarget::Realm);

    tags.insert(TARGET_TAG_KEY.to_string(), "local".to_string());
    assert_eq!(task_target(&tags).unwrap(), ExecutionTarget::Local);

    tags.insert(TARGET_TAG_KEY.to_string(), "elsewhere".to_string());
    let error = task_target(&tags).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn keeps_target_tag() {
    // A local task reports the target it was created with.
    let group = Ulid::from_bytes([5u8; 16]);
    let mut task = sample_task(group);
    task.tags
        .insert(TARGET_TAG_KEY.to_string(), "local".to_string());

    let (spec, _) = map_execution_spec(&task, None, ExecutionTarget::Local).unwrap();

    assert_eq!(
        project_tags(&spec).get(TARGET_TAG_KEY).map(String::as_str),
        Some("local")
    );
}

#[test]
fn rejects_derived_tag() {
    let group = Ulid::from_bytes([5u8; 16]);
    let mut task = sample_task(group);
    task.tags
        .insert(EXECUTOR_KIND_TAG_KEY.to_string(), "docker".to_string());

    let error = map_execution_spec(&task, None, ExecutionTarget::Realm).unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code.as_deref(), Some("reserved_tag"));
}

#[test]
fn projects_full_command() {
    // Docker runs entrypoint + command together; the projection shows both.
    let mut spec = ExecutionSpec {
        group_id: Ulid::from_bytes([5u8; 16]),
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: "img".to_string(),
        entrypoint: Some(vec!["/bin/tool".to_string()]),
        command: vec!["--flag".to_string(), "x".to_string()],
        workdir: None,
        env: BTreeMap::new(),
        resources: ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };
    let record = execution_record(JobId::from_bytes([3u8; 16]), user(2), spec.clone());
    let task = project_task(&record, &TaskDetails::default(), TesView::Basic, "http://x");
    assert_eq!(task.executors[0].command, vec!["/bin/tool", "--flag", "x"]);

    spec.entrypoint = None;
    let record = execution_record(JobId::from_bytes([4u8; 16]), user(2), spec);
    let task = project_task(&record, &TaskDetails::default(), TesView::Basic, "http://x");
    assert_eq!(task.executors[0].command, vec!["--flag", "x"]);
}

#[tokio::test]
async fn rejects_basic() {
    let (_dir, state) = build_state().await;
    let group = Ulid::from_bytes([5u8; 16]);
    let access = issued_access(&state, group);
    let mut revoked = access.clone();
    revoked.revoked_at = Some(SystemTime::now());
    let mut expired = access.clone();
    expired.expiry = SystemTime::UNIX_EPOCH;
    let mut foreign_issuer = access.clone();
    foreign_issuer.issued_by = [0u8; 32];

    for (access, secret) in [
        (access, "wrong-secret"),
        (revoked, "tes-secret"),
        (expired, "tes-secret"),
        (foreign_issuer, "tes-secret"),
    ] {
        write_credential(&state, &access).await;
        let error = authenticate_tes(&state, None, &basic_headers(&access, secret))
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn rejects_restricted_basic() {
    let (_dir, state) = build_state().await;
    let mut access = issued_access(&state, Ulid::from_bytes([5u8; 16]));
    access.path_restrictions = Some(Vec::new());
    write_credential(&state, &access).await;

    let error = authenticate_tes(&state, None, &basic_headers(&access, TES_SECRET))
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn creates_tagless_basic() {
    // Tagless auth reaches admission, then this holder-free fixture returns the expected 503.
    let (_dir, state) = build_state().await;
    let group = Ulid::from_bytes([5u8; 16]);
    let access = issued_access(&state, group);
    write_credential(&state, &access).await;
    write_auth(&state, group, access.user_identity).await;
    let mut task = sample_task(group);
    task.tags.remove(GROUP_TAG_KEY);

    let (spec, workspace) = map_execution_spec(&task, Some(group), ExecutionTarget::Realm).unwrap();
    assert_eq!(spec.group_id, group);
    assert!(workspace.is_none());

    let response = create_task(
        State(state.clone()),
        Extension(None),
        Extension(None),
        basic_headers(&access, TES_SECRET),
        Json(task),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["msg"], "job_placement_unavailable");
}

#[tokio::test]
async fn maps_target_modes() {
    // Neither target owns a bucket; a device refuses mounts, so a local
    // run snapshots the same objects instead.
    let (_dir, state) = build_state().await;
    let group = Ulid::from_bytes([5u8; 16]);
    let access = issued_access(&state, group);
    write_credential(&state, &access).await;
    write_auth(&state, group, access.user_identity).await;

    let (spec, _) = map_execution_spec(&sample_task(group), None, ExecutionTarget::Realm).unwrap();
    assert_eq!(spec.inputs[0].mode, InputMode::Mount);
    assert_eq!(
        target_modes(ExecutionTarget::Realm),
        (InputMode::Mount, WorkspaceMode::None)
    );
    let (local, _) = map_execution_spec(&sample_task(group), None, ExecutionTarget::Local).unwrap();
    assert_eq!(local.inputs[0].mode, InputMode::Snapshot);
    assert_eq!(
        target_modes(ExecutionTarget::Local),
        (InputMode::Snapshot, WorkspaceMode::None)
    );

    // The handle-less fixture has no family holder, so 503 is the honest
    // outcome of a mapping that reached admission.
    let response = create_task(
        State(state.clone()),
        Extension(None),
        Extension(None),
        basic_headers(&access, TES_SECRET),
        Json(sample_task(group)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn basic_scopes_tasks() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let group = Ulid::from_bytes([5u8; 16]);
    let sibling = Ulid::from_bytes([6u8; 16]);
    let access = issued_access(&state, group);
    write_credential(&state, &access).await;
    let headers = basic_headers(&access, TES_SECRET);
    let caller = authenticate_tes(&state, None, &headers).await.unwrap();
    assert_eq!(caller.auth.user_id, owner);
    assert_eq!(caller.credential_group, Some(group));

    let visible_id = JobId::from_bytes([9u8; 16]);
    let hidden_id = JobId::from_bytes([10u8; 16]);
    for (job_id, group_id) in [(visible_id, group), (hidden_id, sibling)] {
        let (spec, _) =
            map_execution_spec(&sample_task(group_id), None, ExecutionTarget::Realm).unwrap();
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(job_id, owner, spec),
        )
        .await
        .unwrap();
    }

    let visible = get_task(
        State(state.clone()),
        Extension(None),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        headers.clone(),
        Path(visible_id.to_string()),
        Query(ViewQuery::default()),
    )
    .await;
    assert_eq!(visible.status(), StatusCode::OK);
    let hidden = get_task(
        State(state.clone()),
        Extension(None),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        headers.clone(),
        Path(hidden_id.to_string()),
        Query(ViewQuery::default()),
    )
    .await;
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

    let hidden_cancel = cancel_task(
        State(state.clone()),
        Extension(None),
        Extension(None),
        headers.clone(),
        Path(format!("{hidden_id}:cancel")),
    )
    .await;
    assert_eq!(hidden_cancel.status(), StatusCode::NOT_FOUND);
    let visible_cancel = cancel_task(
        State(state.clone()),
        Extension(None),
        Extension(None),
        headers.clone(),
        Path(format!("{visible_id}:cancel")),
    )
    .await;
    assert_eq!(visible_cancel.status(), StatusCode::OK);

    let listed = list_tasks(
        State(state),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        headers,
        RawQuery(None),
        Query(ListTasksQuery::default()),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(page.tasks.len(), 1);
    assert_eq!(page.tasks[0].id, Some(visible_id.to_string()));
}

#[tokio::test]
async fn lists_zero_pagesize() {
    // page_size=0 must fall back to the default, not report an empty page.
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let group = Ulid::from_bytes([5u8; 16]);
    let access = issued_access(&state, group);
    write_credential(&state, &access).await;
    let headers = basic_headers(&access, TES_SECRET);
    let (spec, _) = map_execution_spec(&sample_task(group), None, ExecutionTarget::Realm).unwrap();
    insert_job(
        &state.get_ctx().storage_handle,
        &execution_record(JobId::from_bytes([9u8; 16]), owner, spec),
    )
    .await
    .unwrap();

    let listed = list_tasks(
        State(state),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        headers,
        RawQuery(None),
        Query(ListTasksQuery {
            page_size: Some(0),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(page.tasks.len(), 1);
}

#[tokio::test]
async fn lists_derived_tags() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let group = Ulid::from_bytes([5u8; 16]);
    let access = issued_access(&state, group);
    write_credential(&state, &access).await;
    let headers = basic_headers(&access, TES_SECRET);
    let target = JobId::from_bytes([9u8; 16]);
    for job_id in [
        JobId::from_bytes([8u8; 16]),
        target,
        JobId::from_bytes([10u8; 16]),
    ] {
        let (spec, _) =
            map_execution_spec(&sample_task(group), None, ExecutionTarget::Realm).unwrap();
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(job_id, owner, spec),
        )
        .await
        .unwrap();
    }

    let raw_query = format!("tag_key=aruna-engine.org%2Fjob-id&tag_value={target}");
    let listed = list_tasks(
        State(state),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        headers,
        RawQuery(Some(raw_query)),
        Query(ListTasksQuery {
            view: Some("BASIC".to_string()),
            page_size: Some(1),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
        .await
        .unwrap();
    let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(page.tasks.len(), 1);
    assert_eq!(page.tasks[0].id, Some(target.to_string()));
    assert_eq!(
        page.tasks[0].tags.get(JOB_ID_TAG_KEY),
        Some(&target.to_string())
    );
    assert!(page.next_page_token.is_none());
}

#[tokio::test]
async fn get_resolves() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let (spec, _) = map_execution_spec(
        &sample_task(Ulid::from_bytes([5u8; 16])),
        None,
        ExecutionTarget::Realm,
    )
    .unwrap();
    let job_id = JobId::from_bytes([9u8; 16]);
    insert_job(
        &state.get_ctx().storage_handle,
        &execution_record(job_id, owner, spec),
    )
    .await
    .unwrap();

    let response = get_task(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        HeaderMap::new(),
        Path(job_id.to_string()),
        Query(ViewQuery {
            view: Some("BASIC".to_string()),
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    // A foreign caller cannot see it.
    let foreign = get_task(
        State(state.clone()),
        Extension(auth_for(user(3))),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        HeaderMap::new(),
        Path(job_id.to_string()),
        Query(ViewQuery::default()),
    )
    .await;
    assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cancel_maps_through() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let (spec, _) = map_execution_spec(
        &sample_task(Ulid::from_bytes([5u8; 16])),
        None,
        ExecutionTarget::Realm,
    )
    .unwrap();
    let job_id = JobId::from_bytes([9u8; 16]);
    insert_job(
        &state.get_ctx().storage_handle,
        &execution_record(job_id, owner, spec),
    )
    .await
    .unwrap();

    let ok = cancel_task(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        HeaderMap::new(),
        Path(format!("{job_id}:cancel")),
    )
    .await;
    assert_eq!(ok.status(), StatusCode::OK);
    let body = to_bytes(ok.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"{}");

    // Missing the action suffix is a bad request.
    let bad = cancel_task(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        HeaderMap::new(),
        Path(job_id.to_string()),
    )
    .await;
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn service_info_shape() {
    let info = TesServiceType {
        group: "org.ga4gh",
        artifact: "tes",
        version: TES_VERSION.to_string(),
    };
    assert_eq!(info.artifact, "tes");
    assert_eq!(info.version, "1.1.0");
}

#[test]
fn openapi_has_tes() {
    let openapi = crate::openapi::ApiDoc::openapi();
    assert!(
        openapi
            .paths
            .paths
            .contains_key("/ga4gh/tes/v1/service-info")
    );
    assert!(openapi.paths.paths.contains_key("/ga4gh/tes/v1/tasks"));
    assert!(openapi.paths.paths.contains_key("/ga4gh/tes/v1/tasks/{id}"));
    assert!(
        openapi
            .components
            .is_some_and(|components| components.security_schemes.contains_key("basic_auth"))
    );
}
