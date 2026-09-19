//! Tests session route authorization, cell streaming and the staging of session inputs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_compute::ExecutorRegistry;
use aruna_compute::executor::{BackendCaps, ExecutorBackend, SessionChannel};
use aruna_compute::session::{SessionConfig, SessionPhase};
use aruna_core::UserId;
use aruna_core::compute::{
    AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits,
    LogTails, NOBODY, ReconcileEvidence, TaskOutput, TaskSpec, UserSpec,
};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    CollisionPolicy, ComputeResources, ExecutionSpec, JobId, SessionReportDetail, SessionReportRow,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::store::{
    ClaimOutcome, cancel_running_job, claim_job, insert_job, put_job_entry, read_job_record,
};
use aruna_storage::{FjallStorage, StorageHandle};
use async_trait::async_trait;
use byteview::ByteView;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::UNIX_EPOCH;
use tempfile::TempDir;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

/// A backend whose channel is an in-memory pipe, so a route test needs no
/// container.
struct FakeBackend {
    helper: AsyncMutex<Option<DuplexStream>>,
}

#[async_trait]
impl ExecutorBackend for FakeBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Docker
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            session: true,
            ..BackendCaps::default()
        }
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
        Err(BackendError::InvalidSpec("not used".to_string()))
    }

    async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::InvalidSpec("not used".to_string()))
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
        Err(BackendError::InvalidSpec("not used".to_string()))
    }

    async fn open_session(&self, _context: &FenceContext) -> Result<SessionChannel, BackendError> {
        let stream = self
            .helper
            .lock()
            .await
            .take()
            .ok_or_else(|| BackendError::Conflict("channel already open".to_string()))?;
        let (output, input) = tokio::io::split(stream);
        Ok(SessionChannel {
            input: Box::pin(input),
            output: Box::pin(output),
        })
    }

    async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
        ReconcileEvidence::Absent
    }

    async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }
}

fn realm() -> RealmId {
    RealmId::from_bytes([1u8; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2u8; 32]).public()
}

fn user(seed: u8) -> UserId {
    UserId::new(ulid::Ulid::from_bytes([seed; 16]), realm())
}

fn auth_for(user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    })
}

fn session_spec() -> ExecutionSpec {
    let mut tags = BTreeMap::new();
    tags.insert(
        aruna_core::compute::runtimes::SESSION_TAG.to_string(),
        aruna_core::compute::runtimes::SESSION_TAG_NOTEBOOK.to_string(),
    );
    tags.insert(
        aruna_core::compute::runtimes::SESSION_RUNTIME_TAG.to_string(),
        "python-notebook".to_string(),
    );
    ExecutionSpec {
        group_id: ulid::Ulid::from_bytes([6u8; 16]),
        name: None,
        description: None,
        tags,
        image: "img".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: Some("/work".to_string()),
        env: BTreeMap::new(),
        resources: ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: CollisionPolicy::default(),
    }
}

/// A node holding one running session job of `owner`, plus the helper end
/// of its channel.
async fn build_node(owner: UserId) -> (TempDir, Arc<ServerState>, JobId, DuplexStream) {
    let dir = tempfile::tempdir().expect("a temporary store");
    let storage = FjallStorage::open(dir.path().to_str().expect("a store path")).expect("a store");
    let (node_side, helper) = tokio::io::duplex(64 * 1024);
    let backend = Arc::new(FakeBackend {
        helper: AsyncMutex::new(Some(node_side)),
    });
    let registry = Arc::new(ExecutorRegistry::new().with_backend(backend.clone()));
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: Some(registry.clone()),
    });
    let job_id = JobId::from_bytes([3u8; 16]);
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(session_spec()),
        owner,
        node(),
        1_000,
        1_000,
        None,
    );
    record.state = JobState::Running;
    record.workspace_bucket = Some("lab-data".to_string());
    insert_job(&ctx.storage_handle, &record)
        .await
        .expect("the job is stored");
    registry.sessions().open(
        SessionConfig {
            job_id: job_id.to_string(),
            public_job_id: job_id.to_string(),
            runtime: "python-notebook".to_string(),
            workspace_bucket: "lab-data".to_string(),
            executor_node_id: node().to_string(),
            idle_after_ms: 600_000,
            credential_expires_ms: 0,
        },
        backend,
        FenceContext {
            attempt: AttemptRef::new(job_id.to_string().to_lowercase(), 1),
            attempt_epoch: 1,
            controller_generation: 1,
        },
    );
    let state = ServerState::new(
        ctx,
        realm(),
        node(),
        NodeCapabilities::user_node(realm()).expect("node capabilities"),
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state), job_id, helper)
}

/// Reports the kernel ready and waits until the manager saw it.
async fn make_ready(state: &Arc<ServerState>, job_id: JobId, helper: &mut DuplexStream) {
    helper
        .write_all(b"{\"kind\":\"kernel\",\"state\":\"idle\"}\n")
        .await
        .expect("the helper announces readiness");
    let session = state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&job_id.to_string()))
        .expect("the session is registered");
    let (_, mut receiver) = session.subscribe_all();
    for _ in 0..64 {
        if session.snapshot().state != SessionPhase::Starting {
            return;
        }
        let _ = tokio::time::timeout(Duration::from_secs(60), receiver.recv()).await;
    }
    panic!("the session never became ready");
}

#[tokio::test]
async fn hides_foreign_job() {
    // Absence and foreign ownership must be indistinguishable.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let response = get_session(
        State(state),
        Extension(auth_for(user(9))),
        Path(job_id.to_string()),
    )
    .await;
    assert!(matches!(response, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn hides_unknown_job() {
    let owner = user(2);
    let (_dir, state, _job_id, _helper) = build_node(owner).await;
    let response = get_session(
        State(state),
        Extension(auth_for(owner)),
        Path(JobId::from_bytes([8u8; 16]).to_string()),
    )
    .await;
    assert!(matches!(response, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn refuses_restricted_token() {
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let auth = Some(AuthContext {
        user_id: owner,
        realm_id: realm(),
        path_restrictions: Some(Vec::new()),
        session: None,
    });
    let response = get_session(State(state), Extension(auth), Path(job_id.to_string())).await;
    assert!(matches!(response, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn refuses_early_cells() {
    // A session that has not reached its kernel yet accepts no cell.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let response = submit_cell(
        State(state),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SubmitCellRequest {
            cell_id: "c1".to_string(),
            code: "1".to_string(),
        }),
    )
    .await
    .expect("the handler answers");
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn refuses_unregistered_cell() {
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    drop_session(&state, job_id);
    let response = submit_cell(
        State(state),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SubmitCellRequest {
            cell_id: "c1".to_string(),
            code: "1".to_string(),
        }),
    )
    .await
    .expect("the handler answers");
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn limits_submit_burst() {
    let owner = user(2);
    let (_dir, state, job_id, mut helper) = build_node(owner).await;
    make_ready(&state, job_id, &mut helper).await;
    for index in 0..aruna_core::compute::session::MAX_SUBMITS {
        let response = submit_cell(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            Json(SubmitCellRequest {
                cell_id: format!("c{index}"),
                code: "1".to_string(),
            }),
        )
        .await
        .expect("the handler answers");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    let refused = submit_cell(
        State(state),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SubmitCellRequest {
            cell_id: "late".to_string(),
            code: "1".to_string(),
        }),
    )
    .await
    .expect("the handler answers");
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn busy_cell_conflicts() {
    let owner = user(2);
    let (_dir, state, job_id, mut helper) = build_node(owner).await;
    make_ready(&state, job_id, &mut helper).await;
    for expected in [StatusCode::ACCEPTED, StatusCode::CONFLICT] {
        let response = submit_cell(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            Json(SubmitCellRequest {
                cell_id: "c1".to_string(),
                code: "1".to_string(),
            }),
        )
        .await
        .expect("the handler answers");
        assert_eq!(response.status(), expected);
    }
}

#[tokio::test]
async fn resumes_from_after() {
    // The portal sends the resume point as `after`, not as a header.
    let owner = user(2);
    let (_dir, state, job_id, mut helper) = build_node(owner).await;
    make_ready(&state, job_id, &mut helper).await;
    let response = stream_session(
        State(state.clone()),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Query(EventsQuery { after: Some(1) }),
        HeaderMap::new(),
    )
    .await
    .expect("the handler answers");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
}

#[tokio::test]
async fn stream_reports_gap() {
    // A resume point the ring dropped is answered with a gap frame.
    let owner = user(2);
    let (_dir, state, job_id, mut helper) = build_node(owner).await;
    make_ready(&state, job_id, &mut helper).await;
    let session = state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&job_id.to_string()))
        .expect("the session is registered");
    for _ in 0..(aruna_core::compute::session::MAX_RING_EVENTS + 4) {
        session.announce();
    }
    assert!(session.subscribe(1).is_err(), "the ring rolled over");
    let response = stream_session(
        State(state),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Query(EventsQuery { after: Some(1) }),
        HeaderMap::new(),
    )
    .await
    .expect("the handler answers");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn end_answers_status() {
    // Ending answers with the job status; the session stays registered as
    // ended until the supervisor closes it, so ending twice is harmless.
    let owner = user(2);
    let (_dir, state, job_id, mut helper) = build_node(owner).await;
    make_ready(&state, job_id, &mut helper).await;
    for _ in 0..2 {
        let response = end_session(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await
        .expect("the handler answers");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    let body = session_body(&state, owner, job_id).await;
    assert_eq!(body.state, "ended");
    drop_session(&state, job_id);
    assert!(registry_session(&state, job_id).is_none());
}

#[tokio::test]
async fn live_job_starts() {
    // A re-adopted job has no session yet, which is starting, never ended.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    drop_session(&state, job_id);
    let body = session_body(&state, owner, job_id).await;
    assert_eq!(body.state, "starting");
    assert!(body.ended.is_none());
}

#[tokio::test]
async fn finished_job_ends() {
    // A settled session reports why it stopped, not a live state.
    let owner = user(2);
    let (_dir, state, _job_id, _helper) = build_node(owner).await;
    let settled = JobId::from_bytes([4u8; 16]);
    let mut record = JobRecord::new(
        settled,
        JobPayload::Execution(session_spec()),
        owner,
        node(),
        1_000,
        1_000,
        None,
    );
    record.state = JobState::Cancelled;
    record.finished_at_ms = Some(2_000);
    record.workspace_bucket = Some("lab-data".to_string());
    insert_job(&state.get_ctx().storage_handle, &record)
        .await
        .expect("the settled job is stored");
    let body = session_body(&state, owner, settled).await;
    assert_eq!(body.state, "ended");
    assert_eq!(
        body.ended.map(|ended| ended.reason),
        Some("cancelled".to_string())
    );
}

#[tokio::test]
async fn reported_reason_wins() {
    // The reason the workflow recorded beats the state's own default.
    let owner = user(2);
    let (_dir, state, _job_id, _helper) = build_node(owner).await;
    let storage = state.get_ctx().storage_handle.clone();
    let settled = JobId::from_bytes([5u8; 16]);
    let mut record = JobRecord::new(
        settled,
        JobPayload::Execution(session_spec()),
        owner,
        node(),
        1_000,
        1_000,
        None,
    );
    record.workspace_bucket = Some("lab-data".to_string());
    insert_job(&storage, &record)
        .await
        .expect("the queued job is stored");
    let ClaimOutcome::Claimed(claimed) = claim_job(&storage, settled, node(), 1_100)
        .await
        .expect("the job is claimable")
    else {
        panic!("the job was not claimed");
    };
    let token = claimed.claim.expect("a claim token").claim_token;
    let row = SessionReportRow {
        entry_key: "end".to_string(),
        detail: SessionReportDetail::End {
            reason: "idle".to_string(),
        },
    };
    put_job_entry(&storage, settled, token, b"end", &row)
        .await
        .expect("the report row is stored");
    cancel_running_job(&storage, settled, token, 2_000)
        .await
        .expect("the job settles");

    let body = session_body(&state, owner, settled).await;
    assert_eq!(body.state, "ended");
    assert_eq!(
        body.ended.map(|ended| ended.reason),
        Some("idle".to_string())
    );
}

#[test]
fn failed_item_redacted() {
    let failed = failed_input(
        "data/b.txt".to_string(),
        &ServerError::InternalError("private backend detail".to_string()),
    );
    assert!(!failed.error.contains("private backend detail"));
}

fn registry_session(state: &Arc<ServerState>, job_id: JobId) -> Option<Arc<Session>> {
    state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&job_id.to_string()))
}

/// Ends and closes the session like a finished supervisor, standing in for
/// a node restart.
fn drop_session(state: &Arc<ServerState>, job_id: JobId) {
    if let Some(session) = registry_session(state, job_id) {
        session.end(EndReason::Ended);
        if let Some(registry) = state.get_ctx().compute_handle.as_ref() {
            registry.sessions().close(&session);
        }
    }
}

async fn write_row(storage: &StorageHandle, key_space: &str, key: ByteView, value: Vec<u8>) {
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await;
}

/// A group in the realm with the documents a permission check reads.
/// `owner` holds every role of it, or none when `member` is false.
async fn seed_group(state: &Arc<ServerState>, owner: UserId, member: bool) -> Ulid {
    let realm_id = realm();
    let group_id = Ulid::generate();
    let actor = Actor {
        node_id: node(),
        user_id: owner,
        realm_id,
    };
    let mut auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    if !member {
        for role in auth.roles.values_mut() {
            role.assigned_users.remove(&owner);
        }
    }
    let group = Group {
        display_name: "lab".to_string(),
        group_id,
        realm_id,
        owner,
        roles: auth.roles.keys().copied().collect(),
    };
    let config = DocumentTarget::RealmConfig { realm_id };
    let storage = &state.get_ctx().storage_handle;
    write_row(
        storage,
        AUTH_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        RealmAuthorizationDocument::default_realm_doc(realm_id)
            .to_bytes(&actor)
            .unwrap(),
    )
    .await;
    write_row(
        storage,
        config.storage_keyspace(),
        config.storage_key(),
        RealmConfigDocument::default_for_realm(realm_id, Vec::new())
            .to_bytes(&actor)
            .unwrap(),
    )
    .await;
    write_row(
        storage,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_row(
        storage,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
    group_id
}

async fn seed_bucket(state: &Arc<ServerState>, bucket: &str, group_id: Ulid, owner: UserId) {
    let info = BucketInfo {
        group_id,
        created_at: UNIX_EPOCH,
        created_by: owner,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 1,
    };
    write_row(
        &state.get_ctx().storage_handle,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec().into(),
        info.to_bytes().unwrap(),
    )
    .await;
}

/// Puts `input.txt` into `bucket` as a reference bound to a connector.
async fn seed_reference(state: &Arc<ServerState>, bucket: &str, owner: UserId) {
    let version_id = Ulid::generate();
    let version = BlobVersion::reference(
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "genomes/ref.fna".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::generate()),
        },
        SourceMetadata {
            content_length: 15,
            content_type: None,
            etag: Some("etag-1".to_string()),
            last_modified: None,
            source_version: None,
        },
        UNIX_EPOCH,
        owner,
        UNIX_EPOCH,
    );
    let storage = &state.get_ctx().storage_handle;
    write_row(
        storage,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new(bucket, "input.txt")
            .to_bytes()
            .unwrap()
            .into(),
        CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
    )
    .await;
    write_row(
        storage,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, "input.txt", version_id)
            .to_bytes()
            .unwrap()
            .into(),
        version.to_bytes().unwrap(),
    )
    .await;
}

async fn inputs_body(response: Response) -> SessionInputsResponse {
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("the body reads");
    serde_json::from_slice(&bytes).expect("the body parses")
}

fn input(bucket: &str, dest_key: &str) -> SessionInputRequest {
    SessionInputRequest {
        bucket: bucket.to_string(),
        key: "input.txt".to_string(),
        version_id: None,
        source_node_id: None,
        dest_key: dest_key.to_string(),
        strategy: SessionInputStrategy::Snapshot,
    }
}

#[tokio::test]
async fn checks_keys_first() {
    // A traversing key in a later item must refuse the whole call, before
    // the first source is even looked up.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let response = stage_inputs(
        State(state),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SessionInputsRequest {
            items: vec![input("source", "data/a.txt"), input("source", "../escape")],
        }),
    )
    .await;
    assert!(matches!(response, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn queues_reference_copy() {
    // A reference behind a connector is not pulled inside the request: a
    // copy job is queued, answered as pending and remembered by the session.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let group_id = seed_group(&state, owner, true).await;
    seed_bucket(&state, "source", group_id, owner).await;
    seed_bucket(&state, "lab-data", group_id, owner).await;
    seed_reference(&state, "source", owner).await;

    let response = stage_inputs(
        State(state.clone()),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SessionInputsRequest {
            items: vec![input("source", "data/genomes/ref.fna")],
        }),
    )
    .await
    .expect("the copy is queued");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = inputs_body(response).await;
    assert!(body.staged.is_empty() && body.failed.is_empty());
    assert_eq!(body.pending.len(), 1);
    assert_eq!(body.pending[0].dest_key, "data/genomes/ref.fna");
    assert_eq!(body.pending[0].source_node_id, node().to_string());

    let copy_id = JobId::from_str(&body.pending[0].job_id).expect("a job id");
    let record = read_job_record(&state.get_ctx().storage_handle, copy_id, None)
        .await
        .expect("the record reads")
        .expect("the copy job is stored");
    let JobPayload::CopyObject(spec) = record.payload else {
        panic!("a copy job was queued");
    };
    assert_eq!(
        (spec.source_bucket.as_str(), spec.dest_bucket.as_str()),
        ("source", "lab-data")
    );
    assert_eq!(spec.dest_key, "data/genomes/ref.fna");
    assert_eq!(spec.auth_context.user_id, owner);
    assert_eq!(record.progress.unit, "bytes");

    let session = registry_session(&state, job_id).expect("session is live");
    assert_eq!(
        session
            .pending()
            .into_iter()
            .map(|p| p.job_id)
            .collect::<Vec<_>>(),
        vec![body.pending[0].job_id.clone()]
    );
    assert!(session.inventory().is_empty());
}

#[tokio::test]
async fn link_crosses_groups() {
    // A linked reference is cloned from the source version, so it needs
    // neither the connector in the workspace's group nor any byte.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let own_group = seed_group(&state, owner, true).await;
    let shared_group = seed_group(&state, owner, true).await;
    seed_bucket(&state, "lab-data", own_group, owner).await;
    seed_bucket(&state, "shared", shared_group, owner).await;
    seed_reference(&state, "shared", owner).await;

    let mut item = input("shared", "data/genomes/ref.fna");
    item.strategy = SessionInputStrategy::Reference;
    let response = stage_inputs(
        State(state.clone()),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SessionInputsRequest { items: vec![item] }),
    )
    .await
    .expect("the reference is linked");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = inputs_body(response).await;
    assert!(body.pending.is_empty() && body.failed.is_empty());
    assert_eq!(body.staged.len(), 1);
    assert!(body.staged[0].linked);
    assert_eq!(body.staged[0].bytes, 15);
    assert!(body.staged[0].blake3.is_empty());

    let head = drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: "lab-data".to_string(),
            key: "data/genomes/ref.fna".to_string(),
            version_id: None,
        }),
        &state.get_ctx(),
    )
    .await
    .expect("the head succeeds");
    assert!(head.location.is_none(), "no bytes were stored");
    let binding = head
        .source_binding
        .expect("the workspace holds a reference");
    assert_eq!(binding.descriptor.source_path, "genomes/ref.fna");
    let session = registry_session(&state, job_id).expect("session is live");
    assert_eq!(session.inventory().len(), 1);
}

#[tokio::test]
async fn refuses_unreadable_source() {
    // The caller's read permission on the source bucket is checked before
    // any lookup of the object, so a foreign group's data never moves.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let own_group = seed_group(&state, owner, true).await;
    let foreign_group = seed_group(&state, owner, false).await;
    seed_bucket(&state, "lab-data", own_group, owner).await;
    seed_bucket(&state, "foreign", foreign_group, user(3)).await;
    seed_reference(&state, "foreign", user(3)).await;

    let response = stage_inputs(
        State(state.clone()),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SessionInputsRequest {
            items: vec![input("foreign", "data/ref.fna")],
        }),
    )
    .await;
    assert!(matches!(response, Err(ServerError::Forbidden)));
    let session = registry_session(&state, job_id).expect("session is live");
    assert!(session.pending().is_empty());
}

#[tokio::test]
async fn keeps_refusal_reason() {
    // Nothing landed, so the caller keeps the coded refusal instead of a
    // 202 that claims a partial result, and the session is not kept alive.
    let owner = user(2);
    let (_dir, state, job_id, _helper) = build_node(owner).await;
    let session = registry_session(&state, job_id).expect("session is live");
    let quiet = session.snapshot().last_event_id;

    let response = stage_inputs(
        State(state.clone()),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
        Json(SessionInputsRequest {
            items: vec![input("missing", "data/a.txt")],
        }),
    )
    .await;

    assert!(matches!(response, Err(ServerError::NotFound)));
    assert_eq!(session.snapshot().last_event_id, quiet);
}

#[tokio::test]
async fn reports_failed_items() {
    let staged = vec![StagedInputResponse {
        dest_key: "data/a.txt".to_string(),
        bytes: 4,
        blake3: String::new(),
        source_node_id: node().to_string(),
        version_id: String::new(),
        linked: false,
    }];
    let failed = vec![FailedInputResponse {
        dest_key: "data/b.txt".to_string(),
        error: "Not found".to_string(),
    }];
    let response = inputs_outcome(staged, Vec::new(), failed, Some(ServerError::NotFound))
        .expect("a partial result is accepted");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = inputs_body(response).await;
    assert_eq!(body.staged.len(), 1);
    assert_eq!(body.failed[0].dest_key, "data/b.txt");
}

#[test]
fn queued_is_progress() {
    // A call that only queued copies still answers 202 instead of the
    // refusal a later item earned.
    let pending = vec![PendingInputResponse {
        dest_key: "data/a.txt".to_string(),
        job_id: "01JJCPYJB00123456789ABCDEF".to_string(),
        source_node_id: node().to_string(),
    }];
    let response = inputs_outcome(Vec::new(), pending, Vec::new(), Some(ServerError::NotFound))
        .expect("a queued copy is progress");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(matches!(
        inputs_outcome(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some(ServerError::NotFound)
        ),
        Err(ServerError::NotFound)
    ));
}

/// The parsed body of a session read.
async fn session_body(state: &Arc<ServerState>, owner: UserId, job_id: JobId) -> SessionResponse {
    let response = get_session(
        State(state.clone()),
        Extension(auth_for(owner)),
        Path(job_id.to_string()),
    )
    .await
    .expect("the handler answers");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("the body reads");
    serde_json::from_slice(&bytes).expect("the body parses")
}
