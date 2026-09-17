use super::finalize::{collect_or_park, exit_message, finalize_attempt, finalize_cancel};
use super::prepare::{NETWORK_TAG_KEY, PreparedTask, build_task_spec, prepare_workspace};
use super::recovery::{pre_submit_failure, recover_failed_submit};
use super::supervise::{supervise_and_finalize, walltime_anchor};
use super::*;
use crate::driver::drive;
use crate::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use crate::jobs::store::{
    ClaimOutcome, claim_job, handoff_external_attempt, insert_job, put_crate_status,
    read_attempt_control, record_attempt_started, set_cancel_requested,
};
use crate::jobs::workflow::workspace::mint_workspace_credential;
use crate::jobs::{JOB_HEARTBEAT_MS, JOB_MAX_ATTEMPTS};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_compute::ExecutorRegistry;
use aruna_compute::session::{EndReason, Session, SessionConfig};
use aruna_core::UserId;
use aruna_core::compute::{
    AdoptableEvidence, AttemptPhase, AttemptStatus, CancelEvidence, LogLimits, LogTails, NOBODY,
    NetworkAccess, ReconcileEvidence, ResumePoint, StagingMode, TaskOutput, TaskSpec, UserSpec,
};
use aruna_core::structs::execution::job::{
    JobErrorKind, JobResultPayload, JobState, MAX_MESSAGE_BYTES, OutputDestination,
    OutputSelection, SessionReportDetail, SessionReportRow, WorkspaceMode,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::sync::Notify;
use ulid::Ulid;

use crate::tests::workflow::{execution_spec, node_id};

fn job_id() -> JobId {
    crate::jobs::submit::mint_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap()
}

enum StubReconcile {
    NotFound,
    Unavailable,
    Waiting,
    Pending,
    Adopted,
}

struct StubBackend {
    reconcile: StubReconcile,
    submits: Mutex<Vec<String>>,
    wait_started: Notify,
    logs_started: Notify,
    logs_release: Notify,
    logs_fail: AtomicBool,
    cancel_lost: AtomicBool,
    cancels: AtomicUsize,
    reconcile_started: Notify,
    image_started: Notify,
    image_stall: AtomicBool,
}

impl StubBackend {
    fn new(reconcile: StubReconcile) -> Arc<Self> {
        Arc::new(Self {
            reconcile,
            submits: Mutex::new(Vec::new()),
            wait_started: Notify::new(),
            logs_started: Notify::new(),
            logs_release: Notify::new(),
            logs_fail: AtomicBool::new(false),
            cancel_lost: AtomicBool::new(false),
            cancels: AtomicUsize::new(0),
            reconcile_started: Notify::new(),
            image_started: Notify::new(),
            image_stall: AtomicBool::new(false),
        })
    }
}

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
        _image: &str,
        _cancel: &CancellationToken,
    ) -> Result<String, BackendError> {
        if self.image_stall.load(Ordering::Relaxed) {
            self.image_started.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(
            "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        )
    }
    async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }
    async fn submit(
        &self,
        _context: &FenceContext,
        spec: &TaskSpec,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        self.submits
            .lock()
            .unwrap()
            .push(spec.attempt.external_name());
        Err(BackendError::Unavailable("stub submit".to_string()))
    }
    async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::Unavailable("stub status".to_string()))
    }
    async fn wait(
        &self,
        _context: &FenceContext,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        if matches!(self.reconcile, StubReconcile::Waiting) {
            self.wait_started.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(AttemptStatus {
            phase: AttemptPhase::Exited { code: 0 },
            backend_ref: "done".to_string(),
            started_at_ms: Some(1),
            finished_at_ms: Some(2),
            detail: None,
        })
    }
    async fn cancel(&self, _context: &FenceContext) -> Result<CancelEvidence, BackendError> {
        self.cancels.fetch_add(1, Ordering::Relaxed);
        if self.cancel_lost.load(Ordering::Relaxed) {
            return Ok(CancelEvidence::Stopped(AttemptStatus {
                phase: AttemptPhase::SystemError {
                    reason: "node evicted".to_string(),
                },
                backend_ref: "c1".to_string(),
                started_at_ms: Some(1),
                finished_at_ms: Some(2),
                detail: None,
            }));
        }
        Ok(CancelEvidence::AlreadyGone)
    }
    async fn fetch_logs(
        &self,
        _context: &FenceContext,
        _limits: &LogLimits,
    ) -> Result<LogTails, BackendError> {
        if self.logs_fail.load(Ordering::Relaxed) {
            return Err(BackendError::Api("log stream truncated".to_string()));
        }
        self.logs_started.notify_one();
        self.logs_release.notified().await;
        Ok(LogTails::default())
    }
    async fn fetch_output(
        &self,
        _context: &FenceContext,
        _path: &str,
    ) -> Result<TaskOutput, BackendError> {
        Err(BackendError::InvalidSpec("no output".to_string()))
    }
    async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
        match self.reconcile {
            StubReconcile::NotFound => ReconcileEvidence::Absent,
            StubReconcile::Unavailable => {
                ReconcileEvidence::Unavailable(BackendError::Unavailable("down".to_string()))
            }
            StubReconcile::Waiting => ReconcileEvidence::Absent,
            StubReconcile::Pending => {
                self.reconcile_started.notify_one();
                std::future::pending::<ReconcileEvidence>().await
            }
            StubReconcile::Adopted => ReconcileEvidence::Adoptable(AdoptableEvidence {
                status: AttemptStatus {
                    phase: AttemptPhase::Running,
                    backend_ref: "attempt".to_string(),
                    started_at_ms: Some(1),
                    finished_at_ms: None,
                    detail: None,
                },
                resume: ResumePoint::Observe,
            }),
        }
    }
    async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }
}

fn fence(attempt: &AttemptRef) -> FenceContext {
    FenceContext {
        attempt: attempt.clone(),
        attempt_epoch: 1,
        controller_generation: 1,
    }
}

fn context(storage: StorageHandle) -> Arc<DriverContext> {
    Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    })
}

#[test]
fn exit_message_bounds() {
    // An over-long detail must not push the message past the stored cap,
    // because storage drops an over-cap message entirely.
    assert_eq!(exit_message(2, None), "container exited with code 2");
    assert_eq!(exit_message(2, Some("  ")), "container exited with code 2");
    assert_eq!(
        exit_message(2, Some(" Error: boom ")),
        "container exited with code 2: Error: boom"
    );

    let detail = "a".repeat(MAX_MESSAGE_BYTES) + "end";
    let message = exit_message(2, Some(&detail));
    assert!(message.len() <= MAX_MESSAGE_BYTES);
    assert!(message.starts_with("container exited with code 2: "));
    assert!(message.ends_with("end"));
}

#[tokio::test]
async fn denied_write_blocks() {
    // Queued execution must not outlive the submitter's group write access.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = context(storage);
    let spec = execution_spec();
    let job_id = job_id();
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(spec.clone()),
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1,
        1,
        None,
    );
    record.workspace_mode = WorkspaceMode::Existing;
    let bucket = "shared-workspace".to_string();
    record.workspace_bucket = Some(bucket.clone());

    let Err(error) = prepare_workspace(&context, &spec, &record, node_id(7), &bucket).await else {
        panic!("workspace preparation unexpectedly succeeded");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
    assert!(
        matches!(
            drive(GetBucketOperation::new(bucket.clone()), context.as_ref())
                .await
                .unwrap_err(),
            GetBucketError::NotFound
        ),
        "a run never creates the bucket it names"
    );

    let Err(error) = Box::pin(mint_workspace_credential(
        &context,
        &spec,
        &record,
        node_id(7),
        &bucket,
    ))
    .await
    else {
        panic!("workspace credential mint unexpectedly succeeded");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

/// A claimed execution job in `Ready` with its attempt intent written, i.e.
/// the exact state at the moment `backend.submit()` fails.
async fn ready_with_intent(storage: &StorageHandle) -> (JobRecord, ulid::Ulid, AttemptRef) {
    let job_id = job_id();
    let record = JobRecord::new(
        job_id,
        JobPayload::Execution(execution_spec()),
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1,
        1,
        None,
    );
    insert_job(storage, &record).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(storage, job_id, node_id(7), 2).await.unwrap()
    else {
        panic!("claim failed");
    };
    let token = claimed.claim.as_ref().unwrap().claim_token;
    transition_to_preparing(storage, job_id, token, 3)
        .await
        .unwrap();
    transition_to_ready(storage, job_id, token, 4)
        .await
        .unwrap();
    let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), claimed.attempts);
    let intent = AttemptIntent {
        attempt_no: claimed.attempts,
        external_name: attempt.external_name(),
        executor_kind: "docker".to_string(),
        pinned_image:
            "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        attempt_epoch: 0,
    };
    let record = record_attempt_intent(storage, job_id, token, intent, None, 5)
        .await
        .unwrap();
    (record.record, token, attempt)
}

// A submit error with an unobservable backend parks the job Indeterminate and
// keeps the write-ahead intent, so container a{N} stays adoptable, never a{N+1}.
#[tokio::test]
async fn ambiguous_submit_parks() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = context(storage.clone());
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::Unavailable);

    recover_failed_submit(
        &ctx,
        record.job_id,
        token,
        &backend,
        &fence(&attempt),
        &execution_spec(),
        "ws-test",
        &CancellationToken::new(),
        BackendError::Unavailable("io fault".to_string()),
    )
    .await;

    let stored = read_job_record(&storage, record.job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Indeterminate);
    assert_eq!(stored.attempts, 1, "the park charges an attempt");
    let intent = stored.attempt_intent.expect("intent retained");
    assert_eq!(intent.external_name, attempt.external_name());
}

// An output-inventory failure at finalize parks the job for a retry instead
// of terminalizing Succeeded with a false-empty output manifest.
#[tokio::test]
async fn inventory_failure_parks() {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
    use byteview::ByteView;

    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, _net) = net_context(storage.clone()).await;
    let (record, token, _attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, 6)
        .await
        .unwrap();

    // Poison the workspace listing: an undecodable head row fails the scan.
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: ByteView::from(b"ws-test/poison".to_vec()),
            value: ByteView::from(vec![0xFF]),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event: {other:?}"),
    }

    let mut spec = execution_spec();
    spec.file_outputs.push(OutputSelection {
        container_path: "/out/result".to_string(),
        path_prefix: None,
        destination_node_id: Some(ctx.net_handle.as_ref().unwrap().node_id()),
        destination: OutputDestination::S3 {
            bucket: "dest".to_string(),
            key: "result".to_string(),
        },
        name: None,
        description: None,
    });
    spec.output_prefixes = vec!["poison".to_string()];
    let epoch = record.attempt_intent.as_ref().unwrap().attempt_epoch;
    let control = read_attempt_control(&storage, job_id, epoch, None)
        .await
        .unwrap()
        .unwrap();
    assert!(
        Box::pin(collect_or_park(
            &ctx, job_id, token, &spec, "ws-test", &control,
        ))
        .await
        .is_none()
    );

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Indeterminate);
    assert!(stored.result.is_none(), "no false-empty manifest recorded");
}

// Infrastructure evidence proves nothing about the job, so a system error must
// park instead of writing a terminal row the family would read as a verdict.
#[tokio::test]
async fn system_error_parks() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, Some(1), 6)
        .await
        .unwrap();
    let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::NotFound);

    Box::pin(finalize_attempt(
        &ctx,
        job_id,
        token,
        &backend,
        &fence(&attempt),
        &execution_spec(),
        "ws-test",
        Ok(AttemptStatus {
            phase: AttemptPhase::SystemError {
                reason: "node evicted".to_string(),
            },
            backend_ref: "c1".to_string(),
            started_at_ms: Some(1),
            finished_at_ms: Some(2),
            detail: None,
        }),
    ))
    .await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Indeterminate);
    assert!(!stored.locally_exhausted);
    assert!(stored.finished_at_ms.is_none());
    assert_eq!(
        stored.last_error.as_ref().map(|error| error.kind),
        Some(JobErrorKind::Retryable)
    );
}

// A stopped cancellation whose only evidence is infrastructure must terminalize
// as Cancelled: a Failed row would read as a job-specific verdict.
#[tokio::test]
async fn cancel_lost_evidence() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, Some(1), 6)
        .await
        .unwrap();
    let stub = StubBackend::new(StubReconcile::NotFound);
    stub.cancel_lost.store(true, Ordering::Relaxed);
    stub.logs_release.notify_one();
    let backend: Arc<dyn ExecutorBackend> = stub;

    Box::pin(finalize_cancel(
        &ctx,
        job_id,
        token,
        &backend,
        &fence(&attempt),
        &execution_spec(),
        "ws-test",
    ))
    .await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    assert!(stored.finished_at_ms.is_some());
}

// A retryably failing capture must terminalize at the attempt cap; without the
// charge it parks Indeterminate and the lease sweep re-drives it forever.
#[tokio::test]
async fn capture_failure_caps() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, Some(1), 6)
        .await
        .unwrap();
    let stub = StubBackend::new(StubReconcile::NotFound);
    stub.logs_fail.store(true, Ordering::Relaxed);
    let backend: Arc<dyn ExecutorBackend> = stub;

    for _ in 0..JOB_MAX_ATTEMPTS {
        Box::pin(finalize_attempt(
            &ctx,
            job_id,
            token,
            &backend,
            &fence(&attempt),
            &execution_spec(),
            "ws-test",
            Ok(AttemptStatus {
                phase: AttemptPhase::Exited { code: 0 },
                backend_ref: "c1".to_string(),
                started_at_ms: Some(1),
                finished_at_ms: Some(2),
                detail: None,
            }),
        ))
        .await;
    }

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Indeterminate);
    assert!(stored.locally_exhausted);
    assert_eq!(stored.attempts, JOB_MAX_ATTEMPTS);
    assert!(stored.claim.is_none());
    assert!(
        matches!(stored.result, Some(JobResultPayload::Execution { .. })),
        "cleanup and the run crate need a result"
    );
}

#[tokio::test]
async fn finalize_renews_lease() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::NotFound);

    let task = tokio::spawn(supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        execution_spec(),
        "ws-test".to_string(),
        CancellationToken::new(),
    ));
    backend.logs_started.notified().await;
    let before = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap()
        .claim
        .unwrap()
        .lease_expires_ms;

    tokio::time::pause();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(JOB_HEARTBEAT_MS)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    let mut after = before;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        after = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap()
            .claim
            .unwrap()
            .lease_expires_ms;
        if after > before {
            break;
        }
    }
    assert!(after > before);

    backend.logs_release.notify_one();
    task.await.unwrap();
}

/// A context that can sign: terminal success stores an output record, which
/// needs this node's key.
async fn net_context(storage: StorageHandle) -> (Arc<DriverContext>, aruna_net::NetHandle) {
    let net = aruna_net::NetHandle::new(
        aruna_net::NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id: aruna_core::structs::identity::realm::RealmId([1; 32]),
            discovery_method: aruna_net::DiscoveryMethod::None,
            relay_method: aruna_net::RelayMethod::None,
            ..aruna_net::NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    (context, net)
}

/// A session job's spec: the tags the node writes at submit, and a short
/// idle wait so the timer test does not depend on the realm default.
fn session_spec(idle_after_ms: Option<u64>) -> ExecutionSpec {
    use aruna_core::compute::runtimes::{
        SESSION_IDLE_TAG, SESSION_RUNTIME_TAG, SESSION_TAG, SESSION_TAG_NOTEBOOK,
    };
    let mut spec = execution_spec();
    spec.tags
        .insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
    spec.tags.insert(
        SESSION_RUNTIME_TAG.to_string(),
        "python-notebook".to_string(),
    );
    if let Some(idle) = idle_after_ms {
        spec.tags
            .insert(SESSION_IDLE_TAG.to_string(), idle.to_string());
    }
    spec
}

/// A context whose compute plane can register sessions.
async fn session_context(
    storage: StorageHandle,
) -> (
    Arc<DriverContext>,
    aruna_net::NetHandle,
    Arc<ExecutorRegistry>,
) {
    let (context, net) = net_context(storage).await;
    let registry = Arc::new(ExecutorRegistry::new());
    let mut context = context;
    Arc::get_mut(&mut context).unwrap().compute_handle = Some(registry.clone());
    (context, net, registry)
}

/// Waits for the supervisor to register the session it is about to watch.
async fn wait_for_session(
    registry: &Arc<ExecutorRegistry>,
    job_id: JobId,
    backend: &StubBackend,
) -> Arc<Session> {
    tokio::time::timeout(Duration::from_secs(120), backend.wait_started.notified())
        .await
        .expect("the supervisor never started waiting");
    registry
        .sessions()
        .get(&job_id.to_string())
        .expect("the supervisor registers its session before waiting")
}

#[tokio::test]
async fn session_end_succeeds() {
    // Ending a session finishes its job, so quota is released at once.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net, registry) = session_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    crate::jobs::store::mutate_job(&storage, job_id, |record| {
        record.payload = JobPayload::Execution(session_spec(None));
        Ok(crate::jobs::store::JobMutation::Persist)
    })
    .await
    .unwrap();
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::Waiting);

    let supervisor = tokio::spawn(supervise_and_finalize(
        ctx.clone(),
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        session_spec(None),
        "ws-test".to_string(),
        CancellationToken::new(),
    ));
    let session = wait_for_session(&registry, job_id, &backend).await;
    session.end(EndReason::Ended);
    supervisor.await.unwrap();

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Succeeded);
    assert!(stored.report_digest.is_some());
    assert_eq!(session.finished().await, EndReason::Ended);
    assert!(registry.sessions().get(&job_id.to_string()).is_none());
    let report =
        crate::jobs::service::read_owned_report(&ctx, stored.created_by, job_id, None, None, 10)
            .await
            .unwrap();
    let crate::jobs::service::JobReportLookup::Ready { rows, .. } = report else {
        panic!("ended session report must be readable");
    };
    assert_eq!(rows.len(), 1);
    let row: SessionReportRow = postcard::from_bytes(&rows[0].1).unwrap();
    assert_eq!(
        row.detail,
        SessionReportDetail::End {
            reason: "ended".to_string()
        }
    );
    net.shutdown().await;
}

#[tokio::test]
async fn session_idle_succeeds() {
    // The idle timer finishes the job on its own, with reason `idle`.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net, _registry) = session_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::Waiting);

    supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        session_spec(Some(1)),
        "ws-test".to_string(),
        CancellationToken::new(),
    )
    .await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Succeeded);
    net.shutdown().await;
}

#[tokio::test]
async fn session_kernel_fails() {
    // A kernel that died is a permanent execution failure, not a clean end.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net, registry) = session_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::Waiting);

    let supervisor = tokio::spawn(supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        session_spec(None),
        "ws-test".to_string(),
        CancellationToken::new(),
    ));
    wait_for_session(&registry, job_id, &backend)
        .await
        .end(EndReason::KernelExit);
    supervisor.await.unwrap();

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Failed);
    assert_eq!(stored.last_error.unwrap().message, "session kernel exited");
    net.shutdown().await;
}

/// Moves virtual time past the next execution heartbeat so a rotated claim is
/// noticed without a real wait.
async fn skip_heartbeat() {
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(2 * JOB_HEARTBEAT_MS)).await;
    tokio::time::resume();
}

// A lost claim while the attempt wait is still pending must stop and release
// this node's session instead of leaking it in the registry.
#[tokio::test]
async fn superseded_releases_session() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net, registry) = session_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::Waiting);

    let supervisor = tokio::spawn(supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        session_spec(None),
        "ws-test".to_string(),
        CancellationToken::new(),
    ));
    let session = wait_for_session(&registry, job_id, &backend).await;
    let taken_over = lose_claim(&storage, job_id, token).await;

    skip_heartbeat().await;
    tokio::time::timeout(
        Duration::from_secs(3 * JOB_HEARTBEAT_MS / 1_000),
        supervisor,
    )
    .await
    .expect("the superseded supervisor must return")
    .expect("the supervisor task must not panic");

    assert_eq!(session.finished().await, EndReason::Cancelled);
    assert!(
        registry.sessions().get(&job_id.to_string()).is_none(),
        "the superseded session must be released"
    );
    let after = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, taken_over, "a lost claim must not be written");
    net.shutdown().await;
}

// A replacement registered under the same job id while the old attempt is
// still supervised must survive the superseded supervisor's cleanup.
#[tokio::test]
async fn superseded_keeps_replacement() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net, registry) = session_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::Waiting);

    let supervisor = tokio::spawn(supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        session_spec(None),
        "ws-test".to_string(),
        CancellationToken::new(),
    ));
    let first = wait_for_session(&registry, job_id, &backend).await;
    // The attempt's session ends without terminalizing, so a replacement can
    // register under the same job id while the supervisor still waits.
    first.end(EndReason::Cancelled);
    let replacement = registry.sessions().open(
        SessionConfig {
            job_id: job_id.to_string(),
            public_job_id: job_id.to_string(),
            runtime: "python-notebook".to_string(),
            workspace_bucket: "ws-test".to_string(),
            executor_node_id: node_id(7).to_string(),
            idle_after_ms: 600_000,
            credential_expires_ms: 0,
        },
        backend.clone(),
        fence(&attempt),
    );
    assert!(!Arc::ptr_eq(&first, &replacement));
    let taken_over = lose_claim(&storage, job_id, token).await;

    skip_heartbeat().await;
    tokio::time::timeout(
        Duration::from_secs(3 * JOB_HEARTBEAT_MS / 1_000),
        supervisor,
    )
    .await
    .expect("the superseded supervisor must return")
    .expect("the supervisor task must not panic");

    let registered = registry
        .sessions()
        .get(&job_id.to_string())
        .expect("the replacement must survive the old cleanup");
    assert!(Arc::ptr_eq(&registered, &replacement));
    let after = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, taken_over, "a lost claim must not be written");
    net.shutdown().await;
}

// A cancellation that ends supervision must still release the session.
#[tokio::test]
async fn cancel_releases_session() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net, registry) = session_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    set_cancel_requested(&storage, job_id, 7).await.unwrap();
    let backend = StubBackend::new(StubReconcile::NotFound);

    supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        session_spec(None),
        "ws-test".to_string(),
        CancellationToken::new(),
    )
    .await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    assert!(
        registry.sessions().get(&job_id.to_string()).is_none(),
        "the session must be released"
    );
    net.shutdown().await;
}

#[tokio::test]
async fn cancel_beats_success() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (ctx, net) = net_context(storage.clone()).await;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, None, unix_timestamp_millis())
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::NotFound);

    let task = tokio::spawn(supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        execution_spec(),
        "ws-test".to_string(),
        CancellationToken::new(),
    ));
    backend.logs_started.notified().await;
    set_cancel_requested(&storage, job_id, 7).await.unwrap();
    backend.logs_release.notify_one();
    task.await.unwrap();

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    net.shutdown().await;
}

#[tokio::test]
async fn walltime_fails_job() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    begin_external_running(&storage, job_id, token, Some(1), 6)
        .await
        .unwrap();
    let backend = StubBackend::new(StubReconcile::Waiting);
    let mut spec = execution_spec();
    spec.resources.max_walltime_ms = Some(0);

    supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        spec,
        "ws-test".to_string(),
        CancellationToken::new(),
    )
    .await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(backend.cancels.load(Ordering::Relaxed), 1);
    assert_eq!(stored.state, JobState::Failed);
    assert_eq!(
        stored.last_error.unwrap().message,
        "walltime limit exceeded"
    );
}

#[tokio::test]
async fn arms_missing_start() {
    // A record carrying no backend start evidence must still be capped,
    // anchored at the moment supervision begins.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    assert!(record.started_at_ms.is_none());
    let backend = StubBackend::new(StubReconcile::Waiting);
    let mut spec = execution_spec();
    spec.resources.max_walltime_ms = Some(0);

    supervise_and_finalize(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        spec,
        "ws-test".to_string(),
        CancellationToken::new(),
    )
    .await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(backend.cancels.load(Ordering::Relaxed), 1);
    assert_eq!(stored.state, JobState::Failed);
    assert_eq!(
        stored.last_error.unwrap().message,
        "walltime limit exceeded"
    );
}

#[tokio::test]
async fn anchor_survives_restart() {
    // A crashlooping supervisor must not keep granting the attempt a fresh
    // pre-Running window, so the first derived anchor is the durable one.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (record, token, _attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    assert!(record.started_at_ms.is_none());

    let first = Box::pin(walltime_anchor(&storage, job_id, token)).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.started_at_ms, Some(first));
    assert_eq!(
        Box::pin(walltime_anchor(&storage, job_id, token)).await,
        first
    );
    // Backend start evidence never moves an anchor that already exists.
    record_attempt_started(&storage, job_id, token, first.saturating_add(60_000))
        .await
        .unwrap();
    assert_eq!(
        Box::pin(walltime_anchor(&storage, job_id, token)).await,
        first
    );
}

#[tokio::test]
async fn reconcile_skips_submit() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let mut ctx = context(storage.clone());
    Arc::get_mut(&mut ctx).unwrap().task_handle = None;
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;
    set_cancel_requested(&storage, job_id, 6).await.unwrap();
    let backend = StubBackend::new(StubReconcile::NotFound);

    reconcile::resume_attempt(
        ctx,
        job_id,
        token,
        backend.clone(),
        fence(&attempt),
        AttemptPhase::Submitted,
        execution_spec(),
        "ws-test".to_string(),
        record,
        CancellationToken::new(),
    )
    .await;

    assert!(backend.submits.lock().unwrap().is_empty());
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
}

// Absence after intent retries the same name and retains the lineage.
#[tokio::test]
async fn absent_retries_same() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = context(storage.clone());
    let (record, token, attempt) = ready_with_intent(&storage).await;
    let backend: Arc<dyn ExecutorBackend> = StubBackend::new(StubReconcile::NotFound);

    recover_failed_submit(
        &ctx,
        record.job_id,
        token,
        &backend,
        &fence(&attempt),
        &execution_spec(),
        "ws-test",
        &CancellationToken::new(),
        BackendError::Unavailable("io fault".to_string()),
    )
    .await;

    let stored = read_job_record(&storage, record.job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Indeterminate);
    assert_eq!(stored.attempts, 1);
    assert!(stored.attempt_intent.is_some());
}

// A re-driven crate job must return the already-written resource, not mint a
// second document; without the durable-status return it fails on no net handle.
#[tokio::test]
async fn crate_write_idempotent() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let (record, token, _attempt) = ready_with_intent(&storage).await;
    let job_id = record.job_id;

    put_crate_status(
        &storage,
        job_id,
        &aruna_core::structs::execution::job::RunCrateStatus::Written {
            resource: "already-there".to_string(),
        },
    )
    .await
    .unwrap();

    let ctx = JobContext {
        driver: context(storage.clone()),
        job_id,
        owner_node_id: record.owner_node_id,
        claim_token: token,
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&record.progress),
    };
    let outcome = super::run_crate::write_run_crate(&ctx, job_id).await;
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource }) => {
            assert_eq!(resource, "already-there");
        }
        _ => panic!("re-drive must return the already-written resource"),
    }
}

#[test]
fn defaults_task_walltime() {
    let spec = build_task_spec(
        &execution_spec(),
        &AttemptRef::new("job", 1),
        "alpine@sha256:digest",
        PreparedTask {
            inputs: Vec::new(),
            mounts: Vec::new(),
            secrets: BTreeMap::new(),
            staging: StagingMode::Files,
            workspace: None,
        },
        NOBODY,
    );

    assert_eq!(
        spec.resources.max_walltime,
        Some(Duration::from_secs(86_400))
    );
}

#[test]
fn carries_backend_identity() {
    // The manifest must report the backend's real identity, not the 65534 default.
    let run_as = UserSpec {
        uid: NOBODY.uid ^ 1,
        gid: NOBODY.gid ^ 1,
    };
    let spec = build_task_spec(
        &execution_spec(),
        &AttemptRef::new("job", 1),
        "alpine@sha256:digest",
        PreparedTask {
            inputs: Vec::new(),
            mounts: Vec::new(),
            secrets: BTreeMap::new(),
            staging: StagingMode::Files,
            workspace: None,
        },
        run_as,
    );

    assert_eq!(spec.security.run_as, run_as);
}

#[test]
fn carries_network_access() {
    let mut execution = execution_spec();
    execution
        .tags
        .insert(NETWORK_TAG_KEY.to_string(), "open".to_string());
    let spec = build_task_spec(
        &execution,
        &AttemptRef::new("job", 1),
        "alpine@sha256:digest",
        PreparedTask {
            inputs: Vec::new(),
            mounts: Vec::new(),
            secrets: BTreeMap::new(),
            staging: StagingMode::Files,
            workspace: None,
        },
        NOBODY,
    );

    assert_eq!(spec.security.network, NetworkAccess::Open);
}

/// Writes the receipted reservation and the node subject a fenced start
/// compares against.
async fn store_site(storage: &StorageHandle, job_id: JobId, generation: u64, digest: [u8; 32]) {
    use aruna_core::compute::quota::JobReservationRecord;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{JOB_RESERVATION_KEYSPACE, NODE_SUBJECT_KEYSPACE};
    use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
    use aruna_core::structs::placement::policy::PlacementSubject;

    let reservation = JobReservationRecord {
        execution_id: Ulid::from_bytes([0xA1; 16]),
        job_id,
        logical_job_id: job_id,
        resources: aruna_core::structs::execution::job::EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        },
        created_at_ms: 1,
        subject_generation: generation,
        subject_digest: digest,
    };
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: JOB_RESERVATION_KEYSPACE.to_string(),
            key: reservation.execution_id.to_bytes().as_slice().into(),
            value: postcard::to_allocvec(&reservation).unwrap().into(),
            txn_id: None,
        })
        .await;
    let record = NodeSubjectRecord::seed(PlacementSubject {
        node_id: node_id(7),
        generation: 1,
        location: "eu-west".to_string(),
        labels: Default::default(),
        executor_kind: None,
        local_to_controller: true,
    })
    .unwrap();
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: NODE_SUBJECT_KEY.to_vec().into(),
            value: record.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
}

fn compute_context(storage: StorageHandle) -> Arc<DriverContext> {
    let mut registry = aruna_compute::ExecutorRegistry::new();
    registry.register(StubBackend::new(StubReconcile::NotFound));
    Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: Some(Arc::new(registry)),
    })
}

#[tokio::test]
async fn unreceipted_start_unfenced() {
    // A local job never reserved capacity, so no receipt fences its start.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = compute_context(storage.clone());

    assert!(
        resolve_backend(&ctx, &execution_spec(), job_id())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn drifted_site_refuses() {
    // The receipt stored one execution site; a node advertising another one
    // must refuse to start the accepted work, retryably.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = compute_context(storage.clone());
    let job_id = job_id();
    store_site(&storage, job_id, 99, [7u8; 32]).await;

    let Err(error) = resolve_backend(&ctx, &execution_spec(), job_id).await else {
        panic!("a drifted subject must refuse the start");
    };
    assert_eq!(error.kind, JobErrorKind::Retryable);
    assert!(error.message.contains("drifted"));
}

#[tokio::test]
async fn drift_requeues_attempt() {
    // A fenced refusal must return the job to the queue, not terminalize it:
    // the site may return, or another target may take the work.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = compute_context(storage.clone());
    let (record, token, _) = ready_with_intent(&storage).await;
    store_site(&storage, record.job_id, 99, [7u8; 32]).await;
    let Err(error) = resolve_backend(&ctx, &execution_spec(), record.job_id).await else {
        panic!("a drifted subject must refuse the start");
    };

    pre_submit_failure(&ctx, record.job_id, token, &record, error, false).await;

    let stored = read_job_record(&storage, record.job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Queued);
    assert!(
        stored
            .last_error
            .as_ref()
            .is_some_and(|error| error.message.contains("drifted"))
    );
}

#[tokio::test]
async fn stored_site_starts() {
    // The exact stored generation and digest still admit the start.
    use aruna_compute::ExecutorRegistry;
    use aruna_core::compute::ExecutorCapability;
    use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
    use aruna_core::structs::placement::policy::PlacementSubject;

    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = compute_context(storage.clone());
    let job_id = job_id();
    let subject = NodeSubjectRecord::seed(PlacementSubject {
        node_id: node_id(7),
        generation: 1,
        location: "eu-west".to_string(),
        labels: Default::default(),
        executor_kind: None,
        local_to_controller: true,
    })
    .unwrap()
    .subject;
    let registry = ExecutorRegistry::new().with_backend(StubBackend::new(StubReconcile::NotFound));
    let capability: ExecutorCapability = registry
        .capabilities(&subject, false)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store_site(
        &storage,
        job_id,
        capability.subject.generation,
        capability.subject_digest,
    )
    .await;

    assert!(
        resolve_backend(&ctx, &execution_spec(), job_id)
            .await
            .is_ok()
    );
}

/// A claimed external execution exactly as `run_execution_job` receives it,
/// with a context that resolves `backend`.
async fn claimed_execution(
    storage: &StorageHandle,
    backend: Arc<StubBackend>,
) -> (
    Arc<DriverContext>,
    aruna_net::NetHandle,
    JobRecord,
    ulid::Ulid,
) {
    let (context, net) = net_context(storage.clone()).await;
    let mut context = context;
    Arc::get_mut(&mut context).unwrap().compute_handle =
        Some(Arc::new(ExecutorRegistry::new().with_backend(backend)));

    let job_id = job_id();
    let record = JobRecord::new(
        job_id,
        JobPayload::Execution(execution_spec()),
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1,
        1,
        None,
    );
    insert_job(storage, &record).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(storage, job_id, node_id(7), 2).await.unwrap()
    else {
        panic!("claim failed");
    };
    let token = claimed.claim.as_ref().unwrap().claim_token;
    (context, net, claimed, token)
}

/// Rotate or release the claim so the running heartbeat's next renewal loses
/// the token: a recorded intent rotates it, a still-staging job is released.
async fn lose_claim(storage: &StorageHandle, job_id: JobId, token: ulid::Ulid) -> JobRecord {
    handoff_external_attempt(storage, job_id, token, unix_timestamp_millis())
        .await
        .unwrap();
    let taken_over = read_job_record(storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert!(
        taken_over
            .claim
            .as_ref()
            .is_none_or(|claim| claim.claim_token != token),
        "the stored claim must no longer match the run's token"
    );
    taken_over
}

// The reconciliation select may consume the heartbeat's completion when the
// claim is lost; polling the finished handle again panics and must not adopt.
#[tokio::test]
async fn heartbeat_consumed_once() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let backend = StubBackend::new(StubReconcile::Pending);
    let (ctx, net, record, token) = claimed_execution(&storage, backend.clone()).await;
    let job_id = record.job_id;

    let run = tokio::spawn(run_execution_job(ctx, record, CancellationToken::new()));
    backend.reconcile_started.notified().await;
    assert_eq!(backend.submits.lock().unwrap().len(), 1);
    let taken_over = lose_claim(&storage, job_id, token).await;

    // The heartbeat notices the rotated token at its next real interval; the
    // claim-loss path must consume that completion exactly once.
    tokio::time::timeout(Duration::from_secs(3 * JOB_HEARTBEAT_MS / 1_000), run)
        .await
        .expect("the heartbeat completion must be consumed exactly once")
        .expect("the workflow task must not panic");

    let after = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, taken_over, "a lost claim must not be written");
    assert_eq!(after.state, JobState::Ready);
    assert!(after.result.is_none());
    assert_eq!(
        backend.submits.lock().unwrap().len(),
        1,
        "recovery must not submit again"
    );
    net.shutdown().await;
}

// Recovery's adoption verdict starts supervision; the still-pending heartbeat
// is stopped and awaited once before the adopted attempt is supervised.
#[tokio::test]
async fn recovery_wins_adoption() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let backend = StubBackend::new(StubReconcile::Adopted);
    backend.logs_fail.store(true, Ordering::Relaxed);
    let (ctx, net, record, _token) = claimed_execution(&storage, backend.clone()).await;
    let job_id = record.job_id;

    Box::pin(run_execution_job(ctx, record, CancellationToken::new())).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(stored.state, JobState::Ready, "the adopted attempt ran");
    assert!(stored.started_at_ms.is_some());
    assert_eq!(
        backend.submits.lock().unwrap().len(),
        1,
        "adoption must not submit again"
    );
    net.shutdown().await;
}

// A lease renewal failure ends the run while preparation is still pending; the
// lost claim must not be written to or submitted to.
#[tokio::test]
async fn heartbeat_failure_stops() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let backend = StubBackend::new(StubReconcile::NotFound);
    backend.image_stall.store(true, Ordering::Relaxed);
    let (ctx, net, record, token) = claimed_execution(&storage, backend.clone()).await;
    let job_id = record.job_id;

    let run = tokio::spawn(run_execution_job(ctx, record, CancellationToken::new()));
    backend.image_started.notified().await;
    let taken_over = lose_claim(&storage, job_id, token).await;

    tokio::time::timeout(Duration::from_secs(3 * JOB_HEARTBEAT_MS / 1_000), run)
        .await
        .expect("a lost claim ends the run")
        .expect("the workflow task must not panic");

    let after = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, taken_over, "a lost claim must not be written");
    assert_eq!(after.state, JobState::Queued);
    assert!(backend.submits.lock().unwrap().is_empty());
    net.shutdown().await;
}

// A cancellation seen by reconciliation stops the heartbeat once and
// terminalizes without adopting the ambiguous attempt.
#[tokio::test]
async fn cancellation_skips_adoption() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let backend = StubBackend::new(StubReconcile::Pending);
    let (ctx, net, record, _token) = claimed_execution(&storage, backend.clone()).await;
    let job_id = record.job_id;
    let cancel = CancellationToken::new();
    cancel.cancel();

    Box::pin(run_execution_job(ctx, record, cancel)).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    assert_eq!(backend.submits.lock().unwrap().len(), 1);
    net.shutdown().await;
}
