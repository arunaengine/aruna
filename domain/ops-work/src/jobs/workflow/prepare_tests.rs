//! Tests how an attempt is prepared, by the calls and results of its operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::NOBODY;
use aruna_core::compute::runtimes::{
    MOUNT_PATH_TAG, MOUNT_PREFIX_TAG, SESSION_TAG, SESSION_TAG_NOTEBOOK,
};
use aruna_core::structs::execution::job::input::ExecutionSpec;
use aruna_core::structs::execution::job::local::JobErrorKind;
use ulid::Ulid;

use super::super::fixtures::{
    BUCKET, PINNED, child, compute, facts, is_child, job, node, realm, record, spec,
};
use super::*;

const NOW: u64 = 17;

fn run() -> RunIdentity {
    RunIdentity::new(&spec(), &record(), realm(), node())
}

fn session_spec() -> ExecutionSpec {
    let mut spec = spec();
    spec.tags
        .insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
    spec
}

fn prepare_of(
    spec: ExecutionSpec,
    job: JobRecord,
    bucket: &str,
    check_workspace: bool,
) -> PrepareAttemptOperation {
    let held = (Arc::new(spec), Arc::new(job));
    let mut prepare = PrepareAttemptOperation::new(run(), held, bucket, None, check_workspace);
    prepare.stamp(NOW);
    prepare
}

fn in_bucket() -> JobRecord {
    let mut job = record();
    job.workspace_mode = WorkspaceMode::Existing;
    job
}

fn select_backend() -> Next<PreparedTask> {
    let select = ComputeEffect::Select {
        backend: None,
        constraint: None,
    };
    Next::Call(compute_call(select))
}

fn read_endpoint() -> Next<PreparedTask> {
    let backend = "docker".to_string();
    Next::Call(compute_call(ComputeEffect::Endpoint { backend }))
}

/// A preparation past its backend selection, about to read the endpoints.
fn begun(prepare: &mut PrepareAttemptOperation) {
    begun_on(prepare, facts());
}

fn begun_on(prepare: &mut PrepareAttemptOperation, facts: BackendFacts) {
    assert_eq!(prepare.first(), Ok(select_backend()));
    let selected = compute(ComputeAnswer::Selected(Some(facts)));
    assert_eq!(prepare.next(selected), Ok(read_endpoint()));
}

fn address(endpoint: Option<&str>, session_endpoint: Option<&str>) -> IoResult {
    compute(ComputeAnswer::Endpoint(WorkspaceAddress {
        endpoint: endpoint.map(str::to_string),
        session_endpoint: session_endpoint.map(str::to_string),
        region: "eu".to_string(),
    }))
}

fn loaded() -> IoResult {
    child(Ok::<Vec<TaskInput>, JobError>(Vec::new()))
}

fn credential() -> WorkspaceCredential {
    WorkspaceCredential {
        access_key: "access".to_string(),
        secret: "secret".to_string(),
        expires_at_ms: 1,
    }
}

/// A preparation that has read the node's endpoints and loaded no inputs.
fn loaded_with(mut prepare: PrepareAttemptOperation, endpoint: IoResult) -> PrepareNext {
    begun(&mut prepare);
    assert!(is_child(&prepare.next(endpoint)));
    prepare.next(loaded())
}

// A first preparation of a run inside a bucket authorizes that bucket.
#[test]
fn existing_bucket_authorized() {
    let mut prepare = prepare_of(spec(), in_bucket(), BUCKET, true);
    let access = WorkspaceAccessOperation::existing(run(), BUCKET);
    assert_eq!(prepare.first(), Ok(Next::Call(IoCall::child(access))));
    let denied = JobError::permanent("denied");
    assert_eq!(
        prepare.next(child(Err::<(), _>(denied.clone()))),
        Err(denied)
    );
}

// An authorized bucket goes on, and a recovery does not authorize it again.
#[test]
fn recovery_skips_authorization() {
    let mut first = prepare_of(spec(), in_bucket(), BUCKET, true);
    first.first().unwrap();
    assert_eq!(
        first.next(child(Ok::<(), JobError>(()))),
        Ok(select_backend())
    );
    let mut recovery = prepare_of(spec(), in_bucket(), BUCKET, false);
    assert_eq!(recovery.first(), Ok(select_backend()));
}

// A run bound to one executor reads what that backend can do.
#[test]
fn constrained_backend_read() {
    let mut constrained = spec();
    constrained.executor_constraint = Some("docker".to_string());
    let mut prepare = prepare_of(constrained, record(), "", false);
    let read = compute_call(ComputeEffect::Select {
        backend: None,
        constraint: Some(ExecutorKind::Docker),
    });
    assert_eq!(prepare.first(), Ok(Next::Call(read)));
    let answer = compute(ComputeAnswer::Selected(Some(facts())));
    assert_eq!(prepare.next(answer), Ok(read_endpoint()));
    assert_eq!(prepare.facts, Some(facts()));
}

// A plain run copies its inputs and needs no credential.
#[test]
fn plain_run_prepared() {
    let mut prepare = prepare_of(spec(), record(), "", false);
    begun(&mut prepare);
    let job = Arc::new(record());
    let load = LoadInputsOperation::new(run(), &spec(), job, &BTreeSet::new());
    assert_eq!(
        prepare.next(address(None, None)),
        Ok(Next::Call(IoCall::child(load)))
    );
    let expected = PreparedTask {
        inputs: Vec::new(),
        mounts: Vec::new(),
        secrets: BTreeMap::new(),
        vault_env: BTreeMap::new(),
        staging: StagingMode::Files,
        workspace: None,
        nfs_mounts: NfsBindings::default(),
    };
    assert_eq!(prepare.next(loaded()), Ok(Next::Done(expected)));
}

// A session cannot start without a bucket or an endpoint its container reaches.
#[test]
fn session_needs_workspace() {
    let no_bucket = prepare_of(session_spec(), record(), "", false);
    assert_eq!(
        loaded_with(no_bucket, address(Some("http://s3"), None)),
        Err(JobError::permanent("a session needs a workspace bucket"))
    );
    let no_endpoint = prepare_of(session_spec(), record(), BUCKET, false);
    assert_eq!(
        loaded_with(no_endpoint, address(None, None)),
        Err(JobError::permanent(
            "a session needs a container-reachable S3 endpoint"
        ))
    );
}

// A session gets one credential for its bucket and the endpoint made for sessions.
#[test]
fn session_credential_bound() {
    let mut prepare = prepare_of(session_spec(), record(), BUCKET, false);
    begun(&mut prepare);
    prepare
        .next(address(Some("http://s3"), Some("http://session")))
        .unwrap();
    assert!(is_child(&prepare.next(loaded())));
    let minted = child(Ok::<_, JobError>(credential()));
    let Ok(Next::Done(task)) = prepare.next(minted) else {
        panic!("the credential ends the preparation");
    };
    assert_eq!(
        task.workspace,
        Some(WorkspaceBinding {
            s3_endpoint: "http://session".to_string(),
            bucket_name: BUCKET.to_string(),
            region: "eu".to_string(),
        })
    );
    let names: Vec<_> = task.secrets.keys().map(String::as_str).collect();
    assert_eq!(names, ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]);
}

// A batch run in its bucket gets S3 only when its backend offers sessions and has an endpoint.
#[test]
fn batch_workspace_conditional() {
    let with_session = |session| {
        let mut prepare = prepare_of(spec(), in_bucket(), BUCKET, false);
        prepare.facts = Some(BackendFacts { session, ..facts() });
        begun(&mut prepare);
        prepare.next(address(Some("http://s3"), None)).unwrap();
        prepare.next(loaded())
    };
    assert!(is_child(&with_session(true)));
    assert!(matches!(with_session(false), Ok(Next::Done(_))));
}

// Input loading and credential failures end the preparation as they are.
#[test]
fn failures_end_preparation() {
    let error = JobError::retryable("slow");
    let mut unloaded = prepare_of(spec(), record(), "", false);
    begun(&mut unloaded);
    unloaded.next(address(None, None)).unwrap();
    let failed = child(Err::<Vec<TaskInput>, _>(error.clone()));
    assert_eq!(unloaded.next(failed), Err(error.clone()));
    let mut unminted = prepare_of(session_spec(), record(), BUCKET, false);
    begun(&mut unminted);
    unminted.next(address(Some("http://s3"), None)).unwrap();
    unminted.next(loaded()).unwrap();
    let failed = child(Err::<WorkspaceCredential, _>(error.clone()));
    assert_eq!(unminted.next(failed), Err(error));
}

fn task_spec() -> ExecutionSpec {
    use aruna_core::structs::execution::job::input::{
        OutputDestination, OutputSelection, PLUGIN_INSTALL_TAG, PLUGIN_WORK_TAG,
    };
    let mut spec = spec();
    let id = ulid::Ulid::from_parts(1, 1).to_string();
    spec.tags.insert(PLUGIN_INSTALL_TAG.to_string(), id.clone());
    spec.tags.insert(PLUGIN_WORK_TAG.to_string(), id);
    spec.file_outputs.push(OutputSelection {
        container_path: "/out/**".to_string(),
        path_prefix: Some("/out".to_string()),
        destination_node_id: None,
        destination: OutputDestination::S3 {
            bucket: "results".to_string(),
            key: "maps".to_string(),
        },
        name: None,
        description: None,
    });
    spec
}

// A plugin task checks its install again at every attempt start; it needs its reservation and
// the family the receiver adopted, and a launch before adoption retries.
#[test]
fn task_checked_again() {
    let mut prepare = prepare_of(task_spec(), record(), "", false);
    assert!(is_child(&prepare.first()));
    let config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    assert!(is_child(
        &prepare.next(child(Ok::<_, GetConfigError>(config.clone())))
    ));
    let none = child(Ok::<Vec<JobReservationRecord>, LifecycleError>(Vec::new()));
    let refused = prepare.next(none).unwrap_err();
    assert_eq!(
        refused.kind,
        aruna_core::structs::execution::job::local::JobErrorKind::Permanent
    );

    let mut pending = prepare_of(task_spec(), record(), "", false);
    pending.step = PrepareStep::TaskAdmit;
    let answer = child(Err::<PluginInstall, _>(ScreenError::Pending));
    let retried = pending.next(answer).unwrap_err();
    assert_eq!(
        retried.kind,
        aruna_core::structs::execution::job::local::JobErrorKind::Retryable
    );
    pending.step = PrepareStep::TaskAdmit;
    let admitted = child(Ok::<_, ScreenError>(
        crate::jobs::lifecycle::screen::tests::install(),
    ));
    assert!(matches!(pending.next(admitted), Ok(Next::Call(_))));
}

const HOST: Ulid = Ulid::from_parts(9, 9);

/// A session whose `team/` folder is mounted, on a backend whose NFS host serves both shares.
fn nfs_session() -> PrepareAttemptOperation {
    let mut spec = session_spec();
    spec.tags
        .insert(MOUNT_PATH_TAG.to_string(), "/work".to_string());
    spec.tags
        .insert(MOUNT_PREFIX_TAG.to_string(), "team/".to_string());
    let mut prepare = prepare_of(spec, record(), BUCKET, false);
    let nfs_share = Some(NfsShareConfig {
        host_id: HOST,
        inputs: true,
        sessions: true,
    });
    begun_on(
        &mut prepare,
        BackendFacts {
            nfs_share,
            ..facts()
        },
    );
    prepare.next(address(Some("http://s3"), None)).unwrap();
    assert!(is_child(&prepare.next(loaded())));
    prepare
}

/// The worker-pool binding the test session asks for on `target`.
fn share_create(target: NfsMountTarget) -> Next<PreparedTask> {
    let job_id = match &target {
        NfsMountTarget::JobInputView { job_id } => Some(*job_id),
        _ => None,
    };
    let read_only = job_id.is_some();
    let context = VfsAccessContext {
        realm_id: realm(),
        node_id: node(),
        access_key: "access".to_string(),
        protocol: VfsProtocol::Nfs,
        target,
        read_only,
        now_ms: NOW,
    };
    let (bucket, prefix) = match read_only {
        true => (String::new(), None),
        false => (BUCKET.to_string(), Some("team/".to_string())),
    };
    let input = MountInput {
        context,
        caller: record().created_by,
        host_id: HOST,
        group_id: spec().group_id,
        bucket,
        prefix,
        job_id,
        read_only,
        allowed_networks: Vec::new(),
        display_uid: NOBODY.uid,
        display_gid: NOBODY.gid,
        mode: if read_only { 0o555 } else { 0o700 },
    };
    let mount_id = binding_id(HOST, job(), record().attempts, !read_only);
    let create = CreateMountOperation::new(input).for_worker_pool();
    Next::Call(IoCall::child(create.with_mount_id(mount_id)))
}

fn bound(mount_id: Ulid) -> IoResult {
    child(Ok::<_, VfsError>(NfsMountBinding {
        mount_id,
        host_id: HOST,
        owner: record().created_by,
        access_key: "access".to_string(),
        group_id: spec().group_id,
        target: NfsMountTarget::JobInputView { job_id: job() },
        read_only: true,
        allowed_networks: Vec::new(),
        display_uid: NOBODY.uid,
        display_gid: NOBODY.gid,
        mode: 0o555,
        created_at_ms: NOW,
    }))
}

/// Answers the session's bucket scope and expects its folder binding.
fn scope_read(prepare: &mut PrepareAttemptOperation) {
    let scope = child(Ok::<_, VfsError>(BucketScope::new(BUCKET, 7)));
    let folder = NfsMountTarget::for_bucket(BUCKET, 7).unwrap();
    assert_eq!(prepare.next(scope), Ok(share_create(folder)));
}

// A session with mounted inputs binds its input view and its folder on the backend's NFS host,
// each under its own id, and the task spec carries both.
#[test]
fn provisions_nfs_bindings() {
    let mut prepare = nfs_session();
    let mount = S3Mount {
        bucket: view_bucket(job()),
        key: "in".to_string(),
        path: "/in".to_string(),
        cached: false,
    };
    prepare.prepared.as_mut().unwrap().mounts.push(mount);
    prepare.step = PrepareStep::SessionCredential { view: true };
    let minted = child(Ok::<_, JobError>(credential()));
    let view = NfsMountTarget::JobInputView { job_id: job() };
    assert_eq!(prepare.next(minted), Ok(share_create(view)));
    let scope = BucketScopeOperation::new(BUCKET);
    let attempt = record().attempts;
    let input = binding_id(HOST, job(), attempt, false);
    let session = binding_id(HOST, job(), attempt, true);
    assert_ne!(input, session);
    assert_eq!(
        prepare.next(bound(input)),
        Ok(Next::Call(IoCall::child(scope)))
    );
    scope_read(&mut prepare);
    let Ok(Next::Done(task)) = prepare.next(bound(session)) else {
        panic!("the session binding ends the preparation");
    };
    let bindings = NfsBindings {
        input: Some(input.to_string()),
        session: Some(session.to_string()),
    };
    assert_eq!(task.nfs_mounts, bindings);
    let attempt = AttemptRef::new("job", 1);
    let spec = build_task_spec(&session_spec(), &attempt, PINNED, task, NOBODY);
    assert_eq!(spec.nfs_mounts, bindings);
}

// A refused binding fails the preparation for good before any submit; an unreachable host
// retries it.
#[test]
fn refuses_nfs_host() {
    let cases = [
        (VfsError::Denied, JobErrorKind::Permanent),
        (VfsError::NotFound, JobErrorKind::Permanent),
        (
            VfsError::Unavailable("down".to_string()),
            JobErrorKind::Retryable,
        ),
    ];
    for (refusal, kind) in cases {
        let mut prepare = nfs_session();
        assert!(is_child(
            &prepare.next(child(Ok::<_, JobError>(credential())))
        ));
        scope_read(&mut prepare);
        let refused = child(Err::<NfsMountBinding, _>(refusal));
        assert_eq!(prepare.next(refused).unwrap_err().kind, kind);
    }
}

// Every preparation of one attempt asks for the same binding ids, so a retried volume names
// the bindings the first one made; another attempt, job or share gets its own.
#[test]
fn reuses_attempt_bindings() {
    let view = NfsMountTarget::JobInputView { job_id: job() };
    let folder = NfsMountTarget::for_bucket(BUCKET, 7).unwrap();
    let ask = |attempt: u32, job_id: JobId, target: &NfsMountTarget| {
        let mut prepare = nfs_session().with_attempt(attempt);
        prepare.run.job_id = job_id;
        prepare.share_mount(target.clone()).unwrap()
    };
    let other = JobId::from_bytes([9; 16]);
    for target in [&view, &folder] {
        assert_eq!(ask(1, job(), target), ask(1, job(), target));
        assert_ne!(ask(1, job(), target), ask(2, job(), target));
        assert_ne!(ask(1, job(), target), ask(1, other, target));
    }
    let ids = [
        binding_id(HOST, job(), 1, false),
        binding_id(HOST, job(), 1, true),
        binding_id(HOST, job(), 2, false),
        binding_id(HOST, other, 1, false),
    ];
    let distinct: BTreeSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len());
    assert!(ids.iter().all(|id| !id.is_nil()));
}
