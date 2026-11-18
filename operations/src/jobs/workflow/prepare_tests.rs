//! Tests how an attempt is prepared, by the calls and results of its operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::runtimes::{SESSION_TAG, SESSION_TAG_NOTEBOOK};
use aruna_core::structs::execution::job::ExecutionSpec;

use super::super::fixtures::{BUCKET, child, compute, facts, is_child, node, realm, record, spec};
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

fn read_endpoint() -> Next<PreparedTask> {
    Next::Call(compute_call(ComputeEffect::Endpoint))
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
    assert_eq!(prepare.first(), Ok(read_endpoint()));
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
        Ok(read_endpoint())
    );
    let mut recovery = prepare_of(spec(), in_bucket(), BUCKET, false);
    assert_eq!(recovery.first(), Ok(read_endpoint()));
}

// A run bound to one executor reads what that backend can do.
#[test]
fn constrained_backend_read() {
    let mut constrained = spec();
    constrained.executor_constraint = Some("docker".to_string());
    let mut prepare = prepare_of(constrained, record(), "", false);
    let kind = ExecutorKind::Docker;
    let read = compute_call(ComputeEffect::Facts { kind });
    assert_eq!(prepare.first(), Ok(Next::Call(read)));
    let answer = compute(ComputeAnswer::Facts(facts()));
    assert_eq!(prepare.next(answer), Ok(read_endpoint()));
    assert_eq!(prepare.facts, Some(facts()));
}

// A plain run copies its inputs and needs no credential.
#[test]
fn plain_run_prepared() {
    let mut prepare = prepare_of(spec(), record(), "", false);
    prepare.first().unwrap();
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
        staging: StagingMode::Files,
        workspace: None,
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
    prepare.first().unwrap();
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
        prepare.first().unwrap();
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
    unloaded.first().unwrap();
    unloaded.next(address(None, None)).unwrap();
    let failed = child(Err::<Vec<TaskInput>, _>(error.clone()));
    assert_eq!(unloaded.next(failed), Err(error.clone()));
    let mut unminted = prepare_of(session_spec(), record(), BUCKET, false);
    unminted.first().unwrap();
    unminted.next(address(Some("http://s3"), None)).unwrap();
    unminted.next(loaded()).unwrap();
    let failed = child(Err::<WorkspaceCredential, _>(error.clone()));
    assert_eq!(unminted.next(failed), Err(error));
}
