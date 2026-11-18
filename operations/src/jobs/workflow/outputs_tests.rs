//! Tests how file outputs are captured, by the calls and results of their operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::compute::BackendError;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{
    InputMode, InputSelection, InputSource, OutputCommitIntent,
};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::format::StoredFormat;

use super::super::attempt::stops_call;
use super::super::fixtures::{
    EPOCH, child, compute, control, fence, is_child, job, node, realm, record, spec, stops,
};
use super::*;

const NOW: u64 = 11;
const PATH: &str = "/out/a.txt";

fn version() -> Ulid {
    Ulid::from_bytes([7; 16])
}

fn execution() -> Ulid {
    control().execution_id
}

fn other_node() -> NodeId {
    iroh::SecretKey::from_bytes(&[9; 32]).public()
}

fn output(container_path: &str, node_id: NodeId) -> OutputSelection {
    OutputSelection {
        container_path: container_path.to_string(),
        path_prefix: None,
        destination_node_id: Some(node_id),
        destination: OutputDestination::S3 {
            bucket: "out".to_string(),
            key: "a.txt".to_string(),
        },
        name: None,
        description: None,
    }
}

fn scope_of(spec: ExecutionSpec, until_stop: bool) -> CaptureScope {
    let record = record();
    CaptureScope {
        kind: ExecutorKind::Docker,
        fence: fence(),
        run: RunIdentity::new(&spec, &record, realm(), node()),
        spec: Arc::new(spec),
        record: Arc::new(record),
        routing: NodeRouting::default(),
        until_stop,
    }
}

fn spec_with(selection: OutputSelection) -> ExecutionSpec {
    let mut spec = spec();
    spec.file_outputs.push(selection);
    spec
}

fn capture_of(selection: OutputSelection, until_stop: bool) -> CaptureOutputsOperation {
    CaptureOutputsOperation::new(scope_of(spec_with(selection), until_stop))
}

fn reserve_call(destinations: Vec<(NodeId, String, String)>) -> Next<Vec<OutputObject>> {
    let reserve = reserve_output_commits(job(), destinations, BTreeMap::new());
    Next::Call(IoCall::child(reserve))
}

fn destination() -> (NodeId, String, String) {
    (node(), "out".to_string(), "a.txt".to_string())
}

fn committed() -> AttemptControl {
    let mut reserved = control();
    reserved.output_commits.push(OutputCommitIntent {
        node_id: node(),
        bucket: "out".to_string(),
        key: "a.txt".to_string(),
        version_id: version(),
    });
    reserved
}

fn reserved(control: AttemptControl) -> IoResult {
    child(Ok::<_, JobMutationError>(control))
}

fn no_bucket() -> IoResult {
    child(Err::<BucketInfo, _>(GetBucketError::NotFound))
}

fn location() -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::from_bytes([3; 16]),
        format: StoredFormat::default(),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        staging: false,
        partial: false,
        blob_size: 3,
        hashes: HashMap::new(),
    }
}

fn put_result(replayed: bool) -> PutObjectResult {
    PutObjectResult {
        location: location(),
        version_id: version(),
        replayed,
    }
}

fn object(node_id: NodeId) -> OutputObject {
    let selection = output(PATH, node_id);
    output_object(
        &selection,
        node_id,
        "out",
        "a.txt",
        &put_result(false),
        execution(),
    )
}

fn put_call(until_stop: bool) -> IoCall {
    let selection = output(PATH, node());
    let scope = Arc::new(scope_of(spec_with(selection.clone()), until_stop));
    let ids = (version(), execution());
    let put = PutOutputOperation::new(scope, selection, node(), ids, Vec::new());
    IoCall::child(put)
}

/// A capture of one literal output whose commit reservation came back as `control`.
fn reserved_capture(control: AttemptControl, until_stop: bool) -> CaptureOutputsOperation {
    let mut capture = capture_of(output(PATH, node()), until_stop);
    capture.first().unwrap();
    assert_eq!(
        capture.next(no_bucket()),
        Ok(reserve_call(vec![destination()]))
    );
    capture
        .next(reserved(control))
        .expect("the reservation is accepted");
    capture
}

// A run without outputs still reserves, so a replay finds the same empty set.
#[test]
fn empty_capture_reserves() {
    let mut capture = CaptureOutputsOperation::new(scope_of(spec(), true));
    assert_eq!(capture.first(), Ok(reserve_call(Vec::new())));
    assert_eq!(
        capture.next(reserved(control())),
        Ok(Next::Done(Vec::new()))
    );
}

// A reservation that failed or belongs to a later attempt stops the capture for a retry.
#[test]
fn bad_reservation_retries() {
    let mut moved = control();
    moved.attempt_epoch = EPOCH + 1;
    let error = JobMutationError::NotFound;
    let cases = [
        (
            reserved(moved),
            "attempt fence moved during output capture".to_string(),
        ),
        (
            child(Err::<AttemptControl, _>(JobMutationError::NotFound)),
            format!("output commit reservation failed: {error}"),
        ),
    ];
    for (reply, message) in cases {
        let mut capture = CaptureOutputsOperation::new(scope_of(spec(), true));
        capture.first().unwrap();
        assert_eq!(capture.next(reply), Err(JobError::retryable(message)));
    }
}

// A wildcard is listed against the attempt, and every failed listing ends the capture.
#[test]
fn wildcard_listed_first() {
    let lost = BackendError::Unavailable("lost".to_string());
    let cases = [
        (ComputeAnswer::Stopped, capture_cancelled()),
        (ComputeAnswer::NoBackend, no_backend()),
        (
            ComputeAnswer::Outputs(Err(lost.clone())),
            output_read_error(&lost),
        ),
    ];
    for (answer, expected) in cases {
        let mut capture = capture_of(output("/out/*.txt", node()), true);
        let request = ComputeRequest::ListOutputs {
            pattern: "/out/*.txt".to_string(),
            until_stop: true,
        };
        let list = attempt_call(&ExecutorKind::Docker, &fence(), request);
        assert_eq!(capture.first(), Ok(Next::Call(list)));
        assert_eq!(capture.next(compute(answer)), Err(expected));
    }
}

// A local destination's bucket is read for its hint; a remote one has none here.
#[test]
fn only_local_hinted() {
    let mut local = capture_of(output(PATH, node()), true);
    let read = GetBucketOperation::new("out".to_string());
    assert_eq!(local.first(), Ok(Next::Call(IoCall::child(read))));
    let mut remote = capture_of(output(PATH, other_node()), true);
    let target = (other_node(), "out".to_string(), "a.txt".to_string());
    assert_eq!(remote.first(), Ok(reserve_call(vec![target])));
}

// A job stop is checked before every put, and the finished put is collected.
#[test]
fn stop_checked_first() {
    let mut stopped = reserved_capture(committed(), true);
    assert_eq!(stopped.step, CaptureStep::Stops);
    assert_eq!(stopped.next(stops(true)), Err(capture_cancelled()));
    let mut running = reserved_capture(committed(), true);
    assert_eq!(running.next(stops(false)), Ok(Next::Call(put_call(true))));
    let put = child(Ok::<_, JobError>(object(node())));
    assert_eq!(running.next(put), Ok(Next::Done(vec![object(node())])));
}

// The stop check itself asks the job's host.
#[test]
fn stop_asks_host() {
    let mut capture = capture_of(output(PATH, node()), true);
    capture.first().unwrap();
    capture.next(no_bucket()).unwrap();
    assert_eq!(
        capture.next(reserved(committed())),
        Ok(Next::Call(stops_call()))
    );
}

// The cancel path captures what finished, so it never checks for a stop.
#[test]
fn cancel_capture_continues() {
    let mut capture = capture_of(output(PATH, node()), false);
    capture.first().unwrap();
    capture.next(no_bucket()).unwrap();
    assert_eq!(
        capture.next(reserved(committed())),
        Ok(Next::Call(put_call(false)))
    );
}

// An output without its reserved version is never written under another one.
#[test]
fn lost_commit_retries() {
    let mut capture = reserved_capture(control(), true);
    assert_eq!(
        capture.next(stops(false)),
        Err(JobError::retryable("output commit reservation lost"))
    );
}

fn spec_with_input() -> ExecutionSpec {
    let mut spec = spec();
    spec.inputs.push(InputSelection {
        source: InputSource::S3 {
            bucket: "in".to_string(),
            key: "a.txt".to_string(),
            version_id: None,
        },
        source_node_id: None,
        dest_key: "a.txt".to_string(),
        mode: InputMode::Snapshot,
        container_path: Some("/in/a.txt".to_string()),
        name: None,
        description: None,
    });
    spec
}

// Only a device may lack captured inputs: a realm node must not guess their refs.
#[test]
fn uncaptured_needs_device() {
    let start = || {
        let mut capture = CaptureOutputsOperation::new(scope_of(spec_with_input(), true));
        capture.first().unwrap();
        let role = LocalUserOperation::new(realm());
        assert_eq!(
            capture.next(reserved(control())),
            Ok(Next::Call(IoCall::child(role)))
        );
        capture
    };
    let device = child(Ok::<_, IoError>(true));
    assert_eq!(start().next(device), Ok(Next::Done(Vec::new())));
    let refused = JobError::permanent("input `a.txt` has no captured inputs to inherit refs from");
    for reply in [
        child(Ok::<_, IoError>(false)),
        child(Err::<bool, _>(IoError::Unfinished)),
    ] {
        assert_eq!(start().next(reply), Err(refused.clone()));
    }
}

fn put_to(destination_node: NodeId) -> PutOutputOperation {
    let selection = output(PATH, destination_node);
    let scope = Arc::new(scope_of(spec_with(selection.clone()), true));
    let ids = (version(), execution());
    let mut put = PutOutputOperation::new(scope, selection, destination_node, ids, Vec::new());
    put.stamp(NOW);
    put
}

fn bucket_info(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH + Duration::from_millis(NOW),
        created_by: record().created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn read_config() -> Next<OutputObject> {
    Next::Call(IoCall::child(GetConfigOperation::new(realm())))
}

/// A local put that has passed its first call and waits at `step`.
fn put_at(step: PutStep) -> PutOutputOperation {
    let mut put = put_to(node());
    put.first().unwrap();
    put.step = step;
    put
}

// A destination bucket of another group is refused before any byte moves.
#[test]
fn foreign_bucket_refused() {
    let mut put = put_to(node());
    let read = GetBucketOperation::new("out".to_string());
    assert_eq!(put.first(), Ok(Next::Call(IoCall::child(read))));
    let foreign = bucket_info(Ulid::from_bytes([9; 16]));
    assert_eq!(
        put.next(child(Ok::<_, GetBucketError>(foreign))),
        Err(JobError::permanent(
            "output bucket is outside the execution group"
        ))
    );
}

// The creator needs write permission on the exact destination object.
#[test]
fn write_permission_checked() {
    let mut put = put_to(node());
    put.first().unwrap();
    let owned = bucket_info(spec().group_id);
    assert!(is_child(&put.next(child(Ok::<_, GetBucketError>(owned)))));
    let denied = child(Ok::<_, AuthorizationError>(false));
    assert_eq!(
        put.next(denied),
        Err(JobError::permanent("output out/a.txt access denied"))
    );
    let mut allowed = put_at(PutStep::Allowed);
    let granted = child(Ok::<_, AuthorizationError>(true));
    assert_eq!(allowed.next(granted), Ok(read_config()));
}

// An output for another node goes to a staging bucket, which may already exist.
#[test]
fn remote_output_staged() {
    let mut put = put_to(other_node());
    let info = bucket_info(spec().group_id);
    let create = CreateBucketOperation::new(output_stage_bucket(&record()), info);
    assert_eq!(put.first(), Ok(Next::Call(IoCall::child(create))));
    assert_eq!(put.write_key, version().to_string());
    let exists = child(Err::<BucketInfo, _>(CreateBucketError::BucketAlreadyExists));
    assert_eq!(put.next(exists), Ok(read_config()));
}

// The output is opened under the stop rule of the capture.
#[test]
fn output_fetch_requested() {
    let request = ComputeRequest::FetchOutput {
        path: PATH.to_string(),
        until_stop: true,
    };
    let fetch = attempt_call(&ExecutorKind::Docker, &fence(), request);
    assert_eq!(put_at(PutStep::Fetch).fetch(), Ok(Next::Call(fetch)));
}

// Every failed fetch ends the put with the error of its cause.
#[test]
fn failed_fetch_classified() {
    let lost = BackendError::Unavailable("lost".to_string());
    let cases = [
        (ComputeAnswer::Stopped, capture_cancelled()),
        (ComputeAnswer::NoBackend, no_backend()),
        (
            ComputeAnswer::Output(Err(lost.clone())),
            output_read_error(&lost),
        ),
    ];
    for (answer, expected) in cases {
        let mut put = put_at(PutStep::Fetch);
        assert_eq!(put.next(compute(answer)), Err(expected));
    }
}

// An opened output keeps its size and stream for the write.
#[test]
fn opened_output_kept() {
    let mut put = put_at(PutStep::Fetch);
    let body = OutputBody {
        size: 3,
        bytes: BackendStream(Box::pin(futures_util::stream::empty())),
        failure: OutputFailure::default(),
    };
    let opened = compute(ComputeAnswer::Output(Ok(body)));
    assert!(is_child(&put.next(opened)));
    assert_eq!(put.size, 3);
    assert!(put.body.is_some());
}

// A failure of the container's stream decides over the put error it caused.
#[test]
fn stream_failure_decides() {
    let lost = BackendError::Unavailable("lost".to_string());
    let mut broken = put_at(PutStep::Put);
    broken.failure.record(lost.clone());
    let failed = child(Err::<PutObjectResult, _>(PutObjectError::Unmapped));
    assert_eq!(broken.next(failed), Err(output_read_error(&lost)));
    let mut plain = put_at(PutStep::Put);
    let failed = child(Err::<PutObjectResult, _>(PutObjectError::Unmapped));
    assert_eq!(
        plain.next(failed),
        Err(put_object_error("output write", PutObjectError::Unmapped))
    );
}

// A new local version is announced; announcing is best effort.
#[test]
fn new_put_announced() {
    let mut put = put_at(PutStep::Put);
    put.size = 3;
    let scope = put.scope.clone();
    let announce = CompletePutOperation::new(CompletePutInput {
        realm_id: realm(),
        node_id: node(),
        auth: scope.auth(),
        group_id: spec().group_id,
        bucket: "out".to_string(),
        key: "a.txt".to_string(),
        version_id: version(),
        size_bytes: 3,
    });
    let written = child(Ok::<_, PutObjectError>(put_result(false)));
    assert_eq!(put.next(written), Ok(Next::Call(IoCall::child(announce))));
    let failed = child(Err::<(), _>(IoError::Unfinished));
    assert_eq!(put.next(failed), Ok(Next::Done(object(node()))));
}

// A replayed put was announced by the capture that committed it.
#[test]
fn replayed_put_ends() {
    let mut put = put_at(PutStep::Put);
    let replayed = child(Ok::<_, PutObjectError>(put_result(true)));
    assert_eq!(put.next(replayed), Ok(Next::Done(object(node()))));
}

// A staged copy is authorized for its way to the destination node.
#[test]
fn staged_put_authorized() {
    let mut put = put_to(other_node());
    put.first().unwrap();
    put.step = PutStep::Put;
    let scope = put.scope.clone();
    let source = SourceAuthOperation::new(scope.auth(), spec().group_id, record().owner_node_id);
    let written = child(Ok::<_, PutObjectError>(put_result(false)));
    assert_eq!(put.next(written), Ok(Next::Call(IoCall::child(source))));
}

// The staged copy and its bucket go away; both may be gone already.
#[test]
fn staging_removed_after() {
    let mut put = put_to(other_node());
    put.first().unwrap();
    put.result = Some(put_result(false));
    put.step = PutStep::DeleteCopy;
    let gone = child(Err::<DeleteObjectResult, _>(
        DeleteObjectError::NoSuchVersion,
    ));
    let bucket = DeleteBucketOperation::new(put.write_bucket.clone());
    assert_eq!(put.next(gone), Ok(Next::Call(IoCall::child(bucket))));
    let gone = child(Err::<(), _>(DeleteBucketError::NotFound));
    assert_eq!(put.next(gone), Ok(Next::Done(object(other_node()))));
}
