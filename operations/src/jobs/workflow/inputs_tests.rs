//! Tests how the inputs of an attempt are found, by the calls and results of their operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{InputContent, InputMode, JobClaim};

use super::super::fixtures::{child, is_child, job, node, realm, record, spec, token};
use super::*;

fn version() -> Ulid {
    Ulid::from_bytes([7; 16])
}

fn holder() -> NodeId {
    iroh::SecretKey::from_bytes(&[9; 32]).public()
}

fn run() -> RunIdentity {
    RunIdentity::new(&spec(), &record(), realm(), node())
}

fn input(source_node_id: Option<NodeId>, version_id: Option<Ulid>) -> InputSelection {
    InputSelection {
        source: InputSource::S3 {
            bucket: "in".to_string(),
            key: "a.txt".to_string(),
            version_id: version_id.map(|version| version.to_string()),
        },
        source_node_id,
        dest_key: "a.txt".to_string(),
        mode: InputMode::Snapshot,
        container_path: Some("/in/a.txt".to_string()),
        name: None,
        description: None,
    }
}

fn captured(content: InputContent) -> CapturedInput {
    CapturedInput {
        destination_key: "a.txt".to_string(),
        source_node_id: holder(),
        version_id: version(),
        content,
        bytes: 3,
        policies: Vec::new(),
        source_group_id: Ulid::from_bytes([8; 16]),
        public: false,
        link: None,
        source_pin: None,
        served_by: None,
        database: None,
        releases: Vec::new(),
        releases_complete: false,
        grant: None,
    }
}

fn hashed() -> CapturedInput {
    captured(InputContent::Blake3([8; 32]))
}

fn reference() -> CapturedInput {
    captured(InputContent::Reference {
        etag: None,
        range_bound: false,
    })
}

fn with_input(input: CapturedInput) -> JobRecord {
    let mut job = record();
    job.captured_inputs.push(input);
    job
}

fn find(job: JobRecord, input: InputSelection) -> InputSourceOperation {
    let source = source_object(&input).unwrap();
    InputSourceOperation::new(run(), Arc::new(job), input, source)
}

fn blob() -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    BackendStream(Box::pin(futures_util::stream::empty()))
}

fn staged(size: Option<u64>) -> StagedSource {
    StagedSource {
        blob: blob(),
        location: None,
        size,
        served_source: None,
    }
}

fn stage_call(job: &JobRecord, input: &InputSelection) -> IoCall {
    let serving = Some((node(), realm()));
    let stage = StageInputOperation::new(job, input, version(), [8; 32], serving);
    IoCall::child(stage)
}

// A local input is authorized as an S3 read of its creator, then read under the bucket's group.
#[test]
fn local_input_read() {
    let group = Ulid::from_bytes([3; 16]);
    let mut find = find(record(), input(None, None));
    let auth = creator_auth(&run());
    let read = SourceReadOperation::of_object(&auth, node(), ("in", "a.txt"), READ_OPERATION);
    assert_eq!(find.first(), Ok(Next::Call(IoCall::child(read))));
    let expected = find.get_call(&find.source, group);
    let authorized = child(Ok::<_, SourceReadError>(group));
    assert_eq!(find.next(authorized), Ok(Next::Call(expected)));
}

// A refused authorization names the input, and only an undecided one may be retried.
#[test]
fn refused_input_named() {
    let cases = [
        (
            SourceReadError::Denied,
            JobError::permanent("input in/a.txt access denied"),
        ),
        (
            SourceReadError::NotFound,
            JobError::permanent("input bucket in not found"),
        ),
        (
            SourceReadError::Unavailable("down".to_string()),
            JobError::retryable("input authorization failed: down"),
        ),
    ];
    for (error, expected) in cases {
        let mut find = find(record(), input(None, None));
        find.first().unwrap();
        let refused = child(Err::<GroupId, _>(error));
        assert_eq!(find.next(refused), Err(expected));
    }
}

// Only a device may read from a holder without a captured input.
#[test]
fn uncaptured_holder_refused() {
    let replies = [
        child(Ok::<_, IoError>(false)),
        child(Err::<bool, _>(IoError::Unfinished)),
    ];
    for reply in replies {
        let mut find = find(record(), input(Some(holder()), Some(version())));
        let role = LocalUserOperation::new(realm());
        assert_eq!(find.first(), Ok(Next::Call(IoCall::child(role))));
        assert_eq!(
            find.next(reply),
            Err(JobError::permanent("captured inputs are missing"))
        );
    }
}

// A device fetches one exact version, so a request without one is refused.
#[test]
fn device_needs_version() {
    let mut exact = find(record(), input(Some(holder()), Some(version())));
    exact.first().unwrap();
    assert!(is_child(&exact.next(child(Ok::<_, IoError>(true)))));
    let mut open = find(record(), input(Some(holder()), None));
    open.first().unwrap();
    assert_eq!(
        open.next(child(Ok::<_, IoError>(true))),
        Err(JobError::permanent(
            "realm input in/a.txt names no exact version"
        ))
    );
}

// A captured input on another node is staged at its version, and any other version is refused.
#[test]
fn captured_holder_staged() {
    let job = with_input(hashed());
    let selected = input(Some(holder()), Some(version()));
    let mut exact = find(job.clone(), selected.clone());
    assert_eq!(exact.first(), Ok(Next::Call(stage_call(&job, &selected))));
    let other = Ulid::from_bytes([1; 16]);
    let mut moved = find(job, input(Some(holder()), Some(other)));
    assert_eq!(
        moved.first().unwrap_err(),
        JobError::permanent("captured remote input does not match the physical input")
    );
}

// A reference on another node can only be read under a grant.
#[test]
fn reference_needs_grant() {
    let selected = input(Some(holder()), Some(version()));
    let mut find = find(with_input(reference()), selected);
    assert_eq!(
        find.first().unwrap_err(),
        JobError::permanent("a reference input on another node has no read grant")
    );
}

// Staged bytes must have the captured size.
#[test]
fn staged_size_checked() {
    let stage = |size| {
        let mut find = find(with_input(hashed()), input(None, Some(version())));
        find.step = SourceStep::Stage;
        let blob = blob();
        find.next(child(Ok::<_, StageError>(StagedInput { blob, size })))
    };
    assert!(matches!(
        stage(3),
        Ok(Next::Done(StagedSource { size: Some(3), .. }))
    ));
    assert_eq!(
        stage(2).unwrap_err(),
        JobError::permanent("staged input size differs from the captured input")
    );
}

// A failed local read looks for copies of the bytes, then stages from a holder.
#[test]
fn read_seeks_copies() {
    let job = with_input(hashed());
    let selected = input(None, Some(version()));
    let mut find = find(job.clone(), selected.clone());
    find.step = SourceStep::Read;
    let failed = child(Err::<GetObjectResult, _>(GetObjectError::NoSuchKey));
    let paths = ResolvePathsOperation::new([8; 32]);
    assert_eq!(find.next(failed), Ok(Next::Call(IoCall::child(paths))));
    let none = child(Ok::<Vec<HashIndex>, ResolvePathsError>(Vec::new()));
    assert_eq!(find.next(none), Ok(Next::Call(stage_call(&job, &selected))));
}

// When no holder serves either, the local read's error says the most.
#[test]
fn local_failure_reported() {
    let mut find = find(with_input(reference()), input(None, Some(version())));
    find.step = SourceStep::Read;
    let failed = child(Err::<GetObjectResult, _>(GetObjectError::NoSuchKey));
    assert_eq!(
        find.next(failed),
        Err(source_input_error(GetObjectError::NoSuchKey))
    );
}

fn claimed() -> JobRecord {
    let mut job = record();
    job.claim = Some(JobClaim {
        holder_node_id: node(),
        claim_token: token(),
        lease_expires_ms: 1,
    });
    job
}

fn spec_with_input() -> ExecutionSpec {
    let mut spec = spec();
    spec.inputs.push(input(None, None));
    spec
}

fn load(spec: &ExecutionSpec, job: JobRecord) -> LoadInputsOperation {
    LoadInputsOperation::new(run(), spec, Arc::new(job), &BTreeSet::new())
}

// A mounted input opens no stream, and a run without a claim stores nothing.
#[test]
fn mounted_input_skipped() {
    let mounted = BTreeSet::from(["a.txt".to_string()]);
    let job = Arc::new(record());
    let mut load = LoadInputsOperation::new(run(), &spec_with_input(), job, &mounted);
    assert_eq!(load.first(), Ok(Next::Done(Vec::new())));
}

// A claim stores which sources served it; a lost write stops the attempt from starting.
#[test]
fn served_sources_stored() {
    let start = || {
        let mut load = load(&spec(), claimed());
        let keep = keep_served(job(), token(), BTreeMap::new());
        assert_eq!(load.first(), Ok(Next::Call(IoCall::child(keep))));
        load
    };
    let kept = child(Ok::<_, JobMutationError>(claimed()));
    assert_eq!(start().next(kept), Ok(Next::Done(Vec::new())));
    let lost = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    let message = format!(
        "served sources not kept: {}",
        JobMutationError::TokenMismatch
    );
    assert_eq!(start().next(lost), Err(JobError::retryable(message)));
}

// A found input becomes one stream of the attempt.
#[test]
fn found_input_opened() {
    let mut load = load(&spec_with_input(), record());
    assert!(is_child(&load.first()));
    let found = child(Ok::<_, JobError>(staged(Some(3))));
    let Ok(Next::Done(files)) = load.next(found) else {
        panic!("the only input ends the load");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "/in/a.txt");
}

// The inputs together may not exceed the disk the run asked for.
#[test]
fn disk_budget_refused() {
    let mut spec = spec_with_input();
    spec.resources.disk_bytes = Some(2);
    let mut load = load(&spec, record());
    load.first().unwrap();
    let found = child(Ok::<_, JobError>(staged(Some(3))));
    assert_eq!(
        load.next(found),
        Err(JobError::permanent(
            "staged inputs need more than 3 bytes, the run requests 2"
        ))
    );
}

// A source of unknown size may be known later; a failed one ends the load as it failed.
#[test]
fn unusable_source_ends() {
    let mut unsized_load = load(&spec_with_input(), record());
    unsized_load.first().unwrap();
    assert_eq!(
        unsized_load.next(child(Ok::<_, JobError>(staged(None)))),
        Err(JobError::retryable("input in/a.txt has no size"))
    );
    let mut failed = load(&spec_with_input(), record());
    failed.first().unwrap();
    let error = JobError::permanent("denied");
    let reply = child(Err::<StagedSource, _>(error.clone()));
    assert_eq!(failed.next(reply), Err(error));
}
