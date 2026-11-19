//! Tests the copy job by the calls and results of its operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::errors::StorageError;
use aruna_core::jobs::JobStops;
use aruna_core::structs::execution::job::JobErrorKind;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::format::Compression;
use ulid::Ulid;

use super::*;

fn realm() -> RealmId {
    RealmId([1; 32])
}

fn group() -> Ulid {
    Ulid::from_bytes([3; 16])
}

fn spec() -> CopyJobSpec {
    CopyJobSpec {
        auth_context: AuthContext {
            user_id: UserId::local(Ulid::from_bytes([5; 16]), realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        },
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        source_bucket: "source".to_string(),
        source_key: "a.txt".to_string(),
        source_version_id: None,
        source_group_id: group(),
        dest_bucket: "dest".to_string(),
        dest_key: "b.txt".to_string(),
        group_id: group(),
    }
}

fn copy() -> CopyJobOperation {
    let job_id = JobId::from_bytes([7; 16]);
    let pulled = Arc::new(AtomicU64::new(0));
    CopyJobOperation::new(spec(), job_id, NodeRouting::default(), pulled)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn stops(cancelled: bool, shutdown: bool) -> IoResult {
    let stops = JobStops {
        cancelled,
        shutdown,
    };
    IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
}

fn reported() -> IoResult {
    IoResult::Event(Box::new(Event::Job(JobEvent::Reported)))
}

fn bucket(group_id: Ulid) -> IoResult {
    child(Ok::<_, GetBucketError>(BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::local(Ulid::from_bytes([5; 16]), realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }))
}

fn bucket_call(name: &str) -> Next<JobRunOutcome> {
    Next::Call(IoCall::child(GetBucketOperation::new(name.to_string())))
}

/// A copy past its stop check, with both buckets read and the realm quota known.
fn configured() -> CopyJobOperation {
    let mut copy = copy();
    copy.first().unwrap();
    assert_eq!(copy.next(stops(false, false)), Ok(bucket_call("source")));
    assert_eq!(copy.next(bucket(group())), Ok(bucket_call("dest")));
    let config = GetConfigOperation::new(realm());
    assert_eq!(
        copy.next(bucket(group())),
        Ok(Next::Call(IoCall::child(config)))
    );
    let document = RealmConfigDocument::new(realm(), Vec::new(), 1);
    assert!(matches!(
        copy.next(child(Ok::<_, GetConfigError>(document))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    copy
}

fn failure_kind(next: CopyNext) -> JobErrorKind {
    let Ok(Next::Done(JobRunOutcome::Failed(error))) = next else {
        panic!("the copy fails: {next:?}");
    };
    error.kind
}

// A job told to stop never starts the copy.
#[test]
fn stopped_job_skips() {
    let mut cancelled = copy();
    let stops_call = IoCall::Effect(Box::new(Effect::Job(JobEffect::Stops)));
    assert_eq!(cancelled.first(), Ok(Next::Call(stops_call)));
    assert_eq!(
        cancelled.next(stops(true, false)),
        Ok(Next::Done(JobRunOutcome::Cancelled))
    );
    let mut stopping = copy();
    stopping.first().unwrap();
    assert_eq!(
        stopping.next(stops(false, true)),
        Ok(Next::Done(JobRunOutcome::Interrupted))
    );
}

// A bucket that is gone fails the copy for good; one that cannot be read now retries.
#[test]
fn bucket_failures_judged() {
    let mut missing = copy();
    missing.first().unwrap();
    missing.next(stops(false, false)).unwrap();
    let gone = child(Err::<BucketInfo, _>(GetBucketError::NotFound));
    assert_eq!(failure_kind(missing.next(gone)), JobErrorKind::Permanent);
    let mut unread = copy();
    unread.first().unwrap();
    unread.next(stops(false, false)).unwrap();
    unread.next(bucket(group())).unwrap();
    let broken = GetBucketError::StorageError(StorageError::ReadError("down".to_string()));
    let failed = child(Err::<BucketInfo, _>(broken));
    assert_eq!(failure_kind(unread.next(failed)), JobErrorKind::Retryable);
}

// A bucket that moved to another group after the copy was queued refuses it.
#[test]
fn moved_bucket_refused() {
    let mut copy = copy();
    copy.first().unwrap();
    copy.next(stops(false, false)).unwrap();
    copy.next(bucket(group())).unwrap();
    let moved = bucket(Ulid::from_bytes([9; 16]));
    assert_eq!(failure_kind(copy.next(moved)), JobErrorKind::Permanent);
}

// Without a known size the copy starts at once, and its result is reported and returned.
#[test]
fn copied_object_reported() {
    let mut copy = configured();
    let unknown = HeadObjectError::StorageError(StorageError::ReadError("gone".to_string()));
    let head = child(Err::<HeadObjectResult, _>(unknown));
    assert!(matches!(copy.next(head), Ok(Next::Call(IoCall::Child(_)))));
    let version = Ulid::from_bytes([6; 16]);
    let copied = CopyResultData {
        location: None,
        size: 4,
        source_metadata: None,
        version_id: Ulid::from_bytes([8; 16]),
        created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        source_version_id: Some(version),
        source_last_modified: None,
    };
    let report = JobReport {
        current: Some(4),
        ..JobReport::default()
    };
    let report_call = IoCall::Effect(Box::new(Effect::Job(JobEffect::Report(report))));
    assert_eq!(
        copy.next(child(Ok::<_, CopyObjectError>(copied))),
        Ok(Next::Call(report_call))
    );
    let payload = JobResultPayload::CopyObject {
        version_id: version.to_string(),
        bytes: 4,
        blake3: String::new(),
    };
    assert_eq!(
        copy.next(reported()),
        Ok(Next::Done(JobRunOutcome::Succeeded(payload)))
    );
}
