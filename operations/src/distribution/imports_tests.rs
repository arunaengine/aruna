//! Tests handing a range's open imports to its new coordinator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;

use super::*;
use crate::distribution::test_record::{member, record, secret};

fn node(seed: u8) -> NodeId {
    secret(seed).public()
}

fn transfer() -> Ulid {
    Ulid::from_bytes([7; 16])
}

fn job(seed: u8) -> JobId {
    JobId::from_bytes([seed; 16])
}

fn child<T>(value: T) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(Ok::<T, ImportError>(value)))
}

fn row(bytes: Vec<u8>) -> IoResult {
    IoResult::Values(vec![Some(Value::from(bytes))])
}

fn none() -> IoResult {
    IoResult::Values(vec![None])
}

fn no_open() -> IoResult {
    child(Vec::<(JobId, StagingJobSpec)>::new())
}

// Without open imports only the empty job list is kept.
#[test]
fn keeps_empty_list() {
    let mut keep = KeepResumeOperation::new("spire", transfer());
    assert!(matches!(keep.first(), Ok(Next::Call(IoCall::Child(_)))));
    let empty = postcard::to_allocvec(&Vec::<JobId>::new()).unwrap();
    let written = state_row(jobs_key(transfer()), empty);
    assert_eq!(keep.next(no_open()), Ok(Next::Call(written)));
    assert_eq!(keep.next(IoResult::Done), Ok(Next::Done(())));
}

// The jobs kept before the fence stay listed, finished or not.
#[test]
fn lists_kept_jobs() {
    let mut fenced = FencedImportsOperation::new("spire", transfer());
    let read = state_read(jobs_key(transfer()));
    assert_eq!(fenced.first(), Ok(Next::Call(read)));
    let kept = postcard::to_allocvec(&vec![job(1)]).unwrap();
    assert!(matches!(
        fenced.next(row(kept)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(fenced.next(no_open()), Ok(Next::Done(vec![job(1)])));
}

// A job the former coordinator no longer knows is not served by anything else.
#[test]
fn unknown_job_missing() {
    let mut spec = StagingSpecOperation::new(node(2), (transfer(), job(1)));
    let read = state_read(resume_key(transfer(), job(1)));
    assert_eq!(spec.first(), Ok(Next::Call(read)));
    assert_eq!(spec.next(none()), Err(MetadataReadError::NotFound));
}

fn fetch(jobs: Vec<JobId>) -> FetchImportsOperation {
    let range = (
        Ulid::from_bytes([1; 16]),
        Ulid::from_bytes([2; 16]),
        transfer(),
    );
    FetchImportsOperation::new(range, node(1), node(2), jobs)
}

// Nothing to fetch finishes at once, with the kept row written.
#[test]
fn fetch_without_jobs() {
    let mut fetch = fetch(Vec::new());
    fetch.first().unwrap();
    assert!(matches!(
        fetch.next(none()),
        Ok(Next::Call(IoCall::Write(_)))
    ));
    assert_eq!(fetch.next(IoResult::Done), Ok(Next::Done(true)));
}

// A peer that does not answer leaves the job pending for the next pass.
#[test]
fn unanswered_job_waits() {
    use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
    let mut fetch = fetch(vec![job(1)]);
    fetch.first().unwrap();
    assert!(matches!(
        fetch.next(none()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let answer = MetadataAnswer {
        peer: node(2),
        elapsed: std::time::Duration::ZERO,
        result: Err(PeerRequestError::TimedOut),
    };
    let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
    let write = fetch.next(IoResult::Event(Box::new(event)));
    assert!(matches!(write, Ok(Next::Call(IoCall::Write(_)))));
    assert_eq!(fetch.next(IoResult::Done), Ok(Next::Done(false)));
}

fn handover_row(set_id: Ulid, range_id: Ulid) -> IoResult {
    let kept = Kept {
        from: node(2),
        pending: Vec::new(),
        ready: Vec::new(),
    };
    let key = Key::from(handover_key(set_id, range_id));
    let value = Value::from(postcard::to_allocvec(&kept).unwrap());
    IoResult::Page {
        rows: vec![(key, value)],
        next: None,
    }
}

// A row whose set is gone is dropped; one of a range coordinated here starts its jobs.
#[test]
fn continues_coordinated_ranges() {
    let (snapshot, bytes) = record(true);
    let set_id = snapshot.set_id();
    let range_id = snapshot.config.ranges[0].range_id;
    assert_eq!(snapshot.config.ranges[0].coordinator, member(1));

    let mut gone = ContinueImportsOperation::new(node(1));
    gone.first().unwrap();
    assert_eq!(
        gone.next(handover_row(set_id, range_id)),
        Ok(Next::Call(record_read(set_id)))
    );
    let delete = state_delete(handover_key(set_id, range_id));
    assert_eq!(gone.next(none()), Ok(Next::Call(delete)));
    assert_eq!(gone.next(IoResult::Done), Ok(Next::Done(())));

    let mut here = ContinueImportsOperation::new(node(1));
    here.first().unwrap();
    here.next(handover_row(set_id, range_id)).unwrap();
    assert!(matches!(
        here.next(row(bytes)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}

// Handover keys name their set and range; other keys name nothing.
#[test]
fn parses_handover_keys() {
    let (set_id, range_id) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
    let key = handover_key(set_id, range_id);
    assert_eq!(handover_parts(&key), Some((set_id, range_id)));
    assert_eq!(handover_parts(b"other"), None);
}
