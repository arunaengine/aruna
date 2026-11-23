//! Tests the start discard: when it runs, and its calls for executions, orphan reservations and
//! the witness state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::family::EffectiveResources;
use aruna_core::structs::execution::job::identity::JobId;
use ulid::Ulid;

use super::*;
use crate::tests::records::{REALM, node};

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn reservation(seed: u8) -> JobReservationRecord {
    JobReservationRecord {
        execution_id: Ulid::from_bytes([seed; 16]),
        job_id: JobId::from_bytes([seed; 16]),
        logical_job_id: JobId::from_bytes([seed; 16]),
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 0,
            disk_bytes: 0,
            max_walltime_ms: 0,
            preemptible: false,
        },
        created_at_ms: 1,
        subject_generation: 0,
        subject_digest: [0u8; 32],
        config_clock: Default::default(),
        backend: "default".to_string(),
        config_generation: 0,
        config_digest: [0; 32],
    }
}

fn serving() -> ServingNode {
    Some((node(1), REALM))
}

/// A discard at `now_ms` past its row read of `marker` and `contact`.
fn read(now_ms: u64, marker: bool, contact: Option<u64>) -> DiscardOperation {
    let mut discard = DiscardOperation::new(serving());
    discard.stamp(now_ms);
    let rows = vec![node_row(DISCARD_KEY), node_row(LAST_CONTACT_KEY)];
    assert_eq!(discard.first(), Ok(Next::Call(IoCall::Read(rows))));
    let marker = marker.then(|| Value::from(&[][..]));
    let contact = contact.map(|at| Value::from(at.to_be_bytes().as_slice()));
    let held = IoCall::child(HeldReservations::all());
    let answer = discard.next(IoResult::Values(vec![marker, contact]));
    assert_eq!(answer, Ok(Next::Call(held)));
    discard
}

fn held(rows: Vec<JobReservationRecord>) -> IoResult {
    child(Ok::<_, LifecycleError>(rows))
}

fn empty_page() -> IoResult {
    IoResult::Page {
        rows: Vec::new(),
        next: None,
    }
}

// A marker always discards; otherwise a long absence, a clock behind the last contact or an
// unknown contact with execution state does.
#[test]
fn decides_discard() {
    let day = 24 * 60 * 60 * 1000;
    let now = 100 * day;
    assert_eq!(
        discard_reason(true, LastContact::At(now), false, now),
        Some(DiscardReason::Marker)
    );
    let away = LastContact::At(now - RETENTION_MS - 1);
    assert_eq!(
        discard_reason(false, away, false, now),
        Some(DiscardReason::Offline)
    );
    let ahead = LastContact::At(now + 1);
    assert_eq!(
        discard_reason(false, ahead, false, now),
        Some(DiscardReason::ClockBack)
    );
    assert_eq!(
        discard_reason(false, LastContact::At(now - 1), true, now),
        None
    );
    assert_eq!(
        discard_reason(false, LastContact::Missing, true, now),
        Some(DiscardReason::Unknown)
    );
    assert_eq!(
        discard_reason(false, LastContact::Missing, false, now),
        None
    );
    assert_eq!(last_contact(Some(b"short")), LastContact::Unreadable);
    assert_eq!(last_contact(Some(&7u64.to_be_bytes())), LastContact::At(7));
}

// A node in recent contact keeps its execution state.
#[test]
fn recent_keeps_state() {
    let mut discard = read(5_000, false, Some(4_000));
    let answer = discard.next(held(vec![reservation(2)]));
    assert_eq!(answer, Ok(Next::Done(None)));
}

// A discard writes its marker, ends an execution with its job, releases an orphan reservation
// and then drops the witness state.
#[test]
fn ends_and_releases() {
    let mut discard = read(5_000, true, None);
    let (space, key) = node_row(DISCARD_KEY);
    let marker = IoCall::Write(vec![(space, key, Value::from(&[][..]))]);
    let held_rows = held(vec![reservation(2), reservation(4)]);
    assert_eq!(discard.next(held_rows), Ok(Next::Call(marker)));
    let job = IoCall::child(read_job_record(JobId::from_bytes([2; 16])));
    assert_eq!(discard.next(IoResult::Done), Ok(Next::Call(job)));
    let record = crate::jobs::workflow::fixtures::claimed();
    let found = child(Ok::<_, JobMutationError>(Some(record.clone())));
    let error = JobError::retryable(DISCARD_REASON);
    let end = discard_execution(JobId::from_bytes([2; 16]), 5_000, error);
    assert_eq!(discard.next(found), Ok(Next::Call(IoCall::child(end))));
    let publish = PublishTerminalOperation::new(record.clone(), serving(), 5_000);
    let ended = child(Ok::<_, JobMutationError>(record));
    assert_eq!(discard.next(ended), Ok(Next::Call(IoCall::child(publish))));
    let orphan = IoCall::child(read_job_record(JobId::from_bytes([4; 16])));
    let published = child(Ok::<_, LifecycleError>(false));
    assert_eq!(discard.next(published), Ok(Next::Call(orphan)));
    let release = ReleaseExecutionOperation::new(Ulid::from_bytes([4; 16]));
    let missing = child(Ok::<Option<JobRecord>, JobMutationError>(None));
    assert_eq!(
        discard.next(missing),
        Ok(Next::Call(IoCall::child(release)))
    );
    let released = discard.next(child(Ok::<_, LifecycleError>(true)));
    assert!(matches!(released, Ok(Next::Call(IoCall::Page { .. }))));
    let row = (Key::from(&b"e"[..]), Value::from(&[][..]));
    let page = IoResult::Page {
        rows: vec![row],
        next: None,
    };
    let delete = vec![(PLAN_EXPLAIN_KEYSPACE.to_string(), Key::from(&b"e"[..]))];
    assert_eq!(discard.next(page), Ok(Next::Call(IoCall::Delete(delete))));
    assert!(matches!(
        discard.next(IoResult::Done),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    for _ in 0..WITNESS_SPACES.len() - 1 {
        assert!(matches!(
            discard.next(empty_page()),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
    }
    assert_eq!(
        discard.next(empty_page()),
        Ok(Next::Done(Some(DiscardReason::Marker)))
    );
}
