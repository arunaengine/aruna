//! Tests the start fence by its calls and results: raising, the fence check, and the lift that
//! marks executions of a closed interval first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentDot, NodeInterval};
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::EffectiveResources;
use ulid::Ulid;

use super::*;
use crate::tests::records::REALM;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// The realm config view in which `local` was added at 1 and removed at 3.
fn removed(local: NodeId) -> WriterView {
    let origin = node(20);
    let dot = |seq: u64| AdminDocumentDot {
        event_id: Ulid::from_bytes([seq as u8; 16]),
        origin_node_id: origin,
        origin_seq: seq,
    };
    WriterView {
        clock: AdminDocumentClock::default().with_observed(origin, 3),
        intervals: vec![NodeInterval {
            node_id: local,
            added: dot(1),
            removed: vec![dot(3)],
        }],
    }
}

/// A reservation row of `job_id` admitted at realm config clock 2.
fn reserved(job_id: JobId) -> (Key, Value) {
    let row = JobReservationRecord {
        execution_id: Ulid::from_bytes([7u8; 16]),
        job_id,
        logical_job_id: job_id,
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 512,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        },
        created_at_ms: 5,
        subject_generation: 1,
        subject_digest: [0u8; 32],
        config_clock: AdminDocumentClock::default().with_observed(node(20), 2),
        backend: "default".to_string(),
        config_generation: 0,
        config_digest: [0; 32],
    };
    let value = Value::from(postcard::to_allocvec(&row).unwrap().as_slice());
    (Key::from(&b"r"[..]), value)
}

fn page(rows: Vec<(Key, Value)>) -> IoResult {
    IoResult::Page { rows, next: None }
}

/// A lift for node 4 past its membership read of `view`.
fn viewed(view: WriterView) -> LiftFenceOperation {
    let mut lift = LiftFenceOperation::new(node(4), REALM);
    lift.stamp(9);
    let read = IoCall::child(MembershipViewOperation::new(REALM));
    assert_eq!(lift.first(), Ok(Next::Call(read)));
    assert!(matches!(
        lift.next(child(Ok::<_, IoError>(Some(view)))),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    lift
}

// The raise writes the fence row; an unreadable fence row counts as up.
#[test]
fn raises_and_reads() {
    let mut raise = RaiseFenceOperation;
    let Ok(Next::Call(IoCall::Write(rows))) = raise.first() else {
        panic!("the fence row is written");
    };
    assert_eq!(
        (rows[0].0.as_str(), rows[0].1.as_ref()),
        (COMPUTE_DEPARTURE_KEYSPACE, START_FENCE_KEY)
    );
    assert_eq!(raise.next(IoResult::Done), Ok(Next::Done(())));
    assert!(fence_up(IoResult::Values(vec![Some(Value::from(&[][..]))])));
    assert!(!fence_up(IoResult::Values(vec![None])));
    assert!(fence_up(IoResult::Done));
}

// Without membership evidence nothing can be judged, so the fence stays up.
#[test]
fn unknown_membership_holds() {
    let mut lift = LiftFenceOperation::new(node(4), REALM);
    lift.first().unwrap();
    let unknown = lift.next(child(Ok::<Option<WriterView>, IoError>(None)));
    assert_eq!(unknown, Ok(Next::Done(false)));
}

// An execution of a closed interval is marked before the fence row goes and the node advertises.
#[test]
fn marks_then_lifts() {
    let job_id = JobId::from_bytes([8u8; 16]);
    let mut lift = viewed(removed(node(4)));
    let mark = IoCall::child(set_cancel_requested(job_id, 9));
    assert_eq!(
        lift.next(page(vec![reserved(job_id)])),
        Ok(Next::Call(mark))
    );
    let marked = child(Err::<CancelRequestOutcome, _>(JobMutationError::NotFound));
    assert_eq!(lift.next(marked), Ok(Next::Call(fence_read())));
    let up = IoResult::Values(vec![Some(Value::from(&[][..]))]);
    assert_eq!(
        lift.next(up),
        Ok(Next::Call(IoCall::Delete(vec![fence_row()])))
    );
    let Ok(Next::Then { output, calls }) = lift.next(IoResult::Done) else {
        panic!("the node advertises again");
    };
    assert!(output);
    assert_eq!(calls.len(), 1);
}

// When the mark fails the fence stays up, so the execution cannot start in between; a node that
// is not fenced has nothing to lift.
#[test]
fn failed_mark_holds() {
    let job_id = JobId::from_bytes([8u8; 16]);
    let mut lift = viewed(removed(node(4)));
    lift.next(page(vec![reserved(job_id)])).unwrap();
    let failed = child(Err::<CancelRequestOutcome, _>(
        JobMutationError::TokenMismatch,
    ));
    assert_eq!(lift.next(failed), Ok(Next::Done(false)));
    let mut open = viewed(WriterView::default());
    assert_eq!(
        open.next(page(vec![reserved(job_id)])),
        Ok(Next::Call(fence_read()))
    );
    assert_eq!(
        open.next(IoResult::Values(vec![None])),
        Ok(Next::Done(true))
    );
}
