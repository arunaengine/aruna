//! Tests the stop record of an ended execution by its calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::{JobErrorKind, PhysicalExecutionState};
use aruna_core::{NodeId, UserId};
use ulid::Ulid;

use super::*;
use crate::tests::records::{Family, REALM};

/// The physical job of execution 1 in the record fixture.
fn physical() -> JobId {
    JobId::from_bytes([31; 16])
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn records(envelopes: Vec<JobRecordEnvelope>) -> IoResult {
    child(Ok::<_, FamilyReadError>(envelopes))
}

fn of_kind(run: &[JobRecordEnvelope], kind: JobRecordKind) -> Vec<JobRecordEnvelope> {
    let matching = |envelope: &&JobRecordEnvelope| envelope.record.kind() == kind;
    run.iter().filter(matching).cloned().collect()
}

/// The stop operation of the fixture's executor, past the run link of `family`.
fn linked(family: &Family, local: NodeId) -> StopRecordOperation {
    let mut stop = StopRecordOperation::new(physical(), StopEvidence::Tombstoned, (local, REALM));
    stop.stamp(2_000);
    assert_eq!(
        stop.first(),
        Ok(Next::Call(IoCall::child(read_run_link(physical()))))
    );
    let link = RunLink {
        family: Some(family.family()),
        logical_job_id: Some(family.job_id),
        group_id: Ulid::from_bytes([3; 16]),
        created_by: UserId::new(Ulid::from_bytes([2; 16]), REALM),
        run_crate: None,
    };
    let receipts = LoadFamilyOperation::kind(family.family(), JobRecordKind::Receipt);
    let answer = stop.next(child(Ok::<_, JobMutationError>(Some(link))));
    assert_eq!(answer, Ok(Next::Call(IoCall::child(receipts))));
    stop
}

// The stop names the receipt and the first terminal update and is signed by this node.
#[test]
fn signs_terminal_stop() {
    let family = Family::new([42u8; 32]);
    let run = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let mut stop = linked(&family, family.target.public());
    let stops = LoadFamilyOperation::kind(family.family(), JobRecordKind::Stop);
    let answer = stop.next(records(of_kind(&run, JobRecordKind::Receipt)));
    assert_eq!(answer, Ok(Next::Call(IoCall::child(stops))));
    stop.next(records(Vec::new())).unwrap();
    let signing = stop.next(records(of_kind(&run, JobRecordKind::Update)));
    let Ok(Next::Call(IoCall::Effect(effect))) = signing else {
        panic!("the stop is signed: {signing:?}");
    };
    let Effect::Net(NetEffect::ExecutionSign(claim)) = *effect else {
        panic!("not a signing effect");
    };
    let JobFamilyRecord::Stop(record) = &claim.record else {
        panic!("not a stop record");
    };
    assert_eq!((record.update_sequence, record.stopped_at_ms), (1, 2_000));
    assert!(record.output_digest.is_some(), "a success names its output");
    let envelope = family.sign(&family.target, claim.record.clone());
    let signed = ExecutionSignEvent::Signed(Box::new(envelope));
    let event = IoResult::Event(Box::new(Event::Net(NetEvent::ExecutionSign(signed))));
    let Ok(Next::Done(rows)) = stop.next(event) else {
        panic!("the record and its outbox entry are written");
    };
    let spaces: Vec<&str> = rows.iter().map(|(space, _, _)| space.as_str()).collect();
    let prune = aruna_core::keyspaces::FAMILY_PRUNE_KEYSPACE;
    let expected = [FAMILY_RECORD_KEYSPACE, FAMILY_OUTBOX_KEYSPACE, prune, prune];
    assert_eq!(spaces, expected);
    assert_eq!(rows[0].1, rows[1].1);
}

// Another node's receipt, or a stop already stored, writes nothing.
#[test]
fn nothing_to_stop() {
    let family = Family::new([43u8; 32]);
    let run = family.run(1, 0, PhysicalExecutionState::Failed);
    let mut foreign = linked(&family, family.holder.public());
    let answer = foreign.next(records(of_kind(&run, JobRecordKind::Receipt)));
    assert_eq!(answer, Ok(Next::Done(Vec::new())));
    let mut stored = linked(&family, family.target.public());
    stored
        .next(records(of_kind(&run, JobRecordKind::Receipt)))
        .unwrap();
    let JobFamilyRecord::Receipt(receipt) = &of_kind(&run, JobRecordKind::Receipt)[0].record else {
        panic!("the fixture holds a receipt");
    };
    let earlier = ExecutionStop {
        execution_id: receipt.execution_id,
        submission_id: family.submission_id,
        request_digest: family.request_digest,
        executor_node_id: family.target.public(),
        receipt_digest: [0; 32],
        update_sequence: 1,
        update_digest: [0; 32],
        output_digest: None,
        evidence: StopEvidence::Tombstoned,
        stopped_at_ms: 1,
    };
    let earlier = family.sign(&family.target, JobFamilyRecord::Stop(Box::new(earlier)));
    assert_eq!(
        stored.next(records(vec![earlier])),
        Ok(Next::Done(Vec::new()))
    );
}

// A stop waits for the terminal update it must name; a job without a family has none.
#[test]
fn waits_for_terminal() {
    let family = Family::new([44u8; 32]);
    let run = family.run(1, 0, PhysicalExecutionState::Failed);
    let mut stop = linked(&family, family.target.public());
    stop.next(records(of_kind(&run, JobRecordKind::Receipt)))
        .unwrap();
    stop.next(records(Vec::new())).unwrap();
    let running = of_kind(&run, JobRecordKind::Update)[..1].to_vec();
    let error = stop.next(records(running)).unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Retryable);
    let mut unlinked = StopRecordOperation::new(
        physical(),
        StopEvidence::Tombstoned,
        (family.target.public(), REALM),
    );
    unlinked.first().unwrap();
    let none = unlinked.next(child(Ok::<Option<RunLink>, JobMutationError>(None)));
    assert_eq!(none, Ok(Next::Done(Vec::new())));
}
