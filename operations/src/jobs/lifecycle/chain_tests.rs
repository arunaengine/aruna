//! Call and result tests of the execution chain read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    EffectiveResources, ExecutionReceipt, JobFamilyRecord, JobId, JobRecordBody, JobRecordEnvelope,
    JobRecordKind, PhysicalExecutionState,
};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::{ExecutionChain, ExecutionChainOperation};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::alias::AliasFamilyOperation;
use crate::jobs::lifecycle::held::HeldReservations;
use crate::jobs::records::family::LoadFamilyOperation;
use crate::tests::records::Family;
use aruna_core::structs::execution::job::record_keys::{alias_key, record_key};
use aruna_core::structs::execution::job::record_rows::to_bytes;

fn fixture() -> Family {
    Family::new([5u8; 32])
}

fn physical() -> JobId {
    JobId::from_bytes([7u8; 16])
}

/// The records of one run and the receipt among them.
fn run() -> (Vec<JobRecordEnvelope>, ExecutionReceipt) {
    let records = fixture().run(1, 0, PhysicalExecutionState::Succeeded);
    let receipt = records
        .iter()
        .find_map(|envelope| match &envelope.record {
            JobFamilyRecord::Receipt(receipt) => Some(receipt.as_ref().clone()),
            _ => None,
        })
        .expect("the run holds a receipt");
    (records, receipt)
}

fn reservation(receipt: &ExecutionReceipt) -> JobReservationRecord {
    JobReservationRecord {
        execution_id: receipt.execution_id,
        job_id: physical(),
        logical_job_id: fixture().job_id,
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 0,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        },
        created_at_ms: 1,
        subject_generation: 0,
        subject_digest: [0; 32],
    }
}

fn page(rows: Vec<(Key, Value)>) -> IoResult {
    IoResult::Page { rows, next: None }
}

fn held(rows: &[JobReservationRecord]) -> IoResult {
    page(
        rows.iter()
            .map(|row| {
                let bytes = to_bytes(row).expect("row encodes");
                (Key::from(&[1u8][..]), Value::from(bytes.as_slice()))
            })
            .collect(),
    )
}

fn alias() -> IoResult {
    let family = fixture().family();
    page(vec![(
        alias_key(fixture().job_id, &family),
        Value::from(&[0u8; 0][..]),
    )])
}

fn receipts(records: &[JobRecordEnvelope]) -> IoResult {
    page(
        records
            .iter()
            .filter(|envelope| matches!(envelope.record, JobFamilyRecord::Receipt(_)))
            .map(|envelope| {
                let bytes = to_bytes(envelope).expect("record encodes");
                (record_key(&envelope.key()), Value::from(bytes.as_slice()))
            })
            .collect(),
    )
}

/// The call a child read makes first.
fn call_of<T>(next: Result<Next<T>, impl std::fmt::Debug>) -> IoCall {
    match next.expect("child starts") {
        Next::Call(call) => call,
        _ => panic!("child makes a call first"),
    }
}

fn expected(receipt: &ExecutionReceipt) -> ExecutionChain {
    ExecutionChain {
        family: fixture().family(),
        execution_id: receipt.execution_id,
        executor_node_id: receipt.executor_node_id,
        spec_digest: receipt.spec_digest,
        receipt_digest: receipt.digest().expect("receipt digest"),
        job_id: receipt.job_id,
    }
}

fn chain_call(next: Result<Next<Option<ExecutionChain>>, LifecycleError>) -> IoCall {
    call_of(next)
}

#[test]
fn resolves_job_chain() {
    let (records, receipt) = run();
    let mut chain = ExecutionChainOperation::of_job(physical());
    assert_eq!(
        chain_call(chain.first()),
        call_of(HeldReservations::of_job(physical()).first())
    );
    assert_eq!(
        chain_call(chain.next(held(&[reservation(&receipt)]))),
        call_of(AliasFamilyOperation::new(fixture().job_id).first())
    );
    assert_eq!(
        chain_call(chain.next(alias())),
        call_of(LoadFamilyOperation::kind(fixture().family(), JobRecordKind::Receipt).first())
    );
    assert_eq!(
        chain.next(receipts(&records)),
        Ok(Next::Done(Some(expected(&receipt))))
    );
}

#[test]
fn resolves_execution_chain() {
    let (records, receipt) = run();
    let mut chain = ExecutionChainOperation::of_execution(fixture().job_id, receipt.execution_id);
    assert_eq!(
        chain_call(chain.first()),
        call_of(AliasFamilyOperation::new(fixture().job_id).first())
    );
    chain.next(alias()).expect("receipt read");
    assert_eq!(
        chain.next(receipts(&records)),
        Ok(Next::Done(Some(expected(&receipt))))
    );
}

#[test]
fn skips_local_jobs() {
    // No reservation is a purely local job: no chain, no error.
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.first().expect("reservation read");
    assert_eq!(chain.next(held(&[])), Ok(Next::Done(None)));
}

#[test]
fn skips_other_attempts() {
    let (_, receipt) = run();
    let mut chain = ExecutionChainOperation::of_attempt(physical(), Ulid::from_bytes([9; 16]));
    chain.first().expect("reservation read");
    assert_eq!(
        chain.next(held(&[reservation(&receipt)])),
        Ok(Next::Done(None))
    );
}

#[test]
fn failures_give_none() {
    let (records, receipt) = run();
    // A corrupt reservation, no alias row, or a family without this execution's
    // receipt leaves the chain unresolved.
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.first().expect("reservation read");
    let corrupt = page(vec![(Key::from(&[1u8][..]), Value::from(&[0xffu8][..]))]);
    assert_eq!(chain.next(corrupt), Ok(Next::Done(None)));

    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.first().expect("reservation read");
    chain
        .next(held(&[reservation(&receipt)]))
        .expect("alias read");
    assert_eq!(chain.next(page(Vec::new())), Ok(Next::Done(None)));

    let mut chain =
        ExecutionChainOperation::of_execution(fixture().job_id, Ulid::from_bytes([3; 16]));
    chain.first().expect("alias read");
    chain.next(alias()).expect("receipt read");
    assert_eq!(chain.next(receipts(&records)), Ok(Next::Done(None)));
}

#[test]
fn rejects_unfit_result() {
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.first().expect("reservation read");
    assert!(matches!(chain.next(IoResult::Done), Ok(Next::Done(None))));
    // After the chain ended, any further result is refused.
    assert!(matches!(
        chain.next(IoResult::Done),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}
