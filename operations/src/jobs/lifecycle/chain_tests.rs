//! State and event tests of the execution chain read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::operation_check::{check_abort, rejects_events, rejects_finalize, sample_events};
use aruna_core::structs::execution::job::{
    EffectiveResources, ExecutionReceipt, JobFamilyRecord, JobId, JobRecordEnvelope,
    PhysicalExecutionState,
};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::{ExecutionChain, ExecutionChainOperation};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::alias::AliasFamilyOperation;
use crate::jobs::lifecycle::held::HeldReservations;
use crate::jobs::records::family::LoadFamilyOperation;
use crate::jobs::records::keys::{alias_key, record_key};
use crate::jobs::records::rows::to_bytes;
use crate::tests::records::Family;
use aruna_core::structs::execution::job::JobRecordKind;

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

fn page(values: Vec<(Key, Value)>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: None,
    })
}

fn held(rows: &[JobReservationRecord]) -> Event {
    page(
        rows.iter()
            .map(|row| {
                let bytes = to_bytes(row).expect("row encodes");
                (Key::from(&[1u8][..]), Value::from(bytes.as_slice()))
            })
            .collect(),
    )
}

fn alias() -> Event {
    let family = fixture().family();
    page(vec![(
        alias_key(fixture().job_id, &family),
        Value::from(&[0u8; 0][..]),
    )])
}

fn receipts(records: &[JobRecordEnvelope]) -> Event {
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

fn failed() -> Event {
    Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    })
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

#[test]
fn resolves_job_chain() {
    let (records, receipt) = run();
    let mut chain = ExecutionChainOperation::of_job(physical());
    assert_eq!(chain.start(), HeldReservations::of_job(physical()).start());
    assert_eq!(
        chain.step(held(&[reservation(&receipt)])),
        AliasFamilyOperation::new(fixture().job_id).start()
    );
    assert_eq!(
        chain.step(alias()),
        LoadFamilyOperation::kind(fixture().family(), JobRecordKind::Receipt).start()
    );
    assert!(chain.step(receipts(&records)).is_empty());
    assert!(chain.is_complete());
    assert_eq!(chain.finalize(), Ok(Some(expected(&receipt))));
}

#[test]
fn resolves_execution_chain() {
    let (records, receipt) = run();
    let mut chain = ExecutionChainOperation::of_execution(fixture().job_id, receipt.execution_id);
    assert_eq!(
        chain.start(),
        AliasFamilyOperation::new(fixture().job_id).start()
    );
    chain.step(alias());
    chain.step(receipts(&records));
    assert_eq!(chain.finalize(), Ok(Some(expected(&receipt))));
}

#[test]
fn local_job_has_none() {
    // No reservation is a purely local job: no chain, no error.
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    assert!(chain.step(held(&[])).is_empty());
    assert_eq!(chain.finalize(), Ok(None));
}

#[test]
fn other_attempt_has_none() {
    let (_, receipt) = run();
    let mut chain = ExecutionChainOperation::of_attempt(physical(), Ulid::from_bytes([9; 16]));
    chain.start();
    assert!(chain.step(held(&[reservation(&receipt)])).is_empty());
    assert_eq!(chain.finalize(), Ok(None));
}

#[test]
fn read_failures_give_none() {
    let (records, receipt) = run();
    // A failed reservation, alias or receipt read leaves the chain unresolved.
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    chain.step(failed());
    assert_eq!(chain.finalize(), Ok(None));

    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    chain.step(held(&[reservation(&receipt)]));
    chain.step(failed());
    assert_eq!(chain.finalize(), Ok(None));

    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    chain.step(held(&[reservation(&receipt)]));
    chain.step(page(Vec::new()));
    assert_eq!(chain.finalize(), Ok(None));

    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    chain.step(held(&[reservation(&receipt)]));
    chain.step(alias());
    chain.step(failed());
    assert_eq!(chain.finalize(), Ok(None));

    // A family without this execution's receipt has no chain either.
    let mut chain =
        ExecutionChainOperation::of_execution(fixture().job_id, Ulid::from_bytes([3; 16]));
    chain.start();
    chain.step(alias());
    chain.step(receipts(&records));
    assert_eq!(chain.finalize(), Ok(None));
}

#[test]
fn rejects_unfit_events() {
    let fits = |event: &Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
        )
    };
    let started = || {
        let mut chain = ExecutionChainOperation::of_job(physical());
        chain.start();
        chain
    };
    let refusals = rejects_events(started, sample_events, fits);
    assert!(!refusals.is_empty());
    for (label, effects, error) in refusals {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert!(
            matches!(error, LifecycleError::UnexpectedEvent { .. }),
            "{label} gave {error:?}"
        );
    }
    let unstarted = || ExecutionChainOperation::of_job(physical());
    assert_eq!(
        rejects_events(unstarted, sample_events, |_| false).len(),
        sample_events().len()
    );
    let mut done = started();
    done.step(held(&[]));
    done.step(held(&[]));
    assert!(matches!(
        done.finalize(),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}

#[test]
fn aborts_without_cleanup() {
    check_abort(&mut ExecutionChainOperation::of_job(physical()), &[]);
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    check_abort(&mut chain, &[]);
}

#[test]
fn rejects_early_finalize() {
    rejects_finalize(ExecutionChainOperation::of_job(physical()));
    let mut chain = ExecutionChainOperation::of_job(physical());
    chain.start();
    rejects_finalize(chain);
}
