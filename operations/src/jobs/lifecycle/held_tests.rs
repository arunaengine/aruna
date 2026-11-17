//! Call and result tests of the held reservation read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::keyspaces::JOB_RESERVATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{EffectiveResources, JobId};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::HeldReservations;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::reservation::MAX_RESERVATION_SCAN;
use aruna_core::structs::execution::job::record_rows::to_bytes;

fn row(seed: u8) -> JobReservationRecord {
    JobReservationRecord {
        execution_id: Ulid::from_bytes([seed; 16]),
        job_id: JobId::from_bytes([seed; 16]),
        logical_job_id: JobId::from_bytes([seed + 1; 16]),
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 0,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        },
        created_at_ms: 10,
        subject_generation: 0,
        subject_digest: [0; 32],
    }
}

fn entry(seed: u8) -> (Key, Value) {
    let bytes = to_bytes(&row(seed)).expect("encodable row");
    (Key::from(vec![seed]), Value::from(bytes.as_slice()))
}

fn scan(after: Option<u8>) -> Next<Vec<JobReservationRecord>> {
    Next::Call(IoCall::Page {
        space: JOB_RESERVATION_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after: after.map(|key| Key::from(vec![key])),
        limit: MAX_RESERVATION_SCAN,
    })
}

fn page(rows: Vec<(Key, Value)>, next: Option<u8>) -> IoResult {
    IoResult::Page {
        rows,
        next: next.map(|key| Key::from(vec![key])),
    }
}

#[test]
fn reads_every_page() {
    let mut held = HeldReservations::all();
    assert_eq!(held.first(), Ok(scan(None)));
    assert_eq!(held.next(page(vec![entry(1)], Some(1))), Ok(scan(Some(1))));
    assert_eq!(
        held.next(page(vec![entry(2)], None)),
        Ok(Next::Done(vec![row(1), row(2)]))
    );
}

#[test]
fn keeps_one_job() {
    let mut held = HeldReservations::of_job(JobId::from_bytes([2; 16]));
    held.first().expect("first page");
    let rows = vec![entry(1), entry(2), entry(3)];
    assert_eq!(held.next(page(rows, None)), Ok(Next::Done(vec![row(2)])));
}

#[test]
fn rejects_corrupt_row() {
    let mut held = HeldReservations::all();
    held.first().expect("first page");
    let corrupt = (Key::from(vec![1]), Value::from(&[0xff][..]));
    assert!(matches!(
        held.next(page(vec![corrupt], None)),
        Err(LifecycleError::Conversion(_))
    ));
}

#[test]
fn rejects_unfit_result() {
    let mut held = HeldReservations::all();
    held.first().expect("first page");
    assert!(matches!(
        held.next(IoResult::Done),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}
