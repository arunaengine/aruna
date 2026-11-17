//! State and event tests of the held reservation read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::JOB_RESERVATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use aruna_core::structs::execution::job::{EffectiveResources, JobId};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::HeldReservations;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::reservation::MAX_RESERVATION_SCAN;
use aruna_core::compute::quota::JobReservationRecord;
use crate::jobs::records::rows::to_bytes;

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

fn scan(start_after: Option<u8>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: JOB_RESERVATION_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(|key| IterStart::After(Key::from(vec![key]))),
        limit: MAX_RESERVATION_SCAN,
        txn_id: None,
    })
}

fn page(values: Vec<(Key, Value)>, next: Option<u8>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: next.map(|key| Key::from(vec![key])),
    })
}

#[test]
fn reads_every_page() {
    let mut held = HeldReservations::all();
    check_trace(
        &mut held,
        &[scan(None)],
        vec![
            (page(vec![entry(1)], Some(1)), vec![scan(Some(1))]),
            (page(vec![entry(2)], None), Vec::new()),
        ],
    );
    assert!(held.is_complete());
    assert_eq!(held.finalize().expect("held rows"), vec![row(1), row(2)]);
}

#[test]
fn keeps_one_job() {
    let mut held = HeldReservations::of_job(JobId::from_bytes([2; 16]));
    check_trace(
        &mut held,
        &[scan(None)],
        vec![(page(vec![entry(1), entry(2), entry(3)], None), Vec::new())],
    );
    assert_eq!(held.finalize().expect("held rows"), vec![row(2)]);
}

#[test]
fn reports_storage_error() {
    let mut held = HeldReservations::all();
    held.start();
    let error = StorageError::Timeout;
    assert!(
        held.step(Event::Storage(StorageEvent::Error {
            error: error.clone()
        }))
        .is_empty()
    );
    assert_eq!(held.finalize(), Err(LifecycleError::Storage(error)));
}

#[test]
fn rejects_corrupt_row() {
    let mut held = HeldReservations::all();
    held.start();
    let corrupt = (Key::from(vec![1]), Value::from(&[0xff][..]));
    assert!(held.step(page(vec![corrupt], None)).is_empty());
    assert!(matches!(
        held.finalize(),
        Err(LifecycleError::Conversion(_))
    ));
}

#[test]
fn rejects_unfit_events() {
    let started = || {
        let mut held = HeldReservations::all();
        held.start();
        held
    };
    let refusals = rejects_events(started, sample_events, |event| {
        matches!(
            event,
            Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
        )
    });
    assert!(!refusals.is_empty());
    for (label, effects, error) in refusals {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert!(
            matches!(error, LifecycleError::UnexpectedEvent { .. }),
            "{label} gave {error:?}"
        );
    }
    // Before `start` no event fits.
    let refusals = rejects_events(HeldReservations::all, sample_events, |_| false);
    assert_eq!(refusals.len(), sample_events().len());
}

#[test]
fn aborts_without_cleanup() {
    let mut held = HeldReservations::all();
    check_abort(&mut held, &[]);
    held.start();
    check_abort(&mut held, &[]);
}

#[test]
fn rejects_early_finalize() {
    rejects_finalize(HeldReservations::all());
    let mut held = HeldReservations::all();
    held.start();
    rejects_finalize(held);
}
