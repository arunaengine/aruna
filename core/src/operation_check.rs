//! Drives an operation through fixed events and checks its effects, refusals, abort and finalize.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::effects::Effect;
use crate::errors::StorageError;
use crate::events::{Event, StorageEvent, SubOperationEvent};
use crate::operation::EffectOperation;
use crate::task::TaskEvent;
use crate::types::Effects;
use ulid::Ulid;

/// Starts `operation` and checks that `start` emits `start_effects`, then feeds each event in
/// turn and checks the effects of every step, in order.
pub fn check_trace<O: EffectOperation>(
    operation: &mut O,
    start_effects: &[Effect],
    steps: Vec<(Event, Vec<Effect>)>,
) {
    assert_eq!(
        operation.start().as_slice(),
        start_effects,
        "effects of start"
    );
    for (index, (event, expected)) in steps.into_iter().enumerate() {
        let label = format!("{event:?}");
        let effects = operation.step(event);
        assert_eq!(
            effects.as_slice(),
            expected,
            "effects of step {index}: {label}"
        );
    }
}

/// Steps a fresh operation from `make` with each event of `events` that `expected` does not
/// accept and checks that every one ends the operation with an error. Returns the event text,
/// the effects and the error of each refusal, so the caller can check cleanup and error kind.
pub fn rejects_events<O: EffectOperation>(
    make: impl Fn() -> O,
    events: impl Fn() -> Vec<Event>,
    expected: impl Fn(&Event) -> bool,
) -> Vec<(String, Effects, O::Error)> {
    let mut refusals = Vec::new();
    for index in 0..events().len() {
        let event = events().swap_remove(index);
        if expected(&event) {
            continue;
        }
        let label = format!("{event:?}");
        let mut operation = make();
        let effects = operation.step(event);
        assert!(
            operation.is_complete(),
            "{label} left the operation running"
        );
        match operation.finalize() {
            Ok(output) => panic!("{label} was accepted with {output:?}"),
            Err(error) => refusals.push((label, effects, error)),
        }
    }
    refusals
}

/// One event of each common kind, as the event list of [`rejects_events`].
pub fn sample_events() -> Vec<Event> {
    let txn_id = Ulid::from_parts(1, 1);
    let key = || Vec::<u8>::new().into();
    vec![
        Event::Storage(StorageEvent::TransactionStarted { txn_id }),
        Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
        Event::Storage(StorageEvent::TransactionAborted { txn_id }),
        Event::Storage(StorageEvent::ReadResult {
            key: key(),
            value: None,
        }),
        Event::Storage(StorageEvent::BatchReadResult { values: Vec::new() }),
        Event::Storage(StorageEvent::WriteResult { key: key() }),
        Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }),
        Event::Storage(StorageEvent::DeleteResult { key: key() }),
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }),
        Event::Storage(StorageEvent::SyncAllFinished),
        Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }),
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }),
        Event::Task(TaskEvent::Error {
            key: None,
            message: String::new(),
        }),
        Event::SubOperation(SubOperationEvent::NotificationsEmitted),
    ]
}

/// Checks that `abort` emits exactly `cleanup`.
pub fn check_abort<O: EffectOperation>(operation: &mut O, cleanup: &[Effect]) {
    assert_eq!(operation.abort().as_slice(), cleanup, "effects of abort");
}

/// Checks that an operation that has not finished reports no result.
pub fn rejects_finalize<O: EffectOperation>(operation: O) {
    assert!(!operation.is_complete(), "the operation already finished");
    if let Ok(output) = operation.finalize() {
        panic!("an unfinished operation reported {output:?}");
    }
}
