//! State and event tests of the plugin row operation with a small claim plan.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use ulid::Ulid;

use super::*;

const SPACE: &str = "plugin_state";

/// Claims one row: refuses an empty key, keeps a row that exists, writes it otherwise.
#[derive(Debug, PartialEq)]
struct Claim {
    key: Vec<u8>,
}

impl Plan for Claim {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<bool>, StoreError> {
        match self.key.is_empty() {
            true => Err(StoreError("empty key".into())),
            false => Ok(Turn::Ask(Ask::Read(vec![(SPACE, self.key.clone())]))),
        }
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, StoreError> {
        match answer {
            Answer::Values(values) if values == [Some(b"held".to_vec())] => Ok(Turn::Finish(false)),
            Answer::Values(_) => Ok(Turn::Ask(Ask::Write(vec![row(
                SPACE,
                self.key.clone(),
                b"held".to_vec(),
            )]))),
            Answer::Done => Ok(Turn::Finish(true)),
            Answer::Page(..) => Err(StoreError("page".into())),
        }
    }
}

fn txn() -> TxnId {
    Ulid::from_parts(7, 7)
}

fn claim() -> RowsOperation<Claim> {
    RowsOperation::transaction(Claim { key: b"a".to_vec() })
}

fn storage(effect: StorageEffect) -> Effect {
    Effect::Storage(effect)
}

fn started() -> Event {
    Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
}

fn read(value: Option<&[u8]>) -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(b"a".to_vec().into(), value.map(|value| value.to_vec().into()))],
    })
}

fn written() -> Event {
    Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    })
}

fn read_effect(txn_id: Option<TxnId>) -> Effect {
    storage(StorageEffect::BatchRead {
        reads: vec![(SPACE.to_string(), b"a".to_vec().into())],
        txn_id,
    })
}

fn write_effect(txn_id: Option<TxnId>) -> Effect {
    storage(StorageEffect::BatchWrite {
        writes: vec![(SPACE.to_string(), b"a".to_vec().into(), b"held".to_vec().into())],
        txn_id,
    })
}

fn abort_effect() -> Effect {
    storage(StorageEffect::AbortTransaction { txn_id: txn() })
}

#[test]
fn commits_written_claim() {
    let mut operation = claim();
    let commit = storage(StorageEffect::CommitTransaction { txn_id: txn() });
    check_trace(
        &mut operation,
        &[storage(StorageEffect::StartTransaction { read: false })],
        vec![
            (started(), vec![read_effect(Some(txn()))]),
            (read(None), vec![write_effect(Some(txn()))]),
            (written(), vec![commit]),
            (
                Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() }),
                vec![],
            ),
        ],
    );
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Ok(true));
}

#[test]
fn direct_skips_transaction() {
    let mut operation = RowsOperation::direct(Claim { key: b"a".to_vec() });
    check_trace(
        &mut operation,
        &[read_effect(None)],
        vec![(read(Some(b"held")), vec![])],
    );
    assert_eq!(operation.finalize(), Ok(false));
}

#[test]
fn refusal_before_storage() {
    let mut operation = RowsOperation::transaction(Claim { key: Vec::new() });
    assert!(operation.start().is_empty());
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Err(StoreError("empty key".into())));
}

#[test]
fn storage_error_aborts() {
    let mut operation = claim();
    operation.start();
    operation.step(started());
    let failed = Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    });
    assert_eq!(operation.step(failed).as_slice(), [abort_effect()]);
    assert!(operation.finalize().is_err());
}

#[test]
fn failed_commit_unaborted() {
    let mut operation = claim();
    operation.start();
    operation.step(started());
    operation.step(read(None));
    operation.step(written());
    let failed = Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    });
    assert!(operation.step(failed).is_empty());
    assert!(operation.finalize().is_err());
}

#[test]
fn rejects_unexpected_events() {
    let waiting = || {
        let mut operation = claim();
        operation.start();
        operation.step(started());
        operation
    };
    let refusals = rejects_events(waiting, sample_events, |event| {
        matches!(event, Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == 1)
    });
    for (label, effects, _) in refusals {
        assert_eq!(effects.as_slice(), [abort_effect()], "{label}");
    }
    let starting = || {
        let mut operation = claim();
        operation.start();
        operation
    };
    let refusals = rejects_events(starting, sample_events, |event| {
        matches!(event, Event::Storage(StorageEvent::TransactionStarted { .. }))
    });
    assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
    let committing = || {
        let mut operation = claim();
        operation.start();
        operation.step(started());
        operation.step(read(None));
        operation.step(written());
        operation
    };
    let refusals = rejects_events(committing, sample_events, |event| {
        matches!(event, Event::Storage(StorageEvent::TransactionCommitted { .. }))
    });
    assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
}

#[test]
fn aborts_each_phase() {
    let mut operation = claim();
    check_abort(&mut operation, &[]);
    operation.start();
    check_abort(&mut operation, &[]);
    let mut operation = claim();
    operation.start();
    operation.step(started());
    check_abort(&mut operation, &[abort_effect()]);
    let mut operation = claim();
    operation.start();
    operation.step(started());
    operation.step(read(None));
    operation.step(written());
    check_abort(&mut operation, &[]);
}

#[test]
fn rejects_early_finalize() {
    rejects_finalize(claim());
    let mut operation = claim();
    operation.start();
    operation.step(started());
    rejects_finalize(operation);
}

#[test]
fn empty_write_skipped() {
    #[derive(Debug, PartialEq)]
    struct Nothing;
    impl Plan for Nothing {
        type Output = ();
        type Error = StoreError;
        fn first(&mut self) -> Result<Turn<()>, StoreError> {
            Ok(Turn::Ask(Ask::Write(Vec::new())))
        }
        fn next(&mut self, answer: Answer) -> Result<Turn<()>, StoreError> {
            assert_eq!(answer, Answer::Done);
            Ok(Turn::Finish(()))
        }
    }
    let mut operation = RowsOperation::transaction(Nothing);
    assert!(operation.start().is_empty());
    assert_eq!(operation.finalize(), Ok(()));
}
