//! Drives realm config setters through their write transaction, or a lost commit and a retry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{EffectOperation, Operation, TransactionMode};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, Value};
use std::time::Duration;
use ulid::Ulid;

use super::COMMIT_ATTEMPTS;
use crate::sync::outbox_record::schedule_drain_effect;

/// Checks that a setter's write transaction reruns from its read after a lost commit.
pub(super) fn retries_conflicts<O: Operation>() {
    assert!(O::is_conflict(&O::Error::from(IoError::Conflict)));
    assert_eq!(O::conflict_retries(), COMMIT_ATTEMPTS as u32 - 1);
}

/// Runs a setter's write transaction once on `config` with no admin state yet, and returns the
/// config it writes and its output, which comes only with the drain wake after the commit.
pub(super) fn write_once<O: Operation>(
    write: &mut O,
    actor: &Actor,
    config: &RealmConfigDocument,
) -> (RealmConfigDocument, O::Output) {
    assert_eq!(write.transaction(), TransactionMode::Write);
    assert!(matches!(write.first(), Ok(Next::Call(IoCall::Read(rows))) if rows.len() == 2));
    let read = IoResult::Values(vec![Some(config.to_bytes(actor).unwrap().into()), None]);
    let mut turn = write.next(read);
    let mut written = None;
    loop {
        turn = match turn {
            Ok(Next::Call(IoCall::Write(rows))) => {
                let (_, _, value) = rows
                    .iter()
                    .find(|(space, _, _)| space == REALM_CONFIG_KEYSPACE)
                    .expect("the write stores the realm config");
                written = Some(RealmConfigDocument::from_bytes(value).unwrap());
                write.next(IoResult::Done)
            }
            Ok(Next::Call(IoCall::Delete(_))) => write.next(IoResult::Done),
            Ok(Next::Then { output, calls }) => {
                assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
                return (written.expect("the config is written"), output);
            }
            Ok(_) => panic!("the write asked for another call before its commit"),
            Err(error) => panic!("the write failed: {error:?}"),
        };
    }
}

/// Lets `operation` read `first` and lose its commit, then read `newer` in a fresh transaction
/// and commit; returns the rows the retry wrote. Each read answers the keys in request order.
pub(super) fn conflict_then_write<O: EffectOperation>(
    operation: &mut O,
    first: Vec<Option<Vec<u8>>>,
    newer: Vec<Option<Vec<u8>>>,
) -> Vec<(KeySpace, Key, Value)> {
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::SubOperation(_)]
    ));
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
    ));
    let effects = read_in(operation, effects, 1, first);
    until_commit(operation, effects, 1);
    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));
    assert_eq!(
        effects.as_slice(),
        &[Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })],
        "a lost commit starts over and schedules no drain"
    );

    let effects = read_in(operation, effects, 2, newer);
    let writes = until_commit(operation, effects, 2);
    let txn_id = Ulid::from(2u128);
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    assert_eq!(effects.as_slice(), &[schedule_drain_effect()]);
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainSyncOutbox,
        after: Duration::ZERO,
    }));
    assert!(operation.is_complete());
    writes
}

/// Starts attempt `attempt`, answers its read with `values` in request order, and returns what
/// the operation asks next.
fn read_in<O: EffectOperation>(
    operation: &mut O,
    effects: Effects,
    attempt: u128,
    values: Vec<Option<Vec<u8>>>,
) -> Effects {
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction { .. })]
    ));
    let txn_id = Ulid::from(attempt);
    let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let [
        Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: read_txn,
        }),
    ] = effects.as_slice()
    else {
        panic!("attempt {attempt} reads first: {effects:?}");
    };
    assert_eq!(
        *read_txn,
        Some(txn_id),
        "attempt {attempt} reads in its own transaction"
    );
    assert_eq!(reads.len(), values.len());
    let values = reads
        .iter()
        .zip(values)
        .map(|((_, key), value)| (key.clone(), value.map(Value::from)))
        .collect();
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
}

/// Answers the writes of attempt `attempt` until it asks to commit, and returns those writes.
fn until_commit<O: EffectOperation>(
    operation: &mut O,
    mut effects: Effects,
    attempt: u128,
) -> Vec<(KeySpace, Key, Value)> {
    let txn_id = Ulid::from(attempt);
    let mut writes = Vec::new();
    loop {
        let next = match effects.as_slice() {
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit })] => {
                assert_eq!(
                    *commit, txn_id,
                    "attempt {attempt} commits its own transaction"
                );
                return writes;
            }
            [
                Effect::Storage(StorageEffect::BatchWrite {
                    writes: rows,
                    txn_id: write_txn,
                }),
            ] => {
                assert_eq!(
                    *write_txn,
                    Some(txn_id),
                    "attempt {attempt} writes in its own transaction"
                );
                writes.extend(rows.iter().cloned());
                Event::Storage(StorageEvent::BatchWriteResult {
                    entries: Vec::new(),
                })
            }
            [Effect::Storage(StorageEffect::BatchDelete { .. })] => {
                Event::Storage(StorageEvent::BatchDeleteResult {
                    entries: Vec::new(),
                })
            }
            [Effect::SubOperation(_)] => {
                Event::SubOperation(SubOperationEvent::EpochsMarked { result: Ok(()) })
            }
            other => panic!("attempt {attempt} before its commit: {other:?}"),
        };
        effects = operation.step(next);
    }
}
