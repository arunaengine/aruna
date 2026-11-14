//! Drives a realm config setter through a lost commit and a successful retry in unit tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, Value};
use std::time::Duration;
use ulid::Ulid;

use crate::realm::mutate_placement::node_kind;
use crate::sync::document_outbox::schedule_drain_effect;

/// What another writer commits between the two attempts.
const ELSEWHERE: &str = "changed elsewhere";

/// Lets `operation` read `first` and lose its commit, then read a newer document that another
/// writer changed; returns the document the retry wrote, which keeps that writer's change.
pub(super) fn conflict_then_commit<O: Operation>(
    operation: &mut O,
    actor: &Actor,
    first: &RealmConfigDocument,
) -> RealmConfigDocument {
    let other = iroh::SecretKey::from_bytes(&[0xEE; 32]).public();
    let mut newer = first.clone();
    newer.description = ELSEWHERE.to_string();
    newer.ensure_node(other, RealmNodeKind::Server);

    assert!(matches!(
        operation.start().as_slice(),
        [Effect::SubOperation(_)]
    ));
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
    ));
    let effects = read_in(operation, effects, 1, first.to_bytes(actor).unwrap());
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

    let effects = read_in(operation, effects, 2, newer.to_bytes(actor).unwrap());
    let writes = until_commit(operation, effects, 2);
    let (_, _, value) = writes
        .iter()
        .find(|(space, _, _)| space == REALM_CONFIG_KEYSPACE)
        .expect("the retry rewrites the realm config");
    let written = RealmConfigDocument::from_bytes(value).unwrap();
    assert_eq!(written.description, ELSEWHERE);
    assert_eq!(node_kind(&written, other), Some(RealmNodeKind::Server));

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
    written
}

/// Starts attempt `attempt`, answers its read with `document` and no reducer state, and returns
/// what the operation asks next.
fn read_in<O: Operation>(
    operation: &mut O,
    effects: Effects,
    attempt: u128,
    document: Vec<u8>,
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
    assert_eq!(reads.len(), 2);
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (reads[0].1.clone(), Some(document.into())),
            (reads[1].1.clone(), None),
        ],
    }))
}

/// Answers the writes of attempt `attempt` until it asks to commit, and returns those writes.
fn until_commit<O: Operation>(
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
