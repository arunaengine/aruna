//! State and event tests of the remote key wait rows, plus one run against real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::operation_check::{check_abort, rejects_events, rejects_finalize, sample_events};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::{Key, Value};
use aruna_core::{NodeId, structs::execution::job::JobId};
use ulid::Ulid;

use super::{OwedWake, OwedWakesOperation, QueueWakesOperation, WaitRowOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::key_wait::WAKE_PAGE;

fn waiter() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn key(generation: u64) -> BucketKeyRef {
    BucketKeyRef::new(Ulid::from_bytes([8; 16]), generation)
}

fn job() -> JobId {
    JobId::from_bytes([5; 16])
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: aruna_core::UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
        realm_id: RealmId([1; 32]),
        path_restrictions: None,
        session: None,
    }
}

fn failed(error: StorageError) -> Event {
    Event::Storage(StorageEvent::Error { error })
}

fn txn() -> Ulid {
    Ulid::from_bytes([3; 16])
}

#[tokio::test]
async fn wake_until_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let context = DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    for generation in [1, 2] {
        let register = WaitRowOperation::register(key(generation), waiter(), job(), &auth());
        drive(register, &context).await.unwrap();
    }

    assert!(
        !drive(QueueWakesOperation::new(key(1)), &context)
            .await
            .unwrap()
    );
    let owed = OwedWake {
        waiter: waiter(),
        job_id: job(),
        key: key(1),
        auth: auth(),
    };
    let page = drive(OwedWakesOperation::new(None), &context)
        .await
        .unwrap();
    assert_eq!(page, (vec![owed], None));
    // A repeated unlock finds nothing left to queue for this key.
    assert!(
        !drive(QueueWakesOperation::new(key(1)), &context)
            .await
            .unwrap()
    );
    let page = drive(OwedWakesOperation::new(None), &context)
        .await
        .unwrap();
    assert_eq!(page.0.len(), 1);

    let acknowledge = WaitRowOperation::acknowledge(waiter(), job(), key(1));
    drive(acknowledge, &context).await.unwrap();
    let page = drive(OwedWakesOperation::new(None), &context)
        .await
        .unwrap();
    assert!(page.0.is_empty());
}

#[test]
fn row_reports_failure() {
    let mut row = WaitRowOperation::acknowledge(waiter(), job(), key(1));
    assert!(matches!(
        row.start().as_slice(),
        [Effect::Storage(StorageEffect::BatchDelete { .. })]
    ));
    assert!(row.step(failed(StorageError::Timeout)).is_empty());
    assert!(matches!(row.finalize(), Err(JobMutationError::Storage(_))));
}

#[test]
fn queue_commits_page() {
    let mut queue = QueueWakesOperation::new(key(1));
    assert!(matches!(
        queue.start().as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
    let effects = queue.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { limit, txn_id: Some(_), .. })] if *limit == WAKE_PAGE
    ));
    let mut row = vec![b'r'];
    row.extend_from_slice(&key(1).key());
    row.extend_from_slice(waiter().as_bytes());
    row.extend_from_slice(&job().to_bytes());
    let value = Value::from(postcard::to_allocvec(&auth()).unwrap().as_slice());
    let effects = queue.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(Key::from(row.as_slice()), value)],
        next_start_after: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite {
            txn_id: Some(_),
            ..
        })]
    ));
    let effects = queue.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchDelete {
            txn_id: Some(_),
            ..
        })]
    ));
    let effects = queue.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    }));
    assert_eq!(
        effects.as_slice(),
        &[Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: txn()
        })]
    );
    let committed = Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() });
    assert!(queue.step(committed).is_empty());
    assert_eq!(queue.finalize().ok(), Some(false));
}

#[test]
fn queue_names_conflict() {
    // A lost commit is a conflict the runner retries; a failed page aborts its transaction.
    let mut queue = QueueWakesOperation::new(key(1));
    queue.start();
    queue.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));
    let empty = Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    });
    queue.step(empty);
    queue.step(failed(StorageError::TransactionConflict));
    let error = queue.finalize().unwrap_err();
    assert!(QueueWakesOperation::is_conflict(&error));

    let mut queue = QueueWakesOperation::new(key(1));
    queue.start();
    queue.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: txn(),
    }));
    let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
    check_abort(&mut queue, std::slice::from_ref(&abort));
    assert_eq!(
        queue.step(failed(StorageError::Timeout)).as_slice(),
        &[abort]
    );
    assert!(!queue.is_complete());
    queue.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: txn(),
    }));
    let error = queue.finalize().unwrap_err();
    assert!(!QueueWakesOperation::is_conflict(&error));
}

#[test]
fn owed_names_cursor() {
    let rows: Vec<(Key, Value)> = (0..WAKE_PAGE as u8)
        .map(|seed| (Key::from(vec![b'd', seed]), Value::from(&[0u8; 0][..])))
        .collect();
    let mut owed = OwedWakesOperation::new(None);
    owed.start();
    owed.step(Event::Storage(StorageEvent::IterResult {
        values: rows.clone(),
        next_start_after: None,
    }));
    let (wakes, next) = owed.finalize().unwrap();
    // Undecodable rows are skipped, yet a full page still names the next cursor.
    assert!(wakes.is_empty());
    assert_eq!(next, rows.last().map(|(row, _)| row.clone()));
}

#[test]
fn rejects_unfit_events() {
    let row = || {
        let mut row = WaitRowOperation::acknowledge(waiter(), job(), key(1));
        row.start();
        row
    };
    let fits = |event: &Event| {
        matches!(
            event,
            Event::Storage(
                StorageEvent::BatchWriteResult { .. }
                    | StorageEvent::BatchDeleteResult { .. }
                    | StorageEvent::Error { .. }
            )
        )
    };
    assert!(!rejects_events(row, sample_events, fits).is_empty());
    let owed = || {
        let mut owed = OwedWakesOperation::new(None);
        owed.start();
        owed
    };
    let fits = |event: &Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
        )
    };
    assert!(!rejects_events(owed, sample_events, fits).is_empty());
    let queue = || {
        let mut queue = QueueWakesOperation::new(key(1));
        queue.start();
        queue
    };
    let fits = |event: &Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::TransactionStarted { .. } | StorageEvent::Error { .. })
        )
    };
    assert!(!rejects_events(queue, sample_events, fits).is_empty());
}

#[test]
fn aborts_and_finalizes() {
    check_abort(
        &mut WaitRowOperation::acknowledge(waiter(), job(), key(1)),
        &[],
    );
    check_abort(&mut OwedWakesOperation::new(None), &[]);
    check_abort(&mut QueueWakesOperation::new(key(1)), &[]);
    rejects_finalize(WaitRowOperation::acknowledge(waiter(), job(), key(1)));
    rejects_finalize(OwedWakesOperation::new(None));
    rejects_finalize(QueueWakesOperation::new(key(1)));
}
