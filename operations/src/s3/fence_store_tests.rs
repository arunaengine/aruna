//! Tests the purge fence check, acquisition and terminal release against fixed storage events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use ulid::Ulid;

use super::*;

fn job(seed: u8) -> JobId {
    JobId::from_bytes([seed; 16])
}

fn scope() -> StoragePurgeScope {
    StoragePurgeScope::Prefix {
        bucket: "bucket".to_string(),
        prefix: "logs/".to_string(),
    }
}

fn fence(job_id: JobId, scope: StoragePurgeScope) -> Vec<u8> {
    StoragePurgeFence { job_id, scope }.to_bytes().unwrap()
}

fn txn() -> TxnId {
    Ulid::from_parts(5, 5)
}

fn started() -> Event {
    Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
}

fn read(value: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: fence_key("bucket"),
        value: value.map(Into::into),
    })
}

fn written() -> Event {
    Event::Storage(StorageEvent::WriteResult {
        key: fence_key("bucket"),
    })
}

fn failed(error: StorageError) -> Event {
    Event::Storage(StorageEvent::Error { error })
}

fn begin() -> Effect {
    Effect::Storage(StorageEffect::StartTransaction { read: false })
}

fn abort() -> Effect {
    Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() })
}

fn acquire_after(events: Vec<Event>) -> AcquireFenceOperation {
    let mut operation = AcquireFenceOperation::new(job(1), scope());
    operation.start();
    for event in events {
        operation.step(event);
    }
    operation
}

#[test]
fn check_suspends_covered_keys() {
    let mut covered = FenceCheckOperation::new("bucket", "logs/a");
    assert_eq!(
        covered.start().as_slice(),
        [write_fence_read("bucket", None)]
    );
    assert!(covered.step(read(Some(fence(job(1), scope())))).is_empty());
    assert_eq!(covered.finalize(), Err(PurgeFenceError::Suspended));
    let mut free = FenceCheckOperation::new("bucket", "data/a");
    free.start();
    free.step(read(Some(fence(job(1), scope()))));
    assert_eq!(free.finalize(), Ok(()));
    let mut unfenced = FenceCheckOperation::new("bucket", "logs/a");
    unfenced.start();
    unfenced.step(read(None));
    assert_eq!(unfenced.finalize(), Ok(()));
}

#[test]
fn check_errors_and_contract() {
    let mut storage = FenceCheckOperation::new("bucket", "k");
    storage.start();
    storage.step(failed(StorageError::Timeout));
    assert_eq!(
        storage.finalize(),
        Err(PurgeFenceError::Storage(StorageError::Timeout))
    );
    let make = || {
        let mut operation = FenceCheckOperation::new("bucket", "k");
        operation.start();
        operation
    };
    let refused = rejects_events(make, sample_events, |event| {
        matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
        )
    });
    assert!(!refused.is_empty());
    let mut fresh = FenceCheckOperation::new("bucket", "k");
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
}

#[test]
fn acquires_free_fence() {
    let mut operation = AcquireFenceOperation::new(job(1), scope());
    let write = Effect::Storage(StorageEffect::Write {
        key_space: PURGE_FENCE_KEYSPACE.to_string(),
        key: fence_key("bucket"),
        value: fence(job(1), scope()).into(),
        txn_id: Some(txn()),
    });
    let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id: txn() });
    let committed = Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() });
    let steps = vec![
        (started(), vec![write_fence_read("bucket", Some(txn()))]),
        (read(None), vec![write]),
        (written(), vec![commit]),
        (committed, vec![]),
    ];
    check_trace(&mut operation, &[begin()], steps);
    assert_eq!(operation.finalize(), Ok(()));
}

#[test]
fn existing_fence_decides() {
    // The holder's own fence is success, another job's is busy, another scope is invalid.
    let cases = [
        (fence(job(1), scope()), Ok(())),
        (fence(job(2), scope()), Err(PurgeFenceError::Busy)),
        (
            fence(
                job(1),
                StoragePurgeScope::Bucket {
                    bucket: "bucket".to_string(),
                },
            ),
            Err(PurgeFenceError::Invalid),
        ),
        (vec![0xff], Err(PurgeFenceError::Invalid)),
    ];
    for (row, expected) in cases {
        let mut operation = acquire_after(vec![started()]);
        assert_eq!(operation.step(read(Some(row))).as_slice(), [abort()]);
        assert_eq!(operation.finalize(), expected);
    }
}

#[test]
fn conflicts_retry_then_fail() {
    let mut operation = acquire_after(Vec::new());
    for attempt in 1..=FENCE_MUTATE_ATTEMPTS {
        operation.step(started());
        operation.step(read(None));
        operation.step(written());
        let effects = operation.step(failed(StorageError::TransactionConflict));
        if attempt < FENCE_MUTATE_ATTEMPTS {
            assert_eq!(effects.as_slice(), [begin()], "attempt {attempt}");
        } else {
            assert!(effects.is_empty());
        }
    }
    assert_eq!(
        operation.finalize(),
        Err(PurgeFenceError::Storage(StorageError::TransactionConflict))
    );
}

#[test]
fn acquire_errors_abort() {
    let mut starting = acquire_after(Vec::new());
    assert!(starting.step(failed(StorageError::Timeout)).is_empty());
    let mut reading = acquire_after(vec![started()]);
    assert_eq!(
        reading.step(failed(StorageError::Timeout)).as_slice(),
        [abort()]
    );
    let mut writing = acquire_after(vec![started(), read(None)]);
    assert_eq!(
        writing.step(failed(StorageError::Timeout)).as_slice(),
        [abort()]
    );
    let mut committing = acquire_after(vec![started(), read(None), written()]);
    assert!(committing.step(failed(StorageError::Timeout)).is_empty());
    for operation in [starting, reading, writing, committing] {
        assert_eq!(
            operation.finalize(),
            Err(PurgeFenceError::Storage(StorageError::Timeout))
        );
    }
}

#[test]
fn acquire_contract() {
    let phases: Vec<Vec<Event>> = vec![
        Vec::new(),
        vec![started()],
        vec![started(), read(None)],
        vec![started(), read(None), written()],
    ];
    for (index, phase) in phases.into_iter().enumerate() {
        let make = || acquire_after(phase.clone());
        let accepted = |event: &Event| match event {
            Event::Storage(StorageEvent::TransactionStarted { .. }) => index == 0,
            Event::Storage(StorageEvent::ReadResult { .. }) => index == 1,
            Event::Storage(StorageEvent::WriteResult { .. }) => index == 2,
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => index == 3,
            _ => false,
        };
        let refused = rejects_events(make, sample_events, accepted);
        assert_eq!(refused.len(), sample_events().len() - 1);
    }
    let mut fresh = AcquireFenceOperation::new(job(1), scope());
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
    let mut open = acquire_after(vec![started()]);
    check_abort(&mut open, &[abort()]);
    rejects_finalize(open);
}

#[test]
fn terminal_delete_only_when_owned() {
    let owned = fence(job(1), scope());
    assert_eq!(
        owned_fence_delete(Some(&owned), job(1), &scope()),
        Ok(Some((
            PURGE_FENCE_KEYSPACE.to_string(),
            fence_key("bucket")
        )))
    );
    let other = fence(job(2), scope());
    assert_eq!(owned_fence_delete(Some(&other), job(1), &scope()), Ok(None));
    assert_eq!(owned_fence_delete(None, job(1), &scope()), Ok(None));
    let wider = StoragePurgeScope::Bucket {
        bucket: "bucket".to_string(),
    };
    assert_eq!(
        owned_fence_delete(Some(&owned), job(1), &wider),
        Err(PurgeFenceError::Invalid)
    );
}
