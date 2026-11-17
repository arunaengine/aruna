//! Tests the purge fence check, acquisition and terminal release as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoError;
use aruna_core::errors::StorageError;

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

fn row(value: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![value.map(Into::into)])
}

fn read() -> Next<()> {
    Next::Call(IoCall::Read(vec![(
        PURGE_FENCE_KEYSPACE.to_string(),
        fence_key("bucket"),
    )]))
}

#[test]
fn suspends_covered_keys() {
    let mut covered = FenceCheckOperation::new("bucket", "logs/a");
    assert_eq!(covered.first(), Ok(read()));
    let held = row(Some(fence(job(1), scope())));
    assert_eq!(covered.next(held), Err(PurgeFenceError::Suspended));
    let mut free = FenceCheckOperation::new("bucket", "data/a");
    let held = row(Some(fence(job(1), scope())));
    assert_eq!(free.next(held), Ok(Next::Done(())));
    let mut unfenced = FenceCheckOperation::new("bucket", "logs/a");
    assert_eq!(unfenced.next(row(None)), Ok(Next::Done(())));
    assert_eq!(
        unfenced.next(row(Some(vec![0xff]))),
        Err(PurgeFenceError::Invalid)
    );
}

#[test]
fn acquires_free_fence() {
    let mut operation = AcquireFenceOperation::new(job(1), scope());
    assert_eq!(operation.first(), Ok(read()));
    assert!(operation.transaction());
    let write = IoCall::Write(vec![(
        PURGE_FENCE_KEYSPACE.to_string(),
        fence_key("bucket"),
        fence(job(1), scope()).into(),
    )]);
    assert_eq!(operation.next(row(None)), Ok(Next::Call(write)));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn existing_fence_decides() {
    // The holder's own fence is success, another job's is busy, another scope is invalid.
    let wider = StoragePurgeScope::Bucket {
        bucket: "bucket".to_string(),
    };
    let cases = [
        (fence(job(1), scope()), Ok(Next::Done(()))),
        (fence(job(2), scope()), Err(PurgeFenceError::Busy)),
        (fence(job(1), wider), Err(PurgeFenceError::Invalid)),
        (vec![0xff], Err(PurgeFenceError::Invalid)),
    ];
    for (stored, expected) in cases {
        let mut operation = AcquireFenceOperation::new(job(1), scope());
        assert_eq!(operation.next(row(Some(stored))), expected);
    }
}

#[test]
fn conflict_retried_outside() {
    // A commit conflict ends the attempt; the runner retries it with a fresh operation.
    let error = PurgeFenceError::from(IoError::Conflict);
    assert!(AcquireFenceOperation::is_conflict(&error));
    assert!(!AcquireFenceOperation::is_conflict(&PurgeFenceError::Busy));
    let storage = PurgeFenceError::from(IoError::Storage(StorageError::Timeout));
    assert_eq!(storage, PurgeFenceError::Storage(StorageError::Timeout));
}

#[test]
fn owned_terminal_delete() {
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
