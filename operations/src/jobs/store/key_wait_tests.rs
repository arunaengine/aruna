//! Tests parking jobs on locked bucket keys, waking them and cancelling them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::jobs::store::test_calls::{claim_job, insert_job, mutate_job, park_job, read_job_record, read_key_waits, set_cancel_requested, wake_key_page, wake_key_waits};
use super::*;
use crate::jobs::store::attempt::*;
use crate::jobs::store::park_wake::{ParkJobOperation, SatisfyWaitOperation};
use crate::jobs::store::query::*;
use crate::jobs::store::row_calls::park_mutation;
use crate::jobs::store::state::*;
use crate::storage_read::iter_prefix_page;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_storage::{FjallStorage, StorageHandle};

fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    (dir, storage)
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn wait(bucket: &str, seed: u8) -> KeyWait {
    KeyWait {
        node_id: node(1),
        bucket: bucket.to_string(),
        group_id: Some(Ulid::from_bytes([seed; 16])),
        key: BucketKeyRef::new(Ulid::from_bytes([seed; 16]), 1),
    }
}

/// Inserts and claims a probe job, returning its claim token.
async fn claimed_job(context: &DriverContext, seed: u8) -> (JobId, Ulid) {
    let job_id = JobId::from_bytes([seed; 16]);
    let payload = JobPayload::Probe {
        steps: 1,
        step_sleep_ms: 0,
        fail_at: None,
        panic_at: None,
        cleanup_marker: None,
    };
    let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
    let record = JobRecord::new(job_id, payload, owner, node(7), 1_000, 1_000, None);
    insert_job(context, &record).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(context, job_id, node(3), 2_000).await.unwrap()
    else {
        panic!("job must be claimed")
    };
    (job_id, claimed.claim.unwrap().claim_token)
}

async fn rows(context: &DriverContext, key_space: &str) -> Vec<Key> {
    let (values, _) = iter_prefix_page(&context.storage_handle, key_space, None, None, 64, None)
        .await
        .unwrap();
    values.into_iter().map(|(key, _)| key).collect()
}

async fn park(context: &DriverContext, job_id: JobId, token: Ulid, waits: Vec<KeyWait>) {
    let outcome = park_job(context, job_id, token, 3_000, waits)
        .await
        .unwrap();
    assert!(matches!(outcome, AwaitOutcome::Parked(_)));
}

#[tokio::test]
async fn park_keeps_attempts() {
    let (_dir, storage) = temp_storage();
    let context = crate::testing::test_context(storage.clone());
    let (job_id, token) = claimed_job(&context, 5).await;
    park(&context, job_id, token, vec![wait("a", 8), wait("b", 9)]).await;

    let record = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::AwaitingKey);
    assert_eq!(record.attempts, 0);
    assert_eq!(record.due_at_ms, 1_000);
    assert!(record.claim.is_none());
    assert!(record.last_error.is_none());
    assert!(rows(&context, SCHEDULE_INDEX_KEYSPACE).await.is_empty());
    assert_eq!(read_key_waits(&context, job_id).await.unwrap().len(), 2);
    // The old claim no longer mutates the parked job.
    let again = park_job(&context, job_id, token, 3_000, vec![wait("a", 8)]).await;
    assert!(matches!(again, Err(JobMutationError::TokenMismatch)));
}

#[tokio::test]
async fn waits_for_keys() {
    let (_dir, storage) = temp_storage();
    let context = crate::testing::test_context(storage.clone());
    let (job_id, token) = claimed_job(&context, 5).await;
    let (first, second) = (wait("a", 8), wait("b", 9));
    park(&context, job_id, token, vec![first.clone(), second.clone()]).await;

    assert_eq!(wake_key_waits(&context, first.key, 4_000).await.unwrap(), 0);
    let record = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::AwaitingKey);
    assert_eq!(
        read_key_waits(&context, job_id).await.unwrap(),
        vec![second.clone()]
    );

    assert_eq!(
        wake_key_waits(&context, second.key, 5_000).await.unwrap(),
        1
    );
    let record = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Queued);
    assert_eq!(record.due_at_ms, 5_000);
    assert_eq!(record.attempts, 0);
    assert!(rows(&context, KEY_WAIT_KEYSPACE).await.is_empty());
    assert_eq!(rows(&context, SCHEDULE_INDEX_KEYSPACE).await.len(), 1);
}

#[tokio::test]
async fn wake_pages_bounded() {
    let (_dir, storage) = temp_storage();
    let context = crate::testing::test_context(storage.clone());
    let shared = wait("a", 8);
    let count = WAKE_PAGE + 3;
    for seed in 0..count {
        let (job_id, token) = claimed_job(&context, 100 + seed as u8).await;
        park(&context, job_id, token, vec![shared.clone()]).await;
    }
    let (woken, more) = wake_key_page(&context, shared.key, 4_000).await.unwrap();
    assert_eq!((woken, more), (WAKE_PAGE, true));
    assert_eq!(
        wake_key_waits(&context, shared.key, 4_000).await.unwrap(),
        3
    );
    assert!(rows(&context, KEY_WAIT_KEYSPACE).await.is_empty());
}

#[tokio::test]
async fn cancel_parked_job() {
    let (_dir, storage) = temp_storage();
    let context = crate::testing::test_context(storage.clone());
    let (job_id, token) = claimed_job(&context, 5).await;
    let key = wait("a", 8);
    park(&context, job_id, token, vec![key.clone()]).await;

    let outcome = set_cancel_requested(&context, job_id, 4_000).await.unwrap();
    let CancelRequestOutcome::Cancelled(record) = outcome else {
        panic!("a parked job that never ran cancels at once")
    };
    assert_eq!(record.state, JobState::Cancelled);
    assert!(read_key_waits(&context, job_id).await.unwrap().is_empty());
    // The leftover wake row is dropped without reviving the job.
    assert_eq!(wake_key_waits(&context, key.key, 5_000).await.unwrap(), 0);
    assert!(rows(&context, KEY_WAIT_KEYSPACE).await.is_empty());
    let record = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Cancelled);
}

#[tokio::test]
async fn cancel_requeues_ran() {
    let (_dir, storage) = temp_storage();
    let context = crate::testing::test_context(storage.clone());
    let (job_id, token) = claimed_job(&context, 5).await;
    mutate_job(&context, job_id, |record| {
        record.has_run = true;
        Ok(JobMutation::Persist)
    })
    .await
    .unwrap();
    park(&context, job_id, token, vec![wait("a", 8)]).await;

    set_cancel_requested(&context, job_id, 4_000).await.unwrap();
    let record = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    // Cleanup of the earlier run goes through the normal claimed cancel path.
    assert_eq!(record.state, JobState::Queued);
    assert!(record.cancel_requested);
    assert_eq!(record.due_at_ms, 4_000);
}

#[tokio::test]
async fn cancellation_beats_parking() {
    let (_dir, storage) = temp_storage();
    let context = crate::testing::test_context(storage.clone());
    let (job_id, token) = claimed_job(&context, 5).await;
    set_cancel_requested(&context, job_id, 2_500).await.unwrap();
    let outcome = park_job(&context, job_id, token, 3_000, vec![wait("a", 8)]).await;
    assert!(matches!(outcome, Ok(AwaitOutcome::Skipped)));
    assert!(rows(&context, KEY_WAIT_KEYSPACE).await.is_empty());
}

/// A claimed probe job in an in-memory store, with its claim token.
fn claimed_rows(seed: u8) -> (crate::jobs::store::mutation::Rows, JobId, Ulid) {
    let job_id = JobId::from_bytes([seed; 16]);
    let payload = JobPayload::Probe {
        steps: 1,
        step_sleep_ms: 0,
        fail_at: None,
        panic_at: None,
        cleanup_marker: None,
    };
    let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
    let mut record = JobRecord::new(job_id, payload, owner, node(7), 1_000, 1_000, None);
    let token = Ulid::from_bytes([9; 16]);
    record.state = JobState::Claimed;
    record.claim = Some(JobClaim {
        holder_node_id: node(3),
        claim_token: token,
        lease_expires_ms: 60_000,
    });
    (
        crate::jobs::store::mutation::seeded_rows(&record),
        job_id,
        token,
    )
}

// Parking writes the wait rows with the parked job; the last satisfied key wakes it again.
#[test]
fn parks_and_wakes_rows() {
    use crate::jobs::store::mutation::run_in_memory;
    let (mut rows, job_id, token) = claimed_rows(11);
    let first = wait("a", 8);
    let second = wait("b", 9);
    let waits = vec![first.clone(), second.clone()];
    let park = ParkJobOperation::new(job_id, park_mutation(token, 3_000), waits);
    let Ok(AwaitOutcome::Parked(parked)) = run_in_memory(park, &mut rows) else {
        panic!("a claimed job parks");
    };
    assert_eq!(parked.state, JobState::AwaitingKey);
    let list = (KEY_WAIT_KEYSPACE.to_string(), job_wait_key(job_id).to_vec());
    assert!(rows.contains_key(&list));

    let one = SatisfyWaitOperation::new(job_id, first.key, 4_000);
    assert_eq!(run_in_memory(one, &mut rows), Ok(false));
    let last = SatisfyWaitOperation::new(job_id, second.key, 5_000);
    assert_eq!(run_in_memory(last, &mut rows), Ok(true));
    assert!(!rows.contains_key(&list));
    let job = (JOB_KEYSPACE.to_string(), job_record_key(job_id).to_vec());
    let woken = JobRecord::from_bytes(&rows[&job]).expect("job row");
    assert_eq!((woken.state, woken.due_at_ms), (JobState::Queued, 5_000));
}

// A foreign token parks nothing, and a satisfied key of a job without a list wakes nothing.
#[test]
fn refuses_foreign_park() {
    use crate::jobs::store::mutation::run_in_memory;
    let (mut rows, job_id, _) = claimed_rows(12);
    let before = rows.clone();
    let foreign = ParkJobOperation::new(
        job_id,
        park_mutation(Ulid::from_bytes([1; 16]), 3_000),
        vec![wait("a", 8)],
    );
    assert!(matches!(
        run_in_memory(foreign, &mut rows),
        Err(JobMutationError::TokenMismatch)
    ));
    assert_eq!(rows, before);
    let stray = SatisfyWaitOperation::new(job_id, wait("a", 8).key, 4_000);
    assert_eq!(run_in_memory(stray, &mut rows), Ok(false));
}
