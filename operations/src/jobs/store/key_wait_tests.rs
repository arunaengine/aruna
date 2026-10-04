//! Tests parking jobs on locked bucket keys, waking them and cancelling them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::FjallStorage;

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
async fn claimed_job(storage: &StorageHandle, seed: u8) -> (JobId, Ulid) {
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
    insert_job(storage, &record).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(storage, job_id, node(3), 2_000).await.unwrap()
    else {
        panic!("job must be claimed")
    };
    (job_id, claimed.claim.unwrap().claim_token)
}

async fn rows(storage: &StorageHandle, key_space: &str) -> Vec<Key> {
    let (values, _) = iter_prefix_page(storage, key_space, None, None, 64, None)
        .await
        .unwrap();
    values.into_iter().map(|(key, _)| key).collect()
}

async fn park(storage: &StorageHandle, job_id: JobId, token: Ulid, waits: Vec<KeyWait>) {
    let outcome = park_job(storage, job_id, token, 3_000, waits)
        .await
        .unwrap();
    assert!(matches!(outcome, AwaitOutcome::Parked(_)));
}

#[tokio::test]
async fn park_keeps_attempts() {
    let (_dir, storage) = temp_storage();
    let (job_id, token) = claimed_job(&storage, 5).await;
    park(&storage, job_id, token, vec![wait("a", 8), wait("b", 9)]).await;

    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::AwaitingKey);
    assert_eq!(record.attempts, 0);
    assert_eq!(record.due_at_ms, 1_000);
    assert!(record.claim.is_none());
    assert!(record.last_error.is_none());
    assert!(rows(&storage, SCHEDULE_INDEX_KEYSPACE).await.is_empty());
    assert_eq!(read_key_waits(&storage, job_id).await.unwrap().len(), 2);
    // The old claim no longer mutates the parked job.
    let again = park_job(&storage, job_id, token, 3_000, vec![wait("a", 8)]).await;
    assert!(matches!(again, Err(JobMutationError::TokenMismatch)));
}

#[tokio::test]
async fn wakes_after_all_keys() {
    let (_dir, storage) = temp_storage();
    let (job_id, token) = claimed_job(&storage, 5).await;
    let (first, second) = (wait("a", 8), wait("b", 9));
    park(&storage, job_id, token, vec![first.clone(), second.clone()]).await;

    assert_eq!(wake_key_waits(&storage, first.key, 4_000).await.unwrap(), 0);
    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::AwaitingKey);
    assert_eq!(
        read_key_waits(&storage, job_id).await.unwrap(),
        vec![second.clone()]
    );

    assert_eq!(
        wake_key_waits(&storage, second.key, 5_000).await.unwrap(),
        1
    );
    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Queued);
    assert_eq!(record.due_at_ms, 5_000);
    assert_eq!(record.attempts, 0);
    assert!(rows(&storage, JOB_KEY_WAIT_KEYSPACE).await.is_empty());
    assert_eq!(rows(&storage, SCHEDULE_INDEX_KEYSPACE).await.len(), 1);
}

#[tokio::test]
async fn wake_pages_bounded() {
    let (_dir, storage) = temp_storage();
    let shared = wait("a", 8);
    let count = WAKE_PAGE + 3;
    for seed in 0..count {
        let (job_id, token) = claimed_job(&storage, 100 + seed as u8).await;
        park(&storage, job_id, token, vec![shared.clone()]).await;
    }
    let (woken, more) = wake_key_page(&storage, shared.key, 4_000).await.unwrap();
    assert_eq!((woken, more), (WAKE_PAGE, true));
    assert_eq!(
        wake_key_waits(&storage, shared.key, 4_000).await.unwrap(),
        3
    );
    assert!(rows(&storage, JOB_KEY_WAIT_KEYSPACE).await.is_empty());
}

#[tokio::test]
async fn cancel_parked_job() {
    let (_dir, storage) = temp_storage();
    let (job_id, token) = claimed_job(&storage, 5).await;
    let key = wait("a", 8);
    park(&storage, job_id, token, vec![key.clone()]).await;

    let outcome = set_cancel_requested(&storage, job_id, 4_000).await.unwrap();
    let CancelRequestOutcome::Cancelled(record) = outcome else {
        panic!("a parked job that never ran cancels at once")
    };
    assert_eq!(record.state, JobState::Cancelled);
    assert!(read_key_waits(&storage, job_id).await.unwrap().is_empty());
    // The leftover wake row is dropped without reviving the job.
    assert_eq!(wake_key_waits(&storage, key.key, 5_000).await.unwrap(), 0);
    assert!(rows(&storage, JOB_KEY_WAIT_KEYSPACE).await.is_empty());
    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Cancelled);
}

#[tokio::test]
async fn cancel_requeues_ran() {
    let (_dir, storage) = temp_storage();
    let (job_id, token) = claimed_job(&storage, 5).await;
    mutate_job(&storage, job_id, |record| {
        record.has_run = true;
        Ok(JobMutation::Persist)
    })
    .await
    .unwrap();
    park(&storage, job_id, token, vec![wait("a", 8)]).await;

    set_cancel_requested(&storage, job_id, 4_000).await.unwrap();
    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    // Cleanup of the earlier run goes through the normal claimed cancel path.
    assert_eq!(record.state, JobState::Queued);
    assert!(record.cancel_requested);
    assert_eq!(record.due_at_ms, 4_000);
}

#[tokio::test]
async fn cancel_wins_over_park() {
    let (_dir, storage) = temp_storage();
    let (job_id, token) = claimed_job(&storage, 5).await;
    set_cancel_requested(&storage, job_id, 2_500).await.unwrap();
    let outcome = park_job(&storage, job_id, token, 3_000, vec![wait("a", 8)]).await;
    assert!(matches!(outcome, Ok(AwaitOutcome::Skipped)));
    assert!(rows(&storage, JOB_KEY_WAIT_KEYSPACE).await.is_empty());
}
