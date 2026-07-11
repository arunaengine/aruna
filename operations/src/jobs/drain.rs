use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::keyspaces::JOB_SCHEDULE_INDEX_KEYSPACE;
use aruna_core::structs::{
    JOB_DUE_INDEX_PREFIX, JOB_LEASE_INDEX_PREFIX, JobError, JobId, JobRecord, job_due_index_key,
    job_lease_index_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, NodeId};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;

use super::JOB_DRAIN_BATCH_SIZE;
use super::store::{
    ClaimOutcome, JobMutationError, batch_delete, claim_job, first_schedule_entry,
    iter_prefix_page, requeue_job,
};

#[derive(Debug, Default)]
pub struct JobDrainResult {
    pub claimed: Vec<JobRecord>,
    pub cancelled_fresh: usize,
    pub swept: usize,
    pub next_due_after: Option<Duration>,
}

/// Claim up to `capacity` due jobs and re-queue expired leases; claimed records are returned.
pub async fn process_job_queue_batch(
    storage: &StorageHandle,
    holder_node_id: NodeId,
    capacity: usize,
) -> Result<JobDrainResult, String> {
    let now_ms = unix_timestamp_millis();
    let mut result = JobDrainResult::default();

    let (due_ready, _) = scan_ready(storage, JOB_DUE_INDEX_PREFIX, now_ms, capacity).await?;
    for (ts, job_id) in due_ready {
        match claim_job(storage, job_id, holder_node_id, now_ms).await {
            Ok(ClaimOutcome::Claimed(record)) => result.claimed.push(record),
            Ok(ClaimOutcome::CancelledFresh(_)) => {
                result.cancelled_fresh = result.cancelled_fresh.saturating_add(1)
            }
            Ok(ClaimOutcome::NotEligible) => {}
            // Orphaned index row (record gone/quarantined): drop it so it cannot
            // pin the drain timer at zero forever.
            Err(JobMutationError::NotFound) => {
                delete_schedule_row(storage, job_due_index_key(ts, job_id)).await?
            }
            Err(error) => return Err(error.to_string()),
        }
    }

    let (expired_leases, _) = scan_ready(
        storage,
        JOB_LEASE_INDEX_PREFIX,
        now_ms,
        JOB_DRAIN_BATCH_SIZE,
    )
    .await?;
    for (ts, job_id) in expired_leases {
        match requeue_job(
            storage,
            job_id,
            None,
            now_ms,
            Some(now_ms),
            Some(JobError::retryable("lease expired")),
        )
        .await
        {
            Ok(_) => result.swept = result.swept.saturating_add(1),
            Err(JobMutationError::NotFound) => {
                delete_schedule_row(storage, job_lease_index_key(ts, job_id)).await?
            }
            Err(error) => return Err(error.to_string()),
        }
    }

    result.next_due_after = next_job_drain_timer_after(storage).await?;
    Ok(result)
}

async fn delete_schedule_row(storage: &StorageHandle, key: Key) -> Result<(), String> {
    batch_delete(
        storage,
        vec![(JOB_SCHEDULE_INDEX_KEYSPACE.to_string(), key)],
        None,
    )
    .await
}

/// Earliest `due/`/`lease/` head as a delay from now; `ZERO` when work is due.
pub async fn next_job_drain_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, String> {
    let now_ms = unix_timestamp_millis();
    let due = first_schedule_entry(storage, JOB_DUE_INDEX_PREFIX).await?;
    let lease = first_schedule_entry(storage, JOB_LEASE_INDEX_PREFIX).await?;
    let next = match (due, lease) {
        (Some((due_ts, _)), Some((lease_ts, _))) => Some(due_ts.min(lease_ts)),
        (Some((due_ts, _)), None) => Some(due_ts),
        (None, Some((lease_ts, _))) => Some(lease_ts),
        (None, None) => None,
    };
    Ok(next.map(|ts| Duration::from_millis(ts.saturating_sub(now_ms))))
}

/// ShortenTimer restore (startup + re-arm loop); never pushes a deadline later.
pub async fn restore_job_queue_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let after = match next_job_drain_timer_after(storage).await {
        Ok(Some(after)) => after,
        Ok(None) => return,
        Err(error) => {
            warn!(error = %error, "Failed to scan job schedule index");
            return;
        }
    };
    let event = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainJobQueue,
            after,
        }))
        .await;
    if let Event::Task(TaskEvent::Error { message, .. }) = event {
        warn!(message = %message, "Failed to restore job drain timer");
    }
}

async fn scan_ready(
    storage: &StorageHandle,
    prefix: &[u8],
    now_ms: u64,
    limit: usize,
) -> Result<(Vec<(u64, JobId)>, Option<u64>), String> {
    if limit == 0 {
        return Ok((Vec::new(), None));
    }
    let (values, _) = iter_prefix_page(
        storage,
        JOB_SCHEDULE_INDEX_KEYSPACE,
        Some(ByteView::from(prefix.to_vec())),
        None,
        limit,
        None,
    )
    .await?;

    let mut ready = Vec::new();
    let mut next_at = None;
    for (key, _) in values {
        match aruna_core::structs::parse_job_schedule_index_key(key.as_ref()) {
            Ok((ts, job_id)) => {
                if ts <= now_ms {
                    ready.push((ts, job_id));
                } else {
                    next_at = Some(ts);
                    break;
                }
            }
            Err(error) => {
                warn!(error = %error, "Deleting malformed job schedule index row during scan");
                delete_schedule_row(storage, key).await?;
            }
        }
    }
    Ok((ready, next_at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JOB_LEASE_MS;
    use crate::jobs::store::{insert_job, read_job_record};
    use aruna_core::structs::{JobClaim, JobPayload, JobState, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn queued_record(job_id: JobId, due_at_ms: u64) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            due_at_ms,
            due_at_ms,
            None,
        )
    }

    #[tokio::test]
    async fn claims_up_to_capacity() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        for seq in 1..=3u128 {
            insert_job(
                &storage,
                &queued_record(JobId(Ulid::from_parts(seq as u64, 0)), 1),
            )
            .await
            .unwrap();
        }

        let result = process_job_queue_batch(&storage, node_id(3), 2)
            .await
            .unwrap();
        assert_eq!(result.claimed.len(), 2, "capacity caps claims");
        // A due job remains, so the re-arm is immediate.
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
    }

    #[tokio::test]
    async fn expired_lease_requeued() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let job_id = JobId::from_bytes([4u8; 16]);
        let mut record = queued_record(job_id, 1);
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::new(),
            lease_expires_at_ms: 1,
        });
        insert_job(&storage, &record).await.unwrap();

        let result = process_job_queue_batch(&storage, node_id(3), 8)
            .await
            .unwrap();
        assert_eq!(result.swept, 1);
        let requeued = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(requeued.state, JobState::Queued);
        assert_eq!(requeued.attempts, 1);
        assert!(requeued.claim.is_none());
    }

    #[tokio::test]
    async fn future_lease_kept() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut record = queued_record(job_id, 1);
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::new(),
            lease_expires_at_ms: unix_timestamp_millis() + JOB_LEASE_MS,
        });
        insert_job(&storage, &record).await.unwrap();

        let result = process_job_queue_batch(&storage, node_id(3), 8)
            .await
            .unwrap();
        assert_eq!(result.swept, 0);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn restore_shortens_timer() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let due_far_out = unix_timestamp_millis() + 7_200_000;
        insert_job(
            &storage,
            &queued_record(JobId::from_bytes([1u8; 16]), due_far_out),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainJobQueue,
                after: Duration::from_secs(3600),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };

        restore_job_queue_timer(&storage, &task_handle).await;

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainJobQueue,
                after: Duration::from_secs(10_000),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };
        assert!(
            after <= Duration::from_secs(3600),
            "restore must ShortenTimer, not push the deadline out"
        );
    }

    #[tokio::test]
    async fn orphan_row_heals() {
        use aruna_core::effects::StorageEffect;
        use aruna_core::events::StorageEvent;

        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let orphan = JobId::from_bytes([9u8; 16]);
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: JOB_SCHEDULE_INDEX_KEYSPACE.to_string(),
                key: job_due_index_key(1, orphan),
                value: ByteView::from(Vec::new()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let result = process_job_queue_batch(&storage, node_id(3), 8)
            .await
            .unwrap();
        assert!(result.claimed.is_empty());
        // Self-healed: the orphan is gone and the drain timer stops returning zero.
        assert_eq!(next_job_drain_timer_after(&storage).await.unwrap(), None);
        let (rows, _) =
            iter_prefix_page(&storage, JOB_SCHEDULE_INDEX_KEYSPACE, None, None, 8, None)
                .await
                .unwrap();
        assert!(rows.is_empty());
    }
}
