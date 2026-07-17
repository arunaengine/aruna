use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::keyspaces::JOB_SCHEDULE_INDEX_KEYSPACE;
use aruna_core::structs::{
    JOB_DUE_INDEX_PREFIX, JOB_LEASE_INDEX_PREFIX, JobError, JobExecutionClass, JobId, JobRecord,
    job_lease_index_key, parse_job_schedule_index_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, NodeId};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;

use super::reconcile::ExternalReconciler;
use super::store::{
    ClaimOutcome, JobMutationError, RequeueOutcome, batch_delete, claim_job, first_schedule_entry,
    iter_prefix_page, read_job_record, requeue_job,
};
use super::{JOB_DRAIN_BATCH_SIZE, JOB_RECONCILE_REARM};

/// Per-class claim budget. Both classes share one due index, so a saturated class
/// must be skipped during the scan rather than claimed and released again.
#[derive(Clone, Copy, Debug, Default)]
pub struct JobClassBudget {
    pub in_process: usize,
    pub external: usize,
}

impl JobClassBudget {
    fn remaining(&self, class: JobExecutionClass) -> usize {
        match class {
            JobExecutionClass::InProcess => self.in_process,
            JobExecutionClass::ExternalAttempt => self.external,
        }
    }

    fn take(&mut self, class: JobExecutionClass) {
        match class {
            JobExecutionClass::InProcess => self.in_process = self.in_process.saturating_sub(1),
            JobExecutionClass::ExternalAttempt => self.external = self.external.saturating_sub(1),
        }
    }

    fn is_empty(&self) -> bool {
        self.in_process == 0 && self.external == 0
    }
}

#[derive(Debug, Default)]
pub struct JobDrainResult {
    pub claimed: Vec<JobRecord>,
    pub cancelled_fresh: usize,
    pub swept: usize,
    /// Expired external attempts routed to the reconcile hook instead of a requeue.
    pub reconciled: usize,
    pub next_due_after: Option<Duration>,
    /// A per-job error stopped the batch early; the caller should re-drive after a
    /// backoff. Set so `claimed` is never discarded by a later failure.
    pub retry_after_error: bool,
    /// A due job was left for a later pass because its execution class has no free
    /// slot. Re-arming at zero would busy-loop until a slot opens.
    pub deferred_saturated: bool,
}

/// Claim due jobs within each class's `budget` and re-queue expired leases; claimed
/// records are returned. An expired external attempt is routed to `reconciler`
/// instead of requeued.
pub async fn process_job_queue_batch(
    storage: &StorageHandle,
    holder_node_id: NodeId,
    budget: JobClassBudget,
    reconciler: Option<&Arc<dyn ExternalReconciler>>,
) -> Result<JobDrainResult, String> {
    let now_ms = unix_timestamp_millis();
    let mut result = JobDrainResult::default();

    claim_due_jobs(storage, holder_node_id, now_ms, budget, &mut result).await?;

    if !result.retry_after_error {
        let mut start_after = None;
        for _ in 0..2 {
            match scan_ready(
                storage,
                JOB_LEASE_INDEX_PREFIX,
                now_ms,
                JOB_DRAIN_BATCH_SIZE,
                start_after.take(),
            )
            .await
            {
                Ok((expired_leases, _, next)) => {
                    let expired_count = expired_leases.len();
                    let reconciled_before = result.reconciled;
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
                            Ok(RequeueOutcome::NeedsReconcile(record)) => {
                                if let Some(reconciler) = reconciler {
                                    reconciler.reconcile_lost_attempt(storage, record).await;
                                }
                                result.reconciled = result.reconciled.saturating_add(1);
                            }
                            Ok(_) => result.swept = result.swept.saturating_add(1),
                            Err(JobMutationError::NotFound) => {
                                if let Err(error) =
                                    delete_schedule_row(storage, job_lease_index_key(ts, job_id))
                                        .await
                                {
                                    warn!(error = %error, "Failed to drop orphaned job lease index row");
                                    result.retry_after_error = true;
                                    break;
                                }
                            }
                            Err(error) => {
                                warn!(job_id = %job_id, error = %error, "Failed to requeue expired lease");
                                result.retry_after_error = true;
                                break;
                            }
                        }
                    }
                    if result.retry_after_error
                        || expired_count != JOB_DRAIN_BATCH_SIZE
                        || result.reconciled.saturating_sub(reconciled_before) != expired_count
                    {
                        break;
                    }
                    let Some(next) = next else {
                        break;
                    };
                    start_after = Some(next);
                }
                Err(error) => {
                    warn!(error = %error, "Failed to scan expired job leases");
                    result.retry_after_error = true;
                    break;
                }
            }
        }
    }

    match next_drain_delays(storage).await {
        Ok((due, lease)) => {
            result.next_due_after = min_delay(due, lease);
        }
        Err(error) => {
            warn!(error = %error, "Failed to compute next job drain timer");
            result.retry_after_error = true;
        }
    }
    Ok(result)
}

/// Walk the due head, claiming each job against its own class budget. A job whose
/// class is saturated is skipped without a write: claiming it would only release it
/// again, churning storage while the drain re-arms at zero.
async fn claim_due_jobs(
    storage: &StorageHandle,
    holder_node_id: NodeId,
    now_ms: u64,
    mut budget: JobClassBudget,
    result: &mut JobDrainResult,
) -> Result<(), String> {
    if budget.is_empty() {
        result.deferred_saturated = true;
        return Ok(());
    }
    let mut start_after = None;
    'pages: loop {
        let (values, next) = match iter_prefix_page(
            storage,
            JOB_SCHEDULE_INDEX_KEYSPACE,
            Some(ByteView::from(JOB_DUE_INDEX_PREFIX.to_vec())),
            start_after.take(),
            JOB_DRAIN_BATCH_SIZE,
            None,
        )
        .await
        {
            Ok(page) => page,
            // Nothing to hand off yet, so a plain failure is enough.
            Err(error) if result.claimed.is_empty() => return Err(error),
            Err(error) => {
                warn!(error = %error, "Failed to page due jobs");
                result.retry_after_error = true;
                break 'pages;
            }
        };
        if values.is_empty() {
            break 'pages;
        }
        for (key, _) in values {
            let (ts, job_id) = match parse_job_schedule_index_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(error = %error, "Deleting malformed job due index row");
                    if let Err(error) = delete_schedule_row(storage, key).await {
                        warn!(error = %error, "Failed to drop malformed job due index row");
                        result.retry_after_error = true;
                        break 'pages;
                    }
                    continue;
                }
            };
            // The index is timestamp-ordered, so the first future row ends the scan.
            if ts > now_ms {
                break 'pages;
            }
            let class = match read_job_record(storage, job_id, None).await {
                Ok(Some(record)) => record.execution_class,
                // Orphaned index row (record gone/quarantined): drop it so it cannot
                // pin the drain timer at zero forever.
                Ok(None) => {
                    if let Err(error) = delete_schedule_row(storage, key).await {
                        warn!(error = %error, "Failed to drop orphaned job due index row");
                        result.retry_after_error = true;
                        break 'pages;
                    }
                    continue;
                }
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Failed to read due job");
                    result.retry_after_error = true;
                    break 'pages;
                }
            };
            if budget.remaining(class) == 0 {
                result.deferred_saturated = true;
                continue;
            }
            match claim_job(storage, job_id, holder_node_id, now_ms).await {
                Ok(ClaimOutcome::Claimed(record)) => {
                    budget.take(record.execution_class);
                    result.claimed.push(record);
                    if budget.is_empty() {
                        break 'pages;
                    }
                }
                Ok(ClaimOutcome::CancelledFresh(_)) => {
                    result.cancelled_fresh = result.cancelled_fresh.saturating_add(1)
                }
                Ok(ClaimOutcome::NotEligible) => {}
                Err(JobMutationError::NotFound) => {
                    if let Err(error) = delete_schedule_row(storage, key).await {
                        warn!(error = %error, "Failed to drop orphaned job due index row");
                        result.retry_after_error = true;
                        break 'pages;
                    }
                }
                // Never discard jobs already claimed in this batch: stop here, hand them off,
                // and let the caller re-drive the remainder after a backoff.
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Failed to claim due job");
                    result.retry_after_error = true;
                    break 'pages;
                }
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break 'pages,
        }
    }
    Ok(())
}

async fn delete_schedule_row(storage: &StorageHandle, key: Key) -> Result<(), String> {
    batch_delete(
        storage,
        vec![(JOB_SCHEDULE_INDEX_KEYSPACE.to_string(), key)],
        None,
    )
    .await
}

/// Earliest `due/` and `lease/` heads as delays from now; lease delays observe the
/// reconcile re-arm floor.
async fn next_drain_delays(
    storage: &StorageHandle,
) -> Result<(Option<Duration>, Option<Duration>), String> {
    let now_ms = unix_timestamp_millis();
    let delay = |ts: u64| Duration::from_millis(ts.saturating_sub(now_ms));
    let due = first_schedule_entry(storage, JOB_DUE_INDEX_PREFIX).await?;
    let lease = first_schedule_entry(storage, JOB_LEASE_INDEX_PREFIX).await?;
    // A reconciled attempt keeps its expired lease row in place by design, which
    // would otherwise pin the lease head at zero and busy-loop the drain; only an
    // already-due head needs the floor, a still-future one must still fire on time.
    Ok((
        due.map(|(ts, _)| delay(ts)),
        lease.map(|(ts, _)| {
            let raw = delay(ts);
            if raw.is_zero() {
                JOB_RECONCILE_REARM
            } else {
                raw
            }
        }),
    ))
}

fn min_delay(due: Option<Duration>, lease: Option<Duration>) -> Option<Duration> {
    match (due, lease) {
        (Some(due), Some(lease)) => Some(due.min(lease)),
        (due, None) => due,
        (None, lease) => lease,
    }
}

/// Earliest `due/`/`lease/` head as a delay from now, with the lease re-arm floor.
pub async fn next_job_drain_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, String> {
    let (due, lease) = next_drain_delays(storage).await?;
    Ok(min_delay(due, lease))
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
    start_after: Option<Key>,
) -> Result<(Vec<(u64, JobId)>, Option<u64>, Option<Key>), String> {
    if limit == 0 {
        return Ok((Vec::new(), None, None));
    }
    let (values, next) = iter_prefix_page(
        storage,
        JOB_SCHEDULE_INDEX_KEYSPACE,
        Some(ByteView::from(prefix.to_vec())),
        start_after,
        limit,
        None,
    )
    .await?;

    let mut ready = Vec::new();
    let mut next_at = None;
    for (key, _) in values {
        match parse_job_schedule_index_key(key.as_ref()) {
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
    Ok((ready, next_at, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JOB_LEASE_MS;
    use crate::jobs::store::insert_job;
    use aruna_core::structs::{
        AttemptIntent, JobClaim, JobPayload, JobState, RealmId, job_due_index_key,
    };
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[derive(Default)]
    struct RecordingReconciler {
        seen: Mutex<Vec<JobId>>,
    }

    #[async_trait::async_trait]
    impl ExternalReconciler for RecordingReconciler {
        async fn reconcile_lost_attempt(&self, _storage: &StorageHandle, record: JobRecord) {
            self.seen.lock().unwrap().push(record.job_id);
        }
    }

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn budget_of(slots: usize) -> JobClassBudget {
        JobClassBudget {
            in_process: slots,
            external: slots,
        }
    }

    fn queued_record(job_id: JobId, due_at_ms: u64) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
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

        let budget = JobClassBudget {
            in_process: 2,
            external: 0,
        };
        let result = process_job_queue_batch(&storage, node_id(3), budget, None)
            .await
            .unwrap();
        assert_eq!(result.claimed.len(), 2, "capacity caps claims");
        // A due job remains, so the re-arm is immediate.
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
    }

    #[tokio::test]
    async fn skips_saturated_class() {
        // External slots are gone while in-process slots are free: the shared due
        // index must not claim and release the external row it cannot run.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let external = JobId(Ulid::from_parts(1, 0));
        let mut record = queued_record(external, 1);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        insert_job(&storage, &record).await.unwrap();
        let internal = JobId(Ulid::from_parts(2, 0));
        insert_job(&storage, &queued_record(internal, 1))
            .await
            .unwrap();

        let budget = JobClassBudget {
            in_process: 4,
            external: 0,
        };
        let result = process_job_queue_batch(&storage, node_id(3), budget, None)
            .await
            .unwrap();

        assert_eq!(
            result
                .claimed
                .iter()
                .map(|record| record.job_id)
                .collect::<Vec<_>>(),
            vec![internal],
            "a saturated class must not starve the free one"
        );
        assert!(
            result.deferred_saturated,
            "the skipped row must be reported"
        );
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
        let stored = read_job_record(&storage, external, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued, "no claim/release churn");
        assert!(stored.claim.is_none());
        assert_eq!(stored.updated_at_ms, 1, "the record was never rewritten");
        assert_eq!(stored.due_at_ms, 1);
    }

    #[tokio::test]
    async fn empty_budget_defers() {
        // Both classes saturated with a due row present: the caller must be told to
        // back off instead of re-arming the drain at zero.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let job_id = JobId(Ulid::from_parts(1, 0));
        insert_job(&storage, &queued_record(job_id, 1))
            .await
            .unwrap();

        let result = process_job_queue_batch(&storage, node_id(3), JobClassBudget::default(), None)
            .await
            .unwrap();

        assert!(result.claimed.is_empty());
        assert!(result.deferred_saturated);
        assert_eq!(result.next_due_after, Some(Duration::ZERO));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Queued);
        assert_eq!(stored.updated_at_ms, 1);
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
            claim_token: Ulid::r#gen(),
            lease_expires_at_ms: 1,
        });
        insert_job(&storage, &record).await.unwrap();

        let result = process_job_queue_batch(&storage, node_id(3), budget_of(8), None)
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

    // An external attempt with an expired lease routes to the reconcile hook and is
    // NOT requeued: no second container can be spawned. Its lease row stays in place,
    // so the re-arm must be floored to a non-zero delay instead of busy-looping.
    #[tokio::test]
    async fn external_lease_reconciled() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let job_id = JobId::from_bytes([0xE1; 16]);
        let mut record = queued_record(job_id, 1);
        record.execution_class = JobExecutionClass::ExternalAttempt;
        record.state = JobState::Running;
        record.attempt_intent = Some(AttemptIntent {
            attempt_no: 1,
            external_name: "attempt".to_string(),
            executor_kind: "docker".to_string(),
            pinned_image: "alpine@sha256:digest".to_string(),
            attempt_epoch: 1,
        });
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::r#gen(),
            lease_expires_at_ms: 1,
        });
        insert_job(&storage, &record).await.unwrap();

        let recorder = Arc::new(RecordingReconciler::default());
        let reconciler: Arc<dyn ExternalReconciler> = recorder.clone();
        let result = process_job_queue_batch(&storage, node_id(3), budget_of(8), Some(&reconciler))
            .await
            .unwrap();
        assert_eq!(result.swept, 0, "external attempt is not swept");
        assert_eq!(result.reconciled, 1, "external attempt is reconciled");
        let after = result.next_due_after.expect("lease row still present");
        assert!(
            after >= JOB_RECONCILE_REARM,
            "reconciled row must not re-arm the drain at zero, got {after:?}"
        );

        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Running, "not requeued");
        assert_eq!(stored.attempts, 0);
        assert!(stored.claim.is_some());
        // The hook saw exactly this job, proving no blind re-run path was taken.
        assert_eq!(recorder.seen.lock().unwrap().as_slice(), &[job_id]);
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
            claim_token: Ulid::r#gen(),
            lease_expires_at_ms: unix_timestamp_millis() + JOB_LEASE_MS,
        });
        insert_job(&storage, &record).await.unwrap();

        let result = process_job_queue_batch(&storage, node_id(3), budget_of(8), None)
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

        let result = process_job_queue_batch(&storage, node_id(3), budget_of(8), None)
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
