use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::keyspaces::JOB_SCHEDULE_INDEX_KEYSPACE;
use aruna_core::structs::{
    JOB_LEASE_INDEX_PREFIX, JobError, JobErrorKind, JobId, JobRecord, JobState,
    parse_job_schedule_index_key,
};
use aruna_core::task::TaskEvent;
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use tokio::sync::watch;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::executor::{JobContext, JobRunOutcome, ProgressReporter, dispatch_payload, run_cleanup};
use super::store::{
    JobMutationError, cancel_running_job, complete_job, fail_job, flush_progress, iter_prefix_page,
    read_job_record, renew_lease, requeue_job, transition_to_running,
};
use super::submit::schedule_job_drain_effect;
use super::{
    JOB_CONCURRENCY_CAP, JOB_DRAIN_BATCH_SIZE, JOB_HEARTBEAT_MS, JOB_PROGRESS_FLUSH_INTERVAL_MS,
};
use crate::driver::DriverContext;

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(250);

struct RunningJob {
    cancel: CancellationToken,
    completion: watch::Sender<bool>,
}

/// Registry of locally executing jobs: the cancellation tokens, the completion
/// signals backing `wait_for_terminal`, and the concurrency cap.
pub struct JobsRuntime {
    running: Mutex<HashMap<JobId, RunningJob>>,
    cap: usize,
}

impl std::fmt::Debug for JobsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobsRuntime")
            .field("cap", &self.cap)
            .field("running", &self.running_count())
            .finish_non_exhaustive()
    }
}

impl JobsRuntime {
    pub fn new() -> Arc<Self> {
        Self::with_capacity(JOB_CONCURRENCY_CAP)
    }

    pub fn with_capacity(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            running: Mutex::new(HashMap::new()),
            cap,
        })
    }

    pub fn running_count(&self) -> usize {
        self.running.lock().expect("runtime mutex poisoned").len()
    }

    pub fn available_slots(&self) -> usize {
        self.cap.saturating_sub(self.running_count())
    }

    /// Poke a locally running job's cancellation token. Returns `false` if the job
    /// is not running here; the persisted `cancel_requested` flag then drives it.
    pub fn request_cancel(&self, job_id: JobId) -> bool {
        match self
            .running
            .lock()
            .expect("runtime mutex poisoned")
            .get(&job_id)
        {
            Some(job) => {
                job.cancel.cancel();
                true
            }
            None => false,
        }
    }

    pub fn spawn(self: &Arc<Self>, context: Arc<DriverContext>, record: JobRecord) {
        let job_id = record.job_id;
        let cancel = CancellationToken::new();
        let (completion, _) = watch::channel(false);
        self.running.lock().expect("runtime mutex poisoned").insert(
            job_id,
            RunningJob {
                cancel: cancel.clone(),
                completion,
            },
        );
        let runtime = self.clone();
        tokio::spawn(async move {
            run_job(context.clone(), record, cancel).await;
            runtime.finish(&context, job_id).await;
        });
    }

    async fn finish(&self, context: &DriverContext, job_id: JobId) {
        let removed = self
            .running
            .lock()
            .expect("runtime mutex poisoned")
            .remove(&job_id);
        if let Some(job) = removed {
            let _ = job.completion.send(true);
        }
        if let Some(task_handle) = context.task_handle.as_ref()
            && let Event::Task(TaskEvent::Error { message, .. }) =
                task_handle.send_effect(schedule_job_drain_effect()).await
        {
            warn!(message = %message, "Failed to kick job drain after completion");
        }
    }

    /// Block until the job is terminal or `timeout` elapses. Uses the local
    /// completion signal when the job runs here, else polls storage every 250ms.
    pub async fn wait_for_terminal(
        &self,
        storage: &StorageHandle,
        job_id: JobId,
        timeout: Duration,
    ) -> Option<JobState> {
        let deadline = Instant::now() + timeout;
        let mut receiver = self
            .running
            .lock()
            .expect("runtime mutex poisoned")
            .get(&job_id)
            .map(|job| job.completion.subscribe());
        loop {
            if let Ok(Some(record)) = read_job_record(storage, job_id, None).await
                && record.state.is_terminal()
            {
                return Some(record.state);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let poll = remaining.min(WAIT_POLL_INTERVAL);
            match receiver.as_mut() {
                Some(rx) => {
                    let _ = tokio::time::timeout(poll, rx.changed()).await;
                }
                None => tokio::time::sleep(poll).await,
            }
        }
    }

    /// Fjall is single-process, so at startup every claimed/running job's holder is
    /// definitionally dead: re-queue them all immediately.
    pub async fn recover_stale_jobs(&self, storage: &StorageHandle) -> Result<usize, String> {
        let now_ms = unix_timestamp_millis();
        let mut job_ids = Vec::new();
        let mut start_after = None;
        loop {
            let (values, next) = iter_prefix_page(
                storage,
                JOB_SCHEDULE_INDEX_KEYSPACE,
                Some(ByteView::from(JOB_LEASE_INDEX_PREFIX.to_vec())),
                start_after,
                JOB_DRAIN_BATCH_SIZE,
                None,
            )
            .await?;
            for (key, _) in &values {
                if let Ok((_, job_id)) = parse_job_schedule_index_key(key.as_ref()) {
                    job_ids.push(job_id);
                }
            }
            match next {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }

        let mut recovered = 0;
        for job_id in job_ids {
            match requeue_job(
                storage,
                job_id,
                None,
                now_ms,
                Some(JobError::retryable(
                    "node restarted while job was in flight",
                )),
            )
            .await
            {
                Ok(_) => recovered += 1,
                Err(JobMutationError::NotFound) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        if recovered > 0 {
            info!(recovered, "Re-queued stale in-flight jobs after restart");
        }
        Ok(recovered)
    }
}

enum SuperviseResult {
    Outcome(JobRunOutcome),
    Zombie,
}

async fn run_job(context: Arc<DriverContext>, record: JobRecord, cancel: CancellationToken) {
    let storage = &context.storage_handle;
    let job_id = record.job_id;
    let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
        warn!(job_id = %job_id, "Spawned job has no claim token; skipping");
        return;
    };

    if record.cancel_requested {
        run_cleanup(&record.payload);
        terminal_or_none(
            cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await,
            job_id,
        );
        return;
    }

    match transition_to_running(storage, job_id, token, unix_timestamp_millis()).await {
        Ok(_) => {}
        Err(JobMutationError::TokenMismatch) => {
            warn!(job_id = %job_id, "Lost claim before running; aborting");
            return;
        }
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Failed to start job");
            return;
        }
    }

    let progress = ProgressReporter::from_progress(&record.progress);
    let ctx = JobContext {
        driver: context.clone(),
        cancel: cancel.clone(),
        progress: progress.clone(),
    };

    let now_ms = unix_timestamp_millis();
    match supervise(storage, job_id, token, &record, &ctx).await {
        SuperviseResult::Zombie => {}
        SuperviseResult::Outcome(JobRunOutcome::Succeeded(result)) => {
            terminal_or_none(
                complete_job(storage, job_id, token, result, progress.snapshot(), now_ms).await,
                job_id,
            );
        }
        SuperviseResult::Outcome(JobRunOutcome::Failed(error)) => {
            if error.kind == JobErrorKind::Retryable {
                if let Err(error) =
                    requeue_job(storage, job_id, Some(token), now_ms, Some(error)).await
                    && !matches!(error, JobMutationError::TokenMismatch)
                {
                    warn!(job_id = %job_id, error = %error, "Failed to requeue job");
                }
            } else {
                terminal_or_none(
                    fail_job(storage, job_id, token, error, now_ms).await,
                    job_id,
                );
            }
        }
        SuperviseResult::Outcome(JobRunOutcome::Cancelled) => {
            run_cleanup(&record.payload);
            terminal_or_none(
                cancel_running_job(storage, job_id, token, now_ms).await,
                job_id,
            );
        }
    }
}

fn terminal_or_none(result: Result<JobRecord, JobMutationError>, job_id: JobId) {
    match result {
        Ok(_) => {}
        Err(JobMutationError::TokenMismatch) => {
            warn!(job_id = %job_id, "Terminal write rejected: job was re-claimed")
        }
        Err(JobMutationError::IllegalTransition(_)) => {
            info!(job_id = %job_id, "Lost terminal transition race; leaving winner's state")
        }
        Err(error) => warn!(job_id = %job_id, error = %error, "Failed terminal transition"),
    }
}

async fn supervise(
    storage: &StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    ctx: &JobContext,
) -> SuperviseResult {
    let payload_future = dispatch_payload(ctx, &record.payload);
    tokio::pin!(payload_future);

    let mut heartbeat = interval(Duration::from_millis(JOB_HEARTBEAT_MS));
    heartbeat.tick().await;
    let mut flush = interval(Duration::from_millis(JOB_PROGRESS_FLUSH_INTERVAL_MS));
    flush.tick().await;

    loop {
        tokio::select! {
            outcome = &mut payload_future => return SuperviseResult::Outcome(outcome),
            _ = heartbeat.tick() => {
                match renew_lease(storage, job_id, token, unix_timestamp_millis(), Some(ctx.progress.snapshot())).await {
                    Ok(renew) => if renew.cancel_requested { ctx.cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return SuperviseResult::Zombie,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Lease renew failed"),
                }
            }
            _ = flush.tick() => {
                match flush_progress(storage, job_id, token, ctx.progress.snapshot(), unix_timestamp_millis()).await {
                    Ok(renew) => if renew.cancel_requested { ctx.cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return SuperviseResult::Zombie,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Progress flush failed"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::store::{claim_job, complete_job, insert_job};
    use aruna_core::structs::{JobClaim, JobPayload, JobResultPayload, RealmId};
    use aruna_core::types::{NodeId, UserId};
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn context(storage: StorageHandle) -> Arc<DriverContext> {
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        })
    }

    fn probe_record(job_id: JobId, steps: u32, sleep_ms: u64, marker: Option<String>) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps,
                step_sleep_ms: sleep_ms,
                fail_at: None,
                cleanup_marker: marker,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        )
    }

    async fn claim(storage: &StorageHandle, record: JobRecord) -> JobRecord {
        insert_job(storage, &record).await.unwrap();
        claim_job(storage, record.job_id, node_id(3), 1)
            .await
            .unwrap();
        read_job_record(storage, record.job_id, None)
            .await
            .unwrap()
            .unwrap()
    }

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        (dir, storage)
    }

    #[tokio::test]
    async fn probe_runs_to_success() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([1u8; 16]);
        let claimed = claim(&storage, probe_record(job_id, 3, 0, None)).await;

        runtime.spawn(ctx.clone(), claimed);
        let state = runtime
            .wait_for_terminal(&storage, job_id, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(state, JobState::Succeeded);

        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.result,
            Some(JobResultPayload::Probe { completed_steps: 3 })
        );
    }

    #[tokio::test]
    async fn cancel_running_cleanup() {
        let (dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([2u8; 16]);
        let marker = dir.path().join("probe-marker");
        let marker_str = marker.to_str().unwrap().to_string();
        let claimed = claim(&storage, probe_record(job_id, 10_000, 5, Some(marker_str))).await;

        runtime.spawn(ctx.clone(), claimed);
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            marker.exists(),
            "probe should create its marker while running"
        );
        assert!(runtime.request_cancel(job_id));

        let state = runtime
            .wait_for_terminal(&storage, job_id, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(state, JobState::Cancelled);
        assert!(!marker.exists(), "cleanup hook must remove the marker");
    }

    #[tokio::test]
    async fn cancel_before_run() {
        let (dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([3u8; 16]);
        let marker = dir.path().join("marker");
        std::fs::write(&marker, b"partial").unwrap();
        let mut record = probe_record(job_id, 5, 0, Some(marker.to_str().unwrap().to_string()));
        record.cancel_requested = true;
        // A prior attempt means partial state is possible, so the job is claimed and
        // its cleanup hook runs (unlike a never-attempted fresh cancel).
        record.attempts = 1;
        let claimed = claim(&storage, record).await;

        runtime.spawn(ctx.clone(), claimed);
        let state = runtime
            .wait_for_terminal(&storage, job_id, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(state, JobState::Cancelled);
        assert!(!marker.exists(), "cleanup removes partial state on reclaim");
    }

    #[tokio::test]
    async fn completion_wins_race() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([4u8; 16]);
        let mut record = probe_record(job_id, 1, 0, None);
        record.state = JobState::Running;
        let token = Ulid::new();
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: token,
            lease_expires_at_ms: 60_000,
        });
        insert_job(&storage, &record).await.unwrap();

        complete_job(
            &storage,
            job_id,
            token,
            JobResultPayload::Probe { completed_steps: 1 },
            record.progress.clone(),
            5_000,
        )
        .await
        .unwrap();

        // The cancel loser hits a cleared claim and is rejected without corrupting state.
        let loser = cancel_running_job(&storage, job_id, token, 5_001).await;
        assert!(matches!(loser, Err(JobMutationError::TokenMismatch)));
        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Succeeded);
    }

    #[tokio::test]
    async fn recover_requeues_inflight() {
        let (_dir, storage) = temp_storage();
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([5u8; 16]);
        let mut record = probe_record(job_id, 3, 0, None);
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(3),
            claim_token: Ulid::new(),
            lease_expires_at_ms: unix_timestamp_millis() + 60_000,
        });
        insert_job(&storage, &record).await.unwrap();

        assert_eq!(runtime.recover_stale_jobs(&storage).await.unwrap(), 1);
        let recovered = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.state, JobState::Queued);
        assert_eq!(recovered.attempts, 1);
        assert!(recovered.claim.is_none());
    }

    // Perf budget: progress is throttled, so a 10k-step job writes O(1), not O(steps).
    #[tokio::test]
    async fn progress_writes_throttled() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([7u8; 16]);
        let claimed = claim(&storage, probe_record(job_id, 10_000, 0, None)).await;

        let before = storage.snapshot_metrics().requests_total;
        runtime.spawn(ctx.clone(), claimed);
        let state = runtime
            .wait_for_terminal(&storage, job_id, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(state, JobState::Succeeded);

        let delta = storage.snapshot_metrics().requests_total - before;
        assert!(
            delta < 200,
            "10k-step run must not scale with steps, got {delta}"
        );

        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.progress.current, 10_000);
    }

    #[tokio::test]
    async fn wait_times_out() {
        let (_dir, storage) = temp_storage();
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([6u8; 16]);
        insert_job(&storage, &probe_record(job_id, 1, 0, None))
            .await
            .unwrap();

        let result = runtime
            .wait_for_terminal(&storage, job_id, Duration::from_millis(100))
            .await;
        assert_eq!(result, None);
    }
}
