use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
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
use futures_util::future::FutureExt;
use tokio::sync::watch;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::executor::{JobContext, JobRunOutcome, ProgressReporter, dispatch_payload, run_cleanup};
use super::store::{
    JobMutationError, ReleaseOutcome, cancel_running_job, complete_job, fail_job, flush_progress,
    iter_prefix_page, read_job_record, release_job, renew_lease, requeue_job,
    transition_to_running,
};
use super::submit::schedule_job_drain_effect;
use super::{
    JOB_CONCURRENCY_CAP, JOB_DRAIN_BATCH_SIZE, JOB_HEARTBEAT_MS, JOB_PROGRESS_FLUSH_INTERVAL_MS,
};
use crate::driver::DriverContext;

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(250);

struct RunningJob {
    /// Per-execution identity: a re-spawned job at the same id gets a fresh nonce so a
    /// zombie's `finish` cannot evict the newer execution that replaced it.
    nonce: u64,
    cancel: CancellationToken,
    completion: watch::Sender<bool>,
    claim_token: ulid::Ulid,
}

#[derive(Default)]
struct RuntimeState {
    running: HashMap<JobId, RunningJob>,
    /// A claim that raced the drain stop is released by a detached task; `shutdown` must
    /// be able to await it, or the process can exit before the lease is handed back.
    pending_releases: Vec<tokio::task::JoinHandle<()>>,
    draining: bool,
}

/// What a shutdown did with the jobs that were in flight.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct JobShutdownReport {
    /// Wound down inside the grace period and wrote their own state.
    pub finished: usize,
    /// Still running at grace expiry: lease handed back, no attempt spent.
    pub released: usize,
    /// Claim was no longer ours, so the lease was left alone.
    pub skipped: usize,
}

/// Registry of locally executing jobs, their cancellation tokens, and the concurrency cap.
pub struct JobsRuntime {
    state: Mutex<RuntimeState>,
    next_nonce: AtomicU64,
    cap: usize,
    /// Node shutdown. Deliberately not the per-job cancel token: that one means the
    /// user asked for a `Cancelled` job and runs cleanup handlers, while this one only
    /// means "stop here, the lease goes back to the queue and the job runs again".
    shutdown: CancellationToken,
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
            state: Mutex::new(RuntimeState::default()),
            next_nonce: AtomicU64::new(0),
            cap,
            shutdown: CancellationToken::new(),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RuntimeState> {
        self.state.lock().expect("runtime mutex poisoned")
    }

    pub fn running_count(&self) -> usize {
        self.state().running.len()
    }

    /// Zero once draining, so the drain claims nothing more.
    pub fn available_slots(&self) -> usize {
        let state = self.state();
        if state.draining {
            return 0;
        }
        self.cap.saturating_sub(state.running.len())
    }

    /// Poke a locally running job's cancel token; `false` if it is not running here.
    pub fn request_cancel(&self, job_id: JobId) -> bool {
        match self.state().running.get(&job_id) {
            Some(job) => {
                job.cancel.cancel();
                true
            }
            None => false,
        }
    }

    pub fn spawn(self: &Arc<Self>, context: Arc<DriverContext>, record: JobRecord) {
        let job_id = record.job_id;
        let Some(claim_token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
            warn!(job_id = %job_id, "Spawned job has no claim token; skipping");
            return;
        };
        let nonce = self.next_nonce.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        let (completion, _) = watch::channel(false);
        {
            let mut state = self.state();
            // Claimed just as the drain was stopping: hand the lease straight back
            // rather than let it rot until the sweep spends an attempt on it.
            if state.draining {
                let storage = context.storage_handle.clone();
                let release = tokio::spawn(async move {
                    let _ =
                        release_job(&storage, job_id, claim_token, unix_timestamp_millis()).await;
                });
                state.pending_releases.push(release);
                return;
            }
            state.running.insert(
                job_id,
                RunningJob {
                    nonce,
                    cancel: cancel.clone(),
                    completion,
                    claim_token,
                },
            );
        }
        let runtime = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            // A panicking payload is turned into a job failure inside `supervise`; this
            // guard still runs `finish` if anything else unwinds, so a panic can never
            // leak the concurrency slot and wedge the runtime.
            let _ = AssertUnwindSafe(run_job(
                context.clone(),
                record,
                claim_token,
                cancel,
                shutdown,
            ))
            .catch_unwind()
            .await;
            runtime.finish(&context, job_id, nonce).await;
        });
    }

    /// Stop claiming, signal the jobs in flight, and give them `grace` to wind down.
    /// Whatever is still running then has its lease handed back, so a restart costs a
    /// job no attempt. Storage must outlive this call: releasing a lease writes.
    pub async fn shutdown(&self, storage: &StorageHandle, grace: Duration) -> JobShutdownReport {
        let waiters: Vec<_> = {
            let mut state = self.state();
            state.draining = true;
            state
                .running
                .iter()
                .map(|(job_id, job)| (*job_id, job.claim_token, job.completion.subscribe()))
                .collect()
        };
        self.shutdown.cancel();

        let deadline = Instant::now() + grace;
        let mut report = JobShutdownReport::default();
        for (job_id, claim_token, mut completion) in waiters {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                let _ = tokio::time::timeout(remaining, completion.changed()).await;
            }
            if !self.state().running.contains_key(&job_id) {
                report.finished += 1;
                continue;
            }
            match release_job(storage, job_id, claim_token, unix_timestamp_millis()).await {
                Ok(ReleaseOutcome::Released(_)) => report.released += 1,
                Ok(ReleaseOutcome::Skipped) => report.skipped += 1,
                Err(JobMutationError::TokenMismatch) => {
                    info!(job_id = %job_id, "Job was taken over elsewhere; leaving its lease");
                    report.skipped += 1;
                }
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Failed to hand back job lease");
                    report.skipped += 1;
                }
            }
        }
        // A claim that raced the drain stop released itself on a detached task; wait for
        // those too, or the process can exit with the lease still held.
        loop {
            let pending_releases = std::mem::take(&mut self.state().pending_releases);
            if pending_releases.is_empty() {
                break;
            }
            for release in pending_releases {
                let _ = release.await;
            }
        }
        info!(
            finished = report.finished,
            released = report.released,
            skipped = report.skipped,
            "Jobs runtime shut down"
        );
        report
    }

    async fn finish(&self, context: &DriverContext, job_id: JobId, nonce: u64) {
        let removed = {
            let mut state = self.state();
            match state.running.get(&job_id) {
                Some(job) if job.nonce == nonce => state.running.remove(&job_id),
                _ => None,
            }
        };
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

    #[cfg(test)]
    fn register_test_execution(&self, job_id: JobId) -> u64 {
        let nonce = self.next_nonce.fetch_add(1, Ordering::Relaxed);
        let (completion, _) = watch::channel(false);
        self.state().running.insert(
            job_id,
            RunningJob {
                nonce,
                cancel: CancellationToken::new(),
                completion,
                claim_token: ulid::Ulid::r#gen(),
            },
        );
        nonce
    }

    /// Block until the job is terminal or `timeout` elapses (local signal, else 250ms poll).
    pub async fn wait_for_terminal(
        &self,
        storage: &StorageHandle,
        job_id: JobId,
        timeout: Duration,
    ) -> Option<JobState> {
        let deadline = Instant::now() + timeout;
        let mut receiver = self
            .state()
            .running
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

    /// At startup every claimed/running holder is definitionally dead: re-queue them all.
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
                None,
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

async fn run_job(
    context: Arc<DriverContext>,
    record: JobRecord,
    token: ulid::Ulid,
    cancel: CancellationToken,
    shutdown: CancellationToken,
) {
    let storage = &context.storage_handle;
    let job_id = record.job_id;

    if record.cancel_requested {
        run_cleanup(&record.payload);
        terminal_or_none(
            retry_terminal(|| cancel_running_job(storage, job_id, token, unix_timestamp_millis()))
                .await,
            job_id,
        );
        return;
    }

    match transition_to_running(storage, job_id, token, unix_timestamp_millis()).await {
        Ok(fresh_record) if fresh_record.cancel_requested => {
            run_cleanup(&record.payload);
            terminal_or_none(
                retry_terminal(|| {
                    cancel_running_job(storage, job_id, token, unix_timestamp_millis())
                })
                .await,
                job_id,
            );
            return;
        }
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
        shutdown,
        progress: progress.clone(),
    };

    // Capture the timestamp AFTER the payload runs, so finished_at, backoff, and
    // retention reflect completion time, not the job's start.
    match supervise(storage, job_id, token, &record, &ctx).await {
        SuperviseResult::Zombie => {}
        SuperviseResult::Outcome(JobRunOutcome::Succeeded(result)) => {
            terminal_or_none(
                retry_terminal(|| {
                    complete_job(
                        storage,
                        job_id,
                        token,
                        result.clone(),
                        progress.snapshot(),
                        unix_timestamp_millis(),
                    )
                })
                .await,
                job_id,
            );
        }
        SuperviseResult::Outcome(JobRunOutcome::Failed(error)) => {
            if error.kind == JobErrorKind::Retryable {
                if let Err(error) = requeue_job(
                    storage,
                    job_id,
                    Some(token),
                    unix_timestamp_millis(),
                    None,
                    Some(error),
                )
                .await
                    && !matches!(error, JobMutationError::TokenMismatch)
                {
                    warn!(job_id = %job_id, error = %error, "Failed to requeue job");
                }
            } else {
                terminal_or_none(
                    retry_terminal(|| {
                        fail_job(
                            storage,
                            job_id,
                            token,
                            error.clone(),
                            unix_timestamp_millis(),
                        )
                    })
                    .await,
                    job_id,
                );
            }
        }
        SuperviseResult::Outcome(JobRunOutcome::Cancelled) => {
            let terminal = retry_terminal(|| {
                cancel_running_job(storage, job_id, token, unix_timestamp_millis())
            })
            .await;
            if !matches!(&terminal, Err(JobMutationError::TokenMismatch)) {
                run_cleanup(&record.payload);
            }
            terminal_or_none(terminal, job_id);
        }
        // Shutdown, not cancellation: no cleanup handler, no attempt spent.
        SuperviseResult::Outcome(JobRunOutcome::Interrupted) => {
            match release_job(storage, job_id, token, unix_timestamp_millis()).await {
                Ok(_) => {}
                Err(JobMutationError::TokenMismatch) => {
                    info!(job_id = %job_id, "Lease already handed back or re-claimed")
                }
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Failed to hand back job lease")
                }
            }
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

const TERMINAL_WRITE_MAX_ATTEMPTS: u32 = 5;

/// Retry a terminal write past transient storage failures. The execution already
/// finished, so a bare storage error would otherwise leave the job `Running` until the
/// sweep re-runs it and can flip a succeeded job to `Failed`; token/transition races are
/// legitimate outcomes and are returned unretried.
async fn retry_terminal<F, Fut>(mut op: F) -> Result<JobRecord, JobMutationError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<JobRecord, JobMutationError>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(JobMutationError::Storage(error)) if attempt + 1 < TERMINAL_WRITE_MAX_ATTEMPTS => {
                warn!(error = %error, "Retrying job terminal write after storage failure");
                tokio::time::sleep(Duration::from_millis(20u64 << attempt)).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

async fn supervise(
    storage: &StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    ctx: &JobContext,
) -> SuperviseResult {
    // Renew the lease and flush progress on a SEPARATE task so a payload stuck in a
    // non-yielding section cannot starve its own renewals and be swept out from under
    // itself into a second concurrent execution.
    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(heartbeat_loop(
        storage.clone(),
        job_id,
        token,
        ctx.progress.clone(),
        ctx.cancel.clone(),
        stop.clone(),
    ));
    tokio::pin!(heartbeat);

    let payload = AssertUnwindSafe(dispatch_payload(ctx, &record.payload)).catch_unwind();
    tokio::pin!(payload);

    tokio::select! {
        outcome = &mut payload => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            match outcome {
                Ok(outcome) => SuperviseResult::Outcome(outcome),
                Err(_panic) => {
                    warn!(job_id = %job_id, "Job payload panicked; failing the attempt");
                    SuperviseResult::Outcome(JobRunOutcome::Failed(JobError::retryable(
                        "job payload panicked",
                    )))
                }
            }
        }
        // The heartbeat only returns before the payload when it loses the claim: another
        // execution has taken over, so abandon this one without writing a terminal state.
        _ = &mut heartbeat => SuperviseResult::Zombie,
    }
}

/// Returns only when `stop` fires (payload finished) or the claim is lost. A lost claim
/// ends the task, which `supervise` reads as this execution having been superseded.
async fn heartbeat_loop(
    storage: StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    progress: ProgressReporter,
    cancel: CancellationToken,
    stop: CancellationToken,
) {
    let mut heartbeat = interval(Duration::from_millis(JOB_HEARTBEAT_MS));
    heartbeat.tick().await;
    let mut flush = interval(Duration::from_millis(JOB_PROGRESS_FLUSH_INTERVAL_MS));
    flush.tick().await;

    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = heartbeat.tick() => {
                match renew_lease(&storage, job_id, token, unix_timestamp_millis(), Some(progress.snapshot())).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Lease renew failed"),
                }
            }
            _ = flush.tick() => {
                match flush_progress(&storage, job_id, token, progress.snapshot(), unix_timestamp_millis()).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Progress flush failed"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JOB_MAX_ATTEMPTS;
    use crate::jobs::store::{
        ClaimOutcome, claim_job, complete_job, insert_job, set_cancel_requested,
    };
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
                panic_at: None,
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

    async fn reclaim(storage: &StorageHandle, job_id: JobId) -> JobRecord {
        claim_job(storage, job_id, node_id(3), unix_timestamp_millis())
            .await
            .unwrap();
        read_job_record(storage, job_id, None)
            .await
            .unwrap()
            .unwrap()
    }

    async fn wait_running(storage: &StorageHandle, job_id: JobId) {
        for _ in 0..400 {
            if let Ok(Some(record)) = read_job_record(storage, job_id, None).await
                && record.state == JobState::Running
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("job never reached Running");
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
        // A prior run means it is claimed (not fresh-cancelled) so cleanup runs.
        record.has_run = true;
        let claimed = claim(&storage, record).await;

        runtime.spawn(ctx.clone(), claimed);
        let state = runtime
            .wait_for_terminal(&storage, job_id, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(state, JobState::Cancelled);
        assert!(!marker.exists(), "cleanup removes partial state on reclaim");
    }

    // A cancel committed after claim must beat payload execution from the stale snapshot.
    #[tokio::test]
    async fn cancel_after_claim() {
        let (dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let job_id = JobId::from_bytes([0x3A; 16]);
        let marker = dir.path().join("late-cancel-marker");
        std::fs::write(&marker, b"partial").unwrap();
        let mut record = probe_record(job_id, 1, 0, Some(marker.to_str().unwrap().to_string()));
        let JobPayload::Probe { fail_at, .. } = &mut record.payload;
        *fail_at = Some(0);
        let claimed = claim(&storage, record).await;
        let token = claimed.claim.as_ref().unwrap().claim_token;
        set_cancel_requested(&storage, job_id, unix_timestamp_millis())
            .await
            .unwrap();

        run_job(
            ctx,
            claimed,
            token,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await;

        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Cancelled);
        assert!(!marker.exists(), "late cancellation must run cleanup");
    }

    // Cleanup is claim-owned; a reclaimed zombie must leave the side effect alone.
    #[tokio::test]
    async fn zombie_skips_cleanup() {
        let (dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let job_id = JobId::from_bytes([0x2A; 16]);
        let marker = dir.path().join("zombie-marker");
        let marker_str = marker.to_str().unwrap().to_string();
        let claimed = claim(&storage, probe_record(job_id, 1, 60_000, Some(marker_str))).await;
        let stale_token = claimed.claim.as_ref().unwrap().claim_token;
        let cancel = CancellationToken::new();
        let execution = tokio::spawn(run_job(
            ctx,
            claimed,
            stale_token,
            cancel.clone(),
            CancellationToken::new(),
        ));
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(marker.exists(), "probe must reach its cleanup side effect");

        requeue_job(
            &storage,
            job_id,
            Some(stale_token),
            unix_timestamp_millis(),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(matches!(
            claim_job(&storage, job_id, node_id(4), unix_timestamp_millis())
                .await
                .unwrap(),
            ClaimOutcome::Claimed(_)
        ));
        assert!(
            !execution.is_finished(),
            "stale payload must still be running"
        );

        cancel.cancel();
        execution.await.unwrap();
        assert!(marker.exists(), "zombie must not run cleanup");
    }

    #[tokio::test]
    async fn completion_wins_race() {
        let (_dir, storage) = temp_storage();
        let job_id = JobId::from_bytes([4u8; 16]);
        let mut record = probe_record(job_id, 1, 0, None);
        record.state = JobState::Running;
        let token = Ulid::r#gen();
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
            claim_token: Ulid::r#gen(),
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

    // finished_at must reflect completion time, not the job's start.
    #[tokio::test]
    async fn finished_at_fresh() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([8u8; 16]);
        let claimed = claim(&storage, probe_record(job_id, 30, 10, None)).await;

        let t_before = unix_timestamp_millis();
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
        assert!(
            record.finished_at_ms.unwrap() >= t_before + 150,
            "finished_at must be set after the payload ran, not at start"
        );
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

    // A job interrupted by a shutdown is queued again, and the restart costs it nothing.
    #[tokio::test]
    async fn shutdown_releases_lease() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let job_id = JobId::from_bytes([0x11; 16]);
        let claimed = claim(&storage, probe_record(job_id, 100, 20, None)).await;

        let runtime = JobsRuntime::new();
        runtime.spawn(ctx.clone(), claimed);
        wait_running(&storage, job_id).await;
        let report = runtime.shutdown(&storage, Duration::ZERO).await;
        assert_eq!(report.released, 1);

        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Queued);
        assert_eq!(record.attempts, 0, "a shutdown is not a spent attempt");
        assert!(record.claim.is_none());
        assert!(
            record.due_at_ms <= unix_timestamp_millis(),
            "due immediately"
        );

        // The next node picks it straight back up and drives it to success.
        let restarted = JobsRuntime::new();
        let reclaimed = reclaim(&storage, job_id).await;
        restarted.spawn(ctx.clone(), reclaimed);
        let state = restarted
            .wait_for_terminal(&storage, job_id, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(state, JobState::Succeeded);
        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.attempts, 0);
    }

    // Rolling restarts must never exhaust JOB_MAX_ATTEMPTS on a job that never failed.
    #[tokio::test]
    async fn restarts_keep_attempts() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let job_id = JobId::from_bytes([0x12; 16]);
        insert_job(&storage, &probe_record(job_id, 10_000, 5, None))
            .await
            .unwrap();

        for restart in 0..(JOB_MAX_ATTEMPTS + 2) {
            let runtime = JobsRuntime::new();
            let claimed = reclaim(&storage, job_id).await;
            runtime.spawn(ctx.clone(), claimed);
            wait_running(&storage, job_id).await;
            runtime.shutdown(&storage, Duration::ZERO).await;

            let record = read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.state, JobState::Queued, "restart {restart}");
            assert_eq!(record.attempts, 0, "restart {restart}");
        }
    }

    // Shutdown is not cancellation: no Cancelled state and no cleanup handler.
    #[tokio::test]
    async fn shutdown_skips_cleanup() {
        let (dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let job_id = JobId::from_bytes([0x13; 16]);
        let marker = dir.path().join("probe-marker");
        let marker_str = marker.to_str().unwrap().to_string();
        let claimed = claim(&storage, probe_record(job_id, 10_000, 5, Some(marker_str))).await;

        let runtime = JobsRuntime::new();
        runtime.spawn(ctx.clone(), claimed);
        wait_running(&storage, job_id).await;
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(marker.exists(), "probe should create its marker");

        let report = runtime.shutdown(&storage, Duration::from_secs(10)).await;
        assert_eq!(report.finished, 1, "the probe winds down inside the grace");

        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Queued);
        assert_eq!(record.attempts, 0);
        assert!(marker.exists(), "cleanup must not run for an interruption");
    }

    // Releasing a lease another node holds would double-run the job.
    #[tokio::test]
    async fn shutdown_spares_takeover() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let job_id = JobId::from_bytes([0x14; 16]);
        let claimed = claim(&storage, probe_record(job_id, 10_000, 5, None)).await;

        let runtime = JobsRuntime::new();
        runtime.spawn(ctx.clone(), claimed);
        wait_running(&storage, job_id).await;

        // The lease lapses and a peer takes the job over while we still hold it locally.
        requeue_job(&storage, job_id, None, unix_timestamp_millis(), None, None)
            .await
            .unwrap();
        claim_job(&storage, job_id, node_id(4), unix_timestamp_millis())
            .await
            .unwrap();

        let report = runtime.shutdown(&storage, Duration::ZERO).await;
        assert_eq!(report.released, 0);
        assert_eq!(report.skipped, 1);

        let record = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, JobState::Claimed);
        assert_eq!(record.claim.unwrap().holder_node_id, node_id(4));
    }

    // A saturated executor never drains, so a never-run job must terminalize in the store
    // transaction itself rather than wait for a claim that is not coming.
    #[tokio::test]
    async fn cancel_beats_capacity() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::with_capacity(0);
        let job_id = JobId::from_bytes([0x6C; 16]);
        let record = probe_record(job_id, 5, 0, None);
        let owner = record.created_by;
        insert_job(&storage, &record).await.unwrap();
        assert_eq!(runtime.available_slots(), 0);

        let outcome = crate::jobs::service::cancel_owned_job(&ctx, &runtime, owner, job_id)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            crate::jobs::service::CancelJobOutcome::Requested(_)
        ));

        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, JobState::Cancelled);
        assert!(stored.finished_at_ms.is_some());
        assert!(stored.claim.is_none());
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

    // A panicking payload must free its slot, not wedge the runtime forever.
    #[tokio::test]
    async fn panic_frees_slot() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::with_capacity(1);
        let boom = JobId::from_bytes([0xEE; 16]);
        let mut record = probe_record(boom, 5, 0, None);
        let JobPayload::Probe { panic_at, .. } = &mut record.payload;
        *panic_at = Some(0);
        let claimed = claim(&storage, record).await;

        runtime.spawn(ctx.clone(), claimed);
        let mut freed = false;
        for _ in 0..400 {
            if runtime.available_slots() == 1 {
                freed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(freed, "panic must release the concurrency slot");

        let ok = JobId::from_bytes([0xAB; 16]);
        let claimed = claim(&storage, probe_record(ok, 2, 0, None)).await;
        runtime.spawn(ctx.clone(), claimed);
        let state = runtime
            .wait_for_terminal(&storage, ok, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(
            state,
            JobState::Succeeded,
            "runtime still drains after a panic"
        );
    }

    // A zombie execution's finish must not evict the newer execution that replaced it.
    #[tokio::test]
    async fn zombie_keeps_survivor() {
        let (_dir, storage) = temp_storage();
        let ctx = context(storage.clone());
        let runtime = JobsRuntime::new();
        let job_id = JobId::from_bytes([0x5A; 16]);

        let zombie = runtime.register_test_execution(job_id);
        let survivor = runtime.register_test_execution(job_id);
        assert_ne!(zombie, survivor);

        runtime.finish(&ctx, job_id, zombie).await;
        assert_eq!(runtime.running_count(), 1, "zombie must not evict survivor");
        assert!(runtime.request_cancel(job_id), "survivor still registered");

        runtime.finish(&ctx, job_id, survivor).await;
        assert_eq!(runtime.running_count(), 0);
    }
}
