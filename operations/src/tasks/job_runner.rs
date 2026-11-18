//! Runs leased jobs on tasks of this node: the spawning, heartbeats, shutdown and restart
//! recovery around the job runtime's registry. Every decision is an operation it drives.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::any::Any;
use std::collections::BTreeSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use aruna_core::compute::AttemptPhase;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::jobs::{JobReport, JobStops};
use aruna_core::operation::Runnable;
use aruna_core::structs::execution::job::{
    JobError, JobExecutionClass, JobId, JobPayload, JobRecord, JobState,
};
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use futures_util::future::{BoxFuture, FutureExt};
use tokio::sync::RwLockReadGuard;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use ulid::Ulid;

use super::job_context::{JobContext, PayloadHandler};
use super::job_dispatch::dispatch_payload;
use crate::driver::{DriverContext, drive, node_routing};
use crate::effect_adapters::job::{JobHost, with_job_host};
use crate::jobs::executor::{JobRunOutcome, ProgressReporter, RangeTurns, run_cleanup};
use crate::jobs::runtime::{Admission, JobShutdownReport, JobsRuntime, RecoveredAdmission};
use crate::jobs::runtime_steps::{
    LeasePageOperation, RecoverJobOperation, Recovery, SettleRunOperation, TerminalWrite,
    TerminalWriteOperation, log_terminal,
};
use crate::jobs::store::job_calls::{
    ReleaseOutcome, flush_progress, handoff_external_attempt, release_job, renew_lease,
    transition_to_running,
};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls::read_job_record;
use crate::jobs::submit::schedule_drain_effect;
use crate::jobs::workflow::ExecutionRunOperation;
use crate::jobs::workflow::attempt::{AttemptScope, RunWiring};
use crate::jobs::workflow::reconcile::{
    AdoptAttemptOperation, AdoptedEnd, Adoption, ResumeAttemptOperation, SettleAdoptedOperation,
};
use crate::jobs::workflow::supervise::AbandonSessionOperation;
use crate::jobs::{FLUSH_INTERVAL_MS, JOB_HEARTBEAT_MS, JOB_MAX_ATTEMPTS};
use crate::tasks::job_reconcile::ExternalReconciler;

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(250);

impl JobsRuntime {
    pub(crate) async fn claim_producer(&self) -> Option<RwLockReadGuard<'_, ()>> {
        if !self.is_started() {
            return None;
        }
        let guard = self.claim_gate().read().await;
        match self.is_draining() {
            true => None,
            false => Some(guard),
        }
    }

    pub fn spawn(self: &Arc<Self>, context: Arc<DriverContext>, record: JobRecord) {
        let job_id = record.job_id;
        // Claimed just as the drain was stopping: the lease goes straight back rather than
        // rot until the sweep spends an attempt on it.
        let release_context = context.clone();
        let admission = self.admit(&record, unix_timestamp_millis(), move |token| {
            tokio::spawn(async move {
                let release = release_job(job_id, token, unix_timestamp_millis());
                let _ = drive(release, &release_context).await;
            })
        });
        let slot = match admission {
            Admission::Run(slot) => slot,
            Admission::Draining => return,
            Admission::NoClaim => {
                warn!(job_id = %job_id, "Spawned job has no claim token; skipping");
                return;
            }
        };
        let runtime = self.clone();
        let shutdown = self.shutdown_signal();
        let turns = self.turns();
        let handlers = self.handlers();
        tokio::spawn(async move {
            // A panicking payload is turned into a failure inside `supervise`; this guard
            // still runs `finish` on other unwinds, so a panic cannot leak the slot.
            let _ = AssertUnwindSafe(run_job(
                context.clone(),
                record,
                slot.claim_token,
                (slot.cancel, slot.progress),
                (shutdown, turns, handlers),
            ))
            .catch_unwind()
            .await;
            runtime.finish(&context, job_id, slot.nonce).await;
        });
    }

    /// Register an adopted external supervisor under the normal cap, cancellation,
    /// completion, and shutdown accounting. `false` leaves handoff to the caller.
    pub(crate) fn spawn_recovered<F, Fut>(
        self: &Arc<Self>,
        context: Arc<DriverContext>,
        record: &JobRecord,
        supervisor: F,
    ) -> bool
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let job_id = record.job_id;
        let (nonce, cancel) = match self.admit_recovered(record, unix_timestamp_millis()) {
            RecoveredAdmission::Run { nonce, cancel } => (nonce, cancel),
            RecoveredAdmission::AlreadyRunning => return true,
            RecoveredAdmission::Refused => return false,
            RecoveredAdmission::NoClaim => {
                warn!(job_id = %job_id, "Recovered job has no claim token; skipping");
                return false;
            }
        };
        let runtime = self.clone();
        tokio::spawn(async move {
            let _ = AssertUnwindSafe(supervisor(cancel)).catch_unwind().await;
            runtime.finish(&context, job_id, nonce).await;
        });
        true
    }

    /// Stop claiming, signal the jobs in flight, and give them `grace` to wind down.
    /// Whatever is still running then has its lease handed back, so a restart costs a
    /// job no attempt. Storage must outlive this call: releasing a lease writes.
    pub async fn shutdown(&self, context: &DriverContext, grace: Duration) -> JobShutdownReport {
        self.close_admission();
        let waiters = self.running_waiters();
        let deadline = Instant::now() + grace;
        let mut report = JobShutdownReport::default();
        for (job_id, class, claim_token, mut completion) in waiters {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                let _ = tokio::time::timeout(remaining, completion.changed()).await;
            }
            if !self.is_running(job_id) {
                report.finished += 1;
                continue;
            }
            let now_ms = unix_timestamp_millis();
            let released = match class {
                JobExecutionClass::InProcess => {
                    drive(release_job(job_id, claim_token, now_ms), context).await
                }
                JobExecutionClass::ExternalAttempt => {
                    drive(
                        handoff_external_attempt(job_id, claim_token, now_ms),
                        context,
                    )
                    .await
                }
            };
            match released {
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
        let _claim_producers = self.claim_gate().write().await;
        // A claim that raced the drain stop released itself on a detached task; wait for
        // those too, or the process can exit with the lease still held.
        loop {
            let pending_releases = self.take_releases();
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

    pub(crate) async fn finish(&self, context: &DriverContext, job_id: JobId, nonce: u64) {
        self.retire(job_id, nonce);
        if let Some(task_handle) = context.task_handle.as_ref()
            && let Event::Task(TaskEvent::Error { message, .. }) =
                task_handle.send_effect(schedule_drain_effect()).await
        {
            warn!(message = %message, "Failed to kick job drain after completion");
        }
    }

    /// Block until the job is terminal or `timeout` elapses (local signal, else 250ms poll).
    pub async fn wait_for_terminal(
        &self,
        context: &DriverContext,
        job_id: JobId,
        timeout: Duration,
    ) -> Option<JobState> {
        let deadline = Instant::now() + timeout;
        let mut receiver = self.completion_of(job_id);
        loop {
            if let Ok(Some(record)) = drive(read_job_record(job_id), context).await
                && record.is_settled()
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

    /// At startup every claimed/running holder is definitionally dead. Re-queue
    /// the in-process ones and route external attempts to the reconcile hook,
    /// since a blind requeue would double-run; a restart costs them no attempt.
    pub async fn recover_stale_jobs(&self, context: &DriverContext) -> Result<usize, String> {
        self.recover_until_stopped(context, &CancellationToken::new(), || false)
            .await
    }

    /// [`Self::recover_stale_jobs`] with an explicit stop and a required-service
    /// failure probe. Both are checked before every recovery unit, so an accepted
    /// stop or observed failure lets the current unit finish and admits no next.
    pub async fn recover_until_stopped(
        &self,
        context: &DriverContext,
        stop: &CancellationToken,
        should_stop: impl Fn() -> bool,
    ) -> Result<usize, String> {
        let stopped = || stop.is_cancelled() || should_stop();
        let now_ms = unix_timestamp_millis();
        let mut job_ids = Vec::new();
        let mut after = None;
        loop {
            if stopped() {
                break;
            }
            let (page, next) = drive(LeasePageOperation::new(after), context)
                .await
                .map_err(|error| error.to_string())?;
            job_ids.extend(page);
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }

        let mut recovered = 0;
        for job_id in job_ids {
            if stopped() {
                break;
            }
            let recovery = drive(RecoverJobOperation::new(job_id, now_ms), context)
                .await
                .map_err(|error| error.to_string())?;
            match recovery {
                Recovery::Recovered => recovered += 1,
                Recovery::Untouched => {}
                Recovery::Reconcile(record) => {
                    if let Some(reconciler) = self.reconciler() {
                        reconciler.reconcile_lost_attempt(context, *record).await;
                    }
                }
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

/// Writes `Cancelled` for a job that never ran its payload.
async fn cancel_unstarted(context: &DriverContext, job_id: JobId, token: ulid::Ulid) {
    let write = TerminalWrite::Cancel;
    let cancel = TerminalWriteOperation::new(job_id, token, write, unix_timestamp_millis());
    log_terminal(&drive(cancel, context).await, job_id);
}

pub(crate) async fn run_job(
    context: Arc<DriverContext>,
    record: JobRecord,
    token: ulid::Ulid,
    (cancel, progress): (CancellationToken, Option<ProgressReporter>),
    (shutdown, range_turns, handlers): (
        CancellationToken,
        RangeTurns,
        Vec<Arc<dyn PayloadHandler>>,
    ),
) {
    // External attempts drive a container through the fenced lifecycle; a lost
    // lease reconciles, and a mid-supervise shutdown hands it back for adoption.
    if record.execution_class == JobExecutionClass::ExternalAttempt {
        Box::pin(run_execution_job(context, record, cancel)).await;
        return;
    }

    let job_id = record.job_id;
    // These jobs settle their own cancellation: an import cleans up, a purge hands over files.
    let import_cleanup = matches!(
        &record.payload,
        JobPayload::ImportRoCrate(_) | JobPayload::StoragePurgeFiles(_)
    );

    if record.cancel_requested && !import_cleanup {
        run_cleanup(&record.payload);
        cancel_unstarted(&context, job_id, token).await;
        return;
    }
    if record.cancel_requested {
        cancel.cancel();
    }

    let running = transition_to_running(job_id, token, unix_timestamp_millis());
    match drive(running, &context).await {
        Ok(fresh_record) => {
            if fresh_record.cancel_requested {
                if import_cleanup {
                    cancel.cancel();
                } else {
                    run_cleanup(&record.payload);
                    cancel_unstarted(&context, job_id, token).await;
                    return;
                }
            }
        }
        Err(JobMutationError::TokenMismatch) => {
            warn!(job_id = %job_id, "Lost claim before running; aborting");
            return;
        }
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Failed to start job");
            return;
        }
    }

    let progress = progress.unwrap_or_else(|| ProgressReporter::from_progress(&record.progress));
    let ctx = JobContext {
        // Bulk lane: imports and other background payloads must not delay API requests.
        driver: Arc::new(context.bulk()),
        job_id,
        owner_node_id: record.owner_node_id,
        claim_token: token,
        final_attempt: record.attempts.saturating_add(1) >= JOB_MAX_ATTEMPTS,
        cancel: cancel.clone(),
        shutdown,
        progress: progress.clone(),
        range_turns,
        handlers,
    };

    let SuperviseResult::Outcome(outcome) = supervise(&context, job_id, token, &record, &ctx).await
    else {
        return;
    };
    let settle = SettleRunOperation::new(
        (job_id, token),
        outcome,
        (progress.snapshot(), progress.checkpointed()),
        unix_timestamp_millis(),
    );
    match drive(settle, &context).await {
        Ok(true) => run_cleanup(&record.payload),
        Ok(false) => {}
        Err(error) => warn!(job_id = %job_id, error = %error, "Failed to settle job run"),
    }
}

async fn supervise(
    context: &DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    ctx: &JobContext,
) -> SuperviseResult {
    // Renew the lease and flush progress on a separate task so a non-yielding
    // payload cannot starve its renewals and be swept into a second execution.
    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(heartbeat_loop(
        context.clone(),
        job_id,
        token,
        ctx.progress.clone(),
        ctx.cancel.clone(),
        stop.clone(),
    ));
    tokio::pin!(heartbeat);

    let payload = async {
        match AssertUnwindSafe(dispatch_payload(ctx, &record.payload))
            .catch_unwind()
            .await
        {
            Ok(outcome) => outcome,
            Err(_panic) => handle_panic(ctx),
        }
    };
    tokio::pin!(payload);

    tokio::select! {
        outcome = &mut payload => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            SuperviseResult::Outcome(outcome)
        }
        // The heartbeat only returns before the payload when it loses the claim: another
        // execution has taken over, so abandon this one without writing a terminal state.
        _ = &mut heartbeat => SuperviseResult::Zombie,
    }
}

pub(crate) fn handle_panic(ctx: &JobContext) -> JobRunOutcome {
    warn!(job_id = %ctx.job_id, "Job payload panicked; failing the attempt");
    JobRunOutcome::Failed(JobError::retryable("job payload panicked"))
}

/// Returns only when `stop` fires (payload finished) or the claim is lost. A lost claim
/// ends the task, which `supervise` reads as this execution having been superseded.
async fn heartbeat_loop(
    context: DriverContext,
    job_id: JobId,
    token: ulid::Ulid,
    progress: ProgressReporter,
    cancel: CancellationToken,
    stop: CancellationToken,
) {
    let mut heartbeat = interval(Duration::from_millis(JOB_HEARTBEAT_MS));
    heartbeat.tick().await;
    let mut flush = interval(Duration::from_millis(FLUSH_INTERVAL_MS));
    flush.tick().await;

    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = heartbeat.tick() => {
                let snapshot = Some(progress.snapshot());
                let renew = renew_lease(job_id, token, unix_timestamp_millis(), snapshot);
                match drive(renew, &context).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Lease renew failed"),
                }
            }
            // A committed checkpoint is persisted at once, so the record keeps up with its rows.
            _ = futures_util::future::select(
                Box::pin(flush.tick()),
                Box::pin(progress.flush_requested()),
            ) => {
                let write =
                    flush_progress(job_id, token, progress.snapshot(), unix_timestamp_millis());
                match drive(write, &context).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Progress flush failed"),
                }
            }
        }
    }
}

/// The stop signal of an execution run: only a cancel stops it. A node shutdown leaves the
/// container running and hands the attempt back for adoption.
struct ExecutionHost {
    cancel: CancellationToken,
}

impl JobHost for ExecutionHost {
    fn stops(&self) -> JobStops {
        JobStops {
            cancelled: self.cancel.is_cancelled(),
            shutdown: false,
        }
    }

    fn report(&self, _report: JobReport) {}

    fn stopped(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.cancel.cancelled())
    }

    fn take_turns<'a>(
        &'a self,
        _turns: &'a BTreeSet<(Ulid, Ulid)>,
    ) -> BoxFuture<'a, Box<dyn Any + Send>> {
        Box::pin(std::future::ready(Box::new(()) as Box<dyn Any + Send>))
    }
}

/// The host of an execution run with this cancel signal.
pub(crate) fn execution_host(cancel: CancellationToken) -> Arc<dyn JobHost> {
    Arc::new(ExecutionHost { cancel })
}

/// What this node gives every execution run.
pub fn run_wiring(context: &DriverContext) -> RunWiring {
    RunWiring {
        serving: context.node_and_realm(),
        routing: node_routing(context),
        encryption_key: (context.net_handle.as_ref()).map(|net| net.credential_encryption_key()),
    }
}

/// Drives `operation` of one execution while its lease is renewed. `None` means the claim was
/// lost first: another execution took over, so the work is abandoned.
pub async fn under_lease<O, K>(
    context: &DriverContext,
    (job_id, token): (JobId, Ulid),
    cancel: CancellationToken,
    operation: O,
) -> Option<Result<O::Output, O::Error>>
where
    O: Runnable<K>,
{
    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(execution_heartbeat(
        context.clone(),
        (job_id, token),
        cancel.clone(),
        stop.clone(),
    ));
    tokio::pin!(heartbeat);
    let work = with_job_host(execution_host(cancel), drive(operation, context));
    let work = Box::pin(work);
    tokio::select! {
        result = work => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            Some(result)
        }
        _ = &mut heartbeat => None,
    }
}

/// Renews the lease and surfaces a cancel request. Returns on a lost token, which the run reads
/// as a takeover.
async fn execution_heartbeat(
    context: DriverContext,
    (job_id, token): (JobId, Ulid),
    cancel: CancellationToken,
    stop: CancellationToken,
) {
    let mut interval = interval(Duration::from_millis(JOB_HEARTBEAT_MS));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = interval.tick() => {
                let renew = renew_lease(job_id, token, unix_timestamp_millis(), None);
                match drive(renew, &context).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => {
                        warn!(job_id = %job_id, error = %error, "Execution lease renew failed")
                    }
                }
            }
        }
    }
}

/// Ends the bookkeeping of a run driven under its lease.
async fn finish_run<E: std::fmt::Display>(
    context: &DriverContext,
    job_id: JobId,
    finished: Option<Result<(), E>>,
) {
    match finished {
        Some(Ok(())) => {}
        Some(Err(error)) => warn!(job_id = %job_id, %error, "Execution run failed"),
        // This node no longer owns the attempt, so its session stops here even though nothing
        // else delivered an end for it.
        None => {
            info!(job_id = %job_id, "Execution supervisor superseded; abandoning");
            if let Err(error) = drive(AbandonSessionOperation::new(job_id), context).await {
                warn!(job_id = %job_id, %error, "Abandoned session did not close");
            }
        }
    }
}

/// Runs one claimed execution job from prepare to its terminal state.
pub async fn run_execution_job(
    context: Arc<DriverContext>,
    record: JobRecord,
    cancel: CancellationToken,
) {
    let job_id = record.job_id;
    let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
        warn!(job_id = %job_id, "Execution job has no claim token; skipping");
        return;
    };
    let run = ExecutionRunOperation::new(record, run_wiring(&context));
    let finished = under_lease(&context, (job_id, token), cancel, run).await;
    finish_run(&context, job_id, finished).await;
}

/// Supervises an adopted attempt.
pub(crate) async fn resume_attempt(
    context: Arc<DriverContext>,
    scope: Arc<AttemptScope>,
    record: Arc<JobRecord>,
    phase: AttemptPhase,
    cancel: CancellationToken,
) {
    let ids = (scope.job_id, scope.token);
    let resume = ResumeAttemptOperation::new(scope, record, phase);
    let finished = under_lease(&context, ids, cancel, resume).await;
    finish_run(&context, ids.0, finished).await;
}

/// The reconcile seam of the node: a lost external attempt is adopted by name and resolved by
/// evidence. It never submits a new attempt name, so a container is never run twice.
pub struct ComputeReconciler {
    context: Arc<DriverContext>,
    runtime: Weak<JobsRuntime>,
}

impl ComputeReconciler {
    pub fn new(context: Arc<DriverContext>, runtime: Weak<JobsRuntime>) -> Arc<Self> {
        Arc::new(Self { context, runtime })
    }

    /// Adopts a live container without blocking the drain, registering its supervisor with the
    /// runtime for caps, cancellation and shutdown.
    async fn resume(&self, scope: Arc<AttemptScope>, record: Arc<JobRecord>, phase: AttemptPhase) {
        let (job_id, token) = (scope.job_id, scope.token);
        let context = self.context.clone();
        let supervised = record.clone();
        let supervisor = move |cancel| resume_attempt(context, scope, supervised, phase, cancel);
        let registered = self.runtime.upgrade().is_some_and(|runtime| {
            runtime.spawn_recovered(self.context.clone(), &record, supervisor)
        });
        if registered {
            return;
        }
        let handoff = handoff_external_attempt(job_id, token, unix_timestamp_millis());
        if let Err(error) = drive(handoff, &self.context).await {
            warn!(job_id = %job_id, error = %error, "Failed to hand off unregistered recovered attempt");
        }
    }
}

#[async_trait::async_trait]
impl ExternalReconciler for ComputeReconciler {
    async fn reconcile_lost_attempt(&self, context: &DriverContext, record: JobRecord) {
        let job_id = record.job_id;
        if !matches!(record.payload, JobPayload::Execution(_)) {
            return;
        }
        let holder = (self.context.net_handle.as_ref())
            .map(|net| net.node_id())
            .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[0u8; 32]).public());
        let adopt = AdoptAttemptOperation::new(job_id, holder, run_wiring(&self.context));
        let (scope, adopted) = match drive(adopt, context).await {
            Ok(Adoption::Adopted { scope, record }) => (scope, record),
            Ok(Adoption::Done) => return,
            Err(error) => {
                warn!(job_id = %job_id, %error, "Adoption failed");
                return;
            }
        };
        let ids = (job_id, scope.token);
        let settle = SettleAdoptedOperation::new(scope.clone(), adopted.clone());
        let cancel = CancellationToken::new();
        match under_lease(context, ids, cancel, settle).await {
            Some(Ok(AdoptedEnd::Resume(phase))) => self.resume(scope, adopted, phase).await,
            Some(Ok(AdoptedEnd::Settled)) => {}
            Some(Err(error)) => warn!(job_id = %job_id, %error, "Adopted attempt failed"),
            None => info!(job_id = %job_id, "Reconcile finalizer superseded; abandoning"),
        }
    }
}
