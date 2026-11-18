//! Runs the queued runs of the installs this node receives: claims due runs within each
//! install's bound, compiles the pinned artifact, invokes the handler and finishes the run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::NodeId;
use aruna_core::plugin_runs::{Decisions, EVENTS_QUEUED, EventBatch, PluginRun};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::{CancellationToken, Supervisor};
use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use super::broker::RunBroker;
use super::jobs::{retire_work, settle_work};
use super::{read_config, ready_artifact};
use crate::driver::{DriverContext, drive};
use crate::plugins::attempt::{
    AttemptStart, BeginAttempt, ClaimRuns, ClaimedRuns, FinishRun, invocation, revocation,
};
use crate::plugins::check::RunScope;
use crate::plugins::cleanup::sweep_events;
use crate::plugins::clock::{AskClock, ClockReading, SKEW_MARGIN_MS, local_start};
use crate::plugins::decide::{DecideBatch, prune_seen};
use crate::plugins::dispatch::EnqueueTimers;
use crate::plugins::finished::DueCompletions;
use crate::plugins::forward::{Backoff, CollectOutbox, REQUEST_BYTES, SendEvents, apply};
use crate::plugins::journal::JOURNAL_PAGE;
use crate::plugins::journal::prune_journal;
use crate::plugins::lifecycle::{match_targets, targets};
use crate::plugins::marks::SweepMarks;
use crate::plugins::matcher::MatchJournal;
use crate::plugins::prune::{PruneAudit, prune_hooks};
use crate::plugins::receiver::lost_reason;
use crate::plugins::replay::{PlanReplay, ReplayPlan, RunReplays, StartReplay};
use crate::plugins::rows::StoreError;
use crate::plugins::runs::{Ended, PruneRuns, RunError, TICK, ended};
use crate::plugins::waiting::RecheckWaiting;
use crate::plugins::work::prune_work;
use crate::realm::mutate_placement::node_kind;

/// How often the worker rechecks the events waiting for their install record or account.
const RECHECK_MS: u64 = 10 * 1000;
/// How often the worker prunes runs that ended before the retention period.
const PRUNE_MS: u64 = 3600 * 1000;
/// How often the worker keeps the summaries of finished plugin work.
const SETTLE_MS: u64 = 30 * 1000;

/// The node service that runs the queued runs under the plugin supervisor.
pub struct Worker {
    ctx: Arc<DriverContext>,
    realm_id: RealmId,
    local: NodeId,
    supervisor: Supervisor,
    /// When each receiver of forwarded events is tried again.
    backoff: tokio::sync::Mutex<Backoff>,
    /// When the waiting events were last rechecked.
    rechecked: AtomicU64,
}

impl Worker {
    pub fn new(
        ctx: Arc<DriverContext>,
        realm_id: RealmId,
        local: NodeId,
    ) -> Result<Arc<Self>, String> {
        let config = RuntimeConfig::default();
        Self::with_budget(ctx, (realm_id, local), config, Limits::default())
    }

    /// A worker whose runtime has `config` and whose runs get the budget `limits`.
    pub fn with_budget(
        ctx: Arc<DriverContext>,
        (realm_id, local): (RealmId, NodeId),
        config: RuntimeConfig,
        limits: Limits,
    ) -> Result<Arc<Self>, String> {
        let supervisor = Supervisor::new(config, limits).map_err(|error| error.to_string())?;
        Ok(Arc::new(Self {
            ctx,
            realm_id,
            local,
            supervisor,
            backoff: tokio::sync::Mutex::default(),
            rechecked: AtomicU64::new(0),
        }))
    }

    /// Queues due timer occurrences, then starts the due runs of the active installs this node
    /// receives, at most `RUNS_PARALLEL` per install. A `running` run no task runs starts again.
    pub async fn claim(self: &Arc<Self>, now_ms: u64) -> Result<usize, String> {
        if self.supervisor.stopping() {
            return Ok(0);
        }
        let config = read_config(&self.ctx, self.realm_id)
            .await
            .map_err(|error| error.to_string())?;
        self.revoke(&config);
        let timers = drive(EnqueueTimers::new(&config, self.local, now_ms), &self.ctx).await;
        timers.map_err(|error| error.to_string())?;
        let probe = |node| probe_clock(&self.ctx, node);
        if let Err(error) = plan_replays(&self.ctx, &config, self.local, now_ms, probe).await {
            warn!(%error, "Could not plan plugin event replays");
        }
        let matched = match_journal(&self.ctx, &config, self.local, (now_ms, EVENTS_QUEUED));
        if let Err(error) = matched.await {
            warn!(%error, "Could not match plugin events to installs");
        }
        if let Err(error) = run_replays(&self.ctx, now_ms, (JOURNAL_PAGE, usize::MAX)).await {
            warn!(%error, "Could not replay plugin events");
        }
        let mut backoff = self.backoff.lock().await;
        let send = |node, batch| self.forward(node, batch, (&config, now_ms));
        if let Err(error) = send_outbox(&self.ctx, &config, now_ms, &mut backoff, send).await {
            warn!(%error, "Could not forward plugin events");
        }
        drop(backoff);
        let rechecked = self.rechecked.load(Ordering::Relaxed);
        if now_ms.saturating_sub(rechecked) >= RECHECK_MS {
            self.rechecked.store(now_ms, Ordering::Relaxed);
            let waiting = recheck_waiting(&self.ctx, &config, self.local, now_ms).await;
            if let Err(error) = waiting {
                warn!(%error, "Could not recheck waiting plugin events");
            }
        }
        let active = self.supervisor.active();
        let claim = ClaimRuns::new(Arc::new(config), self.local, active, now_ms);
        let claimed = drive(claim, &self.ctx).await;
        let ClaimedRuns { runs, failure } = claimed.map_err(|error| error.to_string())?;
        let started = runs.len();
        for run in runs {
            let cancel = self.supervisor.admit(run.run_id, run.install_id);
            let worker = self.clone();
            self.supervisor
                .spawn(async move { worker.execute(run, cancel).await });
        }
        match failure {
            Some(error) => Err(error.to_string()),
            None => Ok(started),
        }
    }

    /// Delivers the ends of jobs whose starting run succeeded as `job_finished` runs.
    async fn notify(&self, now_ms: u64) -> Result<usize, String> {
        let config = read_config(&self.ctx, self.realm_id).await;
        let config = config.map_err(|error| error.to_string())?;
        let notified = notify_work(&self.ctx, &config, self.local, now_ms).await;
        notified.map_err(|error| error.to_string())
    }

    async fn sweep(&self, now_ms: u64) -> Result<usize, String> {
        let config = read_config(&self.ctx, self.realm_id).await;
        let config = config.map_err(|error| error.to_string())?;
        let swept = drive(sweep_events(&config, now_ms), &self.ctx).await;
        let swept = swept.map_err(|error| error.to_string())?;
        let hooks = drive(prune_hooks(&config, now_ms), &self.ctx).await;
        let hooks = hooks.map_err(|error| error.to_string())?;
        let retired = retire_work(&self.ctx, &config, now_ms).await;
        Ok(swept + hooks + retired.map_err(|error| error.to_string())?)
    }

    /// Hands a batch to its receiver; this node decides the batches of the installs it runs.
    async fn forward(
        &self,
        node: NodeId,
        batch: EventBatch,
        (config, now_ms): (&RealmConfigDocument, u64),
    ) -> Result<Decisions, String> {
        if node == self.local {
            let nodes = (self.local, self.local);
            let decided = decide_batch(&self.ctx, config, nodes, batch, now_ms);
            return decided.await.map_err(|error| error.to_string());
        }
        let decided = drive(SendEvents::new(node, batch), &self.ctx).await;
        decided.map_err(|error| format!("{error:?}"))
    }

    /// Cancels the running guests of installs that may no longer run.
    fn revoke(&self, config: &RealmConfigDocument) {
        let reason = |install_id| revocation(config, self.local, install_id);
        self.supervisor.revoke(reason);
    }

    /// Stops claiming, cancels every running guest and waits until each attempt ended.
    pub async fn stop(&self) {
        self.supervisor.stop().await;
    }

    /// Waits until the attempts started so far ended.
    pub async fn settle(&self) {
        self.supervisor.settle().await;
    }

    async fn execute(self: Arc<Self>, mut run: PluginRun, cancel: CancellationToken) {
        let scope = RunScope {
            realm_id: self.realm_id,
            local: self.local,
            run: run.clone(),
        };
        let broker = Arc::new(RunBroker::new(self.ctx.clone(), scope));
        let ended = self.attempt(&run, broker.clone(), cancel).await;
        let run_id = run.run_id;
        if matches!(ended, Ended::Stopped) {
            self.supervisor.finish(run_id);
            return;
        }
        run.log = broker.take_log();
        let ended = match scrub_ended(&broker, ended) {
            Ended::Cancelled(message) => {
                let reason = self.supervisor.take_reason(run_id);
                Ended::Cancelled(reason.map_or(message, str::to_string))
            }
            other => other,
        };
        let now = unix_timestamp_millis();
        let finish = FinishRun::new(run, ended, broker.changed(), now);
        if let Err(error) = drive(finish, &self.ctx).await {
            warn!(run = %run_id, %error, "Could not save a finished plugin run");
        }
        self.supervisor.finish(run_id);
    }

    async fn attempt(
        &self,
        run: &PluginRun,
        broker: Arc<RunBroker>,
        cancel: CancellationToken,
    ) -> Ended {
        let begin = BeginAttempt::new((self.realm_id, self.local), run);
        let AttemptStart { config, install } = match drive(begin, &self.ctx).await {
            Ok(start) => start,
            Err(ended) => return ended,
        };
        let local = Some(self.local);
        let mut assembled = match ready_artifact(&self.ctx, &config, &run.digest, local).await {
            Ok(assembled) => assembled,
            Err(error) => return Ended::Retry(format!("the artifact is not available: {error}")),
        };
        // The artifact's memory stays reserved until it is compiled.
        let bytes = std::mem::take(&mut assembled.bytes);
        let compiled = self.supervisor.compile(bytes).await;
        drop(assembled);
        let compiled = match compiled {
            Ok(compiled) => compiled,
            Err(error) => return Ended::Retry(format!("the artifact does not compile: {error}")),
        };
        let limits = self.supervisor.limits();
        let call = invocation(run, (&install, self.local), limits, cancel);
        let invoked = self.supervisor.invoke(&compiled, call, broker).await;
        ended(invoked, self.supervisor.stopping())
    }
}

/// The attempt's end with every value handed to the code removed from the text the run keeps.
fn scrub_ended(broker: &RunBroker, ended: Ended) -> Ended {
    match ended {
        Ended::Output(output) => Ended::Output(output.map(|text| broker.scrub(&text))),
        Ended::Failed(message) => Ended::Failed(broker.scrub(&message)),
        Ended::Retry(message) => Ended::Retry(broker.scrub(&message)),
        Ended::Cancelled(message) => Ended::Cancelled(broker.scrub(&message)),
        Ended::Busy(message) => Ended::Busy(broker.scrub(&message)),
        Ended::Stopped => Ended::Stopped,
    }
}

/// Matches every pending journal row of this node with at most `bound` forward rows per install;
/// returns how many rows were read.
pub(crate) async fn match_journal(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    (now_ms, bound): (u64, usize),
) -> Result<usize, StoreError> {
    let targets = match_targets(config, local);
    drive(MatchJournal::new(targets, now_ms, bound), ctx).await
}

/// Removes the runs past their retention; answers how many went.
pub(crate) async fn prune_runs(ctx: &DriverContext, now_ms: u64) -> Result<usize, RunError> {
    drive(PruneRuns::new(now_ms), ctx).await
}

/// Deletes the use audit rows past their retention; answers how many records went.
pub(crate) async fn prune_audit(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    drive(PruneAudit::new(now_ms), ctx).await
}

/// Drops the marks nothing can need any more; answers how many went.
pub(crate) async fn sweep_marks(ctx: &DriverContext) -> Result<usize, StoreError> {
    drive(SweepMarks::new(), ctx).await
}

/// Decides each event of a batch from `sender` and keeps its newest gap reports.
pub(crate) async fn decide_batch(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    nodes: (NodeId, NodeId),
    batch: EventBatch,
    now_ms: u64,
) -> Result<Decisions, StoreError> {
    let decide = DecideBatch::new(Arc::new(config.clone()), nodes, batch, now_ms);
    drive(decide, ctx).await
}

/// Sends each reachable receiver its batches and applies their decisions; returns how many
/// rows were decided. Rows of removed installs are dropped, rows past their send expiry become
/// gaps.
pub(crate) async fn send_outbox<F: Future<Output = Result<Decisions, String>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
    backoff: &mut Backoff,
    send: impl Fn(NodeId, EventBatch) -> F,
) -> Result<usize, StoreError> {
    send_bounded(ctx, config, (now_ms, REQUEST_BYTES), backoff, send).await
}

pub(crate) async fn send_bounded<F: Future<Output = Result<Decisions, String>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    (now_ms, bound): (u64, usize),
    backoff: &mut Backoff,
    send: impl Fn(NodeId, EventBatch) -> F,
) -> Result<usize, StoreError> {
    let open = (config.installs.iter())
        .map(|install| install.receiver)
        .filter(|node| lost_reason(config, *node).is_none() && backoff.ready(*node, now_ms))
        .collect();
    let metadata = ctx.net_handle.is_some();
    let collect = CollectOutbox::new(Arc::new(config.clone()), open, (now_ms, bound), metadata);
    let batches = drive(collect, ctx).await?;
    let mut decided = 0;
    for (receiver, requests) in batches {
        let mut failed = false;
        for ((batch, keys), _) in requests.open {
            let Ok(decisions) = send(receiver, batch).await else {
                failed = true;
                break;
            };
            let (count, busy) = drive(apply(&keys, decisions), ctx).await?;
            decided += count;
            if busy {
                failed = true;
                break;
            }
        }
        match failed {
            true => backoff.failed(receiver, now_ms),
            false => backoff.cleared(receiver),
        }
    }
    Ok(decided)
}

/// Admits a `job_finished` run for every settled work whose starting run succeeded and marks the
/// work notified in the same transaction; a manifest without the trigger is notified without a
/// run. Answers how many works were notified.
pub(crate) async fn notify_work(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, RunError> {
    let due = DueCompletions::new(Arc::new(config.clone()), local, now_ms);
    let due = drive(due, ctx).await.map_err(RunError::from)?;
    let mut notified = 0;
    for notify in due {
        let work = notify.work_id();
        match drive(notify, ctx).await {
            Ok(true) => notified += 1,
            Ok(false) => {}
            // A full queue takes the completion at a later pass.
            Err(RunError::Full(_)) => {}
            Err(error) => {
                warn!(%error, %work, "Could not deliver a finished plugin job")
            }
        }
    }
    Ok(notified)
}

/// Decides each waiting event that is ready or expired; returns how many left the waiting set.
pub(crate) async fn recheck_waiting(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, StoreError> {
    let recheck = RecheckWaiting::new(Arc::new(config.clone()), local, now_ms);
    drive(recheck, ctx).await
}

/// Reads another node's clock with the times around the round trip.
async fn probe_clock(ctx: &DriverContext, node: NodeId) -> Option<ClockReading> {
    let sent_ms = unix_timestamp_millis();
    let remote_ms = drive(AskClock(node), ctx).await.ok()??;
    let received_ms = unix_timestamp_millis();
    Some(ClockReading {
        sent_ms,
        remote_ms,
        received_ms,
    })
}

/// Starts a replay for each active install whose coverage grew here since its coverage row;
/// `probe` reads another node's clock. Returns how many replays started.
pub(crate) async fn plan_replays<F: Future<Output = Option<ClockReading>>>(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
    probe: impl Fn(NodeId) -> F,
) -> Result<usize, StoreError> {
    if matches!(node_kind(config, local), Some(RealmNodeKind::User { .. })) {
        return Ok(0);
    }
    let mut started = 0;
    for target in targets(config) {
        let id = target.install_id;
        let Some(install) = config.installs.iter().find(|i| i.install_id == id) else {
            continue;
        };
        let plan = PlanReplay::new(install.clone(), target);
        let ReplayPlan::Start {
            target,
            next,
            old,
            boundary,
            by,
        } = drive(plan, ctx).await?
        else {
            continue;
        };
        let start = match by {
            Some(node) if node == local => Some(boundary.saturating_sub(SKEW_MARGIN_MS)),
            Some(node) => local_start(boundary, probe(node).await),
            None => None,
        };
        let replay = StartReplay::new(*target, (*next, old), (start, by), now_ms);
        drive(replay, ctx).await?;
        started += 1;
    }
    Ok(started)
}

/// Continues every replay, at most `pages` pages of `page` rows each; returns the rows read.
pub(crate) async fn run_replays(
    ctx: &DriverContext,
    now_ms: u64,
    pages: (usize, usize),
) -> Result<usize, StoreError> {
    drive(RunReplays::new(now_ms, pages), ctx).await
}

/// Runs the worker until the node stops: it claims due runs every tick, then stops the guests.
pub fn spawn_worker(
    ctx: Arc<DriverContext>,
    realm_id: RealmId,
    local: NodeId,
    shutdown: &Shutdown,
) {
    let worker = match Worker::new(ctx, realm_id, local) {
        Ok(worker) => worker,
        Err(error) => {
            warn!(%error, "The plugin runtime did not start; plugin runs wait");
            return;
        }
    };
    let token = shutdown.token();
    shutdown.spawn(async move {
        let (mut pruned_at, mut settled_at) = (0, 0);
        loop {
            let now = unix_timestamp_millis();
            if let Err(error) = worker.claim(now).await {
                warn!(%error, "Could not start plugin runs");
            }
            if now.saturating_sub(settled_at) >= SETTLE_MS {
                settled_at = now;
                if let Err(error) = settle_work(&worker.ctx, now).await {
                    warn!(%error, "Could not keep the summaries of finished plugin work");
                }
                if let Err(error) = worker.notify(now).await {
                    warn!(%error, "Could not deliver finished plugin jobs");
                }
            }
            if now.saturating_sub(pruned_at) >= PRUNE_MS {
                pruned_at = now;
                if let Err(error) = prune_runs(&worker.ctx, now).await {
                    warn!(%error, "Could not prune finished plugin runs");
                }
                if let Err(error) = drive(prune_journal(now), &worker.ctx).await {
                    warn!(%error, "Could not prune the plugin event journal");
                }
                if let Err(error) = drive(prune_seen(now), &worker.ctx).await {
                    warn!(%error, "Could not prune plugin event dedup records");
                }
                if let Err(error) = prune_audit(&worker.ctx, now).await {
                    warn!(%error, "Could not prune plugin secret use audit rows");
                }
                if let Err(error) = drive(prune_work(now), &worker.ctx).await {
                    warn!(%error, "Could not prune settled plugin work");
                }
                if let Err(error) = sweep_marks(&worker.ctx).await {
                    warn!(%error, "Could not drop the marks of absent object versions");
                }
                if let Err(error) = worker.sweep(now).await {
                    warn!(%error, "Could not delete stale plugin event rows");
                }
            }
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(TICK) => {}
            }
        }
        worker.stop().await;
    });
}

#[cfg(test)]
#[path = "worker_tests.rs"]
pub(crate) mod tests;
