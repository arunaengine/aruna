//! Runs the queued runs of the installs this node receives: claims due runs within each
//! install's bound, compiles the pinned artifact, invokes the handler and finishes the run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::NodeId;
use aruna_core::plugin_runs::{EVENTS_QUEUED, PluginRun};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::{CancellationToken, Supervisor};
use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use super::broker::RunBroker;
use super::{read_config, ready_artifact};
use crate::driver::{DriverContext, drive};
use crate::plugins::attempt::{
    AttemptStart, BeginAttempt, ClaimRuns, ClaimedRuns, FinishRun, invocation, revocation,
};
use crate::plugins::check::RunScope;
use crate::plugins::cleanup::sweep_events;
use crate::plugins::decide::prune_seen;
use crate::plugins::dispatch::EnqueueTimers;
use crate::plugins::finished::NotifyDue;
use crate::plugins::forward::{Backoff, OutboxRoute, SendOutbox};
use crate::plugins::job_work::{RetireWork, SettleWork};
use crate::plugins::journal::JOURNAL_PAGE;
use crate::plugins::journal::prune_journal;
use crate::plugins::lifecycle::match_targets;
use crate::plugins::marks::SweepMarks;
use crate::plugins::matcher::MatchJournal;
use crate::plugins::prune::{PruneAudit, prune_hooks};
use crate::plugins::replay::{PlanReplays, RunReplays};
use crate::plugins::runs::{Ended, PruneRuns, TICK, ended};
use crate::plugins::waiting::RecheckWaiting;
use crate::plugins::work::prune_work;

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
        let shared = Arc::new(config.clone());
        let plan = PlanReplays::new(shared.clone(), self.local, now_ms);
        if let Err(error) = drive(plan, &self.ctx).await {
            warn!(%error, "Could not plan plugin event replays");
        }
        let targets = match_targets(&config, self.local);
        let matched = MatchJournal::new(targets, now_ms, EVENTS_QUEUED);
        if let Err(error) = drive(matched, &self.ctx).await {
            warn!(%error, "Could not match plugin events to installs");
        }
        let replays = RunReplays::new(now_ms, (JOURNAL_PAGE, usize::MAX));
        if let Err(error) = drive(replays, &self.ctx).await {
            warn!(%error, "Could not replay plugin events");
        }
        self.forward(&shared, now_ms).await;
        let rechecked = self.rechecked.load(Ordering::Relaxed);
        if now_ms.saturating_sub(rechecked) >= RECHECK_MS {
            self.rechecked.store(now_ms, Ordering::Relaxed);
            let recheck = RecheckWaiting::new(shared.clone(), self.local, now_ms);
            if let Err(error) = drive(recheck, &self.ctx).await {
                warn!(%error, "Could not recheck waiting plugin events");
            }
        }
        let active = self.supervisor.active();
        let claim = ClaimRuns::new(shared, self.local, active, now_ms);
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
        let notify = NotifyDue::new(Arc::new(config), self.local, now_ms);
        let notified = drive(notify, &self.ctx).await;
        notified.map_err(|error| error.to_string())
    }

    async fn sweep(&self, now_ms: u64) -> Result<usize, String> {
        let config = read_config(&self.ctx, self.realm_id).await;
        let config = config.map_err(|error| error.to_string())?;
        let swept = drive(sweep_events(&config, now_ms), &self.ctx).await;
        let swept = swept.map_err(|error| error.to_string())?;
        let hooks = drive(prune_hooks(&config, now_ms), &self.ctx).await;
        let hooks = hooks.map_err(|error| error.to_string())?;
        let retired = drive(RetireWork::new(&config, now_ms), &self.ctx).await;
        Ok(swept + hooks + retired.map_err(|error| error.to_string())?)
    }

    /// Sends the outbox: this node's own batches first, then the other receivers'.
    async fn forward(&self, config: &Arc<RealmConfigDocument>, now_ms: u64) {
        let mut backoff = self.backoff.lock().await;
        let metadata = self.ctx.net_handle.is_some();
        for route in [OutboxRoute::Local, OutboxRoute::Remote] {
            let place = (route, self.local);
            let waits = std::mem::take(&mut *backoff);
            let send = SendOutbox::new(config.clone(), place, (now_ms, metadata), waits);
            match drive(send, &self.ctx).await {
                Ok(pass) => {
                    *backoff = pass.backoff;
                    if let Some(error) = pass.failure {
                        warn!(%error, "Could not forward plugin events");
                    }
                }
                Err(error) => warn!(%error, "Could not forward plugin events"),
            }
        }
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
                if let Err(error) = drive(SettleWork::new(now), &worker.ctx).await {
                    warn!(%error, "Could not keep the summaries of finished plugin work");
                }
                if let Err(error) = worker.notify(now).await {
                    warn!(%error, "Could not deliver finished plugin jobs");
                }
            }
            if now.saturating_sub(pruned_at) >= PRUNE_MS {
                pruned_at = now;
                if let Err(error) = drive(PruneRuns::new(now), &worker.ctx).await {
                    warn!(%error, "Could not prune finished plugin runs");
                }
                if let Err(error) = drive(prune_journal(now), &worker.ctx).await {
                    warn!(%error, "Could not prune the plugin event journal");
                }
                if let Err(error) = drive(prune_seen(now), &worker.ctx).await {
                    warn!(%error, "Could not prune plugin event dedup records");
                }
                if let Err(error) = drive(PruneAudit::new(now), &worker.ctx).await {
                    warn!(%error, "Could not prune plugin secret use audit rows");
                }
                if let Err(error) = drive(prune_work(now), &worker.ctx).await {
                    warn!(%error, "Could not prune settled plugin work");
                }
                if let Err(error) = drive(SweepMarks::new(), &worker.ctx).await {
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
