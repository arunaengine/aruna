//! Runs the queued runs of the installs this node receives: claims due runs within each
//! install's bound, compiles the pinned artifact, invokes the handler and finishes the run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use aruna_core::NodeId;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, PluginRun, RUNS_PARALLEL, RunState, RunTrigger, SourceEvent,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::ErrorKind;
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::permission::{Grant, Permission};
use aruna_plugins::{
    CancellationToken, Field, Invocation, Runtime, Trigger, WatchEvent, WebhookDelivery,
    WebhookRequest,
};
use aruna_tasks::shutdown::Shutdown;
use tokio_util::task::TaskTracker;
use tracing::warn;
use ulid::Ulid;

use super::artifacts::fetch_artifact;
use super::artifacts::local_config;
use super::broker::RunBroker;
use super::check::{CallError, CheckCall, RunScope};
use super::cleanup::sweep_events;
use super::clock::{ClockReading, SKEW_MARGIN_MS, local_start, probe_clock};
use super::decide::{DecideBatch, prune_seen};
use super::dispatch::enqueue_timers;
use super::finished::DueCompletions;
use super::forward::{Backoff, CollectOutbox, REQUEST_BYTES, apply};
use super::grants::{parse_grants, patterns_for, short_path};
use super::install::read_config;
use super::intents::open_intent;
use super::jobs::{retire_work, settle_work};
use super::journal::JOURNAL_PAGE;
use super::journal::prune_journal;
use super::lifecycle::{match_targets, targets};
use super::marks::SweepMarks;
use super::matcher::{MatchJournal, event_trigger};
use super::outbound::Outbound;
use super::prune::{PruneAudit, prune_hooks};
use super::receiver::{eligible, lost_reason};
use super::replay::{PlanReplay, ReplayPlan, RunReplays, StartReplay};
use super::rows::StoreError;
use super::runs::{
    ADMISSIONS, Ended, PruneRuns, RunError, TICK, conclude, ended, read_run, save_run, unfinished,
};
use super::waiting::RecheckWaiting;
use super::work::prune_work;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataReadError;
use crate::realm::mutate_placement::node_kind;
use aruna_core::plugin_runs::{Decisions, EventBatch};

/// How often the worker rechecks the events waiting for their install record or account.
const RECHECK_MS: u64 = 10 * 1000;
/// How often the worker prunes runs that ended before the retention period.
const PRUNE_MS: u64 = 3600 * 1000;
/// How often the worker keeps the summaries of finished plugin work.
const SETTLE_MS: u64 = 30 * 1000;

/// The node service that owns the plugin runtime and the runs it executes.
pub struct Worker {
    ctx: Arc<DriverContext>,
    realm_id: RealmId,
    local: NodeId,
    runtime: Runtime,
    /// The budget of each run.
    limits: Limits,
    /// Runs executing now, with their install and cancel token.
    running: Mutex<HashMap<Ulid, (Ulid, CancellationToken)>>,
    /// Why a running guest was cancelled, by run.
    reasons: Mutex<HashMap<Ulid, &'static str>>,
    /// When each receiver of forwarded events is tried again.
    backoff: tokio::sync::Mutex<Backoff>,
    /// Sends connection requests instead of the node's egress client, in tests.
    outbound: Mutex<Option<Arc<dyn Outbound>>>,
    /// When the waiting events were last rechecked.
    rechecked: AtomicU64,
    tasks: TaskTracker,
    stopping: CancellationToken,
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
        let runtime = Runtime::new(config).map_err(|error| error.to_string())?;
        Ok(Arc::new(Self {
            ctx,
            realm_id,
            local,
            runtime,
            limits,
            running: Mutex::new(HashMap::new()),
            reasons: Mutex::new(HashMap::new()),
            backoff: tokio::sync::Mutex::default(),
            outbound: Mutex::new(None),
            rechecked: AtomicU64::new(0),
            tasks: TaskTracker::new(),
            stopping: CancellationToken::new(),
        }))
    }

    fn running(&self) -> std::sync::MutexGuard<'_, HashMap<Ulid, (Ulid, CancellationToken)>> {
        self.running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Queues due timer occurrences, then starts the due runs of the active installs this node
    /// receives, at most `RUNS_PARALLEL` per install. A `running` run no task runs starts again.
    pub async fn claim(self: &Arc<Self>, now_ms: u64) -> Result<usize, String> {
        if self.stopping.is_cancelled() {
            return Ok(0);
        }
        let config = read_config(&self.ctx, self.realm_id)
            .await
            .map_err(|error| error.to_string())?;
        self.revoke(&config);
        let timers = enqueue_timers(&self.ctx, &config, self.local, now_ms).await;
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
        let queued = drive(unfinished(), &self.ctx)
            .await
            .map_err(|error| error.to_string())?;
        let here = eligible(&config, self.local);
        let mut started = 0;
        for (install_id, run_id) in queued {
            if self.running().contains_key(&run_id) {
                continue;
            }
            let Some(install) =
                (config.installs.iter()).find(|install| install.install_id == install_id)
            else {
                self.drop_run(install_id, run_id, now_ms).await?;
                continue;
            };
            let busy = (self.running().values())
                .filter(|(install, _)| *install == install_id)
                .count();
            let ready = install.receiver == self.local && install.state == InstallState::Active;
            if !ready || !here || busy >= RUNS_PARALLEL {
                continue;
            }
            let mut run = drive(read_run(install_id, run_id), &self.ctx)
                .await
                .map_err(|error| error.to_string())?;
            if run.due_ms > now_ms || run.state.finished() {
                continue;
            }
            run.state = RunState::Running;
            run.attempts += 1;
            self.save(&run).await.map_err(|error| error.to_string())?;
            let cancel = CancellationToken::new();
            self.running().insert(run_id, (install_id, cancel.clone()));
            let worker = self.clone();
            self.tasks
                .spawn(async move { worker.execute(run, cancel).await });
            started += 1;
        }
        Ok(started)
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

    /// Sends connection requests through `outbound` instead of the node's egress client.
    pub fn use_outbound(&self, outbound: Arc<dyn Outbound>) {
        if let Ok(mut held) = self.outbound.lock() {
            *held = Some(outbound);
        }
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
        let handle = self.ctx.metadata_handle.as_ref();
        let handle = handle.ok_or("the metadata transport is not running")?;
        let decided = handle.request_forward(node, batch).await;
        decided.map_err(|error| format!("{error:?}"))
    }

    /// Cancels the running guests of installs that were paused or removed, or of every install
    /// while this node can no longer run installs; other changes apply at the next call.
    fn revoke(&self, config: &RealmConfigDocument) {
        let lost = lost_reason(config, self.local);
        let running = self.running();
        for (run_id, (install_id, cancel)) in running.iter() {
            let install =
                (config.installs.iter()).find(|install| install.install_id == *install_id);
            let reason = match install {
                None => Some("the install was removed"),
                Some(install) if install.state == InstallState::Removing => {
                    Some("the install was removed")
                }
                Some(_) if lost.is_some() => lost,
                Some(install) if install.paused => Some("the install was paused"),
                Some(_) => None,
            };
            if let Some(reason) = reason {
                self.reasons().insert(*run_id, reason);
                cancel.cancel();
            }
        }
    }

    fn reasons(&self) -> std::sync::MutexGuard<'_, HashMap<Ulid, &'static str>> {
        self.reasons
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Ends a queued run whose install is gone, unless its intents decide otherwise.
    async fn drop_run(&self, install_id: Ulid, run_id: Ulid, now_ms: u64) -> Result<(), String> {
        let mut run = drive(read_run(install_id, run_id), &self.ctx)
            .await
            .map_err(|error| error.to_string())?;
        let open = drive(open_intent(run_id), &self.ctx).await;
        let ended = Ended::Cancelled("the install was removed".to_string());
        conclude(&mut run, ended, false, open, now_ms);
        self.save(&run).await.map_err(|error| error.to_string())
    }

    /// Writes the run; one that ended leaves the queue in the same transaction.
    async fn save(&self, run: &PluginRun) -> Result<(), RunError> {
        Ok(drive(save_run(run)?, &self.ctx).await?)
    }

    /// Stops claiming, cancels every running guest and waits until each attempt ended.
    pub async fn stop(&self) {
        self.stopping.cancel();
        for (_, cancel) in self.running().values() {
            cancel.cancel();
        }
        self.tasks.close();
        self.tasks.wait().await;
        self.runtime.close();
    }

    /// Waits until the attempts started so far ended.
    pub async fn settle(&self) {
        self.tasks.close();
        self.tasks.wait().await;
        self.tasks.reopen();
    }

    async fn execute(self: Arc<Self>, mut run: PluginRun, cancel: CancellationToken) {
        let scope = RunScope {
            realm_id: self.realm_id,
            local: self.local,
            run: run.clone(),
        };
        let mut broker = RunBroker::new(self.ctx.clone(), scope);
        let outbound = self.outbound.lock().ok().and_then(|held| held.clone());
        if let Some(outbound) = outbound {
            broker = broker.with_outbound(outbound);
        }
        let broker = Arc::new(broker);
        let ended = self.attempt(&run, broker.clone(), cancel).await;
        if matches!(ended, Ended::Stopped) {
            self.running().remove(&run.run_id);
            return;
        }
        run.log = broker.take_log();
        let ended = scrub_ended(&broker, ended);
        let now = unix_timestamp_millis();
        let open = drive(open_intent(run.run_id), &self.ctx).await;
        let ended = match ended {
            Ended::Cancelled(message) => {
                let reason = self.reasons().remove(&run.run_id);
                Ended::Cancelled(reason.map_or(message, str::to_string))
            }
            other => other,
        };
        conclude(&mut run, ended, broker.changed(), open, now);
        if let Err(error) = self.save(&run).await {
            warn!(run = %run.run_id, %error, "Could not save a finished plugin run");
        }
        self.reasons().remove(&run.run_id);
        self.running().remove(&run.run_id);
    }

    /// The trigger check again for an event run's path before each attempt, so a grant removed
    /// after admission cancels the run before the guest sees the event.
    async fn event_allowed(&self, run: &PluginRun, event: &SourceEvent) -> Result<(), Ended> {
        let scope = RunScope {
            realm_id: self.realm_id,
            local: self.local,
            run: run.clone(),
        };
        let needs = event_trigger(event.kind).needs();
        let target = needs.map(|permission| (permission, event.path.as_str()));
        match drive(CheckCall::new(&scope, target), &self.ctx).await {
            Ok(_) => Ok(()),
            Err(CallError(error)) if error.kind == ErrorKind::Unavailable => {
                Err(Ended::Retry(error.message))
            }
            Err(CallError(error)) => Err(Ended::Cancelled(error.message)),
        }
    }

    async fn attempt(
        &self,
        run: &PluginRun,
        broker: Arc<RunBroker>,
        cancel: CancellationToken,
    ) -> Ended {
        let config = match read_config(&self.ctx, self.realm_id).await {
            Ok(config) => config,
            Err(error) => return Ended::Retry(error.to_string()),
        };
        let Some(install) = config
            .installs
            .iter()
            .find(|install| install.install_id == run.install_id)
        else {
            return Ended::Cancelled("the install was removed".into());
        };
        if let RunTrigger::Event(event) = &run.trigger
            && let Err(refused) = self.event_allowed(run, event).await
        {
            return refused;
        }
        let mut assembled = match fetch_artifact(&self.ctx, &config, &run.digest).await {
            Ok(assembled) => assembled,
            Err(error) => return Ended::Retry(format!("the artifact is not available: {error}")),
        };
        // The artifact's memory stays reserved until it is compiled.
        let compiled = self
            .runtime
            .prepare(std::mem::take(&mut assembled.bytes))
            .await;
        drop(assembled);
        let compiled = match compiled {
            Ok(compiled) => compiled,
            Err(error) => return Ended::Retry(format!("the artifact does not compile: {error}")),
        };
        let call = invocation(run, (install, self.local), self.limits, cancel);
        let invoked = self.runtime.invoke(&compiled, call, broker).await;
        ended(invoked, self.stopping.is_cancelled())
    }
}

/// The handler call of a run: its saved invocation and the install's grants. An event's object on
/// this node is named by its short `bucket/key` path; `path` keeps the permission path.
fn invocation(
    run: &PluginRun,
    (install, local): (&PluginInstall, NodeId),
    limits: Limits,
    cancel: CancellationToken,
) -> Invocation {
    let grants = parse_grants(&install.grants);
    let permissions: BTreeSet<Permission> = (Permission::ALL.into_iter())
        .filter(|permission| !patterns_for(&grants, *permission).is_empty())
        .collect();
    // A service's grant path ends in the connection's name.
    let connections = (grants.get(&Grant::Services).into_iter().flatten())
        .filter_map(|path| path.rsplit('/').next().map(str::to_string))
        .collect();
    let trigger = match &run.trigger {
        RunTrigger::Manual { input, .. } => Trigger::Manual(input.clone()),
        RunTrigger::Timer { occurrence_ms } => Trigger::Timer(*occurrence_ms),
        RunTrigger::Event(event) => Trigger::Event(WatchEvent {
            kind: event.kind.name().into(),
            path: event.path.clone(),
            resource: short_path(&event.resource, (install, local)),
            revision: event.revision.clone(),
        }),
        RunTrigger::Webhook(hook) => Trigger::Webhook(WebhookDelivery {
            delivery_id: hook.delivery_id.clone(),
            request: WebhookRequest {
                method: hook.method.clone(),
                query: hook.query.clone(),
                headers: (hook.headers.iter())
                    .map(|(name, value)| Field {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                body: hook.body.clone(),
            },
        }),
    };
    Invocation {
        run: run.run_id.to_string(),
        install: run.install_id.to_string(),
        group: install.group_id.to_string(),
        settings: run.settings.clone(),
        trigger,
        permissions,
        connections,
        limits,
        cancel,
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

/// The receiver side of a forward from an infrastructure peer.
pub(crate) async fn serve_events(
    ctx: &DriverContext,
    peer: NodeId,
    batch: EventBatch,
) -> Result<Decisions, MetadataReadError> {
    let (config, local) = local_config(ctx).await?;
    if !node_kind(&config, peer).is_some_and(|kind| kind.is_sync_eligible()) {
        return Err(MetadataReadError::Forbidden);
    }
    let decided = decide_batch(ctx, &config, (local, peer), batch, unix_timestamp_millis());
    decided.await.map_err(|_| MetadataReadError::Unavailable)
}

/// Decides each event of a batch from `sender` and keeps its newest gap reports.
pub(crate) async fn decide_batch(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    nodes: (NodeId, NodeId),
    batch: EventBatch,
    now_ms: u64,
) -> Result<Decisions, StoreError> {
    let _admitting = ADMISSIONS.lock().await;
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
    let metadata = ctx.metadata_handle.is_some();
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
        let _admitting = ADMISSIONS.lock().await;
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
    let _admitting = ADMISSIONS.lock().await;
    let recheck = RecheckWaiting::new(Arc::new(config.clone()), local, now_ms);
    drive(recheck, ctx).await
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
