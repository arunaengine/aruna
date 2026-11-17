//! Runs the queued runs of the installs this node receives: claims due runs within each
//! install's bound, compiles the pinned artifact, invokes the handler and finishes the run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, MAX_ATTEMPTS, PluginRun, RUNS_PARALLEL, RunState, RunTrigger, SourceEvent,
};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::ErrorKind;
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::permission::{Grant, Permission};
use aruna_plugins::{
    CancellationToken, FailureKind, Field, Invocation, InvokeError, Runtime, Trigger, WatchEvent,
    WebhookDelivery, WebhookRequest,
};
use tokio_util::task::TaskTracker;
use tracing::warn;
use ulid::Ulid;

use super::artifacts::fetch_artifact;
use super::broker::{RunBroker, absent, short_path};
use super::check::{RunScope, check_call};
use super::cleanup::sweep_events;
use super::clock::probe_clock;
use super::decide::{decide_batch, prune_seen};
use super::dispatch::enqueue_timers;
use super::finished::notify_work;
use super::forward::{Backoff, send_outbox};
use super::grants::{parse_grants, patterns_for};
use super::install::read_config;
use super::intents::open_intent;
use super::jobs::{retire_work, settle_work};
use super::journal::JOURNAL_PAGE;
use super::journal::prune_journal;
use super::lifecycle::match_targets;
use super::marks::{MARK_ROW, Mark, MarkEnded};
use super::matcher::event_trigger;
use super::matcher::match_page;
use super::outbound::Outbound;
use super::prune::{self, prune_hooks, prune_uses};
use super::receiver::{eligible, lost_reason};
use super::release::ReleaseKeys;
use super::replay::{plan_replays, replay_page, replay_scan};
use super::rows::{StoreError, decode, page_rows, scan_rows};
use super::runs::{RunError, prune_runs, read_run, save_run, unfinished};
use super::waiting::recheck_waiting;
use super::work::prune_work;
use crate::driver::{DriverContext, drive};
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use aruna_core::plugin_runs::{Decisions, EventBatch};

/// How often the worker looks for due runs.
pub const TICK: Duration = Duration::from_secs(1);
/// How often the worker rechecks the events waiting for their install record or account.
const RECHECK_MS: u64 = 10 * 1000;
/// How often the worker prunes runs that ended before the retention period.
const PRUNE_MS: u64 = 3600 * 1000;
/// How often the worker keeps the summaries of finished plugin work.
const SETTLE_MS: u64 = 30 * 1000;
/// Longest wait before a run that failed in the host starts again.
const MAX_BACKOFF_MS: u64 = 10 * 60 * 1000;

/// How one attempt of a run ended, before its intents are inspected.
#[derive(Debug)]
enum Ended {
    Output(Option<String>),
    Failed(String),
    /// A host or infrastructure error: the run starts again later.
    Retry(String),
    Cancelled(String),
    /// The runtime had no free slot: the run waits without using an attempt.
    Busy(String),
    /// The node stops; the run starts again from the top at the next start.
    Stopped,
}

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
    /// Releases with these realm keys instead of the node's, in tests.
    keys: Mutex<Option<ReleaseKeys>>,
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
            keys: Mutex::new(None),
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

    /// Releases secrets with `keys` instead of the node's realm keys.
    pub fn use_keys(&self, keys: ReleaseKeys) {
        if let Ok(mut held) = self.keys.lock() {
            *held = Some(keys);
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
        if let Some(keys) = self.keys.lock().ok().and_then(|held| held.clone()) {
            broker = broker.with_keys(keys);
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
        match check_call(&self.ctx, &scope, target).await {
            Ok(_) => Ok(()),
            Err(error) if error.kind == ErrorKind::Unavailable => Err(Ended::Retry(error.message)),
            Err(error) => Err(Ended::Cancelled(error.message)),
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

/// How an invocation ended. A runtime without a free slot never entered the guest, so the run
/// waits without using an attempt.
fn ended(invoked: Result<Option<String>, InvokeError>, stopping: bool) -> Ended {
    match invoked {
        Ok(output) => Ended::Output(output),
        Err(InvokeError::Guest(failure)) => match failure.kind {
            FailureKind::Permanent | FailureKind::InvalidInput => Ended::Failed(failure.message),
            FailureKind::Retryable => Ended::Retry(failure.message),
        },
        Err(InvokeError::Cancelled) if stopping => Ended::Stopped,
        Err(InvokeError::Cancelled) => Ended::Cancelled("the run was cancelled".into()),
        Err(error @ (InvokeError::Overloaded | InvokeError::Closed)) => {
            Ended::Busy(error.to_string())
        }
        Err(error) => Ended::Failed(error.to_string()),
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

/// Ends or queues a run after an attempt. Its intents decide first: one pending or unknown
/// makes it need attention, and unreadable intents keep it queued without using an attempt.
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

fn conclude(
    run: &mut PluginRun,
    ended: Ended,
    changed: bool,
    open: Result<Option<String>, StoreError>,
    now_ms: u64,
) {
    match open {
        Ok(Some(reason)) => return run.finish(RunState::NeedsAttention, Some(reason), now_ms),
        Err(error) => return hold(run, format!("its calls are not readable: {error}"), now_ms),
        Ok(None) => {}
    }
    if changed {
        let message = "the run made a call that differs from its earlier attempt";
        return run.finish(RunState::Failed, Some(message.into()), now_ms);
    }
    match ended {
        Ended::Output(output) => {
            run.output = output;
            run.finish(RunState::Succeeded, None, now_ms);
        }
        Ended::Failed(message) => run.finish(RunState::Failed, Some(message), now_ms),
        Ended::Cancelled(message) => run.finish(RunState::Cancelled, Some(message), now_ms),
        Ended::Retry(message) => retry(run, message, now_ms),
        Ended::Busy(message) => hold(run, message, now_ms),
        Ended::Stopped => {}
    }
}

/// Queues the run again soon without counting the attempt that just ended.
fn hold(run: &mut PluginRun, message: String, now_ms: u64) {
    run.attempts = run.attempts.saturating_sub(1);
    run.state = RunState::Queued;
    run.message = Some(message);
    run.due_ms = now_ms.saturating_add(TICK.as_millis() as u64);
}

/// Queues the run again after a backoff, or fails it after its last attempt.
fn retry(run: &mut PluginRun, message: String, now_ms: u64) {
    if run.attempts >= MAX_ATTEMPTS {
        run.finish(RunState::Failed, Some(message), now_ms);
        return;
    }
    let backoff = 1000u64
        .saturating_mul(1 << run.attempts.min(20))
        .min(MAX_BACKOFF_MS);
    run.state = RunState::Queued;
    run.message = Some(message);
    run.due_ms = now_ms.saturating_add(backoff);
}

/// Matches every pending journal row of this node with at most `bound` forward rows per install,
/// a page per transaction; returns how many rows were read.
pub(crate) async fn match_journal(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    (now_ms, bound): (u64, usize),
) -> Result<usize, StoreError> {
    let (targets, keep) = match_targets(config, local);
    let mut read = 0;
    loop {
        let count = drive(match_page((&targets, keep), now_ms, bound), ctx).await?;
        read += count;
        if count < JOURNAL_PAGE {
            return Ok(read);
        }
    }
}

/// Deletes the use audit rows past their retention, a page per transaction; answers how many
/// records went.
pub(crate) async fn prune_audit(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let mut pruned = 0;
    loop {
        let (count, more) = drive(prune_uses(now_ms, prune::PAGE), ctx).await?;
        pruned += count;
        if !more {
            return Ok(pruned);
        }
    }
}

/// Marks one sweep pass reads at a time.
const MARK_PAGE: usize = 100;

/// Drops the marks nothing can need any more, a page at a time: the work that minted the
/// version can write no more, as its row is settled or gone, and only then the version is
/// absent. The delete reads the row again, so it conflicts with a settlement or reservation.
pub(crate) async fn sweep_marks(ctx: &DriverContext) -> Result<usize, StoreError> {
    let (mut dropped, mut after) = (0, None);
    loop {
        let page = page_rows(PLUGIN_WORK_KEYSPACE, vec![MARK_ROW], after, MARK_PAGE);
        let (rows, next) = drive(page, ctx).await?;
        for (key, value) in rows {
            let mark: Mark = decode(&value)?;
            let (bucket, object, version_id) = decode::<(String, String, Ulid)>(&key[1..])?;
            if !drive(MarkEnded::check(&mark, None), ctx).await? {
                continue;
            }
            let head = HeadObjectInput {
                bucket,
                key: object,
                version_id: Some(version_id),
            };
            match drive(HeadObjectOperation::new(head), ctx).await {
                Err(error) if absent(&error) => {}
                // Present, or not known now: the mark stays.
                _ => continue,
            }
            dropped += usize::from(drive(MarkEnded::check(&mark, Some(key)), ctx).await?);
        }
        match next {
            Some(key) => after = Some(key),
            None => return Ok(dropped),
        }
    }
}

/// Continues every replay, at most `pages` pages of `page` rows each, a page per transaction;
/// returns the rows read.
pub(crate) async fn run_replays(
    ctx: &DriverContext,
    now_ms: u64,
    (page, pages): (usize, usize),
) -> Result<usize, StoreError> {
    let mut read = 0;
    for (key, _) in drive(scan_rows(replay_scan()), ctx).await? {
        for _ in 0..pages {
            let (rows, ended) = drive(replay_page(key.clone(), now_ms, page), ctx).await?;
            read += rows;
            if ended {
                break;
            }
        }
    }
    Ok(read)
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
                if let Err(error) = drive(prune_runs(now), &worker.ctx).await {
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
