//! Runs the timer and task scheduler behind TaskHandle, with admission and ordered drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

mod pulls;
pub mod shutdown;
pub mod wipe;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::{error, warn};

const TASK_COMMAND_BUFFER: usize = 1024;
const HANDLER_WARN_AFTER: Duration = Duration::from_secs(30);
/// Handlers of distinct keys run at most this many at once; later deadlines wait their turn.
const HANDLER_LIMIT: usize = 64;

#[async_trait]
pub trait InboundTaskHandler: Send + Sync {
    async fn handle_timer(&self, key: TaskKey);

    /// Handles one firing; `stamp` names the durable row the firing timer was armed from, if a
    /// stamped change armed it.
    async fn handle_firing(&self, key: TaskKey, _stamp: Option<u64>) {
        self.handle_timer(key).await;
    }
}

#[derive(Clone)]
pub struct TaskHandle {
    command_tx: mpsc::Sender<TaskCommand>,
    admission_closed: Arc<AtomicBool>,
    pulls: Arc<pulls::PullRegistry>,
}

enum TaskCommand {
    SetInboundHandler {
        handler: Arc<dyn InboundTaskHandler>,
        response: oneshot::Sender<()>,
    },
    ClearInboundHandler {
        response: oneshot::Sender<()>,
    },
    ResetTimer {
        key: TaskKey,
        after: Duration,
        stamp: Option<u64>,
        /// The deadline planned before the row write; the wake keeps it however long this
        /// command waited, else it is due `after` from when the scheduler reads the command.
        deadline: Option<Instant>,
        response: oneshot::Sender<TaskEvent>,
    },
    ShortenTimer {
        key: TaskKey,
        after: Duration,
        stamp: Option<u64>,
        mode: Shorten,
        /// The plan this arming completes, from an earlier [`Shorten::Plan`].
        plan: Option<ShortenPlan>,
        /// The event, and the plan whose holder is the admission whose wake holds the key: this
        /// one when it armed, else the earlier one it kept.
        response: oneshot::Sender<(TaskEvent, Option<ShortenPlan>)>,
    },
    ScheduleIdleTimer {
        key: TaskKey,
        after: Duration,
        response: oneshot::Sender<TaskEvent>,
    },
    CancelTimer {
        key: TaskKey,
        response: oneshot::Sender<TaskEvent>,
    },
    AbortRunningHandlers {
        key: TaskKey,
        response: oneshot::Sender<TaskEvent>,
    },
    HandlerCompleted {
        run_id: u64,
        key: TaskKey,
        elapsed: Duration,
    },
    StopAdmission {
        response: oneshot::Sender<usize>,
    },
    AwaitDrained {
        response: oneshot::Sender<()>,
    },
    AbortAllHandlers {
        response: oneshot::Sender<usize>,
    },
    #[cfg(test)]
    RegisteredDrainWaiters {
        response: oneshot::Sender<usize>,
    },
}

/// What a shorten does with the wake of its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shorten {
    /// Arms the earlier wake.
    Arm,
    /// Only answers what arming would do.
    Plan,
    /// Arms; a kept earlier wake takes over the row `stamp` owns, so its firing deletes it.
    Claim,
}

/// A shorten planned before its durable row is written, so arming it afterwards neither starts
/// its delay again nor arms a second wake when the wake that held the key fired meanwhile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShortenPlan {
    /// The admission whose wake would hold the key: the planned one, or an earlier kept wake.
    pub holder: Option<u64>,
    deadline: Instant,
    /// The wake that held the key when planned, by timer id, with the admission it fires as.
    replaced: Option<(u64, Option<u64>)>,
}

/// Outcome of draining the scheduler's timer handlers on shutdown. This is the
/// handler boundary only: the scheduler loop itself terminates when the last
/// command sender drops, which every clone of the handle owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskShutdownReport {
    /// Handlers still running when admission stopped.
    pub in_flight: usize,
    /// Handlers force-stopped because they outlived the drain deadline. Their
    /// completion stays owned by the scheduler, so this is not a drain.
    pub aborted: usize,
    /// The scheduler never accepted or never acknowledged a command, so no
    /// handler lifecycle could be observed.
    pub scheduler_unavailable: bool,
}

impl TaskShutdownReport {
    /// True only when no handler had to be force-stopped and no
    /// acknowledgement was lost.
    pub fn drained(&self) -> bool {
        self.aborted == 0 && !self.scheduler_unavailable
    }
}

struct SchedulerState {
    admission_closed: Arc<AtomicBool>,
    timers_by_key: HashMap<TaskKey, TimerEntry>,
    timers_by_deadline: BTreeMap<(Instant, u64), TaskKey>,
    running_by_id: HashMap<u64, RunningTaskEntry>,
    running_warn_deadlines: BTreeSet<(Instant, u64)>,
    in_flight_keys: HashMap<TaskKey, usize>,
    /// Keys that fired while a run held them, with the stamp of that firing.
    refire_requested: HashMap<TaskKey, Option<u64>>,
    inbound_handler: Option<Arc<dyn InboundTaskHandler>>,
    drained_waiters: Vec<oneshot::Sender<()>>,
    next_timer_id: u64,
    next_run_id: u64,
    handler_limit: usize,
}

struct TimerEntry {
    id: u64,
    deadline: Instant,
    /// The durable row this timer was armed from, so its firing deletes only that row.
    stamp: Option<u64>,
}

#[derive(Clone)]
struct RunningTask {
    id: Option<u64>,
    key: TaskKey,
    stamp: Option<u64>,
    started_at: Instant,
}

struct RunningTaskEntry {
    key: TaskKey,
    started_at: Instant,
    warn_at: Instant,
    warned: bool,
    /// Taken by a forced abort so the scheduler can observe the task ending
    /// itself instead of handing completion to the caller.
    task: Option<JoinHandle<()>>,
    /// Cooperative stop for one key's run. The entry, its key exclusion, and
    /// its drain accounting stay until the handler reports completion.
    cancel: Option<oneshot::Sender<()>>,
}

impl SchedulerState {
    fn new(admission_closed: Arc<AtomicBool>) -> Self {
        Self {
            admission_closed,
            timers_by_key: HashMap::new(),
            timers_by_deadline: BTreeMap::new(),
            running_by_id: HashMap::new(),
            running_warn_deadlines: BTreeSet::new(),
            in_flight_keys: HashMap::new(),
            refire_requested: HashMap::new(),
            inbound_handler: None,
            drained_waiters: Vec::new(),
            next_timer_id: 1,
            next_run_id: 1,
            handler_limit: HANDLER_LIMIT,
        }
    }

    fn at_capacity(&self) -> bool {
        self.running_by_id.len() >= self.handler_limit
    }

    fn allocate_timer_id(&mut self, deadline: Instant) -> Option<u64> {
        for _ in 0..=self.timers_by_key.len() {
            let id = self.next_timer_id;
            self.next_timer_id = next_id(self.next_timer_id);

            if !self.timers_by_deadline.contains_key(&(deadline, id)) {
                return Some(id);
            }
        }

        None
    }

    fn allocate_run_id(&mut self) -> Option<u64> {
        for _ in 0..=self.running_by_id.len() {
            let id = self.next_run_id;
            self.next_run_id = next_id(self.next_run_id);

            if !self.running_by_id.contains_key(&id) {
                return Some(id);
            }
        }

        None
    }

    fn insert_timer(&mut self, key: TaskKey, entry: TimerEntry) {
        self.timers_by_deadline
            .insert((entry.deadline, entry.id), key.clone());
        self.timers_by_key.insert(key, entry);
    }

    fn remove_timer(&mut self, key: &TaskKey) -> Option<TimerEntry> {
        let entry = self.timers_by_key.remove(key)?;
        self.timers_by_deadline.remove(&(entry.deadline, entry.id));
        Some(entry)
    }

    fn prepare_running_task(
        &mut self,
        key: TaskKey,
        stamp: Option<u64>,
        started_at: Instant,
    ) -> RunningTask {
        let id = self.allocate_run_id();
        RunningTask {
            id,
            key,
            stamp,
            started_at,
        }
    }

    fn track_running_task(
        &mut self,
        task: &RunningTask,
        handle: JoinHandle<()>,
        cancel: Option<oneshot::Sender<()>>,
    ) {
        let Some(id) = task.id else {
            return;
        };

        let warn_at = task
            .started_at
            .checked_add(HANDLER_WARN_AFTER)
            .unwrap_or(task.started_at);
        self.running_by_id.insert(
            id,
            RunningTaskEntry {
                key: task.key.clone(),
                started_at: task.started_at,
                warn_at,
                warned: false,
                task: Some(handle),
                cancel,
            },
        );
        self.running_warn_deadlines.insert((warn_at, id));
        *self.in_flight_keys.entry(task.key.clone()).or_insert(0) += 1;
    }

    fn spawn_handler(
        &mut self,
        (key, stamp): (TaskKey, Option<u64>),
        started_at: Instant,
        command_tx: &mpsc::WeakSender<TaskCommand>,
    ) {
        if self.admission_closed.load(Ordering::Acquire) {
            return;
        }
        if let Some(handler) = self.inbound_handler.clone() {
            let task = self.prepare_running_task(key, stamp, started_at);
            let (cancel_tx, cancel_rx) = oneshot::channel();
            let handle = spawn_timer_handler(handler, command_tx.clone(), task.clone(), cancel_rx);
            self.track_running_task(&task, handle, Some(cancel_tx));
        }
    }

    fn release_key(&mut self, key: &TaskKey) -> bool {
        if let Some(count) = self.in_flight_keys.get_mut(key) {
            *count = count.saturating_sub(1);
            if *count > 0 {
                return false;
            }
            self.in_flight_keys.remove(key);
        }
        true
    }

    fn next_deadline(&self) -> Option<Instant> {
        // With every slot taken a completion wakes the loop, so overdue timers cause no spin.
        let next_timer = (self.timers_by_deadline.first_key_value())
            .filter(|_| !self.at_capacity())
            .map(|(&(deadline, _), _)| deadline);
        let next_running_warning = self
            .running_warn_deadlines
            .first()
            .map(|(deadline, _)| *deadline);

        match (next_timer, next_running_warning) {
            (Some(timer), Some(warning)) => Some(timer.min(warning)),
            (Some(timer), None) => Some(timer),
            (None, Some(warning)) => Some(warning),
            (None, None) => None,
        }
    }

    fn dispatch_due_timers(&mut self, now: Instant, command_tx: &mpsc::WeakSender<TaskCommand>) {
        while let Some((&(deadline, timer_id), key)) = self.timers_by_deadline.first_key_value() {
            if deadline > now || self.at_capacity() {
                break;
            }

            let key = key.clone();
            self.timers_by_deadline.pop_first();

            if matches!(self.timers_by_key.get(&key), Some(entry) if entry.id == timer_id && entry.deadline == deadline)
            {
                let stamp = self
                    .timers_by_key
                    .remove(&key)
                    .and_then(|entry| entry.stamp);

                if self.in_flight_keys.contains_key(&key) {
                    self.refire_requested.insert(key, stamp);
                } else {
                    self.spawn_handler((key, stamp), now, command_tx);
                }
            }
        }
    }

    fn warn_long_tasks(&mut self, now: Instant) {
        while let Some(&(warn_at, run_id)) = self.running_warn_deadlines.first() {
            if warn_at > now {
                break;
            }

            self.running_warn_deadlines.pop_first();

            if let Some(entry) = self.running_by_id.get_mut(&run_id)
                && !entry.warned
            {
                entry.warned = true;
                warn!(
                    task_run_id = run_id,
                    key = ?entry.key,
                    elapsed_ms = now.saturating_duration_since(entry.started_at).as_millis(),
                    threshold_ms = HANDLER_WARN_AFTER.as_millis(),
                    "Timer handler task is still running after warning threshold"
                );
            }
        }
    }

    fn reset_timer(
        &mut self,
        (key, stamp): (TaskKey, Option<u64>),
        after: Duration,
        now: Instant,
    ) -> TaskEvent {
        let Some(deadline) = now.checked_add(after) else {
            return TaskEvent::Error {
                key: Some(key),
                message: "timer deadline overflow".to_string(),
            };
        };

        let Some(id) = self.allocate_timer_id(deadline) else {
            return TaskEvent::Error {
                key: Some(key),
                message: "timer id space exhausted".to_string(),
            };
        };

        self.remove_timer(&key);
        let entry = TimerEntry {
            id,
            deadline,
            stamp,
        };
        self.insert_timer(key.clone(), entry);

        TaskEvent::TimerScheduled { key, after }
    }

    /// Moves the wake of `key` earlier; answers the admission whose wake holds the key: `stamp`
    /// when it armed a new wake, else the earlier live one it kept.
    fn shorten_timer(
        &mut self,
        (key, stamp): (TaskKey, Option<u64>),
        after: Duration,
        (now, mode): (Instant, Shorten),
    ) -> (TaskEvent, Option<u64>) {
        let Some(requested_deadline) = now.checked_add(after) else {
            let message = "timer deadline overflow".to_string();
            let key = Some(key);
            return (TaskEvent::Error { key, message }, None);
        };

        // A wake that came due while its key ran fires again right after the run, earliest of all.
        let kept = match self.refire_requested.get_mut(&key) {
            Some(pending) => Some((Duration::ZERO, pending)),
            None => (self.timers_by_key.get_mut(&key))
                .filter(|existing| existing.deadline <= requested_deadline)
                .map(|existing| {
                    (
                        existing.deadline.saturating_duration_since(now),
                        &mut existing.stamp,
                    )
                }),
        };
        if let Some((after, holder)) = kept {
            if mode == Shorten::Claim {
                *holder = stamp;
            }
            return (TaskEvent::TimerScheduled { key, after }, *holder);
        }
        if mode == Shorten::Plan {
            return (TaskEvent::TimerScheduled { key, after }, stamp);
        }

        let Some(id) = self.allocate_timer_id(requested_deadline) else {
            let message = "timer id space exhausted".to_string();
            let key = Some(key);
            return (TaskEvent::Error { key, message }, None);
        };

        self.remove_timer(&key);
        self.insert_timer(
            key.clone(),
            TimerEntry {
                id,
                deadline: requested_deadline,
                stamp,
            },
        );

        (TaskEvent::TimerScheduled { key, after }, stamp)
    }

    /// [`Self::shorten_timer`], answered as a plan. An arming that completes `plan` keeps its
    /// deadline; when the wake that held the key fired since, that run started after the request
    /// and serves it, so no second wake is armed.
    fn shorten_planned(
        &mut self,
        (key, stamp): (TaskKey, Option<u64>),
        after: Duration,
        (now, mode): (Instant, Shorten),
        plan: Option<ShortenPlan>,
    ) -> (TaskEvent, Option<ShortenPlan>) {
        let replaced = (self.timers_by_key.get(&key)).map(|entry| (entry.id, entry.stamp));
        let fired = plan.and_then(|plan| plan.replaced).filter(|(id, _)| {
            !self.refire_requested.contains_key(&key)
                && replaced.is_none_or(|(current, _)| current != *id)
        });
        let Some(deadline) =
            plan.map_or_else(|| now.checked_add(after), |plan| Some(plan.deadline))
        else {
            return (self.shorten_timer((key, stamp), after, (now, mode)).0, None);
        };
        let (event, holder) = match fired {
            Some((_, holder)) => {
                let after = Duration::ZERO;
                (TaskEvent::TimerScheduled { key, after }, holder)
            }
            None => {
                let after = deadline.saturating_duration_since(now);
                self.shorten_timer((key, stamp), after, (now, mode))
            }
        };
        let planned = ShortenPlan {
            holder,
            deadline,
            replaced,
        };
        let scheduled = matches!(event, TaskEvent::TimerScheduled { .. });
        (event, scheduled.then_some(planned))
    }

    fn schedule_idle_timer(&mut self, key: TaskKey, after: Duration, now: Instant) -> TaskEvent {
        if let Some(existing) = self.timers_by_key.get(&key) {
            return TaskEvent::TimerScheduled {
                key,
                after: existing.deadline.saturating_duration_since(now),
            };
        }

        if self.in_flight_keys.contains_key(&key) {
            return TaskEvent::TimerScheduled { key, after };
        }

        self.reset_timer((key, None), after, now)
    }

    fn cancel_timer(&mut self, key: TaskKey) -> TaskEvent {
        self.remove_timer(&key);
        // A firing that waited for a running handler is cancelled too, so no run follows it.
        self.refire_requested.remove(&key);
        TaskEvent::TimerCancelled { key }
    }

    fn complete_handler(
        &mut self,
        run_id: u64,
        key: TaskKey,
        elapsed: Duration,
        now: Instant,
        command_tx: &mpsc::WeakSender<TaskCommand>,
    ) {
        let Some(entry) = self.running_by_id.remove(&run_id) else {
            return;
        };

        self.running_warn_deadlines.remove(&(entry.warn_at, run_id));

        if !entry.warned && elapsed >= HANDLER_WARN_AFTER {
            warn!(
                task_run_id = run_id,
                key = ?key,
                elapsed_ms = elapsed.as_millis(),
                threshold_ms = HANDLER_WARN_AFTER.as_millis(),
                "Timer handler task exceeded warning threshold before completing"
            );
        }

        if self.release_key(&entry.key)
            && let Some(stamp) = self.refire_requested.remove(&entry.key)
        {
            self.spawn_handler((entry.key, stamp), now, command_tx);
        }

        self.notify_if_drained();
    }

    /// Stops new handler runs: without an inbound handler no timer can spawn
    /// work, and pending timers stay durable in storage for the next boot.
    fn stop_admission(&mut self) -> usize {
        self.inbound_handler = None;
        self.timers_by_key.clear();
        self.timers_by_deadline.clear();
        self.refire_requested.clear();
        self.running_by_id.len()
    }

    /// Stops every running handler and keeps it owned until the task actually
    /// ends. The scheduler watches each aborted handle itself, so an interrupted
    /// caller cannot lose the completion or release the drain early.
    fn abort_all_handlers(&mut self, command_tx: &mpsc::WeakSender<TaskCommand>) -> usize {
        let mut aborted = 0;
        for (&run_id, entry) in self.running_by_id.iter_mut() {
            if let Some(task) = entry.task.take() {
                task.abort();
                let key = entry.key.clone();
                let started_at = entry.started_at;
                let command_tx = command_tx.clone();
                tokio::spawn(async move {
                    let _ = task.await;
                    if let Some(command_tx) = command_tx.upgrade() {
                        let _ = command_tx
                            .send(TaskCommand::HandlerCompleted {
                                run_id,
                                key,
                                elapsed: started_at.elapsed(),
                            })
                            .await;
                    }
                });
                aborted += 1;
            }
        }
        aborted
    }

    fn notify_if_drained(&mut self) {
        if self.running_by_id.is_empty() {
            for waiter in self.drained_waiters.drain(..) {
                let _ = waiter.send(());
            }
        }
    }

    /// Requests a cooperative stop for every run of one key. Entries stay owned
    /// and keep the key exclusion until each handler reports completion, so the
    /// key cannot be rescheduled and the drain cannot be declared early.
    fn abort_running_handlers(&mut self, key: TaskKey) -> TaskEvent {
        let mut count = 0usize;
        for entry in self.running_by_id.values_mut() {
            if entry.key == key {
                if let Some(cancel) = entry.cancel.take() {
                    let _ = cancel.send(());
                }
                count += 1;
            }
        }
        self.refire_requested.remove(&key);

        TaskEvent::RunningHandlersAborted { key, count }
    }

    fn handle_command(
        &mut self,
        command: TaskCommand,
        now: Instant,
        command_tx: &mpsc::WeakSender<TaskCommand>,
    ) {
        match command {
            TaskCommand::SetInboundHandler { handler, response } => {
                self.inbound_handler = Some(handler);
                let _ = response.send(());
            }
            TaskCommand::ClearInboundHandler { response } => {
                self.inbound_handler = None;
                let _ = response.send(());
            }
            TaskCommand::ResetTimer {
                key,
                after,
                stamp,
                deadline,
                response,
            } => {
                let after = deadline.map_or(after, |due| due.saturating_duration_since(now));
                let _ = response.send(self.reset_timer((key, stamp), after, now));
            }
            TaskCommand::ShortenTimer {
                key,
                after,
                stamp,
                mode,
                plan,
                response,
            } => {
                let answer = self.shorten_planned((key, stamp), after, (now, mode), plan);
                let _ = response.send(answer);
            }
            TaskCommand::ScheduleIdleTimer {
                key,
                after,
                response,
            } => {
                let _ = response.send(self.schedule_idle_timer(key, after, now));
            }
            TaskCommand::CancelTimer { key, response } => {
                let _ = response.send(self.cancel_timer(key));
            }
            TaskCommand::AbortRunningHandlers { key, response } => {
                let _ = response.send(self.abort_running_handlers(key));
            }
            TaskCommand::HandlerCompleted {
                run_id,
                key,
                elapsed,
            } => self.complete_handler(run_id, key, elapsed, now, command_tx),
            TaskCommand::StopAdmission { response } => {
                let _ = response.send(self.stop_admission());
            }
            TaskCommand::AwaitDrained { response } => {
                if self.running_by_id.is_empty() {
                    let _ = response.send(());
                } else {
                    self.drained_waiters.push(response);
                }
            }
            TaskCommand::AbortAllHandlers { response } => {
                let _ = response.send(self.abort_all_handlers(command_tx));
            }
            #[cfg(test)]
            TaskCommand::RegisteredDrainWaiters { response } => {
                let _ = response.send(self.drained_waiters.len());
            }
        }
    }
}

fn next_id(id: u64) -> u64 {
    id.checked_add(1).unwrap_or(1)
}

async fn run_scheduler(
    mut command_rx: mpsc::Receiver<TaskCommand>,
    command_tx: mpsc::WeakSender<TaskCommand>,
    admission_closed: Arc<AtomicBool>,
) {
    let mut state = SchedulerState::new(admission_closed);

    loop {
        let now = Instant::now();
        state.dispatch_due_timers(now, &command_tx);
        state.warn_long_tasks(now);

        match state.next_deadline() {
            Some(deadline) => {
                tokio::select! {
                    maybe_command = command_rx.recv() => {
                        let Some(command) = maybe_command else { break };
                        // Relative durations anchor when the command is processed,
                        // never at the loop iteration that started before the wait.
                        state.handle_command(command, Instant::now(), &command_tx);
                    }
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => {
                let Some(command) = command_rx.recv().await else {
                    break;
                };
                state.handle_command(command, Instant::now(), &command_tx);
            }
        }
    }
}

fn spawn_timer_handler(
    handler: Arc<dyn InboundTaskHandler>,
    command_tx: mpsc::WeakSender<TaskCommand>,
    task: RunningTask,
    mut cancel: oneshot::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if task.id.is_none() {
            warn!(
                key = ?task.key,
                "Timer handler task is running without runtime tracking because run id space is exhausted"
            );
        }

        // A cooperative stop drops the handler future at its next await, then
        // still reports completion so the scheduler releases the key and the
        // drain only after the run really ended. A panic reports completion too.
        let firing = handler.handle_firing(task.key.clone(), task.stamp);
        let run = std::panic::AssertUnwindSafe(firing);
        tokio::select! {
            biased;
            _ = &mut cancel => {}
            ran = run.catch_unwind() => {
                if ran.is_err() {
                    error!(key = ?task.key, "Timer handler panicked");
                }
            }
        }

        let elapsed = task.started_at.elapsed();
        if let Some(run_id) = task.id {
            if let Some(command_tx) = command_tx.upgrade() {
                let _ = command_tx
                    .send(TaskCommand::HandlerCompleted {
                        run_id,
                        key: task.key,
                        elapsed,
                    })
                    .await;
            }
        } else if elapsed >= HANDLER_WARN_AFTER {
            warn!(
                key = ?task.key,
                elapsed_ms = elapsed.as_millis(),
                threshold_ms = HANDLER_WARN_AFTER.as_millis(),
                "Untracked timer handler task exceeded warning threshold before completing"
            );
        }
    })
}

/// The task scheduler could not start because no Tokio runtime is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSchedulerUnavailable;

impl std::fmt::Display for TaskSchedulerUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("task scheduler requires an active Tokio runtime")
    }
}

impl std::error::Error for TaskSchedulerUnavailable {}

impl TaskHandle {
    /// Starts the scheduler on the current Tokio runtime. Production startup
    /// uses this so a missing runtime is a concrete error instead of a handle
    /// that only looks like it is running.
    pub fn try_new() -> Result<Self, TaskSchedulerUnavailable> {
        let (command_tx, command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
        let admission_closed = Arc::new(AtomicBool::new(false));
        let handle = tokio::runtime::Handle::try_current().map_err(|_| TaskSchedulerUnavailable)?;
        handle.spawn(run_scheduler(
            command_rx,
            command_tx.downgrade(),
            admission_closed.clone(),
        ));
        Ok(Self {
            command_tx,
            admission_closed,
            pulls: Arc::default(),
        })
    }

    /// Test convenience for a running scheduler. A missing runtime is a
    /// concrete panic, never a handle that only looks started; use
    /// [`TaskHandle::try_new`] where the failure must be handled.
    pub fn new() -> Self {
        Self::try_new().expect("task scheduler requires an active Tokio runtime")
    }

    /// An explicitly inactive handle: no scheduler runs, so every effect
    /// reports the scheduler as unavailable. Only tests of that behavior use
    /// this mode; production constructs through [`TaskHandle::try_new`].
    pub fn inactive() -> Self {
        let (command_tx, _command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
        Self {
            command_tx,
            admission_closed: Arc::new(AtomicBool::new(false)),
            pulls: Arc::default(),
        }
    }

    pub async fn set_inbound_handler(&self, handler: Arc<dyn InboundTaskHandler>) {
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::SetInboundHandler { handler, response })
            .await
            .is_ok()
        {
            let _ = result.await;
        }
    }

    pub async fn clear_inbound_handler(&self) {
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::ClearInboundHandler { response })
            .await
            .is_ok()
        {
            let _ = result.await;
        }
    }

    async fn dispatch_command<F>(&self, key: TaskKey, build: F) -> TaskEvent
    where
        F: FnOnce(TaskKey, oneshot::Sender<TaskEvent>) -> TaskCommand,
    {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        if self.command_tx.send(build(key, response)).await.is_err() {
            return scheduler_unavailable(command_key);
        }

        result
            .await
            .unwrap_or_else(|_| scheduler_unavailable(command_key))
    }

    async fn reset_timer(
        &self,
        key: TaskKey,
        after: Duration,
        (stamp, deadline): (Option<u64>, Option<Instant>),
    ) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::ResetTimer {
            key,
            after,
            stamp,
            deadline,
            response,
        })
        .await
    }

    async fn shorten_timer(
        &self,
        key: TaskKey,
        after: Duration,
        (stamp, mode, plan): (Option<u64>, Shorten, Option<ShortenPlan>),
    ) -> (TaskEvent, Option<ShortenPlan>) {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        let command = TaskCommand::ShortenTimer {
            key,
            after,
            stamp,
            mode,
            plan,
            response,
        };
        if self.command_tx.send(command).await.is_err() {
            return (scheduler_unavailable(command_key), None);
        }
        (result.await).unwrap_or_else(|_| (scheduler_unavailable(command_key), None))
    }

    /// Moves the wake of `key` earlier as admission `stamp`; answers the admission whose wake
    /// holds the key: `stamp` when the scheduler armed it, else the earlier wake it kept.
    pub async fn shorten_owned(
        &self,
        key: TaskKey,
        after: Duration,
        stamp: u64,
    ) -> (Event, Option<u64>) {
        let (event, plan) = self
            .shorten_timer(key, after, (Some(stamp), Shorten::Arm, None))
            .await;
        (Event::Task(event), plan.and_then(|plan| plan.holder))
    }

    /// Answers what [`TaskHandle::shorten_owned`] would, without arming or changing any wake; the
    /// plan lets the arming after the row write keep this deadline.
    pub async fn plan_shorten(
        &self,
        key: TaskKey,
        after: Duration,
        stamp: u64,
    ) -> (Event, Option<ShortenPlan>) {
        let (event, plan) = self
            .shorten_timer(key, after, (Some(stamp), Shorten::Plan, None))
            .await;
        (Event::Task(event), plan)
    }

    /// Arms `plan` as admission `stamp` once its row is stored: an earlier wake that holds the
    /// key now owns that row, so its firing deletes it.
    pub async fn claim_shorten(
        &self,
        key: TaskKey,
        plan: ShortenPlan,
        stamp: u64,
    ) -> (Event, Option<u64>) {
        let planned = (Some(stamp), Shorten::Claim, Some(plan));
        let (event, plan) = self.shorten_timer(key, Duration::ZERO, planned).await;
        (Event::Task(event), plan.and_then(|plan| plan.holder))
    }

    /// Arms `plan` as admission `stamp` when its row could not be stored.
    pub async fn arm_planned(
        &self,
        key: TaskKey,
        plan: ShortenPlan,
        stamp: u64,
    ) -> (Event, Option<u64>) {
        let planned = (Some(stamp), Shorten::Arm, Some(plan));
        let (event, plan) = self.shorten_timer(key, Duration::ZERO, planned).await;
        (Event::Task(event), plan.and_then(|plan| plan.holder))
    }

    /// Hands a timer change to the scheduler with the due time of the durable row it wrote, so
    /// the firing it arms deletes only that row and never a newer one. A reset keeps `deadline`.
    pub async fn send_stamped(
        &self,
        effect: TaskEffect,
        stamp: Option<u64>,
        deadline: Option<Instant>,
    ) -> Event {
        let event = match effect {
            TaskEffect::ResetTimer { key, after } => {
                self.reset_timer(key, after, (stamp, deadline)).await
            }
            TaskEffect::ShortenTimer { key, after } => {
                self.shorten_timer(key, after, (stamp, Shorten::Arm, None))
                    .await
                    .0
            }
            other => return self.send_effect(Effect::Task(other)).await,
        };
        Event::Task(event)
    }

    pub async fn schedule_idle_timer(&self, key: TaskKey, after: Duration) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::ScheduleIdleTimer {
            key,
            after,
            response,
        })
        .await
    }

    async fn cancel_timer(&self, key: TaskKey) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::CancelTimer {
            key,
            response,
        })
        .await
    }

    pub async fn abort_running_handlers(&self, key: TaskKey) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::AbortRunningHandlers {
            key,
            response,
        })
        .await
    }

    /// Permanently stops new timer handlers without waiting for the scheduler.
    pub fn close_admission(&self) {
        self.admission_closed.store(true, Ordering::Release);
    }

    async fn stop_admission(&self) -> Option<usize> {
        let (response, stopped) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::StopAdmission { response })
            .await
            .is_err()
        {
            return None;
        }
        stopped.await.ok()
    }

    /// Stops admitting timer handlers, waits up to `drain`, then forces a stop
    /// and observes the runs ending. Unfinished handlers stay owned for a
    /// retried shutdown; durable timers retry next boot. The loop may still run.
    pub async fn shutdown(&self, drain: Duration) -> TaskShutdownReport {
        self.close_admission();
        let Some(in_flight) = self.stop_admission().await else {
            return TaskShutdownReport {
                in_flight: 0,
                aborted: 0,
                scheduler_unavailable: true,
            };
        };
        if in_flight == 0 {
            return TaskShutdownReport {
                in_flight: 0,
                aborted: 0,
                scheduler_unavailable: false,
            };
        }

        let (response, drained) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::AwaitDrained { response })
            .await
            .is_err()
        {
            return TaskShutdownReport {
                in_flight,
                aborted: 0,
                scheduler_unavailable: true,
            };
        }
        match tokio::time::timeout(drain, drained).await {
            Ok(Ok(())) => {
                return TaskShutdownReport {
                    in_flight,
                    aborted: 0,
                    scheduler_unavailable: false,
                };
            }
            // The scheduler dropped the waiter without observing a drain.
            Ok(Err(_)) => {
                return TaskShutdownReport {
                    in_flight,
                    aborted: 0,
                    scheduler_unavailable: true,
                };
            }
            Err(_) => {}
        }

        let (response, aborted) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::AbortAllHandlers { response })
            .await
            .is_err()
        {
            return TaskShutdownReport {
                in_flight,
                aborted: 0,
                scheduler_unavailable: true,
            };
        }
        let aborted = match aborted.await {
            Ok(aborted) => aborted,
            Err(_) => {
                return TaskShutdownReport {
                    in_flight,
                    aborted: 0,
                    scheduler_unavailable: true,
                };
            }
        };
        if aborted == 0 {
            // Everything finished between the deadline and the forced command;
            // the drain condition already held and was observed.
            return TaskShutdownReport {
                in_flight,
                aborted: 0,
                scheduler_unavailable: false,
            };
        }
        warn!(
            aborted,
            drain_ms = drain.as_millis(),
            "Forced stop of timer handlers that outlived the shutdown drain deadline"
        );

        // Observe the forced stops through the scheduler's own accounting: the
        // drain waiters fire only once every aborted task has actually ended.
        let (response, settled) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::AwaitDrained { response })
            .await
            .is_err()
        {
            return TaskShutdownReport {
                in_flight,
                aborted,
                scheduler_unavailable: true,
            };
        }
        let settle_timeout = drain.max(Duration::from_secs(5));
        match tokio::time::timeout(settle_timeout, settled).await {
            Ok(Ok(())) => TaskShutdownReport {
                in_flight,
                aborted,
                scheduler_unavailable: false,
            },
            Ok(Err(_)) | Err(_) => TaskShutdownReport {
                in_flight,
                aborted,
                scheduler_unavailable: true,
            },
        }
    }
}

fn scheduler_unavailable(key: TaskKey) -> TaskEvent {
    TaskEvent::Error {
        key: Some(key),
        message: "task scheduler unavailable".to_string(),
    }
}

impl Default for TaskHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle").finish()
    }
}

#[async_trait]
impl Handle for TaskHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Task(task_effect) => {
                let event = match task_effect {
                    TaskEffect::ResetTimer { key, after } => {
                        self.reset_timer(key, after, (None, None)).await
                    }
                    TaskEffect::ShortenTimer { key, after } => {
                        self.shorten_timer(key, after, (None, Shorten::Arm, None))
                            .await
                            .0
                    }
                    TaskEffect::ScheduleIdleTimer { key, after } => {
                        self.schedule_idle_timer(key, after).await
                    }
                    TaskEffect::CancelTimer { key } => self.cancel_timer(key).await,
                    TaskEffect::AbortRunningHandlers { key } => {
                        self.abort_running_handlers(key).await
                    }
                    TaskEffect::Pull(effect) => TaskEvent::Pull(self.pulls.answer(effect)),
                };
                Event::Task(event)
            }
            _ => Event::Task(TaskEvent::Error {
                key: None,
                message: "invalid effect for task handle".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests;

/// Timer-decision coverage with explicit times: no runtime, spawning, or
/// command loop is involved.
#[cfg(test)]
mod decision_tests {
    use super::*;

    fn state() -> SchedulerState {
        SchedulerState::new(Arc::new(AtomicBool::new(false)))
    }

    fn key() -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: aruna_core::structs::identity::realm::RealmId([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        }
    }

    #[test]
    fn dispatch_at_deadline() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.reset_timer((key.clone(), None), Duration::from_secs(10), start);
        let (tx, _rx) = mpsc::channel(1);
        let weak = tx.downgrade();

        state.dispatch_due_timers(start + Duration::from_secs(9), &weak);
        assert!(
            state.timers_by_key.contains_key(&key),
            "a timer before its deadline stays pending"
        );
        state.dispatch_due_timers(start + Duration::from_secs(10), &weak);
        assert!(
            !state.timers_by_key.contains_key(&key),
            "a timer dispatches at its deadline"
        );
    }

    // A cancellation acknowledged while a handler runs also drops the firing that waited for it.
    #[test]
    fn cancel_drops_refire() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.in_flight_keys.insert(key.clone(), 1);
        state.reset_timer((key.clone(), None), Duration::ZERO, start);
        let (tx, _rx) = mpsc::channel(1);
        state.dispatch_due_timers(start, &tx.downgrade());
        assert!(
            state.refire_requested.contains_key(&key),
            "the firing waits"
        );
        state.cancel_timer(key.clone());
        assert!(
            !state.refire_requested.contains_key(&key),
            "no run follows the cancel"
        );
    }

    // A shorten while a due wake waits for its running key keeps that wake and arms no other.
    #[test]
    fn pending_refire_kept() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.refire_requested.insert(key.clone(), Some(5));
        let (event, holder) = state.shorten_timer(
            (key.clone(), Some(6)),
            Duration::from_secs(30),
            (start, Shorten::Arm),
        );
        let expected = TaskEvent::TimerScheduled {
            key: key.clone(),
            after: Duration::ZERO,
        };
        assert_eq!((event, holder), (expected, Some(5)));
        assert!(!state.timers_by_key.contains_key(&key));
    }

    // A planned shorten answers the wake it would arm and leaves the armed one as it is.
    #[test]
    fn plan_arms_nothing() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.reset_timer((key.clone(), Some(4)), Duration::from_secs(60), start);
        let planned = (start, Shorten::Plan);
        let (_, holder) =
            state.shorten_timer((key.clone(), Some(6)), Duration::from_secs(5), planned);
        assert_eq!(holder, Some(6));
        let entry = state.timers_by_key.get(&key).expect("the wake stays");
        let later = start + Duration::from_secs(60);
        assert_eq!((entry.deadline, entry.stamp), (later, Some(4)));
    }

    // A claim kept by an earlier wake hands that wake the claimed row, so its firing deletes it.
    #[test]
    fn claim_adopts_row() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.refire_requested.insert(key.clone(), Some(5));
        let claimed = (start, Shorten::Claim);
        let (_, holder) =
            state.shorten_timer((key.clone(), Some(6)), Duration::from_secs(30), claimed);
        assert_eq!(holder, Some(6));
        assert_eq!(state.refire_requested.get(&key), Some(&Some(6)));
        assert!(!state.timers_by_key.contains_key(&key));
    }

    // A claim after its row write arms at the planned deadline, not its delay counted again.
    #[test]
    fn claim_keeps_deadline() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        let delay = Duration::from_secs(5);
        let planned = (start, Shorten::Plan);
        let (_, plan) = state.shorten_planned((key.clone(), Some(6)), delay, planned, None);
        let claimed = (start + Duration::from_secs(2), Shorten::Claim);
        let (event, plan) =
            state.shorten_planned((key.clone(), Some(6)), Duration::ZERO, claimed, plan);
        let after = Duration::from_secs(3);
        assert_eq!(
            event,
            TaskEvent::TimerScheduled {
                key: key.clone(),
                after
            }
        );
        assert_eq!(plan.and_then(|plan| plan.holder), Some(6));
        let entry = state.timers_by_key.get(&key).expect("armed");
        assert_eq!(entry.deadline, start + delay);
    }

    // A later wake that fired between plan and claim started after the request and serves it: the
    // claim arms no second wake and answers that wake's admission.
    #[test]
    fn fired_wake_serves() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.reset_timer((key.clone(), Some(4)), Duration::from_secs(10), start);
        let planned = (start, Shorten::Plan);
        let delay = Duration::from_secs(5);
        let (_, plan) = state.shorten_planned((key.clone(), Some(6)), delay, planned, None);
        assert_eq!(plan.and_then(|plan| plan.holder), Some(6));
        let (tx, _rx) = mpsc::channel(1);
        let fired_at = start + Duration::from_secs(10);
        state.dispatch_due_timers(fired_at, &tx.downgrade());
        let claimed = (fired_at, Shorten::Claim);
        let (_, plan) =
            state.shorten_planned((key.clone(), Some(6)), Duration::ZERO, claimed, plan);
        assert_eq!(plan.and_then(|plan| plan.holder), Some(4));
        assert!(!state.timers_by_key.contains_key(&key), "no second wake");
    }

    #[test]
    fn shorten_earlier_deadlines() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.reset_timer((key.clone(), None), Duration::from_secs(10), start);

        state.shorten_timer(
            (key.clone(), None),
            Duration::from_secs(30),
            (start, Shorten::Arm),
        );
        assert_eq!(
            state.timers_by_key.get(&key).unwrap().deadline,
            start + Duration::from_secs(10),
            "a later request keeps the existing deadline"
        );
        state.shorten_timer(
            (key.clone(), None),
            Duration::from_secs(5),
            (start, Shorten::Arm),
        );
        assert_eq!(
            state.timers_by_key.get(&key).unwrap().deadline,
            start + Duration::from_secs(5),
            "an earlier request replaces the deadline"
        );
    }

    #[test]
    fn idle_timer_suppressed() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.in_flight_keys.insert(key.clone(), 1);
        state.schedule_idle_timer(key.clone(), Duration::from_secs(1), start);
        assert!(
            state.timers_by_key.is_empty(),
            "a key with a running handler gets no idle timer"
        );

        state.release_key(&key);
        let event = state.schedule_idle_timer(key.clone(), Duration::from_secs(1), start);
        assert!(matches!(event, TaskEvent::TimerScheduled { .. }));
        assert!(state.timers_by_key.contains_key(&key));
    }

    #[test]
    fn cancel_removes_indexes() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.reset_timer((key.clone(), None), Duration::from_secs(10), start);
        assert!(!state.timers_by_deadline.is_empty());

        state.cancel_timer(key.clone());

        assert!(state.timers_by_key.is_empty());
        assert!(
            state.timers_by_deadline.is_empty(),
            "a cancelled timer leaves no deadline entry"
        );
    }
}
