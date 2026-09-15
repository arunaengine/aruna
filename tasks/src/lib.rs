//! The timer and task scheduler behind `TaskHandle`: due timers, running
//! handlers, admission, and the ordered drain.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub mod join_registry;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::warn;

const TASK_COMMAND_BUFFER: usize = 1024;
const TIMER_HANDLER_WARN_AFTER: Duration = Duration::from_secs(30);

#[async_trait]
pub trait InboundTaskHandler: Send + Sync {
    async fn handle_timer(&self, key: TaskKey);
}

#[derive(Clone)]
pub struct TaskHandle {
    command_tx: mpsc::Sender<TaskCommand>,
    admission_closed: Arc<AtomicBool>,
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
        response: oneshot::Sender<TaskEvent>,
    },
    ShortenTimer {
        key: TaskKey,
        after: Duration,
        response: oneshot::Sender<TaskEvent>,
    },
    ScheduleTimerIfIdle {
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
    AbortAllRunningHandlers {
        response: oneshot::Sender<usize>,
    },
    #[cfg(test)]
    RegisteredDrainWaiters {
        response: oneshot::Sender<usize>,
    },
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
    refire_requested: HashSet<TaskKey>,
    inbound_handler: Option<Arc<dyn InboundTaskHandler>>,
    drained_waiters: Vec<oneshot::Sender<()>>,
    next_timer_id: u64,
    next_run_id: u64,
}

struct TimerEntry {
    id: u64,
    deadline: Instant,
}

#[derive(Clone)]
struct RunningTask {
    id: Option<u64>,
    key: TaskKey,
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
            refire_requested: HashSet::new(),
            inbound_handler: None,
            drained_waiters: Vec::new(),
            next_timer_id: 1,
            next_run_id: 1,
        }
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

    fn prepare_running_task(&mut self, key: TaskKey, started_at: Instant) -> RunningTask {
        let id = self.allocate_run_id();
        RunningTask {
            id,
            key,
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
            .checked_add(TIMER_HANDLER_WARN_AFTER)
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
        key: TaskKey,
        started_at: Instant,
        command_tx: &mpsc::WeakSender<TaskCommand>,
    ) {
        if self.admission_closed.load(Ordering::Acquire) {
            return;
        }
        if let Some(handler) = self.inbound_handler.clone() {
            let task = self.prepare_running_task(key, started_at);
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
        let next_timer = self
            .timers_by_deadline
            .first_key_value()
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
            if deadline > now {
                break;
            }

            let key = key.clone();
            self.timers_by_deadline.pop_first();

            if matches!(self.timers_by_key.get(&key), Some(entry) if entry.id == timer_id && entry.deadline == deadline)
            {
                self.timers_by_key.remove(&key);

                if self.in_flight_keys.contains_key(&key) {
                    self.refire_requested.insert(key);
                } else {
                    self.spawn_handler(key, now, command_tx);
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
                    threshold_ms = TIMER_HANDLER_WARN_AFTER.as_millis(),
                    "Timer handler task is still running after warning threshold"
                );
            }
        }
    }

    fn reset_timer(&mut self, key: TaskKey, after: Duration, now: Instant) -> TaskEvent {
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
        self.insert_timer(key.clone(), TimerEntry { id, deadline });

        TaskEvent::TimerScheduled { key, after }
    }

    fn shorten_timer(&mut self, key: TaskKey, after: Duration, now: Instant) -> TaskEvent {
        let Some(requested_deadline) = now.checked_add(after) else {
            return TaskEvent::Error {
                key: Some(key),
                message: "timer deadline overflow".to_string(),
            };
        };

        if let Some(existing) = self.timers_by_key.get(&key)
            && existing.deadline <= requested_deadline
        {
            return TaskEvent::TimerScheduled {
                key,
                after: existing.deadline.saturating_duration_since(now),
            };
        }

        let Some(id) = self.allocate_timer_id(requested_deadline) else {
            return TaskEvent::Error {
                key: Some(key),
                message: "timer id space exhausted".to_string(),
            };
        };

        self.remove_timer(&key);
        self.insert_timer(
            key.clone(),
            TimerEntry {
                id,
                deadline: requested_deadline,
            },
        );

        TaskEvent::TimerScheduled { key, after }
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

        self.reset_timer(key, after, now)
    }

    fn cancel_timer(&mut self, key: TaskKey) -> TaskEvent {
        self.remove_timer(&key);
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

        if !entry.warned && elapsed >= TIMER_HANDLER_WARN_AFTER {
            warn!(
                task_run_id = run_id,
                key = ?key,
                elapsed_ms = elapsed.as_millis(),
                threshold_ms = TIMER_HANDLER_WARN_AFTER.as_millis(),
                "Timer handler task exceeded warning threshold before completing"
            );
        }

        if self.release_key(&entry.key) && self.refire_requested.remove(&entry.key) {
            self.spawn_handler(entry.key, now, command_tx);
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
            }
            aborted += 1;
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
                response,
            } => {
                let _ = response.send(self.reset_timer(key, after, now));
            }
            TaskCommand::ShortenTimer {
                key,
                after,
                response,
            } => {
                let _ = response.send(self.shorten_timer(key, after, now));
            }
            TaskCommand::ScheduleTimerIfIdle {
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
            TaskCommand::AbortAllRunningHandlers { response } => {
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
        // drain only after the run really ended.
        tokio::select! {
            biased;
            _ = &mut cancel => {}
            _ = handler.handle_timer(task.key.clone()) => {}
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
        } else if elapsed >= TIMER_HANDLER_WARN_AFTER {
            warn!(
                key = ?task.key,
                elapsed_ms = elapsed.as_millis(),
                threshold_ms = TIMER_HANDLER_WARN_AFTER.as_millis(),
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

    async fn reset_timer(&self, key: TaskKey, after: Duration) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::ResetTimer {
            key,
            after,
            response,
        })
        .await
    }

    async fn shorten_timer(&self, key: TaskKey, after: Duration) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::ShortenTimer {
            key,
            after,
            response,
        })
        .await
    }

    pub async fn schedule_idle_timer(&self, key: TaskKey, after: Duration) -> TaskEvent {
        self.dispatch_command(key, |key, response| TaskCommand::ScheduleTimerIfIdle {
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
            .send(TaskCommand::AbortAllRunningHandlers { response })
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
        match settled.await {
            Ok(()) => TaskShutdownReport {
                in_flight,
                aborted,
                scheduler_unavailable: false,
            },
            Err(_) => TaskShutdownReport {
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
                    TaskEffect::ResetTimer { key, after } => self.reset_timer(key, after).await,
                    TaskEffect::ShortenTimer { key, after } => self.shorten_timer(key, after).await,
                    TaskEffect::CancelTimer { key } => self.cancel_timer(key).await,
                    TaskEffect::AbortRunningHandlers { key } => {
                        self.abort_running_handlers(key).await
                    }
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
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;

    #[derive(Clone)]
    struct ReschedulingHandler {
        handle: TaskHandle,
        count: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for ReschedulingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let seen = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            if seen == 1 {
                let _ = self
                    .handle
                    .send_effect(Effect::Task(TaskEffect::ResetTimer {
                        key,
                        after: Duration::from_millis(10),
                    }))
                    .await;
            }

            self.notify.notify_waiters();
        }
    }

    #[derive(Clone)]
    struct SelfReschedulingHandler {
        handle: TaskHandle,
        started: Arc<Notify>,
        finished: Arc<Notify>,
    }

    struct NotifyOnDrop(Arc<Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    #[derive(Clone)]
    struct BlockingHandler {
        started: Arc<Notify>,
        dropped: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for SelfReschedulingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            self.started.notify_one();
            let _ = self
                .handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key,
                    after: Duration::from_secs(3600),
                }))
                .await;

            tokio::task::yield_now().await;
            self.finished.notify_one();
        }
    }

    #[async_trait]
    impl InboundTaskHandler for BlockingHandler {
        async fn handle_timer(&self, _key: TaskKey) {
            let _notify_on_drop = NotifyOnDrop(self.dropped.clone());
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
    }

    #[derive(Clone)]
    struct CountingGatedHandler {
        runs: Arc<AtomicUsize>,
        started: Arc<Notify>,
        gate: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait]
    impl InboundTaskHandler for CountingGatedHandler {
        async fn handle_timer(&self, _key: TaskKey) {
            self.runs.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            let permit = self
                .gate
                .acquire()
                .await
                .expect("handler gate should stay open");
            permit.forget();
        }
    }

    #[derive(Clone)]
    struct RecordingHandler {
        runs: Arc<Mutex<Vec<(TaskKey, Instant)>>>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            self.runs
                .lock()
                .expect("run log lock should stay open")
                .push((key, Instant::now()));
        }
    }

    async fn fire_timer(handle: &TaskHandle, key: TaskKey) {
        let _ = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key,
                after: Duration::ZERO,
            }))
            .await;
    }

    async fn wait_for_runs(runs: &Arc<AtomicUsize>, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while runs.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {expected} handler runs"));
    }

    async fn registered_drain_waiters(handle: &TaskHandle) -> usize {
        let (response, count) = oneshot::channel();
        handle
            .command_tx
            .send(TaskCommand::RegisteredDrainWaiters { response })
            .await
            .expect("drain waiter query should reach the scheduler");
        count.await.expect("drain waiter query should be answered")
    }

    async fn wait_for_records(runs: &Arc<Mutex<Vec<(TaskKey, Instant)>>>, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while runs.lock().expect("run log lock should stay open").len() < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {expected} handler runs"));
    }

    // Yields keep the paused clock still while a wrongly anchored timer can run.
    async fn yield_runtime() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    fn test_key() -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: aruna_core::structs::identity::realm::RealmId([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        }
    }

    fn other_key() -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: aruna_core::structs::identity::realm::RealmId([8u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        }
    }

    // Production construction must not return a scheduler-less handle as if it
    // were running; outside a runtime the failure is concrete.
    #[test]
    fn requires_runtime() {
        assert_eq!(TaskHandle::try_new().unwrap_err(), TaskSchedulerUnavailable);
    }

    #[tokio::test]
    async fn starts_in_runtime() {
        assert!(TaskHandle::try_new().is_ok());
    }

    // The ambiguous fallback is gone: without a runtime construction panics
    // instead of handing back an inactive scheduler.
    #[test]
    #[should_panic(expected = "task scheduler requires an active Tokio runtime")]
    fn new_without_runtime() {
        let _ = TaskHandle::new();
    }

    #[tokio::test]
    async fn reset_keeps_reschedule() {
        let handle = TaskHandle::new();
        let count = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(ReschedulingHandler {
                handle: handle.clone(),
                count: count.clone(),
                notify: notify.clone(),
            }))
            .await;

        let _ = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key,
                after: Duration::from_millis(10),
            }))
            .await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while count.load(Ordering::SeqCst) < 2 {
                notify.notified().await;
            }
        })
        .await
        .expect("rescheduled timer should fire twice");
    }

    #[tokio::test]
    async fn handler_reset_survives() {
        let handle = TaskHandle::new();
        let started = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(SelfReschedulingHandler {
                handle: handle.clone(),
                started: started.clone(),
                finished: finished.clone(),
            }))
            .await;

        let _ = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key,
                after: Duration::from_millis(10),
            }))
            .await;

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");
        tokio::time::timeout(Duration::from_secs(1), finished.notified())
            .await
            .expect("handler should continue after resetting its own timer");
    }

    #[tokio::test]
    async fn abort_stops_handler() {
        let handle = TaskHandle::new();
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(BlockingHandler {
                started: started.clone(),
                dropped: dropped.clone(),
            }))
            .await;

        let _ = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(10),
            }))
            .await;

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers { key }))
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 1);

        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("handler future should be dropped after abort");
    }

    #[tokio::test]
    async fn overlap_coalesces_refire() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate: gate.clone(),
            }))
            .await;

        fire_timer(&handle, key.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("first handler run should start");

        for _ in 0..3 {
            fire_timer(&handle, key.clone()).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        gate.add_permits(16);
        wait_for_runs(&runs, 2).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn running_fire_refires() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate: gate.clone(),
            }))
            .await;

        fire_timer(&handle, key.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("first handler run should start");

        fire_timer(&handle, key.clone()).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        gate.add_permits(16);
        wait_for_runs(&runs, 2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fires_run_once() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(100));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate: gate.clone(),
            }))
            .await;

        fire_timer(&handle, key.clone()).await;
        wait_for_runs(&runs, 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        fire_timer(&handle, key).await;
        wait_for_runs(&runs, 2).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shorten_preserves_earlier() {
        let handle = TaskHandle::new();
        let key = test_key();

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_secs(3600),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert_eq!(after, Duration::from_secs(3600));

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key,
                after: Duration::from_secs(7200),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert!(after <= Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn idle_keeps_timer() {
        let handle = TaskHandle::new();
        let key = test_key();

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_secs(3600),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert_eq!(after, Duration::from_secs(3600));

        let TaskEvent::TimerScheduled { after, .. } =
            handle.schedule_idle_timer(key, Duration::ZERO).await
        else {
            panic!("expected timer scheduled event");
        };
        assert!(after > Duration::from_secs(3000));
    }

    // An idle scheduler must anchor a relative timer when the command arrives,
    // not when its loop iteration started before the command wait.
    #[tokio::test(start_paused = true)]
    async fn idle_reset_anchors() {
        let handle = TaskHandle::new();
        let runs = Arc::new(Mutex::new(Vec::new()));
        let key = test_key();
        handle
            .set_inbound_handler(Arc::new(RecordingHandler { runs: runs.clone() }))
            .await;

        tokio::time::advance(Duration::from_secs(3600)).await;

        let submitted_at = Instant::now();
        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_secs(10),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert_eq!(after, Duration::from_secs(10));

        yield_runtime().await;
        assert!(
            runs.lock()
                .expect("run log lock should stay open")
                .is_empty(),
            "an idle wait must not consume the relative timer"
        );

        tokio::time::advance(Duration::from_secs(9)).await;
        yield_runtime().await;
        assert!(
            runs.lock()
                .expect("run log lock should stay open")
                .is_empty(),
            "the timer must not fire before its full delay"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        wait_for_records(&runs, 1).await;
        let recorded = runs.lock().expect("run log lock should stay open").clone();
        assert_eq!(recorded[0].0, key);
        assert_eq!(recorded[0].1, submitted_at + Duration::from_secs(10));
    }

    // A pending deadline must not anchor a newly submitted relative timer.
    #[tokio::test(start_paused = true)]
    async fn pending_reset_anchors() {
        let handle = TaskHandle::new();
        let runs = Arc::new(Mutex::new(Vec::new()));
        let waiting = test_key();
        let added = other_key();
        handle
            .set_inbound_handler(Arc::new(RecordingHandler { runs: runs.clone() }))
            .await;

        let _ = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: waiting.clone(),
                after: Duration::from_secs(10_000),
            }))
            .await;

        tokio::time::advance(Duration::from_secs(5000)).await;

        let submitted_at = Instant::now();
        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: added.clone(),
                after: Duration::from_secs(10),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert_eq!(after, Duration::from_secs(10));

        yield_runtime().await;
        assert!(
            runs.lock()
                .expect("run log lock should stay open")
                .is_empty(),
            "a pending deadline must not shorten the new timer"
        );

        tokio::time::advance(Duration::from_secs(9)).await;
        yield_runtime().await;
        assert!(
            runs.lock()
                .expect("run log lock should stay open")
                .is_empty(),
            "the new timer must not fire before its full delay"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        wait_for_records(&runs, 1).await;
        {
            let recorded = runs.lock().expect("run log lock should stay open");
            assert_eq!(recorded.len(), 1, "the pending timer must stay pending");
            assert_eq!(recorded[0].0, added);
            assert_eq!(recorded[0].1, submitted_at + Duration::from_secs(10));
        }

        tokio::time::advance(Duration::from_secs(4990)).await;
        wait_for_records(&runs, 2).await;
        let recorded = runs.lock().expect("run log lock should stay open").clone();
        assert_eq!(recorded[1].0, waiting);
    }

    // A handler that finishes inside the drain budget is joined, not cut.
    #[tokio::test]
    async fn shutdown_drains_handler() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate: gate.clone(),
            }))
            .await;
        fire_timer(&handle, key).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        gate.add_permits(1);
        let report = handle.shutdown(Duration::from_secs(5)).await;

        assert_eq!(report.in_flight, 1);
        assert!(report.drained());
    }

    // A handler that ignores the deadline is aborted instead of hanging exit.
    #[tokio::test]
    async fn shutdown_aborts_stuck() {
        let handle = TaskHandle::new();
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(BlockingHandler {
                started: started.clone(),
                dropped: dropped.clone(),
            }))
            .await;
        fire_timer(&handle, key).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        let report = handle.shutdown(Duration::from_millis(50)).await;

        assert_eq!(report.aborted, 1);
        assert!(!report.drained());
        // The run stays owned: its end is observed by the scheduler, not by the
        // caller that asked for the forced stop.
        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("the force-stopped handler future must still drop");
        let settled = handle.shutdown(Duration::from_secs(1)).await;
        assert!(
            settled.drained(),
            "observed completion must release the run"
        );
    }

    #[test]
    fn shutdown_reports_unavailable() {
        let handle = TaskHandle::inactive();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime should build");
        let report = runtime.block_on(handle.shutdown(Duration::ZERO));

        assert!(report.scheduler_unavailable);
        assert!(!report.drained());
    }

    // Dropping the drain future does not detach the run: the scheduler keeps it
    // owned, so the forced stop is still observed and a resumed drain is clean.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_keeps_run() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let key = test_key();
        handle
            .set_inbound_handler(Arc::new(SlowSyncHandler {
                runs: runs.clone(),
                started: started.clone(),
                finished: finished.clone(),
            }))
            .await;
        fire_timer(&handle, key.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        // The caller is dropped while it waits for the drain deadline.
        let interrupted = tokio::time::timeout(
            Duration::from_millis(10),
            handle.shutdown(Duration::from_secs(30)),
        )
        .await;
        assert!(interrupted.is_err(), "the drain wait must be interrupted");

        // The forced stop is requested, but the caller is dropped again while
        // it observes the force-stopped run ending.
        let forced =
            tokio::time::timeout(Duration::from_millis(10), handle.shutdown(Duration::ZERO)).await;
        assert!(forced.is_err(), "the forced settle must be interruptible");

        // The run is still owned and force-stopped, not silently released.
        let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
                key: key.clone(),
            }))
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 1, "the force-stopped run must stay owned");

        tokio::time::timeout(Duration::from_secs(1), finished.notified())
            .await
            .expect("handler should reach its completion");
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        let settled = handle.shutdown(Duration::from_secs(1)).await;
        assert!(
            settled.drained(),
            "observed completion must release the run"
        );
    }

    // Concurrent drains wait on the same completion: both must be released by
    // the single handler finish instead of only the first waiter.
    #[tokio::test]
    async fn drains_share_completion() {
        let handle = TaskHandle::new();
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();
        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: Arc::new(AtomicUsize::new(0)),
                started: started.clone(),
                gate: gate.clone(),
            }))
            .await;
        fire_timer(&handle, key).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        let first = handle.shutdown(Duration::from_secs(5));
        let second = handle.shutdown(Duration::from_secs(5));
        let release = async {
            // Both drains must have registered before the handler may finish.
            tokio::time::timeout(Duration::from_secs(2), async {
                while registered_drain_waiters(&handle).await < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both drains should register");
            gate.add_permits(1);
        };
        let (first, second, ()) = tokio::join!(first, second, release);

        assert_eq!(first.in_flight, 1);
        assert_eq!(second.in_flight, 1);
        assert!(
            first.drained(),
            "the first drain must observe the completion"
        );
        assert!(
            second.drained(),
            "the second drain must observe the same completion"
        );
    }

    // A drain waiter that the scheduler drops without a completion is not a
    // successful drain.
    #[tokio::test]
    async fn lost_drain_unavailable() {
        let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
        let handle = TaskHandle {
            command_tx,
            admission_closed: Arc::new(AtomicBool::new(false)),
        };
        let scheduler = tokio::spawn(async move {
            match command_rx.recv().await {
                Some(TaskCommand::StopAdmission { response }) => {
                    let _ = response.send(1);
                }
                _ => panic!("expected stop admission command"),
            }
            match command_rx.recv().await {
                Some(TaskCommand::AwaitDrained { response }) => drop(response),
                _ => panic!("expected await drained command"),
            }
        });

        let report = handle.shutdown(Duration::from_secs(1)).await;
        scheduler.await.expect("fake scheduler should finish");

        assert_eq!(report.in_flight, 1);
        assert!(report.scheduler_unavailable);
        assert!(!report.drained());
    }

    // A dropped stop-admission response must not become a confirmed zero
    // in-flight count and a clean report.
    #[tokio::test]
    async fn lost_stop_unavailable() {
        let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
        let handle = TaskHandle {
            command_tx,
            admission_closed: Arc::new(AtomicBool::new(false)),
        };
        let scheduler = tokio::spawn(async move {
            match command_rx.recv().await {
                Some(TaskCommand::StopAdmission { response }) => drop(response),
                _ => panic!("expected stop admission command"),
            }
        });

        let report = handle.shutdown(Duration::from_secs(1)).await;
        scheduler.await.expect("fake scheduler should finish");

        assert!(report.scheduler_unavailable);
        assert!(!report.drained());
    }

    // A lost forced-abort acknowledgement must not collapse to zero aborted
    // work and a clean report.
    #[tokio::test]
    async fn lost_abort_unavailable() {
        let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
        let handle = TaskHandle {
            command_tx,
            admission_closed: Arc::new(AtomicBool::new(false)),
        };
        let scheduler = tokio::spawn(async move {
            match command_rx.recv().await {
                Some(TaskCommand::StopAdmission { response }) => {
                    let _ = response.send(1);
                }
                _ => panic!("expected stop admission command"),
            }
            match command_rx.recv().await {
                Some(TaskCommand::AwaitDrained { response }) => {
                    // Hold the waiter past the caller's drain deadline.
                    let _held = response;
                    match command_rx.recv().await {
                        Some(TaskCommand::AbortAllRunningHandlers { response }) => drop(response),
                        _ => panic!("expected abort-all command"),
                    }
                }
                _ => panic!("expected await drained command"),
            }
        });

        let report = handle.shutdown(Duration::from_millis(5)).await;
        scheduler.await.expect("fake scheduler should finish");

        assert_eq!(report.in_flight, 1);
        assert!(report.scheduler_unavailable);
        assert!(!report.drained());
    }

    // Admission stops first: timers that fire after shutdown find no handler.
    #[tokio::test]
    async fn shutdown_stops_admission() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate,
            }))
            .await;

        let report = handle.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.in_flight, 0);

        fire_timer(&handle, key.clone()).await;
        let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers { key }))
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 0);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    // Admission closes on its own, before any drain wait: a timer that fires
    // afterwards finds no handler even though nothing was waited on.
    #[tokio::test]
    async fn close_stops_handlers() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate,
            }))
            .await;
        handle.close_admission();

        fire_timer(&handle, key.clone()).await;
        let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers { key }))
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 0);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    /// Runs a synchronous section with no await point, so an abort request
    /// cannot be observed until the section ends. The markers make both the
    /// start and the end of the run explicit.
    #[derive(Clone)]
    struct SlowSyncHandler {
        runs: Arc<AtomicUsize>,
        started: Arc<Notify>,
        finished: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for SlowSyncHandler {
        async fn handle_timer(&self, _key: TaskKey) {
            self.runs.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            std::thread::sleep(Duration::from_millis(150));
            self.finished.notify_one();
        }
    }

    // A per-key abort keeps the run owned until its completion is observed: no
    // second abort can see an empty entry while the handler is still running,
    // and a third abort only reports zero after the completion arrived.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_key_retention() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let key = test_key();
        handle
            .set_inbound_handler(Arc::new(SlowSyncHandler {
                runs: runs.clone(),
                started: started.clone(),
                finished: finished.clone(),
            }))
            .await;
        fire_timer(&handle, key.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
                key: key.clone(),
            }))
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 1);

        // The handler is inside its synchronous section, so it cannot have
        // reported completion; the entry must still be owned and abortable.
        let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
                key: key.clone(),
            }))
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 1, "the aborted run must stay owned until completion");

        tokio::time::timeout(Duration::from_secs(1), finished.notified())
            .await
            .expect("handler should reach its completion");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
                    .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
                        key: key.clone(),
                    }))
                    .await
                else {
                    panic!("expected running handler abort event");
                };
                if count == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completion must clear the retained entry");
    }

    // After a per-key abort, the same key can be rescheduled; the new run only
    // starts once the aborted run reported completion.
    #[tokio::test]
    async fn reschedule_after_abort() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let key = test_key();
        handle
            .set_inbound_handler(Arc::new(SlowSyncHandler {
                runs: runs.clone(),
                started: started.clone(),
                finished: finished.clone(),
            }))
            .await;
        fire_timer(&handle, key.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler should start");

        let _ = handle
            .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
                key: key.clone(),
            }))
            .await;
        fire_timer(&handle, key.clone()).await;

        wait_for_runs(&runs, 2).await;
    }

    // Repeating shutdown after handlers were already drained is a clean no-op
    // instead of a second teardown.
    #[tokio::test]
    async fn repeat_shutdown_clean() {
        let handle = TaskHandle::new();
        let first = handle.shutdown(Duration::from_millis(10)).await;
        let second = handle.shutdown(Duration::from_millis(10)).await;

        assert!(!first.scheduler_unavailable);
        assert_eq!(first.in_flight, 0);
        assert!(first.drained());
        assert!(!second.scheduler_unavailable);
        assert_eq!(second.in_flight, 0);
        assert_eq!(second.aborted, 0);
        assert!(second.drained());
    }

    // A retained clone owns the command channel just like the original, so
    // dropping one handle must not stop the scheduler.
    #[tokio::test]
    async fn clone_keeps_scheduler() {
        let handle = TaskHandle::new();
        let clone = handle.clone();
        let key = test_key();
        drop(handle);

        let Event::Task(TaskEvent::TimerCancelled { .. }) = clone
            .send_effect(Effect::Task(TaskEffect::CancelTimer { key }))
            .await
        else {
            panic!("the retained clone must reach the scheduler");
        };
    }

    #[tokio::test]
    async fn idle_ignores_running() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let key = test_key();

        handle
            .set_inbound_handler(Arc::new(CountingGatedHandler {
                runs: runs.clone(),
                started: started.clone(),
                gate: gate.clone(),
            }))
            .await;

        fire_timer(&handle, key.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("first handler run should start");

        let TaskEvent::TimerScheduled { .. } =
            handle.schedule_idle_timer(key, Duration::ZERO).await
        else {
            panic!("expected timer scheduled event");
        };
        gate.add_permits(1);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}

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
        state.reset_timer(key.clone(), Duration::from_secs(10), start);
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

    #[test]
    fn shorten_earlier_deadlines() {
        let mut state = state();
        let start = Instant::now();
        let key = key();
        state.reset_timer(key.clone(), Duration::from_secs(10), start);

        state.shorten_timer(key.clone(), Duration::from_secs(30), start);
        assert_eq!(
            state.timers_by_key.get(&key).unwrap().deadline,
            start + Duration::from_secs(10),
            "a later request keeps the existing deadline"
        );
        state.shorten_timer(key.clone(), Duration::from_secs(5), start);
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
        state.reset_timer(key.clone(), Duration::from_secs(10), start);
        assert!(!state.timers_by_deadline.is_empty());

        state.cancel_timer(key.clone());

        assert!(state.timers_by_key.is_empty());
        assert!(
            state.timers_by_deadline.is_empty(),
            "a cancelled timer leaves no deadline entry"
        );
    }
}
