use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

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
}

/// Outcome of draining the scheduler on shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskShutdownReport {
    /// Handlers still running when admission stopped.
    pub in_flight: usize,
    /// Handlers aborted because they outlived the drain deadline.
    pub aborted: usize,
}

impl TaskShutdownReport {
    pub fn drained(&self) -> bool {
        self.aborted == 0
    }
}

struct SchedulerState {
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
    task: JoinHandle<()>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
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

    fn track_running_task(&mut self, task: &RunningTask, handle: JoinHandle<()>) {
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
                task: handle,
            },
        );
        self.running_warn_deadlines.insert((warn_at, id));
        *self.in_flight_keys.entry(task.key.clone()).or_insert(0) += 1;
    }

    fn spawn_handler_for_key(
        &mut self,
        key: TaskKey,
        started_at: Instant,
        command_tx: &mpsc::WeakSender<TaskCommand>,
    ) {
        if let Some(handler) = self.inbound_handler.clone() {
            let task = self.prepare_running_task(key, started_at);
            let handle = spawn_timer_handler(handler, command_tx.clone(), task.clone());
            self.track_running_task(&task, handle);
        }
    }

    fn release_in_flight_key(&mut self, key: &TaskKey) -> bool {
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

    fn dispatch_due_timers(&mut self, command_tx: &mpsc::WeakSender<TaskCommand>) {
        let now = Instant::now();

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
                    self.spawn_handler_for_key(key, now, command_tx);
                }
            }
        }
    }

    fn warn_for_long_running_tasks(&mut self) {
        let now = Instant::now();

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

    fn reset_timer(&mut self, key: TaskKey, after: Duration) -> TaskEvent {
        let Some(deadline) = Instant::now().checked_add(after) else {
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

    fn shorten_timer(&mut self, key: TaskKey, after: Duration) -> TaskEvent {
        let now = Instant::now();
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

    fn schedule_timer_if_idle(&mut self, key: TaskKey, after: Duration) -> TaskEvent {
        let now = Instant::now();
        if let Some(existing) = self.timers_by_key.get(&key) {
            return TaskEvent::TimerScheduled {
                key,
                after: existing.deadline.saturating_duration_since(now),
            };
        }

        if self.in_flight_keys.contains_key(&key) {
            return TaskEvent::TimerScheduled { key, after };
        }

        self.reset_timer(key, after)
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

        if self.release_in_flight_key(&entry.key) && self.refire_requested.remove(&entry.key) {
            self.spawn_handler_for_key(entry.key, Instant::now(), command_tx);
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

    fn abort_all_running_handlers(&mut self) -> usize {
        let aborted = self.running_by_id.len();
        for (_, entry) in self.running_by_id.drain() {
            entry.task.abort();
        }
        self.running_warn_deadlines.clear();
        self.in_flight_keys.clear();
        self.notify_if_drained();
        aborted
    }

    fn notify_if_drained(&mut self) {
        if self.running_by_id.is_empty() {
            for waiter in self.drained_waiters.drain(..) {
                let _ = waiter.send(());
            }
        }
    }

    fn abort_running_handlers(&mut self, key: TaskKey) -> TaskEvent {
        let run_ids = self
            .running_by_id
            .iter()
            .filter_map(|(run_id, entry)| (entry.key == key).then_some(*run_id))
            .collect::<Vec<_>>();

        for run_id in &run_ids {
            if let Some(entry) = self.running_by_id.remove(run_id) {
                self.running_warn_deadlines
                    .remove(&(entry.warn_at, *run_id));
                entry.task.abort();
                self.release_in_flight_key(&entry.key);
            }
        }
        self.refire_requested.remove(&key);
        self.notify_if_drained();

        TaskEvent::RunningHandlersAborted {
            key,
            count: run_ids.len(),
        }
    }

    fn handle_command(&mut self, command: TaskCommand, command_tx: &mpsc::WeakSender<TaskCommand>) {
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
                let _ = response.send(self.reset_timer(key, after));
            }
            TaskCommand::ShortenTimer {
                key,
                after,
                response,
            } => {
                let _ = response.send(self.shorten_timer(key, after));
            }
            TaskCommand::ScheduleTimerIfIdle {
                key,
                after,
                response,
            } => {
                let _ = response.send(self.schedule_timer_if_idle(key, after));
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
            } => self.complete_handler(run_id, key, elapsed, command_tx),
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
                let _ = response.send(self.abort_all_running_handlers());
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
) {
    let mut state = SchedulerState::new();

    loop {
        state.dispatch_due_timers(&command_tx);
        state.warn_for_long_running_tasks();

        match state.next_deadline() {
            Some(deadline) => {
                tokio::select! {
                    maybe_command = command_rx.recv() => {
                        let Some(command) = maybe_command else { break };
                        state.handle_command(command, &command_tx);
                    }
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => {
                let Some(command) = command_rx.recv().await else {
                    break;
                };
                state.handle_command(command, &command_tx);
            }
        }
    }
}

fn spawn_timer_handler(
    handler: Arc<dyn InboundTaskHandler>,
    command_tx: mpsc::WeakSender<TaskCommand>,
    task: RunningTask,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if task.id.is_none() {
            warn!(
                key = ?task.key,
                "Timer handler task is running without runtime tracking because run id space is exhausted"
            );
        }

        handler.handle_timer(task.key.clone()).await;

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

impl TaskHandle {
    pub fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(run_scheduler(command_rx, command_tx.downgrade()));
        } else {
            warn!("TaskHandle created without an active Tokio runtime; task scheduler unavailable");
        }

        Self { command_tx }
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

    async fn reset_timer(&self, key: TaskKey, after: Duration) -> TaskEvent {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::ResetTimer {
                key,
                after,
                response,
            })
            .await
            .is_err()
        {
            return scheduler_unavailable(command_key);
        }

        result
            .await
            .unwrap_or_else(|_| scheduler_unavailable(command_key))
    }

    async fn shorten_timer(&self, key: TaskKey, after: Duration) -> TaskEvent {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::ShortenTimer {
                key,
                after,
                response,
            })
            .await
            .is_err()
        {
            return scheduler_unavailable(command_key);
        }

        result
            .await
            .unwrap_or_else(|_| scheduler_unavailable(command_key))
    }

    pub async fn schedule_timer_if_idle(&self, key: TaskKey, after: Duration) -> TaskEvent {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::ScheduleTimerIfIdle {
                key,
                after,
                response,
            })
            .await
            .is_err()
        {
            return scheduler_unavailable(command_key);
        }

        result
            .await
            .unwrap_or_else(|_| scheduler_unavailable(command_key))
    }

    async fn cancel_timer(&self, key: TaskKey) -> TaskEvent {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::CancelTimer { key, response })
            .await
            .is_err()
        {
            return scheduler_unavailable(command_key);
        }

        result
            .await
            .unwrap_or_else(|_| scheduler_unavailable(command_key))
    }

    pub async fn abort_running_handlers(&self, key: TaskKey) -> TaskEvent {
        let command_key = key.clone();
        let (response, result) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::AbortRunningHandlers { key, response })
            .await
            .is_err()
        {
            return scheduler_unavailable(command_key);
        }

        result
            .await
            .unwrap_or_else(|_| scheduler_unavailable(command_key))
    }

    /// Stops admitting timer handlers, waits up to `drain` for the ones already
    /// running, then aborts whatever is left. Pending timers are durable, so an
    /// undrained handler is retried on the next boot rather than lost.
    pub async fn shutdown(&self, drain: Duration) -> TaskShutdownReport {
        let (response, stopped) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::StopAdmission { response })
            .await
            .is_err()
        {
            return TaskShutdownReport {
                in_flight: 0,
                aborted: 0,
            };
        }
        let in_flight = stopped.await.unwrap_or(0);
        if in_flight == 0 {
            return TaskShutdownReport {
                in_flight: 0,
                aborted: 0,
            };
        }

        let (response, drained) = oneshot::channel();
        if self
            .command_tx
            .send(TaskCommand::AwaitDrained { response })
            .await
            .is_ok()
            && tokio::time::timeout(drain, drained).await.is_ok()
        {
            return TaskShutdownReport {
                in_flight,
                aborted: 0,
            };
        }

        let (response, aborted) = oneshot::channel();
        let aborted = if self
            .command_tx
            .send(TaskCommand::AbortAllRunningHandlers { response })
            .await
            .is_ok()
        {
            aborted.await.unwrap_or(0)
        } else {
            0
        };
        if aborted > 0 {
            warn!(
                aborted,
                drain_ms = drain.as_millis(),
                "Aborted timer handlers that outlived the shutdown drain deadline"
            );
        }

        TaskShutdownReport { in_flight, aborted }
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

    fn test_key() -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: aruna_core::structs::RealmId([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        }
    }

    #[tokio::test]
    async fn reset_timer_keeps_newly_rescheduled_entry() {
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
    async fn reset_timer_from_handler_does_not_cancel_running_handler() {
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
    async fn abort_running_handlers_aborts_matching_handler_task() {
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
    async fn overlapping_fires_coalesce_into_one_follow_up_run() {
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
    async fn fire_during_running_handler_triggers_follow_up_run() {
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
    async fn sequential_fires_run_once_each() {
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
    async fn shorten_timer_does_not_lengthen_existing_entry() {
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
    async fn schedule_timer_if_idle_keeps_existing_timer() {
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
            handle.schedule_timer_if_idle(key, Duration::ZERO).await
        else {
            panic!("expected timer scheduled event");
        };
        assert!(after > Duration::from_secs(3000));
    }

    // A handler that finishes inside the drain budget is joined, not cut.
    #[tokio::test]
    async fn shutdown_drains_running_handler() {
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
    async fn shutdown_aborts_stuck_handler() {
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
        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("stuck handler future should be dropped");
    }

    // Admission stops first: timers that fire after shutdown find no handler.
    #[tokio::test]
    async fn shutdown_stops_new_handlers() {
        let handle = TaskHandle::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(100));
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

        fire_timer(&handle, key).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn schedule_timer_if_idle_ignores_running_handler() {
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
            handle.schedule_timer_if_idle(key, Duration::ZERO).await
        else {
            panic!("expected timer scheduled event");
        };
        gate.add_permits(1);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
