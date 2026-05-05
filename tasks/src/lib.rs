use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};

const TASK_COMMAND_BUFFER: usize = 1024;

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
    CancelTimer {
        key: TaskKey,
        response: oneshot::Sender<TaskEvent>,
    },
}

struct SchedulerState {
    timers_by_key: HashMap<TaskKey, TimerEntry>,
    timers_by_deadline: BTreeMap<(Instant, u64), TaskKey>,
    inbound_handler: Option<Arc<dyn InboundTaskHandler>>,
    next_timer_id: u64,
}

struct TimerEntry {
    id: u64,
    deadline: Instant,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            timers_by_key: HashMap::new(),
            timers_by_deadline: BTreeMap::new(),
            inbound_handler: None,
            next_timer_id: 1,
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

    fn next_deadline(&self) -> Option<Instant> {
        self.timers_by_deadline
            .first_key_value()
            .map(|(&(deadline, _), _)| deadline)
    }

    fn dispatch_due_timers(&mut self) {
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

                if let Some(handler) = self.inbound_handler.clone() {
                    tokio::spawn(async move {
                        handler.handle_timer(key).await;
                    });
                }
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

    fn cancel_timer(&mut self, key: TaskKey) -> TaskEvent {
        self.remove_timer(&key);
        TaskEvent::TimerCancelled { key }
    }

    fn handle_command(&mut self, command: TaskCommand) {
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
            TaskCommand::CancelTimer { key, response } => {
                let _ = response.send(self.cancel_timer(key));
            }
        }
    }
}

fn next_id(id: u64) -> u64 {
    id.checked_add(1).unwrap_or(1)
}

async fn run_scheduler(mut command_rx: mpsc::Receiver<TaskCommand>) {
    let mut state = SchedulerState::new();

    loop {
        state.dispatch_due_timers();

        match state.next_deadline() {
            Some(deadline) => {
                tokio::select! {
                    maybe_command = command_rx.recv() => {
                        let Some(command) = maybe_command else { break };
                        state.handle_command(command);
                    }
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => {
                let Some(command) = command_rx.recv().await else {
                    break;
                };
                state.handle_command(command);
            }
        }
    }
}

impl TaskHandle {
    pub fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(run_scheduler(command_rx));
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
}
