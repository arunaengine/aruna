use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait InboundTaskHandler: Send + Sync {
    async fn handle_timer(&self, key: TaskKey);
}

#[derive(Clone)]
pub struct TaskHandle {
    inner: Arc<TaskInner>,
}

struct TaskInner {
    timers: Mutex<HashMap<TaskKey, TimerEntry>>,
    inbound_handler: RwLock<Option<Arc<dyn InboundTaskHandler>>>,
    next_timer_id: AtomicU64,
}

struct TimerEntry {
    id: u64,
    deadline: Instant,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl TaskHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TaskInner {
                timers: Mutex::new(HashMap::new()),
                inbound_handler: RwLock::new(None),
                next_timer_id: AtomicU64::new(1),
            }),
        }
    }

    pub async fn set_inbound_handler(&self, handler: Arc<dyn InboundTaskHandler>) {
        *self.inner.inbound_handler.write().await = Some(handler);
    }

    pub async fn clear_inbound_handler(&self) {
        self.inner.inbound_handler.write().await.take();
    }

    fn spawn_timer(&self, key: TaskKey, deadline: Instant) -> TimerEntry {
        let timer_id = self.inner.next_timer_id.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let key_for_task = key.clone();
        let inner = self.inner.clone();

        let task = tokio::spawn(async move {
            tokio::select! {
                _ = cancel_for_task.cancelled() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    let handler = inner.inbound_handler.read().await.clone();
                    if let Some(handler) = handler {
                        handler.handle_timer(key_for_task.clone()).await;
                    }

                    let mut timers = inner.timers.lock().await;
                    if matches!(timers.get(&key_for_task), Some(entry) if entry.id == timer_id) {
                        timers.remove(&key_for_task);
                    }
                }
            }
        });

        TimerEntry {
            id: timer_id,
            deadline,
            cancel,
            task,
        }
    }

    async fn reset_timer(&self, key: TaskKey, after: std::time::Duration) -> TaskEvent {
        let deadline = Instant::now() + after;
        let entry = self.spawn_timer(key.clone(), deadline);

        let mut timers = self.inner.timers.lock().await;
        if let Some(existing) = timers.insert(key.clone(), entry) {
            existing.cancel.cancel();
            existing.task.abort();
        }

        TaskEvent::TimerScheduled { key, after }
    }

    async fn shorten_timer(&self, key: TaskKey, after: std::time::Duration) -> TaskEvent {
        let now = Instant::now();
        let requested_deadline = now + after;
        let mut timers = self.inner.timers.lock().await;

        if let Some(existing) = timers.get(&key)
            && existing.deadline <= requested_deadline
        {
            return TaskEvent::TimerScheduled {
                key,
                after: existing.deadline.saturating_duration_since(now),
            };
        }

        let entry = self.spawn_timer(key.clone(), requested_deadline);
        if let Some(existing) = timers.insert(key.clone(), entry) {
            existing.cancel.cancel();
            existing.task.abort();
        }

        TaskEvent::TimerScheduled { key, after }
    }

    async fn cancel_timer(&self, key: TaskKey) -> TaskEvent {
        let mut timers = self.inner.timers.lock().await;
        if let Some(existing) = timers.remove(&key) {
            existing.cancel.cancel();
            existing.task.abort();
        }
        TaskEvent::TimerCancelled { key }
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

    #[tokio::test]
    async fn reset_timer_keeps_newly_rescheduled_entry() {
        let handle = TaskHandle::new();
        let count = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let key = TaskKey::RealmPresence {
            realm_id: aruna_core::structs::RealmId([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        };

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
    async fn shorten_timer_does_not_lengthen_existing_entry() {
        let handle = TaskHandle::new();
        let key = TaskKey::RealmPresence {
            realm_id: aruna_core::structs::RealmId([7u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        };

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(10),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert_eq!(after, Duration::from_millis(10));

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key,
                after: Duration::from_millis(50),
            }))
            .await
        else {
            panic!("expected timer scheduled event");
        };
        assert!(after <= Duration::from_millis(10));
    }
}
