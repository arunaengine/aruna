use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
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
}

struct TimerEntry {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl TaskHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TaskInner {
                timers: Mutex::new(HashMap::new()),
                inbound_handler: RwLock::new(None),
            }),
        }
    }

    pub async fn set_inbound_handler(&self, handler: Arc<dyn InboundTaskHandler>) {
        *self.inner.inbound_handler.write().await = Some(handler);
    }

    pub async fn clear_inbound_handler(&self) {
        self.inner.inbound_handler.write().await.take();
    }

    async fn reset_timer(&self, key: TaskKey, after: std::time::Duration) -> TaskEvent {
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let key_for_task = key.clone();
        let inner = self.inner.clone();

        let task = tokio::spawn(async move {
            tokio::select! {
                _ = cancel_for_task.cancelled() => {}
                _ = tokio::time::sleep(after) => {
                    let handler = inner.inbound_handler.read().await.clone();
                    if let Some(handler) = handler {
                        handler.handle_timer(key_for_task.clone()).await;
                    }

                    let mut timers = inner.timers.lock().await;
                    timers.remove(&key_for_task);
                }
            }
        });

        let mut timers = self.inner.timers.lock().await;
        if let Some(existing) = timers.insert(key.clone(), TimerEntry { cancel, task }) {
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
