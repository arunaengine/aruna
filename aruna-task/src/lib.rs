use aruna_storage::storage::store::Store;
use error::ArunaTaskError;
use iroh::NodeAddr;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{Arc, atomic::AtomicU8},
};
use task_trait::TaskExecutor;
use tokio::{sync::{Notify, RwLock}, task::JoinSet};
use tracing::{error, trace, warn};
use ulid::Ulid;

pub mod error;
pub mod task_trait;

// Table where tasks are stored
pub const TASK_DB_NAME: &str = "tasks";

#[derive(Clone)]
pub struct TaskHandler<S>
where
    for<'a> S: Store<'a> + 'static,
{
    registry_idx: Arc<AtomicU8>,
    task_registry: TaskRegistry<S>,
    sender: tokio::sync::mpsc::Sender<(Task, Option<Arc<Notify>>)>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Task {
    pub id: Ulid,
    pub distribution_strategy: DistributionStrategy,
    pub last_accessed: i64,
    pub retry_strategy: RetryStrategy,
    pub status: TaskStatus,
    pub executor_idx: u8,
    pub payload: Vec<u8>,
}

impl TryFrom<&[u8]> for Task {
    type Error = ArunaTaskError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let res = postcard::from_bytes(value)?;
        Ok(res)
    }
}
impl TryFrom<Task> for Vec<u8> {
    type Error = ArunaTaskError;
    fn try_from(value: Task) -> Result<Self, Self::Error> {
        let res = postcard::to_allocvec(&value)?;
        Ok(res)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DistributionStrategy {
    All,
    AllInRealm,
    Limited(u32),
    LimitedRealm(u32),
    Specific(Vec<NodeAddr>),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RetryStrategy {
    Forever,
    Max(u32),
    Once,
}

pub const TASK_EXECUTORS: usize = 3;

impl<S> TaskHandler<S>
where
    for<'a> S: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(store))]
    pub async fn new(store: S) -> Result<Self, ArunaTaskError> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1000);
        let store_clone = store.clone();
        let current_span = tracing::Span::current();
        let queue = Arc::new(Mutex::new(
            tokio::task::spawn_blocking(move || {
                current_span.in_scope(|| {
                    let txn = store_clone.create_txn(false)?;

                    let mut queue = VecDeque::new();

                    let task_iterator = store_clone.iter_db(&txn, TASK_DB_NAME)?;
                    for (_id, task) in task_iterator {
                        let task = task.as_ref().try_into()?;
                        queue.push_back((task, None));
                    }

                    store_clone.commit(txn)?;

                    Ok::<VecDeque<(Task, Option<Arc<Notify>>)>, ArunaTaskError>(queue)
                })
            })
            .await??,
        ));

        let task_registry = TaskRegistry {
            store: store.clone(),
            queue: queue.clone(),
            registry: Arc::new(RwLock::new(Vec::with_capacity(3))),
        };

        let message_handler = MessageHandler::<S> {
            store,
            queue,
            channel: receiver,
        };
        TaskHandler::start_tasks(task_registry.clone(), message_handler).await.detach_all();

        Ok(TaskHandler {
            // This is stupid, just remove it
            registry_idx: Arc::new(AtomicU8::new(0)),
            task_registry,
            sender,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, executor))]
    pub async fn add_executor(&mut self, executor: Box<dyn TaskExecutor + 'static>) -> u8 {
        let idx = self
            .registry_idx
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.task_registry
            .registry
            .write()
            .await
            .insert(idx as usize, executor);
        idx
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn register_task(
        &self,
        distribution_strategy: DistributionStrategy,
        retry_strategy: RetryStrategy,
        executor_idx: u8,
        payload: Vec<u8>,
        backchannel: Option<Arc<tokio::sync::Notify>>,
    ) -> Result<(), ArunaTaskError> {
        Ok(self
            .sender
            .send((
                Task {
                    id: Ulid::new(),
                    distribution_strategy,
                    last_accessed: chrono::Utc::now().timestamp(),
                    retry_strategy,
                    status: TaskStatus::Pending,
                    executor_idx,
                    payload,
                },
                backchannel,
            ))
            .await
            .map_err(|e| {
                error!("{e}");
                e
            })?)
    }

    #[tracing::instrument(level = "trace", skip(task_registry, message_handler))]
    async fn start_tasks(
        task_registry: TaskRegistry<S>,
        message_handler: MessageHandler<S>,
    ) -> JoinSet<()> {
        // Message receiving loop
        let current_span = tracing::Span::current();
        let mut set = JoinSet::new();
        set.spawn(current_span.in_scope(async || {
            let mut message_handler = message_handler;
            loop {
                if let Err(err) = message_handler.handle_messages().await {
                    error!("{err}");
                    continue;
                }
            }
        }));

        // Task handling loop
        let current_span = tracing::Span::current();
        let registry = task_registry;
        set.spawn(current_span.in_scope(async || {
            let registry = registry;
            loop {
                if let Err(err) = registry.handle_tasks().await {
                    error!("{err}");
                    continue;
                }
            }
        }));

        set
    }
}

// #[derive(Clone)]
struct TaskRegistry<S> {
    store: S,
    queue: Arc<Mutex<VecDeque<(Task, Option<Arc<Notify>>)>>>,
    registry: Arc<RwLock<Vec<Box<dyn TaskExecutor>>>>,
}

impl<S> Clone for TaskRegistry<S>
where
    for<'a> S: Store<'a> + 'static,
{
    fn clone(&self) -> Self {
        TaskRegistry {
            store: self.store.clone(),
            queue: self.queue.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl<S> TaskRegistry<S>
where
    for<'a> S: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_tasks(&self) -> Result<(), ArunaTaskError> {
        let (mut task, notify) = {
            let mut lock = self.queue.lock().await;
            if !lock.is_empty() {
                trace!("Processing task {:?}", lock);
            }
            let Some((task, notify)) = lock.pop_front() else {
                return Ok(());
            };
            drop(lock);
            (task, notify)
        };
        match self.registry.read().await.get(task.executor_idx as usize) {
            Some(executor) => {
                trace!("Got task");
                // Run
                let store = self.store.clone();
                task.status = TaskStatus::Running;
                task.last_accessed = chrono::Utc::now().timestamp();
                let task_clone = task.clone();
                let current_span = tracing::Span::current();
                tokio::task::spawn_blocking(move || {
                    current_span.in_scope(|| {
                        let mut txn = store.create_txn(true)?;
                        store.put(
                            &mut txn,
                            TASK_DB_NAME,
                            task.id.to_bytes().as_slice(),
                            <Vec<u8>>::try_from(task_clone)?.as_slice(),
                        )?;

                        store.commit(txn)?;
                        Ok::<(), ArunaTaskError>(())
                    })
                })
                .await??;

                trace!("Executing ... ");
                match executor.execute(task.clone()).await {
                    Ok(_) => {
                        trace!("Executing ok ");
                        task.status = TaskStatus::Completed;
                        if let Some(notify) = notify {
                            trace!("Notify process");
                            notify.notify_waiters();
                        }
                    }
                    Err(err) => {
                        error!("Execution error: {err}");
                        task.status = TaskStatus::Failed(err.to_string());
                        let mut lock = self.queue.lock().await;
                        lock.push_back((task.clone(), notify));
                        drop(lock);
                    }
                };
                task.last_accessed = chrono::Utc::now().timestamp();

                let store = self.store.clone();
                let current_span = tracing::Span::current();
                tokio::task::spawn_blocking(move || {
                    current_span.in_scope(|| {
                        let mut txn = store.create_txn(true)?;
                        store.put(
                            &mut txn,
                            TASK_DB_NAME,
                            task.id.to_bytes().as_slice(),
                            <Vec<u8>>::try_from(task.clone())?.as_slice(),
                        )?;
                        store.commit(txn)?;
                        Ok::<(), ArunaTaskError>(())
                    })
                })
                .await??;
                trace!("Return");
            }
            None => {
                warn!("Task handler executors not initialized!");
                task.status = TaskStatus::Failed("Task handler cannot find executor".to_string());
                let mut lock = self.queue.lock().await;
                lock.push_back((task.clone(), notify));
                drop(lock);
            }
        }

        trace!("Next task");
        Ok(())
    }
}

struct MessageHandler<S> {
    store: S,
    queue: Arc<Mutex<VecDeque<(Task, Option<Arc<Notify>>)>>>,
    channel: tokio::sync::mpsc::Receiver<(Task, Option<Arc<Notify>>)>,
}

impl<S> MessageHandler<S>
where
    for<'a> S: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_messages(&mut self) -> Result<(), ArunaTaskError> {
        if let Some((task, notify)) = self.channel.recv().await {
            trace!("Got msg task");
            let store = self.store.clone();
            let task_clone = task.clone();
            let current_span = tracing::Span::current();
            tokio::task::spawn_blocking(move || {
                current_span.in_scope(|| {
                    let mut txn = store.create_txn(true)?;
                    store.put(
                        &mut txn,
                        TASK_DB_NAME,
                        task.id.to_bytes().as_slice(),
                        <Vec<u8>>::try_from(task_clone)?.as_slice(),
                    )?;
                    store.commit(txn)?;
                    Ok::<(), ArunaTaskError>(())
                })
            })
            .await??;
            let mut lock = self.queue.lock().await;
            lock.push_back((task, notify));
            trace!("{:?}", lock);
            drop(lock);
        }
        Ok(())
    }
}
