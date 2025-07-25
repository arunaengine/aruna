use aruna_storage::storage::store::Store;
use error::ArunaTaskError;
use iroh::NodeAddr;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{Arc, atomic::AtomicU8},
};
use task_trait::TaskExecutor;
use tracing::error;
use ulid::Ulid;

pub mod error;
pub mod task_trait;

// Table where tasks are stored
pub const TASK_DB_NAME: &str = "tasks";

pub struct TaskHandler<S, const N: usize, E: TaskExecutor>
where
    for<'a> S: Store<'a>,
{
    task_registry: TaskRegistry<S, N, E>,
}

#[derive(Serialize, Deserialize, Clone)]
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

pub static REGISTRY_IDX: AtomicU8 = AtomicU8::new(0);

impl<S, const N: usize, E: TaskExecutor> TaskHandler<S, N, E>
where
    for<'a> S: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(store))]
    pub async fn init(store: S) -> Result<Self, ArunaTaskError> {
        let store_clone = store.clone();
        let current_span = tracing::Span::current();
        let queue = tokio::task::spawn_blocking(move || {
            current_span.in_scope(|| {
                let txn = store_clone.create_txn(false)?;

                let mut queue = VecDeque::new();

                let task_iterator = store_clone.iter_db(&txn, TASK_DB_NAME)?;
                for (_id, task) in task_iterator {
                    let task = task.as_ref().try_into()?;
                    queue.push_back(task);
                }

                store_clone.commit(txn)?;

                Ok::<VecDeque<Task>, ArunaTaskError>(queue)
            })
        })
        .await??;

        let handler = TaskHandler {
            task_registry: TaskRegistry {
                store,
                queue: Arc::new(Mutex::new(queue)),
                registry: Vec::with_capacity(N),
            },
        };
        Ok(handler)
    }

    #[tracing::instrument(level = "trace", skip(self, executor))]
    pub async fn add_executor(&mut self, executor: E) {
        let idx = REGISTRY_IDX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.task_registry.registry.insert(idx as usize, executor);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn start_tasks(self) -> Result<(), ArunaTaskError> {
        // Message receiving loop
        let current_span = tracing::Span::current();
        let registry = self.task_registry;

        // Task handling loop
        tokio::spawn(async move {
            current_span.in_scope(async || {
                let registry = registry;
                loop {
                    if let Err(err) = registry.handle_task().await {
                        error!("{err}");
                        continue;
                    }
                }
            })
        });

        Ok(())
    }
}

struct TaskRegistry<S, const N: usize, E: TaskExecutor> {
    store: S,
    queue: Arc<Mutex<VecDeque<Task>>>,
    registry: Vec<E>,
}

impl<S, const N: usize, E: TaskExecutor> TaskRegistry<S, N, E>
where
    for<'a> S: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_task(&self) -> Result<(), ArunaTaskError> {
        let Some(mut task) = self.queue.lock().pop_front() else {
            return Ok(());
        };
        if let Some(executor) = self.registry.get(task.executor_idx as usize) {
            // Run
            let store = self.store.clone();
            task.status = TaskStatus::Running;
            task.last_accessed = chrono::Utc::now().timestamp();
            let task_clone = task.clone();
            tokio::task::spawn_blocking(move || {
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
            .await??;

            match executor.execute(task.clone()).await {
                Ok(_) => {
                    task.status = TaskStatus::Completed;
                }
                Err(err) => {
                    task.status = TaskStatus::Failed(err.to_string());
                    self.queue.lock().push_back(task.clone());
                }
            };
            task.last_accessed = chrono::Utc::now().timestamp();

            let store = self.store.clone();
            tokio::task::spawn_blocking(move || {
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
            .await??;
        }
        Ok(())
    }
}
