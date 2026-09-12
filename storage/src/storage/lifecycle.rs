use std::collections::HashMap;
use std::thread;

use fjall::OptimisticTxDatabase;

use crate::compaction::Compactor;
use crate::errors::StorageLibError;

use super::{
    BULK_EFFECT_QUEUE_CAPACITY, BULK_READ_POOL_THREADS, FjallPersistPolicy, FjallStorage,
    READ_POOL_THREADS, STORAGE_EFFECT_QUEUE_CAPACITY, StorageHandle, Store, WorkerLifecycleGuard,
    spawn_read_pool,
};

impl FjallStorage {
    #[tracing::instrument(name = "storage.open", level = "debug", fields(path = %path))]
    pub fn open(path: &str) -> Result<StorageHandle, StorageLibError> {
        Self::open_with_policy(path, FjallPersistPolicy::default())
    }

    #[doc(hidden)]
    pub fn open_test(path: &str) -> Result<StorageHandle, StorageLibError> {
        Self::open_pools(path, FjallPersistPolicy::default(), 1, 1)
    }

    #[tracing::instrument(
        name = "storage.open",
        level = "debug",
        fields(path = %path, persist_policy = policy.label())
    )]
    pub fn open_with_policy(
        path: &str,
        policy: FjallPersistPolicy,
    ) -> Result<StorageHandle, StorageLibError> {
        Self::open_pools(path, policy, READ_POOL_THREADS, BULK_READ_POOL_THREADS)
    }

    fn open_pools(
        path: &str,
        policy: FjallPersistPolicy,
        read_threads: usize,
        bulk_threads: usize,
    ) -> Result<StorageHandle, StorageLibError> {
        let db = OptimisticTxDatabase::builder(path)
            .manual_journal_persist(true)
            .open()?;

        let (sender, receivers) = StorageHandle::new();
        let mut storage = Self::new(Store::new(db), policy, &sender, read_threads, bulk_threads);
        let channel_closed = sender.metrics.channel_closed.clone();

        let worker = thread::spawn(move || {
            let _lifecycle = WorkerLifecycleGuard(channel_closed);
            storage.receive_loop(receivers);
            storage.close();
        });
        sender
            .worker
            .lock()
            .expect("storage worker mutex poisoned")
            .replace(worker);

        Ok(sender)
    }

    fn close(mut self) {
        self.compactor.shutdown();
        self.read_pool.clear();
        self.bulk_read_pool.clear();
        for reader in std::mem::take(&mut self.pool_threads) {
            let _ = reader.join();
        }
    }

    pub(super) fn new(
        store: Store,
        policy: FjallPersistPolicy,
        handle: &StorageHandle,
        read_threads: usize,
        bulk_threads: usize,
    ) -> Self {
        let (read_pool, mut pool_threads) =
            spawn_read_pool(store.clone(), read_threads, STORAGE_EFFECT_QUEUE_CAPACITY);
        let (bulk_read_pool, bulk_threads) =
            spawn_read_pool(store.clone(), bulk_threads, BULK_EFFECT_QUEUE_CAPACITY);
        pool_threads.extend(bulk_threads);
        Self {
            store,
            persist_policy: policy,
            txns: HashMap::new(),
            transaction_cleanup: handle.transaction_cleanup.clone(),
            metrics: handle.metrics.clone(),
            read_pool,
            next_reader: 0,
            bulk_read_pool,
            next_bulk_reader: 0,
            pool_threads,
            compactor: Compactor::spawn(),
            deletes: HashMap::new(),
            txn_deletes: HashMap::new(),
        }
    }
}
