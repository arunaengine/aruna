//! The Fjall-backed storage handle, its worker, and transaction ownership.
//! This file is the facade: behavior lives in focused modules (`handle`, `owner`,
//! `worker`, `lifecycle`, `persistence`, `metrics`, `telemetry`) re-exported here.

mod handle;
mod lifecycle;
mod metrics;
mod owner;
mod persistence;
mod records;
mod shutdown;
mod telemetry;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_persistence;
mod transactions;
mod worker;

pub use handle::{
    EffectHandle, EffectReceiver, EffectSender, ResponseSender, StorageHandle, StorageReceivers,
};
pub use metrics::{InFlightGuard, StorageMetricsSnapshot};
pub use owner::TransactionOwner;
pub use persistence::FjallPersistPolicy;
pub use worker::FjallStorage;

pub(in crate::storage) use handle::{
    CleanupEntry, CleanupKind, EFFECT_QUEUE_CAPACITY, MAX_TRANSACTION_CLEANUP, QUEUE_CAPACITY,
};
pub(in crate::storage) use metrics::WorkerLifecycleGuard;
pub(in crate::storage) use worker::{
    BULK_POOL_THREADS, READ_POOL_THREADS, Store, Txn, batch_read_with, iterate_page,
    read_last_with, spawn_read_pool, store_batch_read, store_iterate, store_last, store_read,
};

#[cfg(test)]
pub(in crate::storage) use handle::{
    MAX_CLEANUP_ATTEMPTS, ResponseToken, finish_cleanup, is_cleanup_write, reserve_cleanup,
    response_channel,
};
#[cfg(test)]
pub(in crate::storage) use metrics::StorageMetrics;
#[cfg(test)]
pub(in crate::storage) use telemetry::storage_effect_span;
#[cfg(test)]
pub(in crate::storage) use worker::{
    FOREGROUND_PER_BULK, LaneScheduler, MAX_GROUP_COMMIT, SlowQueueAggregator, effect_keyspace,
};
