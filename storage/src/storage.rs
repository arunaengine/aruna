//! Ties the storage submodules together; the handle, worker and owner types are its public modules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod handle;
#[cfg(debug_assertions)]
mod ledger;
pub mod lifecycle;
pub mod metrics;
pub mod owner;
pub mod persistence;
mod policies;
mod preimage;
mod rebuild;
mod records;
pub mod sealing;
mod shutdown;
mod telemetry;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_config;
#[cfg(test)]
mod tests_counters;
#[cfg(test)]
mod tests_persistence;
#[cfg(test)]
mod tests_pool;
#[cfg(test)]
mod tests_range;
#[cfg(test)]
mod tests_rebuild;
mod transactions;
mod vault;
pub mod worker;

pub(in crate::storage) use handle::{
    CleanupEntry, CleanupKind, EFFECT_QUEUE_CAPACITY, MAX_TRANSACTION_CLEANUP, QUEUE_CAPACITY,
};
pub(in crate::storage) use metrics::WorkerLifecycleGuard;
pub(in crate::storage) use worker::{
    BULK_POOL_THREADS, READ_POOL_THREADS, Store, Txn, batch_read_with, iterate_page, range_page,
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
