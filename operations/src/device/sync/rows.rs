//! Reads and writes the synced folder rows from the folder sync passes, one storage effect
//! per call; an unreadable or refused row counts as work for the next pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, TxnId, Value};

/// Reads one row, answering `None` for both an absent row and a failed read:
/// every caller here treats an unreadable row as work for the next pass.
pub(crate) async fn read_value(
    context: &Arc<crate::driver::DriverContext>,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Option<Value> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        _ => None,
    }
}

pub(crate) async fn write_rows(
    context: &Arc<crate::driver::DriverContext>,
    writes: Vec<(String, Key, Value)>,
    txn_id: Option<TxnId>,
) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite { writes, txn_id })
            .await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    )
}

pub(crate) async fn delete_rows(
    context: &Arc<crate::driver::DriverContext>,
    deletes: Vec<(String, Key)>,
    txn_id: Option<TxnId>,
) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchDelete { deletes, txn_id })
            .await,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    )
}

/// One page of a keyspace, with the cursor of the next page. `None` means the
/// scan itself failed.
pub(crate) async fn scan_page(
    context: &Arc<crate::driver::DriverContext>,
    effect: Effect,
) -> Option<(Vec<(Key, Value)>, Option<Key>)> {
    let Effect::Storage(effect) = effect else {
        return None;
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Some((values, next_start_after)),
        _ => None,
    }
}

pub(crate) async fn start_txn(context: &Arc<crate::driver::DriverContext>) -> Option<TxnId> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Some(txn_id),
        _ => None,
    }
}

pub(crate) async fn commit_txn(context: &Arc<crate::driver::DriverContext>, txn_id: TxnId) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    )
}

/// Whether storage confirmed that every committed transaction is on disk.
pub(crate) async fn sync_all(context: &Arc<crate::driver::DriverContext>) -> bool {
    matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::SyncAll)
            .await,
        Event::Storage(StorageEvent::SyncAllFinished)
    )
}

pub(crate) async fn abort_txn(context: &Arc<crate::driver::DriverContext>, txn_id: TxnId) {
    context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}
