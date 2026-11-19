//! Reads one page of a keyspace range straight from storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, TxnId, Value};

use crate::StorageHandle;

/// One page of `key_space` under `prefix` after `start_after`, and the key the next page
/// starts after.
pub async fn iter_prefix_page(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<Key>,
    start_after: Option<Key>,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix,
        start: start_after.map(IterStart::After),
        limit,
        txn_id,
    };
    match storage.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}
