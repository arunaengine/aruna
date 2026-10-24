//! Drives inventory updates and accounting operations against a real temporary storage, one
//! effect at a time, so tests can interleave transactions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::structs::storage::distribution::accounting::{
    RangeAccount, RangeCounters, account_key,
};
use aruna_core::structs::storage::distribution::inventory::{HeadEntry, VersionEntry};
use aruna_core::structs::storage::distribution::{KeyRange, OriginalEpoch};
use aruna_core::types::{Effects, TxnId};
use aruna_storage::storage::{FjallStorage, StorageHandle};
use byteview::ByteView;
use tempfile::TempDir;
use ulid::Ulid;

use super::inventory::{
    AddedVersion, HeadChange, InventoryChange, InventoryError, InventoryUpdate, open_ranges,
};

pub(crate) fn open() -> (TempDir, StorageHandle) {
    let dir = tempfile::tempdir().expect("tempdir must be created");
    let handle = FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage");
    (dir, handle)
}

pub(crate) fn range(seed: u8, lower: &str, upper: Option<&str>) -> KeyRange {
    KeyRange {
        range_id: Ulid::from_bytes([seed; 16]),
        lower: lower.to_string(),
        upper: upper.map(str::to_string),
        lineage: None,
        coordinator: Ulid::nil(),
        epoch: 1,
        replicas: Vec::new(),
    }
}

pub(crate) async fn send(handle: &StorageHandle, effect: Effect) -> Event {
    let Effect::Storage(effect) = effect else {
        panic!("only storage effects are expected: {effect:?}");
    };
    handle.send_storage_effect(effect).await
}

pub(crate) async fn begin(handle: &StorageHandle) -> TxnId {
    match handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// True when the commit succeeded, false on a conflict.
pub(crate) async fn commit(handle: &StorageHandle, txn_id: TxnId) -> bool {
    match handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => true,
        Event::Storage(StorageEvent::Error { .. }) => false,
        other => panic!("unexpected storage event: {other:?}"),
    }
}

pub(crate) async fn install(handle: &StorageHandle, set_id: Ulid, ranges: &[KeyRange]) {
    let writes = open_ranges(set_id, ranges).expect("ranges encode");
    let event = handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

/// Runs `update` to the end inside the open transaction `txn_id`.
pub(crate) async fn stage(
    handle: &StorageHandle,
    txn_id: TxnId,
    update: &mut InventoryUpdate,
) -> Result<(), InventoryError> {
    let mut effects: Effects = update.start(txn_id);
    while let Some(effect) = effects.pop() {
        let event = send(handle, effect).await;
        match update.step(event, txn_id)? {
            Some(next) => effects = next,
            None => return Ok(()),
        }
    }
    Ok(())
}

/// Stages and commits `update` in its own transaction.
pub(crate) async fn apply(
    handle: &StorageHandle,
    update: InventoryUpdate,
) -> Result<bool, InventoryError> {
    let mut update = update;
    let txn_id = begin(handle).await;
    stage(handle, txn_id, &mut update).await?;
    Ok(commit(handle, txn_id).await)
}

pub(crate) async fn get(handle: &StorageHandle, key_space: &str, key: Vec<u8>) -> Option<Vec<u8>> {
    let event = handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|value| value.to_vec()),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

pub(crate) async fn counters(handle: &StorageHandle, key: Vec<u8>) -> RangeCounters {
    get(handle, RANGE_COUNTER_KEYSPACE, key)
        .await
        .map_or_else(RangeCounters::default, |value| {
            RangeCounters::from_bytes(&value).expect("counters decode")
        })
}

pub(crate) async fn has_row(handle: &StorageHandle, position: Vec<u8>) -> bool {
    get(handle, INVENTORY_KEYSPACE, position).await.is_some()
}

pub(crate) fn context(handle: &StorageHandle) -> crate::driver::DriverContext {
    crate::driver::DriverContext {
        storage_handle: handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

pub(crate) async fn account(
    handle: &StorageHandle,
    set_id: Ulid,
    range_id: Ulid,
) -> Option<RangeAccount> {
    get(
        handle,
        RANGE_ACCOUNT_KEYSPACE,
        account_key(set_id, range_id),
    )
    .await
    .map(|value| RangeAccount::from_bytes(&value).expect("account decodes"))
}

/// A write of a new current version `seed` of `size` bytes at generation 1.
pub(crate) fn write(set_id: Ulid, key: &str, seed: u8, size: u64) -> InventoryUpdate {
    let version_id = Ulid::from_bytes([seed; 16]);
    let entry = VersionEntry {
        marker: false,
        size,
        referenced: false,
        identity: Some([seed; 32]),
        pending: false,
    };
    let mut change = InventoryChange::new(set_id, key);
    change.added.push(AddedVersion {
        version_id,
        epoch: OriginalEpoch(1),
        generation: 1,
        entry,
    });
    change.head = HeadChange::Set(HeadEntry {
        version_id,
        epoch: OriginalEpoch(1),
        generation: 1,
        version: entry,
    });
    InventoryUpdate::new(change)
}

/// Removes a key's head and the versions `seeds`.
pub(crate) fn purge(set_id: Ulid, key: &str, seeds: &[u8]) -> InventoryUpdate {
    let mut change = InventoryChange::new(set_id, key);
    change.removed = seeds
        .iter()
        .map(|seed| Ulid::from_bytes([*seed; 16]))
        .collect();
    change.head = HeadChange::Remove;
    InventoryUpdate::new(change)
}

/// Counters of `objects` current versions holding `bytes`, one version each, at epoch 1.
pub(crate) fn exact(objects: i128, bytes: i128) -> RangeCounters {
    RangeCounters {
        objects,
        current_bytes: bytes,
        versions: objects,
        logical_bytes: bytes,
        max_epoch: OriginalEpoch(1),
        ..RangeCounters::default()
    }
}
