//! Read and write helpers that document sync runs against the storage actor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl DocumentSyncService {
    pub(super) async fn storage_read(
        &self,
        key_space: String,
        key: ByteView,
    ) -> Result<Option<Value>> {
        storage_read_from(&self.storage, key_space, key).await
    }

    pub(super) async fn storage_batch_write(
        &self,
        writes: Vec<(String, ByteView, Value)>,
    ) -> Result<()> {
        batch_write_to(&self.storage, writes).await
    }
}

pub(super) async fn storage_read_from(
    storage: &StorageHandle,
    key_space: String,
    key: ByteView,
) -> Result<Option<Value>> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space,
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while applying document sync read: {other:?}"
        ))),
    }
}

pub(crate) async fn batch_write_to(
    storage: &StorageHandle,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while applying document sync batch write: {other:?}"
        ))),
    }
}
