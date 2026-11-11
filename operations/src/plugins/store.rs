//! Node-local rows of plugin runs: reads, pages and transactions that commit all or nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::TxnId;

use crate::driver::DriverContext;

/// A storage failure, with the event that was not expected.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("plugin run storage failed: {0}")]
pub struct StoreError(pub String);

pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// One open storage transaction; `transact` commits or aborts it.
pub struct Txn<'a> {
    ctx: &'a DriverContext,
    id: Option<TxnId>,
}

/// Runs `body` in one transaction and commits it, or aborts it when `body` fails.
pub async fn transact<T, E: From<StoreError>>(
    ctx: &DriverContext,
    body: impl AsyncFnOnce(&Txn<'_>) -> Result<T, E>,
) -> Result<T, E> {
    let start = StorageEffect::StartTransaction { read: false };
    let id = match ctx.storage_handle.send_storage_effect(start).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(failed(other).into()),
    };
    let txn = Txn { ctx, id: Some(id) };
    match body(&txn).await {
        Ok(value) => {
            let commit = StorageEffect::CommitTransaction { txn_id: id };
            match ctx.storage_handle.send_storage_effect(commit).await {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(value),
                other => Err(failed(other).into()),
            }
        }
        Err(error) => {
            let abort = StorageEffect::AbortTransaction { txn_id: id };
            ctx.storage_handle.send_storage_effect(abort).await;
            Err(error)
        }
    }
}

impl<'a> Txn<'a> {
    /// Reads and writes outside a transaction, each on its own.
    pub fn direct(ctx: &'a DriverContext) -> Self {
        Self { ctx, id: None }
    }

    pub async fn read(&self, space: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let read = StorageEffect::Read {
            key_space: space.to_string(),
            key: key.to_vec().into(),
            txn_id: self.id,
        };
        match self.ctx.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                Ok(value.map(|value| value.to_vec()))
            }
            other => Err(failed(other)),
        }
    }

    /// At most `limit` rows below `prefix` after the key `after`, and the key to continue after.
    pub async fn page(
        &self,
        space: &str,
        prefix: &[u8],
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Rows, Option<Vec<u8>>), StoreError> {
        let iter = StorageEffect::Iter {
            key_space: space.to_string(),
            prefix: Some(prefix.to_vec().into()),
            start: after.map(|key| IterStart::After(key.into())),
            limit,
            txn_id: self.id,
        };
        match self.ctx.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => Ok((
                (values.into_iter())
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .collect(),
                next_start_after.map(|key| key.to_vec()),
            )),
            other => Err(failed(other)),
        }
    }

    /// Every row below `prefix`, read page by page.
    pub async fn scan(&self, space: &str, prefix: &[u8]) -> Result<Rows, StoreError> {
        let (mut rows, mut after) = (Vec::new(), None);
        loop {
            let (page, next) = self.page(space, prefix, after, 1_000).await?;
            rows.extend(page);
            match next {
                Some(key) => after = Some(key),
                None => return Ok(rows),
            }
        }
    }

    pub async fn write(&self, space: &str, rows: Rows) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        let writes = (rows.into_iter())
            .map(|(key, value)| (space.to_string(), key.into(), value.into()))
            .collect();
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: self.id,
        };
        match self.ctx.storage_handle.send_storage_effect(write).await {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
            other => Err(failed(other)),
        }
    }

    pub async fn delete(&self, space: &str, keys: Vec<Vec<u8>>) -> Result<(), StoreError> {
        if keys.is_empty() {
            return Ok(());
        }
        let deletes = (keys.into_iter())
            .map(|key| (space.to_string(), key.into()))
            .collect();
        let delete = StorageEffect::BatchDelete {
            deletes,
            txn_id: self.id,
        };
        match self.ctx.storage_handle.send_storage_effect(delete).await {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
            other => Err(failed(other)),
        }
    }
}

fn failed(event: Event) -> StoreError {
    StoreError(format!("{event:?}"))
}

pub fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    postcard::to_allocvec(value).map_err(|error| StoreError(error.to_string()))
}

pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
    postcard::from_bytes(bytes).map_err(|error| StoreError(error.to_string()))
}
