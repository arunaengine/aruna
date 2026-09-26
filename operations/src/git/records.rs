//! Stores immutable repository bindings and exact LFS versions in local transactions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::GitError;
use crate::driver::DriverContext;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use serde::{Serialize, de::DeserializeOwned};

pub(super) async fn save<T: Serialize>(
    context: &DriverContext,
    space: &str,
    key: Vec<u8>,
    value: &T,
) -> Result<(), GitError> {
    let value = postcard::to_allocvec(value).map_err(|_| GitError::Invalid)?;
    if !matches!(
        context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: space.into(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            }))
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ) {
        return Err(GitError::Unavailable);
    }
    if !matches!(
        context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::SyncAll))
            .await,
        Event::Storage(StorageEvent::SyncAllFinished)
    ) {
        return Err(GitError::Unavailable);
    }
    Ok(())
}

pub(super) async fn load<T: DeserializeOwned>(
    context: &DriverContext,
    space: &str,
    key: Vec<u8>,
) -> Result<Option<T>, GitError> {
    match context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: space.into(),
            key: key.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(|_| GitError::Unavailable))
            .transpose(),
        _ => Err(GitError::Unavailable),
    }
}

pub(super) async fn insert<T: Serialize + DeserializeOwned>(
    context: &DriverContext,
    space: &str,
    key: Vec<u8>,
    value: &T,
) -> Result<T, GitError> {
    let storage = &context.storage_handle;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
        .send_effect(Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        }))
        .await
    else {
        return Err(GitError::Unavailable);
    };
    let mut commit_started = false;
    let result = async {
        let Event::Storage(StorageEvent::ReadResult { value: old, .. }) = storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: space.into(),
                key: key.clone().into(),
                txn_id: Some(txn_id),
            }))
            .await
        else {
            return Err(GitError::Unavailable);
        };
        let bytes = if let Some(old) = old {
            old.to_vec()
        } else {
            let bytes = postcard::to_allocvec(value).map_err(|_| GitError::Invalid)?;
            if !matches!(
                storage
                    .send_effect(Effect::Storage(StorageEffect::Write {
                        key_space: space.into(),
                        key: key.into(),
                        value: bytes.clone().into(),
                        txn_id: Some(txn_id),
                    }))
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ) {
                return Err(GitError::Unavailable);
            }
            bytes
        };
        commit_started = true;
        if !matches!(
            storage
                .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
                .await,
            Event::Storage(StorageEvent::TransactionCommitted { .. })
        ) {
            return Err(GitError::Unavailable);
        }
        if !matches!(
            storage
                .send_effect(Effect::Storage(StorageEffect::SyncAll))
                .await,
            Event::Storage(StorageEvent::SyncAllFinished)
        ) {
            return Err(GitError::Unavailable);
        }
        postcard::from_bytes(&bytes).map_err(|_| GitError::Unavailable)
    }
    .await;
    if result.is_err() && !commit_started {
        storage
            .send_effect(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))
            .await;
    }
    result
}

/// Every stored Git record of one document, in record id order.
pub(super) async fn scan(
    context: &DriverContext,
    document_id: ulid::Ulid,
) -> Result<Vec<aruna_core::git::GitRecord>, GitError> {
    let prefix = aruna_core::git::git_record_prefix(document_id);
    let mut records = Vec::new();
    let mut start = None;
    loop {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: aruna_core::keyspaces::GIT_RECORD_KEYSPACE.into(),
                prefix: Some(prefix.clone()),
                start: start.take().map(aruna_core::effects::IterStart::After),
                limit: 256,
                txn_id: None,
            }))
            .await
        else {
            return Err(GitError::Unavailable);
        };
        for (_, value) in values {
            records.push(postcard::from_bytes(&value).map_err(|_| GitError::Unavailable)?);
        }
        // Checkpoints stay small, but every record is still read; this bounds memory use.
        if records.len() > 1024 * aruna_core::git::MAX_RECORDS {
            return Err(GitError::Unavailable);
        }
        match next_start_after {
            Some(key) => start = Some(key),
            None => return Ok(records),
        }
    }
}

/// Writes all entries in one transaction and syncs them before returning.
pub(super) async fn commit(
    context: &DriverContext,
    writes: Vec<(String, byteview::ByteView, byteview::ByteView)>,
) -> Result<(), GitError> {
    let storage = &context.storage_handle;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
        .send_effect(Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        }))
        .await
    else {
        return Err(GitError::Unavailable);
    };
    if !matches!(
        storage
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(txn_id),
            }))
            .await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ) {
        storage
            .send_effect(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))
            .await;
        return Err(GitError::Unavailable);
    }
    if !matches!(
        storage
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
            .await,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    ) || !matches!(
        storage
            .send_effect(Effect::Storage(StorageEffect::SyncAll))
            .await,
        Event::Storage(StorageEvent::SyncAllFinished)
    ) {
        return Err(GitError::Unavailable);
    }
    Ok(())
}

/// Every row in `space` whose key starts with `prefix`, in key order.
pub(super) async fn prefixed<T: DeserializeOwned>(
    context: &DriverContext,
    space: &str,
    prefix: Vec<u8>,
) -> Result<Vec<(Vec<u8>, T)>, GitError> {
    let mut rows = Vec::new();
    let mut start = None;
    loop {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: space.into(),
                prefix: Some(prefix.clone().into()),
                start: start.take().map(aruna_core::effects::IterStart::After),
                limit: 1024,
                txn_id: None,
            }))
            .await
        else {
            return Err(GitError::Unavailable);
        };
        for (key, value) in values {
            let value = postcard::from_bytes(&value).map_err(|_| GitError::Unavailable)?;
            rows.push((key.to_vec(), value));
        }
        match next_start_after {
            Some(key) => start = Some(key),
            None => return Ok(rows),
        }
    }
}

pub(super) async fn remove(
    context: &DriverContext,
    space: &str,
    key: Vec<u8>,
) -> Result<(), GitError> {
    match context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Delete {
            key_space: space.into(),
            key: key.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        _ => Err(GitError::Unavailable),
    }
}
