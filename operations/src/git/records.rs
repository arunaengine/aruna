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
