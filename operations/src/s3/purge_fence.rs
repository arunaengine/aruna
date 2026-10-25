//! Reads, takes and releases the per bucket write fence that a permanent purge holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::types::{Key, TxnId};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use thiserror::Error;

use crate::distribution::guard::{GuardError, WriteAuthority, authority_reads, check_authority};

const FENCE_MUTATE_ATTEMPTS: usize = 8;

#[derive(Debug, Error, PartialEq)]
pub enum PurgeFenceError {
    #[error("writes to this object scope are suspended while a permanent purge is in progress")]
    Suspended,
    #[error("another permanent purge already owns this bucket's write fence")]
    Busy,
    #[error("the permanent-purge fence record is invalid")]
    Invalid,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected permanent-purge fence event: {0}")]
    Unexpected(String),
    /// The distributed bucket write lost its authority before it committed.
    #[error(transparent)]
    Distribution(GuardError),
}

pub fn fence_key(bucket: &str) -> Key {
    ByteView::from(bucket.as_bytes().to_vec())
}

pub fn write_fence_read(bucket: &str, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: PURGE_FENCE_KEYSPACE.to_string(),
        key: fence_key(bucket),
        txn_id,
    })
}

pub fn check_write_fence(event: Event, bucket: &str, key: &str) -> Result<(), PurgeFenceError> {
    let value = match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        event => return Err(PurgeFenceError::Unexpected(format!("{event:?}"))),
    };
    let fence = value
        .map(|value| {
            StoragePurgeFence::from_bytes(value.as_ref()).map_err(|_| PurgeFenceError::Invalid)
        })
        .transpose()?;
    check_decoded_fence(fence.as_ref(), bucket, key)
}

/// The purge fence of `bucket` and, for a write to a distributed bucket, the record and fence
/// rows its authority is checked against, all read in the publication transaction.
pub fn publication_read(
    bucket: &str,
    authority: Option<&WriteAuthority>,
    txn_id: Option<TxnId>,
) -> Effect {
    let Some(authority) = authority else {
        return write_fence_read(bucket, txn_id);
    };
    let mut reads = vec![(PURGE_FENCE_KEYSPACE.to_string(), fence_key(bucket))];
    reads.extend(authority_reads(authority.binding.set_id));
    Effect::Storage(StorageEffect::BatchRead { reads, txn_id })
}

/// Checks the answer to [`publication_read`]: no purge fence covers the key and the write
/// still holds its distributed authority.
pub fn check_publication(
    event: Event,
    bucket: &str,
    key: &str,
    authority: Option<&WriteAuthority>,
) -> Result<(), PurgeFenceError> {
    let Some(authority) = authority else {
        return check_write_fence(event, bucket, key);
    };
    let values = match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == 3 => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        event => return Err(PurgeFenceError::Unexpected(format!("{event:?}"))),
    };
    let mut values = values.into_iter().map(|(_, value)| value);
    let (purge, record, fence) = (values.next(), values.next(), values.next());
    let purge = (purge.flatten())
        .map(|value| {
            StoragePurgeFence::from_bytes(value.as_ref()).map_err(|_| PurgeFenceError::Invalid)
        })
        .transpose()?;
    check_decoded_fence(purge.as_ref(), bucket, key)?;
    let (record, fence) = (record.flatten(), fence.flatten());
    check_authority(authority, key, record.as_deref(), fence.as_deref())
        .map_err(PurgeFenceError::Distribution)
}

/// Cheap rejection before a writer moves payload bytes. Mutating operations
/// still read the fence again in their commit transaction to close the race
/// with a fence acquired after this read.
pub async fn ensure_write_allowed(
    storage: &StorageHandle,
    bucket: &str,
    key: &str,
) -> Result<(), PurgeFenceError> {
    let fence = read_fence(storage, bucket, None).await?;
    check_decoded_fence(fence.as_ref(), bucket, key)
}

pub(crate) fn check_decoded_fence(
    fence: Option<&StoragePurgeFence>,
    bucket: &str,
    key: &str,
) -> Result<(), PurgeFenceError> {
    let Some(fence) = fence else {
        return Ok(());
    };
    if fence.scope.bucket() != bucket {
        return Err(PurgeFenceError::Invalid);
    }
    if fence.scope.matches_key(bucket, key) {
        Err(PurgeFenceError::Suspended)
    } else {
        Ok(())
    }
}

pub async fn acquire_purge_fence(
    storage: &StorageHandle,
    job_id: JobId,
    scope: &StoragePurgeScope,
) -> Result<(), PurgeFenceError> {
    for attempt in 0..FENCE_MUTATE_ATTEMPTS {
        let txn_id = start_transaction(storage).await?;
        let existing = match read_fence(storage, scope.bucket(), Some(txn_id)).await {
            Ok(existing) => existing,
            Err(error) => {
                abort_transaction(storage, txn_id).await;
                return Err(error);
            }
        };
        if let Some(existing) = existing {
            abort_transaction(storage, txn_id).await;
            return if existing.job_id == job_id && existing.scope == *scope {
                Ok(())
            } else if existing.job_id == job_id {
                Err(PurgeFenceError::Invalid)
            } else {
                Err(PurgeFenceError::Busy)
            };
        }

        let value = StoragePurgeFence {
            job_id,
            scope: scope.clone(),
        }
        .to_bytes()
        .map(ByteView::from)
        .map_err(|_| PurgeFenceError::Invalid)?;
        let write = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: PURGE_FENCE_KEYSPACE.to_string(),
                key: fence_key(scope.bucket()),
                value,
                txn_id: Some(txn_id),
            })
            .await;
        if let Event::Storage(StorageEvent::Error { error }) = write {
            abort_transaction(storage, txn_id).await;
            return Err(error.into());
        }
        if !matches!(write, Event::Storage(StorageEvent::WriteResult { .. })) {
            abort_transaction(storage, txn_id).await;
            return Err(PurgeFenceError::Unexpected(format!("{write:?}")));
        }

        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => return Ok(()),
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) if attempt + 1 < FENCE_MUTATE_ATTEMPTS => continue,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            event => return Err(PurgeFenceError::Unexpected(format!("{event:?}"))),
        }
    }
    Err(StorageError::TransactionConflict.into())
}

/// Read the bucket fence in a job-terminal transaction and return its delete only
/// when the terminal job still owns it. A cancelled waiter must never clear the
/// fence held by the purge that beat it to acquisition.
pub async fn delete_owned_terminal(
    storage: &StorageHandle,
    txn_id: TxnId,
    job_id: JobId,
    scope: &StoragePurgeScope,
) -> Result<Option<(String, Key)>, PurgeFenceError> {
    match read_fence(storage, scope.bucket(), Some(txn_id)).await? {
        Some(fence) if fence.job_id == job_id && fence.scope == *scope => Ok(Some((
            PURGE_FENCE_KEYSPACE.to_string(),
            fence_key(scope.bucket()),
        ))),
        Some(fence) if fence.job_id == job_id => Err(PurgeFenceError::Invalid),
        Some(_) | None => Ok(None),
    }
}

async fn read_fence(
    storage: &StorageHandle,
    bucket: &str,
    txn_id: Option<TxnId>,
) -> Result<Option<StoragePurgeFence>, PurgeFenceError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: PURGE_FENCE_KEYSPACE.to_string(),
            key: fence_key(bucket),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| {
                StoragePurgeFence::from_bytes(value.as_ref()).map_err(|_| PurgeFenceError::Invalid)
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(PurgeFenceError::Unexpected(format!("{event:?}"))),
    }
}

async fn start_transaction(storage: &StorageHandle) -> Result<TxnId, PurgeFenceError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(PurgeFenceError::Unexpected(format!("{event:?}"))),
    }
}

async fn abort_transaction(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::guard::write_authority;
    use crate::distribution::test_record::{binding, record};
    use aruna_core::structs::storage::distribution::RouteError;
    use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
    use ulid::Ulid;

    fn answer(record: Vec<u8>, fence: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (fence_key("bucket"), None),
                (b"record".to_vec().into(), Some(record.into())),
                (b"fence".to_vec().into(), fence.map(Into::into)),
            ],
        })
    }

    #[test]
    fn fence_set_later_refuses_publication() {
        // A write routed before its range was fenced fails in its publication transaction.
        let (snapshot, bytes) = record(true);
        let own = binding(&snapshot, 1, 5);
        let authority = write_authority(&own, "a", Some(&bytes), None).unwrap();
        let Effect::Storage(StorageEffect::BatchRead { reads, txn_id }) =
            publication_read("bucket", Some(&authority), Some(Ulid::from_bytes([1; 16])))
        else {
            panic!("a distributed write reads the purge fence, record and fence row together");
        };
        assert_eq!((reads.len(), txn_id.is_some()), (3, true));
        let accepted =
            check_publication(answer(bytes.clone(), None), "bucket", "a", Some(&authority));
        assert_eq!(accepted, Ok(()));
        let mut fence = LocalFence::default();
        fence.fence_range(RangeFence {
            range_id: authority.range_id,
            transfer_id: Ulid::from_bytes([2; 16]),
            epoch: authority.epoch.0,
            since_ms: 0,
        });
        let fenced = answer(bytes, Some(fence.to_bytes().unwrap()));
        assert_eq!(
            check_publication(fenced, "bucket", "a", Some(&authority)),
            Err(PurgeFenceError::Distribution(GuardError::Route(
                RouteError::Moving(authority.range_id)
            )))
        );
    }

    #[test]
    fn ordinary_write_reads_purge_fence_only() {
        assert!(matches!(
            publication_read("bucket", None, None),
            Effect::Storage(StorageEffect::Read { .. })
        ));
    }
}
