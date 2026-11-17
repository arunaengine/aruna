//! Keys, reads and checks the per bucket write fence that a permanent purge holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::types::{Key, TxnId};
use byteview::ByteView;
use thiserror::Error;

use crate::distribution::guard::{GuardError, WriteAuthority, authority_reads, check_authority};

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
        event => return Err(PurgeFenceError::Unexpected(event.label().to_string())),
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
        event => return Err(PurgeFenceError::Unexpected(event.label().to_string())),
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
    fn late_fence_refuses() {
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
            uploads_open: false,
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
    fn ordinary_reads_purge() {
        assert!(matches!(
            publication_read("bucket", None, None),
            Effect::Storage(StorageEffect::Read { .. })
        ));
    }
}
