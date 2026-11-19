//! Keys, reads and checks the per bucket write fence that a permanent purge holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoError;
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::types::{Key, Value};
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

impl From<IoError> for PurgeFenceError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Unexpected(other.to_string()),
        }
    }
}

pub fn fence_key(bucket: &str) -> Key {
    ByteView::from(bucket.as_bytes().to_vec())
}

/// Checks the fence row of [`fence_key`] that a write to `key` read.
pub fn check_fence_row(
    value: Option<&[u8]>,
    bucket: &str,
    key: &str,
) -> Result<(), PurgeFenceError> {
    let fence = value
        .map(|value| StoragePurgeFence::from_bytes(value).map_err(|_| PurgeFenceError::Invalid))
        .transpose()?;
    check_decoded_fence(fence.as_ref(), bucket, key)
}

/// The purge fence of `bucket` and, for a write to a distributed bucket, the record and fence
/// rows its authority is checked against, all read in the publication transaction.
pub fn publication_rows(bucket: &str, authority: Option<&WriteAuthority>) -> Vec<(String, Key)> {
    let mut reads = vec![(PURGE_FENCE_KEYSPACE.to_string(), fence_key(bucket))];
    if let Some(authority) = authority {
        reads.extend(authority_reads(authority.binding.set_id));
    }
    reads
}

/// Checks the values of [`publication_rows`]: no purge fence covers the key and the write still
/// holds its distributed authority.
pub fn check_published(
    values: Vec<Option<Value>>,
    bucket: &str,
    key: &str,
    authority: Option<&WriteAuthority>,
) -> Result<(), PurgeFenceError> {
    let expected = 1 + 2 * usize::from(authority.is_some());
    if values.len() != expected {
        return Err(PurgeFenceError::Unexpected(
            "a publication read".to_string(),
        ));
    }
    let mut values = values.into_iter();
    let purge = values.next().flatten();
    check_fence_row(purge.as_deref(), bucket, key)?;
    let Some(authority) = authority else {
        return Ok(());
    };
    let (record, fence) = (values.next().flatten(), values.next().flatten());
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

    fn answer(record: Vec<u8>, fence: Option<Vec<u8>>) -> Vec<Option<Value>> {
        vec![None, Some(record.into()), fence.map(Into::into)]
    }

    #[test]
    fn late_fence_refuses() {
        // A write routed before its range was fenced fails in its publication transaction.
        let (snapshot, bytes) = record(true);
        let own = binding(&snapshot, 1, 5);
        let authority = write_authority(&own, "a", Some(&bytes), None).unwrap();
        // A distributed write reads the purge fence, record and fence row together.
        assert_eq!(publication_rows("bucket", Some(&authority)).len(), 3);
        let accepted =
            check_published(answer(bytes.clone(), None), "bucket", "a", Some(&authority));
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
            check_published(fenced, "bucket", "a", Some(&authority)),
            Err(PurgeFenceError::Distribution(GuardError::Route(
                RouteError::Moving(authority.range_id)
            )))
        );
    }

    #[test]
    fn ordinary_reads_purge() {
        assert_eq!(publication_rows("bucket", None).len(), 1);
    }
}
