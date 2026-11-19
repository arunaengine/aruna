//! Reads the current metadata of a local bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE, DISTRIBUTION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, StoredMetadata};
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

/// Why the metadata of a bucket could not be read.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("bucket metadata read failed: {0}")]
pub struct BucketMetadataError(String);

impl From<IoError> for BucketMetadataError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl From<ConversionError> for BucketMetadataError {
    fn from(error: ConversionError) -> Self {
        Self(error.to_string())
    }
}

/// The current metadata of a local bucket: its distributed set's settings, or the row of this
/// bucket's current incarnation. `None` when there is none.
#[derive(Debug, PartialEq)]
pub struct BucketMetadataOperation {
    bucket: String,
    set_id: Option<Ulid>,
}

impl BucketMetadataOperation {
    pub fn new(bucket: &str, info: &BucketInfo) -> Self {
        Self {
            bucket: bucket.to_string(),
            set_id: info.distribution.as_ref().map(|binding| binding.set_id),
        }
    }
}

impl Operation for BucketMetadataOperation {
    type Output = Option<BucketMetadata>;
    type Error = BucketMetadataError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let reads = match self.set_id {
            Some(set_id) => {
                let key = Key::from(set_id.to_bytes().to_vec());
                vec![(DISTRIBUTION_KEYSPACE.to_string(), key)]
            }
            None => {
                let key = Key::from(self.bucket.as_bytes().to_vec());
                vec![
                    (BUCKET_METADATA_KEYSPACE.to_string(), key.clone()),
                    (BUCKET_CREATION_KEYSPACE.to_string(), key),
                ]
            }
        };
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let metadata = if self.set_id.is_some() {
            let [row] = values::<1>(result)?;
            let view = row
                .map(|row| DistributionView::from_bytes(&row))
                .transpose()?;
            view.and_then(|view| view.snapshot().config.settings.metadata.clone())
                .map(|metadata| *metadata)
        } else {
            let [row, creation] = values::<2>(result)?;
            current_row(creation.as_ref(), row.as_ref())?
        };
        Ok(Next::Done(metadata))
    }
}

/// The metadata a stored row gives the bucket's current incarnation.
pub(super) fn current_row(
    creation: Option<&Value>,
    row: Option<&Value>,
) -> Result<Option<BucketMetadata>, ConversionError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let stored = StoredMetadata::from_bytes(row.as_ref())?;
    let current = incarnation(creation.map(|value| value.as_ref()));
    Ok((stored.incarnation == current).then_some(stored.metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(incarnation: u128) -> Value {
        let metadata = BucketMetadata::default();
        let row = StoredMetadata {
            incarnation,
            metadata,
        };
        Value::from(row.to_bytes().unwrap())
    }

    fn local() -> BucketMetadataOperation {
        BucketMetadataOperation {
            bucket: "db".to_string(),
            set_id: None,
        }
    }

    // A local bucket reads its row and its creation marker in one call.
    #[test]
    fn local_reads_both() {
        let Ok(Next::Call(IoCall::Read(reads))) = local().first() else {
            panic!("expected one read");
        };
        let spaces: Vec<_> = reads.iter().map(|(space, _)| space.as_str()).collect();
        assert_eq!(spaces, [BUCKET_METADATA_KEYSPACE, BUCKET_CREATION_KEYSPACE]);
    }

    // A row of an earlier incarnation of the bucket name gives no metadata.
    #[test]
    fn stale_row_hidden() {
        let creation = Value::from(7u128.to_be_bytes().to_vec());
        let read = |row: u128| {
            let rows = vec![Some(stored(row)), Some(creation.clone())];
            local().next(IoResult::Values(rows))
        };
        assert_eq!(read(6), Ok(Next::Done(None)));
        assert_eq!(read(7), Ok(Next::Done(Some(BucketMetadata::default()))));
    }

    // A distributed bucket reads its set's record, and a missing record gives no metadata.
    #[test]
    fn set_reads_record() {
        let set_id = Ulid::from_parts(2, 2);
        let mut operation = BucketMetadataOperation {
            bucket: "db".to_string(),
            set_id: Some(set_id),
        };
        let row = (
            DISTRIBUTION_KEYSPACE.to_string(),
            Key::from(set_id.to_bytes().to_vec()),
        );
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }
}
