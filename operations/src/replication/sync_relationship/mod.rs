//! Creates, reads, lists and deletes sync relationships in both direction keyspaces.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod create;
mod delete;
mod get;
mod list;
mod remove;
mod store;
#[cfg(test)]
mod tests;

pub use create::CreateRelationshipOperation;
pub use delete::DeleteRelationshipOperation;
pub use get::GetRelationshipOperation;
pub use list::ListRelationshipsOperation;
pub use remove::RemoveOutgoingOperation;
pub use store::StoreRelationshipOperation;

use aruna_core::calls::{IoCall, IoError};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::rows::PrefixPages;
use aruna_core::structs::{SyncRelationship, sync_relationship_key, sync_relationship_prefix};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;

const RELATIONSHIP_PAGE_SIZE: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRelationshipDirection {
    Outgoing,
    Incoming,
}

impl SyncRelationshipDirection {
    fn keyspace(self) -> &'static str {
        match self {
            Self::Outgoing => RELATIONSHIP_OUT_KEYSPACE,
            Self::Incoming => RELATIONSHIP_IN_KEYSPACE,
        }
    }

    fn bucket(self, relationship: &SyncRelationship) -> Option<&str> {
        match self {
            Self::Outgoing => relationship.source.bucket(),
            Self::Incoming => relationship.target.bucket(),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum SyncRelationshipError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("sync relationship not found")]
    NotFound,
    #[error("sync relationship already exists")]
    Duplicate,
    #[error("sync relationship creation was rolled back")]
    RolledBack,
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for SyncRelationshipError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

fn same_create_identity(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.source == right.source && left.target == right.target && left.mode == right.mode
}

fn storage_key(
    direction: SyncRelationshipDirection,
    relationship: &SyncRelationship,
) -> Result<Key, ConversionError> {
    let bucket = direction.bucket(relationship).ok_or_else(|| {
        ConversionError::FromStrError("sync relationship endpoint is not an S3 ARN".to_string())
    })?;
    Ok(ByteView::from(sync_relationship_key(
        bucket,
        relationship.id,
    )))
}

/// The pages of relationships of `direction`, of one bucket or of all.
fn pages(direction: SyncRelationshipDirection, bucket: Option<&str>) -> PrefixPages {
    let prefix = ByteView::from(bucket.map(sync_relationship_prefix).unwrap_or_default());
    PrefixPages::new(direction.keyspace(), prefix, RELATIONSHIP_PAGE_SIZE)
}

/// Writes `relationship` in the keyspace of `direction`.
fn write_call(
    direction: SyncRelationshipDirection,
    relationship: &SyncRelationship,
) -> Result<IoCall, ConversionError> {
    let row = (
        direction.keyspace().to_string(),
        storage_key(direction, relationship)?,
        ByteView::from(relationship.to_bytes()?),
    );
    Ok(IoCall::Write(vec![row]))
}

/// Deletes `relationship` from the keyspace of `direction`.
fn delete_call(
    direction: SyncRelationshipDirection,
    relationship: &SyncRelationship,
) -> Result<IoCall, ConversionError> {
    relationship.validate()?;
    let row = (
        direction.keyspace().to_string(),
        storage_key(direction, relationship)?,
    );
    Ok(IoCall::Delete(vec![row]))
}

fn parse_values(
    direction: SyncRelationshipDirection,
    values: Vec<(Key, Value)>,
) -> Result<Vec<SyncRelationship>, ConversionError> {
    values
        .into_iter()
        .map(|(key, value)| {
            let relationship = SyncRelationship::from_bytes(value.as_ref())?;
            if storage_key(direction, &relationship)? != key {
                return Err(ConversionError::FromStrError(
                    "sync relationship key does not match payload".to_string(),
                ));
            }
            Ok(relationship)
        })
        .collect()
}
