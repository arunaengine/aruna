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

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::structs::{SyncRelationship, sync_relationship_key, sync_relationship_prefix};
use aruna_core::types::{Key, TxnId, Value};
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
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    UnexpectedEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
    #[error("sync relationship operation did not finish")]
    NotFinished,
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

fn iter_effect(
    direction: SyncRelationshipDirection,
    bucket: Option<&str>,
    start_after: Option<Key>,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: direction.keyspace().to_string(),
        prefix: bucket.map(sync_relationship_prefix).map(ByteView::from),
        start: start_after.map(IterStart::After),
        limit: RELATIONSHIP_PAGE_SIZE,
        txn_id,
    })
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
