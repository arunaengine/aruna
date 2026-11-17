//! Creates, reads, lists and deletes sync relationships in both direction keyspaces.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod delete;
mod get;
mod list;
mod store;
#[cfg(test)]
mod tests;

pub use delete::DeleteRelationshipOperation;
pub use get::GetRelationshipOperation;
pub use list::ListRelationshipsOperation;
pub use store::StoreRelationshipOperation;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    SyncRelationship, SyncState, sync_relationship_key, sync_relationship_prefix,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::sync::mirror_repair::claim_activation;

const RELATIONSHIP_PAGE_SIZE: usize = 128;
const CREATE_RELATIONSHIP_ATTEMPTS: usize = 3;

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

/// Activates a new relationship: claims its staged repair record and stores the outgoing
/// record unless an enabled relationship with the same endpoints and mode exists.
pub async fn create_sync_relationship(
    context: &crate::driver::DriverContext,
    relationship: SyncRelationship,
) -> Result<SyncRelationship, SyncRelationshipError> {
    let bucket = relationship.source.bucket().ok_or_else(|| {
        ConversionError::FromStrError("sync relationship endpoint is not an S3 ARN".to_string())
    })?;
    let prefix = ByteView::from(sync_relationship_prefix(bucket));
    store_outgoing(context, relationship, Some(prefix)).await
}

/// Activates a relationship that replaces older ones of the same endpoints, so it skips the
/// duplicate check. It fails with `RolledBack` once repair claimed the create for a rollback.
pub async fn activate_relationship(
    context: &crate::driver::DriverContext,
    relationship: SyncRelationship,
) -> Result<SyncRelationship, SyncRelationshipError> {
    store_outgoing(context, relationship, None).await
}

async fn store_outgoing(
    context: &crate::driver::DriverContext,
    relationship: SyncRelationship,
    prefix: Option<Key>,
) -> Result<SyncRelationship, SyncRelationshipError> {
    let storage = &context.storage_handle;
    let key = storage_key(SyncRelationshipDirection::Outgoing, &relationship)?;
    let value = ByteView::from(relationship.to_bytes()?);

    for attempt in 0..CREATE_RELATIONSHIP_ATTEMPTS {
        let txn_id = match start_create_txn(storage).await {
            Err(SyncRelationshipError::Storage(StorageError::TransactionConflict))
                if attempt + 1 < CREATE_RELATIONSHIP_ATTEMPTS =>
            {
                continue;
            }
            result => result?,
        };
        if let Err(error) = create_relationship_once(
            storage,
            &relationship,
            prefix.as_ref(),
            &key,
            &value,
            txn_id,
        )
        .await
        {
            abort_create_txn(storage, txn_id).await;
            return Err(error);
        }

        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => return Ok(relationship),
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) if attempt + 1 < CREATE_RELATIONSHIP_ATTEMPTS => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            received => {
                abort_create_txn(storage, txn_id).await;
                return Err(SyncRelationshipError::UnexpectedEvent {
                    state: "CommittingCreate",
                    expected: "storage transaction committed",
                    received,
                });
            }
        }
    }

    Err(StorageError::TransactionConflict.into())
}

/// Removes an outgoing relationship on behalf of a delete flow. Relationships
/// serving references are detached, not deleted: reads are authorized through the
/// outgoing record, so a stub must survive for retained data to stay readable.
pub async fn remove_outgoing_relationship(
    context: &crate::driver::DriverContext,
    relationship: SyncRelationship,
) -> Result<(), SyncRelationshipError> {
    if relationship.serves_references() {
        let stub = SyncRelationship {
            state: SyncState::Detached,
            ..relationship
        };
        crate::driver::drive(
            StoreRelationshipOperation::new(stub, SyncRelationshipDirection::Outgoing),
            context,
        )
        .await
        .map(|_| ())
    } else {
        crate::driver::drive(
            DeleteRelationshipOperation::new(relationship, SyncRelationshipDirection::Outgoing),
            context,
        )
        .await
    }
}

async fn create_relationship_once(
    storage: &StorageHandle,
    relationship: &SyncRelationship,
    prefix: Option<&Key>,
    key: &Key,
    value: &Value,
    txn_id: TxnId,
) -> Result<(), SyncRelationshipError> {
    let mut start = None;
    while let Some(prefix) = prefix {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start.map(IterStart::After),
                limit: RELATIONSHIP_PAGE_SIZE,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                if parse_values(SyncRelationshipDirection::Outgoing, values)?
                    .iter()
                    .any(|existing| {
                        // Detached stubs keep retained references readable without blocking recreation.
                        existing.state != SyncState::Detached
                            && same_create_identity(existing, relationship)
                    })
                {
                    return Err(SyncRelationshipError::Duplicate);
                }
                let Some(next_start_after) = next_start_after else {
                    break;
                };
                start = Some(next_start_after);
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            received => {
                return Err(SyncRelationshipError::UnexpectedEvent {
                    state: "ScanningCreate",
                    expected: "storage iteration result",
                    received,
                });
            }
        }
    }
    claim_activation(storage, relationship, txn_id).await?;

    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.clone(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        received => Err(SyncRelationshipError::UnexpectedEvent {
            state: "WritingCreate",
            expected: "storage write result",
            received,
        }),
    }
}

async fn start_create_txn(storage: &StorageHandle) -> Result<TxnId, SyncRelationshipError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        received => Err(SyncRelationshipError::UnexpectedEvent {
            state: "StartingCreate",
            expected: "storage transaction started",
            received,
        }),
    }
}

async fn abort_create_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
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
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: direction.keyspace().to_string(),
        prefix: bucket.map(sync_relationship_prefix).map(ByteView::from),
        start: start_after.map(IterStart::After),
        limit: RELATIONSHIP_PAGE_SIZE,
        txn_id: None,
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
