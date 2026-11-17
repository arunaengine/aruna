//! Allocates placement handles from bootstrap-assigned ranges on the local node.
//! The cursor is stored before returning, so a crash may skip a handle but never reissue one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{done, read_row, values};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::handle_allocation::HandleAllocationCursor;
use aruna_core::structs::placement::record::HandleRange;
use aruna_core::structured_id::PlacementHandle;
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

use crate::document_repository::GetConfigError;
use crate::realm::mutate_placement::MutatePlacementError;

/// Node-local key for the durable allocation cursor. Kept in the non-replicated
/// node-state keyspace, one record per realm, distinct from the singleton
/// `node_state` record.
fn allocation_cursor_key(realm_id: &RealmId) -> Key {
    let mut bytes = b"handle_allocation_cursor:".to_vec();
    bytes.extend_from_slice(realm_id.as_bytes());
    Key::from(bytes)
}

/// A handle drawn from a granted range, with the provenance a binding must carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatedHandle {
    pub handle: PlacementHandle,
    pub allocator_range_id: Ulid,
    pub allocated_by: NodeId,
    pub allocated_at_ms: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum HandleAllocationError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected storage event: {0}")]
    UnexpectedStorageEvent(String),
    #[error("placement_handle_exhausted: node {node} has spent every handle in its granted ranges")]
    PlacementHandleExhausted { node: NodeId },
    #[error(transparent)]
    Append(#[from] MutatePlacementError),
    #[error(transparent)]
    ReadConfig(#[from] GetConfigError),
    #[error("placement strategy {0} does not exist")]
    StrategyNotFound(Ulid),
}

impl From<IoError> for HandleAllocationError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            other => Self::UnexpectedStorageEvent(other.to_string()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct AllocateHandleOperation {
    realm_id: RealmId,
    node_id: NodeId,
    ranges: Vec<HandleRange>,
    allocated_at_ms: u64,
    /// The handle drawn, held until its cursor is stored.
    allocated: Option<AllocatedHandle>,
}

impl AllocateHandleOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        node_id: NodeId,
        ranges: Vec<HandleRange>,
        allocated_at_ms: u64,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            ranges,
            allocated_at_ms,
            allocated: None,
        }
    }

    /// Draws the next handle from the stored cursor and writes the advanced cursor.
    fn allocate(
        &mut self,
        result: IoResult,
    ) -> Result<Next<AllocatedHandle>, HandleAllocationError> {
        let [value] = values::<1>(result)?;
        let mut cursor = match value {
            Some(bytes) => postcard::from_bytes(&bytes).map_err(ConversionError::from)?,
            None => HandleAllocationCursor::new(),
        };
        let Some((handle, allocator_range_id)) = cursor.allocate(&self.ranges) else {
            let node = self.node_id;
            return Err(HandleAllocationError::PlacementHandleExhausted { node });
        };
        let value = postcard::to_allocvec(&cursor).map_err(ConversionError::from)?;
        self.allocated = Some(AllocatedHandle {
            handle,
            allocator_range_id,
            allocated_by: self.node_id,
            allocated_at_ms: self.allocated_at_ms,
        });
        let key = allocation_cursor_key(&self.realm_id);
        let row = (NODE_STATE_KEYSPACE.to_string(), key, value.into());
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for AllocateHandleOperation {
    type Output = AllocatedHandle;
    type Error = HandleAllocationError;

    fn first(&mut self) -> Result<Next<AllocatedHandle>, HandleAllocationError> {
        let key = allocation_cursor_key(&self.realm_id);
        Ok(Next::Call(read_row(NODE_STATE_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AllocatedHandle>, HandleAllocationError> {
        let Some(allocated) = self.allocated else {
            return self.allocate(result);
        };
        done(result)?;
        Ok(Next::Done(allocated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(end: u32) -> AllocateHandleOperation {
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let range = HandleRange {
            range_id: Ulid::from_bytes([4; 16]),
            owner: node_id,
            start: 3,
            end,
        };
        AllocateHandleOperation::new(RealmId::from_bytes([1; 32]), node_id, vec![range], 9)
    }

    #[test]
    fn stores_cursor_first() {
        // The handle is returned only after the advanced cursor is written.
        let mut allocate = operation(6);
        allocate.first().unwrap();
        let write = allocate.next(IoResult::Values(vec![None]));
        assert!(matches!(write, Ok(Next::Call(IoCall::Write(_)))));
        let Ok(Next::Done(allocated)) = allocate.next(IoResult::Done) else {
            panic!("expected the handle");
        };
        assert_eq!(allocated.handle, PlacementHandle::new(4).unwrap());
        let mut empty = operation(4);
        empty.first().unwrap();
        assert!(matches!(
            empty.next(IoResult::Values(vec![None])),
            Err(HandleAllocationError::PlacementHandleExhausted { .. })
        ));
    }
}
