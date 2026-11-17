//! Allocates placement handles from bootstrap-assigned ranges on the local node.
//! The cursor is stored before returning, so a crash may skip a handle but never reissue one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::handle_allocation::HandleAllocationCursor;
use aruna_core::structs::placement::record::HandleRange;
use aruna_core::structured_id::PlacementHandle;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleAllocationState {
    Init,
    ReadCursor,
    WriteCursor,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub(crate) struct AllocateHandleOperation {
    realm_id: RealmId,
    node_id: NodeId,
    ranges: Vec<HandleRange>,
    allocated_at_ms: u64,
    state: HandleAllocationState,
    output: Option<Result<AllocatedHandle, HandleAllocationError>>,
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
            state: HandleAllocationState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: HandleAllocationError) -> Effects {
        self.state = HandleAllocationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(HandleAllocationError::UnexpectedStorageEvent(
            event.label().to_string(),
        ))
    }
}

impl EffectOperation for AllocateHandleOperation {
    type Output = AllocatedHandle;
    type Error = HandleAllocationError;

    fn start(&mut self) -> Effects {
        self.state = HandleAllocationState::ReadCursor;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: allocation_cursor_key(&self.realm_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            HandleAllocationState::ReadCursor => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let mut cursor = match value {
                        Some(bytes) => match postcard::from_bytes(&bytes) {
                            Ok(cursor) => cursor,
                            Err(error) => return self.fail(ConversionError::from(error).into()),
                        },
                        None => HandleAllocationCursor::new(),
                    };
                    let Some((handle, allocator_range_id)) = cursor.allocate(&self.ranges) else {
                        return self.fail(HandleAllocationError::PlacementHandleExhausted {
                            node: self.node_id,
                        });
                    };
                    let value = match postcard::to_allocvec(&cursor) {
                        Ok(value) => value,
                        Err(error) => return self.fail(ConversionError::from(error).into()),
                    };
                    self.output = Some(Ok(AllocatedHandle {
                        handle,
                        allocator_range_id,
                        allocated_by: self.node_id,
                        allocated_at_ms: self.allocated_at_ms,
                    }));
                    self.state = HandleAllocationState::WriteCursor;
                    smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: NODE_STATE_KEYSPACE.to_string(),
                        key: allocation_cursor_key(&self.realm_id),
                        value: value.into(),
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected(other),
            },
            HandleAllocationState::WriteCursor => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.state = HandleAllocationState::Finish;
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected(other),
            },
            HandleAllocationState::Init => self.unexpected(event),
            HandleAllocationState::Finish | HandleAllocationState::Error => self.unexpected(event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            HandleAllocationState::Finish | HandleAllocationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or_else(|| {
            Err(HandleAllocationError::UnexpectedStorageEvent(
                "handle allocation finalized before completion".to_string(),
            ))
        })
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
