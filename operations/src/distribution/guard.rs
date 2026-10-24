//! Guards local writes to a distributed bucket: only the coordinator of the key's range writes,
//! and its versions carry the range's epoch. Every other member forwards or refuses the write
//! with the coordinator named; nothing waits in a queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, OriginalEpoch, RouteError, WriteRoute,
};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum GuardError {
    #[error(
        "range {range_id} of distributed bucket {set_id} is coordinated by node {node_id}; \
         send the write there"
    )]
    NotCoordinator {
        set_id: Ulid,
        range_id: Ulid,
        node_id: NodeId,
    },
    #[error(transparent)]
    Route(#[from] RouteError),
    #[error("the record of distributed bucket {0} has not reached this node yet; retry shortly")]
    RecordMissing(Ulid),
    #[error("this bucket is not the admitted member of distributed bucket {0}")]
    Unbound(Ulid),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the record read failed: {0}")]
    Unreadable(String),
}

/// Reads the set's record outside any transaction; a write reads it before its bytes move.
pub fn record_read(binding: &DistributionBinding) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: binding.set_id.to_bytes().to_vec().into(),
        txn_id: None,
    })
}

/// Where a write to `key` goes, from the record bytes this node holds.
pub fn write_target(
    binding: &DistributionBinding,
    key: &str,
    record: Option<&[u8]>,
) -> Result<WriteRoute, GuardError> {
    let set_id = binding.set_id;
    let view = record
        .map(DistributionView::from_bytes)
        .transpose()?
        .ok_or(GuardError::RecordMissing(set_id))?;
    let snapshot = view.snapshot();
    let member = snapshot
        .member(binding.member_id)
        .ok_or(RouteError::NotMember(set_id))?;
    // A replaced local bucket of the name is never taken for the admitted member.
    if member
        .incarnation
        .is_some_and(|admitted| admitted != binding.incarnation)
    {
        return Err(GuardError::Unbound(set_id));
    }
    Ok(snapshot.write_route(binding.member_id, key)?)
}

/// The epoch this member's write to `key` carries, or why it may not write here.
pub fn write_epoch(
    binding: &DistributionBinding,
    key: &str,
    record: Option<&[u8]>,
) -> Result<OriginalEpoch, GuardError> {
    match write_target(binding, key, record)? {
        WriteRoute::Local { epoch, .. } => Ok(epoch),
        WriteRoute::Forward {
            range_id, node_id, ..
        } => Err(GuardError::NotCoordinator {
            set_id: binding.set_id,
            range_id,
            node_id,
        }),
    }
}

/// Reads the epoch from the answer to [`record_read`].
pub fn record_epoch(
    binding: &DistributionBinding,
    key: &str,
    event: Event,
) -> Result<OriginalEpoch, GuardError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            write_epoch(binding, key, value.as_deref())
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(GuardError::Unreadable(error.to_string()))
        }
        other => Err(GuardError::Unreadable(format!(
            "unexpected answer {other:?}"
        ))),
    }
}

#[cfg(test)]
#[path = "guard_tests.rs"]
mod tests;
