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
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, OriginalEpoch, ReadRoute, RouteError, WriteRoute,
};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;

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

/// Where a read of `key`, or with `listing` of every key under it, is answered.
pub fn read_target(
    binding: &DistributionBinding,
    key: &str,
    listing: bool,
    record: Option<&[u8]>,
) -> Result<ReadRoute, GuardError> {
    let set_id = binding.set_id;
    let view = record
        .map(DistributionView::from_bytes)
        .transpose()?
        .ok_or(GuardError::RecordMissing(set_id))?;
    let snapshot = view.snapshot();
    let member = snapshot
        .member(binding.member_id)
        .ok_or(RouteError::NotMember(set_id))?;
    if member
        .incarnation
        .is_some_and(|admitted| admitted != binding.incarnation)
    {
        return Err(GuardError::Unbound(set_id));
    }
    Ok(snapshot.read_route(binding.member_id, key, listing)?)
}

/// Where a read of a bound bucket is answered, from this node's record.
pub async fn route_read(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    listing: bool,
) -> Result<ReadRoute, GuardError> {
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: binding.set_id.to_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            read_target(binding, key, listing, value.as_deref())
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(GuardError::Unreadable(error.to_string()))
        }
        other => Err(GuardError::Unreadable(format!(
            "unexpected answer {other:?}"
        ))),
    }
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

/// Where a write to `key` of a bound bucket goes, from this node's record; transports use it
/// to forward a write before its bytes move.
pub async fn route_write(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<WriteRoute, GuardError> {
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: binding.set_id.to_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            write_target(binding, key, value.as_deref())
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(GuardError::Unreadable(error.to_string()))
        }
        other => Err(GuardError::Unreadable(format!(
            "unexpected answer {other:?}"
        ))),
    }
}

/// This node's record of the bound set, when the bucket is still the admitted member.
pub async fn bound_view(
    context: &DriverContext,
    binding: &DistributionBinding,
) -> Result<DistributionView, GuardError> {
    let set_id = binding.set_id;
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        txn_id: None,
    };
    let view = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| DistributionView::from_bytes(&value))
            .transpose()?
            .ok_or(GuardError::RecordMissing(set_id))?,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(GuardError::Unreadable(error.to_string()));
        }
        other => {
            return Err(GuardError::Unreadable(format!(
                "unexpected answer {other:?}"
            )));
        }
    };
    let member = view
        .snapshot()
        .member(binding.member_id)
        .ok_or(RouteError::NotMember(set_id))?;
    if member
        .incarnation
        .is_some_and(|admitted| admitted != binding.incarnation)
    {
        return Err(GuardError::Unbound(set_id));
    }
    Ok(view)
}

/// Names a new upload of `key` by the range and epoch this member coordinates it under.
pub async fn upload_tag(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    upload_id: Ulid,
) -> Result<UploadTag, GuardError> {
    match route_write(context, binding, key).await? {
        WriteRoute::Local { range_id, epoch } => Ok(UploadTag {
            upload_id,
            set_id: binding.set_id,
            range_id,
            epoch: epoch.0,
        }),
        WriteRoute::Forward {
            range_id, node_id, ..
        } => Err(GuardError::NotCoordinator {
            set_id: binding.set_id,
            range_id,
            node_id,
        }),
    }
}

/// Whether this member still holds the authority the upload `tag` was opened under: the key's
/// range descends from the named one, at the same epoch, coordinated here.
pub async fn upload_route(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    tag: &UploadTag,
) -> Result<(), GuardError> {
    let view = bound_view(context, binding).await?;
    Ok(view
        .snapshot()
        .upload_authority(binding.member_id, key, tag)?)
}

/// The tags of listed uploads, from the range and epoch that hold each key now.
pub async fn upload_tags(
    context: &DriverContext,
    binding: &DistributionBinding,
    uploads: &[(String, Ulid)],
) -> Result<Vec<UploadTag>, GuardError> {
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    uploads
        .iter()
        .map(|(key, upload_id)| {
            let range = snapshot
                .range_for(key)
                .ok_or(RouteError::NoRange(binding.set_id))?;
            Ok(UploadTag {
                upload_id: *upload_id,
                set_id: binding.set_id,
                range_id: range.range_id,
                epoch: range.epoch,
            })
        })
        .collect()
}

/// Resolves the range a forwarded request names to the key's range, which this member must
/// coordinate; an ancestor id from before a split resolves to its descendant.
pub async fn delegated_range(
    context: &DriverContext,
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> Result<(), GuardError> {
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    let range = snapshot.resolve(range_id, key)?;
    if range.coordinator == binding.member_id {
        return Ok(());
    }
    let node_id = snapshot
        .member(range.coordinator)
        .map(|member| member.node_id)
        .ok_or(RouteError::NoRange(binding.set_id))?;
    Err(GuardError::NotCoordinator {
        set_id: binding.set_id,
        range_id: range.range_id,
        node_id,
    })
}

/// The nodes that answer reads of `key`'s range from their own records, coordinator last.
pub async fn record_holders(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<Vec<NodeId>, GuardError> {
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    let range = snapshot
        .range_for(key)
        .ok_or(RouteError::NoRange(binding.set_id))?;
    Ok((snapshot.record_holders(range).into_iter())
        .map(|member| member.node_id)
        .collect())
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
