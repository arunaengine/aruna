//! Reads this node's distribution records to route reads, writes and uploads of a bound bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, ReadRoute, RouteError, WriteRoute,
};
use ulid::Ulid;

use crate::distribution::guard::{
    GuardError, authority_reads, authority_rows, read_target, target_for,
};
use crate::driver::DriverContext;

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

/// Where a write to `key` of a bound bucket goes, from this node's record; transports use it
/// to forward a write before its bytes move.
pub async fn route_write(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<WriteRoute, GuardError> {
    route_for(context, binding, key, false).await
}

/// Where a request on an open upload of `key` goes; a range fence in its upload grace period
/// keeps it on this node, so the upload can still complete.
pub async fn route_upload(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<WriteRoute, GuardError> {
    route_for(context, binding, key, true).await
}

async fn route_for(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    completion: bool,
) -> Result<WriteRoute, GuardError> {
    let read = StorageEffect::BatchRead {
        reads: authority_reads(binding.set_id),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(read).await;
    let (record, fence) = authority_rows(event)?;
    target_for(
        binding,
        key,
        (record.as_deref(), fence.as_deref()),
        completion,
    )
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
