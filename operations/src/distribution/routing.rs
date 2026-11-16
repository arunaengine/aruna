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

/// This node's fence row of the set.
pub async fn local_fence(context: &DriverContext, set_id: Ulid) -> Result<LocalFence, GuardError> {
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
        key: LocalFence::key(set_id).into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(LocalFence::decode(value.as_deref())?)
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(GuardError::Unreadable(error.to_string()))
        }
        other => Err(GuardError::Unreadable(format!(
            "unexpected answer {other:?}"
        ))),
    }
}

/// The range id and node of the coordinator of `key`'s range, or `None` when this member
/// coordinates it and has not fenced it for a transfer.
pub async fn read_coordinator(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<Option<(Ulid, NodeId)>, GuardError> {
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    let set_id = binding.set_id;
    let range = snapshot.range_for(key).ok_or(RouteError::NoRange(set_id))?;
    if range.coordinator == binding.member_id {
        let fence = local_fence(context, set_id).await?;
        fence.check_read(snapshot, range)?;
        return Ok(None);
    }
    let coordinator = snapshot
        .member(range.coordinator)
        .ok_or(RouteError::NoRange(set_id))?;
    Ok(Some((range.range_id, coordinator.node_id)))
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

/// Resolves the range a forwarded read names to the key's range, whose records this ready
/// member must keep; an ancestor id from before a split resolves to its descendant.
pub async fn delegated_read(
    context: &DriverContext,
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> Result<(), GuardError> {
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    let range = snapshot.resolve(range_id, key)?;
    match (snapshot.record_holders(range).iter())
        .any(|member| member.member_id == binding.member_id)
    {
        true => Ok(()),
        false => Err(GuardError::Route(RouteError::StaleRange(range.range_id))),
    }
}

/// Whether this member answers listings of every key in `[lower, upper)` from its records.
pub async fn lists_interval(
    context: &DriverContext,
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> Result<bool, GuardError> {
    let view = bound_view(context, binding).await?;
    let fence = local_fence(context, binding.set_id).await?;
    Ok(view
        .snapshot()
        .lists_interval(binding.member_id, (lower, upper), &fence))
}

/// Whether this member, as a ready copy, may answer listings of every key in `[lower, upper)`
/// while their coordinators cannot.
pub async fn copies_interval(
    context: &DriverContext,
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> Result<bool, GuardError> {
    let view = bound_view(context, binding).await?;
    Ok(view
        .snapshot()
        .copies_interval(binding.member_id, (lower, upper)))
}

/// Who answers a read of `key` while its range's coordinator cannot: the range, whether this
/// member keeps the range's records, and the other nodes with a ready copy.
pub async fn copy_readers(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<(Ulid, bool, Vec<NodeId>), GuardError> {
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    let range = snapshot
        .range_for(key)
        .ok_or(RouteError::NoRange(binding.set_id))?;
    let here =
        (snapshot.record_holders(range).iter()).any(|member| member.member_id == binding.member_id);
    let copies = (snapshot.copy_holders(range).into_iter())
        .filter(|member| member.member_id != binding.member_id)
        .map(|member| member.node_id)
        .collect();
    Ok((range.range_id, here, copies))
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
