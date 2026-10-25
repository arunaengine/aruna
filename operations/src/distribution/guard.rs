//! Guards local writes to a distributed bucket: only the coordinator of the key's range writes,
//! and its versions carry the range's epoch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every other member forwards or refuses the write with the coordinator named; nothing waits.

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, OriginalEpoch, ReadRoute, RouteError, WriteRoute,
};
use aruna_core::types::{Key, TxnId, Value};
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

impl GuardError {
    /// Whether the key is not this member's to write anymore: another coordinator holds or
    /// takes over its range, or the member or the set goes away.
    pub fn moved_away(&self) -> bool {
        matches!(
            self,
            Self::NotCoordinator { .. }
                | Self::Route(
                    RouteError::Moving(_) | RouteError::Deleting(_) | RouteError::NotMember(_)
                )
        )
    }
}

/// The authority one write was routed under: the member, the key's range and its epoch.
/// Its publication transaction checks it again against the rows [`authority_reads`] names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAuthority {
    pub binding: DistributionBinding,
    pub range_id: Ulid,
    pub epoch: OriginalEpoch,
    /// The write completes an open upload, which a fence in its grace period still admits.
    pub completion: bool,
}

/// The set's record and its local fence row, in this order.
pub fn authority_reads(set_id: Ulid) -> Vec<(String, Key)> {
    let key: Key = set_id.to_bytes().to_vec().into();
    vec![
        (DISTRIBUTION_KEYSPACE.to_string(), key.clone()),
        (DISTRIBUTION_FENCE_KEYSPACE.to_string(), key),
    ]
}

/// Reads the set's record and fence row; a write reads them before its bytes move, and with
/// `txn_id` its publication transaction reads them again.
pub fn record_read(binding: &DistributionBinding) -> Effect {
    authority_read(binding.set_id, None)
}

pub fn authority_read(set_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::BatchRead {
        reads: authority_reads(set_id),
        txn_id,
    })
}

/// The record and fence values from the answer to [`authority_read`].
pub fn authority_rows(event: Event) -> Result<(Option<Value>, Option<Value>), GuardError> {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == 2 => {
            let mut values = values.into_iter().map(|(_, value)| value);
            Ok((values.next().flatten(), values.next().flatten()))
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(GuardError::Unreadable(error.to_string()))
        }
        other => Err(GuardError::Unreadable(format!(
            "unexpected answer {other:?}"
        ))),
    }
}

/// The record of the bound set, while this bucket is still its admitted member.
fn bound_record(
    binding: &DistributionBinding,
    record: Option<&[u8]>,
) -> Result<DistributionView, GuardError> {
    let set_id = binding.set_id;
    let view = record
        .map(DistributionView::from_bytes)
        .transpose()?
        .ok_or(GuardError::RecordMissing(set_id))?;
    let member = view
        .snapshot()
        .member(binding.member_id)
        .ok_or(RouteError::NotMember(set_id))?;
    // A replaced local bucket of the name is never taken for the admitted member.
    if member
        .incarnation
        .is_some_and(|admitted| admitted != binding.incarnation)
    {
        return Err(GuardError::Unbound(set_id));
    }
    Ok(view)
}

/// Where a write to `key` goes, from the record and fence bytes this node holds. A range this
/// node fenced refuses the write even while its record still names it as the coordinator.
pub fn write_target(
    binding: &DistributionBinding,
    key: &str,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<WriteRoute, GuardError> {
    target_for(binding, key, (record, fence), false)
}

/// [`write_target`]; a `completion` of an open upload passes a fence in its grace period.
fn target_for(
    binding: &DistributionBinding,
    key: &str,
    (record, fence): (Option<&[u8]>, Option<&[u8]>),
    completion: bool,
) -> Result<WriteRoute, GuardError> {
    let view = bound_record(binding, record)?;
    let snapshot = view.snapshot();
    let route = snapshot.write_route(binding.member_id, key)?;
    if let WriteRoute::Local { epoch, .. } = &route {
        let range = (snapshot.range_for(key)).ok_or(RouteError::NoRange(binding.set_id))?;
        let fence = LocalFence::decode(fence)?;
        match completion {
            true => fence.check_completion(snapshot, range, epoch.0)?,
            false => fence.check_write(snapshot, range, epoch.0)?,
        }
    }
    Ok(route)
}

/// The authority a write to `key` takes here, or why it may not write here.
pub fn write_authority(
    binding: &DistributionBinding,
    key: &str,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<WriteAuthority, GuardError> {
    authority_for(binding, key, (record, fence), false)
}

fn authority_for(
    binding: &DistributionBinding,
    key: &str,
    rows: (Option<&[u8]>, Option<&[u8]>),
    completion: bool,
) -> Result<WriteAuthority, GuardError> {
    match target_for(binding, key, rows, completion)? {
        WriteRoute::Local { range_id, epoch } => Ok(WriteAuthority {
            binding: binding.clone(),
            range_id,
            epoch,
            completion,
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

/// Checks inside a publication transaction that the write still holds `authority`: same
/// incarnation, the key's range descends from the routed one at the same epoch, and no fence
/// set since covers it. A fence or record written after this read conflicts the commit.
pub fn check_authority(
    authority: &WriteAuthority,
    key: &str,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<(), GuardError> {
    let binding = &authority.binding;
    let view = bound_record(binding, record)?;
    let snapshot = view.snapshot();
    let current = authority_for(binding, key, (record, fence), authority.completion)?;
    let range = (snapshot.range_for(key)).ok_or(RouteError::NoRange(binding.set_id))?;
    if current.epoch != authority.epoch || !snapshot.descends(range, authority.range_id) {
        return Err(RouteError::StaleRange(authority.range_id).into());
    }
    Ok(())
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
    fence: Option<&[u8]>,
) -> Result<OriginalEpoch, GuardError> {
    write_authority(binding, key, record, fence).map(|authority| authority.epoch)
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
    Ok(view
        .snapshot()
        .lists_interval(binding.member_id, lower, upper))
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

/// Reads the write authority from the answer to [`record_read`].
pub fn record_authority(
    binding: &DistributionBinding,
    key: &str,
    event: Event,
) -> Result<WriteAuthority, GuardError> {
    let (record, fence) = authority_rows(event)?;
    write_authority(binding, key, record.as_deref(), fence.as_deref())
}

/// Reads the authority of an upload completion from the answer to [`record_read`].
pub fn record_completion(
    binding: &DistributionBinding,
    key: &str,
    event: Event,
) -> Result<WriteAuthority, GuardError> {
    let (record, fence) = authority_rows(event)?;
    authority_for(binding, key, (record.as_deref(), fence.as_deref()), true)
}

/// Reads the epoch from the answer to [`record_read`].
pub fn record_epoch(
    binding: &DistributionBinding,
    key: &str,
    event: Event,
) -> Result<OriginalEpoch, GuardError> {
    record_authority(binding, key, event).map(|authority| authority.epoch)
}

/// Checks `authority` from the answer to [`authority_read`] inside a publication transaction.
pub fn authority_answer(
    authority: &WriteAuthority,
    key: &str,
    event: Event,
) -> Result<(), GuardError> {
    let (record, fence) = authority_rows(event)?;
    check_authority(authority, key, record.as_deref(), fence.as_deref())
}

#[cfg(test)]
#[path = "guard_tests.rs"]
mod tests;
