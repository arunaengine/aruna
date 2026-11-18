//! Reads this node's distribution records to route reads, writes and uploads of a bound bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, ReadRoute, RouteError, WriteRoute,
};
use aruna_core::types::{Key, KeySpace, Value};
use ulid::Ulid;

use crate::distribution::guard::{
    GuardError, authority_reads, bound_record, read_target, target_for,
};
use crate::driver::{DriverContext, drive};

/// Decides a routing question from its arguments and the rows read for it.
type Decide<A, T> = fn(&A, &[Option<Value>]) -> Result<T, GuardError>;

/// Reads the rows of one routing question, then decides it without further I/O.
struct RouteOperation<A, T> {
    reads: Vec<(KeySpace, Key)>,
    args: A,
    decide: Decide<A, T>,
}

impl<A: Debug, T> Debug for RouteOperation<A, T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteOperation")
            .field("reads", &self.reads)
            .field("args", &self.args)
            .finish_non_exhaustive()
    }
}

impl<A: PartialEq, T> PartialEq for RouteOperation<A, T> {
    fn eq(&self, other: &Self) -> bool {
        self.reads == other.reads && self.args == other.args
    }
}

impl<A, T> Operation for RouteOperation<A, T>
where
    A: Send + Debug + PartialEq,
    T: Send + Debug,
{
    type Output = T;
    type Error = GuardError;

    fn first(&mut self) -> Result<Next<T>, GuardError> {
        Ok(Next::Call(IoCall::Read(std::mem::take(&mut self.reads))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, GuardError> {
        let values = result.values()?;
        Ok(Next::Done((self.decide)(&self.args, &values)?))
    }
}

async fn ask<A, T>(
    context: &DriverContext,
    reads: Vec<(KeySpace, Key)>,
    args: A,
    decide: Decide<A, T>,
) -> Result<T, GuardError>
where
    A: Send + Debug + PartialEq + 'static,
    T: Send + Debug + 'static,
{
    let question = RouteOperation {
        reads,
        args,
        decide,
    };
    drive(question, context).await
}

fn record_read(set_id: Ulid) -> Vec<(KeySpace, Key)> {
    let key = set_id.to_bytes().to_vec().into();
    vec![(DISTRIBUTION_KEYSPACE.to_string(), key)]
}

fn row(values: &[Option<Value>], index: usize) -> Option<&[u8]> {
    values.get(index).and_then(Option::as_deref)
}

/// The record of the bound set among the rows, while the bucket is still its admitted member.
fn bound(
    binding: &DistributionBinding,
    values: &[Option<Value>],
) -> Result<DistributionView, GuardError> {
    bound_record(binding, row(values, 0))
}

/// Asks a question of the bound set's record alone.
async fn ask_record<A, T>(
    context: &DriverContext,
    binding: &DistributionBinding,
    args: A,
    decide: Decide<(DistributionBinding, A), T>,
) -> Result<T, GuardError>
where
    A: Send + Debug + PartialEq + 'static,
    T: Send + Debug + 'static,
{
    let reads = record_read(binding.set_id);
    ask(context, reads, (binding.clone(), args), decide).await
}

/// Asks a question of the bound set's record and this node's fence row.
async fn ask_authority<A, T>(
    context: &DriverContext,
    binding: &DistributionBinding,
    args: A,
    decide: Decide<(DistributionBinding, A), T>,
) -> Result<T, GuardError>
where
    A: Send + Debug + PartialEq + 'static,
    T: Send + Debug + 'static,
{
    let reads = authority_reads(binding.set_id);
    ask(context, reads, (binding.clone(), args), decide).await
}

/// Where a read of a bound bucket is answered, from this node's record.
pub async fn route_read(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    listing: bool,
) -> Result<ReadRoute, GuardError> {
    let args = (key.to_string(), listing);
    ask_record(context, binding, args, |(binding, (key, listing)), rows| {
        read_target(binding, key, *listing, row(rows, 0))
    })
    .await
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
    let args = (key.to_string(), completion);
    ask_authority(
        context,
        binding,
        args,
        |(binding, (key, completion)), rows| {
            target_for(binding, key, (row(rows, 0), row(rows, 1)), *completion)
        },
    )
    .await
}

/// This node's record of the bound set, when the bucket is still the admitted member.
pub async fn bound_view(
    context: &DriverContext,
    binding: &DistributionBinding,
) -> Result<DistributionView, GuardError> {
    ask_record(context, binding, (), |(binding, ()), rows| {
        bound(binding, rows)
    })
    .await
}

/// This node's fence row of the set.
pub async fn local_fence(context: &DriverContext, set_id: Ulid) -> Result<LocalFence, GuardError> {
    let key = LocalFence::key(set_id).into();
    let reads = vec![(DISTRIBUTION_FENCE_KEYSPACE.to_string(), key)];
    ask(context, reads, (), |(), rows| {
        Ok(LocalFence::decode(row(rows, 0))?)
    })
    .await
}

/// The range id and node of the coordinator of `key`'s range, or `None` when this member
/// coordinates it and has not fenced it for a transfer.
pub async fn read_coordinator(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<Option<(Ulid, NodeId)>, GuardError> {
    let args = key.to_string();
    ask_authority(context, binding, args, |(binding, key), rows| {
        let view = bound(binding, rows)?;
        let snapshot = view.snapshot();
        let set_id = binding.set_id;
        let range = snapshot.range_for(key).ok_or(RouteError::NoRange(set_id))?;
        if range.coordinator == binding.member_id {
            LocalFence::decode(row(rows, 1))?.check_read(snapshot, range)?;
            return Ok(None);
        }
        let coordinator = snapshot
            .member(range.coordinator)
            .ok_or(RouteError::NoRange(set_id))?;
        Ok(Some((range.range_id, coordinator.node_id)))
    })
    .await
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
    let args = (key.to_string(), tag.clone());
    ask_record(context, binding, args, |(binding, (key, tag)), rows| {
        let view = bound(binding, rows)?;
        Ok(view
            .snapshot()
            .upload_authority(binding.member_id, key, tag)?)
    })
    .await
}

/// The tags of listed uploads, from the range and epoch that hold each key now.
pub async fn upload_tags(
    context: &DriverContext,
    binding: &DistributionBinding,
    uploads: &[(String, Ulid)],
) -> Result<Vec<UploadTag>, GuardError> {
    let args = uploads.to_vec();
    ask_record(context, binding, args, |(binding, uploads), rows| {
        let view = bound(binding, rows)?;
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
    })
    .await
}

/// Resolves the range a forwarded request names to the key's range, which this member must
/// coordinate; an ancestor id from before a split resolves to its descendant.
pub async fn delegated_range(
    context: &DriverContext,
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> Result<(), GuardError> {
    let args = (range_id, key.to_string());
    ask_record(context, binding, args, |(binding, (range_id, key)), rows| {
        let view = bound(binding, rows)?;
        let snapshot = view.snapshot();
        let range = snapshot.resolve(*range_id, key)?;
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
    })
    .await
}

/// Resolves the range a forwarded read names to the key's range, whose records this ready
/// member must keep; an ancestor id from before a split resolves to its descendant.
pub async fn delegated_read(
    context: &DriverContext,
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> Result<(), GuardError> {
    let args = (range_id, key.to_string());
    ask_record(context, binding, args, |(binding, (range_id, key)), rows| {
        let view = bound(binding, rows)?;
        let snapshot = view.snapshot();
        let range = snapshot.resolve(*range_id, key)?;
        match (snapshot.record_holders(range).iter())
            .any(|member| member.member_id == binding.member_id)
        {
            true => Ok(()),
            false => Err(GuardError::Route(RouteError::StaleRange(range.range_id))),
        }
    })
    .await
}

/// Whether this member answers listings of every key in `[lower, upper)` from its records.
pub async fn lists_interval(
    context: &DriverContext,
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> Result<bool, GuardError> {
    let args = (lower.to_string(), upper.map(str::to_string));
    ask_authority(context, binding, args, |(binding, (lower, upper)), rows| {
        let view = bound(binding, rows)?;
        let fence = LocalFence::decode(row(rows, 1))?;
        let interval = (lower.as_str(), upper.as_deref());
        Ok(view
            .snapshot()
            .lists_interval(binding.member_id, interval, &fence))
    })
    .await
}

/// Whether this member, as a ready copy, may answer listings of every key in `[lower, upper)`
/// while their coordinators cannot.
pub async fn copies_interval(
    context: &DriverContext,
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> Result<bool, GuardError> {
    let args = (lower.to_string(), upper.map(str::to_string));
    ask_record(context, binding, args, |(binding, (lower, upper)), rows| {
        let view = bound(binding, rows)?;
        let interval = (lower.as_str(), upper.as_deref());
        Ok(view.snapshot().copies_interval(binding.member_id, interval))
    })
    .await
}

/// Who answers a read of `key` while its range's coordinator cannot: the range, whether this
/// member keeps the range's records, and the other nodes with a ready copy.
pub async fn copy_readers(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<(Ulid, bool, Vec<NodeId>), GuardError> {
    let args = key.to_string();
    ask_record(context, binding, args, |(binding, key), rows| {
        let view = bound(binding, rows)?;
        let snapshot = view.snapshot();
        let range = snapshot
            .range_for(key)
            .ok_or(RouteError::NoRange(binding.set_id))?;
        let here = (snapshot.record_holders(range).iter())
            .any(|member| member.member_id == binding.member_id);
        let copies = (snapshot.copy_holders(range).into_iter())
            .filter(|member| member.member_id != binding.member_id)
            .map(|member| member.node_id)
            .collect();
        Ok((range.range_id, here, copies))
    })
    .await
}

/// The nodes that answer reads of `key`'s range from their own records, coordinator last.
pub async fn record_holders(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<Vec<NodeId>, GuardError> {
    let args = key.to_string();
    ask_record(context, binding, args, |(binding, key), rows| {
        let view = bound(binding, rows)?;
        let snapshot = view.snapshot();
        let range = snapshot
            .range_for(key)
            .ok_or(RouteError::NoRange(binding.set_id))?;
        Ok((snapshot.record_holders(range).into_iter())
            .map(|member| member.node_id)
            .collect())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, record};
    use aruna_core::calls::IoError;
    use aruna_core::errors::StorageError;

    fn question() -> RouteOperation<DistributionBinding, DistributionView> {
        let (snapshot, _) = record(true);
        let bound_to = binding(&snapshot, 1, 5);
        RouteOperation {
            reads: record_read(bound_to.set_id),
            args: bound_to,
            decide: |binding, rows| bound(binding, rows),
        }
    }

    // A question reads its rows once and is decided from the answer.
    #[test]
    fn decides_from_rows() {
        let (snapshot, bytes) = record(true);
        let mut operation = question();
        let read = IoCall::Read(record_read(snapshot.set_id()));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let answer = IoResult::Values(vec![Some(bytes.into())]);
        let Ok(Next::Done(view)) = operation.next(answer) else {
            panic!("expected the record");
        };
        assert_eq!(view.snapshot().set_id(), snapshot.set_id());
    }

    // A set without a record here is missing; a failed read is unreadable.
    #[test]
    fn refuses_missing_record() {
        let (snapshot, _) = record(true);
        let mut operation = question();
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(GuardError::RecordMissing(snapshot.set_id()))
        );
        let failed = GuardError::from(IoError::Storage(StorageError::Timeout));
        assert!(matches!(failed, GuardError::Unreadable(_)));
    }
}
