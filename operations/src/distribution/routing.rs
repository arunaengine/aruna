//! Reads this node's distribution records to route reads, writes and uploads of a bound bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::NodeId;
use aruna_core::keyspaces::DISTRIBUTION_FENCE_KEYSPACE;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, ReadRoute, WriteRoute,
};
use aruna_core::types::{Key, KeySpace};
use ulid::Ulid;

use super::route_query::{
    Bound, Decide, RouteOperation, coordinator_read, copied_interval, fence_row, held_view,
    interval, listed_interval, listed_tags, range_coordinated, range_holders, range_kept,
    range_readers, read_route, record_read, tag_of, upload_held, write_route,
};
use crate::distribution::guard::{GuardError, authority_reads};
use crate::driver::{DriverContext, drive};

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

/// Asks a question of the bound set's record alone.
async fn ask_record<A, T>(
    context: &DriverContext,
    binding: &DistributionBinding,
    args: A,
    decide: Decide<Bound<A>, T>,
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
    decide: Decide<Bound<A>, T>,
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
    ask_record(context, binding, (key.to_string(), listing), read_route).await
}

/// Where a write to `key` of a bound bucket goes, from this node's record; transports use it
/// to forward a write before its bytes move.
pub async fn route_write(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<WriteRoute, GuardError> {
    ask_authority(context, binding, (key.to_string(), false), write_route).await
}

/// Where a request on an open upload of `key` goes; a range fence in its upload grace period
/// keeps it on this node, so the upload can still complete.
pub async fn route_upload(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<WriteRoute, GuardError> {
    ask_authority(context, binding, (key.to_string(), true), write_route).await
}

/// This node's record of the bound set, when the bucket is still the admitted member.
pub async fn bound_view(
    context: &DriverContext,
    binding: &DistributionBinding,
) -> Result<DistributionView, GuardError> {
    ask_record(context, binding, (), held_view).await
}

/// This node's fence row of the set.
pub async fn local_fence(context: &DriverContext, set_id: Ulid) -> Result<LocalFence, GuardError> {
    let key = LocalFence::key(set_id).into();
    let reads = vec![(DISTRIBUTION_FENCE_KEYSPACE.to_string(), key)];
    ask(context, reads, (), fence_row).await
}

/// The range id and node of the coordinator of `key`'s range, or `None` when this member
/// coordinates it and has not fenced it for a transfer.
pub async fn read_coordinator(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<Option<(Ulid, NodeId)>, GuardError> {
    drive(coordinator_read(binding.clone(), key.to_string()), context).await
}

/// Names a new upload of `key` by the range and epoch this member coordinates it under.
pub async fn upload_tag(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    upload_id: Ulid,
) -> Result<UploadTag, GuardError> {
    tag_of(
        binding,
        upload_id,
        route_write(context, binding, key).await?,
    )
}

/// Whether this member still holds the authority the upload `tag` was opened under: the key's
/// range descends from the named one, at the same epoch, coordinated here.
pub async fn upload_route(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
    tag: &UploadTag,
) -> Result<(), GuardError> {
    ask_record(
        context,
        binding,
        (key.to_string(), tag.clone()),
        upload_held,
    )
    .await
}

/// The tags of listed uploads, from the range and epoch that hold each key now.
pub async fn upload_tags(
    context: &DriverContext,
    binding: &DistributionBinding,
    uploads: &[(String, Ulid)],
) -> Result<Vec<UploadTag>, GuardError> {
    ask_record(context, binding, uploads.to_vec(), listed_tags).await
}

/// Resolves the range a forwarded request names to the key's range, which this member must
/// coordinate; an ancestor id from before a split resolves to its descendant.
pub async fn delegated_range(
    context: &DriverContext,
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> Result<(), GuardError> {
    ask_record(
        context,
        binding,
        (range_id, key.to_string()),
        range_coordinated,
    )
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
    ask_record(context, binding, (range_id, key.to_string()), range_kept).await
}

/// Whether this member answers listings of every key in `[lower, upper)` from its records.
pub async fn lists_interval(
    context: &DriverContext,
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> Result<bool, GuardError> {
    ask_authority(context, binding, interval(lower, upper), listed_interval).await
}

/// Whether this member, as a ready copy, may answer listings of every key in `[lower, upper)`
/// while their coordinators cannot.
pub async fn copies_interval(
    context: &DriverContext,
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> Result<bool, GuardError> {
    ask_record(context, binding, interval(lower, upper), copied_interval).await
}

/// Who answers a read of `key` while its range's coordinator cannot: the range, whether this
/// member keeps the range's records, and the other nodes with a ready copy.
pub async fn copy_readers(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<(Ulid, bool, Vec<NodeId>), GuardError> {
    ask_record(context, binding, key.to_string(), range_readers).await
}

/// The nodes that answer reads of `key`'s range from their own records, coordinator last.
pub async fn record_holders(
    context: &DriverContext,
    binding: &DistributionBinding,
    key: &str,
) -> Result<Vec<NodeId>, GuardError> {
    ask_record(context, binding, key.to_string(), range_holders).await
}
