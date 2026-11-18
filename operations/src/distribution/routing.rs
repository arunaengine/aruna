//! Names the routing questions of a bound bucket: each returns the operation that reads this
//! node's distribution rows and decides the answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::DISTRIBUTION_FENCE_KEYSPACE;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, ReadRoute, WriteRoute,
};
use ulid::Ulid;

use super::route_query::{
    Bound, Decide, Interval, coordinator_of, copied_interval, fence_row, held_view, interval,
    listed_interval, listed_tags, new_tag, range_coordinated, range_holders, range_kept,
    range_readers, read_route, record_read, upload_held, write_route,
};
pub use super::route_query::RouteOperation;
use crate::distribution::guard::authority_reads;

/// A question of one bound set, answered with `T`.
pub type BoundQuestion<A, T> = RouteOperation<Bound<A>, T>;

/// A question of the bound set's record alone.
fn of_record<A, T>(
    binding: &DistributionBinding,
    args: A,
    decide: Decide<Bound<A>, T>,
) -> BoundQuestion<A, T> {
    RouteOperation {
        reads: record_read(binding.set_id),
        args: (binding.clone(), args),
        decide,
    }
}

/// A question of the bound set's record and this node's fence row.
fn of_authority<A, T>(
    binding: &DistributionBinding,
    args: A,
    decide: Decide<Bound<A>, T>,
) -> BoundQuestion<A, T> {
    RouteOperation {
        reads: authority_reads(binding.set_id),
        args: (binding.clone(), args),
        decide,
    }
}

/// Where a read of a bound bucket is answered, from this node's record.
pub fn route_read(
    binding: &DistributionBinding,
    key: &str,
    listing: bool,
) -> BoundQuestion<(String, bool), ReadRoute> {
    of_record(binding, (key.to_string(), listing), read_route)
}

/// Where a write to `key` of a bound bucket goes, from this node's record; transports use it
/// to forward a write before its bytes move.
pub fn route_write(
    binding: &DistributionBinding,
    key: &str,
) -> BoundQuestion<(String, bool), WriteRoute> {
    of_authority(binding, (key.to_string(), false), write_route)
}

/// Where a request on an open upload of `key` goes; a range fence in its upload grace period
/// keeps it on this node, so the upload can still complete.
pub fn route_upload(
    binding: &DistributionBinding,
    key: &str,
) -> BoundQuestion<(String, bool), WriteRoute> {
    of_authority(binding, (key.to_string(), true), write_route)
}

/// This node's record of the bound set, when the bucket is still the admitted member.
pub fn bound_view(binding: &DistributionBinding) -> BoundQuestion<(), DistributionView> {
    of_record(binding, (), held_view)
}

/// This node's fence row of the set.
pub fn local_fence(set_id: Ulid) -> RouteOperation<(), LocalFence> {
    let key = LocalFence::key(set_id).into();
    RouteOperation {
        reads: vec![(DISTRIBUTION_FENCE_KEYSPACE.to_string(), key)],
        args: (),
        decide: fence_row,
    }
}

/// The range id and node of the coordinator of `key`'s range, or `None` when this member
/// coordinates it and has not fenced it for a transfer.
pub fn read_coordinator(
    binding: &DistributionBinding,
    key: &str,
) -> BoundQuestion<String, Option<(Ulid, NodeId)>> {
    of_authority(binding, key.to_string(), coordinator_of)
}

/// Names a new upload of `key` by the range and epoch this member coordinates it under.
pub fn upload_tag(
    binding: &DistributionBinding,
    key: &str,
    upload_id: Ulid,
) -> BoundQuestion<(String, Ulid), UploadTag> {
    of_authority(binding, (key.to_string(), upload_id), new_tag)
}

/// Whether this member still holds the authority the upload `tag` was opened under: the key's
/// range descends from the named one, at the same epoch, coordinated here.
pub fn upload_route(
    binding: &DistributionBinding,
    key: &str,
    tag: &UploadTag,
) -> BoundQuestion<(String, UploadTag), ()> {
    of_record(binding, (key.to_string(), tag.clone()), upload_held)
}

/// The tags of listed uploads, from the range and epoch that hold each key now.
pub fn upload_tags(
    binding: &DistributionBinding,
    uploads: &[(String, Ulid)],
) -> BoundQuestion<Vec<(String, Ulid)>, Vec<UploadTag>> {
    of_record(binding, uploads.to_vec(), listed_tags)
}

/// Resolves the range a forwarded request names to the key's range, which this member must
/// coordinate; an ancestor id from before a split resolves to its descendant.
pub fn delegated_range(
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> BoundQuestion<(Ulid, String), ()> {
    of_record(binding, (range_id, key.to_string()), range_coordinated)
}

/// Resolves the range a forwarded read names to the key's range, whose records this ready
/// member must keep; an ancestor id from before a split resolves to its descendant.
pub fn delegated_read(
    binding: &DistributionBinding,
    range_id: Ulid,
    key: &str,
) -> BoundQuestion<(Ulid, String), ()> {
    of_record(binding, (range_id, key.to_string()), range_kept)
}

/// Whether this member answers listings of every key in `[lower, upper)` from its records.
pub fn lists_interval(
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> BoundQuestion<Interval, bool> {
    of_authority(binding, interval(lower, upper), listed_interval)
}

/// Whether this member, as a ready copy, may answer listings of every key in `[lower, upper)`
/// while their coordinators cannot.
pub fn copies_interval(
    binding: &DistributionBinding,
    lower: &str,
    upper: Option<&str>,
) -> BoundQuestion<Interval, bool> {
    of_record(binding, interval(lower, upper), copied_interval)
}

/// Who answers a read of `key` while its range's coordinator cannot: the range, whether this
/// member keeps the range's records, and the other nodes with a ready copy.
pub fn copy_readers(
    binding: &DistributionBinding,
    key: &str,
) -> BoundQuestion<String, (Ulid, bool, Vec<NodeId>)> {
    of_record(binding, key.to_string(), range_readers)
}

/// The nodes that answer reads of `key`'s range from their own records, coordinator last.
pub fn record_holders(
    binding: &DistributionBinding,
    key: &str,
) -> BoundQuestion<String, Vec<NodeId>> {
    of_record(binding, key.to_string(), range_holders)
}
