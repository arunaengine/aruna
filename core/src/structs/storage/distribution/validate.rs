//! Checks a distribution configuration: complete ranges, known members, valid strategy and
//! coverage combinations, bounded counts and a bounded encoded size.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use thiserror::Error;
use ulid::Ulid;

use super::{
    DataStrategy, DistributionConfig, DistributionRecord, KeyRange, MAX_BOUND_BYTES, MAX_MEMBERS,
    MAX_RANGES, MAX_RECORD_BYTES, MetadataCoverage,
};

const MAX_BUCKET_BYTES: usize = 63;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DistributionError {
    #[error("a distributed bucket needs a bucket name of at most 63 bytes without `/`")]
    InvalidBucket,
    #[error(
        "a distributed bucket needs between 1 and {} members",
        super::MAX_MEMBERS
    )]
    MemberCount,
    #[error("member {0} appears twice")]
    DuplicateMember(Ulid),
    #[error("a node may hold only one member of a set")]
    DuplicateNode,
    #[error(
        "a distributed bucket needs between 1 and {} key ranges",
        super::MAX_RANGES
    )]
    RangeCount,
    #[error("range {0} appears twice")]
    DuplicateRange(Ulid),
    #[error("key ranges must cover the whole key space in order without gaps or overlaps")]
    IncompleteRanges,
    #[error("range bounds must be at most {} bytes", super::MAX_BOUND_BYTES)]
    BoundTooLong,
    #[error("range {0} names a coordinator or replica that is not a member")]
    UnknownMember(Ulid),
    #[error("range {0} lists its coordinator or one replica more than once")]
    DuplicateReplica(Ulid),
    #[error("the coordinator of range {0} must keep the `copy` strategy")]
    CoordinatorNotCopy(Ulid),
    #[error("member {0} has `assigned` coverage but neither coordinates nor replicates a range")]
    IdleMember(Ulid),
    #[error("range {0} has epoch 0, which belongs to ordinary buckets")]
    ZeroEpoch(Ulid),
    #[error("the refresh interval must be at least one second")]
    InvalidRefresh,
    #[error(
        "the encoded distribution record exceeds {} bytes",
        super::MAX_RECORD_BYTES
    )]
    TooLarge,
    #[error("the distribution change names set {0}, not this record")]
    SetMismatch(Ulid),
    #[error("set {0} does not belong to the group that creates it")]
    ForeignSet(Ulid),
    #[error("the origin already has the most changes waiting for their predecessor")]
    TooManyPending,
    #[error("the distribution change predecessor is stale or unknown")]
    StaleRevision,
    #[error("the distributed bucket is not pending")]
    NotPending,
    #[error("member {0} is not part of the set")]
    NoMember(Ulid),
    #[error("member {0} may only be admitted by its own node")]
    AdmissionOrigin(Ulid),
    #[error("member {0} has already admitted another incarnation")]
    IncarnationChanged(Ulid),
    #[error("member {0} has not admitted itself yet")]
    MissingAdmission(Ulid),
    #[error("a new member must start in the created state without an incarnation")]
    InvalidCreate,
    #[error("the distribution change is malformed: {0}")]
    Malformed(&'static str),
    #[error("distribution change `{0}` is not supported yet")]
    Unsupported(&'static str),
}

/// Checks the shape of a configuration; record-level checks add the encoded size.
pub fn validate_config(config: &DistributionConfig) -> Result<(), DistributionError> {
    if config.bucket.is_empty()
        || config.bucket.len() > MAX_BUCKET_BYTES
        || config.bucket.contains('/')
    {
        return Err(DistributionError::InvalidBucket);
    }
    if config.settings.refresh_secs == Some(0) {
        return Err(DistributionError::InvalidRefresh);
    }
    let members = validate_members(config)?;
    validate_ranges(&config.ranges)?;
    let mut active = BTreeSet::new();
    for range in &config.ranges {
        let mut named = BTreeSet::from([range.coordinator]);
        if !range.replicas.iter().all(|replica| named.insert(*replica)) {
            return Err(DistributionError::DuplicateReplica(range.range_id));
        }
        if named.iter().any(|member| !members.contains(member)) {
            return Err(DistributionError::UnknownMember(range.range_id));
        }
        let coordinator = config
            .members
            .iter()
            .find(|member| member.member_id == range.coordinator);
        if coordinator.is_some_and(|member| member.strategy != DataStrategy::Copy) {
            return Err(DistributionError::CoordinatorNotCopy(range.range_id));
        }
        if range.epoch == 0 {
            return Err(DistributionError::ZeroEpoch(range.range_id));
        }
        active.extend(named);
    }
    if let Some(idle) = config.members.iter().find(|member| {
        member.coverage == MetadataCoverage::Assigned && !active.contains(&member.member_id)
    }) {
        return Err(DistributionError::IdleMember(idle.member_id));
    }
    Ok(())
}

/// Checks a whole record, including the bound on its encoded size.
pub fn validate_record(record: &DistributionRecord) -> Result<(), DistributionError> {
    validate_config(&record.config)?;
    let bytes = postcard::to_allocvec(record).map_err(|_| DistributionError::TooLarge)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(DistributionError::TooLarge);
    }
    Ok(())
}

fn validate_members(config: &DistributionConfig) -> Result<BTreeSet<Ulid>, DistributionError> {
    if config.members.is_empty() || config.members.len() > MAX_MEMBERS {
        return Err(DistributionError::MemberCount);
    }
    let mut ids = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    for member in &config.members {
        if !ids.insert(member.member_id) {
            return Err(DistributionError::DuplicateMember(member.member_id));
        }
        if !nodes.insert(*member.node_id.as_bytes()) {
            return Err(DistributionError::DuplicateNode);
        }
    }
    Ok(ids)
}

/// Ranges must be sorted, start at the empty key, meet exactly and end unbounded.
pub fn validate_ranges(ranges: &[KeyRange]) -> Result<(), DistributionError> {
    if ranges.is_empty() || ranges.len() > MAX_RANGES {
        return Err(DistributionError::RangeCount);
    }
    let mut ids = BTreeSet::new();
    let mut expected_lower = Some("");
    for range in ranges {
        if !ids.insert(range.range_id) {
            return Err(DistributionError::DuplicateRange(range.range_id));
        }
        let too_long = range.lower.len() > MAX_BOUND_BYTES
            || range
                .upper
                .as_ref()
                .is_some_and(|upper| upper.len() > MAX_BOUND_BYTES);
        if too_long {
            return Err(DistributionError::BoundTooLong);
        }
        let empty = range
            .upper
            .as_deref()
            .is_some_and(|upper| upper <= range.lower.as_str());
        if expected_lower != Some(range.lower.as_str()) || empty {
            return Err(DistributionError::IncompleteRanges);
        }
        expected_lower = range.upper.as_deref();
    }
    match expected_lower {
        None => Ok(()),
        Some(_) => Err(DistributionError::IncompleteRanges),
    }
}
