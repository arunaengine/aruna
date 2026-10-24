//! Resolves a distributed bucket's global version id on any realm node, member or not. The
//! hint in the id names candidate sets among the records this node holds; the key comes from
//! a local member's version id index or from one probe per set (a member with `all` coverage)
//! or per range. Budgets are bounded, and an exhausted budget or a silent range is "not
//! resolvable now", never absence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, VERSION_INDEX_KEYSPACE};
use aruna_core::structs::storage::distribution::inventory::{IndexEntry, Position, index_key};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSnapshot, DistributionView, MemberState, MetadataCoverage, SetHint,
};
use tokio::time::Instant;
use ulid::Ulid;

use crate::distribution::version_key::VersionKeyOperation;
use crate::driver::{DriverContext, drive, drive_until};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// At most this many distribution records are read to find a version's candidate sets.
const MAX_SETS: usize = 4096;
/// At most this many candidate sets are looked at for one id.
const MAX_CANDIDATES: usize = 4;
/// At most this many members are asked for one id, over all candidate sets.
pub const MAX_PROBES: usize = 16;

/// Where one global version id lives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedVersion {
    pub set_id: Ulid,
    /// The one bucket name every member's local bucket carries.
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    /// This node's member bucket is bound to the set and its index holds the version.
    pub local: bool,
    /// Ready members that keep the bytes of the key's range: its coordinator and its `copy`
    /// replicas, this node included when it is one.
    pub holders: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GlobalResolution {
    Found(ResolvedVersion),
    /// Every candidate set answered and none holds the version.
    NotFound,
    /// No record names a candidate set yet, the budget ran out or a range did not answer.
    Unavailable,
    /// More than one set holds the version id.
    Ambiguous,
}

/// The ready members that keep the bytes of `key`'s range.
pub fn range_holders(snapshot: &DistributionSnapshot, key: &str) -> Vec<NodeId> {
    let Some(range) = snapshot.range_for(key) else {
        return Vec::new();
    };
    let ready = |member_id: &Ulid, copy_only: bool| {
        snapshot.member(*member_id).filter(|member| {
            member.state == MemberState::Ready
                && (!copy_only || member.strategy == DataStrategy::Copy)
        })
    };
    std::iter::once(ready(&range.coordinator, false))
        .chain(range.replicas.iter().map(|replica| ready(replica, true)))
        .flatten()
        .map(|member| member.node_id)
        .collect()
}

/// Whom to ask for the key of a version in `snapshot`: one ready member with `all` coverage,
/// otherwise the coordinator of every range. `local` is never asked.
pub fn probe_targets(snapshot: &DistributionSnapshot, local: NodeId) -> Vec<NodeId> {
    let full = snapshot.config.members.iter().find(|member| {
        member.node_id != local
            && member.state == MemberState::Ready
            && member.coverage == MetadataCoverage::All
    });
    if let Some(member) = full {
        return vec![member.node_id];
    }
    let mut targets: Vec<NodeId> = snapshot
        .config
        .ranges
        .iter()
        .filter_map(|range| snapshot.member(range.coordinator))
        .map(|member| member.node_id)
        .filter(|node_id| *node_id != local)
        .collect();
    targets.dedup();
    targets
}

/// The snapshots of the sets whose hint `version_id` carries, from this node's records.
pub async fn candidate_sets(
    context: &DriverContext,
    version_id: Ulid,
) -> Result<Vec<DistributionSnapshot>, String> {
    let hint = SetHint::of_version(version_id);
    let mut found = Vec::new();
    let mut start = None;
    let mut seen = 0usize;
    loop {
        let iter = StorageEffect::Iter {
            key_space: DISTRIBUTION_KEYSPACE.to_string(),
            prefix: None,
            start: start.take().map(IterStart::After),
            limit: 256,
            txn_id: None,
        };
        let (values, next) = match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected storage answer {other:?}")),
        };
        for (_, value) in values {
            seen += 1;
            let Ok(view) = DistributionView::from_bytes(&value) else {
                continue;
            };
            let snapshot = view.snapshot();
            if SetHint::of_set(snapshot.set_id()) == hint && found.len() < MAX_CANDIDATES {
                found.push(snapshot.clone());
            }
        }
        match next {
            Some(next) if seen < MAX_SETS => start = Some(next),
            _ => return Ok(found),
        }
    }
}

/// The key this node's version id index holds for `version_id` in `set_id`.
pub async fn indexed_key(
    context: &DriverContext,
    set_id: Ulid,
    version_id: Ulid,
) -> Result<Option<String>, String> {
    let read = StorageEffect::Read {
        key_space: VERSION_INDEX_KEYSPACE.to_string(),
        key: index_key(set_id, version_id).into(),
        txn_id: None,
    };
    let value = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected storage answer {other:?}")),
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let entry = IndexEntry::from_bytes(&value).map_err(|error| error.to_string())?;
    let position = Position::decode(&entry.position).map_err(|error| error.to_string())?;
    Ok(Some(position.key))
}

/// Whether this node's bucket of the set's name is bound to the set.
async fn bound_here(context: &DriverContext, snapshot: &DistributionSnapshot) -> bool {
    let bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
    match drive(bucket, context).await {
        Ok(info) => info
            .distribution
            .is_some_and(|binding| binding.set_id == snapshot.set_id()),
        Err(GetBucketError::NotFound) | Err(_) => false,
    }
}

/// What one candidate set says about a version.
enum SetAnswer {
    Found { key: String, local: bool },
    Absent,
    Unknown,
}

async fn ask_set(
    context: &DriverContext,
    snapshot: &DistributionSnapshot,
    version_id: Ulid,
    probes: &mut usize,
    deadline: Instant,
) -> SetAnswer {
    let set_id = snapshot.set_id();
    if bound_here(context, snapshot).await {
        match indexed_key(context, set_id, version_id).await {
            Ok(Some(key)) => return SetAnswer::Found { key, local: true },
            Ok(None) => {}
            Err(_) => return SetAnswer::Unknown,
        }
    }
    let Some(net) = context.net_handle.as_ref() else {
        return SetAnswer::Unknown;
    };
    let targets = probe_targets(snapshot, net.node_id());
    if targets.is_empty() {
        return SetAnswer::Absent;
    }
    let mut complete = true;
    for node_id in targets {
        if *probes >= MAX_PROBES || Instant::now() >= deadline {
            return SetAnswer::Unknown;
        }
        *probes += 1;
        let lookup = VersionKeyOperation::new(node_id, *net.realm_id(), set_id, version_id);
        match drive_until(lookup, context, deadline).await {
            Ok(Some(key)) => return SetAnswer::Found { key, local: false },
            Ok(None) => {}
            Err(_) => complete = false,
        }
    }
    match complete {
        true => SetAnswer::Absent,
        false => SetAnswer::Unknown,
    }
}

/// Resolves `version_id` within `deadline`.
pub async fn resolve_version(
    context: &DriverContext,
    version_id: Ulid,
    deadline: Instant,
) -> GlobalResolution {
    let Ok(candidates) = candidate_sets(context, version_id).await else {
        return GlobalResolution::Unavailable;
    };
    if candidates.is_empty() {
        return GlobalResolution::Unavailable;
    }
    let mut probes = 0usize;
    let mut found: Option<ResolvedVersion> = None;
    let mut complete = true;
    for snapshot in &candidates {
        match ask_set(context, snapshot, version_id, &mut probes, deadline).await {
            SetAnswer::Found { key, local } => {
                if found.is_some() {
                    return GlobalResolution::Ambiguous;
                }
                found = Some(ResolvedVersion {
                    set_id: snapshot.set_id(),
                    bucket: snapshot.config.bucket.clone(),
                    holders: range_holders(snapshot, &key),
                    key,
                    version_id,
                    local,
                });
            }
            SetAnswer::Absent => {}
            SetAnswer::Unknown => complete = false,
        }
    }
    match (found, complete) {
        (Some(found), _) => GlobalResolution::Found(found),
        (None, true) => GlobalResolution::NotFound,
        (None, false) => GlobalResolution::Unavailable,
    }
}

#[cfg(test)]
#[path = "resolve_tests.rs"]
mod tests;
