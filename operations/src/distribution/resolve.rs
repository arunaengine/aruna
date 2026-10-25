//! Resolves a distributed bucket's global version id on any realm node: candidate sets by hint,
//! the key from a local version id index or one probe per set or range. A bare content hash is
//! resolved the same way through content indexes. Budgets are bounded; an exhausted budget or a
//! silent range is "not resolvable now", never absence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    CONTENT_INDEX_KEYSPACE, DISTRIBUTION_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::structs::storage::distribution::inventory::{
    IndexEntry, Position, content_prefix, index_key,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSnapshot, DistributionView, MemberState, MetadataCoverage, SetHint,
};
use tokio::time::Instant;
use ulid::Ulid;

use crate::distribution::version_key::{MemberAnswer, VersionKeyOperation};
use crate::driver::{DriverContext, drive, drive_until};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// At most this many distribution records are read to find a version's candidate sets.
const MAX_SETS: usize = 4096;
/// At most this many candidate sets are looked at for one id.
const MAX_CANDIDATES: usize = 4;
/// At most this many members are asked for one id, over all candidate sets.
pub const MAX_PROBES: usize = 16;
/// At most this many versions of one content identity are read from one index or returned.
pub const MAX_CONTENT_VERSIONS: usize = 16;
/// How long resolving one id inside a longer request may take.
pub const RESOLVE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

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
    /// Ready members that keep the bytes of the key's range: its coordinator, its `copy`
    /// replicas and then the replicas keeping copies on use, this node included when it is one.
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

/// The ready members that keep the bytes of `key`'s range: its coordinator and `copy` replicas,
/// then the replicas that keep copies on use, which serve a kept copy or read it through.
pub fn range_holders(snapshot: &DistributionSnapshot, key: &str) -> Vec<NodeId> {
    let Some(range) = snapshot.range_for(key) else {
        return Vec::new();
    };
    let ready = |member_id: &Ulid, strategies: &[DataStrategy]| {
        snapshot.member(*member_id).filter(|member| {
            member.state == MemberState::Ready
                && (strategies.is_empty() || strategies.contains(&member.strategy))
        })
    };
    let keeping = [DataStrategy::CopyOnUse, DataStrategy::CacheOnUse];
    std::iter::once(ready(&range.coordinator, &[]))
        .chain(
            range
                .replicas
                .iter()
                .map(|id| ready(id, &[DataStrategy::Copy])),
        )
        .chain(range.replicas.iter().map(|id| ready(id, &keeping)))
        .flatten()
        .map(|member| member.node_id)
        .collect()
}

/// The ready members that keep the bytes of `key`'s range in `set_id`, from this node's
/// record; none without a readable record.
pub async fn key_holders(context: &DriverContext, set_id: Ulid, key: &str) -> Vec<NodeId> {
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => DistributionView::from_bytes(&value)
            .map(|view| range_holders(view.snapshot(), key))
            .unwrap_or_default(),
        _ => Vec::new(),
    }
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

/// The snapshots of this node's records that `keep` accepts, at most `wanted` of them, and
/// whether records were left unread because [`MAX_SETS`] were read.
async fn scan_sets(
    context: &DriverContext,
    keep: impl Fn(&DistributionSnapshot) -> bool,
    wanted: usize,
) -> Result<(Vec<DistributionSnapshot>, bool), String> {
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
            if keep(snapshot) && found.len() < wanted {
                found.push(snapshot.clone());
            }
        }
        match next {
            Some(next) if seen < MAX_SETS => start = Some(next),
            Some(_) => return Ok((found, true)),
            None => return Ok((found, false)),
        }
    }
}

/// The snapshots of the sets whose hint `version_id` carries, from this node's records.
pub async fn candidate_sets(
    context: &DriverContext,
    version_id: Ulid,
) -> Result<Vec<DistributionSnapshot>, String> {
    let hint = SetHint::of_version(version_id);
    let keep = |snapshot: &DistributionSnapshot| SetHint::of_set(snapshot.set_id()) == hint;
    Ok(scan_sets(context, keep, MAX_CANDIDATES).await?.0)
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
            Ok(MemberAnswer::Key(Some(key))) => return SetAnswer::Found { key, local: false },
            Ok(MemberAnswer::Key(None)) => {}
            Ok(MemberAnswer::Versions { .. }) | Err(_) => complete = false,
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

/// Where `version_id` lives, within [`RESOLVE_BUDGET`]; `None` when it is not resolvable now.
pub async fn version_holders(context: &DriverContext, version_id: Ulid) -> Option<ResolvedVersion> {
    match resolve_version(context, version_id, Instant::now() + RESOLVE_BUDGET).await {
        GlobalResolution::Found(found) => Some(found),
        _ => None,
    }
}

/// The versions of one content identity this node's content index holds in `set_id`, at most
/// `limit`, and whether more exist.
pub async fn content_versions(
    context: &DriverContext,
    set_id: Ulid,
    identity: &[u8; 32],
    limit: usize,
) -> Result<(Vec<(Ulid, String)>, bool), String> {
    let iter = StorageEffect::Iter {
        key_space: CONTENT_INDEX_KEYSPACE.to_string(),
        prefix: Some(content_prefix(set_id, identity).into()),
        start: None,
        limit: limit.saturating_add(1),
        txn_id: None,
    };
    let values = match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected storage answer {other:?}")),
    };
    let more = values.len() > limit;
    let mut versions = Vec::new();
    for (row, _) in values.into_iter().take(limit) {
        let Some(id) = row.len().checked_sub(16).and_then(|at| row.get(at..)) else {
            continue;
        };
        let Ok(id) = <[u8; 16]>::try_from(id) else {
            continue;
        };
        let version_id = Ulid::from_bytes(id);
        if let Some(key) = indexed_key(context, set_id, version_id).await? {
            versions.push((version_id, key));
        }
    }
    Ok((versions, more))
}

/// The versions of a content identity across distributed buckets.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContentResolution {
    pub versions: Vec<ResolvedVersion>,
    /// A set or range was not asked or did not answer, or a page was cut: more may exist.
    pub incomplete: bool,
}

impl ContentResolution {
    fn add(&mut self, snapshot: &DistributionSnapshot, found: Vec<(Ulid, String)>, local: bool) {
        for (version_id, key) in found {
            if self.versions.len() >= MAX_CONTENT_VERSIONS {
                self.incomplete = true;
                return;
            }
            if self
                .versions
                .iter()
                .any(|known| known.version_id == version_id)
            {
                continue;
            }
            self.versions.push(ResolvedVersion {
                set_id: snapshot.set_id(),
                bucket: snapshot.config.bucket.clone(),
                holders: range_holders(snapshot, &key),
                key,
                version_id,
                local,
            });
        }
    }
}

/// Whether this node's member of `snapshot` holds the records of every range.
fn holds_every_range(snapshot: &DistributionSnapshot, local: NodeId) -> bool {
    let member = snapshot
        .config
        .members
        .iter()
        .find(|member| member.node_id == local);
    member.is_some_and(|member| {
        snapshot.held_ranges(member.member_id).len() == snapshot.config.ranges.len()
    })
}

/// Resolves a content identity within `deadline`: this node's content index for its own
/// members, otherwise one `all` coverage member per set or the coordinator of every range.
/// Distributed buckets do not announce their objects in the DHT; this is their lookup.
pub async fn resolve_content(
    context: &DriverContext,
    identity: [u8; 32],
    deadline: Instant,
) -> ContentResolution {
    let mut resolution = ContentResolution::default();
    let (sets, truncated) = match scan_sets(context, |_| true, MAX_SETS).await {
        Ok(found) => found,
        Err(_) => {
            resolution.incomplete = true;
            return resolution;
        }
    };
    resolution.incomplete = truncated;
    let Some(net) = context.net_handle.as_ref() else {
        resolution.incomplete |= !sets.is_empty();
        return resolution;
    };
    let local = net.node_id();
    let mut probes = 0usize;
    for snapshot in &sets {
        if resolution.versions.len() >= MAX_CONTENT_VERSIONS {
            resolution.incomplete = true;
            break;
        }
        let set_id = snapshot.set_id();
        if bound_here(context, snapshot).await {
            match content_versions(context, set_id, &identity, MAX_CONTENT_VERSIONS).await {
                Ok((found, more)) => {
                    resolution.incomplete |= more;
                    resolution.add(snapshot, found, true);
                }
                Err(_) => resolution.incomplete = true,
            }
            if holds_every_range(snapshot, local) {
                continue;
            }
        }
        for node_id in probe_targets(snapshot, local) {
            if probes >= MAX_PROBES || Instant::now() >= deadline {
                resolution.incomplete = true;
                return resolution;
            }
            probes += 1;
            let lookup = VersionKeyOperation::content(node_id, *net.realm_id(), set_id, identity);
            match drive_until(lookup, context, deadline).await {
                Ok(MemberAnswer::Versions { versions, more }) => {
                    resolution.incomplete |= more;
                    resolution.add(snapshot, versions, false);
                }
                Ok(MemberAnswer::Key(_)) | Err(_) => resolution.incomplete = true,
            }
        }
    }
    resolution
}

#[cfg(test)]
#[path = "resolve_tests.rs"]
mod tests;
