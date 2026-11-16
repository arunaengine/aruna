//! Holds the metadata, bucket and object search request, cursor and result types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    ApiQueryMode, AuthContext, BucketSearchHit, Deserialize, GroupId, MetadataFanoutStats,
    MetadataSearchHit, NodeId, ObjectInventoryHit, ObjectKeyMatch, Serialize, SignedCursor,
    SystemTime,
};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::index_feed::IndexProgress;

#[derive(Debug, Clone)]
pub struct MetadataSearchRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub conforms_to: Option<String>,
    pub group_id: Option<GroupId>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<ApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchExecution {
    pub hits: Vec<MetadataSearchHit>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct BucketSearchRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    /// An empty query lists every readable bucket.
    pub query: String,
    /// Admits a bucket by READ on this key or key prefix inside it instead of on its root.
    pub key: Option<String>,
    /// An MCP tool's policy context, operation and parameters, whose policies must also allow
    /// each hit on each node before members merge; it only adds to the mandatory read check.
    pub extras: Option<PolicyRequestExtras>,
    pub limit: usize,
    /// Only buckets of this group.
    pub group_id: Option<GroupId>,
    /// `next_cursor` of the previous page.
    pub cursor: Option<String>,
    pub target_nodes: Option<Vec<NodeId>>,
    /// Only buckets whose metadata marks them as a database.
    pub database: bool,
}

#[derive(Debug, Clone)]
pub struct BucketSearchExecution {
    pub hits: Vec<BucketSearchHit>,
    /// Continues the listing; `None` once every node reached its end.
    pub next_cursor: Option<String>,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectQueryMode {
    Local,
    DistributedBestEffort,
    DistributedStrict,
}

impl ObjectQueryMode {
    pub(super) fn fanout_mode(self) -> ApiQueryMode {
        match self {
            Self::Local => ApiQueryMode::Local,
            Self::DistributedBestEffort | Self::DistributedStrict => ApiQueryMode::Distributed,
        }
    }

    pub(super) fn allow_partial(self) -> bool {
        !matches!(self, Self::DistributedStrict)
    }
}

#[derive(Debug, Clone)]
pub struct SearchQueryRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub query: String,
    pub key_match: ObjectKeyMatch,
    pub bucket: Option<String>,
    pub limit: usize,
    pub cursor: Option<String>,
    pub mode: ObjectQueryMode,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct ObjectPartitionCoverage {
    pub node_id: NodeId,
    pub observed_at: SystemTime,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct ObjectExecution {
    pub hits: Vec<ObjectInventoryHit>,
    pub next_cursor: Option<String>,
    pub as_of: SystemTime,
    pub partitions: Vec<ObjectPartitionCoverage>,
    pub fanout_stats: MetadataFanoutStats,
    pub omitted_partitions: usize,
    /// Distributed bucket ranges this page could not search: a missing or building index,
    /// or in local mode a range another member coordinates.
    pub incomplete_ranges: usize,
    /// Index progress of incomplete ranges, by the node that coordinates them.
    pub index_progress: Vec<(NodeId, ulid::Ulid, IndexProgress)>,
    /// Incomplete ranges answered up to a key, by their coordinator.
    pub partial_ranges: Vec<(NodeId, ulid::Ulid, Option<String>)>,
    pub complete: bool,
    /// Readable keys of the first page's snapshot replaced or deleted since, through this page;
    /// `None` once a searched range index changed after the snapshot.
    pub changed_keys: Option<usize>,
    /// The part of `changed_keys` this page passed.
    pub page_changed: usize,
    /// A distributed bucket's ranges changed during the search, so later pages stay incomplete.
    pub layout_changed: bool,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectPartitionState {
    pub(super) node_id: NodeId,
    pub(super) start_after: Option<Vec<u8>>,
    pub(super) exhausted: bool,
    pub(super) observed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectCursorPartition {
    pub(super) node_id: [u8; 32],
    pub(super) start_after: Option<Vec<u8>>,
    pub(super) exhausted: bool,
    pub(super) observed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectCursorPayload {
    pub(super) as_of: SystemTime,
    pub(super) partitions: Vec<ObjectCursorPartition>,
    pub(super) failed_partitions: Vec<[u8; 32]>,
    pub(super) discovery_failed: bool,
    pub(super) omitted_partitions: usize,
    pub(super) pending: Vec<PendingBucket>,
    pub(super) changed_keys: Option<usize>,
    pub(super) layout: RangeLayout,
}

/// The key ranges of the distributed buckets the pages of one search were answered under.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RangeLayout {
    /// The layout digest each distributed set answered with first, by set id.
    pub(super) seen: Vec<(ulid::Ulid, [u8; 32])>,
    /// A set's ranges changed coordinator or bounds between pages or between nodes, so a
    /// member may have skipped keys another member no longer searched.
    pub(super) changed: bool,
}

impl RangeLayout {
    pub(super) fn observe(mut self, (set_id, digest): (ulid::Ulid, [u8; 32])) -> Self {
        match self.seen.iter().find(|(seen_id, _)| *seen_id == set_id) {
            Some((_, seen)) => self.changed |= *seen != digest,
            None => self.seen.push((set_id, digest)),
        }
        self
    }
}

/// A distributed bucket a realm-wide search still owes: its name and the cursor of its own
/// bucket search, `None` before its first page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingBucket {
    pub(super) bucket: String,
    pub(super) cursor: Option<String>,
}

pub(super) type ObjectCursor = SignedCursor<ObjectCursorPayload>;
