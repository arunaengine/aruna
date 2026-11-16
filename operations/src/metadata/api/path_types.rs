//! Holds the path lookup types and picks and merges the holders of a path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{AuthContext, GroupId, MetadataPathCandidate, MetadataPathWinner, NodeId, Ulid};
use futures_util::StreamExt;

#[derive(Debug, Clone)]
pub struct MetadataLookupRequest {
    pub group_id: GroupId,
    pub document_path: String,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone)]
pub struct MetadataLookupResult {
    pub winner: MetadataPathWinner,
    pub conflicts: Vec<Ulid>,
}

#[derive(Debug)]
pub(super) struct PathHolderSelection {
    pub(super) node_id: NodeId,
    pub(super) shards: Vec<u32>,
}

pub(super) struct PathShardView {
    pub(super) shard: u32,
    pub(super) candidates: Vec<MetadataPathCandidate>,
}
