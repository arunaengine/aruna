//! Holds the pages a node answers to bucket and object key searches across the realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::structs::storage::bucket_metadata::BucketMetadata;
use crate::structs::storage::distribution::search::IndexProgress;
use crate::types::GroupId;

/// One node's page: its readable hits, and where its scan stopped when the row budget ran out
/// before the end.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPage {
    pub hits: Vec<BucketSearchHit>,
    /// The last bucket scanned, past every hit, sealed so only this node reads it; `None` when
    /// the scan was not cut.
    pub scanned_to: Option<Vec<u8>>,
    /// The cursor's position no longer opens here, for example after a restart, so this node's
    /// part of the listing ends incomplete.
    pub retired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSearchHit {
    pub arn: String,
    pub bucket: String,
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub group_name: Option<String>,
    pub created_at: SystemTime,
    /// The distributed bucket this local bucket is a member of; `None` for an ordinary bucket.
    pub set_id: Option<ulid::Ulid>,
    /// The bucket's current metadata, if it has any.
    pub metadata: Option<BucketMetadata>,
    /// Anyone, also without a login, may read the bucket; set for a database search only.
    pub public: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectKeyMatch {
    Substring,
    Prefix,
    /// Matches searchable object metadata instead of keys: free text plus `column=value` terms.
    Metadata,
}

impl ObjectKeyMatch {
    pub fn matches(self, key: &str, query: &str) -> bool {
        match self {
            Self::Substring => key.contains(query),
            Self::Prefix => key.starts_with(query),
            Self::Metadata => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectInventoryChecksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectInventoryHit {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub bucket: String,
    pub key: String,
    pub content_w3id: Option<String>,
    pub checksum: Option<ObjectInventoryChecksum>,
    pub size: Option<u64>,
    pub updated_at: Option<SystemTime>,
    /// A metadata search hit's kept columns, without the search selections.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchNodeHit {
    pub hit: ObjectInventoryHit,
    pub cursor_key: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchNodePage {
    pub hits: Vec<SearchNodeHit>,
    pub next_start_after: Option<Vec<u8>>,
    pub observed_at: SystemTime,
    /// Ranges of a distributed bucket this node coordinates whose index is missing or still
    /// being built, and distributed buckets a realm-wide scan skipped; the page does not
    /// cover their keys.
    #[serde(default)]
    pub incomplete_ranges: usize,
    /// Ranges of a distributed bucket another member coordinates; their keys come from
    /// that member's page.
    #[serde(default)]
    pub remote_ranges: usize,
    /// This node's index progress of the incomplete ranges it coordinates.
    #[serde(default)]
    pub index_progress: Vec<(ulid::Ulid, IndexProgress)>,
    /// Incomplete ranges whose building index answered for the keys up to a key, named only
    /// if the caller may read it.
    #[serde(default)]
    pub partial_ranges: Vec<(ulid::Ulid, Option<String>)>,
    /// The distributed buckets this realm-wide page skipped, each counted in `incomplete_ranges`.
    #[serde(default)]
    pub skipped_buckets: Vec<String>,
    /// Cursor keys of readable keys this page passed whose head was replaced or deleted after
    /// `as_of`; they are counted, not shown. `None` when a searched range index changed since.
    #[serde(default)]
    pub changed: Option<Vec<Vec<u8>>>,
    /// The distributed set this page searched and a digest of its key ranges and their
    /// coordinators; `None` for other buckets.
    #[serde(default)]
    pub layout: Option<(ulid::Ulid, [u8; 32])>,
}

/// Where craqle keeps its full-text search index.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataSearchStorage {
    #[default]
    Disk,
    Memory,
}

impl From<MetadataSearchStorage> for craqle::SearchStorage {
    fn from(search_storage: MetadataSearchStorage) -> Self {
        match search_storage {
            MetadataSearchStorage::Disk => Self::Disk,
            MetadataSearchStorage::Memory => Self::Memory,
        }
    }
}
