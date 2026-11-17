//! Holds the object key search input, hit, page and error types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::PolicyEnforcementError;
use crate::distribution::index_feed::IndexProgress;
use aruna_core::NodeId;
use aruna_core::errors::ConversionError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;

pub const SEARCH_MAX_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectKeyMatch {
    Substring,
    Prefix,
    /// Matches searchable object metadata instead of keys: free text plus `column=value` terms.
    Metadata,
}

impl ObjectKeyMatch {
    pub(crate) fn matches(self, key: &str, query: &str) -> bool {
        match self {
            Self::Substring => key.contains(query),
            Self::Prefix => key.starts_with(query),
            Self::Metadata => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SearchObjectsInput {
    pub auth: AuthContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub query: String,
    pub key_match: ObjectKeyMatch,
    pub bucket: Option<String>,
    pub limit: usize,
    pub start_after: Option<Vec<u8>>,
    pub as_of: SystemTime,
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

#[derive(Debug, Error)]
pub enum SearchObjectsError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PolicyEnforcementError),
    #[error("object inventory storage failed: {0}")]
    Storage(String),
    #[error("object inventory scan budget exhausted")]
    Unavailable,
    #[error("the search position is no longer valid")]
    Position,
}
