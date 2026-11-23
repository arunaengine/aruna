//! Entries, queries, pages and effects of the per-range key indexes of distributed buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::ops::Bound;
use std::time::SystemTime;

use thiserror::Error;
use ulid::Ulid;

/// One live key of a range: its current version and size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedKey {
    pub key: String,
    pub size: u64,
    pub version_id: Ulid,
    /// The search terms of a searchable version.
    pub terms: Option<MetaTerms>,
}

/// Free text tokens and exact `column=value` terms of one version's metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetaTerms {
    pub text: BTreeSet<String>,
    pub exact: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyChange {
    Put(IndexedKey),
    Remove(String),
}

/// Keys between `start` and `end` in key order; `names` or `meta` keep keys with every token or
/// term. Text tokens of three or more characters match by start; an overfull metadata one is left
/// out, so callers check loaded versions. Terms cap matches at `limit`, else keys per segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyQuery {
    pub start: Bound<String>,
    pub end: Bound<String>,
    pub names: Option<String>,
    pub meta: Option<MetaTerms>,
    pub limit: usize,
}

/// The live matching keys of the examined part of a window, in key order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyPage {
    pub keys: Vec<String>,
    /// Keys looked at: the matches of a query with terms, every walked key of one without.
    pub examined: usize,
    /// The last examined key when the window continues after it; `None` once it is read.
    pub last: Option<String>,
    /// When the index last committed, read after the search; `None` without a recorded time.
    pub committed_at: Option<SystemTime>,
}

/// Work on the key index generation `{set id}/{range id}/{generation}` of this node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexEffect {
    /// Applies the changes in order, creating the generation when missing, and commits.
    Apply {
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
        changes: Vec<KeyChange>,
    },
    /// Searches an existing generation; a missing one fails with [`IndexError::Missing`].
    Search {
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
        query: KeyQuery,
    },
    /// Whether the generation holds an index of the current schema.
    Exists {
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    },
    /// Whether the generation holds an index of an older schema.
    Outdated {
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    },
    /// Live keys in the generation as of its last commit; a missing one holds none.
    Count {
        set_id: Ulid,
        range_id: Ulid,
        generation: u64,
    },
    /// Removes every generation of the range except `keep`.
    Prune {
        set_id: Ulid,
        range_id: Ulid,
        keep: Option<u64>,
    },
}

/// The answer to an [`IndexEffect`], one variant per effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexEvent {
    Applied,
    Searched(Box<KeyPage>),
    Exists(bool),
    Outdated(bool),
    Counted(u64),
    Pruned,
    Error(IndexError),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IndexError {
    #[error("this node keeps no range key indexes")]
    Unavailable,
    #[error("the range key index generation does not exist")]
    Missing,
    #[error("range key index failed: {0}")]
    Failed(String),
}
