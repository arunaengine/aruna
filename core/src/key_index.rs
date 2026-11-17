//! Entries, queries and pages of the per-range key indexes of distributed buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::ops::Bound;
use std::time::SystemTime;

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
