//! Delivers a distributed bucket's reference records to a member in bounded pages: the
//! coordinator builds a page from its queued member jobs, the member applies it in one
//! transaction and answers only after that commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod apply;
pub mod build;
pub mod deliver;
mod plan;

use aruna_core::structs::storage::distribution::Publication;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::protocol::VersionReplicationManifest;

/// Records one page carries at most.
pub const PAGE_RECORDS: usize = 4096;
/// Encoded bytes one page carries at most, below the 16 MiB metadata message limit.
pub const PAGE_BYTES: usize = 12 * 1024 * 1024;

/// Reference records of one range, published by its coordinator to one member.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReferencePage {
    pub bucket: String,
    pub publication: Publication,
    /// At most one record per key, each a reference with the page's publication.
    pub manifests: Vec<VersionReplicationManifest>,
}

/// The member's answer to one page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PageAnswer {
    /// Committed in one transaction: `applied` records are stored, including replays of
    /// stored ones. `single` names the versions the page left for single-record delivery.
    Applied { applied: u32, single: Vec<Ulid> },
    /// Nothing was written; with `retry` the same page may pass later.
    Refused { reason: String, retry: bool },
}
