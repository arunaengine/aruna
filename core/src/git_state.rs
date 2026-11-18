//! The reduced state of a document's Git records and the projection a holder keeps of it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ulid::Ulid;

use crate::NodeId;
use crate::git::{GitPack, GitRecord, LfsLock, StoredObject};
use crate::structs::storage::dataset_location::DatasetLocation;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GitState {
    pub refs: BTreeMap<String, String>,
    pub lfs: BTreeMap<String, StoredObject>,
    pub locks: BTreeMap<String, LfsLock>,
    /// Claims that lost to a held lock; they are decided when that lock's unlock is known.
    pub waiting: Vec<LfsLock>,
    /// Released locks and the record that released them.
    pub released: Vec<(LfsLock, Ulid)>,
    /// Releases the next checkpoint adds.
    pub new_released: Vec<(LfsLock, Ulid)>,
    pub revision: Option<Ulid>,
    /// The graph digest of the newest applied snapshot.
    pub digest: Option<[u8; 32]>,
    pub packs: Vec<GitPack>,
    /// Commits nodes made themselves; only their Aruna trailers are trusted.
    pub made: BTreeSet<String>,
    /// The newest checkpoint the state starts from.
    pub checkpoint: Option<Ulid>,
    /// Records applied on top of that checkpoint; the next checkpoint covers exactly these.
    pub applied: Vec<Ulid>,
    /// Packs and LFS objects those records added.
    pub new_packs: Vec<GitPack>,
    pub new_lfs: Vec<StoredObject>,
    pub new_made: Vec<String>,
    /// The newest chosen storage location and the record that chose it.
    pub location: Option<(Ulid, DatasetLocation)>,
}

impl GitState {
    /// Records the next checkpoint covers: all applied ones but the newest location choice.
    pub fn coverable(&self) -> Vec<Ulid> {
        let kept = self.location.as_ref().map(|(id, _)| *id);
        self.applied
            .iter()
            .copied()
            .filter(|id| Some(*id) != kept)
            .collect()
    }
}

/// Known answers to "is the first commit an ancestor of the second".
pub type Ancestry = BTreeMap<(String, String), bool>;

/// A holder's repository cache after applying a document's records.
#[derive(Clone, Debug, PartialEq)]
pub struct Projection {
    pub state: GitState,
    /// Shared, so reusing a remembered projection copies no records.
    pub records: Arc<Vec<GitRecord>>,
    pub holders: Vec<NodeId>,
    /// The document's record write count before the records were read.
    pub writes: u64,
}

/// What a remembered projection depends on besides the records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionInputs {
    pub node: Option<NodeId>,
    pub last_event_id: Ulid,
    pub updated_at_ms: u64,
    /// Whether other holders may generate the revision yet.
    pub overdue: bool,
}
