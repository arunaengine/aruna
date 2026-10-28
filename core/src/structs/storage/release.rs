//! Shapes of database release checks: which exact versions a release pins, what a range holder
//! answers, and the effects that read a release crate or ask a holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::identity::auth::AuthContext;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::bucket_metadata::ReleaseRef;
use crate::structs::storage::origin::{OriginVersion, SourcePin};
use crate::types::GroupId;

/// One exact version whose releases are asked for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipQuery {
    pub key: String,
    pub version_id: Ulid,
}

/// The releases that pin one queried version; `complete` is false when some release could not
/// be checked, so one may be missing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseMembers {
    pub releases: Vec<ReleaseRef>,
    pub complete: bool,
}

impl ReleaseMembers {
    pub fn complete() -> Self {
        Self {
            releases: Vec::new(),
            complete: true,
        }
    }

    pub fn merge(&mut self, other: ReleaseMembers) {
        for release in other.releases {
            if !self.releases.contains(&release) {
                self.releases.push(release);
            }
        }
        self.complete &= other.complete;
    }
}

/// A release whose tagged crate version was read and checked, with where it lives.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidRelease {
    pub release: ReleaseRef,
    pub key: String,
    pub version_id: Ulid,
    pub prefix: String,
}

/// A holder's membership answer for some ranges; the ranges it does not serve come back
/// refused.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeMembers {
    pub answers: Vec<ReleaseMembers>,
    pub releases: Vec<ValidRelease>,
    /// `false` when a release crate in the answered ranges could not be read.
    pub complete: bool,
    pub refused: Vec<Ulid>,
}

/// One current file a release pins, all of it from one exact version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseFile {
    pub key: String,
    pub version: OriginVersion,
    pub size: u64,
    /// The content id, when the bytes have a known BLAKE3.
    pub content: Option<String>,
    /// What pins a reference version: its source and the source's ETag.
    pub pin: Option<(SourcePin, Option<String>)>,
}

/// A range coordinator's current files under a prefix; ranges it does not list come back
/// refused.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeFiles {
    pub files: Vec<ReleaseFile>,
    pub refused: Vec<Ulid>,
}

/// What another member of a distributed bucket is asked about its ranges.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HolderAsk {
    Members {
        range_ids: Vec<Ulid>,
        queries: Vec<MembershipQuery>,
    },
    Files {
        range_ids: Vec<Ulid>,
        prefix: String,
        skip: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HolderAnswer {
    Members(RangeMembers),
    Files(RangeFiles),
}

/// Reads one release crate version on this node with the caller's identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrateRead {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub group_id: GroupId,
    pub auth: AuthContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrateBytes {
    pub metadata: HashMap<String, String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrateReadError {
    /// The version is gone, for example purged or of an earlier bucket of the name.
    Missing,
    Failed(String),
}

/// Asks one holder with the caller's bearer, which the holder validates itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolderRequest {
    pub node_id: NodeId,
    pub bearer: String,
    pub bucket: String,
    pub ask: HolderAsk,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseEffect {
    ReadCrate(Box<CrateRead>),
    Holder(HolderRequest),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseEvent {
    CrateRead(Result<CrateBytes, CrateReadError>),
    Holder(Result<HolderAnswer, String>),
}
