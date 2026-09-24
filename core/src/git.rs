//! Native repository bindings, LFS identities and Git adapter requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub const REPOSITORIES: &str = "git_repositories";
pub const LFS_OBJECTS: &str = "git_lfs_objects";
pub const STATUS: &str = "git_status";
pub const MAX_GIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepository {
    pub document_id: Ulid,
    pub group_id: Ulid,
    pub bucket: String,
    pub arc: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LfsObject {
    pub oid: String,
    pub size: u64,
}

impl LfsObject {
    pub fn valid(&self) -> bool {
        self.oid.len() == 64
            && self
                .oid
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LfsVersion {
    pub object: LfsObject,
    pub version_id: Ulid,
}

pub struct GitRequest {
    pub repository: GitRepository,
    pub method: String,
    pub action: String,
    pub query: String,
    pub content_type: String,
    pub content_encoding: String,
    pub protocol: String,
    pub body: Bytes,
    pub token: String,
    pub lfs_url: String,
    pub metadata_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitSnapshot {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub occurred_at_ms: u64,
    pub jsonld: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitStatus {
    pub event_id: Ulid,
    pub commit: Option<String>,
    pub error: Option<String>,
}

pub enum GitEffect {
    Initialize(Ulid),
    Snapshot(GitSnapshot),
    Export { document_id: Ulid, revision: String },
    Http(Box<GitRequest>),
}

pub enum GitEvent {
    Initialized,
    Snapshot(GitStatus),
    Exported(Bytes),
    Response {
        status: u16,
        headers: Vec<(String, String)>,
        body: Bytes,
    },
}
