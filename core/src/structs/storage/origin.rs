//! Records where a file version came from, set once when the version is minted and never
//! changed, and the hop record a sync adds to its destination. Both are server-authored and
//! bounded; they never live in the user metadata map.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::id::NodeId;
use crate::structs::storage::global_id::GlobalId;
use crate::structs::storage::replication::VersionedObjectArn;

/// A device name in an upload origin is at most this long.
pub const MAX_DEVICE_BYTES: usize = 255;
/// A storage key in an import origin is at most one S3 key long.
pub const MAX_ORIGIN_KEY_BYTES: usize = 1024;

/// One exact object version elsewhere: the global id of a distributed bucket's version, or
/// the versioned node ARN of an ordinary bucket's version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OriginVersion {
    Global(GlobalId),
    Node(VersionedObjectArn),
}

impl OriginVersion {
    /// The identifier a dataset names the version by.
    pub fn to_id(&self) -> String {
        match self {
            Self::Global(global) => global.to_url(),
            Self::Node(arn) => arn.to_w3id(),
        }
    }
}

/// How a version's content got into its bucket.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VersionOrigin {
    /// A client wrote the bytes; the uploader and time are the version's creator and time.
    Upload {
        node_id: NodeId,
        /// The authenticated device the file came from, when known.
        device: Option<String>,
    },
    /// An external source or a database, registered without copying or copied in. The source
    /// root, path and observation are the version's own source binding, never copied here.
    Source {
        /// The database list entry of the bucket, when the bucket was listed at registration.
        database: Option<Ulid>,
    },
    /// CopyObject or a snapshot copy minted this version from another one.
    Copy { from: OriginVersion },
    /// Registered in place from a storage backend folder of the group.
    Storage { backend_id: Ulid, key: String },
    /// A compute job wrote the version as a result.
    Job { run_id: Ulid },
}

impl VersionOrigin {
    /// Whether the record stays within its size budget.
    pub fn bounded(&self) -> bool {
        match self {
            Self::Upload { device, .. } => device
                .as_ref()
                .is_none_or(|device| device.len() <= MAX_DEVICE_BYTES),
            Self::Storage { key, .. } => key.len() <= MAX_ORIGIN_KEY_BYTES,
            Self::Copy {
                from: OriginVersion::Node(arn),
            } => arn.key.len() <= MAX_ORIGIN_KEY_BYTES,
            Self::Source { .. } | Self::Copy { .. } | Self::Job { .. } => true,
        }
    }
}

/// The last sync that carried a version into this bucket: separate from its origin, which the
/// destination keeps unchanged.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncHop {
    pub relationship_id: Ulid,
    /// The exact source version the sync delivered.
    pub from: OriginVersion,
}

/// What a dataset may say about a file's origin, read from the same pinned version. Uploads
/// and imports in place give nothing: users, devices and storage paths stay private.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OriginProjection {
    /// `isBasedOn` an identifier or URL.
    BasedOn { id: String },
    /// The run crate convention: a `CreateAction` of the run with this file as `result`.
    CreatedBy { run_id: Ulid },
}

/// The publication-safe projection of an origin and hop. `source_url` is the public URL of
/// the version's external source, when it has one.
pub fn project(
    origin: Option<&VersionOrigin>,
    hop: Option<&SyncHop>,
    source_url: Option<String>,
) -> Vec<OriginProjection> {
    let mut projection = Vec::new();
    match origin {
        Some(VersionOrigin::Source { .. }) => {
            projection.extend(source_url.map(|id| OriginProjection::BasedOn { id }));
        }
        Some(VersionOrigin::Copy { from }) => {
            projection.push(OriginProjection::BasedOn { id: from.to_id() })
        }
        Some(VersionOrigin::Job { run_id }) => {
            projection.push(OriginProjection::CreatedBy { run_id: *run_id })
        }
        Some(VersionOrigin::Upload { .. } | VersionOrigin::Storage { .. }) | None => {}
    }
    if let Some(hop) = hop {
        let id = hop.from.to_id();
        if !projection.contains(&OriginProjection::BasedOn { id: id.clone() }) {
            projection.push(OriginProjection::BasedOn { id });
        }
    }
    projection
}

#[cfg(test)]
#[path = "origin_tests.rs"]
mod tests;
