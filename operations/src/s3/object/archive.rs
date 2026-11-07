//! Streams an explicit selection of S3 object versions into one ZIP with a report of omissions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::group_backend::valid_bucket;
use aruna_core::types::GroupId;
use futures_util::StreamExt;
use futures_util::io::AsyncWriteExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWrite;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::jobs::export::{safe_zip_path, zip_entry};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{GetObjectError, GetObjectInput, get_object_routed};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};

pub const MAX_ENTRIES: usize = 1000;
/// Bucket names never contain `_`, so these paths cannot meet a `bucket/key` path.
pub const REPORT_PATH: &str = "_report.json";
const RENAMED_DIR: &str = "_renamed";

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectionEntry {
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub version_id: Option<Ulid>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStatus {
    Included,
    Denied,
    Missing,
    Locked,
    Unavailable,
}

/// One row per requested entry, in request order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportEntry {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub path: Option<String>,
    pub status: EntryStatus,
    pub size: Option<u64>,
    pub blake3: Option<String>,
}

#[derive(Debug, Error)]
pub enum SelectionError {
    #[error("a selection names between 1 and {MAX_ENTRIES} objects")]
    Size,
    #[error("`{0}` is not a bucket name")]
    Bucket(String),
    #[error("object `{0}` is selected twice")]
    Duplicate(String),
    #[error("the archive stream failed: {0}")]
    Stream(String),
    #[error("the archive reader went away")]
    Closed,
}

/// A bounded selection without repeated entries.
pub fn validate_selection(entries: &[SelectionEntry]) -> Result<(), SelectionError> {
    if entries.is_empty() || entries.len() > MAX_ENTRIES {
        return Err(SelectionError::Size);
    }
    let mut seen = HashSet::new();
    for entry in entries {
        if !valid_bucket(&entry.bucket) {
            return Err(SelectionError::Bucket(entry.bucket.clone()));
        }
        if !seen.insert(entry) {
            return Err(SelectionError::Duplicate(format!(
                "{}/{}",
                entry.bucket, entry.key
            )));
        }
    }
    Ok(())
}

/// Deterministic, collision-free paths for exact `(bucket, key, version)` identities.
/// A key keeps `bucket/key` when no other identity claims that path or a path below it;
/// otherwise it moves to `_renamed/bucket/version`. Repeated identities share one path.
pub fn plan_paths(entries: &[(&str, &str, Ulid)]) -> Vec<String> {
    let mut distinct = HashMap::new();
    for (bucket, key, version) in entries {
        let primary = safe_zip_path(key).map(|key| format!("{bucket}/{key}"));
        distinct.insert((*bucket, *key, *version), primary);
    }
    let mut claims: HashMap<&str, usize> = HashMap::new();
    let mut parents = HashSet::new();
    for path in distinct.values().flatten() {
        *claims.entry(path.as_str()).or_default() += 1;
        let mut parent = path.as_str();
        while let Some((head, _)) = parent.rsplit_once('/') {
            parents.insert(head);
            parent = head;
        }
    }
    entries
        .iter()
        .map(|identity| match &distinct[identity] {
            Some(path) if claims[path.as_str()] == 1 && !parents.contains(path.as_str()) => {
                path.clone()
            }
            _ => format!("{RENAMED_DIR}/{}/{}", identity.0, identity.2),
        })
        .collect()
}

struct Captured {
    group_id: GroupId,
    version_id: Ulid,
}

/// Captures the exact versions first, then streams each readable one under a fresh READ check.
/// A failure inside an entry ends the archive with an error instead of a silent gap.
pub async fn write_selection<W: AsyncWrite + Unpin>(
    context: &DriverContext,
    (auth, realm_id, node_id): (&AuthContext, RealmId, NodeId),
    entries: Vec<SelectionEntry>,
    writer: W,
) -> Result<Vec<ReportEntry>, SelectionError> {
    validate_selection(&entries)?;
    let scope = (auth, realm_id, node_id);
    let mut rows = Vec::with_capacity(entries.len());
    let mut captured = Vec::with_capacity(entries.len());
    for entry in &entries {
        let state = capture(context, scope, entry).await;
        rows.push(ReportEntry {
            bucket: entry.bucket.clone(),
            key: entry.key.clone(),
            version_id: state
                .as_ref()
                .ok()
                .map(|found| found.version_id)
                .or(entry.version_id)
                .map(|version| version.to_string()),
            path: None,
            status: state
                .as_ref()
                .err()
                .copied()
                .unwrap_or(EntryStatus::Included),
            size: None,
            blake3: None,
        });
        captured.push(state.ok());
    }
    let readable: Vec<_> = (0..entries.len())
        .filter_map(|index| captured[index].as_ref().map(|found| (index, found)))
        .collect();
    let identities: Vec<_> = readable
        .iter()
        .map(|(index, found)| {
            let entry = &entries[*index];
            (entry.bucket.as_str(), entry.key.as_str(), found.version_id)
        })
        .collect();
    let mut planned: Vec<_> = readable
        .iter()
        .zip(plan_paths(&identities))
        .map(|((index, found), path)| (path, *index, *found))
        .collect();
    planned.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

    let mut archive = async_zip::base::write::ZipFileWriter::with_tokio(writer);
    let mut written: HashMap<String, (u64, String)> = HashMap::new();
    for (path, index, found) in planned {
        if let Some((size, digest)) = written.get(&path) {
            (rows[index].size, rows[index].blake3) = (Some(*size), Some(digest.clone()));
            rows[index].path = Some(path);
            continue;
        }
        let entry = &entries[index];
        let opened = open_entry(context, scope, entry, found).await;
        let mut blob = match opened {
            Ok(blob) => blob,
            Err(status) => {
                rows[index].status = status;
                continue;
            }
        };
        let modified_ms = found.version_id.timestamp_ms();
        let mut stream = archive
            .write_entry_stream(zip_entry(&path, modified_ms))
            .await
            .map_err(|_| SelectionError::Closed)?;
        let (mut hasher, mut size) = (blake3::Hasher::new(), 0u64);
        while let Some(chunk) = blob.next().await {
            let chunk = chunk.map_err(|error| SelectionError::Stream(error.to_string()))?;
            hasher.update(&chunk);
            size += chunk.len() as u64;
            stream
                .write_all(&chunk)
                .await
                .map_err(|_| SelectionError::Closed)?;
        }
        stream.close().await.map_err(|_| SelectionError::Closed)?;
        let digest = hasher.finalize().to_hex().to_string();
        (rows[index].size, rows[index].blake3) = (Some(size), Some(digest.clone()));
        rows[index].path = Some(path.clone());
        written.insert(path, (size, digest));
    }
    let report = serde_json::to_vec_pretty(&serde_json::json!({ "entries": &rows }))
        .map_err(|error| SelectionError::Stream(error.to_string()))?;
    let now = aruna_core::time::unix_timestamp_millis();
    archive
        .write_entry_whole(zip_entry(REPORT_PATH, now), &report)
        .await
        .map_err(|_| SelectionError::Closed)?;
    archive.close().await.map_err(|_| SelectionError::Closed)?;
    Ok(rows)
}

async fn check_read(
    context: &DriverContext,
    (auth, realm_id, node_id): (&AuthContext, RealmId, NodeId),
    group_id: GroupId,
    entry: &SelectionEntry,
) -> Result<(), EntryStatus> {
    let path = object_permission_path(realm_id, group_id, node_id, &entry.bucket, &entry.key);
    let extras = PolicyRequestExtras::rest();
    match authorize(context, realm_id, auth, &path, &Permission::READ, extras).await {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(EntryStatus::Denied)
        }
        Err(AuthorizeError::Storage(_) | AuthorizeError::CheckFailed(_)) => {
            Err(EntryStatus::Unavailable)
        }
    }
}

async fn capture(
    context: &DriverContext,
    scope: (&AuthContext, RealmId, NodeId),
    entry: &SelectionEntry,
) -> Result<Captured, EntryStatus> {
    let bucket = match drive(GetBucketOperation::new(entry.bucket.clone()), context).await {
        Ok(bucket) => bucket,
        Err(GetBucketError::NotFound) => return Err(EntryStatus::Missing),
        Err(_) => return Err(EntryStatus::Unavailable),
    };
    // A denied caller learns nothing about whether the key exists.
    check_read(context, scope, bucket.group_id, entry).await?;
    let head = HeadObjectOperation::new(HeadObjectInput {
        bucket: entry.bucket.clone(),
        key: entry.key.clone(),
        version_id: entry.version_id,
    });
    match drive(head, context).await {
        Ok(head) => head
            .resolved_version_id
            .or(head.version_id)
            .map(|version_id| Captured {
                group_id: bucket.group_id,
                version_id,
            })
            .ok_or(EntryStatus::Missing),
        Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) => Err(EntryStatus::Missing),
        Err(_) => Err(EntryStatus::Unavailable),
    }
}

async fn open_entry(
    context: &DriverContext,
    scope: (&AuthContext, RealmId, NodeId),
    entry: &SelectionEntry,
    found: &Captured,
) -> Result<
    aruna_core::stream::BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>,
    EntryStatus,
> {
    check_read(context, scope, found.group_id, entry).await?;
    let (auth, _, node_id) = scope;
    let input = GetObjectInput {
        bucket: entry.bucket.clone(),
        key: entry.key.clone(),
        version_id: Some(found.version_id),
        range: None,
        group_id: found.group_id,
        user_identity: auth.user_id,
        realm_id: auth.realm_id,
        node_id,
    };
    match get_object_routed(context, input, auth.path_restrictions.clone()).await {
        Ok(read) => Ok(read.blob),
        Err(error) => Err(read_status(&error)),
    }
}

fn read_status(error: &GetObjectError) -> EntryStatus {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker => EntryStatus::Missing,
        GetObjectError::HolderAccessDenied | GetObjectError::GovernedUnavailable => {
            EntryStatus::Denied
        }
        GetObjectError::PendingContent(_) | GetObjectError::Abe(_) => EntryStatus::Locked,
        _ => EntryStatus::Unavailable,
    }
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
