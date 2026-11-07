//! Streams a selection of files and folders of this node's buckets into one ZIP, with a report
//! of the entries left out or renamed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::errors::ConversionError;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::ReadRoute;
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::structs::storage::group_backend::valid_bucket;
use async_zip::base::write::ZipFileWriter;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::io::{AsyncWrite, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::guard::route_read;
use crate::driver::drive;
use crate::jobs::export::{safe_zip_path, zip_entry};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{GetObjectError, GetObjectInput, get_object_routed};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use crate::s3::object::pages::{
    Caller, ListRequest, PageObject, PageScope, RangeRelay, listing_path, object_page,
};

/// Bucket names never contain `_`, so these paths cannot meet a `bucket/key` path.
pub const REPORT_PATH: &str = "_report.json";
const RENAMED_DIR: &str = "_renamed";
/// Objects one folder page lists; tests use small pages so a folder spans several.
#[cfg(not(test))]
const FOLDER_PAGE: usize = 1000;
#[cfg(test)]
const FOLDER_PAGE: usize = 2;

/// One selected file, pinned to `version_id` when given, or one folder of a bucket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectionEntry {
    File {
        bucket: String,
        key: String,
        version_id: Option<Ulid>,
    },
    Folder {
        bucket: String,
        prefix: String,
    },
}

impl SelectionEntry {
    fn bucket(&self) -> &str {
        match self {
            Self::File { bucket, .. } | Self::Folder { bucket, .. } => bucket,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStatus {
    Included,
    Renamed,
    Denied,
    Missing,
    Locked,
    Unavailable,
}

/// One entry that is not in the archive at its own path: a file, a folder, or a key range of a
/// folder's distributed bucket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportRow {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub status: EntryStatus,
}

/// Entries per status and the bytes written for them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    pub included: u64,
    pub renamed: u64,
    pub denied: u64,
    pub missing: u64,
    pub locked: u64,
    pub unavailable: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub totals: Totals,
    pub entries: Vec<ReportRow>,
}

impl Report {
    fn add(&mut self, row: ReportRow) {
        self.count(row.status);
        self.entries.push(row);
    }

    fn count(&mut self, status: EntryStatus) {
        let totals = &mut self.totals;
        let counter = match status {
            EntryStatus::Included => &mut totals.included,
            EntryStatus::Renamed => &mut totals.renamed,
            EntryStatus::Denied => &mut totals.denied,
            EntryStatus::Missing => &mut totals.missing,
            EntryStatus::Locked => &mut totals.locked,
            EntryStatus::Unavailable => &mut totals.unavailable,
        };
        *counter += 1;
    }
}

#[derive(Debug, Error)]
pub enum SelectionError {
    #[error("a selection names at least one file or folder")]
    Empty,
    #[error("`{0}` is not a bucket name")]
    Bucket(String),
    #[error("the archive stream failed: {0}")]
    Stream(String),
    #[error("the archive could not be written; its reader may have gone away")]
    Write,
}

/// A selection with at least one entry, each of a valid bucket name.
pub fn validate_selection(entries: &[SelectionEntry]) -> Result<(), SelectionError> {
    if entries.is_empty() {
        return Err(SelectionError::Empty);
    }
    match entries.iter().find(|entry| !valid_bucket(entry.bucket())) {
        Some(entry) => Err(SelectionError::Bucket(entry.bucket().to_string())),
        None => Ok(()),
    }
}

/// The archive paths written so far and their folders, so every new path stays apart from them.
#[derive(Default)]
pub struct WrittenPaths {
    files: HashSet<String>,
    folders: HashSet<String>,
}

impl WrittenPaths {
    /// `bucket/key` when it is safe and apart from every written path, else the renamed path;
    /// `true` marks a renamed one.
    pub fn place(&self, bucket: &str, key: &str, version: Ulid) -> (String, bool) {
        let primary = safe_zip_path(key).map(|key| format!("{bucket}/{key}"));
        match primary.filter(|path| self.fits(path)) {
            Some(path) => (path, false),
            None => (format!("{RENAMED_DIR}/{bucket}/{version}"), true),
        }
    }

    /// Keeps a written primary path and its folders.
    pub fn record(&mut self, path: &str) {
        for folder in folders(path) {
            self.folders.insert(folder.to_string());
        }
        self.files.insert(path.to_string());
    }

    fn fits(&self, path: &str) -> bool {
        !self.files.contains(path)
            && !self.folders.contains(path)
            && folders(path).all(|folder| !self.files.contains(folder))
    }
}

/// Every folder above `path`, nearest first.
fn folders(path: &str) -> impl Iterator<Item = &str> {
    std::iter::successors(path.rsplit_once('/').map(|(head, _)| head), |folder| {
        folder.rsplit_once('/').map(|(head, _)| head)
    })
}

/// Writes every selected entry, bucket by bucket in request order and key by key in key order,
/// then the report. Each key is resolved and its version pinned only when the writer reaches it.
/// A failure inside an entry ends the archive with an error instead of a silent gap.
pub async fn write_selection<R: RangeRelay, W: tokio::io::AsyncWrite + Unpin>(
    scope: &PageScope<'_, R>,
    caller: Caller<'_>,
    entries: Vec<SelectionEntry>,
    writer: W,
) -> Result<Report, SelectionError> {
    validate_selection(&entries)?;
    let mut archive = Archive {
        zip: ZipFileWriter::with_tokio(writer),
        paths: WrittenPaths::default(),
        report: Report::default(),
    };
    for (bucket, picks) in by_bucket(entries) {
        archive.write_bucket(scope, caller, &bucket, picks).await?;
    }
    let report = serde_json::to_vec_pretty(&archive.report)
        .map_err(|error| SelectionError::Stream(error.to_string()))?;
    let now = aruna_core::time::unix_timestamp_millis();
    let mut zip = archive.zip;
    zip.write_entry_whole(zip_entry(REPORT_PATH, now), &report)
        .await
        .map_err(|_| SelectionError::Write)?;
    zip.close().await.map_err(|_| SelectionError::Write)?;
    Ok(archive.report)
}

/// The entries of each bucket, buckets in the order the request first names them.
fn by_bucket(entries: Vec<SelectionEntry>) -> Vec<(String, Vec<SelectionEntry>)> {
    let mut order: Vec<(String, Vec<SelectionEntry>)> = Vec::new();
    let mut index = HashMap::new();
    for entry in entries {
        let bucket = entry.bucket().to_string();
        let at = *index.entry(bucket.clone()).or_insert_with(|| {
            order.push((bucket, Vec::new()));
            order.len() - 1
        });
        order[at].1.push(entry);
    }
    order
}

struct Archive<W> {
    zip: ZipFileWriter<W>,
    paths: WrittenPaths,
    report: Report,
}

fn file_row(bucket: &str, key: &str, version: Option<Ulid>, status: EntryStatus) -> ReportRow {
    ReportRow {
        bucket: bucket.to_string(),
        key: Some(key.to_string()),
        prefix: None,
        version_id: version.map(|version| version.to_string()),
        range_id: None,
        path: None,
        status,
    }
}

fn folder_row(bucket: &str, prefix: &str, range_id: Option<String>) -> ReportRow {
    ReportRow {
        bucket: bucket.to_string(),
        key: None,
        prefix: Some(prefix.to_string()),
        version_id: None,
        range_id,
        path: None,
        status: EntryStatus::Unavailable,
    }
}

impl<W: AsyncWrite + Unpin> Archive<W> {
    /// Merges the bucket's explicit files into its folder listings in key order.
    async fn write_bucket<R: RangeRelay>(
        &mut self,
        scope: &PageScope<'_, R>,
        caller: Caller<'_>,
        bucket: &str,
        picks: Vec<SelectionEntry>,
    ) -> Result<(), SelectionError> {
        let info = match drive(GetBucketOperation::new(bucket.to_string()), scope.context).await {
            Ok(info) => info,
            Err(error) => {
                let status = match error {
                    GetBucketError::NotFound => EntryStatus::Missing,
                    _ => EntryStatus::Unavailable,
                };
                for pick in picks {
                    let mut row = match pick {
                        SelectionEntry::File {
                            key, version_id, ..
                        } => file_row(bucket, &key, version_id, status),
                        SelectionEntry::Folder { prefix, .. } => folder_row(bucket, &prefix, None),
                    };
                    row.status = status;
                    self.report.add(row);
                }
                return Ok(());
            }
        };
        let (mut files, mut prefixes) = (Vec::new(), Vec::new());
        for pick in picks {
            match pick {
                SelectionEntry::File {
                    key, version_id, ..
                } => files.push((key, version_id)),
                SelectionEntry::Folder { prefix, .. } => prefixes.push(prefix),
            }
        }
        files.sort();
        files.dedup();
        prefixes.sort();
        prefixes.dedup();
        let mut walk = FolderWalk::default();
        for prefix in prefixes {
            // A denied folder is never enumerated; one inside a listed folder adds nothing.
            if let Err(status) = may_list(scope, caller.0, (bucket, &info), &prefix).await {
                self.report.add(ReportRow {
                    status,
                    ..folder_row(bucket, &prefix, None)
                });
                continue;
            }
            if !walk
                .folders
                .back()
                .is_some_and(|kept| prefix.starts_with(kept.as_str()))
            {
                walk.folders.push_back(prefix);
            }
        }
        let mut files = files.into_iter().peekable();
        loop {
            let listed = walk
                .peek(scope, caller, (bucket, &info), &mut self.report)
                .await;
            let key = match (listed, files.peek()) {
                (None, None) => return Ok(()),
                (Some(listed), Some((file, _))) => listed.min(file.clone()),
                (Some(listed), None) => listed,
                (None, Some((file, _))) => file.clone(),
            };
            let mut claims = Vec::new();
            if walk.buffer.front() == Some(&key) {
                walk.buffer.pop_front();
                claims.push(None);
            }
            while let Some((_, version)) = files.next_if(|(file, _)| *file == key) {
                claims.push(version);
            }
            claims.sort();
            claims.dedup();
            self.write_key(scope, caller.0, (bucket, &info), &key, claims)
                .await?;
        }
    }

    /// Writes each distinct version the claims on `key` resolve to, once.
    async fn write_key<R: RangeRelay>(
        &mut self,
        scope: &PageScope<'_, R>,
        auth: &AuthContext,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
        claims: Vec<Option<Ulid>>,
    ) -> Result<(), SelectionError> {
        let mut written = Vec::new();
        for requested in claims {
            let version = match resolve(scope, auth, (bucket, info), key, requested).await {
                Ok(version) => version,
                Err(status) => {
                    self.report.add(file_row(bucket, key, requested, status));
                    continue;
                }
            };
            if written.contains(&version) {
                continue;
            }
            match self
                .write_object(scope, auth, (bucket, info), key, version)
                .await?
            {
                Ok(()) => written.push(version),
                Err(status) => self
                    .report
                    .add(file_row(bucket, key, Some(version), status)),
            }
        }
        Ok(())
    }

    /// Streams one pinned version into the archive; a version that cannot be opened is
    /// reported by its status and takes no path.
    async fn write_object<R: RangeRelay>(
        &mut self,
        scope: &PageScope<'_, R>,
        auth: &AuthContext,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
        version: Ulid,
    ) -> Result<Result<(), EntryStatus>, SelectionError> {
        let (path, renamed) = self.paths.place(bucket, key, version);
        let input = GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version),
            range: None,
            group_id: info.group_id,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: scope.node_id,
        };
        let restrictions = auth.path_restrictions.clone();
        let read = match get_object_routed(scope.context, input, restrictions).await {
            Ok(read) => read,
            Err(error) => return Ok(Err(read_status(&error))),
        };
        let size = write_entry(&mut self.zip, &path, version.timestamp_ms(), read.blob).await?;
        self.report.totals.bytes += size;
        if renamed {
            self.report.add(ReportRow {
                path: Some(path),
                ..file_row(bucket, key, Some(version), EntryStatus::Renamed)
            });
        } else {
            self.paths.record(&path);
            self.report.count(EntryStatus::Included);
        }
        Ok(Ok(()))
    }
}

/// Streams `blob` into one stored archive entry and returns its size.
async fn write_entry<W: AsyncWrite + Unpin>(
    zip: &mut ZipFileWriter<W>,
    path: &str,
    modified_ms: u64,
    mut blob: BackendStream<Result<Bytes, StreamError>>,
) -> Result<u64, SelectionError> {
    let mut stream = zip
        .write_entry_stream(zip_entry(path, modified_ms))
        .await
        .map_err(|_| SelectionError::Write)?;
    let mut size = 0u64;
    while let Some(chunk) = blob.next().await {
        let chunk = chunk.map_err(|error| SelectionError::Stream(error.to_string()))?;
        size += chunk.len() as u64;
        stream
            .write_all(&chunk)
            .await
            .map_err(|_| SelectionError::Write)?;
    }
    stream.close().await.map_err(|_| SelectionError::Write)?;
    Ok(size)
}

/// The folders of one bucket, listed one after another a page at a time.
#[derive(Default)]
struct FolderWalk {
    folders: VecDeque<String>,
    current: Option<FolderListing>,
    buffer: VecDeque<String>,
}

struct FolderListing {
    prefix: String,
    cursor: Option<String>,
    done: bool,
    /// Ranges already reported for this folder.
    ranges: HashSet<String>,
}

impl FolderWalk {
    /// The next listed key; ranges a page could not list from current records are reported.
    async fn peek<R: RangeRelay>(
        &mut self,
        scope: &PageScope<'_, R>,
        caller: Caller<'_>,
        (bucket, info): (&str, &BucketInfo),
        report: &mut Report,
    ) -> Option<String> {
        loop {
            if let Some(key) = self.buffer.front() {
                return Some(key.clone());
            }
            let listing = match self.current.as_mut() {
                Some(listing) if !listing.done => listing,
                _ => {
                    let prefix = self.folders.pop_front()?;
                    self.current = Some(FolderListing {
                        prefix,
                        cursor: None,
                        done: false,
                        ranges: HashSet::new(),
                    });
                    continue;
                }
            };
            let request = ListRequest {
                bucket: bucket.to_string(),
                prefix: Some(listing.prefix.clone()),
                delimiter: None,
                cursor: listing.cursor.take(),
                limit: FOLDER_PAGE,
                partial: true,
            };
            match object_page(scope, info, Some(caller), request).await {
                Ok(page) => {
                    let ranges = page.stale_ranges.into_iter().chain(page.unavailable_ranges);
                    for range in ranges {
                        if listing.ranges.insert(range.clone()) {
                            report.add(folder_row(bucket, &listing.prefix, Some(range)));
                        }
                    }
                    let keys = page.objects.iter().map(|object| object.key().to_string());
                    self.buffer.extend(keys);
                    listing.done = page.next_cursor.is_none();
                    listing.cursor = page.next_cursor;
                }
                // The rest of the folder cannot be listed, so the folder is reported once.
                Err(_) => {
                    report.add(folder_row(bucket, &listing.prefix, None));
                    listing.done = true;
                }
            }
        }
    }
}

/// Whether the caller may list `prefix`, as the bucket's own listing requires.
async fn may_list<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    auth: &AuthContext,
    (bucket, info): (&str, &BucketInfo),
    prefix: &str,
) -> Result<(), EntryStatus> {
    let ids = (scope.realm_id, info.group_id, scope.node_id);
    let path = listing_path(ids, bucket, Some(prefix));
    authorization(scope, auth, &path).await
}

async fn authorization<R>(
    scope: &PageScope<'_, R>,
    auth: &AuthContext,
    path: &str,
) -> Result<(), EntryStatus> {
    let extras = PolicyRequestExtras::rest();
    let read = &Permission::READ;
    match authorize(scope.context, scope.realm_id, auth, path, read, extras).await {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(EntryStatus::Denied)
        }
        Err(AuthorizeError::Storage(_) | AuthorizeError::CheckFailed(_)) => {
            Err(EntryStatus::Unavailable)
        }
    }
}

/// The exact version `requested` names for the caller, or the one current now.
async fn resolve<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    auth: &AuthContext,
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    requested: Option<Ulid>,
) -> Result<Ulid, EntryStatus> {
    let (realm_id, node_id) = (scope.realm_id, scope.node_id);
    let path = object_permission_path(realm_id, info.group_id, node_id, bucket, key);
    // A denied caller learns nothing about whether the key exists.
    authorization(scope, auth, &path).await?;
    // Without records of the key's range here, a miss would prove nothing.
    let mut catching_up = false;
    if let Some(binding) = info.distribution.as_ref() {
        match route_read(scope.context, binding, key, false).await {
            Ok(ReadRoute::Local {
                catching_up: behind,
            }) => catching_up = behind,
            Ok(ReadRoute::Holder { .. }) | Err(_) => return Err(EntryStatus::Unavailable),
        }
    }
    let head = HeadObjectOperation::new(HeadObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: requested,
    });
    match drive(head, scope.context).await {
        Ok(head) => (head.resolved_version_id.or(head.version_id)).ok_or(EntryStatus::Missing),
        Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) if !catching_up => Err(EntryStatus::Missing),
        Err(HeadObjectError::ConversionError(error)) if locked(&error) => Err(EntryStatus::Locked),
        Err(_) => Err(EntryStatus::Unavailable),
    }
}

fn locked(error: &ConversionError) -> bool {
    matches!(error, ConversionError::BucketKey(BucketKeyError::Locked(_)))
}

/// A reference version that cannot prove its recorded bytes is unavailable, never read anew.
fn read_status(error: &GetObjectError) -> EntryStatus {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker => EntryStatus::Missing,
        GetObjectError::HolderAccessDenied | GetObjectError::GovernedUnavailable => {
            EntryStatus::Denied
        }
        GetObjectError::PendingContent(_) | GetObjectError::Abe(_) => EntryStatus::Locked,
        GetObjectError::ConversionError(error) if locked(error) => EntryStatus::Locked,
        _ => EntryStatus::Unavailable,
    }
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
