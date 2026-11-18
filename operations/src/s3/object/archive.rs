//! Streams a selection of files and folders of this node's buckets into one ZIP, with a report
//! of the entries left out or renamed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::archive_path::safe_zip_path;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::jobs::{ByteStream, JobEffect, JobEvent, ZipFailure, ZipWork};
use aruna_core::operation::Operation;
use aruna_core::outbound::RangeEntry;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::ReadRoute;
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::structs::storage::group_backend::valid_bucket;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::guard::GuardError;
use crate::distribution::routing::route_read;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::list::ListedObject;
use crate::s3::object::member_read::{MemberCaller, MemberKey, MemberRead, MemberReadOperation};
use crate::s3::object::pages::{
    Caller, ListRequest, ObjectPage, ObjectPageOperation, PageError, PageNode, PageObject,
    listing_path,
};
use crate::s3::object::range_list::MemberAccess;
use crate::s3::object::read::GetRoutedOperation;

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
    /// Written from this node's copy of a range while its coordinator could not answer, so it
    /// may be behind.
    Stale,
}

/// A failed read counts as an entry that cannot be served now.
impl From<IoError> for EntryStatus {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
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
    pub stale: u64,
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
            EntryStatus::Stale => &mut totals.stale,
        };
        *counter += 1;
    }
}

/// A key a folder listing named.
#[derive(Debug, PartialEq)]
struct FolderKey(String);

impl From<&ListedObject> for FolderKey {
    fn from(object: &ListedObject) -> Self {
        Self(object.head.key.clone())
    }
}

impl From<RangeEntry> for FolderKey {
    fn from(entry: RangeEntry) -> Self {
        Self(entry.key)
    }
}

impl PageObject for FolderKey {
    fn key(&self) -> &str {
        &self.0
    }
}

/// A version pinned for writing: its bytes when another member already sent them, and the
/// range whose copy here answered for a coordinator that could not.
#[derive(Debug, PartialEq)]
struct Pinned {
    version: Ulid,
    blob: Option<ByteStream>,
    stale: Option<Ulid>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SelectionError {
    #[error("a selection names at least one file or folder")]
    Empty,
    #[error("`{0}` is not a bucket name")]
    Bucket(String),
    #[error("the archive stream failed: {0}")]
    Stream(String),
    #[error("the archive could not be written; its reader may have gone away")]
    Write,
    #[error(transparent)]
    Io(#[from] IoError),
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
#[derive(Debug, Default, PartialEq)]
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

/// The folders of one bucket, listed one after another a page at a time.
#[derive(Debug, Default, PartialEq)]
struct FolderWalk {
    folders: VecDeque<String>,
    current: Option<FolderListing>,
    buffer: VecDeque<String>,
}

#[derive(Debug, PartialEq)]
struct FolderListing {
    prefix: String,
    cursor: Option<String>,
    done: bool,
    /// Ranges already reported for this folder.
    ranges: HashSet<String>,
}

/// The key being written: its claims still to resolve and the versions already written.
#[derive(Debug, PartialEq)]
struct KeyWork {
    key: String,
    claims: VecDeque<Option<Ulid>>,
    written: Vec<Ulid>,
}

/// The bucket being written: its explicit files merged into its folder listings in key order.
#[derive(Debug, PartialEq)]
struct BucketWork {
    name: String,
    info: BucketInfo,
    files: VecDeque<(String, Option<Ulid>)>,
    /// Folders whose listing right is still to check.
    prefixes: VecDeque<String>,
    walk: FolderWalk,
    key: Option<KeyWork>,
}

/// A pinned version and where it goes in the archive.
#[derive(Debug, PartialEq)]
struct Placed {
    version: Ulid,
    path: String,
    renamed: bool,
    stale: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum ArchiveStep {
    Bucket(String, Vec<SelectionEntry>),
    Folder(String),
    Page,
    Resolve(Option<Ulid>),
    Read(Placed),
    Entry(Placed),
    Report,
    Close,
}

type ArchiveTurn = Result<Next<Report>, SelectionError>;

fn job_call(effect: JobEffect) -> IoCall {
    Effect::Job(effect).into()
}

fn job_event(result: IoResult) -> Result<JobEvent, IoError> {
    match result.event()? {
        Event::Job(event) => Ok(event),
        other => Err(IoError::Unexpected {
            expected: "Event::Job",
            got: other.label(),
        }),
    }
}

fn unexpected_job() -> IoError {
    IoError::Unexpected {
        expected: "the archive event of the running step",
        got: "another job event",
    }
}

/// The entry status of a failed permission check.
fn access_status(answer: Result<(), AuthorizeError>) -> Result<(), EntryStatus> {
    match answer {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(EntryStatus::Denied)
        }
        Err(AuthorizeError::Storage(_) | AuthorizeError::CheckFailed(_)) => {
            Err(EntryStatus::Unavailable)
        }
    }
}

fn read_check(realm_id: RealmId, auth: &AuthContext, path: &str) -> IoCall {
    let extras = PolicyRequestExtras::rest();
    let check = AuthorizeOperation::new(realm_id, auth, path, &Permission::READ, extras);
    IoCall::child(check)
}

/// Starts an archive: its writer for [`WriteSelectionOperation`] and the stream of its bytes.
#[derive(Debug, PartialEq)]
pub struct OpenArchiveOperation;

impl Operation for OpenArchiveOperation {
    type Output = (ZipWork, ByteStream);
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Call(job_call(JobEffect::OpenZip)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        match job_event(result)? {
            JobEvent::ZipOpened { zip, stream } => Ok(Next::Done((zip, stream))),
            _ => Err(unexpected_job()),
        }
    }
}

/// Writes every selected entry into `zip`, bucket by bucket in request order and key by key in
/// key order, then the report. Each key is resolved and its version pinned only when the writer
/// reaches it. A failure inside an entry ends the archive with an error, never a silent gap.
#[derive(Debug, PartialEq)]
pub struct WriteSelectionOperation {
    zip: Option<ZipWork>,
    node: PageNode,
    access: MemberAccess,
    caller: (AuthContext, u64),
    entries: Vec<SelectionEntry>,
    buckets: VecDeque<(String, Vec<SelectionEntry>)>,
    bucket: Option<BucketWork>,
    paths: WrittenPaths,
    report: Report,
    now_ms: u64,
    step: ArchiveStep,
}

impl WriteSelectionOperation {
    pub fn new(
        zip: ZipWork,
        node: PageNode,
        access: MemberAccess,
        (auth, expiry): Caller<'_>,
        entries: Vec<SelectionEntry>,
    ) -> Self {
        Self {
            zip: Some(zip),
            node,
            access,
            caller: (auth.clone(), expiry),
            entries,
            buckets: VecDeque::new(),
            bucket: None,
            paths: WrittenPaths::default(),
            report: Report::default(),
            now_ms: 0,
            step: ArchiveStep::Report,
        }
    }

    fn zip(&mut self) -> Result<ZipWork, IoError> {
        self.zip.take().ok_or(IoError::Unfinished)
    }

    fn work(&mut self) -> Result<&mut BucketWork, IoError> {
        self.bucket.as_mut().ok_or(IoError::Unfinished)
    }

    /// The bucket and key being written.
    fn names(&self) -> Result<(String, String), IoError> {
        let work = self.bucket.as_ref().ok_or(IoError::Unfinished)?;
        let key = work.key.as_ref().ok_or(IoError::Unfinished)?;
        Ok((work.name.clone(), key.key.clone()))
    }

    fn add_file(&mut self, version: Option<Ulid>, status: EntryStatus) -> Result<(), IoError> {
        let (bucket, key) = self.names()?;
        self.report.add(file_row(&bucket, &key, version, status));
        Ok(())
    }

    fn next_bucket(&mut self) -> ArchiveTurn {
        self.bucket = None;
        let Some((name, picks)) = self.buckets.pop_front() else {
            return self.write_report();
        };
        let get = GetBucketOperation::new(name.clone());
        self.step = ArchiveStep::Bucket(name, picks);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn bucket_read(
        &mut self,
        (name, picks): (String, Vec<SelectionEntry>),
        info: Result<BucketInfo, GetBucketError>,
    ) -> ArchiveTurn {
        let info = match info {
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
                        } => file_row(&name, &key, version_id, status),
                        SelectionEntry::Folder { prefix, .. } => folder_row(&name, &prefix, None),
                    };
                    row.status = status;
                    self.report.add(row);
                }
                return self.next_bucket();
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
        self.bucket = Some(BucketWork {
            name,
            info,
            files: files.into(),
            prefixes: prefixes.into(),
            walk: FolderWalk::default(),
            key: None,
        });
        self.next_folder()
    }

    /// Checks the next folder as the bucket's own listing requires.
    fn next_folder(&mut self) -> ArchiveTurn {
        let node = self.node;
        let work = self.work()?;
        let Some(prefix) = work.prefixes.pop_front() else {
            return self.advance();
        };
        let ids = (node.realm_id, work.info.group_id, node.node_id);
        let path = listing_path(ids, &work.name, Some(&prefix));
        let check = read_check(node.realm_id, &self.caller.0, &path);
        self.step = ArchiveStep::Folder(prefix);
        Ok(Next::Call(check))
    }

    /// A denied folder is never enumerated; one inside a listed folder adds nothing.
    fn folder_checked(&mut self, prefix: String, status: Result<(), EntryStatus>) -> ArchiveTurn {
        let work = self.work()?;
        if let Err(status) = status {
            let row = ReportRow {
                status,
                ..folder_row(&work.name, &prefix, None)
            };
            self.report.add(row);
            return self.next_folder();
        }
        let folders = &mut work.walk.folders;
        if !(folders.back()).is_some_and(|kept| prefix.starts_with(kept.as_str())) {
            folders.push_back(prefix);
        }
        self.next_folder()
    }

    /// Goes on with the next listed key or explicit file, listing a page when none is known.
    fn advance(&mut self) -> ArchiveTurn {
        loop {
            let walk = &mut self.work()?.walk;
            if let Some(key) = walk.buffer.front() {
                let key = key.clone();
                return self.take_key(Some(key));
            }
            if (walk.current.as_ref()).is_some_and(|listing| !listing.done) {
                return self.list_page();
            }
            let Some(prefix) = walk.folders.pop_front() else {
                return self.take_key(None);
            };
            walk.current = Some(FolderListing {
                prefix,
                cursor: None,
                done: false,
                ranges: HashSet::new(),
            });
        }
    }

    /// Writes each distinct version the claims on `key` resolve to, once.
    async fn write_key<R: MemberReader>(
        &mut self,
        scope: &ArchiveScope<'_, R>,
        auth: &AuthContext,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
        claims: Vec<Option<Ulid>>,
    ) -> Result<(), SelectionError> {
        let mut written = Vec::new();
        for requested in claims {
            let pinned = match resolve(scope, auth, (bucket, info), key, requested).await {
                Ok(pinned) => pinned,
                Err(status) => {
                    self.report.add(file_row(bucket, key, requested, status));
                    continue;
                }
            };
            let version = pinned.version;
            if written.contains(&version) {
                continue;
            }
            match self
                .write_object(scope, auth, (bucket, info), key, pinned)
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
    async fn write_object<R: MemberReader>(
        &mut self,
        scope: &ArchiveScope<'_, R>,
        auth: &AuthContext,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
        pinned: Pinned,
    ) -> Result<Result<(), EntryStatus>, SelectionError> {
        let Pinned {
            version,
            blob,
            stale,
        } = pinned;
        let (path, renamed) = self.paths.place(bucket, key, version);
        let input = GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version),
            range: None,
            group_id: info.group_id,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: scope.node.node_id,
        };
        let restrictions = auth.path_restrictions.clone();
        let blob = match blob {
            Some(blob) => blob,
            None => match drive(
                GetRoutedOperation::new(input, restrictions, None),
                scope.context,
            )
            .await
            {
                Ok(read) => read.blob,
                Err(error) => return Ok(Err(read_status(&error))),
            },
        };
        let size = write_entry(&mut self.zip, &path, version.timestamp_ms(), blob).await?;
        self.report.totals.bytes += size;
        if !renamed {
            self.paths.record(&path);
        }
        if let Some(range_id) = stale {
            self.report.add(ReportRow {
                range_id: Some(range_id.to_string()),
                path: renamed.then_some(path),
                ..file_row(bucket, key, Some(version), EntryStatus::Stale)
            });
        } else if renamed {
            self.report.add(ReportRow {
                path: Some(path),
                ..file_row(bucket, key, Some(version), EntryStatus::Renamed)
            });
        } else {
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
    async fn peek<R: MemberReader>(
        &mut self,
        scope: &ArchiveScope<'_, R>,
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
            let list = ObjectPageOperation::<FolderKey>::new(
                scope.node,
                scope.access.clone(),
                info,
                Some(caller),
                request,
            );
            match drive(list, scope.context).await {
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
async fn may_list<R: MemberReader>(
    scope: &ArchiveScope<'_, R>,
    auth: &AuthContext,
    (bucket, info): (&str, &BucketInfo),
    prefix: &str,
) -> Result<(), EntryStatus> {
    let ids = (scope.node.realm_id, info.group_id, scope.node.node_id);
    let path = listing_path(ids, bucket, Some(prefix));
    authorization(scope, auth, &path).await
}

async fn authorization<R>(
    scope: &ArchiveScope<'_, R>,
    auth: &AuthContext,
    path: &str,
) -> Result<(), EntryStatus> {
    let extras = PolicyRequestExtras::rest();
    let read = &Permission::READ;
    match drive(
        AuthorizeOperation::new(scope.node.realm_id, auth, path, read, extras),
        scope.context,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(EntryStatus::Denied)
        }
        Err(AuthorizeError::Storage(_) | AuthorizeError::CheckFailed(_)) => {
            Err(EntryStatus::Unavailable)
        }
    }
}

/// The exact version `requested` names for the caller, or the one current now. A distributed
/// bucket's key is read where its S3 GET would be, so a current version is the coordinator's.
async fn resolve<R: MemberReader>(
    scope: &ArchiveScope<'_, R>,
    auth: &AuthContext,
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    requested: Option<Ulid>,
) -> Result<Pinned, EntryStatus> {
    let (realm_id, node_id) = (scope.node.realm_id, scope.node.node_id);
    let path = object_permission_path(realm_id, info.group_id, node_id, bucket, key);
    // A denied caller learns nothing about whether the key exists.
    authorization(scope, auth, &path).await?;
    let (mut catching_up, mut stale) = (false, None);
    if let Some(binding) = info.distribution.as_ref() {
        let call = MemberCall {
            auth,
            bucket,
            info,
            key,
            version: requested,
        };
        match scope.reader.member_read(call).await? {
            MemberRead::Remote {
                version,
                blob,
                stale,
            } => {
                let blob = Some(blob);
                return Ok(Pinned {
                    version,
                    blob,
                    stale,
                });
            }
            MemberRead::Local(range_id) => stale = range_id,
        }
        // Without records of the key's range here, a miss would prove nothing.
        match drive(route_read(binding, key, false), scope.context).await {
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
    let version = match drive(head, scope.context).await {
        Ok(head) => (head.resolved_version_id.or(head.version_id)).ok_or(EntryStatus::Missing),
        Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) if !catching_up => Err(EntryStatus::Missing),
        Err(HeadObjectError::ConversionError(error)) if locked(&error) => Err(EntryStatus::Locked),
        Err(_) => Err(EntryStatus::Unavailable),
    }?;
    Ok(Pinned {
        version,
        blob: None,
        stale,
    })
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
