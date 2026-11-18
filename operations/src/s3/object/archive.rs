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
/// Objects one folder page lists.
const FOLDER_PAGE: usize = 1000;

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
    let event = result.event()?;
    let got = event.label();
    let Event::Job(event) = event else {
        let expected = "Event::Job";
        return Err(IoError::Unexpected { expected, got });
    };
    Ok(event)
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
    folder_page: usize,
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
            folder_page: FOLDER_PAGE,
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

    fn list_page(&mut self) -> ArchiveTurn {
        let work = self.bucket.as_mut().ok_or(IoError::Unfinished)?;
        let listing = work.walk.current.as_mut().ok_or(IoError::Unfinished)?;
        let request = ListRequest {
            bucket: work.name.clone(),
            prefix: Some(listing.prefix.clone()),
            delimiter: None,
            cursor: listing.cursor.take(),
            limit: self.folder_page,
            partial: true,
        };
        let list = ObjectPageOperation::<FolderKey>::new(
            self.node,
            self.access.clone(),
            &work.info,
            Some((&self.caller.0, self.caller.1)),
            request,
        );
        self.step = ArchiveStep::Page;
        Ok(Next::Call(IoCall::child(list)))
    }

    /// Ranges a page could not list from current records are reported; a folder whose rest
    /// cannot be listed is reported once.
    fn page_read(&mut self, page: Result<ObjectPage<FolderKey>, PageError>) -> ArchiveTurn {
        let work = self.bucket.as_mut().ok_or(IoError::Unfinished)?;
        let listing = work.walk.current.as_mut().ok_or(IoError::Unfinished)?;
        match page {
            Ok(page) => {
                let ranges = page.stale_ranges.into_iter().chain(page.unavailable_ranges);
                for range in ranges {
                    if listing.ranges.insert(range.clone()) {
                        let row = folder_row(&work.name, &listing.prefix, Some(range));
                        self.report.add(row);
                    }
                }
                let keys = page.objects.iter().map(|object| object.key().to_string());
                work.walk.buffer.extend(keys);
                listing.done = page.next_cursor.is_none();
                listing.cursor = page.next_cursor;
            }
            Err(_) => {
                self.report
                    .add(folder_row(&work.name, &listing.prefix, None));
                listing.done = true;
            }
        }
        self.advance()
    }

    /// Starts the lowest of the listed key and the next explicit file, with all claims on it.
    fn take_key(&mut self, listed: Option<String>) -> ArchiveTurn {
        let work = self.work()?;
        let file = work.files.front().map(|(file, _)| file.clone());
        let Some(key) = [listed, file].into_iter().flatten().min() else {
            return self.next_bucket();
        };
        let mut claims = Vec::new();
        if work.walk.buffer.front() == Some(&key) {
            work.walk.buffer.pop_front();
            claims.push(None);
        }
        while work.files.front().is_some_and(|(file, _)| *file == key) {
            claims.extend(work.files.pop_front().map(|(_, version)| version));
        }
        claims.sort();
        claims.dedup();
        work.key = Some(KeyWork {
            key,
            claims: claims.into(),
            written: Vec::new(),
        });
        self.next_claim()
    }

    fn next_claim(&mut self) -> ArchiveTurn {
        let work = self.bucket.as_mut().ok_or(IoError::Unfinished)?;
        let claim = work.key.as_mut().ok_or(IoError::Unfinished)?;
        let Some(requested) = claim.claims.pop_front() else {
            work.key = None;
            return self.advance();
        };
        let resolve = ResolveKeyOperation::new(
            self.node,
            (&self.caller.0, self.caller.1),
            (&work.name, &work.info),
            &claim.key,
            requested,
        );
        self.step = ArchiveStep::Resolve(requested);
        Ok(Next::Call(IoCall::child(resolve)))
    }

    /// Each distinct version the claims resolve to is written once.
    fn claim_resolved(
        &mut self,
        requested: Option<Ulid>,
        pinned: Result<Pinned, EntryStatus>,
    ) -> ArchiveTurn {
        let pinned = match pinned {
            Ok(pinned) => pinned,
            Err(status) => {
                self.add_file(requested, status)?;
                return self.next_claim();
            }
        };
        let work = self.work()?;
        let claim = work.key.as_ref().ok_or(IoError::Unfinished)?;
        if claim.written.contains(&pinned.version) {
            return self.next_claim();
        }
        let Pinned {
            version,
            blob,
            stale,
        } = pinned;
        let (bucket, key) = self.names()?;
        let (path, renamed) = self.paths.place(&bucket, &key, version);
        let placed = Placed {
            version,
            path,
            renamed,
            stale,
        };
        match blob {
            Some(blob) => self.write_entry(placed, blob),
            None => self.read_here(placed, bucket, key),
        }
    }

    fn read_here(&mut self, placed: Placed, bucket: String, key: String) -> ArchiveTurn {
        let group_id = self.work()?.info.group_id;
        let auth = &self.caller.0;
        let input = GetObjectInput {
            bucket,
            key,
            version_id: Some(placed.version),
            range: None,
            group_id,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: self.node.node_id,
        };
        let read = GetRoutedOperation::new(input, auth.path_restrictions.clone(), None);
        self.step = ArchiveStep::Read(placed);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Streams one pinned version into the archive.
    fn write_entry(&mut self, placed: Placed, stream: ByteStream) -> ArchiveTurn {
        let effect = JobEffect::ZipCounted {
            zip: self.zip()?,
            path: placed.path.clone(),
            modified_ms: placed.version.timestamp_ms(),
            stream,
        };
        self.step = ArchiveStep::Entry(placed);
        Ok(Next::Call(job_call(effect)))
    }

    fn entry_written(&mut self, placed: Placed, size: u64) -> ArchiveTurn {
        let Placed {
            version,
            path,
            renamed,
            stale,
        } = placed;
        let (bucket, key) = self.names()?;
        self.report.totals.bytes += size;
        if !renamed {
            self.paths.record(&path);
        }
        let row = file_row(&bucket, &key, Some(version), EntryStatus::Stale);
        if let Some(range_id) = stale {
            self.report.add(ReportRow {
                range_id: Some(range_id.to_string()),
                path: renamed.then_some(path),
                ..row
            });
        } else if renamed {
            self.report.add(ReportRow {
                path: Some(path),
                status: EntryStatus::Renamed,
                ..row
            });
        } else {
            self.report.count(EntryStatus::Included);
        }
        let claim = self.work()?.key.as_mut().ok_or(IoError::Unfinished)?;
        claim.written.push(version);
        self.next_claim()
    }

    fn write_report(&mut self) -> ArchiveTurn {
        let bytes = serde_json::to_vec_pretty(&self.report)
            .map_err(|error| SelectionError::Stream(error.to_string()))?;
        let effect = JobEffect::ZipWhole {
            zip: self.zip()?,
            path: REPORT_PATH.to_string(),
            modified_ms: self.now_ms,
            bytes,
        };
        self.step = ArchiveStep::Report;
        Ok(Next::Call(job_call(effect)))
    }
}

impl Operation for WriteSelectionOperation {
    type Output = Report;
    type Error = SelectionError;

    fn first(&mut self) -> ArchiveTurn {
        validate_selection(&self.entries)?;
        self.buckets = by_bucket(std::mem::take(&mut self.entries)).into();
        self.next_bucket()
    }

    fn next(&mut self, result: IoResult) -> ArchiveTurn {
        match std::mem::replace(&mut self.step, ArchiveStep::Close) {
            ArchiveStep::Bucket(name, picks) => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                self.bucket_read((name, picks), info)
            }
            ArchiveStep::Folder(prefix) => {
                let answer = result.child::<(), AuthorizeError>()?;
                self.folder_checked(prefix, access_status(answer))
            }
            ArchiveStep::Page => {
                let page = result.child::<ObjectPage<FolderKey>, PageError>()?;
                self.page_read(page)
            }
            ArchiveStep::Resolve(requested) => {
                let pinned = result.child::<Pinned, EntryStatus>()?;
                self.claim_resolved(requested, pinned)
            }
            ArchiveStep::Read(placed) => match result.child::<GetObjectResult, GetObjectError>()? {
                Ok(read) => self.write_entry(placed, read.blob),
                // A version that cannot be opened is reported and takes no path.
                Err(error) => {
                    self.add_file(Some(placed.version), read_status(&error))?;
                    self.next_claim()
                }
            },
            ArchiveStep::Entry(placed) => {
                let JobEvent::ZipCounted { zip, result } = job_event(result)? else {
                    return Err(unexpected_job().into());
                };
                self.zip = zip;
                match result {
                    Ok(size) => self.entry_written(placed, size),
                    Err(ZipFailure::Source { message, .. }) => Err(SelectionError::Stream(message)),
                    Err(ZipFailure::Sink(_) | ZipFailure::Stopped) => Err(SelectionError::Write),
                }
            }
            ArchiveStep::Report => {
                let JobEvent::ZipWritten { zip, result } = job_event(result)? else {
                    return Err(unexpected_job().into());
                };
                self.zip = zip;
                result.map_err(|_| SelectionError::Write)?;
                self.step = ArchiveStep::Close;
                Ok(Next::Call(job_call(JobEffect::ZipClose(self.zip()?))))
            }
            ArchiveStep::Close => {
                let JobEvent::ZipClosed(closed) = job_event(result)? else {
                    return Err(unexpected_job().into());
                };
                closed.map_err(|_| SelectionError::Write)?;
                Ok(Next::Done(std::mem::take(&mut self.report)))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[derive(Debug, PartialEq)]
enum ResolveStep {
    Access,
    Member,
    Route,
    Head,
}

type ResolveTurn = Result<Next<Pinned>, EntryStatus>;

/// The exact version `requested` names for the caller, or the one current now. A distributed
/// bucket's key is read where its S3 GET would be, so a current version is the coordinator's.
#[derive(Debug, PartialEq)]
struct ResolveKeyOperation {
    node: PageNode,
    auth: AuthContext,
    expiry: u64,
    bucket: String,
    info: BucketInfo,
    key: String,
    requested: Option<Ulid>,
    catching_up: bool,
    stale: Option<Ulid>,
    step: ResolveStep,
}

impl ResolveKeyOperation {
    fn new(
        node: PageNode,
        (auth, expiry): Caller<'_>,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
        requested: Option<Ulid>,
    ) -> Self {
        Self {
            node,
            auth: auth.clone(),
            expiry,
            bucket: bucket.to_string(),
            info: info.clone(),
            key: key.to_string(),
            requested,
            catching_up: false,
            stale: None,
            step: ResolveStep::Access,
        }
    }

    fn head(&mut self) -> ResolveTurn {
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            version_id: self.requested,
        });
        self.step = ResolveStep::Head;
        Ok(Next::Call(IoCall::child(head)))
    }

    fn member_read(&mut self) -> ResolveTurn {
        let Some(binding) = self.info.distribution.clone() else {
            return self.head();
        };
        let caller = MemberCaller {
            auth: self.auth.clone(),
            group_id: self.info.group_id,
            credential_expiry: self.expiry,
        };
        let object = MemberKey {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            version: self.requested,
        };
        let read = MemberReadOperation::new(self.node.ids, caller, binding, object);
        self.step = ResolveStep::Member;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn pinned(&self, head: Result<HeadObjectResult, HeadObjectError>) -> ResolveTurn {
        let version = match head {
            Ok(head) => (head.resolved_version_id.or(head.version_id)).ok_or(EntryStatus::Missing),
            Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) if !self.catching_up => Err(EntryStatus::Missing),
            Err(HeadObjectError::ConversionError(error)) if locked(&error) => {
                Err(EntryStatus::Locked)
            }
            Err(_) => Err(EntryStatus::Unavailable),
        }?;
        Ok(Next::Done(Pinned {
            version,
            blob: None,
            stale: self.stale,
        }))
    }
}

impl Operation for ResolveKeyOperation {
    type Output = Pinned;
    type Error = EntryStatus;

    /// A denied caller learns nothing about whether the key exists.
    fn first(&mut self) -> ResolveTurn {
        let (realm_id, node_id) = (self.node.realm_id, self.node.node_id);
        let group_id = self.info.group_id;
        let path = object_permission_path(realm_id, group_id, node_id, &self.bucket, &self.key);
        Ok(Next::Call(read_check(realm_id, &self.auth, &path)))
    }

    fn next(&mut self, result: IoResult) -> ResolveTurn {
        match self.step {
            ResolveStep::Access => {
                access_status(result.child::<(), AuthorizeError>()?)?;
                self.member_read()
            }
            ResolveStep::Member => match result.child::<MemberRead, EntryStatus>()?? {
                MemberRead::Remote {
                    version,
                    blob,
                    stale,
                } => Ok(Next::Done(Pinned {
                    version,
                    blob: Some(blob),
                    stale,
                })),
                MemberRead::Local(range_id) => {
                    self.stale = range_id;
                    let binding = (self.info.distribution.as_ref()).ok_or(IoError::Unfinished)?;
                    let route = route_read(binding, &self.key, false);
                    self.step = ResolveStep::Route;
                    Ok(Next::Call(IoCall::child(route)))
                }
            },
            // Without records of the key's range here, a miss would prove nothing.
            ResolveStep::Route => match result.child::<ReadRoute, GuardError>()? {
                Ok(ReadRoute::Local { catching_up }) => {
                    self.catching_up = catching_up;
                    self.head()
                }
                Ok(ReadRoute::Holder { .. }) | Err(_) => Err(EntryStatus::Unavailable),
            },
            ResolveStep::Head => self.pinned(result.child()?),
        }
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
