//! Finds keys whose versions keep the same content in several linked files, for the preview and
//! the job that lets those versions read one file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::request_authorization::BucketAccess;
use crate::blob::import_rows::{ImportRowsError, SecondCopiesOperation, SecondCopy};
use crate::blob::migration::shared::{MigrationError, routing_call, routing_result};
pub use crate::blob::unify_stage::Unified;
use crate::blob::unify_stage::UnifyOperation;
use crate::s3::bucket::job_progress::JobStep;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{BackendImport, PathIdentity, names_version};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobVersion, BlobVersionState, VersionKey,
};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::linked_job::LinkedJob;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::{Key, Value};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use ulid::Ulid;

/// Version rows one storage read returns, and one preview or job page passes over at most.
const VERSION_PAGE: usize = 1024;
const PAGE_SCAN: usize = 10_000;
const TOTAL_SCAN: usize = 10_000;
const JOB_SCAN: usize = 20_000;
/// Keys one preview request checks for copies at most, for its totals and again for its page.
const PREVIEW_CHECKS: usize = 5_000;
/// Keys with copies one job page unifies at most.
const JOB_KEYS: usize = 64;

type Turn<T> = Result<Next<T>, MigrationError>;

/// The linked files with the same content of one key: the one its versions keep and the
/// copies the cleanup releases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Duplicate {
    pub key: String,
    pub keep: BackendLocation,
    pub copies: Vec<BackendLocation>,
    /// The versions that read a copy now, with their stored rows.
    pub(super) versions: Vec<(Ulid, Vec<u8>)>,
    /// Copies no version reads, which only a row names, by location id with their index row
    /// and the version that row named.
    pub(super) twins: Vec<(Ulid, Key, Ulid)>,
}

impl Duplicate {
    /// Bytes the release of the copies frees.
    pub fn freed(&self) -> u64 {
        self.copies.iter().map(|copy| copy.blob_size).sum()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnifyPage {
    /// Keys with copies, and the bytes their release frees; lower bounds unless `complete`.
    pub keys: u64,
    pub bytes: u64,
    pub complete: bool,
    pub entries: Vec<Duplicate>,
    /// Where the next page starts: after a key the caller may read, or within it.
    pub next: Option<Cursor>,
}

/// A key the caller may read and, to continue within it, the content groups already shown.
pub type Cursor = (String, Option<usize>);

/// What a [`KeyWalk`] needs or found next.
#[derive(Debug, PartialEq)]
enum WalkStep {
    Read(IoCall),
    Key(String, Vec<Reader>),
    End,
}

/// The versions of the keys of a bucket in storage order, one key at a time.
#[derive(Debug, PartialEq)]
struct KeyWalk {
    bucket: String,
    start: Option<Key>,
    rows: VecDeque<(Key, Value)>,
    end: bool,
    scanned: usize,
    /// Versions one key yields at most; a key with more is skipped and marks `cut`.
    limit: usize,
    cut: bool,
    key: Option<String>,
    versions: Vec<Reader>,
}

impl KeyWalk {
    /// Starts after every version of `after`.
    fn new(bucket: &str, after: Option<&str>) -> Self {
        let start = after.and_then(|key| VersionKey::object_prefix(bucket, key).ok());
        // Every version of the key sorts before its prefix followed by these bytes.
        let start = start.map(|mut prefix| {
            prefix.extend([u8::MAX; 32]);
            prefix.into()
        });
        Self {
            bucket: bucket.to_string(),
            start,
            rows: VecDeque::new(),
            end: false,
            scanned: 0,
            limit: usize::MAX,
            cut: false,
            key: None,
            versions: Vec::new(),
        }
    }

    /// Reads at most `limit` versions of one key before skipping the rest of it.
    fn capped(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Starts at the first version of `key`.
    fn from_key(bucket: &str, key: &str) -> Self {
        let mut walk = Self::new(bucket, None);
        walk.start = VersionKey::object_prefix(bucket, key).ok().map(Into::into);
        walk
    }

    /// Takes the page a [`WalkStep::Read`] asked for.
    fn fill(&mut self, result: IoResult) -> Result<(), MigrationError> {
        let (rows, next) = result.page()?;
        self.end = next.is_none();
        self.start = rows.last().map(|(key, _)| key.clone());
        self.rows.extend(rows);
        Ok(())
    }

    fn emit(&mut self) -> WalkStep {
        match self.key.take() {
            Some(key) => WalkStep::Key(key, std::mem::take(&mut self.versions)),
            None => WalkStep::End,
        }
    }

    /// The next key and its versions, the page it needs first, or the end of the bucket.
    fn advance(&mut self) -> Result<WalkStep, MigrationError> {
        if self.key.is_none() {
            self.cut = false;
        }
        loop {
            let Some((raw, value)) = self.rows.front().cloned() else {
                if self.end {
                    return Ok(self.emit());
                }
                return Ok(WalkStep::Read(IoCall::Page {
                    space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    prefix: VersionKey::bucket_prefix(&self.bucket)?.into(),
                    after: self.start.take(),
                    limit: VERSION_PAGE,
                }));
            };
            let version = VersionKey::from_bytes(&raw)?;
            match self.key.as_deref() {
                Some(current) if current != version.key => return Ok(self.emit()),
                Some(current) if self.versions.len() == self.limit => {
                    // Every version of the key sorts before its prefix followed by these bytes.
                    let mut after = VersionKey::object_prefix(&self.bucket, current)?;
                    after.extend([u8::MAX; 32]);
                    (self.start, self.end, self.cut) = (Some(after.into()), false, true);
                    self.rows.clear();
                    return Ok(self.emit());
                }
                Some(_) => {}
                None => self.key = Some(version.key.clone()),
            }
            self.versions.push((version.version_id, value.to_vec()));
            self.scanned += 1;
            self.rows.pop_front();
        }
    }
}

/// A version with its stored row, and the linked file it reads.
type Member = (Ulid, Vec<u8>, Ulid);
/// A version with its stored row.
type Reader = (Ulid, Vec<u8>);
/// A linked file with the versions that read it.
type File = (BackendLocation, Vec<Reader>);
/// The members of one content on one backend.
type Group = (([u8; 32], Vec<u8>), Vec<Member>);

#[derive(Debug, PartialEq)]
enum DuplicateStep {
    Twins,
    Locations(Group, Vec<Reader>),
}

/// The linked files of `key` with the same content as another of its files, by content.
#[derive(Debug, PartialEq)]
struct DuplicatesOperation {
    routing: Option<Arc<RoutingSnapshot>>,
    bucket: String,
    key: String,
    versions: Vec<Reader>,
    groups: VecDeque<Group>,
    twins: Vec<SecondCopy>,
    step: DuplicateStep,
    found: Vec<Duplicate>,
}

impl DuplicatesOperation {
    fn new(
        routing: Option<Arc<RoutingSnapshot>>,
        (bucket, key): (&str, &str),
        versions: Vec<Reader>,
    ) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            key: key.to_string(),
            versions,
            groups: VecDeque::new(),
            twins: Vec::new(),
            step: DuplicateStep::Twins,
            found: Vec::new(),
        }
    }

    fn twins_of(&self, (hash, backend): &([u8; 32], Vec<u8>)) -> Vec<SecondCopy> {
        (self.twins.iter())
            .filter(|(file, ..)| {
                file.get_blake3() == Some(&hash[..]) && file.backend.key_bytes() == *backend
            })
            .cloned()
            .collect()
    }

    fn next_group(&mut self) -> Turn<Vec<Duplicate>> {
        while let Some(group) = self.groups.pop_front() {
            // Only content that two files hold needs its location rows read.
            let twins = self.twins_of(&group.0);
            let mut files: HashSet<_> = group.1.iter().map(|(.., at)| *at).collect();
            files.extend(twins.iter().map(|(file, ..)| file.ulid));
            if files.len() < 2 {
                continue;
            }
            let mut reads = Vec::new();
            let mut readers = Vec::new();
            for (id, value, _) in &group.1 {
                let version = BlobVersion::from_bytes(value)?;
                let Some(location_key) = version.location_key() else {
                    continue;
                };
                reads.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    location_key.to_bytes().into(),
                ));
                readers.push((*id, value.clone()));
            }
            if reads.is_empty() {
                self.add_group(&group, Vec::new(), Vec::new())?;
                continue;
            }
            self.step = DuplicateStep::Locations(group, readers);
            return Ok(Next::Call(IoCall::Read(reads)));
        }
        Ok(Next::Done(std::mem::take(&mut self.found)))
    }

    fn add_group(
        &mut self,
        group: &Group,
        readers: Vec<Reader>,
        values: Vec<Option<Value>>,
    ) -> Result<(), MigrationError> {
        let mut files: Vec<File> = Vec::new();
        for ((id, value), row) in readers.into_iter().zip(values) {
            let Some(row) = row else {
                continue;
            };
            let location = BackendLocation::from_bytes(&row)?;
            if !location.is_external() || location.staging || location.partial {
                continue;
            }
            let known = files
                .iter_mut()
                .find(|(file, _)| file.ulid == location.ulid);
            match known {
                Some((_, readers)) => readers.push((id, value)),
                None => files.push((location, vec![(id, value)])),
            }
        }
        let twins = self.twins_of(&group.0);
        for (file, ..) in &twins {
            if !files.iter().any(|(known, _)| known.ulid == file.ulid) {
                files.push((file.clone(), Vec::new()));
            }
        }
        if files.len() < 2 {
            return Ok(());
        }
        let routing = self.routing.as_deref();
        let keep = kept_file(routing, (&self.bucket, &self.key), &files);
        let (keep, _) = files.remove(keep);
        let versions = files
            .iter()
            .flat_map(|(_, readers)| readers.clone())
            .collect();
        let copies: Vec<BackendLocation> = files.into_iter().map(|(file, _)| file).collect();
        let listed = |file: &BackendLocation| {
            file.ulid == keep.ulid || copies.iter().any(|copy| copy.ulid == file.ulid)
        };
        let twins = (twins.into_iter())
            .filter(|(file, ..)| listed(file))
            .map(|(file, index, version)| (file.ulid, index, version))
            .collect();
        self.found.push(Duplicate {
            key: self.key.clone(),
            keep,
            copies,
            versions,
            twins,
        });
        Ok(())
    }
}

impl Operation for DuplicatesOperation {
    type Output = Vec<Duplicate>;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Vec<Duplicate>> {
        let groups = content_groups(std::mem::take(&mut self.versions))?;
        let ids = versions_of(&groups);
        self.groups = groups.into_iter().collect();
        let twins =
            SecondCopiesOperation::new(self.routing.as_deref(), (&self.bucket, &self.key), &ids)
                .map_err(MigrationError::failed)?;
        Ok(Next::Call(IoCall::child(twins)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Vec<Duplicate>> {
        match std::mem::replace(&mut self.step, DuplicateStep::Twins) {
            DuplicateStep::Twins => {
                self.twins = (result.child::<Vec<SecondCopy>, ImportRowsError>()?)
                    .map_err(MigrationError::failed)?;
                self.next_group()
            }
            DuplicateStep::Locations(group, readers) => {
                self.add_group(&group, readers, result.values()?)?;
                self.next_group()
            }
        }
    }
}

/// The versions on tenant storage with a linked or shared raw file, by content and backend.
fn content_groups(
    versions: Vec<Reader>,
) -> Result<BTreeMap<([u8; 32], Vec<u8>), Vec<Member>>, MigrationError> {
    let mut groups: BTreeMap<([u8; 32], Vec<u8>), Vec<Member>> = BTreeMap::new();
    for (id, value) in versions {
        let version = BlobVersion::from_bytes(&value)?;
        let BlobVersionState::Materialized {
            blob_hash,
            backend: backend @ BackendRef::Group(_),
            encoding,
            ..
        } = &version.state
        else {
            continue;
        };
        // An import's shared raw copy is one file for all its readers.
        let file = match encoding {
            EncodingClass::Plain { location } => *location,
            EncodingClass::Raw => Ulid::nil(),
            _ => continue,
        };
        let group = (*blob_hash, backend.key_bytes());
        groups.entry(group).or_default().push((id, value, file));
    }
    Ok(groups)
}

/// The file a key keeps. A plain name is kept, one a version read before a second copy first; a
/// file whose name carries the id of a version reading it goes. Without a plain name the newest
/// stays.
fn kept_file(
    routing: Option<&RoutingSnapshot>,
    (bucket, key): (&str, &str),
    files: &[File],
) -> usize {
    let rules = routing.map_or(&[][..], |routing| routing.imports.as_slice());
    let plain = |(file, readers): &File| {
        !readers
            .iter()
            .any(|(id, _)| id_named(rules, &file.backend_path, *id))
    };
    let newest = |(_, readers): &File| readers.iter().map(|(id, _)| *id).max();
    // Among files all named by ids, the one the current pattern names stays.
    let current = |(file, readers): &File| {
        readers.iter().any(|(id, _)| {
            let target = routing.and_then(|routing| routing.plain_target(bucket, key, *id));
            target.is_some_and(|target| target.path == file.backend_path)
        })
    };
    let read = files
        .iter()
        .position(|file| plain(file) && !file.1.is_empty());
    let named = files.iter().position(current);
    match read.or_else(|| files.iter().position(plain)).or(named) {
        Some(at) => at,
        None => (0..files.len())
            .max_by_key(|at| newest(&files[*at]))
            .unwrap_or_default(),
    }
}

/// Whether a rule's write pattern gives `path` the version id `id`, or its file name has it
/// in the id slot of Aruna's names.
pub(super) fn id_named(rules: &[BackendImport], path: &str, id: Ulid) -> bool {
    names_version(path, id)
        || rules.iter().any(|rule| {
        matches!(rule.decode_any(path, &[]), PathIdentity::Decoded { id: named, .. } if named == id)
    })
}

/// The ids of the versions grouped by content.
fn versions_of(groups: &BTreeMap<([u8; 32], Vec<u8>), Vec<Member>>) -> Vec<Ulid> {
    groups.values().flatten().map(|(id, ..)| *id).collect()
}

/// Which keys a duplicate preview covers and which part of them it shows.
#[derive(Debug, Default, PartialEq)]
pub struct PreviewQuery {
    pub prefix: String,
    pub filter: Option<String>,
    pub after: Option<Cursor>,
    pub limit: usize,
}

#[derive(Debug, PartialEq)]
enum PreviewStep {
    Routing,
    TotalPage,
    TotalKey,
    ListPage,
    ListKey(String),
}

/// One preview page of the keys under the prefix after `after` with copies, as far as the
/// caller may read them, and the totals.
#[derive(Debug, PartialEq)]
pub struct PreviewPageOperation {
    routing: NodeRouting,
    bucket: String,
    access: Arc<BucketAccess>,
    query: PreviewQuery,
    step: PreviewStep,
    snapshot: Option<Arc<RoutingSnapshot>>,
    walk: KeyWalk,
    checked: usize,
    last: Option<Cursor>,
    page: UnifyPage,
}

impl PreviewPageOperation {
    pub fn new(
        routing: NodeRouting,
        bucket: &str,
        access: Arc<BucketAccess>,
        query: PreviewQuery,
    ) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            access,
            query,
            step: PreviewStep::Routing,
            snapshot: None,
            walk: KeyWalk::new(bucket, None).capped(PAGE_SCAN),
            checked: 0,
            last: None,
            page: UnifyPage {
                complete: true,
                ..UnifyPage::default()
            },
        }
    }

    fn visible(&self, key: &str) -> bool {
        self.access.allows(Some(key), &Permission::READ)
    }

    fn readable(&self, key: &str) -> bool {
        key.starts_with(&self.query.prefix)
            && (self.query.filter.as_deref()).is_none_or(|filter| key.contains(filter))
            && self.visible(key)
    }

    fn call(&mut self, step: PreviewStep, call: IoCall) -> Turn<UnifyPage> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn check_key(
        &mut self,
        step: PreviewStep,
        key: &str,
        versions: Vec<Reader>,
    ) -> Turn<UnifyPage> {
        self.checked += 1;
        let scope = (self.bucket.as_str(), key);
        let found = DuplicatesOperation::new(self.snapshot.clone(), scope, versions);
        self.call(step, IoCall::child(found))
    }

    fn finish(&mut self) -> Turn<UnifyPage> {
        Ok(Next::Done(std::mem::take(&mut self.page)))
    }

    fn walk_totals(&mut self) -> Turn<UnifyPage> {
        loop {
            match self.walk.advance()? {
                WalkStep::Read(call) => return self.call(PreviewStep::TotalPage, call),
                WalkStep::End => return self.start_list(),
                WalkStep::Key(key, versions) => {
                    if self.walk.scanned > TOTAL_SCAN || self.checked == PREVIEW_CHECKS {
                        self.page.complete = false;
                        return self.start_list();
                    }
                    if self.walk.cut {
                        self.page.complete = false;
                    }
                    if !self.readable(&key) || self.walk.cut {
                        continue;
                    }
                    return self.check_key(PreviewStep::TotalKey, &key, versions);
                }
            }
        }
    }

    fn start_list(&mut self) -> Turn<UnifyPage> {
        let walk = match &self.query.after {
            Some((key, Some(_))) => KeyWalk::from_key(&self.bucket, key),
            Some((key, None)) => KeyWalk::new(&self.bucket, Some(key)),
            None => KeyWalk::new(&self.bucket, None),
        };
        self.walk = walk.capped(PAGE_SCAN);
        self.checked = 0;
        self.walk_list()
    }

    fn walk_list(&mut self) -> Turn<UnifyPage> {
        loop {
            match self.walk.advance()? {
                WalkStep::Read(call) => return self.call(PreviewStep::ListPage, call),
                WalkStep::End => return self.finish(),
                WalkStep::Key(key, versions) => {
                    if self.page.entries.len() == self.query.limit
                        || self.walk.scanned > PAGE_SCAN
                        || self.checked == PREVIEW_CHECKS
                    {
                        self.page.next = self.last.take();
                        if self.page.next.is_none() {
                            return Err(MigrationError::Failed(
                                "too many keys in a row are hidden from the caller".to_string(),
                            ));
                        }
                        return self.finish();
                    }
                    // A cursor may name any key the caller reads, also one the filter leaves out.
                    if self.visible(&key) {
                        self.last = Some((key.clone(), None));
                    }
                    // A key with more versions than a page reads is skipped and leaves the page
                    // incomplete.
                    if self.walk.cut {
                        self.page.complete = false;
                    }
                    if !self.readable(&key) || self.walk.cut {
                        continue;
                    }
                    let step = PreviewStep::ListKey(key.clone());
                    return self.check_key(step, &key, versions);
                }
            }
        }
    }

    fn listed(&mut self, key: String, found: Vec<Duplicate>) -> Turn<UnifyPage> {
        let skip = match &self.query.after {
            Some((after, Some(shown))) if *after == key => *shown,
            _ => 0,
        };
        // A key with more contents than the page holds continues on the next page.
        for (index, duplicate) in found.into_iter().enumerate().skip(skip) {
            if self.page.entries.len() == self.query.limit {
                self.page.next = Some((key, Some(index)));
                return self.finish();
            }
            self.page.entries.push(duplicate);
        }
        self.walk_list()
    }
}

impl Operation for PreviewPageOperation {
    type Output = UnifyPage;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<UnifyPage> {
        Ok(Next::Call(routing_call(self.routing.clone(), &self.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Turn<UnifyPage> {
        match std::mem::replace(&mut self.step, PreviewStep::TotalPage) {
            PreviewStep::Routing => {
                self.snapshot = routing_result(result)?.map(Arc::new);
                self.walk_totals()
            }
            PreviewStep::TotalPage => {
                self.walk.fill(result)?;
                self.walk_totals()
            }
            PreviewStep::TotalKey => {
                let found = result.child::<Vec<Duplicate>, MigrationError>()??;
                self.page.keys += u64::from(!found.is_empty());
                self.page.bytes += found.iter().map(Duplicate::freed).sum::<u64>();
                self.walk_totals()
            }
            PreviewStep::ListPage => {
                self.walk.fill(result)?;
                self.walk_list()
            }
            PreviewStep::ListKey(key) => {
                let found = result.child::<Vec<Duplicate>, MigrationError>()??;
                self.listed(key, found)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum JobStage {
    Routing,
    Page,
    Key(String, bool),
    Unify(String, bool, Box<LinkedJob>),
}

/// Unifies the next keys after the job's cursor; keys the caller may not write or written
/// after the start keep their files. Each key changes with the job's progress, and only while
/// the job is stored as read, so a paused or aborted job changes nothing more.
#[derive(Debug, PartialEq)]
pub struct UnifyJobOperation {
    routing: NodeRouting,
    bucket: String,
    prefix: String,
    access: Arc<BucketAccess>,
    started_at_ms: u64,
    stage: JobStage,
    snapshot: Option<Arc<RoutingSnapshot>>,
    current: LinkedJob,
    progress: JobStep,
    walk: KeyWalk,
    found: VecDeque<Duplicate>,
    unified: usize,
}

impl UnifyJobOperation {
    pub fn new(
        routing: NodeRouting,
        (bucket, prefix): (&str, &str),
        access: Arc<BucketAccess>,
        job: &LinkedJob,
    ) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            access,
            started_at_ms: job.started_at_ms,
            stage: JobStage::Routing,
            snapshot: None,
            current: job.clone(),
            progress: JobStep {
                cursor: job.cursor.clone(),
                ..JobStep::default()
            },
            walk: KeyWalk::new(bucket, job.cursor.as_deref()),
            found: VecDeque::new(),
            unified: 0,
        }
    }

    fn finish(&mut self) -> Turn<JobStep> {
        Ok(Next::Done(std::mem::take(&mut self.progress)))
    }

    fn next_key(&mut self) -> Turn<JobStep> {
        loop {
            if self.unified == JOB_KEYS || self.walk.scanned > JOB_SCAN {
                return self.finish();
            }
            match self.walk.advance()? {
                WalkStep::Read(call) => {
                    self.stage = JobStage::Page;
                    return Ok(Next::Call(call));
                }
                WalkStep::End => {
                    self.progress.finished = true;
                    return self.finish();
                }
                WalkStep::Key(key, versions) => {
                    if !key.starts_with(&self.prefix) {
                        self.progress.cursor = Some(key);
                        continue;
                    }
                    let started = self.started_at_ms;
                    let fresh = (versions.iter()).all(|(id, _)| id.timestamp_ms() <= started);
                    let allowed = self.access.allows(Some(&key), &Permission::WRITE) && fresh;
                    let scope = (self.bucket.as_str(), key.as_str());
                    let found = DuplicatesOperation::new(self.snapshot.clone(), scope, versions);
                    self.stage = JobStage::Key(key, allowed);
                    return Ok(Next::Call(IoCall::child(found)));
                }
            }
        }
    }

    fn next_duplicate(&mut self, key: String, allowed: bool) -> Turn<JobStep> {
        while let Some(found) = self.found.pop_front() {
            let group_id = (self.snapshot.as_ref())
                .filter(|_| allowed)
                .map(|routing| routing.group_id);
            let Some(group_id) = group_id else {
                self.progress.skipped += 1;
                continue;
            };
            // The key's progress is stored with it; the cursor names the key before it.
            let mut next = self.current.clone();
            next.cursor = self.progress.cursor.clone();
            next.done += 1;
            next.skipped += self.progress.skipped;
            next.failed += self.progress.failed;
            let fence = Some((self.current.clone(), next.clone()));
            let unify = UnifyOperation::new((&self.bucket, group_id), found, fence);
            self.stage = JobStage::Unify(key, allowed, Box::new(next));
            return Ok(Next::Call(IoCall::child(unify)));
        }
        self.progress.cursor = Some(key);
        self.next_key()
    }

    fn unified(
        &mut self,
        (key, allowed, next): (String, bool, LinkedJob),
        outcome: Unified,
    ) -> Turn<JobStep> {
        match outcome {
            Unified::Done => {
                (self.progress.skipped, self.progress.failed) = (0, 0);
                self.current = next;
            }
            Unified::Kept => self.progress.skipped += 1,
            Unified::Changed => self.progress.failed += 1,
            Unified::Stopped => {
                return Ok(Next::Done(JobStep {
                    cursor: self.current.cursor.clone(),
                    ..JobStep::default()
                }));
            }
        }
        self.unified += 1;
        self.next_duplicate(key, allowed)
    }
}

impl Operation for UnifyJobOperation {
    type Output = JobStep;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<JobStep> {
        Ok(Next::Call(routing_call(self.routing.clone(), &self.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Turn<JobStep> {
        match std::mem::replace(&mut self.stage, JobStage::Page) {
            JobStage::Routing => {
                self.snapshot = routing_result(result)?.map(Arc::new);
                self.next_key()
            }
            JobStage::Page => {
                self.walk.fill(result)?;
                self.next_key()
            }
            JobStage::Key(key, allowed) => {
                let found = result.child::<Vec<Duplicate>, MigrationError>()??;
                self.found = found.into();
                self.next_duplicate(key, allowed)
            }
            JobStage::Unify(key, allowed, next) => {
                let outcome = result.child::<Unified, MigrationError>()??;
                self.unified((key, allowed, *next), outcome)
            }
        }
    }
}

#[cfg(test)]
#[path = "unify_tests.rs"]
mod tests;
