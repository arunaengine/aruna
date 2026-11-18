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

    async fn run(&self, fence: Option<(&LinkedJob, &LinkedJob)>) -> Result<Unified, String> {
        let found = self.found;
        let error = |error: aruna_core::errors::ConversionError| error.to_string();
        let mut writes = Vec::new();
        let name: Key = self.bucket.as_bytes().to_vec().into();
        if let Some((expected, next)) = fence {
            let read = vec![(LINKED_JOB_KEYSPACE.to_string(), name.clone())];
            let stored = self.batch(read).await?.pop().flatten();
            if stored.as_deref() != Some(expected.to_bytes().map_err(error)?.as_slice()) {
                return Ok(Unified::Stopped);
            }
            let next = next.to_bytes().map_err(error)?;
            writes.push((LINKED_JOB_KEYSPACE.to_string(), name, next.into()));
        }
        let keep_key = found.keep.location_key().map_err(error)?;
        let mut reads = vec![(
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            Key::from(keep_key.to_bytes()),
        )];
        for (id, _) in &found.versions {
            let version = VersionKey::new(self.bucket, &found.key, *id);
            let version = version.to_bytes().map_err(error)?;
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
        }
        for copy in &found.copies {
            reads.push((BACKEND_VERSION_KEYSPACE.to_string(), version_key(copy.ulid)));
            for rule in self.rules {
                let row = object_key(rule.import_id, &copy.backend_path);
                reads.push((IMPORTED_OBJECT_KEYSPACE.to_string(), row));
            }
        }
        let mut values = self.batch(reads).await?.into_iter();
        let kept = values.next().flatten();
        // Compared decoded: the encoding of a location's hash map is not stable.
        let kept = kept.as_deref().map(BackendLocation::from_bytes).transpose();
        let mut deletes = Vec::new();
        let twin = found
            .twins
            .iter()
            .find(|(ulid, ..)| *ulid == found.keep.ulid);
        match (kept.map_err(error)?, twin) {
            (Some(kept), None) if kept == found.keep => {}
            // A kept second copy becomes the versions' own file with a location row.
            (None, Some((_, index, version))) => {
                if !self.adoptable(index, *version).await? {
                    return Ok(Unified::Changed);
                }
                let value = found.keep.to_bytes().map_err(error)?;
                let location = Key::from(keep_key.to_bytes());
                writes.push((BLOB_LOCATIONS_KEYSPACE.to_string(), location, value.into()));
                deletes.push((RENAMED_ROW_KEYSPACE.to_string(), index.clone()));
                self.charge(&found.keep).await?;
            }
            _ => return Ok(Unified::Changed),
        }
        // A kept file a release is deleting, at its backend version or without one, takes no
        // new reader.
        let read = vec![(
            BACKEND_VERSION_KEYSPACE.to_string(),
            version_key(found.keep.ulid),
        )];
        let provider = self.batch(read).await?.pop().flatten();
        let provider = provider.map(|provider| String::from_utf8_lossy(&provider).into_owned());
        let unversioned = provider.is_none();
        let claim = claim_key(&found.keep, &provider.unwrap_or_default()).map_err(error)?;
        let read = vec![(DELETE_CLAIM_KEYSPACE.to_string(), claim)];
        if self.batch(read).await?.pop().flatten().is_some() {
            return Ok(Unified::Changed);
        }
        // Without a backend version the kept file must still hold the bytes it was found with.
        if unversioned && !self.kept_unchanged().await? {
            return Ok(Unified::Changed);
        }
        // Which copy each version reads now; any change refuses the whole key.
        let mut readers: HashMap<Ulid, Vec<(Ulid, BlobVersion)>> = HashMap::new();
        for (id, stored) in &found.versions {
            if values.next().flatten().as_deref() != Some(stored.as_slice()) {
                return Ok(Unified::Changed);
            }
            let version = BlobVersion::from_bytes(stored).map_err(error)?;
            // A raw copy is the copy whose location row the version reads.
            let file = match &version.state {
                BlobVersionState::Materialized {
                    encoding: EncodingClass::Plain { location },
                    ..
                } => Some(*location),
                BlobVersionState::Materialized {
                    encoding: EncodingClass::Raw,
                    ..
                } => (found.copies.iter())
                    .find(|copy| copy.location_key().ok() == version.location_key())
                    .map(|copy| copy.ulid),
                _ => None,
            };
            if let Some(file) = file {
                readers.entry(file).or_default().push((*id, version));
            }
        }
        let encoding = found.keep.format.encoding();
        let mut changed = 0;
        for copy in &found.copies {
            let provider = values.next().flatten();
            let mine = readers.remove(&copy.ulid).unwrap_or_default();
            let twin = found.twins.iter().find(|(ulid, ..)| *ulid == copy.ulid);
            let mut owner = None;
            let mut foreign = false;
            for rule in self.rules {
                let row = values.next().flatten();
                let row = row.as_deref().map(ImportedObject::from_bytes).transpose();
                let Some(row) = row.map_err(error)? else {
                    continue;
                };
                // A row of another key or version makes the file that version's own.
                let named = match twin {
                    Some((.., version)) => *version == row.version_id,
                    None => mine.iter().any(|(id, _)| *id == row.version_id),
                };
                let ours = row.key.as_deref().is_none_or(|key| key == found.key) && named;
                match ours {
                    true => owner = owner.or(Some((rule.import_id, row.version_id))),
                    false => foreign = true,
                }
            }
            if foreign || (mine.is_empty() && twin.is_none()) {
                continue;
            }
            // A second copy goes with its row while its index row still names it; that row
            // goes now.
            if let Some((_, index, _)) = twin {
                let read = vec![(RENAMED_ROW_KEYSPACE.to_string(), index.clone())];
                let stored = self.batch(read).await?.pop().flatten();
                let stored = stored.filter(|value| !value.is_empty());
                let stored = stored
                    .as_deref()
                    .map(BackendLocation::from_bytes)
                    .transpose();
                if stored.map_err(error)?.as_ref() != Some(copy) {
                    continue;
                }
                deletes.push((RENAMED_ROW_KEYSPACE.to_string(), index.clone()));
            }
            for (id, mut version) in mine {
                if let BlobVersionState::Materialized { encoding: old, .. } = &mut version.state {
                    *old = encoding;
                }
                let key = VersionKey::new(self.bucket, &found.key, id);
                writes.extend(self.copies(&key, copy).await?);
                let value = version.to_bytes().map_err(error)?;
                let key = key.to_bytes().map_err(error)?;
                writes.push((BLOB_VERSIONS_KEYSPACE.to_string(), key.into(), value.into()));
            }
            // A file named by its version's id was written by Aruna, so it goes without a
            // backend version too.
            let named = owner.is_some_and(|(import, version)| {
                let rules = self.rules.iter().filter(|rule| rule.import_id == import);
                id_named(
                    &rules.cloned().collect::<Vec<_>>(),
                    &copy.backend_path,
                    version,
                )
            });
            writes.push(release(copy, provider, owner, named)?);
            changed += 1;
        }
        if changed == 0 {
            return Ok(Unified::Kept);
        }
        if !deletes.is_empty() {
            let delete = StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(self.txn_id),
            };
            match self
                .context
                .storage_handle
                .send_storage_effect(delete)
                .await
            {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
                other => return Err(format!("could not drop the copy index: {}", other.label())),
            }
        }
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: Some(self.txn_id),
        };
        match self.context.storage_handle.send_storage_effect(write).await {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(Unified::Done),
            other => Err(format!(
                "could not write the unification: {}",
                other.label()
            )),
        }
    }

    /// Whether the kept second copy's index row still names it and its row still names
    /// `version` under this key, as this transaction reads them.
    async fn adoptable(&self, index: &Key, version: Ulid) -> Result<bool, String> {
        let error = |error: aruna_core::errors::ConversionError| error.to_string();
        let found = self.found;
        let mut reads = vec![(RENAMED_ROW_KEYSPACE.to_string(), index.clone())];
        for rule in self.rules {
            let row = object_key(rule.import_id, &found.keep.backend_path);
            reads.push((IMPORTED_OBJECT_KEYSPACE.to_string(), row));
        }
        let mut values = self.batch(reads).await?.into_iter();
        let stored = values.next().flatten().filter(|value| !value.is_empty());
        let stored = stored
            .as_deref()
            .map(BackendLocation::from_bytes)
            .transpose();
        if stored.map_err(error)?.as_ref() != Some(&found.keep) {
            return Ok(false);
        }
        for row in values.flatten() {
            let row = ImportedObject::from_bytes(&row).map_err(error)?;
            if row.key.as_deref() == Some(found.key.as_str()) && row.version_id == version {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether the kept file still has the size and the full ETag its row recorded, as a
    /// listing of its path shows them now.
    async fn kept_unchanged(&self) -> Result<bool, String> {
        let keep = &self.found.keep;
        let Some(blob) = self.context.blob_handle.as_ref() else {
            return Ok(false);
        };
        let reads = (self.rules.iter())
            .map(|rule| {
                let row = object_key(rule.import_id, &keep.backend_path);
                (IMPORTED_OBJECT_KEYSPACE.to_string(), row)
            })
            .collect();
        let mut recorded = None;
        for row in self.batch(reads).await?.into_iter().flatten() {
            let row = ImportedObject::from_bytes(&row).map_err(|error| error.to_string())?;
            if row.key.as_deref() == Some(self.found.key.as_str()) {
                recorded = row.etag;
            }
        }
        let Some(recorded) = recorded else {
            return Ok(false);
        };
        let list = aruna_core::effects::BlobEffect::ListExternal {
            backend: keep.backend.clone(),
            prefix: keep.backend_path.clone(),
            start_after: None,
            limit: 1,
        };
        let unquoted = |etag: &str| etag.trim_matches('"').to_string();
        match blob.send_blob_effect(list).await {
            Event::Blob(aruna_core::events::BlobEvent::ExternalListed { objects, .. }) => {
                Ok(objects.first().is_some_and(|object| {
                    object.key == keep.backend_path
                        && object.size == keep.blob_size
                        && object.etag.as_deref().map(unquoted) == Some(unquoted(&recorded))
                }))
            }
            other => Err(format!("could not list the kept file: {}", other.label())),
        }
    }

    /// Charges the kept file's new location row, as reclaim debits the released one.
    async fn charge(&self, location: &BackendLocation) -> Result<(), String> {
        use crate::s3::usage::{StoredDelta, UsageCounterUpdate};
        let bytes = i128::from(location.stored_size());
        let Some(stored) = StoredDelta::of_copy(location, 1, bytes) else {
            return Ok(());
        };
        let mut update = UsageCounterUpdate::for_stored(stored);
        let mut effects = update.start(self.txn_id);
        loop {
            let mut answer = None;
            for effect in effects {
                let Effect::Storage(effect) = effect else {
                    return Err(format!("not a storage effect: {effect:?}"));
                };
                answer = Some(
                    self.context
                        .storage_handle
                        .send_storage_effect(effect)
                        .await,
                );
            }
            let Some(answer) = answer else {
                return Ok(());
            };
            match update.step(answer, self.txn_id) {
                Ok(Some(next)) => effects = next,
                Ok(None) => return Ok(()),
                Err(error) => return Err(error.to_string()),
            }
        }
    }

    /// The copy registrations of `version` that name `copy`, moved to the kept file.
    async fn copies(
        &self,
        version: &VersionKey,
        copy: &BackendLocation,
    ) -> Result<Vec<(String, Key, Value)>, String> {
        let prefix = ManagedCopyKey::version_prefix(version).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        let mut after = None;
        loop {
            let page = iter_prefix_page(
                &self.context.storage_handle,
                MANAGED_COPY_KEYSPACE,
                Some(prefix.clone().into()),
                after.take(),
                RULE_PAGE,
                Some(self.txn_id),
            );
            let (page, next) = page.await?;
            rows.extend(page);
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        let mut writes = Vec::new();
        for (key, value) in rows {
            let record =
                ManagedCopyRecord::from_bytes(&value).map_err(|error| error.to_string())?;
            if record.location.same_object(copy) {
                let record = ManagedCopyRecord {
                    location: self.found.keep.clone(),
                    ..record
                };
                let value = record.to_bytes().map_err(|error| error.to_string())?;
                writes.push((MANAGED_COPY_KEYSPACE.to_string(), key, value.into()));
            }
        }
        Ok(writes)
    }
}

/// The release of `copy` at its backend version, or of a file its version's id names, or else
/// the drop of its location row, which leaves the file in place.
fn release(
    copy: &BackendLocation,
    provider: Option<Value>,
    owner: Option<(Ulid, Ulid)>,
    named: bool,
) -> Result<(String, Key, Value), String> {
    let error = |error: aruna_core::errors::ConversionError| error.to_string();
    let provider = provider.or_else(|| named.then(|| Value::from(Vec::new())));
    let Some(provider) = provider else {
        let location = copy.location_key().map_err(error)?;
        let candidate =
            ReclaimCandidateKey::new(location.backend, location.encoding, location.blake3_hash);
        let value = ReclaimCandidate {
            enqueued_at: SystemTime::now(),
        };
        let value = value.to_bytes().map_err(error)?;
        let key = candidate.to_bytes().into();
        return Ok((BLOB_RECLAIM_KEYSPACE.to_string(), key, value.into()));
    };
    let (import_id, version_id) = owner.unwrap_or((Ulid::nil(), Ulid::nil()));
    let work = BlobCleanupWork::ReleaseRenamed {
        location: copy.clone(),
        import_id,
        version_id,
        version: String::from_utf8(provider.to_vec()).map_err(|error| error.to_string())?,
    };
    let value = work.to_bytes().map_err(error)?;
    let key = Ulid::generate().to_bytes().to_vec().into();
    Ok((BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into()))
}

/// The group's rules that read or write on `backend`, whose rows may record a copy.
pub(crate) async fn rules_on(
    context: &DriverContext,
    group_id: GroupId,
    backend: &BackendRef,
) -> Result<Vec<BackendImport>, String> {
    let operation = BackendRulesOperation::new(group_id, backend.clone());
    let rules = crate::driver::drive(operation, context).await;
    rules.map_err(|error| error.to_string())
}

async fn read(context: &DriverContext, space: &str, key: Key) -> Result<Option<Vec<u8>>, String> {
    let read = StorageEffect::Read {
        key_space: space.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|v| v.to_vec())),
        other => Err(format!("could not read {space}: {}", other.label())),
    }
}

#[cfg(test)]
#[path = "unify_tests.rs"]
mod tests;
