//! Finds the linked-storage files of purged versions, keeps them planned while the purge runs
//! and hands them to the release of renamed files once the versions are gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::import_rows::{
    BackendRulesOperation, BucketRoutingOperation, ImportRowsError, SecondCopiesOperation,
    SecondCopy,
};
use crate::blob::migration::misplaced::KeyRule;
use crate::blob::migration::shared::{MigrationError, routing_call, routing_result};
use crate::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
    ListVersionsResult,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, PATHS_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, names_version, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion, HashIndex,
    VersionKey,
};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::string::FromUtf8Error;
use thiserror::Error;
use ulid::Ulid;

const ALIAS_PAGE: usize = 256;
/// Hash aliases one preview checks at most for other readers of its files.
const ALIAS_BUDGET: usize = 10_000;

/// Why a previewed file may stay when its other readers were not all checked.
pub const UNCHECKED_READERS: &str = "the preview could not check every other reader of the file";

/// One linked-storage file of a purged version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageFile {
    pub key: String,
    pub version_id: Ulid,
    pub location: BackendLocation,
    /// The backend version the file was written as; only that version is deleted. Empty for
    /// a file of an unversioned storage named by its version's id.
    pub version: Option<String>,
    /// The rule that records the file and the version its row names.
    pub owner: Option<(Ulid, Ulid)>,
    /// Why the file would stay after the purge, as far as a preview can tell.
    pub kept: Option<String>,
    /// The key index row of a second copy no version reads, which goes with the versions.
    pub index: Option<Key>,
}

impl StorageFile {
    /// The release of this file once purge `job_id` ended, which deletes it only once nothing
    /// reads it.
    fn work(&self, job_id: Ulid) -> Option<BlobCleanupWork> {
        let (import_id, version_id) = self.owner.unwrap_or((Ulid::nil(), self.version_id));
        Some(BlobCleanupWork::ReleasePlanned {
            job_id,
            location: self.location.clone(),
            import_id,
            version_id,
            version: self.version.clone()?,
        })
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum PurgeFilesError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Rows(#[from] ImportRowsError),
    #[error("a stored backend version is not text: {0}")]
    BackendVersion(#[from] FromUtf8Error),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FilesStep {
    Versions,
    Locations,
    Routing,
    Copies,
    Rules,
    FileRows,
    Elsewhere,
}

type FilesTurn = Result<Next<Vec<StorageFile>>, PurgeFilesError>;

/// The linked-storage files the `versions` of `bucket` read, one entry per file. With `pins`,
/// the purge scope, each file says why it would stay.
#[derive(Debug, PartialEq)]
pub struct StorageFilesOperation {
    routing: NodeRouting,
    bucket: String,
    group_id: GroupId,
    versions: Vec<(String, Ulid)>,
    pins: Option<StoragePurgeScope>,
    step: FilesStep,
    named: Vec<(String, Ulid)>,
    files: Vec<StorageFile>,
    snapshot: Option<RoutingSnapshot>,
    keys: VecDeque<(String, Vec<Ulid>)>,
    copies_of: String,
    backends: VecDeque<BackendRef>,
    rules: HashMap<Vec<u8>, Vec<BackendImport>>,
    /// The file whose rows are being read.
    at: usize,
    budget: usize,
}

impl StorageFilesOperation {
    pub fn new(
        routing: NodeRouting,
        (bucket, group_id): (&str, GroupId),
        versions: &[(String, Ulid)],
        pins: Option<&StoragePurgeScope>,
    ) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            group_id,
            versions: versions.to_vec(),
            pins: pins.cloned(),
            step: FilesStep::Versions,
            named: Vec::new(),
            files: Vec::new(),
            snapshot: None,
            keys: VecDeque::new(),
            copies_of: String::new(),
            backends: VecDeque::new(),
            rules: HashMap::new(),
            at: 0,
            budget: ALIAS_BUDGET,
        }
    }

    fn call(&mut self, step: FilesStep, call: IoCall) -> FilesTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn versions_read(&mut self, values: Vec<Option<Value>>) -> FilesTurn {
        let mut reads = Vec::new();
        for ((key, id), value) in self.versions.iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            if let Some(location) = BlobVersion::from_bytes(&value)?.location_key() {
                self.named.push((key.clone(), *id));
                let row = location.to_bytes().into();
                reads.push((BLOB_LOCATIONS_KEYSPACE.to_string(), row));
            }
        }
        if reads.is_empty() {
            return self.read_routing();
        }
        self.call(FilesStep::Locations, IoCall::Read(reads))
    }

    fn locations_read(&mut self, values: Vec<Option<Value>>) -> FilesTurn {
        for ((key, id), value) in std::mem::take(&mut self.named).into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let location = BackendLocation::from_bytes(&value)?;
            let linked = location.is_external() && !location.staging && !location.partial;
            if linked && !self.holds(&location) {
                self.push_file(key, id, location, None);
            }
        }
        self.read_routing()
    }

    fn holds(&self, location: &BackendLocation) -> bool {
        (self.files.iter()).any(|file| file.location.ulid == location.ulid)
    }

    fn push_file(&mut self, key: String, id: Ulid, location: BackendLocation, index: Option<Key>) {
        self.files.push(StorageFile {
            key,
            version_id: id,
            location,
            version: None,
            owner: None,
            kept: None,
            index,
        });
    }

    /// A second copy an import registered for a purged version goes with that version.
    fn read_routing(&mut self) -> FilesTurn {
        let routing = BucketRoutingOperation::new(self.routing.clone(), &self.bucket);
        self.call(FilesStep::Routing, IoCall::child(routing))
    }

    fn routing_read(&mut self, snapshot: Option<RoutingSnapshot>) -> FilesTurn {
        self.snapshot = snapshot;
        let mut keys: BTreeMap<String, Vec<Ulid>> = BTreeMap::new();
        for (key, id) in &self.versions {
            keys.entry(key.clone()).or_default().push(*id);
        }
        self.keys = keys.into_iter().collect();
        self.next_key()
    }

    fn next_key(&mut self) -> FilesTurn {
        let Some((key, ids)) = self.keys.pop_front() else {
            return self.collect_backends();
        };
        let place = (self.bucket.as_str(), key.as_str());
        let copies = SecondCopiesOperation::new(self.snapshot.as_ref(), place, &ids)?;
        self.copies_of = key;
        self.call(FilesStep::Copies, IoCall::child(copies))
    }

    fn copies_read(&mut self, copies: Vec<SecondCopy>) -> FilesTurn {
        let key = self.copies_of.clone();
        for (location, index, version_id) in copies {
            if !self.holds(&location) {
                self.push_file(key.clone(), version_id, location, Some(index));
            }
        }
        self.next_key()
    }

    fn collect_backends(&mut self) -> FilesTurn {
        let mut seen = Vec::new();
        for file in &self.files {
            let backend = file.location.backend.key_bytes();
            if !seen.contains(&backend) {
                seen.push(backend);
                self.backends.push_back(file.location.backend.clone());
            }
        }
        self.next_backend()
    }

    fn next_backend(&mut self) -> FilesTurn {
        let Some(backend) = self.backends.front().cloned() else {
            return self.read_file();
        };
        let rules = BackendRulesOperation::new(self.group_id, backend);
        self.call(FilesStep::Rules, IoCall::child(rules))
    }

    fn rules_read(&mut self, rules: Vec<BackendImport>) -> FilesTurn {
        if let Some(backend) = self.backends.pop_front() {
            self.rules.insert(backend.key_bytes(), rules);
        }
        self.next_backend()
    }

    fn file_rules(&self, file: &StorageFile) -> &[BackendImport] {
        let rules = self.rules.get(&file.location.backend.key_bytes());
        rules.map_or(&[], Vec::as_slice)
    }

    fn read_file(&mut self) -> FilesTurn {
        let Some(file) = self.files.get(self.at) else {
            return Ok(Next::Done(std::mem::take(&mut self.files)));
        };
        let mut reads = vec![(
            BACKEND_VERSION_KEYSPACE.to_string(),
            version_key(file.location.ulid),
        )];
        for rule in self.file_rules(file) {
            let row = object_key(rule.import_id, &file.location.backend_path);
            reads.push((IMPORTED_OBJECT_KEYSPACE.to_string(), row));
        }
        self.call(FilesStep::FileRows, IoCall::Read(reads))
    }

    fn next_file(&mut self) -> FilesTurn {
        self.at += 1;
        self.read_file()
    }

    fn file_read(&mut self, values: Vec<Option<Value>>) -> FilesTurn {
        let file = self.files.get(self.at).ok_or(IoError::Conflict)?;
        let import_ids: Vec<Ulid> = (self.file_rules(file).iter())
            .map(|rule| rule.import_id)
            .collect();
        let mut values = values.into_iter();
        let version = values.next().flatten();
        let version = version.map(|value| String::from_utf8(value.to_vec()));
        let version = version.transpose()?;
        let mut owners = Vec::new();
        for (import_id, row) in import_ids.into_iter().zip(values) {
            if let Some(row) = row.as_deref().map(ImportedObject::from_bytes).transpose()? {
                owners.push((import_id, row));
            }
        }
        let file = self.files.get_mut(self.at).ok_or(IoError::Conflict)?;
        // "null" names whatever the path holds later, never the file's bytes.
        file.version = version.filter(|version| version != "null");
        let own = owners.iter().position(|(_, row)| {
            row.key.as_deref() == Some(file.key.as_str()) || row.version_id == file.version_id
        });
        file.owner = own.map(|at| (owners[at].0, owners[at].1.version_id));
        // Without a backend version, only a file named by its version's id, whose ETag the
        // import recorded, goes; its release checks size and ETag first.
        let recorded = own.is_some_and(|at| owners[at].1.etag.is_some());
        let named =
            (file.owner).is_some_and(|(_, id)| names_version(&file.location.backend_path, id));
        if file.version.is_none() && named && recorded {
            file.version = Some(String::new());
        }
        let Some(scope) = self.pins.clone() else {
            return self.next_file();
        };
        if file.version.is_none() {
            file.kept = Some("the storage keeps no exact version of the file".to_string());
            return self.next_file();
        }
        if owners.len() > usize::from(own.is_some()) {
            file.kept = Some("another link records the file".to_string());
            return self.next_file();
        }
        let wanted = file.location.location_key()?;
        let elsewhere = ReadElsewhere::new(wanted, scope, self.budget);
        self.call(FilesStep::Elsewhere, IoCall::child(elsewhere))
    }

    fn elsewhere_read(&mut self, (answer, budget): (Option<bool>, usize)) -> FilesTurn {
        self.budget = budget;
        let file = self.files.get_mut(self.at).ok_or(IoError::Conflict)?;
        file.kept = match answer {
            Some(true) => Some("a version outside the deleted scope reads the file".to_string()),
            Some(false) => None,
            None => Some(UNCHECKED_READERS.to_string()),
        };
        self.next_file()
    }
}

impl Operation for StorageFilesOperation {
    type Output = Vec<StorageFile>;
    type Error = PurgeFilesError;

    fn first(&mut self) -> FilesTurn {
        let mut reads = Vec::with_capacity(self.versions.len());
        for (key, id) in &self.versions {
            let version = VersionKey::new(&self.bucket, key, *id).to_bytes()?;
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
        }
        if reads.is_empty() {
            return self.read_routing();
        }
        self.call(FilesStep::Versions, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> FilesTurn {
        match self.step {
            FilesStep::Versions => self.versions_read(result.values()?),
            FilesStep::Locations => self.locations_read(result.values()?),
            FilesStep::Routing => {
                let routing = result.child::<Option<RoutingSnapshot>, ImportRowsError>()?;
                self.routing_read(routing?)
            }
            FilesStep::Copies => {
                self.copies_read(result.child::<Vec<SecondCopy>, ImportRowsError>()??)
            }
            FilesStep::Rules => {
                self.rules_read(result.child::<Vec<BackendImport>, ImportRowsError>()??)
            }
            FilesStep::FileRows => self.file_read(result.values()?),
            FilesStep::Elsewhere => {
                self.elsewhere_read(result.child::<(Option<bool>, usize), PurgeFilesError>()??)
            }
        }
    }
}

type ElsewhereTurn = Result<Next<(Option<bool>, usize)>, PurgeFilesError>;

/// Whether a version of a key outside `scope` reads the file at `wanted`; `None` once the
/// aliases of the budget ran out first. Also outputs the budget left.
#[derive(Debug, PartialEq)]
struct ReadElsewhere {
    wanted: BlobLocationKey,
    scope: StoragePurgeScope,
    budget: usize,
    after: Option<Key>,
    more: bool,
    aliases: VecDeque<Key>,
    reading: bool,
}

impl ReadElsewhere {
    fn new(wanted: BlobLocationKey, scope: StoragePurgeScope, budget: usize) -> Self {
        Self {
            wanted,
            scope,
            budget,
            after: None,
            more: true,
            aliases: VecDeque::new(),
            reading: false,
        }
    }

    fn page(&mut self) -> ElsewhereTurn {
        if self.budget == 0 {
            return Ok(Next::Done((None, 0)));
        }
        Ok(Next::Call(IoCall::Page {
            space: PATHS_INDEX_KEYSPACE.to_string(),
            prefix: HashIndex::hash_prefix(&self.wanted.blake3_hash)?.into(),
            after: self.after.clone(),
            limit: ALIAS_PAGE.min(self.budget),
        }))
    }

    fn next_alias(&mut self) -> ElsewhereTurn {
        while let Some(key) = self.aliases.pop_front() {
            self.budget = self.budget.saturating_sub(1);
            let alias = HashIndex::from_bytes(&key)?;
            if self.scope.matches_key(&alias.bucket, &alias.key) {
                continue;
            }
            let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
            self.reading = true;
            let row = version.to_bytes()?;
            return Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, row)));
        }
        match self.more {
            true => self.page(),
            false => Ok(Next::Done((Some(false), self.budget))),
        }
    }
}

impl Operation for ReadElsewhere {
    type Output = (Option<bool>, usize);
    type Error = PurgeFilesError;

    fn first(&mut self) -> ElsewhereTurn {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> ElsewhereTurn {
        if !self.reading {
            let (rows, next) = result.page()?;
            self.aliases = rows.into_iter().map(|(key, _)| key).collect();
            self.more = next.is_some();
            self.after = next;
            return self.next_alias();
        }
        self.reading = false;
        if let Some(value) = result.value()? {
            let version = BlobVersion::from_bytes(&value)?;
            if version.location_key().as_ref() == Some(&self.wanted) {
                return Ok(Next::Done((Some(true), self.budget)));
            }
        }
        self.next_alias()
    }
}

/// Versions a deletion preview counts at most before its totals become lower bounds.
const TOTAL_VERSIONS: u64 = 5_000;
const LIST_PAGE: usize = 1000;

/// What a purge of a scope removes: keys and versions in Aruna, files in linked storage.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeTotals {
    pub keys: u64,
    pub versions: u64,
    /// Bytes of the versions' content in Aruna, each version counted.
    pub bytes: u64,
    pub storage_files: u64,
    pub storage_bytes: u64,
    /// `false` when the count stopped at its limit; the numbers are then lower bounds.
    pub complete: bool,
}

#[derive(Debug, PartialEq)]
enum TotalsStep {
    Routing,
    Listing,
    /// The second copies of the key that ended, before the rest of the walk.
    Copies(AfterCopies),
}

#[derive(Debug, PartialEq)]
enum AfterCopies {
    /// A version of the next key, with its location.
    Version(Ulid, Option<BackendLocation>),
    /// The end of the count; whether it covered everything.
    End(bool),
}

type TotalsTurn = Result<Next<ScopeTotals>, MigrationError>;

/// Counts what a purge of `scope` removes, from the version listing alone. Only keys `readable`
/// allows count.
#[derive(Debug, PartialEq)]
pub struct ScopeTotalsOperation<R> {
    routing: NodeRouting,
    scope: StoragePurgeScope,
    readable: R,
    step: TotalsStep,
    snapshot: Option<RoutingSnapshot>,
    totals: ScopeTotals,
    seen: HashSet<Ulid>,
    key_marker: Option<String>,
    version_marker: Option<Ulid>,
    last_key: Option<String>,
    scanned: u64,
    /// The versions of the key passed over last, whose second copies count once it ends.
    ids: Vec<Ulid>,
    items: VecDeque<ListVersionsItem>,
    truncated: bool,
}

impl<R: KeyRule> ScopeTotalsOperation<R> {
    pub fn new(routing: NodeRouting, scope: &StoragePurgeScope, readable: R) -> Self {
        Self {
            routing,
            scope: scope.clone(),
            readable,
            step: TotalsStep::Routing,
            snapshot: None,
            totals: ScopeTotals::default(),
            seen: HashSet::new(),
            key_marker: None,
            version_marker: None,
            last_key: None,
            scanned: 0,
            ids: Vec::new(),
            items: VecDeque::new(),
            truncated: false,
        }
    }

    fn finish(&mut self) -> TotalsTurn {
        Ok(Next::Done(std::mem::take(&mut self.totals)))
    }

    fn list(&mut self) -> TotalsTurn {
        let input = ListVersionsInput {
            bucket: self.scope.bucket().to_string(),
            prefix: self.scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: self.key_marker.take(),
            version_id_marker: self.version_marker.take(),
            max_keys: Some(LIST_PAGE),
        };
        // A listing may read one key past its page to tell whether more follow.
        let budget = (TOTAL_VERSIONS - self.scanned.min(TOTAL_VERSIONS)) as usize + LIST_PAGE;
        let listing = ListVersionsOperation::new(input).with_row_budget(budget);
        self.step = TotalsStep::Listing;
        Ok(Next::Call(IoCall::child(listing)))
    }

    /// Counts the second copies an import registered for the versions of the key that ended,
    /// then goes on with `after`.
    fn key_ended(&mut self, key: Option<String>, after: AfterCopies) -> TotalsTurn {
        let ids = std::mem::take(&mut self.ids);
        let Some(key) = key.filter(|_| !ids.is_empty()) else {
            return self.after_copies(after);
        };
        let scope = (self.scope.bucket(), key.as_str());
        let copies = SecondCopiesOperation::new(self.snapshot.as_ref(), scope, &ids)?;
        self.step = TotalsStep::Copies(after);
        Ok(Next::Call(IoCall::child(copies)))
    }

    fn after_copies(&mut self, after: AfterCopies) -> TotalsTurn {
        match after {
            AfterCopies::Version(id, location) => {
                self.count_version(id, location);
                self.next_item()
            }
            AfterCopies::End(complete) => {
                self.totals.complete = complete;
                self.finish()
            }
        }
    }

    fn count_file(&mut self, location: &BackendLocation) {
        if self.seen.insert(location.ulid) {
            self.totals.storage_files += 1;
            self.totals.storage_bytes += location.blob_size;
        }
    }

    fn count_version(&mut self, id: Ulid, location: Option<BackendLocation>) {
        self.ids.push(id);
        let Some(location) = location else {
            return;
        };
        self.totals.bytes += location.blob_size;
        if location.is_external() && !location.staging && !location.partial {
            self.count_file(&location);
        }
    }

    fn next_item(&mut self) -> TotalsTurn {
        while let Some(item) = self.items.pop_front() {
            let (key, id, location) = match item {
                ListVersionsItem::Version {
                    key,
                    version_id,
                    location,
                    ..
                } => (key, version_id, location),
                ListVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => (key, version_id, None),
            };
            // The bound counts every version passed over, also the hidden ones.
            self.scanned += 1;
            let bucket = self.scope.bucket();
            if !self.scope.matches_key(bucket, &key) || !self.readable.allows(&key) {
                continue;
            }
            self.totals.versions += 1;
            if self.last_key.as_deref() != Some(key.as_str()) {
                self.totals.keys += 1;
                let ended = self.last_key.replace(key);
                return self.key_ended(ended, AfterCopies::Version(id, location));
            }
            self.count_version(id, location);
        }
        self.page_done()
    }

    fn page_done(&mut self) -> TotalsTurn {
        let last = self.last_key.take();
        if self.scanned >= TOTAL_VERSIONS && self.truncated {
            return self.key_ended(last, AfterCopies::End(false));
        }
        if self.truncated && self.key_marker.is_some() {
            self.last_key = last;
            return self.list();
        }
        self.key_ended(last, AfterCopies::End(true))
    }
}

impl<R: KeyRule> Operation for ScopeTotalsOperation<R> {
    type Output = ScopeTotals;
    type Error = MigrationError;

    fn first(&mut self) -> TotalsTurn {
        let call = routing_call(self.routing.clone(), self.scope.bucket());
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> TotalsTurn {
        match std::mem::replace(&mut self.step, TotalsStep::Listing) {
            TotalsStep::Routing => {
                self.snapshot = routing_result(result)?;
                self.list()
            }
            TotalsStep::Listing => match result.child::<ListVersionsResult, ListVersionsError>()? {
                Ok(page) => {
                    self.items = page.items.into();
                    self.truncated = page.is_truncated;
                    self.key_marker = page.next_key_marker;
                    self.version_marker = page.next_version_marker;
                    self.next_item()
                }
                // A key with more versions than the budget leaves the totals as lower bounds.
                Err(ListVersionsError::TooManyVersions) => self.finish(),
                Err(error) => Err(MigrationError::failed(error)),
            },
            TotalsStep::Copies(after) => {
                let copies = (result.child::<Vec<SecondCopy>, ImportRowsError>()?)
                    .map_err(MigrationError::failed)?;
                for (file, ..) in &copies {
                    self.count_file(file);
                }
                self.after_copies(after)
            }
        }
    }
}

fn plan_key(job_id: Ulid, location: Ulid) -> Key {
    [&job_id.to_bytes()[..], b"f", &location.to_bytes()[..]]
        .concat()
        .into()
}

/// Plans the release of `files` in the cleanup queue before their versions go; the queue
/// releases them once the purge has ended, whichever way. A file without a backend version
/// is not planned.
#[derive(Debug, PartialEq)]
pub struct PlanFilesOperation {
    job_id: Ulid,
    files: Vec<StorageFile>,
}

impl PlanFilesOperation {
    pub fn new(job_id: Ulid, files: &[StorageFile]) -> Self {
        Self {
            job_id,
            files: files.to_vec(),
        }
    }
}

impl Operation for PlanFilesOperation {
    type Output = ();
    type Error = PurgeFilesError;

    fn first(&mut self) -> Result<Next<()>, PurgeFilesError> {
        let mut writes = Vec::new();
        for file in &self.files {
            let Some(work) = file.work(self.job_id) else {
                continue;
            };
            let key = plan_key(self.job_id, file.location.ulid);
            let value = work.to_bytes()?.into();
            writes.push((BLOB_CLEANUP_KEYSPACE.to_string(), key, value));
        }
        if writes.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PurgeFilesError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
#[path = "purge_tests.rs"]
mod tests;
