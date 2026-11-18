//! Counts the versions of a bucket that a storage move would move now, and the linked files
//! it would rename.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BROKEN_VERSION_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, copy_provable, object_key, object_prefix,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobLocationKey, BlobVersion, VersionKey,
};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
use aruna_core::structs::storage::routing::{NodeRouting, PlainTarget, RoutingSnapshot};
use aruna_core::types::{Key, Value};

use super::shared::{
    EarlierPatternsOperation, MigrationError, encryption_read, is_encrypted, routing_call,
    routing_result,
};
use super::{Earlier, place, renames, source_key, stays};
use crate::auth::request_authorization::BucketAccess;
use crate::groups::backends::backend_key;

/// Versions under the prefix one count reads at most, so a request stays bounded.
const COUNT_LIMIT: usize = 10_000;
/// Version rows of the bucket one count passes over at most, under the prefix or not.
const SCAN_LIMIT: usize = 200_000;
const COUNT_PAGE: usize = 256;

type Turn<T> = Result<Next<T>, MigrationError>;

/// Versions under a prefix that are not where the rules place a new write of their key.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Misplaced {
    pub count: u64,
    /// `false` when the count stopped at its limit and more versions exist.
    pub complete: bool,
    /// Linked files of an earlier pattern among them, which a move gives their current name.
    pub renamed: u64,
    /// The first of them, as the old and the new path.
    pub renames: Vec<(String, String)>,
    /// Renames of keys the caller may not use; they are neither counted nor listed.
    pub withheld: u64,
}

/// A version the count still has to look at.
#[derive(Debug, PartialEq)]
struct CountRow {
    key: VersionKey,
    plain: Option<PlainTarget>,
    source: BlobLocationKey,
    target: (BackendRef, EncodingClass),
}

#[derive(Debug, PartialEq)]
enum CountStep {
    Encryption,
    Routing,
    Page,
    Broken(CountRow),
    Location(CountRow),
    Linked(VersionKey),
}

/// Counts what a move of `prefix` would move, with the move's own skips: an encrypted bucket
/// moves nothing; linked-folder files, unfinished copies and recorded broken versions stay.
#[derive(Debug, PartialEq)]
pub struct CountMisplacedOperation {
    routing: NodeRouting,
    bucket: String,
    prefix: String,
    step: CountStep,
    snapshot: Option<RoutingSnapshot>,
    after: Option<Key>,
    rows: VecDeque<(Key, Value)>,
    more: Option<Key>,
    seen: usize,
    scanned: usize,
    misplaced: Misplaced,
}

impl CountMisplacedOperation {
    pub fn new(routing: NodeRouting, bucket: &str, prefix: &str) -> Self {
        Self {
            routing,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            step: CountStep::Encryption,
            snapshot: None,
            after: None,
            rows: VecDeque::new(),
            more: None,
            seen: 0,
            scanned: 0,
            misplaced: Misplaced {
                complete: true,
                ..Misplaced::default()
            },
        }
    }

    fn finish(&mut self) -> Turn<Misplaced> {
        Ok(Next::Done(std::mem::take(&mut self.misplaced)))
    }

    fn incomplete(&mut self) -> Turn<Misplaced> {
        self.misplaced.complete = false;
        self.finish()
    }

    fn full(&self) -> bool {
        self.seen >= COUNT_LIMIT || self.scanned >= SCAN_LIMIT
    }

    fn read_page(&mut self) -> Turn<Misplaced> {
        self.step = CountStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: VersionKey::bucket_prefix(&self.bucket)?.into(),
            after: self.after.take(),
            limit: COUNT_PAGE,
        }))
    }

    fn next_row(&mut self) -> Turn<Misplaced> {
        let Some(routing) = self.snapshot.as_ref() else {
            return Err(no_routing());
        };
        while let Some((key, value)) = self.rows.pop_front() {
            if self.seen >= COUNT_LIMIT || self.scanned >= SCAN_LIMIT {
                return self.incomplete();
            }
            self.scanned += 1;
            let version_key = VersionKey::from_bytes(key.as_ref())?;
            if !version_key.key.starts_with(&self.prefix) {
                continue;
            }
            self.seen += 1;
            let version = BlobVersion::from_bytes(value.as_ref())?;
            // A key the rules cannot place now fails its move; it is not counted.
            let Ok((resolved, plain)) = place(routing, &version_key) else {
                continue;
            };
            let target = (resolved.backend.clone(), resolved.compression.into());
            let wanted = |_: &_| target.clone();
            let Some(source) = source_key(&version, wanted, plain.is_some()) else {
                continue;
            };
            let row = CountRow {
                key: version_key,
                plain,
                source,
                target,
            };
            self.step = CountStep::Broken(row);
            return Ok(Next::Call(read_row(BROKEN_VERSION_KEYSPACE, key)));
        }
        match self.more.take() {
            Some(next) if !self.full() => {
                self.after = Some(next);
                self.read_page()
            }
            Some(_) => self.incomplete(),
            None => self.finish(),
        }
    }

    fn counted(&mut self) -> Turn<Misplaced> {
        self.misplaced.count += 1;
        self.next_row()
    }

    fn location_read(&mut self, row: CountRow, value: Option<Value>) -> Turn<Misplaced> {
        let Some(value) = value else {
            return self.next_row();
        };
        let location = BackendLocation::from_bytes(value.as_ref())?;
        let Some(routing) = self.snapshot.as_ref() else {
            return Err(no_routing());
        };
        // Only a move asked to rename gives linked files their current name, and this count
        // stands for a move that was not.
        let earlier = Earlier::new();
        let CountRow {
            key,
            plain,
            source,
            target,
        } = row;
        let renamed = (plain.as_ref())
            .is_some_and(|plain| renames(routing, &earlier, plain, &location, &key));
        if renamed {
            self.misplaced.withheld += 1;
            return self.next_row();
        }
        if stays(&location, true) {
            return self.next_row();
        }
        // A copy in place with its linked file recorded for this version is done.
        match plain.filter(|_| (source.backend.clone(), source.encoding) == target) {
            Some(plain) => {
                let read = read_row(
                    IMPORTED_OBJECT_KEYSPACE,
                    object_key(plain.import_id, &plain.path),
                );
                self.step = CountStep::Linked(key);
                Ok(Next::Call(read))
            }
            None => self.counted(),
        }
    }
}

fn no_routing() -> MigrationError {
    MigrationError::Failed("the move has no routing".to_string())
}

impl Operation for CountMisplacedOperation {
    type Output = Misplaced;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Misplaced> {
        Ok(Next::Call(encryption_read(&self.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Misplaced> {
        match std::mem::replace(&mut self.step, CountStep::Page) {
            CountStep::Encryption => {
                if is_encrypted(result.value()?)? {
                    return self.finish();
                }
                self.step = CountStep::Routing;
                let call = routing_call(self.routing.clone(), &self.bucket);
                Ok(Next::Call(call))
            }
            CountStep::Routing => {
                self.snapshot = routing_result(result)?;
                match self.snapshot {
                    Some(_) => self.read_page(),
                    None => self.finish(),
                }
            }
            CountStep::Page => {
                let (rows, more) = result.page()?;
                self.rows = rows.into();
                self.more = more;
                self.next_row()
            }
            CountStep::Broken(row) => match result.value()? {
                Some(_) => self.next_row(),
                None => {
                    let read = read_row(BLOB_LOCATIONS_KEYSPACE, row.source.to_bytes());
                    self.step = CountStep::Location(row);
                    Ok(Next::Call(read))
                }
            },
            CountStep::Location(row) => self.location_read(row, result.value()?),
            CountStep::Linked(key) => {
                let row = result.value()?;
                let row = (row.as_deref())
                    .map(ImportedObject::from_bytes)
                    .transpose()?;
                match row.is_some_and(|row| row.version_id == key.version_id) {
                    true => self.next_row(),
                    false => self.counted(),
                }
            }
        }
    }
}

/// Entries one rename preview page shows by default and at most.
pub const DEFAULT_PAGE: usize = 100;
pub const MAX_PAGE: usize = 1000;
/// Index rows one storage read returns, so other requests go on between the reads.
const INDEX_PAGE: usize = 1024;

/// Decides per key what a rename plan shows, or counts as writable.
pub trait KeyRule: Send + std::fmt::Debug + PartialEq + 'static {
    fn allows(&self, key: &str) -> bool;
}

/// Every key.
#[derive(Debug, PartialEq)]
pub struct AnyKey;

impl KeyRule for AnyKey {
    fn allows(&self, _key: &str) -> bool {
        true
    }
}

/// The keys the caller holds `permission` on.
#[derive(Debug, PartialEq)]
pub struct AccessRule {
    access: Arc<BucketAccess>,
    permission: Permission,
}

impl AccessRule {
    pub fn new(access: Arc<BucketAccess>, permission: Permission) -> Self {
        Self { access, permission }
    }
}

impl KeyRule for AccessRule {
    fn allows(&self, key: &str) -> bool {
        self.access.allows(Some(key), &self.permission)
    }
}

/// What a rename of a rule's files would do, as far as the caller may read their keys.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenamePlan {
    /// Files an earlier pattern named for their version, and their bytes.
    pub files: u64,
    pub bytes: u64,
    /// Of them, files whose copy passes through the node unless the provider keeps a version.
    pub streamed_files: u64,
    pub streamed_bytes: u64,
    /// Of them, files whose new name another file of the rule already holds.
    pub taken_files: u64,
    /// Of them, files the caller may not write; they keep their name.
    pub withheld_files: u64,
    /// The rule's other files, which keep their name.
    pub unchanged_files: u64,
    pub unchanged_bytes: u64,
    /// Always `true`: the plan covers every file of the rule.
    pub complete: bool,
}

/// The plan of a rename and one page of the files it renames.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenamePage {
    pub plan: RenamePlan,
    /// The old and the new path, and whether the caller may write the key.
    pub entries: Vec<(String, String, bool)>,
    /// The index row of the last listed file, after which the next page starts.
    pub next: Option<Vec<u8>>,
}

/// Which files of a rename plan are listed.
#[derive(Debug, Default, PartialEq)]
pub struct RenameQuery {
    /// Only keys holding this text count.
    pub filter: Option<String>,
    /// The index row to list after.
    pub after: Option<Vec<u8>>,
    pub limit: usize,
}

/// An index row a rename may touch.
#[derive(Debug, PartialEq)]
struct Candidate {
    row_key: Key,
    key: String,
    path: String,
    row: ImportedObject,
    to: String,
}

#[derive(Debug, PartialEq)]
enum PlanStep {
    Encryption,
    Routing,
    Earlier,
    Backend,
    Index,
    Versions,
    Locations,
    Taken,
}

/// Plans a rename of `rule`'s files from the rule's own index in one pass, without asking
/// the provider, and lists up to `limit` of them in path order after the row `after`, each
/// checked against its version. Keys `readable` refuses are left out.
#[derive(Debug, PartialEq)]
pub struct RenamePageOperation<R, W> {
    routing: NodeRouting,
    rule: BackendImport,
    readable: R,
    writable: W,
    query: RenameQuery,
    step: PlanStep,
    snapshot: Option<RoutingSnapshot>,
    earlier: Earlier,
    native: bool,
    index: PrefixPages,
    candidates: Vec<Candidate>,
    keys: Vec<VersionKey>,
    places: Vec<(usize, PlainTarget)>,
    renames: Vec<(String, ImportedObject, String)>,
    page: RenamePage,
}

impl<R: KeyRule, W: KeyRule> RenamePageOperation<R, W> {
    pub fn new(
        routing: NodeRouting,
        rule: &BackendImport,
        (readable, writable): (R, W),
        query: RenameQuery,
    ) -> Self {
        let index = PrefixPages::new(
            IMPORTED_OBJECT_KEYSPACE,
            object_prefix(rule.import_id),
            INDEX_PAGE,
        );
        let mut page = RenamePage::default();
        page.plan.complete = true;
        Self {
            routing,
            rule: rule.clone(),
            readable,
            writable,
            query,
            step: PlanStep::Encryption,
            snapshot: None,
            earlier: Earlier::new(),
            native: false,
            index,
            candidates: Vec::new(),
            keys: Vec::new(),
            places: Vec::new(),
            renames: Vec::new(),
            page,
        }
    }

    fn call(&mut self, step: PlanStep, call: IoCall) -> Turn<RenamePage> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn finish(&mut self) -> Turn<RenamePage> {
        Ok(Next::Done(std::mem::take(&mut self.page)))
    }

    /// The rule as the move reads it; a removed rule renames nothing.
    fn routed(&mut self, snapshot: Option<RoutingSnapshot>) -> Turn<RenamePage> {
        let Some(snapshot) = snapshot else {
            return self.finish();
        };
        let import_id = self.rule.import_id;
        let rule = snapshot.imports.iter().find(|r| r.import_id == import_id);
        let Some(rule) = rule.cloned() else {
            return self.finish();
        };
        self.rule = rule;
        let earlier = EarlierPatternsOperation::new(&snapshot);
        self.snapshot = Some(snapshot);
        self.call(PlanStep::Earlier, IoCall::child(earlier))
    }

    fn shown(&self, key: &str) -> bool {
        key.starts_with(&self.rule.key_prefix)
            && (self.query.filter.as_deref()).is_none_or(|filter| key.contains(filter))
            && self.readable.allows(key)
    }

    fn read_index(&mut self) -> Turn<RenamePage> {
        let page = self.index.call();
        self.call(PlanStep::Index, page)
    }

    fn index_read(&mut self, rows: Vec<(Key, Value)>) -> Turn<RenamePage> {
        let Some(routing) = self.snapshot.as_ref() else {
            return Err(no_routing());
        };
        let patterns = (self.earlier.get(&self.rule.import_id)).map_or(&[][..], Vec::as_slice);
        let mut candidates = Vec::new();
        for (row_key, value) in rows {
            let (key, path, row) = row_parts(&self.rule, (&row_key, &value))?;
            let Some(key) = key.filter(|key| self.shown(key)) else {
                continue;
            };
            // A mapped key names its file by the storage side of the mapping.
            let named = routing.storage_key(&self.rule, &key);
            match self
                .rule
                .rename_target(patterns, &path, &named, row.version_id)
            {
                Some(to) => candidates.push(Candidate {
                    row_key,
                    key,
                    path,
                    row,
                    to,
                }),
                None => {
                    self.page.plan.unchanged_files += 1;
                    self.page.plan.unchanged_bytes += row.size;
                }
            }
        }
        self.candidates = candidates;
        self.read_versions()
    }

    /// Only a file its version still reads is work; any other record keeps its name.
    fn read_versions(&mut self) -> Turn<RenamePage> {
        if self.candidates.is_empty() {
            return self.page_done();
        }
        let bucket = &self.rule.bucket;
        self.keys = (self.candidates.iter())
            .map(|found| VersionKey::new(bucket, &found.key, found.row.version_id))
            .collect();
        let mut reads = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), key.to_bytes()?.into()));
        }
        self.call(PlanStep::Versions, IoCall::Read(reads))
    }

    /// The copy each version reads, where a move would take it.
    fn versions_read(&mut self, values: Vec<Option<Value>>) -> Turn<RenamePage> {
        let Some(routing) = self.snapshot.as_ref() else {
            return Err(no_routing());
        };
        let mut reads = Vec::new();
        for (index, (key, value)) in self.keys.iter().zip(values).enumerate() {
            let Some(value) = value else {
                continue;
            };
            let version = BlobVersion::from_bytes(&value)?;
            let Ok((resolved, Some(plain))) = place(routing, key) else {
                continue;
            };
            let target = (resolved.backend.clone(), resolved.compression.into());
            if let Some(source) = source_key(&version, |_: &_| target.clone(), true) {
                reads.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    source.to_bytes().into(),
                ));
                self.places.push((index, plain));
            }
        }
        if reads.is_empty() {
            return self.apply_found(vec![None; self.candidates.len()]);
        }
        self.call(PlanStep::Locations, IoCall::Read(reads))
    }

    fn locations_read(&mut self, values: Vec<Option<Value>>) -> Turn<RenamePage> {
        let Some(routing) = self.snapshot.as_ref() else {
            return Err(no_routing());
        };
        let mut found = vec![None; self.candidates.len()];
        for ((index, plain), value) in std::mem::take(&mut self.places).into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let location = BackendLocation::from_bytes(&value)?;
            let candidate = &self.candidates[index];
            let renamed = location.backend_path == candidate.path
                && plain.path == candidate.to
                && renames(routing, &self.earlier, &plain, &location, &self.keys[index]);
            found[index] = renamed.then_some(location);
        }
        self.apply_found(found)
    }

    fn apply_found(&mut self, found: Vec<Option<BackendLocation>>) -> Turn<RenamePage> {
        let candidates = std::mem::take(&mut self.candidates);
        for (candidate, found) in candidates.into_iter().zip(found) {
            let Candidate {
                row_key,
                key,
                path,
                row,
                to,
            } = candidate;
            let plan = &mut self.page.plan;
            let Some(location) = found else {
                plan.unchanged_files += 1;
                plan.unchanged_bytes += row.size;
                continue;
            };
            plan.files += 1;
            plan.bytes += row.size;
            // Like the copy: on S3 a recorded ETag or a one-part MD5 keeps it in the provider.
            if !self.native || (row.etag.is_none() && !copy_provable(&location)) {
                plan.streamed_files += 1;
                plan.streamed_bytes += row.size;
            }
            let writable = self.writable.allows(&key);
            plan.withheld_files += u64::from(!writable);
            let after = self.query.after.as_deref();
            let listed = after.is_none_or(|after| row_key.as_ref() > after);
            if listed && self.page.entries.len() < self.query.limit {
                self.page.entries.push((path, to.clone(), writable));
                self.page.next = Some(row_key.to_vec());
            }
            self.renames.push((key, row, to));
        }
        if self.renames.is_empty() {
            return self.page_done();
        }
        let import_id = self.rule.import_id;
        let reads = (self.renames.iter())
            .map(|(_, _, to)| {
                let row = object_key(import_id, to);
                (IMPORTED_OBJECT_KEYSPACE.to_string(), row)
            })
            .collect();
        self.call(PlanStep::Taken, IoCall::Read(reads))
    }

    /// Counts renames whose new path a record of the rule keeps for another key or version.
    fn taken_read(&mut self, values: Vec<Option<Value>>) -> Turn<RenamePage> {
        let renames = std::mem::take(&mut self.renames);
        for ((key, row, to), held) in renames.iter().zip(values) {
            let Some(held) = held else {
                continue;
            };
            let held = ImportedObject::from_bytes(&held)?;
            let holder = held.key.clone().or_else(|| self.rule.aruna_key(to));
            let other = holder.as_deref() != Some(key.as_str());
            self.page.plan.taken_files += u64::from(other || held.version_id != row.version_id);
        }
        self.page_done()
    }

    fn page_done(&mut self) -> Turn<RenamePage> {
        if !self.index.done() {
            return self.read_index();
        }
        // A cursor only when more files may follow the last listed one.
        if self.page.entries.len() < self.query.limit {
            self.page.next = None;
        }
        self.finish()
    }
}

impl<R: KeyRule, W: KeyRule> Operation for RenamePageOperation<R, W> {
    type Output = RenamePage;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<RenamePage> {
        Ok(Next::Call(encryption_read(&self.rule.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Turn<RenamePage> {
        match self.step {
            PlanStep::Encryption => {
                if is_encrypted(result.value()?)? {
                    return self.finish();
                }
                let call = routing_call(self.routing.clone(), &self.rule.bucket);
                self.call(PlanStep::Routing, call)
            }
            PlanStep::Routing => self.routed(routing_result(result)?),
            PlanStep::Earlier => {
                self.earlier = result.child::<Earlier, MigrationError>()??;
                // Only S3 copies inside the provider; other storages pass files through the node.
                let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(self.rule.backend_id));
                self.call(PlanStep::Backend, read)
            }
            PlanStep::Backend => {
                let storage = result.value()?;
                let storage = (storage.as_deref())
                    .map(GroupStorage::from_bytes)
                    .transpose()?;
                self.native = storage.is_some_and(|storage| storage.kind == GroupBackendKind::S3);
                self.read_index()
            }
            PlanStep::Index => {
                let rows = self.index.take(result)?;
                self.index_read(rows)
            }
            PlanStep::Versions => self.versions_read(result.values()?),
            PlanStep::Locations => self.locations_read(result.values()?),
            PlanStep::Taken => self.taken_read(result.values()?),
        }
    }
}

/// The key an index row keeps its file at, the file's path and the record.
fn row_parts(
    rule: &BackendImport,
    (row_key, value): (&[u8], &[u8]),
) -> Result<(Option<String>, String, ImportedObject), MigrationError> {
    let path = row_key
        .get(object_prefix(rule.import_id).len()..)
        .unwrap_or_default();
    let path = String::from_utf8(path.to_vec()).map_err(MigrationError::failed)?;
    let row = ImportedObject::from_bytes(value)?;
    // An older row without a key keeps the rule's own mapping.
    let key = row.key.clone().or_else(|| rule.aruna_key(&path));
    Ok((key, path, row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive, node_routing};
    use crate::s3::bucket::rename::tests::{BUCKET, group, put, rule, setup};
    use aruna_core::keyspaces::{
        BACKEND_INDEX_KEYSPACE, IMPORT_PATTERN_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
    use aruna_core::structs::storage::backend_import::{COPY_PART, write_patterns};
    use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
    use aruna_core::structs::storage::format::{
        Compression, EncodingClass, StoredFormat, StoredLayout,
    };
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::routing::{RoutingTarget, StorageRoutingRule};
    use std::time::SystemTime;
    use ulid::Ulid;

    /// A bucket that writes to the rule's storage of `kind`, which the group holds, and the
    /// rule's earlier pattern `{dir}{id}_{name}`.
    async fn linked_bucket(context: &DriverContext, kind: GroupBackendKind) {
        let backend = rule().backend_id;
        let target = RoutingTarget::Backend(BackendRef::Group(backend));
        let info = BucketInfo {
            group_id: group(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: vec![StorageRoutingRule {
                key_prefix: String::new(),
                exact: false,
                target,
            }],
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let name = BUCKET.as_bytes().to_vec();
        put(context, S3_BUCKET_KEYSPACE, name, info.to_bytes().unwrap()).await;
        let storage = GroupStorage {
            backend_id: backend,
            group_id: group(),
            name: "lab".to_string(),
            kind,
            public_config: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: Default::default(),
        };
        let index = crate::groups::backends::index_key(group(), backend).to_vec();
        put(
            context,
            BACKEND_INDEX_KEYSPACE,
            index,
            storage.to_bytes().unwrap(),
        )
        .await;
        let record = crate::groups::backends::backend_key(backend).to_vec();
        let value = storage.to_bytes().unwrap();
        put(context, STORAGE_BACKEND_KEYSPACE, record, value).await;
        let earlier = write_patterns(&["{dir}{id}_{name}".to_string()]).unwrap();
        let patterns = object_prefix(rule().import_id).to_vec();
        put(context, IMPORT_PATTERN_KEYSPACE, patterns, earlier).await;
    }

    /// Version `id` of `key` reads the linked file at `path`, which the rule's index records.
    async fn linked_file(context: &DriverContext, (key, id): (&str, Ulid), path: &str, size: u64) {
        let backend = BackendRef::Group(rule().backend_id);
        let hash = [size as u8; 32];
        let location = BackendLocation {
            backend: backend.clone(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: path.to_string(),
            ulid: id,
            format: StoredFormat {
                layout: StoredLayout::Plain(id),
                ..StoredFormat::default()
            },
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: [
                (HASH_BLAKE3.to_string(), hash.to_vec()),
                (HASH_MD5.to_string(), hash[..16].to_vec()),
            ]
            .into(),
        };
        let row = location.location_key().unwrap().to_bytes();
        put(
            context,
            BLOB_LOCATIONS_KEYSPACE,
            row,
            location.to_bytes().unwrap(),
        )
        .await;
        let encoding = EncodingClass::Plain { location: id };
        let at = SystemTime::UNIX_EPOCH;
        let version =
            BlobVersion::materialized(hash, backend, encoding, at, Default::default(), None);
        let version_key = VersionKey::new(BUCKET, key, id).to_bytes().unwrap();
        put(
            context,
            BLOB_VERSIONS_KEYSPACE,
            version_key,
            version.to_bytes().unwrap(),
        )
        .await;
        recorded(context, (key, id), path, size).await;
    }

    /// The rule's index records the file at `path` for version `id` of `key`.
    async fn recorded(context: &DriverContext, (key, id): (&str, Ulid), path: &str, size: u64) {
        let recorded = ImportedObject::written(id, key.to_string(), size, None);
        let index = object_key(rule().import_id, path).to_vec();
        put(
            context,
            IMPORTED_OBJECT_KEYSPACE,
            index,
            recorded.to_bytes().unwrap(),
        )
        .await;
    }

    #[derive(Debug, PartialEq)]
    struct NotSecret;

    impl KeyRule for NotSecret {
        fn allows(&self, key: &str) -> bool {
            !key.starts_with("secret/")
        }
    }

    #[derive(Debug, PartialEq)]
    struct NotB;

    impl KeyRule for NotB {
        fn allows(&self, key: &str) -> bool {
            key != "b.txt"
        }
    }

    async fn rename_page(
        context: &DriverContext,
        (filter, after, limit): (Option<&str>, Option<Vec<u8>>, usize),
    ) -> Result<RenamePage, MigrationError> {
        let query = RenameQuery {
            filter: filter.map(str::to_string),
            after,
            limit,
        };
        let routing = node_routing(context);
        let page = RenamePageOperation::new(routing, &rule(), (NotSecret, NotB), query);
        drive(page, context).await
    }

    fn plain_bucket() -> IoResult {
        IoResult::Values(vec![None])
    }

    #[test]
    fn gone_bucket_counts() {
        let mut count = CountMisplacedOperation::new(NodeRouting::default(), BUCKET, "");
        assert!(matches!(count.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = count.next(plain_bucket());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let routed: Result<Option<RoutingSnapshot>, crate::blob::import_rows::ImportRowsError> =
            Ok(None);
        let answer = IoResult::Child(aruna_core::calls::ChildOutcome::new(routed));
        let expected = Misplaced {
            complete: true,
            ..Misplaced::default()
        };
        assert_eq!(count.next(answer), Ok(Next::Done(expected)));
    }

    #[test]
    fn gone_bucket_plans() {
        let query = RenameQuery::default();
        let rules = (AnyKey, AnyKey);
        let mut plan = RenamePageOperation::new(NodeRouting::default(), &rule(), rules, query);
        assert!(matches!(plan.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = plan.next(plain_bucket());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let routed: Result<Option<RoutingSnapshot>, crate::blob::import_rows::ImportRowsError> =
            Ok(None);
        let answer = IoResult::Child(aruna_core::calls::ChildOutcome::new(routed));
        let Ok(Next::Done(page)) = plan.next(answer) else {
            panic!("a bucket without routing has no plan");
        };
        assert!(page.plan.complete && page.entries.is_empty());
    }

    #[tokio::test]
    async fn plans_indexed_renames() {
        // The plan reads the rule's index and counts only files their version still reads.
        let (_directory, context) = setup().await;
        linked_bucket(&context, GroupBackendKind::S3).await;
        let id = |at: u64| Ulid::from_parts(at, 1);
        let files = [
            ("a.txt", id(11), format!("raw/{}_a.txt", id(11)), 3),
            ("b.txt", id(12), format!("raw/{}_b.txt", id(12)), 5),
            (
                "secret/c.txt",
                id(13),
                format!("raw/secret/{}_c.txt", id(13)),
                7,
            ),
            ("d.txt", id(14), format!("raw/d.txt~{}", id(14)), 9),
            ("e.txt", id(15), "raw/e.txt".to_string(), 11),
        ];
        for (key, id, path, size) in &files {
            linked_file(&context, (key, *id), path, *size).await;
        }
        // A name whose id is not its version's names no version of it.
        recorded(
            &context,
            ("f.txt", id(17)),
            &format!("raw/{}_f.txt", id(16)),
            13,
        )
        .await;
        // Another file already holds the new name of `b.txt`.
        recorded(
            &context,
            ("g.txt", id(18)),
            &format!("raw/b.txt~{}", id(12)),
            1,
        )
        .await;
        let large = COPY_PART + 1;
        let path = format!("raw/{}_h.bin", id(19));
        linked_file(&context, ("h.bin", id(19)), &path, large).await;
        // A record whose version is gone is no work; its file keeps its name.
        let stale = format!("raw/{}_i.txt", id(20));
        recorded(&context, ("i.txt", id(20)), &stale, 17).await;
        let renamed = |key: &str, at: u64| {
            let from = format!("raw/{}_{key}", id(at));
            (from, format!("raw/{key}~{}", id(at)))
        };

        let page = rename_page(&context, (None, None, 1)).await.unwrap();
        let plan = RenamePlan {
            files: 3,
            bytes: 8 + large,
            streamed_files: 1,
            streamed_bytes: large,
            taken_files: 1,
            withheld_files: 1,
            unchanged_files: 5,
            unchanged_bytes: 51,
            complete: true,
        };
        assert_eq!(page.plan, plan);
        let (from, to) = renamed("a.txt", 11);
        assert_eq!(page.entries, vec![(from, to, true)]);
        let rest = rename_page(&context, (None, page.next, 1)).await;
        let rest = rest.unwrap();
        let (from, to) = renamed("b.txt", 12);
        assert_eq!(rest.entries, vec![(from, to, false)]);
        let large = rename_page(&context, (None, rest.next, 1)).await;
        let large = large.unwrap();
        assert_eq!(
            large.entries,
            vec![(path, format!("raw/h.bin~{}", id(19)), true)]
        );
        // The stale record and the hidden key are not listed; no cursor follows.
        let last = rename_page(&context, (None, large.next, 1)).await;
        let last = last.unwrap();
        assert_eq!((last.entries.len(), last.next), (0, None));
        // A filter limits the plan and the pages alike.
        let filtered = (Some("b.txt"), None, 10);
        let page = rename_page(&context, filtered).await.unwrap();
        let plan = (page.plan.files, page.plan.bytes, page.plan.taken_files);
        assert_eq!((plan, page.plan.unchanged_files), ((1, 5, 1), 0));
        assert_eq!((page.entries.len(), page.next), (1, None));
        // A storage other than S3 passes every renamed file through the node.
        linked_bucket(&context, GroupBackendKind::Gcs).await;
        let page = rename_page(&context, (None, None, 1)).await;
        let plan = page.unwrap().plan;
        let streamed = (plan.streamed_files, plan.streamed_bytes);
        assert_eq!(streamed, (3, 8 + COPY_PART + 1));
    }
}
