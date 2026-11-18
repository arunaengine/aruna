//! Reads the rows one backend import decides on: page rows, write intents, delete trails and
//! the versions of a key, each as a small operation or a call with its decoding.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DELETE_TRAIL_KEYSPACE,
    IMPORT_INTENT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, values};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ExternalObject, ImportedObject, WriteIntent, object_key, object_prefix,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
    ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::delete_audit::BlobAuditRecord;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::{
    Claim, Exact, Hashed, ImportJob, ImportRun, ItemError, LIST_PAGE, PageReads, TWIN_SCAN,
    etag_differs, free_path, hash_failure, item, parse, read_listed, write_intents,
};
use crate::jobs::blob_request::BlobRequest;
use crate::jobs::import_listing::{ListingNote, NoteError, NoteListingOperation};
use crate::jobs::page_intents::intent_path;
use crate::s3::imports::shared::SharedRule;
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use crate::s3::object::put::{Trail, trail_ranges};

/// Reads these rows in one batch.
pub(super) fn read_call<'a>(reads: impl IntoIterator<Item = (&'a str, Key)>) -> IoCall {
    let reads = reads
        .into_iter()
        .map(|(key_space, key)| (key_space.to_string(), key))
        .collect();
    IoCall::Read(reads)
}

/// Reads the head of `key`.
pub(super) fn head_call(bucket: &str, key: &str) -> Result<IoCall, ItemError> {
    let head_key = BlobHeadKey::new(bucket, key)
        .to_bytes()
        .map_err(|error| item(error.to_string()))?;
    Ok(read_call([(BLOB_HEAD_KEYSPACE, head_key.into())]))
}

/// The answer to [`head_call`].
pub(super) fn head_of(result: IoResult) -> Result<Option<Ulid>, ItemError> {
    let [head] = values::<1>(result)?;
    Ok(parse(head, CurrentVersionPointer::from_bytes)?.map(|head| head.version_id))
}

pub(super) fn version_row(bucket: &str, key: &str, version_id: Ulid) -> Result<Key, ItemError> {
    let row = VersionKey::new(bucket, key, version_id).to_bytes();
    Ok(row.map_err(|error| item(error.to_string()))?.into())
}

/// The state of version `version_id` of `key`, from its stored row.
pub(super) fn exact_of(value: Option<Value>) -> Result<Exact, ItemError> {
    let Some(value) = value else {
        return Ok(Exact::Absent);
    };
    let version = BlobVersion::from_bytes(&value);
    let version = version.map_err(|error| ItemError::System(error.to_string()))?;
    Ok(match version.blob_hash() {
        Some(hash) => Exact::Materialized(*hash, value),
        None => Exact::Incompatible,
    })
}

/// Hashes the listed `object` on its backend.
pub(super) fn hash_call(
    job: &ImportJob,
    resolved: ResolvedBackend,
    object: &ExternalObject,
) -> IoCall {
    IoCall::child(BlobRequest::new(BlobEffect::HashExternal {
        resolved,
        path: object.key.clone(),
        created_by: job.spec.auth_context.user_id,
    }))
}

/// The answer to [`hash_call`]; bytes with another ETag than the listing's are left for the
/// next run, so a row never pairs one file's hash with another's ETag.
pub(super) fn hashed_of(result: IoResult, object: &ExternalObject) -> Result<Hashed, ItemError> {
    match result.child::<BlobEvent, BlobError>()? {
        Ok(BlobEvent::ExternalHashed {
            location,
            version,
            etag,
        }) => match read_listed(object.etag.as_deref(), etag.as_deref()) {
            Some(verified) => Ok((location, version, verified)),
            None => Err(ItemError::Changed),
        },
        Ok(other) => Err(ItemError::System(format!(
            "unexpected answer {}",
            Event::Blob(other).label()
        ))),
        Err(error) => Err(hash_failure(error)),
    }
}

/// [`hashed_of`], where a size other than the listed one means the object changed meanwhile.
pub(super) fn listed_hash(result: IoResult, object: &ExternalObject) -> Result<Hashed, ItemError> {
    let hashed = hashed_of(result, object)?;
    if hashed.0.blob_size != object.size {
        return Err(item("the object changed while it was read"));
    }
    Ok(hashed)
}

/// Replaces the row only while the rule exists and the row still holds `seen`.
pub(super) fn note_call(
    rule: &BackendImport,
    row_key: Key,
    seen: Option<Value>,
    row: Vec<u8>,
    note: ListingNote,
) -> IoCall {
    let operation = NoteListingOperation::new(rule.clone(), row_key, seen, row.into(), note);
    IoCall::child(operation)
}

/// The answer to [`note_call`]; a lost conflict reads as not written.
pub(super) fn noted(result: IoResult) -> Result<bool, ItemError> {
    match result.child::<bool, NoteError>()? {
        Ok(written) => Ok(written),
        Err(NoteError::Io(IoError::Conflict)) => Ok(false),
        Err(error) => Err(ItemError::System(error.to_string())),
    }
}

/// Every row below a prefix, read in pages.
#[derive(Debug, PartialEq)]
pub(super) struct PrefixRowsOperation {
    pages: PrefixPages,
    rows: Vec<(Key, Value)>,
}

impl PrefixRowsOperation {
    pub(super) fn new(key_space: &str, prefix: Key, limit: usize) -> Self {
        Self {
            pages: PrefixPages::new(key_space, prefix, limit),
            rows: Vec::new(),
        }
    }
}

impl Operation for PrefixRowsOperation {
    type Output = Vec<(Key, Value)>;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<Self::Output>, ItemError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ItemError> {
        self.rows.extend(self.pages.take(result)?);
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.rows)),
            false => Next::Call(self.pages.call()),
        })
    }
}

/// `write_intents` and the intents of `id` at any tagged path of the file's path or of its free
/// path, as a move tags with its attempt id; a suffix pattern's name already ends in one.
#[derive(Debug, PartialEq)]
pub(super) struct StoredIntentsOperation {
    import_id: Ulid,
    id: Ulid,
    bases: Vec<String>,
    base: String,
    keys: Vec<Key>,
}

impl StoredIntentsOperation {
    pub(super) fn new(import_id: Ulid, path: &str, id: Ulid) -> Self {
        Self {
            import_id,
            id,
            // Taken from the end: the path itself comes first.
            bases: vec![free_path(path).to_string(), path.to_string()],
            base: String::new(),
            keys: Vec::new(),
        }
    }

    fn next_base(&mut self) -> Next<Vec<Key>> {
        let Some(base) = self.bases.pop() else {
            return Next::Done(std::mem::take(&mut self.keys));
        };
        for key in write_intents(self.import_id, &base, self.id) {
            if !self.keys.contains(&key) {
                self.keys.push(key);
            }
        }
        let prefix = object_key(self.import_id, &format!("{base}~"));
        self.base = base;
        let rows = PrefixRowsOperation::new(IMPORT_INTENT_KEYSPACE, prefix, LIST_PAGE);
        Next::Call(IoCall::child(rows))
    }
}

impl Operation for StoredIntentsOperation {
    type Output = Vec<Key>;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<Vec<Key>>, ItemError> {
        Ok(self.next_base())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Key>>, ItemError> {
        let id = self.id.to_bytes();
        for (key, _) in result.child::<Vec<(Key, Value)>, ItemError>()?? {
            let tagged = intent_path(&key).is_some_and(|path| free_path(path) == self.base);
            if tagged && key.ends_with(&id) && !self.keys.contains(&key) {
                self.keys.push(key);
            }
        }
        Ok(self.next_base())
    }
}

/// Whether the delete trail removed version `id` of `key`, or the key without a head. Only the
/// deletes of this key and the purges of this incarnation of the bucket are read.
#[derive(Debug, PartialEq)]
pub(super) struct TrailOperation {
    bucket: String,
    key: String,
    id: Ulid,
    ranges: Vec<Key>,
    trail: Trail,
}

impl TrailOperation {
    pub(super) fn new(rule: &BackendImport, incarnation: u128, key: &str, id: Ulid) -> Self {
        let mut ranges: Vec<Key> =
            trail_ranges(rule.group_id, (&rule.bucket, incarnation), key).into_iter().collect();
        ranges.reverse();
        Self {
            bucket: rule.bucket.clone(),
            key: key.to_string(),
            id,
            ranges,
            trail: Trail::Clean,
        }
    }

    fn next_range(&mut self) -> Next<bool> {
        match self.ranges.pop() {
            Some(prefix) => Next::Call(IoCall::child(PrefixRowsOperation::new(
                DELETE_TRAIL_KEYSPACE,
                prefix,
                LIST_PAGE,
            ))),
            None => Next::Done(self.trail != Trail::Clean),
        }
    }
}

impl Operation for TrailOperation {
    type Output = bool;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<bool>, ItemError> {
        Ok(self.next_range())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ItemError> {
        for (_, value) in result.child::<Vec<(Key, Value)>, ItemError>()?? {
            let record = BlobAuditRecord::from_bytes(&value);
            let record = record.map_err(|error| ItemError::System(error.to_string()))?;
            self.trail = self.trail.add(&record, &self.bucket, &self.key, self.id);
        }
        Ok(self.next_range())
    }
}

/// The intents stored for a path, each with whether its version committed.
#[derive(Debug, PartialEq)]
pub(super) struct ClaimsOperation {
    bucket: String,
    intents: Vec<(Key, WriteIntent)>,
}

impl ClaimsOperation {
    pub(super) fn new(rule: &BackendImport, intents: Vec<(Key, WriteIntent)>) -> Self {
        Self {
            bucket: rule.bucket.clone(),
            intents,
        }
    }
}

impl Operation for ClaimsOperation {
    type Output = Vec<Claim>;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<Vec<Claim>>, ItemError> {
        let mut reads = Vec::with_capacity(self.intents.len());
        for (_, intent) in &self.intents {
            let row = version_row(&self.bucket, &intent.key, intent.version_id)?;
            reads.push((BLOB_VERSIONS_KEYSPACE, row));
        }
        Ok(Next::Call(read_call(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Claim>>, ItemError> {
        let mut claims = Vec::with_capacity(self.intents.len());
        let found = result.values()?;
        for ((key, intent), value) in std::mem::take(&mut self.intents).into_iter().zip(found) {
            claims.push((key, intent, exact_of(value)? != Exact::Absent));
        }
        Ok(Next::Done(claims))
    }
}

#[derive(Debug, PartialEq)]
enum Md5Step {
    Version,
    Location,
}

/// `None` when the committed version is gone; else the MD5 of its stored copy, read only for a
/// listing with an ETag.
#[derive(Debug, PartialEq)]
pub(super) struct CommittedMd5Operation {
    version_row: Key,
    has_etag: bool,
    step: Md5Step,
}

impl CommittedMd5Operation {
    pub(super) fn new(
        bucket: &str,
        key: &str,
        row: &ImportedObject,
        object: &ExternalObject,
    ) -> Result<Self, ItemError> {
        Ok(Self {
            version_row: version_row(bucket, key, row.version_id)?,
            has_etag: object.etag.is_some(),
            step: Md5Step::Version,
        })
    }
}

impl Operation for CommittedMd5Operation {
    type Output = Option<Option<Vec<u8>>>;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<Self::Output>, ItemError> {
        Ok(Next::Call(read_call([(
            BLOB_VERSIONS_KEYSPACE,
            self.version_row.clone(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ItemError> {
        let [value] = values::<1>(result)?;
        match self.step {
            Md5Step::Version => {
                let Some(version) = parse(value, BlobVersion::from_bytes)? else {
                    return Ok(Next::Done(None));
                };
                let Some(location_key) = version.location_key() else {
                    return Ok(Next::Done(None));
                };
                if !self.has_etag {
                    return Ok(Next::Done(Some(None)));
                }
                self.step = Md5Step::Location;
                let location = location_key.to_bytes().into();
                Ok(Next::Call(read_call([(BLOB_LOCATIONS_KEYSPACE, location)])))
            }
            Md5Step::Location => {
                let location = parse(value, BackendLocation::from_bytes)?;
                let md5 = location.and_then(|mut location| location.hashes.remove(HASH_MD5));
                Ok(Next::Done(Some(md5)))
            }
        }
    }
}

/// A version of a key whose bytes a file may hold: its row key, stored row, id and content hash.
pub(super) type Candidate = (Key, Value, Ulid, [u8; 32]);

#[derive(Debug, PartialEq)]
enum CandidateStep {
    Versions,
    Location,
}

/// The versions of `key` whose bytes `object` may hold: read from a file of the rule's storage,
/// of its size, and not refuted by its single-part ETag. Every version is compared, so a miss
/// proves other bytes.
#[derive(Debug, PartialEq)]
pub(super) struct CandidatesOperation {
    prefix: Vec<u8>,
    backend: BackendRef,
    object: ExternalObject,
    rows: Vec<(Key, Value, [u8; 32], Vec<u8>)>,
    current: Option<(Key, Value, [u8; 32])>,
    found: Vec<Candidate>,
    step: CandidateStep,
}

impl CandidatesOperation {
    pub(super) fn new(
        rule: &BackendImport,
        key: &str,
        object: &ExternalObject,
    ) -> Result<Self, ItemError> {
        let prefix = VersionKey::object_prefix(&rule.bucket, key)
            .map_err(|error| item(error.to_string()))?;
        Ok(Self {
            prefix,
            backend: BackendRef::Group(rule.backend_id),
            object: object.clone(),
            rows: Vec::new(),
            current: None,
            found: Vec::new(),
            step: CandidateStep::Versions,
        })
    }

    fn next_location(&mut self) -> Next<Vec<Candidate>> {
        match self.rows.pop() {
            Some((stored, value, hash, location)) => {
                self.current = Some((stored, value, hash));
                self.step = CandidateStep::Location;
                Next::Call(read_call([(BLOB_LOCATIONS_KEYSPACE, location.into())]))
            }
            None => Next::Done(std::mem::take(&mut self.found)),
        }
    }
}

impl Operation for CandidatesOperation {
    type Output = Vec<Candidate>;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<Vec<Candidate>>, ItemError> {
        let prefix: Key = self.prefix.clone().into();
        let rows = PrefixRowsOperation::new(BLOB_VERSIONS_KEYSPACE, prefix, TWIN_SCAN);
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Candidate>>, ItemError> {
        match self.step {
            CandidateStep::Versions => {
                for (stored, value) in result.child::<Vec<(Key, Value)>, ItemError>()?? {
                    let version = BlobVersion::from_bytes(&value);
                    let version = version.map_err(|error| ItemError::System(error.to_string()))?;
                    let BlobVersionState::Materialized {
                        blob_hash,
                        backend: on,
                        encoding: EncodingClass::Plain { .. } | EncodingClass::Raw,
                        ..
                    } = &version.state
                    else {
                        continue;
                    };
                    let location = version.location_key().filter(|_| *on == self.backend);
                    let Some(location) = location else {
                        continue;
                    };
                    let (hash, location) = (*blob_hash, location.to_bytes());
                    self.rows.push((stored, value, hash, location));
                }
                // Taken from the end: the versions are compared in key order.
                self.rows.reverse();
                Ok(self.next_location())
            }
            CandidateStep::Location => {
                let [location] = values::<1>(result)?;
                let current = self.current.take();
                let current = current.ok_or_else(|| ItemError::System("no version".to_string()))?;
                let (stored, value, hash) = current;
                let Some(mut location) = parse(location, BackendLocation::from_bytes)? else {
                    return Ok(self.next_location());
                };
                let md5 = location.hashes.remove(HASH_MD5);
                let refuted = etag_differs(self.object.etag.as_deref(), md5.as_deref());
                if location.blob_size == self.object.size && !refuted {
                    let id = VersionKey::from_bytes(&stored)
                        .map_err(|error| ItemError::System(error.to_string()))?
                        .version_id;
                    self.found.push((stored, value, id, hash));
                }
                Ok(self.next_location())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum HideStep {
    Head,
    Delete,
}

/// Covers the literal-name head `version_id` of `literal` with a delete marker. The normal delete
/// path adjusts the live charges, keeps the version for pinned references and keeps the bytes.
#[derive(Debug, PartialEq)]
pub(super) struct HideAliasOperation {
    job: ImportJob,
    rule: BackendImport,
    literal: String,
    version_id: Ulid,
    step: HideStep,
}

impl HideAliasOperation {
    pub(super) fn new(
        job: ImportJob,
        rule: BackendImport,
        literal: String,
        version_id: Ulid,
    ) -> Self {
        Self {
            job,
            rule,
            literal,
            version_id,
            step: HideStep::Head,
        }
    }
}

impl Operation for HideAliasOperation {
    type Output = ();
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<()>, ItemError> {
        Ok(Next::Call(head_call(&self.rule.bucket, &self.literal)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ItemError> {
        match self.step {
            HideStep::Head => {
                if head_of(result)? != Some(self.version_id) {
                    return Ok(Next::Done(()));
                }
                let auth = &self.job.spec.auth_context;
                let input = DeleteObjectInput {
                    bucket: self.rule.bucket.clone(),
                    key: self.literal.clone(),
                    version_id: None,
                    group_id: self.rule.group_id,
                    realm_id: auth.realm_id,
                    node_id: self.job.spec.node_id,
                    deleted_by: auth.user_id,
                };
                let delete = DeleteObjectOperation::new(input)
                    .with_head(self.version_id)
                    .with_restrictions(auth.path_restrictions.clone());
                self.step = HideStep::Delete;
                Ok(Next::Call(IoCall::child(delete)))
            }
            HideStep::Delete => match result.child::<DeleteObjectResult, DeleteObjectError>()? {
                // A write to the literal key meanwhile is the tenant's own and stays current.
                Ok(_) | Err(DeleteObjectError::VersionChanged) => Ok(Next::Done(())),
                Err(error @ DeleteObjectError::StorageError(_)) => {
                    Err(ItemError::System(error.to_string()))
                }
                Err(error) => Err(item(error.to_string())),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Rows,
    Named,
}

/// Reads the rows of a page in one batch, then the heads and named versions of their keys.
#[derive(Debug, PartialEq)]
pub(super) struct PageReadsOperation {
    rule: BackendImport,
    objects: Vec<ExternalObject>,
    page: PageReads,
    named: Vec<(String, String)>,
    step: PageStep,
}

impl PageReadsOperation {
    pub(super) fn new(rule: BackendImport, objects: Vec<ExternalObject>) -> Self {
        Self {
            rule,
            objects,
            page: PageReads::default(),
            named: Vec::new(),
            step: PageStep::Rows,
        }
    }

    fn rows_read(&mut self, result: IoResult) -> Result<Next<PageReads>, ItemError> {
        let rows = result.values()?;
        let mut reads = Vec::new();
        for (object, row) in self.objects.iter().zip(rows) {
            let parsed = parse(row.clone(), ImportedObject::from_bytes)?;
            self.page.rows.insert(object.key.clone(), row);
            let Some(parsed) = parsed else { continue };
            let key = parsed.key.clone().or_else(|| self.rule.aruna_key(&object.key));
            let Some(key) = key else { continue };
            let head = BlobHeadKey::new(&self.rule.bucket, &key).to_bytes();
            let version = VersionKey::new(&self.rule.bucket, &key, parsed.version_id).to_bytes();
            let (Ok(head), Ok(version)) = (head, version) else {
                continue;
            };
            reads.push((BLOB_HEAD_KEYSPACE, Key::from(head)));
            reads.push((BLOB_VERSIONS_KEYSPACE, Key::from(version)));
            self.named.push((object.key.clone(), key));
        }
        self.step = PageStep::Named;
        Ok(Next::Call(read_call(reads)))
    }
}

impl Operation for PageReadsOperation {
    type Output = PageReads;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<PageReads>, ItemError> {
        let import_id = self.rule.import_id;
        let reads = (self.objects.iter())
            .map(|object| (IMPORTED_OBJECT_KEYSPACE, object_key(import_id, &object.key)));
        Ok(Next::Call(read_call(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PageReads>, ItemError> {
        match self.step {
            PageStep::Rows => self.rows_read(result),
            PageStep::Named => {
                let mut values = result.values()?.into_iter();
                for (path, key) in std::mem::take(&mut self.named) {
                    let (Some(head), Some(version)) = (values.next(), values.next()) else {
                        return Err(ItemError::System("short batch read".to_string()));
                    };
                    let head = parse(head, CurrentVersionPointer::from_bytes)?;
                    self.page.heads.insert(key, head.map(|head| head.version_id));
                    self.page.versions.insert(path, version);
                }
                Ok(Next::Done(std::mem::take(&mut self.page)))
            }
        }
    }
}

/// The key spaces whose rows mark a path as another rule's.
const TAKEN_SPACES: [&str; 2] = [IMPORTED_OBJECT_KEYSPACE, IMPORT_INTENT_KEYSPACE];

/// The paths among a page's that another rule recorded or has a write intent for. Each rule costs
/// one range read per key space over the paths, however many there are.
#[derive(Debug, PartialEq)]
pub(super) struct TakenPathsOperation {
    rules: Vec<Ulid>,
    wanted: HashSet<String>,
    bounds: Option<(String, String)>,
    rule: usize,
    space: usize,
    taken: HashSet<String>,
}

impl TakenPathsOperation {
    pub(super) fn new(shared: &[SharedRule], objects: &[ExternalObject]) -> Self {
        let paths = objects.iter().map(|object| object.key.as_str());
        let bounds = paths.clone().min().zip(paths.clone().max());
        Self {
            rules: shared.iter().map(|other| other.rule.import_id).collect(),
            wanted: paths.map(str::to_string).collect(),
            bounds: bounds.map(|(first, last)| (first.to_string(), last.to_string())),
            rule: 0,
            space: 0,
            taken: HashSet::new(),
        }
    }

    fn range(&self, start: IterStart) -> Option<IoCall> {
        let import_id = *self.rules.get(self.rule)?;
        let iter = StorageEffect::Iter {
            key_space: TAKEN_SPACES[self.space].to_string(),
            prefix: Some(object_prefix(import_id)),
            start: Some(start),
            limit: LIST_PAGE,
            txn_id: None,
        };
        Some(IoCall::Effect(Box::new(Effect::Storage(iter))))
    }

    /// The first read of the current rule and key space, or the end.
    fn start_range(&self) -> Next<HashSet<String>> {
        let call = (self.bounds.as_ref()).and_then(|(first, _)| {
            let import_id = *self.rules.get(self.rule)?;
            self.range(IterStart::At(object_key(import_id, first)))
        });
        match call {
            Some(call) => Next::Call(call),
            None => Next::Done(self.taken.clone()),
        }
    }

    fn next_range(&mut self) -> Next<HashSet<String>> {
        self.space += 1;
        if self.space == TAKEN_SPACES.len() {
            self.space = 0;
            self.rule += 1;
        }
        self.start_range()
    }
}

impl Operation for TakenPathsOperation {
    type Output = HashSet<String>;
    type Error = ItemError;

    fn first(&mut self) -> Result<Next<HashSet<String>>, ItemError> {
        Ok(self.start_range())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HashSet<String>>, ItemError> {
        let event = result.event()?;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            let got = event.label();
            return Err(ItemError::System(format!("unexpected answer {got}")));
        };
        let last = (self.bounds.as_ref()).map_or("", |(_, last)| last.as_str());
        let intents = TAKEN_SPACES[self.space] == IMPORT_INTENT_KEYSPACE;
        let mut past = false;
        for (key, _) in values {
            let rest = key.get(16..).unwrap_or_default();
            // An intent key ends in a zero byte and the 16 bytes of its version id.
            let path = match intents {
                true => rest.get(..rest.len().saturating_sub(17)).unwrap_or_default(),
                false => rest,
            };
            let path = std::str::from_utf8(path).unwrap_or_default();
            past |= path > last;
            if self.wanted.contains(path) {
                self.taken.insert(path.to_string());
            }
        }
        match next_start_after.filter(|_| !past) {
            Some(next) => {
                let call = self.range(IterStart::After(next));
                call.map(Next::Call)
                    .ok_or_else(|| ItemError::System("no rule to read".to_string()))
            }
            None => Ok(self.next_range()),
        }
    }
}

/// The shared run data a child needs beside its job.
pub(super) type RunRef<'a> = (&'a ImportJob, &'a ImportRun);
