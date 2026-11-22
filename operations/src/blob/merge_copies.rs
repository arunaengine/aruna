//! Points the versions of a key's duplicate files at the kept file and queues the release of
//! the others, in one transaction that rereads them all.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id_mint::IdMint;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DELETE_CLAIM_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
    LINKED_JOB_KEYSPACE, MANAGED_COPY_KEYSPACE, RENAMED_ROW_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::storage::backend_import::{
    BackendImport, ExternalObject, ImportedObject, claim_key, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobVersion, BlobVersionState, ManagedCopyKey,
    ManagedCopyRecord, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::linked_job::LinkedJob;
use aruna_core::types::{GroupId, Key, Value};
use ulid::Ulid;

use super::import_rows::{BackendRulesOperation, ImportRowsError};
use super::migration::shared::MigrationError;
use super::unify::{Duplicate, id_named};
use crate::s3::usage::{StoredDelta, UsageCounterUpdate};
use aruna_core::structs::storage::cleanup::schedule_cleanup_effect;

const RULE_PAGE: usize = 128;

type Turn<T> = Result<Next<T>, MigrationError>;
type Row = (String, Key, Value);

/// What unifying did with one key's copies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unified {
    Done,
    /// Another key or version records every copy, so the key keeps its files.
    Kept,
    /// The versions or the kept file changed meanwhile; nothing changed.
    Changed,
    /// The job was paused, aborted or replaced; nothing changed.
    Stopped,
}

/// The job row a unification must still find, and the row it writes instead.
pub type JobFence = (LinkedJob, LinkedJob);

/// The first object a listing of the kept file's path found, `None` when it found none or no
/// blob storage could tell, or the label of the answer that failed it.
type KeptListing = Result<Option<ExternalObject>, &'static str>;

#[derive(Debug, PartialEq)]
enum UnifyStep {
    Rules,
    Provider(Vec<BackendImport>),
    Listing(Vec<BackendImport>),
    Stage,
}

/// Reads the rules that may record a copy and lists a kept file without a backend version, then
/// unifies the key in one transaction. With a fence, the bucket's job must still be stored as
/// its first value and gets its second.
#[derive(Debug, PartialEq)]
pub struct UnifyOperation {
    bucket: String,
    group_id: GroupId,
    found: Option<Duplicate>,
    fence: Option<JobFence>,
    step: UnifyStep,
}

impl UnifyOperation {
    pub fn new(
        (bucket, group_id): (&str, GroupId),
        found: Duplicate,
        fence: Option<JobFence>,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            group_id,
            found: Some(found),
            fence,
            step: UnifyStep::Rules,
        }
    }

    fn found(&self) -> Result<&Duplicate, MigrationError> {
        let missing = || MigrationError::Failed("no copies to unify".to_string());
        self.found.as_ref().ok_or_else(missing)
    }

    /// Lists outside the transaction, which rereads the backend version before it trusts this.
    fn provider_read(
        &mut self,
        rules: Vec<BackendImport>,
        provider: Option<Value>,
    ) -> Turn<Unified> {
        if provider.is_some() {
            return self.stage(rules, None);
        }
        let keep = &self.found()?.keep;
        let list = IoCall::blob(BlobEffect::ListExternal {
            backend: keep.backend.clone(),
            prefix: keep.backend_path.clone(),
            start_after: None,
            limit: 1,
        });
        self.step = UnifyStep::Listing(rules);
        Ok(Next::Call(list))
    }

    fn stage(&mut self, rules: Vec<BackendImport>, listing: Option<KeptListing>) -> Turn<Unified> {
        let (Some(found), fence) = (self.found.take(), self.fence.take()) else {
            return Err(MigrationError::Failed("no copies to unify".to_string()));
        };
        self.step = UnifyStep::Stage;
        let stage = UnifyStageOperation::new(&self.bucket, rules, found, fence, listing);
        Ok(Next::Call(IoCall::child(stage)))
    }
}

fn kept_object(event: Event) -> KeptListing {
    match event.into_blob() {
        Ok(BlobEvent::ExternalListed { objects, .. }) => Ok(objects.into_iter().next()),
        // A node without blob storage cannot tell.
        Ok(BlobEvent::Error(BlobError::HandleMissing)) => Ok(None),
        Ok(other) => Err(other.label()),
        Err(other) => Err(other.label()),
    }
}

impl Operation for UnifyOperation {
    type Output = Unified;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Unified> {
        let backend = self.found()?.keep.backend.clone();
        let rules = BackendRulesOperation::new(self.group_id, backend);
        Ok(Next::Call(IoCall::child(rules)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Unified> {
        match std::mem::replace(&mut self.step, UnifyStep::Stage) {
            UnifyStep::Rules => {
                let rules = (result.child::<Vec<BackendImport>, ImportRowsError>()?)
                    .map_err(MigrationError::failed)?;
                let ulid = self.found()?.keep.ulid;
                self.step = UnifyStep::Provider(rules);
                Ok(Next::Call(read_row(
                    BACKEND_VERSION_KEYSPACE,
                    version_key(ulid),
                )))
            }
            UnifyStep::Provider(rules) => self.provider_read(rules, result.value()?),
            UnifyStep::Listing(rules) => {
                let listing = kept_object(result.event()?);
                self.stage(rules, Some(listing))
            }
            // A transaction that lost to a concurrent change wrote nothing.
            UnifyStep::Stage => match result.child::<Unified, MigrationError>()? {
                Ok(unified) => Ok(Next::Done(unified)),
                Err(MigrationError::Io(IoError::Conflict)) => Ok(Next::Done(Unified::Changed)),
                Err(error) => Err(error),
            },
        }
    }
}

/// The copy the transaction works on.
#[derive(Debug, PartialEq)]
struct CopyWork {
    at: usize,
    provider: Option<Value>,
    owner: Option<(Ulid, Ulid)>,
    mine: VecDeque<(Ulid, BlobVersion)>,
}

#[derive(Debug, PartialEq)]
enum StageStep {
    Fence,
    Rows,
    Adopt(Key),
    Provider,
    Claim(bool),
    KeptRows,
    TwinRow(CopyWork),
    Managed(
        CopyWork,
        VersionKey,
        BlobVersion,
        PrefixPages,
        Vec<(Key, Value)>,
    ),
    Usage(Box<UsageCounterUpdate>),
    Deleted,
    Written,
}

/// The reads and writes of one unification. It decides from reads only; every write waits for
/// the end, so an outcome other than `Done` leaves the transaction empty.
#[derive(Debug, PartialEq)]
struct UnifyStageOperation {
    now_ms: u64,
    ids: IdMint,
    bucket: String,
    rules: Vec<BackendImport>,
    found: Duplicate,
    fence: Option<JobFence>,
    /// The listing of the kept file taken before the transaction, when it had no backend version.
    listing: Option<KeptListing>,
    step: StageStep,
    writes: Vec<Row>,
    deletes: Vec<(String, Key)>,
    values: VecDeque<Option<Value>>,
    /// The version whose row must still name a kept second copy.
    adopting: Option<Ulid>,
    charge: bool,
    readers: HashMap<Ulid, Vec<(Ulid, BlobVersion)>>,
    next_copy: usize,
    changed: usize,
}

impl UnifyStageOperation {
    fn new(
        bucket: &str,
        rules: Vec<BackendImport>,
        found: Duplicate,
        fence: Option<JobFence>,
        listing: Option<KeptListing>,
    ) -> Self {
        Self {
            now_ms: 0,
            ids: IdMint::default(),
            bucket: bucket.to_string(),
            rules,
            found,
            fence,
            listing,
            step: StageStep::Rows,
            writes: Vec::new(),
            deletes: Vec::new(),
            values: VecDeque::new(),
            adopting: None,
            charge: false,
            readers: HashMap::new(),
            next_copy: 0,
            changed: 0,
        }
    }

    fn call(&mut self, step: StageStep, call: IoCall) -> Turn<Unified> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn rule_rows(&self, path: &str) -> Vec<(String, Key)> {
        (self.rules.iter())
            .map(|rule| {
                let row = object_key(rule.import_id, path);
                (IMPORTED_OBJECT_KEYSPACE.to_string(), row)
            })
            .collect()
    }

    fn fence_read(&mut self, stored: Option<Value>) -> Turn<Unified> {
        let Some((expected, next)) = self.fence.take() else {
            return self.read_rows();
        };
        if stored.as_deref() != Some(expected.to_bytes()?.as_slice()) {
            return Ok(Next::Done(Unified::Stopped));
        }
        let name: Key = self.bucket.as_bytes().to_vec().into();
        let row = (
            LINKED_JOB_KEYSPACE.to_string(),
            name,
            next.to_bytes()?.into(),
        );
        self.writes.push(row);
        self.read_rows()
    }

    fn read_rows(&mut self) -> Turn<Unified> {
        let found = &self.found;
        let keep_key = found.keep.location_key()?;
        let mut reads = vec![(
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            Key::from(keep_key.to_bytes()),
        )];
        for (id, _) in &found.versions {
            let version = VersionKey::new(&self.bucket, &found.key, *id).to_bytes()?;
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
        }
        for copy in &found.copies {
            reads.push((BACKEND_VERSION_KEYSPACE.to_string(), version_key(copy.ulid)));
            reads.extend(self.rule_rows(&copy.backend_path));
        }
        self.call(StageStep::Rows, IoCall::Read(reads))
    }

    fn rows_read(&mut self, values: Vec<Option<Value>>) -> Turn<Unified> {
        self.values = values.into();
        let kept = self.values.pop_front().flatten();
        // Compared decoded: the encoding of a location's hash map is not stable.
        let kept = kept
            .as_deref()
            .map(BackendLocation::from_bytes)
            .transpose()?;
        let found = &self.found;
        let twin = found
            .twins
            .iter()
            .find(|(ulid, ..)| *ulid == found.keep.ulid);
        match (kept, twin) {
            (Some(kept), None) if kept == found.keep => self.read_provider(),
            // A kept second copy becomes the versions' own file with a location row.
            (None, Some((_, index, version))) => {
                let (index, version) = (index.clone(), *version);
                let mut reads = vec![(RENAMED_ROW_KEYSPACE.to_string(), index.clone())];
                reads.extend(self.rule_rows(&self.found.keep.backend_path));
                self.adopting = Some(version);
                self.call(StageStep::Adopt(index), IoCall::Read(reads))
            }
            _ => Ok(Next::Done(Unified::Changed)),
        }
    }

    /// Whether the kept second copy's index row still names it and its row still names the
    /// version under this key, as this transaction reads them.
    fn adopt_read(&mut self, index: Key, values: Vec<Option<Value>>) -> Turn<Unified> {
        let found = &self.found;
        let mut values = values.into_iter();
        let stored = values.next().flatten().filter(|value| !value.is_empty());
        let stored = stored
            .as_deref()
            .map(BackendLocation::from_bytes)
            .transpose()?;
        if stored.as_ref() != Some(&found.keep) {
            return Ok(Next::Done(Unified::Changed));
        }
        let mut adoptable = false;
        for row in values.flatten() {
            let row = ImportedObject::from_bytes(&row)?;
            adoptable |= row.key.as_deref() == Some(found.key.as_str())
                && Some(row.version_id) == self.adopting;
        }
        if !adoptable {
            return Ok(Next::Done(Unified::Changed));
        }
        let location = Key::from(found.keep.location_key()?.to_bytes());
        let row = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            location,
            found.keep.to_bytes()?.into(),
        );
        self.writes.push(row);
        self.deletes.push((RENAMED_ROW_KEYSPACE.to_string(), index));
        self.charge = true;
        self.read_provider()
    }

    /// A kept file a release is deleting, at its backend version or without one, takes no new
    /// reader.
    fn read_provider(&mut self) -> Turn<Unified> {
        let read = read_row(BACKEND_VERSION_KEYSPACE, version_key(self.found.keep.ulid));
        self.call(StageStep::Provider, read)
    }

    fn provider_read(&mut self, provider: Option<Value>) -> Turn<Unified> {
        let provider = provider.map(|provider| String::from_utf8_lossy(&provider).into_owned());
        let unversioned = provider.is_none();
        let claim = claim_key(&self.found.keep, &provider.unwrap_or_default())?;
        let read = read_row(DELETE_CLAIM_KEYSPACE, claim);
        self.call(StageStep::Claim(unversioned), read)
    }

    fn claim_read(&mut self, unversioned: bool, claim: Option<Value>) -> Turn<Unified> {
        if claim.is_some() {
            return Ok(Next::Done(Unified::Changed));
        }
        if !unversioned {
            return self.check_versions();
        }
        // Without a backend version the kept file must still hold the bytes it was found with.
        let reads = self.rule_rows(&self.found.keep.backend_path);
        self.call(StageStep::KeptRows, IoCall::Read(reads))
    }

    /// Compares the kept file's listing with the size and ETag its row recorded.
    fn kept_rows(&mut self, values: Vec<Option<Value>>) -> Turn<Unified> {
        let mut recorded = None;
        for row in values.into_iter().flatten() {
            let row = ImportedObject::from_bytes(&row)?;
            if row.key.as_deref() == Some(self.found.key.as_str()) {
                recorded = row.etag;
            }
        }
        let Some(recorded) = recorded else {
            return Ok(Next::Done(Unified::Changed));
        };
        let keep = &self.found.keep;
        let unquoted = |etag: &str| etag.trim_matches('"').to_string();
        let unchanged = match self.listing.take() {
            // The kept file had a backend version when the listing was skipped.
            None => false,
            Some(Ok(object)) => object.is_some_and(|object| {
                object.key == keep.backend_path
                    && object.size == keep.blob_size
                    && object.etag.as_deref().map(unquoted) == Some(unquoted(&recorded))
            }),
            Some(Err(label)) => {
                return Err(MigrationError::Failed(format!(
                    "could not list the kept file: {label}"
                )));
            }
        };
        match unchanged {
            true => self.check_versions(),
            false => Ok(Next::Done(Unified::Changed)),
        }
    }

    /// Which copy each version reads now; any change refuses the whole key.
    fn check_versions(&mut self) -> Turn<Unified> {
        for (id, stored) in &self.found.versions {
            let value = self.values.pop_front().flatten();
            if value.as_deref() != Some(stored.as_slice()) {
                return Ok(Next::Done(Unified::Changed));
            }
            let version = BlobVersion::from_bytes(stored)?;
            // A raw copy is the copy whose location row the version reads.
            let file = match &version.state {
                BlobVersionState::Materialized {
                    encoding: EncodingClass::Plain { location },
                    ..
                } => Some(*location),
                BlobVersionState::Materialized {
                    encoding: EncodingClass::Raw,
                    ..
                } => (self.found.copies.iter())
                    .find(|copy| copy.location_key().ok() == version.location_key())
                    .map(|copy| copy.ulid),
                _ => None,
            };
            if let Some(file) = file {
                self.readers.entry(file).or_default().push((*id, version));
            }
        }
        self.next_copy()
    }

    fn next_copy(&mut self) -> Turn<Unified> {
        while self.next_copy < self.found.copies.len() {
            let at = self.next_copy;
            self.next_copy += 1;
            let copy_ulid = self.found.copies[at].ulid;
            let provider = self.values.pop_front().flatten();
            let mine = self.readers.remove(&copy_ulid).unwrap_or_default();
            let twin = self
                .found
                .twins
                .iter()
                .find(|(ulid, ..)| *ulid == copy_ulid);
            let twin = twin.map(|(_, index, version)| (index.clone(), *version));
            let mut owner = None;
            let mut foreign = false;
            for rule in &self.rules {
                let row = self.values.pop_front().flatten();
                let row = row.as_deref().map(ImportedObject::from_bytes).transpose()?;
                let Some(row) = row else {
                    continue;
                };
                // A row of another key or version makes the file that version's own.
                let named = match &twin {
                    Some((_, version)) => *version == row.version_id,
                    None => mine.iter().any(|(id, _)| *id == row.version_id),
                };
                let key = &self.found.key;
                let ours = row.key.as_deref().is_none_or(|row_key| row_key == key) && named;
                match ours {
                    true => owner = owner.or(Some((rule.import_id, row.version_id))),
                    false => foreign = true,
                }
            }
            if foreign || (mine.is_empty() && twin.is_none()) {
                continue;
            }
            let work = CopyWork {
                at,
                provider,
                owner,
                mine: mine.into(),
            };
            // A second copy goes with its row while its index row still names it; that row
            // goes now.
            return match twin {
                Some((index, _)) => {
                    let read = read_row(RENAMED_ROW_KEYSPACE, index);
                    self.call(StageStep::TwinRow(work), read)
                }
                None => self.next_reader(work),
            };
        }
        self.finish_copies()
    }

    fn twin_row(&mut self, work: CopyWork, stored: Option<Value>) -> Turn<Unified> {
        let stored = stored.filter(|value| !value.is_empty());
        let stored = stored
            .as_deref()
            .map(BackendLocation::from_bytes)
            .transpose()?;
        let copy = &self.found.copies[work.at];
        if stored.as_ref() != Some(copy) {
            return self.next_copy();
        }
        let twin = self
            .found
            .twins
            .iter()
            .find(|(ulid, ..)| *ulid == copy.ulid);
        if let Some((_, index, _)) = twin {
            self.deletes
                .push((RENAMED_ROW_KEYSPACE.to_string(), index.clone()));
        }
        self.next_reader(work)
    }

    /// Moves the next version of the copy to the kept file, with its copy registrations.
    fn next_reader(&mut self, mut work: CopyWork) -> Turn<Unified> {
        let Some((id, mut version)) = work.mine.pop_front() else {
            return self.release_copy(work);
        };
        let encoding = self.found.keep.format.encoding();
        if let BlobVersionState::Materialized { encoding: old, .. } = &mut version.state {
            *old = encoding;
        }
        let key = VersionKey::new(&self.bucket, &self.found.key, id);
        let prefix = ManagedCopyKey::version_prefix(&key)?;
        let pages = PrefixPages::new(MANAGED_COPY_KEYSPACE, prefix, RULE_PAGE);
        let page = pages.call();
        self.call(
            StageStep::Managed(work, key, version, pages, Vec::new()),
            page,
        )
    }

    fn managed_read(
        &mut self,
        work: CopyWork,
        key: VersionKey,
        version: BlobVersion,
        rows: Vec<(Key, Value)>,
    ) -> Turn<Unified> {
        let copy = &self.found.copies[work.at];
        for (row, value) in rows {
            let record = ManagedCopyRecord::from_bytes(&value)?;
            if record.location.same_object(copy) {
                let record = ManagedCopyRecord {
                    location: self.found.keep.clone(),
                    ..record
                };
                let value = record.to_bytes()?.into();
                self.writes
                    .push((MANAGED_COPY_KEYSPACE.to_string(), row, value));
            }
        }
        let value = version.to_bytes()?.into();
        let row = key.to_bytes()?.into();
        self.writes
            .push((BLOB_VERSIONS_KEYSPACE.to_string(), row, value));
        self.next_reader(work)
    }

    fn release_copy(&mut self, work: CopyWork) -> Turn<Unified> {
        let copy = &self.found.copies[work.at];
        // A file named by its version's id was written by Aruna, so it goes without a backend
        // version too.
        let named = work.owner.is_some_and(|(import, version)| {
            let rules: Vec<_> = (self.rules.iter())
                .filter(|rule| rule.import_id == import)
                .cloned()
                .collect();
            id_named(&rules, &copy.backend_path, version)
        });
        let key = self.ids.mint(self.now_ms);
        let row = release(copy, work.provider, (work.owner, named), (self.now_ms, key))?;
        self.writes.push(row);
        self.changed += 1;
        self.next_copy()
    }

    fn finish_copies(&mut self) -> Turn<Unified> {
        if self.changed == 0 {
            return Ok(Next::Done(Unified::Kept));
        }
        if std::mem::take(&mut self.charge) {
            // Charges the kept file's new location row, as reclaim debits the released one.
            let keep = &self.found.keep;
            let bytes = i128::from(keep.stored_size());
            if let Some(stored) = StoredDelta::of_copy(keep, 1, bytes) {
                let mut update = UsageCounterUpdate::for_stored(stored);
                let call = update.start_calls(self.ids.mint(self.now_ms)).into_call();
                return self.call(StageStep::Usage(Box::new(update)), call);
            }
        }
        self.apply_deletes()
    }

    fn apply_deletes(&mut self) -> Turn<Unified> {
        if self.deletes.is_empty() {
            return self.apply_writes();
        }
        let deletes = std::mem::take(&mut self.deletes);
        self.call(StageStep::Deleted, IoCall::Delete(deletes))
    }

    fn apply_writes(&mut self) -> Turn<Unified> {
        let writes = std::mem::take(&mut self.writes);
        self.call(StageStep::Written, IoCall::Write(writes))
    }
}

impl Operation for UnifyStageOperation {
    type Output = Unified;
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<Unified> {
        if self.fence.is_none() {
            return self.read_rows();
        }
        let read = read_row(LINKED_JOB_KEYSPACE, self.bucket.as_bytes().to_vec());
        self.call(StageStep::Fence, read)
    }

    fn next(&mut self, result: IoResult) -> Turn<Unified> {
        match std::mem::replace(&mut self.step, StageStep::Written) {
            StageStep::Fence => self.fence_read(result.value()?),
            StageStep::Rows => self.rows_read(result.values()?),
            StageStep::Adopt(index) => self.adopt_read(index, result.values()?),
            StageStep::Provider => self.provider_read(result.value()?),
            StageStep::Claim(unversioned) => self.claim_read(unversioned, result.value()?),
            StageStep::KeptRows => self.kept_rows(result.values()?),
            StageStep::TwinRow(work) => self.twin_row(work, result.value()?),
            StageStep::Managed(work, key, version, mut pages, mut rows) => {
                rows.extend(pages.take(result)?);
                if pages.done() {
                    return self.managed_read(work, key, version, rows);
                }
                let page = pages.call();
                self.call(StageStep::Managed(work, key, version, pages, rows), page)
            }
            StageStep::Usage(mut update) => {
                let next = update.step_calls(result).map_err(MigrationError::failed)?;
                match next {
                    Some(call) => self.call(StageStep::Usage(update), call.into_call()),
                    None => self.apply_deletes(),
                }
            }
            StageStep::Deleted => {
                result.done()?;
                self.apply_writes()
            }
            StageStep::Written => {
                result.done()?;
                Ok(Next::Then {
                    output: Unified::Done,
                    calls: vec![schedule_cleanup_effect().into()],
                })
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// The release of `copy` at its backend version, or of a file its version's id names, or else
/// the drop of its location row, which leaves the file in place.
fn release(
    copy: &BackendLocation,
    provider: Option<Value>,
    (owner, named): (Option<(Ulid, Ulid)>, bool),
    (now_ms, key): (u64, Ulid),
) -> Result<Row, MigrationError> {
    let provider = provider.or_else(|| named.then(|| Value::from(Vec::new())));
    let Some(provider) = provider else {
        let location = copy.location_key()?;
        let candidate =
            ReclaimCandidateKey::new(location.backend, location.encoding, location.blake3_hash);
        let value = ReclaimCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH + Duration::from_millis(now_ms),
        };
        let key = candidate.to_bytes().into();
        return Ok((
            BLOB_RECLAIM_KEYSPACE.to_string(),
            key,
            value.to_bytes()?.into(),
        ));
    };
    let (import_id, version_id) = owner.unwrap_or((Ulid::nil(), Ulid::nil()));
    let work = BlobCleanupWork::ReleaseRenamed {
        location: copy.clone(),
        import_id,
        version_id,
        version: String::from_utf8(provider.to_vec()).map_err(MigrationError::failed)?,
    };
    let key = key.to_bytes().to_vec().into();
    Ok((
        BLOB_CLEANUP_KEYSPACE.to_string(),
        key,
        work.to_bytes()?.into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::{StoredFormat, StoredLayout};

    fn location(id: u8) -> BackendLocation {
        let ulid = Ulid::from_bytes([id; 16]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: format!("file-{id}"),
            ulid,
            format: StoredFormat {
                layout: StoredLayout::Plain(ulid),
                ..StoredFormat::default()
            },
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: [(HASH_BLAKE3.to_string(), vec![id; 32])].into(),
        }
    }

    fn duplicate() -> Duplicate {
        Duplicate {
            key: "key".to_string(),
            keep: location(1),
            copies: vec![location(2)],
            versions: Vec::new(),
            twins: Vec::new(),
        }
    }

    fn stage() -> UnifyStageOperation {
        UnifyStageOperation::new("bucket", Vec::new(), duplicate(), None, None)
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn rule() -> BackendImport {
        BackendImport {
            import_id: Ulid::from_bytes([7; 16]),
            group_id: Ulid::nil(),
            backend_id: Ulid::nil(),
            backend_prefix: String::new(),
            bucket: "bucket".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: String::new(),
        }
    }

    /// A stage over a kept file without a backend version whose row recorded ETag `e1`, walked
    /// to its outcome; every call it makes is a storage read.
    fn walk_unversioned(listing: KeptListing) -> Turn<Unified> {
        let mut stage =
            UnifyStageOperation::new("bucket", vec![rule()], duplicate(), None, Some(listing));
        let keep = Value::from(location(1).to_bytes().unwrap());
        assert!(matches!(stage.first(), Ok(Next::Call(IoCall::Read(_)))));
        // The rows, the kept file's backend version and its delete claim.
        for values in [vec![Some(keep), None, None], vec![None], vec![None]] {
            let turn = stage.next(IoResult::Values(values));
            assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))), "{turn:?}");
        }
        let etag = Some("\"e1\"".to_string());
        let recorded = ImportedObject::written(Ulid::nil(), "key".to_string(), 10, etag);
        let recorded = Value::from(recorded.to_bytes().unwrap());
        stage.next(IoResult::Values(vec![Some(recorded)]))
    }

    #[test]
    fn stage_never_lists() {
        // The stage compares the listing taken before its transaction and lists nothing itself.
        let object = |etag: &str| ExternalObject {
            key: location(1).backend_path,
            size: 10,
            etag: Some(etag.to_string()),
            modified_ms: None,
        };
        let same = walk_unversioned(Ok(Some(object("e1"))));
        assert_eq!(same, Ok(Next::Done(Unified::Kept)));
        let other = walk_unversioned(Ok(Some(object("e2"))));
        assert_eq!(other, Ok(Next::Done(Unified::Changed)));
        assert_eq!(walk_unversioned(Ok(None)), Ok(Next::Done(Unified::Changed)));
        assert!(matches!(
            walk_unversioned(Err("PlainWritten")),
            Err(MigrationError::Failed(_))
        ));
    }

    #[test]
    fn unify_lists_unversioned() {
        // Only a kept file without a backend version is listed, before the transaction.
        let mut unify = UnifyOperation::new(("bucket", Ulid::nil()), duplicate(), None);
        unify.first().unwrap();
        let read = unify.next(child::<_, ImportRowsError>(Ok(Vec::<BackendImport>::new())));
        let provider = version_key(location(1).ulid);
        assert_eq!(
            read,
            Ok(Next::Call(read_row(BACKEND_VERSION_KEYSPACE, provider)))
        );
        let keep = location(1);
        let list = IoCall::blob(BlobEffect::ListExternal {
            backend: keep.backend,
            prefix: keep.backend_path,
            start_after: None,
            limit: 1,
        });
        assert_eq!(
            unify.next(IoResult::Values(vec![None])),
            Ok(Next::Call(list))
        );
        let missing = Event::blob(BlobEvent::Error(BlobError::HandleMissing));
        let staged = unify.next(IoResult::Event(Box::new(missing)));
        let stage =
            UnifyStageOperation::new("bucket", Vec::new(), duplicate(), None, Some(Ok(None)));
        assert_eq!(staged, Ok(Next::Call(IoCall::child(stage))));
    }

    #[test]
    fn missing_keep_refused() {
        let mut stage = stage();
        assert!(matches!(stage.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = IoResult::Values(vec![None, None]);
        assert_eq!(stage.next(answer), Ok(Next::Done(Unified::Changed)));
        assert!(stage.writes.is_empty());
    }

    #[test]
    fn unowned_copy_kept() {
        // A copy no version reads and no row names stays, so nothing is written.
        let keep = Value::from(location(1).to_bytes().unwrap());
        let mut stage = stage();
        stage.first().unwrap();
        let answer = IoResult::Values(vec![Some(keep), None]);
        assert!(matches!(
            stage.next(answer),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let provider = IoResult::Values(vec![Some(Value::from(b"v1".to_vec()))]);
        assert!(matches!(
            stage.next(provider),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let claim = IoResult::Values(vec![None]);
        assert_eq!(stage.next(claim), Ok(Next::Done(Unified::Kept)));
    }

    #[test]
    fn conflict_means_changed() {
        let mut unify = UnifyOperation::new(("bucket", Ulid::nil()), duplicate(), None);
        assert!(matches!(unify.first(), Ok(Next::Call(IoCall::Child(_)))));
        let rules: Result<Vec<BackendImport>, ImportRowsError> = Ok(Vec::new());
        assert!(matches!(
            unify.next(child(rules)),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let provider = IoResult::Values(vec![Some(Value::from(b"v1".to_vec()))]);
        assert!(matches!(
            unify.next(provider),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let lost: Result<Unified, MigrationError> = Err(MigrationError::Io(IoError::Conflict));
        assert_eq!(unify.next(child(lost)), Ok(Next::Done(Unified::Changed)));
    }
}
