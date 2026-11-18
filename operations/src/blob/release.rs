//! Releases the file a rename replaced in a linked folder, once nothing else names it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, CLAIM_INDEX_KEYSPACE, DELETE_CLAIM_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, claim_key, names_version, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobLocationKey, BlobVersion, HashIndex, VersionKey,
};
use aruna_core::structs::storage::cleanup::ReclaimCandidateKey;
use aruna_core::types::{Key, Value};
use tracing::warn;
use ulid::Ulid;

use crate::blob::migration::shared::MigrationError;
use crate::blob::reclaim::{ReclaimBlobError, ReclaimBlobOperation, ReclaimVerdict};
use crate::blob::records::{iter_index_effect, owners_scan_effect};

const RULE_PAGE: usize = 128;
/// Claim index pages one pass reads past claims whose delete was sent.
const DROP_PAGES: usize = 16;

/// The value of a pending delete claim, of one whose delete was sent, and of the tombstone of
/// a deleted backend version. Only a pending claim is ever cleared.
const PENDING: &[u8] = b"";
const DELETING: &[u8] = b"deleting";
const TOMBSTONE: &[u8] = b"deleted";

type Turn<T> = Result<Next<T>, MigrationError>;

/// The file a release works on and the claim that holds it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReleaseFile {
    pub(crate) location: BackendLocation,
    pub(crate) import_id: Ulid,
    pub(crate) version_id: Ulid,
    /// The backend version of the file; empty when the storage keeps none.
    pub(crate) version: String,
    /// Whether this node has blob storage to delete the file with.
    pub(crate) has_blob: bool,
}

/// A delete claim and its entry under the file's rule.
#[derive(Clone, Debug, PartialEq)]
struct Claim {
    key: Key,
    index: Key,
}

impl ReleaseFile {
    fn claim(&self) -> Result<Claim, MigrationError> {
        let key = claim_key(&self.location, &self.version)?;
        let index = [&self.import_id.to_bytes()[..], &key[..]].concat().into();
        Ok(Claim { key, index })
    }

    fn row_key(&self) -> Key {
        object_key(self.import_id, &self.location.backend_path)
    }

    fn delete_call(&self, etag: Option<String>) -> IoCall {
        let delete = BlobEffect::DeletePlain {
            location: self.location.clone(),
            version: self.version.clone(),
            etag,
        };
        Effect::Blob(delete).into()
    }
}

/// Stores the claim with `value` and its entry under the file's rule, which removes the rule's
/// claims with the rule, or drops both with `None`.
fn claim_call(claim: &Claim, value: Option<&[u8]>) -> IoCall {
    let Some(value) = value else {
        return IoCall::Delete(vec![
            (DELETE_CLAIM_KEYSPACE.to_string(), claim.key.clone()),
            (CLAIM_INDEX_KEYSPACE.to_string(), claim.index.clone()),
        ]);
    };
    IoCall::Write(vec![
        (
            DELETE_CLAIM_KEYSPACE.to_string(),
            claim.key.clone(),
            value.to_vec().into(),
        ),
        (
            CLAIM_INDEX_KEYSPACE.to_string(),
            claim.index.clone(),
            Vec::new().into(),
        ),
    ])
}

fn imported_row(value: &Option<Value>) -> Result<Option<ImportedObject>, MigrationError> {
    Ok(value
        .as_deref()
        .map(ImportedObject::from_bytes)
        .transpose()?)
}

#[derive(Debug, PartialEq)]
enum ReleaseStep {
    Claim,
    Tail,
    Claimed,
    Synced,
    Released,
    Cleared,
}

/// Deletes the old file of a rename at its recorded backend version, then the import row naming
/// it; any other reference keeps both, and `false` retries. A durable claim of that version
/// comes first and holds until the release settles, against new readers of the file.
#[derive(Debug, PartialEq)]
pub(crate) struct ReleaseRenamedOperation {
    file: ReleaseFile,
    claim: Option<Claim>,
    step: ReleaseStep,
}

impl ReleaseRenamedOperation {
    pub(crate) fn new(file: ReleaseFile) -> Self {
        Self {
            file,
            claim: None,
            step: ReleaseStep::Claim,
        }
    }

    fn claim(&self) -> Result<&Claim, MigrationError> {
        (self.claim.as_ref()).ok_or_else(|| MigrationError::Failed("no claim".to_string()))
    }

    fn tail(&mut self, call: IoCall) -> Turn<bool> {
        self.step = ReleaseStep::Tail;
        Ok(Next::Call(call))
    }

    fn claim_read(&mut self, value: Option<Value>) -> Turn<bool> {
        let claim = self.claim()?.clone();
        // A completed tombstone is never written over or cleared: those bytes are gone.
        match value.as_deref() {
            // An unversioned tombstone lasts only until the row of the deleted file is gone.
            Some(value) if value == TOMBSTONE && self.file.version.is_empty() => {
                let forget = ForgetDeletedOperation::new(self.file.clone(), claim);
                self.tail(IoCall::child(forget))
            }
            Some(value) if value == TOMBSTONE => Ok(Next::Done(true)),
            // A delete was sent without a known outcome: only that delete is finished, never
            // undone.
            Some(value) if value == DELETING => {
                let finish = FinishDeleteOperation::new(self.file.clone(), claim);
                self.tail(IoCall::child(finish))
            }
            _ => {
                self.step = ReleaseStep::Claimed;
                Ok(Next::Call(claim_call(&claim, Some(PENDING))))
            }
        }
    }
}

impl Operation for ReleaseRenamedOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        let file = &self.file;
        // Without a backend version only a file whose name carries the version's id may go.
        if file.version.is_empty() && !names_version(&file.location.backend_path, file.version_id) {
            return Ok(Next::Done(true));
        }
        let Ok(claim) = file.claim() else {
            return Ok(Next::Done(false));
        };
        let read = read_row(DELETE_CLAIM_KEYSPACE, claim.key.clone());
        self.claim = Some(claim);
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match self.step {
            ReleaseStep::Claim => self.claim_read(result.value()?),
            ReleaseStep::Tail => Ok(Next::Done(result.child::<bool, MigrationError>()??)),
            ReleaseStep::Claimed => {
                result.done()?;
                self.step = ReleaseStep::Synced;
                Ok(Next::Call(IoCall::Sync))
            }
            ReleaseStep::Synced => {
                result.done()?;
                let release =
                    ReleaseClaimedOperation::new(self.file.clone(), self.claim()?.clone());
                self.step = ReleaseStep::Released;
                Ok(Next::Call(IoCall::child(release)))
            }
            // A deleted version keeps its claim as a tombstone; only a kept file frees its claim.
            ReleaseStep::Released => match result.child::<Released, MigrationError>()?? {
                Released::Kept => {
                    self.step = ReleaseStep::Cleared;
                    Ok(Next::Call(claim_call(self.claim()?, None)))
                }
                Released::Deleted => Ok(Next::Done(true)),
                Released::Retry => Ok(Next::Done(false)),
            },
            ReleaseStep::Cleared => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum FinishStep {
    Synced,
    Row,
    Delete,
    Tombstone,
    TombstoneSynced,
    Forget,
}

/// Sends the delete of a claim marked deleting once more; it answers for a missing file too.
/// The tombstone follows, then the row that named the version.
#[derive(Debug, PartialEq)]
struct FinishDeleteOperation {
    file: ReleaseFile,
    claim: Claim,
    step: FinishStep,
}

impl FinishDeleteOperation {
    fn new(file: ReleaseFile, claim: Claim) -> Self {
        Self {
            file,
            claim,
            step: FinishStep::Synced,
        }
    }

    fn call(&mut self, step: FinishStep, call: IoCall) -> Turn<bool> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn send_delete(&mut self, etag: Option<String>) -> Turn<bool> {
        let delete = self.file.delete_call(etag);
        self.call(FinishStep::Delete, delete)
    }

    fn deleted(&mut self, event: Event) -> Turn<bool> {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) => {
                let write = claim_call(&self.claim, Some(TOMBSTONE));
                self.call(FinishStep::Tombstone, write)
            }
            // Other bytes at an unversioned path stay, and so does the claim: an earlier send
            // may still land.
            Event::Blob(BlobEvent::Error(BlobError::CleanupUnsupported))
                if self.file.version.is_empty() =>
            {
                Ok(Next::Done(true))
            }
            event => {
                warn!(event = event.label(), "A sent delete could not be finished");
                Ok(Next::Done(false))
            }
        }
    }
}

impl Operation for FinishDeleteOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        if !self.file.has_blob {
            return Ok(Next::Done(false));
        }
        // The deleting mark may be only buffered from a failed sync; it must be on disk first.
        Ok(Next::Call(IoCall::Sync))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match self.step {
            FinishStep::Synced => {
                result.done()?;
                // An unversioned delete must find the ETag the import recorded.
                if !self.file.version.is_empty() {
                    return self.send_delete(None);
                }
                let read = read_row(IMPORTED_OBJECT_KEYSPACE, self.file.row_key());
                self.call(FinishStep::Row, read)
            }
            FinishStep::Row => {
                let row = imported_row(&result.value()?)?;
                let version_id = self.file.version_id;
                let etag = row
                    .filter(|row| row.version_id == version_id)
                    .and_then(|row| row.etag);
                self.send_delete(etag)
            }
            FinishStep::Delete => self.deleted(result.event()?),
            FinishStep::Tombstone => {
                result.done()?;
                self.call(FinishStep::TombstoneSynced, IoCall::Sync)
            }
            FinishStep::TombstoneSynced => {
                result.done()?;
                let forget = ForgetDeletedOperation::new(self.file.clone(), self.claim.clone());
                self.call(FinishStep::Forget, IoCall::child(forget))
            }
            FinishStep::Forget => Ok(Next::Done(result.child::<bool, MigrationError>()??)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ForgetStep {
    Row,
    Pinned(Option<Value>),
    Dropped,
    Cleared,
}

/// Drops the row that named the deleted file, unless another location of the path keeps it;
/// an unversioned claim goes after it.
#[derive(Debug, PartialEq)]
struct ForgetDeletedOperation {
    file: ReleaseFile,
    claim: Claim,
    step: ForgetStep,
}

impl ForgetDeletedOperation {
    fn new(file: ReleaseFile, claim: Claim) -> Self {
        Self {
            file,
            claim,
            step: ForgetStep::Row,
        }
    }

    fn after_row(&mut self) -> Turn<bool> {
        if !self.file.version.is_empty() {
            return Ok(Next::Done(true));
        }
        self.step = ForgetStep::Cleared;
        Ok(Next::Call(claim_call(&self.claim, None)))
    }
}

impl Operation for ForgetDeletedOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        Ok(Next::Call(read_row(
            IMPORTED_OBJECT_KEYSPACE,
            self.file.row_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match std::mem::replace(&mut self.step, ForgetStep::Cleared) {
            ForgetStep::Row => {
                let pinned = PinnedOperation::new(&self.file.location, &self.file.version);
                self.step = ForgetStep::Pinned(result.value()?);
                Ok(Next::Call(IoCall::child(pinned)))
            }
            ForgetStep::Pinned(observed) => {
                // Another location of the path at another backend version keeps the row.
                let pinned = result.child::<Option<bool>, MigrationError>()?;
                let shared = !matches!(pinned, Ok(Some(false)));
                let row = observed.as_deref().map(ImportedObject::from_bytes);
                let version_id = self.file.version_id;
                let named = matches!(row, Some(Ok(row)) if row.version_id == version_id);
                match observed.filter(|_| named && !shared) {
                    Some(observed) => {
                        let drop = DropRowOperation::new(self.file.row_key(), observed.to_vec());
                        self.step = ForgetStep::Dropped;
                        Ok(Next::Call(IoCall::child(drop)))
                    }
                    None => self.after_row(),
                }
            }
            ForgetStep::Dropped => match result.child::<bool, MigrationError>()?? {
                true => self.after_row(),
                false => Ok(Next::Done(false)),
            },
            ForgetStep::Cleared => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}

/// How a release ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Released {
    /// Something still reads the file, or the storage cannot delete one version: it stays.
    Kept,
    Deleted,
    Retry,
}

#[derive(Debug, PartialEq)]
enum ClaimedStep {
    Row,
    Referenced,
    Pinned,
    Retired,
    RetireSynced,
    Deleting,
    DeletingSynced,
    Delete,
    Tombstone,
    TombstoneSynced,
    Dropped,
    Cleared,
}

/// Releases a claimed file: it goes when nothing reads it, its old location retires first, and
/// the claim records each step that cannot be undone.
#[derive(Debug, PartialEq)]
struct ReleaseClaimedOperation {
    file: ReleaseFile,
    claim: Claim,
    step: ClaimedStep,
    observed: Option<Value>,
    etag: Option<String>,
    shared: bool,
    now_ms: u64,
}

impl ReleaseClaimedOperation {
    fn new(file: ReleaseFile, claim: Claim) -> Self {
        Self {
            file,
            claim,
            step: ClaimedStep::Row,
            observed: None,
            etag: None,
            shared: false,
            now_ms: 0,
        }
    }

    fn call(&mut self, step: ClaimedStep, call: IoCall) -> Turn<Released> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn row_read(&mut self, observed: Option<Value>) -> Turn<Released> {
        let row = imported_row(&observed)?;
        self.observed = observed;
        // Another write took the path, so the file there is not the renamed one.
        if row
            .as_ref()
            .is_some_and(|row| row.version_id != self.file.version_id)
        {
            return Ok(Next::Done(Released::Kept));
        }
        self.etag = row.and_then(|row| row.etag);
        let referenced = ReferencedOperation::new(&self.file);
        self.call(ClaimedStep::Referenced, IoCall::child(referenced))
    }

    /// The rename's own old location goes first, whatever the storage's cleanup strategy.
    fn retire(&mut self) -> Turn<Released> {
        let Ok(key) = self.file.location.location_key() else {
            return Ok(Next::Done(Released::Retry));
        };
        let candidate = ReclaimCandidateKey::new(key.backend, key.encoding, key.blake3_hash);
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
        let retire = ReclaimBlobOperation::new(candidate, now, now).forced();
        self.call(ClaimedStep::Retired, IoCall::child(retire))
    }

    fn retired(&mut self, verdict: Result<ReclaimVerdict, ReclaimBlobError>) -> Turn<Released> {
        match verdict {
            Ok(ReclaimVerdict::Freed { .. } | ReclaimVerdict::Dropped) => {
                self.call(ClaimedStep::RetireSynced, IoCall::Sync)
            }
            Ok(ReclaimVerdict::Pinned | ReclaimVerdict::NotDue) => Ok(Next::Done(Released::Retry)),
            Err(error) => {
                warn!(%error, "Old location of a renamed file could not be retired");
                Ok(Next::Done(Released::Retry))
            }
        }
    }

    fn deleted(&mut self, event: Event) -> Turn<Released> {
        match event {
            // The tombstone is on disk before anything else may fail.
            Event::Blob(BlobEvent::DeleteFinished) => {
                let write = claim_call(&self.claim, Some(TOMBSTONE));
                self.call(ClaimedStep::Tombstone, write)
            }
            // A storage that cannot delete this exact version surely kept the file and its row.
            Event::Blob(BlobEvent::Error(BlobError::CleanupUnsupported)) => {
                Ok(Next::Done(Released::Kept))
            }
            event => {
                warn!(event = event.label(), "Renamed file could not be deleted");
                Ok(Next::Done(Released::Retry))
            }
        }
    }

    fn drop_row(&mut self) -> Turn<Released> {
        match self.observed.take().filter(|_| !self.shared) {
            Some(observed) => {
                let drop = DropRowOperation::new(self.file.row_key(), observed.to_vec());
                self.call(ClaimedStep::Dropped, IoCall::child(drop))
            }
            None => self.clear_claim(),
        }
    }

    /// An unversioned path has no exact version to tombstone: once its row is gone, a later
    /// file there is new and may be registered again.
    fn clear_claim(&mut self) -> Turn<Released> {
        if !self.file.version.is_empty() {
            return Ok(Next::Done(Released::Deleted));
        }
        let clear = claim_call(&self.claim, None);
        self.call(ClaimedStep::Cleared, clear)
    }
}

impl Operation for ReleaseClaimedOperation {
    type Output = Released;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Released> {
        Ok(Next::Call(read_row(
            IMPORTED_OBJECT_KEYSPACE,
            self.file.row_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<Released> {
        match self.step {
            ClaimedStep::Row => self.row_read(result.value()?),
            ClaimedStep::Referenced => match result.child::<bool, MigrationError>()? {
                Ok(true) => Ok(Next::Done(Released::Kept)),
                Ok(false) => {
                    let pinned = PinnedOperation::new(&self.file.location, &self.file.version);
                    self.call(ClaimedStep::Pinned, IoCall::child(pinned))
                }
                Err(error) => {
                    warn!(%error, "References of a renamed file could not be read");
                    Ok(Next::Done(Released::Retry))
                }
            },
            // A stored location of the file, also one waiting for reclaim, may still be reused;
            // one of another backend version at the path keeps the path's row.
            ClaimedStep::Pinned => match result.child::<Option<bool>, MigrationError>()? {
                Ok(Some(shared)) => {
                    self.shared = shared;
                    self.retire()
                }
                Ok(None) => Ok(Next::Done(Released::Retry)),
                Err(error) => {
                    warn!(%error, "Locations of a renamed file could not be read");
                    Ok(Next::Done(Released::Retry))
                }
            },
            ClaimedStep::Retired => self.retired(result.child()?),
            // The rename and the retirement are on disk before the file is deleted.
            ClaimedStep::RetireSynced => {
                result.done()?;
                if !self.file.has_blob {
                    return Ok(Next::Done(Released::Retry));
                }
                // From here the claim is never cleared: the delete may land whatever it answers.
                let write = claim_call(&self.claim, Some(DELETING));
                self.call(ClaimedStep::Deleting, write)
            }
            ClaimedStep::Deleting => {
                result.done()?;
                self.call(ClaimedStep::DeletingSynced, IoCall::Sync)
            }
            ClaimedStep::DeletingSynced => {
                result.done()?;
                let etag = self.etag.take().filter(|_| self.file.version.is_empty());
                let delete = self.file.delete_call(etag);
                self.call(ClaimedStep::Delete, delete)
            }
            ClaimedStep::Delete => self.deleted(result.event()?),
            ClaimedStep::Tombstone => {
                result.done()?;
                self.call(ClaimedStep::TombstoneSynced, IoCall::Sync)
            }
            ClaimedStep::TombstoneSynced => {
                result.done()?;
                self.drop_row()
            }
            ClaimedStep::Dropped => match result.child::<bool, MigrationError>()?? {
                true => self.clear_claim(),
                false => Ok(Next::Done(Released::Retry)),
            },
            ClaimedStep::Cleared => {
                result.done()?;
                Ok(Next::Done(Released::Deleted))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum DropStep {
    Page,
    Claims(Vec<(Key, Key)>),
    Deleted,
}

/// Drops a page of the claims and tombstones of a removed rule; a claim whose delete was sent
/// stays until its release settles it. Outputs `true` once none is left.
#[derive(Debug, PartialEq)]
pub(crate) struct DropClaimsOperation {
    pages: PrefixPages,
    read: usize,
    seen: bool,
    step: DropStep,
}

impl DropClaimsOperation {
    pub(crate) fn new(import_id: Ulid) -> Self {
        let prefix = import_id.to_bytes().to_vec();
        Self {
            pages: PrefixPages::new(CLAIM_INDEX_KEYSPACE, prefix, RULE_PAGE),
            read: 0,
            seen: false,
            step: DropStep::Page,
        }
    }

    fn next_page(&mut self) -> Turn<bool> {
        if self.pages.done() || self.read == DROP_PAGES {
            return Ok(Next::Done(!self.seen));
        }
        self.read += 1;
        self.step = DropStep::Page;
        Ok(Next::Call(self.pages.call()))
    }
}

impl Operation for DropClaimsOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        self.next_page()
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match std::mem::replace(&mut self.step, DropStep::Deleted) {
            DropStep::Page => {
                let rows = self.pages.take(result)?;
                self.seen |= !rows.is_empty();
                if rows.is_empty() {
                    return self.next_page();
                }
                let keys: Vec<(Key, Key)> = (rows.into_iter())
                    .map(|(key, _)| {
                        let claim: Key = key.get(16..).unwrap_or_default().to_vec().into();
                        (key, claim)
                    })
                    .collect();
                let reads = (keys.iter())
                    .map(|(_, claim)| (DELETE_CLAIM_KEYSPACE.to_string(), claim.clone()))
                    .collect();
                self.step = DropStep::Claims(keys);
                Ok(Next::Call(IoCall::Read(reads)))
            }
            DropStep::Claims(keys) => {
                let mut deletes = Vec::with_capacity(keys.len() * 2);
                for ((key, claim), value) in keys.into_iter().zip(result.values()?) {
                    if value.as_deref() == Some(DELETING) {
                        continue;
                    }
                    deletes.push((DELETE_CLAIM_KEYSPACE.to_string(), claim));
                    deletes.push((CLAIM_INDEX_KEYSPACE.to_string(), key));
                }
                if deletes.is_empty() {
                    return self.next_page();
                }
                self.step = DropStep::Deleted;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            DropStep::Deleted => {
                result.done()?;
                Ok(Next::Done(false))
            }
        }
    }
}

fn same_path(left: &BackendLocation, right: &BackendLocation) -> bool {
    (&left.root, &left.storage_bucket, &left.backend_path)
        == (&right.root, &right.storage_bucket, &right.backend_path)
}

fn version_read(location: &BackendLocation) -> IoCall {
    read_row(BACKEND_VERSION_KEYSPACE, version_key(location.ulid))
}

/// Whether a backend version row names other bytes than `version`.
fn other_version(value: Option<Value>, version: &str) -> Result<bool, MigrationError> {
    let found = value
        .map(|value| String::from_utf8(value.to_vec()))
        .transpose()
        .map_err(MigrationError::failed)?;
    Ok(found.is_some_and(|found| found != version))
}

fn iter_rows(event: Event) -> Result<(Vec<(Key, Value)>, Option<Key>), MigrationError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        other => Err(MigrationError::Failed(format!(
            "could not read rows: {}",
            other.label()
        ))),
    }
}

#[derive(Debug, PartialEq)]
enum ReferenceStep {
    Aliases,
    Version,
    Location,
    Provider,
    Owners,
    Rules,
    RuleRow,
}

/// Whether a version, an archive owner or another rule's row still names the copy.
#[derive(Debug, PartialEq)]
struct ReferencedOperation {
    location: BackendLocation,
    import_id: Ulid,
    version: String,
    wanted: Option<BlobLocationKey>,
    step: ReferenceStep,
    start: Option<Key>,
    more: Option<Key>,
    aliases: VecDeque<HashIndex>,
    rules: PrefixPages,
    found_rules: VecDeque<Value>,
}

impl ReferencedOperation {
    fn new(file: &ReleaseFile) -> Self {
        Self {
            location: file.location.clone(),
            import_id: file.import_id,
            version: file.version.clone(),
            wanted: None,
            step: ReferenceStep::Aliases,
            start: None,
            more: None,
            aliases: VecDeque::new(),
            rules: PrefixPages::new(BACKEND_IMPORT_KEYSPACE, Key::from(Vec::new()), RULE_PAGE),
            found_rules: VecDeque::new(),
        }
    }

    fn call(&mut self, step: ReferenceStep, call: IoCall) -> Turn<bool> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn wanted(&self) -> Result<&BlobLocationKey, MigrationError> {
        (self.wanted.as_ref()).ok_or_else(|| MigrationError::Failed("no location".to_string()))
    }

    fn read_aliases(&mut self) -> Turn<bool> {
        let hash = self.wanted()?.blake3_hash;
        let aliases = iter_index_effect(&hash, self.start.take(), None)?;
        self.call(ReferenceStep::Aliases, aliases.into())
    }

    fn next_alias(&mut self) -> Turn<bool> {
        let Some(alias) = self.aliases.pop_front() else {
            return match self.more.take() {
                Some(next) => {
                    self.start = Some(next);
                    self.read_aliases()
                }
                None => {
                    let owners = owners_scan_effect(&ArchiveKey::of(&self.location), None);
                    self.call(ReferenceStep::Owners, owners.into())
                }
            };
        };
        let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
        let read = read_row(BLOB_VERSIONS_KEYSPACE, version.to_bytes()?);
        self.call(ReferenceStep::Version, read)
    }

    fn version_read(&mut self, value: Option<Value>) -> Turn<bool> {
        // A missing indexed version proves nothing, so the copy stays.
        let Some(value) = value else {
            return Ok(Next::Done(true));
        };
        let Some(other) = BlobVersion::from_bytes(&value)?.location_key() else {
            return self.next_alias();
        };
        let wanted = self.wanted()?;
        if other == *wanted {
            return Ok(Next::Done(true));
        }
        if other.backend != wanted.backend {
            return self.next_alias();
        }
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, other.to_bytes());
        self.call(ReferenceStep::Location, read)
    }

    /// Another copy of the same bytes, such as a literal-name import, may read the same file:
    /// the same path and no other known backend version.
    fn location_read(&mut self, value: Option<Value>) -> Turn<bool> {
        let Some(value) = value else {
            return self.next_alias();
        };
        let other = BackendLocation::from_bytes(&value)?;
        if !same_path(&other, &self.location) {
            return self.next_alias();
        }
        let read = version_read(&other);
        self.call(ReferenceStep::Provider, read)
    }

    /// Another rule on the same storage, such as one sharing the folder, may have a row there.
    fn next_rule(&mut self) -> Turn<bool> {
        while let Some(value) = self.found_rules.pop_front() {
            let rule = BackendImport::from_bytes(&value)?;
            let backend = &self.location.backend;
            let on = |id: Ulid| BackendRef::Group(id) == *backend;
            if rule.import_id == self.import_id
                || !(on(rule.backend_id) || on(rule.write_backend()))
            {
                continue;
            }
            let key = object_key(rule.import_id, &location.backend_path);
            if read_value(context, IMPORTED_OBJECT_KEYSPACE, key)
                .await?
                .is_some()
            {
                return Ok(true);
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(false),
        }
    }
}

/// Deletes the row only if it is still the one read before the file went; an import may have
/// committed a replacement meanwhile.
async fn drop_row(context: &DriverContext, key: Key, observed: Vec<u8>) -> bool {
    let txn_id = match send(context, StorageEffect::StartTransaction { read: false }).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        event => {
            warn!(?event, "Renamed file row could not be locked");
            return false;
        }
    };
    let read = StorageEffect::Read {
        key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let current = match send(context, read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        event => {
            warn!(?event, "Renamed file row could not be read again");
            send(context, StorageEffect::AbortTransaction { txn_id }).await;
            return false;
        }
    };
    if current.as_deref() != Some(observed.as_slice()) {
        send(context, StorageEffect::AbortTransaction { txn_id }).await;
        return true;
    }
    let delete = StorageEffect::Delete {
        key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
        key,
        txn_id: Some(txn_id),
    };
    if !matches!(
        send(context, delete).await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ) {
        send(context, StorageEffect::AbortTransaction { txn_id }).await;
        return false;
    }
    let commit = StorageEffect::CommitTransaction { txn_id };
    matches!(
        send(context, commit).await,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    )
}

async fn send(context: &DriverContext, effect: StorageEffect) -> Event {
    context.storage_handle.send_storage_effect(effect).await
}

/// The storage effect a records helper built.
fn storage(effect: Effect) -> Result<StorageEffect, String> {
    match effect {
        Effect::Storage(effect) => Ok(effect),
        _ => Err("not a storage effect".to_string()),
    }
}

async fn read_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Vec<u8>>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id: None,
    };
    let event = send(context, read).await;
    parse_read(event, |bytes| Ok(bytes.to_vec())).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat, StoredLayout};
    use aruna_storage::storage::FjallStorage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    const IMPORT: Ulid = Ulid::from_parts(1, 1);
    const VERSION: Ulid = Ulid::from_parts(2, 2);

    fn setup() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (dir, context)
    }

    fn old() -> BackendLocation {
        let ulid = Ulid::from_parts(3, 3);
        BackendLocation {
            backend: BackendRef::Group(Ulid::from_parts(4, 4)),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: format!("raw/{VERSION}_a.csv"),
            ulid,
            format: StoredFormat {
                layout: StoredLayout::Plain(ulid),
                ..StoredFormat::default()
            },
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![7u8; 32])]),
        }
    }

    async fn put(context: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        };
        let event = send(context, write).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn row(context: &DriverContext, import_id: Ulid) -> Option<Vec<u8>> {
        let key = object_key(import_id, &old().backend_path);
        read_value(context, IMPORTED_OBJECT_KEYSPACE, key)
            .await
            .unwrap()
    }

    async fn renamed_row(context: &DriverContext, import_id: Ulid) {
        let row = ImportedObject::written(VERSION, "in/a.csv".to_string(), 3, None);
        let key = object_key(import_id, &old().backend_path);
        put(
            context,
            IMPORTED_OBJECT_KEYSPACE,
            key,
            row.to_bytes().unwrap(),
        )
        .await;
    }

    async fn release(context: &DriverContext) -> bool {
        release_renamed(context, old(), IMPORT, VERSION, "v1".to_string()).await
    }

    /// A version of the same bytes on the old file's storage, stored with `encoding`.
    async fn alias_version(context: &DriverContext, encoding: EncodingClass) {
        let successor = VersionKey::new("b", "in/a.csv", Ulid::from_parts(5, 5));
        let location = old();
        let version = BlobVersion::materialized(
            [7u8; 32],
            location.backend.clone(),
            encoding,
            SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let key = successor.to_bytes().unwrap().into();
        put(
            context,
            BLOB_VERSIONS_KEYSPACE,
            key,
            version.to_bytes().unwrap(),
        )
        .await;
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let alias = HashIndex::new(
            [7u8; 32],
            successor.version_id,
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_parts(6, 6),
            node,
            "b",
            "in/a.csv",
        );
        let alias = alias.to_bytes().unwrap().into();
        put(context, PATHS_INDEX_KEYSPACE, alias, Vec::new()).await;
    }

    #[tokio::test]
    async fn keeps_shared_copy() {
        // A successor version that still names the old copy keeps the file and its row.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        let encoding = old().format.encoding();
        assert!(matches!(encoding, EncodingClass::Plain { .. }));
        alias_version(&context, encoding).await;
        assert!(release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());
    }

    #[tokio::test]
    async fn keeps_literal_alias() {
        // A literal-name import reads the same file through its own raw location.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        alias_version(&context, EncodingClass::Raw).await;
        let mut raw = old();
        raw.format = StoredFormat::default();
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert!(release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());

        // A raw copy elsewhere on the storage does not keep the file.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        alias_version(&context, EncodingClass::Raw).await;
        raw.backend_path = "raw/a.csv".to_string();
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert!(!release(&context).await);
    }

    #[tokio::test]
    async fn keeps_shared_file() {
        // Another rule on the same storage, as one sharing the folder, still lists the file.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        let other = Ulid::from_parts(8, 8);
        let rule = BackendImport {
            import_id: other,
            group_id: Ulid::from_parts(6, 6),
            backend_id: Ulid::from_parts(4, 4),
            backend_prefix: "raw/".to_string(),
            bucket: "c".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{name}".to_string(),
        };
        put(
            &context,
            BACKEND_IMPORT_KEYSPACE,
            rule.key(),
            rule.to_bytes().unwrap(),
        )
        .await;
        renamed_row(&context, other).await;
        assert!(release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());

        // Without any reference the delete waits for the blob storage.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        assert!(!release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());
    }

    #[tokio::test]
    async fn waits_for_locations() {
        // A stored location of the file, referenced or not, keeps the release waiting.
        let (_dir, context) = setup();
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(false));
        // The rename's own old location is retired by the release, so it does not pin.
        let own = old().location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            own,
            old().to_bytes().unwrap(),
        )
        .await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(false));
        let mut raw = old();
        raw.format = StoredFormat::default();
        raw.backend_path = "raw/elsewhere.csv".to_string();
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(false));
        raw.backend_path = old().backend_path;
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), None);
        renamed_row(&context, IMPORT).await;
        assert!(!release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());
        // A row of another backend version at the path reads other bytes: no pin, but the
        // path stays in use.
        let other = version_key(raw.ulid).to_vec();
        let value = b"v0".to_vec();
        put(&context, BACKEND_VERSION_KEYSPACE, other.into(), value).await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(true));
    }

    #[tokio::test]
    async fn claims_before_delete() {
        // The claim of the backend version stays while the release waits and goes once it ends.
        let claim = claim_key(&old(), "v1").unwrap();
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        assert!(!release(&context).await);
        let space = DELETE_CLAIM_KEYSPACE;
        assert!(
            read_value(&context, space, claim.clone())
                .await
                .unwrap()
                .is_some()
        );
        alias_version(&context, old().format.encoding()).await;
        assert!(release(&context).await);
        assert!(read_value(&context, space, claim).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn keeps_plain_unversioned() {
        // Without a backend version a file whose name only contains the id stays untouched.
        let (_dir, context) = setup();
        let mut location = old();
        location.backend_path = format!("raw/backup-{VERSION}.csv");
        let released = release_renamed(&context, location.clone(), IMPORT, VERSION, String::new());
        assert!(released.await);
        let claim = claim_key(&location, "").unwrap();
        let stored = read_value(&context, DELETE_CLAIM_KEYSPACE, claim).await;
        assert!(stored.unwrap().is_none());
    }

    #[tokio::test]
    async fn finishes_unversioned_tombstones() {
        // A tombstone left by a crash is finished without another delete: row and claim go.
        let (_dir, context) = setup();
        let claim = claim_key(&old(), "").unwrap();
        let index: Key = [&IMPORT.to_bytes()[..], &claim[..]].concat().into();
        assert!(claimed(&context, (&claim, &index), Some(TOMBSTONE)).await);
        renamed_row(&context, IMPORT).await;
        let released = release_renamed(&context, old(), IMPORT, VERSION, String::new());
        assert!(released.await);
        assert!(row(&context, IMPORT).await.is_none());
        let stored = read_value(&context, DELETE_CLAIM_KEYSPACE, claim).await;
        assert!(stored.unwrap().is_none());
    }

    #[tokio::test]
    async fn keeps_tombstones() {
        // A replay after the bytes went never clears their tombstone, also when the path
        // holds something else that a version reads now.
        let (_dir, context) = setup();
        let claim = claim_key(&old(), "v1").unwrap();
        let index: Key = [&IMPORT.to_bytes()[..], &claim[..]].concat().into();
        assert!(claimed(&context, (&claim, &index), Some(TOMBSTONE)).await);
        alias_version(&context, old().format.encoding()).await;
        renamed_row(&context, IMPORT).await;
        assert!(release(&context).await);
        let stored = read_value(&context, DELETE_CLAIM_KEYSPACE, claim)
            .await
            .unwrap();
        assert_eq!(stored.as_deref(), Some(TOMBSTONE));
    }

    #[tokio::test]
    async fn finishes_sent_deletes() {
        // A delete that was sent keeps its claim until it is finished, also when a version now
        // reads the path; without the blob storage it waits.
        let (_dir, context) = setup();
        let claim = claim_key(&old(), "v1").unwrap();
        let index: Key = [&IMPORT.to_bytes()[..], &claim[..]].concat().into();
        assert!(claimed(&context, (&claim, &index), Some(DELETING)).await);
        alias_version(&context, old().format.encoding()).await;
        assert!(!release(&context).await);
        let stored = read_value(&context, DELETE_CLAIM_KEYSPACE, claim.clone())
            .await
            .unwrap();
        assert_eq!(stored.as_deref(), Some(DELETING));
        // A removed rule keeps the claim until its delete is finished, then drops it.
        assert_eq!(drop_claims(&context, IMPORT).await, Some(false));
        let kept = read_value(&context, CLAIM_INDEX_KEYSPACE, index.clone()).await;
        assert!(kept.unwrap().is_some());
        assert!(claimed(&context, (&claim, &index), Some(TOMBSTONE)).await);
        assert_eq!(drop_claims(&context, IMPORT).await, Some(false));
        assert_eq!(drop_claims(&context, IMPORT).await, Some(true));
    }

    #[tokio::test]
    async fn drops_rule_claims() {
        // A removed rule's claims and tombstones go page by page, and only that rule's.
        let (_dir, context) = setup();
        let claim = claim_key(&old(), "v1").unwrap();
        let other = Ulid::from_parts(8, 8);
        for import in [IMPORT, other] {
            let index: Key = [&import.to_bytes()[..], &claim[..]].concat().into();
            assert!(claimed(&context, (&claim, &index), Some(PENDING)).await);
        }
        assert_eq!(drop_claims(&context, IMPORT).await, Some(false));
        assert_eq!(drop_claims(&context, IMPORT).await, Some(true));
        let index: Key = [&other.to_bytes()[..], &claim[..]].concat().into();
        let kept = read_value(&context, CLAIM_INDEX_KEYSPACE, index)
            .await
            .unwrap();
        assert!(kept.is_some());
    }

    #[tokio::test]
    async fn syncs_before_delete() {
        // The node storage answers the sync the release waits for before it deletes the file.
        let (_dir, context) = setup();
        assert!(synced(&context).await);
    }

    #[tokio::test]
    async fn drops_unchanged_row() {
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        let key = object_key(IMPORT, &old().backend_path);
        // An import replaced the row after it was read, so the new row stays.
        assert!(drop_row(&context, key.clone(), b"older".to_vec()).await);
        assert!(row(&context, IMPORT).await.is_some());
        let observed = row(&context, IMPORT).await.unwrap();
        assert!(drop_row(&context, key, observed).await);
        assert!(row(&context, IMPORT).await.is_none());
    }
}
