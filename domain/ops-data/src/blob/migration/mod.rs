//! Re-encodes one local version with its bucket's compression or moves it to the storage its rules
//! choose; publishes only if settings and version are unchanged, and reclaim keeps shared copies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod batch;
pub mod broken;
pub mod check;
pub mod count;
pub mod drain;
pub mod job_pages;
pub mod misplaced;
pub mod queue;
pub mod rewrite;
pub mod shared;

use aruna_core::id_mint::IdMint;
use std::collections::BTreeMap;
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, DELETE_CLAIM_KEYSPACE, IMPORT_INTENT_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, MANAGED_COPY_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
    STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, PlainPath, WriteIntent, claim_key, import_key, intent_key,
    intent_prefix, object_key, tagged_path, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, ManagedCopyKey, ManagedCopyRecord, ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::{Compression, EncodingClass, StoredLayout};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::{
    GroupStorageRouting, PlainTarget, RoutingError, RoutingSnapshot, resolve_backend,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::s3::object::put::fenced::{Fenced, FencedWrite};
use crate::s3::object::put::plain::{
    PlainRecord, intent_rows, owner_call, path_owned, same_name_path,
};
use crate::s3::usage::{StoredDelta, UsageCounterUpdate, UsageUpdateError};
use aruna_core::managed_copy::{ManagedCopyError, check_old_copy};
use aruna_core::structs::storage::cleanup::schedule_cleanup_effect;
use aruna_ops_identity::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use aruna_ops_identity::groups::storage_routing::routing_key;

/// Steps of one version migration; errors name the step they stopped in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrateState {
    Init,
    ReadVersion,
    ReadLocation,
    FindTarget,
    ProbeTarget,
    /// Settles the provider upload an earlier attempt recorded before its copy is adopted.
    SettleCopy,
    ReadSource,
    CheckLinked,
    ReadPlainOwner,
    StatPlain,
    ReadBlob,
    /// Stores the identity of a write to a linked folder before its bytes move.
    WriteIntent,
    /// Syncs that intent to disk.
    SyncIntent,
    WriteBlob,
    /// Reads what holds the name a refused rename wanted, which may be its own earlier copy.
    CheckTaken,
    /// Deletes the intents of a write the storage refused, then syncs that delete.
    DropIntent,
    SyncDroppedIntent,
    /// Waits for the child that publishes the copy in one transaction.
    Publish,
    ReadBucket,
    ScanRules,
    FenceTarget,
    CheckVersion,
    CheckCopy,
    ReadTarget,
    /// Reads the intents other rules hold for the linked file's path.
    ReadPeerIntents,
    WriteRows,
    /// Deletes the intents the published version makes obsolete.
    RetireIntents,
    UpdateUsage,
}

/// What happened to one version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrateOutcome {
    /// The version now names a copy in the target encoding.
    Migrated,
    /// Nothing to do: already encoded, not materialized, or changed meanwhile.
    Skipped,
}

#[derive(Debug, Error, PartialEq)]
pub enum MigrateVersionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    ManagedCopy(#[from] ManagedCopyError),
    #[error(transparent)]
    Routing(#[from] RoutingError),
    #[error(transparent)]
    Fence(#[from] BackendFenceError),
    #[error("the copy in the target encoding disappeared before it was adopted")]
    TargetMissing,
    #[error("the re-encoded copy does not match the original bytes")]
    ContentMismatch,
    #[error("version migration did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: {received}")]
    InvalidStateEvent {
        state: MigrateState,
        received: &'static str,
    },
}

impl From<IoError> for MigrateVersionError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type Turn = Result<Next<MigrateOutcome>, MigrateVersionError>;

/// Moves one version to a copy encoded with `target`, on the backend that holds it now, or with
/// routing inputs to the storage and encoding a new write of its key gets.
#[derive(Debug, PartialEq)]
pub struct MigrateVersionOperation {
    now_ms: u64,
    ids: IdMint,
    version_key: VersionKey,
    target: Compression,
    now: SystemTime,
    routing: Option<RoutingSnapshot>,
    /// Where a move writes, resolved from `routing` when the version is read.
    resolved: Option<ResolvedBackend>,
    /// The path in a linked folder a move writes to, and the path its stat settled on.
    plain: Option<PlainTarget>,
    /// Earlier write patterns of the bucket's import rules, by import id.
    earlier: Earlier,
    /// The version's linked file has a name of an earlier pattern and gets the current one.
    renaming: bool,
    /// The backend version of the renamed file; without one the old file is never deleted.
    source_version: Option<String>,
    plain_path: Option<PlainPath>,
    /// The file at the plain path may be replaced: the rule recorded it for this key.
    plain_owned: bool,
    /// The backend's id of the bytes a plain write stored.
    backend_version: Option<String>,
    /// The ETag the rule recorded for the renamed file; an unversioned copy must find it.
    source_etag: Option<String>,
    /// The delete claim of the file a rename adopts, which its publication must not find.
    adopt_claim: Option<Key>,
    /// The start of the storage move this run belongs to, which owns its write intents.
    mover: Option<u64>,
    /// The target row the publication read, kept while the peers' intents are read.
    target_row: Option<Option<Vec<u8>>>,
    /// The other rules of the path whose intents are still to read.
    peers: Vec<Ulid>,
    /// Why the storage refused the write, reported once its intents are gone.
    refusal: Option<BlobError>,
    /// The write intents stored for a write to a linked folder, at both paths it may reach.
    written_intent: Vec<Key>,
    /// The write that waits for its intent to be on disk.
    pending_write: Option<IoCall>,
    backend_etag: Option<String>,
    /// A finished copy found at the target row; on another backend it is probed before use.
    found: Option<BackendLocation>,
    /// The routing the publication transaction read.
    fresh: Option<RoutingSnapshot>,
    state: MigrateState,
    /// This instance is the publication: one write transaction over the rows of the move.
    publishing: bool,
    version: Option<BlobVersion>,
    old: Option<BackendLocation>,
    new: Option<BackendLocation>,
    /// The registration of a governed version, moved to the new copy on commit.
    copy: Option<ManagedCopyRecord>,
    /// Set when the commit made the new copy the owner of its location row.
    owns_row: bool,
    usage: Option<UsageCounterUpdate>,
}

impl MigrateVersionOperation {
    pub fn new(version_key: VersionKey, target: Compression, now: SystemTime) -> Self {
        Self {
            now_ms: 0,
            ids: IdMint::default(),
            version_key,
            target,
            now,
            routing: None,
            resolved: None,
            plain: None,
            plain_path: None,
            earlier: Earlier::new(),
            renaming: false,
            source_version: None,
            plain_owned: false,
            backend_version: None,
            source_etag: None,
            adopt_claim: None,
            mover: None,
            target_row: None,
            peers: Vec::new(),
            refusal: None,
            written_intent: Vec::new(),
            pending_write: None,
            backend_etag: None,
            found: None,
            fresh: None,
            state: MigrateState::Init,
            publishing: false,
            version: None,
            old: None,
            new: None,
            copy: None,
            owns_row: false,
            usage: None,
        }
    }

    /// Moves the version to where `routing` sends a new write of its key.
    pub fn with_routing(mut self, routing: RoutingSnapshot) -> Self {
        self.routing = Some(routing);
        self
    }

    /// Marks the write intents of this run as those of the move started at `started_ms`.
    pub fn with_mover(mut self, started_ms: u64) -> Self {
        self.mover = Some(started_ms);
        self
    }

    /// Lets a move give linked files of an earlier pattern their current name.
    pub fn with_earlier(mut self, earlier: Earlier) -> Self {
        self.earlier = earlier;
        self
    }

    /// The backend and encoding the version ends on, given the backend that holds it now.
    fn wanted(&self, current: &BackendRef) -> (BackendRef, EncodingClass) {
        match self.resolved.as_ref() {
            Some(resolved) => (resolved.backend.clone(), resolved.compression.into()),
            None => (current.clone(), self.target.into()),
        }
    }

    fn unexpected(&self, received: Event) -> Turn {
        Err(MigrateVersionError::InvalidStateEvent {
            state: self.state,
            received: received.label(),
        })
    }

    fn call(&mut self, state: MigrateState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn blob(&mut self, state: MigrateState, effect: BlobEffect) -> Turn {
        self.call(state, Effect::blob(effect).into())
    }

    fn read(&mut self, state: MigrateState, space: &str, key: impl Into<Key>) -> Turn {
        self.call(state, read_row(space, key))
    }

    /// Releases the written copy. One that no commit owns goes to the cleanup queue, whose
    /// reconcile step deletes it.
    fn leave_calls(&mut self) -> Vec<IoCall> {
        let Some(new) = self.new.take() else {
            return Vec::new();
        };
        let release = BlobEffect::ReleaseReservation { id: new.ulid };
        let mut calls = vec![Effect::blob(release).into()];
        if !self.owns_row {
            calls.push(schedule_cleanup_effect().into());
        }
        calls
    }

    fn finish(&mut self, output: MigrateOutcome) -> Turn {
        let calls = self.leave_calls();
        Ok(match calls.is_empty() {
            true => Next::Done(output),
            false => Next::Then { output, calls },
        })
    }

    fn skip(&mut self) -> Turn {
        self.finish(MigrateOutcome::Skipped)
    }

    fn handle_version(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.skip();
        };
        let version = BlobVersion::from_bytes(value.as_ref())?;
        if let Some(routing) = self.routing.as_ref() {
            match place(routing, &self.version_key) {
                Ok((resolved, plain)) => (self.resolved, self.plain) = (Some(resolved), plain),
                Err(error) => return Err(error.into()),
            }
        }
        let linked = self.plain.is_some();
        let Some(key) = source_key(&version, |current| self.wanted(current), linked) else {
            return self.skip();
        };
        self.version = Some(version);
        self.read(
            MigrateState::ReadLocation,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    fn handle_location(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.skip();
        };
        let old = BackendLocation::from_bytes(value.as_ref())?;
        self.renaming = match (self.routing.as_ref(), self.plain.as_ref()) {
            (Some(routing), Some(plain)) => {
                renames(routing, &self.earlier, plain, &old, &self.version_key)
            }
            _ => false,
        };
        if stays(&old, self.routing.is_some()) && !self.renaming {
            return self.skip();
        }
        let in_place = self
            .target_key_of(&old)
            .is_ok_and(|key| old.location_key() == Ok(key));
        let source = version_key(old.ulid);
        self.old = Some(old);
        if self.renaming {
            let (Some(plain), Some(old)) = (self.plain.as_ref(), self.old.as_ref()) else {
                return Err(MigrateVersionError::NotFinished);
            };
            // The rule's record of the old file names the ETag an unversioned copy needs.
            let row = object_key(plain.import_id, &old.backend_path);
            let reads = vec![
                (BACKEND_VERSION_KEYSPACE.to_string(), source),
                (IMPORTED_OBJECT_KEYSPACE.to_string(), row),
            ];
            return self.call(MigrateState::ReadSource, IoCall::Read(reads));
        }
        // A copy already on the rule's backend only needs its file in the linked folder.
        if let Some(plain) = self.plain.as_ref().filter(|_| in_place) {
            let key = object_key(plain.import_id, &plain.path);
            return self.read(MigrateState::CheckLinked, IMPORTED_OBJECT_KEYSPACE, key);
        }
        self.find_target()
    }

    /// The version of the old file, fixed when it was written, names the bytes a release deletes.
    fn handle_source(&mut self, values: Vec<Option<Value>>) -> Turn {
        let mut values = values.into_iter();
        let (value, row) = (values.next().flatten(), values.next().flatten());
        let version = value
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose();
        // An unversioned "null" names whatever the path holds later, never the bytes read.
        match version {
            Ok(version) => self.source_version = version.filter(|version| version != "null"),
            Err(error) => return Err(ConversionError::from(error).into()),
        }
        // Only the record of this version's write names its bytes.
        let id = self.version_key.version_id;
        {
            let row = row
                .map(|row| ImportedObject::from_bytes(&row))
                .transpose()?;
            self.source_etag = row
                .filter(|row| row.version_id == id)
                .and_then(|row| row.etag)
        }
        self.find_target()
    }

    fn find_target(&mut self) -> Turn {
        match self.target_key() {
            Ok(key) => self.read(
                MigrateState::FindTarget,
                BLOB_LOCATIONS_KEYSPACE,
                key.to_bytes(),
            ),
            Err(error) => Err(error),
        }
    }

    /// The row of this content in the target encoding, on the target backend.
    fn target_key(&self) -> Result<BlobLocationKey, MigrateVersionError> {
        let old = self.old.as_ref().ok_or(MigrateVersionError::NotFinished)?;
        self.target_key_of(old)
    }

    fn target_key_of(&self, old: &BackendLocation) -> Result<BlobLocationKey, MigrateVersionError> {
        let mut key = old.location_key()?;
        (key.backend, key.encoding) = self.wanted(&old.backend);
        Ok(key)
    }

    /// Whether the target lies on another backend than the copy the version names now.
    fn crosses(&self) -> bool {
        let old = self.old.as_ref();
        old.is_some_and(|old| self.wanted(&old.backend).0 != old.backend)
    }

    /// A linked-folder file this version already has needs no second one.
    fn handle_linked(&mut self, value: Option<Value>) -> Turn {
        let row = value.map(|value| ImportedObject::from_bytes(value.as_ref()));
        match row.transpose() {
            Ok(Some(row)) if row.version_id == self.version_key.version_id => self.skip(),
            Ok(_) => self.find_target(),
            Err(error) => Err(error.into()),
        }
    }

    /// A finished copy in the target encoding is adopted; on another backend only after a probe
    /// shows its bytes exist. A linked target writes its file first, as an upload does.
    fn handle_found(&mut self, value: Option<Value>) -> Turn {
        let found = match value.map(|value| BackendLocation::from_bytes(value.as_ref())) {
            Some(Ok(found)) => Some(found).filter(|found| !found.staging && !found.partial),
            Some(Err(error)) => return Err(error.into()),
            None => None,
        };
        self.found = found.clone();
        match found {
            Some(location) if self.crosses() => {
                self.blob(MigrateState::ProbeTarget, BlobEffect::StatCopy { location })
            }
            _ => self.after_found(),
        }
    }

    fn handle_probe(&mut self, event: Event) -> Turn {
        match event.into_blob() {
            Ok(BlobEvent::CopyStat { exists: true }) => self.after_found(),
            Ok(BlobEvent::CopyStat { exists: false }) => Err(MigrateVersionError::TargetMissing),
            Ok(BlobEvent::Error(error)) => Err(error.into()),
            Ok(other) => {
                let other = Event::blob(other);
                self.unexpected(other)
            }
            Err(other) => self.unexpected(other),
        }
    }

    fn after_found(&mut self) -> Turn {
        if let Some(read) = self.plain.as_ref().and_then(owner_call) {
            return self.call(MigrateState::ReadPlainOwner, read);
        }
        self.plain_owned = true;
        self.stat_plain()
    }

    fn handle_owner(&mut self, value: Option<Value>) -> Turn {
        if self.renaming {
            return self.check_name(value);
        }
        match path_owned(value.as_deref(), &self.version_key.key) {
            Ok(owned) => {
                self.plain_owned = owned;
                self.stat_plain()
            }
            Err(error) => Err(error.into()),
        }
    }

    /// A new name the rule records for another key or version is taken, so the rename asks
    /// the provider nothing.
    fn check_name(&mut self, value: Option<Value>) -> Turn {
        let row = value
            .map(|value| ImportedObject::from_bytes(&value))
            .transpose()?;
        let (Some(plain), Some(routing)) = (self.plain.as_ref(), self.routing.as_ref()) else {
            return Err(MigrateVersionError::NotFinished);
        };
        let rule = (routing.imports.iter()).find(|rule| rule.import_id == plain.import_id);
        // An older record without a key keeps the rule's own mapping.
        let holder = row.map(|row| {
            let key = row
                .key
                .or_else(|| rule.and_then(|rule| rule.aruna_key(&plain.path)));
            (key, row.version_id)
        });
        let ours = (
            Some(self.version_key.key.clone()),
            self.version_key.version_id,
        );
        match holder {
            Some(holder) if holder != ours => Err(BlobError::PathTaken.into()),
            _ => self.stat_plain(),
        }
    }

    fn stat_plain(&mut self) -> Turn {
        // A rename creates its exact name only while free; a refused copy shows what holds it.
        if self.renaming
            && let Some(plain) = self.plain.as_ref()
        {
            let path = plain.path.clone();
            self.plain_path = Some(PlainPath {
                path,
                ulid: self.attempt(),
                version_id: self.version_key.version_id,
                if_not_exists: true,
                exact: true,
            });
            return self.read_blob();
        }
        if let Some(plain) = self.plain.as_ref() {
            return self.blob(
                MigrateState::StatPlain,
                BlobEffect::StatPlain {
                    backend: plain.backend.clone(),
                    path: plain.path.clone(),
                },
            );
        }
        if self.found.is_none() {
            return self.read_blob();
        }
        self.start_publication()
    }

    /// The path takes the file as an upload would: free, versioned, or tagged. Each attempt
    /// names its own id, so a retry never overwrites a file an earlier attempt left there.
    fn handle_stat(&mut self, event: Event) -> Turn {
        let (exists, version) = match event.into_blob() {
            Ok(BlobEvent::PlainStat { exists, version }) => (exists, version),
            Ok(BlobEvent::Error(error)) => return Err(error.into()),
            Ok(other) => {
                let other = Event::blob(other);
                return self.unexpected(other);
            }
            Err(other) => return self.unexpected(other),
        };
        let attempt = self.attempt();
        let Some(plain) = self.plain.as_ref() else {
            return Err(MigrateVersionError::NotFinished);
        };
        let versioned = plain.same_name && version.is_some() && self.plain_owned;
        let (path, if_not_exists) = same_name_path(&plain.path, attempt, exists, versioned);
        self.plain_path = Some(PlainPath {
            path,
            ulid: attempt,
            version_id: self.version_key.version_id,
            if_not_exists,
            exact: self.renaming,
        });
        self.read_blob()
    }

    /// A refused rename looks at what holds its name: the new name, or on a storage without
    /// create-only writes the tagged one an earlier attempt may have used.
    fn stat_taken(&mut self, refusal: BlobError) -> Turn {
        let (Some(old), Some(plain)) = (self.old.as_ref(), self.plain_path.as_ref()) else {
            return Err(MigrateVersionError::NotFinished);
        };
        let path = match refusal {
            BlobError::ConditionalUnsupported => tagged_path(&plain.path, plain.ulid),
            _ => plain.path.clone(),
        };
        let resolved = ResolvedBackend::new(old.backend.clone(), old.storage_class.clone());
        let created_by = old.created_by;
        self.refusal = Some(refusal);
        self.blob(
            MigrateState::CheckTaken,
            BlobEffect::StatExternal {
                resolved,
                path,
                created_by,
            },
        )
    }

    /// The file there is a copy an earlier attempt left when its size and ETag prove the old
    /// file's bytes; any other file, or one its checksums cannot decide, keeps the name taken.
    fn handle_taken(&mut self, event: Event) -> Turn {
        let (found, version) = match event.into_blob() {
            Ok(BlobEvent::ExternalStat {
                location,
                version,
                etag,
            }) => {
                let old = self.old.as_ref();
                let recorded = self.source_etag.as_deref();
                // A copy may keep the old file's recorded multipart ETag. A write's empty
                // reservation is never found here: a stat shows it as no file.
                let proved = |found: &BackendLocation| {
                    let size = found.blob_size;
                    let kept = |old: &BackendLocation| {
                        let unquoted = |etag: &str| etag.trim_matches('"').to_string();
                        size == old.blob_size
                            && recorded.is_some_and(|recorded| {
                                etag.as_deref().map(unquoted) == Some(unquoted(recorded))
                            })
                    };
                    let same = |old| same_bytes(old, size, etag.as_deref()) || kept(old);
                    old.is_some_and(same)
                };
                (location.filter(proved), version)
            }
            Ok(BlobEvent::Error(_)) => (None, None),
            Ok(other) => {
                let other = Event::blob(other);
                return self.unexpected(other);
            }
            Err(other) => return self.unexpected(other),
        };
        let (Some(mut location), Some(old)) = (found, self.old.as_ref()) else {
            return self.drop_intents();
        };
        location.hashes = old.hashes.clone();
        location.format.layout = StoredLayout::Plain(location.ulid);
        // An adopted file whose backend version a release deleted is no copy to take.
        self.adopt_claim = match version
            .as_deref()
            .map(|version| claim_key(&location, version))
        {
            Some(Ok(claim)) => Some(claim),
            Some(Err(error)) => return Err(error.into()),
            None => None,
        };
        self.refusal = None;
        self.backend_version = version;
        self.new = Some(location);
        // An earlier attempt left this copy and its intents; the publication retires them,
        // after the provider upload that attempt recorded is settled.
        let attempt = self.attempt();
        self.written_intent = self.intent_keys(attempt);
        let attempt = self.attempt();
        self.blob(
            MigrateState::SettleCopy,
            BlobEffect::SettleCopy {
                attempt,
                upload: None,
            },
        )
    }

    /// A refused write moved no byte, so its intents go before the move fails with it.
    fn drop_intents(&mut self) -> Turn {
        if self.written_intent.is_empty() {
            let refusal = self.refusal.take().unwrap_or(BlobError::PathTaken);
            return Err(refusal.into());
        }
        let deletes = self.intent_deletes();
        self.call(MigrateState::DropIntent, deletes)
    }

    fn intent_deletes(&self) -> IoCall {
        let space = IMPORT_INTENT_KEYSPACE.to_string();
        let keys = self.written_intent.iter();
        IoCall::Delete(keys.map(|key| (space.clone(), key.clone())).collect())
    }

    fn handle_settled(&mut self, event: Event) -> Turn {
        match event.into_blob() {
            Ok(BlobEvent::UploadAborted) => self.start_publication(),
            Ok(BlobEvent::Error(error)) => Err(error.into()),
            Ok(other) => {
                let other = Event::blob(other);
                self.unexpected(other)
            }
            Err(other) => self.unexpected(other),
        }
    }

    fn read_blob(&mut self) -> Turn {
        let Some(old) = self.old.clone() else {
            return Err(MigrateVersionError::NotFinished);
        };
        // A rename copies the old file; an S3 storage does so without passing the bytes.
        if let (true, Some(plain), Some(resolved)) = (
            self.renaming,
            self.plain_path.clone(),
            self.resolved.clone(),
        ) {
            let created_by = old.created_by;
            let copy = Effect::blob(BlobEffect::CopyPlain {
                resolved,
                plain,
                source: Box::new(old),
                etag: self.source_etag.clone(),
            });
            let copy = copy.into();
            return self.intent_first(copy, created_by);
        }
        self.blob(MigrateState::ReadBlob, BlobEffect::Read { location: old })
    }

    fn handle_read(&mut self, event: Event) -> Turn {
        let blob = match event.into_blob() {
            Ok(BlobEvent::ReadFinished { blob, .. }) => blob,
            Ok(BlobEvent::Error(error)) => return Err(error.into()),
            Ok(other) => {
                let other = Event::blob(other);
                return self.unexpected(other);
            }
            Err(other) => return self.unexpected(other),
        };
        let Some(old) = self.old.as_ref() else {
            return Err(MigrateVersionError::NotFinished);
        };
        let resolved = self.resolved.clone().unwrap_or_else(|| {
            ResolvedBackend::new(old.backend.clone(), old.storage_class.clone())
                .with_compression(self.target)
        });
        if let Some(plain) = self.plain_path.clone() {
            let created_by = old.created_by;
            let write = Effect::blob(BlobEffect::WritePlain {
                resolved,
                plain,
                created_by,
                blob,
            });
            let write = write.into();
            return self.intent_first(write, created_by);
        }
        let effect = Effect::blob(BlobEffect::Write {
            bucket: self.version_key.bucket.clone(),
            key: self.version_key.key.clone(),
            resolved,
            created_by: old.created_by,
            blob,
            size: None,
        });
        self.call(MigrateState::WriteBlob, effect.into())
    }

    /// A rename keeps one attempt id per old file, apart from the old file's own id, so a
    /// retry stores the same intents and names the same copy.
    fn attempt(&mut self) -> Ulid {
        match (self.renaming, self.old.as_ref(), self.plain_path.as_ref()) {
            (true, Some(old), _) => Ulid::from(u128::from(old.ulid) ^ 1),
            (_, _, Some(path)) => path.ulid,
            _ => self.ids.mint(self.now_ms),
        }
    }

    /// The intent keys of a write of this version to the linked path and its tagged path.
    fn intent_keys(&self, attempt: Ulid) -> Vec<Key> {
        let Some(target) = self.plain.as_ref().filter(|target| target.listed) else {
            return Vec::new();
        };
        let tagged = tagged_path(&target.path, attempt);
        let id = self.version_key.version_id;
        [target.path.as_str(), tagged.as_str()]
            .map(|path| intent_key(target.import_id, path, id))
            .into()
    }

    /// The source bytes are known, so the intent proves the copy even if no later record lands.
    fn write_intent(
        &self,
        target: &PlainTarget,
        created_by: aruna_core::user::id::UserId,
    ) -> WriteIntent {
        let source = self.old.as_ref().and_then(|old| old.get_blake3());
        WriteIntent {
            key: self.version_key.key.clone(),
            version_id: self.version_key.version_id,
            backend: target.backend.clone(),
            created_by,
            written: source.and_then(|hash| hash.try_into().ok()),
            mover: self.mover,
        }
    }

    /// A write to a linked folder stores its identity at the path and at its tagged path, and
    /// syncs it, before any byte moves; an import then registers the file as this version.
    fn intent_first(&mut self, write: IoCall, created_by: aruna_core::user::id::UserId) -> Turn {
        let listed = self.plain.as_ref().filter(|target| target.listed);
        let (Some(target), Some(path)) = (listed, self.plain_path.as_ref()) else {
            return self.call(MigrateState::WriteBlob, write);
        };
        let keys = self.intent_keys(path.ulid);
        let intent = self.write_intent(target, created_by);
        let Some(routing) = self.routing.as_ref() else {
            return Err(MigrateVersionError::NotFinished);
        };
        let rule = import_key(routing.group_id, target.import_id);
        match intent_rows(&keys, &intent) {
            Ok(rows) => {
                self.written_intent = keys;
                self.pending_write = Some(write);
                let fence = IoCall::child(FencedWrite::new(rule, rows));
                self.call(MigrateState::WriteIntent, fence)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// The intent is stored with a read of its rule, then synced before the bytes move; a
    /// removed rule stores nothing and the move leaves the version.
    fn handle_intent(&mut self, result: IoResult) -> Turn {
        match result.child::<Fenced, StorageError>()? {
            Ok(Fenced::Written) => self.call(MigrateState::SyncIntent, IoCall::Sync),
            Ok(Fenced::RuleGone) => {
                self.written_intent.clear();
                self.skip()
            }
            Err(error) => Err(error.into()),
        }
    }

    fn handle_synced(&mut self, result: IoResult) -> Turn {
        result.done()?;
        let write = self.pending_write.take();
        let write = write.ok_or(MigrateVersionError::NotFinished)?;
        self.call(MigrateState::WriteBlob, write)
    }

    /// The publication runs as a child, in one write transaction of its own.
    fn start_publication(&mut self) -> Turn {
        let child = IoCall::child(self.publish_child());
        self.call(MigrateState::Publish, child)
    }

    fn publish_child(&self) -> Self {
        Self {
            routing: self.routing.clone(),
            resolved: self.resolved.clone(),
            plain: self.plain.clone(),
            plain_path: self.plain_path.clone(),
            renaming: self.renaming,
            source_version: self.source_version.clone(),
            backend_version: self.backend_version.clone(),
            source_etag: self.source_etag.clone(),
            adopt_claim: self.adopt_claim.clone(),
            written_intent: self.written_intent.clone(),
            backend_etag: self.backend_etag.clone(),
            found: self.found.clone(),
            publishing: true,
            version: self.version.clone(),
            old: self.old.clone(),
            new: self.new.clone(),
            ..Self::new(self.version_key.clone(), self.target, self.now)
        }
    }

    /// The child released the copy after its commit; its failure leaves that to `abort`.
    fn published(&mut self, result: IoResult) -> Turn {
        let outcome = result.child::<MigrateOutcome, MigrateVersionError>()??;
        self.new = None;
        Ok(Next::Done(outcome))
    }

    /// A refused write moved no byte, so its intents go for good before the move fails.
    fn handle_dropped(&mut self, result: IoResult) -> Turn {
        result.done()?;
        if self.state == MigrateState::DropIntent {
            return self.call(MigrateState::SyncDroppedIntent, IoCall::Sync);
        }
        self.written_intent.clear();
        Err(self.refusal.take().unwrap_or(BlobError::PathTaken).into())
    }

    fn handle_written(&mut self, event: Event) -> Turn {
        let new = match event.into_blob() {
            Ok(BlobEvent::WriteFinished { location }) => location,
            Ok(BlobEvent::PlainWritten {
                location,
                version,
                etag,
            }) => {
                self.backend_version = version;
                self.backend_etag = etag;
                location
            }
            // A refused rename may meet its own earlier copy.
            Ok(BlobEvent::Error(
                error @ (BlobError::PathTaken | BlobError::ConditionalUnsupported),
            )) if self.renaming => return self.stat_taken(error),
            // A refused write moved no byte; its intents go before the move fails with it.
            Ok(BlobEvent::Error(
                error @ (BlobError::PathTaken | BlobError::ConditionalUnsupported),
            )) if !self.written_intent.is_empty() => {
                self.refusal = Some(error);
                return self.drop_intents();
            }
            // The adapter keeps a copy with an unclear outcome for reconciliation.
            Ok(BlobEvent::Error(error)) => return Err(error.into()),
            Ok(other) => {
                let other = Event::blob(other);
                return self.unexpected(other);
            }
            Err(other) => return self.unexpected(other),
        };
        let same = self.old.as_ref().is_some_and(|old| {
            old.get_blake3() == new.get_blake3() && old.blob_size == new.blob_size
        });
        self.new = Some(new);
        if !same {
            return Err(MigrateVersionError::ContentMismatch);
        }
        self.start_publication()
    }

    /// The publication's first read: the bucket, its settings and, for a move, the group's rules.
    fn read_bucket(&mut self) -> Turn {
        let bucket: Key = self.version_key.bucket.as_bytes().to_vec().into();
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket),
        ];
        if let Some(routing) = self.routing.as_ref() {
            let key = routing_key(routing.group_id);
            reads.push((STORAGE_ROUTING_KEYSPACE.to_string(), key));
        }
        self.call(MigrateState::ReadBucket, IoCall::Read(reads))
    }

    /// The rules as this transaction reads them, without import rules yet; `None` when the
    /// bucket is gone or encrypts.
    fn fresh_routing(
        &self,
        values: Vec<Option<Value>>,
    ) -> Result<Option<RoutingSnapshot>, MigrateVersionError> {
        let Some(mut routing) = self.routing.clone() else {
            return Ok(None);
        };
        let mut values = values.into_iter();
        let (Some(info), Some(settings), Some(group)) =
            (values.next(), values.next(), values.next())
        else {
            return Err(MigrateVersionError::NotFinished);
        };
        let Some(info) = info
            .map(|info| BucketInfo::from_bytes(info.as_ref()))
            .transpose()?
        else {
            return Ok(None);
        };
        if BucketEncryption::from_row(settings.as_deref())?.is_encrypted() {
            return Ok(None);
        }
        let group = group.map(|group| GroupStorageRouting::from_bytes(group.as_ref()));
        let default = group.transpose()?.and_then(|group| group.default_target);
        routing = routing
            .with_bucket_rules(info.storage_routing)
            .with_compression(info.compression)
            .with_group_default(default);
        routing.imports.clear();
        Ok(Some(routing))
    }

    /// Reads the group's import rules in this transaction, so a rule created during the copy
    /// counts too.
    fn scan_rules(&mut self, after: Option<Key>) -> Turn {
        let Some(routing) = self.fresh.as_ref() else {
            return Err(MigrateVersionError::NotFinished);
        };
        let scan = IoCall::Page {
            space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: routing.group_id.to_bytes().to_vec().into(),
            after,
            limit: RULE_PAGE,
        };
        self.call(MigrateState::ScanRules, scan)
    }

    /// Publishes only while the fresh rules still choose the target of the move.
    fn handle_rules(&mut self, (values, next): (Vec<(Key, Value)>, Option<Key>)) -> Turn {
        let Some(routing) = self.fresh.as_mut() else {
            return Err(MigrateVersionError::NotFinished);
        };
        // Rules of other buckets count too: they may read the same linked folder.
        for (_, value) in values {
            {
                let rule = BackendImport::from_bytes(value.as_ref())?;
                routing.imports.push(rule)
            }
        }
        if next.is_some() {
            return self.scan_rules(next);
        }
        // Which other rules read the path is checked against their records at publication.
        let unshared = |plain: Option<PlainTarget>| {
            plain.map(|plain| PlainTarget {
                shared: Vec::new(),
                ..plain
            })
        };
        match place(routing, &self.version_key) {
            Ok((resolved, plain))
                if Some(&resolved) == self.resolved.as_ref()
                    && unshared(plain.clone()) == unshared(self.plain.clone()) =>
            {
                self.fence_target()
            }
            _ => self.skip(),
        }
    }

    /// Publishes only while the bucket still asks for this target and stores plain bytes; an
    /// encrypting bucket converts its versions through its own transition.
    fn handle_bucket(&mut self, values: Vec<Option<Value>>) -> Turn {
        if self.routing.is_some() {
            return match self.fresh_routing(values) {
                Ok(Some(routing)) => {
                    self.fresh = Some(routing);
                    self.scan_rules(None)
                }
                Ok(None) => self.skip(),
                Err(error) => Err(error),
            };
        }
        let Ok([value, settings]) = <[_; 2]>::try_from(values) else {
            return Err(MigrateVersionError::NotFinished);
        };
        let current = match value.map(|value| BucketInfo::from_bytes(value.as_ref())) {
            Some(Ok(info)) => Some(info.compression),
            Some(Err(error)) => return Err(error.into()),
            None => None,
        };
        let encrypting = {
            let settings = BucketEncryption::from_row(settings.as_deref())?;
            settings.is_encrypted()
        };
        if current != Some(self.target) || encrypting {
            return self.skip();
        }
        self.read_version()
    }

    /// A group storage disabled during the copy keeps the version where it is.
    fn fence_target(&mut self) -> Turn {
        let backend = self.resolved.as_ref().map(|resolved| &resolved.backend);
        let Some(BackendRef::Group(backend_id)) = backend else {
            return self.read_version();
        };
        let key = backend_key(*backend_id);
        self.read(MigrateState::FenceTarget, STORAGE_BACKEND_KEYSPACE, key)
    }

    fn handle_fence(&mut self, value: Option<Value>) -> Turn {
        let record = (value.as_deref())
            .map(GroupStorage::from_bytes)
            .transpose()
            .map_err(|error| BackendFenceError::Read(RecordReadError::Conversion(error)))?;
        match record {
            Some(record) if !record.disabled => self.read_version(),
            _ => self.skip(),
        }
    }

    fn read_version(&mut self) -> Turn {
        let key = self.version_key.to_bytes()?;
        self.read(MigrateState::CheckVersion, BLOB_VERSIONS_KEYSPACE, key)
    }

    fn handle_check(&mut self, value: Option<Value>) -> Turn {
        let current = match value.map(|value| BlobVersion::from_bytes(value.as_ref())) {
            Some(Ok(version)) => Some(version),
            Some(Err(error)) => return Err(error.into()),
            None => None,
        };
        if current != self.version {
            return self.skip();
        }
        let governed = (self.version.as_ref()).is_some_and(|v| !v.placement_policies.is_empty());
        let Some(old) = self.old.as_ref().filter(|_| governed) else {
            return self.read_target();
        };
        let key = ManagedCopyKey::new(self.version_key.clone(), old.backend.clone());
        let key = key.to_bytes()?;
        self.read(MigrateState::CheckCopy, MANAGED_COPY_KEYSPACE, key)
    }

    /// A governed version moves only while its registration names the old copy and may
    /// serve; the placement the gate admitted stays the same, since the backend does not change.
    /// A copy that cannot serve yet fails, so a retry pass moves it after revalidation.
    fn handle_copy(&mut self, value: Option<Value>) -> Turn {
        let value = value.as_ref().map(|value| value.as_ref());
        self.copy = Some(check_old_copy(value, self.old.as_ref())?);
        self.read_target()
    }

    fn read_target(&mut self) -> Turn {
        let key = self.target_key()?;
        let claim =
            (self.adopt_claim.clone()).map(|claim| (DELETE_CLAIM_KEYSPACE.to_string(), claim));
        let target = (BLOB_LOCATIONS_KEYSPACE.to_string(), key.to_bytes().into());
        let reads = std::iter::once(target)
            .chain(self.own_row())
            .chain(self.shared_rows())
            .chain(claim)
            .collect();
        self.call(MigrateState::ReadTarget, IoCall::Read(reads))
    }

    /// The rule's own record of the linked file's path, which must name this version's key.
    fn own_row(&self) -> Option<(String, Key)> {
        let target = self.plain.as_ref().filter(|target| target.listed)?;
        let key = object_key(target.import_id, &self.new.as_ref()?.backend_path);
        Some((IMPORTED_OBJECT_KEYSPACE.to_string(), key))
    }

    /// Whether a record of the rule names another key or version than this one.
    fn foreign_row(&self, row: Option<&Value>) -> Result<bool, MigrateVersionError> {
        let Some(row) = row else {
            return Ok(false);
        };
        let row = ImportedObject::from_bytes(row)?;
        let key = Some(self.version_key.key.as_str());
        Ok(row.key.as_deref() != key && row.version_id != self.version_key.version_id)
    }

    /// The records other rules that read the linked file's path, as this transaction reads
    /// the rules, would keep of it; any of them makes the file theirs.
    fn shared_rows(&self) -> Vec<(String, Key)> {
        let Some(new) = self.new.as_ref() else {
            return Vec::new();
        };
        let rows = self.readers().into_iter();
        let rows = rows.map(|import_id| object_key(import_id, &new.backend_path));
        rows.map(|key| (IMPORTED_OBJECT_KEYSPACE.to_string(), key))
            .collect()
    }

    /// An existing copy in the target class is adopted; otherwise the new copy
    /// becomes the row owner. The old copy goes to the reclaim queue either way.
    fn handle_target(&mut self, values: Vec<Option<Value>>) -> Turn {
        let own = self.own_row().is_some();
        let mut values = values.into_iter();
        let value = values.next().flatten();
        // The rule's own record of another key, as of another pattern, keeps the file.
        let mine = if own { values.next().flatten() } else { None };
        if self.foreign_row(mine.as_ref())? || values.any(|row| row.is_some()) {
            return Err(BlobError::PathTaken.into());
        }
        self.target_row = Some(value.map(|value| value.to_vec()));
        self.peers = self.readers();
        self.peer_intents()
    }

    /// The other rules that read the linked file's path, as this transaction reads the rules.
    fn readers(&self) -> Vec<Ulid> {
        let (Some(target), Some(new), Some(fresh)) = (&self.plain, &self.new, &self.fresh) else {
            return Vec::new();
        };
        fresh.readers(target.import_id, &target.backend, &new.backend_path)
    }

    /// An unfinished write of another rule to the path claims the file there too, so the move
    /// does not take it; each rule's intents of the path are read in the transaction.
    fn peer_intents(&mut self) -> Turn {
        let (Some(import_id), Some(new)) = (self.peers.pop(), self.new.as_ref()) else {
            let row = self.target_row.take().flatten();
            return self.publish(row);
        };
        let scan = IoCall::Page {
            space: IMPORT_INTENT_KEYSPACE.to_string(),
            prefix: intent_prefix(import_id, &new.backend_path),
            after: None,
            limit: 1,
        };
        self.call(MigrateState::ReadPeerIntents, scan)
    }

    fn handle_peers(&mut self, rows: Vec<(Key, Value)>) -> Turn {
        match rows.is_empty() {
            true => self.peer_intents(),
            false => Err(BlobError::PathTaken.into()),
        }
    }

    fn publish(&mut self, value: Option<Vec<u8>>) -> Turn {
        let writes = self.rows(value)?;
        self.call(MigrateState::WriteRows, IoCall::Write(writes))
    }

    fn rows(
        &mut self,
        existing: Option<Vec<u8>>,
    ) -> Result<Vec<(String, Key, Value)>, MigrateVersionError> {
        let (Some(old), Some(version)) = (self.old.as_ref(), self.version.as_ref()) else {
            return Err(MigrateVersionError::NotFinished);
        };
        let old_key = old.location_key()?;
        let mut writes = Vec::new();
        let published = match existing {
            Some(value) => {
                let existing = BackendLocation::from_bytes(&value)?;
                self.owns_row = (self.new.as_ref()).is_some_and(|new| existing.same_object(new));
                // Another backend's row is adopted only as the copy the probe found.
                let probed = (self.found.as_ref()).is_some_and(|found| existing.same_object(found));
                if self.crosses() && !self.owns_row && !probed {
                    return Err(MigrateVersionError::TargetMissing);
                }
                existing
            }
            None => {
                let new = self.new.clone().ok_or(MigrateVersionError::TargetMissing)?;
                writes.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    new.location_key()?.to_bytes().into(),
                    new.to_bytes()?.into(),
                ));
                self.owns_row = true;
                self.usage = Some(UsageCounterUpdate::for_stored(
                    StoredDelta::for_location(&new, true)
                        .ok_or(MigrateVersionError::NotFinished)?,
                ));
                new
            }
        };
        // A linked file added next to the copy the version keeps reclaims nothing.
        let unchanged = published.location_key()? == old_key;
        let mut migrated = version.clone();
        if let BlobVersionState::Materialized {
            backend, encoding, ..
        } = &mut migrated.state
        {
            (*backend, *encoding) = (published.backend.clone(), published.format.encoding());
        }
        // The rule was read in this transaction, so its importer skips the written file.
        if let (Some(target), Some(written)) = (self.plain.as_ref(), self.new.as_ref()) {
            let record = PlainRecord {
                target,
                written,
                version: self.backend_version.as_deref().filter(|_| self.owns_row),
                etag: self.backend_etag.as_deref(),
                version_id: self.version_key.version_id,
                key: &self.version_key.key,
            };
            writes.extend(record.writes(true)?);
        }
        if let Some(copy) = self.copy.as_ref() {
            let copy = ManagedCopyRecord {
                location: published,
                ..copy.clone()
            };
            writes.push((
                MANAGED_COPY_KEYSPACE.to_string(),
                copy.key().to_bytes()?.into(),
                copy.to_bytes()?.into(),
            ));
        }
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            self.version_key.to_bytes()?.into(),
            migrated.to_bytes()?.into(),
        ));
        // The old file goes only after this commit makes the version name the new one; without
        // a provider version only while its size and recorded ETag still match.
        let version = (self.source_version.clone())
            .or_else(|| self.source_etag.as_ref().map(|_| String::new()));
        if let (true, (Some(plain), Some(version))) =
            (self.renaming && !unchanged, (self.plain.as_ref(), version))
        {
            let work = BlobCleanupWork::ReleaseRenamed {
                location: old.clone(),
                import_id: plain.import_id,
                version_id: self.version_key.version_id,
                version,
            };
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                self.ids.mint(self.now_ms).to_bytes().to_vec().into(),
                work.to_bytes()?.into(),
            ));
        }
        if unchanged {
            return Ok(writes);
        }
        let candidate =
            ReclaimCandidateKey::new(old_key.backend, old_key.encoding, old_key.blake3_hash);
        let enqueued = ReclaimCandidate {
            enqueued_at: self.now,
        }
        .to_bytes()?;
        writes.push((
            BLOB_RECLAIM_KEYSPACE.to_string(),
            candidate.to_bytes().into(),
            enqueued.into(),
        ));
        Ok(writes)
    }

    fn handle_rows(&mut self, result: IoResult) -> Turn {
        result.done()?;
        if !self.written_intent.is_empty() {
            let deletes = self.intent_deletes();
            return self.call(MigrateState::RetireIntents, deletes);
        }
        self.update_usage()
    }

    fn update_usage(&mut self) -> Turn {
        let usage = self.usage.as_mut().filter(|usage| !usage.is_noop());
        let Some(usage) = usage else {
            return self.finish(MigrateOutcome::Migrated);
        };
        let call = usage.start_calls(self.ids.mint(self.now_ms)).into_call();
        self.call(MigrateState::UpdateUsage, call)
    }

    fn handle_usage(&mut self, result: IoResult) -> Turn {
        let usage = self.usage.as_mut();
        let usage = usage.ok_or(MigrateVersionError::NotFinished)?;
        match usage.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.finish(MigrateOutcome::Migrated),
        }
    }
}

impl Operation for MigrateVersionOperation {
    type Output = MigrateOutcome;
    type Error = MigrateVersionError;

    fn first(&mut self) -> Turn {
        if self.publishing {
            return self.read_bucket();
        }
        let key = self.version_key.to_bytes()?;
        self.read(MigrateState::ReadVersion, BLOB_VERSIONS_KEYSPACE, key)
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            MigrateState::Init => Err(MigrateVersionError::NotFinished),
            MigrateState::ReadVersion => self.handle_version(result.value()?),
            MigrateState::ReadLocation => self.handle_location(result.value()?),
            MigrateState::FindTarget => self.handle_found(result.value()?),
            MigrateState::ProbeTarget => self.handle_probe(result.event()?),
            MigrateState::CheckTaken => self.handle_taken(result.event()?),
            MigrateState::SettleCopy => self.handle_settled(result.event()?),
            MigrateState::ReadSource => self.handle_source(result.values()?),
            MigrateState::CheckLinked => self.handle_linked(result.value()?),
            MigrateState::ReadPlainOwner => self.handle_owner(result.value()?),
            MigrateState::StatPlain => self.handle_stat(result.event()?),
            MigrateState::ReadBlob => self.handle_read(result.event()?),
            MigrateState::WriteIntent => self.handle_intent(result),
            MigrateState::SyncIntent => self.handle_synced(result),
            MigrateState::WriteBlob => self.handle_written(result.event()?),
            MigrateState::DropIntent | MigrateState::SyncDroppedIntent => {
                self.handle_dropped(result)
            }
            MigrateState::Publish => self.published(result),
            MigrateState::ReadBucket => self.handle_bucket(result.values()?),
            MigrateState::ScanRules => self.handle_rules(result.page()?),
            MigrateState::FenceTarget => self.handle_fence(result.value()?),
            MigrateState::CheckVersion => self.handle_check(result.value()?),
            MigrateState::CheckCopy => self.handle_copy(result.value()?),
            MigrateState::ReadTarget => self.handle_target(result.values()?),
            MigrateState::ReadPeerIntents => self.handle_peers(result.page()?.0),
            MigrateState::WriteRows => self.handle_rows(result),
            MigrateState::RetireIntents => {
                result.done()?;
                self.update_usage()
            }
            MigrateState::UpdateUsage => self.handle_usage(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        match self.publishing {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
    }

    /// A run that ends early releases its copy, which no commit owns, to the cleanup queue. The
    /// publication leaves that to its parent, which holds the same copy.
    fn abort(&mut self) -> Vec<IoCall> {
        if self.publishing {
            return Vec::new();
        }
        self.owns_row = false;
        self.leave_calls()
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// Resolves a move's target as a new write of the key would; a plain path only on the
/// rule's write backend, as for an upload.
pub fn place(
    routing: &RoutingSnapshot,
    key: &VersionKey,
) -> Result<(ResolvedBackend, Option<PlainTarget>), RoutingError> {
    let resolved = resolve_backend(routing, &key.bucket, &key.key)?;
    let plain = routing
        .plain_target(&key.bucket, &key.key, key.version_id)
        .filter(|plain| plain.backend == resolved.backend);
    Ok((resolved, plain))
}

/// The row a version moves away from, or `None` when it already sits at `wanted`.
/// A linked target also takes a version that already sits at `wanted`, for its file.
fn source_key(
    version: &BlobVersion,
    wanted: impl Fn(&BackendRef) -> (BackendRef, EncodingClass),
    linked: bool,
) -> Option<BlobLocationKey> {
    let BlobVersionState::Materialized {
        backend, encoding, ..
    } = &version.state
    else {
        return None;
    };
    let wanted = wanted(backend);
    // The gate admitted a governed version for its backend only.
    let governed = !version.placement_policies.is_empty() && wanted.0 != *backend;
    let moves = (linked || wanted != (backend.clone(), *encoding)) && !governed;
    moves.then(|| version.location_key()).flatten()
}

/// Earlier write patterns of import rules, by import id.
pub type Earlier = BTreeMap<Ulid, Vec<String>>;

/// Whether a move gives the linked file at `location` the name `plain` of its version: the
/// file is the version's own copy, and an earlier pattern of the rule named it for the version.
pub fn renames(
    routing: &RoutingSnapshot,
    earlier: &Earlier,
    plain: &PlainTarget,
    location: &BackendLocation,
    key: &VersionKey,
) -> bool {
    let rule = routing
        .imports
        .iter()
        .find(|rule| rule.import_id == plain.import_id);
    let Some(rule) = rule.filter(|rule| BackendRef::Group(rule.backend_id) == plain.backend) else {
        return false;
    };
    let patterns = earlier.get(&rule.import_id).map_or(&[][..], Vec::as_slice);
    // A mapped key names its file by the storage side of the mapping.
    let named = routing.storage_key(rule, &key.key);
    let target = rule.rename_target(patterns, &location.backend_path, &named, key.version_id);
    location.is_external()
        && !location.staging
        && !location.partial
        && location.backend == plain.backend
        && matches!(location.format.layout, StoredLayout::Plain(_))
        && target.as_deref() == Some(plain.path.as_str())
}

/// Whether a provider file of `size` with `etag` holds the bytes of `old`: the ETag is the
/// MD5 of one upload, or of the one part a provider copy wrote.
fn same_bytes(old: &BackendLocation, size: u64, etag: Option<&str>) -> bool {
    let (Some(md5), Some(etag)) = (old.hashes.get(HASH_MD5), etag) else {
        return false;
    };
    let etag = etag.trim_matches('"');
    let copied = format!("{:x}-1", md5::compute(md5));
    size == old.blob_size && (etag == hex::encode(md5) || etag == copied)
}

/// Copies that stay: unfinished ones, and on a move files in a linked folder.
fn stays(location: &BackendLocation, moving: bool) -> bool {
    location.staging || location.partial || (moving && location.is_external())
}

/// Import rules one publication read returns at a time.
const RULE_PAGE: usize = 128;

#[cfg(test)]
mod tests;
