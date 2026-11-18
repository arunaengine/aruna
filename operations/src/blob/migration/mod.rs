//! Re-encodes one local version with its bucket's compression or moves it to the storage its rules
//! choose; publishes only if settings and version are unchanged, and reclaim keeps shared copies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod batch;
pub mod broken;
pub mod check;
pub mod drain;
pub mod misplaced;
pub mod queue;
pub mod rewrite;

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

use crate::blob::cleanup_rows::schedule_cleanup_effect;
use crate::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use crate::groups::storage_routing::routing_key;
use crate::s3::object::put::fenced::{Fenced, FencedWrite};
use crate::s3::object::put::{PlainRecord, intent_rows, owner_call, path_owned, same_name_path};
use crate::s3::usage::{StoredDelta, UsageCounterUpdate, UsageUpdateError};
use aruna_core::managed_copy::{ManagedCopyError, check_serveable};

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
        self.call(state, Effect::Blob(effect).into())
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
        let mut calls = vec![Effect::Blob(release).into()];
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
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(error) => return Err(error.into()),
        };
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
        let old = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(old) => old,
            Err(error) => return Err(error.into()),
        };
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
        match row.map(|row| ImportedObject::from_bytes(&row)).transpose() {
            Ok(row) => {
                self.source_etag = row
                    .filter(|row| row.version_id == id)
                    .and_then(|row| row.etag)
            }
            Err(error) => return Err(error.into()),
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
        match event {
            Event::Blob(BlobEvent::CopyStat { exists: true }) => self.after_found(),
            Event::Blob(BlobEvent::CopyStat { exists: false }) => {
                Err(MigrateVersionError::TargetMissing)
            }
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => self.unexpected(other),
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
        let row = match value
            .map(|value| ImportedObject::from_bytes(&value))
            .transpose()
        {
            Ok(row) => row,
            Err(error) => return Err(error.into()),
        };
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
        let (exists, version) = match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => (exists, version),
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            other => return self.unexpected(other),
        };
        let Some(plain) = self.plain.as_ref() else {
            return Err(MigrateVersionError::NotFinished);
        };
        let attempt = self.attempt();
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
        let (found, version) = match event {
            Event::Blob(BlobEvent::ExternalStat {
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
            Event::Blob(BlobEvent::Error(_)) => (None, None),
            other => return self.unexpected(other),
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
        self.written_intent = self.intent_keys(self.attempt());
        self.blob(
            MigrateState::SettleCopy,
            BlobEffect::SettleCopy {
                attempt: self.attempt(),
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
        match event {
            Event::Blob(BlobEvent::UploadAborted) => self.start_publication(),
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => self.unexpected(other),
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
            let copy = Effect::Blob(BlobEffect::CopyPlain {
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
        let blob = match event {
            Event::Blob(BlobEvent::ReadFinished { blob, .. }) => blob,
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            other => return self.unexpected(other),
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
            let write = Effect::Blob(BlobEffect::WritePlain {
                resolved,
                plain,
                created_by,
                blob,
            });
            let write = write.into();
            return self.intent_first(write, created_by);
        }
        let effect = Effect::Blob(BlobEffect::Write {
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
    fn attempt(&self) -> Ulid {
        match (self.renaming, self.old.as_ref()) {
            (true, Some(old)) => Ulid::from(u128::from(old.ulid) ^ 1),
            _ => self
                .plain_path
                .as_ref()
                .map_or_else(Ulid::generate, |path| path.ulid),
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
    fn write_intent(&self, target: &PlainTarget, created_by: aruna_core::UserId) -> WriteIntent {
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
    fn intent_first(&mut self, write: IoCall, created_by: aruna_core::UserId) -> Turn {
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
        let new = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            Event::Blob(BlobEvent::PlainWritten {
                location,
                version,
                etag,
            }) => {
                self.backend_version = version;
                self.backend_etag = etag;
                location
            }
            // A refused rename may meet its own earlier copy.
            Event::Blob(BlobEvent::Error(
                error @ (BlobError::PathTaken | BlobError::ConditionalUnsupported),
            )) if self.renaming => return self.stat_taken(error),
            // A refused write moved no byte; its intents go before the move fails with it.
            Event::Blob(BlobEvent::Error(
                error @ (BlobError::PathTaken | BlobError::ConditionalUnsupported),
            )) if !self.written_intent.is_empty() => {
                self.refusal = Some(error);
                return self.drop_intents();
            }
            // The adapter keeps a copy with an unclear outcome for reconciliation.
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            other => return self.unexpected(other),
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
            match BackendImport::from_bytes(value.as_ref()) {
                Ok(rule) => routing.imports.push(rule),
                Err(error) => return Err(error.into()),
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
        let encrypting = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings.is_encrypted(),
            Err(error) => return Err(error.into()),
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
        let copy = match check_serveable(value.as_ref().map(|value| value.as_ref())) {
            Ok(copy) => copy,
            Err(error) => return Err(error.into()),
        };
        if !(self.old.as_ref()).is_some_and(|old| copy.location.same_object(old)) {
            return Err(ManagedCopyError::Mismatched.into());
        }
        self.copy = Some(copy);
        self.read_target()
    }

    fn read_target(&mut self) -> Turn {
        let key = match self.target_key() {
            Ok(key) => key,
            Err(error) => return Err(error),
        };
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
                Ulid::generate().to_bytes().to_vec().into(),
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
        let call = usage.start_calls().into_call();
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
mod tests {
    use super::*;
    use std::time::Duration;

    use aruna_core::backoff::capped_backoff;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{
        COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE, MIGRATION_AHEAD_KEYSPACE,
        RENAME_MOVE_KEYSPACE,
    };
    use aruna_core::structs::storage::backend_import::RenameRequest;
    use aruna_core::structs::storage::format::{CompressionMigration, MigrationAhead};

    use super::drain::{
        Handover, MIGRATION_TIME, MIGRATION_WORK, RETRY_BASE, RETRY_MAX, end_aborted, granted,
        migrate_page, process_migrations, read_ahead, read_handover, read_progress, release_key,
        rename_access, rename_request, settle_move, store_handover,
    };

    #[test]
    fn retries_unavailable_policy() {
        use crate::auth::error::AuthorizeError;
        use crate::auth::error::PolicyEnforcementError;
        let unavailable = PolicyEnforcementError::Unavailable("down".to_string());
        assert!(granted::<()>(Err(AuthorizeError::Policy(unavailable))).is_err());
        let denied = PolicyEnforcementError::Denied {
            name: "p".to_string(),
            reason: "no".to_string(),
        };
        assert_eq!(granted::<()>(Err(AuthorizeError::Policy(denied))), Ok(None));
        assert_eq!(
            granted::<()>(Err(AuthorizeError::PermissionDenied)),
            Ok(None)
        );
        assert_eq!(granted(Ok(1)), Ok(Some(1)));
    }
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::storage::blob::{BackendRef, ManagedCopyState};
    use aruna_core::structs::storage::format::{FrameLayout, StoredFormat, StoredLayout};
    use std::collections::HashMap;
    use ulid::Ulid;

    const ZSTD: Compression = Compression::Zstd { level: 3 };

    fn location(framed: bool) -> BackendLocation {
        let mut format = StoredFormat::default();
        if framed {
            format.layout = StoredLayout::Frames(Box::new(FrameLayout {
                level: 3,
                frames: 1,
                stored_size: 20,
                index_hash: [0; 32],
            }));
        }
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "store".to_string(),
            backend_path: format!("b/k_{}", Ulid::generate()),
            ulid: Ulid::generate(),
            format,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 40,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![4u8; 32])]),
        }
    }

    fn read_result(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn blob_event(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    fn asked(turn: &Turn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    /// Turns `operation`, which waits for its publication, into that child and starts it.
    fn enter_publish(operation: &mut MigrateVersionOperation) -> Turn {
        assert_eq!(operation.state, MigrateState::Publish);
        *operation = operation.publish_child();
        operation.first()
    }

    /// The calls a run that skipped the version makes after its end.
    fn skipped_calls(turn: Turn) -> Vec<IoCall> {
        match turn {
            Ok(Next::Then {
                output: MigrateOutcome::Skipped,
                calls,
            }) => calls,
            Ok(Next::Done(MigrateOutcome::Skipped)) => Vec::new(),
            other => panic!("expected a skipped version, got {other:?}"),
        }
    }

    /// The written copy is released, and without an owning commit goes to the cleanup queue.
    fn leaves_copy(calls: &[IoCall], cleanup: bool) {
        let IoCall::Effect(release) = &calls[0] else {
            panic!("expected the release, got {calls:?}")
        };
        assert!(matches!(
            **release,
            Effect::Blob(BlobEffect::ReleaseReservation { .. })
        ));
        assert_eq!(calls.len(), 1 + usize::from(cleanup));
        if cleanup {
            assert_eq!(calls[1], IoCall::from(schedule_cleanup_effect()));
        }
    }

    /// The publication's read of the bucket and its encryption settings.
    fn bucket_read(compression: Compression, settings: Option<BucketEncryption>) -> IoResult {
        let settings = settings.map(|settings| settings.to_bytes().unwrap().into());
        IoResult::Values(vec![Some(bucket(compression).into()), settings])
    }

    fn bucket(compression: Compression) -> Vec<u8> {
        BucketInfo {
            group_id: Ulid::from_bytes([1; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression,
            distribution: None,
        }
        .to_bytes()
        .unwrap()
    }

    /// Drives the operation up to the transaction that publishes the new copy.
    fn written(operation: &mut MigrateVersionOperation, version: &BlobVersion) -> Turn {
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        operation.next(read_result(None)).unwrap();
        let blob = BackendStream::new(futures_util::stream::empty::<
            Result<bytes::Bytes, std::io::Error>,
        >());
        let turn = operation.next(blob_event(BlobEvent::ReadFinished {
            blob,
            stream_size: 40,
        }));
        let Effect::Blob(BlobEffect::Write { resolved, .. }) = asked(&turn) else {
            panic!("expected one write, got {turn:?}")
        };
        assert_eq!(resolved.compression, ZSTD);
        operation
            .next(blob_event(BlobEvent::WriteFinished {
                location: location(true),
            }))
            .unwrap();
        enter_publish(operation)
    }

    fn raw_version() -> BlobVersion {
        BlobVersion::materialized(
            [4u8; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        )
    }

    fn operation() -> MigrateVersionOperation {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        MigrateVersionOperation::new(key, ZSTD, SystemTime::UNIX_EPOCH)
    }

    #[test]
    fn skips_encoded_version() {
        let mut operation = operation();
        let mut version = raw_version();
        if let BlobVersionState::Materialized { encoding, .. } = &mut version.state {
            *encoding = EncodingClass::Zstd { level: 3 };
        }
        operation.first().unwrap();

        let turn = operation.next(read_result(Some(version.to_bytes().unwrap())));

        assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Skipped)));
    }

    #[test]
    fn publishes_new_copy() {
        let mut operation = operation();
        let version = raw_version();
        written(&mut operation, &version).unwrap();
        operation.next(bucket_read(ZSTD, None)).unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();

        let effects = operation.next(read_result(None));

        let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
            panic!("expected the row writes, got {effects:?}")
        };
        let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
        assert_eq!(
            spaces,
            [
                BLOB_LOCATIONS_KEYSPACE,
                BLOB_VERSIONS_KEYSPACE,
                BLOB_RECLAIM_KEYSPACE
            ]
        );
        let stored = BlobVersion::from_bytes(&writes[1].2).unwrap();
        let class = stored.location_key().unwrap().encoding;
        assert_eq!(class, EncodingClass::Zstd { level: 3 });
        let old = ReclaimCandidateKey::from_bytes(&writes[2].1).unwrap();
        assert_eq!(old.encoding, EncodingClass::Raw);

        // The commit makes the copy the row's owner, so it is only released afterwards.
        operation.usage = None;
        let Ok(Next::Then { output, calls }) = operation.update_usage() else {
            panic!("expected the result with its release")
        };
        assert_eq!(output, MigrateOutcome::Migrated);
        leaves_copy(&calls, false);
    }

    #[test]
    fn failed_publish_releases() {
        // A failed publication leaves the copy to the parent, which releases it once; the
        // copy no commit owns goes to the cleanup queue.
        let mut parent = operation();
        let version = raw_version();
        parent.first().unwrap();
        parent
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        parent
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        parent.state = MigrateState::Publish;
        parent.new = Some(location(true));
        let mut child = parent.publish_child();
        assert_eq!(child.transaction(), TransactionMode::Write);
        assert!(child.abort().is_empty());
        let error = MigrateVersionError::TargetMissing;
        let failed = IoResult::Child(ChildOutcome::new(Err::<MigrateOutcome, _>(error)));
        assert_eq!(parent.next(failed), Err(MigrateVersionError::TargetMissing));
        leaves_copy(&parent.abort(), true);
        assert!(parent.abort().is_empty());

        // After a published version the child already released the copy.
        let mut parent = operation();
        parent.state = MigrateState::Publish;
        parent.new = Some(location(true));
        let done = IoResult::Child(ChildOutcome::new(Ok::<_, MigrateVersionError>(
            MigrateOutcome::Migrated,
        )));
        let turn = parent.next(done);
        assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Migrated)));
        assert!(parent.abort().is_empty());
    }

    #[test]
    fn adopts_existing_copy() {
        // Another version already moved this content: nothing is read or written again.
        let mut operation = operation();
        let version = raw_version();
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();

        let turn = operation.next(read_result(Some(location(true).to_bytes().unwrap())));

        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        enter_publish(&mut operation).unwrap();
        operation.next(bucket_read(ZSTD, None)).unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        let effects = operation.next(read_result(Some(location(true).to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
            panic!("expected the row writes, got {effects:?}")
        };
        let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
        assert_eq!(spaces, [BLOB_VERSIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE]);
    }

    #[test]
    fn governed_moves_registration() {
        // The registration follows the copy in the same transaction as the version.
        let mut operation = operation();
        let version = raw_version()
            .with_policies(vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([3; 16]),
                digest: [4; 32],
            }])
            .unwrap();
        written(&mut operation, &version).unwrap();
        operation.next(bucket_read(ZSTD, None)).unwrap();
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the registration read, got {effects:?}")
        };
        assert_eq!(reads[0].0, MANAGED_COPY_KEYSPACE);
        let old = operation.old.clone().unwrap();
        let copy = ManagedCopyRecord::new(
            operation.version_key.clone(),
            iroh::SecretKey::from_bytes(&[9; 32]).public(),
            old,
            version.placement_policies.clone(),
            7,
            ManagedCopyState::Registered,
        )
        .unwrap();
        operation
            .next(read_result(Some(copy.to_bytes().unwrap())))
            .unwrap();

        let effects = operation.next(read_result(None));

        let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
            panic!("expected the row writes, got {effects:?}")
        };
        let (space, _, value) = &writes[1];
        assert_eq!(space, MANAGED_COPY_KEYSPACE);
        let moved = ManagedCopyRecord::from_bytes(value).unwrap();
        assert!(moved.location.same_object(operation.new.as_ref().unwrap()));
        assert_eq!(moved.policies, copy.policies);
    }

    #[test]
    fn unserveable_copy_fails() {
        // A copy waiting for revalidation is a failure, so a later retry pass moves it.
        let mut operation = operation();
        let version = raw_version()
            .with_policies(vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([3; 16]),
                digest: [4; 32],
            }])
            .unwrap();
        written(&mut operation, &version).unwrap();
        operation.next(bucket_read(ZSTD, None)).unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();

        let turn = operation.next(read_result(None));

        assert_eq!(
            turn,
            Err(MigrateVersionError::ManagedCopy(
                ManagedCopyError::Unregistered
            ))
        );
    }

    #[test]
    fn retry_waits_double() {
        let retry_delay = |retries| capped_backoff(RETRY_BASE, RETRY_MAX, retries, 0);
        assert_eq!(retry_delay(0), RETRY_BASE);
        assert_eq!(retry_delay(1), 2 * RETRY_BASE);
        assert_eq!(retry_delay(6), RETRY_MAX);
        assert_eq!(retry_delay(u32::MAX), RETRY_MAX);
    }

    #[test]
    fn stale_setting_discards() {
        // The setting changed again while the copy was written: nothing is published
        // and the written copy goes back to the cleanup queue.
        let mut operation = operation();
        written(&mut operation, &raw_version()).unwrap();

        let turn = operation.next(bucket_read(Compression::Off, None));

        leaves_copy(&skipped_calls(turn), true);
    }

    #[test]
    fn encrypting_bucket_skips() {
        // Encryption was enabled while this compression pass waited: a plain copy is never
        // published for the version, and the written copy goes back to the cleanup queue.
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let mut operation = operation();
        written(&mut operation, &raw_version()).unwrap();
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_bytes([3; 16])),
            key_generation: 1,
            ..Default::default()
        };

        let turn = operation.next(bucket_read(ZSTD, Some(settings)));

        leaves_copy(&skipped_calls(turn), true);
    }

    #[test]
    fn encrypting_adoption_skips() {
        // A matching plain copy appeared after encryption was enabled: the retry never adopts
        // it for this version.
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let mut operation = operation();
        let version = raw_version();
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(location(true).to_bytes().unwrap())))
            .unwrap();
        enter_publish(&mut operation).unwrap();
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([3; 16])),
            key_generation: 1,
            ..Default::default()
        };

        let turn = operation.next(bucket_read(ZSTD, Some(settings)));

        assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Skipped)));
    }

    #[tokio::test]
    async fn encryption_retires_compression() {
        // A compression pass waiting for its retry is finished once its bucket encrypts.
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::driver::DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let mut record = CompressionMigration::new(ZSTD, 1);
        record.failed = 2;
        record.retry_at_ms = Some(2);
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_bytes([3; 16])),
            key_generation: 1,
            ..Default::default()
        };
        for (key_space, value) in [
            (COMPRESSION_QUEUE_KEYSPACE, Vec::new()),
            (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
            (BUCKET_ENCRYPTION_KEYSPACE, settings.to_bytes().unwrap()),
            (RENAME_MOVE_KEYSPACE, b"1".to_vec()),
        ] {
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: b"b".to_vec().into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }

        // Resuming after "a" skips the transition queue, which this test does not seed.
        process_migrations(&context, Some(b"a".to_vec().into()))
            .await
            .unwrap();

        let stored = read_progress(&context, &b"b".to_vec().into())
            .await
            .unwrap()
            .unwrap();
        assert!(stored.finished_at_ms.is_some());
        assert_eq!(stored.retry_at_ms, None);
        // The finished move takes its rename request along.
        let marker = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: RENAME_MOVE_KEYSPACE.to_string(),
                key: b"b".to_vec().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            marker,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));
    }

    #[tokio::test]
    async fn checks_rename_caller() {
        // The rename's caller is read with its move, and each key is checked against them.
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::storage::blob::object_permission_path;
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let (realm, group) = (RealmId::from_bytes([1; 32]), Ulid::from_parts(1, 1));
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let secret = object_permission_path(realm, group, node, "b", "secret/**");
        let auth = crate::tests::access::seed_access(&context, (realm, group, node), &secret).await;
        let mut record = CompressionMigration::new(Compression::Off, 1);
        (record.moving, record.started_at_ms) = (true, 7);
        let grant = aruna_core::structs::storage::backend_import::TokenGrant {
            token_hash: "hash".to_string(),
            issuer: realm,
            issued_at_secs: 1,
            expires_at_secs: u64::MAX,
            via: None,
        };
        let request = RenameRequest {
            started_at_ms: 7,
            auth,
            node_id: node,
            grant,
            paused: false,
            aborted_at_ms: None,
        };
        let write = StorageEffect::Write {
            key_space: RENAME_MOVE_KEYSPACE.to_string(),
            key: b"b".to_vec().into(),
            value: request.to_bytes().unwrap().into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
        let found = rename_request(&context, "b", &record).await.unwrap();
        assert_eq!(found.as_ref(), Some(&request));
        // A later move of the bucket is not the one the caller asked to rename.
        record.started_at_ms = 8;
        assert_eq!(rename_request(&context, "b", &record).await.unwrap(), None);
        let routing = RoutingSnapshot::single(group);
        let access = rename_access(&context, &request, &routing, "b").await;
        let access = access.unwrap().unwrap();
        let write = aruna_core::structs::identity::auth::Permission::WRITE;
        assert!(access.allows(Some("in/a.csv"), &write));
        assert!(!access.allows(Some("secret/a.csv"), &write));
        // An expired token renames nothing more.
        let mut expired = request.clone();
        expired.grant.expires_at_secs = 1;
        let access = rename_access(&context, &expired, &routing, "b").await;
        assert!(access.unwrap().is_none());
    }

    #[tokio::test]
    async fn passes_skipped_versions() {
        // Versions with nothing to do do not use up a run; only the others count toward it, and
        // rows left go on at once while a failure pass waits for its retry.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let record = CompressionMigration::new(ZSTD, 1);
        let put = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        let progress = record.to_bytes().unwrap();
        let seeded = put(COMPRESSION_MIGRATION_KEYSPACE, b"b".to_vec(), progress);
        storage.send_storage_effect(seeded).await;
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let row =
            |index: usize| VersionKey::new("b", format!("k{index:03}"), Ulid::from_parts(1, 1));
        for index in 0..200 + MIGRATION_WORK + 44 {
            // Deleted versions are skipped; the later rows cannot be read and fail.
            let value = match index < 200 {
                true => deleted.to_bytes().unwrap(),
                false => b"unreadable".to_vec(),
            };
            let write = put(
                BLOB_VERSIONS_KEYSPACE,
                row(index).to_bytes().unwrap(),
                value,
            );
            storage.send_storage_effect(write).await;
        }

        let after = migrate_page(&context, "b", record, Duration::MAX).await;
        let after = after.unwrap();

        assert_eq!(after, Some(Duration::ZERO));
        let key: Key = b"b".to_vec().into();
        let stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert_eq!(
            (stored.skipped, stored.failed),
            (200, MIGRATION_WORK as u64)
        );
        let last = row(200 + MIGRATION_WORK - 1).to_bytes().unwrap();
        assert_eq!(stored.cursor, Some(last));
        let after = migrate_page(&context, "b", stored, Duration::MAX).await;
        let after = after.unwrap();
        assert_eq!(after, Some(capped_backoff(RETRY_BASE, RETRY_MAX, 0, 0)));
        let stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert_eq!(stored.failed, MIGRATION_WORK as u64 + 44);
        assert!(stored.retry_at_ms.is_some());
    }

    #[tokio::test]
    async fn stores_progress_often() {
        // A run that used up its time stores its progress after the version it started, and
        // the next run goes on at once after it.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let record = CompressionMigration::new(ZSTD, 1);
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let row = |index: u64| VersionKey::new("b", format!("k{index}"), Ulid::from_parts(1, 1));
        let rows = [0, 1, 2].map(|index| (row(index).to_bytes().unwrap(), deleted.to_bytes()));
        let progress = [(b"b".to_vec(), record.to_bytes().unwrap())];
        let writes = rows.into_iter().map(|(key, value)| (key, value.unwrap()));
        for (key_space, (key, value)) in std::iter::repeat_n(BLOB_VERSIONS_KEYSPACE, 3)
            .zip(writes)
            .chain([(COMPRESSION_MIGRATION_KEYSPACE, progress[0].clone())])
        {
            let write = StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        let key: Key = b"b".to_vec().into();
        let mut stored = record;
        for index in 0..3 {
            let after = migrate_page(&context, "b", stored, Duration::ZERO).await;
            assert_eq!(after.unwrap(), (index < 2).then_some(Duration::ZERO));
            stored = read_progress(&context, &key).await.unwrap().unwrap();
            assert_eq!(stored.skipped, index + 1);
            assert_eq!(stored.cursor, Some(row(index).to_bytes().unwrap()));
        }
    }

    #[tokio::test]
    async fn counts_rows_once() {
        // Rows a run finished after its cursor count once: the next run of the same pass
        // passes over them, and a row of another pass or cursor is not trusted.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let row = |index: u64| VersionKey::new("b", format!("k{index}"), Ulid::from_parts(1, 1));
        let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        for index in 0..3 {
            let version = row(index).to_bytes().unwrap();
            let effect = write(BLOB_VERSIONS_KEYSPACE, version, deleted.to_bytes().unwrap());
            storage.send_storage_effect(effect).await;
        }
        let key: Key = b"b".to_vec().into();
        for (started_at_ms, skipped) in [(1, 2), (2, 3)] {
            let record = CompressionMigration::new(ZSTD, 1);
            let progress = record.to_bytes().unwrap();
            let effect = write(COMPRESSION_MIGRATION_KEYSPACE, b"b".to_vec(), progress);
            storage.send_storage_effect(effect).await;
            let ahead = MigrationAhead {
                started_at_ms,
                rows: vec![row(1).to_bytes().unwrap()],
                ..MigrationAhead::default()
            };
            let ahead = ahead.to_bytes().unwrap();
            storage
                .send_storage_effect(write(MIGRATION_AHEAD_KEYSPACE, b"b".to_vec(), ahead))
                .await;
            let after = migrate_page(&context, "b", record, Duration::MAX).await;
            assert_eq!(after.unwrap(), None);
            let stored = read_progress(&context, &key).await.unwrap().unwrap();
            assert_eq!(stored.skipped, skipped);
            assert_eq!(stored.cursor, Some(row(2).to_bytes().unwrap()));
            assert_eq!(read_ahead(&context, "b").await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn survives_lost_progress() {
        // Rows ahead are stored with the progress: a run whose progress is lost leaves them as
        // they were, so the run that repeats it counts each row once.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let row = |index: u64| VersionKey::new("b", format!("k{index}"), Ulid::from_parts(1, 1));
        let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        for index in 0..3 {
            let version = row(index).to_bytes().unwrap();
            let effect = write(BLOB_VERSIONS_KEYSPACE, version, deleted.to_bytes().unwrap());
            storage.send_storage_effect(effect).await;
        }
        // An earlier run counted k0 and k2 while k1 still ran.
        let mut record = CompressionMigration::new(ZSTD, 1);
        (record.cursor, record.skipped) = (Some(row(0).to_bytes().unwrap()), 2);
        let ahead = MigrationAhead {
            started_at_ms: 1,
            cursor: record.cursor.clone(),
            rows: vec![row(2).to_bytes().unwrap()],
            ..MigrationAhead::default()
        };
        let ahead = ahead.to_bytes().unwrap();
        storage
            .send_storage_effect(write(MIGRATION_AHEAD_KEYSPACE, b"b".to_vec(), ahead))
            .await;
        // Without its progress row the next run stores nothing, as if it stopped before storing.
        let key: Key = b"b".to_vec().into();
        let lost = migrate_page(&context, "b", record.clone(), Duration::MAX).await;
        assert_eq!(lost.unwrap(), None);
        assert_eq!(read_progress(&context, &key).await.unwrap(), None);
        let progress = record.to_bytes().unwrap();
        storage
            .send_storage_effect(write(
                COMPRESSION_MIGRATION_KEYSPACE,
                b"b".to_vec(),
                progress,
            ))
            .await;
        let after = migrate_page(&context, "b", record, Duration::MAX).await;
        assert_eq!(after.unwrap(), None);
        let stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert_eq!(stored.skipped, 3);
        assert_eq!(read_ahead(&context, "b").await.unwrap(), None);
    }

    #[tokio::test]
    async fn resumes_without_gap() {
        // The version a run left unfinished is deleted during the pause; the two versions of one
        // key it finished after it now open the next run and still count once.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let row = |at: u64| VersionKey::new("b", "k1", Ulid::from_parts(at, 1));
        let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        for at in [1, 2] {
            let version = row(at).to_bytes().unwrap();
            let effect = write(BLOB_VERSIONS_KEYSPACE, version, deleted.to_bytes().unwrap());
            storage.send_storage_effect(effect).await;
        }
        let mut record = CompressionMigration::new(ZSTD, 1);
        record.skipped = 2;
        let ahead = MigrationAhead {
            started_at_ms: 1,
            rows: [1, 2].map(|at| row(at).to_bytes().unwrap()).to_vec(),
            ..MigrationAhead::default()
        };
        let writes = [
            (MIGRATION_AHEAD_KEYSPACE, ahead.to_bytes().unwrap()),
            (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
        ];
        for (key_space, value) in writes {
            storage
                .send_storage_effect(write(key_space, b"b".to_vec(), value))
                .await;
        }
        let after = migrate_page(&context, "b", record, Duration::MAX).await;
        assert_eq!(after.unwrap(), None);
        let key: Key = b"b".to_vec().into();
        let stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert_eq!(stored.skipped, 2);
        assert_eq!(read_ahead(&context, "b").await.unwrap(), None);
    }

    #[tokio::test]
    async fn ends_aborted_rename() {
        // An aborted rename ends its move with its counts and keeps the request as the abort.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let mut record = CompressionMigration::new(Compression::Off, 7);
        (record.moving, record.migrated) = (true, 3);
        let info = BucketInfo {
            group_id: Ulid::from_parts(1, 1),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let request = RenameRequest {
            started_at_ms: 7,
            auth: aruna_core::structs::identity::auth::AuthContext {
                user_id: Default::default(),
                realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
                path_restrictions: None,
                session: None,
            },
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            grant: aruna_core::structs::storage::backend_import::TokenGrant {
                token_hash: "hash".to_string(),
                issuer: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
                issued_at_secs: 1,
                expires_at_secs: u64::MAX,
                via: None,
            },
            paused: true,
            aborted_at_ms: Some(9),
        };
        for (key_space, value) in [
            (S3_BUCKET_KEYSPACE, info.to_bytes().unwrap()),
            (COMPRESSION_QUEUE_KEYSPACE, Vec::new()),
            (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
            (RENAME_MOVE_KEYSPACE, request.to_bytes().unwrap()),
        ] {
            let write = StorageEffect::Write {
                key_space: key_space.to_string(),
                key: b"b".to_vec().into(),
                value: value.into(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }

        let after = migrate_page(&context, "b", record, MIGRATION_TIME).await;
        assert_eq!(after.unwrap(), None);

        let key: Key = b"b".to_vec().into();
        let stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert!(stored.finished_at_ms.is_some());
        assert_eq!(stored.migrated, 3);
        let read = |key_space: &str| StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.clone(),
            txn_id: None,
        };
        let kept = storage
            .send_storage_effect(read(RENAME_MOVE_KEYSPACE))
            .await;
        assert!(matches!(
            kept,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ));
        let queued = storage
            .send_storage_effect(read(COMPRESSION_QUEUE_KEYSPACE))
            .await;
        assert!(matches!(
            queued,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));
    }

    #[test]
    fn keys_copy_releases() {
        // A replayed settlement writes the release of one copy version at one key only.
        let row = intent_key(
            Ulid::from_parts(2, 2),
            "raw/a.csv~X",
            Ulid::from_parts(9, 9),
        );
        assert_eq!(release_key(&row, "v1"), release_key(&row, "v1"));
        assert_ne!(release_key(&row, "v1"), release_key(&row, "v2"));
        assert_ne!(release_key(&row, "v1"), row.to_vec());
    }

    #[tokio::test]
    async fn settles_before_abort() {
        // An aborted rename hands each version with a move intent to the cleanup queue, which
        // runs it again until its intents are settled.
        use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::testing::test_context(storage.clone());
        let group = Ulid::from_parts(1, 1);
        let rule = BackendImport {
            import_id: Ulid::from_parts(2, 2),
            group_id: group,
            backend_id: Ulid::from_parts(3, 3),
            backend_prefix: "raw/".to_string(),
            bucket: "b".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: DEFAULT_PATTERN.to_string(),
        };
        let info = BucketInfo {
            group_id: group,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let id = Ulid::from_parts(9, 9);
        let intent = WriteIntent {
            key: "a.csv".to_string(),
            version_id: id,
            backend: BackendRef::Group(rule.backend_id),
            created_by: Default::default(),
            written: Some([1; 32]),
            mover: Some(7),
        };
        let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        let intent_row = intent_key(rule.import_id, "raw/a.csv~X", id).to_vec();
        let version = VersionKey::new("b", "a.csv", id).to_bytes().unwrap();
        let backend = aruna_core::structs::storage::group_backend::GroupStorage {
            backend_id: rule.backend_id,
            group_id: group,
            name: "tenant".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        };
        for effect in [
            write(
                aruna_core::keyspaces::BACKEND_INDEX_KEYSPACE,
                crate::groups::backends::index_key(group, rule.backend_id).to_vec(),
                backend.to_bytes().unwrap(),
            ),
            write(S3_BUCKET_KEYSPACE, b"b".to_vec(), info.to_bytes().unwrap()),
            write(
                BACKEND_IMPORT_KEYSPACE,
                rule.key().to_vec(),
                rule.to_bytes().unwrap(),
            ),
            write(
                IMPORT_INTENT_KEYSPACE,
                intent_row.clone(),
                intent.to_bytes().unwrap(),
            ),
            // A version row that cannot be read makes its run fail.
            write(BLOB_VERSIONS_KEYSPACE, version.clone(), vec![0xff; 3]),
        ] {
            storage.send_storage_effect(effect).await;
        }
        let mut record = CompressionMigration::new(Compression::Off, 7);
        record.moving = true;
        let key: Key = b"b".to_vec().into();
        let stored = write(
            COMPRESSION_MIGRATION_KEYSPACE,
            b"b".to_vec(),
            record.to_bytes().unwrap(),
        );
        storage.send_storage_effect(stored).await;

        // An upload's intent with its hash is not the move's and stays where it is.
        let upload = WriteIntent {
            mover: None,
            ..intent.clone()
        };
        let upload_row = intent_key(rule.import_id, "raw/b.csv", id).to_vec();
        let stored = write(
            IMPORT_INTENT_KEYSPACE,
            upload_row.clone(),
            upload.to_bytes().unwrap(),
        );
        storage.send_storage_effect(stored).await;
        // A hand-over stopped in a rule removed since goes on with the next rule.
        let place = Handover {
            started_ms: 7,
            import_id: Some(Ulid::from_parts(1, 1)),
            after: Some(b"zz".to_vec()),
        };
        store_handover(&context, "b", Some(&place)).await.unwrap();
        // The abort hands the version over to the cleanup queue and ends the move.
        assert_eq!(end_aborted(&context, "b", record).await.unwrap(), None);
        assert!(read_handover(&context, "b").await.unwrap().is_none());
        let read = StorageEffect::Read {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: upload_row.into(),
            txn_id: None,
        };
        assert!(matches!(
            storage.send_storage_effect(read).await,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));
        let ended = read_progress(&context, &key).await.unwrap().unwrap();
        assert!(ended.finished_at_ms.is_some());
        let read = StorageEffect::Read {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: intent_row.clone().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(work), ..
        }) = storage.send_storage_effect(read).await
        else {
            panic!("expected the handed over move")
        };
        let BlobCleanupWork::SettleMove {
            version: moved,
            intents,
        } = BlobCleanupWork::from_bytes(&work).unwrap()
        else {
            panic!("expected a move to settle")
        };
        assert_eq!(moved, VersionKey::new("b", "a.csv", id));
        // A version that cannot run keeps its intents for a later pass.
        assert!(!settle_move(&context, &moved, &intents).await.unwrap());
        // Once the version is gone, its copy must be checked before its intents go; without
        // a blob storage to ask, the move stays unsettled and keeps them.
        let delete = StorageEffect::Delete {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version.into(),
            txn_id: None,
        };
        storage.send_storage_effect(delete).await;
        assert!(!settle_move(&context, &moved, &intents).await.unwrap());
        let read = StorageEffect::Read {
            key_space: IMPORT_INTENT_KEYSPACE.to_string(),
            key: intents[0].0.clone().into(),
            txn_id: None,
        };
        assert!(matches!(
            storage.send_storage_effect(read).await,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ));
    }

    #[test]
    fn rejects_wrong_event() {
        let mut operation = operation();
        operation.state = MigrateState::ProbeTarget;

        let turn = operation.next(blob_event(BlobEvent::DeleteFinished));

        assert!(matches!(
            turn,
            Err(MigrateVersionError::InvalidStateEvent { .. })
        ));
    }

    mod moving {
        use super::*;
        use aruna_core::keyspaces::IMPORTED_OBJECT_KEYSPACE;
        use aruna_core::structs::storage::backend_import::{DEFAULT_PATTERN, tagged_path};
        use aruna_core::structs::storage::routing::{
            BackendCatalog, GroupRoutingInputs, RoutingTarget,
        };
        use std::collections::BTreeSet;

        const GROUP: Ulid = Ulid::from_parts(1, 1);
        const STORAGE: Ulid = Ulid::from_parts(5, 5);
        const IMPORT: Ulid = Ulid::from_parts(4, 4);

        fn storage() -> BackendRef {
            BackendRef::Group(STORAGE)
        }

        fn rule() -> BackendImport {
            BackendImport {
                import_id: IMPORT,
                group_id: GROUP,
                backend_id: STORAGE,
                backend_prefix: "raw/".to_string(),
                bucket: "b".to_string(),
                key_prefix: "in/".to_string(),
                schedule: None,
                created_by: Default::default(),
                created_at: SystemTime::UNIX_EPOCH,
                updated_at: SystemTime::UNIX_EPOCH,
                last_job: None,
                last_started_ms: None,
                write_backend_id: None,
                write_prefix: None,
                write_pattern: DEFAULT_PATTERN.to_string(),
            }
        }

        /// A group whose default rule names its own storage, with or without a linked rule.
        fn routing(imports: Vec<BackendImport>) -> RoutingSnapshot {
            let catalog = BackendCatalog::new("default").with_backend("default", None);
            RoutingSnapshot::new(GROUP, catalog).with_group_inputs(GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(storage())),
                backend_ids: BTreeSet::from([STORAGE]),
                imports,
                ..Default::default()
            })
        }

        fn operation(key: &str, imports: Vec<BackendImport>) -> MigrateVersionOperation {
            let key = VersionKey::new("b", key, Ulid::from_bytes([2; 16]));
            MigrateVersionOperation::new(key, Compression::Off, SystemTime::UNIX_EPOCH)
                .with_routing(routing(imports))
        }

        fn on_storage(mut location: BackendLocation) -> BackendLocation {
            location.backend = storage();
            location
        }

        fn empty_blob() -> IoResult {
            let blob = BackendStream::new(futures_util::stream::empty::<
                Result<bytes::Bytes, std::io::Error>,
            >());
            blob_event(BlobEvent::ReadFinished {
                blob,
                stream_size: 40,
            })
        }

        /// Answers the publication's rule reads: bucket, encryption, group default, then the
        /// group's import rules; returns what follows them.
        fn rules_read(
            operation: &mut MigrateVersionOperation,
            default: Option<RoutingTarget>,
            rule: impl IntoIterator<Item = BackendImport>,
        ) -> Turn {
            let group = GroupStorageRouting {
                group_id: GROUP,
                default_target: default,
                updated_at: SystemTime::UNIX_EPOCH,
                updated_by: Default::default(),
            };
            let values = vec![
                Some(bucket(Compression::Off).into()),
                None,
                Some(group.to_bytes().unwrap().into()),
            ];
            let turn = operation.next(IoResult::Values(values));
            let Ok(Next::Call(IoCall::Page { space, .. })) = &turn else {
                panic!("expected the import rule scan, got {turn:?}")
            };
            assert_eq!(space, BACKEND_IMPORT_KEYSPACE);
            let rows = (rule.into_iter())
                .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
                .collect();
            operation.next(IoResult::Page { rows, next: None })
        }

        fn storage_record(disabled: bool) -> IoResult {
            let record = aruna_core::structs::storage::group_backend::GroupStorage {
                backend_id: STORAGE,
                group_id: GROUP,
                name: "tenant".to_string(),
                kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
                public_config: HashMap::new(),
                created_at: SystemTime::UNIX_EPOCH,
                updated_at: SystemTime::UNIX_EPOCH,
                created_by: Default::default(),
                disabled,
                cleanup: Default::default(),
            };
            read_result(Some(record.to_bytes().unwrap()))
        }

        /// Answers the rule reads and the target storage fence, up to the version read.
        fn published(operation: &mut MigrateVersionOperation, rule: Option<BackendImport>) {
            let default = Some(RoutingTarget::Backend(storage()));
            let turn = rules_read(operation, default, rule);
            let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
                panic!("expected the storage fence, got {turn:?}")
            };
            assert_eq!(reads[0].0, aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE);
            operation.next(storage_record(false)).unwrap();
        }

        /// Answers the file's write with the location the adapter reports.
        fn plain_written(plain: &PlainPath) -> IoResult {
            let mut written = on_storage(location(false));
            (written.storage_bucket, written.backend_path) = (String::new(), plain.path.clone());
            written.ulid = plain.ulid;
            written.format.layout = StoredLayout::Plain(plain.ulid);
            blob_event(BlobEvent::PlainWritten {
                location: written,
                version: None,
                etag: None,
            })
        }

        /// Answers the stored and synced intent before a linked write, and returns the write.
        fn past_intent(operation: &mut MigrateVersionOperation, turn: Turn) -> Turn {
            let Ok(Next::Call(IoCall::Child(_))) = &turn else {
                return turn;
            };
            assert_eq!(operation.state, MigrateState::WriteIntent);
            // The source bytes are known, so the intent proves the copy before it is written.
            let target = operation.plain.clone().unwrap();
            let intent = operation.write_intent(&target, Default::default());
            let source = operation.old.as_ref().and_then(|old| old.get_blake3());
            assert_eq!(intent.written.as_ref().map(|hash| &hash[..]), source);
            let stored = Ok::<_, StorageError>(Fenced::Written);
            let turn = operation.next(IoResult::Child(ChildOutcome::new(stored)));
            assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
            operation.next(IoResult::Done)
        }

        /// Drives a move of `version` from Aruna storage up to its publication reads; a linked
        /// target finds its path taken when `taken`.
        fn copied(
            operation: &mut MigrateVersionOperation,
            version: &BlobVersion,
            taken: bool,
        ) -> Turn {
            operation.first().unwrap();
            operation
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();
            let turn = operation.next(read_result(Some(location(false).to_bytes().unwrap())));
            let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
                panic!("expected the target row read, got {turn:?}")
            };
            let key = BlobLocationKey::from_bytes(&reads[0].1).unwrap();
            assert_eq!(key.backend, storage());
            let mut turn = operation.next(read_result(None));
            if let Ok(Next::Call(IoCall::Read(reads))) = &turn {
                assert_eq!(reads[0].0, IMPORTED_OBJECT_KEYSPACE);
                turn = operation.next(read_result(None));
            }
            if let Effect::Blob(BlobEffect::StatPlain { .. }) = asked(&turn) {
                turn = operation.next(blob_event(BlobEvent::PlainStat {
                    exists: taken,
                    version: None,
                }));
            }
            assert!(matches!(
                asked(&turn),
                Effect::Blob(BlobEffect::Read { .. })
            ));
            let turn = operation.next(empty_blob());
            let turn = past_intent(operation, turn);
            let new = match asked(&turn) {
                Effect::Blob(BlobEffect::Write { resolved, .. }) => {
                    assert_eq!(resolved.backend, storage());
                    blob_event(BlobEvent::WriteFinished {
                        location: on_storage(location(false)),
                    })
                }
                Effect::Blob(BlobEffect::WritePlain { plain, .. }) => plain_written(plain),
                other => panic!("expected a write, got {other:?}"),
            };
            operation.next(new).unwrap();
            enter_publish(operation)
        }

        /// Answers the publication's read of the target row, with the rows the rules keep of the
        /// path absent.
        fn answer_target(
            operation: &mut MigrateVersionOperation,
            turn: Turn,
            value: Option<Vec<u8>>,
        ) -> Turn {
            let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
                panic!("expected the target read, got {turn:?}")
            };
            let mut values = vec![None; reads.len()];
            values[0] = value.map(Into::into);
            operation.next(IoResult::Values(values))
        }

        fn batch(turn: &Turn) -> &Vec<(String, Key, Value)> {
            let Ok(Next::Call(IoCall::Write(writes))) = turn else {
                panic!("expected the row writes, got {turn:?}")
            };
            writes
        }

        #[test]
        fn moves_to_storage() {
            // The version names the copy on the group's storage; the old copy is reclaimed.
            let mut operation = operation("k", Vec::new());
            let version = raw_version();
            copied(&mut operation, &version, false).unwrap();
            published(&mut operation, None);
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects = answer_target(&mut operation, effects, None);

            let writes = batch(&effects);
            let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
            let expected = [
                BLOB_LOCATIONS_KEYSPACE,
                BLOB_VERSIONS_KEYSPACE,
                BLOB_RECLAIM_KEYSPACE,
            ];
            assert_eq!(spaces, expected);
            let moved = BlobVersion::from_bytes(&writes[1].2).unwrap();
            assert_eq!(moved.location_key().unwrap().backend, storage());
            let old = ReclaimCandidateKey::from_bytes(&writes[2].1).unwrap();
            assert_eq!(old.backend, BackendRef::node_default());
        }

        #[test]
        fn skips_placed_version() {
            let mut operation = operation("k", Vec::new());
            let mut version = raw_version();
            if let BlobVersionState::Materialized { backend, .. } = &mut version.state {
                *backend = storage();
            }
            operation.first().unwrap();

            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));

            assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
        }

        #[test]
        fn discards_changed_rule() {
            // The default rule went back to Aruna storage during the copy.
            let mut operation = operation("k", Vec::new());
            copied(&mut operation, &raw_version(), false).unwrap();

            let turn = rules_read(&mut operation, None, None);

            // The unpublished copy goes to the cleanup queue.
            leaves_copy(&skipped_calls(turn), true);
        }

        #[test]
        fn disabled_target_discards() {
            // A storage disabled during the copy never takes the version, so nothing is reclaimed.
            let mut operation = operation("k", Vec::new());
            copied(&mut operation, &raw_version(), false).unwrap();
            rules_read(
                &mut operation,
                Some(RoutingTarget::Backend(storage())),
                None,
            )
            .unwrap();

            let turn = operation.next(storage_record(true));

            leaves_copy(&skipped_calls(turn), true);
        }

        #[test]
        fn skips_missing_target() {
            // A row on the target whose bytes are gone is never adopted.
            let mut operation = operation("k", Vec::new());
            operation.first().unwrap();
            operation
                .next(read_result(Some(raw_version().to_bytes().unwrap())))
                .unwrap();
            operation
                .next(read_result(Some(location(false).to_bytes().unwrap())))
                .unwrap();
            let target = on_storage(location(false));

            let effects = operation.next(read_result(Some(target.to_bytes().unwrap())));
            let Effect::Blob(BlobEffect::StatCopy { location }) = asked(&effects) else {
                panic!("expected the target probe, got {effects:?}")
            };
            assert!(location.same_object(&target));
            let turn = operation.next(blob_event(BlobEvent::CopyStat { exists: false }));

            assert_eq!(turn, Err(MigrateVersionError::TargetMissing));
        }

        #[test]
        fn probed_target_revalidated() {
            // The adopted row must still be the probed copy when the version moves to it.
            let mut operation = operation("k", Vec::new());
            let version = raw_version();
            operation.first().unwrap();
            operation
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();
            operation
                .next(read_result(Some(location(false).to_bytes().unwrap())))
                .unwrap();
            operation
                .next(read_result(Some(
                    on_storage(location(false)).to_bytes().unwrap(),
                )))
                .unwrap();
            let turn = operation.next(blob_event(BlobEvent::CopyStat { exists: true }));
            assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
            enter_publish(&mut operation).unwrap();
            published(&mut operation, None);
            operation
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();

            let replaced = on_storage(location(false));
            let turn = operation.next(read_result(Some(replaced.to_bytes().unwrap())));

            assert_eq!(turn, Err(MigrateVersionError::TargetMissing));
        }

        #[test]
        fn writes_linked_file() {
            // The file lands where an upload goes and its importer skips it later.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            let version = raw_version();
            let effects = copied(&mut operation, &version, false);
            let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                panic!("expected the publication reads, got {effects:?}")
            };
            assert_eq!(reads.len(), 3);
            let plain = operation.plain_path.clone().unwrap();
            assert_eq!(
                plain.path,
                format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]))
            );
            // Only a free path is taken, so an outside file is never replaced.
            assert!(plain.if_not_exists);
            published(&mut operation, Some(rule()));
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects = answer_target(&mut operation, effects, None);

            let writes = batch(&effects);
            let (_, key, _) = (writes.iter())
                .find(|(space, ..)| space == IMPORTED_OBJECT_KEYSPACE)
                .expect("the import row");
            assert_eq!(key, &object_key(IMPORT, &plain.path));
            let (_, _, moved) = (writes.iter())
                .find(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
                .unwrap();
            let moved = BlobVersion::from_bytes(moved).unwrap();
            assert_eq!(moved.location_key().unwrap().backend, storage());
        }

        #[test]
        fn records_move_intents() {
            // The move stores its identity at both paths it may reach and retires it on publish.
            let mut moved = operation("in/x/one.csv", vec![rule()]);
            let version = raw_version();
            copied(&mut moved, &version, false).unwrap();
            let plain = moved.plain_path.clone().unwrap();
            let id = Ulid::from_bytes([2; 16]);
            let tagged = tagged_path(&plain.path, plain.ulid);
            let expected = [&plain.path, &tagged].map(|path| intent_key(IMPORT, path, id));
            assert_eq!(moved.written_intent, expected.to_vec());
            assert_eq!(moved.intent_keys(plain.ulid), expected.to_vec());
            published(&mut moved, Some(rule()));
            let effects = moved.next(read_result(Some(version.to_bytes().unwrap())));
            answer_target(&mut moved, effects, None).unwrap();
            let turn = moved.next(IoResult::Done);
            let Ok(Next::Call(IoCall::Delete(deletes))) = &turn else {
                panic!("expected the intent retirement, got {turn:?}")
            };
            let retired: Vec<Key> = deletes.iter().map(|(_, key)| key.clone()).collect();
            assert_eq!(retired, expected.to_vec());

            // A refused write moved no byte, so its intents go before the move fails.
            let mut refused = operation("in/x/one.csv", vec![rule()]);
            refused.first().unwrap();
            refused
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();
            refused
                .next(read_result(Some(location(false).to_bytes().unwrap())))
                .unwrap();
            refused.next(read_result(None)).unwrap();
            refused.next(read_result(None)).unwrap();
            refused
                .next(blob_event(BlobEvent::PlainStat {
                    exists: false,
                    version: None,
                }))
                .unwrap();
            let effects = refused.next(empty_blob());
            past_intent(&mut refused, effects).unwrap();
            let turn = refused.next(blob_event(BlobEvent::Error(BlobError::PathTaken)));
            assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
            assert_eq!(refused.next(IoResult::Done), Ok(Next::Call(IoCall::Sync)));
            let turn = refused.next(IoResult::Done);
            assert!(refused.written_intent.is_empty());
            assert_eq!(turn, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
        }

        #[test]
        fn drops_orphan_intents() {
            // The rule went before the intents were stored, so none are and the move stops.
            let mut moved = operation("in/x/one.csv", vec![rule()]);
            let version = raw_version();
            moved.first().unwrap();
            moved
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();
            moved
                .next(read_result(Some(location(false).to_bytes().unwrap())))
                .unwrap();
            moved.next(read_result(None)).unwrap();
            moved.next(read_result(None)).unwrap();
            moved
                .next(blob_event(BlobEvent::PlainStat {
                    exists: false,
                    version: None,
                }))
                .unwrap();
            moved.next(empty_blob()).unwrap();
            let gone = Ok::<_, StorageError>(Fenced::RuleGone);
            let turn = moved.next(IoResult::Child(ChildOutcome::new(gone)));
            assert!(moved.written_intent.is_empty());
            assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Skipped)));
        }

        #[test]
        fn keeps_shared_target() {
            // Another rule of the folder recorded the new file, so the move does not take it.
            let other = BackendImport {
                import_id: Ulid::from_parts(9, 9),
                key_prefix: "other/".to_string(),
                ..rule()
            };
            let version = raw_version();
            let at_target = || {
                let mut moved = operation("in/x/one.csv", vec![rule(), other.clone()]);
                copied(&mut moved, &version, false).unwrap();
                let default = Some(RoutingTarget::Backend(storage()));
                rules_read(&mut moved, default, [rule(), other.clone()]).unwrap();
                moved.next(storage_record(false)).unwrap();
                let effects = moved.next(read_result(Some(version.to_bytes().unwrap())));
                let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                    panic!("expected the target and shared reads, got {effects:?}")
                };
                let path = moved.new.as_ref().unwrap().backend_path.clone();
                // The rule's own record of the path comes first, then the other rule's.
                assert_eq!(reads[1].1, object_key(IMPORT, &path));
                assert_eq!(reads[2].1, object_key(other.import_id, &path));
                (moved, path)
            };
            let row = |other: Option<Value>| IoResult::Values(vec![None, None, other]);
            let taken = || Err(MigrateVersionError::Blob(BlobError::PathTaken));
            let (mut moved, _) = at_target();
            assert_eq!(moved.next(row(Some(vec![1].into()))), taken());
            // Nor while the other rule has an unfinished write of the path.
            let (mut moved, path) = at_target();
            let turn = moved.next(row(None));
            let Ok(Next::Call(IoCall::Page { prefix, .. })) = &turn else {
                panic!("expected the peer intents, got {turn:?}")
            };
            assert_eq!(*prefix, intent_prefix(other.import_id, &path));
            let rows = vec![(Key::from(vec![1]), Value::from(vec![1]))];
            assert_eq!(moved.next(IoResult::Page { rows, next: None }), taken());
        }

        #[test]
        fn keeps_owned_file() {
            // The rule recorded the new name for another key, as an earlier pattern's write of
            // equal bytes; the move does not take that file over.
            let mut moved = operation("in/x/one.csv", vec![rule()]);
            let version = raw_version();
            copied(&mut moved, &version, false).unwrap();
            published(&mut moved, Some(rule()));
            let effects = moved.next(read_result(Some(version.to_bytes().unwrap())));
            let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                panic!("expected the target and own record reads, got {effects:?}")
            };
            assert_eq!(reads.len(), 2);
            let other =
                ImportedObject::written(Ulid::from_parts(8, 8), "in/y/one.csv".into(), 40, None);
            let values = vec![None, Some(other.to_bytes().unwrap().into())];
            let turn = moved.next(IoResult::Values(values));
            assert_eq!(turn, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
        }

        #[test]
        fn never_overwrites_path() {
            // A file an earlier attempt or a user left at the path is never overwritten.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            copied(&mut operation, &raw_version(), true).unwrap();

            let plain = operation.plain_path.clone().unwrap();
            let path = format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]));
            assert_eq!(plain.path, tagged_path(&path, plain.ulid));
            assert_ne!(plain.ulid, Ulid::from_bytes([2; 16]));
            assert!(plain.if_not_exists);
        }

        #[test]
        fn new_rule_discards() {
            // A linked rule created during an ordinary move is read in the publication.
            let mut operation = operation("in/x/one.csv", Vec::new());
            copied(&mut operation, &raw_version(), false).unwrap();
            let default = Some(RoutingTarget::Backend(storage()));

            let effects = rules_read(&mut operation, default, Some(rule()));

            leaves_copy(&skipped_calls(effects), true);
        }

        #[test]
        fn removed_rule_discards() {
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            copied(&mut operation, &raw_version(), false).unwrap();
            let default = Some(RoutingTarget::Backend(storage()));

            let effects = rules_read(&mut operation, default, None);

            leaves_copy(&skipped_calls(effects), true);
        }

        /// A version whose copy is in Aruna's layout on the rule's storage.
        fn in_place() -> (BlobVersion, BackendLocation) {
            let mut version = raw_version();
            if let BlobVersionState::Materialized { backend, .. } = &mut version.state {
                *backend = storage();
            }
            (version, on_storage(location(false)))
        }

        #[test]
        fn links_existing_copy() {
            // A copy already on the rule's storage still gets its file, as an upload does,
            // and the version keeps the copy it names.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            let (version, old) = in_place();
            operation.first().unwrap();
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects = answer_target(&mut operation, effects, Some(old.to_bytes().unwrap()));
            let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                panic!("expected the import row read, got {effects:?}")
            };
            assert_eq!(reads[0].0, IMPORTED_OBJECT_KEYSPACE);
            operation.next(read_result(None)).unwrap();
            operation
                .next(read_result(Some(old.to_bytes().unwrap())))
                .unwrap();
            let effects = operation.next(read_result(None));
            assert!(matches!(
                asked(&effects),
                Effect::Blob(BlobEffect::StatPlain { .. })
            ));
            operation
                .next(blob_event(BlobEvent::PlainStat {
                    exists: false,
                    version: None,
                }))
                .unwrap();
            let effects = operation.next(empty_blob());
            let effects = past_intent(&mut operation, effects);
            let Effect::Blob(BlobEffect::WritePlain { plain, .. }) = asked(&effects) else {
                panic!("expected the file write, got {effects:?}")
            };
            let written = plain_written(plain);
            operation.next(written).unwrap();
            enter_publish(&mut operation).unwrap();
            published(&mut operation, Some(rule()));
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects = answer_target(&mut operation, effects, Some(old.to_bytes().unwrap()));

            let spaces: Vec<&str> = batch(&effects).iter().map(|(s, ..)| s.as_str()).collect();
            assert_eq!(spaces, [IMPORTED_OBJECT_KEYSPACE, BLOB_VERSIONS_KEYSPACE]);
        }

        #[test]
        fn linked_copy_done() {
            // Once the version's file is recorded, a later move leaves it alone.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            let (version, old) = in_place();
            operation.first().unwrap();
            operation
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();
            operation
                .next(read_result(Some(old.to_bytes().unwrap())))
                .unwrap();
            let row =
                ImportedObject::written(Ulid::from_bytes([2; 16]), "in/x/one.csv".into(), 40, None);

            let effects = operation.next(read_result(Some(row.to_bytes().unwrap())));

            assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
        }

        #[test]
        fn keeps_linked_file() {
            // A file found in a linked folder stays even when the rules point elsewhere.
            let mut operation = operation("k", Vec::new());
            operation.routing = Some(routing(Vec::new()).with_group_default(None));
            let mut version = raw_version();
            if let BlobVersionState::Materialized { backend, .. } = &mut version.state {
                *backend = storage();
            }
            let mut external = on_storage(location(false));
            external.storage_bucket = String::new();
            operation.first().unwrap();
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects =
                answer_target(&mut operation, effects, Some(external.to_bytes().unwrap()));

            assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
        }

        /// A version whose own linked file has the name of the earlier prefix pattern.
        fn prefixed() -> (BlobVersion, BackendLocation) {
            let id = Ulid::from_bytes([2; 16]);
            let mut old = on_storage(location(false));
            old.storage_bucket = String::new();
            old.backend_path = format!("raw/x/{id}_one.csv");
            old.format.layout = StoredLayout::Plain(old.ulid);
            let mut version = raw_version();
            if let BlobVersionState::Materialized {
                backend, encoding, ..
            } = &mut version.state
            {
                (*backend, *encoding) = (storage(), old.format.encoding());
            }
            (version, old)
        }

        /// A move that knows the rule's earlier prefix pattern, up to the read of the rule's
        /// record at the new name.
        fn named(
            version: &BlobVersion,
            old: &BackendLocation,
            (source, etag): (Option<&[u8]>, Option<&str>),
        ) -> MigrateVersionOperation {
            let earlier = Earlier::from([(IMPORT, vec!["{dir}{id}_{name}".to_string()])]);
            let mut operation = operation("in/x/one.csv", vec![rule()]).with_earlier(earlier);
            operation.first().unwrap();
            operation
                .next(read_result(Some(version.to_bytes().unwrap())))
                .unwrap();
            let effects = operation.next(read_result(Some(old.to_bytes().unwrap())));
            // The old file's backend version binds a later delete to the renamed bytes, and the
            // rule's record of the file names the ETag an unversioned copy needs.
            let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                panic!("expected the reads of the old file, got {effects:?}")
            };
            let id = operation.version_key.version_id;
            let etag = etag.map(str::to_string);
            let row = ImportedObject::written(id, "in/x/one.csv".to_string(), 4, etag);
            assert_eq!(reads.len(), 2);
            let values = vec![
                source.map(|source| source.to_vec().into()),
                Some(row.to_bytes().unwrap().into()),
            ];
            operation.next(IoResult::Values(values)).unwrap();
            operation.next(read_result(None)).unwrap();
            operation
        }

        /// A move that knows the rule's earlier prefix pattern, up to its copy to the new name.
        fn renaming(
            version: &BlobVersion,
            old: &BackendLocation,
            (source, recorded): (Option<&[u8]>, Option<&str>),
        ) -> (MigrateVersionOperation, PlainPath) {
            let mut operation = named(version, old, (source, recorded));
            // Nothing records the new name for another key, and no provider is asked first.
            let effects = operation.next(read_result(None));
            let effects = past_intent(&mut operation, effects);
            let Effect::Blob(BlobEffect::CopyPlain {
                plain,
                source: copied,
                etag,
                ..
            }) = asked(&effects)
            else {
                panic!("expected the copy to the new name, got {effects:?}")
            };
            assert_eq!(**copied, *old);
            assert_eq!(etag.as_deref(), recorded);
            // A rename takes its exact new name or nothing.
            assert!(plain.if_not_exists && plain.exact);
            let name = format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]));
            assert_eq!(plain.path, name);
            (operation, plain.clone())
        }

        #[test]
        fn keeps_recorded_name() {
            // A new name the rule records for another key is taken before any provider request.
            let (version, old) = prefixed();
            let mut operation = named(&version, &old, (Some(b"v1"), Some("e1")));
            let id = Ulid::from_bytes([9; 16]);
            let other = ImportedObject::written(id, "in/x/other.csv".to_string(), 4, None);
            let effects = operation.next(read_result(Some(other.to_bytes().unwrap())));
            assert_eq!(
                effects,
                Err(MigrateVersionError::Blob(BlobError::PathTaken))
            );
        }

        #[test]
        fn refuses_unconditional_rename() {
            // A storage that cannot create only a free name refuses the rename; no intent stays.
            let (version, old) = prefixed();
            let (mut operation, plain) = renaming(&version, &old, (Some(b"v1"), Some("e1")));
            let refused = || BlobError::ConditionalUnsupported;
            let effects = operation.next(blob_event(BlobEvent::Error(refused())));
            // Such a storage may hold an earlier attempt at the tagged name.
            let Effect::Blob(BlobEffect::StatExternal { path, .. }) = asked(&effects) else {
                panic!("expected the stat of the tagged name, got {effects:?}")
            };
            assert_eq!(path, &tagged_path(&plain.path, plain.ulid));
            let turn = operation.next(blob_event(BlobEvent::ExternalStat {
                location: None,
                version: None,
                etag: None,
            }));
            assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
            operation.next(IoResult::Done).unwrap();
            let turn = operation.next(IoResult::Done);
            assert!(operation.written_intent.is_empty());
            assert_eq!(turn, Err(MigrateVersionError::Blob(refused())));
        }

        /// Drives a rename to its publication rows.
        fn renamed(
            source: (Option<&[u8]>, Option<&str>),
        ) -> (Vec<(String, Key, Value)>, PlainPath, BackendLocation) {
            let (version, old) = prefixed();
            let (mut operation, plain) = renaming(&version, &old, source);
            operation.next(plain_written(&plain)).unwrap();
            enter_publish(&mut operation).unwrap();
            published(&mut operation, Some(rule()));
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects = answer_target(&mut operation, effects, None);
            (batch(&effects).clone(), plain, old)
        }

        #[test]
        fn renames_linked_file() {
            // The version names the new file; the old one goes only through the cleanup queue.
            let (writes, plain, old) = renamed((Some(b"v1"), Some("e1")));
            let find = |space: &str| writes.iter().find(|(found, ..)| found == space).unwrap();
            let moved = BlobVersion::from_bytes(&find(BLOB_VERSIONS_KEYSPACE).2).unwrap();
            let encoding = EncodingClass::Plain {
                location: plain.ulid,
            };
            assert_eq!(moved.location_key().unwrap().encoding, encoding);
            assert_eq!(
                find(IMPORTED_OBJECT_KEYSPACE).1,
                object_key(IMPORT, &plain.path)
            );
            let work = BlobCleanupWork::from_bytes(&find(BLOB_CLEANUP_KEYSPACE).2).unwrap();
            let BlobCleanupWork::ReleaseRenamed {
                location,
                import_id,
                version_id,
                version,
            } = work
            else {
                panic!("expected the release of the old file")
            };
            assert_eq!(
                (location, import_id, version),
                (old, IMPORT, "v1".to_string())
            );
            assert_eq!(version_id, Ulid::from_bytes([2; 16]));
            // Without a backend version the old file goes only while its recorded ETag holds.
            let release = |writes: &[(String, Key, Value)]| {
                let found = writes
                    .iter()
                    .find(|(space, ..)| space == BLOB_CLEANUP_KEYSPACE);
                found.map(|(.., work)| BlobCleanupWork::from_bytes(work).unwrap())
            };
            for source in [None, Some(&b"null"[..])] {
                let (writes, ..) = renamed((source, Some("e1")));
                let Some(BlobCleanupWork::ReleaseRenamed { version, .. }) = release(&writes) else {
                    panic!("expected the release of the old file")
                };
                assert_eq!(version, "");
            }
            // Without a recorded ETag nothing proves which bytes are there, so the file stays.
            let (writes, ..) = renamed((None, None));
            assert!(release(&writes).is_none());
            assert!(
                writes
                    .iter()
                    .any(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
            );
        }

        /// The old file of `prefixed` with a known MD5, and the provider file `size` and `etag`
        /// found at the name a refused rename wanted.
        fn taken_name(
            (old_size, size): (Option<u64>, u64),
            etag: &str,
        ) -> (MigrateVersionOperation, BlobVersion, BackendLocation, Turn) {
            let (version, mut old) = prefixed();
            old.hashes.insert(HASH_MD5.to_string(), vec![7; 16]);
            if let Some(old_size) = old_size {
                old.blob_size = old_size;
                let md5 = md5::compute(b"").0.to_vec();
                old.hashes.insert(HASH_MD5.to_string(), md5);
            }
            let (mut operation, plain) = renaming(&version, &old, (Some(b"v1"), Some("e1")));
            let effects = operation.next(blob_event(BlobEvent::Error(BlobError::PathTaken)));
            let Effect::Blob(BlobEffect::StatExternal { path, .. }) = asked(&effects) else {
                panic!("expected the stat of the taken name, got {effects:?}")
            };
            assert_eq!(path, &plain.path);
            let mut found = on_storage(location(false));
            (found.storage_bucket, found.backend_path) = (String::new(), path.clone());
            (found.blob_size, found.hashes) = (size, HashMap::new());
            let effects = operation.next(blob_event(BlobEvent::ExternalStat {
                location: Some(found),
                version: None,
                etag: Some(format!("\"{etag}\"")),
            }));
            (operation, version, old, effects)
        }

        #[test]
        fn resumes_taken_name() {
            // An interrupted rename left its copy at the new name; only checksums decide.
            let size = prefixed().1.blob_size;
            let (single, copied) = (
                hex::encode([7; 16]),
                format!("{:x}-1", md5::compute([7; 16])),
            );
            // A provider copy may keep the old file's recorded ETag, and an empty old file's
            // copy is proved like any other; a reservation never shows here as a file.
            let empty = hex::encode(md5::compute(b"").0);
            for (size, etag, adopted) in [
                ((None, size), single.as_str(), true),
                ((None, size), copied.as_str(), true),
                ((None, size), "e1", true),
                ((None, size), "0123", false),
                ((None, size + 1), copied.as_str(), false),
                ((Some(0), 0), empty.as_str(), true),
            ] {
                let (mut operation, _, old, effects) = taken_name(size, etag);
                if adopted {
                    // The upload the earlier attempt recorded is settled before the adoption.
                    let attempt = Ulid::from(u128::from(old.ulid) ^ 1);
                    let settle = BlobEffect::SettleCopy {
                        attempt,
                        upload: None,
                    };
                    assert_eq!(asked(&effects), &Effect::Blob(settle));
                    let turn = operation.next(blob_event(BlobEvent::UploadAborted));
                    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
                    // The intents the earlier attempt stored go with the publication.
                    assert_eq!(operation.written_intent, operation.intent_keys(attempt));
                    assert_eq!(operation.written_intent.len(), 2);
                    // The proved copy names the old file's hashes.
                    assert_eq!(operation.new.as_ref().unwrap().hashes, old.hashes);
                    continue;
                }
                // Another file at the new name keeps it; the intents go before the move fails.
                assert!(matches!(effects, Ok(Next::Call(IoCall::Delete(_)))));
                operation.next(IoResult::Done).unwrap();
                let error = MigrateVersionError::Blob(BlobError::PathTaken);
                assert_eq!(operation.next(IoResult::Done), Err(error));
            }
        }

        #[test]
        fn skips_deleted_version() {
            // A copy at the new name whose backend version a release deleted is not adopted.
            let (version, mut old) = prefixed();
            old.hashes.insert(HASH_MD5.to_string(), vec![7; 16]);
            let (mut operation, plain) = renaming(&version, &old, (Some(b"v1"), Some("e1")));
            operation
                .next(blob_event(BlobEvent::Error(BlobError::PathTaken)))
                .unwrap();
            let mut found = on_storage(location(false));
            (found.storage_bucket, found.backend_path) = (String::new(), plain.path.clone());
            found.blob_size = old.blob_size;
            operation
                .next(blob_event(BlobEvent::ExternalStat {
                    location: Some(found),
                    version: Some("p9".to_string()),
                    etag: Some(hex::encode([7; 16])),
                }))
                .unwrap();
            operation
                .next(blob_event(BlobEvent::UploadAborted))
                .unwrap();
            enter_publish(&mut operation).unwrap();
            published(&mut operation, Some(rule()));
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                panic!("expected the target reads, got {effects:?}")
            };
            assert_eq!(reads.last().unwrap().0, DELETE_CLAIM_KEYSPACE);
            let mut values = vec![None; reads.len()];
            *values.last_mut().unwrap() = Some(Vec::new().into());
            let turn = operation.next(IoResult::Values(values));
            assert_eq!(turn, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
        }

        #[test]
        fn keeps_unknown_name() {
            // Without the earlier pattern the name tells no version, so the file stays.
            let (version, old) = prefixed();
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            operation.first().unwrap();
            let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
            let effects = answer_target(&mut operation, effects, Some(old.to_bytes().unwrap()));
            assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
        }
    }
}
