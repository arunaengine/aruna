//! Re-encodes one local version with its bucket's compression or moves it to the storage its rules
//! choose; publishes only if settings and version are unchanged, and reclaim keeps shared copies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod check;
pub mod misplaced;
pub mod queue;
pub mod rewrite;

use crate::blob::cleanup::schedule_cleanup_effect;
use crate::blob::managed_copy::{ManagedCopyError, check_serveable, read_effect};
use crate::blob::records::{blob_location_read, read_version_effect};
use crate::groups::backends::{BackendFenceError, check_fence, fence_backend};
use crate::groups::storage_routing::routing_key;
use crate::node::usage_stats::{StoredDelta, UsageCounterUpdate, UsageUpdateError};
use crate::s3::bucket::key::rows::settings_read;
use crate::s3::object::put::{PlainRecord, same_name_path};
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, MANAGED_COPY_KEYSPACE, RENAME_MOVE_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, PlainPath, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, ManagedCopyKey, ManagedCopyRecord, ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::{
    Compression, CompressionMigration, EncodingClass, StoredLayout,
};
use aruna_core::structs::storage::routing::{
    GroupStorageRouting, PlainTarget, RoutingError, RoutingSnapshot, resolve_backend,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use ulid::Ulid;

/// Steps of one version migration; errors name the step they stopped in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrateState {
    Init,
    ReadVersion,
    ReadLocation,
    FindTarget,
    ProbeTarget,
    HashTarget,
    ReadSource,
    CheckLinked,
    StatPlain,
    ReadBlob,
    WriteBlob,
    StartTransaction,
    ReadBucket,
    ScanRules,
    FenceTarget,
    CheckVersion,
    CheckCopy,
    ReadTarget,
    WriteRows,
    UpdateUsage,
    Commit,
    Abort,
    Release,
    Finish,
    Error,
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
    #[error("unexpected event in state {state:?}: {received:?}")]
    InvalidStateEvent {
        state: MigrateState,
        received: Box<Event>,
    },
}

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
    /// The backend's id of the bytes a plain write stored.
    backend_version: Option<String>,
    backend_etag: Option<String>,
    /// A finished copy found at the target row; on another backend it is probed before use.
    found: Option<BackendLocation>,
    /// The routing the publication transaction read.
    fresh: Option<RoutingSnapshot>,
    state: MigrateState,
    txn_id: Option<TxnId>,
    version: Option<BlobVersion>,
    old: Option<BackendLocation>,
    new: Option<BackendLocation>,
    /// The registration of a governed version, moved to the new copy on commit.
    copy: Option<ManagedCopyRecord>,
    /// Set when the commit made the new copy the owner of its location row.
    owns_row: bool,
    usage: Option<UsageCounterUpdate>,
    output: Option<Result<MigrateOutcome, MigrateVersionError>>,
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
            backend_version: None,
            backend_etag: None,
            found: None,
            fresh: None,
            state: MigrateState::Init,
            txn_id: None,
            version: None,
            old: None,
            new: None,
            copy: None,
            owns_row: false,
            usage: None,
            output: None,
        }
    }

    /// Moves the version to where `routing` sends a new write of its key.
    pub fn with_routing(mut self, routing: RoutingSnapshot) -> Self {
        self.routing = Some(routing);
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

    fn unexpected(&mut self, received: Event) -> Effects {
        let error = MigrateVersionError::InvalidStateEvent {
            state: self.state,
            received: Box::new(received),
        };
        self.fail(error)
    }

    /// Ends with an error. A written copy that no commit owns is released and
    /// handed to the cleanup queue, whose reconcile step deletes it.
    fn fail(&mut self, error: MigrateVersionError) -> Effects {
        self.output = Some(Err(error));
        self.leave()
    }

    fn skip(&mut self) -> Effects {
        self.output = Some(Ok(MigrateOutcome::Skipped));
        self.leave()
    }

    fn leave(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = MigrateState::Abort;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.release()
    }

    fn release(&mut self) -> Effects {
        match self.new.as_ref() {
            Some(new) => {
                self.state = MigrateState::Release;
                smallvec![Effect::Blob(BlobEffect::ReleaseReservation {
                    id: new.ulid
                })]
            }
            None => self.finish(),
        }
    }

    fn finish(&mut self) -> Effects {
        self.state = match self.output {
            Some(Ok(_)) => MigrateState::Finish,
            _ => MigrateState::Error,
        };
        match (self.new.is_some(), self.owns_row) {
            (true, false) => smallvec![schedule_cleanup_effect()],
            _ => smallvec![],
        }
    }

    fn handle_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.skip();
        };
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        if let Some(routing) = self.routing.as_ref() {
            match place(routing, &self.version_key) {
                Ok((resolved, plain)) => (self.resolved, self.plain) = (Some(resolved), plain),
                Err(error) => return self.fail(error.into()),
            }
        }
        let linked = self.plain.is_some();
        let Some(key) = source_key(&version, |current| self.wanted(current), linked) else {
            return self.skip();
        };
        self.version = Some(version);
        self.state = MigrateState::ReadLocation;
        smallvec![blob_location_read(&key, None)]
    }

    fn handle_location(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.skip();
        };
        let old = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(old) => old,
            Err(error) => return self.fail(error.into()),
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
            self.state = MigrateState::ReadSource;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BACKEND_VERSION_KEYSPACE.to_string(),
                key: source,
                txn_id: None,
            })];
        }
        // A copy already on the rule's backend only needs its file in the linked folder.
        if let Some(plain) = self.plain.as_ref().filter(|_| in_place) {
            self.state = MigrateState::CheckLinked;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
                key: object_key(plain.import_id, &plain.path),
                txn_id: None,
            })];
        }
        self.find_target()
    }

    /// The version of the old file, fixed when it was written, names the bytes a release deletes.
    fn handle_source(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let version = value
            .map(|value| String::from_utf8(value.to_vec()))
            .transpose();
        match version {
            Ok(version) => self.source_version = version,
            Err(error) => return self.fail(ConversionError::from(error).into()),
        }
        self.find_target()
    }

    fn find_target(&mut self) -> Effects {
        match self.target_key() {
            Ok(key) => {
                self.state = MigrateState::FindTarget;
                smallvec![blob_location_read(&key, None)]
            }
            Err(error) => self.fail(error),
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
    fn handle_linked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let row = value.map(|value| ImportedObject::from_bytes(value.as_ref()));
        match row.transpose() {
            Ok(Some(row)) if row.version_id == self.version_key.version_id => self.skip(),
            Ok(_) => self.find_target(),
            Err(error) => self.fail(error.into()),
        }
    }

    /// A finished copy in the target encoding is adopted; on another backend only after a probe
    /// shows its bytes exist. A linked target writes its file first, as an upload does.
    fn handle_found(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let found = match value.map(|value| BackendLocation::from_bytes(value.as_ref())) {
            Some(Ok(found)) => Some(found).filter(|found| !found.staging && !found.partial),
            Some(Err(error)) => return self.fail(error.into()),
            None => None,
        };
        self.found = found.clone();
        match found {
            Some(location) if self.crosses() => {
                self.state = MigrateState::ProbeTarget;
                smallvec![Effect::Blob(BlobEffect::StatCopy { location })]
            }
            _ => self.after_found(),
        }
    }

    fn handle_probe(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::CopyStat { exists: true }) => self.after_found(),
            Event::Blob(BlobEvent::CopyStat { exists: false }) => {
                self.fail(MigrateVersionError::TargetMissing)
            }
            Event::Blob(BlobEvent::Error(error)) => self.fail(error.into()),
            other => self.unexpected(other),
        }
    }

    fn after_found(&mut self) -> Effects {
        if let Some(plain) = self.plain.as_ref() {
            self.state = MigrateState::StatPlain;
            return smallvec![Effect::Blob(BlobEffect::StatPlain {
                backend: plain.backend.clone(),
                path: plain.path.clone(),
            })];
        }
        if self.found.is_none() {
            return self.read_blob();
        }
        self.state = MigrateState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// The path takes the file as an upload would: free, versioned, or tagged. Each attempt
    /// names its own id, so a retry never overwrites a file an earlier attempt left there.
    fn handle_stat(&mut self, event: Event) -> Effects {
        let (exists, version) = match event {
            Event::Blob(BlobEvent::PlainStat { exists, version }) => (exists, version),
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            other => return self.unexpected(other),
        };
        let Some(plain) = self.plain.as_ref() else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        // A rename never tags or replaces a taken name; it adopts only an earlier copy of its own.
        if self.renaming && exists {
            let Some(old) = self.old.as_ref() else {
                return self.fail(MigrateVersionError::NotFinished);
            };
            let resolved = ResolvedBackend::new(old.backend.clone(), old.storage_class.clone());
            self.state = MigrateState::HashTarget;
            return smallvec![Effect::Blob(BlobEffect::HashExternal {
                resolved,
                path: plain.path.clone(),
                created_by: old.created_by,
            })];
        }
        let attempt = Ulid::generate();
        let versioned = plain.same_name && version.is_some();
        let (path, if_not_exists) = same_name_path(&plain.path, attempt, exists, versioned);
        self.plain_path = Some(PlainPath {
            path,
            ulid: attempt,
            if_not_exists,
        });
        self.read_blob()
    }

    /// The file at the new name is a copy an interrupted rename left, if its bytes match.
    fn handle_hashed(&mut self, event: Event) -> Effects {
        let (mut location, version) = match event {
            Event::Blob(BlobEvent::ExternalHashed { location, version }) => (location, version),
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            other => return self.unexpected(other),
        };
        let same = self.old.as_ref().is_some_and(|old| {
            old.get_blake3() == location.get_blake3() && old.blob_size == location.blob_size
        });
        if !same {
            return self.fail(BlobError::PathTaken.into());
        }
        location.format.layout = StoredLayout::Plain(location.ulid);
        self.backend_version = version;
        self.new = Some(location);
        self.state = MigrateState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn read_blob(&mut self) -> Effects {
        let Some(old) = self.old.clone() else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        self.state = MigrateState::ReadBlob;
        smallvec![Effect::Blob(BlobEffect::Read { location: old })]
    }

    fn handle_read(&mut self, event: Event) -> Effects {
        let blob = match event {
            Event::Blob(BlobEvent::ReadFinished { blob, .. }) => blob,
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            other => return self.unexpected(other),
        };
        let Some(old) = self.old.as_ref() else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        let resolved = self.resolved.clone().unwrap_or_else(|| {
            ResolvedBackend::new(old.backend.clone(), old.storage_class.clone())
                .with_compression(self.target)
        });
        if let Some(plain) = self.plain_path.clone() {
            self.state = MigrateState::WriteBlob;
            return smallvec![Effect::Blob(BlobEffect::WritePlain {
                resolved,
                plain,
                created_by: old.created_by,
                blob,
            })];
        }
        let effect = Effect::Blob(BlobEffect::Write {
            bucket: self.version_key.bucket.clone(),
            key: self.version_key.key.clone(),
            resolved,
            created_by: old.created_by,
            blob,
            size: None,
        });
        self.state = MigrateState::WriteBlob;
        smallvec![effect]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
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
            // The adapter keeps a copy with an unclear outcome for reconciliation.
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            other => return self.unexpected(other),
        };
        let same = self.old.as_ref().is_some_and(|old| {
            old.get_blake3() == new.get_blake3() && old.blob_size == new.blob_size
        });
        self.new = Some(new);
        if !same {
            return self.fail(MigrateVersionError::ContentMismatch);
        }
        self.state = MigrateState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event);
        };
        self.txn_id = Some(txn_id);
        self.state = MigrateState::ReadBucket;
        let Some(routing) = self.routing.as_ref() else {
            return smallvec![settings_read(&self.version_key.bucket, Some(txn_id))];
        };
        let bucket: Key = self.version_key.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket),
            (
                STORAGE_ROUTING_KEYSPACE.to_string(),
                routing_key(routing.group_id),
            ),
        ];
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
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
    fn scan_rules(&mut self, after: Option<Key>) -> Effects {
        let Some(routing) = self.fresh.as_ref() else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        self.state = MigrateState::ScanRules;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: Some(routing.group_id.to_bytes().to_vec().into()),
            start: after.map(IterStart::After),
            limit: RULE_PAGE,
            txn_id: self.txn_id,
        })]
    }

    /// Publishes only while the fresh rules still choose the target of the move.
    fn handle_rules(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected(event);
        };
        let Some(routing) = self.fresh.as_mut() else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        for (_, value) in values {
            match BackendImport::from_bytes(value.as_ref()) {
                Ok(rule) if rule.bucket == self.version_key.bucket => routing.imports.push(rule),
                Ok(_) => {}
                Err(error) => return self.fail(error.into()),
            }
        }
        if next_start_after.is_some() {
            return self.scan_rules(next_start_after);
        }
        match place(routing, &self.version_key) {
            Ok((resolved, plain))
                if Some(&resolved) == self.resolved.as_ref() && plain == self.plain =>
            {
                self.fence_target()
            }
            _ => self.skip(),
        }
    }

    /// Publishes only while the bucket still asks for this target and stores plain bytes; an
    /// encrypting bucket converts its versions through its own transition.
    fn handle_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event);
        };
        if self.routing.is_some() {
            let values = values.into_iter().map(|(_, value)| value);
            return match self.fresh_routing(values.collect()) {
                Ok(Some(routing)) => {
                    self.fresh = Some(routing);
                    self.scan_rules(None)
                }
                Ok(None) => self.skip(),
                Err(error) => self.fail(error),
            };
        }
        let Ok([(_, value), (_, settings)]) = <[_; 2]>::try_from(values) else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        let current = match value.map(|value| BucketInfo::from_bytes(value.as_ref())) {
            Some(Ok(info)) => Some(info.compression),
            Some(Err(error)) => return self.fail(error.into()),
            None => None,
        };
        let encrypting = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings.is_encrypted(),
            Err(error) => return self.fail(error.into()),
        };
        if current != Some(self.target) || encrypting {
            return self.skip();
        }
        self.read_version()
    }

    /// A group storage disabled during the copy keeps the version where it is.
    fn fence_target(&mut self) -> Effects {
        let backend = self.resolved.as_ref().map(|resolved| &resolved.backend);
        match backend.and_then(|backend| fence_backend(backend, self.txn_id)) {
            Some(effect) => {
                self.state = MigrateState::FenceTarget;
                smallvec![effect]
            }
            None => self.read_version(),
        }
    }

    fn handle_fence(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.read_version(),
            Err(BackendFenceError::Unavailable) => self.skip(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn read_version(&mut self) -> Effects {
        let effect = match read_version_effect(&self.version_key, self.txn_id) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.state = MigrateState::CheckVersion;
        smallvec![effect]
    }

    fn handle_check(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let current = match value.map(|value| BlobVersion::from_bytes(value.as_ref())) {
            Some(Ok(version)) => Some(version),
            Some(Err(error)) => return self.fail(error.into()),
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
        match read_effect(&key, self.txn_id) {
            Ok(effect) => {
                self.state = MigrateState::CheckCopy;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// A governed version moves only while its registration names the old copy and may
    /// serve; the placement the gate admitted stays the same, since the backend does not change.
    /// A copy that cannot serve yet fails, so a retry pass moves it after revalidation.
    fn handle_copy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let copy = match check_serveable(value.as_ref().map(|value| value.as_ref())) {
            Ok(copy) => copy,
            Err(error) => return self.fail(error.into()),
        };
        if !(self.old.as_ref()).is_some_and(|old| copy.location.same_object(old)) {
            return self.fail(ManagedCopyError::Mismatched.into());
        }
        self.copy = Some(copy);
        self.read_target()
    }

    fn read_target(&mut self) -> Effects {
        match self.target_key() {
            Ok(key) => {
                self.state = MigrateState::ReadTarget;
                smallvec![blob_location_read(&key, self.txn_id)]
            }
            Err(error) => self.fail(error),
        }
    }

    /// An existing copy in the target class is adopted; otherwise the new copy
    /// becomes the row owner. The old copy goes to the reclaim queue either way.
    fn handle_target(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        match self.rows(value.map(|value| value.to_vec())) {
            Ok(writes) => {
                self.state = MigrateState::WriteRows;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.fail(error),
        }
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
        // The old file goes only after this commit makes the version name the new one.
        let release = (self.plain.as_ref(), self.source_version.clone());
        if let (true, (Some(plain), Some(version))) = (self.renaming && !unchanged, release) {
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

    fn handle_rows(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected(event);
        };
        let usage = self.usage.as_mut().filter(|usage| !usage.is_noop());
        let (Some(txn_id), Some(usage)) = (self.txn_id, usage) else {
            return self.commit();
        };
        self.state = MigrateState::UpdateUsage;
        usage.start(txn_id)
    }

    fn handle_usage(&mut self, event: Event) -> Effects {
        let (Some(txn_id), Some(usage)) = (self.txn_id, self.usage.as_mut()) else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        match usage.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.commit(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MigrateVersionError::NotFinished);
        };
        self.state = MigrateState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.output = Some(Ok(MigrateOutcome::Migrated));
                self.release()
            }
            // Without a commit no row owns the new copy, and with an unknown
            // outcome the reconcile step reads the row that decides it.
            Event::Storage(StorageEvent::Error { error }) => {
                self.owns_row = false;
                self.fail(error.into())
            }
            other => self.unexpected(other),
        }
    }
}

impl Operation for MigrateVersionOperation {
    type Output = MigrateOutcome;
    type Error = MigrateVersionError;

    fn start(&mut self) -> Effects {
        match read_version_effect(&self.version_key, None) {
            Ok(effect) => {
                self.state = MigrateState::ReadVersion;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event
            && !matches!(self.state, MigrateState::Commit | MigrateState::Abort)
        {
            let error = error.clone();
            return self.fail(error.into());
        }
        match self.state {
            MigrateState::Init => self.start(),
            MigrateState::ReadVersion => self.handle_version(event),
            MigrateState::ReadLocation => self.handle_location(event),
            MigrateState::FindTarget => self.handle_found(event),
            MigrateState::ProbeTarget => self.handle_probe(event),
            MigrateState::HashTarget => self.handle_hashed(event),
            MigrateState::ReadSource => self.handle_source(event),
            MigrateState::CheckLinked => self.handle_linked(event),
            MigrateState::StatPlain => self.handle_stat(event),
            MigrateState::ReadBlob => self.handle_read(event),
            MigrateState::WriteBlob => self.handle_written(event),
            MigrateState::StartTransaction => self.handle_started(event),
            MigrateState::ReadBucket => self.handle_bucket(event),
            MigrateState::ScanRules => self.handle_rules(event),
            MigrateState::FenceTarget => self.handle_fence(event),
            MigrateState::CheckVersion => self.handle_check(event),
            MigrateState::CheckCopy => self.handle_copy(event),
            MigrateState::ReadTarget => self.handle_target(event),
            MigrateState::WriteRows => self.handle_rows(event),
            MigrateState::UpdateUsage => self.handle_usage(event),
            MigrateState::Commit => self.handle_commit(event),
            MigrateState::Abort => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. })
                | Event::Storage(StorageEvent::Error { .. }) => self.release(),
                other => self.unexpected(other),
            },
            MigrateState::Release => match event {
                Event::Blob(BlobEvent::ReservationReleased { .. })
                | Event::Blob(BlobEvent::Error(_)) => self.finish(),
                other => self.unexpected(other),
            },
            MigrateState::Finish | MigrateState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, MigrateState::Finish | MigrateState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let complete = self.is_complete();
        let output = self.output.filter(|_| complete);
        output.unwrap_or(Err(MigrateVersionError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
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
    let target = rule.rename_target(patterns, &location.backend_path, &key.key, key.version_id);
    location.is_external()
        && !location.staging
        && !location.partial
        && location.backend == plain.backend
        && matches!(location.format.layout, StoredLayout::Plain(_))
        && target.as_deref() == Some(plain.path.as_str())
}

/// Whether the move `record` of `bucket` was started to rename linked files.
pub async fn rename_asked(
    context: &crate::driver::DriverContext,
    bucket: &str,
    record: &CompressionMigration,
) -> Result<bool, String> {
    let read = StorageEffect::Read {
        key_space: RENAME_MOVE_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            let started = record.started_at_ms.to_string();
            Ok(record.moving && value.is_some_and(|value| value.as_ref() == started.as_bytes()))
        }
        other => Err(format!("could not read the rename request: {other:?}")),
    }
}

/// Reads the earlier patterns of the rules a move resolves against.
pub async fn read_earlier(
    context: &crate::driver::DriverContext,
    routing: &RoutingSnapshot,
) -> Result<Earlier, String> {
    let mut earlier = Earlier::new();
    for rule in &routing.imports {
        let patterns = crate::groups::imports::read_earlier(context, rule.import_id).await;
        let patterns = patterns.map_err(|error| error.to_string())?;
        if !patterns.is_empty() {
            earlier.insert(rule.import_id, patterns);
        }
    }
    Ok(earlier)
}

/// Copies that stay: unfinished ones, and on a move files in a linked folder.
fn stays(location: &BackendLocation, moving: bool) -> bool {
    location.staging || location.partial || (moving && location.is_external())
}

/// Import rules one publication read returns at a time.
const RULE_PAGE: usize = 128;

/// Versions one task run handles per bucket before it yields to other timers.
const MIGRATION_PAGE: usize = 64;
/// Queued buckets one task run advances; the next run continues after the last one.
const BUCKET_PAGE: usize = 16;
/// Delay between task runs while a migration still has versions left.
pub const MIGRATION_CONTINUE: Duration = Duration::from_secs(1);
/// Passes started again after a pass with failures, before the migration ends with them.
const MIGRATION_RETRIES: u32 = 10;
/// Wait before the first retry pass; it doubles per pass up to `RETRY_MAX`.
const RETRY_BASE: Duration = Duration::from_secs(60);
const RETRY_MAX: Duration = Duration::from_secs(3600);

/// What one task run left behind.
#[derive(Debug, PartialEq)]
pub struct MigrationRun {
    /// When the task must run again; `None` once no queued migration has work left.
    pub next: Option<Duration>,
    /// Queue key the next run resumes after; `None` starts at the head.
    pub cursor: Option<Key>,
}

/// Advances the unfinished migrations of one page of queued buckets by one page each.
pub async fn process_migrations(
    context: &crate::driver::DriverContext,
    after: Option<Key>,
) -> Result<MigrationRun, String> {
    let resumed = after.is_some();
    let (rows, cursor) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        COMPRESSION_QUEUE_KEYSPACE,
        None,
        after,
        BUCKET_PAGE,
        None,
    )
    .await?;
    let now = crate::effect_adapters::routing::now_ms();
    let mut next = None::<Duration>;
    let mut soon = |after: Duration| next = Some(next.map_or(after, |next| next.min(after)));
    for (key, _) in rows {
        let Some(record) = read_progress(context, &key).await? else {
            continue;
        };
        if record.finished_at_ms.is_some() {
            continue;
        }
        if let Some(at) = record.retry_at_ms.filter(|at| *at > now) {
            soon(Duration::from_millis(at - now));
            continue;
        }
        let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
        // Encryption replaced this work: its transition converts every version.
        if encrypting(context, &key).await? {
            let mut retired = record;
            retired.retry_at_ms = None;
            retired.finished_at_ms = Some(now);
            store_progress(context, &bucket, &retired).await?;
            continue;
        }
        if let Some(after) = migrate_page(context, &bucket, record).await? {
            soon(after);
        }
    }
    // Encryption transitions share this task; a run at the queue head advances them all.
    if !resumed
        && let Some(after) = crate::blob::migration::queue::process_transitions(context).await?
    {
        soon(after);
    }
    // Storage checks share this task too.
    if !resumed && let Some(after) = check::process_checks(context).await? {
        soon(after);
    }
    // Queued buckets after this page, or before it when the run resumed mid-queue.
    if cursor.is_some() || resumed {
        soon(MIGRATION_CONTINUE);
    }
    Ok(MigrationRun { next, cursor })
}

async fn encrypting(context: &crate::driver::DriverContext, bucket: &Key) -> Result<bool, String> {
    let read = StorageEffect::Read {
        key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
        key: bucket.clone(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            BucketEncryption::from_row(value.as_deref())
                .map(|settings| settings.is_encrypted())
                .map_err(|error| error.to_string())
        }
        other => Err(format!("could not read encryption settings: {other:?}")),
    }
}

async fn read_progress(
    context: &crate::driver::DriverContext,
    key: &Key,
) -> Result<Option<CompressionMigration>, String> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| CompressionMigration::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read migration progress: {other:?}")),
    }
}

/// Wait before retry pass `retries + 1`.
fn retry_delay(retries: u32) -> Duration {
    RETRY_BASE
        .saturating_mul(1 << retries.min(16))
        .min(RETRY_MAX)
}

/// Migrates one page of a bucket's versions and stores the progress, unless a newer
/// setting replaced the record meanwhile. Returns when the bucket needs the next run.
async fn migrate_page(
    context: &crate::driver::DriverContext,
    bucket: &str,
    mut record: CompressionMigration,
) -> Result<Option<Duration>, String> {
    // A due retry pass counts its own skips and failures.
    if record.retry_at_ms.take().is_some() {
        record.skipped = 0;
        record.failed = 0;
    }
    let routing = match record.moving {
        true => match move_routing(context, bucket).await? {
            Some(routing) => Some(routing),
            None => {
                record.finished_at_ms = Some(crate::effect_adapters::routing::now_ms());
                store_progress(context, bucket, &record).await?;
                return Ok(None);
            }
        },
        false => None,
    };
    // Without earlier patterns no file is renamed, so only an asked rename reads them.
    let earlier = match routing.as_ref() {
        Some(routing) if rename_asked(context, bucket, &record).await? => {
            read_earlier(context, routing).await?
        }
        _ => Earlier::new(),
    };
    let prefix = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (versions, next) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        BLOB_VERSIONS_KEYSPACE,
        Some(prefix.into()),
        record.cursor.clone().map(Into::into),
        MIGRATION_PAGE,
        None,
    )
    .await?;
    for (key, _) in &versions {
        let version_key = VersionKey::from_bytes(key.as_ref()).map_err(|e| e.to_string())?;
        record.cursor = Some(key.to_vec());
        // Keys outside a move's prefix are only re-encoded.
        let routing = routing
            .clone()
            .filter(|_| version_key.key.starts_with(&record.prefix));
        let moving = routing.is_some();
        let mut operation =
            MigrateVersionOperation::new(version_key, record.target, SystemTime::now());
        if let Some(routing) = routing {
            operation = operation
                .with_routing(routing)
                .with_earlier(earlier.clone());
        }
        let moved = crate::driver::drive(operation, context).await;
        let broken = moved.is_err() && moving && is_broken(context, key).await;
        match moved {
            Ok(MigrateOutcome::Migrated) => record.migrated += 1,
            Ok(MigrateOutcome::Skipped) => record.skipped += 1,
            // A move records a copy that cannot be read as broken instead of retrying it.
            Err(_) if broken => record.skipped += 1,
            Err(error) => {
                record.failed += 1;
                tracing::warn!(bucket, %error, "Failed to re-encode a version");
            }
        }
    }
    let now = crate::effect_adapters::routing::now_ms();
    let after = match (next.is_some(), record.failed > 0) {
        (true, _) => Some(MIGRATION_CONTINUE),
        // Successful versions are already in the target, so a retry pass reads no blob for them.
        (false, true) if record.retries < MIGRATION_RETRIES => {
            let wait = retry_delay(record.retries);
            record.retries += 1;
            record.cursor = None;
            record.retry_at_ms = Some(now.saturating_add(wait.as_millis() as u64));
            Some(wait)
        }
        (false, _) => {
            record.finished_at_ms = Some(now);
            None
        }
    };
    store_progress(context, bucket, &record).await?;
    Ok(after)
}

/// Whether a version whose move failed is broken; a check records it.
async fn is_broken(context: &crate::driver::DriverContext, key: &Key) -> bool {
    let Ok(version_key) = VersionKey::from_bytes(key.as_ref()) else {
        return false;
    };
    let now_ms = crate::effect_adapters::routing::now_ms();
    let operation = check::CheckVersionOperation::new(version_key, now_ms);
    let outcome = crate::driver::drive(operation, context).await;
    matches!(outcome, Ok(check::CheckOutcome::Broken(_)))
}

/// The routing a move resolves against, read once per page; `None` once the bucket is gone.
pub async fn move_routing(
    context: &crate::driver::DriverContext,
    bucket: &str,
) -> Result<Option<RoutingSnapshot>, String> {
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(read).await;
    let info = crate::groups::backends::parse_read(event, BucketInfo::from_bytes);
    let Some(info) = info.map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    let snapshot = crate::effect_adapters::routing::bucket_snapshot(context, &info).await;
    snapshot.map(Some).map_err(|error| error.to_string())
}

async fn store_progress(
    context: &crate::driver::DriverContext,
    bucket: &str,
    record: &CompressionMigration,
) -> Result<(), String> {
    let storage = &context.storage_handle;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("could not start a progress transaction: {other:?}")),
    };
    let staged = stage_progress(storage, txn_id, bucket, record).await;
    if !matches!(staged, Ok(true)) {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return staged.map(|_| ());
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(format!("migration progress was not stored: {other:?}")),
    }
}

/// Writes the progress in `txn_id`. Returns `false` when a newer setting replaced the record.
async fn stage_progress(
    storage: &aruna_storage::StorageHandle,
    txn_id: TxnId,
    bucket: &str,
    record: &CompressionMigration,
) -> Result<bool, String> {
    let key: Key = bucket.as_bytes().to_vec().into();
    let current = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await;
    let stored = match current {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            CompressionMigration::from_bytes(value.as_ref()).map_err(|error| error.to_string())?
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => return Ok(false),
        other => return Err(format!("could not read migration progress: {other:?}")),
    };
    if stored.started_at_ms != record.started_at_ms || stored.target != record.target {
        return Ok(false);
    }
    let value = record.to_bytes().map_err(|error| error.to_string())?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(format!("could not write migration progress: {other:?}")),
    }
    // A finished migration leaves the queue together with its last progress.
    if record.finished_at_ms.is_some() {
        match storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: COMPRESSION_QUEUE_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            other => return Err(format!("could not leave the migration queue: {other:?}")),
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn read_result(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    /// The publication's read of the bucket and its encryption settings.
    fn bucket_read(compression: Compression, settings: Option<BucketEncryption>) -> Event {
        let settings = settings.map(|settings| settings.to_bytes().unwrap().into());
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Vec::new().into(), Some(bucket(compression).into())),
                (Vec::new().into(), settings),
            ],
        })
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
        }
        .to_bytes()
        .unwrap()
    }

    /// Drives the operation up to the transaction that publishes the new copy.
    fn written(operation: &mut MigrateVersionOperation, version: &BlobVersion) -> Effects {
        operation.start();
        operation.step(read_result(Some(version.to_bytes().unwrap())));
        operation.step(read_result(Some(location(false).to_bytes().unwrap())));
        operation.step(read_result(None));
        let blob = BackendStream::new(futures_util::stream::empty::<
            Result<bytes::Bytes, std::io::Error>,
        >());
        let effects = operation.step(Event::Blob(BlobEvent::ReadFinished {
            blob,
            stream_size: 40,
        }));
        let [Effect::Blob(BlobEffect::Write { resolved, .. })] = effects.as_slice() else {
            panic!("expected one write, got {effects:?}")
        };
        assert_eq!(resolved.compression, ZSTD);
        operation.step(Event::Blob(BlobEvent::WriteFinished {
            location: location(true),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }))
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
        operation.start();

        let effects = operation.step(read_result(Some(version.to_bytes().unwrap())));

        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
    }

    #[test]
    fn publishes_new_copy() {
        let mut operation = operation();
        let version = raw_version();
        written(&mut operation, &version);
        operation.step(bucket_read(ZSTD, None));
        operation.step(read_result(Some(version.to_bytes().unwrap())));

        let effects = operation.step(read_result(None));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
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
    }

    #[test]
    fn adopts_existing_copy() {
        // Another version already moved this content: nothing is read or written again.
        let mut operation = operation();
        let version = raw_version();
        operation.start();
        operation.step(read_result(Some(version.to_bytes().unwrap())));
        operation.step(read_result(Some(location(false).to_bytes().unwrap())));

        let effects = operation.step(read_result(Some(location(true).to_bytes().unwrap())));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(bucket_read(ZSTD, None));
        operation.step(read_result(Some(version.to_bytes().unwrap())));
        let effects = operation.step(read_result(Some(location(true).to_bytes().unwrap())));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
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
        written(&mut operation, &version);
        operation.step(bucket_read(ZSTD, None));
        let effects = operation.step(read_result(Some(version.to_bytes().unwrap())));
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the registration read, got {effects:?}")
        };
        assert_eq!(key_space, MANAGED_COPY_KEYSPACE);
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
        operation.step(read_result(Some(copy.to_bytes().unwrap())));

        let effects = operation.step(read_result(None));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
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
        written(&mut operation, &version);
        operation.step(bucket_read(ZSTD, None));
        operation.step(read_result(Some(version.to_bytes().unwrap())));

        operation.step(read_result(None));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::default(),
        }));
        operation.step(Event::Blob(BlobEvent::ReservationReleased {
            id: Ulid::nil(),
        }));

        assert_eq!(
            operation.finalize(),
            Err(MigrateVersionError::ManagedCopy(
                ManagedCopyError::Unregistered
            ))
        );
    }

    #[test]
    fn retry_waits_double() {
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
        written(&mut operation, &raw_version());

        let effects = operation.step(bucket_read(Compression::Off, None));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::default(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation { .. })]
        ));
        let effects = operation.step(Event::Blob(BlobEvent::ReservationReleased {
            id: Ulid::nil(),
        }));
        assert!(matches!(effects.as_slice(), [Effect::Task(_)]));
        assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
    }

    #[test]
    fn encrypting_bucket_skips() {
        // Encryption was enabled while this compression pass waited: a plain copy is never
        // published for the version, and the written copy goes back to the cleanup queue.
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let mut operation = operation();
        written(&mut operation, &raw_version());
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_bytes([3; 16])),
            key_generation: 1,
            ..Default::default()
        };

        let effects = operation.step(bucket_read(ZSTD, Some(settings)));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::default(),
        }));
        operation.step(Event::Blob(BlobEvent::ReservationReleased {
            id: Ulid::nil(),
        }));
        assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
    }

    #[test]
    fn encrypting_adoption_skips() {
        // A matching plain copy appeared after encryption was enabled: the retry never adopts
        // it for this version.
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let mut operation = operation();
        let version = raw_version();
        operation.start();
        operation.step(read_result(Some(version.to_bytes().unwrap())));
        operation.step(read_result(Some(location(false).to_bytes().unwrap())));
        operation.step(read_result(Some(location(true).to_bytes().unwrap())));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([3; 16])),
            key_generation: 1,
            ..Default::default()
        };

        let effects = operation.step(bucket_read(ZSTD, Some(settings)));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::default(),
        }));
        assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
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
            task_handle: None,
            compute_handle: None,
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
    }

    #[test]
    fn rejects_wrong_event() {
        let mut operation = operation();
        operation.start();

        operation.step(Event::Blob(BlobEvent::DeleteFinished));

        assert!(matches!(
            operation.finalize(),
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

        fn empty_blob() -> Event {
            let blob = BackendStream::new(futures_util::stream::empty::<
                Result<bytes::Bytes, std::io::Error>,
            >());
            Event::Blob(BlobEvent::ReadFinished {
                blob,
                stream_size: 40,
            })
        }

        /// The publication's reads: bucket, encryption, group default and the linked rule.
        /// Answers the publication's rule reads: bucket, encryption, group default, then the
        /// group's import rules; returns what follows them.
        fn rules_read(
            operation: &mut MigrateVersionOperation,
            default: Option<RoutingTarget>,
            rule: Option<BackendImport>,
        ) -> Effects {
            let group = GroupStorageRouting {
                group_id: GROUP,
                default_target: default,
                updated_at: SystemTime::UNIX_EPOCH,
                updated_by: Default::default(),
            };
            let values = vec![
                (Vec::new().into(), Some(bucket(Compression::Off).into())),
                (Vec::new().into(), None),
                (Vec::new().into(), Some(group.to_bytes().unwrap().into())),
            ];
            let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
            let [Effect::Storage(StorageEffect::Iter { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the import rule scan, got {effects:?}")
            };
            assert_eq!(key_space, BACKEND_IMPORT_KEYSPACE);
            let values = (rule.into_iter())
                .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
                .collect();
            operation.step(Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after: None,
            }))
        }

        fn storage_record(disabled: bool) -> Event {
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
            let effects = rules_read(operation, default, rule);
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the storage fence, got {effects:?}")
            };
            assert_eq!(key_space, aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE);
            operation.step(storage_record(false));
        }

        /// Answers the file's write with the location the adapter reports.
        fn plain_written(plain: &PlainPath) -> Event {
            let mut written = on_storage(location(false));
            (written.storage_bucket, written.backend_path) = (String::new(), plain.path.clone());
            written.ulid = plain.ulid;
            written.format.layout = StoredLayout::Plain(plain.ulid);
            Event::Blob(BlobEvent::PlainWritten {
                location: written,
                version: None,
                etag: None,
            })
        }

        /// Drives a move of `version` from Aruna storage up to its publication reads; a linked
        /// target finds its path taken when `taken`.
        fn copied(
            operation: &mut MigrateVersionOperation,
            version: &BlobVersion,
            taken: bool,
        ) -> Effects {
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));
            let effects = operation.step(read_result(Some(location(false).to_bytes().unwrap())));
            let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
                panic!("expected the target row read, got {effects:?}")
            };
            assert_eq!(BlobLocationKey::from_bytes(key).unwrap().backend, storage());
            let mut effects = operation.step(read_result(None));
            if let [Effect::Blob(BlobEffect::StatPlain { .. })] = effects.as_slice() {
                effects = operation.step(Event::Blob(BlobEvent::PlainStat {
                    exists: taken,
                    version: None,
                }));
            }
            assert!(matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::Read { .. })]
            ));
            let effects = operation.step(empty_blob());
            let new = match effects.as_slice() {
                [Effect::Blob(BlobEffect::Write { resolved, .. })] => {
                    assert_eq!(resolved.backend, storage());
                    Event::Blob(BlobEvent::WriteFinished {
                        location: on_storage(location(false)),
                    })
                }
                [Effect::Blob(BlobEffect::WritePlain { plain, .. })] => plain_written(plain),
                other => panic!("expected a write, got {other:?}"),
            };
            operation.step(new);
            operation.step(Event::Storage(StorageEvent::TransactionStarted {
                txn_id: TxnId::default(),
            }))
        }

        fn batch(effects: &Effects) -> &Vec<(String, Key, Value)> {
            let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice()
            else {
                panic!("expected the row writes, got {effects:?}")
            };
            writes
        }

        fn aborted(effects: &Effects) -> bool {
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            )
        }

        #[test]
        fn moves_to_storage() {
            // The version names the copy on the group's storage; the old copy is reclaimed.
            let mut operation = operation("k", Vec::new());
            let version = raw_version();
            copied(&mut operation, &version, false);
            published(&mut operation, None);
            operation.step(read_result(Some(version.to_bytes().unwrap())));

            let effects = operation.step(read_result(None));

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
            operation.start();

            let effects = operation.step(read_result(Some(version.to_bytes().unwrap())));

            assert!(effects.is_empty());
            assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
        }

        #[test]
        fn discards_changed_rule() {
            // The default rule went back to Aruna storage during the copy.
            let mut operation = operation("k", Vec::new());
            copied(&mut operation, &raw_version(), false);

            let effects = rules_read(&mut operation, None, None);

            assert!(aborted(&effects));
            operation.step(Event::Storage(StorageEvent::TransactionAborted {
                txn_id: TxnId::default(),
            }));
            let effects = operation.step(Event::Blob(BlobEvent::ReservationReleased {
                id: Ulid::nil(),
            }));
            // The unpublished copy goes to the cleanup queue.
            assert_eq!(effects.len(), 1);
            assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
        }

        #[test]
        fn disabled_target_discards() {
            // A storage disabled during the copy never takes the version, so nothing is reclaimed.
            let mut operation = operation("k", Vec::new());
            copied(&mut operation, &raw_version(), false);
            rules_read(
                &mut operation,
                Some(RoutingTarget::Backend(storage())),
                None,
            );

            let effects = operation.step(storage_record(true));

            assert!(aborted(&effects));
        }

        #[test]
        fn skips_missing_target() {
            // A row on the target whose bytes are gone is never adopted.
            let mut operation = operation("k", Vec::new());
            operation.start();
            operation.step(read_result(Some(raw_version().to_bytes().unwrap())));
            operation.step(read_result(Some(location(false).to_bytes().unwrap())));
            let target = on_storage(location(false));

            let effects = operation.step(read_result(Some(target.to_bytes().unwrap())));
            let [Effect::Blob(BlobEffect::StatCopy { location })] = effects.as_slice() else {
                panic!("expected the target probe, got {effects:?}")
            };
            assert!(location.same_object(&target));
            let effects = operation.step(Event::Blob(BlobEvent::CopyStat { exists: false }));

            assert!(effects.is_empty());
            assert_eq!(
                operation.finalize(),
                Err(MigrateVersionError::TargetMissing)
            );
        }

        #[test]
        fn probed_target_revalidated() {
            // The adopted row must still be the probed copy when the version moves to it.
            let mut operation = operation("k", Vec::new());
            let version = raw_version();
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));
            operation.step(read_result(Some(location(false).to_bytes().unwrap())));
            operation.step(read_result(Some(
                on_storage(location(false)).to_bytes().unwrap(),
            )));
            let effects = operation.step(Event::Blob(BlobEvent::CopyStat { exists: true }));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::StartTransaction { .. })]
            ));
            operation.step(Event::Storage(StorageEvent::TransactionStarted {
                txn_id: TxnId::default(),
            }));
            published(&mut operation, None);
            operation.step(read_result(Some(version.to_bytes().unwrap())));

            let replaced = on_storage(location(false));
            let effects = operation.step(read_result(Some(replaced.to_bytes().unwrap())));

            assert!(aborted(&effects));
        }

        #[test]
        fn writes_linked_file() {
            // The file lands where an upload goes and its importer skips it later.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            let version = raw_version();
            let effects = copied(&mut operation, &version, false);
            let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice()
            else {
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
            operation.step(read_result(Some(version.to_bytes().unwrap())));

            let effects = operation.step(read_result(None));

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
        fn never_overwrites_path() {
            // A file an earlier attempt or a user left at the path is never overwritten.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            copied(&mut operation, &raw_version(), true);

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
            copied(&mut operation, &raw_version(), false);
            let default = Some(RoutingTarget::Backend(storage()));

            let effects = rules_read(&mut operation, default, Some(rule()));

            assert!(aborted(&effects));
        }

        #[test]
        fn removed_rule_discards() {
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            copied(&mut operation, &raw_version(), false);
            let default = Some(RoutingTarget::Backend(storage()));

            let effects = rules_read(&mut operation, default, None);

            assert!(aborted(&effects));
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
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));
            let effects = operation.step(read_result(Some(old.to_bytes().unwrap())));
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the import row read, got {effects:?}")
            };
            assert_eq!(key_space, IMPORTED_OBJECT_KEYSPACE);
            operation.step(read_result(None));
            let effects = operation.step(read_result(Some(old.to_bytes().unwrap())));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::StatPlain { .. })]
            ));
            operation.step(Event::Blob(BlobEvent::PlainStat {
                exists: false,
                version: None,
            }));
            let effects = operation.step(empty_blob());
            let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
                panic!("expected the file write, got {effects:?}")
            };
            operation.step(plain_written(plain));
            operation.step(Event::Storage(StorageEvent::TransactionStarted {
                txn_id: TxnId::default(),
            }));
            published(&mut operation, Some(rule()));
            operation.step(read_result(Some(version.to_bytes().unwrap())));

            let effects = operation.step(read_result(Some(old.to_bytes().unwrap())));

            let spaces: Vec<&str> = batch(&effects).iter().map(|(s, ..)| s.as_str()).collect();
            assert_eq!(spaces, [IMPORTED_OBJECT_KEYSPACE, BLOB_VERSIONS_KEYSPACE]);
        }

        #[test]
        fn linked_copy_done() {
            // Once the version's file is recorded, a later move leaves it alone.
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            let (version, old) = in_place();
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));
            operation.step(read_result(Some(old.to_bytes().unwrap())));
            let row =
                ImportedObject::written(Ulid::from_bytes([2; 16]), "in/x/one.csv".into(), 40, None);

            let effects = operation.step(read_result(Some(row.to_bytes().unwrap())));

            assert!(effects.is_empty());
            assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
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
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));

            let effects = operation.step(read_result(Some(external.to_bytes().unwrap())));

            assert!(effects.is_empty());
            assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
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

        /// A move that knows the rule's earlier prefix pattern, up to the stat of the new name.
        fn renaming(
            version: &BlobVersion,
            old: &BackendLocation,
            source: Option<&[u8]>,
        ) -> MigrateVersionOperation {
            let earlier = Earlier::from([(IMPORT, vec!["{dir}{id}_{name}".to_string()])]);
            let mut operation = operation("in/x/one.csv", vec![rule()]).with_earlier(earlier);
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));
            operation.step(read_result(Some(old.to_bytes().unwrap())));
            // The old file's backend version binds a later delete to the renamed bytes.
            operation.step(read_result(source.map(<[u8]>::to_vec)));
            let effects = operation.step(read_result(None));
            let [Effect::Blob(BlobEffect::StatPlain { path, .. })] = effects.as_slice() else {
                panic!("expected the stat of the new name, got {effects:?}")
            };
            assert_eq!(
                path,
                &format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]))
            );
            operation
        }

        /// Drives a rename to its publication rows.
        fn renamed(
            source: Option<&[u8]>,
        ) -> (Vec<(String, Key, Value)>, PlainPath, BackendLocation) {
            let (version, old) = prefixed();
            let mut operation = renaming(&version, &old, source);
            operation.step(Event::Blob(BlobEvent::PlainStat {
                exists: false,
                version: None,
            }));
            let effects = operation.step(empty_blob());
            let [Effect::Blob(BlobEffect::WritePlain { plain, .. })] = effects.as_slice() else {
                panic!("expected the new file, got {effects:?}")
            };
            assert!(plain.if_not_exists);
            let plain = plain.clone();
            operation.step(plain_written(&plain));
            operation.step(Event::Storage(StorageEvent::TransactionStarted {
                txn_id: TxnId::default(),
            }));
            published(&mut operation, Some(rule()));
            operation.step(read_result(Some(version.to_bytes().unwrap())));

            let effects = operation.step(read_result(None));
            (batch(&effects).clone(), plain, old)
        }

        #[test]
        fn renames_linked_file() {
            // The version names the new file; the old one goes only through the cleanup queue.
            let (writes, plain, old) = renamed(Some(b"v1"));
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
            // Without the old file's backend version nothing names its bytes, so it stays.
            let (writes, ..) = renamed(None);
            assert!(
                !writes
                    .iter()
                    .any(|(space, ..)| space == BLOB_CLEANUP_KEYSPACE)
            );
            assert!(
                writes
                    .iter()
                    .any(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
            );
        }

        #[test]
        fn resumes_taken_name() {
            // An interrupted rename left its copy at the new name; matching bytes are adopted.
            let (version, old) = prefixed();
            for (hash, adopted) in [(4u8, true), (5u8, false)] {
                let mut operation = renaming(&version, &old, Some(b"v1"));
                let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
                    exists: true,
                    version: None,
                }));
                let [Effect::Blob(BlobEffect::HashExternal { path, .. })] = effects.as_slice()
                else {
                    panic!("expected the hash of the taken name, got {effects:?}")
                };
                let mut found = on_storage(location(false));
                (found.storage_bucket, found.backend_path) = (String::new(), path.clone());
                found.hashes = HashMap::from([(HASH_BLAKE3.to_string(), vec![hash; 32])]);
                let effects = operation.step(Event::Blob(BlobEvent::ExternalHashed {
                    location: found,
                    version: None,
                }));
                if adopted {
                    assert!(matches!(
                        effects.as_slice(),
                        [Effect::Storage(StorageEffect::StartTransaction { .. })]
                    ));
                    continue;
                }
                // Another file at the new name is never replaced or renamed around.
                assert!(effects.is_empty());
                let error = MigrateVersionError::Blob(BlobError::PathTaken);
                assert_eq!(operation.finalize(), Err(error));
            }
        }

        #[test]
        fn keeps_unknown_name() {
            // Without the earlier pattern the name tells no version, so the file stays.
            let (version, old) = prefixed();
            let mut operation = operation("in/x/one.csv", vec![rule()]);
            operation.start();
            operation.step(read_result(Some(version.to_bytes().unwrap())));
            let effects = operation.step(read_result(Some(old.to_bytes().unwrap())));
            assert!(effects.is_empty());
            assert_eq!(operation.finalize(), Ok(MigrateOutcome::Skipped));
        }
    }
}
