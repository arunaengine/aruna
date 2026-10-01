//! Re-encodes one local version with its bucket's compression. The new copy is written first and
//! published only if the bucket setting and the version are unchanged; the old copy is queued
//! for reclaim, which keeps it while any other version still names it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::cleanup::schedule_cleanup_effect;
use crate::blob::records::{blob_location_read, read_version_effect};
use crate::node::usage_stats::{StoredDelta, UsageCounterUpdate, UsageUpdateError};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BlobVersionState, BucketInfo, ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::format::{Compression, EncodingClass};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;

/// Steps of one version migration; errors name the step they stopped in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrateState {
    Init,
    ReadVersion,
    ReadLocation,
    ReadBlob,
    WriteBlob,
    StartTransaction,
    ReadBucket,
    CheckVersion,
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
    /// Nothing to do: already encoded, not materialized, governed, or changed meanwhile.
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

/// Moves one version to a copy encoded with `target`, on the backend that holds it now.
#[derive(Debug, PartialEq)]
pub struct MigrateVersionOperation {
    version_key: VersionKey,
    target: Compression,
    now: SystemTime,
    state: MigrateState,
    txn_id: Option<TxnId>,
    version: Option<BlobVersion>,
    old: Option<BackendLocation>,
    new: Option<BackendLocation>,
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
            state: MigrateState::Init,
            txn_id: None,
            version: None,
            old: None,
            new: None,
            owns_row: false,
            usage: None,
            output: None,
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
        // Governed versions keep a registered managed copy; they are left alone.
        let wanted = match self.target {
            Compression::Off => EncodingClass::Raw,
            Compression::Zstd { level } => EncodingClass::Zstd { level },
        };
        let key = match &version.state {
            BlobVersionState::Materialized { encoding, .. }
                if *encoding != wanted && version.placement_policies.is_empty() =>
            {
                version.location_key()
            }
            _ => None,
        };
        let Some(key) = key else {
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
        if old.staging || old.partial {
            return self.skip();
        }
        self.old = Some(old.clone());
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
        let resolved = ResolvedBackend::new(old.backend.clone(), old.storage_class.clone())
            .with_compression(self.target);
        let effect = Effect::Blob(BlobEffect::Write {
            bucket: self.version_key.bucket.clone(),
            key: self.version_key.key.clone(),
            resolved,
            created_by: old.created_by,
            blob,
        });
        self.state = MigrateState::WriteBlob;
        smallvec![effect]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let new = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
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
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.version_key.bucket.as_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    /// Publishes only while the bucket still asks for this target.
    fn handle_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let current = match value.map(|value| BucketInfo::from_bytes(value.as_ref())) {
            Some(Ok(info)) => Some(info.compression),
            Some(Err(error)) => return self.fail(error.into()),
            None => None,
        };
        if current != Some(self.target) {
            return self.skip();
        }
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
        let key = match self.new.as_ref().map(BackendLocation::location_key) {
            Some(Ok(key)) => key,
            Some(Err(error)) => return self.fail(error.into()),
            None => return self.fail(MigrateVersionError::NotFinished),
        };
        self.state = MigrateState::ReadTarget;
        smallvec![blob_location_read(&key, self.txn_id)]
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
        let (Some(new), Some(old), Some(version)) =
            (self.new.as_ref(), self.old.as_ref(), self.version.as_ref())
        else {
            return Err(MigrateVersionError::NotFinished);
        };
        let new_key = new.location_key()?;
        let old_key = old.location_key()?;
        let mut writes = Vec::new();
        match existing {
            Some(value) => {
                let existing = BackendLocation::from_bytes(&value)?;
                self.owns_row = existing.same_object(new);
            }
            None => {
                writes.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    new_key.to_bytes().into(),
                    new.to_bytes()?.into(),
                ));
                self.owns_row = true;
                self.usage = Some(UsageCounterUpdate::for_stored(
                    StoredDelta::for_location(new, true).ok_or(MigrateVersionError::NotFinished)?,
                ));
            }
        }
        let mut migrated = version.clone();
        if let BlobVersionState::Materialized { encoding, .. } = &mut migrated.state {
            *encoding = new_key.encoding;
        }
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            self.version_key.to_bytes()?.into(),
            migrated.to_bytes()?.into(),
        ));
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
            MigrateState::ReadBlob => self.handle_read(event),
            MigrateState::WriteBlob => self.handle_written(event),
            MigrateState::StartTransaction => self.handle_started(event),
            MigrateState::ReadBucket => self.handle_bucket(event),
            MigrateState::CheckVersion => self.handle_check(event),
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
