//! Re-encodes one local version with its bucket's compression. The new copy is published only if
//! the setting and version are unchanged; reclaim keeps the old copy while another version uses it.
//! A copy that already exists in the target encoding is adopted without reading the old one.
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
    BackendLocation, BlobLocationKey, BlobVersion, BlobVersionState, BucketInfo, ResolvedBackend,
    VersionKey,
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
    FindTarget,
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
        let wanted = EncodingClass::from(self.target);
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
        self.old = Some(old);
        match self.target_key() {
            Ok(key) => {
                self.state = MigrateState::FindTarget;
                smallvec![blob_location_read(&key, None)]
            }
            Err(error) => self.fail(error),
        }
    }

    /// The row of this content in the target encoding, on the backend that holds it now.
    fn target_key(&self) -> Result<BlobLocationKey, MigrateVersionError> {
        let old = self.old.as_ref().ok_or(MigrateVersionError::NotFinished)?;
        let mut key = old.location_key()?;
        key.encoding = EncodingClass::from(self.target);
        Ok(key)
    }

    /// A finished copy in the target encoding is adopted; only without one is the old copy read.
    fn handle_found(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let found = match value.map(|value| BackendLocation::from_bytes(value.as_ref())) {
            Some(Ok(found)) => !found.staging && !found.partial,
            Some(Err(error)) => return self.fail(error.into()),
            None => false,
        };
        if found {
            self.state = MigrateState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        }
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
        let mut migrated = version.clone();
        if let BlobVersionState::Materialized { encoding, .. } = &mut migrated.state {
            *encoding = published.format.encoding();
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
            MigrateState::FindTarget => self.handle_found(event),
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

/// Versions one task run handles before it yields to other timers.
const MIGRATION_PAGE: usize = 64;
/// Delay between task runs while a migration still has versions left.
pub const MIGRATION_CONTINUE: std::time::Duration = std::time::Duration::from_secs(1);

/// Advances every unfinished migration by one page. Returns whether work is left.
pub async fn process_migrations(context: &crate::driver::DriverContext) -> Result<bool, String> {
    use aruna_core::keyspaces::COMPRESSION_MIGRATION_KEYSPACE;
    use aruna_core::structs::storage::format::CompressionMigration;
    let (rows, _) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        COMPRESSION_MIGRATION_KEYSPACE,
        None,
        None,
        usize::MAX,
        None,
    )
    .await?;
    let mut pending = false;
    for (key, value) in rows {
        let record = CompressionMigration::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
        if record.finished_at_ms.is_some() {
            continue;
        }
        let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
        pending |= migrate_page(context, &bucket, record).await?;
    }
    Ok(pending)
}

/// Migrates one page of a bucket's versions and stores the progress, unless a
/// newer setting replaced the record meanwhile.
async fn migrate_page(
    context: &crate::driver::DriverContext,
    bucket: &str,
    mut record: aruna_core::structs::storage::format::CompressionMigration,
) -> Result<bool, String> {
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
        let operation = MigrateVersionOperation::new(version_key, record.target, SystemTime::now());
        match crate::driver::drive(operation, context).await {
            Ok(MigrateOutcome::Migrated) => record.migrated += 1,
            Ok(MigrateOutcome::Skipped) => record.skipped += 1,
            Err(error) => {
                record.failed += 1;
                tracing::warn!(bucket, %error, "Failed to re-encode a version");
            }
        }
        record.cursor = Some(key.to_vec());
    }
    let done = next.is_none();
    if done {
        record.finished_at_ms = Some(crate::effect_adapters::routing::now_ms());
    }
    store_progress(context, bucket, &record).await?;
    Ok(!done)
}

async fn store_progress(
    context: &crate::driver::DriverContext,
    bucket: &str,
    record: &aruna_core::structs::storage::format::CompressionMigration,
) -> Result<(), String> {
    use aruna_core::keyspaces::COMPRESSION_MIGRATION_KEYSPACE;
    use aruna_core::structs::storage::format::CompressionMigration;
    let storage = &context.storage_handle;
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        return Err("could not start a progress transaction".to_string());
    };
    let key: Key = bucket.as_bytes().to_vec().into();
    let current = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await;
    let replaced = match current {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => CompressionMigration::from_bytes(value.as_ref())
            .map(|stored| {
                stored.started_at_ms != record.started_at_ms || stored.target != record.target
            })
            .unwrap_or(true),
        _ => true,
    };
    if replaced {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Ok(());
    }
    let value = record.to_bytes().map_err(|error| error.to_string())?;
    storage
        .send_storage_effect(StorageEffect::Write {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key,
            value: value.into(),
            txn_id: Some(txn_id),
        })
        .await;
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(format!("migration progress was not stored: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::{FrameLayout, StoredFormat, StoredLayout};
    use std::collections::HashMap;
    use ulid::Ulid;

    const ZSTD: Compression = Compression::Zstd { level: 3 };

    fn location(framed: bool) -> BackendLocation {
        let mut format = StoredFormat::default();
        if framed {
            format.layout = StoredLayout::Frames(Box::new(FrameLayout {
                level: 3,
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
        operation.step(read_result(Some(bucket(ZSTD))));
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
        operation.step(read_result(Some(bucket(ZSTD))));
        operation.step(read_result(Some(version.to_bytes().unwrap())));
        let effects = operation.step(read_result(Some(location(true).to_bytes().unwrap())));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the row writes, got {effects:?}")
        };
        let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
        assert_eq!(spaces, [BLOB_VERSIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE]);
    }

    #[test]
    fn stale_setting_discards() {
        // The setting changed again while the copy was written: nothing is published
        // and the written copy goes back to the cleanup queue.
        let mut operation = operation();
        written(&mut operation, &raw_version());

        let effects = operation.step(read_result(Some(bucket(Compression::Off))));

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
    fn rejects_wrong_event() {
        let mut operation = operation();
        operation.start();

        operation.step(Event::Blob(BlobEvent::DeleteFinished));

        assert!(matches!(
            operation.finalize(),
            Err(MigrateVersionError::InvalidStateEvent { .. })
        ));
    }
}
