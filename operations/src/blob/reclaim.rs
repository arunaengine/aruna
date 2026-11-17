//! Deletes one blob copy that no alias references any more, after checking its owners.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, COPY_OWNER_KEYSPACE, PATHS_INDEX_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::backend_import::version_key;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion,
    CopyOwner, HashIndex, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::blob::cleanup_rows::schedule_cleanup_effect;
use crate::groups::backends::backend_key;
use crate::s3::usage::{StoredDelta, UsageCounterUpdate, UsageUpdateError};

/// What one candidate resolved to. `Dropped` covers every reason the queue row
/// is stale: retain, a vanished backend, or a location that is already gone.
/// `NotDue` keeps the row: the candidate or grace changed after the sweep read it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclaimVerdict {
    Freed { bytes: u64 },
    Pinned,
    Dropped,
    NotDue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReclaimState {
    ReadCandidate,
    FenceBackend,
    ReadLocation,
    ScanAliases,
    ReadVersions,
    ReadOwners,
    DeleteRows,
    QueueCleanup,
    UpdateUsage,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReclaimBlobError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error("reclaim failed")]
    Failed,
    #[error("indexed blob version is missing")]
    MissingOwner,
    #[error("reclaim storage failed: {0}")]
    Io(IoError),
}

impl From<IoError> for ReclaimBlobError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(_) | IoError::Conflict => Self::Storage(error.into()),
            other => Self::Io(other),
        }
    }
}

type Turn = Result<Next<ReclaimVerdict>, ReclaimBlobError>;

fn parsed<T>(
    value: Option<Value>,
    decode: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, ReclaimBlobError> {
    Ok(value.map(|value| decode(value.as_ref())).transpose()?)
}

/// Deletes one unreferenced copy. Everything that decides it happens inside one
/// transaction: the candidate, tenant fence, location and full alias scan, so
/// a concurrent write either loses its commit or pins the hash first.
#[derive(Debug, PartialEq)]
pub struct ReclaimBlobOperation {
    key: ReclaimCandidateKey,
    enqueued_at: SystemTime,
    sweep_time: SystemTime,
    state: ReclaimState,
    location: Option<BackendLocation>,
    next_alias_page: Option<Key>,
    usage_update: Option<UsageCounterUpdate>,
    /// The verdict the transaction commits with.
    verdict: Option<ReclaimVerdict>,
    /// Frees the copy whatever the strategy and without a queued candidate.
    forced: bool,
}

impl ReclaimBlobOperation {
    /// Frees a renamed file's old location: the rename replaced it, so no strategy keeps it.
    /// The transaction still proves that no version or archive reads it.
    pub fn forced(mut self) -> Self {
        self.forced = true;
        self
    }

    pub fn new(key: ReclaimCandidateKey, enqueued_at: SystemTime, sweep_time: SystemTime) -> Self {
        Self {
            key,
            enqueued_at,
            sweep_time,
            state: ReclaimState::ReadCandidate,
            location: None,
            next_alias_page: None,
            usage_update: None,
            verdict: None,
            forced: false,
        }
    }

    fn location_key(&self) -> BlobLocationKey {
        BlobLocationKey::new(self.key.blake3, self.key.encoding, self.key.backend.clone())
    }

    fn call(&mut self, state: ReclaimState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    /// Ends the candidate without touching the bytes: the queue row goes, the
    /// location row stays.
    fn drop_candidate(&mut self, verdict: ReclaimVerdict) -> Turn {
        self.verdict = Some(verdict);
        let delete = vec![(
            BLOB_RECLAIM_KEYSPACE.to_string(),
            self.key.to_bytes().into(),
        )];
        self.call(ReclaimState::DeleteRows, IoCall::Delete(delete))
    }

    fn handle_candidate(&mut self, value: Option<Value>) -> Turn {
        match parsed(value, ReclaimCandidate::from_bytes)? {
            _ if self.forced => {}
            None => return self.drop_candidate(ReclaimVerdict::Dropped),
            // The candidate changed after the sweep read it: it is simply not due yet.
            Some(candidate) if candidate.enqueued_at != self.enqueued_at => {
                return Ok(Next::Done(ReclaimVerdict::NotDue));
            }
            Some(_) => {}
        }
        match self.key.backend {
            BackendRef::Group(backend_id) => {
                let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(backend_id));
                self.call(ReclaimState::FenceBackend, read)
            }
            BackendRef::Node(_) => self.read_location(),
        }
    }

    /// Re-checks the tenant's strategy under the transaction, so a flip to
    /// retain conflicts with this sweep instead of racing it. A grace the tenant
    /// lengthened since the sweep read it makes the candidate not due again.
    fn handle_fence(&mut self, value: Option<Value>) -> Turn {
        let Some(record) = parsed(value, GroupStorage::from_bytes)? else {
            return self.drop_candidate(ReclaimVerdict::Dropped);
        };
        if self.forced {
            return self.read_location();
        }
        let Some(after) = record.cleanup.grace() else {
            return self.drop_candidate(ReclaimVerdict::Dropped);
        };
        match self.enqueued_at.checked_add(after) {
            Some(due) if due <= self.sweep_time => self.read_location(),
            _ => Ok(Next::Done(ReclaimVerdict::NotDue)),
        }
    }

    fn read_location(&mut self) -> Turn {
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, self.location_key().to_bytes());
        self.call(ReclaimState::ReadLocation, read)
    }

    fn handle_location(&mut self, value: Option<Value>) -> Turn {
        let Some(location) = parsed(value, BackendLocation::from_bytes)? else {
            return self.drop_candidate(ReclaimVerdict::Dropped);
        };
        // Staged and partial copies belong to the hidden sweep and never
        // credited the stored counters, so debiting them would underflow.
        if location.staging || location.partial {
            return self.drop_candidate(ReclaimVerdict::Dropped);
        }
        self.location = Some(location);
        self.scan_aliases(None)
    }

    fn scan_aliases(&mut self, start: Option<Key>) -> Turn {
        let page = IoCall::Page {
            space: PATHS_INDEX_KEYSPACE.to_string(),
            prefix: HashIndex::hash_prefix(&self.key.blake3)?.into(),
            after: start,
            limit: usize::MAX,
        };
        self.call(ReclaimState::ScanAliases, page)
    }

    fn handle_alias_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Turn {
        self.next_alias_page = next;
        let reads = values
            .iter()
            .map(|(key, _)| {
                let alias = HashIndex::from_bytes(key.as_ref())?;
                let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
                Ok((
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    version.to_bytes()?.into(),
                ))
            })
            .collect::<Result<Vec<_>, ConversionError>>()?;
        if reads.is_empty() {
            return self.continue_scan();
        }
        self.call(ReclaimState::ReadVersions, IoCall::Read(reads))
    }

    /// A version naming this exact copy pins it. Missing or undecodable indexed
    /// versions fail the sweep closed and leave the candidate queued.
    fn handle_versions(&mut self, values: Vec<Option<Value>>) -> Turn {
        let wanted = self.location_key();
        for value in values {
            let value = value.ok_or(ReclaimBlobError::MissingOwner)?;
            let version = BlobVersion::from_bytes(value.as_ref())?;
            if version.location_key() == Some(wanted.clone()) {
                return self.drop_candidate(ReclaimVerdict::Pinned);
            }
        }
        self.continue_scan()
    }

    fn continue_scan(&mut self) -> Turn {
        match self.next_alias_page.take() {
            Some(start) => self.scan_aliases(Some(start)),
            None => self.read_owners(),
        }
    }

    /// Versions still pending or mid-promotion own a Pithos archive without a hash alias.
    fn read_owners(&mut self) -> Turn {
        let location = self.location.as_ref().ok_or(ReclaimBlobError::Failed)?;
        let archive = ArchiveKey::of(location);
        let page = IoCall::Page {
            space: COPY_OWNER_KEYSPACE.to_string(),
            prefix: CopyOwner::prefix(&archive).into(),
            after: None,
            limit: 2,
        };
        self.call(ReclaimState::ReadOwners, page)
    }

    /// The free path: location row, queue row, physical delete and the counter
    /// debit all commit together, so no window shows the copy as unreferenced
    /// while its bytes are neither queued for deletion nor still charged.
    fn free_copy(&mut self) -> Turn {
        let location = self.location.clone().ok_or(ReclaimBlobError::Failed)?;
        self.verdict = Some(ReclaimVerdict::Freed {
            bytes: location.stored_size(),
        });
        let mut deletes = vec![
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                self.location_key().to_bytes().into(),
            ),
            (
                BLOB_RECLAIM_KEYSPACE.to_string(),
                self.key.to_bytes().into(),
            ),
        ];
        if location.is_external() {
            let key = version_key(location.ulid);
            deletes.push((BACKEND_VERSION_KEYSPACE.to_string(), key));
        }
        self.call(ReclaimState::DeleteRows, IoCall::Delete(deletes))
    }

    fn freed(&self) -> bool {
        matches!(self.verdict, Some(ReclaimVerdict::Freed { .. }))
    }

    fn handle_rows_deleted(&mut self) -> Turn {
        let Some(location) = self.location.clone().filter(|_| self.freed()) else {
            return self.commit();
        };
        // Bytes Aruna found on a tenant backend are never deleted; only the row goes.
        if location.is_external() {
            return self.debit_usage();
        }
        let work = (BlobCleanupWork::DeleteBlob { location }).to_bytes()?;
        let row = (
            BLOB_CLEANUP_KEYSPACE.to_string(),
            Ulid::generate().to_bytes().to_vec().into(),
            work.into(),
        );
        self.call(ReclaimState::QueueCleanup, IoCall::Write(vec![row]))
    }

    fn debit_usage(&mut self) -> Turn {
        let location = self.location.as_ref().ok_or(ReclaimBlobError::Failed)?;
        let bytes = -i128::from(location.stored_size());
        let stored = StoredDelta::of_copy(location, -1, bytes).unwrap_or_else(|| {
            StoredDelta::new(self.key.blake3, self.key.backend.clone(), -1, bytes)
        });
        let mut update = UsageCounterUpdate::for_stored(stored);
        if update.is_noop() {
            return self.commit();
        }
        let call = update.start_calls().into_call();
        self.usage_update = Some(update);
        self.call(ReclaimState::UpdateUsage, call)
    }

    fn handle_usage(&mut self, result: IoResult) -> Turn {
        let update = self.usage_update.as_mut().ok_or(ReclaimBlobError::Failed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.commit(),
        }
    }

    /// A freed copy wakes the cleanup queue once the transaction commits.
    fn commit(&mut self) -> Turn {
        let verdict = self.verdict.ok_or(ReclaimBlobError::Failed)?;
        let calls = match self.freed() {
            true => vec![schedule_cleanup_effect().into()],
            false => Vec::new(),
        };
        Ok(Next::Then {
            output: verdict,
            calls,
        })
    }
}

impl Operation for ReclaimBlobOperation {
    type Output = ReclaimVerdict;
    type Error = ReclaimBlobError;

    fn first(&mut self) -> Turn {
        let read = read_row(BLOB_RECLAIM_KEYSPACE, self.key.to_bytes());
        self.call(ReclaimState::ReadCandidate, read)
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            ReclaimState::ReadCandidate => self.handle_candidate(result.value()?),
            ReclaimState::FenceBackend => self.handle_fence(result.value()?),
            ReclaimState::ReadLocation => self.handle_location(result.value()?),
            ReclaimState::ScanAliases => {
                let (rows, next) = result.page()?;
                self.handle_alias_page(rows, next)
            }
            ReclaimState::ReadVersions => self.handle_versions(result.values()?),
            ReclaimState::ReadOwners => match result.page()?.0.is_empty() {
                true => self.free_copy(),
                false => self.drop_candidate(ReclaimVerdict::Pinned),
            },
            ReclaimState::DeleteRows => {
                result.done()?;
                self.handle_rows_deleted()
            }
            ReclaimState::QueueCleanup => {
                result.done()?;
                self.debit_usage()
            }
            ReclaimState::UpdateUsage => self.handle_usage(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use aruna_core::structs::storage::cleanup::CleanupStrategy;

    use crate::blob::reclaim_sweep::{RECLAIM_TICK_LIMIT, backend_status, sweep_at};
    use crate::driver::{DriverContext, drive};
    use crate::storage_read::iter_prefix_page;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
    use aruna_core::operation::{EffectOperation, Runnable};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::CopyOwner;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::usage::{UsageCounters, usage_backend_key, usage_hash_key};
    use aruna_core::types::Value;
    use std::collections::HashMap;
    use tempfile::tempdir;

    const HASH: [u8; 32] = [4u8; 32];

    fn answer(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    /// The key space of the first row a step reads.
    fn space(turn: &Turn) -> &str {
        match turn {
            Ok(Next::Call(IoCall::Read(rows))) => &rows[0].0,
            other => panic!("expected a read, got {other:?}"),
        }
    }

    /// Long past its grace, so only the case under test decides the verdict.
    fn reclaim_op(key: ReclaimCandidateKey) -> ReclaimBlobOperation {
        ReclaimBlobOperation::new(key, SystemTime::UNIX_EPOCH, SystemTime::now())
    }

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    fn location(size: u64) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: "bucket/key_01".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: HashMap::from([(
                aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
                HASH.to_vec(),
            )]),
        }
    }

    fn candidate_key() -> ReclaimCandidateKey {
        ReclaimCandidateKey::new(BackendRef::node_default(), EncodingClass::Raw, HASH)
    }

    async fn write(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read(context: &DriverContext, key_space: &str, key: Vec<u8>) -> Option<Value> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    /// Location row, queue row and the counters a write would have credited.
    async fn seed(context: &DriverContext, size: u64) {
        write(
            context,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default()).to_bytes(),
            location(size).to_bytes().unwrap(),
        )
        .await;
        write(
            context,
            BLOB_RECLAIM_KEYSPACE,
            candidate_key().to_bytes(),
            ReclaimCandidate {
                enqueued_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
        let counters = UsageCounters {
            stored_blobs: 1,
            stored_bytes: size,
            ..Default::default()
        };
        write(
            context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_hash_key(&HASH),
            counters.to_bytes().unwrap(),
        )
        .await;
        write(
            context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_backend_key(&BackendRef::node_default(), shard_of_hash()),
            UsageCounters {
                stored_bytes: size,
                ..Default::default()
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    fn shard_of_hash() -> usize {
        aruna_core::structs::storage::usage::shard_for_hash(&HASH)
    }

    async fn add_alias(context: &DriverContext, version_id: Ulid, pins: bool) {
        let alias = HashIndex::new(
            HASH,
            version_id,
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_bytes([2u8; 16]),
            iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            "bucket",
            "key",
        );
        write(
            context,
            PATHS_INDEX_KEYSPACE,
            alias.to_bytes().unwrap(),
            Vec::new(),
        )
        .await;
        let backend = if pins {
            BackendRef::node_default()
        } else {
            BackendRef::Node("cold".to_string())
        };
        write(
            context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket", "key", version_id)
                .to_bytes()
                .unwrap(),
            BlobVersion::materialized(
                HASH,
                backend,
                EncodingClass::Raw,
                SystemTime::UNIX_EPOCH,
                Default::default(),
                None,
            )
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn frees_unreferenced_copy() {
        // The location row, the queue row, the counters and the queued physical
        // delete all move in one transaction.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Freed { bytes: 10 });
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes()
            )
            .await
            .is_none()
        );
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_none()
        );
        let counters = read(
            &context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_hash_key(&HASH),
        )
        .await
        .unwrap();
        assert_eq!(
            UsageCounters::from_bytes(counters.as_ref()).unwrap(),
            UsageCounters::default()
        );
        let (queued, _) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        )
        .await
        .unwrap();
        assert_eq!(queued.len(), 1);
    }

    #[tokio::test]
    async fn archive_owner_pins() {
        // A pending or mid-promotion version owns the archive without any hash alias.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let version = VersionKey::new("bucket", "key", Ulid::from_bytes([6u8; 16]));
        let owner = CopyOwner::new(ArchiveKey::of(&location(10)), version);
        write(
            &context,
            aruna_core::keyspaces::COPY_OWNER_KEYSPACE,
            owner.key().unwrap(),
            Vec::new(),
        )
        .await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Pinned);
        let location_key =
            BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default());
        assert!(
            read(&context, BLOB_LOCATIONS_KEYSPACE, location_key.to_bytes())
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn pinned_copy_survives() {
        // An alias whose version still names this backend cancels the candidate.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        add_alias(&context, Ulid::from_bytes([6u8; 16]), true).await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Pinned);
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes()
            )
            .await
            .is_some()
        );
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn refreshed_candidate_waits() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let refreshed = ReclaimCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH + Duration::from_secs(3_600),
        };
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            candidate_key().to_bytes(),
            refreshed.to_bytes().unwrap(),
        )
        .await;
        let sweep_time = SystemTime::UNIX_EPOCH
            + CleanupStrategy::DEFAULT_RECLAIM_AFTER
            + Duration::from_secs(1);

        let verdict = drive(
            ReclaimBlobOperation::new(candidate_key(), SystemTime::UNIX_EPOCH, sweep_time),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(verdict, ReclaimVerdict::NotDue);
        assert_eq!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes()).await,
            Some(refreshed.to_bytes().unwrap().into())
        );
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes(),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn missing_owner_blocks() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let version_id = Ulid::from_bytes([6u8; 16]);
        add_alias(&context, version_id, true).await;
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "key", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));

        assert_eq!(
            drive(reclaim_op(candidate_key()), &context).await,
            Err(ReclaimBlobError::MissingOwner)
        );
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_some()
        );
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes(),
            )
            .await
            .is_some()
        );
        let (queued, _) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        )
        .await
        .unwrap();
        assert!(queued.is_empty());
    }

    #[tokio::test]
    async fn corrupt_owner_blocks() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let version_id = Ulid::from_bytes([6u8; 16]);
        add_alias(&context, version_id, true).await;
        write(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket", "key", version_id)
                .to_bytes()
                .unwrap(),
            vec![255],
        )
        .await;

        assert!(drive(reclaim_op(candidate_key()), &context).await.is_err());
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_some()
        );
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes(),
            )
            .await
            .is_some()
        );
    }

    #[tokio::test]
    async fn retained_states_survive() {
        use aruna_core::keyspaces::MANAGED_COPY_KEYSPACE;
        use aruna_core::structs::storage::blob::{
            ManagedCopyQuarantine, ManagedCopyRecord, ManagedCopyState,
        };
        for state in [
            ManagedCopyState::Registered,
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::Rejoin),
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::SubjectTransition),
            ManagedCopyState::Quarantined(ManagedCopyQuarantine::PolicyViolation),
            ManagedCopyState::UnresolvedDeparted,
        ] {
            let dir = tempdir().unwrap();
            let context = context(dir.path().to_str().unwrap());
            seed(&context, 10).await;
            let version_id = Ulid::from_bytes([6u8; 16]);
            add_alias(&context, version_id, true).await;
            let copy = ManagedCopyRecord::new(
                VersionKey::new("bucket", "key", version_id),
                iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
                location(10),
                Vec::new(),
                0,
                state,
            )
            .unwrap();
            let key = copy.key().to_bytes().unwrap();
            let value = copy.to_bytes().unwrap();
            write(&context, MANAGED_COPY_KEYSPACE, key.clone(), value.clone()).await;

            assert_eq!(
                drive(reclaim_op(candidate_key()), &context).await.unwrap(),
                ReclaimVerdict::Pinned
            );
            assert_eq!(
                read(&context, MANAGED_COPY_KEYSPACE, key).await,
                Some(value.into())
            );
            assert!(
                read(
                    &context,
                    BLOB_LOCATIONS_KEYSPACE,
                    BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                        .to_bytes(),
                )
                .await
                .is_some()
            );
        }
    }

    #[tokio::test]
    async fn cleanup_survives_restart() {
        let dir = tempdir().unwrap();
        let before = context(dir.path().to_str().unwrap());
        seed(&before, 10).await;
        assert_eq!(
            drive(reclaim_op(candidate_key()), &before).await.unwrap(),
            ReclaimVerdict::Freed { bytes: 10 }
        );
        before.storage_handle.close().await;

        let after = context(dir.path().to_str().unwrap());
        assert_eq!(
            drive(reclaim_op(candidate_key()), &after).await.unwrap(),
            ReclaimVerdict::Dropped
        );
        let counters = read(
            &after,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_hash_key(&HASH),
        )
        .await
        .unwrap();
        assert_eq!(
            UsageCounters::from_bytes(&counters).unwrap(),
            UsageCounters::default()
        );
        let outcome = crate::blob::cleanup::process_cleanup_batch(&after)
            .await
            .unwrap();
        assert_eq!(outcome.failed, 1);
        let (queued, _) = iter_prefix_page(
            &after.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        )
        .await
        .unwrap();
        assert_eq!(queued.len(), 1);
        assert!(matches!(BlobCleanupWork::from_bytes(&queued[0].1).unwrap(),
            BlobCleanupWork::DeleteBlob { location: queued } if queued == location(10)));
    }

    #[tokio::test]
    async fn refresh_conflicts_commit() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let mut operation = reclaim_op(candidate_key()).into_machine();
        let mut effects = std::collections::VecDeque::from_iter(operation.start());
        let mut refreshed = false;
        while !operation.is_complete() {
            let effect = effects.pop_front().expect("reclaim must make progress");
            if matches!(
                effect,
                Effect::Storage(StorageEffect::CommitTransaction { .. })
            ) {
                assert!(!refreshed);
                refreshed = true;
                write(
                    &context,
                    BLOB_RECLAIM_KEYSPACE,
                    candidate_key().to_bytes(),
                    ReclaimCandidate {
                        enqueued_at: SystemTime::UNIX_EPOCH + Duration::from_secs(3_600),
                    }
                    .to_bytes()
                    .unwrap(),
                )
                .await;
            }
            let event = context.storage_handle.send_effect(effect).await;
            effects.extend(operation.step(event));
        }
        assert!(refreshed);
        assert_eq!(
            operation.finalize(),
            Err(ReclaimBlobError::Storage(StorageError::TransactionConflict))
        );
        let (queued, _) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        )
        .await
        .unwrap();
        assert!(queued.is_empty());
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, EncodingClass::Raw, BackendRef::node_default())
                    .to_bytes(),
            )
            .await
            .is_some()
        );
        let counters = read(
            &context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_hash_key(&HASH),
        )
        .await
        .unwrap();
        assert_eq!(
            UsageCounters::from_bytes(&counters).unwrap().stored_bytes,
            10
        );

        let due = SystemTime::UNIX_EPOCH + CleanupStrategy::DEFAULT_RECLAIM_AFTER * 2;
        assert_eq!(sweep_at(&context, due, None).await.unwrap().freed, 1);
    }

    #[tokio::test]
    async fn other_never_pins() {
        // Deduplication is per backend, so a copy elsewhere holds nothing here.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        add_alias(&context, Ulid::from_bytes([7u8; 16]), false).await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Freed { bytes: 10 });
    }

    #[tokio::test]
    async fn missing_location_drops() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            candidate_key().to_bytes(),
            ReclaimCandidate {
                enqueued_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Dropped);
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn grace_defers_candidate() {
        // Inside the grace window the sweep leaves both rows alone.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;

        let outcome = sweep_at(&context, SystemTime::UNIX_EPOCH, None)
            .await
            .unwrap();

        assert_eq!(outcome.not_due, 1);
        assert_eq!(outcome.freed, 0);
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn reclaim_uses_enqueue() {
        // A candidate written after a long apply carries its enqueue time, so a
        // sweep past the operation-start grace but not the enqueue grace must
        // defer it rather than free the bytes early.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let started_at = SystemTime::UNIX_EPOCH;
        let enqueued_at = started_at + Duration::from_secs(6 * 60 * 60);
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            candidate_key().to_bytes(),
            ReclaimCandidate { enqueued_at }.to_bytes().unwrap(),
        )
        .await;

        let started_grace =
            started_at + CleanupStrategy::DEFAULT_RECLAIM_AFTER + Duration::from_secs(1);
        let outcome = sweep_at(&context, started_grace, None).await.unwrap();

        assert_eq!(outcome.not_due, 1);
        assert_eq!(outcome.freed, 0);
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_some()
        );

        let enqueued_grace =
            enqueued_at + CleanupStrategy::DEFAULT_RECLAIM_AFTER + Duration::from_secs(1);
        let outcome = sweep_at(&context, enqueued_grace, None).await.unwrap();

        assert_eq!(outcome.freed, 1);
        assert_eq!(outcome.freed_bytes, 10);
    }

    #[tokio::test]
    async fn sweep_frees_due() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let due = SystemTime::UNIX_EPOCH
            + CleanupStrategy::DEFAULT_RECLAIM_AFTER
            + Duration::from_secs(1);

        let outcome = sweep_at(&context, due, None).await.unwrap();

        assert_eq!(outcome.freed, 1);
        assert_eq!(outcome.freed_bytes, 10);
    }

    #[tokio::test]
    async fn sweep_caps_tick() {
        // A long queue must hand the task loop back instead of draining in one
        // tick; the rows left over are picked up by the retry sweep.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        for index in 0..=RECLAIM_TICK_LIMIT {
            let mut blake3 = [0u8; 32];
            blake3[..8].copy_from_slice(&(index as u64).to_be_bytes());
            write(
                &context,
                BLOB_RECLAIM_KEYSPACE,
                ReclaimCandidateKey::new(BackendRef::node_default(), EncodingClass::Raw, blake3)
                    .to_bytes(),
                ReclaimCandidate {
                    enqueued_at: SystemTime::UNIX_EPOCH,
                }
                .to_bytes()
                .unwrap(),
            )
            .await;
        }
        let due = SystemTime::UNIX_EPOCH
            + CleanupStrategy::DEFAULT_RECLAIM_AFTER
            + Duration::from_secs(1);

        let outcome = sweep_at(&context, due, None).await.unwrap();

        assert!(outcome.capped);
        assert_eq!(outcome.dropped, RECLAIM_TICK_LIMIT);

        // The cursor resumes past the capped tick, so the tail is not starved.
        let resumed = sweep_at(&context, due, outcome.next_start_after)
            .await
            .unwrap();

        assert!(!resumed.capped);
        assert_eq!(resumed.dropped, 1);
        assert!(resumed.next_start_after.is_none());
    }

    #[test]
    fn fence_drops_retain() {
        // A tenant flip to retain is read inside the transaction and stands down.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let mut operation = reclaim_op(ReclaimCandidateKey::new(
            BackendRef::Group(backend_id),
            EncodingClass::Raw,
            HASH,
        ));
        operation.first().unwrap();
        let candidate = ReclaimCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH,
        };
        operation
            .next(answer(Some(candidate.to_bytes().unwrap())))
            .unwrap();

        let turn = operation.next(answer(Some(group_record(
            backend_id,
            CleanupStrategy::Retain,
        ))));

        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("expected the queue row to be dropped, got {turn:?}")
        };
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].0, BLOB_RECLAIM_KEYSPACE);
    }

    #[test]
    fn forced_ignores_retain() {
        // A renamed file's old location goes under retain too, also without a queued candidate.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let key = ReclaimCandidateKey::new(BackendRef::Group(backend_id), EncodingClass::Raw, HASH);
        let mut operation = reclaim_op(key).forced();
        operation.first().unwrap();
        let turn = operation.next(answer(None));
        assert_eq!(space(&turn), STORAGE_BACKEND_KEYSPACE);
        let turn = operation.next(answer(Some(group_record(
            backend_id,
            CleanupStrategy::Retain,
        ))));
        // The location is read next, so the alias scan still decides.
        assert_eq!(space(&turn), BLOB_LOCATIONS_KEYSPACE);
    }

    #[test]
    fn fence_keeps_row() {
        // A grace the tenant lengthened after the sweep read it must leave the
        // queue row alone rather than delete the copy under the old value.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let enqueued_at = SystemTime::UNIX_EPOCH;
        let sweep_time = enqueued_at + Duration::from_secs(60);
        let mut operation = ReclaimBlobOperation::new(
            ReclaimCandidateKey::new(BackendRef::Group(backend_id), EncodingClass::Raw, HASH),
            enqueued_at,
            sweep_time,
        );
        operation.first().unwrap();
        let candidate = ReclaimCandidate { enqueued_at };
        operation
            .next(answer(Some(candidate.to_bytes().unwrap())))
            .unwrap();
        let grace = CleanupStrategy::Reclaim {
            after: Duration::from_secs(3_600),
        };

        let turn = operation.next(answer(Some(group_record(backend_id, grace))));

        assert!(matches!(turn, Ok(Next::Done(ReclaimVerdict::NotDue))));
    }

    fn group_record(backend_id: Ulid, cleanup: CleanupStrategy) -> Vec<u8> {
        GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([2u8; 16]),
            name: "tenant".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup,
        }
        .to_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn status_counts_backend() {
        // Another backend's queue rows must not show up in this one's depth.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            ReclaimCandidateKey::new(
                BackendRef::Node("cold".to_string()),
                EncodingClass::Raw,
                [9u8; 32],
            )
            .to_bytes(),
            ReclaimCandidate {
                enqueued_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let status = backend_status(&context, &BackendRef::node_default())
            .await
            .unwrap();

        assert_eq!(status.pending_candidates, 1);
        assert_eq!(status.queued_cleanups, 0);
        assert_eq!(status.oldest_enqueued_at, Some(SystemTime::UNIX_EPOCH));
        assert!(!status.truncated);
    }

    #[tokio::test]
    async fn status_dates_cleanups() {
        // A stalled physical delete outlives its candidate row, so the queue
        // time has to come from the cleanup key itself.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        let queued = Ulid::from_datetime(SystemTime::UNIX_EPOCH + Duration::from_secs(120));
        write(
            &context,
            BLOB_CLEANUP_KEYSPACE,
            queued.to_bytes().to_vec(),
            BlobCleanupWork::DeleteBlob {
                location: location(10),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let status = backend_status(&context, &BackendRef::node_default())
            .await
            .unwrap();

        assert_eq!(status.pending_candidates, 0);
        assert_eq!(status.queued_cleanups, 1);
        assert_eq!(status.oldest_enqueued_at, Some(queued.datetime()));
    }

    #[test]
    fn rejects_stray_event() {
        // A write acknowledgement is not a transaction start.
        let mut operation = reclaim_op(candidate_key());
        operation.first().unwrap();

        let turn = operation.next(IoResult::Done);

        assert!(matches!(turn, Err(ReclaimBlobError::Io(_))));
    }

    #[tokio::test]
    async fn keeps_external_bytes() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        let backend_id = Ulid::from_bytes([7u8; 16]);
        let backend = BackendRef::Group(backend_id);
        let record = GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([8u8; 16]),
            name: "tenant".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: CleanupStrategy::Reclaim {
                after: Duration::from_secs(60),
            },
        };
        write(
            &context,
            STORAGE_BACKEND_KEYSPACE,
            backend_id.to_bytes().to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;
        let imported = BackendLocation {
            backend: backend.clone(),
            storage_bucket: String::new(),
            backend_path: "raw/a.csv".to_string(),
            ..location(10)
        };
        let location_key = BlobLocationKey::new(HASH, EncodingClass::Raw, backend.clone());
        write(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            location_key.to_bytes(),
            imported.to_bytes().unwrap(),
        )
        .await;
        let backend_version = version_key(imported.ulid).to_vec();
        write(
            &context,
            BACKEND_VERSION_KEYSPACE,
            backend_version.clone(),
            b"v1".to_vec(),
        )
        .await;
        let candidate = ReclaimCandidateKey::new(backend, EncodingClass::Raw, HASH);
        let queued = ReclaimCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH,
        };
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            candidate.to_bytes(),
            queued.to_bytes().unwrap(),
        )
        .await;

        assert_eq!(
            drive(reclaim_op(candidate), &context).await.unwrap(),
            ReclaimVerdict::Freed { bytes: 10 }
        );
        assert!(
            read(&context, BLOB_LOCATIONS_KEYSPACE, location_key.to_bytes())
                .await
                .is_none()
        );
        let version = read(&context, BACKEND_VERSION_KEYSPACE, backend_version).await;
        assert!(version.is_none());
        let (cleanups, _) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        )
        .await
        .unwrap();
        assert!(
            cleanups.is_empty(),
            "no delete is queued for imported bytes"
        );
    }
}
