//! Records the content hash of a pending Pithos archive after unlock and promotes its versions.
//! Each owner page commits in its own transaction; the pending row goes only after the last page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};

use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, COPY_OWNER_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobLocationKey, BlobVersion, BlobVersionState, BucketInfo,
    CopyOwner,
};
use aruna_core::structs::storage::encryption::{BucketKeyError, BucketKeyRef};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

use crate::blob::records::{HeadAliasContext, add_index_effect};
use crate::replication::dht_registration::dht_registration_effect;

/// Owner rows promoted per transaction.
pub const PROMOTE_PAGE: usize = 64;

#[derive(Debug, Error, PartialEq)]
pub enum PromoteError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("pending archive has no bucket key")]
    NoKey,
    #[error("verified read returned no BLAKE3 or a different size")]
    BadHashes,
    #[error("State [{state}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub enum Promotion {
    /// Every pending alias now names the content hash.
    Promoted { blake3: [u8; 32], versions: usize },
    /// The key generation is locked; the work waits for unlock.
    AwaitingKey(BucketKeyRef),
    /// No pending row names this archive any longer.
    Gone,
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    ReadPending,
    Admit,
    Hash,
    StartTransaction,
    Reread,
    WriteLocation,
    ScanOwners,
    ReadVersions,
    ReadBuckets,
    WriteVersions,
    DeletePending,
    Commit,
    Abort,
    Register,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PromotePendingOperation {
    archive: ArchiveKey,
    realm_id: RealmId,
    node_id: NodeId,
    limits: RoCrateLimits,
    state: State,
    txn_id: Option<TxnId>,
    location: Option<BackendLocation>,
    blake3: Option<[u8; 32]>,
    hashes: HashMap<String, Vec<u8>>,
    cursor: Option<Key>,
    last_page: bool,
    pending: Vec<(Key, BlobVersion)>,
    promoted: usize,
    output: Option<Result<Promotion, PromoteError>>,
}

impl PromotePendingOperation {
    pub fn new(
        archive: ArchiveKey,
        realm_id: RealmId,
        node_id: NodeId,
        limits: RoCrateLimits,
    ) -> Self {
        Self {
            archive,
            realm_id,
            node_id,
            limits,
            state: State::Init,
            txn_id: None,
            location: None,
            blake3: None,
            hashes: HashMap::new(),
            cursor: None,
            last_page: false,
            pending: Vec::new(),
            promoted: 0,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Promotion, PromoteError>) -> Effects {
        if output.is_err() && self.txn_id.is_some() {
            self.output = Some(output);
            self.state = State::Abort;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: self.txn_id.take().unwrap_or_default(),
            })];
        }
        self.state = match output {
            Ok(_) => State::Finish,
            Err(_) => State::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn unexpected(&mut self, state: &'static str, expected: &'static str, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.finish(Err(error.into()));
        }
        if let Event::Blob(BlobEvent::Error(error)) = event {
            return self.finish(Err(error.into()));
        }
        self.finish(Err(PromoteError::InvalidStateEvent {
            state,
            expected,
            received: event,
        }))
    }

    fn read_pending(&mut self, state: State, txn_id: Option<TxnId>) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: PENDING_LOCATION_KEYSPACE.to_string(),
            key: self.archive.to_bytes().into(),
            txn_id,
        })]
    }

    fn handle_pending(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("ReadPending", "ReadResult", event);
        };
        let Some(value) = value else {
            return self.finish(Ok(Promotion::Gone));
        };
        let location = match BackendLocation::from_bytes(&value) {
            Ok(location) => location,
            Err(error) => return self.finish(Err(error.into())),
        };
        let Some(key) = location.format.bucket_key() else {
            return self.finish(Err(PromoteError::NoKey));
        };
        self.location = Some(location);
        self.state = State::Admit;
        smallvec![Effect::Blob(BlobEffect::AdmitRead {
            key,
            archive: self.archive.clone(),
        })]
    }

    fn handle_admit(&mut self, event: Event) -> Effects {
        let Some(location) = self.location.clone() else {
            return self.finish(Err(PromoteError::NoKey));
        };
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease }) => {
                self.state = State::Hash;
                smallvec![Effect::Blob(BlobEffect::HashArchive { location, lease })]
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                let key = location.format.bucket_key();
                self.finish(key.map(Promotion::AwaitingKey).ok_or(PromoteError::NoKey))
            }
            event => self.unexpected("Admit", "ReadAdmitted", event),
        }
    }

    fn handle_hash(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ArchiveHashed { hashes, size }) = event else {
            return self.unexpected("Hash", "ArchiveHashed", event);
        };
        let blake3 = hashes
            .get(HASH_BLAKE3)
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
        let expected = self.location.as_ref().map(|location| location.blob_size);
        let Some(blake3) = blake3.filter(|_| Some(size) == expected) else {
            return self.finish(Err(PromoteError::BadHashes));
        };
        self.blake3 = Some(blake3);
        self.hashes = hashes;
        self.start_page()
    }

    fn start_page(&mut self) -> Effects {
        self.state = State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("StartTransaction", "TransactionStarted", event);
        };
        self.txn_id = Some(txn_id);
        self.read_pending(State::Reread, Some(txn_id))
    }

    /// The hashes belong to the exact archive and metadata digest that was read.
    fn handle_reread(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("Reread", "ReadResult", event);
        };
        let current = value.map(|value| BackendLocation::from_bytes(&value));
        let location = match (current, self.location.clone()) {
            (Some(Ok(current)), Some(read)) if current == read => read,
            (Some(Err(error)), _) => return self.finish(Err(error.into())),
            _ => {
                self.output = Some(Ok(Promotion::Gone));
                self.state = State::Abort;
                return smallvec![Effect::Storage(StorageEffect::AbortTransaction {
                    txn_id: self.txn_id.take().unwrap_or_default(),
                })];
            }
        };
        let mut known = location;
        known.hashes.extend(self.hashes.clone());
        let key = BlobLocationKey::new(
            self.blake3.unwrap_or_default(),
            known.format.encoding(),
            known.backend.clone(),
        );
        let value = match known.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.finish(Err(error.into())),
        };
        self.state = State::WriteLocation;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: key.to_bytes().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn scan_owners(&mut self) -> Effects {
        self.state = State::ScanOwners;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: COPY_OWNER_KEYSPACE.to_string(),
            prefix: Some(CopyOwner::prefix(&self.archive).into()),
            start: self.cursor.clone().map(IterStart::After),
            limit: PROMOTE_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn handle_owners(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.unexpected("ScanOwners", "IterResult", event);
        };
        self.last_page = values.len() < PROMOTE_PAGE;
        self.cursor = values
            .last()
            .map(|(key, _)| key.clone())
            .or(self.cursor.take());
        let reads = values
            .iter()
            .map(|(key, _)| {
                let owner = CopyOwner::from_key(key)?;
                Ok((
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    owner.version.to_bytes()?.into(),
                ))
            })
            .collect::<Result<Vec<_>, ConversionError>>();
        match reads {
            Ok(reads) if reads.is_empty() => self.finish_pages(),
            Ok(reads) => {
                self.state = State::ReadVersions;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.finish(Err(error.into())),
        }
    }

    /// Keeps the versions that still pend on this archive; promoted ones are already done.
    fn handle_versions(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("ReadVersions", "BatchReadResult", event);
        };
        self.pending.clear();
        for (key, value) in values {
            let Some(value) = value else { continue };
            let version = match BlobVersion::from_bytes(&value) {
                Ok(version) => version,
                Err(error) => return self.finish(Err(error.into())),
            };
            if version.state.pending_archive() == Some(&self.archive) {
                self.pending.push((key, version));
            }
        }
        let buckets: BTreeSet<String> = self
            .pending
            .iter()
            .filter_map(|(key, _)| {
                aruna_core::structs::storage::blob::VersionKey::from_bytes(key).ok()
            })
            .map(|key| key.bucket)
            .collect();
        if buckets.is_empty() {
            return self.next_page();
        }
        self.state = State::ReadBuckets;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: buckets
                .into_iter()
                .map(|bucket| (S3_BUCKET_KEYSPACE.to_string(), bucket.into_bytes().into()))
                .collect(),
            txn_id: self.txn_id,
        })]
    }

    /// Rewrites each pending version in place: VersionId, creation time, size and metadata stay.
    fn handle_buckets(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("ReadBuckets", "BatchReadResult", event);
        };
        match self.promote_page(values) {
            Ok(effects) => {
                self.state = State::WriteVersions;
                effects
            }
            Err(error) => self.finish(Err(error)),
        }
    }

    fn promote_page(
        &mut self,
        buckets: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, PromoteError> {
        let mut groups = HashMap::new();
        for (key, value) in buckets {
            let bucket = String::from_utf8(key.to_vec()).map_err(ConversionError::from)?;
            if let Some(value) = value {
                groups.insert(bucket, BucketInfo::from_bytes(&value)?.group_id);
            }
        }
        let blake3 = self.blake3.unwrap_or_default();
        let location = self.location.as_ref().ok_or(PromoteError::NoKey)?;
        let mut writes = Vec::new();
        for (key, mut version) in std::mem::take(&mut self.pending) {
            let BlobVersionState::PendingContent { source, .. } = version.state else {
                continue;
            };
            let version_key = aruna_core::structs::storage::blob::VersionKey::from_bytes(&key)?;
            version.state = BlobVersionState::Materialized {
                blob_hash: blake3,
                backend: location.backend.clone(),
                encoding: location.format.encoding(),
                source,
            };
            writes.push((
                BLOB_VERSIONS_KEYSPACE.to_string(),
                key,
                version.to_bytes()?.into(),
            ));
            // A bucket that is gone keeps its promoted version but gets no hash alias.
            if let Some(group_id) = groups.get(&version_key.bucket) {
                let alias = HeadAliasContext::new(
                    self.realm_id,
                    *group_id,
                    self.node_id,
                    version_key.bucket.clone(),
                    version_key.key.clone(),
                );
                let index = add_index_effect(&alias, blake3, version_key.version_id, None)?;
                if let Effect::Storage(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    ..
                }) = index
                {
                    writes.push((key_space, key, value));
                }
            }
            self.promoted += 1;
        }
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })])
    }

    fn next_page(&mut self) -> Effects {
        if self.last_page {
            return self.finish_pages();
        }
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: self.txn_id.take().unwrap_or_default(),
        })]
    }

    /// The pending row goes in the transaction that promotes the last owner.
    fn finish_pages(&mut self) -> Effects {
        self.state = State::DeletePending;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: PENDING_LOCATION_KEYSPACE.to_string(),
            key: self.archive.to_bytes().into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("Commit", "TransactionCommitted", event);
        };
        if !self.last_page {
            return self.start_page();
        }
        let blake3 = self.blake3.unwrap_or_default();
        match dht_registration_effect(&blake3, self.realm_id, &self.limits) {
            Ok(effect) => {
                self.state = State::Register;
                smallvec![effect]
            }
            Err(error) => self.finish(Err(error.into())),
        }
    }
}

impl Operation for PromotePendingOperation {
    type Output = Promotion;
    type Error = PromoteError;

    fn start(&mut self) -> Effects {
        self.read_pending(State::ReadPending, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            State::Init => self.unexpected("Init", "nothing", event),
            State::ReadPending => self.handle_pending(event),
            State::Admit => self.handle_admit(event),
            State::Hash => self.handle_hash(event),
            State::StartTransaction => self.handle_started(event),
            State::Reread => self.handle_reread(event),
            State::WriteLocation => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.scan_owners(),
                event => self.unexpected("WriteLocation", "WriteResult", event),
            },
            State::ScanOwners => self.handle_owners(event),
            State::ReadVersions => self.handle_versions(event),
            State::ReadBuckets => self.handle_buckets(event),
            State::WriteVersions => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.next_page(),
                event => self.unexpected("WriteVersions", "BatchWriteResult", event),
            },
            State::DeletePending => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.last_page = true;
                    self.state = State::Commit;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.take().unwrap_or_default(),
                    })]
                }
                event => self.unexpected("DeletePending", "DeleteResult", event),
            },
            State::Commit => self.handle_committed(event),
            State::Abort => {
                self.state = match self.output {
                    Some(Ok(_)) => State::Finish,
                    _ => State::Error,
                };
                smallvec![]
            }
            // DHT registration is advisory: the hash index already serves local reads.
            State::Register => {
                let blake3 = self.blake3.unwrap_or_default();
                let versions = self.promoted;
                self.finish(Ok(Promotion::Promoted { blake3, versions }))
            }
            State::Finish | State::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PromoteError::BadHashes))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = State::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }
}

/// Promotes every pending archive sealed with `key`, one page of pending rows at a time.
/// Call after `key` is installed; archives of other keys are skipped.
pub async fn promote_unlocked(
    context: &crate::driver::DriverContext,
    key: BucketKeyRef,
    origin: (RealmId, NodeId),
    limits: &RoCrateLimits,
) -> Result<usize, PromoteError> {
    let mut start_after = None;
    let mut promoted = 0;
    loop {
        let (rows, next) = crate::jobs::store::iter_prefix_page(
            &context.storage_handle,
            PENDING_LOCATION_KEYSPACE,
            None,
            start_after,
            PROMOTE_PAGE,
            None,
        )
        .await
        .map_err(|error| PromoteError::Storage(StorageError::ReadError(error)))?;
        for (row, value) in &rows {
            let location = BackendLocation::from_bytes(value)?;
            if location.format.bucket_key() != Some(key) {
                continue;
            }
            let archive = ArchiveKey::from_bytes(row)?;
            let operation =
                PromotePendingOperation::new(archive, origin.0, origin.1, limits.clone());
            match crate::driver::drive(operation, context).await? {
                Promotion::Promoted { .. } => promoted += 1,
                // A lock in between leaves the rest pending until the next unlock.
                Promotion::AwaitingKey(_) => return Ok(promoted),
                Promotion::Gone => {}
            }
        }
        match next {
            Some(next) if !rows.is_empty() => start_after = Some(next),
            _ => return Ok(promoted),
        }
    }
}
