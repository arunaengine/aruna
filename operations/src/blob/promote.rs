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
    ABE_ARCHIVE_KEYSPACE, ABE_VERSION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_QUARANTINE_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BUCKET_KEY_KEYSPACE, COPY_OWNER_KEYSPACE, MANAGED_COPY_KEYSPACE,
    PENDING_CLAIM_KEYSPACE, PENDING_LOCATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::abe::EnvelopeArchive;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobLocationKey, BlobQuarantineRecord, BlobVersion,
    BlobVersionState, BucketInfo, CopyOwner, ManagedCopyKey, ManagedCopyRecord, VersionKey,
};
use aruna_core::structs::storage::encryption::{
    BucketKeyError, BucketKeyRecord, BucketKeyRef, KeyState, ReadLease,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::blob::records::{HeadAliasContext, add_index_effect};
use crate::replication::dht_registration::dht_registration_effect;

/// Owner rows promoted per transaction.
pub const PROMOTE_PAGE: usize = 64;
/// Commit conflicts that restart the promotion before it reports an error.
const MAX_RESTARTS: u8 = 3;

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
    #[error("could not drop a version with a mismatched content hash: {0}")]
    Dropped(String),
}

#[derive(Debug, PartialEq)]
pub enum Promotion {
    /// Every pending alias now names the content hash.
    Promoted { blake3: [u8; 32], versions: usize },
    /// The key generation is locked; the work waits for unlock.
    AwaitingKey(BucketKeyRef),
    /// No pending row names this archive any longer.
    Gone,
    /// The verified hash differs from the hash a sender claimed; nothing was registered.
    Mismatch { claimed: [u8; 32] },
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    ReadPending,
    Admit,
    Hash,
    StartTransaction,
    Reread,
    ReadKey,
    WriteLocation,
    ScanOwners,
    ReadVersions,
    ReadBuckets,
    ReadManaged,
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
    /// The final pass rescans every owner in one transaction before the pending row goes.
    verifying: bool,
    restarts: u8,
    pending: Vec<(Key, BlobVersion)>,
    groups: HashMap<String, Ulid>,
    promoted: usize,
    output: Option<Result<Promotion, PromoteError>>,
    /// A read admitted outside the unlock registry, such as with a token credential.
    lease: Option<ReadLease>,
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
            verifying: false,
            restarts: 0,
            pending: Vec::new(),
            groups: HashMap::new(),
            promoted: 0,
            output: None,
            lease: None,
        }
    }

    /// Hashes the archive under `lease` instead of a registry admission.
    pub fn with_lease(mut self, lease: ReadLease) -> Self {
        self.lease = Some(lease);
        self
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
        let archive = &self.archive;
        let lease = self.lease.take();
        if let Some(lease) = lease.filter(|lease| lease.key == key && &lease.archive == archive) {
            return self.handle_admit(Event::Blob(BlobEvent::ReadAdmitted { lease }));
        }
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
                smallvec![Effect::Blob(BlobEffect::HashArchive {
                    location,
                    lease: Box::new(lease),
                })]
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
        let Some(key) = location.format.bucket_key() else {
            return self.finish(Err(PromoteError::NoKey));
        };
        self.state = State::ReadKey;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
                (
                    PENDING_CLAIM_KEYSPACE.to_string(),
                    self.archive.to_bytes().into()
                ),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn handle_key(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("ReadKey", "BatchReadResult", event);
        };
        let mut values = values.into_iter().map(|(_, value)| value);
        let (Some(value), Some(claim)) = (values.next(), values.next()) else {
            return self.finish(Err(PromoteError::BadHashes));
        };
        // A received archive registers only the content its sender claimed.
        if let Some(claim) = claim
            && Some(claim.as_ref()) != self.blake3.as_ref().map(<[u8; 32]>::as_slice)
        {
            let Ok(claimed) = <[u8; 32]>::try_from(claim.as_ref()) else {
                return self.finish(Err(PromoteError::BadHashes));
            };
            self.output = Some(Ok(Promotion::Mismatch { claimed }));
            self.state = State::Abort;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: self.txn_id.take().unwrap_or_default(),
            })];
        }
        let record = value
            .map(|value| BucketKeyRecord::from_bytes(&value))
            .transpose();
        match record {
            Ok(Some(record)) if record.state != KeyState::Retired => {}
            Ok(_) => return self.finish(Err(PromoteError::NoKey)),
            Err(error) => return self.finish(Err(error.into())),
        }
        let Some(mut known) = self.location.clone() else {
            return self.finish(Err(PromoteError::NoKey));
        };
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
            Ok(reads) if reads.is_empty() => self.end_of_pass(),
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
        match self.read_managed(values) {
            Ok(effects) => {
                self.state = State::ReadManaged;
                effects
            }
            Err(error) => self.finish(Err(error)),
        }
    }

    /// Reads the placement registrations of this page's versions on the archive's backend.
    fn read_managed(
        &mut self,
        buckets: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, PromoteError> {
        self.groups.clear();
        for (key, value) in buckets {
            let bucket = String::from_utf8(key.to_vec()).map_err(ConversionError::from)?;
            if let Some(value) = value {
                self.groups
                    .insert(bucket, BucketInfo::from_bytes(&value)?.group_id);
            }
        }
        let backend = self
            .location
            .as_ref()
            .ok_or(PromoteError::NoKey)?
            .backend
            .clone();
        let mut reads = self
            .pending
            .iter()
            .map(|(key, _)| {
                let version = VersionKey::from_bytes(key)?;
                let managed = ManagedCopyKey::new(version, backend.clone());
                Ok((
                    MANAGED_COPY_KEYSPACE.to_string(),
                    managed.to_bytes()?.into(),
                ))
            })
            .collect::<Result<Vec<_>, ConversionError>>()?;
        // Each version's envelope id follows, in the same order.
        let envelopes = self.pending.iter();
        reads.extend(envelopes.map(|(key, _)| (ABE_VERSION_KEYSPACE.to_string(), key.clone())));
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })])
    }

    fn handle_managed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("ReadManaged", "BatchReadResult", event);
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
        mut managed: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, PromoteError> {
        let blake3 = self.blake3.unwrap_or_default();
        let location = self.location.as_ref().ok_or(PromoteError::NoKey)?;
        if managed.len() != 2 * self.pending.len() {
            return Err(PromoteError::BadHashes);
        }
        let envelopes = managed.split_off(self.pending.len());
        // An envelope mapping now names the location key; completion already charged its size.
        let location_key =
            BlobLocationKey::new(blake3, location.format.encoding(), location.backend.clone());
        let archive = EnvelopeArchive {
            archive: self.archive.clone(),
            location_key: location_key.to_bytes(),
        };
        let mapping = postcard::to_allocvec(&archive).map_err(ConversionError::from)?;
        let mut writes = Vec::new();
        // A governed version's registration names the copy by its hashes, so it gains them too.
        for (key, value) in managed {
            let Some(value) = value else { continue };
            let mut record = ManagedCopyRecord::from_bytes(&value)?;
            if record.location.same_object(location) {
                record.location.hashes.extend(self.hashes.clone());
                writes.push((
                    MANAGED_COPY_KEYSPACE.to_string(),
                    key,
                    record.to_bytes()?.into(),
                ));
            }
        }
        let groups = std::mem::take(&mut self.groups);
        let pending = std::mem::take(&mut self.pending);
        for ((key, mut version), (_, envelope)) in pending.into_iter().zip(envelopes) {
            let BlobVersionState::PendingContent { source, .. } = version.state else {
                continue;
            };
            let version_key = aruna_core::structs::storage::blob::VersionKey::from_bytes(&key)?;
            if let Some(id) = envelope {
                writes.push((ABE_ARCHIVE_KEYSPACE.to_string(), id, mapping.clone().into()));
            }
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
            return self.end_of_pass();
        }
        if self.verifying {
            return self.scan_owners();
        }
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: self.txn_id.take().unwrap_or_default(),
        })]
    }

    /// Owner pages commit separately, so an alias may land behind the cursor. The final
    /// transaction rescans every owner from the start; a later insert conflicts with that scan.
    fn end_of_pass(&mut self) -> Effects {
        if self.verifying {
            return self.finish_pages();
        }
        self.verifying = true;
        self.cursor = None;
        self.last_page = false;
        self.scan_owners()
    }

    /// The pending row and its claim go in the transaction that verified no pending owner remains.
    fn finish_pages(&mut self) -> Effects {
        self.state = State::DeletePending;
        let key = self.archive.to_bytes();
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (PENDING_LOCATION_KEYSPACE.to_string(), key.clone().into()),
                (PENDING_CLAIM_KEYSPACE.to_string(), key.into()),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) = event
            && self.restarts < MAX_RESTARTS
        {
            // A concurrent owner change: promote again from the first owner.
            self.restarts += 1;
            self.verifying = false;
            self.last_page = false;
            self.cursor = None;
            return self.start_page();
        }
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
            State::ReadKey => self.handle_key(event),
            State::WriteLocation => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.scan_owners(),
                event => self.unexpected("WriteLocation", "WriteResult", event),
            },
            State::ScanOwners => self.handle_owners(event),
            State::ReadVersions => self.handle_versions(event),
            State::ReadBuckets => self.handle_buckets(event),
            State::ReadManaged => self.handle_managed(event),
            State::WriteVersions => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.next_page(),
                event => self.unexpected("WriteVersions", "BatchWriteResult", event),
            },
            State::DeletePending => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.last_page = true;
                    self.state = State::Commit;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.take().unwrap_or_default(),
                    })]
                }
                event => self.unexpected("DeletePending", "BatchDeleteResult", event),
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
                PromotePendingOperation::new(archive.clone(), origin.0, origin.1, limits.clone());
            let outcome = match crate::driver::drive(operation, context).await {
                Err(PromoteError::Blob(BlobError::IntegrityCheckFailed(reason))) => {
                    reject_archive(context, &archive, origin, reason).await?;
                    continue;
                }
                result => result?,
            };
            match outcome {
                Promotion::Promoted { .. } => promoted += 1,
                // A lock in between leaves the rest pending until the next unlock.
                Promotion::AwaitingKey(_) => return Ok(promoted),
                Promotion::Gone => {}
                Promotion::Mismatch { claimed } => {
                    drop_mismatch(context, &archive, claimed, origin).await?;
                }
            }
        }
        match next {
            Some(next) if !rows.is_empty() => start_after = Some(next),
            _ => return Ok(promoted),
        }
    }
}

/// Quarantines an archive whose content differs from the hash its sender claimed and deletes the
/// versions that use it; none of them was registered. Returns how many versions were deleted.
pub async fn drop_mismatch(
    context: &crate::driver::DriverContext,
    archive: &ArchiveKey,
    claimed: [u8; 32],
    origin: (RealmId, NodeId),
) -> Result<usize, PromoteError> {
    let reason = "replicated content hash differs from the claimed hash".to_string();
    drop_received(context, archive, claimed, origin, reason).await
}

pub(crate) async fn reject_archive(
    context: &crate::driver::DriverContext,
    archive: &ArchiveKey,
    origin: (RealmId, NodeId),
    reason: String,
) -> Result<usize, PromoteError> {
    let claim = read_value(
        &context.storage_handle,
        PENDING_CLAIM_KEYSPACE,
        archive.to_bytes(),
    )
    .await?;
    let Some(claimed) = claim
        .as_ref()
        .and_then(|claim| <[u8; 32]>::try_from(claim.as_ref()).ok())
    else {
        return Err(BlobError::IntegrityCheckFailed(reason).into());
    };
    drop_received(context, archive, claimed, origin, reason).await
}

async fn drop_received(
    context: &crate::driver::DriverContext,
    archive: &ArchiveKey,
    claimed: [u8; 32],
    (realm_id, node_id): (RealmId, NodeId),
    reason: String,
) -> Result<usize, PromoteError> {
    use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
    let storage = &context.storage_handle;
    let now_ms = aruna_core::time::unix_timestamp_millis();
    let record = BlobQuarantineRecord::new(claimed, archive.backend.clone(), reason, now_ms);
    let write = StorageEffect::Write {
        key_space: BLOB_QUARANTINE_KEYSPACE.to_string(),
        key: record.key().into(),
        value: record.to_bytes()?.into(),
        txn_id: None,
    };
    if let Event::Storage(StorageEvent::Error { error }) = storage.send_storage_effect(write).await
    {
        return Err(error.into());
    }
    let mut dropped = 0;
    loop {
        let fence = RejectOwnersOperation::new(archive.clone(), &record)?;
        let owners = crate::driver::drive(fence, context).await?;
        if owners.is_empty() {
            return Ok(dropped);
        }
        let before = dropped;
        for version in owners {
            let row = version.to_bytes()?;
            let Some(value) = read_value(storage, BLOB_VERSIONS_KEYSPACE, row).await? else {
                continue;
            };
            let stored = BlobVersion::from_bytes(&value)?;
            let bucket = version.bucket.as_bytes().to_vec();
            let info = read_value(storage, S3_BUCKET_KEYSPACE, bucket).await?;
            let Some(info) = info else {
                continue;
            };
            if stored.state.pending_archive() != Some(archive) {
                continue;
            }
            let input = DeleteObjectInput {
                bucket: version.bucket.clone(),
                key: version.key.clone(),
                version_id: Some(version.version_id),
                group_id: BucketInfo::from_bytes(&info)?.group_id,
                realm_id,
                node_id,
                deleted_by: stored.created_by,
            };
            match crate::driver::drive(DeleteObjectOperation::new(input), context).await {
                Ok(_) => dropped += 1,
                Err(error) => return Err(PromoteError::Dropped(error.to_string())),
            }
        }
        if dropped == before {
            return Err(PromoteError::Dropped(
                "pending owners could not be removed".into(),
            ));
        }
    }
}

#[derive(Debug, PartialEq)]
enum RejectState {
    Start,
    Scan,
    Write,
    Commit,
    Abort,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
struct RejectOwnersOperation {
    archive: ArchiveKey,
    row: (Key, Value),
    state: RejectState,
    txn_id: Option<TxnId>,
    cursor: Option<Key>,
    owners: Vec<VersionKey>,
    restarts: u8,
    error: Option<PromoteError>,
}

impl RejectOwnersOperation {
    fn new(archive: ArchiveKey, record: &BlobQuarantineRecord) -> Result<Self, PromoteError> {
        Ok(Self {
            archive,
            row: (record.key().into(), record.to_bytes()?.into()),
            state: RejectState::Start,
            txn_id: None,
            cursor: None,
            owners: Vec::new(),
            restarts: 0,
            error: None,
        })
    }

    fn scan(&mut self) -> Effects {
        self.state = RejectState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: COPY_OWNER_KEYSPACE.to_string(),
            prefix: Some(CopyOwner::prefix(&self.archive).into()),
            start: self.cursor.clone().map(IterStart::After),
            limit: PROMOTE_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn advance(&mut self, event: Event) -> Result<Effects, PromoteError> {
        match (&self.state, event) {
            (RejectState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                Ok(self.scan())
            }
            (RejectState::Scan, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                for (row, _) in &values {
                    self.owners.push(CopyOwner::from_key(row)?.version);
                }
                self.cursor = values.last().map(|(row, _)| row.clone());
                if values.len() == PROMOTE_PAGE {
                    return Ok(self.scan());
                }
                self.state = RejectState::Write;
                Ok(smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: BLOB_QUARANTINE_KEYSPACE.to_string(),
                    key: self.row.0.clone(),
                    value: self.row.1.clone(),
                    txn_id: self.txn_id,
                })])
            }
            (RejectState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.state = RejectState::Commit;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.take().ok_or(PromoteError::BadHashes)?,
                    }
                )])
            }
            (RejectState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = RejectState::Finish;
                Ok(smallvec![])
            }
            (
                RejectState::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) if self.restarts < MAX_RESTARTS => {
                self.restarts += 1;
                self.cursor = None;
                self.owners.clear();
                Ok(self.start())
            }
            (RejectState::Abort, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.state = RejectState::Error;
                Ok(smallvec![])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, received) => Err(PromoteError::InvalidStateEvent {
                state: "RejectOwners",
                expected: "the storage result of the last effect",
                received,
            }),
        }
    }
}

impl Operation for RejectOwnersOperation {
    type Output = Vec<VersionKey>;
    type Error = PromoteError;

    fn start(&mut self) -> Effects {
        self.state = RejectState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.advance(event) {
            Ok(effects) => effects,
            Err(error) => {
                self.error = Some(error);
                self.abort()
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RejectState::Finish | RejectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == RejectState::Finish {
            Ok(self.owners)
        } else {
            Err(self.error.unwrap_or(PromoteError::BadHashes))
        }
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = RejectState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = RejectState::Error;
                smallvec![]
            }
        }
    }
}

async fn read_value(
    storage: &aruna_storage::StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, PromoteError> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(PromoteError::InvalidStateEvent {
            state: "Drop",
            expected: "ReadResult",
            received: other,
        }),
    }
}

#[cfg(test)]
#[path = "promote_tests.rs"]
mod tests;
