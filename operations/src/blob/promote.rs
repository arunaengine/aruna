//! Records the content hash of a pending Pithos archive after unlock and promotes its versions.
//! Each owner page commits in its own transaction; the pending row goes only after the last page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_VERSION_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_QUARANTINE_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_KEY_KEYSPACE, COPY_OWNER_KEYSPACE,
    MANAGED_COPY_KEYSPACE, PENDING_CLAIM_KEYSPACE, PENDING_LOCATION_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::abe::EnvelopeArchive;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobQuarantineRecord, BlobVersion,
    BlobVersionState, BucketInfo, CopyOwner, CurrentVersionPointer, ManagedCopyKey,
    ManagedCopyRecord, VersionKey,
};
use aruna_core::structs::storage::distribution::inventory::HeadEntry;
use aruna_core::structs::storage::encryption::{
    BucketKeyError, BucketKeyRecord, BucketKeyRef, KeyState, ReadLease,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::blob::holders::dht_registration_effect;
use crate::blob::records::{HeadAliasContext, add_index_effect};
use crate::distribution::inventory::{
    AddedVersion, HeadChange, InventoryChange, InventoryError, InventoryUpdate, entry_of,
};

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
    #[error("State [{state}] invalid: expected [{expected}] - received [{received}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: &'static str,
    },
    #[error("could not drop a version with a mismatched content hash: {0}")]
    Dropped(String),
    #[error(transparent)]
    Inventory(#[from] InventoryError),
}

impl From<IoError> for PromoteError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type Turn<T> = Result<Next<T>, PromoteError>;

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
    ReadPending,
    Admit,
    Hash,
    /// One owner page in its own transaction.
    Page,
    Register,
}

#[derive(Debug, PartialEq)]
pub struct PromotePendingOperation {
    archive: ArchiveKey,
    realm_id: RealmId,
    node_id: NodeId,
    limits: RoCrateLimits,
    state: State,
    location: Option<BackendLocation>,
    blake3: Option<[u8; 32]>,
    hashes: HashMap<String, Vec<u8>>,
    cursor: Option<Key>,
    restarts: u8,
    promoted: usize,
    /// A read admitted outside the unlock registry, such as with a token credential.
    lease: Option<ReadLease>,
    /// Whether an owner sits in an ordinary bucket; `None` before any owner bucket was read.
    /// Only owners all in distributed buckets keep the hash out of the DHT.
    announced: Option<bool>,
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
            state: State::ReadPending,
            location: None,
            blake3: None,
            hashes: HashMap::new(),
            cursor: None,
            restarts: 0,
            promoted: 0,
            lease: None,
            announced: None,
        }
    }

    /// Hashes the archive under `lease` instead of a registry admission.
    pub fn with_lease(mut self, lease: ReadLease) -> Self {
        self.lease = Some(lease);
        self
    }

    fn unexpected(state: &'static str, expected: &'static str, event: Event) -> PromoteError {
        if let Event::Blob(BlobEvent::Error(error)) = event {
            return error.into();
        }
        PromoteError::InvalidStateEvent {
            state,
            expected,
            received: event.label(),
        }
    }

    fn handle_pending(&mut self, value: Option<Value>) -> Turn<Promotion> {
        let Some(value) = value else {
            return Ok(Next::Done(Promotion::Gone));
        };
        let location = BackendLocation::from_bytes(&value)?;
        let key = location.format.bucket_key().ok_or(PromoteError::NoKey)?;
        self.location = Some(location);
        self.state = State::Admit;
        let archive = &self.archive;
        let lease = self.lease.take();
        if let Some(lease) = lease.filter(|lease| lease.key == key && &lease.archive == archive) {
            return self.handle_admit(Event::Blob(BlobEvent::ReadAdmitted { lease }));
        }
        let admit = Effect::Blob(BlobEffect::AdmitRead {
            key,
            archive: self.archive.clone(),
        });
        Ok(Next::Call(admit.into()))
    }

    fn handle_admit(&mut self, event: Event) -> Turn<Promotion> {
        let location = self.location.clone().ok_or(PromoteError::NoKey)?;
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease }) => {
                self.state = State::Hash;
                let hash = Effect::Blob(BlobEffect::HashArchive {
                    location,
                    lease: Box::new(lease),
                });
                Ok(Next::Call(hash.into()))
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                let key = location.format.bucket_key().ok_or(PromoteError::NoKey)?;
                Ok(Next::Done(Promotion::AwaitingKey(key)))
            }
            event => Err(Self::unexpected("Admit", "ReadAdmitted", event)),
        }
    }

    fn handle_hash(&mut self, event: Event) -> Turn<Promotion> {
        let Event::Blob(BlobEvent::ArchiveHashed { hashes, size }) = event else {
            return Err(Self::unexpected("Hash", "ArchiveHashed", event));
        };
        let blake3 = hashes
            .get(HASH_BLAKE3)
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
        let expected = self.location.as_ref().map(|location| location.blob_size);
        let blake3 = blake3.filter(|_| Some(size) == expected);
        self.blake3 = Some(blake3.ok_or(PromoteError::BadHashes)?);
        self.hashes = hashes;
        self.start_page()
    }

    fn start_page(&mut self) -> Turn<Promotion> {
        let location = self.location.clone().ok_or(PromoteError::NoKey)?;
        let blake3 = self.blake3.ok_or(PromoteError::BadHashes)?;
        self.state = State::Page;
        let page = PromotePage {
            archive: self.archive.clone(),
            origin: (self.realm_id, self.node_id),
            location,
            blake3,
            hashes: self.hashes.clone(),
            step: PageStep::Reread,
            cursor: self.cursor.clone(),
            last_page: false,
            verifying: false,
            asked: Vec::new(),
            pending: Vec::new(),
            groups: HashMap::new(),
            sets: HashMap::new(),
            learned: Vec::new(),
            updates: Vec::new(),
            update: None,
            promoted: self.promoted,
            announced: self.announced,
        };
        Ok(Next::Call(IoCall::child(page)))
    }

    fn handle_page(&mut self, end: Result<PageEnd, PromoteError>) -> Turn<Promotion> {
        let end = match end {
            // A concurrent owner change: promote again from the first owner.
            Err(PromoteError::Storage(StorageError::TransactionConflict))
                if self.restarts < MAX_RESTARTS =>
            {
                self.restarts += 1;
                self.cursor = None;
                return self.start_page();
            }
            end => end?,
        };
        let (promoted, announced) = match end {
            PageEnd::Gone => return Ok(Next::Done(Promotion::Gone)),
            PageEnd::Mismatch { claimed } => {
                return Ok(Next::Done(Promotion::Mismatch { claimed }));
            }
            PageEnd::Next {
                cursor,
                promoted,
                announced,
            } => {
                (self.cursor, self.promoted, self.announced) = (cursor, promoted, announced);
                return self.start_page();
            }
            PageEnd::Last {
                promoted,
                announced,
            } => (promoted, announced),
        };
        self.promoted = promoted;
        let promotion = Promotion::Promoted {
            blake3: self.blake3.unwrap_or_default(),
            versions: promoted,
        };
        // Distributed buckets are found through their content indexes, not the DHT.
        if announced == Some(false) {
            return Ok(Next::Done(promotion));
        }
        let blake3 = self.blake3.unwrap_or_default();
        let register = dht_registration_effect(&blake3, self.realm_id, &self.limits)?;
        self.state = State::Register;
        Ok(Next::Call(register.into()))
    }
}

impl Operation for PromotePendingOperation {
    type Output = Promotion;
    type Error = PromoteError;

    fn first(&mut self) -> Turn<Promotion> {
        self.state = State::ReadPending;
        Ok(Next::Call(read_row(
            PENDING_LOCATION_KEYSPACE,
            self.archive.to_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<Promotion> {
        match self.state {
            State::ReadPending => self.handle_pending(result.value()?),
            State::Admit => self.handle_admit(result.event()?),
            State::Hash => self.handle_hash(result.event()?),
            State::Page => self.handle_page(result.child::<PageEnd, PromoteError>()?),
            // DHT registration is advisory: the hash index already serves local reads.
            State::Register => Ok(Next::Done(Promotion::Promoted {
                blake3: self.blake3.unwrap_or_default(),
                versions: self.promoted,
            })),
        }
    }
}

/// How one owner page transaction ended.
#[derive(Debug, PartialEq)]
enum PageEnd {
    /// The page committed; the next starts after `cursor`.
    Next {
        cursor: Option<Key>,
        promoted: usize,
        announced: Option<bool>,
    },
    /// The verifying pass found no pending owner left and removed the pending row.
    Last {
        promoted: usize,
        announced: Option<bool>,
    },
    /// No pending row names this exact archive any longer.
    Gone,
    /// The verified hash differs from the hash a sender claimed.
    Mismatch { claimed: [u8; 32] },
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Reread,
    ReadKey,
    WriteLocation,
    ScanOwners,
    ReadVersions,
    ReadBuckets,
    ReadManaged,
    WriteVersions,
    /// Reads the heads of the promoted versions of distributed buckets.
    ReadHeads,
    /// Records their content identity in the inventory and content index.
    Inventory,
    DeletePending,
}

/// Promotes one page of owners in one transaction; the last page also verifies every owner.
#[derive(Debug, PartialEq)]
struct PromotePage {
    archive: ArchiveKey,
    origin: (RealmId, NodeId),
    location: BackendLocation,
    blake3: [u8; 32],
    hashes: HashMap<String, Vec<u8>>,
    step: PageStep,
    cursor: Option<Key>,
    last_page: bool,
    /// The final pass rescans every owner in one transaction before the pending row goes.
    verifying: bool,
    /// The keys of the last read, in order.
    asked: Vec<Key>,
    pending: Vec<(Key, BlobVersion)>,
    groups: HashMap<String, Ulid>,
    /// The set of each distributed bucket among the owners.
    sets: HashMap<String, Ulid>,
    /// Promoted versions of distributed buckets whose inventory entries learn their identity.
    learned: Vec<(VersionKey, Ulid, AddedVersion)>,
    updates: Vec<InventoryUpdate>,
    update: Option<InventoryUpdate>,
    promoted: usize,
    announced: Option<bool>,
}

impl PromotePage {
    /// Reads `reads` and keeps their keys for the answer.
    fn read(&mut self, step: PageStep, reads: Vec<(String, Key)>) -> Turn<PageEnd> {
        self.asked = reads.iter().map(|(_, key)| key.clone()).collect();
        self.step = step;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// The values of the last read with the keys they were read under.
    fn keyed(&mut self, result: IoResult) -> Result<Vec<(Key, Option<Value>)>, PromoteError> {
        let values = result.values()?;
        let keys = std::mem::take(&mut self.asked);
        Ok(keys.into_iter().zip(values).collect())
    }

    /// The hashes belong to the exact archive and metadata digest that was read.
    fn handle_reread(&mut self, value: Option<Value>) -> Turn<PageEnd> {
        let current = value.map(|value| BackendLocation::from_bytes(&value));
        match current {
            Some(Ok(current)) if current == self.location => {}
            Some(Err(error)) => return Err(error.into()),
            _ => return Ok(Next::Done(PageEnd::Gone)),
        }
        let key = self
            .location
            .format
            .bucket_key()
            .ok_or(PromoteError::NoKey)?;
        let reads = vec![
            (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
            (
                PENDING_CLAIM_KEYSPACE.to_string(),
                self.archive.to_bytes().into(),
            ),
        ];
        self.read(PageStep::ReadKey, reads)
    }

    fn handle_key(&mut self, values: Vec<Option<Value>>) -> Turn<PageEnd> {
        let [value, claim] = <[_; 2]>::try_from(values).map_err(|_| PromoteError::BadHashes)?;
        // A received archive registers only the content its sender claimed.
        if let Some(claim) = claim
            && claim.as_ref() != self.blake3.as_slice()
        {
            let claimed =
                <[u8; 32]>::try_from(claim.as_ref()).map_err(|_| PromoteError::BadHashes)?;
            return Ok(Next::Done(PageEnd::Mismatch { claimed }));
        }
        let record = value
            .map(|value| BucketKeyRecord::from_bytes(&value))
            .transpose()?;
        if !record.is_some_and(|record| record.state != KeyState::Retired) {
            return Err(PromoteError::NoKey);
        }
        let mut known = self.location.clone();
        known.hashes.extend(self.hashes.clone());
        let key = BlobLocationKey::new(self.blake3, known.format.encoding(), known.backend.clone());
        let row = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            key.to_bytes().into(),
            known.to_bytes()?.into(),
        );
        self.step = PageStep::WriteLocation;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn scan_owners(&mut self) -> Turn<PageEnd> {
        self.step = PageStep::ScanOwners;
        Ok(Next::Call(IoCall::Page {
            space: COPY_OWNER_KEYSPACE.to_string(),
            prefix: CopyOwner::prefix(&self.archive).into(),
            after: self.cursor.clone(),
            limit: PROMOTE_PAGE,
        }))
    }

    fn handle_owners(&mut self, values: Vec<(Key, Value)>) -> Turn<PageEnd> {
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
            .collect::<Result<Vec<_>, ConversionError>>()?;
        if reads.is_empty() {
            return self.end_of_pass();
        }
        self.read(PageStep::ReadVersions, reads)
    }

    /// Keeps the versions that still pend on this archive; promoted ones are already done.
    fn handle_versions(&mut self, values: Vec<(Key, Option<Value>)>) -> Turn<PageEnd> {
        self.pending.clear();
        for (key, value) in values {
            let Some(value) = value else { continue };
            let version = BlobVersion::from_bytes(&value)?;
            if version.state.pending_archive() == Some(&self.archive) {
                self.pending.push((key, version));
            }
        }
        let buckets: BTreeSet<String> = self
            .pending
            .iter()
            .filter_map(|(key, _)| VersionKey::from_bytes(key).ok())
            .map(|key| key.bucket)
            .collect();
        if buckets.is_empty() {
            return self.next_page();
        }
        let reads = buckets
            .into_iter()
            .map(|bucket| (S3_BUCKET_KEYSPACE.to_string(), bucket.into_bytes().into()))
            .collect();
        self.read(PageStep::ReadBuckets, reads)
    }

    /// Reads the placement registrations of this page's versions on the archive's backend.
    fn read_managed(&mut self, buckets: Vec<(Key, Option<Value>)>) -> Turn<PageEnd> {
        self.groups.clear();
        self.sets.clear();
        for (key, value) in buckets {
            let bucket = String::from_utf8(key.to_vec()).map_err(ConversionError::from)?;
            if let Some(value) = value {
                let info = BucketInfo::from_bytes(&value)?;
                let ordinary = info.distribution.is_none();
                self.announced = Some(self.announced.unwrap_or(false) || ordinary);
                if let Some(binding) = info.distribution {
                    self.sets.insert(bucket.clone(), binding.set_id);
                }
                self.groups.insert(bucket, info.group_id);
            }
        }
        let backend = self.location.backend.clone();
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
        self.read(PageStep::ReadManaged, reads)
    }

    /// Rewrites each pending version in place: VersionId, creation time, size and metadata stay.
    fn promote_page(&mut self, mut managed: Vec<(Key, Option<Value>)>) -> Turn<PageEnd> {
        let blake3 = self.blake3;
        let location = &self.location;
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
            let version_key = VersionKey::from_bytes(&key)?;
            if let Some(id) = envelope {
                writes.push((ABE_ARCHIVE_KEYSPACE.to_string(), id, mapping.clone().into()));
            }
            version.state = BlobVersionState::Materialized {
                blob_hash: blake3,
                backend: location.backend.clone(),
                encoding: location.format.encoding(),
                source,
            };
            // A distributed bucket's version now has the identity its holders look it up by.
            if let Some(set_id) = self.sets.get(&version_key.bucket) {
                version.identity = version.identity.or(Some(blake3));
                let added = AddedVersion {
                    version_id: version_key.version_id,
                    epoch: version.original_epoch,
                    generation: version.generation,
                    entry: entry_of(&version, location.blob_size),
                };
                self.learned.push((version_key.clone(), *set_id, added));
            }
            writes.push((
                BLOB_VERSIONS_KEYSPACE.to_string(),
                key,
                version.to_bytes()?.into(),
            ));
            // A bucket that is gone keeps its promoted version but gets no hash alias.
            if let Some(group_id) = groups.get(&version_key.bucket) {
                let alias = HeadAliasContext::new(
                    self.origin.0,
                    *group_id,
                    self.origin.1,
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
        self.step = PageStep::WriteVersions;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Reads the head of each promoted version of a distributed bucket, whose inventory entry
    /// changes too while the version is the head.
    fn read_heads(&mut self) -> Turn<PageEnd> {
        if self.learned.is_empty() {
            return self.next_page();
        }
        let reads = (self.learned.iter())
            .map(|(key, _, _)| {
                let head = BlobHeadKey::new(&key.bucket, &key.key).to_bytes()?;
                Ok((BLOB_HEAD_KEYSPACE.to_string(), head.into()))
            })
            .collect::<Result<Vec<_>, ConversionError>>()?;
        self.read(PageStep::ReadHeads, reads)
    }

    fn handle_heads(&mut self, values: Vec<Option<Value>>) -> Turn<PageEnd> {
        let learned = std::mem::take(&mut self.learned);
        let mut changes = Vec::with_capacity(learned.len());
        for ((key, set_id, added), head) in learned.into_iter().zip(values) {
            let head = head
                .map(|value| CurrentVersionPointer::from_bytes(&value))
                .transpose()?;
            let mut change = InventoryChange::new(set_id, key.key);
            if head.is_some_and(|head| head.version_id == added.version_id) {
                change.head = HeadChange::Set(HeadEntry {
                    version_id: added.version_id,
                    epoch: added.epoch,
                    generation: added.generation,
                    version: added.entry,
                });
            }
            change.added.push(added);
            changes.push(InventoryUpdate::new(change));
        }
        self.updates = changes;
        self.next_update()
    }

    fn next_update(&mut self) -> Turn<PageEnd> {
        while let Some(mut update) = self.updates.pop() {
            if let Some(call) = update.start_calls() {
                self.step = PageStep::Inventory;
                self.update = Some(update);
                return Ok(Next::Call(call.into_call()));
            }
        }
        self.next_page()
    }

    fn handle_inventory(&mut self, result: IoResult) -> Turn<PageEnd> {
        let update = self.update.as_mut();
        let update = update.ok_or(StorageError::TransactionNotFound)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => {
                self.update = None;
                self.next_update()
            }
        }
    }

    fn next_page(&mut self) -> Turn<PageEnd> {
        if self.last_page {
            return self.end_of_pass();
        }
        if self.verifying {
            return self.scan_owners();
        }
        Ok(Next::Done(PageEnd::Next {
            cursor: self.cursor.take(),
            promoted: self.promoted,
            announced: self.announced,
        }))
    }

    /// Owner pages commit separately, so an alias may land behind the cursor. The final
    /// transaction rescans every owner from the start; a later insert conflicts with that scan.
    fn end_of_pass(&mut self) -> Turn<PageEnd> {
        if self.verifying {
            return self.finish_pages();
        }
        self.verifying = true;
        self.cursor = None;
        self.last_page = false;
        self.scan_owners()
    }

    /// The pending row and its claim go in the transaction that verified no pending owner remains.
    fn finish_pages(&mut self) -> Turn<PageEnd> {
        self.step = PageStep::DeletePending;
        let key = self.archive.to_bytes();
        Ok(Next::Call(IoCall::Delete(vec![
            (PENDING_LOCATION_KEYSPACE.to_string(), key.clone().into()),
            (PENDING_CLAIM_KEYSPACE.to_string(), key.into()),
        ])))
    }
}

impl Operation for PromotePage {
    type Output = PageEnd;
    type Error = PromoteError;

    fn first(&mut self) -> Turn<PageEnd> {
        self.step = PageStep::Reread;
        Ok(Next::Call(read_row(
            PENDING_LOCATION_KEYSPACE,
            self.archive.to_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<PageEnd> {
        match self.step {
            PageStep::Reread => self.handle_reread(result.value()?),
            PageStep::ReadKey => self.handle_key(result.values()?),
            PageStep::WriteLocation => {
                result.done()?;
                self.scan_owners()
            }
            PageStep::ScanOwners => self.handle_owners(result.page()?.0),
            PageStep::ReadVersions => {
                let values = self.keyed(result)?;
                self.handle_versions(values)
            }
            PageStep::ReadBuckets => {
                let values = self.keyed(result)?;
                self.read_managed(values)
            }
            PageStep::ReadManaged => {
                let values = self.keyed(result)?;
                self.promote_page(values)
            }
            PageStep::WriteVersions => {
                result.done()?;
                self.read_heads()
            }
            PageStep::ReadHeads => self.handle_heads(result.values()?),
            PageStep::Inventory => self.handle_inventory(result),
            PageStep::DeletePending => {
                result.done()?;
                Ok(Next::Done(PageEnd::Last {
                    promoted: self.promoted,
                    announced: self.announced,
                }))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Clone, Debug, PartialEq)]
enum RejectState {
    Scan,
    Write,
}

/// Records a quarantine in the transaction that lists the archive's owners; a conflict reruns
/// it under the caller's conflict retry.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RejectOwnersOperation {
    archive: ArchiveKey,
    row: (Key, Value),
    state: RejectState,
    cursor: Option<Key>,
    owners: Vec<VersionKey>,
}

impl RejectOwnersOperation {
    pub(super) fn new(
        archive: ArchiveKey,
        record: &BlobQuarantineRecord,
    ) -> Result<Self, PromoteError> {
        Ok(Self {
            archive,
            row: (record.key().into(), record.to_bytes()?.into()),
            state: RejectState::Scan,
            cursor: None,
            owners: Vec::new(),
        })
    }

    fn scan(&mut self) -> Turn<Vec<VersionKey>> {
        self.state = RejectState::Scan;
        Ok(Next::Call(IoCall::Page {
            space: COPY_OWNER_KEYSPACE.to_string(),
            prefix: CopyOwner::prefix(&self.archive).into(),
            after: self.cursor.clone(),
            limit: PROMOTE_PAGE,
        }))
    }
}

impl Operation for RejectOwnersOperation {
    type Output = Vec<VersionKey>;
    type Error = PromoteError;

    fn first(&mut self) -> Turn<Vec<VersionKey>> {
        self.scan()
    }

    fn next(&mut self, result: IoResult) -> Turn<Vec<VersionKey>> {
        match self.state {
            RejectState::Scan => {
                let (values, _) = result.page()?;
                for (row, _) in &values {
                    self.owners.push(CopyOwner::from_key(row)?.version);
                }
                self.cursor = values.last().map(|(row, _)| row.clone());
                if values.len() == PROMOTE_PAGE {
                    return self.scan();
                }
                self.state = RejectState::Write;
                let (key, value) = self.row.clone();
                let row = (BLOB_QUARANTINE_KEYSPACE.to_string(), key, value);
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            RejectState::Write => {
                result.done()?;
                Ok(Next::Done(std::mem::take(&mut self.owners)))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "promote_tests.rs"]
pub(crate) mod tests;
