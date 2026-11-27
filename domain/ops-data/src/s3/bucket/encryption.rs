//! Enables encryption on a bucket: generates its first key generation, seals copies for its
//! holders and stores settings and keys in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::abe::PrepareAbe;
use crate::s3::bucket::key::rows::{
    SettingsError, audit_row, authority_keys, copy_targets, full_scan, generation_rows,
    group_bucket_key, parse_authority, uploads_open,
};
use crate::s3::imports::LinkScan;
use crate::s3::object::put::abe::keyed;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_CREATION_KEYSPACE,
    BUCKET_HOLDER_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE, NFS_STATE_KEYSPACE, TRANSITION_KEYSPACE,
    UPLOAD_KEYSPACE,
};
use aruna_core::node::vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{outside_answer, outside_call, read_row};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey, incarnation};
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketEncryption, BucketHolder, BucketKeyError, BucketKeyRecord,
    BucketKeyRef, EncryptionMode, SealedCopy,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::holders::{
    HolderReport, KeyLookup, RecoveryState, resolve_holders,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::structs::storage::pithos::MAX_SIZE;
use aruna_core::structs::storage::transition::EncryptionTransition;
use aruna_core::structs::storage::vfs::{BucketScope, STAGE_TAG, VfsKey};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::{VfsEffect, VfsEvent, VfsFailure};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

/// Versions read per page while checking copy sizes before encryption is enabled.
const SIZE_PAGE: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnableState {
    /// Reads the bucket incarnation outside the transaction for the `vault_locked` fence.
    ReadCreation,
    /// Waits for the file servers to fence stage admission of the bucket.
    FenceVault,
    ReadBucket,
    CheckLinks,
    CheckUploads,
    /// Pages through the bucket's versions and reads their copies' sizes.
    ScanVersions,
    ReadSizes,
    /// Reads the bucket incarnation whose file server stages a `vault_locked` change checks.
    ReadIncarnation,
    /// Looks for any live or retained plaintext stage of the bucket.
    CheckStages,
    ReadTransition,
    ReadGrants,
    GenerateKey,
    PrepareAbe,
    SealCopies,
    WriteSettings,
    WriteVault,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnableError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Key(#[from] BucketKeyError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("the bucket already encrypts its writes")]
    AlreadyEncrypted,
    /// The enabling user lost the group admin role before the change committed.
    #[error("the caller is no group admin")]
    NotAdmin,
    #[error("encryption needs the mode node_managed or vault_locked")]
    InvalidMode,
    #[error("the bucket has open multipart uploads")]
    OpenUploads,
    /// A `vault_locked` bucket keeps no plaintext file server stage on this node; a stage
    /// inventory that cannot be read keeps the change refused too.
    #[error("the bucket still has plaintext file server stages")]
    PlainStages,
    /// Replacing an unfinished decryption would strand the copies it still has to move.
    #[error("the bucket's stored copies are still moving to a new encryption")]
    TransitionRunning,
    /// `vault_locked` needs two ready holders, or one whose key declares a recovery code.
    #[error("the key holders do not meet the recovery rule")]
    RecoveryUnmet,
    #[error("{}", crate::s3::imports::MAPPED_REASON)]
    Mapped,
    #[error("unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: &'static str,
    },
    #[error("enabling encryption did not finish")]
    NotFinished,
}

impl From<IoError> for EnableError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type EnableTurn = Result<Next<EnableResult>, EnableError>;

/// What the caller resolved before the transaction: the key directory answers. Admins are read
/// in the transaction; one without a lookup is reported unavailable and gets no copy yet.
#[derive(Debug, PartialEq)]
pub struct EnableInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// The group admin who enables encryption; checked again inside the transaction.
    pub caller: UserId,
    pub mode: EncryptionMode,
    pub cipher: BlockCipher,
    pub block_keys: BlockKeys,
    pub max_unlock_ms: Option<u64>,
    /// The storage generation the caller read; another one means a concurrent change.
    pub expected_generation: u64,
    pub lookups: BTreeMap<UserId, KeyLookup>,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct EnableResult {
    pub settings: BucketEncryption,
    pub key: BucketKeyRecord,
    pub holders: HolderReport,
    /// The new private key, handed on so the caller installs it and a new bucket starts unlocked.
    pub private_key: SharedSecret,
}

#[derive(Debug, PartialEq)]
pub struct EnableEncryptionOperation {
    ids: IdMint,
    input: EnableInput,
    state: EnableState,
    abe: Option<PrepareAbe>,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    compression: Compression,
    grants: Vec<BucketHolder>,
    record: Option<BucketKeyRecord>,
    private_key: Option<SharedSecret>,
    result: Option<EnableResult>,
    /// Last version key of the current size page, and whether another page may follow.
    size_cursor: Option<Key>,
    more_sizes: bool,
    links: Option<LinkScan>,
    /// The file server fence of a `vault_locked` change and the incarnation it covers.
    fence: Option<Ulid>,
    fenced_scope: Option<u128>,
    /// Set once the outcome went to the commit; an unknown commit keeps the fence closed.
    committing: bool,
    rolled_back: bool,
}

impl EnableEncryptionOperation {
    pub fn new(input: EnableInput) -> Self {
        Self {
            ids: IdMint::default(),
            input,
            state: EnableState::ReadBucket,
            abe: None,
            creator: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            compression: Compression::Off,
            grants: Vec::new(),
            record: None,
            private_key: None,
            result: None,
            size_cursor: None,
            more_sizes: false,
            links: None,
            fence: None,
            fenced_scope: None,
            committing: false,
            rolled_back: false,
        }
    }

    /// The authority reads that open the change's transaction.
    fn read_authority(&mut self) -> EnableTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        self.call(EnableState::ReadBucket, IoCall::Read(reads))
    }

    fn creation_read(&mut self, result: IoResult) -> EnableTurn {
        let creation = outside_answer(result)?.value()?;
        let incarnation = incarnation(creation.as_deref());
        self.fenced_scope = Some(incarnation);
        let scope = BucketScope::new(&self.input.bucket, incarnation);
        self.call(EnableState::FenceVault, fence_call(scope))
    }

    fn vault_fenced(&mut self, result: IoResult) -> EnableTurn {
        match fence_answer(result.event()?) {
            FenceAnswer::Held(fence_id) => self.fence = Some(fence_id),
            FenceAnswer::Unconfigured => {}
            FenceAnswer::Refused => return Err(EnableError::PlainStages),
        }
        self.read_authority()
    }

    fn call(&mut self, state: EnableState, call: IoCall) -> EnableTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn refused(&self, received: Event) -> EnableTurn {
        match received.into_blob() {
            Ok(BlobEvent::Error(error)) => Err(error.into()),
            Ok(other) => Err(EnableError::InvalidStateEvent {
                state: format!("{:?}", self.state),
                expected: "the event of the current step",
                received: other.label(),
            }),
            Err(other) => Err(EnableError::InvalidStateEvent {
                state: format!("{:?}", self.state),
                expected: "the event of the current step",
                received: other.label(),
            }),
        }
    }

    fn read_bucket(&mut self, values: Vec<Option<Value>>) -> EnableTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let (info, settings) = match parse_authority(keyed(&values), realm_id, group_id) {
            Ok(state) if !state.admins.contains(&self.input.caller) => {
                return Err(EnableError::NotAdmin);
            }
            Ok(state) => {
                self.admins = state.admins;
                (state.info, state.settings)
            }
            Err(error) => return Err(error.into()),
        };
        if settings.is_encrypted() {
            return Err(EnableError::AlreadyEncrypted);
        }
        if settings.storage_generation != self.input.expected_generation {
            return Err(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: settings.storage_generation,
            }
            .into());
        }
        self.creator = Some(info.created_by);
        self.compression = info.compression;
        self.settings = settings;
        let links = LinkScan::new(self.input.group_id, &self.input.bucket);
        let page = links.page(None);
        self.links = Some(links);
        self.call(EnableState::CheckLinks, page)
    }

    /// A bucket linked whole to a provider bucket keeps its bytes raw.
    fn links_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> EnableTurn {
        let links = self.links.as_mut().ok_or(EnableError::NotFinished)?;
        match links.scanned(values, next)? {
            Some(page) => Ok(Next::Call(page)),
            None if links.linked => Err(EnableError::Mapped),
            None => self.call(EnableState::CheckUploads, full_scan(UPLOAD_KEYSPACE, None)),
        }
    }

    fn check_uploads(&mut self, uploads: &[(Key, Value)]) -> EnableTurn {
        if uploads_open(uploads, &self.input.bucket) {
            return Err(EnableError::OpenUploads);
        }
        self.scan_versions()
    }

    /// Every copy must fit a Pithos archive, or the encryption transition could never move it.
    /// The scan reads in the enable transaction, so a concurrent larger write conflicts.
    fn scan_versions(&mut self) -> EnableTurn {
        let prefix = VersionKey::bucket_prefix(&self.input.bucket)?;
        let page = IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: self.size_cursor.clone(),
            limit: SIZE_PAGE,
        };
        self.call(EnableState::ScanVersions, page)
    }

    fn versions_scanned(&mut self, values: Vec<(Key, Value)>) -> EnableTurn {
        self.more_sizes = values.len() == SIZE_PAGE;
        self.size_cursor = values.last().map(|(key, _)| key.clone());
        let mut locations = BTreeSet::new();
        for (_, value) in &values {
            {
                let version = BlobVersion::from_bytes(value.as_ref())?;
                locations.extend(version.location_key().map(|key| key.to_bytes()))
            }
        }
        if locations.is_empty() {
            return self.after_sizes();
        }
        let reads = locations
            .into_iter()
            .map(|key| (BLOB_LOCATIONS_KEYSPACE.to_string(), key.into()))
            .collect();
        self.call(EnableState::ReadSizes, IoCall::Read(reads))
    }

    fn sizes_read(&mut self, values: Vec<Option<Value>>) -> EnableTurn {
        for value in values.into_iter().flatten() {
            match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) if location.blob_size > MAX_SIZE => {
                    let limit = MAX_SIZE;
                    return Err(BlobError::SizeLimitExceeded { limit }.into());
                }
                Ok(_) => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.after_sizes()
    }

    fn after_sizes(&mut self) -> EnableTurn {
        if self.more_sizes {
            return self.scan_versions();
        }
        if self.input.mode == EncryptionMode::VaultLocked {
            let bucket = self.input.bucket.as_bytes().to_vec();
            let read = read_row(BUCKET_CREATION_KEYSPACE, bucket);
            return self.call(EnableState::ReadIncarnation, read);
        }
        self.after_stages()
    }

    /// Reads one stage row of either kind in this transaction, so a stage admitted meanwhile
    /// conflicts. A recreated bucket is not the incarnation the file server fence covers.
    fn read_stages(&mut self, creation: Option<Value>) -> EnableTurn {
        let incarnation = incarnation(creation.as_deref());
        if self
            .fenced_scope
            .is_some_and(|fenced| fenced != incarnation)
        {
            return Err(EnableError::PlainStages);
        }
        let page = stage_page(&self.input.bucket, incarnation);
        let page = page.ok_or(EnableError::PlainStages)?;
        self.call(EnableState::CheckStages, page)
    }

    fn stages_read(&mut self, rows: &[(Key, Value)]) -> EnableTurn {
        if !rows.is_empty() {
            return Err(EnableError::PlainStages);
        }
        self.after_stages()
    }

    fn after_stages(&mut self) -> EnableTurn {
        if self.settings.bucket_id.is_none() {
            return self.generate();
        }
        // A bucket that was encrypted before may still run its decryption.
        let key = self.input.bucket.as_bytes().to_vec();
        self.call(
            EnableState::ReadTransition,
            read_row(TRANSITION_KEYSPACE, key),
        )
    }

    fn check_transition(&mut self, value: Option<Value>) -> EnableTurn {
        let transition = value.map(|value| EncryptionTransition::from_bytes(value.as_ref()));
        match transition.transpose() {
            Ok(Some(transition)) if transition.finished_at_ms.is_none() => {
                return Err(EnableError::TransitionRunning);
            }
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
        let Some(bucket_id) = self.settings.bucket_id else {
            return self.generate();
        };
        let prefix = Some(bucket_id.to_bytes().to_vec().into());
        let scan = full_scan(BUCKET_HOLDER_KEYSPACE, prefix);
        self.call(EnableState::ReadGrants, scan)
    }

    fn generate(&mut self) -> EnableTurn {
        let generate = Effect::blob(BlobEffect::GenerateBucketKey);
        self.call(EnableState::GenerateKey, generate.into())
    }

    fn key_ref(&self) -> BucketKeyRef {
        let bucket_id = self.settings.bucket_id.unwrap_or_default();
        BucketKeyRef::new(bucket_id, self.settings.key_generation + 1)
    }

    /// Every published key of every eligible holder gets a copy, so any key in the user's vault
    /// opens it.
    fn seal(&mut self, public_key: [u8; 32], private_key: SharedSecret) -> EnableTurn {
        if self.settings.bucket_id.is_none() {
            self.settings.bucket_id = Some(self.ids.mint(self.input.now_ms));
        }
        let key = self.key_ref();
        let record_id = self.ids.mint(self.input.now_ms);
        let mut record = BucketKeyRecord::new(key, record_id, public_key, self.input.now_ms);
        if self.input.mode == EncryptionMode::NodeManaged {
            record.vault_entry = Some(record_id);
        }
        self.record = Some(record);
        self.private_key = Some(private_key.clone());
        let mut abe = PrepareAbe::new(self.input.realm_id, self.input.node_id, key, private_key);
        let call = abe.first()?;
        self.abe = Some(abe);
        self.call(EnableState::PrepareAbe, call)
    }

    fn prepare_abe(&mut self, result: IoResult) -> EnableTurn {
        let abe = self.abe.as_mut().ok_or(EnableError::NotFinished)?;
        if let Some(call) = abe.next::<EnableError>(result)? {
            return Ok(Next::Call(call));
        }
        self.abe = None;
        let (key, public_key) = (self.record.as_ref())
            .map(|record| (record.key, record.public_key))
            .ok_or(EnableError::NotFinished)?;
        let private_key = self.private_key.clone().ok_or(EnableError::NotFinished)?;
        let report = self.report(&[]);
        let holders = copy_targets(&report, &self.input.lookups);
        if holders.is_empty() {
            return self.write_settings(Vec::new());
        }
        let seal = BlobEffect::SealHolderCopies {
            key,
            public_key,
            private_key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        };
        self.call(EnableState::SealCopies, Effect::blob(seal).into())
    }

    fn report(&self, copies: &[SealedCopy]) -> HolderReport {
        let (creator, input) = (self.creator.unwrap_or_default(), &self.input);
        resolve_holders(creator, &self.admins, &self.grants, &input.lookups, copies)
    }

    fn write_settings(&mut self, copies: Vec<SealedCopy>) -> EnableTurn {
        let key = self.key_ref();
        if copies.iter().any(|copy| copy.key != key) {
            return Err(EnableError::NotFinished);
        }
        let report = self.report(&copies);
        if self.input.mode == EncryptionMode::VaultLocked
            && report.recovery.state != RecoveryState::Met
        {
            return Err(EnableError::RecoveryUnmet);
        }
        let (Some(record), Some(private_key)) = (self.record.clone(), self.private_key.clone())
        else {
            return Err(EnableError::NotFinished);
        };
        self.settings = BucketEncryption {
            mode: self.input.mode,
            bucket_id: Some(key.bucket_id),
            key_generation: key.generation,
            storage_generation: self.settings.storage_generation + 1,
            cipher: self.input.cipher,
            block_keys: self.input.block_keys,
            max_unlock_ms: self.input.max_unlock_ms,
        };
        let rows = generation_rows(
            &self.input.bucket,
            &self.settings,
            &record,
            &copies,
            &report,
        );
        let mut writes = rows?;
        // Plain copies already stored start their encrypt transition in the same transaction.
        let (bucket, now_ms) = (&self.input.bucket, self.input.now_ms);
        {
            let rows = encrypt_rows(bucket, &self.settings, &record, self.compression, now_ms)?;
            writes.extend(rows)
        }
        // The mode change and its audit record commit together.
        let audit = BucketAuditRecord {
            event_id: self.ids.mint(self.input.now_ms),
            bucket_id: key.bucket_id,
            at_ms: now_ms,
            action: AuditAction::ModeChange,
            actor: Some(self.input.caller),
            node_id: self.input.node_id,
            generation: Some(key.generation),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: Some(format!("enabled {:?}", self.input.mode)),
            outcome: AuditOutcome::Applied,
        };
        {
            let row = audit_row(&audit)?;
            writes.push(row)
        }
        let group = group_bucket_key(self.input.group_id, &self.input.bucket);
        writes.push((
            GROUP_ENCRYPTED_KEYSPACE.to_string(),
            group,
            Vec::new().into(),
        ));
        self.result = Some(EnableResult {
            settings: self.settings.clone(),
            key: record,
            holders: report,
            private_key,
        });
        self.call(EnableState::WriteSettings, IoCall::Write(writes))
    }

    fn write_vault(&mut self) -> EnableTurn {
        let entry = self.record.as_ref().and_then(|record| record.vault_entry);
        let (Some(id), Some(secret)) = (entry, self.private_key.clone()) else {
            return self.finish();
        };
        let write = StorageEffect::VaultWrite {
            entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            secret,
            txn_id: None,
        };
        self.call(EnableState::WriteVault, Effect::Storage(write).into())
    }

    /// The operation keeps no key after the commit; the output hands its only handle on. The
    /// file server fence opens again once the commit is confirmed.
    fn finish(&mut self) -> EnableTurn {
        self.private_key = None;
        let output = self.result.take().ok_or(EnableError::NotFinished)?;
        let wake = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::MigrateCompression,
            after: std::time::Duration::ZERO,
        });
        let mut calls = vec![wake.into()];
        calls.extend(self.fence.map(|fence_id| release_call(fence_id, false)));
        self.committing = true;
        Ok(Next::Then { output, calls })
    }

    fn grants_read(&mut self, rows: Vec<(Key, Value)>) -> EnableTurn {
        self.grants = rows
            .iter()
            .map(|(_, value)| BucketHolder::from_bytes(value.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        self.generate()
    }
}

impl Operation for EnableEncryptionOperation {
    type Output = EnableResult;
    type Error = EnableError;

    fn first(&mut self) -> EnableTurn {
        if self.input.mode == EncryptionMode::Off || self.input.max_unlock_ms == Some(0) {
            return Err(EnableError::InvalidMode);
        }
        if self.input.mode == EncryptionMode::NodeManaged && self.input.max_unlock_ms.is_some() {
            return Err(BucketKeyError::ManagedLimit.into());
        }
        // The fence is taken before the transaction, which never waits on the file servers.
        if self.input.mode == EncryptionMode::VaultLocked {
            let bucket = self.input.bucket.as_bytes().to_vec();
            let read = outside_call(read_row(BUCKET_CREATION_KEYSPACE, bucket));
            return self.call(EnableState::ReadCreation, read);
        }
        self.read_authority()
    }

    fn next(&mut self, result: IoResult) -> EnableTurn {
        match self.state {
            EnableState::ReadCreation => self.creation_read(result),
            EnableState::FenceVault => self.vault_fenced(result),
            EnableState::ReadBucket => self.read_bucket(result.values()?),
            EnableState::CheckLinks => {
                let (rows, next) = result.page()?;
                self.links_scanned(rows, next)
            }
            EnableState::CheckUploads => self.check_uploads(&result.page()?.0),
            EnableState::ScanVersions => self.versions_scanned(result.page()?.0),
            EnableState::ReadSizes => self.sizes_read(result.values()?),
            EnableState::ReadIncarnation => self.read_stages(result.value()?),
            EnableState::CheckStages => self.stages_read(&result.page()?.0),
            EnableState::ReadTransition => self.check_transition(result.value()?),
            EnableState::ReadGrants => self.grants_read(result.page()?.0),
            EnableState::GenerateKey => match (result.event()?).into_blob() {
                Ok(BlobEvent::BucketKeyGenerated {
                    public_key,
                    private_key,
                }) => self.seal(public_key, private_key),
                Ok(other) => {
                    let other = Event::blob(other);
                    self.refused(other)
                }
                Err(other) => self.refused(other),
            },
            EnableState::PrepareAbe => self.prepare_abe(result),
            EnableState::SealCopies => match (result.event()?).into_blob() {
                Ok(BlobEvent::CopiesSealed { copies }) => self.write_settings(copies),
                Ok(other) => {
                    let other = Event::blob(other);
                    self.refused(other)
                }
                Err(other) => self.refused(other),
            },
            EnableState::WriteSettings => {
                result.done()?;
                self.write_vault()
            }
            EnableState::WriteVault => match result.event()? {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.finish(),
                other => self.refused(other),
            },
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// A failed enable keeps no key. Its fence opens again unless the commit outcome is unknown.
    fn abort(&mut self) -> Vec<IoCall> {
        self.private_key = None;
        let keep_closed = self.committing && !self.rolled_back;
        let fence = self.fence.take();
        fence.map_or_else(Vec::new, |id| vec![release_call(id, keep_closed)])
    }

    fn rolled_back(&mut self) {
        self.rolled_back = true;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

/// The file servers' answer to a `vault_locked` fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FenceAnswer {
    Held(Ulid),
    /// This node runs no file server, so no stage can be admitted meanwhile.
    Unconfigured,
    /// Plaintext stages remain, or the adapter failed; the change stays refused.
    Refused,
}

/// Fences stage admission of one bucket incarnation for a change to `vault_locked`.
pub(crate) fn fence_call(scope: BucketScope) -> IoCall {
    let effect = VfsEffect::FenceVault { scope };
    IoCall::Effect(Box::new(Effect::Vfs(Box::new(effect))))
}

/// Opens a fence again; `keep_closed` holds it after an unknown commit outcome.
pub(crate) fn release_call(fence_id: Ulid, keep_closed: bool) -> IoCall {
    let effect = VfsEffect::ReleaseFence {
        fence_id,
        keep_closed,
    };
    IoCall::Effect(Box::new(Effect::Vfs(Box::new(effect))))
}

pub(crate) fn fence_answer(event: Event) -> FenceAnswer {
    match event {
        Event::Vfs(event) => match *event {
            VfsEvent::VaultFenced { fence_id } => FenceAnswer::Held(fence_id),
            VfsEvent::Failed(VfsFailure::Unconfigured) => FenceAnswer::Unconfigured,
            _ => FenceAnswer::Refused,
        },
        _ => FenceAnswer::Refused,
    }
}

/// The page of one live or retained stage row of a bucket incarnation, read in the change's
/// transaction; `None` when the bucket name cannot scope `nfs_state` rows.
pub(crate) fn stage_page(bucket: &str, incarnation: u128) -> Option<IoCall> {
    let scope = BucketScope::new(bucket, incarnation);
    let prefix = VfsKey::scope_prefix(STAGE_TAG, &scope).ok()?;
    Some(IoCall::Page {
        space: NFS_STATE_KEYSPACE.to_string(),
        prefix: prefix.into(),
        after: None,
        limit: 1,
    })
}

/// The rows that start converting a bucket's plain copies after encryption is enabled. The
/// enabling transaction writes them with its settings, then wakes `TaskKey::MigrateCompression`.
fn encrypt_rows(
    bucket: &str,
    settings: &BucketEncryption,
    record: &BucketKeyRecord,
    compression: Compression,
    now_ms: u64,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let plan = aruna_core::structs::storage::encryption::SealPlan::capture(settings, record)
        .map_err(ConversionError::from)?;
    let target = aruna_core::structs::storage::transition::TransitionTarget { compression, plan };
    let generation = settings.storage_generation;
    let kind = aruna_core::structs::storage::transition::TransitionKind::Encrypt;
    let transition = EncryptionTransition::new(kind, None, target, generation, now_ms);
    let key: Key = bucket.as_bytes().to_vec().into();
    Ok(vec![
        (
            aruna_core::keyspaces::TRANSITION_KEYSPACE.to_string(),
            key.clone(),
            transition.to_bytes()?.into(),
        ),
        (
            aruna_core::keyspaces::TRANSITION_QUEUE_KEYSPACE.to_string(),
            key,
            Vec::new().into(),
        ),
    ])
}

#[cfg(test)]
#[path = "encryption_tests.rs"]
mod tests;
