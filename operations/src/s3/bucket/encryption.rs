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
use aruna_blob::blob::pithos::MAX_SIZE;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_HOLDER_KEYSPACE,
    GROUP_ENCRYPTED_KEYSPACE, TRANSITION_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketEncryption, BucketHolder, BucketKeyError, BucketKeyRecord,
    BucketKeyRef, EncryptionMode, SealedCopy,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::holders::{
    HolderReport, KeyLookup, RecoveryState, resolve_holders,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::structs::storage::transition::EncryptionTransition;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

/// Versions read per page while checking copy sizes before encryption is enabled.
const SIZE_PAGE: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnableState {
    ReadBucket,
    CheckLinks,
    CheckUploads,
    /// Pages through the bucket's versions and reads their copies' sizes.
    ScanVersions,
    ReadSizes,
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
}

impl EnableEncryptionOperation {
    pub fn new(input: EnableInput) -> Self {
        Self {
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
        }
    }

    fn call(&mut self, state: EnableState, call: IoCall) -> EnableTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn refused(&self, received: Event) -> EnableTurn {
        match received {
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => Err(EnableError::InvalidStateEvent {
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
        let prefix = match VersionKey::bucket_prefix(&self.input.bucket) {
            Ok(prefix) => prefix,
            Err(error) => return Err(error.into()),
        };
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
            match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => locations.extend(version.location_key().map(|key| key.to_bytes())),
                Err(error) => return Err(error.into()),
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
        let generate = Effect::Blob(BlobEffect::GenerateBucketKey);
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
            self.settings.bucket_id = Some(Ulid::generate());
        }
        let key = self.key_ref();
        let record_id = Ulid::generate();
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
        self.call(EnableState::SealCopies, Effect::Blob(seal).into())
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
        let mut writes = match rows {
            Ok(writes) => writes,
            Err(error) => return Err(error.into()),
        };
        // Plain copies already stored start their encrypt transition in the same transaction.
        let (bucket, now_ms) = (&self.input.bucket, self.input.now_ms);
        match encrypt_rows(bucket, &self.settings, &record, self.compression, now_ms) {
            Ok(rows) => writes.extend(rows),
            Err(error) => return Err(error.into()),
        }
        // The mode change and its audit record commit together.
        let audit = BucketAuditRecord {
            event_id: Ulid::generate(),
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
        match audit_row(&audit) {
            Ok(row) => writes.push(row),
            Err(error) => return Err(error.into()),
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

    /// The operation keeps no key after the commit; the output hands its only handle on.
    fn finish(&mut self) -> EnableTurn {
        self.private_key = None;
        let output = self.result.take().ok_or(EnableError::NotFinished)?;
        let wake = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::MigrateCompression,
            after: std::time::Duration::ZERO,
        });
        Ok(Next::Then {
            output,
            calls: vec![wake.into()],
        })
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
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        self.call(EnableState::ReadBucket, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> EnableTurn {
        match self.state {
            EnableState::ReadBucket => self.read_bucket(result.values()?),
            EnableState::CheckLinks => {
                let (rows, next) = result.page()?;
                self.links_scanned(rows, next)
            }
            EnableState::CheckUploads => self.check_uploads(&result.page()?.0),
            EnableState::ScanVersions => self.versions_scanned(result.page()?.0),
            EnableState::ReadSizes => self.sizes_read(result.values()?),
            EnableState::ReadTransition => self.check_transition(result.value()?),
            EnableState::ReadGrants => self.grants_read(result.page()?.0),
            EnableState::GenerateKey => match result.event()? {
                Event::Blob(BlobEvent::BucketKeyGenerated {
                    public_key,
                    private_key,
                }) => self.seal(public_key, private_key),
                other => self.refused(other),
            },
            EnableState::PrepareAbe => self.prepare_abe(result),
            EnableState::SealCopies => match result.event()? {
                Event::Blob(BlobEvent::CopiesSealed { copies }) => self.write_settings(copies),
                other => self.refused(other),
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

    /// A failed enable keeps no key.
    fn abort(&mut self) -> Vec<IoCall> {
        self.private_key = None;
        Vec::new()
    }
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
