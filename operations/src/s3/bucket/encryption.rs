//! Enables encryption on a bucket: generates its first key generation, seals copies for its
//! holders and stores settings and keys in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::migration::queue::encrypt_rows;
use crate::s3::bucket::key::rows::{
    SettingsError, audit_row, authority_read, copy_targets, generation_rows, group_bucket_key,
    parse_authority, uploads_open,
};
use aruna_blob::blob::pithos::MAX_SIZE;
use aruna_core::compute::SharedSecret;
use aruna_core::effects::IterStart;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_HOLDER_KEYSPACE,
    GROUP_ENCRYPTED_KEYSPACE, TRANSITION_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
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
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

/// Versions read per page while checking copy sizes before encryption is enabled.
const SIZE_PAGE: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnableState {
    Init,
    StartTransaction,
    ReadBucket,
    CheckUploads,
    /// Pages through the bucket's versions and reads their copies' sizes.
    ScanVersions,
    ReadSizes,
    ReadTransition,
    ReadGrants,
    GenerateKey,
    SealCopies,
    WriteSettings,
    WriteVault,
    CommitTransaction,
    Finish,
    Error,
    PrepareAbe,
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
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("enabling encryption did not finish")]
    NotFinished,
}

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
    abe: Option<crate::s3::bucket::key::abe::PrepareAbeOperation>,
    txn_id: Option<TxnId>,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    compression: Compression,
    grants: Vec<BucketHolder>,
    record: Option<BucketKeyRecord>,
    private_key: Option<SharedSecret>,
    result: Option<EnableResult>,
    output: Option<Result<EnableResult, EnableError>>,
    /// Last version key of the current size page, and whether another page may follow.
    size_cursor: Option<Key>,
    more_sizes: bool,
}

impl EnableEncryptionOperation {
    pub fn new(input: EnableInput) -> Self {
        Self {
            input,
            state: EnableState::Init,
            abe: None,
            txn_id: None,
            creator: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            compression: Compression::Off,
            grants: Vec::new(),
            record: None,
            private_key: None,
            result: None,
            output: None,
            size_cursor: None,
            more_sizes: false,
        }
    }

    fn fail(&mut self, error: impl Into<EnableError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.state = EnableState::Error;
        effects
    }

    fn read_bucket(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let (info, settings) = match parse_authority(values, realm_id, group_id) {
            Ok(state) if !state.admins.contains(&self.input.caller) => {
                return self.fail(EnableError::NotAdmin);
            }
            Ok(state) => {
                self.admins = state.admins;
                (state.info, state.settings)
            }
            Err(error) => return self.fail(error),
        };
        if settings.is_encrypted() {
            return self.fail(EnableError::AlreadyEncrypted);
        }
        if settings.storage_generation != self.input.expected_generation {
            return self.fail(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: settings.storage_generation,
            });
        }
        self.creator = Some(info.created_by);
        self.compression = info.compression;
        self.settings = settings;
        self.state = EnableState::CheckUploads;
        self.scan(UPLOAD_KEYSPACE, None)
    }

    fn scan(&self, key_space: &str, prefix: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix,
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    fn check_uploads(&mut self, uploads: &[(Key, Value)]) -> Effects {
        if uploads_open(uploads, &self.input.bucket) {
            return self.fail(EnableError::OpenUploads);
        }
        self.scan_versions()
    }

    /// Every copy must fit a Pithos archive, or the encryption transition could never move it.
    /// The scan reads in the enable transaction, so a concurrent larger write conflicts.
    fn scan_versions(&mut self) -> Effects {
        let prefix = match VersionKey::bucket_prefix(&self.input.bucket) {
            Ok(prefix) => prefix,
            Err(error) => return self.fail(error),
        };
        self.state = EnableState::ScanVersions;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: self.size_cursor.clone().map(IterStart::After),
            limit: SIZE_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn versions_scanned(&mut self, values: Vec<(Key, Value)>) -> Effects {
        self.more_sizes = values.len() == SIZE_PAGE;
        self.size_cursor = values.last().map(|(key, _)| key.clone());
        let mut locations = BTreeSet::new();
        for (_, value) in &values {
            match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => locations.extend(version.location_key().map(|key| key.to_bytes())),
                Err(error) => return self.fail(error),
            }
        }
        if locations.is_empty() {
            return self.after_sizes();
        }
        self.state = EnableState::ReadSizes;
        let reads = locations
            .into_iter()
            .map(|key| (BLOB_LOCATIONS_KEYSPACE.to_string(), key.into()))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn sizes_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        for value in values.into_iter().filter_map(|(_, value)| value) {
            match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) if location.blob_size > MAX_SIZE => {
                    let limit = MAX_SIZE;
                    return self.fail(BlobError::SizeLimitExceeded { limit });
                }
                Ok(_) => {}
                Err(error) => return self.fail(error),
            }
        }
        self.after_sizes()
    }

    fn after_sizes(&mut self) -> Effects {
        if self.more_sizes {
            return self.scan_versions();
        }
        if self.settings.bucket_id.is_none() {
            return self.generate();
        }
        // A bucket that was encrypted before may still run its decryption.
        self.state = EnableState::ReadTransition;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: TRANSITION_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().to_vec().into(),
            txn_id: self.txn_id,
        })]
    }

    fn check_transition(&mut self, value: Option<Value>) -> Effects {
        let transition = value.map(|value| EncryptionTransition::from_bytes(value.as_ref()));
        match transition.transpose() {
            Ok(Some(transition)) if transition.finished_at_ms.is_none() => {
                return self.fail(EnableError::TransitionRunning);
            }
            Ok(_) => {}
            Err(error) => return self.fail(error),
        }
        let Some(bucket_id) = self.settings.bucket_id else {
            return self.generate();
        };
        self.state = EnableState::ReadGrants;
        self.scan(
            BUCKET_HOLDER_KEYSPACE,
            Some(bucket_id.to_bytes().to_vec().into()),
        )
    }

    fn generate(&mut self) -> Effects {
        self.state = EnableState::GenerateKey;
        smallvec![Effect::Blob(BlobEffect::GenerateBucketKey)]
    }

    fn key_ref(&self) -> BucketKeyRef {
        let bucket_id = self.settings.bucket_id.unwrap_or_default();
        BucketKeyRef::new(bucket_id, self.settings.key_generation + 1)
    }

    /// Every published key of every eligible holder gets a copy, so any key in the user's vault
    /// opens it.
    fn seal(&mut self, public_key: [u8; 32], private_key: SharedSecret) -> Effects {
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
        let Some(txn) = self.txn_id else {
            return self.fail(EnableError::NotFinished);
        };
        let mut abe = crate::s3::bucket::key::abe::PrepareAbeOperation::new(
            self.input.realm_id,
            self.input.node_id,
            key,
            private_key,
            txn,
        );
        self.state = EnableState::PrepareAbe;
        let effects = abe.start();
        self.abe = Some(abe);
        effects
    }

    fn prepare_abe(&mut self, event: Event) -> Effects {
        let Some(abe) = self.abe.as_mut() else {
            return self.fail(EnableError::NotFinished);
        };
        let effects = abe.step(event);
        if !abe.is_complete() {
            return effects;
        }
        let Some(abe) = self.abe.take() else {
            return self.fail(EnableError::NotFinished);
        };
        if let Err(error) = abe.finalize() {
            return self.fail(error);
        }
        let Some((key, public_key)) = self.record.as_ref().map(|r| (r.key, r.public_key)) else {
            return self.fail(EnableError::NotFinished);
        };
        let Some(private_key) = self.private_key.clone() else {
            return self.fail(EnableError::NotFinished);
        };
        let report = self.report(&[]);
        let holders = copy_targets(&report, &self.input.lookups);
        if holders.is_empty() {
            return self.write_settings(Vec::new());
        }
        self.state = EnableState::SealCopies;
        smallvec![Effect::Blob(BlobEffect::SealHolderCopies {
            key,
            public_key,
            private_key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        })]
    }

    fn report(&self, copies: &[SealedCopy]) -> HolderReport {
        let (creator, input) = (self.creator.unwrap_or_default(), &self.input);
        resolve_holders(creator, &self.admins, &self.grants, &input.lookups, copies)
    }

    fn write_settings(&mut self, copies: Vec<SealedCopy>) -> Effects {
        let key = self.key_ref();
        if copies.iter().any(|copy| copy.key != key) {
            return self.fail(EnableError::NotFinished);
        }
        let report = self.report(&copies);
        if self.input.mode == EncryptionMode::VaultLocked
            && report.recovery.state != RecoveryState::Met
        {
            return self.fail(EnableError::RecoveryUnmet);
        }
        let (Some(record), Some(private_key)) = (self.record.clone(), self.private_key.clone())
        else {
            return self.fail(EnableError::NotFinished);
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
            Err(error) => return self.fail(error),
        };
        // Plain copies already stored start their encrypt transition in the same transaction.
        let (bucket, now_ms) = (&self.input.bucket, self.input.now_ms);
        match encrypt_rows(bucket, &self.settings, &record, self.compression, now_ms) {
            Ok(rows) => writes.extend(rows),
            Err(error) => return self.fail(error),
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
            Err(error) => return self.fail(error),
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
        self.state = EnableState::WriteSettings;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn write_vault(&mut self) -> Effects {
        let entry = self.record.as_ref().and_then(|record| record.vault_entry);
        let (Some(id), Some(secret)) = (entry, self.private_key.clone()) else {
            return self.commit();
        };
        self.state = EnableState::WriteVault;
        smallvec![Effect::Storage(StorageEffect::VaultWrite {
            entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            secret,
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(EnableError::NotFinished);
        };
        self.state = EnableState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    /// The operation keeps no key after the commit; the output hands its only handle on.
    fn finish(&mut self) -> Effects {
        self.private_key = None;
        self.state = EnableState::Finish;
        self.output = self.result.take().map(Ok);
        smallvec![Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::MigrateCompression,
            after: std::time::Duration::ZERO,
        })]
    }
}

impl Operation for EnableEncryptionOperation {
    type Output = EnableResult;
    type Error = EnableError;

    fn start(&mut self) -> Effects {
        if self.input.mode == EncryptionMode::Off || self.input.max_unlock_ms == Some(0) {
            return self.fail(EnableError::InvalidMode);
        }
        self.state = EnableState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone());
        }
        match (self.state, event) {
            (EnableState::PrepareAbe, event) => self.prepare_abe(event),
            (
                EnableState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = EnableState::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (EnableState::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_bucket(values)
            }
            (
                EnableState::CheckUploads,
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.check_uploads(&values),
            (
                EnableState::ScanVersions,
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.versions_scanned(values),
            (EnableState::ReadSizes, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.sizes_read(values)
            }
            (
                EnableState::ReadTransition,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.check_transition(value),
            (EnableState::ReadGrants, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let grants = values
                    .iter()
                    .map(|(_, value)| BucketHolder::from_bytes(value.as_ref()))
                    .collect::<Result<Vec<_>, _>>();
                match grants {
                    Ok(grants) => {
                        self.grants = grants;
                        self.generate()
                    }
                    Err(error) => self.fail(error),
                }
            }
            (
                EnableState::GenerateKey,
                Event::Blob(BlobEvent::BucketKeyGenerated {
                    public_key,
                    private_key,
                }),
            ) => self.seal(public_key, private_key),
            (EnableState::SealCopies, Event::Blob(BlobEvent::CopiesSealed { copies })) => {
                self.write_settings(copies)
            }
            (EnableState::WriteSettings, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.write_vault()
            }
            (EnableState::WriteVault, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.commit()
            }
            (
                EnableState::CommitTransaction,
                Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
            ) if Some(txn_id) == self.txn_id => {
                self.txn_id = None;
                self.finish()
            }
            (EnableState::Finish | EnableState::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(EnableError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, EnableState::Finish | EnableState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            EnableState::Finish | EnableState::Error => {
                self.output.unwrap_or(Err(EnableError::NotFinished))
            }
            _ => Err(EnableError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.private_key = None;
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
#[path = "encryption_tests.rs"]
mod tests;
