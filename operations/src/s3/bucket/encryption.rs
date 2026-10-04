//! Enables encryption on a bucket: generates its first key generation, seals copies for its
//! holders and stores settings and keys in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{
    SettingsError, copy_targets, generation_rows, parse_settings, settings_read, uploads_open,
};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, UPLOAD_KEYSPACE};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketEncryption, BucketHolder, BucketKeyError, BucketKeyRecord,
    BucketKeyRef, EncryptionMode, SealedCopy,
};
use aruna_core::structs::storage::holders::{
    HolderReport, KeyLookup, RecoveryState, resolve_holders,
};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnableState {
    Init,
    StartTransaction,
    ReadBucket,
    CheckUploads,
    ReadGrants,
    GenerateKey,
    SealCopies,
    WriteSettings,
    WriteVault,
    CommitTransaction,
    Finish,
    Error,
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
    #[error("encryption needs the mode node_managed or vault_locked")]
    InvalidMode,
    #[error("the bucket has open multipart uploads")]
    OpenUploads,
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

/// What the caller resolved before the transaction: admins and key directory answers.
#[derive(Debug, PartialEq)]
pub struct EnableInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub mode: EncryptionMode,
    pub cipher: BlockCipher,
    pub block_keys: BlockKeys,
    pub max_unlock_ms: Option<u64>,
    /// The storage generation the caller read; another one means a concurrent change.
    pub expected_generation: u64,
    pub admins: BTreeSet<UserId>,
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
    txn_id: Option<TxnId>,
    creator: Option<UserId>,
    settings: BucketEncryption,
    grants: Vec<BucketHolder>,
    record: Option<BucketKeyRecord>,
    private_key: Option<SharedSecret>,
    result: Option<EnableResult>,
    output: Option<Result<EnableResult, EnableError>>,
}

impl EnableEncryptionOperation {
    pub fn new(input: EnableInput) -> Self {
        Self {
            input,
            state: EnableState::Init,
            txn_id: None,
            creator: None,
            settings: BucketEncryption::default(),
            grants: Vec::new(),
            record: None,
            private_key: None,
            result: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<EnableError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.state = EnableState::Error;
        effects
    }

    fn read_bucket(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (info, settings) = match parse_settings(values, self.input.group_id) {
            Ok(read) => read,
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
        resolve_holders(creator, &input.admins, &self.grants, &input.lookups, copies)
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
        let writes = match rows {
            Ok(writes) => writes,
            Err(error) => return self.fail(error),
        };
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
        smallvec![]
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
            (
                EnableState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = EnableState::ReadBucket;
                smallvec![settings_read(&self.input.bucket, Some(txn_id))]
            }
            (EnableState::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_bucket(values)
            }
            (
                EnableState::CheckUploads,
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.check_uploads(&values),
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
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
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
