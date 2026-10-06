//! Rotates bucket keys or changes encryption mode, cipher or block keys, including decryption.
//! Stored copies move afterwards through the bucket's transition record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{
    Row, SettingsError, authority_read, copy_targets, generation_rows, group_bucket_key,
    parse_authority, uploads_open,
};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE, TRANSITION_KEYSPACE,
    TRANSITION_QUEUE_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketEncryption, BucketHolder, BucketKeyError, BucketKeyRecord,
    BucketKeyRef, EncryptionMode, KeyState, SealPlan, SealedCopy, UnlockStatus,
};
use aruna_core::structs::storage::holders::{
    HolderReport, KeyLookup, RecoveryState, resolve_holders,
};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionTarget,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeState {
    Init,
    StartTransaction,
    ReadBucket,
    CheckUploads,
    ReadKey,
    CheckUnlocked,
    ReadGrants,
    GenerateKey,
    SealCopies,
    ReadUnlocked,
    WriteRows,
    WriteVault,
    Commit,
    Finish,
    Error,
    PrepareAbe,
    DeleteIndex,
}

/// The change a holder or admin asked for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyChange {
    /// A new key generation; archives get grants for it and lose the old ones.
    Rotate,
    /// New settings: another mode, `off`, or another cipher or block-key mode.
    Settings {
        mode: EncryptionMode,
        cipher: BlockCipher,
        block_keys: BlockKeys,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum ChangeError {
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
    #[error("the bucket does not encrypt; enable encryption instead")]
    NotEncrypted,
    #[error("the caller is no longer a group admin")]
    NotAdmin,
    #[error("the bucket already uses these settings")]
    Unchanged,
    #[error("the bucket has open multipart uploads")]
    OpenUploads,
    #[error("the key holders do not meet the recovery rule")]
    RecoveryUnmet,
    /// Replacing an unfinished transition would strand the copies it still has to move.
    #[error("the bucket's stored copies are still moving to a new encryption")]
    TransitionRunning,
    #[error("unexpected event in state {state:?}: {received:?}")]
    InvalidStateEvent {
        state: ChangeState,
        received: Box<Event>,
    },
    #[error("the encryption change did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct ChangeInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// The requesting user; they must hold the group admin role inside the transaction.
    pub caller: UserId,
    pub change: KeyChange,
    pub max_unlock_ms: Option<Option<u64>>,
    /// The storage generation the caller read; another one means a concurrent change.
    pub expected_generation: u64,
    /// Key directory answers for the holders of a new generation.
    pub lookups: BTreeMap<UserId, KeyLookup>,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct ChangeResult {
    pub settings: BucketEncryption,
    /// The transition that moves stored copies; none when no copy changes.
    pub transition: Option<EncryptionTransition>,
    /// The new generation and its key, handed on so the caller installs it.
    pub key: Option<(BucketKeyRecord, SharedSecret)>,
}

#[derive(Debug, PartialEq)]
pub struct ChangeEncryptionOperation {
    input: ChangeInput,
    state: ChangeState,
    abe: Option<crate::s3::bucket::key::abe::PrepareAbeOperation>,
    txn_id: Option<TxnId>,
    info: Option<BucketInfo>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    active: Option<BucketKeyRecord>,
    grants: Vec<BucketHolder>,
    new_key: Option<(BucketKeyRecord, SharedSecret)>,
    /// The node vault copy this change writes.
    vault: Option<(Ulid, SharedSecret)>,
    result: Option<ChangeResult>,
    output: Option<Result<ChangeResult, ChangeError>>,
}

impl ChangeEncryptionOperation {
    pub fn new(input: ChangeInput) -> Self {
        Self {
            input,
            state: ChangeState::Init,
            abe: None,
            txn_id: None,
            info: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            active: None,
            grants: Vec::new(),
            new_key: None,
            vault: None,
            result: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<ChangeError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.new_key = None;
        self.vault = None;
        let effects = self.abort();
        self.state = ChangeState::Error;
        effects
    }

    /// Mode and settings the change asks for.
    fn wanted(&self) -> (EncryptionMode, BlockCipher, BlockKeys) {
        match self.input.change {
            KeyChange::Rotate => (
                self.settings.mode,
                self.settings.cipher,
                self.settings.block_keys,
            ),
            KeyChange::Settings {
                mode,
                cipher,
                block_keys,
            } => (mode, cipher, block_keys),
        }
    }

    /// Rotation and leaving the node vault need a new generation; the vault copy of the old
    /// one is removed once no archive needs it.
    fn new_generation(&self) -> bool {
        let (mode, ..) = self.wanted();
        self.input.change == KeyChange::Rotate
            || (self.settings.mode == EncryptionMode::NodeManaged
                && mode == EncryptionMode::VaultLocked)
    }

    fn read_bucket(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = match parse_authority(values, realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return self.fail(error),
        };
        if !state.admins.contains(&self.input.caller) {
            return self.fail(ChangeError::NotAdmin);
        }
        if !state.settings.is_encrypted() {
            return self.fail(ChangeError::NotEncrypted);
        }
        if state.settings.storage_generation != self.input.expected_generation {
            return self.fail(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: state.settings.storage_generation,
            });
        }
        if self.input.max_unlock_ms == Some(Some(0)) {
            return self.fail(BucketKeyError::InvalidDuration);
        }
        self.admins = state.admins;
        self.info = Some(state.info);
        self.settings = state.settings;
        let wanted = self.wanted();
        let current = (
            self.settings.mode,
            self.settings.cipher,
            self.settings.block_keys,
        );
        if self.input.change != KeyChange::Rotate && wanted == current {
            return self.fail(ChangeError::Unchanged);
        }
        self.state = ChangeState::CheckUploads;
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

    /// Writes that captured the old plan fail at publication on the storage generation; open
    /// multipart uploads would publish parts sealed for it, so they must finish first.
    fn check_uploads(&mut self, uploads: &[(Key, Value)]) -> Effects {
        if uploads_open(uploads, &self.input.bucket) {
            return self.fail(ChangeError::OpenUploads);
        }
        let Some(active) = self.settings.active_key() else {
            return self.fail(ChangeError::NotEncrypted);
        };
        self.state = ChangeState::ReadKey;
        let bucket: Key = self.input.bucket.as_bytes().to_vec().into();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (BUCKET_KEY_KEYSPACE.to_string(), active.key().into()),
                (TRANSITION_KEYSPACE.to_string(), bucket),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn read_key(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let mut rows = values.into_iter().map(|(_, value)| value);
        let (Some(Some(record)), Some(transition), None) = (rows.next(), rows.next(), rows.next())
        else {
            return self.fail(ChangeError::NotFinished);
        };
        let transition = transition.map(|row| EncryptionTransition::from_bytes(row.as_ref()));
        match transition.transpose() {
            Ok(Some(transition)) if transition.finished_at_ms.is_none() => {
                return self.fail(ChangeError::TransitionRunning);
            }
            Ok(_) => {}
            Err(error) => return self.fail(error),
        }
        let record = match BucketKeyRecord::from_bytes(record.as_ref()) {
            Ok(record) => record,
            Err(error) => return self.fail(error),
        };
        let (mode, ..) = self.wanted();
        let key = record.key;
        let leaves_vault = mode == EncryptionMode::NodeManaged && record.vault_entry.is_none();
        self.active = Some(record);
        if leaves_vault {
            // Leaving `vault_locked` needs the unlocked key for the node vault copy.
            self.state = ChangeState::ReadUnlocked;
            return smallvec![Effect::Blob(BlobEffect::ReadUnlockedKey { key })];
        }
        // Every other change moves archives that only the unlocked source key opens.
        self.state = ChangeState::CheckUnlocked;
        smallvec![Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id
        })]
    }

    fn check_unlocked(&mut self, generations: &[UnlockStatus]) -> Effects {
        let Some(key) = self.active.as_ref().map(|record| record.key) else {
            return self.fail(ChangeError::NotFinished);
        };
        if !generations
            .iter()
            .any(|status| status.key == key && status.active)
        {
            return self.fail(BucketKeyError::Locked(key.bucket_id));
        }
        if self.new_generation() {
            self.state = ChangeState::ReadGrants;
            let prefix = key.bucket_id.to_bytes().to_vec().into();
            return self.scan(BUCKET_HOLDER_KEYSPACE, Some(prefix));
        }
        self.write_rows(None, Vec::new())
    }

    fn seal(&mut self, public_key: [u8; 32], private_key: SharedSecret) -> Effects {
        let (Some(active), Some(_info)) = (self.active.as_ref(), self.info.as_ref()) else {
            return self.fail(ChangeError::NotFinished);
        };
        let key = BucketKeyRef::new(active.key.bucket_id, active.key.generation + 1);
        let record_id = Ulid::generate();
        let mut record = BucketKeyRecord::new(key, record_id, public_key, self.input.now_ms);
        let (mode, ..) = self.wanted();
        // A new `vault_locked` generation never enters the node vault.
        if mode == EncryptionMode::NodeManaged {
            record.vault_entry = Some(record_id);
        }
        self.new_key = Some((record, private_key.clone()));
        let Some(txn) = self.txn_id else {
            return self.fail(ChangeError::NotFinished);
        };
        let mut abe = crate::s3::bucket::key::abe::PrepareAbeOperation::new(
            self.input.realm_id,
            self.input.node_id,
            key,
            private_key,
            txn,
        );
        self.state = ChangeState::PrepareAbe;
        let effects = abe.start();
        self.abe = Some(abe);
        effects
    }

    fn prepare_abe(&mut self, event: Event) -> Effects {
        let Some(abe) = self.abe.as_mut() else {
            return self.fail(ChangeError::NotFinished);
        };
        let effects = abe.step(event);
        if !abe.is_complete() {
            return effects;
        }
        let Some(abe) = self.abe.take() else {
            return self.fail(ChangeError::NotFinished);
        };
        if let Err(error) = abe.finalize() {
            return self.fail(error);
        }
        let Some((key, public_key)) = self.new_key.as_ref().map(|(r, _)| (r.key, r.public_key))
        else {
            return self.fail(ChangeError::NotFinished);
        };
        let Some(private_key) = self.new_key.as_ref().map(|(_, s)| s.clone()) else {
            return self.fail(ChangeError::NotFinished);
        };
        let report = self.report(&[]);
        let holders = copy_targets(&report, &self.input.lookups);
        if holders.is_empty() {
            return self.write_rows(None, Vec::new());
        }
        self.state = ChangeState::SealCopies;
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
        let creator = self.info.as_ref().map(|info| info.created_by);
        let (input, grants) = (&self.input, &self.grants);
        let creator = creator.unwrap_or_default();
        resolve_holders(creator, &self.admins, grants, &input.lookups, copies)
    }

    /// The new settings, key rows and transition, written in the change's transaction.
    fn write_rows(&mut self, secret: Option<SharedSecret>, copies: Vec<SealedCopy>) -> Effects {
        match self.rows(secret, copies) {
            Ok(writes) => {
                self.state = ChangeState::WriteRows;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.fail(error),
        }
    }
}

impl ChangeEncryptionOperation {
    /// A bucket turned `off` leaves the group index, so member grants no longer visit it.
    fn delete_index(&mut self) -> Effects {
        if self.wanted().0 != EncryptionMode::Off {
            return self.write_vault();
        }
        self.state = ChangeState::DeleteIndex;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: GROUP_ENCRYPTED_KEYSPACE.to_string(),
            key: group_bucket_key(self.input.group_id, &self.input.bucket),
            txn_id: self.txn_id,
        })]
    }

    fn write_vault(&mut self) -> Effects {
        let Some((id, secret)) = self.vault.take() else {
            return self.commit();
        };
        self.state = ChangeState::WriteVault;
        smallvec![Effect::Storage(StorageEffect::VaultWrite {
            entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            secret,
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ChangeError::NotFinished);
        };
        self.state = ChangeState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    /// A change that moves copies wakes the transition worker.
    fn finish(&mut self) -> Effects {
        self.txn_id = None;
        self.state = ChangeState::Finish;
        let result = self.result.take();
        let moves = result
            .as_ref()
            .is_some_and(|result| result.transition.is_some());
        self.output = result.map(Ok);
        match moves {
            true => smallvec![Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::MigrateCompression,
                after: Duration::ZERO,
            })],
            false => smallvec![],
        }
    }
}

impl Operation for ChangeEncryptionOperation {
    type Output = ChangeResult;
    type Error = ChangeError;

    fn start(&mut self) -> Effects {
        self.state = ChangeState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone());
        }
        match (self.state, event) {
            (ChangeState::PrepareAbe, event) => self.prepare_abe(event),
            (
                ChangeState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = ChangeState::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                let bucket = &self.input.bucket;
                smallvec![authority_read(bucket, realm_id, group_id, Some(txn_id))]
            }
            (ChangeState::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_bucket(values)
            }
            (
                ChangeState::CheckUploads,
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.check_uploads(&values),
            (ChangeState::ReadKey, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_key(values)
            }
            (ChangeState::CheckUnlocked, Event::Blob(BlobEvent::KeyStatus { generations })) => {
                self.check_unlocked(&generations)
            }
            (ChangeState::ReadGrants, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let grants = values
                    .iter()
                    .map(|(_, value)| BucketHolder::from_bytes(value.as_ref()))
                    .collect::<Result<Vec<_>, _>>();
                match grants {
                    Ok(grants) => {
                        self.grants = grants;
                        self.state = ChangeState::GenerateKey;
                        smallvec![Effect::Blob(BlobEffect::GenerateBucketKey)]
                    }
                    Err(error) => self.fail(error),
                }
            }
            (
                ChangeState::GenerateKey,
                Event::Blob(BlobEvent::BucketKeyGenerated {
                    public_key,
                    private_key,
                }),
            ) => self.seal(public_key, private_key),
            (ChangeState::SealCopies, Event::Blob(BlobEvent::CopiesSealed { copies })) => {
                let wanted = self.new_key.as_ref().map(|(record, _)| record.key);
                if copies.iter().any(|copy| Some(copy.key) != wanted) {
                    return self.fail(ChangeError::NotFinished);
                }
                self.write_rows(None, copies)
            }
            (
                ChangeState::ReadUnlocked,
                Event::Blob(BlobEvent::UnlockedKeyRead { key, private_key }),
            ) if self.settings.active_key() == Some(key) => {
                self.write_rows(Some(private_key), Vec::new())
            }
            (ChangeState::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.delete_index()
            }
            (ChangeState::DeleteIndex, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.write_vault()
            }
            (ChangeState::WriteVault, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.commit()
            }
            (
                ChangeState::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
            ) if Some(txn_id) == self.txn_id => self.finish(),
            (ChangeState::Finish | ChangeState::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(ChangeError::InvalidStateEvent {
                state,
                received: Box::new(received),
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ChangeState::Finish | ChangeState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ChangeState::Finish | ChangeState::Error => {
                self.output.unwrap_or(Err(ChangeError::NotFinished))
            }
            _ => Err(ChangeError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.new_key = None;
        self.vault = None;
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[path = "rows.rs"]
mod rows;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
