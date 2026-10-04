//! Changes the encryption of an encrypted bucket. Its stored copies move afterwards through
//! the bucket's transition record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{
    Row, SettingsError, authority_read, generation_rows, parse_authority, uploads_open,
};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_KEY_KEYSPACE, TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketEncryption, BucketHolder, BucketKeyError, BucketKeyRecord,
    EncryptionMode, KeyState, SealPlan, SealedCopy,
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
    WriteRows,
    Commit,
    Finish,
    Error,
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
    #[error("the bucket already uses these settings")]
    Unchanged,
    #[error("the bucket has open multipart uploads")]
    OpenUploads,
    #[error("the key holders do not meet the recovery rule")]
    RecoveryUnmet,
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
    pub change: KeyChange,
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
    txn_id: Option<TxnId>,
    info: Option<BucketInfo>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    active: Option<BucketKeyRecord>,
    grants: Vec<BucketHolder>,
    new_key: Option<(BucketKeyRecord, SharedSecret)>,
    result: Option<ChangeResult>,
    output: Option<Result<ChangeResult, ChangeError>>,
}

impl ChangeEncryptionOperation {
    pub fn new(input: ChangeInput) -> Self {
        Self {
            input,
            state: ChangeState::Init,
            txn_id: None,
            info: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            active: None,
            grants: Vec::new(),
            new_key: None,
            result: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<ChangeError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.new_key = None;
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

    /// Rotation and leaving the node vault need a new generation.
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
        if !state.settings.is_encrypted() {
            return self.fail(ChangeError::NotEncrypted);
        }
        if state.settings.storage_generation != self.input.expected_generation {
            return self.fail(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: state.settings.storage_generation,
            });
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
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: active.key().into(),
            txn_id: self.txn_id,
        })]
    }

    fn read_key(&mut self, value: Option<Value>) -> Effects {
        let record = value.map(|value| BucketKeyRecord::from_bytes(value.as_ref()));
        let record = match record.transpose() {
            Ok(Some(record)) => record,
            Ok(None) => return self.fail(ChangeError::NotFinished),
            Err(error) => return self.fail(error),
        };
        let vault_entry = record.vault_entry;
        self.active = Some(record);
        let (mode, ..) = self.wanted();
        let leaves_vault = mode == EncryptionMode::NodeManaged && vault_entry.is_none();
        if self.new_generation() || leaves_vault {
            return self.fail(BucketKeyError::Unsupported);
        }
        self.write_rows(None, Vec::new())
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
            (ChangeState::ReadKey, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.read_key(value)
            }
            (ChangeState::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
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
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[path = "rotate_rows.rs"]
mod rows;
