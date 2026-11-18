//! Rotates bucket keys or changes encryption mode, cipher or block keys, including decryption.
//! Stored copies move afterwards through the bucket's transition record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::abe::PrepareAbe;
use crate::s3::bucket::key::rows::{
    Row, SettingsError, authority_keys, copy_targets, full_scan, generation_rows, group_bucket_key,
    parse_authority, uploads_open,
};
use crate::s3::object::put::abe::keyed;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE, TRANSITION_KEYSPACE,
    TRANSITION_QUEUE_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values as row_values;
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
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeState {
    ReadBucket,
    CheckUploads,
    ReadKey,
    CheckUnlocked,
    ReadGrants,
    GenerateKey,
    PrepareAbe,
    SealCopies,
    ReadUnlocked,
    WriteRows,
    DeleteIndex,
    WriteVault,
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
    #[error("unexpected event in state {state:?}: {received}")]
    InvalidStateEvent {
        state: ChangeState,
        received: &'static str,
    },
    #[error("the encryption change did not finish")]
    NotFinished,
}

impl From<IoError> for ChangeError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type ChangeTurn = Result<Next<ChangeResult>, ChangeError>;

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
    abe: Option<PrepareAbe>,
    info: Option<BucketInfo>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    active: Option<BucketKeyRecord>,
    grants: Vec<BucketHolder>,
    new_key: Option<(BucketKeyRecord, SharedSecret)>,
    /// The node vault copy this change writes.
    vault: Option<(Ulid, SharedSecret)>,
    result: Option<ChangeResult>,
}

impl ChangeEncryptionOperation {
    pub fn new(input: ChangeInput) -> Self {
        Self {
            input,
            state: ChangeState::ReadBucket,
            abe: None,
            info: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            active: None,
            grants: Vec::new(),
            new_key: None,
            vault: None,
            result: None,
        }
    }

    fn call(&mut self, state: ChangeState, call: IoCall) -> ChangeTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn refused(&self, received: Event) -> ChangeTurn {
        match received {
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => Err(ChangeError::InvalidStateEvent {
                state: self.state,
                received: other.label(),
            }),
        }
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

    fn read_bucket(&mut self, values: Vec<Option<Value>>) -> ChangeTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = match parse_authority(keyed(&values), realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return Err(error.into()),
        };
        if !state.admins.contains(&self.input.caller) {
            return Err(ChangeError::NotAdmin);
        }
        if !state.settings.is_encrypted() {
            return Err(ChangeError::NotEncrypted);
        }
        if state.settings.storage_generation != self.input.expected_generation {
            return Err(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: state.settings.storage_generation,
            }
            .into());
        }
        if self.input.max_unlock_ms == Some(Some(0)) {
            return Err(BucketKeyError::InvalidDuration.into());
        }
        self.admins = state.admins;
        self.info = Some(state.info);
        self.settings = state.settings;
        let wanted = self.wanted();
        if wanted.0 == EncryptionMode::NodeManaged
            && matches!(self.input.max_unlock_ms, Some(Some(_)))
        {
            return Err(BucketKeyError::ManagedLimit.into());
        }
        let current = (
            self.settings.mode,
            self.settings.cipher,
            self.settings.block_keys,
        );
        if self.input.change != KeyChange::Rotate && wanted == current {
            return Err(ChangeError::Unchanged);
        }
        self.call(ChangeState::CheckUploads, full_scan(UPLOAD_KEYSPACE, None))
    }

    /// Writes that captured the old plan fail at publication on the storage generation; open
    /// multipart uploads would publish parts sealed for it, so they must finish first.
    fn check_uploads(&mut self, uploads: &[(Key, Value)]) -> ChangeTurn {
        if uploads_open(uploads, &self.input.bucket) {
            return Err(ChangeError::OpenUploads);
        }
        let Some(active) = self.settings.active_key() else {
            return Err(ChangeError::NotEncrypted);
        };
        let bucket: Key = self.input.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (BUCKET_KEY_KEYSPACE.to_string(), active.key().into()),
            (TRANSITION_KEYSPACE.to_string(), bucket),
        ];
        self.call(ChangeState::ReadKey, IoCall::Read(reads))
    }

    fn read_key(&mut self, [record, transition]: [Option<Value>; 2]) -> ChangeTurn {
        let record = record.ok_or(ChangeError::NotFinished)?;
        let transition = transition.map(|row| EncryptionTransition::from_bytes(row.as_ref()));
        match transition.transpose() {
            Ok(Some(transition)) if transition.finished_at_ms.is_none() => {
                return Err(ChangeError::TransitionRunning);
            }
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
        let record = match BucketKeyRecord::from_bytes(record.as_ref()) {
            Ok(record) => record,
            Err(error) => return Err(error.into()),
        };
        let (mode, ..) = self.wanted();
        let key = record.key;
        let leaves_vault = mode == EncryptionMode::NodeManaged && record.vault_entry.is_none();
        self.active = Some(record);
        if leaves_vault {
            // Leaving `vault_locked` needs the unlocked key for the node vault copy.
            let read = Effect::Blob(BlobEffect::ReadUnlockedKey { key });
            return self.call(ChangeState::ReadUnlocked, read.into());
        }
        // Every other change moves archives that only the unlocked source key opens.
        let status = Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id,
        });
        self.call(ChangeState::CheckUnlocked, status.into())
    }

    fn check_unlocked(&mut self, generations: &[UnlockStatus]) -> ChangeTurn {
        let Some(key) = self.active.as_ref().map(|record| record.key) else {
            return Err(ChangeError::NotFinished);
        };
        if !generations
            .iter()
            .any(|status| status.key == key && status.active)
        {
            return Err(BucketKeyError::Locked(key.bucket_id).into());
        }
        if self.new_generation() {
            let prefix = key.bucket_id.to_bytes().to_vec().into();
            let scan = full_scan(BUCKET_HOLDER_KEYSPACE, Some(prefix));
            return self.call(ChangeState::ReadGrants, scan);
        }
        self.write_rows(None, Vec::new())
    }

    fn seal(&mut self, public_key: [u8; 32], private_key: SharedSecret) -> ChangeTurn {
        let (Some(active), Some(_info)) = (self.active.as_ref(), self.info.as_ref()) else {
            return Err(ChangeError::NotFinished);
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
        let mut abe = PrepareAbe::new(self.input.realm_id, self.input.node_id, key, private_key);
        let call = abe.first()?;
        self.abe = Some(abe);
        self.call(ChangeState::PrepareAbe, call)
    }

    fn prepare_abe(&mut self, result: IoResult) -> ChangeTurn {
        let abe = self.abe.as_mut().ok_or(ChangeError::NotFinished)?;
        if let Some(call) = abe.next::<ChangeError>(result)? {
            return Ok(Next::Call(call));
        }
        self.abe = None;
        let (record, private_key) = self.new_key.as_ref().ok_or(ChangeError::NotFinished)?;
        let (key, public_key, private_key) = (record.key, record.public_key, private_key.clone());
        let report = self.report(&[]);
        let holders = copy_targets(&report, &self.input.lookups);
        if holders.is_empty() {
            return self.write_rows(None, Vec::new());
        }
        let seal = BlobEffect::SealHolderCopies {
            key,
            public_key,
            private_key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        };
        self.call(ChangeState::SealCopies, Effect::Blob(seal).into())
    }

    fn report(&self, copies: &[SealedCopy]) -> HolderReport {
        let creator = self.info.as_ref().map(|info| info.created_by);
        let (input, grants) = (&self.input, &self.grants);
        let creator = creator.unwrap_or_default();
        resolve_holders(creator, &self.admins, grants, &input.lookups, copies)
    }

    /// The new settings, key rows and transition, written in the change's transaction.
    fn write_rows(&mut self, secret: Option<SharedSecret>, copies: Vec<SealedCopy>) -> ChangeTurn {
        let writes = self.rows(secret, copies)?;
        self.call(ChangeState::WriteRows, IoCall::Write(writes))
    }

    /// A bucket turned `off` leaves the group index, so member grants no longer visit it.
    fn delete_index(&mut self) -> ChangeTurn {
        if self.wanted().0 != EncryptionMode::Off {
            return self.write_vault();
        }
        let key = group_bucket_key(self.input.group_id, &self.input.bucket);
        let delete = vec![(GROUP_ENCRYPTED_KEYSPACE.to_string(), key)];
        self.call(ChangeState::DeleteIndex, IoCall::Delete(delete))
    }

    fn write_vault(&mut self) -> ChangeTurn {
        let Some((id, secret)) = self.vault.take() else {
            return self.finish();
        };
        let write = StorageEffect::VaultWrite {
            entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            secret,
            txn_id: None,
        };
        self.call(ChangeState::WriteVault, Effect::Storage(write).into())
    }

    /// A change that moves copies wakes the transition worker once it committed.
    fn finish(&mut self) -> ChangeTurn {
        let output = self.result.take().ok_or(ChangeError::NotFinished)?;
        if output.transition.is_none() {
            return Ok(Next::Done(output));
        }
        let wake = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::MigrateCompression,
            after: Duration::ZERO,
        });
        Ok(Next::Then {
            output,
            calls: vec![wake.into()],
        })
    }

    fn grants_read(&mut self, rows: Vec<(Key, Value)>) -> ChangeTurn {
        self.grants = rows
            .iter()
            .map(|(_, value)| BucketHolder::from_bytes(value.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let generate = Effect::Blob(BlobEffect::GenerateBucketKey);
        self.call(ChangeState::GenerateKey, generate.into())
    }

    fn copies_sealed(&mut self, copies: Vec<SealedCopy>) -> ChangeTurn {
        let wanted = self.new_key.as_ref().map(|(record, _)| record.key);
        if copies.iter().any(|copy| Some(copy.key) != wanted) {
            return Err(ChangeError::NotFinished);
        }
        self.write_rows(None, copies)
    }
}

impl Operation for ChangeEncryptionOperation {
    type Output = ChangeResult;
    type Error = ChangeError;

    fn first(&mut self) -> ChangeTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        self.call(ChangeState::ReadBucket, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> ChangeTurn {
        match self.state {
            ChangeState::ReadBucket => self.read_bucket(result.values()?),
            ChangeState::CheckUploads => self.check_uploads(&result.page()?.0),
            ChangeState::ReadKey => self.read_key(row_values::<2>(result)?),
            ChangeState::CheckUnlocked => match result.event()? {
                Event::Blob(BlobEvent::KeyStatus { generations }) => {
                    self.check_unlocked(&generations)
                }
                other => self.refused(other),
            },
            ChangeState::ReadGrants => self.grants_read(result.page()?.0),
            ChangeState::GenerateKey => match result.event()? {
                Event::Blob(BlobEvent::BucketKeyGenerated {
                    public_key,
                    private_key,
                }) => self.seal(public_key, private_key),
                other => self.refused(other),
            },
            ChangeState::PrepareAbe => self.prepare_abe(result),
            ChangeState::SealCopies => match result.event()? {
                Event::Blob(BlobEvent::CopiesSealed { copies }) => self.copies_sealed(copies),
                other => self.refused(other),
            },
            ChangeState::ReadUnlocked => match result.event()? {
                Event::Blob(BlobEvent::UnlockedKeyRead { key, private_key })
                    if self.settings.active_key() == Some(key) =>
                {
                    self.write_rows(Some(private_key), Vec::new())
                }
                other => self.refused(other),
            },
            ChangeState::WriteRows => {
                result.done()?;
                self.delete_index()
            }
            ChangeState::DeleteIndex => {
                result.done()?;
                self.write_vault()
            }
            ChangeState::WriteVault => match result.event()? {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.finish(),
                other => self.refused(other),
            },
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// A failed change keeps no key.
    fn abort(&mut self) -> Vec<IoCall> {
        self.new_key = None;
        self.vault = None;
        Vec::new()
    }
}

#[path = "rows.rs"]
mod rows;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
