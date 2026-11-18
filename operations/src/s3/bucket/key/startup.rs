//! Opens the node-managed bucket keys at startup, from the node vault into the unlock registry.
//! Vault-locked keys stay locked; a key that fails to open stays locked and is reported.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::install::{InstallError, InstallInput, InstallKeyOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, EncryptionMode, KeyState, UnlockStatus,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

const SCAN_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupStep {
    ScanSettings,
    ScanKeys,
    ReadVault,
    Install,
}

#[derive(Debug, Error, PartialEq)]
pub enum StartupError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("opening the managed keys did not finish")]
    NotFinished,
}

impl From<IoError> for StartupError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// What startup opened. A failed key stays locked; its reason names no key material.
#[derive(Debug, Default, PartialEq)]
pub struct ManagedKeys {
    pub opened: Vec<BucketKeyRef>,
    pub failed: Vec<(BucketKeyRef, String)>,
    /// Settings or key rows that do not decode; their buckets stay locked.
    pub unreadable: usize,
}

#[derive(Debug, Error, PartialEq)]
enum VaultReadError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the vault answered another entry")]
    OtherEntry,
}

impl From<IoError> for VaultReadError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Reads one key from the node vault. A child, so a failed read fails only that key.
#[derive(Debug, PartialEq)]
struct VaultKeyRead {
    entry: VaultEntry,
}

impl Operation for VaultKeyRead {
    type Output = Option<SharedSecret>;
    type Error = VaultReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, VaultReadError> {
        let read = StorageEffect::VaultRead {
            entry: self.entry,
            txn_id: None,
        };
        Ok(Next::Call(Effect::Storage(read).into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, VaultReadError> {
        match result.event()? {
            Event::Storage(StorageEvent::VaultResult { entry, secret })
                if entry.id == self.entry.id =>
            {
                Ok(Next::Done(secret.map(SharedSecret::new)))
            }
            _ => Err(VaultReadError::OtherEntry),
        }
    }
}

type StartupTurn = Result<Next<ManagedKeys>, StartupError>;

#[derive(Debug, PartialEq)]
pub struct OpenManagedOperation {
    step: StartupStep,
    buckets: Vec<Ulid>,
    scanning: Option<Ulid>,
    records: Vec<BucketKeyRecord>,
    current: Option<BucketKeyRecord>,
    keys: ManagedKeys,
}

impl Default for OpenManagedOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenManagedOperation {
    pub fn new() -> Self {
        Self {
            step: StartupStep::ScanSettings,
            buckets: Vec::new(),
            scanning: None,
            records: Vec::new(),
            current: None,
            keys: ManagedKeys::default(),
        }
    }

    fn call(&mut self, step: StartupStep, call: IoCall) -> StartupTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scan_from(key_space: &str, prefix: Option<Ulid>, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: key_space.to_string(),
            prefix: prefix
                .map_or_else(Vec::new, |id| id.to_bytes().to_vec())
                .into(),
            after,
            limit: SCAN_LIMIT,
        }
    }

    /// Scans the key rows of the next managed bucket, then opens the collected keys.
    fn next_bucket(&mut self) -> StartupTurn {
        let Some(bucket_id) = self.buckets.pop() else {
            return self.next_key();
        };
        self.scanning = Some(bucket_id);
        let scan = Self::scan_from(BUCKET_KEY_KEYSPACE, Some(bucket_id), None);
        self.call(StartupStep::ScanKeys, scan)
    }

    fn next_key(&mut self) -> StartupTurn {
        while let Some(record) = self.records.pop() {
            let Some(id) = record.vault_entry else {
                continue;
            };
            self.current = Some(record);
            let read = VaultKeyRead {
                entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            };
            return self.call(StartupStep::ReadVault, IoCall::child(read));
        }
        Ok(Next::Done(std::mem::take(&mut self.keys)))
    }

    fn failed(&mut self, reason: String) -> StartupTurn {
        if let Some(record) = self.current.take() {
            self.keys.failed.push((record.key, reason));
        }
        self.next_key()
    }

    fn settings_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> StartupTurn {
        for (_, value) in values {
            match BucketEncryption::from_bytes(value.as_ref()) {
                Ok(BucketEncryption {
                    mode: EncryptionMode::NodeManaged,
                    bucket_id: Some(bucket_id),
                    ..
                }) => self.buckets.push(bucket_id),
                Ok(_) => {}
                Err(_) => self.keys.unreadable += 1,
            }
        }
        match next {
            Some(after) => {
                let scan = Self::scan_from(BUCKET_ENCRYPTION_KEYSPACE, None, Some(after));
                Ok(Next::Call(scan))
            }
            None => self.next_bucket(),
        }
    }

    fn keys_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> StartupTurn {
        for (_, value) in values {
            match BucketKeyRecord::from_bytes(value.as_ref()) {
                Ok(record) if record.state != KeyState::Retired && record.vault_entry.is_some() => {
                    self.records.push(record)
                }
                Ok(_) => {}
                Err(_) => self.keys.unreadable += 1,
            }
        }
        match next {
            Some(after) => {
                let scan = Self::scan_from(BUCKET_KEY_KEYSPACE, self.scanning, Some(after));
                Ok(Next::Call(scan))
            }
            None => self.next_bucket(),
        }
    }

    fn vault_read(&mut self, read: Result<Option<SharedSecret>, VaultReadError>) -> StartupTurn {
        let secret = match read {
            Ok(secret) => secret,
            Err(error) => return self.failed(error.to_string()),
        };
        let record = self.current.as_ref().ok_or(StartupError::NotFinished)?;
        let Some(secret) = secret else {
            return self.failed("the node vault holds no key for it".to_string());
        };
        let install = InstallKeyOperation::new(InstallInput {
            key: record.key,
            public_key: record.public_key,
            private_key: secret,
            duration: None,
            max: None,
        });
        self.call(StartupStep::Install, IoCall::child(install))
    }

    fn installed(&mut self, installed: Result<UnlockStatus, InstallError>) -> StartupTurn {
        match installed {
            Ok(status) => {
                self.keys.opened.push(status.key);
                self.current = None;
                self.next_key()
            }
            Err(error) => self.failed(error.to_string()),
        }
    }
}

impl Operation for OpenManagedOperation {
    type Output = ManagedKeys;
    type Error = StartupError;

    fn first(&mut self) -> StartupTurn {
        let scan = Self::scan_from(BUCKET_ENCRYPTION_KEYSPACE, None, None);
        self.call(StartupStep::ScanSettings, scan)
    }

    fn next(&mut self, result: IoResult) -> StartupTurn {
        match self.step {
            StartupStep::ScanSettings => {
                let (rows, next) = result.page()?;
                self.settings_scanned(rows, next)
            }
            StartupStep::ScanKeys => {
                let (rows, next) = result.page()?;
                self.keys_scanned(rows, next)
            }
            StartupStep::ReadVault => {
                self.vault_read(result.child::<Option<SharedSecret>, VaultReadError>()?)
            }
            StartupStep::Install => self.installed(result.child::<UnlockStatus, InstallError>()?),
        }
    }
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
