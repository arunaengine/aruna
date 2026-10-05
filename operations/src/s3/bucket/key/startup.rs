//! Opens the node-managed bucket keys at startup, from the node vault into the unlock registry.
//! Vault-locked keys stay locked; a key that fails to open stays locked and is reported.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::install::{InstallInput, InstallKeyOperation};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, EncryptionMode, KeyState,
};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const SCAN_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupStep {
    Init,
    ScanSettings,
    ScanKeys,
    ReadVault,
    Install,
    /// The failed install still discards its prepared key.
    DrainInstall,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum StartupError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("opening the managed keys did not finish")]
    NotFinished,
}

/// What startup opened. A failed key stays locked; its reason names no key material.
#[derive(Debug, Default, PartialEq)]
pub struct ManagedKeys {
    pub opened: Vec<BucketKeyRef>,
    pub failed: Vec<(BucketKeyRef, String)>,
    /// Settings or key rows that do not decode; their buckets stay locked.
    pub unreadable: usize,
}

#[derive(Debug, PartialEq)]
pub struct OpenManagedOperation {
    step: StartupStep,
    buckets: Vec<Ulid>,
    scanning: Option<Ulid>,
    records: Vec<BucketKeyRecord>,
    current: Option<BucketKeyRecord>,
    install: Option<InstallKeyOperation>,
    keys: ManagedKeys,
    output: Option<Result<ManagedKeys, StartupError>>,
}

impl Default for OpenManagedOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenManagedOperation {
    pub fn new() -> Self {
        Self {
            step: StartupStep::Init,
            buckets: Vec::new(),
            scanning: None,
            records: Vec::new(),
            current: None,
            install: None,
            keys: ManagedKeys::default(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<StartupError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = StartupStep::Error;
        self.abort()
    }

    fn scan(&mut self, step: StartupStep, key_space: &str, prefix: Option<Ulid>) -> Effects {
        self.step = step;
        self.scan_from(key_space, prefix, None)
    }

    fn scan_from(&self, key_space: &str, prefix: Option<Ulid>, start: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: prefix.map(|id| id.to_bytes().to_vec().into()),
            start: start.map(IterStart::After),
            limit: SCAN_LIMIT,
            txn_id: None,
        })]
    }

    /// Scans the key rows of the next managed bucket, then opens the collected keys.
    fn next_bucket(&mut self) -> Effects {
        match self.buckets.pop() {
            Some(bucket_id) => {
                self.scanning = Some(bucket_id);
                self.scan(StartupStep::ScanKeys, BUCKET_KEY_KEYSPACE, Some(bucket_id))
            }
            None => self.next_key(),
        }
    }

    fn next_key(&mut self) -> Effects {
        let Some(record) = self.records.pop() else {
            self.step = StartupStep::Finish;
            self.output = Some(Ok(std::mem::take(&mut self.keys)));
            return smallvec![];
        };
        let Some(id) = record.vault_entry else {
            return self.next_key();
        };
        self.current = Some(record);
        self.step = StartupStep::ReadVault;
        smallvec![Effect::Storage(StorageEffect::VaultRead {
            entry: VaultEntry::new(VaultPurpose::BucketKey, id),
            txn_id: None,
        })]
    }

    fn failed(&mut self, reason: String) -> Effects {
        if let Some(record) = self.current.take() {
            self.keys.failed.push((record.key, reason));
        }
        self.next_key()
    }

    fn settings_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
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
            Some(start) => self.scan_from(BUCKET_ENCRYPTION_KEYSPACE, None, Some(start)),
            None => self.next_bucket(),
        }
    }

    fn keys_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
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
            Some(start) => self.scan_from(BUCKET_KEY_KEYSPACE, self.scanning, Some(start)),
            None => self.next_bucket(),
        }
    }

    fn vault_read(&mut self, entry: VaultEntry, secret: Option<SharedSecret>) -> Effects {
        let Some(record) = self.current.as_ref() else {
            return self.fail(StartupError::NotFinished);
        };
        if record.vault_entry != Some(entry.id) {
            return self.failed("the vault answered another entry".to_string());
        }
        let Some(secret) = secret else {
            return self.failed("the node vault holds no key for it".to_string());
        };
        let mut install = InstallKeyOperation::new(InstallInput {
            key: record.key,
            public_key: record.public_key,
            private_key: secret,
            duration: None,
            max: None,
        });
        let effects = install.start();
        self.install = Some(install);
        self.step = StartupStep::Install;
        effects
    }

    fn installed(&mut self, event: Event) -> Effects {
        let Some(install) = self.install.as_mut() else {
            return self.fail(StartupError::NotFinished);
        };
        let effects = install.step(event);
        if !install.is_complete() {
            return effects;
        }
        let Some(install) = self.install.take() else {
            return self.fail(StartupError::NotFinished);
        };
        match install.finalize() {
            Ok(status) => {
                self.keys.opened.push(status.key);
                self.current = None;
                self.next_key()
            }
            Err(error) if !effects.is_empty() => {
                let failed = self
                    .current
                    .take()
                    .map(|record| (record.key, error.to_string()));
                self.keys.failed.extend(failed);
                self.step = StartupStep::DrainInstall;
                effects
            }
            Err(error) => self.failed(error.to_string()),
        }
    }
}

impl Operation for OpenManagedOperation {
    type Output = ManagedKeys;
    type Error = StartupError;

    fn start(&mut self) -> Effects {
        self.scan(StartupStep::ScanSettings, BUCKET_ENCRYPTION_KEYSPACE, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (
                StartupStep::ReadVault,
                Event::Storage(StorageEvent::VaultResult { entry, secret }),
            ) => self.vault_read(entry, secret.map(SharedSecret::new)),
            (StartupStep::ReadVault, Event::Storage(StorageEvent::Error { error })) => {
                self.failed(error.to_string())
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                StartupStep::ScanSettings,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.settings_scanned(values, next_start_after),
            (
                StartupStep::ScanKeys,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.keys_scanned(values, next_start_after),
            (StartupStep::Install, event @ Event::Blob(_)) => self.installed(event),
            (StartupStep::DrainInstall, Event::Blob(_)) => self.next_key(),
            (StartupStep::Finish | StartupStep::Error, _) => smallvec![],
            (state, received) => self.fail(StartupError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, StartupStep::Finish | StartupStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(StartupError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.install
            .take()
            .map_or_else(Effects::new, |mut install| install.abort())
    }
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
