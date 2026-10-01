//! Reads, writes and deletes the size capped vault payload stored per user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_VAULT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::user::vault::{MAX_VAULT_BYTES, UserVault};
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

pub const VAULT_CAP: &str = "a vault payload may hold at most 64 KiB";

fn vault_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

fn read_vault(user_id: UserId, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: USER_VAULT_KEYSPACE.to_string(),
        key: vault_key(user_id),
        txn_id,
    })
}

fn decode_vault(value: Option<Value>) -> Result<Option<UserVault>, VaultStoreError> {
    Ok(value
        .map(|bytes| UserVault::from_bytes(bytes.as_ref()))
        .transpose()?)
}

fn write_vault(vault: &UserVault, txn_id: TxnId) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: USER_VAULT_KEYSPACE.to_string(),
        key: vault_key(vault.user_id),
        value: vault.to_bytes()?.into(),
        txn_id: Some(txn_id),
    }))
}

fn abort_effects(txn_id: &mut Option<TxnId>) -> Effects {
    txn_id
        .take()
        .map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
}

#[derive(Debug, Error, PartialEq)]
pub enum VaultStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("{0}")]
    TooLarge(&'static str),
    #[error("vault changed in another browser")]
    Stale,
    #[error("vault operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

fn unexpected(
    state: &impl std::fmt::Debug,
    expected: &'static str,
    event: &Event,
) -> VaultStoreError {
    VaultStoreError::UnexpectedEvent {
        state: format!("{state:?}"),
        expected,
        got: format!("{event:?}"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadVaultState {
    Init,
    ReadVault,
    Finish,
    Error,
}

/// Reads the vault of one user; `None` before the first save.
#[derive(Debug, PartialEq)]
pub struct ReadVaultOperation {
    user_id: UserId,
    state: ReadVaultState,
    output: Option<Result<Option<UserVault>, VaultStoreError>>,
}

impl ReadVaultOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ReadVaultState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: VaultStoreError) -> Effects {
        self.state = ReadVaultState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ReadVaultOperation {
    type Output = Option<UserVault>;
    type Error = VaultStoreError;

    fn start(&mut self) -> Effects {
        self.state = ReadVaultState::ReadVault;
        smallvec![read_vault(self.user_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ReadVaultState::Init => self.start(),
            ReadVaultState::ReadVault => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(unexpected(&self.state, "vault read", &event));
                };
                self.state = ReadVaultState::Finish;
                self.output = Some(decode_vault(value));
                smallvec![]
            }
            ReadVaultState::Finish | ReadVaultState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadVaultState::Finish | ReadVaultState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(VaultStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WriteVaultState {
    Init,
    StartTransaction,
    ReadVault,
    WriteVault,
    CommitTransaction,
    Finish,
    Error,
}

/// Stores the vault of one user. A write is refused when the caller read an
/// older revision than the node holds, so a second browser cannot silently
/// drop what the first one saved.
#[derive(Debug, PartialEq)]
pub struct WriteVaultOperation {
    user_id: UserId,
    payload: String,
    expected_revision: Option<u64>,
    now: u64,
    txn_id: Option<TxnId>,
    state: WriteVaultState,
    output: Option<Result<UserVault, VaultStoreError>>,
}

impl WriteVaultOperation {
    pub fn new(user_id: UserId, payload: String, expected_revision: Option<u64>, now: u64) -> Self {
        Self {
            user_id,
            payload,
            expected_revision,
            now,
            txn_id: None,
            state: WriteVaultState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: VaultStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = WriteVaultState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = WriteVaultState::ReadVault;
        smallvec![read_vault(self.user_id, Some(txn_id))]
    }

    fn handle_current(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "vault read", &event));
        };
        let held = match decode_vault(value) {
            Ok(current) => current.map_or(0, |vault| vault.revision),
            Err(error) => return self.fail(error),
        };
        // No expectation writes over whatever is there; an expectation must match.
        if let Some(expected) = self.expected_revision
            && expected != held
        {
            return self.fail(VaultStoreError::Stale);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let vault = UserVault {
            user_id: self.user_id,
            payload: std::mem::take(&mut self.payload),
            revision: held.saturating_add(1),
            updated_at: self.now,
        };
        let effect = match write_vault(&vault, txn_id) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.state = WriteVaultState::WriteVault;
        self.output = Some(Ok(vault));
        smallvec![effect]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "vault write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = WriteVaultState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = WriteVaultState::Finish;
        smallvec![]
    }
}

impl Operation for WriteVaultOperation {
    type Output = UserVault;
    type Error = VaultStoreError;

    fn start(&mut self) -> Effects {
        if self.payload.len() > MAX_VAULT_BYTES {
            return self.fail(VaultStoreError::TooLarge(VAULT_CAP));
        }
        self.state = WriteVaultState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            WriteVaultState::Init => self.start(),
            WriteVaultState::StartTransaction => self.handle_started(event),
            WriteVaultState::ReadVault => self.handle_current(event),
            WriteVaultState::WriteVault => self.handle_written(event),
            WriteVaultState::CommitTransaction => self.handle_committed(event),
            WriteVaultState::Finish | WriteVaultState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteVaultState::Finish | WriteVaultState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(VaultStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DeleteVaultState {
    Init,
    StartTransaction,
    ReadVault,
    WriteVault,
    CommitTransaction,
    Finish,
    Error,
}

/// Empties the vault of one user but keeps its revision counting, so a browser
/// that still holds the deleted vault cannot overwrite a re-created one. An
/// absent or already emptied vault is left as it is.
#[derive(Debug, PartialEq)]
pub struct DeleteVaultOperation {
    user_id: UserId,
    now: u64,
    txn_id: Option<TxnId>,
    state: DeleteVaultState,
    output: Option<Result<(), VaultStoreError>>,
}

impl DeleteVaultOperation {
    pub fn new(user_id: UserId, now: u64) -> Self {
        Self {
            user_id,
            now,
            txn_id: None,
            state: DeleteVaultState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: VaultStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = DeleteVaultState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = DeleteVaultState::ReadVault;
        smallvec![read_vault(self.user_id, Some(txn_id))]
    }

    fn handle_current(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "vault read", &event));
        };
        let current = match decode_vault(value) {
            Ok(Some(vault)) if !vault.payload.is_empty() => vault,
            Ok(_) => {
                self.state = DeleteVaultState::Finish;
                self.output = Some(Ok(()));
                return self.abort();
            }
            Err(error) => return self.fail(error),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let tombstone = UserVault {
            user_id: self.user_id,
            payload: String::new(),
            revision: current.revision.saturating_add(1),
            updated_at: self.now,
        };
        let effect = match write_vault(&tombstone, txn_id) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.state = DeleteVaultState::WriteVault;
        smallvec![effect]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "vault write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = DeleteVaultState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = DeleteVaultState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }
}

impl Operation for DeleteVaultOperation {
    type Output = ();
    type Error = VaultStoreError;

    fn start(&mut self) -> Effects {
        self.state = DeleteVaultState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            DeleteVaultState::Init => self.start(),
            DeleteVaultState::StartTransaction => self.handle_started(event),
            DeleteVaultState::ReadVault => self.handle_current(event),
            DeleteVaultState::WriteVault => self.handle_written(event),
            DeleteVaultState::CommitTransaction => self.handle_committed(event),
            DeleteVaultState::Finish | DeleteVaultState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteVaultState::Finish | DeleteVaultState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(VaultStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}
