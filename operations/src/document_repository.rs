//! Maps a document target to its storage key, builds the read, write and delete effects, and
//! reads the realm config and group documents that authorization and every domain build on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{value, values};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key, KeySpace, TxnId};
use thiserror::Error;

pub fn read_effect(document: &DocumentTarget, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: document.storage_keyspace().to_string(),
        key: document.storage_key(),
        txn_id,
    })
}

/// The storage row of `document`, for a read call.
pub fn read_row(document: &DocumentTarget) -> (KeySpace, Key) {
    (
        document.storage_keyspace().to_string(),
        document.storage_key(),
    )
}

pub fn write_effect(document: &DocumentTarget, value: Vec<u8>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Write {
        key_space: document.storage_keyspace().to_string(),
        key: document.storage_key(),
        value: value.into(),
        txn_id,
    })
}

/// Reads and decodes the realm config document.
#[derive(Debug, PartialEq)]
pub struct GetConfigOperation {
    document: DocumentTarget,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetConfigError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for GetConfigError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "GetConfig".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

impl GetConfigOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            document: DocumentTarget::RealmConfig { realm_id },
        }
    }
}

impl Operation for GetConfigOperation {
    type Output = RealmConfigDocument;
    type Error = GetConfigError;

    fn first(&mut self) -> Result<Next<RealmConfigDocument>, GetConfigError> {
        Ok(Next::Call(IoCall::Read(vec![read_row(&self.document)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RealmConfigDocument>, GetConfigError> {
        let bytes = value(result)?.ok_or(GetConfigError::DocumentNotFound)?;
        Ok(Next::Done(RealmConfigDocument::from_bytes(&bytes)?))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetGroupConfig {
    pub group_id: GroupId,
}

/// Reads a group and its authorization document from one snapshot.
#[derive(Debug, PartialEq)]
pub struct GetGroupOperation {
    config: GetGroupConfig,
}

impl GetGroupOperation {
    pub fn new(config: GetGroupConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No group found")]
    GroupNotFound,
    #[error("No group found")]
    DocNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for GetGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "GetGroup".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

impl Operation for GetGroupOperation {
    type Output = (Group, GroupAuthorizationDocument);
    type Error = GetGroupError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<Self::Output>, GetGroupError> {
        let key: Key = self.config.group_id.to_bytes().into();
        Ok(Next::Call(IoCall::Read(vec![
            (GROUP_KEYSPACE.to_string(), key.clone()),
            (AUTH_KEYSPACE.to_string(), key),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GetGroupError> {
        let [group, auth_doc] = values::<2>(result)?;
        let group = Group::from_bytes(&group.ok_or(GetGroupError::GroupNotFound)?)?;
        let auth_doc = auth_doc.ok_or(GetGroupError::DocNotFound)?;
        let auth_doc = GroupAuthorizationDocument::from_bytes(&auth_doc)?;
        Ok(Next::Done((group, auth_doc)))
    }
}

#[cfg(test)]
#[path = "document_repository_tests.rs"]
mod tests;
