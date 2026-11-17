//! Maps a document target to its storage key, builds the read, write and delete effects, and
//! reads the realm config and group documents that authorization and every domain build on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, GroupId, Key, KeySpace, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

pub fn storage_keyspace(document: &DocumentTarget) -> &'static str {
    document.storage_keyspace()
}

pub fn storage_key(document: &DocumentTarget) -> Key {
    document.storage_key()
}

pub fn read_effect(document: &DocumentTarget, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        txn_id,
    })
}

/// The storage row of `document`, for a read call.
pub fn read_row(document: &DocumentTarget) -> (KeySpace, Key) {
    (
        storage_keyspace(document).to_string(),
        storage_key(document),
    )
}

pub fn write_effect(document: &DocumentTarget, value: Vec<u8>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Write {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        value: value.into(),
        txn_id,
    })
}

pub fn delete_effect(document: &DocumentTarget, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        txn_id,
    })
}

pub fn parse_document_bytes(event: Event) -> Result<Option<Vec<u8>>, StorageError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        _ => Err(StorageError::ReadError("unexpected event".to_string())),
    }
}

pub fn parse_auth_document(key: &[u8]) -> Result<DocumentTarget, ConversionError> {
    match key.len() {
        16 => {
            let mut group_bytes = [0u8; 16];
            group_bytes.copy_from_slice(key);
            Ok(DocumentTarget::GroupAuthorization {
                group_id: GroupId::from_bytes(group_bytes),
            })
        }
        32 => {
            let mut realm_bytes = [0u8; 32];
            realm_bytes.copy_from_slice(key);
            Ok(DocumentTarget::RealmAuthorization {
                realm_id: RealmId::from_bytes(realm_bytes),
            })
        }
        other => Err(ConversionError::InvalidLength(format!(
            "unexpected auth key length {other}"
        ))),
    }
}

pub fn parse_group_document(key: &[u8]) -> Result<DocumentTarget, ConversionError> {
    if key.len() != 16 {
        return Err(ConversionError::InvalidLength(format!(
            "unexpected group key length {}",
            key.len()
        )));
    }

    let mut group_bytes = [0u8; 16];
    group_bytes.copy_from_slice(key);
    Ok(DocumentTarget::Group {
        group_id: GroupId::from_bytes(group_bytes),
    })
}

pub fn parse_realm_config(key: &[u8]) -> Result<DocumentTarget, ConversionError> {
    if key.len() != 32 {
        return Err(ConversionError::InvalidLength(format!(
            "unexpected realm config key length {}",
            key.len()
        )));
    }

    let mut realm_bytes = [0u8; 32];
    realm_bytes.copy_from_slice(key);
    Ok(DocumentTarget::RealmConfig {
        realm_id: RealmId::from_bytes(realm_bytes),
    })
}

pub fn iter_event_values(event: Event) -> Result<Vec<(Key, ByteView)>, StorageError> {
    match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        _ => Err(StorageError::ReadError("unexpected event".to_string())),
    }
}

pub fn empty_effects() -> Effects {
    smallvec::smallvec![]
}

#[derive(Debug, PartialEq)]
pub struct GetConfigOperation {
    document: DocumentTarget,
    txn_id: Option<TxnId>,
    external_txn: bool,
    state: GetConfigState,
    output: Option<Result<RealmConfigDocument, GetConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetConfigState {
    Init,
    ReadDocument,
    Finish,
    Error,
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

impl GetConfigOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            document: DocumentTarget::RealmConfig { realm_id },
            txn_id: None,
            external_txn: false,
            state: GetConfigState::Init,
            output: None,
        }
    }

    pub fn new_with_txn(realm_id: RealmId, txn_id: TxnId) -> Self {
        Self {
            document: DocumentTarget::RealmConfig { realm_id },
            txn_id: Some(txn_id),
            external_txn: true,
            state: GetConfigState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetConfigError) -> aruna_core::types::Effects {
        self.state = GetConfigState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: &'static str,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl EffectOperation for GetConfigOperation {
    type Output = RealmConfigDocument;
    type Error = GetConfigError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = GetConfigState::ReadDocument;
        smallvec![read_effect(&self.document, self.txn_id)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            GetConfigState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(GetConfigError::DocumentNotFound);
                    };
                    match RealmConfigDocument::from_bytes(&value) {
                        Ok(document) => {
                            self.state = GetConfigState::Finish;
                            self.output = Some(Ok(document));
                            smallvec![]
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", other.label()),
            },
            GetConfigState::Finish | GetConfigState::Error | GetConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetConfigState::Finish | GetConfigState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetConfigError::NotFinished))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        if self.external_txn {
            return smallvec![];
        }
        smallvec![]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetGroupConfig {
    pub group_id: GroupId,
}

#[derive(Debug, PartialEq)]
pub struct GetGroupOperation {
    config: GetGroupConfig,
    txn_id: Option<TxnId>,
    external_txn: bool,
    group: Option<Group>,
    auth_doc: Option<GroupAuthorizationDocument>,
    output: Option<Result<(Group, GroupAuthorizationDocument), GetGroupError>>,
    state: GetGroupState,
}

impl GetGroupOperation {
    pub fn new(config: GetGroupConfig) -> Self {
        GetGroupOperation {
            config,
            txn_id: None,
            external_txn: false,
            output: None,
            group: None,
            auth_doc: None,
            state: GetGroupState::Init,
        }
    }

    pub fn new_with_txn(config: GetGroupConfig, txn_id: TxnId) -> Self {
        Self {
            config,
            txn_id: Some(txn_id),
            external_txn: true,
            output: None,
            group: None,
            auth_doc: None,
            state: GetGroupState::Init,
        }
    }
    fn emit_get_group(&mut self) -> Effects {
        let key = self.config.group_id.to_bytes().into();

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn emit_parse_group(&mut self, value: Option<byteview::ByteView>) -> Result<(), GetGroupError> {
        let value = value.ok_or_else(|| GetGroupError::GroupNotFound)?;
        let group = Group::from_bytes(&value)?;
        self.group = Some(group);
        Ok(())
    }

    fn emit_auth_read(&mut self) -> Effects {
        let key = self.config.group_id.to_bytes().into();

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }
    fn parse_auth_result(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, GetGroupError> {
        let value = value.ok_or_else(|| GetGroupError::DocNotFound)?;
        let auth_doc = GroupAuthorizationDocument::from_bytes(&value)?;
        self.auth_doc = Some(auth_doc);
        if self.external_txn {
            self.state = GetGroupState::Finish;
            self.output = Some(Ok((
                self.group.clone().ok_or(GetGroupError::GroupNotFound)?,
                self.auth_doc.clone().ok_or(GetGroupError::DocNotFound)?,
            )));
            return Ok(smallvec![]);
        }
        let txn_id = self
            .txn_id
            .ok_or_else(|| GetGroupError::NoTransactionFound)?;
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn fail(&mut self, err: GetGroupError) -> Effects {
        self.state = GetGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(&mut self, err: GetGroupError, cleanup_effects: Effects) -> Effects {
        self.state = GetGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: GetGroupState,
        expected: &'static str,
        got: &'static str,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            GetGroupError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn catch_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = event.label();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                GetGroupState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = GetGroupState::GetGroup;
        self.txn_id = Some(txn_id);
        self.emit_get_group()
    }

    fn handle_get_group(&mut self, event: Event) -> Effects {
        let got = event.label();
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                GetGroupState::GetGroup,
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_parse_group(value) {
            Ok(_) => {
                self.state = GetGroupState::GetAuthDoc;
                self.emit_auth_read()
            }
            Err(err) => self.fail(err),
        }
    }

    fn handle_auth_read(&mut self, event: Event) -> Effects {
        let got = event.label();
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                GetGroupState::GetAuthDoc,
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.parse_auth_result(value) {
            Ok(effects) => {
                if !self.external_txn {
                    self.state = GetGroupState::CommitTransaction;
                }
                effects
            }
            Err(err) => self.fail(err),
        }
    }

    fn handle_commit_transaction(&mut self, event: Event) -> Effects {
        let got = event.label();
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                GetGroupState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        if let Some(group) = &self.group
            && let Some(auth) = &self.auth_doc
        {
            self.state = GetGroupState::Finish;
            self.output = Some(Ok((group.clone(), auth.clone())));
            smallvec![]
        } else {
            self.fail(GetGroupError::GroupNotFound)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GetGroupState {
    Init,
    StartTransaction,
    GetGroup,
    GetAuthDoc,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No group found")]
    GroupNotFound,
    #[error("No group found")]
    DocNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: GetGroupState,
        expected: &'static str,
        got: &'static str,
    },
}

impl EffectOperation for GetGroupOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = GetGroupError;

    fn start(&mut self) -> Effects {
        if self.external_txn {
            self.state = GetGroupState::GetGroup;
            return self.emit_get_group();
        }
        self.state = GetGroupState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.catch_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            GetGroupState::StartTransaction => self.handle_start_transaction(event),
            GetGroupState::GetGroup => self.handle_get_group(event),
            GetGroupState::GetAuthDoc => self.handle_auth_read(event),
            GetGroupState::CommitTransaction => self.handle_commit_transaction(event),
            // A terminal state owns no transaction to clean up, so this fails
            // without the abort effects `unexpected_event` would emit.
            GetGroupState::Init | GetGroupState::Finish | GetGroupState::Error => {
                let got = event.label();
                let state = self.state;
                self.fail(GetGroupError::UnexpectedEvent {
                    state,
                    expected: "no event",
                    got,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetGroupState::Finish | GetGroupState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| GetGroupError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        if self.external_txn {
            return smallvec![];
        }
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "document_repository_tests.rs"]
mod tests;
