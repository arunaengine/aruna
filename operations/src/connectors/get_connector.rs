//! Reads one source connector and reports whether it holds a secret config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_connector_read, parse_secret_read, read_connector_effect,
    read_secret_effect,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSourceResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetSourceState {
    Init,
    ReadConnector,
    ReadSecret,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
    #[error("GetSourceConnector failed")]
    GetConnectorFailed,
}

impl From<StorageReadError> for GetSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetSourceOperation {
    input: GetSourceInput,
    state: GetSourceState,
    connector: Option<SourceConnector>,
    has_secret_config: bool,
    output: Option<Result<GetSourceResult, GetSourceError>>,
}

impl GetSourceOperation {
    pub fn new(input: GetSourceInput) -> Self {
        Self {
            input,
            state: GetSourceState::Init,
            connector: None,
            has_secret_config: false,
            output: None,
        }
    }

    fn emit_error(&mut self, error: GetSourceError) -> Effects {
        self.state = GetSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = GetSourceState::ReadConnector;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_connector_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(connector)) => {
                self.connector = Some(connector);
                self.state = GetSourceState::ReadSecret;
                smallvec![read_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(GetSourceError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        match parse_secret_read(event) {
            Ok(secret) => {
                self.has_secret_config = secret.is_some();
                let Some(connector) = self.connector.clone() else {
                    return self.emit_error(GetSourceError::GetConnectorFailed);
                };
                self.state = GetSourceState::Finish;
                self.output = Some(Ok(GetSourceResult {
                    connector,
                    has_secret_config: self.has_secret_config,
                }));
                smallvec![]
            }
            Err(error) => self.emit_error(error.into()),
        }
    }
}

impl Operation for GetSourceOperation {
    type Output = GetSourceResult;
    type Error = GetSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetSourceState::Init => self.handle_init(),
            GetSourceState::ReadConnector => self.handle_connector_read(event),
            GetSourceState::ReadSecret => self.handle_secret_read(event),
            GetSourceState::Finish => smallvec![],
            GetSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetSourceState::Finish | GetSourceState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetSourceError::GetConnectorFailed);
        }

        self.output.ok_or(GetSourceError::GetConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
