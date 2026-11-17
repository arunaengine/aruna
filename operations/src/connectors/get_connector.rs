//! Reads one source connector and reports whether it holds a secret config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_secret_read, read_secret_effect, source_connector_key,
};
use crate::storage_read::decode_row;

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

#[derive(Debug, Error, PartialEq)]
pub enum GetSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
}

impl From<StorageReadError> for GetSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

impl From<IoError> for GetSourceError {
    fn from(error: IoError) -> Self {
        StorageReadError::from(error).into()
    }
}

#[derive(Debug, PartialEq)]
pub struct GetSourceOperation {
    input: GetSourceInput,
    connector: Option<SourceConnector>,
}

impl GetSourceOperation {
    pub fn new(input: GetSourceInput) -> Self {
        Self {
            input,
            connector: None,
        }
    }
}

impl Operation for GetSourceOperation {
    type Output = GetSourceResult;
    type Error = GetSourceError;

    fn first(&mut self) -> Result<Next<Self::Output>, GetSourceError> {
        Ok(Next::Call(rows::read_row(
            SOURCE_INDEX_KEYSPACE,
            source_connector_key(self.input.group_id, self.input.connector_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GetSourceError> {
        let Some(connector) = self.connector.take() else {
            let connector = decode_row(rows::value(result)?, SourceConnector::from_bytes)?;
            self.connector = Some(connector.ok_or(GetSourceError::NotFound)?);
            let read = read_secret_effect(self.input.connector_id, None);
            return Ok(Next::Call(IoCall::Effect(Box::new(read))));
        };
        let secret = parse_secret_read(rows::event(result)?, self.input.connector_id)?;
        Ok(Next::Done(GetSourceResult {
            connector,
            has_secret_config: secret.is_some(),
        }))
    }
}
