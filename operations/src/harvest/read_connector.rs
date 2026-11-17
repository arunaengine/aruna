//! Reads one or all repository connectors of a group, with whether each keeps a secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{CONNECTOR_INDEX_KEYSPACE, CONNECTOR_SECRET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::harvest::RepositoryConnector;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use ulid::Ulid;

use crate::harvest::repository::{
    HARVEST_PAGE_SIZE, StorageReadError, connector_key, connector_prefix, connector_secret_key,
};
use crate::storage_read::decode_row;

/// A stored connector and whether a secret row exists for it; the secret itself stays hidden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectorView {
    pub connector: RepositoryConnector,
    pub has_secret_config: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadConnectorError {
    #[error(transparent)]
    Storage(#[from] StorageReadError),
    #[error("repository connector not found")]
    NotFound,
    #[error("unexpected event while reading repository connectors")]
    Unexpected,
}

impl From<IoError> for ReadConnectorError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Reads one connector under its group, so an id from another group is not found.
#[derive(Debug, PartialEq)]
pub struct GetRepositoryOperation {
    group_id: GroupId,
    connector_id: Ulid,
}

impl GetRepositoryOperation {
    pub fn new(group_id: GroupId, connector_id: Ulid) -> Self {
        Self {
            group_id,
            connector_id,
        }
    }
}

impl Operation for GetRepositoryOperation {
    type Output = ConnectorView;
    type Error = ReadConnectorError;

    fn first(&mut self) -> Result<Next<Self::Output>, ReadConnectorError> {
        Ok(Next::Call(IoCall::Read(vec![
            (
                CONNECTOR_INDEX_KEYSPACE.to_string(),
                connector_key(self.group_id, self.connector_id),
            ),
            (
                CONNECTOR_SECRET_KEYSPACE.to_string(),
                connector_secret_key(self.connector_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ReadConnectorError> {
        let [connector, secret] = rows::values::<2>(result)?;
        let connector = decode_row(connector, RepositoryConnector::from_bytes)?;
        Ok(Next::Done(ConnectorView {
            connector: connector.ok_or(ReadConnectorError::NotFound)?,
            has_secret_config: secret.is_some(),
        }))
    }
}

/// Lists a group's connectors in connector id order, then checks their secret rows in one read.
#[derive(Debug, PartialEq)]
pub struct ListRepositoryOperation {
    group_id: GroupId,
    connectors: Vec<RepositoryConnector>,
    /// Whether every page is read and the secret rows are next.
    listed: bool,
}

impl ListRepositoryOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            connectors: Vec::new(),
            listed: false,
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<ConnectorView>> {
        Next::Call(IoCall::Page {
            space: CONNECTOR_INDEX_KEYSPACE.to_string(),
            prefix: connector_prefix(self.group_id),
            after,
            limit: HARVEST_PAGE_SIZE,
        })
    }
}

impl Operation for ListRepositoryOperation {
    type Output = Vec<ConnectorView>;
    type Error = ReadConnectorError;

    fn first(&mut self) -> Result<Next<Self::Output>, ReadConnectorError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ReadConnectorError> {
        if self.listed {
            let values = rows::all_values(result)?;
            if values.len() != self.connectors.len() {
                return Err(ReadConnectorError::Unexpected);
            }
            let views = (std::mem::take(&mut self.connectors).into_iter().zip(values))
                .map(|(connector, secret)| ConnectorView {
                    connector,
                    has_secret_config: secret.is_some(),
                })
                .collect();
            return Ok(Next::Done(views));
        }
        let (values, next) = rows::page(result)?;
        for (_, value) in values {
            let connector = RepositoryConnector::from_bytes(&value);
            self.connectors
                .push(connector.map_err(StorageReadError::Conversion)?);
        }
        if let Some(after) = next {
            return Ok(self.page(Some(after)));
        }
        self.listed = true;
        let reads = (self.connectors.iter())
            .map(|connector| {
                (
                    CONNECTOR_SECRET_KEYSPACE.to_string(),
                    connector_secret_key(connector.connector_id),
                )
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }
}
