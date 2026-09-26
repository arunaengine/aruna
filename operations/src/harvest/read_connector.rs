//! Reads one or all repository connectors of a group, with whether each keeps a secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::CONNECTOR_SECRET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::harvest::RepositoryConnector;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::harvest::repository::{
    StorageReadError, connector_secret_key, iter_connectors_effect, parse_connector_iter,
    parse_connector_read, read_connector_effect, read_secret_effect,
};

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

#[derive(Debug, PartialEq)]
enum GetState {
    Init,
    ReadConnector,
    ReadSecret,
    Finish,
}

/// Reads one connector under its group, so an id from another group is not found.
#[derive(Debug, PartialEq)]
pub struct GetRepositoryOperation {
    group_id: GroupId,
    connector_id: Ulid,
    state: GetState,
    connector: Option<RepositoryConnector>,
    output: Option<Result<ConnectorView, ReadConnectorError>>,
}

impl GetRepositoryOperation {
    pub fn new(group_id: GroupId, connector_id: Ulid) -> Self {
        Self {
            group_id,
            connector_id,
            state: GetState::Init,
            connector: None,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<ConnectorView, ReadConnectorError>) -> Effects {
        self.state = GetState::Finish;
        self.output = Some(output);
        smallvec![]
    }
}

impl Operation for GetRepositoryOperation {
    type Output = ConnectorView;
    type Error = ReadConnectorError;

    fn start(&mut self) -> Effects {
        self.state = GetState::ReadConnector;
        smallvec![read_connector_effect(
            self.group_id,
            self.connector_id,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetState::ReadConnector => match parse_connector_read(event) {
                Ok(Some(connector)) => {
                    self.connector = Some(connector);
                    self.state = GetState::ReadSecret;
                    smallvec![read_secret_effect(self.connector_id, None)]
                }
                Ok(None) => self.finish(Err(ReadConnectorError::NotFound)),
                Err(error) => self.finish(Err(error.into())),
            },
            GetState::ReadSecret => {
                let has_secret_config = match event {
                    Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
                    Event::Storage(StorageEvent::Error { error }) => {
                        return self.finish(Err(StorageReadError::Storage(error).into()));
                    }
                    _ => return self.finish(Err(ReadConnectorError::Unexpected)),
                };
                let Some(connector) = self.connector.take() else {
                    return self.finish(Err(ReadConnectorError::Unexpected));
                };
                self.finish(Ok(ConnectorView {
                    connector,
                    has_secret_config,
                }))
            }
            GetState::Init | GetState::Finish => self.finish(Err(ReadConnectorError::Unexpected)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == GetState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ReadConnectorError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, PartialEq)]
enum ListState {
    Init,
    Iterate,
    ReadSecrets,
    Finish,
}

/// Lists a group's connectors in connector id order, then checks their secret rows in one read.
#[derive(Debug, PartialEq)]
pub struct ListRepositoryOperation {
    group_id: GroupId,
    state: ListState,
    connectors: Vec<RepositoryConnector>,
    output: Option<Result<Vec<ConnectorView>, ReadConnectorError>>,
}

impl ListRepositoryOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: ListState::Init,
            connectors: Vec::new(),
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Vec<ConnectorView>, ReadConnectorError>) -> Effects {
        self.state = ListState::Finish;
        self.output = Some(output);
        smallvec![]
    }

    fn read_secrets(&mut self) -> Effects {
        if self.connectors.is_empty() {
            return self.finish(Ok(Vec::new()));
        }
        self.state = ListState::ReadSecrets;
        let reads = self
            .connectors
            .iter()
            .map(|connector| {
                (
                    CONNECTOR_SECRET_KEYSPACE.to_string(),
                    connector_secret_key(connector.connector_id),
                )
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })]
    }
}

impl Operation for ListRepositoryOperation {
    type Output = Vec<ConnectorView>;
    type Error = ReadConnectorError;

    fn start(&mut self) -> Effects {
        self.state = ListState::Iterate;
        smallvec![iter_connectors_effect(self.group_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListState::Iterate => match parse_connector_iter(event) {
                Ok((connectors, next)) => {
                    self.connectors.extend(connectors);
                    match next {
                        Some(next) => smallvec![iter_connectors_effect(self.group_id, Some(next))],
                        None => self.read_secrets(),
                    }
                }
                Err(error) => self.finish(Err(error.into())),
            },
            ListState::ReadSecrets => match event {
                Event::Storage(StorageEvent::BatchReadResult { values })
                    if values.len() == self.connectors.len() =>
                {
                    let views = std::mem::take(&mut self.connectors)
                        .into_iter()
                        .zip(values)
                        .map(|(connector, (_, secret))| ConnectorView {
                            connector,
                            has_secret_config: secret.is_some(),
                        })
                        .collect();
                    self.finish(Ok(views))
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.finish(Err(StorageReadError::Storage(error).into()))
                }
                _ => self.finish(Err(ReadConnectorError::Unexpected)),
            },
            ListState::Init | ListState::Finish => self.finish(Err(ReadConnectorError::Unexpected)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ListState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ReadConnectorError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
