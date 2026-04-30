use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::SourceConnector;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_connector_read, parse_connector_secret_read, read_connector_effect,
    read_connector_secret_effect,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSourceConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSourceConnectorResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetSourceConnectorState {
    Init,
    ReadConnector,
    ReadSecret,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetSourceConnectorError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
    #[error("GetSourceConnector failed")]
    GetSourceConnectorFailed,
}

impl From<StorageReadError> for GetSourceConnectorError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetSourceConnectorOperation {
    input: GetSourceConnectorInput,
    state: GetSourceConnectorState,
    connector: Option<SourceConnector>,
    has_secret_config: bool,
    output: Option<Result<GetSourceConnectorResult, GetSourceConnectorError>>,
}

impl GetSourceConnectorOperation {
    pub fn new(input: GetSourceConnectorInput) -> Self {
        Self {
            input,
            state: GetSourceConnectorState::Init,
            connector: None,
            has_secret_config: false,
            output: None,
        }
    }

    fn emit_error(&mut self, error: GetSourceConnectorError) -> Effects {
        self.state = GetSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = GetSourceConnectorState::ReadConnector;
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
                self.state = GetSourceConnectorState::ReadSecret;
                smallvec![read_connector_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(GetSourceConnectorError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        match parse_connector_secret_read(event) {
            Ok(secret) => {
                self.has_secret_config = secret.is_some();
                let Some(connector) = self.connector.clone() else {
                    return self.emit_error(GetSourceConnectorError::GetSourceConnectorFailed);
                };
                self.state = GetSourceConnectorState::Finish;
                self.output = Some(Ok(GetSourceConnectorResult {
                    connector,
                    has_secret_config: self.has_secret_config,
                }));
                smallvec![]
            }
            Err(error) => self.emit_error(error.into()),
        }
    }
}

impl Operation for GetSourceConnectorOperation {
    type Output = GetSourceConnectorResult;
    type Error = GetSourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetSourceConnectorState::Init => self.handle_init(),
            GetSourceConnectorState::ReadConnector => self.handle_connector_read(event),
            GetSourceConnectorState::ReadSecret => self.handle_secret_read(event),
            GetSourceConnectorState::Finish => smallvec![],
            GetSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetSourceConnectorState::Finish | GetSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetSourceConnectorError::GetSourceConnectorFailed);
        }

        self.output
            .ok_or(GetSourceConnectorError::GetSourceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
