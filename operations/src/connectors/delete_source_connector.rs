use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_connector_read, read_connector_effect, source_connector_key,
    source_connector_secret_key,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceConnectorResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteSourceConnectorState {
    Init,
    ReadConnector,
    DeleteRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteSourceConnectorError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
    #[error("DeleteSourceConnector failed")]
    DeleteSourceConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: DeleteSourceConnectorState,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for DeleteSourceConnectorError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DeleteSourceConnectorOperation {
    input: DeleteSourceConnectorInput,
    state: DeleteSourceConnectorState,
    output: Option<Result<DeleteSourceConnectorResult, DeleteSourceConnectorError>>,
}

impl DeleteSourceConnectorOperation {
    pub fn new(input: DeleteSourceConnectorInput) -> Self {
        Self {
            input,
            state: DeleteSourceConnectorState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: DeleteSourceConnectorError) -> Effects {
        self.state = DeleteSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteSourceConnectorState::ReadConnector;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_connector_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(_)) => {
                self.state = DeleteSourceConnectorState::DeleteRecords;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: vec![
                        (
                            aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                            source_connector_key(self.input.group_id, self.input.connector_id),
                        ),
                        (
                            aruna_core::keyspaces::SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                            source_connector_secret_key(self.input.connector_id),
                        ),
                    ],
                    txn_id: None,
                })]
            }
            Ok(None) => self.emit_error(DeleteSourceConnectorError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_records_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.emit_error(DeleteSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };

        self.state = DeleteSourceConnectorState::Finish;
        self.output = Some(Ok(DeleteSourceConnectorResult));
        smallvec![]
    }
}

impl Operation for DeleteSourceConnectorOperation {
    type Output = DeleteSourceConnectorResult;
    type Error = DeleteSourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteSourceConnectorState::Init => self.handle_init(),
            DeleteSourceConnectorState::ReadConnector => self.handle_connector_read(event),
            DeleteSourceConnectorState::DeleteRecords => self.handle_records_deleted(event),
            DeleteSourceConnectorState::Finish => smallvec![],
            DeleteSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteSourceConnectorState::Finish | DeleteSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(DeleteSourceConnectorError::DeleteSourceConnectorFailed);
        }

        self.output
            .ok_or(DeleteSourceConnectorError::DeleteSourceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
