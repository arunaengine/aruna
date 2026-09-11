use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_connector_secret_read, read_connector_secret_effect,
};

#[derive(Debug, PartialEq)]
pub struct ConnectorHasSecretConfigOperation {
    connector_id: Ulid,
    state: ConnectorHasSecretConfigState,
    output: Option<Result<bool, ConnectorHasSecretConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ConnectorHasSecretConfigState {
    Init,
    ReadSecret,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ConnectorHasSecretConfigError {
    #[error(transparent)]
    StorageRead(#[from] StorageReadError),
    #[error("connector secret config check did not finish")]
    NotFinished,
}

impl ConnectorHasSecretConfigOperation {
    pub fn new(connector_id: Ulid) -> Self {
        Self {
            connector_id,
            state: ConnectorHasSecretConfigState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ConnectorHasSecretConfigError) -> Effects {
        self.state = ConnectorHasSecretConfigState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        match parse_connector_secret_read(event) {
            Ok(secret) => {
                self.state = ConnectorHasSecretConfigState::Finish;
                self.output = Some(Ok(secret.is_some()));
                smallvec![]
            }
            Err(error) => self.fail(error.into()),
        }
    }
}

impl Operation for ConnectorHasSecretConfigOperation {
    type Output = bool;
    type Error = ConnectorHasSecretConfigError;

    fn start(&mut self) -> Effects {
        self.state = ConnectorHasSecretConfigState::ReadSecret;
        smallvec![read_connector_secret_effect(self.connector_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ConnectorHasSecretConfigState::ReadSecret => self.handle_secret_read(event),
            ConnectorHasSecretConfigState::Init
            | ConnectorHasSecretConfigState::Finish
            | ConnectorHasSecretConfigState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ConnectorHasSecretConfigState::Finish | ConnectorHasSecretConfigState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(ConnectorHasSecretConfigError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
