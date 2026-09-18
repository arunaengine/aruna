//! Checks whether one source connector has a stored secret config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{StorageReadError, parse_secret_read, read_secret_effect};

#[derive(Debug, PartialEq)]
pub struct HasConfigOperation {
    connector_id: Ulid,
    state: HasConfigState,
    output: Option<Result<bool, HasConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum HasConfigState {
    Init,
    ReadSecret,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum HasConfigError {
    #[error(transparent)]
    StorageRead(#[from] StorageReadError),
    #[error("connector secret config check did not finish")]
    NotFinished,
}

impl HasConfigOperation {
    pub fn new(connector_id: Ulid) -> Self {
        Self {
            connector_id,
            state: HasConfigState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: HasConfigError) -> Effects {
        self.state = HasConfigState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        match parse_secret_read(event) {
            Ok(secret) => {
                self.state = HasConfigState::Finish;
                self.output = Some(Ok(secret.is_some()));
                smallvec![]
            }
            Err(error) => self.fail(error.into()),
        }
    }
}

impl Operation for HasConfigOperation {
    type Output = bool;
    type Error = HasConfigError;

    fn start(&mut self) -> Effects {
        self.state = HasConfigState::ReadSecret;
        smallvec![read_secret_effect(self.connector_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            HasConfigState::ReadSecret => self.handle_secret_read(event),
            HasConfigState::Init | HasConfigState::Finish | HasConfigState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HasConfigState::Finish | HasConfigState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(HasConfigError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
