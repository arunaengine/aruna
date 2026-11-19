//! Checks whether one source connector has a stored secret config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{parse_secret_read, read_secret_effect};
use aruna_core::errors::StorageReadError;

#[derive(Debug, PartialEq)]
pub struct HasConfigOperation {
    connector_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum HasConfigError {
    #[error(transparent)]
    StorageRead(#[from] StorageReadError),
}

impl From<IoError> for HasConfigError {
    fn from(error: IoError) -> Self {
        Self::StorageRead(error.into())
    }
}

impl HasConfigOperation {
    pub fn new(connector_id: Ulid) -> Self {
        Self { connector_id }
    }
}

impl Operation for HasConfigOperation {
    type Output = bool;
    type Error = HasConfigError;

    fn first(&mut self) -> Result<Next<bool>, HasConfigError> {
        let read = read_secret_effect(self.connector_id, None);
        Ok(Next::Call(IoCall::Effect(Box::new(read))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, HasConfigError> {
        let secret = parse_secret_read(result.event()?, self.connector_id)?;
        Ok(Next::Done(secret.is_some()))
    }
}
