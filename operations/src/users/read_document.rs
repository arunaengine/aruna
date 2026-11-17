//! Reads one user document from the local document repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::identity::user::User;
use thiserror::Error;

use crate::document_repository::read_row;

#[derive(Debug, PartialEq)]
pub struct ReadUserOperation {
    user_id: UserId,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadUserError {
    #[error("User not found")]
    NotFound,
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
    #[error("read user document did not finish")]
    NotFinished,
}

impl From<IoError> for ReadUserError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReadUser".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl ReadUserOperation {
    pub fn new(user_id: UserId) -> Self {
        Self { user_id }
    }
}

impl Operation for ReadUserOperation {
    type Output = User;
    type Error = ReadUserError;

    fn first(&mut self) -> Result<Next<User>, ReadUserError> {
        let target = DocumentTarget::User {
            user_id: self.user_id,
        };
        Ok(Next::Call(IoCall::Read(vec![read_row(&target)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<User>, ReadUserError> {
        let [value] = values::<1>(result)?;
        let bytes = value.ok_or(ReadUserError::NotFound)?;
        Ok(Next::Done(User::from_bytes(&bytes)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    #[test]
    fn missing_user_fails() {
        // One read of the user row; a missing row is not found, a wrong answer fails.
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), RealmId::from_bytes([1; 32]));
        let mut operation = ReadUserOperation::new(user_id);
        let target = DocumentTarget::User { user_id };
        let read = Next::Call(IoCall::Read(vec![read_row(&target)]));
        assert_eq!(operation.first(), Ok(read));
        let missing = operation.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(ReadUserError::NotFound));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ReadUserError::UnexpectedEvent { .. })
        ));
    }
}
