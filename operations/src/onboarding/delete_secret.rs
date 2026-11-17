//! Removes an onboarding secret record and its state entry in one storage transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::Operation;
use aruna_core::rows::{done, read_row, values};
use thiserror::Error;
use ulid::Ulid;

use crate::onboarding::create_secret::secret_record_key;
use crate::onboarding::secret_state::secret_state_key;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSecretInput {
    pub enrollment_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct DeleteSecretOperation {
    input: DeleteSecretInput,
    /// The record read, returned once both rows are deleted.
    record: Option<OnboardingSecretRecord>,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("onboarding secret not found")]
    NotFound,
    #[error("deleting onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for DeleteSecretError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "DeleteSecret".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl DeleteSecretOperation {
    pub fn new(input: DeleteSecretInput) -> Self {
        Self {
            input,
            record: None,
        }
    }
}

impl Operation for DeleteSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = DeleteSecretError;

    fn transaction(&self) -> bool {
        true
    }

    fn first(&mut self) -> Result<Next<OnboardingSecretRecord>, DeleteSecretError> {
        let key = secret_record_key(self.input.enrollment_id);
        Ok(Next::Call(read_row(ONBOARDING_KEYSPACE, key)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<OnboardingSecretRecord>, DeleteSecretError> {
        if let Some(record) = self.record.take() {
            done(result)?;
            return Ok(Next::Done(record));
        }
        let [value] = values::<1>(result)?;
        let value = value.ok_or(DeleteSecretError::NotFound)?;
        let record = postcard::from_bytes(&value).map_err(ConversionError::from)?;
        self.record = Some(record);
        let enrollment_id = self.input.enrollment_id;
        Ok(Next::Call(IoCall::Delete(vec![
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_record_key(enrollment_id),
            ),
            (
                ONBOARDING_KEYSPACE.to_string(),
                secret_state_key(enrollment_id),
            ),
        ])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_secret_fails() {
        // The read runs in the delete's transaction; a missing record deletes nothing.
        let enrollment_id = Ulid::from_bytes([1; 16]);
        let mut operation = DeleteSecretOperation::new(DeleteSecretInput { enrollment_id });
        assert!(operation.transaction());
        let read = read_row(ONBOARDING_KEYSPACE, secret_record_key(enrollment_id));
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let missing = operation.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(DeleteSecretError::NotFound));
    }
}
