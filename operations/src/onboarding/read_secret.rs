//! Reads one onboarding secret and its state from one read snapshot, by its enrollment id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use ulid::Ulid;

use crate::onboarding::list_secrets::{ListSecretsError, OnboardingSecretEntry};
use crate::onboarding::secret_reads;
use crate::onboarding::secret_state::resolve_secret_state;

/// The secret of `enrollment_id` with its state, or `None` when no such secret is stored.
#[derive(Debug, PartialEq)]
pub struct ReadSecretOperation {
    enrollment_id: Ulid,
}

impl ReadSecretOperation {
    pub fn new(enrollment_id: Ulid) -> Self {
        Self { enrollment_id }
    }
}

impl Operation for ReadSecretOperation {
    type Output = Option<OnboardingSecretEntry>;
    type Error = ListSecretsError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<Self::Output>, ListSecretsError> {
        Ok(Next::Call(secret_reads(self.enrollment_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ListSecretsError> {
        let [record, state] = values::<2>(result)?;
        let Some(record) = record else {
            return Ok(Next::Done(None));
        };
        let record: OnboardingSecretRecord =
            postcard::from_bytes(&record).map_err(ConversionError::from)?;
        let state = resolve_secret_state(&record, state.as_ref())?;
        Ok(Next::Done(Some(OnboardingSecretEntry { record, state })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::IoCall;

    #[test]
    fn reads_two_rows() {
        // The record and its state come from one snapshot read; a missing record is none.
        let mut operation = ReadSecretOperation::new(Ulid::from_bytes([1; 16]));
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        let Ok(Next::Call(IoCall::Read(keys))) = operation.first() else {
            panic!("expected one read of the two rows");
        };
        assert_eq!(keys.len(), 2);
        let missing = IoResult::Values(vec![None, None]);
        assert_eq!(operation.next(missing), Ok(Next::Done(None)));
    }
}
