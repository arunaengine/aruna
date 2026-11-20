//! Lists the stored onboarding secret records with their resolved state, a page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use byteview::ByteView;
use thiserror::Error;

use crate::onboarding::secret_state::{resolve_secret_state, secret_state_key};

/// Secret records read per page, each with its state row in one batch.
pub const SECRET_PAGE: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnboardingSecretEntry {
    pub record: OnboardingSecretRecord,
    pub state: OnboardingSecretState,
}

/// One page of secrets, and the key the next page starts after.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretPage {
    pub entries: Vec<OnboardingSecretEntry>,
    pub next: Option<ByteView>,
}

/// Reads one page of secret records, then the state row of each record of that page.
#[derive(Debug, Default, PartialEq)]
pub struct ListSecretsOperation {
    after: Option<ByteView>,
    records: Vec<OnboardingSecretRecord>,
    next: Option<ByteView>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListSecretsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("listing onboarding secrets did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ListSecretsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ListSecrets".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl ListSecretsOperation {
    /// The page after the record key `after`, or the first page.
    pub fn new(after: Option<ByteView>) -> Self {
        Self {
            after,
            ..Self::default()
        }
    }

    fn read_states(
        &mut self,
        rows: Vec<(ByteView, ByteView)>,
    ) -> Result<Next<SecretPage>, ListSecretsError> {
        self.records = rows
            .into_iter()
            .map(|(_, value)| postcard::from_bytes::<OnboardingSecretRecord>(&value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ConversionError::from)?;
        let reads = (self.records.iter())
            .map(|record| {
                let key = secret_state_key(record.enrollment_id);
                (ONBOARDING_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }
}

impl Operation for ListSecretsOperation {
    type Output = SecretPage;
    type Error = ListSecretsError;

    fn first(&mut self) -> Result<Next<Self::Output>, ListSecretsError> {
        Ok(Next::Call(IoCall::Page {
            space: ONBOARDING_KEYSPACE.to_string(),
            prefix: ByteView::from(b"secret:".as_slice()),
            after: self.after.take(),
            limit: SECRET_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ListSecretsError> {
        let values = match result {
            IoResult::Page { rows, next } => {
                self.next = next;
                return self.read_states(rows);
            }
            IoResult::Values(values) if values.len() == self.records.len() => values,
            IoResult::Values(_) => {
                return Err(ListSecretsError::UnexpectedEvent {
                    state: "ReadStates".to_string(),
                    expected: "state batch read result matching record count",
                    got: "a batch of another length",
                });
            }
            other => {
                return Err(ListSecretsError::UnexpectedEvent {
                    state: "ListSecrets".to_string(),
                    expected: "storage iteration or batch read result",
                    got: other.label(),
                });
            }
        };
        let entries = std::mem::take(&mut self.records)
            .into_iter()
            .zip(values)
            .map(|(record, state_value)| {
                resolve_secret_state(&record, state_value.as_ref())
                    .map(|state| OnboardingSecretEntry { record, state })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next = self.next.take();
        Ok(Next::Done(SecretPage { entries, next }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_list_ends() {
        // No records means an empty read, which the runner answers without storage.
        let mut operation = ListSecretsOperation::new(None);
        operation.first().unwrap();
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(
            operation.next(page),
            Ok(Next::Call(IoCall::Read(Vec::new())))
        );
        let empty = SecretPage {
            entries: Vec::new(),
            next: None,
        };
        assert_eq!(
            operation.next(IoResult::Values(Vec::new())),
            Ok(Next::Done(empty))
        );
    }

    #[test]
    fn pages_continue() {
        // A page reads a bounded number of records after its cursor and names the next one.
        let after = ByteView::from(b"secret:a".as_slice());
        let mut operation = ListSecretsOperation::new(Some(after.clone()));
        let Ok(Next::Call(IoCall::Page { after: asked, limit, .. })) = operation.first() else {
            panic!("expected a page of records");
        };
        assert_eq!((asked, limit), (Some(after), SECRET_PAGE));
        let next = ByteView::from(b"secret:b".as_slice());
        let page = IoResult::Page {
            rows: Vec::new(),
            next: Some(next.clone()),
        };
        operation.next(page).unwrap();
        let Ok(Next::Done(page)) = operation.next(IoResult::Values(Vec::new())) else {
            panic!("expected the page");
        };
        assert_eq!(page.next, Some(next));
    }
}
