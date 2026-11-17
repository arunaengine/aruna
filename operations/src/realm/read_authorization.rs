//! Reads the realm authorization document and reports absence when no document is stored.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use thiserror::Error;

use crate::document_repository::read_row;

#[derive(Debug, PartialEq)]
pub struct ReadAuthorizationOperation {
    realm_id: RealmId,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadAuthorizationError {
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
    #[error("read realm authorization did not finish")]
    NotFinished,
}

impl From<IoError> for ReadAuthorizationError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReadAuthorization".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
        }
    }
}

impl ReadAuthorizationOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self { realm_id }
    }
}

impl Operation for ReadAuthorizationOperation {
    type Output = Option<RealmAuthorizationDocument>;
    type Error = ReadAuthorizationError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let target = DocumentTarget::RealmAuthorization {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![read_row(&target)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Values(values) = result else {
            let got = "another result";
            return Err(IoError::Unexpected {
                expected: "storage read result",
                got,
            }
            .into());
        };
        let document = (values.into_iter().next().flatten())
            .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
            .transpose()?;
        Ok(Next::Done(document))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn realm_id() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    #[test]
    fn reads_the_document() {
        // One read of the realm authorization row; a missing row is none.
        let mut operation = ReadAuthorizationOperation::new(realm_id());
        let target = DocumentTarget::RealmAuthorization {
            realm_id: realm_id(),
        };
        let read = Next::Call(IoCall::Read(vec![read_row(&target)]));
        assert_eq!(operation.first(), Ok(read));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn read_failures_end() {
        // An undecodable row, a wrong answer or a runner failure ends the read.
        let mut operation = ReadAuthorizationOperation::new(realm_id());
        operation.first().unwrap();
        let broken = IoResult::Values(vec![Some(vec![0xff; 3].into())]);
        assert!(matches!(
            operation.next(broken),
            Err(ReadAuthorizationError::ConversionError(_))
        ));
        let mut operation = ReadAuthorizationOperation::new(realm_id());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ReadAuthorizationError::UnexpectedEvent { .. })
        ));
        let conflict = ReadAuthorizationError::from(IoError::Conflict);
        assert!(ReadAuthorizationOperation::is_conflict(&conflict));
        let timeout = ReadAuthorizationError::from(IoError::Storage(StorageError::Timeout));
        assert!(!ReadAuthorizationOperation::is_conflict(&timeout));
    }
}
