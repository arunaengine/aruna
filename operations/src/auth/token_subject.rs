//! Checks that a user record still owns each of its subject ids in the subject index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::user::User;
use aruna_core::{SUBJECT_INDEX_KEYSPACE, USER_KEYSPACE};
use byteview::ByteView;
use thiserror::Error;

/// Reads the user record, then the index row of every subject id it claims.
#[derive(Debug, PartialEq)]
pub struct SubjectCheckOperation {
    user_id: UserId,
    user_read: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SubjectCheckError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Forbidden")]
    Forbidden,
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
    #[error("canonical user token subject check did not finish")]
    NotFinished,
}

impl From<IoError> for SubjectCheckError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SubjectCheck".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SubjectCheckOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            user_read: false,
        }
    }

    fn read_subjects(&mut self, result: IoResult) -> Result<Next<()>, SubjectCheckError> {
        let [value] = values::<1>(result)?;
        let user = User::from_bytes(&value.ok_or(SubjectCheckError::Unauthorized)?)?;
        if user.user_id != self.user_id {
            return Err(SubjectCheckError::Unauthorized);
        }
        // Nothing claimed means no index entry can resolve this record to a different
        // canonical owner, and the runner answers the empty read at once.
        let reads = (user.subject_ids.iter())
            .map(|subject| {
                let key = ByteView::from(subject.as_bytes().to_vec());
                (SUBJECT_INDEX_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }
}

impl Operation for SubjectCheckOperation {
    type Output = ();
    type Error = SubjectCheckError;

    fn first(&mut self) -> Result<Next<()>, SubjectCheckError> {
        Ok(Next::Call(read_row(USER_KEYSPACE, self.user_id.to_bytes())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SubjectCheckError> {
        if !self.user_read {
            self.user_read = true;
            return self.read_subjects(result);
        }
        let IoResult::Values(owners) = result else {
            return Err(SubjectCheckError::UnexpectedEvent {
                state: "ReadSubjectIndex".to_string(),
                expected: "storage read result",
                got: result.label(),
            });
        };
        for owner in owners {
            let owner = owner.ok_or(SubjectCheckError::Forbidden)?;
            // The index resolves this subject to another user in the realm, so this record is a
            // merged alias or stale: a token here would restore a non-canonical identity.
            if UserId::from_storage_key(&owner)? != self.user_id {
                return Err(SubjectCheckError::Forbidden);
            }
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn user(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn owners(owners: Vec<Option<Vec<u8>>>) -> IoResult {
        IoResult::Values(owners.into_iter().map(|row| row.map(Into::into)).collect())
    }

    #[test]
    fn foreign_subject_forbidden() {
        // Every claimed subject must resolve to this user; a missing or foreign owner refuses.
        let check = |rows| {
            let mut operation = SubjectCheckOperation::new(user(2));
            operation.user_read = true;
            operation.next(owners(rows))
        };
        assert_eq!(check(vec![Some(user(2).to_bytes())]), Ok(Next::Done(())));
        let foreign = vec![Some(user(2).to_bytes()), Some(user(3).to_bytes())];
        assert_eq!(check(foreign), Err(SubjectCheckError::Forbidden));
        assert_eq!(check(vec![None]), Err(SubjectCheckError::Forbidden));
    }
}
