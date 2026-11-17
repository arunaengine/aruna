//! Lists a user's sessions by reading the owner index and then each session row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::index::{decode_index, owner_key};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{USER_OWNER_KEYSPACE, USER_SESSION_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::user::session::UserSession;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ListSessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("session owner index is inconsistent")]
    IndexInconsistent,
    #[error("session list did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ListSessionError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ListSession".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

/// Reads the owner index and the sessions it names in one transaction, newest first.
#[derive(Debug, PartialEq)]
pub struct ListSessionOperation {
    user_id: UserId,
    session_ids: Option<Vec<String>>,
}

impl ListSessionOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            session_ids: None,
        }
    }

    fn read_sessions(
        &mut self,
        result: IoResult,
    ) -> Result<Next<Vec<UserSession>>, ListSessionError> {
        let [index] = values::<1>(result)?;
        let session_ids: Vec<String> = decode_index(index.as_ref())?.into_iter().collect();
        let reads = (session_ids.iter())
            .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
            .collect();
        self.session_ids = Some(session_ids);
        Ok(Next::Call(IoCall::Read(reads)))
    }
}

impl Operation for ListSessionOperation {
    type Output = Vec<UserSession>;
    type Error = ListSessionError;

    /// The index and the session rows come from one snapshot.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<Vec<UserSession>>, ListSessionError> {
        let key = owner_key(self.user_id);
        Ok(Next::Call(read_row(USER_OWNER_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<UserSession>>, ListSessionError> {
        let Some(session_ids) = &self.session_ids else {
            return self.read_sessions(result);
        };
        let IoResult::Values(values) = result else {
            return Err(ListSessionError::UnexpectedEvent {
                state: "ReadSessions".to_string(),
                expected: "session records read",
                got: result.label(),
            });
        };
        if values.len() != session_ids.len() {
            return Err(ListSessionError::IndexInconsistent);
        }
        let mut sessions = Vec::with_capacity(values.len());
        for (sid, value) in session_ids.iter().zip(values) {
            let value = value.ok_or(ListSessionError::IndexInconsistent)?;
            let session = UserSession::from_bytes(value.as_ref())?;
            if session.user_id != self.user_id || &session.sid != sid {
                return Err(ListSessionError::IndexInconsistent);
            }
            sessions.push(session);
        }
        sessions.sort_by_key(|session| std::cmp::Reverse(session.created_at));
        Ok(Next::Done(sessions))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::structs::identity::auth::SessionKind;
    use aruna_core::structs::identity::realm::RealmId;
    use byteview::ByteView;
    use std::collections::BTreeSet;
    use ulid::Ulid;

    #[test]
    fn lists_revoked_session() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let sid = Ulid::from_bytes([5; 16]).to_string();
        let record = UserSession {
            sid: sid.clone(),
            user_id,
            kind: SessionKind::Assistant,
            label: None,
            created_at: 10,
            expires_at: 20,
            token_hash: "a".repeat(64),
            revoked: true,
            via: None,
        };
        let mut operation = ListSessionOperation::new(user_id);
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        operation.first().unwrap();
        let index = postcard::to_allocvec(&BTreeSet::from([sid.clone()])).unwrap();
        let read = operation.next(IoResult::Values(vec![Some(ByteView::from(index))]));
        let reads = vec![(USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into())];
        assert_eq!(read, Ok(Next::Call(IoCall::Read(reads))));
        let row = ByteView::from(record.to_bytes().unwrap());
        let Ok(Next::Done(sessions)) = operation.next(IoResult::Values(vec![Some(row)])) else {
            panic!("expected the sessions");
        };
        assert!(sessions[0].revoked);
    }
}
