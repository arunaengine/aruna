//! Lists a user's S3 sessions by scanning the owner index in batches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{S3SessionError, decode_index};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{S3_SESSION_KEYSPACE, SESSION_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::s3_session::S3Session;
use aruna_core::types::Key;

const OWNER_SCAN_BATCH: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListPhase {
    Owners,
    Sessions,
}

/// Every session of one user, read in one transaction from the owner index and the sessions.
#[derive(Debug, PartialEq)]
pub struct ListSessionsOperation {
    user_identity: UserId,
    access_keys: Vec<String>,
    phase: ListPhase,
}

impl ListSessionsOperation {
    pub fn new(user_identity: UserId) -> Self {
        Self {
            user_identity,
            access_keys: Vec::new(),
            phase: ListPhase::Owners,
        }
    }

    fn owners(&self, after: Option<Key>) -> Next<Vec<S3Session>> {
        Next::Call(IoCall::Page {
            space: SESSION_OWNER_KEYSPACE.to_string(),
            prefix: self.user_identity.to_storage_key().into(),
            after,
            limit: OWNER_SCAN_BATCH,
        })
    }

    fn owners_read(&mut self, result: IoResult) -> Result<Next<Vec<S3Session>>, S3SessionError> {
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            self.access_keys.extend(decode_index(Some(&value))?);
        }
        if next.is_some() {
            return Ok(self.owners(next));
        }
        if self.access_keys.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        self.phase = ListPhase::Sessions;
        let reads = (self.access_keys.iter())
            .map(|key| (S3_SESSION_KEYSPACE.to_string(), key.as_bytes().into()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn sessions_read(&self, result: IoResult) -> Result<Next<Vec<S3Session>>, S3SessionError> {
        let values = result.values()?;
        if values.len() != self.access_keys.len() {
            return Err(S3SessionError::IndexInconsistent);
        }
        let mut sessions = Vec::with_capacity(values.len());
        for (access_key, value) in self.access_keys.iter().zip(values) {
            let value = value.ok_or(S3SessionError::IndexInconsistent)?;
            let session = S3Session::from_bytes(value.as_ref())?;
            if session.access_key != *access_key || session.user_identity != self.user_identity {
                return Err(S3SessionError::IndexInconsistent);
            }
            sessions.push(session);
        }
        Ok(Next::Done(sessions))
    }
}

impl Operation for ListSessionsOperation {
    type Output = Vec<S3Session>;
    type Error = S3SessionError;

    fn first(&mut self) -> Result<Next<Self::Output>, S3SessionError> {
        Ok(self.owners(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, S3SessionError> {
        match self.phase {
            ListPhase::Owners => self.owners_read(result),
            ListPhase::Sessions => self.sessions_read(result),
        }
    }

    fn transaction(&self) -> bool {
        true
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn user() -> UserId {
        UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([3; 32]))
    }

    fn page(after: Option<Key>) -> Next<Vec<S3Session>> {
        Next::Call(IoCall::Page {
            space: SESSION_OWNER_KEYSPACE.to_string(),
            prefix: user().to_storage_key().into(),
            after,
            limit: OWNER_SCAN_BATCH,
        })
    }

    #[test]
    fn pages_owner_index() {
        let mut operation = ListSessionsOperation::new(user());
        assert_eq!(operation.first(), Ok(page(None)));
        let more = IoResult::Page {
            rows: Vec::new(),
            next: Some(b"k".to_vec().into()),
        };
        assert_eq!(operation.next(more), Ok(page(Some(b"k".to_vec().into()))));
        let end = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(operation.next(end), Ok(Next::Done(Vec::new())));
        assert!(operation.transaction());
    }

    #[test]
    fn refuses_missing_sessions() {
        let mut operation = ListSessionsOperation::new(user());
        operation.access_keys = vec!["one".to_string()];
        operation.phase = ListPhase::Sessions;
        let missing = IoResult::Values(vec![None]);
        assert_eq!(
            operation.next(missing),
            Err(S3SessionError::IndexInconsistent)
        );
        let short = IoResult::Values(Vec::new());
        assert_eq!(
            operation.next(short),
            Err(S3SessionError::IndexInconsistent)
        );
    }
}
