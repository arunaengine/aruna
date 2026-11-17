//! Validates an S3 session token and records the last used time; a conflict is retried by the
//! runner with a fresh operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::S3SessionError;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::S3_SESSION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::s3_session::S3Session;
use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq)]
pub struct TouchS3Config {
    pub access_key: String,
    pub token_hash: String,
    pub now: SystemTime,
    pub issued_by: [u8; 32],
}

#[derive(Debug, PartialEq)]
pub struct TouchS3Operation {
    config: TouchS3Config,
    pending: Option<S3Session>,
}

impl TouchS3Operation {
    pub fn new(config: TouchS3Config) -> Self {
        Self {
            config,
            pending: None,
        }
    }

    fn key(&self) -> aruna_core::types::Key {
        self.config.access_key.as_bytes().into()
    }

    /// The session with its last use recorded, once the token proves it current.
    fn touched(
        &self,
        value: Option<aruna_core::types::Value>,
    ) -> Result<S3Session, S3SessionError> {
        let value = value.ok_or(S3SessionError::NotFound)?;
        let mut session = S3Session::from_bytes(value.as_ref())?;
        if session.access_key != self.config.access_key {
            return Err(S3SessionError::IndexInconsistent);
        }
        if session.issued_by != self.config.issued_by {
            return Err(S3SessionError::WrongIssuer);
        }
        if !session.token_matches(&self.config.token_hash, self.config.now) {
            return Err(S3SessionError::InvalidToken);
        }
        if session.is_expired(self.config.now) {
            return Err(S3SessionError::Expired);
        }
        session.last_used_at = Some(self.config.now);
        Ok(session)
    }
}

impl Operation for TouchS3Operation {
    type Output = S3Session;
    type Error = S3SessionError;

    fn first(&mut self) -> Result<Next<S3Session>, S3SessionError> {
        if !S3Session::valid_access_key(&self.config.access_key) {
            return Err(S3SessionError::InvalidAccessKey);
        }
        let read = vec![(S3_SESSION_KEYSPACE.to_string(), self.key())];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<S3Session>, S3SessionError> {
        if let Some(session) = self.pending.take() {
            result.done()?;
            return Ok(Next::Done(session));
        }
        let session = self.touched(result.value()?)?;
        let row = (
            S3_SESSION_KEYSPACE.to_string(),
            self.key(),
            session.to_bytes()?.into(),
        );
        self.pending = Some(session);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> bool {
        true
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::calls::IoError;

    fn config() -> TouchS3Config {
        TouchS3Config {
            access_key: S3Session::build_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            token_hash: "token".to_string(),
            now: SystemTime::UNIX_EPOCH,
            issued_by: [1; 32],
        }
    }

    #[test]
    fn reads_in_transaction() {
        let mut operation = TouchS3Operation::new(config());
        let read = vec![(S3_SESSION_KEYSPACE.to_string(), operation.key())];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(read))));
        assert!(operation.transaction());
        let missing = operation.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(S3SessionError::NotFound));
    }

    #[test]
    fn conflict_retried_outside() {
        let conflict = S3SessionError::from(IoError::Conflict);
        assert!(TouchS3Operation::is_conflict(&conflict));
        assert!(!TouchS3Operation::is_conflict(&S3SessionError::NotFound));
        let mut invalid = TouchS3Operation::new(TouchS3Config {
            access_key: "bad".to_string(),
            ..config()
        });
        assert_eq!(invalid.first(), Err(S3SessionError::InvalidAccessKey));
    }
}
