//! Revokes one S3 session and deletes its session, expiry and owner index rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{S3SessionError, decode_index, encode_index, expiry_key, owner_key};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{S3_SESSION_KEYSPACE, SESSION_EXPIRY_KEYSPACE, SESSION_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::s3_session::S3Session;
use aruna_core::types::{Key, Value};

#[derive(Debug, PartialEq)]
pub struct RevokeS3Config {
    pub access_key: String,
    pub user_identity: UserId,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RevokePhase {
    Session,
    Index,
    Write,
    Delete,
}

#[derive(Debug, PartialEq)]
pub struct RevokeS3Operation {
    config: RevokeS3Config,
    owner_key: Option<Key>,
    deletes: Vec<(String, Key)>,
    phase: RevokePhase,
}

impl RevokeS3Operation {
    pub fn new(config: RevokeS3Config) -> Self {
        Self {
            config,
            owner_key: None,
            deletes: Vec::new(),
            phase: RevokePhase::Session,
        }
    }

    /// Checks the caller owns the session and reads its owner index next.
    fn session_read(&mut self, value: Option<Value>) -> Result<Next<()>, S3SessionError> {
        let value = value.ok_or(S3SessionError::NotFound)?;
        let session = S3Session::from_bytes(value.as_ref())?;
        if session.access_key != self.config.access_key {
            return Err(S3SessionError::IndexInconsistent);
        }
        if session.user_identity != self.config.user_identity {
            return Err(S3SessionError::WrongOwner);
        }
        if session.issued_by != self.config.issued_by {
            return Err(S3SessionError::WrongIssuer);
        }
        let expiry = expiry_key(session.expiry, &session.access_key)?;
        self.deletes = vec![
            (
                S3_SESSION_KEYSPACE.to_string(),
                session.access_key.as_bytes().into(),
            ),
            (SESSION_EXPIRY_KEYSPACE.to_string(), expiry),
        ];
        let owner = owner_key(session.user_identity, session.group_id);
        self.owner_key = Some(owner.clone());
        self.phase = RevokePhase::Index;
        Ok(Next::Call(IoCall::Read(vec![(
            SESSION_OWNER_KEYSPACE.to_string(),
            owner,
        )])))
    }

    /// Drops the session from its owner index: the last one deletes the index row.
    fn index_read(&mut self, value: Option<Value>) -> Result<Next<()>, S3SessionError> {
        let mut index = decode_index(value.as_ref())?;
        index.remove(&self.config.access_key);
        let owner = self.owner_key.clone().ok_or(S3SessionError::Failed)?;
        if index.is_empty() {
            self.deletes
                .push((SESSION_OWNER_KEYSPACE.to_string(), owner));
            return Ok(self.delete());
        }
        self.phase = RevokePhase::Write;
        let row = (
            SESSION_OWNER_KEYSPACE.to_string(),
            owner,
            encode_index(&index)?,
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn delete(&mut self) -> Next<()> {
        self.phase = RevokePhase::Delete;
        Next::Call(IoCall::Delete(std::mem::take(&mut self.deletes)))
    }
}

impl Operation for RevokeS3Operation {
    type Output = ();
    type Error = S3SessionError;

    fn first(&mut self) -> Result<Next<()>, S3SessionError> {
        if !S3Session::valid_access_key(&self.config.access_key) {
            return Err(S3SessionError::InvalidAccessKey);
        }
        let key = self.config.access_key.as_bytes().into();
        Ok(Next::Call(IoCall::Read(vec![(
            S3_SESSION_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, S3SessionError> {
        match self.phase {
            RevokePhase::Session => self.session_read(result.value()?),
            RevokePhase::Index => self.index_read(result.value()?),
            RevokePhase::Write => {
                result.done()?;
                Ok(self.delete())
            }
            RevokePhase::Delete => {
                result.done()?;
                Ok(Next::Done(()))
            }
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

    fn config(access_key: &str) -> RevokeS3Config {
        RevokeS3Config {
            access_key: access_key.to_string(),
            user_identity: UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([1; 32])),
            issued_by: [1; 32],
        }
    }

    fn valid_key() -> String {
        S3Session::build_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
    }

    #[test]
    fn reads_session_first() {
        let key = valid_key();
        let mut operation = RevokeS3Operation::new(config(&key));
        let read = vec![(S3_SESSION_KEYSPACE.to_string(), key.as_bytes().into())];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(read))));
        assert!(operation.transaction());
        let missing = operation.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(S3SessionError::NotFound));
    }

    #[test]
    fn last_drops_index() {
        let key = valid_key();
        let mut operation = RevokeS3Operation::new(config(&key));
        let owner: Key = b"owner".to_vec().into();
        let session: Key = key.as_bytes().into();
        operation.owner_key = Some(owner.clone());
        operation.deletes = vec![(S3_SESSION_KEYSPACE.to_string(), session.clone())];
        operation.phase = RevokePhase::Index;
        let deletes = vec![
            (S3_SESSION_KEYSPACE.to_string(), session),
            (SESSION_OWNER_KEYSPACE.to_string(), owner),
        ];
        let empty = operation.next(IoResult::Values(vec![None]));
        assert_eq!(empty, Ok(Next::Call(IoCall::Delete(deletes))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
        let mut invalid = RevokeS3Operation::new(config("bad"));
        assert_eq!(invalid.first(), Err(S3SessionError::InvalidAccessKey));
    }
}
