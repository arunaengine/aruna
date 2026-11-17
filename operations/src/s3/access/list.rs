//! Lists the S3 credentials a user owns by reading the owner index and each record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::index::{decode_index, owner_key};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ACCESS_OWNER_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::UserAccess;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ListUserInput {
    pub user_identity: UserId,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("credential owner index is inconsistent")]
    IndexInconsistent,
    #[error("ListUserAccess failed: {0}")]
    Io(IoError),
}

impl From<IoError> for ListUserError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<Vec<UserAccess>>, ListUserError>;

/// Reads the owner index and the credentials it names in one read snapshot, so a credential
/// created or deleted meanwhile never shows the index as inconsistent.
#[derive(Debug, PartialEq)]
pub struct ListUserOperation {
    input: ListUserInput,
    /// The access keys the index names, in read order, once the index was read.
    access_keys: Option<Vec<String>>,
}

impl ListUserOperation {
    pub fn new(input: ListUserInput) -> Self {
        Self {
            input,
            access_keys: None,
        }
    }

    fn index_read(&mut self, value: Option<aruna_core::types::Value>) -> Step {
        let index = decode_index(value.as_ref())?;
        if index.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        let access_keys: Vec<String> = index.into_iter().collect();
        let reads = (access_keys.iter())
            .map(|key| (USER_ACCESS_KEYSPACE.to_string(), key.as_bytes().into()))
            .collect();
        self.access_keys = Some(access_keys);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn credentials_read(&self, keys: Vec<String>, result: IoResult) -> Step {
        let values = result.values()?;
        if values.len() != keys.len() {
            return Err(ListUserError::IndexInconsistent);
        }
        let mut credentials = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values) {
            let value = value.ok_or(ListUserError::IndexInconsistent)?;
            let access = UserAccess::from_bytes(value.as_ref())?;
            if &access.access_key != key || access.user_identity != self.input.user_identity {
                return Err(ListUserError::IndexInconsistent);
            }
            credentials.push(access);
        }
        Ok(Next::Done(credentials))
    }
}

impl Operation for ListUserOperation {
    type Output = Vec<UserAccess>;
    type Error = ListUserError;

    fn first(&mut self) -> Step {
        let key = owner_key(self.input.user_identity);
        Ok(Next::Call(read_row(ACCESS_OWNER_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.access_keys.take() {
            Some(keys) => self.credentials_read(keys, result),
            None => self.index_read(result.value()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::s3::access::index::{MAX_ACTIVE_CREDENTIALS, encode_index};
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::structs::identity::realm::RealmId;
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    fn access(key: &str, user_identity: UserId) -> UserAccess {
        UserAccess {
            access_key: key.to_string(),
            user_identity,
            group_id: Ulid::from_parts(3, 3),
            secret: EncryptedS3Secret::empty(),
            expiry: SystemTime::UNIX_EPOCH + Duration::from_secs(1600000120),
            path_restrictions: None,
            issued_by: [0; 32],
            revoked_at: None,
        }
    }

    #[test]
    fn lists_indexed() {
        let user_identity = UserId::local(Ulid::from_parts(1, 1), RealmId([1; 32]));
        let keys = (0..MAX_ACTIVE_CREDENTIALS)
            .map(|index| format!("key{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        let mut operation = ListUserOperation::new(ListUserInput { user_identity });
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        let index_read = read_row(ACCESS_OWNER_KEYSPACE, owner_key(user_identity));
        assert_eq!(operation.first(), Ok(Next::Call(index_read)));
        let index = encode_index(&keys).unwrap();
        let reads = operation.next(IoResult::Values(vec![Some(index)]));
        assert!(matches!(
            &reads,
            Ok(Next::Call(IoCall::Read(reads))) if reads.len() == MAX_ACTIVE_CREDENTIALS
        ));
        let values = (keys.iter())
            .map(|key| Some(access(key, user_identity).to_bytes().unwrap().into()))
            .collect();
        let Ok(Next::Done(credentials)) = operation.next(IoResult::Values(values)) else {
            panic!("expected the credentials");
        };
        assert_eq!(credentials.len(), MAX_ACTIVE_CREDENTIALS);
    }

    #[test]
    fn foreign_record_inconsistent() {
        let user_identity = UserId::local(Ulid::from_parts(1, 1), RealmId([1; 32]));
        let keys = std::collections::BTreeSet::from(["key0".to_string()]);
        let mut operation = ListUserOperation::new(ListUserInput { user_identity });
        let _ = operation.first();
        let index = encode_index(&keys).unwrap();
        let _ = operation.next(IoResult::Values(vec![Some(index)]));
        let other = UserId::local(Ulid::from_parts(9, 9), RealmId([1; 32]));
        let foreign = Some(access("key0", other).to_bytes().unwrap().into());
        let refused = operation.next(IoResult::Values(vec![foreign]));
        assert_eq!(refused, Err(ListUserError::IndexInconsistent));
    }
}
