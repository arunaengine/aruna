//! Revokes one S3 access credential and removes its key id from the owner index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::index::{decode_index, encode_index, owner_key, token_deletes, token_pages};
use crate::auth::epoch_due::DueMarks;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ACCESS_OWNER_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, read_row, value};
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::Key;
use std::time::{Duration, SystemTime};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum RevokeUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Credential not found")]
    NotFound,
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("operation did not finish")]
    NotFinished,
}

impl From<IoError> for RevokeUserError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            _ => Self::InvalidOperationState,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Stage {
    Access,
    Owner,
    Index,
    Credential,
    Tokens,
    TokensDeleted,
    Marks(DueMarks),
}

type RevokeTurn = Result<Next<UserAccess>, RevokeUserError>;

/// Deletes the credential, its owner index entry and its token requests and grants, and marks
/// the group's encrypted buckets due, all in one write transaction.
#[derive(Debug, PartialEq)]
pub struct RevokeUserOperation {
    access_key: String,
    now_ms: u64,
    stage: Stage,
    access: Option<UserAccess>,
    tokens: PrefixPages,
}

impl RevokeUserOperation {
    pub fn new(access_key: String) -> Self {
        let tokens = token_pages(&access_key);
        Self {
            access_key,
            now_ms: 0,
            stage: Stage::Access,
            access: None,
            tokens,
        }
    }

    fn access_read(&mut self, value: Option<aruna_core::types::Value>) -> RevokeTurn {
        let value = value.ok_or(RevokeUserError::NotFound)?;
        let access = UserAccess::from_bytes(value.as_ref())?;
        let owner = owner_key(access.user_identity);
        self.access = Some(access);
        self.stage = Stage::Owner;
        Ok(Next::Call(read_row(ACCESS_OWNER_KEYSPACE, owner)))
    }

    /// A credential that still authenticates must always be revocable, so a missing owner-index
    /// entry leaves nothing to repair and is skipped.
    fn index_read(&mut self, value: Option<aruna_core::types::Value>) -> RevokeTurn {
        let mut index = decode_index(value.as_ref())?;
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
        let access = self.access.as_mut().ok_or(RevokeUserError::NotFinished)?;
        access.revoked_at.get_or_insert(now);
        let owner: Key = owner_key(access.user_identity);
        if !index.remove(&self.access_key) {
            return Ok(self.delete_access());
        }
        self.stage = Stage::Index;
        let row = (
            ACCESS_OWNER_KEYSPACE.to_string(),
            owner,
            encode_index(&index)?,
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn delete_access(&mut self) -> Next<UserAccess> {
        self.stage = Stage::Credential;
        let key = Key::from(self.access_key.as_bytes());
        Next::Call(IoCall::Delete(vec![(
            USER_ACCESS_KEYSPACE.to_string(),
            key,
        )]))
    }

    /// The credential's token requests and grants go in the same transaction, one page at a time.
    fn scan_tokens(&mut self) -> Next<UserAccess> {
        self.stage = Stage::Tokens;
        Next::Call(self.tokens.call())
    }

    fn tokens_read(&mut self, result: IoResult) -> RevokeTurn {
        let rows = self.tokens.take(result)?;
        if rows.is_empty() {
            return self.mark_due();
        }
        self.stage = Stage::TokensDeleted;
        let deletes = token_deletes(&self.access_key, rows)?;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    /// Marks the credential group's encrypted buckets due; they commit with the revocation.
    fn mark_due(&mut self) -> RevokeTurn {
        let access = self.access.as_ref().ok_or(RevokeUserError::NotFinished)?;
        let marks = DueMarks::new(Some(access.group_id), self.now_ms);
        let first = marks.first();
        self.stage = Stage::Marks(marks);
        Ok(Next::Call(first))
    }

    fn marks_answered(&mut self, result: IoResult) -> RevokeTurn {
        let Stage::Marks(marks) = &mut self.stage else {
            return Err(RevokeUserError::InvalidOperationState);
        };
        let next = marks
            .next(result, true)
            .map_err(|error| StorageError::WriteError(error.to_string()))?;
        match next {
            Some(call) => Ok(Next::Call(call)),
            None => (self.access.take())
                .map(Next::Done)
                .ok_or(RevokeUserError::NotFinished),
        }
    }
}

impl Operation for RevokeUserOperation {
    type Output = UserAccess;
    type Error = RevokeUserError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RevokeTurn {
        let key = Key::from(self.access_key.as_bytes());
        Ok(Next::Call(read_row(USER_ACCESS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> RevokeTurn {
        match self.stage {
            Stage::Access => self.access_read(value(result)?),
            Stage::Owner => self.index_read(value(result)?),
            Stage::Index => {
                done(result)?;
                Ok(self.delete_access())
            }
            Stage::Credential => {
                done(result)?;
                Ok(self.scan_tokens())
            }
            Stage::Tokens => self.tokens_read(result),
            Stage::TokensDeleted => {
                done(result)?;
                if self.tokens.done() {
                    return self.mark_due();
                }
                Ok(self.scan_tokens())
            }
            Stage::Marks(_) => self.marks_answered(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        ABE_GRANT_KEYSPACE, ABE_REQUEST_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE, TOKEN_GRANT_KEYSPACE,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::abe_access::token_prefix;
    use aruna_core::structs::storage::blob::UserAccess;
    use aruna_storage::storage;
    use std::collections::BTreeSet;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn credential() -> UserAccess {
        UserAccess {
            access_key: "userkey".to_string(),
            user_identity: UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            group_id: Ulid::from_bytes([3; 16]),
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
            expiry: SystemTime::UNIX_EPOCH + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: [9; 32],
            revoked_at: None,
        }
    }

    fn no_rows() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    /// Reads the credential, so the next call is the owner index read.
    fn started(access: &UserAccess) -> RevokeUserOperation {
        let mut operation = RevokeUserOperation::new(access.access_key.clone());
        operation.stamp(5_000);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let row = read_row(USER_ACCESS_KEYSPACE, access.access_key.as_bytes());
        assert_eq!(operation.first(), Ok(Next::Call(row)));
        let stored = IoResult::Values(vec![Some(access.to_bytes().unwrap().into())]);
        let owner = read_row(ACCESS_OWNER_KEYSPACE, owner_key(access.user_identity));
        assert_eq!(operation.next(stored), Ok(Next::Call(owner)));
        operation
    }

    fn credential_delete(access: &UserAccess) -> Next<UserAccess> {
        let key = Key::from(access.access_key.as_bytes());
        Next::Call(IoCall::Delete(vec![(
            USER_ACCESS_KEYSPACE.to_string(),
            key,
        )]))
    }

    /// Answers the token scan with no rows and the marks scan with no bucket.
    fn finished(operation: &mut RevokeUserOperation, access: &UserAccess) -> UserAccess {
        let Ok(Next::Call(IoCall::Page { space, .. })) = operation.next(IoResult::Done) else {
            panic!("expected the token scan");
        };
        assert_eq!(space, TOKEN_GRANT_KEYSPACE);
        let Ok(Next::Call(IoCall::Page { space, prefix, .. })) = operation.next(no_rows()) else {
            panic!("expected the due marks scan in this transaction");
        };
        assert_eq!(space, GROUP_ENCRYPTED_KEYSPACE);
        assert_eq!(prefix, Key::from(access.group_id.to_bytes().to_vec()));
        match operation.next(no_rows()) {
            Ok(Next::Done(revoked)) => revoked,
            other => panic!("expected the revoked credential, got {other:?}"),
        }
    }

    #[test]
    fn revokes_indexed_credential() {
        let access = credential();
        let mut operation = started(&access);
        let index = BTreeSet::from([access.access_key.clone(), "otherkey".to_string()]);
        let answer = IoResult::Values(vec![Some(encode_index(&index).unwrap())]);
        let kept = encode_index(&BTreeSet::from(["otherkey".to_string()])).unwrap();
        let row = (
            ACCESS_OWNER_KEYSPACE.to_string(),
            owner_key(access.user_identity),
            kept,
        );
        assert_eq!(
            operation.next(answer),
            Ok(Next::Call(IoCall::Write(vec![row])))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(credential_delete(&access))
        );
        let revoked = finished(&mut operation, &access);
        let at = SystemTime::UNIX_EPOCH + Duration::from_millis(5_000);
        assert_eq!(revoked.revoked_at, Some(at));
    }

    #[test]
    fn revokes_without_index() {
        // A credential that still authenticates must be revocable even when the
        // owner index lost its entry.
        let access = credential();
        let mut operation = started(&access);
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(credential_delete(&access))
        );
        assert!(finished(&mut operation, &access).revoked_at.is_some());
    }

    #[test]
    fn keeps_first_revocation() {
        let mut access = credential();
        let earlier = SystemTime::UNIX_EPOCH + Duration::from_millis(7);
        access.revoked_at = Some(earlier);
        let mut operation = started(&access);
        operation.next(IoResult::Values(vec![None])).unwrap();
        assert_eq!(finished(&mut operation, &access).revoked_at, Some(earlier));
    }

    #[test]
    fn deletes_token_pages() {
        // Each page of token rows is deleted before the next is read after its last key.
        let access = credential();
        let mut operation = started(&access);
        operation.next(IoResult::Values(vec![None])).unwrap();
        operation.next(IoResult::Done).unwrap();
        let prefix = aruna_core::structs::storage::abe_access::token_prefix(&access.access_key);
        let key = Key::from([prefix, vec![1; 4]].concat());
        let page = IoResult::Page {
            rows: vec![(key.clone(), Vec::new().into())],
            next: Some(key.clone()),
        };
        let Ok(Next::Call(IoCall::Delete(deletes))) = operation.next(page) else {
            panic!("expected the token deletes");
        };
        assert_eq!(deletes.len(), 3);
        let turn = operation.next(IoResult::Done);
        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { after: Some(after), .. })) if after == key
        ));
    }

    #[test]
    fn missing_credential_fails() {
        let mut operation = RevokeUserOperation::new("userkey".to_string());
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(RevokeUserError::NotFound)
        );
    }

    #[test]
    fn failed_marks_fail() {
        // Without its due marks the revocation must not commit.
        let access = credential();
        let mut operation = started(&access);
        operation.next(IoResult::Values(vec![None])).unwrap();
        operation.next(IoResult::Done).unwrap();
        operation.next(no_rows()).unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(RevokeUserError::StorageError(StorageError::WriteError(_)))
        ));
    }

    #[tokio::test]
    async fn revokes_user_access() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        // Access keys are validated as strictly alphanumeric.
        let access_key = "testaccesskey".to_string();
        let user_access = UserAccess {
            access_key: access_key.clone(),
            user_identity: Default::default(),
            group_id: Ulid::generate(),
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
            expiry: SystemTime::now() + Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access_key.clone().into(),
                value: user_access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let index = encode_index(&std::collections::BTreeSet::from([access_key.clone()])).unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: ACCESS_OWNER_KEYSPACE.to_string(),
                key: owner_key(user_access.user_identity),
                value: index,
                txn_id: None,
            })
            .await;

        let result = drive(RevokeUserOperation::new(access_key), &driver_ctx)
            .await
            .unwrap();
        // The runner's clock stamps the revocation.
        assert!(
            result
                .revoked_at
                .is_some_and(|at| at > SystemTime::UNIX_EPOCH)
        );
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: result.access_key.as_bytes().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("credential read failed");
        };
        assert!(value.is_none());
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: ACCESS_OWNER_KEYSPACE.to_string(),
                key: owner_key(user_access.user_identity),
                txn_id: None,
            })
            .await
        else {
            panic!("owner index read failed");
        };
        assert!(decode_index(value.as_ref()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn revoke_deletes_tokens() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let user_access = UserAccess {
            access_key: "tokenkey".to_string(),
            user_identity: Default::default(),
            group_id: Ulid::generate(),
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
            expiry: SystemTime::now() + Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        };
        let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        storage_handle
            .send_storage_effect(write(
                USER_ACCESS_KEYSPACE,
                b"tokenkey".to_vec(),
                user_access.to_bytes().unwrap(),
            ))
            .await;
        // An open request and a grant of this credential, and a grant of another credential
        // whose key extends this one.
        let rows = [
            ("tokenkey", ABE_REQUEST_KEYSPACE, 1u8),
            ("tokenkey", ABE_GRANT_KEYSPACE, 2),
            ("tokenkeyx", ABE_GRANT_KEYSPACE, 3),
        ];
        let row = |access_key: &str, request: u8| [token_prefix(access_key), vec![request; 80]];
        for (access_key, key_space, request) in rows {
            let [prefix, key] = row(access_key, request);
            storage_handle
                .send_storage_effect(write(key_space, key.clone(), vec![1]))
                .await;
            let index = [prefix, key].concat();
            storage_handle
                .send_storage_effect(write(TOKEN_GRANT_KEYSPACE, index, Vec::new()))
                .await;
        }

        drive(
            RevokeUserOperation::new("tokenkey".to_string()),
            &driver_ctx,
        )
        .await
        .unwrap();

        for ((access_key, key_space, request), kept) in rows.into_iter().zip([false, false, true]) {
            let [prefix, key] = row(access_key, request);
            let index = [prefix, key.clone()].concat();
            for (key_space, key) in [(key_space, key), (TOKEN_GRANT_KEYSPACE, index)] {
                let read = StorageEffect::Read {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    txn_id: None,
                };
                let Event::Storage(StorageEvent::ReadResult { value, .. }) =
                    storage_handle.send_storage_effect(read).await
                else {
                    panic!("token row read failed");
                };
                assert_eq!(value.is_some(), kept, "{key_space} of {access_key}");
            }
        }
    }
}
