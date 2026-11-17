//! Changes the longest unlock of an encrypted bucket. Running sessions keep their bounds; the
//! next unlock uses the new maximum.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{SettingsError, authority_keys, keyless, parse_authority};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError, EncryptionMode};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum UnlockLimitError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Key(#[from] BucketKeyError),
    #[error("the bucket has no keys")]
    NotEncrypted,
    /// The calling user lost the group admin role before the change committed.
    #[error("the caller is no group admin")]
    NotAdmin,
    #[error("the unlock limit change failed: {0}")]
    Io(IoError),
}

impl From<IoError> for UnlockLimitError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<BucketEncryption>, UnlockLimitError>;

/// A change by an authorized group admin; `None` lets an unlock last until lock or restart.
#[derive(Debug, PartialEq)]
pub struct UnlockLimitInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// The group admin who changes the limit; checked again inside the transaction.
    pub caller: UserId,
    pub max_unlock_ms: Option<u64>,
    /// The storage generation the caller read; another one means a concurrent change.
    pub expected_generation: u64,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct UnlockLimitOperation {
    input: UnlockLimitInput,
    /// The settings written; the write answer follows.
    settings: Option<BucketEncryption>,
}

impl UnlockLimitOperation {
    pub fn new(input: UnlockLimitInput) -> Self {
        Self {
            input,
            settings: None,
        }
    }

    fn write(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(values, realm_id, group_id)?;
        if !state.admins.contains(&self.input.caller) {
            return Err(UnlockLimitError::NotAdmin);
        }
        let mut settings = state.settings;
        let (Some(bucket_id), true) = (settings.bucket_id, settings.is_encrypted()) else {
            return Err(UnlockLimitError::NotEncrypted);
        };
        if settings.storage_generation != self.input.expected_generation {
            return Err(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: settings.storage_generation,
            }
            .into());
        }
        if self.input.max_unlock_ms == Some(0) {
            return Err(BucketKeyError::InvalidDuration.into());
        }
        if settings.mode == EncryptionMode::NodeManaged && self.input.max_unlock_ms.is_some() {
            return Err(BucketKeyError::ManagedLimit.into());
        }
        settings.max_unlock_ms = self.input.max_unlock_ms;
        let record = BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id,
            at_ms: self.input.now_ms,
            action: AuditAction::ModeChange,
            actor: Some(self.input.caller),
            node_id: self.input.node_id,
            generation: Some(settings.key_generation),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: Some("unlock maximum changed".to_string()),
            outcome: AuditOutcome::Applied,
        };
        let (value, audit) = (settings.to_bytes()?, record.to_bytes()?);
        let bucket: Key = self.input.bucket.as_bytes().to_vec().into();
        self.settings = Some(settings);
        Ok(Next::Call(IoCall::Write(vec![
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket, value.into()),
            (
                BUCKET_AUDIT_KEYSPACE.to_string(),
                record.key().into(),
                audit.into(),
            ),
        ])))
    }
}

impl Operation for UnlockLimitOperation {
    type Output = BucketEncryption;
    type Error = UnlockLimitError;

    fn first(&mut self) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.settings.take() {
            Some(settings) => {
                result.done()?;
                Ok(Next::Done(settings))
            }
            None => self.write(keyless(result.values()?)),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::encryption::EncryptionMode;
    use aruna_core::structs::storage::format::Compression;
    use std::time::SystemTime;

    fn caller() -> UserId {
        UserId::new(Ulid::from_bytes([5; 16]), RealmId::from_bytes([1; 32]))
    }

    /// The authority read of a bucket whose group admins are `admins`.
    fn rows(settings: &BucketEncryption, admins: &[UserId]) -> Vec<(Key, Option<Value>)> {
        let info = BucketInfo {
            group_id: Ulid::from_bytes([3; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: caller(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        crate::s3::bucket::key::rows::authority_rows(&info, Some(settings), admins)
    }

    fn run(max_unlock_ms: Option<u64>, expected: u64) -> (UnlockLimitOperation, Step) {
        run_by(
            max_unlock_ms,
            expected,
            &[caller()],
            EncryptionMode::VaultLocked,
        )
    }

    /// Runs a change by `caller()` while `admins` hold the group admin role in the transaction.
    fn run_by(
        max_unlock_ms: Option<u64>,
        expected: u64,
        admins: &[UserId],
        mode: EncryptionMode,
    ) -> (UnlockLimitOperation, Step) {
        let mut operation = UnlockLimitOperation::new(UnlockLimitInput {
            bucket: "raw".to_string(),
            group_id: Ulid::from_bytes([3; 16]),
            realm_id: RealmId::from_bytes([1; 32]),
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            caller: caller(),
            max_unlock_ms,
            expected_generation: expected,
            now_ms: 10,
        });
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let _ = operation.first();
        let settings = BucketEncryption {
            mode,
            bucket_id: Some(Ulid::from_bytes([4; 16])),
            key_generation: 1,
            storage_generation: 2,
            max_unlock_ms: Some(1_000),
            ..Default::default()
        };
        let values = rows(&settings, admins).into_iter().map(|(_, value)| value);
        let turn = operation.next(IoResult::Values(values.collect()));
        (operation, turn)
    }

    #[test]
    fn stores_new_maximum() {
        let (mut operation, turn) = run(Some(60_000), 2);
        let Ok(Next::Call(IoCall::Write(writes))) = &turn else {
            panic!("expected one batch write, got {turn:?}");
        };
        let stored = BucketEncryption::from_bytes(writes[0].2.as_ref()).unwrap();
        assert_eq!(stored.max_unlock_ms, Some(60_000));
        assert_eq!(writes[1].0, BUCKET_AUDIT_KEYSPACE);
        let Ok(Next::Done(stored)) = operation.next(IoResult::Done) else {
            panic!("expected the stored settings");
        };
        assert_eq!(stored.max_unlock_ms, Some(60_000));
    }

    #[test]
    fn managed_refuses_limit() {
        let (_, turn) = run_by(Some(60_000), 2, &[caller()], EncryptionMode::NodeManaged);
        assert_eq!(
            turn,
            Err(UnlockLimitError::Key(BucketKeyError::ManagedLimit))
        );

        let (_, turn) = run_by(None, 2, &[caller()], EncryptionMode::NodeManaged);
        let Ok(Next::Call(IoCall::Write(writes))) = &turn else {
            panic!("expected one batch write, got {turn:?}");
        };
        let stored = BucketEncryption::from_bytes(writes[0].2.as_ref()).unwrap();
        assert_eq!(stored.max_unlock_ms, None);
    }

    #[test]
    fn refuses_bad_changes() {
        for (max, expected, error) in [
            (
                Some(60_000),
                1,
                UnlockLimitError::Key(BucketKeyError::StaleGeneration {
                    requested: 1,
                    current: 2,
                }),
            ),
            (
                Some(0),
                2,
                UnlockLimitError::Key(BucketKeyError::InvalidDuration),
            ),
        ] {
            let (_, turn) = run(max, expected);
            assert_eq!(turn, Err(error));
        }
    }

    #[test]
    fn revoked_admin_refused() {
        // The route saw the caller as admin, but the role was revoked before the transaction.
        let (_, turn) = run_by(Some(60_000), 2, &[], EncryptionMode::VaultLocked);
        assert_eq!(turn, Err(UnlockLimitError::NotAdmin));
    }
}
