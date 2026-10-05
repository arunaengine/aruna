//! Changes the longest unlock of an encrypted bucket. Running sessions keep their bounds; the
//! next unlock uses the new maximum.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
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
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the unlock limit change did not finish")]
    NotFinished,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LimitStep {
    Init,
    StartTransaction,
    ReadBucket,
    WriteSettings,
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct UnlockLimitOperation {
    input: UnlockLimitInput,
    step: LimitStep,
    txn_id: Option<TxnId>,
    settings: Option<BucketEncryption>,
    output: Option<Result<BucketEncryption, UnlockLimitError>>,
}

impl UnlockLimitOperation {
    pub fn new(input: UnlockLimitInput) -> Self {
        Self {
            input,
            step: LimitStep::Init,
            txn_id: None,
            settings: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<UnlockLimitError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.step = LimitStep::Error;
        effects
    }

    fn write(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let mut settings = match parse_authority(values, realm_id, group_id) {
            Ok(state) if state.admins.contains(&self.input.caller) => state.settings,
            Ok(_) => return self.fail(UnlockLimitError::NotAdmin),
            Err(error) => return self.fail(error),
        };
        let (Some(bucket_id), true) = (settings.bucket_id, settings.is_encrypted()) else {
            return self.fail(UnlockLimitError::NotEncrypted);
        };
        if settings.storage_generation != self.input.expected_generation {
            return self.fail(BucketKeyError::StaleGeneration {
                requested: self.input.expected_generation,
                current: settings.storage_generation,
            });
        }
        if self.input.max_unlock_ms == Some(0) {
            return self.fail(BucketKeyError::InvalidDuration);
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
            deadline_ms: None,
            reason: Some("unlock maximum changed".to_string()),
            outcome: AuditOutcome::Applied,
        };
        let (value, audit) = match (settings.to_bytes(), record.to_bytes()) {
            (Ok(value), Ok(audit)) => (value, audit),
            (Err(error), _) | (_, Err(error)) => return self.fail(error),
        };
        let bucket: Key = self.input.bucket.as_bytes().to_vec().into();
        self.settings = Some(settings);
        self.step = LimitStep::WriteSettings;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket, value.into()),
                (
                    BUCKET_AUDIT_KEYSPACE.to_string(),
                    record.key().into(),
                    audit.into()
                ),
            ],
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for UnlockLimitOperation {
    type Output = BucketEncryption;
    type Error = UnlockLimitError;

    fn start(&mut self) -> Effects {
        self.step = LimitStep::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (LimitStep::Finish | LimitStep::Error, _) => smallvec![],
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                LimitStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.step = LimitStep::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (LimitStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.write(values)
            }
            (LimitStep::WriteSettings, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(UnlockLimitError::NotFinished);
                };
                self.step = LimitStep::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (LimitStep::Commit, Event::Storage(StorageEvent::TransactionCommitted { txn_id }))
                if Some(txn_id) == self.txn_id =>
            {
                self.txn_id = None;
                self.output = self.settings.take().map(Ok);
                self.step = LimitStep::Finish;
                smallvec![]
            }
            (state, received) => self.fail(UnlockLimitError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, LimitStep::Finish | LimitStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UnlockLimitError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
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

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

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
        };
        crate::s3::bucket::key_rows::authority_rows(&info, Some(settings), admins)
    }

    fn run(max_unlock_ms: Option<u64>, expected: u64) -> (UnlockLimitOperation, Effects) {
        run_by(max_unlock_ms, expected, &[caller()])
    }

    /// Runs a change by `caller()` while `admins` hold the group admin role in the transaction.
    fn run_by(
        max_unlock_ms: Option<u64>,
        expected: u64,
        admins: &[UserId],
    ) -> (UnlockLimitOperation, Effects) {
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
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([4; 16])),
            key_generation: 1,
            storage_generation: 2,
            max_unlock_ms: Some(1_000),
            ..Default::default()
        };
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: rows(&settings, admins),
        }));
        (operation, effects)
    }

    #[test]
    fn stores_new_maximum() {
        let (mut operation, effects) = run(Some(60_000), 2);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = &effects[..] else {
            panic!("expected one batch write, got {effects:?}");
        };
        let stored = BucketEncryption::from_bytes(writes[0].2.as_ref()).unwrap();
        assert_eq!(stored.max_unlock_ms, Some(60_000));
        assert_eq!(writes[1].0, BUCKET_AUDIT_KEYSPACE);
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TXN,
        }));
        assert_eq!(operation.finalize().unwrap().max_unlock_ms, Some(60_000));
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
            let (operation, effects) = run(max, expected);
            assert!(matches!(
                &effects[..],
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ));
            assert_eq!(operation.finalize(), Err(error));
        }
    }

    #[test]
    fn revoked_admin_refused() {
        // The route saw the caller as admin, but the role was revoked before the transaction.
        let (operation, effects) = run_by(Some(60_000), 2, &[]);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(operation.finalize(), Err(UnlockLimitError::NotAdmin));
    }
}
