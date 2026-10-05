//! Records the restart lock of encrypted buckets: one audit record per bucket and generation
//! and one notice per bucket and holder, written in one transaction with ids stable per boot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::notifications::outbox::schedule_drain_effect;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationOutboxRecord, NotificationRecord,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{Effects, GroupId, TxnId};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum RestartNoticeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the restart notice did not finish")]
    NotFinished,
}

/// A bucket whose last recorded session was unlocked before the restart, with the generations
/// that were unlocked and the holders to tell.
#[derive(Clone, Debug, PartialEq)]
pub struct RestartedBucket {
    pub bucket: String,
    pub bucket_id: Ulid,
    pub group_id: GroupId,
    pub generations: Vec<u64>,
    pub holders: Vec<UserId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NoticeStep {
    Init,
    StartTransaction,
    WriteRows,
    Commit,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct RestartNoticeOperation {
    node_id: NodeId,
    boot_id: Ulid,
    buckets: Vec<RestartedBucket>,
    step: NoticeStep,
    txn_id: Option<TxnId>,
    output: Option<Result<(), RestartNoticeError>>,
}

/// An id stable for one boot and subject, so a retried startup rewrites the same rows.
fn stable_id(boot_id: Ulid, parts: &[&[u8]]) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&boot_id.to_bytes());
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&hasher.finalize().as_bytes()[..10]);
    Ulid::from_parts(boot_id.timestamp_ms(), u128::from_be_bytes(random))
}

impl RestartNoticeOperation {
    /// `boot_id` names this start of the node; its time is the time of every record.
    pub fn new(node_id: NodeId, boot_id: Ulid, buckets: Vec<RestartedBucket>) -> Self {
        Self {
            node_id,
            boot_id,
            buckets,
            step: NoticeStep::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<RestartNoticeError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = NoticeStep::Error;
        self.abort()
    }

    fn audit(&self, bucket: &RestartedBucket, generation: u64) -> BucketAuditRecord {
        let id = stable_id(
            self.boot_id,
            &[
                b"audit",
                &bucket.bucket_id.to_bytes(),
                &generation.to_be_bytes(),
            ],
        );
        BucketAuditRecord {
            event_id: id,
            bucket_id: bucket.bucket_id,
            at_ms: self.boot_id.timestamp_ms(),
            action: AuditAction::RestartLock,
            actor: None,
            node_id: self.node_id,
            generation: Some(generation),
            session_id: None,
            intent_id: None,
            deadline_ms: None,
            reason: Some("node restart".to_string()),
            outcome: AuditOutcome::Applied,
        }
    }

    fn notice(&self, bucket: &RestartedBucket, holder: UserId) -> NotificationOutboxRecord {
        let subject = [&bucket.bucket_id.to_bytes()[..], &holder.to_storage_key()].concat();
        let kind = NotificationKind::BucketLockedByRestart {
            bucket: bucket.bucket.clone(),
            node_id: self.node_id,
            group_id: bucket.group_id,
        };
        let mut record = NotificationRecord::new(
            holder,
            NotificationClass::Direct,
            kind,
            self.boot_id.timestamp_ms(),
        );
        record.notification_id = stable_id(self.boot_id, &[b"notice", &subject]);
        NotificationOutboxRecord {
            outbox_id: stable_id(self.boot_id, &[b"outbox", &subject]),
            record,
        }
    }

    fn write(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        let mut writes = Vec::new();
        for bucket in &self.buckets {
            for generation in &bucket.generations {
                let record = self.audit(bucket, *generation);
                match record.to_bytes() {
                    Ok(value) => writes.push((
                        BUCKET_AUDIT_KEYSPACE.to_string(),
                        record.key().into(),
                        value.into(),
                    )),
                    Err(error) => return self.fail(error),
                }
            }
            for holder in &bucket.holders {
                match outbox_write_entry(&self.notice(bucket, *holder)) {
                    Ok(entry) => writes.push(entry),
                    Err(error) => return self.fail(error),
                }
            }
        }
        self.step = NoticeStep::WriteRows;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for RestartNoticeOperation {
    type Output = ();
    type Error = RestartNoticeError;

    fn start(&mut self) -> Effects {
        if self.buckets.is_empty() {
            self.output = Some(Ok(()));
            self.step = NoticeStep::Finish;
            return smallvec![];
        }
        self.step = NoticeStep::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (NoticeStep::Finish | NoticeStep::Error, _) => smallvec![],
            // Delivery is retried from the stored outbox, so a lost drain timer only delays it.
            (NoticeStep::ScheduleDrain, Event::Task(_)) => {
                self.output = Some(Ok(()));
                self.step = NoticeStep::Finish;
                smallvec![]
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                NoticeStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => self.write(txn_id),
            (NoticeStep::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(RestartNoticeError::NotFinished);
                };
                self.step = NoticeStep::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (NoticeStep::Commit, Event::Storage(StorageEvent::TransactionCommitted { txn_id }))
                if Some(txn_id) == self.txn_id =>
            {
                self.txn_id = None;
                self.step = NoticeStep::ScheduleDrain;
                smallvec![schedule_drain_effect()]
            }
            (state, received) => self.fail(RestartNoticeError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, NoticeStep::Finish | NoticeStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RestartNoticeError::NotFinished))
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
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::task::{TaskEvent, TaskKey};
    use std::time::Duration;

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn operation(boot: u64) -> RestartNoticeOperation {
        let bucket = RestartedBucket {
            bucket: "raw".to_string(),
            bucket_id: Ulid::from_bytes([4; 16]),
            group_id: Ulid::from_bytes([3; 16]),
            generations: vec![1, 2],
            holders: vec![user(5), user(6)],
        };
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        RestartNoticeOperation::new(node_id, Ulid::from_parts(boot, 9), vec![bucket])
    }

    fn rows(operation: &mut RestartNoticeOperation) -> Vec<(String, Vec<u8>, Vec<u8>)> {
        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from_bytes([7; 16]),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = &effects[..] else {
            panic!("expected one batch write, got {effects:?}");
        };
        assert!(txn_id.is_some());
        writes
            .iter()
            .map(|(space, key, value)| (space.clone(), key.to_vec(), value.to_vec()))
            .collect()
    }

    #[test]
    fn writes_audit_and_notices() {
        let mut operation = operation(1_000);
        let rows = rows(&mut operation);
        let audits: Vec<_> = rows
            .iter()
            .filter(|(space, ..)| space == BUCKET_AUDIT_KEYSPACE)
            .map(|(_, _, value)| BucketAuditRecord::from_bytes(value).unwrap())
            .collect();
        assert_eq!(audits.len(), 2);
        assert!(
            audits.iter().all(|record| {
                record.action == AuditAction::RestartLock && record.actor.is_none()
            })
        );
        let notices: Vec<_> = rows
            .iter()
            .filter(|(space, ..)| space == NOTIFICATION_OUTBOX_KEYSPACE)
            .map(|(_, _, value)| NotificationOutboxRecord::from_bytes(value).unwrap())
            .collect();
        assert_eq!(notices.len(), 2);
        assert_eq!(notices[0].record.recipient, user(5));
        assert_eq!(notices[0].record.kind.name(), "bucket_locked_by_restart");
        assert_eq!(notices[0].record.created_at_ms, 1_000);

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from_bytes([7; 16]),
        }));
        assert_eq!(effects[..], [schedule_drain_effect()]);
        operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::ZERO,
        }));
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn stable_per_boot() {
        let keys = |boot| -> Vec<Vec<u8>> {
            rows(&mut operation(boot))
                .into_iter()
                .map(|(_, key, _)| key)
                .collect()
        };
        assert_eq!(keys(1_000), keys(1_000));
        assert_ne!(keys(1_000), keys(2_000));
    }

    #[test]
    fn failed_write_aborts() {
        let mut operation = operation(1_000);
        rows(&mut operation);
        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError("disk".to_string()),
        }));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(operation.finalize().is_err());
    }
}
