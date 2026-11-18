//! Records the restart lock of encrypted buckets: one audit record per bucket and generation
//! and one notice per bucket and holder, written in one transaction with ids stable per boot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::notifications::outbox_record::schedule_drain_effect;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationOutboxRecord, NotificationRecord,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum RestartNoticeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

impl From<IoError> for RestartNoticeError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
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

#[derive(Debug, PartialEq)]
pub struct RestartNoticeOperation {
    node_id: NodeId,
    boot_id: Ulid,
    buckets: Vec<RestartedBucket>,
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
        }
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
            sequence: None,
            deadline_ms: None,
            reason: Some("node restart".to_string()),
            outcome: AuditOutcome::Applied,
        }
    }

    fn notice(&self, bucket: &RestartedBucket, holder: UserId) -> NotificationOutboxRecord {
        let subject = [&bucket.bucket_id.to_bytes()[..], &holder.to_storage_key()].concat();
        let kind = NotificationKind::BucketRestartLocked {
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

    fn rows(&self) -> Result<Vec<(String, Key, Value)>, RestartNoticeError> {
        let mut writes = Vec::new();
        for bucket in &self.buckets {
            for generation in &bucket.generations {
                let record = self.audit(bucket, *generation);
                let value = record.to_bytes()?.into();
                writes.push((
                    BUCKET_AUDIT_KEYSPACE.to_string(),
                    record.key().into(),
                    value,
                ));
            }
            for holder in &bucket.holders {
                writes.push(outbox_write_entry(&self.notice(bucket, *holder))?);
            }
        }
        Ok(writes)
    }
}

impl Operation for RestartNoticeOperation {
    type Output = ();
    type Error = RestartNoticeError;

    fn first(&mut self) -> Result<Next<()>, RestartNoticeError> {
        if self.buckets.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(IoCall::Write(self.rows()?)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RestartNoticeError> {
        result.done()?;
        // Delivery is retried from the stored outbox, so a lost drain timer only delays it.
        let calls = vec![schedule_drain_effect().into()];
        Ok(Next::Then { output: (), calls })
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::structs::identity::realm::RealmId;

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
        let Ok(Next::Call(IoCall::Write(writes))) = operation.first() else {
            panic!("expected one write");
        };
        writes
            .iter()
            .map(|(space, key, value)| (space.clone(), key.to_vec(), value.to_vec()))
            .collect()
    }

    #[test]
    fn writes_audit_notices() {
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

        // The drain is woken once the rows are committed.
        let drain = vec![schedule_drain_effect().into()];
        let then = Next::Then {
            output: (),
            calls: drain,
        };
        assert_eq!(operation.next(IoResult::Done), Ok(then));
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
    fn nothing_writes_nothing() {
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = RestartNoticeOperation::new(node_id, Ulid::nil(), Vec::new());
        assert_eq!(operation.first(), Ok(Next::Done(())));
    }
}
