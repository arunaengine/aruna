//! Stages, clears or reschedules one mirror repair record after reading it in a transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::structs::SyncRelationship;
use aruna_core::types::{Key, Value};
use byteview::ByteView;

use super::{
    SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, decode_record, one_value, repair_key,
    repair_read, same_relationship,
};
use crate::tasks::queue_backoff::retry_delay_ms;

/// What the operation does with the record it reads.
#[derive(Debug, PartialEq)]
enum Change {
    /// Writes a new record of this intent, due from the runner's clock.
    Stage(SyncRelationship, SyncMirrorIntent),
    /// Writes the record, unless a staged delete supersedes a reconcile; a claimed rollback of
    /// the same relationship stays claimed.
    Store(SyncMirrorRecord),
    /// Deletes a record of the expected intent; a reconcile must still name `relationship`.
    Clear {
        relationship: SyncRelationship,
        expected: SyncMirrorIntent,
    },
    /// Deletes the record only while it is exactly the scanned one.
    ClearScanned(SyncMirrorRecord),
    /// Moves the scanned record behind its retry delay, only while it is exactly the scanned one.
    Reschedule {
        scanned: SyncMirrorRecord,
        error: String,
    },
}

/// Changes one repair record inside one transaction, deciding from the record it reads.
#[derive(Debug, PartialEq)]
pub struct MirrorRecordOperation {
    key: Key,
    change: Change,
    now_ms: u64,
    /// Set once the change was asked for; its answer ends the operation.
    changing: bool,
}

impl MirrorRecordOperation {
    /// Stages a reconcile or delete intent for `relationship`.
    pub fn stage(relationship: SyncRelationship, intent: SyncMirrorIntent) -> Self {
        Self::with(
            repair_key(&relationship),
            Change::Stage(relationship, intent),
        )
    }

    /// Clears the record of `relationship` once its `expected` intent finished.
    pub fn clear(relationship: SyncRelationship, expected: SyncMirrorIntent) -> Self {
        Self::with(
            repair_key(&relationship),
            Change::Clear {
                relationship,
                expected,
            },
        )
    }

    pub(super) fn store(record: SyncMirrorRecord) -> Self {
        Self::with(repair_key(&record.relationship), Change::Store(record))
    }

    pub(super) fn clear_scanned(scanned: SyncMirrorRecord) -> Self {
        Self::with(
            repair_key(&scanned.relationship),
            Change::ClearScanned(scanned),
        )
    }

    /// Moves a failed record behind its retry delay and keeps the error.
    pub(super) fn reschedule(scanned: SyncMirrorRecord, error: String) -> Self {
        Self::with(
            repair_key(&scanned.relationship),
            Change::Reschedule { scanned, error },
        )
    }

    fn with(key: Key, change: Change) -> Self {
        Self {
            key,
            change,
            now_ms: 0,
            changing: false,
        }
    }

    fn write(&mut self, record: &SyncMirrorRecord) -> Result<Next<()>, SyncMirrorError> {
        let value = ByteView::from(record.to_bytes()?);
        self.changing = true;
        let row = (MIRROR_REPAIR_KEYSPACE.to_string(), self.key.clone(), value);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn delete(&mut self) -> Result<Next<()>, SyncMirrorError> {
        self.changing = true;
        let row = (MIRROR_REPAIR_KEYSPACE.to_string(), self.key.clone());
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn decide(&mut self, value: Option<Value>) -> Result<Next<()>, SyncMirrorError> {
        let existing = decode_record(&self.key, value)?;
        let record = match &self.change {
            Change::Stage(relationship, intent) => {
                SyncMirrorRecord::new(relationship.clone(), *intent, self.now_ms)
            }
            Change::Store(record) => record.clone(),
            Change::Clear {
                relationship,
                expected,
            } => {
                let keep = existing.is_none_or(|existing| {
                    existing.intent != *expected
                        || (*expected == SyncMirrorIntent::Reconcile
                            && existing.relationship != *relationship)
                });
                return if keep {
                    Ok(Next::Done(()))
                } else {
                    self.delete()
                };
            }
            Change::ClearScanned(scanned) => {
                return if existing.as_ref() == Some(scanned) {
                    self.delete()
                } else {
                    Ok(Next::Done(()))
                };
            }
            Change::Reschedule { scanned, error } => {
                if existing.as_ref() != Some(scanned) {
                    return Ok(Next::Done(()));
                }
                let attempts = scanned.attempts.saturating_add(1);
                let next = SyncMirrorRecord {
                    due_at_ms: self.now_ms.saturating_add(retry_delay_ms(attempts)),
                    attempts,
                    last_error: Some(error.clone()),
                    ..scanned.clone()
                };
                return self.write(&next);
            }
        };
        let superseded = record.intent == SyncMirrorIntent::Reconcile
            && existing
                .as_ref()
                .is_some_and(|existing| existing.intent == SyncMirrorIntent::Delete);
        if superseded {
            return Ok(Next::Done(()));
        }
        // A delete staged after a refused activation keeps the claimed rollback.
        let rollback = existing.is_some_and(|existing| {
            existing.rollback && same_relationship(&existing.relationship, &record.relationship)
        });
        let record = SyncMirrorRecord {
            rollback: record.rollback || rollback,
            ..record
        };
        self.write(&record)
    }
}

impl Operation for MirrorRecordOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<()>, SyncMirrorError> {
        Ok(Next::Call(repair_read(&self.key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncMirrorError> {
        if self.changing {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let value = one_value(result)?;
        self.decide(value)
    }

    fn transaction(&self) -> bool {
        true
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::IoError;

    const NOW: u64 = 1_000;

    fn record(intent: SyncMirrorIntent) -> SyncMirrorRecord {
        SyncMirrorRecord::new(relationship(), intent, 5)
    }

    fn values(record: Option<&SyncMirrorRecord>) -> IoResult {
        let value = record.map(|record| ByteView::from(record.to_bytes().unwrap()));
        IoResult::Values(vec![value])
    }

    fn write(record: &SyncMirrorRecord) -> Next<()> {
        Next::Call(IoCall::Write(vec![(
            MIRROR_REPAIR_KEYSPACE.to_string(),
            repair_key(&record.relationship),
            ByteView::from(record.to_bytes().unwrap()),
        )]))
    }

    fn delete() -> Next<()> {
        Next::Call(IoCall::Delete(vec![(
            MIRROR_REPAIR_KEYSPACE.to_string(),
            repair_key(&relationship()),
        )]))
    }

    /// The operation after its first call, stamped at `NOW`.
    fn reading(mut operation: MirrorRecordOperation) -> MirrorRecordOperation {
        operation.stamp(NOW);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(repair_read(&repair_key(&relationship()))))
        );
        operation
    }

    #[test]
    fn stage_writes_record() {
        let mut operation = reading(MirrorRecordOperation::stage(
            relationship(),
            SyncMirrorIntent::Delete,
        ));
        assert!(operation.transaction());
        let staged = SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Delete, NOW);
        assert_eq!(operation.next(values(None)), Ok(write(&staged)));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn delete_supersedes_reconcile() {
        let mut operation = reading(MirrorRecordOperation::store(record(
            SyncMirrorIntent::Reconcile,
        )));
        let staged = record(SyncMirrorIntent::Delete);
        assert_eq!(operation.next(values(Some(&staged))), Ok(Next::Done(())));
    }

    #[test]
    fn delete_keeps_rollback() {
        let claimed = SyncMirrorRecord {
            rollback: true,
            ..record(SyncMirrorIntent::Delete)
        };
        let next = record(SyncMirrorIntent::Delete);
        let mut operation = reading(MirrorRecordOperation::store(next.clone()));
        let expected = SyncMirrorRecord {
            rollback: true,
            ..next
        };
        assert_eq!(operation.next(values(Some(&claimed))), Ok(write(&expected)));
    }

    #[test]
    fn clear_matches_intent() {
        let staged = record(SyncMirrorIntent::Reconcile);
        let clear = || MirrorRecordOperation::clear(relationship(), SyncMirrorIntent::Reconcile);
        assert_eq!(reading(clear()).next(values(Some(&staged))), Ok(delete()));
        let mut newer = staged.clone();
        newer.relationship.status.last_error = Some("newer".to_string());
        let kept = reading(clear()).next(values(Some(&newer)));
        assert_eq!(kept, Ok(Next::Done(())));
        assert_eq!(reading(clear()).next(values(None)), Ok(Next::Done(())));
        let deleting = MirrorRecordOperation::clear(relationship(), SyncMirrorIntent::Delete);
        let staged_delete = SyncMirrorRecord {
            relationship: newer.relationship,
            ..record(SyncMirrorIntent::Delete)
        };
        let deleted = reading(deleting).next(values(Some(&staged_delete)));
        assert_eq!(deleted, Ok(delete()));
    }

    #[test]
    fn scanned_changes_only() {
        let scanned = record(SyncMirrorIntent::Reconcile);
        let mut current = scanned.clone();
        current.due_at_ms = 10;
        let clear = || MirrorRecordOperation::clear_scanned(scanned.clone());
        assert_eq!(
            reading(clear()).next(values(Some(&current))),
            Ok(Next::Done(()))
        );
        assert_eq!(reading(clear()).next(values(Some(&scanned))), Ok(delete()));

        let reschedule = || MirrorRecordOperation::reschedule(scanned.clone(), "down".into());
        let moved = reading(reschedule()).next(values(Some(&current)));
        assert_eq!(moved, Ok(Next::Done(())));
        let next = SyncMirrorRecord {
            due_at_ms: NOW + retry_delay_ms(1),
            attempts: 1,
            last_error: Some("down".to_string()),
            ..scanned.clone()
        };
        assert_eq!(
            reading(reschedule()).next(values(Some(&scanned))),
            Ok(write(&next))
        );
    }

    #[test]
    fn refuses_bad_results() {
        let stage = || MirrorRecordOperation::stage(relationship(), SyncMirrorIntent::Delete);
        let corrupt = IoResult::Values(vec![Some(ByteView::from(vec![0xff]))]);
        assert!(matches!(
            reading(stage()).next(corrupt),
            Err(SyncMirrorError::Conversion(_))
        ));
        assert!(matches!(
            reading(stage()).next(IoResult::Done),
            Err(SyncMirrorError::Io(IoError::Unexpected { .. }))
        ));
        let mut writing = reading(stage());
        writing.next(values(None)).unwrap();
        assert!(matches!(
            writing.next(IoResult::Values(vec![None])),
            Err(SyncMirrorError::Io(IoError::Unexpected { .. }))
        ));
    }
}
