//! Stages, clears or reschedules one mirror repair record after reading it in a transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::SyncRelationship;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;

use super::{
    SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, decode_record, repair_key, repair_read,
    same_relationship,
};
use crate::tasks::queue_backoff::retry_delay_ms;

/// What the operation does with the record it reads.
#[derive(Debug, PartialEq)]
enum Change {
    /// Writes the record, unless a staged delete supersedes a reconcile; a claimed rollback
    /// of the same relationship stays claimed.
    Store(SyncMirrorRecord),
    /// Deletes a record of the expected intent; a reconcile must still name `relationship`.
    Clear {
        relationship: SyncRelationship,
        expected: SyncMirrorIntent,
    },
    /// Deletes the record only while it is exactly the scanned one.
    ClearScanned(SyncMirrorRecord),
    /// Replaces the record with `next` only while it is exactly the scanned one.
    Reschedule {
        scanned: SyncMirrorRecord,
        next: SyncMirrorRecord,
    },
}

#[derive(Debug, PartialEq)]
enum RecordState {
    Ready,
    Starting,
    Reading,
    Changing,
    Committing,
    Done(Result<(), SyncMirrorError>),
}

/// Changes one repair record inside its own transaction, deciding from the record it reads.
#[derive(Debug, PartialEq)]
pub struct MirrorRecordOperation {
    key: Key,
    change: Change,
    txn_id: Option<TxnId>,
    state: RecordState,
}

impl MirrorRecordOperation {
    /// Stages a reconcile or delete intent for `relationship`.
    pub fn stage(relationship: SyncRelationship, intent: SyncMirrorIntent) -> Self {
        Self::store(SyncMirrorRecord::new(relationship, intent))
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
        let attempts = scanned.attempts.saturating_add(1);
        let next = SyncMirrorRecord {
            due_at_ms: unix_timestamp_millis().saturating_add(retry_delay_ms(attempts)),
            attempts,
            last_error: Some(error),
            ..scanned.clone()
        };
        Self::with(
            repair_key(&scanned.relationship),
            Change::Reschedule { scanned, next },
        )
    }

    fn with(key: Key, change: Change) -> Self {
        Self {
            key,
            change,
            txn_id: None,
            state: RecordState::Ready,
        }
    }

    fn finish(&mut self, result: Result<(), SyncMirrorError>) -> Effects {
        self.state = RecordState::Done(result);
        abort_effects(&mut self.txn_id)
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = RecordState::Committing;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn write(&mut self, record: &SyncMirrorRecord, txn_id: TxnId) -> Effects {
        match record.to_bytes() {
            Ok(value) => {
                self.state = RecordState::Changing;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                    key: self.key.clone(),
                    value: ByteView::from(value),
                    txn_id: Some(txn_id),
                })]
            }
            Err(error) => self.finish(Err(error.into())),
        }
    }

    fn delete(&mut self, txn_id: TxnId) -> Effects {
        self.state = RecordState::Changing;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: self.key.clone(),
            txn_id: Some(txn_id),
        })]
    }

    fn decide(&mut self, value: Option<Value>, txn_id: TxnId) -> Effects {
        let existing = match decode_record(&self.key, value) {
            Ok(existing) => existing,
            Err(error) => return self.finish(Err(error.into())),
        };
        let write = match &mut self.change {
            Change::Store(record) => {
                let superseded = record.intent == SyncMirrorIntent::Reconcile
                    && existing
                        .as_ref()
                        .is_some_and(|existing| existing.intent == SyncMirrorIntent::Delete);
                if superseded {
                    return self.commit(txn_id);
                }
                // A delete staged after a refused activation keeps the claimed rollback.
                if existing.is_some_and(|existing| {
                    existing.rollback
                        && same_relationship(&existing.relationship, &record.relationship)
                }) {
                    record.rollback = true;
                }
                record.clone()
            }
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
                    self.commit(txn_id)
                } else {
                    self.delete(txn_id)
                };
            }
            Change::ClearScanned(scanned) => {
                return if existing.as_ref() == Some(scanned) {
                    self.delete(txn_id)
                } else {
                    self.commit(txn_id)
                };
            }
            Change::Reschedule { scanned, next } => {
                if existing.as_ref() != Some(scanned) {
                    return self.commit(txn_id);
                }
                next.clone()
            }
        };
        self.write(&write, txn_id)
    }
}

impl EffectOperation for MirrorRecordOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn start(&mut self) -> Effects {
        if self.state != RecordState::Ready {
            return Effects::new();
        }
        self.state = RecordState::Starting;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, RecordState::Ready);
        match (state, self.txn_id, event) {
            (
                RecordState::Starting,
                None,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = RecordState::Reading;
                smallvec![repair_read(&self.key, Some(txn_id))]
            }
            (
                RecordState::Reading,
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.decide(value, txn_id),
            (
                RecordState::Changing,
                Some(txn_id),
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => self.commit(txn_id),
            (
                RecordState::Committing,
                Some(_),
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.finish(Ok(()))
            }
            (RecordState::Committing, _, event) => {
                // A failed commit consumed the transaction; nothing is left to abort.
                self.txn_id = None;
                self.finish(Err(SyncMirrorError::from_event(event)))
            }
            (_, _, event) => self.finish(Err(SyncMirrorError::from_event(event))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RecordState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            RecordState::Done(result) => result,
            _ => Err(SyncMirrorError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.finish(Err(SyncMirrorError::Unfinished))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

    const TXN: TxnId = Ulid(9);

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN })
    }

    fn read(record: Option<&SyncMirrorRecord>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: record.map(|record| ByteView::from(record.to_bytes().unwrap())),
        })
    }

    fn start_effect() -> Effect {
        Effect::Storage(StorageEffect::StartTransaction { read: false })
    }

    fn commit() -> Effect {
        Effect::Storage(StorageEffect::CommitTransaction { txn_id: TXN })
    }

    fn committed() -> Event {
        Event::Storage(StorageEvent::TransactionCommitted { txn_id: TXN })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    fn write(record: &SyncMirrorRecord) -> Effect {
        Effect::Storage(StorageEffect::Write {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: repair_key(&record.relationship),
            value: ByteView::from(record.to_bytes().unwrap()),
            txn_id: Some(TXN),
        })
    }

    fn delete() -> Effect {
        Effect::Storage(StorageEffect::Delete {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: repair_key(&relationship()),
            txn_id: Some(TXN),
        })
    }

    fn record(intent: SyncMirrorIntent) -> SyncMirrorRecord {
        SyncMirrorRecord {
            due_at_ms: 5,
            ..SyncMirrorRecord::new(relationship(), intent)
        }
    }

    fn reading(operation: MirrorRecordOperation) -> MirrorRecordOperation {
        let mut operation = operation;
        operation.start();
        operation.step(started());
        operation
    }

    #[test]
    fn store_writes_record() {
        let staged = record(SyncMirrorIntent::Reconcile);
        let mut operation = MirrorRecordOperation::store(staged.clone());
        check_trace(
            &mut operation,
            &[start_effect()],
            vec![
                (
                    started(),
                    vec![repair_read(&repair_key(&relationship()), Some(TXN))],
                ),
                (read(None), vec![write(&staged)]),
                (
                    Event::Storage(StorageEvent::WriteResult {
                        key: Vec::<u8>::new().into(),
                    }),
                    vec![commit()],
                ),
                (committed(), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn delete_supersedes_reconcile() {
        let mut operation = reading(MirrorRecordOperation::store(record(
            SyncMirrorIntent::Reconcile,
        )));
        let staged = record(SyncMirrorIntent::Delete);
        assert_eq!(operation.step(read(Some(&staged))).as_slice(), &[commit()]);
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
        assert_eq!(
            operation.step(read(Some(&claimed))).as_slice(),
            &[write(&expected)]
        );
    }

    #[test]
    fn clear_matches_intent() {
        let staged = record(SyncMirrorIntent::Reconcile);
        let clear = || MirrorRecordOperation::clear(relationship(), SyncMirrorIntent::Reconcile);
        assert_eq!(
            reading(clear()).step(read(Some(&staged))).as_slice(),
            &[delete()]
        );
        let mut newer = staged.clone();
        newer.relationship.status.last_error = Some("newer".to_string());
        assert_eq!(
            reading(clear()).step(read(Some(&newer))).as_slice(),
            &[commit()]
        );
        assert_eq!(reading(clear()).step(read(None)).as_slice(), &[commit()]);
        let deleting = MirrorRecordOperation::clear(relationship(), SyncMirrorIntent::Delete);
        let staged_delete = SyncMirrorRecord {
            relationship: newer.relationship,
            ..record(SyncMirrorIntent::Delete)
        };
        assert_eq!(
            reading(deleting)
                .step(read(Some(&staged_delete)))
                .as_slice(),
            &[delete()]
        );
    }

    #[test]
    fn scanned_changes_only() {
        let scanned = record(SyncMirrorIntent::Reconcile);
        let mut current = scanned.clone();
        current.due_at_ms = 10;
        let clear = MirrorRecordOperation::clear_scanned(scanned.clone());
        assert_eq!(
            reading(clear).step(read(Some(&current))).as_slice(),
            &[commit()]
        );
        let clear = MirrorRecordOperation::clear_scanned(scanned.clone());
        assert_eq!(
            reading(clear).step(read(Some(&scanned))).as_slice(),
            &[delete()]
        );

        let reschedule = MirrorRecordOperation::reschedule(scanned.clone(), "down".to_string());
        assert_eq!(
            reading(reschedule).step(read(Some(&current))).as_slice(),
            &[commit()]
        );
        let mut reschedule = reading(MirrorRecordOperation::reschedule(
            scanned.clone(),
            "down".to_string(),
        ));
        let effects = reschedule.step(read(Some(&scanned)));
        let [
            Effect::Storage(StorageEffect::Write {
                value,
                txn_id: Some(TXN),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("reschedule must write the record: {effects:?}");
        };
        let next = SyncMirrorRecord::from_bytes(&repair_key(&relationship()), value).unwrap();
        assert_eq!(
            (next.attempts, next.last_error.as_deref()),
            (1, Some("down"))
        );
        assert!(next.due_at_ms > scanned.due_at_ms);
    }

    #[test]
    fn errors_abort_transaction() {
        let failed = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let stage = || MirrorRecordOperation::stage(relationship(), SyncMirrorIntent::Delete);
        let mut operation = reading(stage());
        assert_eq!(operation.step(failed()).as_slice(), &[abort()]);
        assert_eq!(
            operation.finalize(),
            Err(SyncMirrorError::Storage(StorageError::Timeout))
        );
        let mut corrupt = reading(stage());
        let bad = Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(vec![0xff].into()),
        });
        assert_eq!(corrupt.step(bad).as_slice(), &[abort()]);
        let mut committing = reading(stage());
        committing.step(read(None));
        committing.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::<u8>::new().into(),
        }));
        assert!(committing.step(failed()).is_empty());
        assert!(committing.finalize().is_err());
        let mut unstarted = stage();
        unstarted.start();
        assert!(unstarted.step(failed()).is_empty());
    }

    #[test]
    fn rejects_foreign_events() {
        let make = || {
            reading(MirrorRecordOperation::clear(
                relationship(),
                SyncMirrorIntent::Delete,
            ))
        };
        let refusals = rejects_events(make, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert_eq!(effects.as_slice(), &[abort()], "{label}");
            assert!(matches!(error, SyncMirrorError::Unexpected(_)), "{label}");
        }
        let mut done = reading(MirrorRecordOperation::clear(
            relationship(),
            SyncMirrorIntent::Delete,
        ));
        done.step(read(None));
        done.step(committed());
        done.step(committed());
        assert!(done.finalize().is_err());
    }

    #[test]
    fn abort_and_finalize() {
        let stage = || MirrorRecordOperation::stage(relationship(), SyncMirrorIntent::Delete);
        rejects_finalize(stage());
        rejects_finalize(reading(stage()));
        let mut fresh = stage();
        check_abort(&mut fresh, &[]);
        let mut operation = reading(stage());
        check_abort(&mut operation, &[abort()]);
        assert_eq!(operation.finalize(), Err(SyncMirrorError::Unfinished));
    }
}
