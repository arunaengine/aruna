//! Settles a scanned reconcile record: keeps it for an active relationship or claims a rollback.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::{SyncRelationship, sync_relationship_key};
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;

use super::{
    SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, decode_record, outgoing_read, repair_key,
    repair_read,
};

/// What a reconcile pass finds when it tries to claim a rollback.
#[derive(Debug, PartialEq)]
pub(super) enum RollbackClaim {
    /// The scanned record changed, so this pass has nothing to do.
    Stale,
    /// The relationship was activated and is reconciled as usual.
    Active(SyncRelationship),
    /// The create never activated; the record now asks for a rollback.
    Claimed,
}

#[derive(Debug, PartialEq)]
enum SettleState {
    Ready,
    Starting,
    ReadingRecord,
    ReadingOutgoing,
    Claiming,
    Committing(RollbackClaim),
    Done(Result<RollbackClaim, SyncMirrorError>),
}

/// Turns a scanned reconcile record whose outgoing record is absent into a rollback, in one
/// transaction. Activation rewrites the same record, so only one of the two commits.
#[derive(Debug, PartialEq)]
pub(super) struct SettleReconcileOperation {
    scanned: SyncMirrorRecord,
    key: Key,
    txn_id: Option<TxnId>,
    state: SettleState,
}

impl SettleReconcileOperation {
    pub(super) fn new(scanned: SyncMirrorRecord) -> Self {
        Self {
            key: repair_key(&scanned.relationship),
            scanned,
            txn_id: None,
            state: SettleState::Ready,
        }
    }

    fn finish(&mut self, result: Result<RollbackClaim, SyncMirrorError>) -> Effects {
        self.state = SettleState::Done(result);
        abort_effects(&mut self.txn_id)
    }

    fn commit(&mut self, claim: RollbackClaim, txn_id: TxnId) -> Effects {
        self.state = SettleState::Committing(claim);
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn check_record(&mut self, value: Option<Value>, txn_id: TxnId) -> Effects {
        match decode_record(&self.key, value) {
            Ok(existing) if existing.as_ref() == Some(&self.scanned) => {}
            Ok(_) => return self.commit(RollbackClaim::Stale, txn_id),
            Err(error) => return self.finish(Err(error.into())),
        }
        let Some(bucket) = self.scanned.relationship.source.bucket() else {
            return self.finish(Err(SyncMirrorError::Mirror(
                "invalid source ARN".to_string(),
            )));
        };
        let key = ByteView::from(sync_relationship_key(bucket, self.scanned.relationship.id));
        self.state = SettleState::ReadingOutgoing;
        smallvec![outgoing_read(&key, Some(txn_id))]
    }

    fn check_outgoing(&mut self, value: Option<Value>, txn_id: TxnId) -> Effects {
        if let Some(value) = value {
            return match SyncRelationship::from_bytes(&value) {
                Ok(existing) => self.commit(RollbackClaim::Active(existing), txn_id),
                Err(error) => self.finish(Err(error.into())),
            };
        }
        let claimed = SyncMirrorRecord {
            rollback: true,
            ..SyncMirrorRecord::new(self.scanned.relationship.clone(), SyncMirrorIntent::Delete)
        };
        match claimed.to_bytes() {
            Ok(value) => {
                self.state = SettleState::Claiming;
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
}

impl EffectOperation for SettleReconcileOperation {
    type Output = RollbackClaim;
    type Error = SyncMirrorError;

    fn start(&mut self) -> Effects {
        if self.state != SettleState::Ready {
            return Effects::new();
        }
        self.state = SettleState::Starting;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, SettleState::Ready);
        match (state, self.txn_id, event) {
            (
                SettleState::Starting,
                None,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = SettleState::ReadingRecord;
                smallvec![repair_read(&self.key, Some(txn_id))]
            }
            (
                SettleState::ReadingRecord,
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.check_record(value, txn_id),
            (
                SettleState::ReadingOutgoing,
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.check_outgoing(value, txn_id),
            (
                SettleState::Claiming,
                Some(txn_id),
                Event::Storage(StorageEvent::WriteResult { .. }),
            ) => self.commit(RollbackClaim::Claimed, txn_id),
            (
                SettleState::Committing(claim),
                Some(_),
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.finish(Ok(claim))
            }
            (SettleState::Committing(_), _, event) => {
                // A failed commit consumed the transaction; nothing is left to abort.
                self.txn_id = None;
                self.finish(Err(SyncMirrorError::from_event(event)))
            }
            (_, _, event) => self.finish(Err(SyncMirrorError::from_event(event))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SettleState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            SettleState::Done(result) => result,
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

    const TXN: TxnId = Ulid(11);

    fn scanned() -> SyncMirrorRecord {
        SyncMirrorRecord {
            due_at_ms: 3,
            ..SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Reconcile)
        }
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: value.map(ByteView::from),
        })
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

    fn outgoing_key() -> Key {
        ByteView::from(sync_relationship_key("source", relationship().id))
    }

    fn reading() -> SettleReconcileOperation {
        let mut operation = SettleReconcileOperation::new(scanned());
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        operation
    }

    fn outgoing_phase() -> SettleReconcileOperation {
        let mut operation = reading();
        operation.step(read(Some(scanned().to_bytes().unwrap())));
        operation
    }

    #[test]
    fn claims_missing_activation() {
        let mut operation = SettleReconcileOperation::new(scanned());
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![repair_read(&repair_key(&relationship()), Some(TXN))],
                ),
                (
                    read(Some(scanned().to_bytes().unwrap())),
                    vec![outgoing_read(&outgoing_key(), Some(TXN))],
                ),
            ],
        );
        let effects = operation.step(read(None));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("a missing activation must claim the rollback: {effects:?}");
        };
        assert_eq!(key_space, MIRROR_REPAIR_KEYSPACE);
        let claimed = SyncMirrorRecord::from_bytes(&repair_key(&relationship()), value).unwrap();
        assert!(claimed.rollback);
        assert_eq!(claimed.intent, SyncMirrorIntent::Delete);
        let written = Event::Storage(StorageEvent::WriteResult {
            key: Vec::<u8>::new().into(),
        });
        assert_eq!(operation.step(written).as_slice(), &[commit()]);
        operation.step(committed());
        assert_eq!(operation.finalize(), Ok(RollbackClaim::Claimed));
    }

    #[test]
    fn active_and_stale() {
        let mut active = outgoing_phase();
        let stored = relationship().to_bytes().unwrap();
        assert_eq!(active.step(read(Some(stored))).as_slice(), &[commit()]);
        active.step(committed());
        assert_eq!(active.finalize(), Ok(RollbackClaim::Active(relationship())));

        let mut changed = scanned();
        changed.due_at_ms = 99;
        let mut stale = reading();
        assert_eq!(
            stale
                .step(read(Some(changed.to_bytes().unwrap())))
                .as_slice(),
            &[commit()]
        );
        stale.step(committed());
        assert_eq!(stale.finalize(), Ok(RollbackClaim::Stale));
    }

    #[test]
    fn errors_abort_transaction() {
        let mut operation = outgoing_phase();
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(operation.step(failed).as_slice(), &[abort()]);
        assert_eq!(
            operation.finalize(),
            Err(SyncMirrorError::Storage(StorageError::Timeout))
        );
        let mut corrupt = outgoing_phase();
        assert_eq!(corrupt.step(read(Some(vec![0xff]))).as_slice(), &[abort()]);
        let mut conflict = outgoing_phase();
        conflict.step(read(Some(relationship().to_bytes().unwrap())));
        let conflicted = Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        });
        assert!(conflict.step(conflicted).is_empty());
        assert_eq!(
            conflict.finalize(),
            Err(SyncMirrorError::Storage(StorageError::TransactionConflict))
        );
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(outgoing_phase, sample_events, |event| {
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
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(SettleReconcileOperation::new(scanned()));
        rejects_finalize(reading());
        let mut operation = outgoing_phase();
        check_abort(&mut operation, &[abort()]);
        assert_eq!(operation.finalize(), Err(SyncMirrorError::Unfinished));
    }
}
