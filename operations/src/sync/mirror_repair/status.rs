//! Stores a relationship's sync status with a reconcile record, journaling a stop as failed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{RELATIONSHIP_OUT_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::{SyncRelationship, SyncState, sync_relationship_key};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use super::{
    SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, decode_record, outgoing_read,
    repair_entry, repair_key, repair_read, same_relationship,
};

type Writes = Vec<(String, Key, Value)>;

#[derive(Debug, PartialEq)]
enum StatusState {
    Ready,
    Starting,
    ReadingOutgoing,
    ReadingRecord(SyncState),
    ReadingBucket(Writes),
    Writing,
    Committing(bool),
    Done(Result<bool, SyncMirrorError>),
}

/// Writes the status of an existing relationship and stages its reconcile record in one
/// transaction. Reports false and writes nothing for a missing or detached relationship, or
/// while a delete is staged. A change to failed also journals one plugin event.
#[derive(Debug, PartialEq)]
pub struct StoreStatusOperation {
    relationship: SyncRelationship,
    outgoing_key: Key,
    txn_id: Option<TxnId>,
    state: StatusState,
}

impl StoreStatusOperation {
    pub fn new(relationship: SyncRelationship) -> Self {
        let checked = relationship
            .validate()
            .map_err(SyncMirrorError::from)
            .and_then(|()| {
                relationship
                    .source
                    .bucket()
                    .map(|bucket| ByteView::from(sync_relationship_key(bucket, relationship.id)))
                    .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))
            });
        let (outgoing_key, state) = match checked {
            Ok(key) => (key, StatusState::Ready),
            Err(error) => (Key::from(Vec::new()), StatusState::Done(Err(error))),
        };
        Self {
            relationship,
            outgoing_key,
            txn_id: None,
            state,
        }
    }

    fn finish(&mut self, result: Result<bool, SyncMirrorError>) -> Effects {
        self.state = StatusState::Done(result);
        abort_effects(&mut self.txn_id)
    }

    fn commit(&mut self, stored: bool, txn_id: TxnId) -> Effects {
        self.state = StatusState::Committing(stored);
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn check_outgoing(&mut self, value: Option<Value>, txn_id: TxnId) -> Effects {
        let Some(value) = value else {
            return self.commit(false, txn_id);
        };
        let existing = match SyncRelationship::from_bytes(&value) {
            Ok(existing) => existing,
            Err(error) => return self.finish(Err(error.into())),
        };
        if existing.state == SyncState::Detached {
            // A detached serving stub cannot be overwritten or restage its deleted mirror.
            return self.commit(false, txn_id);
        }
        if !same_relationship(&existing, &self.relationship) {
            return self.finish(Err(SyncMirrorError::Mirror(
                "sync relationship identity changed during status update".to_string(),
            )));
        }
        self.state = StatusState::ReadingRecord(existing.state);
        smallvec![repair_read(&repair_key(&self.relationship), Some(txn_id))]
    }

    fn check_record(
        &mut self,
        previous: SyncState,
        value: Option<Value>,
        txn_id: TxnId,
    ) -> Effects {
        match decode_record(&repair_key(&self.relationship), value) {
            Ok(Some(record)) if record.intent == SyncMirrorIntent::Delete => {
                return self.commit(false, txn_id);
            }
            Ok(_) => {}
            Err(error) => return self.finish(Err(error.into())),
        }
        let repair = SyncMirrorRecord::new(self.relationship.clone(), SyncMirrorIntent::Reconcile);
        let writes = self
            .relationship
            .to_bytes()
            .map_err(SyncMirrorError::from)
            .and_then(|row| {
                Ok(vec![
                    (
                        RELATIONSHIP_OUT_KEYSPACE.to_string(),
                        self.outgoing_key.clone(),
                        ByteView::from(row),
                    ),
                    repair_entry(repair)?,
                ])
            });
        let writes = match writes {
            Ok(writes) => writes,
            Err(error) => return self.finish(Err(error)),
        };
        let failed = |state: &SyncState| matches!(state, SyncState::Failed { .. });
        if failed(&self.relationship.state) && !failed(&previous) {
            let bucket = self.relationship.source.bucket().unwrap_or_default();
            self.state = StatusState::ReadingBucket(writes);
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: ByteView::from(bucket.as_bytes().to_vec()),
                txn_id: Some(txn_id),
            })];
        }
        self.write(writes, txn_id)
    }

    /// Adds the failure event of the source bucket row; none once that bucket is gone.
    fn journal(&mut self, mut writes: Writes, value: Option<Value>, txn_id: TxnId) -> Effects {
        if let Some(value) = value {
            let entries = BucketInfo::from_bytes(&value)
                .map(|info| failure_event(&self.relationship, &info))
                .and_then(|event| journal_entries(&event));
            match entries {
                Ok(entries) => writes.extend(entries),
                Err(error) => return self.finish(Err(error.into())),
            }
        }
        self.write(writes, txn_id)
    }

    fn write(&mut self, writes: Writes, txn_id: TxnId) -> Effects {
        self.state = StatusState::Writing;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for StoreStatusOperation {
    type Output = bool;
    type Error = SyncMirrorError;

    fn start(&mut self) -> Effects {
        if self.state != StatusState::Ready {
            return Effects::new();
        }
        self.state = StatusState::Starting;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, StatusState::Ready);
        match (state, self.txn_id, event) {
            (
                StatusState::Starting,
                None,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = StatusState::ReadingOutgoing;
                smallvec![outgoing_read(&self.outgoing_key, Some(txn_id))]
            }
            (
                StatusState::ReadingOutgoing,
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.check_outgoing(value, txn_id),
            (
                StatusState::ReadingRecord(previous),
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.check_record(previous, value, txn_id),
            (
                StatusState::ReadingBucket(writes),
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.journal(writes, value, txn_id),
            (
                StatusState::Writing,
                Some(txn_id),
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => self.commit(true, txn_id),
            (
                StatusState::Committing(stored),
                Some(_),
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.finish(Ok(stored))
            }
            (StatusState::Committing(_), _, event) => {
                // A failed commit consumed the transaction; nothing is left to abort.
                self.txn_id = None;
                self.finish(Err(SyncMirrorError::from_event(event)))
            }
            (_, _, event) => self.finish(Err(SyncMirrorError::from_event(event))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, StatusState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            StatusState::Done(result) => result,
            _ => Err(SyncMirrorError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.finish(Err(SyncMirrorError::Unfinished))
    }
}

/// The plugin event of a relationship that stops as failed.
fn failure_event(relationship: &SyncRelationship, info: &BucketInfo) -> SourceEvent {
    let source = &relationship.source;
    let bucket = source.bucket().unwrap_or_default();
    let prefix = source.key_prefix().unwrap_or_default();
    let path = object_permission_path(
        source.realm_id,
        info.group_id,
        source.node_id,
        bucket,
        prefix,
    );
    SourceEvent::sync_failure(
        (info.group_id, path),
        (relationship.id, relationship.created_by),
        (source.node_id, Ulid::generate()),
        unix_timestamp_millis(),
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const TXN: TxnId = Ulid(13);

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

    fn reading(relationship: SyncRelationship) -> StoreStatusOperation {
        let mut operation = StoreStatusOperation::new(relationship);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        operation
    }

    fn batch(effects: &[Effect]) -> Vec<String> {
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(TXN),
            }),
        ] = effects
        else {
            panic!("expected one batch write: {effects:?}");
        };
        writes
            .iter()
            .map(|(key_space, ..)| key_space.clone())
            .collect()
    }

    #[test]
    fn stores_status() {
        let mut operation = StoreStatusOperation::new(relationship());
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![outgoing_read(&outgoing_key(), Some(TXN))],
                ),
                (
                    read(Some(relationship().to_bytes().unwrap())),
                    vec![repair_read(&repair_key(&relationship()), Some(TXN))],
                ),
            ],
        );
        let effects = operation.step(read(None));
        assert_eq!(
            batch(&effects),
            vec![
                RELATIONSHIP_OUT_KEYSPACE.to_string(),
                aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE.to_string()
            ]
        );
        let written = Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        });
        assert_eq!(operation.step(written).as_slice(), &[commit()]);
        operation.step(committed());
        assert_eq!(operation.finalize(), Ok(true));
    }

    #[test]
    fn failure_reads_bucket() {
        let mut failed = relationship();
        failed.state = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        let mut operation = reading(failed);
        operation.step(read(Some(relationship().to_bytes().unwrap())));
        let effects = operation.step(read(None));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == S3_BUCKET_KEYSPACE
        ));
        // A missing source bucket journals nothing.
        assert_eq!(batch(&operation.step(read(None))).len(), 2);
    }

    #[test]
    fn skips_without_change() {
        let mut missing = reading(relationship());
        assert_eq!(missing.step(read(None)).as_slice(), &[commit()]);
        missing.step(committed());
        assert_eq!(missing.finalize(), Ok(false));

        let mut stub = relationship();
        stub.state = SyncState::Detached;
        let mut detached = reading(relationship());
        assert_eq!(
            detached
                .step(read(Some(stub.to_bytes().unwrap())))
                .as_slice(),
            &[commit()]
        );

        let staged = SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Delete);
        let mut deleting = reading(relationship());
        deleting.step(read(Some(relationship().to_bytes().unwrap())));
        assert_eq!(
            deleting
                .step(read(Some(staged.to_bytes().unwrap())))
                .as_slice(),
            &[commit()]
        );
    }

    #[test]
    fn errors_abort_transaction() {
        let mut changed = relationship();
        changed.replicate_deletes = false;
        let mut operation = reading(changed);
        assert_eq!(
            operation
                .step(read(Some(relationship().to_bytes().unwrap())))
                .as_slice(),
            &[abort()]
        );
        assert!(matches!(
            operation.finalize(),
            Err(SyncMirrorError::Mirror(_))
        ));
        let mut failed = reading(relationship());
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(failed.step(error).as_slice(), &[abort()]);
    }

    #[test]
    fn rejects_foreign_events() {
        let make = || reading(relationship());
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
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(StoreStatusOperation::new(relationship()));
        rejects_finalize(reading(relationship()));
        let mut operation = reading(relationship());
        check_abort(&mut operation, &[abort()]);
        assert_eq!(operation.finalize(), Err(SyncMirrorError::Unfinished));
    }
}
