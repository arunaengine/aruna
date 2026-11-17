//! Activates a sync relationship: claims its staged repair record and stores the outgoing row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{MIRROR_REPAIR_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;

use super::{
    SyncRelationshipDirection, SyncRelationshipError, iter_effect, parse_values,
    same_create_identity, storage_key,
};
use crate::sync::mirror_repair::{SyncMirrorIntent, SyncMirrorRecord, same_relationship};

const CREATE_ATTEMPTS: usize = 3;

#[derive(Debug, PartialEq)]
enum CreateState {
    Ready,
    Starting,
    Scanning,
    Claiming,
    WritingClaim,
    Writing,
    Committing,
    Done(Result<(), SyncRelationshipError>),
}

/// Stores the outgoing record and rewrites the staged reconcile record in one transaction, so a
/// concurrent rollback claim fails at commit; a missing or changed record means a rollback.
/// A transaction conflict starts the attempt again, up to three times.
#[derive(Debug, PartialEq)]
pub struct CreateRelationshipOperation {
    relationship: SyncRelationship,
    /// Scans the source bucket for an enabled relationship with the same endpoints and mode.
    check_duplicates: bool,
    attempt: usize,
    txn_id: Option<TxnId>,
    state: CreateState,
}

impl CreateRelationshipOperation {
    /// A new relationship, refused when an enabled one with the same endpoints and mode exists.
    pub fn new(relationship: SyncRelationship) -> Self {
        let has_bucket = relationship.source.bucket().is_some();
        let mut operation = Self::activate(relationship);
        operation.check_duplicates = true;
        if !has_bucket {
            operation.state = CreateState::Done(Err(not_s3_arn()));
        }
        operation
    }

    /// A relationship that replaces older ones of the same endpoints, so no duplicate check.
    pub fn activate(relationship: SyncRelationship) -> Self {
        Self {
            relationship,
            check_duplicates: false,
            attempt: 0,
            txn_id: None,
            state: CreateState::Ready,
        }
    }

    fn finish(&mut self, result: Result<(), SyncRelationshipError>) -> Effects {
        self.state = CreateState::Done(result);
        abort_effects(&mut self.txn_id)
    }

    fn unexpected(&mut self, state: &'static str, expected: &'static str, event: Event) -> Effects {
        let error = match event {
            Event::Storage(StorageEvent::Error { error }) => error.into(),
            received => SyncRelationshipError::UnexpectedEvent {
                state,
                expected,
                received,
            },
        };
        self.finish(Err(error))
    }

    fn begin(&mut self) -> Effects {
        self.state = CreateState::Starting;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// A conflict starts a new attempt while attempts remain.
    fn retry(&mut self, error: StorageError) -> Effects {
        if error == StorageError::TransactionConflict && self.attempt + 1 < CREATE_ATTEMPTS {
            self.attempt += 1;
            return self.begin();
        }
        self.finish(Err(error.into()))
    }

    fn claim_read(&mut self, txn_id: TxnId) -> Effects {
        self.state = CreateState::Claiming;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: repair_key(&self.relationship),
            txn_id: Some(txn_id),
        })]
    }

    fn scan(&mut self, values: Vec<(Key, Value)>, next: Option<Key>, txn_id: TxnId) -> Effects {
        let existing = match parse_values(SyncRelationshipDirection::Outgoing, values) {
            Ok(existing) => existing,
            Err(error) => return self.finish(Err(error.into())),
        };
        // Detached stubs keep retained references readable without blocking recreation.
        if existing.iter().any(|existing| {
            existing.state != SyncState::Detached
                && same_create_identity(existing, &self.relationship)
        }) {
            return self.finish(Err(SyncRelationshipError::Duplicate));
        }
        match next {
            Some(next) => smallvec![self.scan_effect(Some(next), txn_id)],
            None => self.claim_read(txn_id),
        }
    }

    fn scan_effect(&self, start_after: Option<Key>, txn_id: TxnId) -> Effect {
        iter_effect(
            SyncRelationshipDirection::Outgoing,
            self.relationship.source.bucket(),
            start_after,
            Some(txn_id),
        )
    }

    fn claim(&mut self, value: Option<Value>, txn_id: TxnId) -> Effects {
        let Some(value) = value else {
            return self.finish(Err(SyncRelationshipError::RolledBack));
        };
        let key = repair_key(&self.relationship);
        let record = match SyncMirrorRecord::from_bytes(&key, &value) {
            Ok(record) => record,
            Err(error) => return self.finish(Err(error.into())),
        };
        if record.intent != SyncMirrorIntent::Reconcile
            || !same_relationship(&record.relationship, &self.relationship)
        {
            return self.finish(Err(SyncRelationshipError::RolledBack));
        }
        self.state = CreateState::WritingClaim;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })]
    }

    fn write(&mut self, txn_id: TxnId) -> Effects {
        let entry = storage_key(SyncRelationshipDirection::Outgoing, &self.relationship)
            .and_then(|key| Ok((key, ByteView::from(self.relationship.to_bytes()?))));
        let (key, value) = match entry {
            Ok(entry) => entry,
            Err(error) => return self.finish(Err(error.into())),
        };
        self.state = CreateState::Writing;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for CreateRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        match self.state {
            CreateState::Ready => self.begin(),
            _ => Effects::new(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::replace(&mut self.state, CreateState::Ready);
        match (state, self.txn_id, event) {
            (
                CreateState::Starting,
                None,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                if self.check_duplicates {
                    self.state = CreateState::Scanning;
                    smallvec![self.scan_effect(None, txn_id)]
                } else {
                    self.claim_read(txn_id)
                }
            }
            (CreateState::Starting, None, Event::Storage(StorageEvent::Error { error })) => {
                self.retry(error)
            }
            (
                CreateState::Scanning,
                Some(txn_id),
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.state = CreateState::Scanning;
                self.scan(values, next_start_after, txn_id)
            }
            (
                CreateState::Claiming,
                Some(txn_id),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.claim(value, txn_id),
            (
                CreateState::WritingClaim,
                Some(txn_id),
                Event::Storage(StorageEvent::WriteResult { .. }),
            ) => self.write(txn_id),
            (
                CreateState::Writing,
                Some(txn_id),
                Event::Storage(StorageEvent::WriteResult { .. }),
            ) => {
                self.state = CreateState::Committing;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                CreateState::Committing,
                Some(_),
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.finish(Ok(()))
            }
            (CreateState::Committing, Some(_), Event::Storage(StorageEvent::Error { error })) => {
                // A failed commit consumed the transaction; nothing is left to abort.
                self.txn_id = None;
                self.retry(error)
            }
            (CreateState::Starting, _, event) => {
                self.unexpected("StartingCreate", "storage transaction started", event)
            }
            (CreateState::Scanning, _, event) => {
                self.unexpected("ScanningCreate", "storage iteration result", event)
            }
            (CreateState::Claiming, _, event) => {
                self.unexpected("ClaimingActivation", "storage read result", event)
            }
            (CreateState::WritingClaim, _, event) => {
                self.unexpected("ClaimingActivation", "storage write result", event)
            }
            (CreateState::Writing, _, event) => {
                self.unexpected("WritingCreate", "storage write result", event)
            }
            (CreateState::Committing, _, event) => {
                self.unexpected("CommittingCreate", "storage transaction committed", event)
            }
            (_, _, event) => self.unexpected("terminal", "no event", event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, CreateState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            CreateState::Done(Ok(())) => Ok(self.relationship),
            CreateState::Done(Err(error)) => Err(error),
            _ => Err(SyncRelationshipError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.finish(Err(SyncRelationshipError::NotFinished))
    }
}

fn repair_key(relationship: &SyncRelationship) -> Key {
    ByteView::from(relationship.id.to_bytes().to_vec())
}

fn not_s3_arn() -> SyncRelationshipError {
    ConversionError::FromStrError("sync relationship endpoint is not an S3 ARN".to_string()).into()
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use crate::sync::mirror_repair::mirror_delete_entry;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

    const TXN: TxnId = Ulid(21);

    fn subject() -> SyncRelationship {
        relationship(1, "source-a", "target-a")
    }

    fn start_effect() -> Effect {
        Effect::Storage(StorageEffect::StartTransaction { read: false })
    }

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN })
    }

    fn scan(start_after: Option<Key>) -> Effect {
        iter_effect(
            SyncRelationshipDirection::Outgoing,
            Some("source-a"),
            start_after,
            Some(TXN),
        )
    }

    fn page(values: Vec<(Key, Value)>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: next,
        })
    }

    fn claim_read() -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: repair_key(&subject()),
            txn_id: Some(TXN),
        })
    }

    /// The staged reconcile record, as `MirrorRecordOperation::stage` writes it.
    fn staged() -> Value {
        let delete = mirror_delete_entry(&subject()).unwrap().2;
        let mut record = SyncMirrorRecord::from_bytes(&repair_key(&subject()), &delete).unwrap();
        record.intent = SyncMirrorIntent::Reconcile;
        ByteView::from(postcard::to_allocvec(&record).unwrap())
    }

    fn read(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value,
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: Vec::<u8>::new().into(),
        })
    }

    fn commit() -> Effect {
        Effect::Storage(StorageEffect::CommitTransaction { txn_id: TXN })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    fn conflict() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    }

    fn scanning() -> CreateRelationshipOperation {
        let mut operation = CreateRelationshipOperation::new(subject());
        operation.start();
        operation.step(started());
        operation
    }

    #[test]
    fn creates_relationship() {
        let row = storage_key(SyncRelationshipDirection::Outgoing, &subject()).unwrap();
        let other = relationship(2, "source-a", "target-b");
        let other_row = (
            storage_key(SyncRelationshipDirection::Outgoing, &other).unwrap(),
            ByteView::from(other.to_bytes().unwrap()),
        );
        let mut operation = CreateRelationshipOperation::new(subject());
        check_trace(
            &mut operation,
            &[start_effect()],
            vec![
                (started(), vec![scan(None)]),
                (
                    page(vec![other_row.clone()], Some(other_row.0.clone())),
                    vec![scan(Some(other_row.0))],
                ),
                (page(Vec::new(), None), vec![claim_read()]),
                (
                    read(Some(staged())),
                    vec![Effect::Storage(StorageEffect::Write {
                        key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
                        key: repair_key(&subject()),
                        value: staged(),
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    written(),
                    vec![Effect::Storage(StorageEffect::Write {
                        key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                        key: row,
                        value: ByteView::from(subject().to_bytes().unwrap()),
                        txn_id: Some(TXN),
                    })],
                ),
                (written(), vec![commit()]),
                (
                    Event::Storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    vec![],
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(subject()));
    }

    #[test]
    fn activation_skips_scan() {
        let mut operation = CreateRelationshipOperation::activate(subject());
        assert_eq!(operation.start().as_slice(), &[start_effect()]);
        assert_eq!(operation.step(started()).as_slice(), &[claim_read()]);
    }

    #[test]
    fn refuses_duplicates() {
        let twin = relationship(2, "source-a", "target-a");
        let twin_row = (
            storage_key(SyncRelationshipDirection::Outgoing, &twin).unwrap(),
            ByteView::from(twin.to_bytes().unwrap()),
        );
        let mut duplicate = scanning();
        assert_eq!(
            duplicate.step(page(vec![twin_row], None)).as_slice(),
            &[abort()]
        );
        assert_eq!(duplicate.finalize(), Err(SyncRelationshipError::Duplicate));

        let mut stub = twin.clone();
        stub.state = SyncState::Detached;
        let stub_row = (
            storage_key(SyncRelationshipDirection::Outgoing, &stub).unwrap(),
            ByteView::from(stub.to_bytes().unwrap()),
        );
        let mut detached = scanning();
        assert_eq!(
            detached.step(page(vec![stub_row], None)).as_slice(),
            &[claim_read()]
        );

        let mut rolled_back = scanning();
        rolled_back.step(page(Vec::new(), None));
        assert_eq!(rolled_back.step(read(None)).as_slice(), &[abort()]);
        assert_eq!(
            rolled_back.finalize(),
            Err(SyncRelationshipError::RolledBack)
        );

        let deleting = mirror_delete_entry(&subject()).unwrap().2;
        let mut claimed = scanning();
        claimed.step(page(Vec::new(), None));
        assert_eq!(claimed.step(read(Some(deleting))).as_slice(), &[abort()]);
        assert_eq!(claimed.finalize(), Err(SyncRelationshipError::RolledBack));
    }

    #[test]
    fn conflicts_retry() {
        let mut operation = CreateRelationshipOperation::activate(subject());
        operation.start();
        assert_eq!(operation.step(conflict()).as_slice(), &[start_effect()]);
        operation.step(started());
        operation.step(read(Some(staged())));
        operation.step(written());
        operation.step(written());
        assert_eq!(operation.step(conflict()).as_slice(), &[start_effect()]);
        assert_eq!(operation.step(conflict()).as_slice(), &[] as &[Effect]);
        assert_eq!(
            operation.finalize(),
            Err(SyncRelationshipError::Storage(
                StorageError::TransactionConflict
            ))
        );
    }

    #[test]
    fn errors_abort_transaction() {
        let mut operation = scanning();
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(operation.step(failed).as_slice(), &[abort()]);
        assert_eq!(
            operation.finalize(),
            Err(SyncRelationshipError::Storage(StorageError::Timeout))
        );
        let mut corrupt = scanning();
        let bad = page(vec![(vec![1].into(), vec![0xff].into())], None);
        assert_eq!(corrupt.step(bad).as_slice(), &[abort()]);
        assert!(matches!(
            corrupt.finalize(),
            Err(SyncRelationshipError::Conversion(_))
        ));
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(scanning, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert_eq!(effects.as_slice(), &[abort()], "{label}");
            assert!(
                matches!(error, SyncRelationshipError::UnexpectedEvent { .. }),
                "{label}"
            );
        }
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(CreateRelationshipOperation::new(subject()));
        rejects_finalize(scanning());
        let mut fresh = CreateRelationshipOperation::new(subject());
        check_abort(&mut fresh, &[]);
        let mut operation = scanning();
        check_abort(&mut operation, &[abort()]);
        assert_eq!(
            operation.finalize(),
            Err(SyncRelationshipError::NotFinished)
        );
    }
}
