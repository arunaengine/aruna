//! Removes an outgoing relationship for a delete flow, keeping a stub when it serves references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_core::types::Effects;

use super::{
    DeleteRelationshipOperation, StoreRelationshipOperation, SyncRelationshipDirection,
    SyncRelationshipError,
};

#[derive(Debug, PartialEq)]
enum Removal {
    Detach(StoreRelationshipOperation),
    Delete(DeleteRelationshipOperation),
}

/// Relationships serving references are detached, not deleted: reads are authorized through
/// the outgoing record, so a stub must survive for retained data to stay readable.
#[derive(Debug, PartialEq)]
pub struct RemoveOutgoingOperation {
    removal: Removal,
}

impl RemoveOutgoingOperation {
    pub fn new(relationship: SyncRelationship) -> Self {
        let removal = if relationship.serves_references() {
            let stub = SyncRelationship {
                state: SyncState::Detached,
                ..relationship
            };
            Removal::Detach(StoreRelationshipOperation::new(
                stub,
                SyncRelationshipDirection::Outgoing,
            ))
        } else {
            Removal::Delete(DeleteRelationshipOperation::new(
                relationship,
                SyncRelationshipDirection::Outgoing,
            ))
        };
        Self { removal }
    }
}

impl EffectOperation for RemoveOutgoingOperation {
    type Output = ();
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        match &mut self.removal {
            Removal::Detach(operation) => operation.start(),
            Removal::Delete(operation) => operation.start(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match &mut self.removal {
            Removal::Detach(operation) => operation.step(event),
            Removal::Delete(operation) => operation.step(event),
        }
    }

    fn is_complete(&self) -> bool {
        match &self.removal {
            Removal::Detach(operation) => operation.is_complete(),
            Removal::Delete(operation) => operation.is_complete(),
        }
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.removal {
            Removal::Detach(operation) => operation.finalize().map(|_| ()),
            Removal::Delete(operation) => operation.finalize(),
        }
    }

    fn abort(&mut self) -> Effects {
        match &mut self.removal {
            Removal::Detach(operation) => operation.abort(),
            Removal::Delete(operation) => operation.abort(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage_key;
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::RELATIONSHIP_OUT_KEYSPACE;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn plain() -> SyncRelationship {
        relationship(1, "source-a", "target-a")
    }

    fn serving() -> SyncRelationship {
        SyncRelationship {
            reference_serving: true,
            ..plain()
        }
    }

    fn key() -> aruna_core::types::Key {
        storage_key(SyncRelationshipDirection::Outgoing, &plain()).unwrap()
    }

    fn started(relationship: SyncRelationship) -> RemoveOutgoingOperation {
        let mut operation = RemoveOutgoingOperation::new(relationship);
        operation.start();
        operation
    }

    #[test]
    fn deletes_plain_relationship() {
        let mut operation = RemoveOutgoingOperation::new(plain());
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::Delete {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                key: key(),
                txn_id: None,
            })],
            vec![(
                Event::Storage(StorageEvent::DeleteResult { key: key() }),
                vec![],
            )],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn detaches_serving_relationship() {
        let stub = SyncRelationship {
            state: SyncState::Detached,
            ..serving()
        };
        let mut operation = RemoveOutgoingOperation::new(serving());
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::Write {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                key: key(),
                value: stub.to_bytes().unwrap().into(),
                txn_id: None,
            })],
            vec![(
                Event::Storage(StorageEvent::WriteResult { key: key() }),
                vec![],
            )],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = started(plain());
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            operation.finalize(),
            Err(SyncRelationshipError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn rejects_foreign_events() {
        for relationship in [plain(), serving()] {
            let make = || started(relationship.clone());
            let refusals = rejects_events(make, sample_events, |event| {
                matches!(
                    event,
                    Event::Storage(
                        StorageEvent::DeleteResult { .. }
                            | StorageEvent::WriteResult { .. }
                            | StorageEvent::Error { .. }
                    )
                )
            });
            assert!(!refusals.is_empty());
            for (label, effects, error) in refusals {
                assert!(effects.is_empty(), "{label}");
                assert!(
                    matches!(error, SyncRelationshipError::UnexpectedEvent { .. }),
                    "{label}"
                );
            }
        }
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(started(plain()));
        rejects_finalize(started(serving()));
        let mut operation = started(plain());
        check_abort(&mut operation, &[]);
        assert_eq!(
            operation.finalize(),
            Err(SyncRelationshipError::NotFinished)
        );
    }
}
