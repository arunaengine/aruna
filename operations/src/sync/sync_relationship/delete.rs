//! Deletes one sync relationship record from the keyspace of its direction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::SyncRelationship;
use aruna_core::types::Effects;
use smallvec::smallvec;

use super::{SyncRelationshipDirection, SyncRelationshipError, storage_key};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeleteState {
    Init,
    Deleting,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct DeleteRelationshipOperation {
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
    state: DeleteState,
    output: Option<Result<(), SyncRelationshipError>>,
}

impl DeleteRelationshipOperation {
    pub fn new(relationship: SyncRelationship, direction: SyncRelationshipDirection) -> Self {
        Self {
            relationship,
            direction,
            state: DeleteState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = DeleteState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl EffectOperation for DeleteRelationshipOperation {
    type Output = ();
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.relationship.validate() {
            return self.fail(error.into());
        }
        let key = match storage_key(self.direction, &self.relationship) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };

        self.state = DeleteState::Deleting;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: self.direction.keyspace().to_string(),
            key,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            DeleteState::Deleting => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.state = DeleteState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                received => self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "Deleting",
                    expected: "storage delete result",
                    received,
                }),
            },
            DeleteState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            DeleteState::Finish | DeleteState::Error => {
                self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DeleteState::Finish | DeleteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
