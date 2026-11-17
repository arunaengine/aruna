//! Writes one sync relationship record in the keyspace of its direction.
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
enum StoreState {
    Init,
    Writing,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct StoreRelationshipOperation {
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
    state: StoreState,
    output: Option<Result<SyncRelationship, SyncRelationshipError>>,
}

impl StoreRelationshipOperation {
    pub fn new(relationship: SyncRelationship, direction: SyncRelationshipDirection) -> Self {
        Self {
            relationship,
            direction,
            state: StoreState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = StoreState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl EffectOperation for StoreRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        let value = match self.relationship.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        let key = match storage_key(self.direction, &self.relationship) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };

        self.state = StoreState::Writing;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: self.direction.keyspace().to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            StoreState::Writing => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.state = StoreState::Finish;
                    self.output = Some(Ok(self.relationship.clone()));
                    smallvec![]
                }
                received => self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "Writing",
                    expected: "storage write result",
                    received,
                }),
            },
            StoreState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            StoreState::Finish | StoreState::Error => {
                self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, StoreState::Finish | StoreState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
