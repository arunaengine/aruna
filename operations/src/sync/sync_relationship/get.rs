//! Finds one sync relationship of a direction by its id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::SyncRelationship;
use aruna_core::types::Effects;
use smallvec::smallvec;
use ulid::Ulid;

use super::{SyncRelationshipDirection, SyncRelationshipError, iter_effect, parse_values};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetState {
    Init,
    Listing,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct GetRelationshipOperation {
    id: Ulid,
    direction: SyncRelationshipDirection,
    state: GetState,
    output: Option<Result<SyncRelationship, SyncRelationshipError>>,
}

impl GetRelationshipOperation {
    pub fn new(id: Ulid, direction: SyncRelationshipDirection) -> Self {
        Self {
            id,
            direction,
            state: GetState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = GetState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl EffectOperation for GetRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        self.state = GetState::Listing;
        smallvec![iter_effect(self.direction, None, None, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            GetState::Listing => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(SyncRelationshipError::UnexpectedEvent {
                        state: "Listing",
                        expected: "storage iteration result",
                        received: event,
                    });
                };

                let relationships = match parse_values(self.direction, values) {
                    Ok(relationships) => relationships,
                    Err(error) => return self.fail(error.into()),
                };
                if let Some(relationship) = relationships
                    .into_iter()
                    .find(|relationship| relationship.id == self.id)
                {
                    self.state = GetState::Finish;
                    self.output = Some(Ok(relationship));
                    return smallvec![];
                }

                if let Some(start_after) = next_start_after {
                    return smallvec![iter_effect(self.direction, None, Some(start_after), None)];
                }

                self.fail(SyncRelationshipError::NotFound)
            }
            GetState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            GetState::Finish | GetState::Error => {
                self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetState::Finish | GetState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
