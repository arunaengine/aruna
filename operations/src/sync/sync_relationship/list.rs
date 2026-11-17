//! Lists the sync relationships of one direction, optionally for one bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRelationship;
use aruna_core::types::Effects;
use smallvec::smallvec;

use super::{SyncRelationshipDirection, SyncRelationshipError, iter_effect, parse_values};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListState {
    Init,
    Listing,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListRelationshipsOperation {
    direction: SyncRelationshipDirection,
    bucket: Option<String>,
    state: ListState,
    relationships: Vec<SyncRelationship>,
    output: Option<Result<Vec<SyncRelationship>, SyncRelationshipError>>,
}

impl ListRelationshipsOperation {
    pub fn new(direction: SyncRelationshipDirection, bucket: Option<String>) -> Self {
        Self {
            direction,
            bucket,
            state: ListState::Init,
            relationships: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: SyncRelationshipError) -> Effects {
        self.state = ListState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ListRelationshipsOperation {
    type Output = Vec<SyncRelationship>;
    type Error = SyncRelationshipError;

    fn start(&mut self) -> Effects {
        self.state = ListState::Listing;
        smallvec![iter_effect(
            self.direction,
            self.bucket.as_deref(),
            None,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }

        match self.state {
            ListState::Listing => {
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
                self.relationships.extend(relationships);

                if let Some(start_after) = next_start_after {
                    return smallvec![iter_effect(
                        self.direction,
                        self.bucket.as_deref(),
                        Some(start_after),
                        None
                    )];
                }

                self.state = ListState::Finish;
                self.output = Some(Ok(std::mem::take(&mut self.relationships)));
                smallvec![]
            }
            ListState::Init => self.fail(SyncRelationshipError::UnexpectedEvent {
                state: "Init",
                expected: "operation start",
                received: event,
            }),
            ListState::Finish | ListState::Error => {
                self.fail(SyncRelationshipError::UnexpectedEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListState::Finish | ListState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SyncRelationshipError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
