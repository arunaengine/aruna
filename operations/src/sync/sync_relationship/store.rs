//! Writes one sync relationship record in the keyspace of its direction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::structs::SyncRelationship;

use super::{SyncRelationshipDirection, SyncRelationshipError, write_call};

/// Writes `relationship` and answers it back.
#[derive(Debug, PartialEq)]
pub struct StoreRelationshipOperation {
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
}

impl StoreRelationshipOperation {
    pub fn new(relationship: SyncRelationship, direction: SyncRelationshipDirection) -> Self {
        Self {
            relationship,
            direction,
        }
    }
}

impl Operation for StoreRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        Ok(Next::Call(write_call(self.direction, &self.relationship)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        done(result)?;
        Ok(Next::Done(self.relationship.clone()))
    }
}
