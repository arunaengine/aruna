//! Deletes one sync relationship record from the keyspace of its direction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRelationship;

use super::{SyncRelationshipDirection, SyncRelationshipError, delete_call, finished};

/// Deletes the record of a valid `relationship`.
#[derive(Debug, PartialEq)]
pub struct DeleteRelationshipOperation {
    relationship: SyncRelationship,
    direction: SyncRelationshipDirection,
}

impl DeleteRelationshipOperation {
    pub fn new(relationship: SyncRelationship, direction: SyncRelationshipDirection) -> Self {
        Self {
            relationship,
            direction,
        }
    }
}

impl Operation for DeleteRelationshipOperation {
    type Output = ();
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<()>, SyncRelationshipError> {
        Ok(Next::Call(delete_call(self.direction, &self.relationship)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncRelationshipError> {
        finished(result)?;
        Ok(Next::Done(()))
    }
}
