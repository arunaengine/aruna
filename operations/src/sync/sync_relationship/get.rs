//! Finds one sync relationship of a direction by its id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRelationship;
use ulid::Ulid;

use super::{SyncRelationshipDirection, SyncRelationshipError, page_call, page_rows, parse_values};

/// The relationship `id` of `direction`, searched page by page; `NotFound` when absent.
#[derive(Debug, PartialEq)]
pub struct GetRelationshipOperation {
    id: Ulid,
    direction: SyncRelationshipDirection,
}

impl GetRelationshipOperation {
    pub fn new(id: Ulid, direction: SyncRelationshipDirection) -> Self {
        Self { id, direction }
    }
}

impl Operation for GetRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        Ok(Next::Call(page_call(self.direction, None, None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        let (rows, next) = page_rows(result)?;
        let found = parse_values(self.direction, rows)?
            .into_iter()
            .find(|relationship| relationship.id == self.id);
        match (found, next) {
            (Some(relationship), _) => Ok(Next::Done(relationship)),
            (None, Some(after)) => Ok(Next::Call(page_call(self.direction, None, Some(after)))),
            (None, None) => Err(SyncRelationshipError::NotFound),
        }
    }
}
