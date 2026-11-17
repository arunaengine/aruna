//! Finds one sync relationship of a direction by its id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::SyncRelationship;
use ulid::Ulid;

use super::{SyncRelationshipDirection, SyncRelationshipError, pages, parse_values};

/// The relationship `id` of `direction`, searched page by page; `NotFound` when absent.
#[derive(Debug, PartialEq)]
pub struct GetRelationshipOperation {
    id: Ulid,
    direction: SyncRelationshipDirection,
    pages: PrefixPages,
}

impl GetRelationshipOperation {
    pub fn new(id: Ulid, direction: SyncRelationshipDirection) -> Self {
        Self {
            id,
            direction,
            pages: pages(direction, None),
        }
    }
}

impl Operation for GetRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        let rows = self.pages.take(result)?;
        let found = parse_values(self.direction, rows)?
            .into_iter()
            .find(|relationship| relationship.id == self.id);
        match found {
            Some(relationship) => Ok(Next::Done(relationship)),
            None if self.pages.done() => Err(SyncRelationshipError::NotFound),
            None => Ok(Next::Call(self.pages.call())),
        }
    }
}
