//! Lists the sync relationships of one direction, optionally for one bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::SyncRelationship;

use super::{SyncRelationshipDirection, SyncRelationshipError, pages, parse_values};

/// Every relationship of `direction`, of one bucket or of all, page by page.
#[derive(Debug, PartialEq)]
pub struct ListRelationshipsOperation {
    direction: SyncRelationshipDirection,
    pages: PrefixPages,
    relationships: Vec<SyncRelationship>,
}

impl ListRelationshipsOperation {
    pub fn new(direction: SyncRelationshipDirection, bucket: Option<String>) -> Self {
        Self {
            direction,
            pages: pages(direction, bucket.as_deref()),
            relationships: Vec::new(),
        }
    }
}

impl Operation for ListRelationshipsOperation {
    type Output = Vec<SyncRelationship>;
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<Self::Output>, SyncRelationshipError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SyncRelationshipError> {
        let rows = self.pages.take(result)?;
        self.relationships
            .extend(parse_values(self.direction, rows)?);
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.relationships)),
            false => Next::Call(self.pages.call()),
        })
    }
}
