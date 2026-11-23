//! Lists every directory this node offers as a read-only bucket, page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::OFFERED_DIRECTORY_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::offered_directory::OfferedDirectory;
use aruna_core::types::Key;
use thiserror::Error;

/// Offers one scan page reads.
const OFFER_PAGE: usize = 512;

#[derive(Debug, Error, PartialEq)]
pub enum ListOffersError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

impl From<IoError> for ListOffersError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Outputs every offered directory in bucket order; an unreadable row fails the listing.
#[derive(Debug, Default, PartialEq)]
pub struct ListOffersOperation {
    offers: Vec<OfferedDirectory>,
}

impl ListOffersOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn page(after: Option<Key>) -> Next<Vec<OfferedDirectory>> {
        Next::Call(IoCall::Page {
            space: OFFERED_DIRECTORY_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: OFFER_PAGE,
        })
    }
}

impl Operation for ListOffersOperation {
    type Output = Vec<OfferedDirectory>;
    type Error = ListOffersError;

    fn first(&mut self) -> Result<Next<Self::Output>, ListOffersError> {
        Ok(Self::page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ListOffersError> {
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            self.offers.push(OfferedDirectory::from_bytes(&value)?);
        }
        Ok(match next {
            Some(next) => Self::page(Some(next)),
            None => Next::Done(std::mem::take(&mut self.offers)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(values: Vec<Vec<u8>>, next: Option<Key>) -> IoResult {
        let rows = values.into_iter();
        IoResult::Page {
            rows: rows
                .map(|value| (Vec::new().into(), value.into()))
                .collect(),
            next,
        }
    }

    // Pages are read until the cursor ends; an empty node offers nothing.
    #[test]
    fn lists_every_page() {
        let mut list = ListOffersOperation::new();
        let cursor: Key = b"a".to_vec().into();
        assert_eq!(list.first(), Ok(ListOffersOperation::page(None)));
        let next = ListOffersOperation::page(Some(cursor.clone()));
        assert_eq!(list.next(page(Vec::new(), Some(cursor))), Ok(next));
        assert_eq!(
            list.next(page(Vec::new(), None)),
            Ok(Next::Done(Vec::new()))
        );
    }

    // An unreadable row fails the listing, and so does a result that answers no page.
    #[test]
    fn failures_fail() {
        let mut list = ListOffersOperation::new();
        list.first().unwrap();
        assert!(matches!(
            list.next(page(vec![vec![0xFF]], None)),
            Err(ListOffersError::Conversion(_))
        ));
        let mut list = ListOffersOperation::new();
        list.first().unwrap();
        assert!(list.next(IoResult::Done).is_err());
    }
}
