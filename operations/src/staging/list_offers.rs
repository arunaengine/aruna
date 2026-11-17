//! Lists every directory this node offers as a read-only bucket, page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::OFFERED_DIRECTORY_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::offered_directory::OfferedDirectory;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

/// Offers one scan page reads.
const OFFER_PAGE: usize = 512;

#[derive(Debug, Error, PartialEq)]
pub enum ListOffersError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected {0} while listing offered directories")]
    UnexpectedEvent(&'static str),
    #[error("listing offered directories did not finish")]
    NotFinished,
}

/// Outputs every offered directory in bucket order; an unreadable row fails the listing.
#[derive(Debug, Default, PartialEq)]
pub struct ListOffersOperation {
    started: bool,
    offers: Vec<OfferedDirectory>,
    output: Option<Result<Vec<OfferedDirectory>, ListOffersError>>,
}

impl ListOffersOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn page(start_after: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: OFFERED_DIRECTORY_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: OFFER_PAGE,
            txn_id: None,
        })]
    }

    fn finish(&mut self, output: Result<Vec<OfferedDirectory>, ListOffersError>) -> Effects {
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for ListOffersOperation {
    type Output = Vec<OfferedDirectory>;
    type Error = ListOffersError;

    fn start(&mut self) -> Effects {
        self.started = true;
        Self::page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.started => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) if self.started => {
                return self.finish(Err(error.into()));
            }
            event => {
                let label = event.label();
                return self.finish(Err(ListOffersError::UnexpectedEvent(label)));
            }
        };
        for (_, value) in values {
            match OfferedDirectory::from_bytes(&value) {
                Ok(offer) => self.offers.push(offer),
                Err(error) => return self.finish(Err(error.into())),
            }
        }
        match next {
            Some(next) => Self::page(Some(next)),
            None => {
                let offers = std::mem::take(&mut self.offers);
                self.finish(Ok(offers))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ListOffersError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn page(values: Vec<Vec<u8>>, next: Option<Key>) -> Event {
        let values = values.into_iter();
        Event::Storage(StorageEvent::IterResult {
            values: values
                .map(|value| (Vec::new().into(), value.into()))
                .collect(),
            next_start_after: next,
        })
    }

    // Pages are read until the cursor ends; an empty node offers nothing.
    #[test]
    fn lists_every_page() {
        let mut list = ListOffersOperation::new();
        let cursor: Key = b"a".to_vec().into();
        let next = ListOffersOperation::page(Some(cursor.clone()));
        check_trace(
            &mut list,
            &ListOffersOperation::page(None),
            vec![
                (page(Vec::new(), Some(cursor)), next.into_vec()),
                (page(Vec::new(), None), Vec::new()),
            ],
        );
        assert_eq!(list.finalize(), Ok(Vec::new()));
    }

    // An unreadable row or a failed page fails the listing; other events are refused.
    #[test]
    fn failures_fail() {
        let mut list = ListOffersOperation::new();
        list.start();
        list.step(page(vec![vec![0xFF]], None));
        assert!(matches!(
            list.finalize(),
            Err(ListOffersError::Conversion(_))
        ));
        let mut list = ListOffersOperation::new();
        list.start();
        list.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(list.finalize(), Err(StorageError::Timeout.into()));

        let all = sample_events().len();
        let refused = rejects_events(ListOffersOperation::new, sample_events, |_| false);
        assert_eq!(refused.len(), all);
        rejects_finalize(ListOffersOperation::new());
        assert!(ListOffersOperation::new().abort().is_empty());
    }
}
