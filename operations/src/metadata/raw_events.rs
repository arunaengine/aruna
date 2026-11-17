//! Reads a document's raw event log in budgeted pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::EVENT_LOG_KEYSPACE;
use aruna_core::metadata::{
    EVENT_LIMIT, MetadataError, MetadataEventRecord, MetadataRawRevision, RAW_BYTES_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{event_log_key, event_log_prefix, pending_projection_target};
use aruna_core::types::{Key, TxnId, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::page_rows;

pub(crate) const RAW_PAGE_SIZE: usize = 1;
pub(crate) const RAW_EVENT_LIMIT: usize = EVENT_LIMIT as usize;
pub(crate) const RAW_ENCODED_LIMIT: usize = RAW_BYTES_LIMIT as usize;

#[derive(Debug, Error, PartialEq)]
pub enum RawReadError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("unexpected metadata raw-read event: {0}")]
    UnexpectedEvent(String),
    #[error("metadata raw-read limit exceeded: {0}")]
    LimitExceeded(String),
    /// The event log itself is inconsistent, so retrying the same read cannot
    /// help. Kept apart from [`Self::UnexpectedEvent`], which is adapter noise.
    #[error("inconsistent metadata raw event log: {0}")]
    InconsistentLog(String),
}

impl From<IoError> for RawReadError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::UnexpectedEvent(other.to_string()),
        }
    }
}

/// Bounds one raw replay by event count and encoded bytes.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct RawLoadBudget {
    events: usize,
    encoded_bytes: usize,
}

impl RawLoadBudget {
    pub(crate) fn inspect(&mut self, encoded_bytes: usize) -> Result<(), RawReadError> {
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| budget_error("encoded bytes overflow"))?;
        if self.encoded_bytes > RAW_ENCODED_LIMIT {
            return Err(budget_error("encoded byte limit exceeded"));
        }
        Ok(())
    }

    pub(crate) fn accept(&mut self) -> Result<(), RawReadError> {
        self.events = self
            .events
            .checked_add(1)
            .ok_or_else(|| budget_error("event count overflow"))?;
        if self.events > RAW_EVENT_LIMIT {
            return Err(budget_error("event count limit exceeded"));
        }
        Ok(())
    }
}

pub(crate) fn budget_error(message: &str) -> RawReadError {
    RawReadError::LimitExceeded(message.to_string())
}

pub(crate) fn validate_raw_entry(
    document_id: Ulid,
    key: &[u8],
    event: &MetadataEventRecord,
) -> Result<(), RawReadError> {
    let Some((key_document_id, key_event_id)) = pending_projection_target(key) else {
        return Err(RawReadError::InconsistentLog(
            "raw event key has invalid identity".to_string(),
        ));
    };
    if key_document_id != document_id {
        return Err(RawReadError::InconsistentLog(format!(
            "raw event key belongs to document {key_document_id}"
        )));
    }
    if event.record.document_id != document_id {
        return Err(RawReadError::InconsistentLog(format!(
            "raw event belongs to document {}",
            event.record.document_id
        )));
    }
    if event.event_id != key_event_id {
        return Err(RawReadError::InconsistentLog(format!(
            "raw event key mismatch for {document_id}/{key_event_id}"
        )));
    }
    Ok(())
}

/// The base event a raw revision was folded from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct RawBaseIdentity {
    pub(crate) updated_at_ms: u64,
    pub(crate) event_id: Ulid,
}

/// The folded raw state stored per document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RawRevisionState {
    pub(crate) base: Option<RawBaseIdentity>,
    pub(crate) last_event_id: Ulid,
    pub(crate) revision: Option<MetadataRawRevision>,
    /// Set once a batch has merged into the graph. From then on the rendered
    /// graph owns this document's revision and no text event rebuilds it.
    pub(crate) merged: bool,
}

/// Reads a document's event log in pages after `start_after`, stopping before the first
/// event past `event_cursor`. Every event is validated and counted against `budget`.
#[derive(Debug, PartialEq)]
pub struct RawEventsOperation {
    document_id: Ulid,
    start: Option<Key>,
    event_cursor: Option<Ulid>,
    txn_id: Option<TxnId>,
    budget: RawLoadBudget,
    page_limit: usize,
    events: Vec<MetadataEventRecord>,
}

impl RawEventsOperation {
    pub(crate) fn new(
        document_id: Ulid,
        start_after: Option<Ulid>,
        event_cursor: Option<Ulid>,
        txn_id: Option<TxnId>,
        budget: RawLoadBudget,
    ) -> Self {
        Self {
            document_id,
            start: start_after.map(|event_id| event_log_key(document_id, event_id)),
            event_cursor,
            txn_id,
            budget,
            page_limit: 0,
            events: Vec::new(),
        }
    }

    fn read_page(&mut self) -> Result<Next<Vec<MetadataEventRecord>>, RawReadError> {
        let remaining = RAW_EVENT_LIMIT.saturating_sub(self.budget.events);
        if remaining == 0 {
            return Err(budget_error("event count limit exceeded"));
        }
        self.page_limit = remaining.min(RAW_PAGE_SIZE);
        let prefix = event_log_prefix(self.document_id);
        let after = self.start.take();
        Ok(Next::Call(match self.txn_id {
            // The caller's transaction is not the runner's, so the page goes out as an effect.
            Some(txn_id) => IoCall::Effect(Effect::Storage(StorageEffect::Iter {
                key_space: EVENT_LOG_KEYSPACE.to_string(),
                prefix: Some(prefix),
                start: after.map(IterStart::After),
                limit: self.page_limit,
                txn_id: Some(txn_id),
            })),
            None => IoCall::Page {
                space: EVENT_LOG_KEYSPACE.to_string(),
                prefix,
                after,
                limit: self.page_limit,
            },
        }))
    }

    /// Takes one page; `Ok(true)` once the cursor is passed and the read ends.
    fn take_page(&mut self, values: Vec<(Key, Value)>) -> Result<bool, RawReadError> {
        for (key, value) in values {
            self.budget.inspect(value.len())?;
            let event: MetadataEventRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            validate_raw_entry(self.document_id, &key, &event)?;
            if self
                .event_cursor
                .is_some_and(|cursor| event.event_id > cursor)
            {
                return Ok(true);
            }
            self.budget.accept()?;
            self.events.push(event);
        }
        Ok(false)
    }
}

/// The rows of an event page read alone or inside the caller's transaction.
fn page_answer(result: IoResult) -> Result<(Vec<(Key, Value)>, Option<Key>), RawReadError> {
    match result {
        IoResult::Event(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        })) => Ok((values, next_start_after)),
        IoResult::Event(Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
        other => Ok(page_rows(other)?),
    }
}

impl Operation for RawEventsOperation {
    type Output = Vec<MetadataEventRecord>;
    type Error = RawReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, RawReadError> {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, RawReadError> {
        let (values, next) = page_answer(result)?;
        if values.len() > self.page_limit {
            return Err(budget_error("page size limit exceeded"));
        }
        if values.is_empty() && next.is_some() {
            return Err(budget_error("empty page has continuation"));
        }
        let passed = self.take_page(values)?;
        match next {
            Some(next) if !passed => {
                self.start = Some(next);
                self.read_page()
            }
            _ => Ok(Next::Done(std::mem::take(&mut self.events))),
        }
    }
}

#[cfg(test)]
pub(crate) mod call_tests {
    use super::*;
    use crate::metadata::raw_revision::tests::test_event;

    pub(crate) fn event(seed: u64) -> MetadataEventRecord {
        test_event(Ulid::from_parts(1, seed), seed)
    }

    fn document() -> Ulid {
        event(1).record.document_id
    }

    fn key(event: &MetadataEventRecord) -> Key {
        event_log_key(event.record.document_id, event.event_id)
    }

    pub(crate) fn row(event: &MetadataEventRecord) -> (Key, Value) {
        (key(event), postcard::to_allocvec(event).unwrap().into())
    }

    pub(crate) fn page_call<T>(after: Option<Key>) -> Next<T> {
        Next::Call(IoCall::Page {
            space: EVENT_LOG_KEYSPACE.to_string(),
            prefix: event_log_prefix(document()),
            after,
            limit: RAW_PAGE_SIZE,
        })
    }

    pub(crate) fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn whole_log() -> RawEventsOperation {
        RawEventsOperation::new(document(), None, None, None, RawLoadBudget::default())
    }

    fn reading() -> RawEventsOperation {
        let mut operation = whole_log();
        operation.first().unwrap();
        operation
    }

    #[test]
    fn reads_to_end() {
        let first = event(10);
        let mut operation = whole_log();
        assert_eq!(operation.first(), Ok(page_call(None)));
        assert_eq!(
            operation.next(page(vec![row(&first)], Some(key(&first)))),
            Ok(page_call(Some(key(&first))))
        );
        assert_eq!(
            operation.next(page(Vec::new(), None)),
            Ok(Next::Done(vec![first]))
        );
    }

    #[test]
    fn cursor_stops_reading() {
        // The read starts after `start_after` and keeps no event past the cursor.
        let (start, next, late) = (event(10), event(11), event(12));
        let mut operation = RawEventsOperation::new(
            document(),
            Some(start.event_id),
            Some(next.event_id),
            None,
            RawLoadBudget::default(),
        );
        assert_eq!(operation.first(), Ok(page_call(Some(key(&start)))));
        assert_eq!(
            operation.next(page(vec![row(&next)], Some(key(&next)))),
            Ok(page_call(Some(key(&next))))
        );
        assert_eq!(
            operation.next(page(vec![row(&late)], Some(key(&late)))),
            Ok(Next::Done(vec![next]))
        );
    }

    #[test]
    fn pages_in_transaction() {
        let txn_id = Ulid::from_bytes([5; 16]);
        let first = event(10);
        let mut operation = RawEventsOperation::new(
            document(),
            None,
            None,
            Some(txn_id),
            RawLoadBudget::default(),
        );
        let iter = StorageEffect::Iter {
            key_space: EVENT_LOG_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(document())),
            start: None,
            limit: RAW_PAGE_SIZE,
            txn_id: Some(txn_id),
        };
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Effect::Storage(iter))))
        );
        let answer = StorageEvent::IterResult {
            values: vec![row(&first)],
            next_start_after: None,
        };
        assert_eq!(
            operation.next(IoResult::Event(Event::Storage(answer))),
            Ok(Next::Done(vec![first]))
        );
    }

    #[test]
    fn broken_pages_fail() {
        let first = event(10);
        let foreign_key = event_log_key(document(), Ulid::from_parts(2, 10));
        let bad_bytes = (key(&first), Value::from(vec![0xff]));
        let cases = [
            (page(vec![bad_bytes], None), "conversion"),
            (page(vec![(foreign_key, row(&first).1)], None), "log"),
            (page(vec![row(&first), row(&event(11))], None), "limit"),
            (page(Vec::new(), Some(key(&first))), "limit"),
        ];
        for (answer, kind) in cases {
            let error = reading().next(answer).unwrap_err();
            let matched = match kind {
                "conversion" => matches!(error, RawReadError::Conversion(_)),
                "log" => matches!(error, RawReadError::InconsistentLog(_)),
                _ => matches!(error, RawReadError::LimitExceeded(_)),
            };
            assert!(matched, "{kind}: {error:?}");
        }
    }

    #[test]
    fn spent_budget_refuses() {
        let spent = RawLoadBudget {
            events: RAW_EVENT_LIMIT,
            encoded_bytes: 0,
        };
        let mut operation = RawEventsOperation::new(document(), None, None, None, spent);
        assert!(matches!(
            operation.first(),
            Err(RawReadError::LimitExceeded(_))
        ));
    }

    #[test]
    fn wrong_result_fails() {
        assert!(matches!(
            reading().next(IoResult::Done),
            Err(RawReadError::UnexpectedEvent(_))
        ));
        assert_eq!(
            RawReadError::from(IoError::Storage(StorageError::Timeout)),
            RawReadError::Storage(StorageError::Timeout)
        );
    }
}
