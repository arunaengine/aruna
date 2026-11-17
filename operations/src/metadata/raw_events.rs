//! Reads a document's raw event log in budgeted pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::EVENT_LOG_KEYSPACE;
use aruna_core::metadata::{
    EVENT_LIMIT, MetadataError, MetadataEventRecord, MetadataRawRevision, RAW_BYTES_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{event_log_key, event_log_prefix, pending_projection_target};
use aruna_core::types::{Effects, Key, TxnId, Value};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

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

#[derive(Debug, PartialEq)]
enum EventsState {
    Init,
    Page,
    Done,
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
    state: EventsState,
    output: Option<Result<Vec<MetadataEventRecord>, RawReadError>>,
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
            state: EventsState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Vec<MetadataEventRecord>, RawReadError>) -> Effects {
        self.state = EventsState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn read_page(&mut self) -> Effects {
        let remaining = RAW_EVENT_LIMIT.saturating_sub(self.budget.events);
        if remaining == 0 {
            return self.finish(Err(budget_error("event count limit exceeded")));
        }
        self.page_limit = remaining.min(RAW_PAGE_SIZE);
        self.state = EventsState::Page;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: EVENT_LOG_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.document_id)),
            start: self.start.take().map(IterStart::After),
            limit: self.page_limit,
            txn_id: self.txn_id,
        })]
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

    fn page_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        if values.len() > self.page_limit {
            return self.finish(Err(budget_error("page size limit exceeded")));
        }
        if values.is_empty() && next.is_some() {
            return self.finish(Err(budget_error("empty page has continuation")));
        }
        let passed = match self.take_page(values) {
            Ok(passed) => passed,
            Err(error) => return self.finish(Err(error)),
        };
        match next {
            Some(next) if !passed => {
                self.start = Some(next);
                self.read_page()
            }
            _ => {
                let events = std::mem::take(&mut self.events);
                self.finish(Ok(events))
            }
        }
    }
}

impl Operation for RawEventsOperation {
    type Output = Vec<MetadataEventRecord>;
    type Error = RawReadError;

    fn start(&mut self) -> Effects {
        self.read_page()
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = self.state == EventsState::Page;
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if reading => self.page_read(values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            other => {
                let error = RawReadError::UnexpectedEvent(other.label().to_string());
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == EventsState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RawReadError::UnexpectedEvent(
            "the raw event read did not finish".to_string(),
        )))
    }

    // Pages are read without writing, and a caller's transaction stays the caller's.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::metadata::raw_revision::tests::test_event;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn event(seed: u64) -> MetadataEventRecord {
        test_event(Ulid::from_parts(1, seed), seed)
    }

    fn document() -> Ulid {
        event(1).record.document_id
    }

    fn key(event: &MetadataEventRecord) -> Key {
        event_log_key(event.record.document_id, event.event_id)
    }

    fn row(event: &MetadataEventRecord) -> (Key, Value) {
        (key(event), postcard::to_allocvec(event).unwrap().into())
    }

    fn page_effect(start: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: EVENT_LOG_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(document())),
            start: start.map(IterStart::After),
            limit: RAW_PAGE_SIZE,
            txn_id: None,
        })
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: next,
        })
    }

    fn whole_log() -> RawEventsOperation {
        RawEventsOperation::new(document(), None, None, None, RawLoadBudget::default())
    }

    fn reading() -> RawEventsOperation {
        let mut operation = whole_log();
        operation.start();
        operation
    }

    #[test]
    fn reads_to_end() {
        let first = event(10);
        let mut operation = whole_log();
        check_trace(
            &mut operation,
            &[page_effect(None)],
            vec![
                (
                    page(vec![row(&first)], Some(key(&first))),
                    vec![page_effect(Some(key(&first)))],
                ),
                (page(Vec::new(), None), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(vec![first]));
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
        check_trace(
            &mut operation,
            &[page_effect(Some(key(&start)))],
            vec![
                (
                    page(vec![row(&next)], Some(key(&next))),
                    vec![page_effect(Some(key(&next)))],
                ),
                (page(vec![row(&late)], Some(key(&late))), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(vec![next]));
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = reading();
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(operation.step(error).is_empty());
        assert_eq!(
            operation.finalize(),
            Err(RawReadError::Storage(StorageError::Timeout))
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
        for (event, kind) in cases {
            let mut operation = reading();
            assert!(operation.step(event).is_empty());
            let error = operation.finalize().unwrap_err();
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
        assert!(operation.start().is_empty());
        assert!(matches!(
            operation.finalize(),
            Err(RawReadError::LimitExceeded(_))
        ));
    }

    #[test]
    fn rejects_other_events() {
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in rejects_events(reading, sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(error, RawReadError::UnexpectedEvent(_)));
        }
        let mut idle = whole_log();
        idle.step(page(Vec::new(), None));
        assert!(matches!(
            idle.finalize(),
            Err(RawReadError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = whole_log();
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(whole_log());
        rejects_finalize(reading());
    }
}
