//! Pages registry records by update time through the timestamp index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, UPDATED_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{metadata_document_key, parse_updated_key, updated_index_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Storage rows read per index page.
const INDEX_SCAN_BATCH: usize = 256;

/// One page of registry records ordered by `updated_at_ms`, plus stale index
/// keys the caller may sweep.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdatedRecordsPage {
    pub records: Vec<MetadataRegistryRecord>,
    /// Exclusive cursor to resume after; `None` when the window is exhausted.
    pub next_after: Option<Key>,
    /// Index keys whose record moved to a newer datestamp or was deleted. Safe to
    /// delete in a sweep; skipping them keeps this read path read-only.
    pub stale_keys: Vec<Key>,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdatedPageError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the updated records page did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum PageState {
    Init,
    Scan,
    Records,
    Done,
}

impl PageState {
    fn name(&self) -> &'static str {
        match self {
            Self::Init => "starting",
            Self::Scan => "reading an index page",
            Self::Records => "reading registry records",
            Self::Done => "finished",
        }
    }
}

/// Enumerates registry records whose `updated_at_ms` is in `[from_ms, until_ms]`,
/// ascending, up to `limit` records. Keys are validated against the current record;
/// stale keys are skipped, never under-listing.
#[derive(Debug, PartialEq)]
pub struct UpdatedPageOperation {
    start: IterStart,
    until_ms: u64,
    limit: usize,
    records: Vec<MetadataRegistryRecord>,
    stale_keys: Vec<Key>,
    /// The candidate keys of the current page with their datestamp, in scan order.
    candidates: Vec<(Key, u64)>,
    /// Where the scan continues after this page; `None` ends the window.
    after_page: Option<IterStart>,
    state: PageState,
    output: Option<Result<UpdatedRecordsPage, UpdatedPageError>>,
}

impl UpdatedPageOperation {
    pub fn new(from_ms: u64, until_ms: u64, after: Option<Key>, limit: usize) -> Self {
        let start = match after {
            Some(cursor) => IterStart::After(cursor),
            None => IterStart::At(updated_index_key(from_ms, Ulid::nil())),
        };
        Self {
            start,
            until_ms,
            limit,
            records: Vec::new(),
            stale_keys: Vec::new(),
            candidates: Vec::new(),
            after_page: None,
            state: PageState::Init,
            output: None,
        }
    }

    fn scan(&mut self, start: IterStart) -> Effects {
        self.state = PageState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: UPDATED_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: Some(start),
            limit: INDEX_SCAN_BATCH,
            txn_id: None,
        })]
    }

    fn finish(&mut self, output: Result<UpdatedRecordsPage, UpdatedPageError>) -> Effects {
        self.state = PageState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn page(&mut self, next_after: Option<Key>) -> Effects {
        let page = UpdatedRecordsPage {
            records: std::mem::take(&mut self.records),
            next_after,
            stale_keys: std::mem::take(&mut self.stale_keys),
        };
        self.finish(Ok(page))
    }

    fn scanned_page(&mut self, entries: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        if entries.is_empty() {
            return self.page(None);
        }
        let mut documents = Vec::new();
        let mut past_window = false;
        for (key, _) in entries {
            let (updated_at_ms, document_id) = match parse_updated_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => return self.finish(Err(error.into())),
            };
            if updated_at_ms > self.until_ms {
                past_window = true;
                break;
            }
            self.candidates.push((key, updated_at_ms));
            documents.push(document_id);
        }
        self.after_page = match (past_window, next) {
            (false, Some(next)) => Some(IterStart::After(next)),
            _ => None,
        };
        if documents.is_empty() {
            return self.page(None);
        }
        let reads = documents
            .into_iter()
            .map(|document_id| {
                (
                    DOCUMENT_INDEX_KEYSPACE.to_string(),
                    metadata_document_key(document_id),
                )
            })
            .collect();
        self.state = PageState::Records;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn read_records(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let candidates = std::mem::take(&mut self.candidates);
        if values.len() != candidates.len() {
            return self.finish(Err(UpdatedPageError::UnexpectedEvent {
                state: self.state.name(),
                got: "StorageEvent::BatchReadResult with a different length",
            }));
        }
        for ((key, updated_at_ms), (_, value)) in candidates.into_iter().zip(values) {
            let record = match value
                .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
            {
                Some(Ok(record)) => Some(record),
                Some(Err(error)) => return self.finish(Err(ConversionError::from(error).into())),
                None => None,
            };
            match record {
                Some(record) if record.updated_at_ms == updated_at_ms => {
                    self.records.push(record);
                    if self.records.len() >= self.limit {
                        return self.page(Some(key));
                    }
                }
                _ => self.stale_keys.push(key),
            }
        }
        match self.after_page.take() {
            Some(next) => self.scan(next),
            None => self.page(None),
        }
    }
}

impl Operation for UpdatedPageOperation {
    type Output = UpdatedRecordsPage;
    type Error = UpdatedPageError;

    fn start(&mut self) -> Effects {
        let start = self.start.clone();
        self.scan(start)
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = matches!(self.state, PageState::Scan | PageState::Records);
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.state == PageState::Scan => self.scanned_page(values, next_start_after),
            Event::Storage(StorageEvent::BatchReadResult { values })
                if self.state == PageState::Records =>
            {
                self.read_records(values)
            }
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            event => {
                let error = UpdatedPageError::UnexpectedEvent {
                    state: self.state.name(),
                    got: event.label(),
                };
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == PageState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UpdatedPageError::Unfinished))
    }

    // Every read runs outside a transaction, so nothing is left to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
