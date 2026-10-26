//! Lists the open multipart uploads of one key range of a distributed bucket by seeking in its
//! upload inventory in key order, then reads only the page's upload records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Nothing walks the node's uploads of other buckets or sorts uploads in memory.

use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{UPLOAD_INVENTORY_KEYSPACE, UPLOAD_KEYSPACE, prefix_upper_bound};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::inventory::{
    Entry, Position, bound_position, head_position, range_interval, upload_position,
};
use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::listing::common_prefix_of;
use crate::s3::multipart::uploads::ListUploadsResult;

/// Interval reads one page issues at most before it ends truncated.
const MAX_ROUNDS: usize = 64;
/// Bytes of keys one interval read returns at most.
const ROUND_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryUploadsInput {
    pub set_id: Ulid,
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    /// The nil id starts at the marker key itself, where a merged page reached a range bound.
    pub upload_id_marker: Option<Ulid>,
    pub max_uploads: usize,
    /// The key range `[lower, upper)` the listing stays inside.
    pub lower: String,
    pub upper: Option<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum InventoryUploadsError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the upload listing received an unexpected event: {0:?}")]
    Unexpected(Box<Event>),
    #[error("the upload listing did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Init,
    Scan,
    Records,
    Done,
}

#[derive(Debug, PartialEq)]
pub struct InventoryUploadsOperation {
    input: InventoryUploadsInput,
    state: State,
    start: Vec<u8>,
    /// The start is the first row after `start` rather than `start` itself.
    after: bool,
    end: Vec<u8>,
    candidates: Vec<(String, Ulid)>,
    prefixes: Vec<String>,
    last: Option<(String, Option<Ulid>)>,
    rounds: usize,
    truncated: bool,
    output: Option<Result<ListUploadsResult, InventoryUploadsError>>,
}

impl InventoryUploadsOperation {
    pub fn new(input: InventoryUploadsInput) -> Self {
        Self {
            input,
            state: State::Init,
            start: Vec::new(),
            after: false,
            end: Vec::new(),
            candidates: Vec::new(),
            prefixes: Vec::new(),
            last: None,
            rounds: 0,
            truncated: false,
            output: None,
        }
    }

    fn prefix(&self) -> &str {
        self.input.prefix.as_deref().unwrap_or_default()
    }

    fn emitted(&self) -> usize {
        self.candidates.len() + self.prefixes.len()
    }

    /// The interval the page may read: the range, narrowed to the prefix.
    fn bounds(&mut self) {
        let set_id = self.input.set_id;
        let (lower, end) = range_interval(set_id, &self.input.lower, self.input.upper.as_deref());
        let prefix = bound_position(set_id, self.prefix());
        self.start = lower.max(prefix.clone());
        self.end = match self.prefix().is_empty() {
            true => end,
            false => prefix_upper_bound(&prefix).map_or(end.clone(), |upper| upper.min(end)),
        };
    }

    /// Moves the start past `position` when it lies ahead of the current one.
    fn resume(&mut self, position: Vec<u8>, after: bool) {
        if position >= self.start {
            self.start = position;
            self.after = after;
        }
    }

    /// The first row after every upload of `key`.
    fn past_key(&self, key: &str) -> Vec<u8> {
        let mut rows = head_position(self.input.set_id, key);
        rows.pop();
        prefix_upper_bound(&rows).unwrap_or(rows)
    }

    /// The first row after every key in the common prefix `group`.
    fn past_group(&self, group: &str) -> Vec<u8> {
        let rows = bound_position(self.input.set_id, group);
        prefix_upper_bound(&rows).unwrap_or(rows)
    }

    fn begin(&mut self) -> Effects {
        if self.input.max_uploads == 0 {
            return self.finish(Vec::new());
        }
        self.bounds();
        let set_id = self.input.set_id;
        let marker = self.input.key_marker.clone().filter(|key| !key.is_empty());
        if let Some(key) = marker {
            let delimited = (self.input.delimiter.as_deref())
                .is_some_and(|delimiter| !delimiter.is_empty() && key.ends_with(delimiter));
            let (position, after) = match self.input.upload_id_marker {
                Some(id) if id.is_nil() => (bound_position(set_id, &key), false),
                Some(id) => (upload_position(set_id, &key, id), true),
                None if delimited => (self.past_group(&key), false),
                None => (self.past_key(&key), false),
            };
            self.resume(position, after);
        }
        self.scan()
    }

    fn scan(&mut self) -> Effects {
        if self.start >= self.end || self.rounds >= MAX_ROUNDS {
            self.truncated = self.start < self.end && self.last.is_some();
            return self.hydrate();
        }
        self.rounds += 1;
        self.state = State::Scan;
        let remaining = self.input.max_uploads.saturating_sub(self.emitted());
        let start = match self.after {
            true => IterStart::After(self.start.clone().into()),
            false => IterStart::At(self.start.clone().into()),
        };
        smallvec![Effect::Storage(StorageEffect::IterRange {
            key_space: UPLOAD_INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(self.end.clone().into()),
            limit: remaining.saturating_add(1),
            max_bytes: ROUND_BYTES,
            txn_id: None,
        })]
    }

    fn scanned(
        &mut self,
        values: Vec<(Key, Value)>,
        more: bool,
    ) -> Result<Effects, InventoryUploadsError> {
        for (row, _) in values {
            let position = Position::decode(&row)?;
            let Entry::Upload(upload_id) = position.entry else {
                self.resume(row.to_vec(), true);
                continue;
            };
            if self.emitted() >= self.input.max_uploads {
                self.truncated = true;
                return Ok(self.hydrate());
            }
            self.resume(row.to_vec(), true);
            let group = common_prefix_of(
                &position.key,
                self.input.prefix.as_deref(),
                self.input.delimiter.as_deref(),
            );
            let Some(group) = group else {
                self.last = Some((position.key.clone(), Some(upload_id)));
                self.candidates.push((position.key, upload_id));
                continue;
            };
            if self.prefixes.last() != Some(&group) {
                self.prefixes.push(group.clone());
                self.last = Some((group.clone(), None));
            }
            // The whole group is one entry; the next read starts past it.
            let past = self.past_group(&group);
            self.resume(past, false);
            return Ok(self.scan());
        }
        match more {
            true => Ok(self.scan()),
            false => Ok(self.hydrate()),
        }
    }

    fn hydrate(&mut self) -> Effects {
        if self.candidates.is_empty() {
            return self.finish(Vec::new());
        }
        let reads = (self.candidates.iter())
            .map(|(_, id)| (UPLOAD_KEYSPACE.to_string(), id.to_bytes().to_vec().into()))
            .collect();
        self.state = State::Records;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn records(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, InventoryUploadsError> {
        let mut uploads = Vec::with_capacity(values.len());
        for (_, value) in values {
            // A row whose record is gone or no longer open is being completed or aborted.
            let Some(value) = value else { continue };
            let upload = MultipartUpload::from_bytes(&value)?;
            if upload.bucket == self.input.bucket && upload.status == MultipartUploadStatus::Open {
                uploads.push(upload);
            }
        }
        Ok(self.finish(uploads))
    }

    fn finish(&mut self, uploads: Vec<MultipartUpload>) -> Effects {
        let (next_key_marker, next_upload_marker) = match (self.truncated, self.last.take()) {
            (true, Some((key, upload_id))) => (Some(key), upload_id),
            _ => (None, None),
        };
        self.state = State::Done;
        self.output = Some(Ok(ListUploadsResult {
            uploads,
            common_prefixes: std::mem::take(&mut self.prefixes),
            is_truncated: next_key_marker.is_some(),
            next_key_marker,
            next_upload_marker,
        }));
        smallvec![]
    }

    fn handle(&mut self, event: Event) -> Result<Effects, InventoryUploadsError> {
        match (self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (
                State::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.scanned(values, next_start_after.is_some()),
            (State::Records, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.records(values)
            }
            (_, event) => Err(InventoryUploadsError::Unexpected(Box::new(event))),
        }
    }
}

impl Operation for InventoryUploadsOperation {
    type Output = ListUploadsResult;
    type Error = InventoryUploadsError;

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Done {
            return smallvec![];
        }
        match self.handle(event) {
            Ok(effects) => effects,
            Err(error) => {
                self.state = State::Done;
                self.output = Some(Err(error));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<ListUploadsResult, InventoryUploadsError> {
        self.output
            .unwrap_or(Err(InventoryUploadsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "uploads_tests.rs"]
mod tests;
