//! Lists the open multipart uploads of one key range of a distributed bucket by seeking in its
//! upload inventory in key order, then reads only the page's upload records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Nothing walks the node's uploads of other buckets or sorts uploads in memory.

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{UPLOAD_INVENTORY_KEYSPACE, UPLOAD_KEYSPACE, prefix_upper_bound};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::inventory::{
    Entry, Position, bound_position, head_position, prefix_interval, upload_position,
};
use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
use aruna_core::types::{Key, Value};
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
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the upload listing received an unexpected event: {0}")]
    Unexpected(&'static str),
}

/// What the pending call reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Scan,
    Records,
}

type Step = Result<Next<ListUploadsResult>, InventoryUploadsError>;

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
}

impl InventoryUploadsOperation {
    pub fn new(input: InventoryUploadsInput) -> Self {
        Self {
            input,
            state: State::Scan,
            start: Vec::new(),
            after: false,
            end: Vec::new(),
            candidates: Vec::new(),
            prefixes: Vec::new(),
            last: None,
            rounds: 0,
            truncated: false,
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
        let (lower, upper) = (&self.input.lower, self.input.upper.as_deref());
        (self.start, self.end) = prefix_interval(self.input.set_id, lower, upper, self.prefix());
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

    fn begin(&mut self) -> Step {
        if self.input.max_uploads == 0 {
            return Ok(self.finish(Vec::new()));
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
        Ok(self.scan())
    }

    fn scan(&mut self) -> Next<ListUploadsResult> {
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
        let range = StorageEffect::IterRange {
            key_space: UPLOAD_INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(self.end.clone().into()),
            limit: remaining.saturating_add(1),
            max_bytes: ROUND_BYTES,
            txn_id: None,
        };
        Next::Call(IoCall::Effect(Box::new(Effect::Storage(range))))
    }

    fn scanned(&mut self, values: Vec<(Key, Value)>, more: bool) -> Step {
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

    fn hydrate(&mut self) -> Next<ListUploadsResult> {
        if self.candidates.is_empty() {
            return self.finish(Vec::new());
        }
        let reads = (self.candidates.iter())
            .map(|(_, id)| (UPLOAD_KEYSPACE.to_string(), id.to_bytes().to_vec().into()))
            .collect();
        self.state = State::Records;
        Next::Call(IoCall::Read(reads))
    }

    fn records(&mut self, values: Vec<Option<Value>>) -> Step {
        let mut uploads = Vec::with_capacity(values.len());
        for value in values {
            // A row whose record is gone or no longer open is being completed or aborted.
            let Some(value) = value else { continue };
            let upload = MultipartUpload::from_bytes(&value)?;
            if upload.bucket == self.input.bucket && upload.status == MultipartUploadStatus::Open {
                uploads.push(upload);
            }
        }
        Ok(self.finish(uploads))
    }

    fn finish(&mut self, uploads: Vec<MultipartUpload>) -> Next<ListUploadsResult> {
        let (next_key_marker, next_upload_marker) = match (self.truncated, self.last.take()) {
            (true, Some((key, upload_id))) => (Some(key), upload_id),
            _ => (None, None),
        };
        Next::Done(ListUploadsResult {
            uploads,
            common_prefixes: std::mem::take(&mut self.prefixes),
            is_truncated: next_key_marker.is_some(),
            next_key_marker,
            next_upload_marker,
        })
    }
}

impl Operation for InventoryUploadsOperation {
    type Output = ListUploadsResult;
    type Error = InventoryUploadsError;

    fn first(&mut self) -> Step {
        self.begin()
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            State::Scan => match result.event()? {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => self.scanned(values, next_start_after.is_some()),
                Event::Storage(StorageEvent::Error { error }) => {
                    Err(IoError::Storage(error).into())
                }
                other => Err(InventoryUploadsError::Unexpected(other.label())),
            },
            State::Records => self.records(result.values()?),
        }
    }
}

#[cfg(test)]
#[path = "uploads_tests.rs"]
mod tests;
