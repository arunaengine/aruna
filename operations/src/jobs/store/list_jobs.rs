//! Lists one user's jobs page by page, newest first, behind an opaque cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use std::collections::VecDeque;

#[derive(Debug, Error, PartialEq)]
pub enum ListJobsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// A page of a user's jobs and the cursor of the next page.
pub type JobsPage = (Vec<JobRecord>, Option<Vec<u8>>);

/// Owner-scoped listing, newest first, with an opaque cursor and record filter.
/// Scans for `limit` records plus one live lookahead. A row without a record reads its job.
pub struct ListJobsOperation<F> {
    prefix: Key,
    after: Option<Key>,
    limit: usize,
    filter: F,
    records: Vec<JobRecord>,
    page_cursor: Option<Vec<u8>>,
    /// Index rows of the current page still to judge, and how many the page held.
    rows: VecDeque<(Key, Value)>,
    scanned: usize,
    /// The row whose job record is being read.
    reading: Option<(u64, JobId)>,
}

impl<F> std::fmt::Debug for ListJobsOperation<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListJobsOperation")
            .field("prefix", &self.prefix)
            .field("after", &self.after)
            .field("limit", &self.limit)
            .field("records", &self.records.len())
            .finish_non_exhaustive()
    }
}

/// Two listings are equal when they stand at the same place; the filter is not compared.
impl<F> PartialEq for ListJobsOperation<F> {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix
            && self.after == other.after
            && self.limit == other.limit
            && self.records == other.records
            && self.page_cursor == other.page_cursor
            && self.rows == other.rows
            && self.scanned == other.scanned
            && self.reading == other.reading
    }
}

impl<F: Fn(&JobRecord) -> bool> ListJobsOperation<F> {
    pub fn new(user_id: UserId, cursor: Option<Vec<u8>>, limit: usize, filter: F) -> Self {
        let after = cursor.map(|cursor| {
            let mut key = user_id.to_storage_key();
            key.extend_from_slice(&cursor);
            ByteView::from(key)
        });
        Self {
            prefix: owner_index_prefix(user_id),
            after,
            limit,
            filter,
            records: Vec::new(),
            page_cursor: None,
            rows: VecDeque::new(),
            scanned: 0,
            reading: None,
        }
    }

    fn page(&self) -> Next<JobsPage> {
        Next::Call(IoCall::Page {
            space: JOB_INDEX_KEYSPACE.to_string(),
            prefix: self.prefix.clone(),
            after: self.after.clone(),
            limit: self.limit,
        })
    }

    /// Keeps `record` when it is visible and passes the filter; a full page ends the listing.
    fn keep(&mut self, record: Option<JobRecord>, (created_at_ms, job_id): (u64, JobId)) -> bool {
        let Some(record) = record else {
            return false;
        };
        if record.payload.is_internal() || !(self.filter)(&record) {
            return false;
        }
        if self.records.len() == self.limit {
            return true;
        }
        self.records.push(record);
        if self.records.len() == self.limit {
            self.page_cursor = Some(job_owner_cursor(created_at_ms, job_id));
        }
        false
    }

    fn done(&mut self, cursor: Option<Vec<u8>>) -> Next<JobsPage> {
        Next::Done((std::mem::take(&mut self.records), cursor))
    }

    fn judge(&mut self) -> Result<Next<JobsPage>, ListJobsError> {
        while let Some((key, value)) = self.rows.pop_front() {
            let (_, created_at_ms, job_id) = parse_owner_key(key.as_ref())?;
            self.after = Some(key);
            if value.is_empty() {
                self.reading = Some((created_at_ms, job_id));
                let row = (JOB_KEYSPACE.to_string(), job_record_key(job_id));
                return Ok(Next::Call(IoCall::Read(vec![row])));
            }
            let record = JobRecord::from_bytes(value.as_ref()).ok();
            if self.keep(record, (created_at_ms, job_id)) {
                let cursor = self.page_cursor.take();
                return Ok(self.done(cursor));
            }
        }
        match self.scanned < self.limit {
            true => Ok(self.done(None)),
            false => Ok(self.page()),
        }
    }
}

impl<F: Fn(&JobRecord) -> bool + Send> Operation for ListJobsOperation<F> {
    type Output = JobsPage;
    type Error = ListJobsError;

    fn first(&mut self) -> Result<Next<JobsPage>, ListJobsError> {
        match self.limit {
            0 => Ok(self.done(None)),
            _ => Ok(self.page()),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobsPage>, ListJobsError> {
        if let Some(row) = self.reading.take() {
            let [value] = aruna_core::rows::values::<1>(result)?;
            let record = value.and_then(|value| match JobRecord::from_bytes(&value) {
                Ok(record) => Some(record),
                Err(error) => {
                    warn!(job_id = %row.1, error = %error, "Malformed job record retained");
                    None
                }
            });
            if self.keep(record, row) {
                let cursor = self.page_cursor.take();
                return Ok(self.done(cursor));
            }
            return self.judge();
        }
        let IoResult::Page { rows, .. } = result else {
            return Err(IoError::Unexpected {
                expected: "IoResult::Page",
                got: result.label(),
            }
            .into());
        };
        if rows.is_empty() {
            return Ok(self.done(None));
        }
        self.scanned = rows.len();
        self.rows = rows.into();
        self.judge()
    }
}
