//! Counts the jobs this node finished since a given time, per group, without reading their data.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::jobs::RunningJobView;
use aruna_core::keyspaces::JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::execution::job::JobRecord;
use aruna_core::types::GroupId;
use std::collections::HashMap;
use thiserror::Error;

const PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum RecentJobsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// Pages through the job records; callers authorize before they show the counts.
#[derive(Debug, PartialEq)]
pub struct RecentJobsOperation {
    since_ms: u64,
    pages: PrefixPages,
    counts: HashMap<GroupId, u64>,
}

impl RecentJobsOperation {
    pub fn new(since_ms: u64) -> Self {
        Self {
            since_ms,
            pages: PrefixPages::new(JOB_KEYSPACE, Vec::<u8>::new(), PAGE),
            counts: HashMap::new(),
        }
    }
}

impl Operation for RecentJobsOperation {
    type Output = HashMap<GroupId, u64>;
    type Error = RecentJobsError;

    fn first(&mut self) -> Result<Next<Self::Output>, RecentJobsError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, RecentJobsError> {
        for (_, value) in self.pages.take(result)? {
            let record = JobRecord::from_bytes(&value)?;
            let recent = record
                .finished_at_ms
                .is_some_and(|finished| finished >= self.since_ms);
            let group = RunningJobView::new(&record, 0).group_id;
            if let (true, Some(group)) = (recent, group) {
                *self.counts.entry(group).or_default() += 1;
            }
        }
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.counts)),
            false => Next::Call(self.pages.call()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::IoCall;
    use aruna_core::types::{Key, Value};

    fn page(after: Option<Key>) -> Next<HashMap<GroupId, u64>> {
        Next::Call(IoCall::Page {
            space: JOB_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: PAGE,
        })
    }

    #[test]
    fn reads_every_page() {
        let mut operation = RecentJobsOperation::new(5);
        assert_eq!(operation.first(), Ok(page(None)));
        let next = Some(Key::from(vec![1]));
        let first = IoResult::Page {
            rows: Vec::new(),
            next: next.clone(),
        };
        assert_eq!(operation.next(first), Ok(page(next)));
        let last = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(operation.next(last), Ok(Next::Done(HashMap::new())));
    }

    #[test]
    fn rejects_corrupt_row() {
        let mut operation = RecentJobsOperation::new(5);
        operation.first().expect("first page");
        let rows = vec![(Key::from(vec![1]), Value::from(&[0xff][..]))];
        let result = operation.next(IoResult::Page { rows, next: None });
        assert!(matches!(result, Err(RecentJobsError::Conversion(_))));
    }
}
