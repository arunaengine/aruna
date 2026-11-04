//! Counts the jobs this node finished since a given time, per group, without reading their data.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::jobs::RunningJobView;
use aruna_core::keyspaces::JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use std::collections::HashMap;
use thiserror::Error;

use crate::jobs::store::decode_job_record;

const PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum RecentJobsError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("recent job count did not finish")]
    NotFinished,
    #[error("unexpected event: expected a job page, got {0}")]
    UnexpectedEvent(String),
}

/// Pages through the job records; callers authorize before they show the counts.
#[derive(Debug, PartialEq)]
pub struct RecentJobsOperation {
    since_ms: u64,
    counts: HashMap<GroupId, u64>,
    done: bool,
    output: Option<Result<HashMap<GroupId, u64>, RecentJobsError>>,
}

impl RecentJobsOperation {
    pub fn new(since_ms: u64) -> Self {
        Self {
            since_ms,
            counts: HashMap::new(),
            done: false,
            output: None,
        }
    }

    fn page(start: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: PAGE,
            txn_id: None,
        })]
    }

    fn finish(&mut self, result: Result<HashMap<GroupId, u64>, RecentJobsError>) -> Effects {
        self.done = true;
        self.output = Some(result);
        smallvec![]
    }
}

impl Operation for RecentJobsOperation {
    type Output = HashMap<GroupId, u64>;
    type Error = RecentJobsError;

    fn start(&mut self) -> Effects {
        Self::page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.done {
            return smallvec![];
        }
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return self.finish(Err(error.into())),
            other => {
                return self.finish(Err(RecentJobsError::UnexpectedEvent(format!("{other:?}"))));
            }
        };
        for (_, value) in values {
            let record = match decode_job_record(&value) {
                Ok(record) => record,
                Err(error) => return self.finish(Err(error.into())),
            };
            let recent = record
                .finished_at_ms
                .is_some_and(|finished| finished >= self.since_ms);
            let group = RunningJobView::new(&record, 0).group_id;
            if let (true, Some(group)) = (recent, group) {
                *self.counts.entry(group).or_default() += 1;
            }
        }
        match next {
            Some(start) => Self::page(Some(start)),
            None => {
                let counts = std::mem::take(&mut self.counts);
                self.finish(Ok(counts))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(RecentJobsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
