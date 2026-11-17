//! Reads the execution reservations this node holds, page by page in key order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::keyspaces::JOB_RESERVATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::types::Key;

use super::LifecycleError;
use super::reservation::MAX_RESERVATION_SCAN;

/// Reads every held reservation, or only the one of `job_id`, oldest key first.
#[derive(Debug, PartialEq)]
pub struct HeldReservations {
    job_id: Option<JobId>,
    held: Vec<JobReservationRecord>,
}

impl HeldReservations {
    /// Every reservation this node holds.
    pub fn all() -> Self {
        Self::new(None)
    }

    /// The reservation of one job's accepted execution, so the local attempt
    /// can bind the exact ExecutionId the receipt named.
    pub fn of_job(job_id: JobId) -> Self {
        Self::new(Some(job_id))
    }

    fn new(job_id: Option<JobId>) -> Self {
        Self {
            job_id,
            held: Vec::new(),
        }
    }
}

/// The page of reservation rows after `after`.
fn page<T>(after: Option<Key>) -> Next<T> {
    Next::Call(IoCall::Page {
        space: JOB_RESERVATION_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after,
        limit: MAX_RESERVATION_SCAN,
    })
}

impl Operation for HeldReservations {
    type Output = Vec<JobReservationRecord>;
    type Error = LifecycleError;

    fn first(&mut self) -> Result<Next<Self::Output>, LifecycleError> {
        Ok(page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, LifecycleError> {
        let IoResult::Page { rows, next } = result else {
            return Err(IoError::Unexpected {
                expected: "reservation page",
                got: "another result",
            }
            .into());
        };
        for (_, value) in rows {
            let row = from_bytes::<JobReservationRecord>(&value)?;
            if self.job_id.is_none_or(|job_id| row.job_id == job_id) {
                self.held.push(row);
            }
        }
        Ok(match next {
            Some(after) => page(Some(after)),
            None => Next::Done(std::mem::take(&mut self.held)),
        })
    }
}

#[cfg(test)]
#[path = "held_tests.rs"]
mod state_machine_tests;
