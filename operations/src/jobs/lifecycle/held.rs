//! Reads the execution reservations this node holds, page by page in key order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::JOB_RESERVATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;

use super::LifecycleError;
use super::reservation::MAX_RESERVATION_SCAN;
use crate::jobs::records::rows::from_bytes;
use aruna_core::compute::quota::JobReservationRecord;

/// Reads every held reservation, or only the one of `job_id`, oldest key first.
#[derive(Debug, PartialEq)]
pub struct HeldReservations {
    job_id: Option<JobId>,
    held: Vec<JobReservationRecord>,
    state: HeldState,
    outcome: Option<Result<Vec<JobReservationRecord>, LifecycleError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeldState {
    Init,
    Page,
    Done,
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
            state: HeldState::Init,
            outcome: None,
        }
    }

    fn page(&mut self, start_after: Option<Key>) -> Effects {
        self.state = HeldState::Page;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_RESERVATION_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: MAX_RESERVATION_SCAN,
            txn_id: None,
        })]
    }

    fn finish(&mut self, outcome: Result<Vec<JobReservationRecord>, LifecycleError>) -> Effects {
        self.outcome = Some(outcome);
        self.state = HeldState::Done;
        smallvec![]
    }
}

impl Operation for HeldReservations {
    type Output = Vec<JobReservationRecord>;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        self.page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                HeldState::Page,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                for (_, value) in values {
                    match from_bytes::<JobReservationRecord>(&value) {
                        Ok(row) if self.job_id.is_none_or(|job_id| row.job_id == job_id) => {
                            self.held.push(row);
                        }
                        Ok(_) => {}
                        Err(error) => return self.finish(Err(error.into())),
                    }
                }
                match next_start_after {
                    Some(next) => self.page(Some(next)),
                    None => {
                        let held = std::mem::take(&mut self.held);
                        self.finish(Ok(held))
                    }
                }
            }
            (HeldState::Page, Event::Storage(StorageEvent::Error { error })) => {
                self.finish(Err(error.into()))
            }
            (state, event) => self.finish(Err(LifecycleError::UnexpectedEvent {
                state: format!("{state:?}"),
                expected: "reservation page",
                got: event.label(),
            })),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == HeldState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "held_tests.rs"]
mod state_machine_tests;
