//! Discards this node's execution state at a start after it was offline longer than the job
//! retention, or when the time since its last contact is unknown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::keyspaces::{
    COMPUTE_DEPARTURE_KEYSPACE, DEADLINE_INDEX_KEYSPACE, PLAN_EXPLAIN_KEYSPACE,
    WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::node_state::{DISCARD_KEY, LAST_CONTACT_KEY};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobError, JobRecord, RETENTION_MS};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use super::lifecycle::LifecycleError;
use super::lifecycle::held::HeldReservations;
use super::lifecycle::reservation::ReleaseExecutionOperation;
use super::lifecycle::updates::PublishTerminalOperation;
use super::records::pages::ServingNode;
use super::store::job_calls::discard_execution;
use super::store::mutation_error::JobMutationError;
use super::store::row_calls::read_job_record;

/// The error a discarded execution ends with; the API shows it as not run on this node.
pub const DISCARD_REASON: &str = "state discarded after offline longer than retention";
/// Rows one delete of witness state removes.
const DELETE_PAGE: usize = 256;
/// The witness state of this node, which a discard drops whole.
const WITNESS_SPACES: [&str; 3] = [
    PLAN_EXPLAIN_KEYSPACE,
    WITNESS_DEADLINE_KEYSPACE,
    DEADLINE_INDEX_KEYSPACE,
];

/// What the last contact row says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastContact {
    Missing,
    Unreadable,
    At(u64),
}

/// Why a start discards its execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardReason {
    /// An earlier discard did not finish.
    Marker,
    /// Longer than the job retention since the last contact.
    Offline,
    /// The clock is earlier than the last contact.
    ClockBack,
    /// The last contact is unreadable, or missing while execution state exists.
    Unknown,
}

#[derive(Debug, Error, PartialEq)]
pub enum DiscardError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("a discard step failed: {0}")]
    Failed(String),
}

pub fn last_contact(value: Option<&[u8]>) -> LastContact {
    match value.map(<[u8; 8]>::try_from) {
        None => LastContact::Missing,
        Some(Ok(bytes)) => LastContact::At(u64::from_be_bytes(bytes)),
        Some(Err(_)) => LastContact::Unreadable,
    }
}

/// Decides at start whether this node discards its execution state. A forward clock jump can
/// discard early, which is the safe side: local executions end as errors and continue elsewhere.
pub fn discard_reason(
    marker: bool,
    contact: LastContact,
    has_state: bool,
    now_ms: u64,
) -> Option<DiscardReason> {
    if marker {
        return Some(DiscardReason::Marker);
    }
    match contact {
        LastContact::At(at_ms) if now_ms < at_ms => Some(DiscardReason::ClockBack),
        LastContact::At(at_ms) if now_ms - at_ms > RETENTION_MS => Some(DiscardReason::Offline),
        LastContact::At(_) => None,
        LastContact::Missing if has_state => Some(DiscardReason::Unknown),
        LastContact::Missing => None,
        LastContact::Unreadable => Some(DiscardReason::Unknown),
    }
}

fn node_row(key: &[u8]) -> (KeySpace, Key) {
    (COMPUTE_DEPARTURE_KEYSPACE.to_string(), Key::from(key))
}

#[derive(Debug, PartialEq)]
enum DiscardStep {
    Rows,
    Held,
    Marker,
    Job(Box<JobReservationRecord>),
    Ended,
    Published,
    Released,
    Witness(usize),
    Deleted(usize),
}

type DiscardNext = Result<Next<Option<DiscardReason>>, DiscardError>;

/// Runs before any job work of this start, while the start fence holds. Outbox rows, family
/// copies, tombstones and unfinished deletions stay; cleanup jobs then stop the backends.
#[derive(Debug, PartialEq)]
pub struct DiscardOperation {
    serving: ServingNode,
    now_ms: u64,
    marker: bool,
    contact: Option<Value>,
    reason: Option<DiscardReason>,
    held: VecDeque<JobReservationRecord>,
    step: DiscardStep,
}

impl DiscardOperation {
    pub fn new(serving: ServingNode) -> Self {
        Self {
            serving,
            now_ms: 0,
            marker: false,
            contact: None,
            reason: None,
            held: VecDeque::new(),
            step: DiscardStep::Rows,
        }
    }

    fn call(&mut self, step: DiscardStep, call: IoCall) -> DiscardNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn decide(&mut self, held: Vec<JobReservationRecord>) -> DiscardNext {
        let contact = last_contact(self.contact.as_deref());
        let reason = discard_reason(self.marker, contact, !held.is_empty(), self.now_ms);
        let Some(reason) = reason else {
            return Ok(Next::Done(None));
        };
        warn!(
            ?reason,
            reservations = held.len(),
            "Discarding execution state at start"
        );
        self.reason = Some(reason);
        self.held = held.into();
        let (space, key) = node_row(DISCARD_KEY);
        let marker = (space, key, Value::from(&[][..]));
        self.call(DiscardStep::Marker, IoCall::Write(vec![marker]))
    }

    fn next_job(&mut self) -> DiscardNext {
        let Some(reservation) = self.held.pop_front() else {
            return self.witness(0);
        };
        let read = read_job_record(reservation.job_id);
        self.call(DiscardStep::Job(Box::new(reservation)), IoCall::child(read))
    }

    /// An execution with its job ends as an error; without one nothing can resume the
    /// reservation, so it is released.
    fn job_read(&mut self, reservation: JobReservationRecord, result: IoResult) -> DiscardNext {
        let read = result.child::<Option<JobRecord>, JobMutationError>()?;
        match read.map_err(|error| DiscardError::Failed(error.to_string()))? {
            Some(_) => {
                let error = JobError::retryable(DISCARD_REASON);
                let end = discard_execution(reservation.job_id, self.now_ms, error);
                self.call(DiscardStep::Ended, IoCall::child(end))
            }
            None => {
                warn!(execution_id = %reservation.execution_id, "Releasing an orphan reservation");
                let release = ReleaseExecutionOperation::new(reservation.execution_id);
                self.call(DiscardStep::Released, IoCall::child(release))
            }
        }
    }

    fn witness(&mut self, space: usize) -> DiscardNext {
        let Some(name) = WITNESS_SPACES.get(space) else {
            return Ok(Next::Done(self.reason));
        };
        self.call(
            DiscardStep::Witness(space),
            IoCall::Page {
                space: name.to_string(),
                prefix: Key::from(Vec::new()),
                after: None,
                limit: DELETE_PAGE,
            },
        )
    }
}

fn failed(error: impl std::fmt::Display) -> DiscardError {
    DiscardError::Failed(error.to_string())
}

impl Operation for DiscardOperation {
    type Output = Option<DiscardReason>;
    type Error = DiscardError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> DiscardNext {
        let rows = vec![node_row(DISCARD_KEY), node_row(LAST_CONTACT_KEY)];
        Ok(Next::Call(IoCall::Read(rows)))
    }

    fn next(&mut self, result: IoResult) -> DiscardNext {
        match std::mem::replace(&mut self.step, DiscardStep::Released) {
            DiscardStep::Rows => {
                let IoResult::Values(values) = result else {
                    return Err(IoError::Unfinished.into());
                };
                let mut values = values.into_iter();
                self.marker = values.next().flatten().is_some();
                self.contact = values.next().flatten();
                self.call(DiscardStep::Held, IoCall::child(HeldReservations::all()))
            }
            DiscardStep::Held => {
                let held = result.child::<Vec<JobReservationRecord>, LifecycleError>()?;
                self.decide(held.map_err(failed)?)
            }
            DiscardStep::Marker => {
                result.done()?;
                self.next_job()
            }
            DiscardStep::Job(reservation) => self.job_read(*reservation, result),
            // A failed publication stays a pending obligation that settlement retries.
            DiscardStep::Ended => {
                let record = result.child::<JobRecord, JobMutationError>()?;
                let publish = PublishTerminalOperation::new(
                    record.map_err(failed)?,
                    self.serving,
                    self.now_ms,
                );
                self.call(DiscardStep::Published, IoCall::child(publish))
            }
            DiscardStep::Published => {
                if !matches!(result.child::<bool, LifecycleError>()?, Ok(true)) {
                    warn!("A discarded execution waits for its terminal settlement");
                }
                self.next_job()
            }
            DiscardStep::Released => {
                result.child::<bool, LifecycleError>()?.map_err(failed)?;
                self.next_job()
            }
            DiscardStep::Witness(space) => {
                let IoResult::Page { rows, .. } = result else {
                    return Err(IoError::Unfinished.into());
                };
                if rows.is_empty() {
                    return self.witness(space + 1);
                }
                let name = WITNESS_SPACES[space];
                let deletes = (rows.into_iter()).map(|(key, _)| (name.to_string(), key));
                self.call(
                    DiscardStep::Deleted(space),
                    IoCall::Delete(deletes.collect()),
                )
            }
            DiscardStep::Deleted(space) => {
                result.done()?;
                self.witness(space)
            }
        }
    }
}

#[cfg(test)]
#[path = "discard_tests.rs"]
mod tests;
