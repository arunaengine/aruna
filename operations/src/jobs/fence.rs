//! The start fence: after every start this node takes and starts no new work until it caught up
//! with the realm config, then cancels the local executions whose membership interval left.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::effects::Effect;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{COMPUTE_DEPARTURE_KEYSPACE, JOB_RESERVATION_KEYSPACE};
use aruna_core::node_state::START_FENCE_KEY;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobId, left_executions};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use tracing::{info, warn};

use crate::jobs::lifecycle::membership::MembershipViewOperation;
use crate::jobs::store::job_calls::{CancelRequestOutcome, set_cancel_requested};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::secrets::owner::WriterView;

/// Delay after which fenced work is looked at again.
pub const FENCE_RECHECK: Duration = Duration::from_secs(10);
/// Reservation rows one page of the lift reads.
const RESERVATION_PAGE: usize = 256;

fn fence_row() -> (KeySpace, Key) {
    (
        COMPUTE_DEPARTURE_KEYSPACE.to_string(),
        Key::from(START_FENCE_KEY),
    )
}

/// The read of the fence row, for an operation that checks the fence before new work.
pub fn fence_read() -> IoCall {
    IoCall::Read(vec![fence_row()])
}

/// Whether the fence holds after [`fence_read`]. An unreadable row counts as up: fenced work
/// only waits.
pub fn fence_up(result: IoResult) -> bool {
    result.value().map_or(true, |value| value.is_some())
}

/// Raises the fence at start, before any peer can reach this node.
#[derive(Debug, Default, PartialEq)]
pub struct RaiseFenceOperation;

impl Operation for RaiseFenceOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let (space, key) = fence_row();
        let row = (space, key, Value::from(&[][..]));
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum LiftStep {
    View,
    Page,
    Mark(JobId),
    Fence,
    Lift,
}

/// Lifts the fence once caught up: every local execution whose intervals all left is marked for
/// cancellation first, then the node advertises again. The marking runs on every later pass too.
/// Unknown membership or a failed mark keeps the fence up; the output says whether it is down.
#[derive(Debug, PartialEq)]
pub struct LiftFenceOperation {
    local: NodeId,
    realm_id: RealmId,
    now_ms: u64,
    view: Option<WriterView>,
    left: VecDeque<JobId>,
    /// The next reservation page, once the current one is marked.
    cursor: Option<Key>,
    step: LiftStep,
}

type LiftNext = Result<Next<bool>, IoError>;

impl LiftFenceOperation {
    pub fn new(local: NodeId, realm_id: RealmId) -> Self {
        Self {
            local,
            realm_id,
            now_ms: 0,
            view: None,
            left: VecDeque::new(),
            cursor: None,
            step: LiftStep::View,
        }
    }

    fn page(&mut self, after: Option<Key>) -> LiftNext {
        self.step = LiftStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: JOB_RESERVATION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: RESERVATION_PAGE,
        }))
    }

    fn paged(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> LiftNext {
        let view = self.view.as_ref().ok_or(IoError::Unfinished)?;
        let records: Vec<JobReservationRecord> = (rows.iter())
            .filter_map(|(_, value)| postcard::from_bytes(value.as_ref()).ok())
            .collect();
        let cited = records.iter().map(|row| (row.job_id, &row.config_clock));
        let left = left_executions(self.local, cited, &view.intervals, &view.clock);
        self.left.extend(left);
        self.cursor = next;
        self.mark_next()
    }

    fn mark_next(&mut self) -> LiftNext {
        if let Some(job_id) = self.left.pop_front() {
            info!(job_id = %job_id, "Cancelling an execution whose membership interval left");
            self.step = LiftStep::Mark(job_id);
            let mark = set_cancel_requested(job_id, self.now_ms);
            return Ok(Next::Call(IoCall::child(mark)));
        }
        if let Some(after) = self.cursor.take() {
            return self.page(Some(after));
        }
        self.step = LiftStep::Fence;
        Ok(Next::Call(fence_read()))
    }
}

impl Operation for LiftFenceOperation {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> LiftNext {
        let view = MembershipViewOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(view)))
    }

    fn next(&mut self, result: IoResult) -> LiftNext {
        match std::mem::replace(&mut self.step, LiftStep::Lift) {
            LiftStep::View => match result.child::<Option<WriterView>, IoError>()? {
                Ok(Some(view)) => {
                    self.view = Some(view);
                    self.page(None)
                }
                _ => {
                    warn!("Membership is unknown; the start fence stays up");
                    Ok(Next::Done(false))
                }
            },
            LiftStep::Page => match result {
                IoResult::Page { rows, next } => self.paged(rows, next),
                other => Err(IoError::Unexpected {
                    expected: "IoResult::Page",
                    got: other.label(),
                }),
            },
            // A reservation without its job cannot resume; the discard or the doctor handles it.
            LiftStep::Mark(job_id) => {
                match result.child::<CancelRequestOutcome, JobMutationError>()? {
                    Ok(_) | Err(JobMutationError::NotFound) => self.mark_next(),
                    Err(error) => {
                        warn!(job_id = %job_id, error = %error, "Execution of a left interval was not marked");
                        Ok(Next::Done(false))
                    }
                }
            }
            LiftStep::Fence => match result.value()? {
                Some(_) => {
                    self.step = LiftStep::Lift;
                    Ok(Next::Call(IoCall::Delete(vec![fence_row()])))
                }
                None => Ok(Next::Done(true)),
            },
            LiftStep::Lift => {
                result.done()?;
                info!("Caught up with the realm config; the start fence is lifted");
                let publish = Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::PublishNodeInfo,
                    after: Duration::ZERO,
                });
                Ok(Next::Then {
                    output: true,
                    calls: vec![IoCall::Effect(Box::new(publish))],
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "fence_tests.rs"]
mod tests;
