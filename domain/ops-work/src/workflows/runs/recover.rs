//! Ends an attempt whose coordinator broke off: the run waits for the next attempt, which replays
//! its stored log and adopts its keyed children, or fails on the last attempt.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord};
use aruna_core::workflow::WorkflowEffect;
use ulid::Ulid;

use super::run::session_name;
use super::{RunChange, change_call, changed};
use crate::workflows::CatalogError;

/// What an interrupted attempt leaves behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interrupted {
    /// Another attempt runs the run again.
    Retry,
    /// The run ended in this state.
    Ended(RunState),
}

/// The state an interrupted attempt leaves: queued again while attempts remain, failed with the
/// reason on the last one. A run being cancelled ends as cancelled either way.
pub fn interrupted_state(record: &WorkflowRunRecord, final_attempt: bool) -> RunState {
    match (record.state, final_attempt) {
        (RunState::Cancelling, _) => RunState::Cancelled,
        (_, true) => RunState::Failed,
        (_, false) => RunState::Queued,
    }
}

#[derive(Debug, PartialEq)]
enum RecoverStep {
    Read,
    Change,
    Close(Interrupted),
}

/// Records an attempt whose coordinator ended without a result, and closes what is left of its
/// session. Children keep running: a later attempt finds them by their keys.
#[derive(Debug, PartialEq)]
pub struct InterruptRunOperation {
    run_id: Ulid,
    final_attempt: bool,
    reason: String,
    step: RecoverStep,
}

impl InterruptRunOperation {
    pub fn new(run_id: Ulid, final_attempt: bool, reason: impl Into<String>) -> Self {
        Self {
            run_id,
            final_attempt,
            reason: reason.into(),
            step: RecoverStep::Read,
        }
    }
}

impl Operation for InterruptRunOperation {
    type Output = Interrupted;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Interrupted>, CatalogError> {
        self.step = RecoverStep::Read;
        Ok(Next::Call(super::read_run(self.run_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Interrupted>, CatalogError> {
        match std::mem::replace(&mut self.step, RecoverStep::Change) {
            RecoverStep::Read => {
                let record = super::run_answer(result)?.ok_or(CatalogError::NotFound)?;
                if record.state.is_terminal() {
                    self.step = RecoverStep::Close(Interrupted::Ended(record.state));
                    return Ok(Next::Call(close(self.run_id)));
                }
                let state = interrupted_state(&record, self.final_attempt);
                let error = match state {
                    RunState::Failed => Some(format!(
                        "the coordinator ended without a result: {}",
                        self.reason
                    )),
                    _ => None,
                };
                Ok(Next::Call(change_call(
                    self.run_id,
                    vec![RunChange::State { state, error }],
                )))
            }
            RecoverStep::Change => {
                let record = changed(result)?;
                let outcome = match record.state.is_terminal() {
                    true => Interrupted::Ended(record.state),
                    false => Interrupted::Retry,
                };
                self.step = RecoverStep::Close(outcome);
                Ok(Next::Call(close(self.run_id)))
            }
            // The session may be gone with its coordinator; either answer ends it here.
            RecoverStep::Close(outcome) => {
                let _ = result.event()?;
                Ok(Next::Done(outcome))
            }
        }
    }
}

fn close(run_id: Ulid) -> IoCall {
    let effect = WorkflowEffect::Close {
        session: session_name(run_id),
    };
    IoCall::Effect(Box::new(Effect::Workflow(Box::new(effect))))
}
