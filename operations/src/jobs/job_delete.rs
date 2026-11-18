//! Removes a caller's finished run from this node's lists.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord, JobState, cleanup_job_id};
use aruna_core::structs::identity::auth::AuthContext;
use tracing::warn;

use super::lifecycle::cancel::PublishDeletionOperation;
use super::prune::PruneTimerOperation;
use super::records::pages::ServingNode;
use super::route::JobRouteError;
use super::store::RunDelete;
use super::store::job_calls::{CancelRequestOutcome, delete_finished_run, set_cancel_requested};
use super::store::mutation_error::JobMutationError;
use super::store::row_calls::read_job_record;

#[derive(Debug, PartialEq)]
enum DeleteStep {
    Record,
    Cleanup { deletable: bool },
    Publish,
    Remove,
    Flag(RunDelete),
    Timer(RunDelete),
}

/// Removes the caller's finished run from this node's lists. A deletable run is recorded in its
/// replicated family first, so a failed record keeps it listed and a retry records it again. A
/// queued job is cancelled first; one that never ran settles at once and goes with it.
#[derive(Debug, PartialEq)]
pub struct DeleteOwnedOperation {
    auth: AuthContext,
    job_id: JobId,
    serving: ServingNode,
    now_ms: u64,
    cancelled: bool,
    step: DeleteStep,
}

type DeleteNext = Result<Next<RunDelete>, JobMutationError>;

impl DeleteOwnedOperation {
    pub fn new(auth: &AuthContext, job_id: JobId, serving: ServingNode, now_ms: u64) -> Self {
        Self {
            auth: auth.clone(),
            job_id,
            serving,
            now_ms,
            cancelled: false,
            step: DeleteStep::Record,
        }
    }

    fn read_record(&mut self) -> DeleteNext {
        self.step = DeleteStep::Record;
        Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
    }

    fn remove(&mut self, recorded: bool) -> DeleteNext {
        self.step = DeleteStep::Remove;
        let delete = delete_finished_run(self.auth.user_id, self.job_id, recorded);
        Ok(Next::Call(IoCall::child(delete)))
    }

    /// A deleted job other than a run is pruned by the timer, which is moved up here.
    fn finish(&mut self, outcome: RunDelete) -> DeleteNext {
        if outcome != RunDelete::Deleted {
            return Ok(Next::Done(outcome));
        }
        self.step = DeleteStep::Timer(outcome);
        Ok(Next::Call(IoCall::child(PruneTimerOperation::new(
            self.now_ms,
        ))))
    }

    fn removed(&mut self, outcome: RunDelete) -> DeleteNext {
        let queued = matches!(
            outcome,
            RunDelete::Unfinished(JobState::Queued | JobState::AwaitingKey)
        );
        if !queued || self.cancelled {
            return self.finish(outcome);
        }
        self.step = DeleteStep::Flag(outcome);
        let request = set_cancel_requested(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(request)))
    }
}

fn stored_record(result: IoResult) -> Result<Option<JobRecord>, JobMutationError> {
    result.child::<Option<JobRecord>, JobMutationError>()?
}

impl Operation for DeleteOwnedOperation {
    type Output = RunDelete;
    type Error = JobMutationError;

    fn first(&mut self) -> DeleteNext {
        self.read_record()
    }

    fn next(&mut self, result: IoResult) -> DeleteNext {
        match std::mem::replace(&mut self.step, DeleteStep::Record) {
            DeleteStep::Record => {
                let user_id = self.auth.user_id;
                let deletable = stored_record(result)?.is_some_and(|record| {
                    record.created_by == user_id
                        && record.state.is_terminal()
                        && matches!(record.payload, JobPayload::Execution(_))
                });
                self.step = DeleteStep::Cleanup { deletable };
                let cleanup = read_job_record(cleanup_job_id(self.job_id));
                Ok(Next::Call(IoCall::child(cleanup)))
            }
            DeleteStep::Cleanup { deletable } => {
                let cleanup = stored_record(result)?;
                let settled = cleanup.is_none_or(|cleanup| cleanup.state.is_terminal());
                if !(deletable && settled) {
                    return self.remove(false);
                }
                self.step = DeleteStep::Publish;
                let deletion = PublishDeletionOperation::new(
                    &self.auth,
                    self.job_id,
                    self.serving,
                    self.now_ms,
                );
                Ok(Next::Call(IoCall::child(deletion)))
            }
            DeleteStep::Publish => match result.child::<(), JobRouteError>()? {
                Ok(()) => self.remove(true),
                Err(error) => self.removed(RunDelete::Unrecorded(error.to_string())),
            },
            DeleteStep::Remove => {
                let outcome = result.child::<RunDelete, JobMutationError>()??;
                self.removed(outcome)
            }
            DeleteStep::Flag(outcome) => {
                match result.child::<CancelRequestOutcome, JobMutationError>()?? {
                    CancelRequestOutcome::Cancelled(_) => {
                        self.cancelled = true;
                        self.read_record()
                    }
                    _ => self.finish(outcome),
                }
            }
            DeleteStep::Timer(outcome) => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(error = %error, "Failed to move up the job prune timer");
                }
                Ok(Next::Done(outcome))
            }
        }
    }
}

#[cfg(test)]
#[path = "job_delete_tests.rs"]
mod tests;
