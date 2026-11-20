//! Runs the metadata materialization and graph prune timers: drains one batch of due jobs and,
//! once none is due, checks whether a later job waits before it plans the next run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::GitNode;
use aruna_core::operation::Operation;
use aruna_core::repository::remote::NodeScope;
use tracing::warn;

use crate::metadata::materialization_queue::{
    JobsExistOperation, MATERIALIZATION_POLL_AFTER, MATERIALIZATION_RETRY_AFTER,
    MaterializationBatchOperation, MetadataDrainResult, MetadataMaterializationError,
    NEXT_BATCH_AFTER, group_concurrency,
};
use crate::metadata::prune_jobs::DuePrune;
use crate::metadata::prune_queue::{
    GRAPH_POLL_AFTER, GRAPH_RETRY_AFTER, GraphDrainResult, MetadataGraphError, PruneBatchOperation,
    jobs_queued,
};
use crate::triggers::TimerInput;

/// When the materialization drain runs again while jobs are due. Nothing processed means the
/// due head is blocked, so the next scan waits instead of respinning at the batch pace.
pub fn drain_delay(result: &MetadataDrainResult) -> Duration {
    if result.processed == 0 {
        MATERIALIZATION_RETRY_AFTER
    } else {
        NEXT_BATCH_AFTER
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum QueueStage {
    Batch,
    Probe,
}

type RunTurn = Result<Next<Option<Duration>>, IoError>;

/// One materialization timer run. Outputs when the timer runs again.
#[derive(Clone, Debug, PartialEq)]
pub struct MaterializationRunOperation {
    node: GitNode,
    concurrency: usize,
    has_tasks: bool,
    stage: QueueStage,
}

impl MaterializationRunOperation {
    pub fn new(input: &TimerInput) -> Self {
        let node = GitNode {
            ids: input.serving,
            routing: input.routing.clone(),
        };
        Self {
            node,
            concurrency: group_concurrency(input.host_cores),
            has_tasks: input.has_tasks,
            stage: QueueStage::Batch,
        }
    }
}

impl Operation for MaterializationRunOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> RunTurn {
        let (node, concurrency) = (self.node.clone(), self.concurrency);
        let batch = MaterializationBatchOperation::new(node, concurrency, self.has_tasks);
        Ok(Next::Call(IoCall::child(batch)))
    }

    fn next(&mut self, result: IoResult) -> RunTurn {
        if self.stage == QueueStage::Probe {
            return match result.child::<bool, MetadataMaterializationError>()? {
                Ok(waiting) => Ok(Next::Done(waiting.then_some(MATERIALIZATION_POLL_AFTER))),
                Err(error) => {
                    warn!(error = ?error, "Failed to probe metadata materialization jobs");
                    Ok(Next::Done(Some(MATERIALIZATION_RETRY_AFTER)))
                }
            };
        }
        match result.child::<MetadataDrainResult, MetadataMaterializationError>()? {
            Ok(drained) if drained.has_more_due => Ok(Next::Done(Some(drain_delay(&drained)))),
            Ok(drained) if drained.next_due_after.is_some() => {
                Ok(Next::Done(drained.next_due_after))
            }
            Ok(_) => {
                self.stage = QueueStage::Probe;
                Ok(Next::Call(IoCall::child(JobsExistOperation::new())))
            }
            Err(error) => {
                warn!(error = ?error, "Failed to drain metadata materialization queue");
                Ok(Next::Done(Some(MATERIALIZATION_RETRY_AFTER)))
            }
        }
    }
}

/// One graph prune timer run. Outputs when the timer runs again.
#[derive(Clone, Debug, PartialEq)]
pub struct PruneRunOperation {
    scope: NodeScope,
    stage: QueueStage,
}

impl PruneRunOperation {
    pub fn new(input: &TimerInput) -> Self {
        // The prune reads only whether this node runs tasks, so no credential key is needed.
        let scope = NodeScope {
            node: input.serving,
            credential_key: None,
            has_blob: input.has_blob,
            has_tasks: input.has_tasks,
        };
        Self {
            scope,
            stage: QueueStage::Batch,
        }
    }
}

impl Operation for PruneRunOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> RunTurn {
        let batch = PruneBatchOperation::new(self.scope.clone());
        Ok(Next::Call(IoCall::child(batch)))
    }

    fn next(&mut self, result: IoResult) -> RunTurn {
        if self.stage == QueueStage::Probe {
            return match result.child::<DuePrune, IoError>()? {
                Ok(due) => Ok(Next::Done(
                    (!due.jobs.is_empty()).then_some(GRAPH_POLL_AFTER),
                )),
                Err(error) => {
                    warn!(error = ?error, "Failed to probe metadata graph prune jobs");
                    Ok(Next::Done(Some(GRAPH_RETRY_AFTER)))
                }
            };
        }
        match result.child::<GraphDrainResult, MetadataGraphError>()? {
            Ok(drained) if drained.has_more_due => Ok(Next::Done(Some(Duration::ZERO))),
            Ok(drained) if drained.next_due_after.is_some() => {
                Ok(Next::Done(drained.next_due_after))
            }
            Ok(_) => {
                self.stage = QueueStage::Probe;
                Ok(Next::Call(IoCall::child(jobs_queued())))
            }
            Err(error) => {
                warn!(error = ?error, "Failed to drain metadata graph prune queue");
                Ok(Next::Done(Some(GRAPH_RETRY_AFTER)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    #[test]
    fn blocked_batch_waits() {
        // Nothing processed means the due head is blocked, so the next scan
        // waits instead of respinning at the 25ms batch pace.
        let blocked = MetadataDrainResult {
            processed: 0,
            has_more_due: true,
            next_due_after: None,
        };
        let progressing = MetadataDrainResult {
            processed: 4,
            ..blocked
        };
        assert_eq!(drain_delay(&blocked), MATERIALIZATION_RETRY_AFTER);
        assert_eq!(drain_delay(&progressing), NEXT_BATCH_AFTER);
    }

    // An idle queue with a waiting job polls again; one without waits for the next queued job.
    #[test]
    fn idle_queue_probes() {
        let idle = MetadataDrainResult {
            processed: 0,
            has_more_due: false,
            next_due_after: None,
        };
        let mut run = MaterializationRunOperation::new(&TimerInput::new(1_000, None));
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let drained = child(Ok::<_, MetadataMaterializationError>(idle));
        assert!(matches!(
            run.next(drained),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let waiting = child(Ok::<_, MetadataMaterializationError>(true));
        assert_eq!(
            run.next(waiting),
            Ok(Next::Done(Some(MATERIALIZATION_POLL_AFTER)))
        );
        let mut prune = PruneRunOperation::new(&TimerInput::new(1_000, None));
        assert!(matches!(prune.first(), Ok(Next::Call(IoCall::Child(_)))));
        let error = MetadataGraphError::Conversion("broken".to_string());
        let failed = child(Err::<GraphDrainResult, _>(error));
        assert_eq!(prune.next(failed), Ok(Next::Done(Some(GRAPH_RETRY_AFTER))));
    }
}
