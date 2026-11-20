//! Runs the replication drain timer: drains one batch of due replication jobs from the catch-up
//! cursors the timer kept and plans its next run with the cursors the batch left.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;
use tracing::warn;

use crate::replication::drain::{BlobDrainResult, ProcessBatchOperation, REPLICATION_RETRY_AFTER};
use crate::replication::page::deliver::MetadataPages;
use crate::replication::queue::BlobQueueError;
use crate::triggers::{TimerInput, TimerMemory, TimerPlan};

/// One replication drain run. A failed batch keeps the cursors as they were.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationTimerOperation {
    routing: NodeRouting,
    serving: Option<(NodeId, RealmId)>,
    memory: TimerMemory,
}

impl ReplicationTimerOperation {
    pub fn new(input: &TimerInput) -> Self {
        Self {
            routing: input.routing.clone(),
            serving: input.serving,
            memory: input.memory.clone(),
        }
    }

    /// More due jobs run at once, the next one when it is due, with the cursors the batch left.
    fn plan(&self, drained: BlobDrainResult) -> TimerPlan {
        let after = match drained.has_more_due {
            true => Some(Duration::ZERO),
            false => drained.next_due_after,
        };
        let memory = TimerMemory {
            catch_up: drained.catch_up,
            ..self.memory.clone()
        };
        TimerPlan { after, memory }
    }
}

impl Operation for ReplicationTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        let (routing, serving) = (self.routing.clone(), self.serving);
        let batch = ProcessBatchOperation::new(MetadataPages, routing, serving);
        let batch = batch.with_catch_up(self.memory.catch_up.clone());
        Ok(Next::Call(IoCall::child(batch)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        match result.child::<BlobDrainResult, BlobQueueError>()? {
            Ok(drained) => Ok(Next::Done(self.plan(drained))),
            Err(error) => {
                warn!(%error, "Failed to drain blob replication queue");
                Ok(Next::Done(TimerPlan {
                    after: Some(REPLICATION_RETRY_AFTER),
                    memory: self.memory.clone(),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::catch_up::CatchUpCursors;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;

    fn drained(has_more_due: bool, set_after: &[u8]) -> BlobDrainResult {
        let catch_up = CatchUpCursors {
            set_after: Some(set_after.to_vec()),
            ..CatchUpCursors::default()
        };
        BlobDrainResult {
            processed: 0,
            succeeded: 0,
            failed: 0,
            has_more_due,
            next_due_after: None,
            catch_up,
        }
    }

    fn run(kept: &[u8], result: Result<BlobDrainResult, BlobQueueError>) -> TimerPlan {
        let mut input = TimerInput::new(1_000, None);
        input.memory.catch_up.set_after = Some(kept.to_vec());
        let mut timer = ReplicationTimerOperation::new(&input);
        assert!(matches!(timer.first(), Ok(Next::Call(IoCall::Child(_)))));
        let Ok(Next::Done(plan)) = timer.next(IoResult::Child(ChildOutcome::new(result))) else {
            panic!("expected the next drain plan");
        };
        plan
    }

    // A batch hands its cursors to the next run; a failed one keeps the cursors it started from.
    #[test]
    fn drain_cursors_kept() {
        let more = run(b"kept", Ok(drained(true, b"left")));
        assert_eq!(more.after, Some(Duration::ZERO));
        assert_eq!(
            more.memory.catch_up.set_after.as_deref(),
            Some(&b"left"[..])
        );
        let error = BlobQueueError::Storage(StorageError::KeyNotFound);
        let failed = run(b"kept", Err(error));
        assert_eq!(failed.after, Some(REPLICATION_RETRY_AFTER));
        assert_eq!(
            failed.memory.catch_up.set_after.as_deref(),
            Some(&b"kept"[..])
        );
    }
}
