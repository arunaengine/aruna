//! Runs the blob cleanup timer: drains the cleanup queue, sweeps stale multipart uploads and
//! decides when the timer runs again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use tracing::warn;

use crate::blob::cleanup::{
    BLOB_CLEANUP_AFTER, BLOB_CLEANUP_RETRY, BlobCleanupOutcome, CleanupNode,
    ProcessCleanupOperation, SweepUploadsOperation, UploadSweepOutcome,
};
use crate::blob::migration::shared::MigrationError;
use crate::timer::{TimerInput, TimerPlan};

/// When the drain goes on after a pass stopped at its row limit.
const NEXT_PASS_AFTER: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq)]
enum CleanupStage {
    Drain,
    Sweep,
}

/// One cleanup timer run. The parts a reclaimed upload frees become cleanup rows, so the sweep
/// rides the same timer that drains them.
#[derive(Clone, Debug, PartialEq)]
pub struct CleanupTimerOperation {
    node: CleanupNode,
    now_ms: u64,
    after: Duration,
    stage: CleanupStage,
}

impl CleanupTimerOperation {
    pub fn new(input: &TimerInput) -> Self {
        let node = CleanupNode {
            routing: input.routing.clone(),
            has_blob: input.has_blob,
        };
        Self {
            node,
            now_ms: input.now_ms,
            after: BLOB_CLEANUP_AFTER,
            stage: CleanupStage::Drain,
        }
    }
}

/// When the timer runs again after a drain pass: soon after a failed row or pass, at once
/// after a pass that stopped at its row limit, and at the interval otherwise.
fn drain_after(drained: Result<BlobCleanupOutcome, MigrationError>) -> Duration {
    match drained {
        Ok(outcome) if outcome.failed > 0 => BLOB_CLEANUP_RETRY,
        Ok(outcome) if outcome.more => NEXT_PASS_AFTER,
        Ok(_) => BLOB_CLEANUP_AFTER,
        Err(error) => {
            warn!(error = %error, "Failed to drain blob cleanup");
            BLOB_CLEANUP_RETRY
        }
    }
}

impl Operation for CleanupTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        let drain = ProcessCleanupOperation::new(self.node.clone());
        Ok(Next::Call(IoCall::child(drain)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        match self.stage {
            CleanupStage::Drain => {
                self.after = drain_after(result.child::<BlobCleanupOutcome, MigrationError>()?);
                self.stage = CleanupStage::Sweep;
                let sweep = SweepUploadsOperation::new(self.now_ms);
                Ok(Next::Call(IoCall::child(sweep)))
            }
            CleanupStage::Sweep => {
                let swept = result.child::<UploadSweepOutcome, MigrationError>()?;
                if let Err(error) = swept {
                    warn!(error = %error, "Failed to sweep stale multipart uploads");
                }
                Ok(Next::Done(TimerPlan::from(Some(self.after))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn child<T>(result: Result<T, MigrationError>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    // A failed row retries soon even when the upload sweep fails; a pass at its row limit goes on
    // at once; a clean pass waits for the interval.
    #[test]
    fn cleanup_timer_plans() {
        let plan = |drained: BlobCleanupOutcome| {
            let mut run = CleanupTimerOperation::new(&TimerInput::new(1_000, None));
            run.first().unwrap();
            assert!(matches!(run.next(child(Ok(drained))), Ok(Next::Call(_))));
            let swept: Result<UploadSweepOutcome, _> = Err(MigrationError::failed("boom"));
            run.next(child(swept))
        };
        let failed = BlobCleanupOutcome {
            failed: 1,
            ..BlobCleanupOutcome::default()
        };
        let retry = TimerPlan::from(Some(BLOB_CLEANUP_RETRY));
        assert_eq!(plan(failed), Ok(Next::Done(retry)));
        let more = BlobCleanupOutcome {
            more: true,
            ..BlobCleanupOutcome::default()
        };
        assert_eq!(
            plan(more),
            Ok(Next::Done(TimerPlan::from(Some(NEXT_PASS_AFTER))))
        );
        let clean = TimerPlan::from(Some(BLOB_CLEANUP_AFTER));
        assert_eq!(plan(BlobCleanupOutcome::default()), Ok(Next::Done(clean)));
    }
}
