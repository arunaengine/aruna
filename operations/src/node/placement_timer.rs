//! Runs the placement timer: observes this node's placement subject, reconciles its held shard
//! topics, and decides when the timer runs again, with the pull retry ladder in its memory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::node_facts::NodeFacts;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use tracing::warn;

use crate::node::info::flows::ObservePlacementOperation;
use crate::placement::policy::subject::{SubjectScanError, SubjectScanResult};
use crate::placement::process_placements::{
    PlacementReconcileOutcome, PlacementReconcileStatus, ReconcilePlacementsOperation,
};
use crate::sync::shard_placement::{PLACEMENT_RETRY_AFTER, PULL_RETRY_AFTER, PULL_RETRY_MAX};
use crate::triggers::{TimerInput, TimerMemory, TimerPlan};

#[derive(Clone, Debug, PartialEq)]
enum PlacementStage {
    Observe,
    Reconcile,
}

/// One run of the placement timer. A reconcile that withheld a pull it could not do yet runs
/// again on a ladder that starts prompt and doubles up to the placement interval; a clean one
/// clears the ladder, and a storage failure runs again after the placement retry.
#[derive(Clone, Debug, PartialEq)]
pub struct PlacementTimerOperation {
    realm_id: RealmId,
    node_id: NodeId,
    now_ms: u64,
    facts: NodeFacts,
    memory: TimerMemory,
    stage: PlacementStage,
}

impl PlacementTimerOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, input: &TimerInput) -> Self {
        Self {
            realm_id,
            node_id,
            now_ms: input.now_ms,
            facts: input.facts.clone(),
            memory: input.memory.clone(),
            stage: PlacementStage::Observe,
        }
    }

    /// When the timer runs again after `outcome`, and the ladder it keeps until then.
    fn plan(&self, outcome: PlacementReconcileOutcome) -> TimerPlan {
        let kept = TimerPlan {
            after: None,
            memory: self.memory.clone(),
        };
        match outcome.status {
            PlacementReconcileStatus::Clean => TimerPlan::default(),
            // New holders only need their co-holders to apply the same config, so the ladder
            // starts prompt and never jumps to the placement interval.
            PlacementReconcileStatus::RetryScheduled if outcome.pull_pending => {
                let (after, memory) = self.memory.next_retry(PULL_RETRY_AFTER, PULL_RETRY_MAX);
                TimerPlan {
                    after: Some(after),
                    memory,
                }
            }
            // The reconciler armed its own retry.
            PlacementReconcileStatus::RetryScheduled => kept,
            PlacementReconcileStatus::StorageFailure => TimerPlan {
                after: Some(PLACEMENT_RETRY_AFTER),
                ..kept
            },
        }
    }
}

impl Operation for PlacementTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        // The same observation that reconciles shards reconciles this node's placement subject:
        // a moved, draining or removed node stops admitting governed data.
        let facts = std::mem::take(&mut self.facts);
        let (realm_id, node_id) = (self.realm_id, self.node_id);
        let observe = ObservePlacementOperation::new(realm_id, node_id, self.now_ms, facts);
        Ok(Next::Call(IoCall::child(observe)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        match self.stage {
            PlacementStage::Observe => {
                let observed = result.child::<Option<SubjectScanResult>, SubjectScanError>()?;
                if let Err(error) = observed {
                    warn!(%error, "Placement subject reconcile failed");
                }
                self.stage = PlacementStage::Reconcile;
                let (realm_id, node_id) = (self.realm_id, self.node_id);
                let reconcile = ReconcilePlacementsOperation::new(realm_id, node_id, true);
                Ok(Next::Call(IoCall::child(reconcile)))
            }
            PlacementStage::Reconcile => {
                let outcome = result.child::<PlacementReconcileOutcome, IoError>()?;
                let failed = |_| PlacementReconcileOutcome::storage_failure();
                let outcome = outcome.unwrap_or_else(failed);
                Ok(Next::Done(self.plan(outcome)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timer(attempts: u32) -> PlacementTimerOperation {
        let mut input = TimerInput::new(1_000, None);
        input.memory.attempts = attempts;
        let node_id = iroh::SecretKey::from_bytes(&[1; 32]).public();
        PlacementTimerOperation::new(RealmId::from_bytes([2; 32]), node_id, &input)
    }

    fn outcome(status: PlacementReconcileStatus, pull_pending: bool) -> PlacementReconcileOutcome {
        PlacementReconcileOutcome {
            retry_scheduled: status == PlacementReconcileStatus::RetryScheduled,
            pull_pending,
            status,
        }
    }

    // A pending pull climbs the ladder from its prompt base, kept in the timer's memory; a clean
    // reconcile clears it; a storage failure and a self-armed retry keep it as it is.
    #[test]
    fn placement_retry_ladder() {
        let pending = outcome(PlacementReconcileStatus::RetryScheduled, true);
        let first = timer(0).plan(pending);
        assert_eq!(first.after, Some(PULL_RETRY_AFTER));
        assert_eq!(first.memory.attempts, 1);
        let second = timer(1).plan(pending);
        assert_eq!(second.after, Some(PULL_RETRY_AFTER * 2));
        assert_eq!(second.memory.attempts, 2);
        let capped = timer(40).plan(pending);
        assert_eq!(capped.after, Some(PULL_RETRY_MAX.min(PULL_RETRY_AFTER * 128)));

        let clean = outcome(PlacementReconcileStatus::Clean, false);
        assert_eq!(timer(3).plan(clean), TimerPlan::default());
        let failed = timer(3).plan(PlacementReconcileOutcome::storage_failure());
        assert_eq!((failed.after, failed.memory.attempts), (Some(PLACEMENT_RETRY_AFTER), 3));
        let armed = timer(3).plan(outcome(PlacementReconcileStatus::RetryScheduled, false));
        assert_eq!((armed.after, armed.memory.attempts), (None, 3));
    }

    // A failed reconcile reads as a storage failure, which runs the timer again.
    #[test]
    fn failed_reconcile_retries() {
        let mut run = timer(0);
        run.first().unwrap();
        let observed: Result<Option<SubjectScanResult>, SubjectScanError> = Ok(None);
        let observed = IoResult::Child(aruna_core::calls::ChildOutcome::new(observed));
        assert!(matches!(run.next(observed), Ok(Next::Call(IoCall::Child(_)))));
        let failed: Result<PlacementReconcileOutcome, IoError> = Err(IoError::Unfinished);
        let failed = IoResult::Child(aruna_core::calls::ChildOutcome::new(failed));
        let plan = TimerPlan::from(Some(PLACEMENT_RETRY_AFTER));
        assert_eq!(run.next(failed), Ok(Next::Done(plan)));
    }
}
