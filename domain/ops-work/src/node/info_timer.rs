//! Runs the node info timer: publishes this node's heartbeat with its next sequence and plans the
//! next one, also after a failed publish, so a transient failure never stops the heartbeat.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::heartbeat::{INFO_PUBLISH_INTERVAL, RunningJobs};
use aruna_core::id::NodeId;
use aruna_core::node::facts::NodeFacts;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use tracing::warn;

use crate::node::info::flows::RefreshInfoOperation;
use crate::node::info::inputs::InfoFlowError;
use crate::triggers::{TimerInput, TimerMemory, TimerPlan};

/// One heartbeat run. The sequence counts the runs since this node started.
#[derive(Clone, Debug, PartialEq)]
pub struct InfoTimerOperation {
    realm_id: RealmId,
    node_id: NodeId,
    sequence: u64,
    jobs: Option<RunningJobs>,
    facts: NodeFacts,
}

impl InfoTimerOperation {
    pub fn new(node_id: NodeId, realm_id: RealmId, input: &TimerInput) -> Self {
        Self {
            realm_id,
            node_id,
            sequence: input.memory.sequence.saturating_add(1),
            jobs: input.running,
            facts: input.facts.clone(),
        }
    }

    /// The next heartbeat, which continues from this run's sequence.
    fn plan(&self) -> TimerPlan {
        let memory = TimerMemory {
            sequence: self.sequence,
            ..TimerMemory::default()
        };
        TimerPlan::keep(Some(INFO_PUBLISH_INTERVAL), memory)
    }
}

impl Operation for InfoTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<TimerPlan>, IoError> {
        let facts = std::mem::take(&mut self.facts);
        let (realm_id, node_id) = (self.realm_id, self.node_id);
        let refresh = RefreshInfoOperation::new(realm_id, node_id, self.sequence, self.jobs, facts);
        Ok(Next::Call(IoCall::child(refresh)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerPlan>, IoError> {
        if let Err(error) = result.child::<(), InfoFlowError>()? {
            warn!(%error, "Failed to publish node info heartbeat");
        }
        Ok(Next::Done(self.plan()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    // A failed publish still plans the next heartbeat, which goes on from this run's sequence.
    #[test]
    fn heartbeat_sequence_advances() {
        let mut input = TimerInput::new(1_000, None);
        input.memory.sequence = 4;
        let node_id = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let mut run = InfoTimerOperation::new(node_id, RealmId::from_bytes([2; 32]), &input);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed: Result<(), InfoFlowError> = Err(InfoFlowError::Step("boom".to_string()));
        let Ok(Next::Done(plan)) = run.next(IoResult::Child(ChildOutcome::new(failed))) else {
            panic!("expected the next heartbeat plan");
        };
        assert_eq!(plan.after, Some(INFO_PUBLISH_INTERVAL));
        assert_eq!(plan.memory.sequence, 5);
    }
}
