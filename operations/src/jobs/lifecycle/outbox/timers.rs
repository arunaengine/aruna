//! Names the outbox and witness drain timers a restart has to bring back, and runs the outbox
//! drain timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::WITNESS_DEADLINE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Key;
use tracing::warn;

use super::drain::DrainOutboxOperation;
use super::{OUTBOX_RETRY_AFTER, schedule_outbox_drain};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::witness::arm::schedule_witness_drain;

/// The timers to arm, with the witness drain only while witness deadlines are stored.
pub fn lifecycle_timers(witness_pending: bool) -> Vec<Effect> {
    let mut timers = vec![schedule_outbox_drain(OUTBOX_RETRY_AFTER)];
    if witness_pending {
        timers.push(schedule_witness_drain(Duration::from_secs(1)));
    }
    timers
}

/// Finds the timers to re-arm after a restart. Both queues are durable rows, so the timers only
/// have to be brought back, never the work.
#[derive(Debug, Default, PartialEq)]
pub struct LifecycleTimersOperation;

impl Operation for LifecycleTimersOperation {
    type Output = Vec<Effect>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<Effect>>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: WITNESS_DEADLINE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Effect>>, IoError> {
        let (rows, _) = result.page()?;
        Ok(Next::Done(lifecycle_timers(!rows.is_empty())))
    }
}

/// One outbox drain timer run. The pass is bounded, so a backlog or a failed pass runs again
/// after the retry instead of blocking. Outputs when the timer runs again.
#[derive(Clone, Debug, PartialEq)]
pub struct FamilyTimerOperation {
    realm_id: RealmId,
    local: NodeId,
}

impl FamilyTimerOperation {
    pub fn new(realm_id: RealmId, local: NodeId) -> Self {
        Self { realm_id, local }
    }
}

impl Operation for FamilyTimerOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<Duration>>, IoError> {
        let drain = DrainOutboxOperation::new(self.realm_id, self.local);
        Ok(Next::Call(IoCall::child(drain)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Duration>>, IoError> {
        let remaining = result
            .child::<bool, LifecycleError>()?
            .unwrap_or_else(|error| {
                warn!(error = %error, "Job family outbox drain failed");
                true
            });
        Ok(Next::Done(remaining.then_some(OUTBOX_RETRY_AFTER)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::types::Value;

    // A failed or unfinished pass runs again after the retry; a drained outbox waits.
    #[test]
    fn family_drain_retries() {
        let node_id = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let mut run = FamilyTimerOperation::new(RealmId::from_bytes([2; 32]), node_id);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed = Err::<bool, _>(LifecycleError::Codec("broken".to_string()));
        let failed = IoResult::Child(ChildOutcome::new(failed));
        assert_eq!(run.next(failed), Ok(Next::Done(Some(OUTBOX_RETRY_AFTER))));
        let drained = IoResult::Child(ChildOutcome::new(Ok::<_, LifecycleError>(false)));
        assert_eq!(run.next(drained), Ok(Next::Done(None)));
    }

    // A stored witness deadline brings the witness drain back with the outbox drain.
    #[test]
    fn witness_needs_rows() {
        let mut timers = LifecycleTimersOperation;
        assert!(matches!(
            timers.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(timers.next(empty), Ok(Next::Done(lifecycle_timers(false))));
        let row = (Key::from(vec![1]), Value::from(vec![2]));
        let stored = IoResult::Page {
            rows: vec![row],
            next: None,
        };
        let Ok(Next::Done(armed)) = timers.next(stored) else {
            panic!("the timers are named");
        };
        assert_eq!(armed.len(), 2);
    }
}
