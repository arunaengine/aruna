//! Names the outbox and witness drain timers a restart has to bring back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::WITNESS_DEADLINE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::page;
use aruna_core::types::Key;

use super::{OUTBOX_RETRY_AFTER, schedule_outbox_drain};
use crate::jobs::lifecycle::witness_arm::schedule_witness_drain;

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
        let (rows, _) = page(result)?;
        Ok(Next::Done(lifecycle_timers(!rows.is_empty())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::Value;

    // A stored witness deadline brings the witness drain back with the outbox drain.
    #[test]
    fn witness_timer_needs_rows() {
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
