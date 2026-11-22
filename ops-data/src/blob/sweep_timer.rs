//! Arms the timer of a periodic blob sweep.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use tracing::warn;

/// Makes a sweep run now. The task adapter persists the timer first; a failure only warns, and
/// the next start arms it again.
#[derive(Debug, PartialEq)]
pub struct ArmSweepOperation {
    key: TaskKey,
}

impl ArmSweepOperation {
    pub fn new(key: TaskKey) -> Self {
        Self { key }
    }
}

impl Operation for ArmSweepOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let arm = Effect::Task(TaskEffect::ShortenTimer {
            key: self.key.clone(),
            after: Duration::ZERO,
        });
        Ok(Next::Call(arm.into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match result.event()? {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(message = %message, "Failed to schedule a blob sweep");
            }
            other => warn!(event = other.label(), "Unexpected blob sweep timer result"),
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::IoCall;

    #[test]
    fn asks_for_timer() {
        let mut arm = ArmSweepOperation::new(TaskKey::DrainReclaimQueue);
        assert!(matches!(arm.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let scheduled = TaskEvent::TimerScheduled {
            key: TaskKey::DrainReclaimQueue,
            after: Duration::ZERO,
        };
        let answer = IoResult::Event(Box::new(Event::Task(scheduled)));
        assert_eq!(arm.next(answer), Ok(Next::Done(())));
    }
}
