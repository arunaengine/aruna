//! Asks the mirror repair drain to run now.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};

use super::{SyncMirrorError, unexpected};

/// Shortens the mirror repair timer to now; a refusal of the task backend is the error.
#[derive(Debug, Default, PartialEq)]
pub struct KickRepairOperation;

impl KickRepairOperation {
    pub fn new() -> Self {
        Self
    }
}

impl Operation for KickRepairOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<()>, SyncMirrorError> {
        let shorten = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(shorten))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncMirrorError> {
        let IoResult::Event(event) = result else {
            return Err(unexpected("TaskEvent::TimerScheduled", &result));
        };
        match *event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(Next::Done(())),
            Event::Task(TaskEvent::Error { message, .. }) => Err(SyncMirrorError::Mirror(message)),
            other => Err(SyncMirrorError::Io(IoError::Unexpected {
                expected: "TaskEvent::TimerScheduled",
                got: other.label(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    #[test]
    fn shortens_timer() {
        let mut operation = KickRepairOperation::new();
        let shorten = TaskEffect::ShortenTimer {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        };
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(Effect::Task(shorten)))))
        );
        let scheduled = TaskEvent::TimerScheduled {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        };
        assert_eq!(
            operation.next(event(Event::Task(scheduled))),
            Ok(Next::Done(()))
        );
    }

    #[test]
    fn refusal_fails() {
        let refused = TaskEvent::Error {
            key: Some(TaskKey::DrainMirrorRepair),
            message: "stopped".to_string(),
        };
        assert_eq!(
            KickRepairOperation::new().next(event(Event::Task(refused))),
            Err(SyncMirrorError::Mirror("stopped".to_string()))
        );
        let cancelled = TaskEvent::TimerCancelled {
            key: TaskKey::DrainMirrorRepair,
        };
        assert!(matches!(
            KickRepairOperation::new().next(event(Event::Task(cancelled))),
            Err(SyncMirrorError::Io(IoError::Unexpected { .. }))
        ));
    }
}
