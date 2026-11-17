//! Asks the mirror repair drain to run now.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::EffectOperation;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Effects;
use smallvec::smallvec;

use super::SyncMirrorError;

#[derive(Debug, PartialEq)]
enum KickState {
    Ready,
    Scheduling,
    Done(Result<(), SyncMirrorError>),
}

/// Shortens the mirror repair timer to now; a refusal of the task backend is the error.
#[derive(Debug, PartialEq)]
pub struct KickRepairOperation {
    state: KickState,
}

impl KickRepairOperation {
    pub fn new() -> Self {
        Self {
            state: KickState::Ready,
        }
    }
}

impl Default for KickRepairOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl EffectOperation for KickRepairOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn start(&mut self) -> Effects {
        if self.state != KickState::Ready {
            return Effects::new();
        }
        self.state = KickState::Scheduling;
        smallvec![Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (&self.state, event) {
            (KickState::Scheduling, Event::Task(TaskEvent::TimerScheduled { .. })) => Ok(()),
            (KickState::Scheduling, Event::Task(TaskEvent::Error { message, .. })) => {
                Err(SyncMirrorError::Mirror(message))
            }
            (_, event) => Err(SyncMirrorError::Unexpected(event)),
        };
        self.state = KickState::Done(result);
        Effects::new()
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, KickState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            KickState::Done(result) => result,
            _ => Err(SyncMirrorError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.state = KickState::Done(Err(SyncMirrorError::Unfinished));
        Effects::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn scheduling() -> KickRepairOperation {
        let mut operation = KickRepairOperation::new();
        operation.start();
        operation
    }

    #[test]
    fn shortens_timer() {
        let mut operation = KickRepairOperation::new();
        check_trace(
            &mut operation,
            &[Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainMirrorRepair,
                after: Duration::ZERO,
            })],
            vec![(
                Event::Task(TaskEvent::TimerScheduled {
                    key: TaskKey::DrainMirrorRepair,
                    after: Duration::ZERO,
                }),
                vec![],
            )],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn refusal_fails() {
        let mut operation = scheduling();
        operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainMirrorRepair),
            message: "stopped".to_string(),
        }));
        assert_eq!(
            operation.finalize(),
            Err(SyncMirrorError::Mirror("stopped".to_string()))
        );
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(scheduling, sample_events, |event| {
            matches!(event, Event::Task(TaskEvent::Error { .. }))
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label}");
            assert!(matches!(error, SyncMirrorError::Unexpected(_)), "{label}");
        }
        let mut early = KickRepairOperation::new();
        early.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        }));
        assert!(early.finalize().is_err());
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(KickRepairOperation::new());
        rejects_finalize(scheduling());
        let mut operation = scheduling();
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Err(SyncMirrorError::Unfinished));
    }
}
