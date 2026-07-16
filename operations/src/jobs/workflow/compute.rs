use aruna_core::compute::{ComputeEffect, ComputeEvent};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleState {
    Ready,
    Waiting,
    Complete,
    Failed,
}

#[derive(Debug, PartialEq)]
pub struct ComputeLifecycle {
    state: LifecycleState,
    effect: Option<ComputeEffect>,
    output: Option<ComputeEvent>,
    error: Option<ComputeLifecycleError>,
}

impl ComputeLifecycle {
    pub fn new(effect: ComputeEffect) -> Self {
        Self {
            state: LifecycleState::Ready,
            effect: Some(effect),
            output: None,
            error: None,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ComputeLifecycleError {
    #[error("unexpected event for compute lifecycle")]
    UnexpectedEvent,
    #[error("compute lifecycle aborted")]
    Aborted,
    #[error("compute lifecycle is incomplete")]
    Incomplete,
}

impl Operation for ComputeLifecycle {
    type Output = ComputeEvent;
    type Error = ComputeLifecycleError;

    fn start(&mut self) -> Effects {
        match (self.state, self.effect.take()) {
            (LifecycleState::Ready, Some(effect)) => {
                self.state = LifecycleState::Waiting;
                smallvec::smallvec![Effect::Compute(effect)]
            }
            _ => {
                self.state = LifecycleState::Failed;
                self.error = Some(ComputeLifecycleError::UnexpectedEvent);
                Effects::new()
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (LifecycleState::Waiting, Event::Compute(event)) => {
                self.output = Some(event);
                self.state = LifecycleState::Complete;
            }
            _ => {
                self.state = LifecycleState::Failed;
                self.error = Some(ComputeLifecycleError::UnexpectedEvent);
            }
        }
        Effects::new()
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            LifecycleState::Complete | LifecycleState::Failed
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.output.ok_or(ComputeLifecycleError::Incomplete)
    }

    fn abort(&mut self) -> Effects {
        self.state = LifecycleState::Failed;
        self.error = Some(ComputeLifecycleError::Aborted);
        Effects::new()
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::{AttemptRef, ComputeEffect, ExecutorKind};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;

    use super::*;

    #[test]
    fn emits_compute_effect() {
        let effect = ComputeEffect::Status {
            backend: ExecutorKind::Docker,
            attempt: AttemptRef::new("job", 1),
        };
        let mut operation = ComputeLifecycle::new(effect);
        assert!(matches!(operation.start().as_slice(), [Effect::Compute(_)]));
    }

    #[test]
    fn rejects_wrong_event() {
        let effect = ComputeEffect::Status {
            backend: ExecutorKind::Docker,
            attempt: AttemptRef::new("job", 1),
        };
        let mut operation = ComputeLifecycle::new(effect);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: ulid::Ulid::from(1_u128),
        }));
        assert_eq!(
            operation.finalize(),
            Err(ComputeLifecycleError::UnexpectedEvent)
        );
    }
}
