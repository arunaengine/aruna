use crate::staging::describe_event;
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::ResolvedSourceAccess;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckStagingSourceState {
    Init,
    Check,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CheckStagingSourceError {
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CheckStagingSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("Check staging source failed")]
    CheckFailed,
}

#[derive(Debug, PartialEq)]
pub struct CheckStagingSourceOperation {
    access: ResolvedSourceAccess,
    state: CheckStagingSourceState,
    output: Option<Result<(), CheckStagingSourceError>>,
}

impl CheckStagingSourceOperation {
    pub fn new(access: ResolvedSourceAccess) -> Self {
        Self {
            access,
            state: CheckStagingSourceState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CheckStagingSourceError) -> Effects {
        self.state = CheckStagingSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for CheckStagingSourceOperation {
    type Output = ();
    type Error = CheckStagingSourceError;

    fn start(&mut self) -> Effects {
        self.state = CheckStagingSourceState::Check;
        smallvec![Effect::StagingSource(StagingSourceEffect::Check {
            access: self.access.clone(),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                CheckStagingSourceState::Check,
                Event::StagingSource(StagingSourceEvent::CheckResult),
            ) => {
                self.state = CheckStagingSourceState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            (
                CheckStagingSourceState::Check,
                Event::StagingSource(StagingSourceEvent::Error { error }),
            ) => self.emit_error(error.into()),
            (CheckStagingSourceState::Finish, _) => smallvec![],
            (CheckStagingSourceState::Error, _) => self.abort(),
            (_, event) => self.emit_error(CheckStagingSourceError::UnexpectedEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::CheckResult)",
                got: describe_event(&event),
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CheckStagingSourceState::Finish | CheckStagingSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CheckStagingSourceError::CheckFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::SourceConnectorKind;
    use std::collections::HashMap;

    fn sample_access() -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: String::new(),
            version: None,
        }
    }

    #[test]
    fn check_emits_effect() {
        let mut operation = CheckStagingSourceOperation::new(sample_access());

        let effects = operation.start();

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Check { .. })]
        ));
    }

    #[test]
    fn check_finishes() {
        let mut operation = CheckStagingSourceOperation::new(sample_access());
        operation.start();

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::CheckResult));

        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn check_exposes_error() {
        let mut operation = CheckStagingSourceOperation::new(sample_access());
        operation.start();

        operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::CheckError("unreachable".to_string()),
        }));

        assert_eq!(
            operation.finalize(),
            Err(CheckStagingSourceError::Staging(
                StagingSourceError::CheckError("unreachable".to_string())
            ))
        );
    }
}
