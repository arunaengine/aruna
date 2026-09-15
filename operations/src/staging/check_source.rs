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
pub enum CheckSourceState {
    Init,
    Check,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CheckSourceError {
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CheckSourceState,
        expected: &'static str,
        got: String,
    },
    #[error("Check staging source failed")]
    CheckFailed,
}

#[derive(Debug, PartialEq)]
pub struct CheckSourceOperation {
    access: ResolvedSourceAccess,
    state: CheckSourceState,
    output: Option<Result<(), CheckSourceError>>,
}

impl CheckSourceOperation {
    pub fn new(access: ResolvedSourceAccess) -> Self {
        Self {
            access,
            state: CheckSourceState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CheckSourceError) -> Effects {
        self.state = CheckSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for CheckSourceOperation {
    type Output = ();
    type Error = CheckSourceError;

    fn start(&mut self) -> Effects {
        self.state = CheckSourceState::Check;
        smallvec![Effect::StagingSource(StagingSourceEffect::Check {
            access: self.access.clone(),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (CheckSourceState::Check, Event::StagingSource(StagingSourceEvent::CheckResult)) => {
                self.state = CheckSourceState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            (
                CheckSourceState::Check,
                Event::StagingSource(StagingSourceEvent::Error { error }),
            ) => self.emit_error(error.into()),
            (CheckSourceState::Finish, _) => smallvec![],
            (CheckSourceState::Error, _) => self.abort(),
            (_, event) => self.emit_error(CheckSourceError::UnexpectedEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::CheckResult)",
                got: describe_event(&event),
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CheckSourceState::Finish | CheckSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CheckSourceError::CheckFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
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
        let mut operation = CheckSourceOperation::new(sample_access());

        let effects = operation.start();

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Check { .. })]
        ));
    }

    #[test]
    fn check_finishes() {
        let mut operation = CheckSourceOperation::new(sample_access());
        operation.start();

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::CheckResult));

        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn check_rejects_event() {
        let mut operation = CheckSourceOperation::new(sample_access());
        operation.start();

        let effects = operation.step(Event::Search());

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(CheckSourceError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn check_exposes_error() {
        let mut operation = CheckSourceOperation::new(sample_access());
        operation.start();

        operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::CheckError("unreachable".to_string()),
        }));

        assert_eq!(
            operation.finalize(),
            Err(CheckSourceError::Staging(StagingSourceError::CheckError(
                "unreachable".to_string()
            )))
        );
    }
}
