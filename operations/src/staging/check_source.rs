//! Runs the operation that checks whether a resolved staging source is reachable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum CheckSourceError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Staging(#[from] StagingSourceError),
    #[error("unexpected staging answer {0}")]
    UnexpectedEvent(&'static str),
}

#[derive(Debug, PartialEq)]
pub struct CheckSourceOperation {
    access: ResolvedSourceAccess,
}

impl CheckSourceOperation {
    pub fn new(access: ResolvedSourceAccess) -> Self {
        Self { access }
    }
}

impl Operation for CheckSourceOperation {
    type Output = ();
    type Error = CheckSourceError;

    fn first(&mut self) -> Result<Next<()>, CheckSourceError> {
        let check = StagingSourceEffect::Check {
            access: self.access.clone(),
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::StagingSource(
            check,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CheckSourceError> {
        match result.event()? {
            Event::StagingSource(StagingSourceEvent::CheckResult) => Ok(Next::Done(())),
            Event::StagingSource(StagingSourceEvent::Error { error }) => Err(error.into()),
            other => Err(CheckSourceError::UnexpectedEvent(other.label())),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;

    fn sample_access() -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: String::new(),
            version: None,
        }
    }

    fn answer(event: StagingSourceEvent) -> IoResult {
        IoResult::Event(Box::new(Event::StagingSource(event)))
    }

    #[test]
    fn check_finishes() {
        let mut operation = CheckSourceOperation::new(sample_access());
        let check = StagingSourceEffect::Check {
            access: sample_access(),
        };
        let call = IoCall::Effect(Box::new(Effect::StagingSource(check)));
        assert_eq!(operation.first(), Ok(Next::Call(call)));
        let checked = answer(StagingSourceEvent::CheckResult);
        assert_eq!(operation.next(checked), Ok(Next::Done(())));
    }

    #[test]
    fn check_exposes_error() {
        let mut operation = CheckSourceOperation::new(sample_access());
        operation.first().unwrap();
        let error = || StagingSourceError::CheckError("unreachable".to_string());
        let failed = answer(StagingSourceEvent::Error { error: error() });
        assert_eq!(
            operation.next(failed),
            Err(CheckSourceError::Staging(error()))
        );
    }
}
