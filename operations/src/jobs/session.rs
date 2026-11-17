//! Asks the live session of a job on this node one request and returns its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::mem;

use aruna_core::compute::session::{SessionAnswer, SessionEffect, SessionRequest};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct SessionOperation {
    state: State,
}

#[derive(Debug, PartialEq)]
enum State {
    Ready(SessionEffect),
    Waiting,
    Answered(Box<SessionAnswer>),
    Failed(SessionCallError),
}

#[derive(Debug, Error, PartialEq)]
pub enum SessionCallError {
    #[error("the session request got an unexpected event")]
    Unexpected,
    #[error("the session request did not finish")]
    Unfinished,
}

impl SessionOperation {
    /// `job_id` is the physical job id the session registry knows the session by.
    pub fn new(job_id: String, request: SessionRequest) -> Self {
        Self {
            state: State::Ready(SessionEffect { job_id, request }),
        }
    }
}

impl Operation for SessionOperation {
    type Output = SessionAnswer;
    type Error = SessionCallError;

    fn start(&mut self) -> Effects {
        match mem::replace(&mut self.state, State::Waiting) {
            State::Ready(effect) => smallvec![Effect::Session(Box::new(effect))],
            other => {
                self.state = other;
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        self.state = match (&self.state, event) {
            (State::Waiting, Event::Session(answer)) => State::Answered(answer),
            _ => State::Failed(SessionCallError::Unexpected),
        };
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Answered(_) | State::Failed(_))
    }

    fn finalize(self) -> Result<SessionAnswer, SessionCallError> {
        match self.state {
            State::Answered(answer) => Ok(*answer),
            State::Failed(error) => Err(error),
            State::Ready(_) | State::Waiting => Err(SessionCallError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{rejects_events, sample_events};

    fn touch() -> SessionOperation {
        SessionOperation::new("job".to_string(), SessionRequest::Touch)
    }

    #[test]
    fn answers_one_request() {
        let mut operation = touch();
        let effect = SessionEffect {
            job_id: "job".to_string(),
            request: SessionRequest::Touch,
        };
        assert_eq!(
            operation.start().as_slice(),
            [Effect::Session(Box::new(effect))]
        );
        assert!(!operation.is_complete());
        let answer = Box::new(SessionAnswer::Missing);
        assert!(operation.step(Event::Session(answer)).is_empty());
        assert!(operation.is_complete());
        assert!(operation.abort().is_empty());
        assert_eq!(operation.finalize(), Ok(SessionAnswer::Missing));
    }

    #[test]
    fn rejects_other_events() {
        let make = || {
            let mut operation = touch();
            operation.start();
            operation
        };
        let refusals = rejects_events(make, sample_events, |_| false);
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label}");
            assert_eq!(error, SessionCallError::Unexpected, "{label}");
        }
    }

    #[test]
    fn rejects_early_finalize() {
        assert_eq!(touch().finalize(), Err(SessionCallError::Unfinished));
        let mut started = touch();
        started.start();
        assert_eq!(started.finalize(), Err(SessionCallError::Unfinished));
    }

    #[test]
    fn refuses_early_answer() {
        let mut operation = touch();
        operation.step(Event::Session(Box::new(SessionAnswer::Done)));
        assert_eq!(operation.finalize(), Err(SessionCallError::Unexpected));
    }
}
