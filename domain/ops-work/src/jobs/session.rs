//! Asks the live session of a job on this node one request and returns its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::session::{SessionAnswer, SessionEffect, SessionRequest};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;

#[derive(Debug, PartialEq)]
pub struct SessionOperation {
    effect: Option<SessionEffect>,
}

impl SessionOperation {
    /// `job_id` is the physical job id the session registry knows the session by.
    pub fn new(job_id: String, request: SessionRequest) -> Self {
        Self {
            effect: Some(SessionEffect { job_id, request }),
        }
    }
}

impl Operation for SessionOperation {
    type Output = SessionAnswer;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<SessionAnswer>, IoError> {
        let effect = self.effect.take().ok_or(IoError::Unexpected {
            expected: "an unsent session request",
            got: "a sent one",
        })?;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Session(
            Box::new(effect),
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SessionAnswer>, IoError> {
        match result {
            IoResult::Event(event) => match *event {
                Event::Session(answer) => Ok(Next::Done(*answer)),
                other => Err(IoError::Unexpected {
                    expected: "Event::Session",
                    got: other.label(),
                }),
            },
            other => Err(IoError::Unexpected {
                expected: "Event::Session",
                got: other.label(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let call = IoCall::Effect(Box::new(Effect::Session(Box::new(effect))));
        assert_eq!(operation.first(), Ok(Next::Call(call)));
        let answer = Event::Session(Box::new(SessionAnswer::Missing));
        assert_eq!(
            operation.next(IoResult::Event(Box::new(answer))),
            Ok(Next::Done(SessionAnswer::Missing))
        );
    }

    #[test]
    fn rejects_other_results() {
        let mut operation = touch();
        operation.first().expect("session call");
        assert!(operation.next(IoResult::Woke).is_err());
        assert!(
            operation
                .next(IoResult::Event(Box::new(Event::Delayed)))
                .is_err()
        );
    }

    #[test]
    fn sends_once() {
        let mut operation = touch();
        operation.first().expect("session call");
        assert!(operation.first().is_err());
    }
}
