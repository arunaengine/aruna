//! Reads this node's durable compute drain state: the operator's flag and the departure report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::quota::ComputeDepartureReport;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

/// The single durable departure-report row of this node.
pub(crate) const DEPARTURE_KEY: &[u8] = b"departure";
/// The operator's own compute drain, kept apart from an observed departure so
/// returning to placement can never silently undrain a node an operator drained.
pub(crate) const OPERATOR_DRAIN_KEY: &[u8] = b"operator_drain";

#[derive(Debug, Error, PartialEq)]
pub enum DrainStateError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the departure report is unreadable: {0}")]
    Decode(String),
    #[error("unexpected event for the drain state")]
    UnexpectedEvent,
    #[error("the drain state was not read")]
    Unfinished,
}

/// Whether an operator drains this node's compute plane, and the report of its last departure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainState {
    pub operator_draining: bool,
    pub departure: Option<ComputeDepartureReport>,
}

/// Reads both drain rows in one batch read.
#[derive(Debug, Default, PartialEq)]
pub struct DrainStateOperation {
    started: bool,
    result: Option<Result<DrainState, DrainStateError>>,
}

impl DrainStateOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn decide(&self, event: Event) -> Result<DrainState, DrainStateError> {
        if !self.started {
            return Err(DrainStateError::UnexpectedEvent);
        }
        let values = match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            _ => return Err(DrainStateError::UnexpectedEvent),
        };
        let [(_, drain), (_, departure)] = values.as_slice() else {
            return Err(DrainStateError::UnexpectedEvent);
        };
        let departure = departure
            .as_ref()
            .map(|bytes| postcard::from_bytes::<ComputeDepartureReport>(bytes.as_ref()))
            .transpose()
            .map_err(|error| DrainStateError::Decode(error.to_string()))?;
        Ok(DrainState {
            operator_draining: drain.is_some(),
            departure,
        })
    }
}

impl Operation for DrainStateOperation {
    type Output = DrainState;
    type Error = DrainStateError;

    fn start(&mut self) -> Effects {
        self.started = true;
        let read = |key: &[u8]| (COMPUTE_DEPARTURE_KEYSPACE.to_string(), Key::from(key.to_vec()));
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![read(OPERATOR_DRAIN_KEY), read(DEPARTURE_KEY)],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.result = Some(match self.result {
            Some(_) => Err(DrainStateError::UnexpectedEvent),
            None => self.decide(event),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(DrainStateError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::types::Value;
    use ulid::Ulid;

    use super::*;

    fn batch(drain: Option<Vec<u8>>, departure: Option<Vec<u8>>) -> Event {
        let row = |key: &[u8], value: Option<Vec<u8>>| (Key::from(key.to_vec()), value.map(Value::from));
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![row(OPERATOR_DRAIN_KEY, drain), row(DEPARTURE_KEY, departure)],
        })
    }

    fn report() -> ComputeDepartureReport {
        ComputeDepartureReport {
            departed_at_ms: 5,
            membership_generation: 2,
            unresolved: vec![Ulid::from_parts(1, 1)],
            truncated: false,
        }
    }

    fn read_effect() -> Effect {
        let read = |key: &[u8]| (COMPUTE_DEPARTURE_KEYSPACE.to_string(), Key::from(key.to_vec()));
        Effect::Storage(StorageEffect::BatchRead {
            reads: vec![read(OPERATOR_DRAIN_KEY), read(DEPARTURE_KEY)],
            txn_id: None,
        })
    }

    #[test]
    fn reads_both_rows() {
        let encoded = postcard::to_allocvec(&report()).unwrap();
        let cases = [
            (batch(None, None), DrainState::default()),
            (
                batch(Some(vec![1]), Some(encoded)),
                DrainState {
                    operator_draining: true,
                    departure: Some(report()),
                },
            ),
        ];
        for (event, expected) in cases {
            let mut operation = DrainStateOperation::new();
            check_trace(&mut operation, &[read_effect()], vec![(event, vec![])]);
            assert!(operation.is_complete());
            assert_eq!(operation.finalize(), Ok(expected));
        }
    }

    #[test]
    fn failures_end_reads() {
        let mut failed = DrainStateOperation::new();
        failed.start();
        failed.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(failed.finalize(), Err(StorageError::Timeout.into()));

        let mut garbled = DrainStateOperation::new();
        garbled.start();
        garbled.step(batch(None, Some(vec![0xff; 3])));
        assert!(matches!(garbled.finalize(), Err(DrainStateError::Decode(_))));

        let mut short = DrainStateOperation::new();
        short.start();
        short.step(Event::Storage(StorageEvent::BatchReadResult { values: Vec::new() }));
        assert_eq!(short.finalize(), Err(DrainStateError::UnexpectedEvent));
    }

    #[test]
    fn rejects_stray_events() {
        let started = || {
            let mut operation = DrainStateOperation::new();
            operation.start();
            operation
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::Error { .. })
            )
        };
        for (label, effects, error) in rejects_events(started, sample_events, expected) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert_eq!(error, DrainStateError::UnexpectedEvent, "{label}");
        }
        // Before start and after the answer, every event is refused.
        let refused = rejects_events(DrainStateOperation::new, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        let mut answered = started();
        answered.step(batch(None, None));
        answered.step(batch(None, None));
        assert_eq!(answered.finalize(), Err(DrainStateError::UnexpectedEvent));
    }

    #[test]
    fn abort_and_finalize() {
        let mut operation = DrainStateOperation::new();
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }
}
