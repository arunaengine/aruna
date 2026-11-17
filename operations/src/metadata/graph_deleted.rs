//! Reads whether a metadata graph carries a delete tombstone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{GraphLifecycleRecord, MetadataError};
use aruna_core::operation::EffectOperation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::metadata::repository::read_lifecycle_effect;

#[derive(Debug, Error, PartialEq)]
pub enum GraphDeletedError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the graph lifecycle read did not finish")]
    Unfinished,
}

impl GraphDeletedError {
    /// Folds the error into a caller's storage and conversion variants.
    pub fn into_error<E: From<StorageError> + From<ConversionError>>(self) -> E {
        match self {
            Self::Storage(error) => error.into(),
            Self::Conversion(error) => error.into(),
            other => StorageError::ReadError(other.to_string()).into(),
        }
    }
}

impl From<GraphDeletedError> for MetadataError {
    fn from(error: GraphDeletedError) -> Self {
        match error {
            GraphDeletedError::Storage(error) => Self::Storage(error),
            other => Self::Backend(other.to_string()),
        }
    }
}

#[derive(Debug, PartialEq)]
enum DeletedState {
    Init,
    Reading,
    Done,
}

/// Reads the lifecycle row of `graph_iri`; a graph without one is not deleted.
#[derive(Debug, PartialEq)]
pub struct GraphDeletedOperation {
    graph_iri: String,
    state: DeletedState,
    output: Option<Result<bool, GraphDeletedError>>,
}

impl GraphDeletedOperation {
    pub fn new(graph_iri: &str) -> Self {
        Self {
            graph_iri: graph_iri.to_string(),
            state: DeletedState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<bool, GraphDeletedError>) -> Effects {
        self.state = DeletedState::Done;
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for GraphDeletedOperation {
    type Output = bool;
    type Error = GraphDeletedError;

    fn start(&mut self) -> Effects {
        self.state = DeletedState::Reading;
        smallvec![read_lifecycle_effect(&self.graph_iri, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = self.state == DeletedState::Reading;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) if reading => {
                let deleted = value
                    .map(|bytes| postcard::from_bytes::<GraphLifecycleRecord>(&bytes))
                    .transpose()
                    .map(|record| record.is_some_and(|record| record.is_deleted()))
                    .map_err(|error| ConversionError::from(error).into());
                self.finish(deleted)
            }
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            event => {
                let error = GraphDeletedError::UnexpectedEvent {
                    state: if reading {
                        "reading the lifecycle"
                    } else {
                        "idle"
                    },
                    got: event.label(),
                };
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == DeletedState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GraphDeletedError::Unfinished))
    }

    // The read runs outside a transaction, so nothing is left to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    const GRAPH: &str = "urn:graph:one";

    fn tombstone() -> Vec<u8> {
        let record = GraphLifecycleRecord::deleted(
            GRAPH.to_string(),
            RealmId([1; 32]),
            Ulid::from_bytes([2; 16]),
            Ulid::from_bytes([3; 16]),
            2,
        );
        postcard::to_allocvec(&record).unwrap()
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: value.map(Into::into),
        })
    }

    fn reading() -> GraphDeletedOperation {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        operation.start();
        operation
    }

    #[test]
    fn tombstone_reads_deleted() {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        check_trace(
            &mut operation,
            &[read_lifecycle_effect(GRAPH, None)],
            vec![(read(Some(tombstone())), vec![])],
        );
        assert_eq!(operation.finalize(), Ok(true));
    }

    #[test]
    fn missing_row_live() {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        check_trace(
            &mut operation,
            &[read_lifecycle_effect(GRAPH, None)],
            vec![(read(None), vec![])],
        );
        assert_eq!(operation.finalize(), Ok(false));
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = reading();
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(operation.step(error).is_empty());
        let error = operation.finalize().unwrap_err();
        assert_eq!(error, GraphDeletedError::Storage(StorageError::Timeout));
        assert!(matches!(
            MetadataError::from(error),
            MetadataError::Storage(StorageError::Timeout)
        ));
    }

    #[test]
    fn broken_row_fails() {
        // A row that does not decode is an error, never a live graph.
        let mut operation = reading();
        operation.step(read(Some(vec![0xff])));
        let error = operation.finalize().unwrap_err();
        assert!(matches!(error, GraphDeletedError::Conversion(_)));
        let folded: StorageOrConversion = error.into_error();
        assert!(matches!(folded, StorageOrConversion::Conversion(_)));
    }

    #[derive(Debug)]
    enum StorageOrConversion {
        Storage(#[allow(dead_code)] StorageError),
        Conversion(#[allow(dead_code)] ConversionError),
    }

    impl From<StorageError> for StorageOrConversion {
        fn from(error: StorageError) -> Self {
            Self::Storage(error)
        }
    }

    impl From<ConversionError> for StorageOrConversion {
        fn from(error: ConversionError) -> Self {
            Self::Conversion(error)
        }
    }

    #[test]
    fn rejects_other_events() {
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in rejects_events(reading, sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(error, GraphDeletedError::UnexpectedEvent { .. }));
            // Unexpected events fold into a storage read error for queue callers.
            let folded: StorageOrConversion = error.into_error();
            assert!(matches!(folded, StorageOrConversion::Storage(_)));
        }
        let mut idle = GraphDeletedOperation::new(GRAPH);
        idle.step(read(None));
        assert!(matches!(
            idle.finalize(),
            Err(GraphDeletedError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(GraphDeletedOperation::new(GRAPH));
        rejects_finalize(reading());
    }
}
