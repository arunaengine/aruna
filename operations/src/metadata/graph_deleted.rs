//! Reads whether a metadata graph carries a delete tombstone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{GraphLifecycleRecord, MetadataError};
use aruna_core::operation::Operation;
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

impl Operation for GraphDeletedOperation {
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
