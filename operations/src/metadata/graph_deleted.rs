//! Reads whether a metadata graph carries a delete tombstone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::{GraphLifecycleRecord, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::graph_lifecycle_key;
use thiserror::Error;

use crate::metadata::call_results::read_value;

#[derive(Debug, Error, PartialEq)]
pub enum GraphDeletedError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for GraphDeletedError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
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

/// Reads the lifecycle row of `graph_iri`; a graph without one is not deleted.
#[derive(Debug, PartialEq)]
pub struct GraphDeletedOperation {
    graph_iri: String,
}

impl GraphDeletedOperation {
    pub fn new(graph_iri: &str) -> Self {
        Self {
            graph_iri: graph_iri.to_string(),
        }
    }
}

impl Operation for GraphDeletedOperation {
    type Output = bool;
    type Error = GraphDeletedError;

    fn first(&mut self) -> Result<Next<bool>, GraphDeletedError> {
        let key = graph_lifecycle_key(&self.graph_iri);
        let read = vec![(GRAPH_LIFECYCLE_KEYSPACE.to_string(), key)];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, GraphDeletedError> {
        let deleted = read_value(result)?
            .map(|bytes| postcard::from_bytes::<GraphLifecycleRecord>(&bytes))
            .transpose()
            .map_err(ConversionError::from)?
            .is_some_and(|record| record.is_deleted());
        Ok(Next::Done(deleted))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
    use ulid::Ulid;

    const GRAPH: &str = "urn:graph:one";

    fn tombstone() -> Value {
        let record = GraphLifecycleRecord::deleted(
            GRAPH.to_string(),
            RealmId([1; 32]),
            Ulid::from_bytes([2; 16]),
            Ulid::from_bytes([3; 16]),
            2,
        );
        postcard::to_allocvec(&record).unwrap().into()
    }

    fn read_call() -> Next<bool> {
        let read = vec![(
            GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            graph_lifecycle_key(GRAPH),
        )];
        Next::Call(IoCall::Read(read))
    }

    #[test]
    fn tombstone_reads_deleted() {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        assert_eq!(operation.first(), Ok(read_call()));
        let answer = IoResult::Values(vec![Some(tombstone())]);
        assert_eq!(operation.next(answer), Ok(Next::Done(true)));
    }

    #[test]
    fn missing_row_live() {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn broken_row_fails() {
        // A row that does not decode is an error, never a live graph.
        let mut operation = GraphDeletedOperation::new(GRAPH);
        operation.first().unwrap();
        let error = operation
            .next(IoResult::Values(vec![Some(vec![0xff].into())]))
            .unwrap_err();
        assert!(matches!(error, GraphDeletedError::Conversion(_)));
        let folded: StorageOrConversion = error.into_error();
        assert!(matches!(folded, StorageOrConversion::Conversion(_)));
    }

    #[test]
    fn wrong_result_fails() {
        let mut operation = GraphDeletedOperation::new(GRAPH);
        operation.first().unwrap();
        let error = operation.next(IoResult::Done).unwrap_err();
        assert!(matches!(
            error,
            GraphDeletedError::Io(IoError::Unexpected { .. })
        ));
        // Errors outside storage fold into a storage read error for queue callers.
        let folded: StorageOrConversion = error.into_error();
        assert!(matches!(folded, StorageOrConversion::Storage(_)));
    }

    #[test]
    fn runner_errors_map() {
        let timeout = GraphDeletedError::from(IoError::Storage(StorageError::Timeout));
        assert_eq!(timeout, GraphDeletedError::Storage(StorageError::Timeout));
        assert!(matches!(
            MetadataError::from(timeout),
            MetadataError::Storage(StorageError::Timeout)
        ));
        assert_eq!(
            GraphDeletedError::from(IoError::Conflict),
            GraphDeletedError::Storage(StorageError::TransactionConflict)
        );
        assert_eq!(
            GraphDeletedError::from(IoError::Aborted),
            GraphDeletedError::Io(IoError::Aborted)
        );
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
}
