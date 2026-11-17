//! Lists the source connectors stored for one group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;

use crate::connectors::repository::{
    LIST_CONNECTOR_SIZE, StorageReadError, source_connector_prefix,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSourceInput {
    pub group_id: GroupId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSourceResult {
    pub connectors: Vec<SourceConnector>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
}

impl From<StorageReadError> for ListSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

impl From<IoError> for ListSourceError {
    fn from(error: IoError) -> Self {
        StorageReadError::from(error).into()
    }
}

#[derive(Debug, PartialEq)]
pub struct ListSourceOperation {
    input: ListSourceInput,
    connectors: Vec<SourceConnector>,
}

impl ListSourceOperation {
    pub fn new(input: ListSourceInput) -> Self {
        Self {
            input,
            connectors: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<ListSourceResult> {
        Next::Call(IoCall::Page {
            space: SOURCE_INDEX_KEYSPACE.to_string(),
            prefix: source_connector_prefix(self.input.group_id),
            after,
            limit: LIST_CONNECTOR_SIZE,
        })
    }
}

impl Operation for ListSourceOperation {
    type Output = ListSourceResult;
    type Error = ListSourceError;

    fn first(&mut self) -> Result<Next<Self::Output>, ListSourceError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ListSourceError> {
        let (values, next) = rows::page(result)?;
        for (_, value) in values {
            self.connectors.push(SourceConnector::from_bytes(&value)?);
        }
        Ok(match next {
            Some(after) => self.page(Some(after)),
            None => Next::Done(ListSourceResult {
                connectors: std::mem::take(&mut self.connectors),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn lists_created_connectors() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let group_id = ulid::Ulid::generate();

        let created = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id,
                created_by: Default::default(),
                name: "refdata".to_string(),
                kind: SourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                    ("skip_signature".to_string(), "true".to_string()),
                ]),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap();

        let listed = drive(
            ListSourceOperation::new(ListSourceInput { group_id }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(listed.connectors.len(), 1);
        assert_eq!(listed.connectors[0], created.connector);
    }
}
