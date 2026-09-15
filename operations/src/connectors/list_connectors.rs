use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::SourceConnector;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::connectors::repository::{
    StorageReadError, iter_connectors_effect, parse_connector_iter,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSourceInput {
    pub group_id: GroupId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSourceResult {
    pub connectors: Vec<SourceConnector>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListSourceState {
    Init,
    IterConnectors,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("ListSourceConnectors failed")]
    ListSourceConnectorsFailed,
}

impl From<StorageReadError> for ListSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ListSourceOperation {
    input: ListSourceInput,
    state: ListSourceState,
    next_start_after: Option<Key>,
    connectors: Vec<SourceConnector>,
    output: Option<Result<ListSourceResult, ListSourceError>>,
}

impl ListSourceOperation {
    pub fn new(input: ListSourceInput) -> Self {
        Self {
            input,
            state: ListSourceState::Init,
            next_start_after: None,
            connectors: Vec::new(),
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListSourceError) -> Effects {
        self.state = ListSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListSourceState::IterConnectors;
        smallvec![iter_connectors_effect(self.input.group_id, None, None)]
    }

    fn handle_connector_iter(&mut self, event: Event) -> Effects {
        match parse_connector_iter(event) {
            Ok((connectors, next_start_after)) => {
                self.connectors.extend(connectors);
                self.next_start_after = next_start_after;

                if let Some(start_after) = self.next_start_after.clone() {
                    return smallvec![iter_connectors_effect(
                        self.input.group_id,
                        Some(start_after),
                        None,
                    )];
                }

                self.state = ListSourceState::Finish;
                self.output = Some(Ok(ListSourceResult {
                    connectors: self.connectors.clone(),
                }));
                smallvec![]
            }
            Err(error) => self.emit_error(error.into()),
        }
    }
}

impl Operation for ListSourceOperation {
    type Output = ListSourceResult;
    type Error = ListSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListSourceState::Init => self.handle_init(),
            ListSourceState::IterConnectors => self.handle_connector_iter(event),
            ListSourceState::Finish => smallvec![],
            ListSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListSourceState::Finish | ListSourceState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListSourceError::ListSourceConnectorsFailed);
        }

        self.output
            .ok_or(ListSourceError::ListSourceConnectorsFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::SourceConnectorKind;
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
            task_handle: None,
            compute_handle: None,
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
