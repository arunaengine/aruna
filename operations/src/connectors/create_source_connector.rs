use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{SourceConnector, SourceConnectorKind, SourceConnectorSecret};
use aruna_core::types::{Effects, GroupId, UserId};
use smallvec::smallvec;
use thiserror::Error;

use crate::connectors::repository::{source_connector_key, source_connector_secret_key};
use crate::connectors::validation::{ValidationError, validate_connector_input};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSourceConnectorInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSourceConnectorResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateSourceConnectorState {
    Init,
    WriteRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateSourceConnectorError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("CreateSourceConnector failed")]
    CreateSourceConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateSourceConnectorState,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct CreateSourceConnectorOperation {
    input: CreateSourceConnectorInput,
    state: CreateSourceConnectorState,
    connector: Option<SourceConnector>,
    secret: Option<SourceConnectorSecret>,
    output: Option<Result<CreateSourceConnectorResult, CreateSourceConnectorError>>,
}

impl CreateSourceConnectorOperation {
    pub fn new(input: CreateSourceConnectorInput) -> Self {
        Self {
            input,
            state: CreateSourceConnectorState::Init,
            connector: None,
            secret: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateSourceConnectorError) -> Effects {
        self.state = CreateSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if let Err(error) = validate_connector_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        ) {
            return self.emit_error(error.into());
        }

        let now = SystemTime::now();
        let connector_id = ulid::Ulid::new();
        let connector = SourceConnector::new(
            connector_id,
            self.input.group_id,
            self.input.name.clone(),
            self.input.kind,
            self.input.public_config.clone(),
            now,
            now,
            self.input.created_by,
        );
        let secret =
            SourceConnectorSecret::new(connector_id, self.input.secret_config.clone(), now);

        let connector_bytes = match connector.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.emit_error(error.into()),
        };
        let mut writes = vec![(
            aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
            source_connector_key(connector.group_id, connector.connector_id),
            connector_bytes.into(),
        )];
        if let Some(secret) = secret.as_ref() {
            let secret_bytes = match secret.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => return self.emit_error(error.into()),
            };
            writes.push((
                aruna_core::keyspaces::SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                source_connector_secret_key(secret.connector_id),
                secret_bytes.into(),
            ));
        }

        self.connector = Some(connector);
        self.secret = secret;
        self.state = CreateSourceConnectorState::WriteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })]
    }

    fn handle_records_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.emit_error(CreateSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };

        let Some(connector) = self.connector.clone() else {
            return self.emit_error(CreateSourceConnectorError::CreateSourceConnectorFailed);
        };

        self.state = CreateSourceConnectorState::Finish;
        self.output = Some(Ok(CreateSourceConnectorResult {
            connector,
            has_secret_config: self.secret.is_some(),
        }));
        smallvec![]
    }
}

impl Operation for CreateSourceConnectorOperation {
    type Output = CreateSourceConnectorResult;
    type Error = CreateSourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateSourceConnectorState::Init => self.handle_init(),
            CreateSourceConnectorState::WriteRecords => self.handle_records_written(event),
            CreateSourceConnectorState::Finish => smallvec![],
            CreateSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateSourceConnectorState::Finish | CreateSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == CreateSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CreateSourceConnectorError::CreateSourceConnectorFailed);
        }

        self.output
            .ok_or(CreateSourceConnectorError::CreateSourceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::{parse_connector_read, read_connector_effect};
    use crate::driver::{DriverContext, drive};
    use aruna_core::handle::Handle;
    use aruna_storage::storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_source_connector_persists_connector_and_secret() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id: ulid::Ulid::new(),
                created_by: ulid::Ulid::new(),
                name: "refdata".to_string(),
                kind: SourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::from([("access_key_id".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        let stored = context
            .storage_handle
            .send_effect(read_connector_effect(
                result.connector.group_id,
                result.connector.connector_id,
                None,
            ))
            .await;

        assert_eq!(
            parse_connector_read(stored).unwrap().unwrap(),
            result.connector
        );
        assert!(result.has_secret_config);
    }
}
