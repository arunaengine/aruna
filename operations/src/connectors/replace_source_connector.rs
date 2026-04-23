use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{SourceConnector, SourceConnectorKind, SourceConnectorSecret};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, delete_connector_secret_effect, parse_connector_read, read_connector_effect,
    source_connector_key, source_connector_secret_key,
};
use crate::connectors::validation::{ValidationError, validate_connector_input};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceSourceConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceSourceConnectorResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaceSourceConnectorState {
    Init,
    ReadCurrent,
    WriteRecords,
    DeleteSecret,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplaceSourceConnectorError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("Connector not found")]
    NotFound,
    #[error("ReplaceSourceConnector failed")]
    ReplaceSourceConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ReplaceSourceConnectorState,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for ReplaceSourceConnectorError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ReplaceSourceConnectorOperation {
    input: ReplaceSourceConnectorInput,
    state: ReplaceSourceConnectorState,
    replacement: Option<SourceConnector>,
    replacement_secret: Option<SourceConnectorSecret>,
    output: Option<Result<ReplaceSourceConnectorResult, ReplaceSourceConnectorError>>,
}

impl ReplaceSourceConnectorOperation {
    pub fn new(input: ReplaceSourceConnectorInput) -> Self {
        Self {
            input,
            state: ReplaceSourceConnectorState::Init,
            replacement: None,
            replacement_secret: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ReplaceSourceConnectorError) -> Effects {
        self.state = ReplaceSourceConnectorState::Error;
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

        self.state = ReplaceSourceConnectorState::ReadCurrent;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_current_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(existing)) => {
                let now = SystemTime::now();
                let replacement = SourceConnector::new(
                    existing.connector_id,
                    existing.group_id,
                    self.input.name.clone(),
                    self.input.kind,
                    self.input.public_config.clone(),
                    existing.created_at,
                    now,
                    existing.created_by,
                );
                let replacement_secret = SourceConnectorSecret::new(
                    existing.connector_id,
                    self.input.secret_config.clone(),
                    now,
                );

                self.replacement = Some(replacement.clone());
                self.replacement_secret = replacement_secret;
                self.state = ReplaceSourceConnectorState::WriteRecords;

                if let Some(secret) = self.replacement_secret.as_ref() {
                    let connector_bytes = match replacement.to_bytes() {
                        Ok(bytes) => bytes,
                        Err(error) => return self.emit_error(error.into()),
                    };
                    let secret_bytes = match secret.to_bytes() {
                        Ok(bytes) => bytes,
                        Err(error) => return self.emit_error(error.into()),
                    };
                    smallvec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: vec![
                            (
                                aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                                source_connector_key(
                                    replacement.group_id,
                                    replacement.connector_id
                                ),
                                connector_bytes.into(),
                            ),
                            (
                                aruna_core::keyspaces::SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                                source_connector_secret_key(secret.connector_id),
                                secret_bytes.into(),
                            ),
                        ],
                        txn_id: None,
                    })]
                } else {
                    let connector_bytes = match replacement.to_bytes() {
                        Ok(bytes) => bytes,
                        Err(error) => return self.emit_error(error.into()),
                    };
                    smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE
                            .to_string(),
                        key: source_connector_key(replacement.group_id, replacement.connector_id),
                        value: connector_bytes.into(),
                        txn_id: None,
                    })]
                }
            }
            Ok(None) => self.emit_error(ReplaceSourceConnectorError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_records_written(&mut self, event: Event) -> Effects {
        match (&self.replacement_secret, event) {
            (Some(_), Event::Storage(StorageEvent::BatchWriteResult { .. })) => self.finish(),
            (None, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.state = ReplaceSourceConnectorState::DeleteSecret;
                smallvec![delete_connector_secret_effect(
                    self.input.connector_id,
                    None
                )]
            }
            (_, received) => self.emit_error(ReplaceSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult | WriteResult)",
                received,
            }),
        }
    }

    fn handle_secret_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(ReplaceSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::DeleteResult)",
                received: event,
            });
        };

        self.finish()
    }

    fn finish(&mut self) -> Effects {
        let Some(connector) = self.replacement.clone() else {
            return self.emit_error(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed);
        };

        self.state = ReplaceSourceConnectorState::Finish;
        self.output = Some(Ok(ReplaceSourceConnectorResult {
            connector,
            has_secret_config: self.replacement_secret.is_some(),
        }));
        smallvec![]
    }
}

impl Operation for ReplaceSourceConnectorOperation {
    type Output = ReplaceSourceConnectorResult;
    type Error = ReplaceSourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplaceSourceConnectorState::Init => self.handle_init(),
            ReplaceSourceConnectorState::ReadCurrent => self.handle_current_read(event),
            ReplaceSourceConnectorState::WriteRecords => self.handle_records_written(event),
            ReplaceSourceConnectorState::DeleteSecret => self.handle_secret_deleted(event),
            ReplaceSourceConnectorState::Finish => smallvec![],
            ReplaceSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplaceSourceConnectorState::Finish | ReplaceSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReplaceSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed);
        }

        self.output
            .ok_or(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_source_connector::{
        CreateSourceConnectorInput, CreateSourceConnectorOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn replace_source_connector_can_remove_secret_config() {
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

        let created = drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id: ulid::Ulid::new(),
                created_by: ulid::Ulid::new(),
                name: "old".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::from([("token".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        let replaced = drive(
            ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
                group_id: created.connector.group_id,
                connector_id: created.connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org/v2".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(replaced.connector.name, "new");
        assert!(!replaced.has_secret_config);
    }
}
