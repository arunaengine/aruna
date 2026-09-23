//! Creates a source connector record together with its validated secret config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_connector::{
    SourceConnector, SourceConnectorKind, SourceConnectorSecret,
};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;

use crate::connectors::repository::{connector_secret_key, source_connector_key};
use crate::connectors::validation::{ValidationError, validate_connector_input};

#[derive(Clone, PartialEq, Eq)]
pub struct SourceConnectorInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for SourceConnectorInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceConnectorInput")
            .field("group_id", &self.group_id)
            .field("created_by", &self.created_by)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("public_config", &self.public_config)
            .field(
                "secret_keys",
                &self.secret_config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceConnectorResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceConnectorState {
    Init,
    WriteRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SourceConnectorError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("CreateSourceConnector failed")]
    CreateConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: SourceConnectorState,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct SourceConnectorOperation {
    input: SourceConnectorInput,
    state: SourceConnectorState,
    connector: Option<SourceConnector>,
    secret: Option<SourceConnectorSecret>,
    output: Option<Result<SourceConnectorResult, SourceConnectorError>>,
}

impl SourceConnectorOperation {
    pub fn new(input: SourceConnectorInput) -> Self {
        Self {
            input,
            state: SourceConnectorState::Init,
            connector: None,
            secret: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: SourceConnectorError) -> Effects {
        self.state = SourceConnectorState::Error;
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
        let connector_id = ulid::Ulid::generate();
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
            aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE.to_string(),
            source_connector_key(connector.group_id, connector.connector_id),
            connector_bytes.into(),
        )];
        if let Some(secret) = secret.as_ref() {
            let secret_bytes = match secret.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => return self.emit_error(error.into()),
            };
            writes.push((
                aruna_core::keyspaces::SOURCE_SECRET_KEYSPACE.to_string(),
                connector_secret_key(secret.connector_id),
                secret_bytes.into(),
            ));
        }

        self.connector = Some(connector);
        self.secret = secret;
        self.state = SourceConnectorState::WriteRecords;
        smallvec![crate::groups::fence::write_group_records(
            self.input.group_id,
            writes
        )]
    }

    fn handle_records_written(&mut self, event: Event) -> Effects {
        if let Err(error) = crate::groups::fence::group_write_result(event) {
            return self.emit_error(error.into());
        }

        let Some(connector) = self.connector.clone() else {
            return self.emit_error(SourceConnectorError::CreateConnectorFailed);
        };

        self.state = SourceConnectorState::Finish;
        self.output = Some(Ok(SourceConnectorResult {
            connector,
            has_secret_config: self.secret.is_some(),
        }));
        smallvec![]
    }
}

impl Operation for SourceConnectorOperation {
    type Output = SourceConnectorResult;
    type Error = SourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            SourceConnectorState::Init => self.handle_init(),
            SourceConnectorState::WriteRecords => self.handle_records_written(event),
            SourceConnectorState::Finish => smallvec![],
            SourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SourceConnectorState::Finish | SourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == SourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(SourceConnectorError::CreateConnectorFailed);
        }

        self.output
            .ok_or(SourceConnectorError::CreateConnectorFailed)?
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
    async fn persists_connector_secret() {
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

        let result = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "refdata".to_string(),
                kind: SourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("access_key_id".to_string(), "AKIA".to_string()),
                    ("secret_access_key".to_string(), "secret".to_string()),
                ]),
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
