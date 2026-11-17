//! Replaces or deletes a repository connector and its secret in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{CONNECTOR_INDEX_KEYSPACE, CONNECTOR_SECRET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::harvest::{
    RepositoryConnector, RepositoryConnectorKind, RepositoryConnectorSecret,
};
use aruna_core::structs::identity::group_delete::{GroupWriteError, check_group_write};
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::groups::fence::group_fence_key;
use crate::harvest::create_connector::{CreateConnectorError, validate_connector};
use crate::harvest::read_connector::ConnectorView;
use crate::harvest::repository::{connector_key, connector_secret_key, connector_writes};

#[derive(Clone, PartialEq, Eq)]
pub struct UpdateConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub name: String,
    pub kind: RepositoryConnectorKind,
    pub endpoint: String,
    pub public_config: HashMap<String, String>,
    /// `None` keeps the stored secret; an empty map removes it.
    pub secret_config: Option<HashMap<String, String>>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for UpdateConnectorInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateConnectorInput")
            .field("group_id", &self.group_id)
            .field("connector_id", &self.connector_id)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("endpoint", &self.endpoint)
            .field("public_config", &self.public_config)
            .field(
                "secret_keys",
                &self
                    .secret_config
                    .as_ref()
                    .map(|config| config.keys().collect::<Vec<_>>()),
            )
            .finish()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateConnectorError {
    #[error(transparent)]
    Invalid(#[from] CreateConnectorError),
    #[error(transparent)]
    GroupWrite(#[from] GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("repository connector not found")]
    NotFound,
    #[error("a changed endpoint needs a new secret or an explicit secret removal")]
    SecretEndpoint,
    #[error("repository connector is used by repository links; remove them first")]
    InUse,
    #[error("a repository connector keeps its kind; register a new connector instead")]
    KindChanged,
    #[error("unexpected event while changing a repository connector")]
    Unexpected,
}

impl From<IoError> for UpdateConnectorError {
    fn from(error: IoError) -> Self {
        error.into_storage().map_or(Self::Unexpected, Self::Storage)
    }
}

/// The call an update waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    /// The write, then the secret row removal it may need.
    Write(Vec<(String, Key)>),
    Delete,
}

/// Replaces the public definition; the id, group, creator and creation time are kept.
#[derive(Debug, PartialEq)]
pub struct UpdateRepositoryOperation {
    input: UpdateConnectorInput,
    stage: Stage,
    view: Option<ConnectorView>,
}

impl UpdateRepositoryOperation {
    pub fn new(input: UpdateConnectorInput) -> Self {
        Self {
            input,
            stage: Stage::Read,
            view: None,
        }
    }

    /// Decides the change from the stored record, the group fence and the stored secret row.
    fn write(
        &mut self,
        [record, fence, secret]: [Option<Value>; 3],
    ) -> Result<Next<ConnectorView>, UpdateConnectorError> {
        check_group_write(fence.as_deref())?;
        let record = record.ok_or(UpdateConnectorError::NotFound)?;
        let existing = RepositoryConnector::from_bytes(&record).map_err(GroupWriteError::from)?;
        let stored = secret.is_some();
        if existing.kind != self.input.kind {
            return Err(UpdateConnectorError::KindChanged);
        }
        if stored && self.input.secret_config.is_none() && existing.endpoint != self.input.endpoint
        {
            return Err(UpdateConnectorError::SecretEndpoint);
        }
        let now = SystemTime::now();
        let connector = RepositoryConnector {
            name: self.input.name.clone(),
            kind: self.input.kind,
            endpoint: self.input.endpoint.clone(),
            public_config: self.input.public_config.clone(),
            updated_at: now,
            ..existing
        };
        let secret =
            self.input.secret_config.clone().and_then(|config| {
                RepositoryConnectorSecret::new(connector.connector_id, config, now)
            });
        let has_secret_config = match &self.input.secret_config {
            None => stored,
            Some(_) => secret.is_some(),
        };
        let mut deletes = Vec::new();
        if stored && !has_secret_config {
            deletes.push((
                CONNECTOR_SECRET_KEYSPACE.to_string(),
                connector_secret_key(connector.connector_id),
            ));
        }
        let writes =
            connector_writes(&connector, secret.as_ref()).map_err(GroupWriteError::from)?;
        self.view = Some(ConnectorView {
            connector,
            has_secret_config,
        });
        self.stage = Stage::Write(deletes);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for UpdateRepositoryOperation {
    type Output = ConnectorView;
    type Error = UpdateConnectorError;

    fn first(&mut self) -> Result<Next<Self::Output>, UpdateConnectorError> {
        validate_connector(
            &self.input.name,
            self.input.kind,
            &self.input.endpoint,
            &self.input.public_config,
            self.input.secret_config.as_ref().unwrap_or(&HashMap::new()),
        )?;
        let (group_id, connector_id) = (self.input.group_id, self.input.connector_id);
        Ok(Next::Call(IoCall::Read(vec![
            (
                CONNECTOR_INDEX_KEYSPACE.to_string(),
                connector_key(group_id, connector_id),
            ),
            group_fence_key(group_id),
            (
                CONNECTOR_SECRET_KEYSPACE.to_string(),
                connector_secret_key(connector_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, UpdateConnectorError> {
        match std::mem::replace(&mut self.stage, Stage::Delete) {
            Stage::Read => self.write(rows::values::<3>(result)?),
            Stage::Write(deletes) => {
                rows::done(result)?;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            Stage::Delete => {
                rows::done(result)?;
                self.view
                    .take()
                    .map(Next::Done)
                    .ok_or(UpdateConnectorError::Unexpected)
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::harvest::create_connector::{CreateConnectorInput, CreateConnectorOperation};
    use crate::harvest::read_connector::GetRepositoryOperation;
    use aruna_storage::storage;
    use tempfile::{TempDir, tempdir};

    pub(crate) fn context() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
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
        (dir, context)
    }

    pub(crate) async fn create(context: &DriverContext, group_id: GroupId) -> RepositoryConnector {
        drive(
            CreateConnectorOperation::new(CreateConnectorInput {
                group_id,
                created_by: Default::default(),
                name: "zenodo".into(),
                kind: RepositoryConnectorKind::Invenio,
                endpoint: "https://zenodo.org/api/".into(),
                public_config: HashMap::new(),
                secret_config: HashMap::from([("token".into(), "t".into())]),
            }),
            context,
        )
        .await
        .unwrap()
        .connector
    }

    fn update(connector: &RepositoryConnector, endpoint: &str) -> UpdateConnectorInput {
        UpdateConnectorInput {
            group_id: connector.group_id,
            connector_id: connector.connector_id,
            name: "renamed".into(),
            kind: RepositoryConnectorKind::Invenio,
            endpoint: endpoint.into(),
            public_config: HashMap::from([("community".into(), "c".into())]),
            secret_config: None,
        }
    }

    #[tokio::test]
    async fn update_secret_rules() {
        let (_dir, context) = context();
        let connector = create(&context, Ulid::generate()).await;

        let kept = drive(
            UpdateRepositoryOperation::new(update(&connector, &connector.endpoint)),
            &context,
        )
        .await
        .unwrap();
        assert!(kept.has_secret_config);
        assert_eq!(kept.connector.name, "renamed");
        assert_eq!(kept.connector.created_at, connector.created_at);

        let moved = drive(
            UpdateRepositoryOperation::new(update(&connector, "https://other.example/api/")),
            &context,
        )
        .await;
        assert_eq!(moved.unwrap_err(), UpdateConnectorError::SecretEndpoint);

        let rekinded = drive(
            UpdateRepositoryOperation::new(UpdateConnectorInput {
                kind: RepositoryConnectorKind::OaiPmh,
                public_config: HashMap::new(),
                secret_config: Some(HashMap::new()),
                ..update(&connector, &connector.endpoint)
            }),
            &context,
        )
        .await;
        assert_eq!(rekinded.unwrap_err(), UpdateConnectorError::KindChanged);

        let removed = drive(
            UpdateRepositoryOperation::new(UpdateConnectorInput {
                secret_config: Some(HashMap::new()),
                ..update(&connector, "https://other.example/api/")
            }),
            &context,
        )
        .await
        .unwrap();
        assert!(!removed.has_secret_config);
        let read = drive(
            GetRepositoryOperation::new(connector.group_id, connector.connector_id),
            &context,
        )
        .await
        .unwrap();
        assert!(!read.has_secret_config);
        assert_eq!(read.connector.endpoint, "https://other.example/api/");
    }
}
