//! Replaces or deletes a repository connector and its secret in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{CONNECTOR_INDEX_KEYSPACE, CONNECTOR_SECRET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::harvest::{
    RepositoryConnector, RepositoryConnectorKind, RepositoryConnectorSecret,
};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::groups::fence::{parse_group_record, read_group_record};
use crate::harvest::create_connector::{CreateConnectorError, validate_connector};
use crate::harvest::read_connector::ConnectorView;
use crate::harvest::repository::{
    connector_key, connector_secret_key, connector_writes, read_secret_effect,
};

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

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    ReadRecord,
    ReadSecret,
    Write,
    Delete,
    Commit,
    Aborting,
    Finish,
}

/// Replaces the public definition; the id, group, creator and creation time are kept.
#[derive(Debug, PartialEq)]
pub struct UpdateRepositoryOperation {
    input: UpdateConnectorInput,
    state: State,
    txn_id: Option<TxnId>,
    existing: Option<RepositoryConnector>,
    deletes: Vec<(String, Key)>,
    view: Option<ConnectorView>,
    output: Option<Result<ConnectorView, UpdateConnectorError>>,
}

impl UpdateRepositoryOperation {
    pub fn new(input: UpdateConnectorInput) -> Self {
        Self {
            input,
            state: State::Init,
            txn_id: None,
            existing: None,
            deletes: Vec::new(),
            view: None,
            output: None,
        }
    }

    fn fail(&mut self, error: UpdateConnectorError) -> Effects {
        self.output = Some(Err(error));
        self.abort()
    }

    fn read_record(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = State::ReadRecord;
        smallvec![read_group_record(
            self.input.group_id,
            CONNECTOR_INDEX_KEYSPACE,
            connector_key(self.input.group_id, self.input.connector_id),
            txn_id,
        )]
    }

    fn handle_record(&mut self, event: Event) -> Effects {
        let record = match parse_group_record(event) {
            Ok(Some(record)) => record,
            Ok(None) => return self.fail(UpdateConnectorError::NotFound),
            Err(error) => return self.fail(error.into()),
        };
        match RepositoryConnector::from_bytes(&record) {
            Ok(existing) => self.existing = Some(existing),
            Err(error) => return self.fail(GroupWriteError::from(error).into()),
        }
        self.state = State::ReadSecret;
        smallvec![read_secret_effect(self.input.connector_id, self.txn_id)]
    }

    fn handle_secret(&mut self, event: Event) -> Effects {
        let stored = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            _ => return self.fail(UpdateConnectorError::Unexpected),
        };
        let Some(existing) = self.existing.take() else {
            return self.fail(UpdateConnectorError::Unexpected);
        };
        if existing.kind != self.input.kind {
            return self.fail(UpdateConnectorError::KindChanged);
        }
        if stored && self.input.secret_config.is_none() && existing.endpoint != self.input.endpoint
        {
            return self.fail(UpdateConnectorError::SecretEndpoint);
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
        if stored && !has_secret_config {
            self.deletes.push((
                CONNECTOR_SECRET_KEYSPACE.to_string(),
                connector_secret_key(connector.connector_id),
            ));
        }
        let writes = match connector_writes(&connector, secret.as_ref()) {
            Ok(writes) => writes,
            Err(error) => return self.fail(GroupWriteError::from(error).into()),
        };
        self.view = Some(ConnectorView {
            connector,
            has_secret_config,
        });
        self.state = State::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn after_write(&mut self) -> Effects {
        if self.deletes.is_empty() {
            return self.commit();
        }
        self.state = State::Delete;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(UpdateConnectorError::Unexpected);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for UpdateRepositoryOperation {
    type Output = ConnectorView;
    type Error = UpdateConnectorError;

    fn start(&mut self) -> Effects {
        if let Err(error) = validate_connector(
            &self.input.name,
            self.input.kind,
            &self.input.endpoint,
            &self.input.public_config,
            self.input.secret_config.as_ref().unwrap_or(&HashMap::new()),
        ) {
            return self.fail(error.into());
        }
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Aborting {
            self.state = State::Finish;
            return smallvec![];
        }
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.read_record(txn_id)
            }
            (State::ReadRecord, event) => self.handle_record(event),
            (State::ReadSecret, event) => self.handle_secret(event),
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.after_write()
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.commit()
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = State::Finish;
                self.output = self.view.take().map(Ok);
                smallvec![]
            }
            _ => self.fail(UpdateConnectorError::Unexpected),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UpdateConnectorError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) if self.state != State::Commit => {
                self.state = State::Aborting;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => {
                self.state = State::Finish;
                smallvec![]
            }
        }
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
            task_handle: None,
            compute_handle: None,
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
