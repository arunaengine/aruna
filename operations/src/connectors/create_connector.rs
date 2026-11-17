//! Creates a source connector record together with its validated secret config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{SOURCE_ID_KEYSPACE, SOURCE_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::source_connector::{
    SourceConnector, SourceConnectorKind, SourceConnectorSecret,
};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    LIST_CONNECTOR_SIZE, StorageReadError, connector_id_key, parse_secret_read, read_secret_effect,
    source_connector_key,
};
use crate::connectors::validation::{ValidationError, validate_connector_input};
use crate::groups::fence::{group_write_result, write_group_new};
use crate::storage_read::decode_row;

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

/// The call a connector creation waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    /// Reads the record a connector with a given id may already have in this group.
    Existing,
    Secret,
    /// Pages through every connector of the node for another group using the same id.
    Scan,
    Write,
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
    /// The id is in use on this node with another definition, credentials or group.
    #[error("connector id is already in use on this node")]
    IdTaken,
}

impl From<IoError> for SourceConnectorError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<StorageReadError> for SourceConnectorError {
    fn from(error: StorageReadError) -> Self {
        match error {
            StorageReadError::Storage(error) => error.into(),
            StorageReadError::Conversion(error) => error.into(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SourceConnectorOperation {
    input: SourceConnectorInput,
    stage: Stage,
    result: Option<SourceConnectorResult>,
    /// The id a connector registered on several nodes keeps; `None` mints a fresh one.
    chosen_id: Option<Ulid>,
    /// An identical record of the chosen id this group already holds.
    existing: Option<SourceConnector>,
}

impl SourceConnectorOperation {
    pub fn new(input: SourceConnectorInput) -> Self {
        Self {
            input,
            stage: Stage::Existing,
            result: None,
            chosen_id: None,
            existing: None,
        }
    }

    /// Registers the connector under `connector_id`, as the same connector on another node is.
    /// An identical record without credentials is returned as is; any other use is refused.
    pub fn with_id(input: SourceConnectorInput, connector_id: Ulid) -> Self {
        Self {
            chosen_id: Some(connector_id),
            ..Self::new(input)
        }
    }

    fn same_record(&self, existing: &SourceConnector) -> bool {
        existing.name == self.input.name
            && existing.kind == self.input.kind
            && existing.public_config == self.input.public_config
            && existing.namespace.is_none()
            && self.input.secret_config.is_empty()
    }

    fn scan_ids(&mut self, start_after: Option<Key>) -> Next<SourceConnectorResult> {
        self.stage = Stage::Scan;
        let iter = StorageEffect::Iter {
            key_space: SOURCE_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: LIST_CONNECTOR_SIZE,
            txn_id: None,
        };
        Next::Call(IoCall::Effect(Box::new(Effect::Storage(iter))))
    }

    fn scanned(
        &mut self,
        event: Event,
        connector_id: Ulid,
    ) -> Result<Next<SourceConnectorResult>, SourceConnectorError> {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            let got = event.label();
            let expected = "StorageEvent::IterResult";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let id = connector_id.to_bytes();
        if values.iter().any(|(key, _)| key.get(16..) == Some(&id[..])) {
            return Err(SourceConnectorError::IdTaken);
        }
        match next_start_after {
            Some(next) => Ok(self.scan_ids(Some(next))),
            None => self.write_records(connector_id),
        }
    }

    fn write_records(
        &mut self,
        connector_id: Ulid,
    ) -> Result<Next<SourceConnectorResult>, SourceConnectorError> {
        let now = SystemTime::now();
        let connector = SourceConnector::new(
            connector_id,
            self.input.group_id,
            self.input.name.clone(),
            self.input.kind,
            self.input.public_config.clone(),
            now,
            now,
            self.input.created_by,
        )
        .with_namespace(&self.input.secret_config);
        let secret =
            SourceConnectorSecret::new(connector_id, self.input.secret_config.clone(), now);
        let id_row = (
            SOURCE_ID_KEYSPACE.to_string(),
            connector_id_key(connector_id),
        );
        let writes = vec![
            (
                SOURCE_INDEX_KEYSPACE.to_string(),
                source_connector_key(connector.group_id, connector.connector_id),
                connector.to_bytes()?.into(),
            ),
            (
                id_row.0.clone(),
                id_row.1.clone(),
                self.input.group_id.to_bytes().to_vec().into(),
            ),
        ];
        let sealed = match secret.as_ref().map(SourceConnectorSecret::to_secret) {
            Some(bytes) => Some((SourceConnectorSecret::vault_entry(connector_id), bytes?)),
            None => None,
        };
        // The id row is read in the write's transaction: a second creator of the id conflicts.
        let write = write_group_new(self.input.group_id, writes, sealed, id_row);
        self.result = Some(SourceConnectorResult {
            connector,
            has_secret_config: secret.is_some(),
        });
        self.stage = Stage::Write;
        Ok(Next::Call(write.into()))
    }

    fn written(
        &mut self,
        event: Event,
    ) -> Result<Next<SourceConnectorResult>, SourceConnectorError> {
        match group_write_result(event) {
            Ok(()) => {}
            Err(GroupWriteError::Taken) => return Err(SourceConnectorError::IdTaken),
            // A given id another creator wrote first conflicts with this transaction.
            Err(GroupWriteError::Storage(StorageError::TransactionConflict))
                if self.chosen_id.is_some() =>
            {
                return Err(SourceConnectorError::IdTaken);
            }
            Err(error) => return Err(error.into()),
        }
        let result = self.result.take();
        result
            .map(Next::Done)
            .ok_or(SourceConnectorError::CreateConnectorFailed)
    }
}

impl Operation for SourceConnectorOperation {
    type Output = SourceConnectorResult;
    type Error = SourceConnectorError;

    fn first(&mut self) -> Result<Next<Self::Output>, SourceConnectorError> {
        validate_connector_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        )?;
        let Some(connector_id) = self.chosen_id else {
            return self.write_records(Ulid::generate());
        };
        Ok(Next::Call(rows::read_row(
            SOURCE_INDEX_KEYSPACE,
            source_connector_key(self.input.group_id, connector_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SourceConnectorError> {
        let chosen = self.chosen_id.unwrap_or_default();
        match self.stage {
            Stage::Existing => {
                let existing = decode_row(rows::value(result)?, SourceConnector::from_bytes)?;
                if existing
                    .as_ref()
                    .is_some_and(|existing| !self.same_record(existing))
                {
                    return Err(SourceConnectorError::IdTaken);
                }
                self.existing = existing;
                self.stage = Stage::Secret;
                let read = read_secret_effect(chosen, None);
                Ok(Next::Call(IoCall::Effect(Box::new(read))))
            }
            Stage::Secret => {
                let secret = parse_secret_read(rows::event(result)?, chosen)?;
                match (secret, self.existing.take()) {
                    (Some(_), _) => Err(SourceConnectorError::IdTaken),
                    (None, Some(existing)) => Ok(Next::Done(SourceConnectorResult {
                        connector: existing,
                        has_secret_config: false,
                    })),
                    (None, None) => Ok(self.scan_ids(None)),
                }
            }
            Stage::Scan => self.scanned(rows::event(result)?, chosen),
            Stage::Write => self.written(rows::event(result)?),
        }
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
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
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

    fn public_input(group_id: ulid::Ulid, bucket: &str) -> SourceConnectorInput {
        SourceConnectorInput {
            group_id,
            created_by: Default::default(),
            name: "bakrep".to_string(),
            kind: SourceConnectorKind::S3,
            public_config: HashMap::from([
                ("bucket".to_string(), bucket.to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ("skip_signature".to_string(), "true".to_string()),
            ]),
            secret_config: HashMap::new(),
        }
    }

    // A connector keeps its id on every node it is registered on; any other use of the id,
    // by another definition, another group or a connector with credentials, is refused.
    #[tokio::test]
    async fn given_id_registered() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
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
        let (group, other) = (
            ulid::Ulid::from_bytes([1; 16]),
            ulid::Ulid::from_bytes([2; 16]),
        );
        let id = ulid::Ulid::from_bytes([3; 16]);
        let with_id = |input| drive(SourceConnectorOperation::with_id(input, id), &context);

        let first = with_id(public_input(group, "bakrep")).await.unwrap();
        let again = with_id(public_input(group, "bakrep")).await.unwrap();
        assert_eq!(
            (first.connector.connector_id, &again.connector),
            (id, &first.connector)
        );
        let changed = with_id(public_input(group, "other")).await;
        assert_eq!(changed, Err(SourceConnectorError::IdTaken));
        let elsewhere = with_id(public_input(other, "bakrep")).await;
        assert_eq!(elsewhere, Err(SourceConnectorError::IdTaken));

        let mut private = public_input(group, "private");
        private.public_config.remove("skip_signature");
        private.secret_config = HashMap::from([
            ("access_key_id".to_string(), "AKIA".to_string()),
            ("secret_access_key".to_string(), "secret".to_string()),
        ]);
        let private = drive(SourceConnectorOperation::new(private), &context)
            .await
            .unwrap();
        let reuse = drive(
            SourceConnectorOperation::with_id(
                public_input(other, "bakrep"),
                private.connector.connector_id,
            ),
            &context,
        )
        .await;
        assert_eq!(reuse, Err(SourceConnectorError::IdTaken));
    }

    // Two groups registering one new id at the same time: exactly one gets it.
    #[tokio::test]
    async fn concurrent_id_once() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
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
        let id = ulid::Ulid::from_bytes([4; 16]);
        let create = |group: u8| {
            let input = public_input(ulid::Ulid::from_bytes([group; 16]), "bakrep");
            drive(SourceConnectorOperation::with_id(input, id), &context)
        };

        let (first, second) = tokio::join!(create(1), create(2));

        let outcomes = [first.is_ok(), second.is_ok()];
        assert_eq!(outcomes.iter().filter(|ok| **ok).count(), 1);
        let refused = [first, second].into_iter().find_map(Result::err);
        assert_eq!(refused, Some(SourceConnectorError::IdTaken));
    }
}
