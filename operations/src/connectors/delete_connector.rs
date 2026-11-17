//! Deletes a source connector and its secret unless an object version still uses it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{SOURCE_ID_KEYSPACE, SOURCE_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::reference_prune::schedule_prune;
use crate::connectors::reference_scan::{ReferenceScan, ScanStep};
use crate::connectors::repository::{
    StorageReadError, connector_id_key, delete_secret_effect, parse_secret_read,
    read_secret_effect, source_connector_key,
};
use crate::storage_read::decode_row;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceResult;

/// The call a delete waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Connector,
    Secret,
    Scan(ReferenceScan),
    Records,
    SecretDeleted,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
    #[error("Connector credentials are referenced by object versions")]
    ReferencedObjectVersion,
    #[error("Connector references are still being checked; retry later")]
    ReferencesPending,
}

impl From<IoError> for DeleteSourceError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<StorageReadError> for DeleteSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

/// Deletes a connector in one transaction. Its output is `None` while the reference check is
/// pending: the stale page and the pruning queue row commit, and the caller retries later.
#[derive(Debug, PartialEq)]
pub struct DeleteSourceOperation {
    input: DeleteSourceInput,
    stage: Stage,
}

impl DeleteSourceOperation {
    pub fn new(input: DeleteSourceInput) -> Self {
        Self {
            input,
            stage: Stage::Connector,
        }
    }

    fn delete_records(&mut self) -> Next<Option<DeleteSourceResult>> {
        self.stage = Stage::Records;
        Next::Call(IoCall::Delete(vec![
            (
                SOURCE_INDEX_KEYSPACE.to_string(),
                source_connector_key(self.input.group_id, self.input.connector_id),
            ),
            (
                SOURCE_ID_KEYSPACE.to_string(),
                connector_id_key(self.input.connector_id),
            ),
        ]))
    }

    fn scanned(
        &mut self,
        step: ScanStep,
    ) -> Result<Next<Option<DeleteSourceResult>>, DeleteSourceError> {
        match step {
            ScanStep::Referenced => Err(DeleteSourceError::ReferencedObjectVersion),
            ScanStep::Next(call) => Ok(Next::Call(call)),
            ScanStep::Deferred => Ok(Next::Then {
                output: None,
                calls: vec![schedule_prune().into()],
            }),
            ScanStep::Complete => Ok(self.delete_records()),
        }
    }
}

impl Operation for DeleteSourceOperation {
    type Output = Option<DeleteSourceResult>;
    type Error = DeleteSourceError;

    fn first(&mut self) -> Result<Next<Self::Output>, DeleteSourceError> {
        Ok(Next::Call(rows::read_row(
            SOURCE_INDEX_KEYSPACE,
            source_connector_key(self.input.group_id, self.input.connector_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, DeleteSourceError> {
        let connector_id = self.input.connector_id;
        match &mut self.stage {
            Stage::Connector => {
                decode_row(rows::value(result)?, SourceConnector::from_bytes)?
                    .ok_or(DeleteSourceError::NotFound)?;
                self.stage = Stage::Secret;
                let read = read_secret_effect(connector_id, None);
                Ok(Next::Call(read.into()))
            }
            Stage::Secret => {
                if parse_secret_read(rows::event(result)?, connector_id)?.is_none() {
                    return Ok(self.delete_records());
                }
                let mut scan = ReferenceScan::new(connector_id);
                let call = scan.start();
                self.stage = Stage::Scan(scan);
                Ok(Next::Call(call))
            }
            Stage::Scan(scan) => {
                let step = scan.step(result)?;
                self.scanned(step)
            }
            Stage::Records => {
                rows::done(result)?;
                self.stage = Stage::SecretDeleted;
                Ok(Next::Call(delete_secret_effect(connector_id, None).into()))
            }
            Stage::SecretDeleted => match rows::event(result)? {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    Ok(Next::Done(Some(DeleteSourceResult)))
                }
                other => Err(IoError::Unexpected {
                    expected: "StorageEvent::DeleteResult",
                    got: other.label(),
                }
                .into()),
            },
        }
    }

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::descriptor::build_source_binding;
    use crate::connectors::replace_connector::{ReplaceSourceInput, ReplaceSourceOperation};
    use crate::connectors::repository::{
        connector_reference_key, connector_reference_write, parse_connector_read,
        read_connector_effect,
    };
    use crate::connectors::resolver::{ResolveBindingInput, ResolveBindingOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
    };
    use aruna_core::operation::{EffectOperation, Runnable};
    use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
    use aruna_core::structs::execution::source_connector::{
        SourceConnector, SourceConnectorKind, SourceConnectorSecret,
    };
    use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
    use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
    use aruna_core::types::Key;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    fn test_context() -> (TempDir, DriverContext) {
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
        (tempdir, context)
    }

    fn source_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }
    }

    async fn create_connector(context: &DriverContext) -> SourceConnector {
        drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "dav-source".to_string(),
                kind: SourceConnectorKind::Webdav,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "https://dav.example.org".to_string(),
                    ),
                    ("root".to_string(), "/datasets".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("username".to_string(), "alice".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            context,
        )
        .await
        .unwrap()
        .connector
    }

    async fn create_public_connector(context: &DriverContext) -> SourceConnector {
        drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "http-source".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
            context,
        )
        .await
        .unwrap()
        .connector
    }

    async fn write_reference_version(context: &DriverContext, source: VersionSourceBinding) {
        let version_key = VersionKey::new("bucket", "key", ulid::Ulid::generate());
        let version = BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        );
        let mut writes = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        )];
        writes.extend(connector_reference_write(&version, &version_key).unwrap());

        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    fn connector_secret(connector_id: Ulid) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            connector_id,
            HashMap::from([("token".to_string(), "secret".to_string())]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    fn reference_blob_version(connector_id: Ulid) -> BlobVersion {
        let connector = SourceConnector::new(
            connector_id,
            Ulid::generate(),
            "ftp-source".to_string(),
            SourceConnectorKind::Ftp,
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        );
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector_id),
        );

        BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        )
    }

    fn stored_connector(group_id: Ulid, connector_id: Ulid) -> IoResult {
        let connector = SourceConnector::new(
            connector_id,
            group_id,
            "ftp-source".to_string(),
            SourceConnectorKind::Ftp,
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        );
        IoResult::Values(vec![Some(connector.to_bytes().unwrap().into())])
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    /// An operation past its connector and secret reads, with the reference scan started.
    fn scanning(connector_id: Ulid) -> DeleteSourceOperation {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id,
            connector_id,
        });
        assert_eq!(operation.transaction(), aruna_core::operation::TransactionMode::Write);
        operation.first().unwrap();
        let read = operation.next(stored_connector(group_id, connector_id));
        assert!(matches!(read, Ok(Next::Call(IoCall::Effect(_)))));
        let secret = event(Event::Storage(StorageEvent::VaultResult {
            entry: SourceConnectorSecret::vault_entry(connector_id),
            secret: Some(connector_secret(connector_id).to_secret().unwrap()),
        }));
        let scan = operation.next(secret);
        assert!(matches!(scan, Ok(Next::Call(IoCall::Page { .. }))));
        operation
    }

    fn reference_rows(connector_id: Ulid, next: Option<Key>) -> IoResult {
        let version_key = VersionKey::new("bucket", "key", Ulid::nil());
        IoResult::Page {
            rows: vec![(
                connector_reference_key(connector_id, &version_key).unwrap(),
                Vec::new().into(),
            )],
            next,
        }
    }

    fn version_read(version: Option<BlobVersion>) -> IoResult {
        IoResult::Values(vec![
            version.map(|version| version.to_bytes().unwrap().into()),
        ])
    }

    #[test]
    fn scan_then_delete() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let mut operation = scanning(connector_id);
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let records = operation.next(empty);
        assert!(matches!(
            records,
            Ok(Next::Call(IoCall::Delete(deletes))) if deletes.len() == 2
        ));
        let Ok(Next::Call(IoCall::Effect(secret))) = operation.next(IoResult::Done) else {
            panic!("expected the secret removal");
        };
        assert!(matches!(
            *secret,
            Effect::Storage(StorageEffect::VaultDelete { entry, txn_id: None })
                if entry.id == connector_id
        ));
        let deleted = event(Event::Storage(StorageEvent::DeleteResult {
            key: vec![].into(),
        }));
        assert_eq!(
            operation.next(deleted),
            Ok(Next::Done(Some(DeleteSourceResult)))
        );
    }

    #[test]
    fn reference_refuses_delete() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let mut operation = scanning(connector_id);
        let reads = operation.next(reference_rows(connector_id, None));
        assert!(matches!(reads, Ok(Next::Call(IoCall::Read(_)))));
        let live = version_read(Some(reference_blob_version(connector_id)));
        assert_eq!(
            operation.next(live),
            Err(DeleteSourceError::ReferencedObjectVersion)
        );
    }

    // A stale page with more rows behind it commits its pruning and the queued background work,
    // then asks the caller to retry instead of scanning on.
    #[test]
    fn pending_defers_check() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let mut operation = scanning(connector_id);
        operation
            .next(reference_rows(connector_id, Some(vec![9u8].into())))
            .unwrap();
        assert!(matches!(
            operation.next(version_read(None)),
            Ok(Next::Call(IoCall::Delete(deletes)))
                if deletes[0].0 == CONNECTOR_REFERENCE_KEYSPACE
        ));
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Write(writes)))
                if writes[0].0 == CONNECTOR_PRUNE_KEYSPACE
        ));
        let Ok(Next::Then {
            output: None,
            calls,
        }) = operation.next(IoResult::Done)
        else {
            panic!("expected the pending outcome");
        };
        assert!(
            matches!(calls.as_slice(), [IoCall::Effect(wake)] if matches!(**wake, Effect::Task(_))),
            "the pruning is woken after the commit"
        );
    }

    #[test]
    fn scan_failure_fails() {
        let mut operation = scanning(Ulid::from_bytes([2u8; 16]));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(DeleteSourceError::StorageError(StorageError::ReadError(_)))
        ));
    }

    #[tokio::test]
    async fn rejects_referenced_connector() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        let result = drive(
            DeleteSourceOperation::new(DeleteSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await;

        assert_eq!(result, Err(DeleteSourceError::ReferencedObjectVersion));

        let access = drive(
            ResolveBindingOperation::new(ResolveBindingInput {
                source,
                group_id: None,
            }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(config.get("username").map(String::as_str), Some("alice"));
    }

    #[tokio::test]
    async fn allows_public_references() {
        let (_tempdir, context) = test_context();
        let connector = create_public_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        drive(
            DeleteSourceOperation::new(DeleteSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await
        .unwrap();

        let access = drive(
            ResolveBindingOperation::new(ResolveBindingInput {
                source,
                group_id: None,
            }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("https://example.org")
        );
    }

    #[tokio::test]
    async fn public_delete_abort() {
        let (_tempdir, context) = test_context();
        let connector = create_public_connector(&context).await;
        let input = DeleteSourceInput {
            group_id: connector.group_id,
            connector_id: connector.connector_id,
        };
        // Runs the delete by hand: its transaction start, connector read and secret read.
        let mut operation = DeleteSourceOperation::new(input.clone()).into_machine();
        let mut effects = operation.start();
        for _ in 0..3 {
            assert_eq!(effects.len(), 1);
            let event = context.storage_handle.send_effect(effects.remove(0)).await;
            effects = operation.step(event);
        }

        drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: connector.name.clone(),
                kind: connector.kind,
                public_config: connector.public_config.clone(),
                secret_config: HashMap::from([("token".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { .. })]
        ));
        let event = context.storage_handle.send_effect(effects.remove(0)).await;
        let effects = operation.step(event);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::VaultDelete { .. })]
        ));
        for effect in operation.abort() {
            let event = context.storage_handle.send_effect(effect).await;
            assert!(matches!(
                event,
                Event::Storage(StorageEvent::TransactionAborted { .. })
            ));
        }

        let event = context
            .storage_handle
            .send_effect(read_connector_effect(
                input.group_id,
                input.connector_id,
                None,
            ))
            .await;
        assert!(parse_connector_read(event).unwrap().is_some());
        let event = context
            .storage_handle
            .send_effect(read_secret_effect(input.connector_id, None))
            .await;
        assert!(
            parse_secret_read(event, input.connector_id)
                .unwrap()
                .is_some()
        );

        drive(DeleteSourceOperation::new(input.clone()), &context)
            .await
            .unwrap();
        let event = context
            .storage_handle
            .send_effect(read_connector_effect(
                input.group_id,
                input.connector_id,
                None,
            ))
            .await;
        assert!(parse_connector_read(event).unwrap().is_none());
        let event = context
            .storage_handle
            .send_effect(read_secret_effect(input.connector_id, None))
            .await;
        assert!(
            parse_secret_read(event, input.connector_id)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn deletes_unreferenced_secret() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;

        drive(
            DeleteSourceOperation::new(DeleteSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await
        .unwrap();

        let secret_event = context
            .storage_handle
            .send_effect(read_secret_effect(connector.connector_id, None))
            .await;
        assert!(
            parse_secret_read(secret_event, connector.connector_id)
                .unwrap()
                .is_none()
        );
    }
}
