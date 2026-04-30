use crate::connectors::repository::{source_connector_key, source_connector_secret_key};
use crate::connectors::resolver::secret_fingerprint;
use crate::driver::{DriverContext, drive};
use crate::staging::descriptor::build_version_source_binding;
use crate::staging::head_source::{
    HeadStagingSourceError, HeadStagingSourceInput, HeadStagingSourceOperation,
};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_CURRENT_VERSION_KEYSPACE, S3_VERSION_KEYSPACE, SOURCE_CONNECTOR_INDEX_KEYSPACE,
    SOURCE_CONNECTOR_SECRET_KEYSPACE,
};
use aruna_core::structs::{
    CurrentVersionPointer, RealmId, SourceConnector, SourceConnectorSecret, SourceMetadata,
    StagingStrategy, VersionKey, VersionMetadata, VersionSourceBinding,
};
use aruna_core::types::{GroupId, NodeId, TxnId, UserId};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeReferenceInput {
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, PartialEq)]
pub struct MaterializeReferenceResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub version_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeReferenceError {
    #[error(transparent)]
    Head(#[from] HeadStagingSourceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

pub async fn materialize_reference(
    context: &DriverContext,
    input: MaterializeReferenceInput,
) -> Result<MaterializeReferenceResult, MaterializeReferenceError> {
    let head_result = drive(
        HeadStagingSourceOperation::new(HeadStagingSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
        }),
        context,
    )
    .await?;

    let version_source = build_version_source_binding(
        StagingStrategy::Reference,
        &head_result.connector,
        &head_result.metadata,
        input.source_path,
        Some(input.node_id),
        Some(head_result.connector.connector_id),
    );
    let version_id = Ulid::new();
    let now = SystemTime::now();

    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::WriteError.into()),
    };

    let result: Result<(), MaterializeReferenceError> = async {
        guard_resolved_connector_unchanged(
            context,
            txn_id,
            &head_result.connector,
            head_result.secret_fingerprint,
        )
        .await?;

        let current_key = LookupKey::object(&input.bucket, &input.key).to_bytes()?;
        let existing_pointer = match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: current_key.clone().into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .as_ref()
                .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
                .transpose()?,
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MaterializeReferenceError::Storage(error));
            }
            _ => return Err(MaterializeReferenceError::Storage(StorageError::ReadError)),
        };
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: current_key.into(),
                value: CurrentVersionPointer::next_for(existing_pointer.as_ref(), version_id)
                    .to_bytes()?
                    .into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MaterializeReferenceError::Storage(error));
            }
            _ => return Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
        }

        let version_key = VersionKey::new(&input.bucket, &input.key, version_id).to_bytes()?;
        let version_value = VersionMetadata::reference(
            version_id,
            version_source.clone(),
            head_result.metadata.clone(),
            now,
            input.user_id,
            now,
        )
        .to_bytes()?;

        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: version_key.into(),
                value: version_value.into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MaterializeReferenceError::Storage(error));
            }
            _ => return Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
        }

        match context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(MaterializeReferenceError::Storage(error))
            }
            _ => Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
        }
    }
    .await;

    if result.is_err() {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }

    result?;

    Ok(MaterializeReferenceResult {
        connector: head_result.connector,
        source_metadata: head_result.metadata,
        version_source,
        version_id,
    })
}

use aruna_core::structs::LookupKey;

async fn guard_resolved_connector_unchanged(
    context: &DriverContext,
    txn_id: TxnId,
    resolved_connector: &SourceConnector,
    resolved_secret_fingerprint: Option<[u8; 16]>,
) -> Result<(), MaterializeReferenceError> {
    let current_connector = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
            key: source_connector_key(resolved_connector.group_id, resolved_connector.connector_id),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| SourceConnector::from_bytes(value.as_ref()))
            .transpose()?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::ReadError.into()),
    };

    if current_connector.as_ref() != Some(resolved_connector) {
        return Err(StorageError::TransactionConflict.into());
    }

    let current_secret = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
            key: source_connector_secret_key(resolved_connector.connector_id),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| SourceConnectorSecret::from_bytes(value.as_ref()))
            .transpose()?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::ReadError.into()),
    };

    let current_secret_fingerprint = current_secret.as_ref().map(secret_fingerprint);
    if current_secret_fingerprint != resolved_secret_fingerprint {
        return Err(StorageError::TransactionConflict.into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{
        SOURCE_CONNECTOR_INDEX_KEYSPACE, SOURCE_CONNECTOR_SECRET_KEYSPACE,
    };
    use aruna_core::structs::{SourceConnectorKind, SourceConnectorSecret};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().expect("tempdir must be created");
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        (tempdir, context)
    }

    fn connector() -> SourceConnector {
        SourceConnector::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "s3-source".to_string(),
            SourceConnectorKind::S3,
            HashMap::from([
                ("bucket".to_string(), "data".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn secret(value: &str) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            Ulid::from_bytes([2u8; 16]),
            HashMap::from([("access_key_id".to_string(), value.to_string())]),
            SystemTime::UNIX_EPOCH,
        )
        .expect("secret must be present")
    }

    async fn write_connector(context: &DriverContext, connector: &SourceConnector) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(connector.group_id, connector.connector_id),
                value: connector.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn write_secret(context: &DriverContext, secret: &SourceConnectorSecret) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                key: source_connector_secret_key(secret.connector_id),
                value: secret.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn delete_connector(context: &DriverContext, connector: &SourceConnector) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(connector.group_id, connector.connector_id),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
    }

    async fn delete_secret(context: &DriverContext, connector_id: Ulid) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                key: source_connector_secret_key(connector_id),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
    }

    async fn start_write_transaction(context: &DriverContext) -> TxnId {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            event => panic!("unexpected event: {event:?}"),
        }
    }

    fn assert_conflict(result: Result<(), MaterializeReferenceError>) {
        assert_eq!(
            result,
            Err(MaterializeReferenceError::Storage(
                StorageError::TransactionConflict,
            )),
        );
    }

    #[tokio::test]
    async fn reference_guard_fails_when_connector_deleted_after_resolve() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        delete_connector(&context, &connector).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_resolved_connector_unchanged(
                &context,
                txn_id,
                &connector,
                Some(secret_fingerprint(&resolved_secret)),
            )
            .await,
        );
    }

    #[tokio::test]
    async fn reference_guard_fails_when_secret_changes_after_resolve() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        write_secret(&context, &secret("new-key")).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_resolved_connector_unchanged(
                &context,
                txn_id,
                &connector,
                Some(secret_fingerprint(&resolved_secret)),
            )
            .await,
        );
    }

    #[tokio::test]
    async fn reference_guard_fails_when_secret_removed_after_resolve() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        delete_secret(&context, connector.connector_id).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_resolved_connector_unchanged(
                &context,
                txn_id,
                &connector,
                Some(secret_fingerprint(&resolved_secret)),
            )
            .await,
        );
    }

    #[tokio::test]
    async fn reference_guard_fails_when_secret_added_after_public_resolve() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        write_connector(&context, &connector).await;

        write_secret(&context, &secret("new-key")).await;
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_resolved_connector_unchanged(&context, txn_id, &connector, None).await,
        );
    }
}
