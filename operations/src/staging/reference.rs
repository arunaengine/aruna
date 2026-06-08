use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, MaterializedHeadAlias, build_head_transition_effects,
    write_blob_version_effect,
};
use crate::connectors::repository::{source_connector_key, source_connector_secret_key};
use crate::connectors::resolver::secret_fingerprint;
use crate::driver::{DriverContext, drive};
use crate::staging::descriptor::build_version_source_binding;
use crate::staging::head_source::{
    HeadStagingSourceError, HeadStagingSourceInput, HeadStagingSourceOperation,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, SOURCE_CONNECTOR_INDEX_KEYSPACE,
    SOURCE_CONNECTOR_SECRET_KEYSPACE,
};
use aruna_core::structs::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, RealmId, SourceConnector,
    SourceConnectorSecret, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
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

        let existing_pointer =
            read_current_pointer(context, txn_id, &input.bucket, &input.key).await?;
        let previous_current_hash = read_previous_current_materialized_hash(
            context,
            txn_id,
            &input.bucket,
            &input.key,
            existing_pointer.as_ref(),
        )
        .await?;
        let next_pointer = CurrentVersionPointer::next_for(existing_pointer.as_ref(), version_id);

        for effect in build_head_transition_effects(
            &HeadAliasContext::new(
                input.realm_id,
                input.group_id,
                input.node_id,
                &input.bucket,
                &input.key,
            ),
            previous_current_hash.map(|blake3_hash| MaterializedHeadAlias {
                blake3_hash,
                version_id: existing_pointer
                    .as_ref()
                    .expect("materialized current hash requires current pointer")
                    .version_id,
            }),
            Some(next_pointer),
            None,
            Some(txn_id),
        )? {
            apply_storage_effect(context, effect).await?;
        }

        let version_key = VersionKey::new(&input.bucket, &input.key, version_id);
        apply_storage_effect(
            context,
            write_blob_version_effect(
                &version_key,
                &BlobVersion::reference(
                    version_source.clone(),
                    head_result.metadata.clone(),
                    now,
                    input.user_id,
                    now,
                ),
                Some(txn_id),
            )?,
        )
        .await?;

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

async fn apply_storage_effect(
    context: &DriverContext,
    effect: Effect,
) -> Result<(), MaterializeReferenceError> {
    let Effect::Storage(storage_effect) = effect else {
        return Err(MaterializeReferenceError::Storage(
            StorageError::InvalidEffect,
        ));
    };

    match context
        .storage_handle
        .send_storage_effect(storage_effect)
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. })
        | Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::WriteError.into()),
    }
}

async fn read_current_pointer(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    key: &str,
) -> Result<Option<CurrentVersionPointer>, MaterializeReferenceError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new(bucket, key).to_bytes()?.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError.into()),
    }
}

async fn read_previous_current_materialized_hash(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    key: &str,
    existing_pointer: Option<&CurrentVersionPointer>,
) -> Result<Option<[u8; 32]>, MaterializeReferenceError> {
    let Some(existing_pointer) = existing_pointer else {
        return Ok(None);
    };

    let version_key = VersionKey::new(bucket, key, existing_pointer.version_id).to_bytes()?;
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| -> Result<Option<[u8; 32]>, ConversionError> {
                let version = BlobVersion::from_bytes(value.as_ref())?;
                Ok(version.blob_hash().copied())
            })
            .transpose()
            .map(Option::flatten)
            .map_err(Into::into),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError.into()),
    }
}

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
    use crate::driver::drive;
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use crate::staging::test_utils::{create_http_connector, setup_driver_context};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
        SOURCE_CONNECTOR_INDEX_KEYSPACE, SOURCE_CONNECTOR_SECRET_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, HashPathIndexKey, SourceConnectorKind,
        SourceConnectorSecret,
    };
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use tokio::net::TcpListener;

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

    async fn read_value(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
    ) -> Option<aruna_core::types::Value> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };

        value
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

    #[tokio::test]
    async fn materialize_reference_retains_historical_hash_path_and_writes_blob_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::new();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let node_id = iroh::SecretKey::generate().public();

        let initial = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "bucket-a".to_string(),
                    key: "object.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
            }),
            context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let initial_hash: [u8; 32] = initial.location.get_blake3().unwrap().try_into().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/folder/file.txt",
                get(|| async {
                    (
                        [("etag", "etag-1"), ("content-type", "text/plain")],
                        "ref-data",
                    )
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });

        let connector = create_http_connector(context, group_id, &format!("http://{addr}")).await;

        let result = materialize_reference(
            context,
            MaterializeReferenceInput {
                group_id,
                user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
            },
        )
        .await
        .unwrap();

        server.abort();
        let _ = server.await;

        let blob_head_value = read_value(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket-a", "object.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry");
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 2)
        );

        let blob_version_value = read_value(
            context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket-a", "object.txt", result.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob version entry");
        let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
        assert!(!blob_version.is_materialized());
        assert!(!blob_version.is_deleted());
        assert_eq!(blob_version.source_binding(), Some(&result.version_source));

        let historical_hash_path = read_value(
            context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                initial_hash,
                initial.version_id,
                realm_id,
                group_id,
                node_id,
                "bucket-a",
                "object.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing historical materialized hash path entry");
        assert!(historical_hash_path.is_empty());
    }
}
