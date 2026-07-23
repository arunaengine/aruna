use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, build_head_transition_effects, write_blob_version_effect,
};
use crate::connectors::repository::{source_connector_key, source_connector_secret_key};
use crate::connectors::resolver::secret_fingerprint;
use crate::driver::{DriverContext, drive};
use crate::staging::descriptor::build_version_source_binding;
use crate::staging::head_source::{
    HeadStagingSourceError, HeadStagingSourceInput, HeadStagingSourceOperation,
};
use crate::task_persistence::persist_task_effect;
use crate::usage_stats::{
    UsageCounterUpdate, UsageUpdateError, schedule_usage_snapshot_publish_effect,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    SOURCE_CONNECTOR_INDEX_KEYSPACE, SOURCE_CONNECTOR_SECRET_KEYSPACE,
};
use aruna_core::structs::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, RealmId,
    SourceConnector, SourceConnectorSecret, SourceMetadata, StagingStrategy, UsageDelta,
    VersionKey, VersionSourceBinding,
};
use aruna_core::types::{Effects, GroupId, NodeId, TxnId, UserId};
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
    pub expected_bucket: BucketInfo,
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
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
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
    let version_id = Ulid::generate();
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

    let result: Result<(Ulid, bool), MaterializeReferenceError> = async {
        guard_expected_bucket(
            context,
            txn_id,
            &input.bucket,
            input.group_id,
            &input.expected_bucket,
        )
        .await?;
        guard_resolved_connector_unchanged(
            context,
            txn_id,
            &head_result.connector,
            head_result.secret_fingerprint,
        )
        .await?;

        let existing_pointer =
            read_current_pointer(context, txn_id, &input.bucket, &input.key).await?;
        let existing_version = match existing_pointer.as_ref() {
            Some(pointer) => {
                read_blob_version(
                    context,
                    txn_id,
                    &input.bucket,
                    &input.key,
                    pointer.version_id,
                )
                .await?
            }
            None => None,
        };
        if let (
            Some(pointer),
            Some(BlobVersion {
                state:
                    BlobVersionState::Reference {
                        source,
                        cached_metadata,
                        ..
                    },
                ..
            }),
        ) = (existing_pointer.as_ref(), existing_version.as_ref())
            && source == &version_source
            && source_metadata_matches(cached_metadata, &head_result.metadata)
        {
            match context
                .storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await
            {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    return Ok((pointer.version_id, false));
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    return Err(MaterializeReferenceError::Storage(error));
                }
                _ => return Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
            }
        }
        let was_live = existing_version.is_some_and(|version| !version.is_deleted());
        let next_pointer = CurrentVersionPointer::next_for(existing_pointer.as_ref(), version_id);

        for effect in build_head_transition_effects(
            &HeadAliasContext::new(
                input.realm_id,
                input.group_id,
                input.node_id,
                &input.bucket,
                &input.key,
            ),
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

        let referenced_bytes = head_result.metadata.content_length;
        let mut usage_update = UsageCounterUpdate::for_group(
            input.group_id,
            UsageDelta {
                objects: if was_live { 0 } else { 1 },
                referenced_bytes: i128::from(referenced_bytes),
                ..Default::default()
            },
        );
        if !usage_update.is_noop() {
            run_usage_update(context, txn_id, &mut usage_update).await?;
        }

        match context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok((version_id, true)),
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

    let (version_id, changed) = result?;
    if changed {
        schedule_usage_snapshot_publish(context).await;
    }

    Ok(MaterializeReferenceResult {
        connector: head_result.connector,
        source_metadata: head_result.metadata,
        version_source,
        version_id,
    })
}

fn source_metadata_matches(left: &SourceMetadata, right: &SourceMetadata) -> bool {
    left.content_length == right.content_length
        && left.content_type == right.content_type
        && left.etag == right.etag
        && left.last_modified == right.last_modified
}

pub async fn stage_reference_blob(
    context: &DriverContext,
    input: MaterializeReferenceInput,
) -> Result<MaterializeReferenceResult, MaterializeReferenceError> {
    materialize_reference(context, input).await
}

async fn apply_storage_effect(
    context: &DriverContext,
    effect: Effect,
) -> Result<(), MaterializeReferenceError> {
    match send_storage_effect(context, effect).await? {
        Event::Storage(StorageEvent::WriteResult { .. })
        | Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        _ => Err(StorageError::WriteError.into()),
    }
}

async fn send_storage_effect(
    context: &DriverContext,
    effect: Effect,
) -> Result<Event, MaterializeReferenceError> {
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
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Ok(event),
    }
}

async fn send_single_storage_effect(
    context: &DriverContext,
    mut effects: Effects,
) -> Result<Event, MaterializeReferenceError> {
    if effects.len() != 1 {
        return Err(StorageError::InvalidEffect.into());
    }
    send_storage_effect(
        context,
        effects
            .pop()
            .expect("effect length was checked before popping"),
    )
    .await
}

async fn run_usage_update(
    context: &DriverContext,
    txn_id: TxnId,
    usage_update: &mut UsageCounterUpdate,
) -> Result<(), MaterializeReferenceError> {
    let mut effects = usage_update.start(txn_id);
    loop {
        let event = send_single_storage_effect(context, effects).await?;
        effects = match usage_update.step(event, txn_id)? {
            Some(effects) => effects,
            None => return Ok(()),
        };
    }
}

async fn schedule_usage_snapshot_publish(context: &DriverContext) {
    let Effect::Task(task_effect) = schedule_usage_snapshot_publish_effect() else {
        return;
    };
    if persist_task_effect(&context.storage_handle, &task_effect)
        .await
        .is_err()
    {
        return;
    }
    if let Some(task_handle) = &context.task_handle {
        let _ = task_handle.send_effect(Effect::Task(task_effect)).await;
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

async fn read_blob_version(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<Option<BlobVersion>, MaterializeReferenceError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(bucket, key, version_id).to_bytes()?.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()
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

async fn guard_expected_bucket(
    context: &DriverContext,
    txn_id: TxnId,
    bucket: &str,
    group_id: GroupId,
    expected: &BucketInfo,
) -> Result<(), MaterializeReferenceError> {
    let current = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::ReadError.into()),
    };
    if expected.group_id != group_id || current.as_ref() != Some(expected) {
        return Err(StorageError::TransactionConflict.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use crate::staging::test_utils::{
        create_http_connector, create_test_bucket, setup_driver_context,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
        SOURCE_CONNECTOR_INDEX_KEYSPACE, SOURCE_CONNECTOR_SECRET_KEYSPACE, USAGE_STATS_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, HashPathIndexKey, SourceConnectorKind,
        SourceConnectorSecret, UsageCounters, usage_global_key_for_group, usage_group_key,
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
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
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

    async fn read_usage_counters(context: &DriverContext, key: Vec<u8>) -> UsageCounters {
        read_value(context, USAGE_STATS_KEYSPACE, key)
            .await
            .map(|value| UsageCounters::from_bytes(value.as_ref()).unwrap())
            .unwrap_or_default()
    }

    async fn spawn_reference_server(body: &'static str) -> (tokio::task::JoinHandle<()>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app =
                Router::new().route(
                    "/folder/file.txt",
                    get(move || async move {
                        ([("etag", "etag-1"), ("content-type", "text/plain")], body)
                    }),
                );
            axum::serve(listener, app).await.unwrap();
        });
        (server, format!("http://{addr}"))
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
    async fn bucket_guard_rejects_recreate() {
        let (_tempdir, context) = test_context();
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let expected = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
            cors_configuration: None,
        };
        let recreated = BucketInfo {
            created_at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
            ..expected.clone()
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                value: recreated.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let txn_id = start_write_transaction(&context).await;

        assert_conflict(
            guard_expected_bucket(&context, txn_id, "bucket-a", group_id, &expected).await,
        );
    }

    #[tokio::test]
    async fn materialize_reference_retains_historical_hash_path_and_writes_blob_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;

        let initial = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
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
                preassigned_version_id: None,
                quota_ceiling: None,
            }),
            context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let initial_hash: [u8; 32] = initial.location.get_blake3().unwrap().try_into().unwrap();

        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;

        let result = materialize_reference(
            context,
            MaterializeReferenceInput {
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
                expected_bucket,
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

        let group_usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(group_usage.objects, 1);
        assert_eq!(group_usage.logical_bytes, 5);
        assert_eq!(group_usage.referenced_bytes, 8);
        let global_usage = read_usage_counters(context, usage_global_key_for_group(group_id)).await;
        assert_eq!(global_usage.objects, 1);
        assert_eq!(global_usage.logical_bytes, 5);
        assert_eq!(global_usage.referenced_bytes, 8);
    }

    #[tokio::test]
    async fn reference_bypasses_quota() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;

        let result = materialize_reference(
            context,
            MaterializeReferenceInput {
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
                expected_bucket,
            },
        )
        .await;

        server.abort();
        let _ = server.await;

        assert!(result.is_ok());
        let usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(usage.logical_bytes, 0);
        assert_eq!(usage.referenced_bytes, 8);
    }

    #[tokio::test]
    async fn reference_reuses_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let input = MaterializeReferenceInput {
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".to_string(),
            bucket: "bucket-a".to_string(),
            key: "object.txt".to_string(),
            expected_bucket,
        };

        let first = materialize_reference(context, input.clone()).await.unwrap();
        let second = materialize_reference(context, input).await.unwrap();
        server.abort();
        let _ = server.await;

        assert_eq!(second.version_id, first.version_id);
        let pointer = read_value(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket-a", "object.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            CurrentVersionPointer::from_bytes(pointer.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(first.version_id, 1)
        );
        let usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(usage.referenced_bytes, 8);
    }
}
