use crate::driver::{DriverContext, drive};
use crate::s3::put_object::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use crate::staging::descriptor::build_version_source_binding;
use crate::staging::read_source::{
    ReadStagingSourceError, ReadStagingSourceInput, ReadStagingSourceOperation,
};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, RealmId,
    SourceConnector, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
};
use aruna_core::types::{GroupId, Key, NodeId, UserId, Value};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeSnapshotInput {
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
    pub quota_ceiling: Option<u64>,
    pub retry_key: Option<String>,
    pub expected_bucket: BucketInfo,
}

#[derive(Debug, PartialEq)]
pub struct MaterializeSnapshotResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub location: BackendLocation,
    pub version_id: Ulid,
}

use aruna_core::structs::BackendLocation;

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeSnapshotError {
    #[error(transparent)]
    Read(#[from] ReadStagingSourceError),
    #[error(transparent)]
    Write(#[from] PutObjectError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

pub async fn materialize_snapshot(
    context: &DriverContext,
    input: MaterializeSnapshotInput,
) -> Result<MaterializeSnapshotResult, MaterializeSnapshotError> {
    let read_result = drive(
        ReadStagingSourceOperation::new(ReadStagingSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
            range: None,
        }),
        context,
    )
    .await?;

    let mut version_source = build_version_source_binding(
        StagingStrategy::Snapshot,
        &read_result.connector,
        &read_result.metadata,
        input.source_path,
        Some(input.node_id),
        Some(read_result.connector.connector_id),
    );
    if let Some(retry_key) = input.retry_key.as_deref() {
        version_source
            .descriptor
            .capabilities
            .push(format!("staging_retry:{retry_key}"));
    }
    if input.retry_key.is_some()
        && let Some((location, version_id)) = find_snapshot(
            context,
            &input.bucket,
            &input.key,
            &version_source,
            read_result.metadata.content_length,
        )
        .await?
    {
        return Ok(MaterializeSnapshotResult {
            connector: read_result.connector,
            source_metadata: read_result.metadata,
            version_source,
            location,
            version_id,
        });
    }
    let put_result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            request: PutObjectInput {
                bucket: input.bucket,
                key: input.key,
                content_length: Some(read_result.metadata.content_length),
                body: Some(read_result.stream),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: Some(version_source.clone()),
            quota_ceiling: input.quota_ceiling,
        })
        .with_bucket_guard(input.expected_bucket),
        context,
    )
    .await
    .and_then(|result| result.transpose())?;
    let put_result = put_result.ok_or(PutObjectError::PutObjectFailed)?;

    Ok(MaterializeSnapshotResult {
        connector: read_result.connector,
        source_metadata: read_result.metadata,
        version_source,
        location: put_result.location,
        version_id: put_result.version_id,
    })
}

async fn find_snapshot(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_source: &VersionSourceBinding,
    content_length: u64,
) -> Result<Option<(BackendLocation, Ulid)>, MaterializeSnapshotError> {
    let head_key = BlobHeadKey::new(bucket, key).to_bytes()?.into();
    let Some(pointer) = read_value(context, BLOB_HEAD_KEYSPACE, head_key)
        .await?
        .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
        .transpose()?
    else {
        return Ok(None);
    };
    let version_key = VersionKey::new(bucket, key, pointer.version_id)
        .to_bytes()?
        .into();
    let Some(version) = read_value(context, BLOB_VERSIONS_KEYSPACE, version_key)
        .await?
        .map(|value| BlobVersion::from_bytes(value.as_ref()))
        .transpose()?
    else {
        return Ok(None);
    };
    let BlobVersionState::Materialized {
        blob_hash,
        source: Some(source),
    } = version.state
    else {
        return Ok(None);
    };
    if &source != version_source {
        return Ok(None);
    }
    let Some(location) = read_value(context, BLOB_LOCATIONS_KEYSPACE, blob_hash.to_vec().into())
        .await?
        .map(|value| BackendLocation::from_bytes(value.as_ref()))
        .transpose()?
    else {
        return Ok(None);
    };
    Ok((location.blob_size == content_length).then_some((location, pointer.version_id)))
}

async fn read_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Value>, MaterializeSnapshotError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError.into()),
    }
}

pub async fn stage_snapshot_blob(
    context: &DriverContext,
    input: MaterializeSnapshotInput,
) -> Result<MaterializeSnapshotResult, MaterializeSnapshotError> {
    materialize_snapshot(context, input).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staging::test_utils::{
        create_http_connector, create_test_bucket, setup_driver_context,
    };
    use axum::Router;
    use axum::routing::get;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn snapshot_retry_scoped() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/folder/file.txt",
                get(|| async { ([("content-type", "text/plain")], "snapshot-data") }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let input = MaterializeSnapshotInput {
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".to_string(),
            bucket: "bucket-a".to_string(),
            key: "object.txt".to_string(),
            quota_ceiling: None,
            retry_key: Some("job-1".to_string()),
            expected_bucket,
        };

        let first = materialize_snapshot(context, input.clone()).await.unwrap();
        let second = materialize_snapshot(context, input.clone()).await.unwrap();
        let mut manual = input;
        manual.retry_key = None;
        let third = materialize_snapshot(context, manual).await.unwrap();
        server.abort();
        let _ = server.await;

        assert_eq!(second.version_id, first.version_id);
        assert_eq!(second.location, first.location);
        assert_ne!(third.version_id, first.version_id);
    }
}
