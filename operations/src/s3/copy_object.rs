use crate::driver::{DriverContext, drive};
use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::put_object::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use aruna_core::UserId;
use aruna_core::structs::{BackendLocation, RealmId, StagingStrategy, VersionSourceBinding};
use aruna_core::types::{GroupId, NodeId};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CopySourceConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<SystemTime>,
    pub if_unmodified_since: Option<SystemTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyObjectInput {
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<Ulid>,
    pub source_group_id: GroupId,
    pub dest_bucket: String,
    pub dest_key: String,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub quota_ceiling: Option<u64>,
    pub conditions: CopySourceConditions,
}

#[derive(Debug, PartialEq)]
pub struct CopyObjectResultData {
    pub location: BackendLocation,
    pub version_id: Ulid,
    pub source_version_id: Option<Ulid>,
    pub source_last_modified: Option<SystemTime>,
}

#[derive(Debug, Error, PartialEq)]
pub enum CopyObjectError {
    #[error(transparent)]
    Get(#[from] GetObjectError),
    #[error(transparent)]
    Put(#[from] PutObjectError),
}

pub async fn copy_object(
    context: &DriverContext,
    input: CopyObjectInput,
) -> Result<CopyObjectResultData, CopyObjectError> {
    let source = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: input.source_bucket,
            key: input.source_key,
            version_id: input.source_version_id,
            range: None,
            group_id: input.source_group_id,
            user_identity: input.user_id,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())?
    .ok_or(CopyObjectError::Get(GetObjectError::GetObjectFailed))?;

    let source_version_id = source.version_id;
    let source_last_modified = source
        .location
        .as_ref()
        .map(|location| location.created_at)
        .or_else(|| {
            source
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_modified)
        });
    let materialized = source.location.is_some();
    let content_length = source.location.as_ref().map(|location| location.blob_size);
    let version_source = if materialized {
        source.source_binding.clone()
    } else {
        source
            .source_binding
            .clone()
            .map(|binding| VersionSourceBinding {
                strategy: StagingStrategy::Snapshot,
                ..binding
            })
    };

    let put_result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: input.user_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            request: PutObjectInput {
                bucket: input.dest_bucket,
                key: input.dest_key,
                content_length,
                body: Some(source.blob),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source,
            quota_ceiling: input.quota_ceiling,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())?;
    let put_result = put_result.ok_or(PutObjectError::PutObjectFailed)?;

    Ok(CopyObjectResultData {
        location: put_result.location,
        version_id: put_result.version_id,
        source_version_id,
        source_last_modified,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::s3::get_object::GetObjectOperation;
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer,
        PortableSourceDescriptor, SourceConnectorKind, SourceMetadata, VersionKey,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use tempfile::{TempDir, tempdir};
    use tokio::net::TcpListener;

    async fn full_context() -> (TempDir, DriverContext) {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };
        (temp_handle, context)
    }

    async fn spawn_reference_server(body: &'static [u8]) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/folder/file.txt",
            get(
                move || async move { ([("content-type", "text/plain"), ("etag", "etag-1")], body) },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    fn put_config(
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        bucket: &str,
        key: &str,
        data: &'static [u8],
    ) -> PutObjectConfig {
        PutObjectConfig {
            user_id: UserId::local(Ulid::new(), realm_id),
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(data))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            quota_ceiling: None,
        }
    }

    async fn write_version(context: &DriverContext, bucket: &str, key: &str, version: BlobVersion) {
        let version_id = Ulid::new();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
    }

    async fn read_dest_version(
        context: &DriverContext,
        bucket: &str,
        key: &str,
        version_id: Ulid,
    ) -> BlobVersion {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob version entry");
        };
        BlobVersion::from_bytes(value.expect("missing blob version").as_ref()).unwrap()
    }

    #[tokio::test]
    async fn materialized_copy_dedups_source_blob() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let source = drive(
            PutObjectOperation::new(put_config(
                realm_id,
                group_id,
                node_id,
                "bucket",
                "source.txt",
                b"hello, world!",
            )),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let result = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id: UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap();

        assert_eq!(result.location, source.location);
        assert_eq!(result.source_version_id, Some(source.version_id));
        assert_eq!(
            result.source_last_modified,
            Some(source.location.created_at)
        );

        let dest_version =
            read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
        assert!(dest_version.is_materialized());
    }

    #[tokio::test]
    async fn reference_copy_materializes_bytes_with_snapshot_binding() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::new()),
        };
        write_version(
            &context,
            "bucket",
            "ref.txt",
            BlobVersion::reference(
                source,
                SourceMetadata {
                    content_length: 15,
                    content_type: Some("text/plain".to_string()),
                    etag: Some("etag-1".to_string()),
                    last_modified: Some(SystemTime::UNIX_EPOCH),
                    source_version: None,
                },
                SystemTime::UNIX_EPOCH,
                UserId::local(Ulid::new(), realm_id),
                SystemTime::UNIX_EPOCH,
            ),
        )
        .await;

        let result = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "ref.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id: UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap();

        let dest_version =
            read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
        assert!(dest_version.is_materialized());
        assert_eq!(
            dest_version
                .source_binding()
                .map(|binding| binding.strategy.clone()),
            Some(StagingStrategy::Snapshot)
        );

        server.abort();
        let _ = server.await;

        let mut blob = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "bucket".to_string(),
                key: "dest.txt".to_string(),
                version_id: None,
                range: None,
                group_id,
                user_identity: UserId::local(Ulid::new(), realm_id),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .blob;
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = blob.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, b"reference-bytes");
    }

    #[tokio::test]
    async fn delete_marker_source_errors() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let version_id = Ulid::new();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "gone.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::deleted(
                    SystemTime::now(),
                    UserId::local(Ulid::new(), realm_id),
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let error = copy_object(
            &context,
            CopyObjectInput {
                source_bucket: "bucket".to_string(),
                source_key: "gone.txt".to_string(),
                source_version_id: Some(version_id),
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                user_id: UserId::local(Ulid::new(), realm_id),
                group_id,
                realm_id,
                node_id,
                quota_ceiling: None,
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, CopyObjectError::Get(GetObjectError::DeleteMarker));
    }
}
