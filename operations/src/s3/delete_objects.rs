use crate::driver::{DriverContext, drive};
use crate::s3::delete_object::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use aruna_core::structs::RealmId;
use aruna_core::types::{GroupId, NodeId, UserId};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteObjectsEntry {
    pub key: String,
    pub version_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct DeleteObjectsInput {
    pub bucket: String,
    pub entries: Vec<DeleteObjectsEntry>,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub deleted_by: UserId,
}

#[derive(Debug)]
pub struct DeleteObjectsEntryOutcome {
    pub key: String,
    pub requested_version_id: Option<Ulid>,
    pub result: Result<DeleteObjectResult, DeleteObjectError>,
}

pub async fn delete_objects(
    context: &DriverContext,
    input: DeleteObjectsInput,
) -> Vec<DeleteObjectsEntryOutcome> {
    let mut outcomes = Vec::with_capacity(input.entries.len());
    for entry in input.entries {
        let result = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: input.bucket.clone(),
                key: entry.key.clone(),
                version_id: entry.version_id,
                group_id: input.group_id,
                realm_id: input.realm_id,
                node_id: input.node_id,
                deleted_by: input.deleted_by,
            }),
            context,
        )
        .await
        .and_then(|output| output.transpose())
        .and_then(|result| result.ok_or(DeleteObjectError::DeleteObjectFailed));

        outcomes.push(DeleteObjectsEntryOutcome {
            key: entry.key,
            requested_version_id: entry.version_id,
            result,
        });
    }
    outcomes
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::drive;
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{Backend, BackendConfig, RealmId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::{TempDir, tempdir};

    async fn test_context() -> (TempDir, DriverContext) {
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

    async fn seed_object(
        context: &DriverContext,
        bucket: &str,
        key: &str,
        user_id: UserId,
        group_id: GroupId,
        realm_id: RealmId,
        node_id: NodeId,
    ) {
        drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                quota_ceiling: None,
            }),
            context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn delete_objects_processes_mixed_batch() {
        let (_temp, context) = test_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        seed_object(
            &context,
            "mybucket",
            "present.txt",
            user_id,
            group_id,
            realm_id,
            node_id,
        )
        .await;

        let missing_version = Ulid::new();
        let outcomes = delete_objects(
            &context,
            DeleteObjectsInput {
                bucket: "mybucket".to_string(),
                entries: vec![
                    DeleteObjectsEntry {
                        key: "present.txt".to_string(),
                        version_id: None,
                    },
                    DeleteObjectsEntry {
                        key: "absent.txt".to_string(),
                        version_id: None,
                    },
                    DeleteObjectsEntry {
                        key: "present.txt".to_string(),
                        version_id: Some(missing_version),
                    },
                ],
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
            },
        )
        .await;

        assert_eq!(outcomes.len(), 3);

        assert_eq!(outcomes[0].key, "present.txt");
        assert_eq!(outcomes[0].requested_version_id, None);
        assert!(outcomes[0].result.as_ref().unwrap().delete_marker);

        assert_eq!(outcomes[1].key, "absent.txt");
        assert!(outcomes[1].result.as_ref().unwrap().delete_marker);

        assert_eq!(outcomes[2].requested_version_id, Some(missing_version));
        assert!(matches!(
            outcomes[2].result,
            Err(DeleteObjectError::NoSuchVersion)
        ));
    }

    #[tokio::test]
    async fn delete_objects_isolates_failures() {
        let (_temp, context) = test_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let group_id = Ulid::new();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        seed_object(
            &context, "mybucket", "keep.txt", user_id, group_id, realm_id, node_id,
        )
        .await;

        let outcomes = delete_objects(
            &context,
            DeleteObjectsInput {
                bucket: "mybucket".to_string(),
                entries: vec![
                    DeleteObjectsEntry {
                        key: "keep.txt".to_string(),
                        version_id: Some(Ulid::new()),
                    },
                    DeleteObjectsEntry {
                        key: "keep.txt".to_string(),
                        version_id: None,
                    },
                ],
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
            },
        )
        .await;

        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0].result,
            Err(DeleteObjectError::NoSuchVersion)
        ));
        assert!(outcomes[1].result.as_ref().unwrap().delete_marker);
    }
}
