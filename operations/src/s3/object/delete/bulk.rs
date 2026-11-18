//! Deletes many keys in one request and reports a per-key outcome for the caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::VecDeque;
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq)]
pub struct BulkDeleteEntry {
    pub key: String,
    pub version_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct BulkDeleteInput {
    pub bucket: String,
    pub entries: Vec<BulkDeleteEntry>,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub deleted_by: UserId,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Rows each permanent delete's transaction must still read as given.
    pub guards: Vec<(String, Key, Option<Value>)>,
}

#[derive(Debug, PartialEq)]
pub struct BulkDeleteOutcome {
    pub key: String,
    pub requested_version_id: Option<Ulid>,
    pub result: Result<DeleteObjectResult, DeleteObjectError>,
}

type BulkTurn = Result<Next<Vec<BulkDeleteOutcome>>, DeleteObjectError>;

/// Deletes the entries one after the other. A failed entry only fails its own outcome.
#[derive(Debug, PartialEq)]
pub struct BulkDeleteOperation {
    input: BulkDeleteInput,
    entries: VecDeque<BulkDeleteEntry>,
    running: Option<BulkDeleteEntry>,
    outcomes: Vec<BulkDeleteOutcome>,
}

impl BulkDeleteOperation {
    pub fn new(mut input: BulkDeleteInput) -> Self {
        let entries: VecDeque<_> = std::mem::take(&mut input.entries).into();
        let outcomes = Vec::with_capacity(entries.len());
        Self {
            input,
            entries,
            running: None,
            outcomes,
        }
    }

    fn next_entry(&mut self) -> BulkTurn {
        let Some(entry) = self.entries.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.outcomes)));
        };
        let input = &self.input;
        let operation = DeleteObjectOperation::new(DeleteObjectInput {
            bucket: input.bucket.clone(),
            key: entry.key.clone(),
            version_id: entry.version_id,
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            deleted_by: input.deleted_by,
        })
        .with_restrictions(input.restrictions.clone());
        let operation = match entry.version_id.is_some() && !input.guards.is_empty() {
            true => operation.with_fence(input.guards.clone(), Vec::new()),
            false => operation,
        };
        self.running = Some(entry);
        Ok(Next::Call(IoCall::child(operation)))
    }
}

impl Operation for BulkDeleteOperation {
    type Output = Vec<BulkDeleteOutcome>;
    type Error = DeleteObjectError;

    fn first(&mut self) -> BulkTurn {
        self.next_entry()
    }

    fn next(&mut self, result: IoResult) -> BulkTurn {
        let result = result.child::<DeleteObjectResult, DeleteObjectError>()?;
        if let Some(entry) = self.running.take() {
            self.outcomes.push(BulkDeleteOutcome {
                key: entry.key,
                requested_version_id: entry.version_id,
                result,
            });
        }
        self.next_entry()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig};
    use aruna_core::structs::storage::routing::RoutingSnapshot;
    use aruna_net::NetHandle;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::{TempDir, tempdir};

    async fn test_context() -> (TempDir, DriverContext) {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();

        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            context,
        )
        .await
        .unwrap();
    }

    fn batch(entries: Vec<BulkDeleteEntry>) -> BulkDeleteInput {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        BulkDeleteInput {
            bucket: "mybucket".to_string(),
            entries,
            group_id: Ulid::from_bytes([2; 16]),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            deleted_by: UserId::nil(realm_id),
            restrictions: None,
            guards: Vec::new(),
        }
    }

    #[test]
    fn empty_batch_finishes() {
        let mut operation = BulkDeleteOperation::new(batch(Vec::new()));
        assert_eq!(operation.first(), Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn outcomes_follow_entries() {
        // A failed entry stays in its place and the next entry still runs.
        let version = Ulid::from_bytes([4; 16]);
        let mut operation = BulkDeleteOperation::new(batch(vec![
            BulkDeleteEntry {
                key: "first".to_string(),
                version_id: Some(version),
            },
            BulkDeleteEntry {
                key: "second".to_string(),
                version_id: None,
            },
        ]));
        let missing = || {
            let failed = Err::<DeleteObjectResult, _>(DeleteObjectError::NoSuchVersion);
            IoResult::Child(ChildOutcome::new(failed))
        };
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            operation.next(missing()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let Ok(Next::Done(outcomes)) = operation.next(missing()) else {
            panic!("the last entry ends the batch");
        };
        let keys: Vec<_> = outcomes
            .iter()
            .map(|outcome| (outcome.key.as_str(), outcome.requested_version_id))
            .collect();
        assert_eq!(keys, [("first", Some(version)), ("second", None)]);
    }

    #[tokio::test]
    async fn mixed_batch_processed() {
        let (_temp, context) = test_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let group_id = Ulid::generate();
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

        let missing_version = Ulid::generate();
        let outcomes = drive(
            BulkDeleteOperation::new(BulkDeleteInput {
                bucket: "mybucket".to_string(),
                entries: vec![
                    BulkDeleteEntry {
                        key: "present.txt".to_string(),
                        version_id: None,
                    },
                    BulkDeleteEntry {
                        key: "absent.txt".to_string(),
                        version_id: None,
                    },
                    BulkDeleteEntry {
                        key: "present.txt".to_string(),
                        version_id: Some(missing_version),
                    },
                ],
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
                restrictions: None,
                guards: Vec::new(),
            }),
            &context,
        )
        .await
        .unwrap();

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
    async fn failures_isolated() {
        let (_temp, context) = test_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        seed_object(
            &context, "mybucket", "keep.txt", user_id, group_id, realm_id, node_id,
        )
        .await;

        let outcomes = drive(
            BulkDeleteOperation::new(BulkDeleteInput {
                bucket: "mybucket".to_string(),
                entries: vec![
                    BulkDeleteEntry {
                        key: "keep.txt".to_string(),
                        version_id: Some(Ulid::generate()),
                    },
                    BulkDeleteEntry {
                        key: "keep.txt".to_string(),
                        version_id: None,
                    },
                ],
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
                restrictions: None,
                guards: Vec::new(),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0].result,
            Err(DeleteObjectError::NoSuchVersion)
        ));
        assert!(outcomes[1].result.as_ref().unwrap().delete_marker);
    }
}
