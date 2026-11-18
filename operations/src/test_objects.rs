//! Seeds buckets and objects for tests that need a blob store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::s3::object::put::PutObjectConfig;
use crate::s3::object::put::PutObjectInput;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::egress::EgressPolicy;
use aruna_core::id::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::Backend;
use aruna_core::structs::storage::blob::BackendConfig;
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::GroupId;
use aruna_net::NetHandle;
use aruna_storage::storage;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use ulid::Ulid;

/// `context` with a file system blob backend below `root` on the same storage.
pub async fn with_blob(context: &DriverContext, root: &std::path::Path) -> DriverContext {
    let storage_handle = context.storage_handle.clone();
    let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
        .await
        .unwrap();
    let config = BackendConfig {
        backend_type: Backend::FileSystem,
        bucket_prefix: Some("aruna_".to_string()),
        max_bucket_size: Some(100000),
        multipart_bucket: Some("multipart".to_string()),
        root: root.to_str().unwrap().to_string(),
        service_config: HashMap::new(),
        timeouts: Default::default(),
    };
    let policy = EgressPolicy::loopback();
    let blob = BlobHandler::with_egress(config, storage_handle, net_handle, policy).await;
    let blob = blob.unwrap();
    let (http, policy) = blob.repository_client();
    DriverContext {
        blob_handle: Some(blob),
        outbound_handle: Some(aruna_outbound::OutboundHandle::new(http, policy)),
        ..context.clone()
    }
}

pub fn put_config(
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    bucket: &str,
    key: &str,
    data: &'static [u8],
) -> PutObjectConfig {
    PutObjectConfig {
        user_id: UserId::local(Ulid::generate(), realm_id),
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
        preassigned_version_id: None,
        quota_ceiling: None,
        routing: aruna_core::structs::storage::routing::RoutingSnapshot::single(group_id),
    }
}

pub async fn seed_bucket(
    context: &DriverContext,
    bucket: &str,
    group_id: GroupId,
    user_id: UserId,
    policies: Vec<aruna_core::structs::placement::policy::PlacementPolicyRef>,
) {
    let info = aruna_core::structs::storage::blob::BucketInfo {
        group_id,
        created_at: std::time::UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: policies,
        placement_policy_generation: 1,
        compression: Compression::Off,
        distribution: None,
    };
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
}
