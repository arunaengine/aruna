//! Builds the put configuration of a small object for tests that need a blob store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::object::put::PutObjectConfig;
use crate::s3::object::put::PutObjectInput;
use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use bytes::Bytes;
use ulid::Ulid;

/// A put of `data` at `bucket`/`key` by a fresh user of `realm_id`.
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
            body: Some(BackendStream::once(Bytes::from_static(data))),
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
