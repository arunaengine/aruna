//! Builds pure fixtures for tests: fresh job ids, reference refreshes and group backends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id_mint::IdMint;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structured_id::{BucketId, ClockHealthError, PlacementHandle};
use aruna_core::time::unix_timestamp_millis;

/// A fresh job id for `(handle, bucket)` at the current time, as a submit would mint it.
pub fn fresh_job_id(handle: PlacementHandle, bucket: BucketId) -> Result<JobId, ClockHealthError> {
    let mut ids = IdMint::new(ulid::Ulid::generate().random());
    crate::jobs::submit::mint_job_id(handle, bucket, &mut ids, unix_timestamp_millis())
}

/// A refresh of `version_id` at `bucket`/`key` with `metadata`, by node 4 for a user of realm 9.
pub fn reference_refresh(
    bucket: String,
    key: String,
    version_id: ulid::Ulid,
    metadata: aruna_core::structs::execution::source_access::SourceMetadata,
    refreshed_at: std::time::SystemTime,
) -> crate::s3::object::metadata::ReferenceRefresh {
    crate::s3::object::metadata::ReferenceRefresh {
        bucket,
        key,
        version_id,
        metadata: Some(metadata),
        refreshed_at,
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        auth_context: aruna_core::structs::identity::auth::AuthContext::new(
            Default::default(),
            RealmId([9u8; 32]),
        ),
    }
}

/// An enabled or `disabled` S3 backend of group 1 that keeps its data on removal.
pub fn group_storage(
    backend_id: ulid::Ulid,
    disabled: bool,
) -> aruna_core::structs::storage::group_backend::GroupStorage {
    aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id,
        group_id: ulid::Ulid::from_bytes([1u8; 16]),
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: std::collections::HashMap::new(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        disabled,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    }
}
