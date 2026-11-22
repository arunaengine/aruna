//! Builds pure fixtures for tests: fresh job ids, reference refreshes and group backends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub use aruna_ops_identity::testing::{fresh_job_id, group_storage};

use aruna_core::metadata::{MetadataEventPayload, MetadataEventRecord};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::time::SystemTime;
use ulid::Ulid;

/// A refresh of `version_id` at `bucket`/`key` with `metadata`, by node 4 for a user of realm 9.
pub fn reference_refresh(
    bucket: String,
    key: String,
    version_id: Ulid,
    metadata: aruna_core::structs::execution::source_access::SourceMetadata,
    refreshed_at: SystemTime,
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

/// A default backend import rule on `bucket` under `raw/`.
pub fn import_rule() -> BackendImport {
    BackendImport {
        import_id: Ulid::from_parts(2, 2),
        group_id: Ulid::from_parts(1, 1),
        backend_id: Ulid::from_parts(3, 3),
        backend_prefix: "raw/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: DEFAULT_PATTERN.to_string(),
    }
}

/// A scaffold metadata creation event for `datasets/raw` at `updated_at_ms`.
pub fn metadata_event(event_id: Ulid, updated_at_ms: u64) -> MetadataEventRecord {
    let realm_id = RealmId::from_bytes([1; 32]);
    let group_id = Ulid::from_parts(1, 1);
    let document_id = Ulid::from_parts(1, 2);
    let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
    MetadataEventRecord {
        event_id,
        record: MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/raw".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/raw",
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![node_id],
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: event_id,
            last_event_id: event_id,
        },
        user_id: aruna_core::user::id::UserId::local(Ulid::from_parts(1, 3), realm_id),
        node_id,
        payload: MetadataEventPayload::Scaffold {
            name: "raw".to_string(),
            description: "raw state".to_string(),
            date_published: "2026-07-24".to_string(),
            license: None,
        },
        occurred_at_ms: updated_at_ms,
    }
}
