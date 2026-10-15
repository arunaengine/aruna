//! Tests that the broken file cleanup authorizes each object and deletes only unchanged versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{CleanupQuery, clean_bucket_check};
use crate::error::ServerError;
use crate::tests::storage_routing::{TestState, setup_state};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BROKEN_VERSION_KEYSPACE};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::storage::blob::{
    BackendRef, BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::broken::{BrokenReason, BrokenVersion};
use aruna_core::structs::storage::format::EncodingClass;
use axum::Extension;
use axum::extract::{Path, Query, State};
use std::time::SystemTime;
use ulid::Ulid;

async fn write(test: &TestState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = (test.state.get_ctx().storage_handle)
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn read(test: &TestState, key: &VersionKey) -> Option<BlobVersion> {
    let event = (test.state.get_ctx().storage_handle)
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected storage event {event:?}")
    };
    value.map(|value| BlobVersion::from_bytes(value.as_ref()).unwrap())
}

/// Stores a current version of `key` and records it broken with the record `seen`.
async fn broken(test: &TestState, key: &str, seed: u8, seen: Option<BlobVersion>) -> VersionKey {
    let version = BlobVersion::materialized(
        [seed; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        test.auth.user_id,
        None,
    );
    let id = Ulid::from_bytes([seed; 16]);
    let version_key = VersionKey::new(&test.bucket, key, id);
    let key_bytes = version_key.to_bytes().unwrap();
    write(
        test,
        BLOB_VERSIONS_KEYSPACE,
        key_bytes.clone(),
        version.to_bytes().unwrap(),
    )
    .await;
    let head = BlobHeadKey::new(&test.bucket, key).to_bytes().unwrap();
    let pointer = CurrentVersionPointer::new(id).to_bytes().unwrap();
    write(test, BLOB_HEAD_KEYSPACE, head, pointer).await;
    let row = BrokenVersion {
        reason: BrokenReason::MissingBlob,
        version: seen.unwrap_or(version),
        found_at_ms: 1,
    };
    write(
        test,
        BROKEN_VERSION_KEYSPACE,
        key_bytes,
        row.to_bytes().unwrap(),
    )
    .await;
    version_key
}

#[tokio::test]
async fn cleanup_checks_objects() {
    let test = setup_state().await;
    let deleted = broken(&test, "a/lost.gz", 1, None).await;
    // The record changed after the check, so the version stays.
    let mut stale = BlobVersion::materialized(
        [2; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        test.auth.user_id,
        None,
    );
    stale
        .metadata
        .insert("note".to_string(), "before".to_string());
    let changed = broken(&test, "b/changed.gz", 2, Some(stale)).await;
    // A token that denies one object path leaves that object alone.
    let denied = broken(&test, "c/denied.gz", 3, None).await;
    let realm = test.state.get_realm_id();
    let node = test.state.get_node_id();
    let object = object_permission_path(realm, test.group_id, node, &test.bucket, "c/denied.gz");
    let restricted = AuthContext {
        path_restrictions: Some(vec![
            PathRestriction {
                pattern: format!("/{realm}/g/{}/**", test.group_id),
                permission: Permission::WRITE,
            },
            PathRestriction {
                pattern: object,
                permission: Permission::DENY,
            },
        ]),
        ..test.auth.clone()
    };

    let response = clean_bucket_check(
        State(test.state.clone()),
        Extension(Some(restricted)),
        Path(test.bucket.clone()),
        Query(CleanupQuery::default()),
    )
    .await
    .unwrap()
    .0;

    assert_eq!(
        (response.deleted, response.kept, response.denied),
        (1, 1, 1)
    );
    assert!(read(&test, &deleted).await.is_none());
    assert!(read(&test, &changed).await.is_some());
    assert!(read(&test, &denied).await.is_some());

    let refused = clean_bucket_check(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path(test.bucket.clone()),
        Query(CleanupQuery::default()),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
}
