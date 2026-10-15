//! Tests that the broken file cleanup authorizes each object and deletes only unchanged versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{BucketCheckQuery, CleanupQuery, clean_bucket_check, get_bucket_check, visible_page};
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
use aruna_operations::blob::migration::check::clean_broken;
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

#[tokio::test]
async fn listing_hides_rows() {
    // A denied object and a version that is no longer current never show up.
    let test = setup_state().await;
    broken(&test, "open/a.gz", 1, None).await;
    broken(&test, "private/b.gz", 2, None).await;
    broken(&test, "old/c.gz", 3, None).await;
    let head = BlobHeadKey::new(&test.bucket, "old/c.gz")
        .to_bytes()
        .unwrap();
    let newer = CurrentVersionPointer::new(Ulid::from_bytes([9; 16]));
    write(&test, BLOB_HEAD_KEYSPACE, head, newer.to_bytes().unwrap()).await;
    let realm = test.state.get_realm_id();
    let node = test.state.get_node_id();
    let private = object_permission_path(realm, test.group_id, node, &test.bucket, "private/**");
    let restricted = AuthContext {
        path_restrictions: Some(vec![
            PathRestriction {
                pattern: format!("/{realm}/g/{}/**", test.group_id),
                permission: Permission::WRITE,
            },
            PathRestriction {
                pattern: private,
                permission: Permission::DENY,
            },
        ]),
        ..test.auth.clone()
    };

    let response = get_bucket_check(
        State(test.state.clone()),
        Extension(Some(restricted)),
        Path(test.bucket.clone()),
        Query(BucketCheckQuery::default()),
    )
    .await
    .unwrap()
    .0;

    let keys: Vec<_> = response
        .broken
        .iter()
        .map(|file| file.key.as_str())
        .collect();
    assert_eq!(keys, ["open/a.gz"]);
}

#[tokio::test]
async fn cleanup_clears_vanished() {
    // A version deleted after the check only loses its record.
    let test = setup_state().await;
    let gone = broken(&test, "a/gone.gz", 1, None).await;
    let event = (test.state.get_ctx().storage_handle)
        .send_storage_effect(StorageEffect::Delete {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: gone.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));

    let response = clean_bucket_check(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Query(CleanupQuery::default()),
    )
    .await
    .unwrap()
    .0;

    assert_eq!((response.deleted, response.kept), (0, 0));
    let listed = get_bucket_check(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Query(BucketCheckQuery::default()),
    )
    .await
    .unwrap()
    .0;
    assert!(listed.broken.is_empty());
}

/// A token that may write the group but neither read nor write `a/**` and `c/**`.
fn hiding(test: &TestState) -> AuthContext {
    let realm = test.state.get_realm_id();
    let node = test.state.get_node_id();
    let deny = |prefix: &str| PathRestriction {
        pattern: object_permission_path(realm, test.group_id, node, &test.bucket, prefix),
        permission: Permission::DENY,
    };
    let group = PathRestriction {
        pattern: format!("/{realm}/g/{}/**", test.group_id),
        permission: Permission::WRITE,
    };
    AuthContext {
        path_restrictions: Some(vec![group, deny("a/**"), deny("c/**")]),
        ..test.auth.clone()
    }
}

fn cursor_key(next: &str) -> String {
    VersionKey::from_bytes(&hex::decode(next).unwrap())
        .unwrap()
        .key
}

#[tokio::test]
async fn cursor_names_visible() {
    // With one file per page the cursor never names a hidden row, and every visible one shows.
    let test = setup_state().await;
    for (key, seed) in [("a/x.gz", 1), ("b/y.gz", 2), ("c/z.gz", 3), ("d/w.gz", 4)] {
        broken(&test, key, seed, None).await;
    }
    let (mut after, mut listed) = (None, Vec::new());
    loop {
        let query = BucketCheckQuery {
            after: after.clone(),
            limit: Some(1),
        };
        let page = get_bucket_check(
            State(test.state.clone()),
            Extension(Some(hiding(&test))),
            Path(test.bucket.clone()),
            Query(query),
        )
        .await
        .unwrap()
        .0;
        listed.extend(page.broken.into_iter().map(|file| file.key));
        let Some(next) = page.next else { break };
        assert!(["b/y.gz", "d/w.gz"].contains(&cursor_key(&next).as_str()));
        after = Some(next);
    }
    assert_eq!(listed, ["b/y.gz", "d/w.gz"]);

    let template = aruna_operations::s3::object::delete::DeleteObjectInput {
        bucket: test.bucket.clone(),
        key: String::new(),
        version_id: None,
        group_id: test.group_id,
        realm_id: test.state.get_realm_id(),
        node_id: test.state.get_node_id(),
        deleted_by: test.auth.user_id,
    };
    let (ctx, auth) = (test.state.get_ctx(), hiding(&test));
    let first = clean_broken(&ctx, &template, &auth, None, 1, 100)
        .await
        .unwrap();
    let next = first.next.clone().unwrap();
    assert_eq!(VersionKey::from_bytes(&next).unwrap().key, "b/y.gz");
    let second = clean_broken(&ctx, &template, &auth, Some(next), 1, 100)
        .await
        .unwrap();
    assert!(
        second
            .next
            .is_none_or(|next| VersionKey::from_bytes(&next).unwrap().key == "d/w.gz")
    );
    assert_eq!(first.deleted + second.deleted, 2);
}

#[tokio::test]
async fn scan_budget_bounds() {
    // A request stops after its row budget: refused if it found nothing to show, else it
    // returns what it found with the cursor at the last shown file.
    let test = setup_state().await;
    for (key, seed) in [("a/x.gz", 1), ("b/y.gz", 2), ("c/z.gz", 3), ("d/w.gz", 4)] {
        broken(&test, key, seed, None).await;
    }
    let auth = hiding(&test);
    let page = |scan| {
        visible_page(
            &test.state,
            &auth,
            test.group_id,
            &test.bucket,
            None,
            10,
            scan,
        )
    };

    assert!(matches!(
        page(1).await,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let (files, next) = page(2).await.unwrap();
    let keys: Vec<_> = files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(keys, ["b/y.gz"]);
    assert_eq!(
        VersionKey::from_bytes(&next.unwrap()).unwrap().key,
        "b/y.gz"
    );

    let template = aruna_operations::s3::object::delete::DeleteObjectInput {
        bucket: test.bucket.clone(),
        key: String::new(),
        version_id: None,
        group_id: test.group_id,
        realm_id: test.state.get_realm_id(),
        node_id: test.state.get_node_id(),
        deleted_by: test.auth.user_id,
    };
    let ctx = test.state.get_ctx();
    let spent = clean_broken(&ctx, &template, &auth, None, 10, 1)
        .await
        .unwrap();
    assert!(spent.exhausted && spent.next.is_none() && spent.deleted == 0);
    let cleaned = clean_broken(&ctx, &template, &auth, None, 10, 2)
        .await
        .unwrap();
    assert!(!cleaned.exhausted);
    assert_eq!(cleaned.deleted, 1);
    assert_eq!(
        VersionKey::from_bytes(&cleaned.next.unwrap()).unwrap().key,
        "b/y.gz"
    );
}
