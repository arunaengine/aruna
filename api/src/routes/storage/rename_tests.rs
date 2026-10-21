//! Tests the folder rename routes: access, validation, the preview and a repeated rename.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::storage_routing::{TestState, setup_state};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::storage::blob::{
    BackendRef, BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::format::EncodingClass;
use std::time::SystemTime;
use ulid::Ulid;

/// Stores `key` with one current version.
async fn object(test: &TestState, key: &str) -> Ulid {
    let id = Ulid::generate();
    let version = BlobVersion::materialized(
        [1; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let head = CurrentVersionPointer::new(id);
    let writes = vec![
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            VersionKey::new(&test.bucket, key, id)
                .to_bytes()
                .unwrap()
                .into(),
            version.to_bytes().unwrap().into(),
        ),
        (
            BLOB_HEAD_KEYSPACE.to_string(),
            BlobHeadKey::new(&test.bucket, key)
                .to_bytes()
                .unwrap()
                .into(),
            head.to_bytes().unwrap().into(),
        ),
    ];
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    let event = test
        .state
        .get_ctx()
        .storage_handle
        .send_storage_effect(write)
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    id
}

fn folders(from: &str, to: &str) -> Json<RenamePreviewRequest> {
    Json(RenamePreviewRequest {
        from: from.to_string(),
        to: to.to_string(),
    })
}

fn keys(from: &str, to: &str, keys: &[&str]) -> Json<RenameRequest> {
    Json(RenameRequest {
        from: from.to_string(),
        to: to.to_string(),
        keys: keys.iter().map(ToString::to_string).collect(),
    })
}

#[tokio::test]
async fn refuses_other_callers() {
    let test = setup_state().await;
    let state = || State(test.state.clone());
    let bucket = || Path(test.bucket.clone());
    let missing = preview_rename(state(), Extension(None), bucket(), folders("", "a/")).await;
    assert!(matches!(missing, Err(ServerError::Unauthorized)));
    let other = Extension(Some(test.other_auth.clone()));
    let denied = rename_keys(state(), other, bucket(), keys("", "a/", &["x"])).await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
    // A token limited to some paths could reach keys outside them.
    let restriction = PathRestriction {
        pattern: format!("/{}/**", test.state.get_realm_id()),
        permission: Permission::WRITE,
    };
    let restricted = Extension(Some(AuthContext {
        path_restrictions: Some(vec![restriction]),
        ..test.auth.clone()
    }));
    let limited = preview_rename(state(), restricted, bucket(), folders("", "a/")).await;
    assert!(matches!(limited, Err(ServerError::Forbidden)));
    let auth = Extension(Some(test.auth.clone()));
    let unknown = Path("missing".to_string());
    let gone = preview_rename(state(), auth, unknown, folders("", "a/")).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn refuses_bad_folders() {
    let test = setup_state().await;
    let auth = || Extension(Some(test.auth.clone()));
    let state = || State(test.state.clone());
    let bucket = || Path(test.bucket.clone());
    for (from, to) in [("a", "b/"), ("a/", "a/"), ("", "../b/")] {
        let refused = preview_rename(state(), auth(), bucket(), folders(from, to)).await;
        assert!(
            matches!(refused, Err(ServerError::BadRequestMessage(_))),
            "{from} {to}"
        );
    }
    for body in [
        keys("", "a/", &[]),
        keys("", "a/", &["x", "x"]),
        keys("sub/", "a/", &["x"]),
    ] {
        let refused = rename_keys(state(), auth(), bucket(), body.clone()).await;
        assert!(
            matches!(refused, Err(ServerError::BadRequestMessage(_))),
            "{body:?}"
        );
    }
}

#[tokio::test]
async fn renames_previewed_keys() {
    let test = setup_state().await;
    let auth = || Extension(Some(test.auth.clone()));
    let state = || State(test.state.clone());
    let bucket = || Path(test.bucket.clone());
    let id = object(&test, "x.csv").await;
    let Json(preview) = preview_rename(state(), auth(), bucket(), folders("", "archive/"))
        .await
        .unwrap();
    assert_eq!(preview.bucket, test.bucket);
    assert!(!preview.truncated);
    let [item] = preview.items.as_slice() else {
        panic!("expected one key: {preview:?}")
    };
    assert_eq!(
        (item.from.as_str(), item.to.as_str()),
        ("x.csv", "archive/x.csv")
    );
    assert_eq!(item.current, Some(id.to_string()));
    assert_eq!(item.conflict, None);

    let body = || keys("", "archive/", &["x.csv"]);
    let Json(renamed) = rename_keys(state(), auth(), bucket(), body())
        .await
        .unwrap();
    assert_eq!(renamed.items, preview.items);
    // The old key is gone, so the same rename again is a conflict and changes nothing.
    let again = rename_keys(state(), auth(), bucket(), body()).await;
    assert!(matches!(again, Err(ServerError::Conflict(_))));
    let Json(after) = preview_rename(state(), auth(), bucket(), folders("archive/", "done/"))
        .await
        .unwrap();
    assert_eq!(after.items[0].from, "archive/x.csv");
}
