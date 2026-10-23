//! Tests that backend import routes demand group admin, validate input and round trip a rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    CreateImportRequest, UpdateImportRequest, create_import, delete_import, get_import,
    get_renames, list_group_imports, run_import, start_renames, update_import,
};
use crate::error::ServerError;
use crate::tests::storage_routing::{TestState, setup_state};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::storage::backend_import::RenameTotal;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::cleanup::CleanupStrategy;
use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
use aruna_core::structs::storage::routing::RoutingTarget;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::routing::GetRoutingOperation;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::collections::HashMap;
use std::time::SystemTime;
use ulid::Ulid;

pub(crate) async fn register_backend(test: &TestState) -> Ulid {
    let backend_id = Ulid::generate();
    let record = GroupStorage {
        backend_id,
        group_id: test.group_id,
        name: "tenant".to_string(),
        kind: GroupBackendKind::S3,
        public_config: HashMap::from([("bucket".to_string(), "data".to_string())]),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        disabled: false,
        cleanup: CleanupStrategy::Retain,
    };
    let event = test
        .state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_id.to_bytes().to_vec().into(),
            value: record.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    backend_id
}

pub(crate) fn request(
    test: &TestState,
    backend_id: Ulid,
    backend_prefix: &str,
) -> CreateImportRequest {
    CreateImportRequest {
        backend_id: backend_id.to_string(),
        backend_prefix: backend_prefix.to_string(),
        bucket: test.bucket.clone(),
        key_prefix: "imported/".to_string(),
        schedule: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: None,
        move_existing: false,
    }
}

#[tokio::test]
async fn requires_group_admin() {
    let test = setup_state().await;
    let other = || Extension(Some(test.other_auth.clone()));
    let group = test.group_id.to_string();
    let rule = (group.clone(), Ulid::generate().to_string());

    let listed = list_group_imports(State(test.state.clone()), other(), Path(group.clone())).await;
    assert!(matches!(listed, Err(ServerError::Forbidden)));
    let body = Json(request(&test, Ulid::generate(), ""));
    let created = create_import(State(test.state.clone()), other(), Path(group), body).await;
    assert!(matches!(created, Err(ServerError::Forbidden)));
    let read = get_import(State(test.state.clone()), other(), Path(rule.clone())).await;
    assert!(matches!(read, Err(ServerError::Forbidden)));
    let body = Json(UpdateImportRequest {
        schedule: None,
        write_pattern: None,
    });
    let updated = update_import(State(test.state.clone()), other(), Path(rule.clone()), body).await;
    assert!(matches!(updated, Err(ServerError::Forbidden)));
    let deleted = delete_import(State(test.state.clone()), other(), Path(rule.clone())).await;
    assert!(matches!(deleted, Err(ServerError::Forbidden)));
    let run = run_import(State(test.state.clone()), other(), Path(rule)).await;
    assert!(matches!(run, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn rejects_bad_input() {
    let test = setup_state().await;
    let backend_id = register_backend(&test).await;
    let auth = || Extension(Some(test.auth.clone()));
    let group = || Path(test.group_id.to_string());

    let folder = Json(request(&test, backend_id, "raw"));
    let result = create_import(State(test.state.clone()), auth(), group(), folder).await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));

    let mut scheduled = request(&test, backend_id, "raw/");
    scheduled.schedule = Some("every night".to_string());
    let result = create_import(State(test.state.clone()), auth(), group(), Json(scheduled)).await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));

    let mut pattern = request(&test, backend_id, "raw/");
    pattern.write_pattern = Some("{dir}{id}".to_string());
    let result = create_import(State(test.state.clone()), auth(), group(), Json(pattern)).await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));

    let mut reserved = request(&test, backend_id, "raw/");
    reserved.write_prefix = Some("_aruna_backend_probe/".to_string());
    let result = create_import(State(test.state.clone()), auth(), group(), Json(reserved)).await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));

    let unknown = Json(request(&test, Ulid::generate(), "raw/"));
    let result = create_import(State(test.state.clone()), auth(), group(), unknown).await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));

    let missing = Path((test.group_id.to_string(), Ulid::generate().to_string()));
    let result = get_import(State(test.state.clone()), auth(), missing).await;
    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn manages_rule() {
    let test = setup_state().await;
    let backend_id = register_backend(&test).await;
    let auth = || Extension(Some(test.auth.clone()));
    let group = test.group_id.to_string();

    let body = Json(request(&test, backend_id, "raw/"));
    let (status, Json(created)) =
        create_import(State(test.state.clone()), auth(), Path(group.clone()), body)
            .await
            .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created.next_run_at, None);
    assert!(created.last_run.is_none());
    let rules = drive(
        GetRoutingOperation::new(test.bucket.clone()),
        &test.state.get_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(rules[0].key_prefix, "imported/");
    assert_eq!(
        rules[0].target,
        RoutingTarget::Backend(BackendRef::Group(backend_id))
    );

    let rule = || Path((group.clone(), created.import_id.clone()));
    let body = |write_pattern: Option<&str>| {
        Json(UpdateImportRequest {
            schedule: Some("0 3 * * *".to_string()),
            write_pattern: write_pattern.map(str::to_string),
        })
    };
    let Json(updated) = update_import(State(test.state.clone()), auth(), rule(), body(None))
        .await
        .unwrap();
    assert_eq!(updated.schedule.as_deref(), Some("0 3 * * *"));
    assert!(updated.next_run_at.is_some());
    // The pattern changes in place, under the same rule id.
    let prefix = Some("{dir}{id}_{name}");
    let Json(updated) = update_import(State(test.state.clone()), auth(), rule(), body(prefix))
        .await
        .unwrap();
    assert_eq!(updated.import_id, created.import_id);
    assert_eq!(updated.write_pattern, "{dir}{id}_{name}");
    let bad = update_import(
        State(test.state.clone()),
        auth(),
        rule(),
        body(Some("{id}")),
    )
    .await;
    assert!(matches!(bad, Err(ServerError::BadRequestMessage(_))));

    let (status, Json(run)) = run_import(State(test.state.clone()), auth(), rule())
        .await
        .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    let again = run_import(State(test.state.clone()), auth(), rule()).await;
    assert!(
        matches!(again, Err(ServerError::Conflict(_))),
        "the first run is still queued"
    );
    let suffix = Some("{dir}{name}~{id}");
    let changed = update_import(State(test.state.clone()), auth(), rule(), body(suffix)).await;
    assert!(
        matches!(changed, Err(ServerError::Conflict(_))),
        "no pattern change while a run is queued"
    );
    let Json(read) = get_import(State(test.state.clone()), auth(), rule())
        .await
        .unwrap();
    assert_eq!(read.last_run.map(|last| last.job_id), Some(run.job_id));

    let listed = list_group_imports(State(test.state.clone()), auth(), Path(group.clone()))
        .await
        .unwrap();
    assert_eq!(listed.imports.len(), 1);
    let status = delete_import(State(test.state.clone()), auth(), rule())
        .await
        .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);
    let gone = get_import(State(test.state.clone()), auth(), rule()).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
}

/// A token of the test realm; its signature is never checked here, as the middleware did that.
pub(crate) fn bearer(test: &TestState) -> Extension<Option<crate::auth::ValidatedBearer>> {
    use aruna_core::structs::identity::auth::TokenClaims;
    let claims = TokenClaims {
        sub: test.auth.user_id.to_string(),
        iss: test.state.get_realm_id().to_base64(),
        iat: 1,
        exp: u64::MAX / 2,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    };
    let key = jsonwebtoken::EncodingKey::from_secret(b"test");
    let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &key).unwrap();
    Extension(Some(crate::auth::ValidatedBearer::new_for_test(token)))
}

#[tokio::test]
async fn starts_renames() {
    let test = setup_state().await;
    let backend_id = register_backend(&test).await;
    let auth = || Extension(Some(test.auth.clone()));
    let group = test.group_id.to_string();
    let body = Json(request(&test, backend_id, "raw/"));
    let (_, Json(created)) =
        create_import(State(test.state.clone()), auth(), Path(group.clone()), body)
            .await
            .unwrap();
    let rule = || Path((group.clone(), created.import_id.clone()));
    // A token limited to some paths could reach files outside them.
    let restricted = || {
        let pattern = format!("/{}/g/{}/**", test.state.get_realm_id(), test.group_id);
        let path = PathRestriction {
            pattern,
            permission: Permission::WRITE,
        };
        let auth = test.auth.clone();
        Extension(Some(AuthContext {
            path_restrictions: Some(vec![path]),
            ..auth
        }))
    };
    let listed = get_renames(
        State(test.state.clone()),
        restricted(),
        rule(),
        Default::default(),
    )
    .await;
    assert!(matches!(listed, Err(ServerError::Forbidden)));
    let started = start_renames(
        State(test.state.clone()),
        restricted(),
        bearer(&test),
        rule(),
    )
    .await;
    assert!(matches!(started, Err(ServerError::Forbidden)));

    let Json(found) = get_renames(
        State(test.state.clone()),
        auth(),
        rule(),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!((found.count, found.files.len()), (0, 0));
    assert!(found.progress.is_none());
    // The rename is a move of the rule's folder, shown as the rule's progress.
    // Without a bearer token nothing could check its revocation later.
    let unsigned = start_renames(State(test.state.clone()), auth(), Extension(None), rule()).await;
    assert!(matches!(unsigned, Err(ServerError::Unauthorized)));
    let (status, Json(started)) =
        start_renames(State(test.state.clone()), auth(), bearer(&test), rule())
            .await
            .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(started.prefix, "imported/");
    // The plan's count of files, none here, is stored for this move.
    let read = StorageEffect::Read {
        key_space: aruna_core::keyspaces::RENAME_TOTAL_KEYSPACE.to_string(),
        key: started.bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    let context = test.state.get_ctx();
    let stored = context.storage_handle.send_storage_effect(read).await;
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(row), ..
    }) = stored
    else {
        panic!("expected the planned total of the rename");
    };
    let planned = RenameTotal {
        started_at_ms: started.progress.started_at_ms,
        total: 0,
    };
    assert_eq!(RenameTotal::from_bytes(&row).unwrap(), planned);
    let Json(found) = get_renames(
        State(test.state.clone()),
        auth(),
        rule(),
        Default::default(),
    )
    .await
    .unwrap();
    assert!(found.progress.is_some());
    // A running rename keeps the pattern its files are read with.
    let body = Json(UpdateImportRequest {
        schedule: None,
        write_pattern: Some("{dir}{id}_{name}".to_string()),
    });
    let changed = update_import(State(test.state.clone()), auth(), rule(), body).await;
    assert!(matches!(changed, Err(ServerError::Conflict(_))));
}

#[tokio::test]
async fn routes_new_uploads() {
    let test = setup_state().await;
    let backend_id = register_backend(&test).await;
    let write_backend = register_backend(&test).await;
    let auth = || Extension(Some(test.auth.clone()));
    let group = || Path(test.group_id.to_string());

    let mut body = request(&test, backend_id, "raw/");
    body.write_backend_id = Some(write_backend.to_string());
    body.write_prefix = Some("uploads/".to_string());
    body.write_pattern = Some("{dir}{name}".to_string());
    let (_, Json(created)) = create_import(State(test.state.clone()), auth(), group(), Json(body))
        .await
        .unwrap();
    assert_eq!(created.write_backend_id, Some(write_backend.to_string()));
    assert_eq!(created.write_prefix.as_deref(), Some("uploads/"));
    assert_eq!(created.write_pattern, "{dir}{name}");
    let rules = drive(
        GetRoutingOperation::new(test.bucket.clone()),
        &test.state.get_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(
        rules[0].target,
        RoutingTarget::Backend(BackendRef::Group(write_backend))
    );

    // A second rule may not read the folder the first one writes to.
    let mut other = request(&test, write_backend, "uploads/x/");
    other.key_prefix = "other/".to_string();
    let result = create_import(State(test.state.clone()), auth(), group(), Json(other)).await;
    assert!(matches!(result, Err(ServerError::Conflict(_))));

    // Defaults stay unset and the pattern keeps the name, adding the id only when it is taken.
    let mut plain = request(&test, backend_id, "elsewhere/");
    plain.key_prefix = "plain/".to_string();
    let (_, Json(created)) = create_import(State(test.state.clone()), auth(), group(), Json(plain))
        .await
        .unwrap();
    assert_eq!(created.write_backend_id, None);
    assert_eq!(created.write_prefix, None);
    assert_eq!(created.write_pattern, "{dir}{name}");
}
