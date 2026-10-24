//! Tests the organize routes: access, validation, paged previews and the job controls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::storage::imports::create_import;
use crate::routes::storage::imports::tests::{bearer, register_backend, request};
use crate::tests::storage_routing::{TestState, setup_state};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
use aruna_core::structs::identity::auth::{PathRestriction, Permission};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};

/// A rule on the test bucket whose uploads go to the bucket root, and its path.
async fn rule(test: &TestState) -> (String, String) {
    let backend_id = register_backend(test).await;
    let mut body = request(test, backend_id, "raw/");
    body.key_prefix = String::new();
    let auth = Extension(Some(test.auth.clone()));
    let group = Path(test.group_id.to_string());
    let (_, Json(created)) = create_import(State(test.state.clone()), auth, group, Json(body))
        .await
        .unwrap();
    (test.group_id.to_string(), created.import_id)
}

/// Stores `count` keys `f{index}.faa.gz` at the bucket root and one taken new key.
async fn keys(test: &TestState, count: usize) {
    let head = CurrentVersionPointer::new(Ulid::from_parts(10, 1))
        .to_bytes()
        .unwrap();
    let names = (0..count)
        .map(|index| format!("f{index:04}.faa.gz"))
        .chain(["proteins/f0000.faa.gz".to_string()]);
    let writes = names
        .map(|key| {
            let key = BlobHeadKey::new(test.bucket.clone(), key)
                .to_bytes()
                .unwrap();
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                key.into(),
                head.clone().into(),
            )
        })
        .collect();
    let event = test
        .state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

fn moving(page: PageRequest) -> Json<MovePreviewRequest> {
    Json(MovePreviewRequest {
        request: MoveRequest {
            pattern: "*.faa.gz".to_string(),
            target: "proteins/{name}".to_string(),
            from: None,
            subfolders: false,
        },
        page,
    })
}

#[tokio::test]
async fn refuses_other_callers() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let path = || Path(ids.clone());
    let other = || Extension(Some(test.other_auth.clone()));
    let restricted = || {
        let pattern = format!("/{}/g/{}/**", test.state.get_realm_id(), test.group_id);
        let restriction = PathRestriction {
            pattern,
            permission: Permission::WRITE,
        };
        Extension(Some(AuthContext {
            path_restrictions: Some(vec![restriction]),
            ..test.auth.clone()
        }))
    };
    for auth in [other(), restricted()] {
        let state = || State(test.state.clone());
        let page = preview_move(
            state(),
            auth.clone(),
            path(),
            moving(PageRequest::default()),
        );
        assert!(matches!(page.await, Err(ServerError::Forbidden)));
        let body = Json(PageRequest::default());
        let page = preview_duplicates(state(), auth.clone(), path(), body);
        assert!(matches!(page.await, Err(ServerError::Forbidden)));
        let job = get_job(state(), auth.clone(), path()).await;
        assert!(matches!(job, Err(ServerError::Forbidden)));
        let (group, rule) = ids.clone();
        let action = Path((group, rule, "abort".to_string()));
        let control = control_job(state(), auth.clone(), action).await;
        assert!(matches!(control, Err(ServerError::Forbidden)));
        let started = start_duplicates(state(), auth, bearer(&test), path()).await;
        assert!(matches!(started, Err(ServerError::Forbidden)));
    }
    let anonymous = get_job(State(test.state.clone()), Extension(None), path()).await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn pages_move_preview() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let path = || Path(ids.clone());
    keys(&test, 250).await;
    let auth = || Extension(Some(test.auth.clone()));
    let state = || State(test.state.clone());

    let (mut after, mut items) = (None, Vec::new());
    loop {
        let page = PageRequest {
            filter: None,
            limit: Some(100),
            after,
        };
        let Json(found) = preview_move(state(), auth(), path(), moving(page))
            .await
            .unwrap();
        assert_eq!((found.total, found.complete), (250, true));
        items.extend(found.items);
        match found.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(items.len(), 250);
    assert!(items[0].conflict.is_some(), "{:?}", items[0]);
    assert!(items[1..].iter().all(|item| item.conflict.is_none()));

    let invalid = [
        PageRequest {
            limit: Some(0),
            ..PageRequest::default()
        },
        PageRequest {
            after: Some("%%".to_string()),
            ..PageRequest::default()
        },
    ];
    for page in invalid {
        let result = preview_move(state(), auth(), path(), moving(page)).await;
        assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
    }
    let mut bad = moving(PageRequest::default());
    bad.request.target = "proteins/".to_string();
    let result = preview_move(state(), auth(), path(), bad).await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn controls_jobs() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let path = || Path(ids.clone());
    keys(&test, 3).await;
    let auth = || Extension(Some(test.auth.clone()));
    let state = || State(test.state.clone());
    let action = |action: &str| {
        let (group, rule) = ids.clone();
        Path((group, rule, action.to_string()))
    };
    let Json(none) = get_job(state(), auth(), path()).await.unwrap();
    assert!(none.is_none());
    let body = || Json(moving(PageRequest::default()).0.request);

    let unsigned = start_move(state(), auth(), Extension(None), path(), body()).await;
    assert!(matches!(unsigned, Err(ServerError::Unauthorized)));
    let (status, Json(started)) = start_move(state(), auth(), bearer(&test), path(), body())
        .await
        .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!((started.kind.as_str(), started.total), ("move", 3));
    let again = start_duplicates(state(), auth(), bearer(&test), path()).await;
    assert!(matches!(
        again,
        Err(ServerError::Refused(StatusCode::CONFLICT, "job_running", _))
    ));

    let Json(paused) = control_job(state(), auth(), action("pause")).await.unwrap();
    assert_eq!(paused.state, "paused");
    let Json(aborted) = control_job(state(), auth(), action("abort")).await.unwrap();
    assert_eq!(
        (aborted.state.as_str(), aborted.controls),
        ("aborted", false)
    );
    let ended = control_job(state(), auth(), action("resume")).await;
    assert!(matches!(
        ended,
        Err(ServerError::Refused(StatusCode::CONFLICT, "no_job", _))
    ));
    let unknown = control_job(state(), auth(), action("stop")).await;
    assert!(matches!(unknown, Err(ServerError::BadRequestMessage(_))));

    // A new job may start after the abort.
    let (_, Json(next)) = start_duplicates(state(), auth(), bearer(&test), path())
        .await
        .unwrap();
    assert_eq!(next.kind, "duplicates");
    let Json(shown) = get_job(state(), auth(), path()).await.unwrap();
    assert_eq!(shown.map(|job| job.kind), Some("duplicates".to_string()));
    let Json(status) = link_status(state(), auth(), path()).await.unwrap();
    assert_eq!((status.files, status.conflicts.len()), (None, 0));
}

#[tokio::test]
async fn hides_unmapped_failures() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let context = test.state.get_ctx();
    let group = Ulid::from_string(&ids.0).unwrap();
    let import = Ulid::from_string(&ids.1).unwrap();
    let rule = aruna_operations::groups::imports::read_import(&context, group, import)
        .await
        .unwrap()
        .unwrap();
    // A file under the rule's folder has keys to check; one outside it has none to show.
    let inside = file_keys(&context, (&rule, &[], &[]), "raw/a.faa.gz")
        .await
        .unwrap();
    assert_eq!(inside, ["a.faa.gz", "a.faa.gz"]);
    let outside = file_keys(&context, (&rule, &[], &[]), "elsewhere/a")
        .await
        .unwrap();
    assert!(outside.is_empty());
    // A pending write names its key as well.
    let id = Ulid::generate();
    let intent = aruna_core::structs::storage::backend_import::WriteIntent {
        key: "secret/a.faa.gz".to_string(),
        version_id: id,
        backend: aruna_core::structs::storage::blob::BackendRef::Group(rule.backend_id),
        created_by: test.auth.user_id,
        written: None,
        mover: None,
    };
    let key = aruna_core::structs::storage::backend_import::intent_key(
        rule.import_id,
        "raw/a.faa.gz",
        id,
    );
    let write = StorageEffect::Write {
        key_space: aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE.to_string(),
        key,
        value: intent.to_bytes().unwrap().into(),
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(write).await;
    let pending = file_keys(&context, (&rule, &[], &[]), "raw/a.faa.gz")
        .await
        .unwrap();
    assert!(pending.contains(&"secret/a.faa.gz".to_string()));
}

#[tokio::test]
async fn shows_rename_total() {
    use aruna_core::keyspaces::{
        COMPRESSION_MIGRATION_KEYSPACE, RENAME_MOVE_KEYSPACE, RENAME_TOTAL_KEYSPACE,
    };
    use aruna_core::structs::storage::backend_import::{RenameRequest, RenameTotal, TokenGrant};
    use aruna_core::structs::storage::format::{Compression, CompressionMigration};
    let test = setup_state().await;
    let ids = rule(&test).await;
    let context = test.state.get_ctx();
    let put = |space: &'static str, value: Vec<u8>| {
        let key = test.bucket.as_bytes().to_vec().into();
        crate::tests::routes::write_doc(&context, space, key, value.into())
    };
    // A running rename of the move started at 7.
    let mut record = CompressionMigration::new(Compression::Off, 7);
    record.moving = true;
    let request = RenameRequest {
        started_at_ms: 7,
        auth: test.auth.clone(),
        node_id: test.state.get_node_id(),
        grant: TokenGrant {
            token_hash: "token".to_string(),
            issuer: test.state.get_realm_id(),
            issued_at_secs: 1,
            expires_at_secs: u64::MAX,
            via: None,
        },
        paused: false,
        aborted_at_ms: None,
    };
    put(COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()).await;
    put(RENAME_MOVE_KEYSPACE, request.to_bytes().unwrap()).await;
    let shown = || async {
        let auth = Extension(Some(test.auth.clone()));
        let Json(job) = get_job(State(test.state.clone()), auth, Path(ids.clone()))
            .await
            .unwrap();
        let job = job.unwrap();
        (job.kind, job.total, job.counted)
    };
    // A rename started without a stored total, as before totals were kept, counts none.
    assert_eq!(shown().await, ("rename".to_string(), 0, false));
    // The total of an older move never counts for a newer one; the move's own does.
    for (started_at_ms, total, counted) in [(6, 0, false), (7, 40, true)] {
        let row = RenameTotal {
            started_at_ms,
            total: 40,
        };
        put(RENAME_TOTAL_KEYSPACE, row.to_bytes().unwrap()).await;
        assert_eq!(shown().await, ("rename".to_string(), total, counted));
    }
    // A pause answers the same total.
    let path = Path((ids.0.clone(), ids.1.clone(), "pause".to_string()));
    let auth = Extension(Some(test.auth.clone()));
    let Json(paused) = control_job(State(test.state.clone()), auth, path)
        .await
        .unwrap();
    assert_eq!((paused.total, paused.counted), (40, true));
}
