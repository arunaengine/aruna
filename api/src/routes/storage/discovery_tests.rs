//! Tests the discovery routes: access, validation and the stored rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::storage::imports::create_import;
use crate::routes::storage::imports::tests::{register_backend, request};
use crate::tests::storage_routing::{TestState, setup_state};
use aruna_core::structs::identity::auth::{PathRestriction, Permission};

async fn rule(test: &TestState) -> (String, String) {
    let backend_id = register_backend(test).await;
    let body = request(test, backend_id, "raw/");
    let auth = Extension(Some(test.auth.clone()));
    let group = Path(test.group_id.to_string());
    let (_, Json(created)) = create_import(State(test.state.clone()), auth, group, Json(body))
        .await
        .unwrap();
    (test.group_id.to_string(), created.import_id)
}

fn rules(pairs: &[(&str, &str)]) -> Json<DiscoveryRules> {
    Json(DiscoveryRules {
        rules: pairs
            .iter()
            .map(|(pattern, folder)| DiscoveryRuleBody {
                pattern: pattern.to_string(),
                folder: folder.to_string(),
            })
            .collect(),
    })
}

#[tokio::test]
async fn stores_rules() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let path = || Path(ids.clone());
    let auth = || Extension(Some(test.auth.clone()));
    let state = || State(test.state.clone());
    let Json(empty) = get_discovery(state(), auth(), path()).await.unwrap();
    assert!(empty.rules.is_empty());
    let body = rules(&[("*.faa.gz", "proteins/"), ("*.fna.gz", "genomes/")]);
    let Json(stored) = put_discovery(state(), auth(), path(), body.clone())
        .await
        .unwrap();
    assert_eq!(stored, body.0);
    let Json(read) = get_discovery(state(), auth(), path()).await.unwrap();
    assert_eq!(read, body.0);
    // An empty list removes every rule.
    put_discovery(state(), auth(), path(), rules(&[]))
        .await
        .unwrap();
    let Json(cleared) = get_discovery(state(), auth(), path()).await.unwrap();
    assert!(cleared.rules.is_empty());
}

#[tokio::test]
async fn refuses_bad_rules() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let path = || Path(ids.clone());
    let auth = || Extension(Some(test.auth.clone()));
    let state = || State(test.state.clone());
    for body in [
        rules(&[("*.faa.gz", "proteins")]),
        rules(&[("a/b", "x/")]),
        rules(&[("[a", "x/")]),
        rules(&[("*.gz", "x/"), ("*.faa.gz", "y/")]),
    ] {
        let refused = put_discovery(state(), auth(), path(), body.clone()).await;
        assert!(
            matches!(refused, Err(ServerError::BadRequestMessage(_))),
            "{body:?}"
        );
    }
    let missing = Path((ids.0.clone(), Ulid::generate().to_string()));
    let gone = get_discovery(state(), auth(), missing).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
    let bad = Path((ids.0.clone(), "not-a-ulid".to_string()));
    let invalid = get_discovery(state(), auth(), bad).await;
    assert!(matches!(invalid, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn refuses_other_callers() {
    let test = setup_state().await;
    let ids = rule(&test).await;
    let path = || Path(ids.clone());
    let state = || State(test.state.clone());
    let other = || Extension(Some(test.other_auth.clone()));
    let read = get_discovery(state(), other(), path()).await;
    assert!(matches!(read, Err(ServerError::Forbidden)));
    let write = put_discovery(state(), other(), path(), rules(&[])).await;
    assert!(matches!(write, Err(ServerError::Forbidden)));
    let preview = DiscoveryPreviewRequest {
        rules: Vec::new(),
        after: None,
    };
    let shown = preview_rules(state(), other(), path(), Json(preview.clone())).await;
    assert!(matches!(shown, Err(ServerError::Forbidden)));
    let anonymous = get_discovery(state(), Extension(None), path()).await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
    // A token limited to some paths could place keys outside them.
    let pattern = format!("/{}/g/{}/**", test.state.get_realm_id(), test.group_id);
    let restricted = Extension(Some(AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern,
            permission: Permission::WRITE,
        }]),
        ..test.auth.clone()
    }));
    let write = put_discovery(state(), restricted.clone(), path(), rules(&[])).await;
    assert!(matches!(write, Err(ServerError::Forbidden)));
    let shown = preview_rules(state(), restricted, path(), Json(preview)).await;
    assert!(matches!(shown, Err(ServerError::Forbidden)));
}
