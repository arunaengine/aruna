//! Tests that selection downloads refuse anonymous callers and invalid selections up front.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;

fn request(version_id: Option<&str>, count: usize) -> ArchiveRequest {
    ArchiveRequest {
        entries: (0..count)
            .map(|index| ArchiveEntry {
                bucket: "lab-data".to_string(),
                key: format!("runs/{index}.csv"),
                version_id: version_id.map(str::to_string),
            })
            .collect(),
    }
}

#[tokio::test]
async fn refuses_bad_selections() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[17u8; 32]).public();
    let state = Arc::new(
        test_state(
            Arc::new(test_context(storage)),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    let auth = AuthContext {
        user_id: UserId::local(ulid::Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let call = |auth: Option<AuthContext>, body: ArchiveRequest| {
        download_selection(State(state.clone()), Extension(auth), Json(body))
    };
    let anonymous = call(None, request(None, 1)).await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
    let empty = call(Some(auth.clone()), request(None, 0)).await;
    assert!(matches!(empty, Err(ServerError::BadRequestReason(_))));
    let oversized = call(Some(auth.clone()), request(None, 1001)).await;
    assert!(matches!(oversized, Err(ServerError::BadRequestReason(_))));
    let version = call(Some(auth), request(Some("not-a-ulid"), 1)).await;
    assert!(matches!(version, Err(ServerError::BadRequestReason(_))));
}
