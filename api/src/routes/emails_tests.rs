//! Tests that email opt-ins need a realm profile and a verified address and never show it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::structs::execution::email::{RealmEmailProfile, SmtpTls};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use ulid::Ulid;

/// Stores the realm config, with an email profile when `email` is set.
async fn store_config(
    context: &aruna_operations::driver::DriverContext,
    actor: &Actor,
    email: bool,
) {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.email = email.then(|| RealmEmailProfile {
        revision: Ulid::generate(),
        host: "smtp.example.org".to_string(),
        port: None,
        tls: SmtpTls::Starttls,
        username: None,
        from: "aruna@example.org".to_string(),
        password: None,
        changed_by: actor.user_id,
        changed_at_ms: 1,
    });
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let write = StorageEffect::Write {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        value: config.to_bytes(actor).unwrap().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;
}

fn request(address: &str) -> Json<EmailRequest> {
    Json(EmailRequest {
        address: address.to_string(),
        categories: BTreeSet::from(["share.upload".to_string()]),
    })
}

#[tokio::test]
async fn route_needs_verification() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
    let context = Arc::new(test_context(storage));
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let user = User {
        user_id,
        name: "Ada".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: [(
            EMAIL_HASH_ATTRIBUTE.to_string(),
            email_hash("ada@example.org"),
        )]
        .into(),
    };
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: user_id.to_bytes().into(),
            value: user.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        }))
        .await;
    let state = Arc::new(
        test_state(
            context.clone(),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    let auth = Some(AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    });
    let set = |address: &'static str| {
        set_email(
            State(state.clone()),
            Extension(auth.clone()),
            request(address),
        )
    };

    let unreadable = set("ada@example.org").await;
    assert!(matches!(
        unreadable,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    store_config(&context, &actor, false).await;
    let unconfigured = set("ada@example.org").await;
    assert!(matches!(unconfigured, Err(ServerError::Conflict(_))));
    store_config(&context, &actor, true).await;
    let foreign = set("eve@example.org").await;
    assert!(matches!(foreign, Err(ServerError::BadRequestReason(_))));
    let Json(created) = set("ada@example.org").await.unwrap();
    assert_eq!(
        created.categories,
        BTreeSet::from(["share.upload".to_string()])
    );

    let Json(read) = get_email(State(state.clone()), Extension(auth.clone()))
        .await
        .unwrap();
    assert_eq!(read, created);
    assert!(!serde_json::to_string(&read).unwrap().contains("ada@"));
    let removed = delete_email(State(state.clone()), Extension(auth.clone())).await;
    assert_eq!(removed.unwrap(), StatusCode::NO_CONTENT);
    let gone = get_email(State(state), Extension(auth)).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
}
