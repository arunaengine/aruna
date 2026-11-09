//! Tests that email opt-ins need a realm profile, a verified address and a management quorum,
//! and never show the address.
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
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::identity::user::User;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use ed25519_dalek::SigningKey;
use tempfile::TempDir;
use ulid::Ulid;

/// Stores the realm config with this node as its only manager, and an email profile when
/// `email` is set.
async fn store_config(
    context: &aruna_operations::driver::DriverContext,
    actor: &Actor,
    email: bool,
) {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Management);
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

/// A management node of a realm whose id is the realm key, and a caller with a verified address.
async fn management_state() -> (TempDir, Arc<ServerState>, Actor, Option<AuthContext>) {
    let (dir, storage) = test_storage();
    let realm_key = SigningKey::from_bytes(&[5u8; 32]);
    let realm_id = RealmId::from_bytes(realm_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
    let context = Arc::new(test_context(storage));
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    seed_user(&context, &actor).await;
    let capabilities = NodeCapabilities::management_node(realm_key).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    let auth = Some(AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    });
    (dir, state, actor, auth)
}

async fn seed_user(context: &aruna_operations::driver::DriverContext, actor: &Actor) {
    let user = User {
        user_id: actor.user_id,
        name: "Ada".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: [(
            EMAIL_HASH_ATTRIBUTE.to_string(),
            email_hash("ada@example.org"),
        )]
        .into(),
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: actor.user_id.to_bytes().into(),
            value: user.to_bytes(actor).unwrap().into(),
            txn_id: None,
        }))
        .await;
}

#[tokio::test]
async fn route_needs_verification() {
    let (_dir, state, actor, auth) = management_state().await;
    let context = state.get_ctx();
    let set = |address: &'static str| {
        set_email(
            State(state.clone()),
            Extension(auth.clone()),
            request(address),
        )
    };
    store_config(&context, &actor, false).await;
    let unconfigured = set("ada@example.org").await;
    assert!(matches!(unconfigured, Err(ServerError::Conflict(_))));
    store_config(&context, &actor, true).await;
    let foreign = set("eve@example.org").await;
    assert!(matches!(foreign, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn short_quorum_unavailable() {
    // The only manager is this node without its realm key, so no write reaches a quorum.
    let (_dir, state, actor, auth) = management_state().await;
    store_config(&state.get_ctx(), &actor, true).await;
    let set = set_email(
        State(state.clone()),
        Extension(auth.clone()),
        request("ada@example.org"),
    );
    assert!(matches!(
        set.await,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let removed = delete_email(State(state.clone()), Extension(auth.clone())).await;
    assert!(matches!(
        removed,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let read = get_email(State(state), Extension(auth)).await;
    assert!(matches!(read, Err(ServerError::NotFound)));
}

#[test]
fn storage_error_unavailable() {
    let stored = setting_error(SettingError::Storage("journal".into()));
    assert!(matches!(stored, ServerError::ServiceUnavailableReason(_)));
    let built = setting_error(SettingError::Build("encode".into()));
    assert!(matches!(built, ServerError::InternalError(_)));
}

#[tokio::test]
async fn config_error_unavailable() {
    let (_dir, state, _actor, auth) = management_state().await;
    let read = get_email(State(state.clone()), Extension(auth.clone())).await;
    assert!(matches!(
        read,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let set = set_email(
        State(state.clone()),
        Extension(auth.clone()),
        request("ada@example.org"),
    );
    assert!(matches!(
        set.await,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    let removed = delete_email(State(state), Extension(auth)).await;
    assert!(matches!(
        removed,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
}

#[tokio::test]
async fn device_node_refuses() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[24u8; 32]).public();
    let context = Arc::new(test_context(storage));
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    let auth = Some(AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    });
    let read = get_email(State(state.clone()), Extension(auth.clone())).await;
    assert!(matches!(read, Err(ServerError::Conflict(_))));
    let set = set_email(
        State(state.clone()),
        Extension(auth.clone()),
        request("ada@example.org"),
    );
    assert!(matches!(set.await, Err(ServerError::Conflict(_))));
    let removed = delete_email(State(state), Extension(auth)).await;
    assert!(matches!(removed, Err(ServerError::Conflict(_))));
}

#[tokio::test]
async fn get_shows_confirmed() {
    // GET shows the newest confirmed setting this node holds, never its sealed address.
    let (_dir, state, actor, auth) = management_state().await;
    let context = state.get_ctx();
    store_config(&context, &actor, true).await;
    for (version, on) in [(1, true), (2, false), (3, true)] {
        let id = Ulid::from_parts(version, 1);
        let preference = EmailPreference {
            preference_id: id,
            user_id: actor.user_id,
            version,
            categories: match on {
                true => BTreeSet::from(["share.upload".to_string()]),
                false => BTreeSet::new(),
            },
            sealed: on.then(|| b"sealed-canary".to_vec()),
            created_at_ms: version,
        };
        let target = SettingChannel::Email.target(actor.user_id, id);
        let write = StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: preference.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context
            .storage_handle
            .send_effect(Effect::Storage(write))
            .await;
    }
    let Json(read) = get_email(State(state), Extension(auth)).await.unwrap();
    assert_eq!(read.created_at_ms, 3);
    assert!(!serde_json::to_string(&read).unwrap().contains("canary"));
}
