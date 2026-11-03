//! Tests that email opt-ins need SMTP and a verified address and never show the address.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use aruna_operations::notifications::email::{MailError, MailSender, OutgoingMail};
use ulid::Ulid;

struct NoSender;

#[async_trait::async_trait]
impl MailSender for NoSender {
    async fn send(&self, _mail: OutgoingMail) -> Result<(), MailError> {
        Err(MailError::Retryable("not used".to_string()))
    }
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
            context,
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

    let unconfigured = set("ada@example.org").await;
    assert!(matches!(unconfigured, Err(ServerError::Conflict(_))));
    state.jobs_runtime().set_mailer(Arc::new(NoSender));
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
