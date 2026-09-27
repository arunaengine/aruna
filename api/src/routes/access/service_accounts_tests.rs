//! Tests service account creation, listing, token issuance and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::handle_token;
use crate::tests::routes::{seed_group_docs, test_context, test_state, test_storage};
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::users::account_status::{AccountStatusConfig, AccountStatusOperation};
use tempfile::TempDir;

struct Fixture {
    _dir: TempDir,
    state: Arc<ServerState>,
    admin: AuthContext,
    group: String,
}

/// A management node whose caller administers one group.
async fn fixture() -> Fixture {
    let (dir, storage) = test_storage();
    let context = Arc::new(test_context(storage));
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();
    let admin = AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let actor = Actor {
        node_id,
        user_id: admin.user_id,
        realm_id,
    };
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                user_id: UserId::nil(realm_id),
                ..actor.clone()
            },
            realm_description: "Realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        &context,
    )
    .await
    .unwrap();
    let group_id = Ulid::generate();
    seed_group_docs(&context, realm_id, &actor, group_id, "bots", admin.user_id).await;
    let state = Arc::new(
        test_state(
            context,
            realm_id,
            node_id,
            NodeCapabilities::management_node(signing_key).unwrap(),
        )
        .await,
    );
    Fixture {
        _dir: dir,
        state,
        admin,
        group: group_id.to_string(),
    }
}

async fn create(fixture: &Fixture, auth: &AuthContext) -> ServerResult<ServiceAccountResponse> {
    create_account(
        State(fixture.state.clone()),
        Extension(Some(auth.clone())),
        Path(fixture.group.clone()),
        Json(CreateServiceRequest {
            name: "Nightly import".to_string(),
        }),
    )
    .await
    .map(|(_, Json(account))| account)
}

async fn token(
    fixture: &Fixture,
    account: &str,
    scopes: Option<Vec<CreatePathRestriction>>,
) -> ServerResult<CreateSessionResponse> {
    create_token(
        State(fixture.state.clone()),
        Extension(Some(fixture.admin.clone())),
        Path((fixture.group.clone(), account.to_string())),
        Json(ServiceTokenRequest {
            expires_in_seconds: Some(600),
            path_restrictions: scopes,
        }),
    )
    .await
    .map(|(_, Json(created))| created)
}

fn as_account(fixture: &Fixture, account: &ServiceAccountResponse) -> AuthContext {
    AuthContext {
        user_id: UserId::from_string(&account.id).unwrap(),
        ..fixture.admin.clone()
    }
}

#[tokio::test]
async fn admin_creates_lists() {
    let fixture = fixture().await;
    let account = create(&fixture, &fixture.admin).await.unwrap();
    assert!(account.active);

    let (_, Json(listed)) = list_accounts(
        State(fixture.state.clone()),
        Extension(Some(fixture.admin.clone())),
        Path(fixture.group.clone()),
    )
    .await
    .unwrap();
    let ids: Vec<&str> = listed
        .accounts
        .iter()
        .map(|entry| entry.id.as_str())
        .collect();
    assert_eq!(ids, [account.id.as_str()]);
}

#[tokio::test]
async fn token_carries_account() {
    // The token carries the account, and scopes are judged by the account's own access.
    let fixture = fixture().await;
    let account = create(&fixture, &fixture.admin).await.unwrap();

    let created = token(&fixture, &account.id, None).await.unwrap();
    let claims = handle_token(&fixture.state, &created.token).await.unwrap();
    assert_eq!(claims.sub, account.id);
    assert_eq!(created.label, SERVICE_LABEL);

    let scope = CreatePathRestriction {
        pattern: format!("/{}/g/{}/data/**", fixture.admin.realm_id, fixture.group),
        permission: "READ".to_string(),
    };
    let error = token(&fixture, &account.id, Some(vec![scope]))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
}

#[tokio::test]
async fn outsiders_refused() {
    let fixture = fixture().await;
    let account = create(&fixture, &fixture.admin).await.unwrap();
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::generate(), fixture.admin.realm_id),
        ..fixture.admin.clone()
    };
    let service = as_account(&fixture, &account);

    for caller in [&stranger, &service] {
        assert!(matches!(
            create(&fixture, caller).await,
            Err(ServerError::Forbidden)
        ));
    }
    let unknown = UserId::local(Ulid::generate(), fixture.admin.realm_id).to_string();
    assert!(matches!(
        token(&fixture, &unknown, None).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn deactivated_gets_nothing() {
    // A group administrator deactivates the group's account, which then gets no token.
    let fixture = fixture().await;
    let account = create(&fixture, &fixture.admin).await.unwrap();
    drive(
        AccountStatusOperation::new(AccountStatusConfig {
            actor: Actor {
                node_id: fixture.state.get_node_id(),
                user_id: fixture.admin.user_id,
                realm_id: fixture.admin.realm_id,
            },
            auth_context: fixture.admin.clone(),
            target: UserId::from_string(&account.id).unwrap(),
            active: false,
            now: unix_timestamp_secs(),
        }),
        &fixture.state.get_ctx(),
    )
    .await
    .unwrap();

    assert!(matches!(
        token(&fixture, &account.id, None).await,
        Err(ServerError::Conflict(_))
    ));
}
