//! Tests service account creation, listing, token and credential issuance, and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::handle_token;
use crate::routes::access::audit::{AdminAuditQuery, list_admin_audit};
use crate::tests::routes::{seed_group_docs, test_context, test_state, test_storage, write_doc};
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::auth::{NodeCapabilities, PathRestriction, Permission};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::s3::access::get::GetAccessOperation;
use aruna_operations::users::account_status::{AccountStatusConfig, AccountStatusOperation};
use axum::extract::Query;
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

async fn credential(
    fixture: &Fixture,
    caller: &AuthContext,
    account: &str,
    request: ServiceCredentialRequest,
) -> ServerResult<CreateS3Response> {
    create_credential(
        State(fixture.state.clone()),
        Extension(Some(caller.clone())),
        Path((fixture.group.clone(), account.to_string())),
        Json(request),
    )
    .await
    .map(|(_, Json(created))| created)
}

fn lifetime(seconds: u64) -> ServiceCredentialRequest {
    ServiceCredentialRequest {
        expires_in_seconds: Some(seconds),
        path_restrictions: None,
    }
}

fn actor(fixture: &Fixture) -> Actor {
    Actor {
        node_id: fixture.state.get_node_id(),
        user_id: fixture.admin.user_id,
        realm_id: fixture.admin.realm_id,
    }
}

/// Rewrites the group's authorization so that `account` holds the role named `role`.
async fn assign_role(fixture: &Fixture, account: &str, role: &str) {
    let (realm_id, group_id) = (
        fixture.admin.realm_id,
        Ulid::from_string(&fixture.group).unwrap(),
    );
    let mut document =
        GroupAuthorizationDocument::default_group_doc(fixture.admin.user_id, realm_id, group_id);
    for entry in document
        .roles
        .values_mut()
        .filter(|entry| entry.name == role)
    {
        entry
            .assigned_users
            .insert(UserId::from_string(account).unwrap());
    }
    let value = document.to_bytes(&actor(fixture)).unwrap();
    let key = group_id.to_bytes().into();
    write_doc(&fixture.state.get_ctx(), AUTH_KEYSPACE, key, value.into()).await;
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
async fn plugin_account_refused() {
    // A plugin's account acts only through its install: no token is issued for it.
    let fixture = fixture().await;
    let install = Ulid::generate();
    let account = UserId::local(install, fixture.admin.realm_id);
    let create = CreateServiceOperation::new(CreateServiceConfig {
        actor: Actor {
            node_id: fixture.state.get_node_id(),
            user_id: fixture.admin.user_id,
            realm_id: fixture.admin.realm_id,
        },
        auth_context: fixture.admin.clone(),
        group_id: Ulid::from_string(&fixture.group).unwrap(),
        name: "plugin announcer (bots)".to_string(),
        user_id: account,
        install: Some(install),
    });
    drive(create, &fixture.state.get_ctx()).await.unwrap();
    let refused = token(&fixture, &account.to_string(), None).await;
    assert!(
        matches!(refused, Err(ServerError::Conflict(_))),
        "{refused:?}"
    );
    let admin = fixture.admin.clone();
    let refused = credential(&fixture, &admin, &account.to_string(), lifetime(600)).await;
    assert!(matches!(refused, Err(ServerError::Conflict(_))));
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
    let refused = credential(&fixture, &fixture.admin, &account.id, lifetime(600)).await;
    assert!(matches!(refused, Err(ServerError::Conflict(_))));
}

#[tokio::test]
async fn audit_rows_scoped() {
    // Group administrators page through their group's rows; other scopes stay closed.
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
    let read = |auth: &AuthContext, group_id: Option<String>, cursor: Option<String>| {
        list_admin_audit(
            State(fixture.state.clone()),
            Extension(Some(auth.clone())),
            Query(AdminAuditQuery {
                group_id,
                cursor,
                limit: Some(1),
                me: None,
            }),
        )
    };
    let group = Some(fixture.group.clone());
    let Json(first) = read(&fixture.admin, group.clone(), None).await.unwrap();
    assert!(first.partial);
    assert_eq!(first.records[0].action, "service_account_created");
    assert_eq!(first.records[0].target, account.id);
    assert_eq!(first.records[0].actor, fixture.admin.user_id.to_string());
    let Json(second) = read(&fixture.admin, group.clone(), first.next_cursor)
        .await
        .unwrap();
    assert_eq!(second.records[0].action, "account_deactivated");
    assert_eq!(second.records[0].group_id, group);
    assert_eq!(second.next_cursor, None);

    let stranger = AuthContext {
        user_id: UserId::local(Ulid::generate(), fixture.admin.realm_id),
        ..fixture.admin.clone()
    };
    let other_group = Some(Ulid::generate().to_string());
    for (caller, scope) in [
        (&stranger, group.clone()),
        (&fixture.admin, None),
        (&fixture.admin, other_group),
    ] {
        let denied = read(caller, scope, None).await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
    }
}

#[tokio::test]
async fn credential_outlives_tokens() {
    // A credential far beyond the 24 hour token limit, confined to one prefix and audited.
    let fixture = fixture().await;
    let account = create(&fixture, &fixture.admin).await.unwrap();
    assign_role(&fixture, &account.id, "user").await;
    let days = 300 * 24 * 60 * 60;
    let scope = CreatePathRestriction {
        pattern: "shared-bucket/elabftw/**".to_string(),
        permission: "WRITE".to_string(),
    };
    let request = ServiceCredentialRequest {
        path_restrictions: Some(vec![scope]),
        ..lifetime(days)
    };
    let before = SystemTime::now();
    let created = credential(&fixture, &fixture.admin, &account.id, request)
        .await
        .unwrap();
    assert!(created.key_requests.is_empty());

    let get = GetAccessOperation::new(created.access_key_id.clone());
    let stored = drive(get, &fixture.state.get_ctx()).await.unwrap();
    assert_eq!(stored.user_identity.to_string(), account.id);
    assert_eq!(stored.group_id.to_string(), fixture.group);
    assert_eq!(stored.issued_by, *fixture.state.get_node_id().as_bytes());
    assert!(stored.expiry >= before + std::time::Duration::from_secs(days));
    let root = group_permission_path(
        fixture.admin.realm_id,
        Ulid::from_string(&fixture.group).unwrap(),
        fixture.state.get_node_id(),
    );
    let confined = PathRestriction {
        pattern: format!("{root}/shared-bucket/elabftw/**"),
        permission: Permission::WRITE,
    };
    assert_eq!(stored.path_restrictions, Some(vec![confined]));

    let Json(page) = list_admin_audit(
        State(fixture.state.clone()),
        Extension(Some(fixture.admin.clone())),
        Query(AdminAuditQuery {
            group_id: Some(fixture.group.clone()),
            cursor: None,
            limit: Some(10),
            me: None,
        }),
    )
    .await
    .unwrap();
    let row = page.records.last().unwrap();
    assert_eq!(row.action, "service_credential_issued");
    assert_eq!(row.actor, fixture.admin.user_id.to_string());
    assert_eq!(row.target, account.id);
    assert_eq!(row.session_id, Some(created.access_key_id));
}

#[tokio::test]
async fn credential_callers_refused() {
    let fixture = fixture().await;
    let account = create(&fixture, &fixture.admin).await.unwrap();
    // Without usable access to the group's data the account gets no credential.
    let refused = credential(&fixture, &fixture.admin, &account.id, lifetime(600)).await;
    assert!(
        matches!(refused, Err(ServerError::Forbidden)),
        "{refused:?}"
    );
    assign_role(&fixture, &account.id, "user").await;

    let realm_id = fixture.admin.realm_id;
    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: format!("/{realm_id}/g/{}/**", fixture.group),
            permission: Permission::WRITE,
        }]),
        ..fixture.admin.clone()
    };
    // The administrator of another group administers only that group's accounts.
    let other_admin = AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        ..fixture.admin.clone()
    };
    let (context, other_group) = (fixture.state.get_ctx(), Ulid::generate());
    seed_group_docs(
        &context,
        realm_id,
        &actor(&fixture),
        other_group,
        "lab",
        other_admin.user_id,
    )
    .await;
    let service = as_account(&fixture, &account);
    for caller in [&restricted, &other_admin, &service] {
        let refused = credential(&fixture, caller, &account.id, lifetime(600)).await;
        assert!(
            matches!(refused, Err(ServerError::Forbidden)),
            "{refused:?}"
        );
    }

    let year = 365 * 24 * 60 * 60;
    let too_long = credential(&fixture, &fixture.admin, &account.id, lifetime(year + 1)).await;
    assert!(matches!(too_long, Err(ServerError::BadRequest)));
    let unknown = UserId::local(Ulid::generate(), realm_id).to_string();
    let missing = credential(&fixture, &fixture.admin, &unknown, lifetime(600)).await;
    assert!(matches!(missing, Err(ServerError::NotFound)));
}
