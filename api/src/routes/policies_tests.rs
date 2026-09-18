//! Tests storing policy sets, stale hash conflicts, admin checks and policy tracing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::DriverContext;
use aruna_operations::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_storage::storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ulid::Ulid;

struct Fixture {
    _dir: tempfile::TempDir,
    state: Arc<ServerState>,
    admin: AuthContext,
    actor: Actor,
    realm_id: RealmId,
}

async fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let realm_id = RealmId::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[31u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let admin_id = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
    let actor = Actor {
        node_id,
        user_id: admin_id,
        realm_id,
    };
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
            realm_description: "policies".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        context.as_ref(),
    )
    .await
    .unwrap();
    drive(
        ClaimInitialOperation::new(ClaimInitialInput {
            actor: actor.clone(),
        }),
        context.as_ref(),
    )
    .await
    .unwrap();
    let state = Arc::new(
        ServerState::new(
            context,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );
    Fixture {
        _dir: dir,
        state,
        admin: AuthContext {
            user_id: admin_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        actor,
        realm_id,
    }
}

fn body(expression: &str) -> SetPoliciesRequest {
    SetPoliciesRequest {
        policies: vec![PolicyBody {
            policy_id: None,
            name: "no-group-writes".to_string(),
            kind: "deny".to_string(),
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }],
        expected_hash: None,
    }
}

#[tokio::test]
async fn stores_and_enforces() {
    // The stored set is served back and denies a previously allowed write.
    let fx = setup().await;
    let denied_path = format!("/{}/admin/roles/example", fx.realm_id);

    let (_, Json(stored)) = set_realm_policies(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(body(
            "permission == 'write' && path.contains('/admin/roles')",
        )),
    )
    .await
    .unwrap();
    assert_eq!(stored.policies.len(), 1);
    assert!(!stored.set_hash.is_empty());

    let (_, Json(read)) =
        get_realm_policies(State(fx.state.clone()), Extension(Some(fx.admin.clone())))
            .await
            .unwrap();
    assert_eq!(read.policies.len(), 1);

    let denied = ensure_permission(&fx.state, &fx.admin, denied_path, Permission::WRITE).await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn stale_hash_conflicts() {
    let fx = setup().await;
    let mut request = body("permission == 'write'");
    // A well-formed but stale digest must abort the write transaction as 409.
    request.expected_hash = Some("ff".repeat(32));
    let result = set_realm_policies(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(request),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Conflict(_))));
}

#[tokio::test]
async fn group_authz_boundary() {
    // The owning group's config admin may set the group set; a stranger cannot.
    let fx = setup().await;
    let (group, _) = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: fx.actor.clone(),
            display_name: "policy group".to_string(),
            owner_cap: None,
        }),
        &fx.state.get_ctx(),
    )
    .await
    .unwrap();

    let (_, Json(stored)) = set_group_policies(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Path(group.group_id.to_string()),
        Json(body("permission == 'write'")),
    )
    .await
    .unwrap();
    assert_eq!(stored.policies.len(), 1);

    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([77; 16]), fx.realm_id),
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let result = set_group_policies(
        State(fx.state.clone()),
        Extension(Some(stranger)),
        Path(group.group_id.to_string()),
        Json(body("true")),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn validate_reports_unknowns() {
    let fx = setup().await;
    let (_, Json(result)) = validate_policy(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(ValidatePolicyRequest {
            kind: "deny".to_string(),
            when: None,
            expression: "mystery(body.kind) && unknown_var".to_string(),
        }),
    )
    .await
    .unwrap();
    assert!(result.valid);
    assert!(
        result
            .unknown_variables
            .contains(&"unknown_var".to_string())
    );
    assert!(result.unknown_functions.contains(&"mystery".to_string()));
}

#[tokio::test]
async fn validate_needs_admin() {
    // Compiling caller-supplied CEL is gated like the dry run: a realm user
    // without config read must not reach the compiler.
    let fx = setup().await;
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([78; 16]), fx.realm_id),
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let result = validate_policy(
        State(fx.state.clone()),
        Extension(Some(stranger)),
        Json(ValidatePolicyRequest {
            kind: "deny".to_string(),
            when: None,
            expression: "true".to_string(),
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn traces_candidates() {
    // A dry run reports the decision of every candidate path it evaluated.
    let fx = setup().await;
    let (_, Json(run)) = dry_run_policy(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(DryRunRequest {
            path: "/r/g/x/data/y".to_string(),
            permission: "write".to_string(),
            user: None,
            operation: None,
            params: None,
            headers: None,
            body: None,
            session: None,
            candidate_policies: Some(vec![PolicyBody {
                policy_id: None,
                name: "dry-run".to_string(),
                kind: "deny".to_string(),
                when: None,
                expression: "permission == 'write'".to_string(),
                enabled: true,
            }]),
            scope: None,
            group_id: None,
        }),
    )
    .await
    .unwrap();
    assert!(run.denied);
    assert_eq!(run.policy_name.as_deref(), Some("dry-run"));
    assert_eq!(run.trace.len(), 1);
    let body = serde_json::to_value(&run).unwrap();
    assert_eq!(body["trace"][0]["kind"], serde_json::json!("Deny"));
}

#[tokio::test]
async fn rejects_oversized_expression() {
    // S10: a candidate expression above the policy limit is refused before compile.
    let fx = setup().await;
    let result = dry_run_policy(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(DryRunRequest {
            path: "/r/g/x/data/y".to_string(),
            permission: "write".to_string(),
            user: None,
            operation: None,
            params: None,
            headers: None,
            body: None,
            session: None,
            candidate_policies: Some(vec![PolicyBody {
                policy_id: None,
                name: "huge".to_string(),
                kind: "deny".to_string(),
                when: None,
                expression: "x".repeat(5000),
                enabled: true,
            }]),
            scope: None,
            group_id: None,
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn deny_covers_routes() {
    // A realm `permission == "write"` deny must block write, delete, and
    // admin routes the admin could otherwise reach, while reads still pass.
    let fx = setup().await;
    let (_, Json(_stored)) = set_realm_policies(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(body("permission == 'write'")),
    )
    .await
    .unwrap();

    let realm = fx.realm_id;
    let write_routes = [
        format!("/{realm}/admin/roles/example"),
        format!("/{realm}/admin/config"),
        format!("/{realm}/admin/onboarding"),
    ];
    for path in write_routes {
        let denied = ensure_permission(&fx.state, &fx.admin, path.clone(), Permission::WRITE).await;
        assert!(
            matches!(denied, Err(ServerError::Forbidden)),
            "write not blocked on {path}"
        );
    }
    // A read on the same admin path is unaffected by the write deny.
    ensure_permission(
        &fx.state,
        &fx.admin,
        format!("/{realm}/admin/config"),
        Permission::READ,
    )
    .await
    .unwrap();

    // Anonymous callers are denied outright on a write route.
    let anonymous = AuthContext::anonymous(realm);
    let denied = ensure_permission(
        &fx.state,
        &anonymous,
        format!("/{realm}/admin/config"),
        Permission::WRITE,
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));

    // The config route itself must honour the deny, not only the RBAC check.
    let denied = set_realm_policies(
        State(fx.state.clone()),
        Extension(Some(fx.admin.clone())),
        Json(body("true")),
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn requires_admin() {
    // A non-admin realm member cannot replace the policy set.
    let fx = setup().await;
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([77; 16]), fx.realm_id),
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let result = set_realm_policies(
        State(fx.state.clone()),
        Extension(Some(stranger)),
        Json(body("true")),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}
