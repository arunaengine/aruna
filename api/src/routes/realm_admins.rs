//! Grants and removes the realm administrator role for one user on a running realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_operations::driver::drive;
use aruna_operations::realm::set_admin::{SetAdminConfig, SetAdminError, SetAdminOperation};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi(tags((name = "system/realm", description = "Realm configuration, placement and quota")))]
pub struct AdminsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(AdminsApiDoc::openapi()).routes(routes!(grant_admin, remove_admin))
}

#[utoipa::path(
    put,
    path = "/system/realm/admins/{user_id}",
    tag = "system/realm",
    summary = "Make a user a realm administrator",
    description = r#"Assigns the realm role `realm_admin` to the user.

**Authentication**: realm bearer token with WRITE on `/{realm}/admin/roles/{user_id}`, which a
realm administrator holds. A management node serves the call and every other node relays it to
one.

**Behavior**
- The role grants WRITE on the whole realm administration tree, `/{realm}/admin/**`.
- The change replicates to every node of the realm. Repeating it succeeds again.
- The user id need not belong to a registered account yet."#,
    params(("user_id" = String, Path, description = "User id in the form `<ulid>@<realm>`")),
    responses(
        (status = 200, description = "All realm administrators after the change, sorted", body = Vec<String>,
            example = json!([
                "01JC8X8Z5W0000000000000000@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "01JC8X9Q2M0000000000000000@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"
            ])),
        (status = 400, description = "The id is not a user id of this realm", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm administrator", body = ErrorResponse),
        (status = 404, description = "This node holds no realm configuration or authorization document", body = ErrorResponse),
        (status = 409, description = "Another update of the realm authorization won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn grant_admin(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(user_id): Path<String>,
) -> ServerResult<Json<Vec<String>>> {
    change_admin(&state, auth, &user_id, true).await
}

#[utoipa::path(
    delete,
    path = "/system/realm/admins/{user_id}",
    tag = "system/realm",
    summary = "Remove a realm administrator",
    description = r#"Removes the realm role `realm_admin` from the user.

**Authentication**: realm bearer token with WRITE on `/{realm}/admin/roles/{user_id}`, which a
realm administrator holds. A management node serves the call and every other node relays it to
one.

**Behavior**
- The user keeps the account, group roles and data. As after any lost READ scope, the serving
  node marks a key epoch raise due in its encrypted buckets.
- The change replicates to every node of the realm. Removing a user without the role succeeds.

**Limits**
- The last realm administrator cannot be removed; the call answers 409."#,
    params(("user_id" = String, Path, description = "User id in the form `<ulid>@<realm>`")),
    responses(
        (status = 200, description = "All realm administrators after the change, sorted", body = Vec<String>,
            example = json!(["01JC8X8Z5W0000000000000000@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"])),
        (status = 400, description = "The id is not a user id of this realm", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm administrator", body = ErrorResponse),
        (status = 404, description = "This node holds no realm configuration or authorization document", body = ErrorResponse),
        (status = 409, description = "The user is the last realm administrator, or another update of the realm authorization won the race", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_admin(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(user_id): Path<String>,
) -> ServerResult<Json<Vec<String>>> {
    change_admin(&state, auth, &user_id, false).await
}

async fn change_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    user_id: &str,
    admin: bool,
) -> ServerResult<Json<Vec<String>>> {
    let auth = require_realm_auth(state, auth)?;
    let realm_id = state.get_realm_id();
    let user_id = UserId::from_string(user_id).map_err(|_| ServerError::BadRequest)?;
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        state,
        &auth,
        format!("/{realm_id}/admin/roles/{user_id}"),
        Permission::WRITE,
    )
    .await?;
    let admins = drive(
        SetAdminOperation::new(SetAdminConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            auth_context: auth,
            user_id,
            admin,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_admin_error)?;
    Ok(Json(sorted_ids(admins)))
}

fn sorted_ids(admins: HashSet<UserId>) -> Vec<String> {
    let mut ids: Vec<String> = admins.iter().map(UserId::to_string).collect();
    ids.sort();
    ids
}

fn map_admin_error(error: SetAdminError) -> ServerError {
    match error {
        SetAdminError::InvalidUser => ServerError::BadRequest,
        SetAdminError::Unauthorized | SetAdminError::NotManagementNode => ServerError::Forbidden,
        SetAdminError::DocumentMissing | SetAdminError::RoleMissing => ServerError::NotFound,
        SetAdminError::LastAdmin => ServerError::Conflict(error.to_string()),
        SetAdminError::StorageError(StorageError::TransactionConflict) => ServerError::Conflict(
            "concurrent realm authorization update conflict; retry".to_string(),
        ),
        SetAdminError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::routes::{test_context, test_state, test_storage};
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use tempfile::TempDir;
    use ulid::Ulid;

    /// A management node with one realm admin; returns the admin.
    async fn setup_state() -> (TempDir, Arc<ServerState>, UserId) {
        let (dir, storage) = test_storage();
        let context = Arc::new(test_context(storage));
        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let actor = Actor {
            node_id: iroh::SecretKey::generate().public(),
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
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
        drive(
            ClaimInitialOperation::new(ClaimInitialInput {
                actor: actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();
        let capabilities = NodeCapabilities::management_node(signing_key).unwrap();
        let state = Arc::new(test_state(context, realm_id, actor.node_id, capabilities).await);
        (dir, state, actor.user_id)
    }

    async fn change(
        state: &Arc<ServerState>,
        caller: UserId,
        target: UserId,
        admin: bool,
    ) -> ServerResult<Vec<String>> {
        let auth = Some(AuthContext {
            user_id: caller,
            realm_id: state.get_realm_id(),
            path_restrictions: None,
            session: None,
        });
        let path = Path(target.to_string());
        let route = if admin {
            grant_admin(State(state.clone()), Extension(auth), path).await
        } else {
            remove_admin(State(state.clone()), Extension(auth), path).await
        };
        route.map(|Json(ids)| ids)
    }

    #[tokio::test]
    async fn admin_grants_role() {
        // The new admin can remove the first one, and the last admin stays.
        let (_dir, state, admin) = setup_state().await;
        let other = UserId::local(Ulid::generate(), state.get_realm_id());
        let ids = change(&state, admin, other, true).await.unwrap();
        assert_eq!(ids, sorted_ids(HashSet::from([admin, other])));

        let ids = change(&state, other, admin, false).await.unwrap();
        assert_eq!(ids, [other.to_string()]);
        let last = change(&state, other, other, false).await;
        assert!(matches!(last, Err(ServerError::Conflict(_))));
    }

    #[tokio::test]
    async fn member_is_forbidden() {
        let (_dir, state, admin) = setup_state().await;
        let member = UserId::local(Ulid::generate(), state.get_realm_id());
        let grant = change(&state, member, member, true).await;
        assert!(matches!(grant, Err(ServerError::Forbidden)));
        let remove = change(&state, member, admin, false).await;
        assert!(matches!(remove, Err(ServerError::Forbidden)));
    }
}
