//! Adds, replaces and removes realm OIDC provider entries on a running realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{OidcProviderConfig, RealmConfigDocument};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_operations::driver::drive;
use aruna_operations::realm::set_provider::{
    ProviderChange, SetProviderConfig, SetProviderError, SetProviderOperation,
};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::info::RealmProviderResponse;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi(tags((name = "system/realm", description = "Realm configuration, placement and quota")))]
pub struct ProvidersApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ProvidersApiDoc::openapi())
        .routes(routes!(set_provider, remove_provider))
}

/// One OIDC provider whose ID tokens this realm accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequest {
    /// The exact `iss` of the provider's tokens, as its discovery document states it.
    pub issuer: String,
    /// The client id a token's `aud` must contain.
    pub audience: String,
    /// The provider's OpenID discovery document.
    pub discovery_url: String,
}

#[utoipa::path(
    put,
    path = "/system/realm/oidc-providers/{id}",
    tag = "system/realm",
    summary = "Add or replace a realm OIDC provider",
    description = r#"Adds the OIDC provider with this id to the realm, or replaces the entry with this id.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- A new id is appended. An existing id is replaced in place, so the order never changes and the
  first provider, which portals use for the plain sign-in, stays first.
- Nodes accept ID tokens whose `iss` and `aud` match an entry and key users by `iss` and `sub`.
- The change replicates to every node of the realm.

**Limits**
- The id holds 1 to 64 characters of `a-z`, `A-Z`, `0-9`, `-` and `_`.
- `issuer` and `discovery_url` are HTTPS URLs, or HTTP to a loopback host. `audience` is not
  empty."#,
    params(("id" = String, Path, description = "Provider id, unique within the realm")),
    request_body(
        content = ProviderRequest,
        description = "The provider entry to store under this id",
        example = json!({
            "issuer": "https://login.aai.lifescience-ri.eu/oidc/",
            "audience": "6cef230f-a068-4255-a342-e1486179cace",
            "discovery_url": "https://login.aai.lifescience-ri.eu/oidc/.well-known/openid-configuration"
        })
    ),
    responses(
        (status = 200, description = "All realm OIDC providers in order, after the change", body = Vec<RealmProviderResponse>,
            example = json!([{
                "id": "lifescience",
                "issuer": "https://login.aai.lifescience-ri.eu/oidc/",
                "audience": "6cef230f-a068-4255-a342-e1486179cace",
                "discovery_url": "https://login.aai.lifescience-ri.eu/oidc/.well-known/openid-configuration"
            }])),
        (status = 400, description = "Malformed body, id, issuer, audience or discovery URL", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_provider(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Json(request): Json<ProviderRequest>,
) -> ServerResult<Json<Vec<RealmProviderResponse>>> {
    let provider = OidcProviderConfig {
        id,
        issuer: request.issuer,
        audience: request.audience,
        discovery_url: request.discovery_url,
    };
    change_provider(&state, auth, ProviderChange::Upsert(provider)).await
}

#[utoipa::path(
    delete,
    path = "/system/realm/oidc-providers/{id}",
    tag = "system/realm",
    summary = "Remove a realm OIDC provider",
    description = r#"Removes the OIDC provider with this id from the realm.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The other entries keep their order. Tokens of the removed provider are refused afterwards, and
  users keep their accounts.
- The change replicates to every node of the realm.

**Limits**
- The last provider cannot be removed."#,
    params(("id" = String, Path, description = "Provider id, unique within the realm")),
    responses(
        (status = 200, description = "All realm OIDC providers in order, after the change", body = Vec<RealmProviderResponse>,
            example = json!([{
                "id": "main",
                "issuer": "https://keycloak.example.test/realms/aruna",
                "audience": "aruna-api",
                "discovery_url": "https://keycloak.example.test/realms/aruna/.well-known/openid-configuration"
            }])),
        (status = 400, description = "Malformed id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "No provider with this id, or this node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "The provider is the last one, or another update of the realm configuration won the race", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_provider(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<Vec<RealmProviderResponse>>> {
    change_provider(&state, auth, ProviderChange::Remove(id)).await
}

async fn change_provider(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    change: ProviderChange,
) -> ServerResult<Json<Vec<RealmProviderResponse>>> {
    let auth = require_realm_auth(state, auth)?;
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let stored = drive(
        SetProviderOperation::new(SetProviderConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            auth_context: auth,
            change,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_provider_error)?;
    Ok(Json(provider_list(stored)))
}

fn provider_list(config: RealmConfigDocument) -> Vec<RealmProviderResponse> {
    config
        .oidc_providers
        .into_iter()
        .map(|provider| RealmProviderResponse {
            id: provider.id,
            issuer: provider.issuer,
            audience: provider.audience,
            discovery_url: provider.discovery_url,
        })
        .collect()
}

fn map_provider_error(error: SetProviderError) -> ServerError {
    match error {
        SetProviderError::ConfigMissing | SetProviderError::UnknownProvider => {
            ServerError::NotFound
        }
        SetProviderError::Unauthorized | SetProviderError::NotManagementNode => {
            ServerError::Forbidden
        }
        SetProviderError::InvalidProvider { reason } => ServerError::BadRequestReason(reason),
        SetProviderError::LastProvider => ServerError::Conflict(error.to_string()),
        SetProviderError::StorageError(StorageError::TransactionConflict) => ServerError::Conflict(
            "concurrent realm configuration update conflict; retry".to_string(),
        ),
        SetProviderError::StorageError(StorageError::CleanupCapacity) => {
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
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use tempfile::TempDir;
    use ulid::Ulid;

    /// A management node with the `main` provider and one realm admin; returns the admin.
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
                oidc_providers: vec![OidcProviderConfig {
                    id: "main".to_string(),
                    issuer: "https://keycloak.example/realms/aruna".to_string(),
                    audience: "aruna-api".to_string(),
                    discovery_url: "https://keycloak.example/discovery".to_string(),
                }],
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

    fn auth(state: &ServerState, user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id: state.get_realm_id(),
            path_restrictions: None,
            session: None,
        })
    }

    fn body(issuer: &str) -> Json<ProviderRequest> {
        Json(ProviderRequest {
            issuer: issuer.to_string(),
            audience: "client".to_string(),
            discovery_url: format!("{issuer}/.well-known/openid-configuration"),
        })
    }

    async fn put(
        state: &Arc<ServerState>,
        user: UserId,
        id: &str,
        issuer: &str,
    ) -> ServerResult<Vec<String>> {
        let Json(list) = set_provider(
            State(state.clone()),
            Extension(auth(state, user)),
            Path(id.to_string()),
            body(issuer),
        )
        .await?;
        Ok(list.into_iter().map(|provider| provider.id).collect())
    }

    #[tokio::test]
    async fn admin_keeps_order() {
        let (_dir, state, admin) = setup_state().await;
        let ids = put(&state, admin, "lifescience", "https://ls.example").await;
        assert_eq!(ids.unwrap(), ["main", "lifescience"]);
        let ids = put(&state, admin, "main", "https://replaced.example").await;
        assert_eq!(ids.unwrap(), ["main", "lifescience"]);

        let Json(list) = remove_provider(
            State(state.clone()),
            Extension(auth(&state, admin)),
            Path("lifescience".to_string()),
        )
        .await
        .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].issuer, "https://replaced.example");

        let last = remove_provider(
            State(state.clone()),
            Extension(auth(&state, admin)),
            Path("main".to_string()),
        )
        .await;
        assert!(matches!(last, Err(ServerError::Conflict(_))));
    }

    #[tokio::test]
    async fn member_is_forbidden() {
        let (_dir, state, _admin) = setup_state().await;
        let member = UserId::local(Ulid::generate(), state.get_realm_id());
        let error = put(&state, member, "lifescience", "https://ls.example").await;
        assert!(matches!(error, Err(ServerError::Forbidden)));
        let error = remove_provider(
            State(state.clone()),
            Extension(auth(&state, member)),
            Path("main".to_string()),
        )
        .await;
        assert!(matches!(error, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn rejects_plain_http() {
        let (_dir, state, admin) = setup_state().await;
        let error = put(&state, admin, "lifescience", "http://ls.example").await;
        assert!(matches!(error, Err(ServerError::BadRequestReason(_))));
    }
}
