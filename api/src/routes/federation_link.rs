//! Linked logins: confirm a fresh local login with a fresh login of another realm, link them,
//! list and remove linked logins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::federation::Signed;
use aruna_core::link::LinkConfirmation;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::auth::bearer_token::decode_bearer_token;
use aruna_operations::driver::drive;
use aruna_operations::federation::link::{
    ConfirmLinkConfig, ConfirmLinkOperation, LinkLoginConfig, LinkLoginError, LinkLoginOperation,
    UnlinkLoginConfig, UnlinkLoginOperation,
};
use aruna_operations::users::update_user::UpdateUserError;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(tags((name = "federation", description = "Native login across realms")))]
pub struct FederationLinkApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(FederationLinkApiDoc::openapi())
        .routes(routes!(create_confirmation))
        .routes(routes!(create_link, list_links))
        .routes(routes!(delete_link))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct LinkConfirmationRequest {
    /// The token of the federated session this realm just issued for the other realm's login.
    pub federated_token: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct LinkRequest {
    #[schema(value_type = Object)]
    pub confirmation: Signed<LinkConfirmation>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct LinkedLoginsResponse {
    pub user_id: String,
    /// Logins of other realms that open sessions of this account.
    pub linked_logins: Vec<String>,
}

fn link_refused(error: LinkLoginError) -> ServerError {
    let message = error.to_string();
    match error {
        LinkLoginError::LocalRefused | LinkLoginError::ForeignRefused => {
            ServerError::Refused(StatusCode::FORBIDDEN, "link_refused", message)
        }
        LinkLoginError::CutOff | LinkLoginError::Confirmation(_) => {
            ServerError::Refused(StatusCode::FORBIDDEN, "link_refused", message)
        }
        LinkLoginError::Unauthorized => ServerError::Forbidden,
        LinkLoginError::NotLinked => ServerError::NotFound,
        LinkLoginError::Update(
            UpdateUserError::AliasClaimed | UpdateUserError::AliasRealmTaken,
        ) => ServerError::Refused(StatusCode::CONFLICT, "link_conflict", message),
        LinkLoginError::Update(UpdateUserError::AliasMissing) => ServerError::NotFound,
        LinkLoginError::Update(UpdateUserError::Unauthorized) => ServerError::Forbidden,
        LinkLoginError::Storage(_) => ServerError::ServiceUnavailableReason(message),
        _ => ServerError::InternalError(message),
    }
}

/// The verified claims of the caller's own bearer token.
async fn bearer_issued_at(
    state: &ServerState,
    bearer: Option<ValidatedBearer>,
) -> ServerResult<u64> {
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let claims = decode_bearer_token(state, bearer.as_str())
        .await
        .map_err(|_| ServerError::Unauthorized)?;
    Ok(claims.iat)
}

fn actor(state: &ServerState, auth: &AuthContext) -> Actor {
    Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    }
}

#[utoipa::path(
    post,
    path = "/federation/link-confirmations",
    tag = "federation",
    summary = "Confirm a login of another realm for linking",
    description = r#"Signs a short-lived confirmation that binds the caller's local account and a login of another realm.

**Authentication**: a portal session of an active local account, opened at most 5 minutes ago,
without path restrictions. Renewed or child tokens are refused when older than that.

**Behavior**
- `federated_token` must be a federated session this realm issued at most 5 minutes ago for a
  user of another realm, not itself a linked login, and that realm must still be admitted.
- The foreign identity comes only from the verified token, never from a request field.
- The confirmation names this realm, both full user ids and the link action, and lives 5 minutes.
  Show both identities to the user before calling the link route."#,
    request_body(
        content = LinkConfirmationRequest,
        example = json!({ "federated_token": "EXAMPLE-FEDERATED-SESSION-TOKEN" })
    ),
    responses(
        (status = 200, description = "The signed link confirmation", body = serde_json::Value,
            example = json!({ "payload": { "realm_id": "<this realm id>", "action": "Link", "local_user": "<local user id>", "foreign_user": "<user id of the other realm>", "foreign_issued_at": 1791000000, "issued_at": 1791000010, "expires_at": 1791000310, "confirmation_id": "01JLINK0123456789ABCDEFGHJ" }, "signer": "Realm", "signature": "<hex>" })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A login is too old, of the wrong kind, of an inactive or service account, or of a realm no longer admitted (code `link_refused`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_confirmation(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<LinkConfirmationRequest>,
) -> ServerResult<Json<Signed<LinkConfirmation>>> {
    let auth = require_realm_auth(&state, auth)?;
    let local_issued_at = bearer_issued_at(&state, bearer).await?;
    let foreign = decode_bearer_token(state.as_ref(), &request.federated_token)
        .await
        .map_err(|_| link_refused(LinkLoginError::ForeignRefused))?;
    let foreign_issued_at = foreign.iat;
    let foreign =
        AuthContext::try_from(foreign).map_err(|_| link_refused(LinkLoginError::ForeignRefused))?;
    let confirm = ConfirmLinkOperation::new(ConfirmLinkConfig {
        auth_context: auth,
        local_issued_at,
        foreign,
        foreign_issued_at,
        node_capabilities: state.node_capabilities().clone(),
        now: unix_timestamp_secs(),
    });
    let confirmation = drive(confirm, &state.get_ctx())
        .await
        .map_err(link_refused)?;
    Ok(Json(confirmation))
}

#[utoipa::path(
    post,
    path = "/federation/links",
    tag = "federation",
    summary = "Link a confirmed login of another realm",
    description = r#"Links the confirmed login of another realm to the caller's local account.

**Authentication**: the same kind of fresh portal session as for the confirmation, of the
account the confirmation names.

**Behavior**
- The confirmation must be signed by this realm for the caller and still be valid; the other
  realm must still be admitted, and a credential cutoff of the foreign login after its session
  was issued voids the confirmation.
- Each account links at most one login per other realm, and each login belongs to at most one
  account; a login another account already claims is refused with code `link_conflict`.
- The link replicates to every node of this realm. Later logins through the other realm open
  sessions of this account with its own roles only.
- Linking never merges accounts and changes nothing at the other realm."#,
    request_body(
        content = LinkRequest,
        example = json!({ "confirmation": { "payload": { "realm_id": "<this realm id>" }, "signer": "Realm", "signature": "<hex>" } })
    ),
    responses(
        (status = 200, description = "The account with its linked logins", body = LinkedLoginsResponse,
            example = json!({ "user_id": "<local user id>", "linked_logins": ["<user id of the other realm>"] })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The confirmation or a login was refused (code `link_refused`)", body = ErrorResponse),
        (status = 409, description = "The login belongs to another account, or the account links another login of that realm (code `link_conflict`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<LinkRequest>,
) -> ServerResult<Json<LinkedLoginsResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let local_issued_at = bearer_issued_at(&state, bearer).await?;
    let link = LinkLoginOperation::new(LinkLoginConfig {
        actor: actor(&state, &auth),
        auth_context: auth,
        local_issued_at,
        confirmation: request.confirmation,
        now: unix_timestamp_secs(),
    });
    let user = drive(link, &state.get_ctx()).await.map_err(link_refused)?;
    Ok(Json(linked_logins(&user)))
}

/// The account's logins of other realms, without the local aliases of merged sign-ins.
pub(crate) fn linked_logins(user: &User) -> LinkedLoginsResponse {
    let mut logins = user
        .alias_user_ids
        .iter()
        .filter(|alias| alias.realm_id != user.user_id.realm_id)
        .map(UserId::to_string)
        .collect::<Vec<_>>();
    logins.sort();
    LinkedLoginsResponse {
        user_id: user.user_id.to_string(),
        linked_logins: logins,
    }
}

#[utoipa::path(
    get,
    path = "/federation/links",
    tag = "federation",
    summary = "List the caller's linked logins",
    description = r#"Lists the logins of other realms linked to the caller's local account.

**Authentication**: bearer token of a local account of this realm, also a linked login."#,
    responses(
        (status = 200, description = "The caller's linked logins", body = LinkedLoginsResponse,
            example = json!({ "user_id": "<local user id>", "linked_logins": ["<user id of the other realm>"] })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller has no local account here", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_links(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<LinkedLoginsResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    if auth.user_id.realm_id != state.get_realm_id() || auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    let read = aruna_operations::users::read_document::ReadUserOperation::new(auth.user_id);
    let user = drive(read, &state.get_ctx())
        .await
        .map_err(|_| ServerError::NotFound)?;
    Ok(Json(linked_logins(&user)))
}

#[utoipa::path(
    delete,
    path = "/federation/links/{user_id}/{login}",
    tag = "federation",
    summary = "Remove a linked login",
    description = r#"Removes a login of another realm from a local account.

**Authentication**: bearer token of the account owner, or of a realm administrator.

**Behavior**
- A credential cutoff for the foreign login is recorded first, so every session that came
  through it ends now and stays invalid if it is linked again later.
- The link is then removed on every node. Later logins through the other realm open ordinary
  sessions for that realm's user. Local sessions and other linked logins are not affected."#,
    params(
        ("user_id" = String, Path, description = "The local account"),
        ("login" = String, Path, description = "The linked login of another realm")
    ),
    responses(
        (status = 200, description = "The account with its remaining linked logins", body = LinkedLoginsResponse,
            example = json!({ "user_id": "<local user id>", "linked_logins": [] })),
        (status = 400, description = "A malformed user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is neither the owner nor a realm administrator", body = ErrorResponse),
        (status = 404, description = "The account has no such linked login", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_link(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((user_id, login)): Path<(String, String)>,
) -> ServerResult<Json<LinkedLoginsResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let parse = |value: &str| {
        UserId::from_string(value)
            .map_err(|_| ServerError::BadRequestReason("malformed user id".to_string()))
    };
    let (user_id, alias) = (parse(&user_id)?, parse(&login)?);
    let unlink = UnlinkLoginOperation::new(UnlinkLoginConfig {
        actor: actor(&state, &auth),
        auth_context: auth,
        user_id,
        alias,
        now: unix_timestamp_secs(),
    });
    let user = drive(unlink, &state.get_ctx())
        .await
        .map_err(link_refused)?;
    Ok(Json(linked_logins(&user)))
}
