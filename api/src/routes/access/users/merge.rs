//! Routes that merge another account into the caller's and attach another sign-in to it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{GetUserResponse, validate_oidc_token};
use crate::auth::{ValidatedBearer, claims_for_revocation, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::federation::link::bearer_auth_time;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::link::MAX_LINK_SECS;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::auth::{Actor, AuthContext, oidc_subject_key};
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::auth::bearer_token::decode_bearer_token;
use aruna_operations::driver::drive;
use aruna_operations::federation::link::fresh_local;
use aruna_operations::users::merge::{MergeError, MergeUsersConfig, MergeUsersOperation};
use aruna_operations::users::update_user::{UpdateUserError, UpdateUserInput, UpdateUserOperation};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const VAULT_DEADLINE: Duration = Duration::from_secs(10);

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(merge_account))
        .routes(routes!(attach_subject))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct MergeAccountRequest {
    /// A realm bearer token of the other account from `GET /access/token`, issued for a provider
    /// login at most 5 minutes old.
    pub token: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct MergeAccountResponse {
    /// The caller's account, the primary that remains.
    pub user_id: String,
    /// The other account, now a deactivated secondary of `user_id`.
    pub merged_user_id: String,
    pub subjects: usize,
    pub groups: usize,
    pub realm_roles: usize,
    pub devices: usize,
    pub attributes: usize,
    /// Earlier secondaries of the other account that now point at `user_id`.
    pub secondaries: usize,
    /// The other account was already merged into the caller's; nothing changed.
    pub repeated: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct AttachSubjectRequest {
    /// An OIDC token of the sign-in to attach, from a provider login at most 5 minutes old.
    pub token: String,
}

fn charge(state: &ServerState, user_id: UserId) -> ServerResult<()> {
    state
        .rate_limits()
        .check_account_link(user_id)
        .map_err(|_| {
            ServerError::Refused(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many attempts; wait a minute".to_string(),
            )
        })
}

fn refused(message: String) -> ServerError {
    ServerError::Refused(StatusCode::FORBIDDEN, "merge_refused", message)
}

fn map_merge_error(error: MergeError) -> ServerError {
    let message = error.to_string();
    match error {
        MergeError::StaleLogin
        | MergeError::TokenInvalid
        | MergeError::SameAccount
        | MergeError::OtherRealm
        | MergeError::Inactive
        | MergeError::ServiceAccount => refused(message),
        MergeError::VaultPresent => {
            ServerError::Refused(StatusCode::CONFLICT, "vault_present", message)
        }
        MergeError::NotFound => ServerError::NotFound,
        MergeError::NotManagementNode => ServerError::Forbidden,
        MergeError::Storage(_) | MergeError::Vault(_) | MergeError::PlacementFenced => {
            ServerError::ServiceUnavailableReason(message)
        }
        _ => ServerError::InternalError(message),
    }
}

#[utoipa::path(
    post,
    path = "/access/users/me/merge",
    tag = "access/users",
    summary = "Merge another account into the caller's",
    description = r#"Merges an account the caller can also sign in with into the caller's account.

**Authentication**: a portal session of an active account of this realm without path restrictions,
whose provider login (`auth_time`) was at most 5 minutes ago. The body carries a realm bearer
token of the other account with the same freshness, from `GET /access/token` after signing in with
that account. Both prove control; a matching email or name is never enough.

**Behavior**
- The caller's account is the primary and stays. The other account becomes its secondary: it is
  deactivated, names the primary in `aruna-engine.org/merged-into`, and the primary lists it in
  `aruna-engine.org/merged.<ulid>`. Secondaries of the other account move to the primary too, so a
  secondary never points at another secondary.
- The other account's sign-ins move to the primary: its next sign-in opens the primary.
- Its group and realm roles move to the primary. A role is added only where it grants the primary
  more than its own roles; the primary never loses a role.
- Its devices are re-owned by the primary and must sign in again.
- Profile and preference attributes the primary lacks are copied.
- Every bearer token, S3 credential and S3 session of the other account issued up to five minutes
  after the merge is cut off on every node. Its share links stop, as for any deactivated account.
- Runs, RO-Crates, provenance and other signed records keep naming the other account; reads of
  its id resolve to the primary.
- The change is one transaction on a management node and replicates to every node. Repeating a
  finished merge answers again with `repeated: true`, also with the then cut-off token.

**Limits**
- The other account must not hold a vault: nodes never open vault payloads, so the portal moves
  its entries first and deletes it (409, code `vault_present`).
- Service accounts and deactivated accounts are never merged. At most 6 attempts per minute."#,
    request_body(
        content = MergeAccountRequest,
        example = json!({ "token": "<realm bearer token of the other account>" })
    ),
    responses(
        (status = 200, description = "The other account is a secondary of the caller's", body = MergeAccountResponse,
            example = json!({ "user_id": "<caller user id>", "merged_user_id": "<other user id>",
                "subjects": 1, "groups": 2, "realm_roles": 0, "devices": 1, "attributes": 3,
                "secondaries": 0, "repeated": false })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A login is too old, the other token is invalid or of another realm, both name the same account, or an account is deactivated or a service account (code `merge_refused`)", body = ErrorResponse),
        (status = 404, description = "This node holds no record of one of the accounts", body = ErrorResponse),
        (status = 409, description = "The other account still holds a vault (code `vault_present`)", body = ErrorResponse),
        (status = 503, description = "Storage or the vault holders were unavailable, or a document moved during the merge; retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn merge_account(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<MergeAccountRequest>,
) -> ServerResult<Json<MergeAccountResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    charge(&state, auth.user_id)?;
    if !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }
    let survivor_login = bearer_auth_time(&state, bearer).await?;
    // A cut-off token still names its account, so a repeat of a finished merge is recognized.
    let (claims, live) = match decode_bearer_token(state.as_ref(), &request.token).await {
        Ok(claims) => (claims, true),
        Err(_) => match claims_for_revocation(&state, &request.token).await {
            Ok(claims) => (claims, false),
            Err(_) => return Err(refused(MergeError::TokenInvalid.to_string())),
        },
    };
    let merged_login = claims.auth_time;
    let merged: AuthContext = claims
        .try_into()
        .map_err(|_| refused(MergeError::TokenInvalid.to_string()))?;
    let config = MergeUsersConfig {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        survivor: auth,
        survivor_login,
        merged,
        merged_login,
        merged_live: live,
        merged_token: AuthToken::bearer(request.token).ok(),
        vault_deadline: VAULT_DEADLINE,
        now: unix_timestamp_secs(),
    };
    let outcome = drive(MergeUsersOperation::new(config), &state.get_ctx())
        .await
        .map_err(map_merge_error)?;
    Ok(Json(MergeAccountResponse {
        user_id: outcome.survivor.to_string(),
        merged_user_id: outcome.merged.to_string(),
        subjects: outcome.subjects,
        groups: outcome.groups,
        realm_roles: outcome.realm_roles,
        devices: outcome.devices,
        attributes: outcome.attributes,
        secondaries: outcome.secondaries,
        repeated: outcome.repeated,
    }))
}

#[utoipa::path(
    post,
    path = "/access/users/me/subjects",
    tag = "access/users",
    summary = "Attach another sign-in to the caller's account",
    description = r#"Attaches a sign-in that has no account yet to the caller's account.

**Authentication**: a portal session of an active account of this realm without path restrictions,
whose provider login (`auth_time`) was at most 5 minutes ago. The body carries the OIDC token of
the sign-in to attach, from a configured issuer, checked like a registration and also at most 5
minutes after its provider login.

**Behavior**
- This is the "this is me" answer to `account_match` from `POST /access/users/register`: the
  person signs in with the existing account, then attaches the new sign-in. Later sign-ins with it
  open the caller's account.
- Attaching a sign-in the account already has succeeds again unchanged.
- A sign-in that already belongs to another account is refused with code `subject_taken`; merge
  that account with `POST /access/users/me/merge` instead.
- The change replicates to every node. At most 6 attempts per minute."#,
    request_body(
        content = AttachSubjectRequest,
        example = json!({ "token": "<OIDC token of the sign-in to attach>" })
    ),
    responses(
        (status = 200, description = "The caller's account with the attached sign-in", body = GetUserResponse,
            example = json!({ "user_id": "<caller user id>", "name": "Alice Example",
                "subject_ids": ["<first sign-in>", "<attached sign-in>"], "attributes": {} })),
        (status = 401, description = "Missing or invalid bearer token, or an invalid OIDC token", body = ErrorResponse),
        (status = 403, description = "A login is too old (code `merge_refused`)", body = ErrorResponse),
        (status = 409, description = "The sign-in belongs to another account (code `subject_taken`)", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn attach_subject(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<AttachSubjectRequest>,
) -> ServerResult<Json<GetUserResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    charge(&state, auth.user_id)?;
    if !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }
    let now = unix_timestamp_secs();
    let login = bearer_auth_time(&state, bearer).await?;
    let identity = validate_oidc_token(&state, &request.token).await?;
    let fresh = identity
        .auth_time
        .is_some_and(|time| time.saturating_add(MAX_LINK_SECS) >= now);
    if !fresh_local(&auth, login, now) || !fresh {
        return Err(refused(MergeError::StaleLogin.to_string()));
    }
    let subject = oidc_subject_key(&identity.issuer, &identity.subject_id)
        .map_err(|_| ServerError::Unauthorized)?;
    let input = UpdateUserInput {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        self_realm_id: state.get_realm_id(),
        user_id: auth.user_id.to_string(),
        auth_context: auth,
        name: None,
        set_attributes: Default::default(),
        remove_attributes: Vec::new(),
        system: true,
        alias: None,
        subject: Some(subject),
    };
    let user = drive(UpdateUserOperation::new(input), &state.get_ctx())
        .await
        .map_err(|error| match error {
            UpdateUserError::SubjectClaimed => {
                ServerError::Refused(StatusCode::CONFLICT, "subject_taken", error.to_string())
            }
            UpdateUserError::UserNotFound => ServerError::NotFound,
            UpdateUserError::StorageError(_) | UpdateUserError::PlacementFenced => {
                ServerError::ServiceUnavailableReason(error.to_string())
            }
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(Json(user.into()))
}
