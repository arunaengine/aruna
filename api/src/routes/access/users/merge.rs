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
use aruna_operations::users::merge::handoff::{HandoffConfig, HandoffOperation};
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
        .routes(routes!(hand_off))
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
    /// Attributes the caller's account had no room for; they stay on the other account.
    pub attributes_not_copied: Vec<String>,
    /// Earlier secondaries of the other account that now point at `user_id`.
    pub secondaries: usize,
    /// The other account was already merged into the caller's; nothing changed.
    pub repeated: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct HandoffRequest {
    /// The management node that runs account merges from now on.
    pub node_id: String,
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

/// A merge or attach that reached a node which does not coordinate merges.
fn not_coordinator(message: String) -> ServerError {
    ServerError::Refused(StatusCode::SERVICE_UNAVAILABLE, "not_coordinator", message)
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
        MergeError::RoleConflict(_) => {
            ServerError::Refused(StatusCode::CONFLICT, "role_conflict", message)
        }
        MergeError::NotFound => ServerError::NotFound,
        MergeError::Unauthorized => ServerError::Forbidden,
        MergeError::InvalidCoordinator => ServerError::BadRequestReason(message),
        MergeError::NotCoordinator => not_coordinator(message),
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
  more than its own roles; the primary never loses a role. A deny of the other account that
  overlaps a moved grant moves with it, and one that would also cut the primary's own access
  refuses the merge.
- Its devices are re-owned by the primary and must sign in again.
- Profile and preference attributes the primary lacks are copied while they fit its 128 own
  attributes; `attributes_not_copied` names the rest. The list of secondaries does not count.
- Every bearer token, S3 credential and S3 session of the other account issued up to five minutes
  after the merge is cut off on every node. Its share links stop, as for any deactivated account.
- Runs, RO-Crates, provenance and other signed records keep naming the other account; reads of
  its id resolve to the primary.
- The change, including the cutoff, is one transaction on the realm's merge coordinator, the
  management node the realm config names (see `PUT /system/realm/merge-coordinator`); every
  other node passes the call there unchanged. Only that node hands the role on, so two opposite
  merges of the same accounts never both commit. It replicates to every node. Repeating a
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
                "attributes_not_copied": [],
                "secondaries": 0, "repeated": false })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A login is too old, the other token is invalid or of another realm, both name the same account, or an account is deactivated or a service account (code `merge_refused`)", body = ErrorResponse),
        (status = 404, description = "This node holds no record of one of the accounts", body = ErrorResponse),
        (status = 409, description = "The other account still holds a vault (code `vault_present`), or a role of it denies part of what it hands over and part of the caller's own access (code `role_conflict`, naming the role)", body = ErrorResponse),
        (status = 502, description = "The call was passed on but its answer was lost, so it may have been applied (code `relay_failed`)", body = ErrorResponse),
        (status = 503, description = "Storage or the vault holders were unavailable, or a document moved during the merge; retry. Code `not_coordinator` when the call reached a node that does not run merges, `no_management_node` when it could not be passed on", body = ErrorResponse)
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
        return Err(not_coordinator(MergeError::NotCoordinator.to_string()));
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
        attributes_not_copied: outcome.skipped_attributes,
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
- Like a merge, it runs on the realm's merge coordinator; every other node passes the call there.
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
        (status = 409, description = "The sign-in belongs to another account (code `subject_taken`)", body = ErrorResponse),
        (status = 502, description = "The call was passed on but its answer was lost, so it may have been applied (code `relay_failed`)", body = ErrorResponse),
        (status = 503, description = "The call reached a node that does not run merges (code `not_coordinator`) or could not be passed on (code `no_management_node`); retry", body = ErrorResponse)
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
        return Err(not_coordinator(MergeError::NotCoordinator.to_string()));
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

#[utoipa::path(
    put,
    path = "/system/realm/merge-coordinator",
    tag = "system/realm",
    summary = "Hand account merges to another management node",
    description = r#"Names the one management node that runs account merges.

**Authentication**: realm bearer token without path restrictions and WRITE on
`/{realm}/admin/config`.

**Behavior**
- Every node passes this call to the current merge coordinator. It writes the new name and stops
  merging in the same commit, so two nodes never merge at once.
- A realm names its first node when it is created. Otherwise only an administrator names one,
  and only while this node sees no coordinator recorded, or sees the recorded one removed from
  the realm or no longer a management node. A recorded node this node does not know yet is
  never replaced.
- Merges run on the named node only and answer 503 everywhere until the name has reached them.
- Name a new coordinator for a lost one only after the old coordinator is shut down: without
  consensus, a node that still believes it coordinates could merge until it learns otherwise."#,
    request_body(
        content = HandoffRequest,
        example = json!({ "node_id": "<node id of a management node>" })
    ),
    responses(
        (status = 204, description = "The node runs account merges from now on"),
        (status = 400, description = "The node id is malformed or names no management node of this realm", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller lacks the administration grant or the token is path-restricted", body = ErrorResponse),
        (status = 503, description = "This node does not coordinate merges and could not pass the call on; retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn hand_off(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<HandoffRequest>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }
    let node_id = request
        .node_id
        .parse()
        .map_err(|_| ServerError::BadRequestReason("malformed node id".to_string()))?;
    let config = HandoffConfig {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        auth_context: auth,
        node_id,
    };
    drive(HandoffOperation::new(config), &state.get_ctx())
        .await
        .map_err(map_merge_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::routes::{test_context, test_state, test_storage};
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn code(error: ServerError) -> (StatusCode, String) {
        (error.status_code(), error.response_body().code.unwrap())
    }

    #[tokio::test]
    async fn refuses_off_coordinator() {
        // A call relayed to a node that does not run merges names why, so a client can retry.
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
        let state = test_state(
            Arc::new(test_context(storage)),
            realm_id,
            node_id,
            capabilities,
        );
        let state = Arc::new(state.await);
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([3u8; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let expected = (
            StatusCode::SERVICE_UNAVAILABLE,
            "not_coordinator".to_string(),
        );
        let merge = MergeAccountRequest {
            token: "unused".to_string(),
        };
        let merged = merge_account(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Extension(None),
            Json(merge),
        )
        .await;
        assert_eq!(code(merged.err().unwrap()), expected);
        let attach = AttachSubjectRequest {
            token: "unused".to_string(),
        };
        let attached = attach_subject(
            State(state),
            Extension(Some(auth)),
            Extension(None),
            Json(attach),
        )
        .await;
        assert_eq!(code(attached.err().unwrap()), expected);
    }
}
