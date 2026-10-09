//! Native federated login: the home realm signs a login handoff for its own user, and the
//! serving realm turns an admitted handoff into a federated session of its own.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::sessions::{CreateSessionResponse, map_create_error, unix_rfc3339};
use crate::server::state::ServerState;
use aruna_core::federation::{RealmDescriptor, Signed};
use aruna_core::handoff::{HandoffError, LoginHandoff};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::federation::login::{
    FederatedLoginConfig, FederatedLoginError, FederatedLoginOperation, IssueHandoffConfig,
    IssueHandoffError, IssueHandoffOperation,
};
use aruna_operations::realm::get_config::GetConfigError;
use aruna_operations::users::search_users::{SearchUsersInput, SearchUsersOperation};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock};
use tracing::info;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Federated session attempts per client address and minute, and their burst.
const LOGINS_PER_MINUTE: u32 = 10;
/// Tracked client addresses before idle ones are dropped.
const LIMITER_ADDRESSES: usize = 4_096;
pub(crate) const SECRET_LEN: usize = 32;

static LOGIN_LIMITER: LazyLock<DefaultKeyedRateLimiter<IpAddr>> = LazyLock::new(login_limiter);

fn login_limiter() -> DefaultKeyedRateLimiter<IpAddr> {
    let rate = NonZeroU32::new(LOGINS_PER_MINUTE).expect("nonzero rate");
    RateLimiter::keyed(Quota::per_minute(rate))
}

#[derive(OpenApi)]
#[openapi(tags((name = "federation", description = "Native login across realms")))]
pub struct FederationLoginApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(FederationLoginApiDoc::openapi())
        .routes(routes!(create_login_handoff))
        .routes(routes!(create_federated_session))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct LoginHandoffRequest {
    /// The serving realm's signed descriptor as its portal sent it.
    #[schema(value_type = Object)]
    pub descriptor: Signed<RealmDescriptor>,
    /// Hex SHA-256 of the secret the serving realm's portal keeps.
    pub nonce: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct FederatedSessionResponse {
    #[serde(flatten)]
    pub session: CreateSessionResponse,
    /// The login is linked, so the session acts as the linked local account.
    pub linked: bool,
    /// A local account shows the same public name; offer to log in and link it. No account is
    /// named or selected.
    pub account_hint: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct FederatedSessionRequest {
    /// The signed login handoff from the user's home realm.
    #[schema(value_type = Object)]
    pub handoff: Signed<LoginHandoff>,
    /// Hex of the 32 byte secret whose SHA-256 is the handoff nonce.
    pub secret: String,
}

fn rejected(error: HandoffError) -> ServerError {
    ServerError::Refused(StatusCode::FORBIDDEN, "handoff_rejected", error.to_string())
}

/// Counts one attempt for `ip`, dropping idle addresses once the table grows large.
fn admit_login(limiter: &DefaultKeyedRateLimiter<IpAddr>, ip: IpAddr) -> ServerResult<()> {
    if limiter.len() > LIMITER_ADDRESSES {
        limiter.retain_recent();
    }
    limiter.check_key(&ip).map_err(|_| {
        ServerError::Refused(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many federated login attempts".to_string(),
        )
    })
}

#[utoipa::path(
    post,
    path = "/federation/login-handoffs",
    tag = "federation",
    summary = "Sign a login handoff for another realm",
    description = r#"Signs a login handoff that lets the caller log in to the realm of `descriptor`.

**Authentication**: an unrestricted `portal` session of a user of this realm. Federated
sessions and other session kinds are refused, so a login cannot chain to a third realm.

**Behavior**
- The descriptor must be signed by the realm it names and use HTTPS URLs; this realm itself is
  refused as audience.
- The handoff names the caller, the audience realm, the digest of that descriptor, the nonce
  and the caller's display name. It is valid for 60 seconds.
- The handoff is returned to the caller's portal only; it is never sent to the other realm by
  this server."#,
    request_body(
        content = LoginHandoffRequest,
        description = "Audience descriptor and nonce",
        example = json!({
            "descriptor": {
            "payload": {
                "realm_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
                "name": "Serving realm",
                "description": "",
                "api_url": "https://b.example.org/api/v1",
                "portal_url": "https://b.example.org/",
                "issued_at": 1791000000
            },
            "signer": "Realm",
            "signature": "<hex ed25519 signature>"
        },
            "nonce": "<hex sha256 of the browser secret>"
        })
    ),
    responses(
        (status = 200, description = "The signed login handoff", body = serde_json::Value,
            example = json!({
        "payload": {
            "issuer": [7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7],
            "user": { "user_ulid": "01JCNCTR0123456789ABCDEFGH", "realm_id": [7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7] },
            "audience": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            "descriptor_digest": "<hex sha256 of the serving descriptor>",
            "nonce": "<hex sha256 of the browser secret>",
            "name": "Ada Lovelace",
            "issued_at": 1791000000,
            "expires_at": 1791000060,
            "handoff_id": "01JCNCTR0123456789ABCDEFGJ"
        },
        "signer": "Realm",
        "signature": "<hex ed25519 signature>"
    })),
        (status = 400, description = "A malformed nonce, or a descriptor that does not verify", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not an unrestricted portal session of a local user, or the user is deactivated", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_login_handoff(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<LoginHandoffRequest>,
) -> ServerResult<Json<Signed<LoginHandoff>>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let nonce_valid = hex::decode(&request.nonce).is_ok_and(|bytes| bytes.len() == SECRET_LEN);
    if !nonce_valid {
        return Err(ServerError::BadRequestReason(
            "nonce must be a hex SHA-256 digest".to_string(),
        ));
    }
    let config = IssueHandoffConfig {
        auth_context: auth,
        descriptor: request.descriptor,
        nonce: request.nonce.to_ascii_lowercase(),
        node_capabilities: state.node_capabilities().clone(),
        now: unix_timestamp_secs(),
    };
    let signed = drive(IssueHandoffOperation::new(config), &state.get_ctx())
        .await
        .map_err(|error| match error {
            IssueHandoffError::Refused | IssueHandoffError::Deactivated => ServerError::Forbidden,
            IssueHandoffError::Invalid(error) => ServerError::BadRequestReason(error.to_string()),
            error => ServerError::InternalError(error.to_string()),
        })?;
    Ok(Json(signed))
}

#[utoipa::path(
    post,
    path = "/federation/sessions",
    tag = "federation",
    summary = "Open a federated session from a login handoff",
    description = r#"Creates a session of this realm for a user of another realm.

**Authentication**: none; the signed handoff and the browser secret authenticate the call.

**Behavior**
- The handoff must be signed by the user's home realm, name this realm as audience and the
  digest of this realm's current descriptor, and match the secret. A superseded descriptor is
  refused.
- The home realm must be admitted by `accepted_realms` in the federation settings.
- The session has kind `federated`, carries the display name for display only, and lasts
  8 hours. It cannot be renewed and cannot create child sessions or tokens.
- When the login is linked to exactly one active local account, the session acts as that
  account (`linked` true) and names the login as `via`; roles come from the account only.
- Otherwise no user record is created for the foreign user. `account_hint` is true when an
  active local account shows the same public name, compared without case; it names nothing.

**Limits**
- A handoff lives at most 60 seconds and may be issued at most 30 seconds in the future.
- At most 10 attempts per client address and minute; more answer 429 with code `rate_limited`."#,
    request_body(
        content = FederatedSessionRequest,
        description = "Signed handoff and browser secret",
        example = json!({
            "handoff": {
            "payload": {
                "issuer": [7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7],
                "user": { "user_ulid": "01JCNCTR0123456789ABCDEFGH", "realm_id": [7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7] },
                "audience": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
                "descriptor_digest": "<hex sha256 of the serving descriptor>",
                "nonce": "<hex sha256 of the browser secret>",
                "name": "Ada Lovelace",
                "issued_at": 1791000000,
                "expires_at": 1791000060,
                "handoff_id": "01JCNCTR0123456789ABCDEFGJ"
            },
            "signer": "Realm",
            "signature": "<hex ed25519 signature>"
        },
            "secret": "<hex of the 32 byte browser secret>"
        })
    ),
    responses(
        (status = 201, description = "Federated session created; the token is shown only here", body = FederatedSessionResponse,
            example = json!({
                "session_id": "01JCNCTR0123456789ABCDEFGH",
                "kind": "federated",
                "label": "",
                "token": "EXAMPLE-SESSION-TOKEN-PLACEHOLDER",
                "expires_at": "2026-04-09T20:00:00Z",
                "linked": false,
                "account_hint": true
            })),
        (status = 400, description = "A malformed secret", body = ErrorResponse),
        (status = 403, description = "The handoff was refused, or this realm has no federation settings; code `handoff_rejected` or `federation_disabled`", body = ErrorResponse),
        (status = 409, description = "The user already holds 256 active sessions here", body = ErrorResponse)
    )
)]
pub async fn create_federated_session(
    State(state): State<Arc<ServerState>>,
    connect: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    Json(request): Json<FederatedSessionRequest>,
) -> ServerResult<(StatusCode, Json<FederatedSessionResponse>)> {
    let peer = connect.map_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED), |Extension(info)| {
        info.0.ip()
    });
    let ip = crate::forwarded::client_ip(state.trusted_proxies(), peer, &headers);
    admit_login(&LOGIN_LIMITER, ip)?;
    let secret = hex::decode(&request.secret)
        .ok()
        .filter(|secret| secret.len() == SECRET_LEN)
        .ok_or_else(|| ServerError::BadRequestReason("secret must be 32 hex bytes".to_string()))?;
    let (handoff_id, user_id) = (
        request.handoff.payload.handoff_id,
        request.handoff.payload.user,
    );
    let name = request.handoff.payload.name.clone();
    let config = FederatedLoginConfig {
        realm_id: state.get_realm_id(),
        handoff: request.handoff,
        secret,
        node_capabilities: state.node_capabilities().clone(),
        now: unix_timestamp_secs(),
    };
    let created = drive(FederatedLoginOperation::new(config), &state.get_ctx())
        .await
        .map_err(|error| match error {
            FederatedLoginError::Config(GetConfigError::DocumentNotFound) => ServerError::NotFound,
            FederatedLoginError::Disabled => ServerError::Refused(
                StatusCode::FORBIDDEN,
                "federation_disabled",
                error.to_string(),
            ),
            FederatedLoginError::Rejected(error) => rejected(error),
            FederatedLoginError::Session(error) => map_create_error(error),
            error => ServerError::InternalError(error.to_string()),
        })?;
    let account = created.session.user_id;
    info!(%handoff_id, %user_id, %account, "Federated login accepted");
    let linked = created.session.via.is_some();
    let account_hint = match name.filter(|_| !linked) {
        Some(name) => name_hint(&state, &name).await?,
        None => false,
    };
    Ok((
        StatusCode::CREATED,
        Json(FederatedSessionResponse {
            session: CreateSessionResponse {
                session_id: created.session.sid,
                kind: created.session.kind.to_string(),
                label: created.session.label.unwrap_or_default(),
                token: created.token.expose().to_string(),
                expires_at: unix_rfc3339(created.session.expires_at),
            },
            linked,
            account_hint,
        }),
    ))
}

/// Whether an active, non-service local account shows `name` as its public name.
async fn name_hint(state: &ServerState, name: &str) -> ServerResult<bool> {
    let search = SearchUsersOperation::new(SearchUsersInput {
        realm_id: state.get_realm_id(),
        query: name.trim().to_string(),
        limit: 1,
        start_after: None,
        exact_name: true,
    });
    let found = drive(search, &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(!found.users.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::handle_token;
    use crate::routes::access::sessions::{CreateSessionRequest, create_session};
    use crate::tests::routes::{test_context, test_state, test_storage};
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::federation::{AcceptedRealms, FederationSettings, RegistrationMode};
    use aruna_core::handoff::secret_nonce;
    use aruna_core::keyspaces::{FEDERATION_KEYSPACE, USER_KEYSPACE};
    use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, SessionKind, SessionRef};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::realm::get_config::GetConfigOperation;
    use axum::response::IntoResponse;
    use ed25519_dalek::SigningKey;
    use tempfile::TempDir;
    use ulid::Ulid;
    use url::Url;

    const SECRET: [u8; SECRET_LEN] = [4; SECRET_LEN];

    fn home_key() -> SigningKey {
        SigningKey::from_bytes(&[11; 32])
    }

    fn home_user() -> UserId {
        UserId::new(
            Ulid::from_bytes([3; 16]),
            RealmId::from_bytes(home_key().verifying_key().to_bytes()),
        )
    }

    /// A serving realm that admits the home realm, with its federation settings.
    async fn serving() -> (TempDir, Arc<ServerState>, FederationSettings) {
        let (dir, storage) = test_storage();
        let context = Arc::new(test_context(storage));
        let key = SigningKey::from_bytes(&[12; 32]);
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();
        let actor = Actor {
            node_id,
            user_id: UserId::nil(realm_id),
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
        let capabilities = NodeCapabilities::management_node(key).unwrap();
        let descriptor = RealmDescriptor {
            realm_id,
            name: "Serving".to_string(),
            description: String::new(),
            api_url: Url::parse("https://b.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://b.example.org").unwrap(),
            issued_at: 10,
        };
        let settings = FederationSettings {
            name: descriptor.name.clone(),
            api_url: descriptor.api_url.clone(),
            portal_url: descriptor.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::Only(vec![home_user().realm_id]),
            descriptor: Signed::sign(descriptor, &capabilities).unwrap(),
        };
        let mut config = drive(GetConfigOperation::new(realm_id), &context)
            .await
            .unwrap();
        config.federation = Some(settings.clone());
        let target = DocumentTarget::RealmConfig { realm_id };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
        (dir, state, settings)
    }

    fn handoff(settings: &FederationSettings) -> Signed<LoginHandoff> {
        let home = NodeCapabilities::management_node(home_key()).unwrap();
        let payload = LoginHandoff::new(
            home_user().realm_id,
            home_user(),
            &settings.descriptor,
            secret_nonce(&SECRET),
            Some("Ada".to_string()),
            unix_timestamp_secs(),
        )
        .unwrap();
        Signed::sign(payload, &home).unwrap()
    }

    async fn login(
        state: &Arc<ServerState>,
        handoff: Signed<LoginHandoff>,
        secret: [u8; SECRET_LEN],
    ) -> ServerResult<(StatusCode, Json<FederatedSessionResponse>)> {
        create_federated_session(
            State(state.clone()),
            None,
            HeaderMap::new(),
            Json(FederatedSessionRequest {
                handoff,
                secret: hex::encode(secret),
            }),
        )
        .await
    }

    #[tokio::test]
    async fn opens_federated_session() {
        // The session names the foreign user, carries the name and cannot create children.
        let (_dir, state, settings) = serving().await;
        let (status, Json(created)) = login(&state, handoff(&settings), SECRET).await.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.session.kind, "federated");
        let claims = handle_token(&state, &created.session.token).await.unwrap();
        let auth = AuthContext::try_from(claims).unwrap();
        assert_eq!(auth.user_id, home_user());
        assert_eq!(auth.realm_id, state.get_realm_id());
        let session = auth.session.clone().unwrap();
        assert_eq!(session.kind, SessionKind::Federated);
        assert_eq!(session.name.as_deref(), Some("Ada"));

        let child = create_session(
            State(state.clone()),
            Extension(Some(auth)),
            Extension(Some(crate::auth::ValidatedBearer::new_for_test(
                created.session.token,
            ))),
            Json(CreateSessionRequest {
                kind: "portal".to_string(),
                label: None,
                expires_in_seconds: None,
                path_restrictions: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(child.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn refuses_wrong_secret() {
        let (_dir, state, settings) = serving().await;
        let error = login(&state, handoff(&settings), [5; SECRET_LEN])
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn refuses_chained_handoff() {
        // A federated session cannot sign a handoff for a third realm; a local portal can.
        let (_dir, state, _settings) = serving().await;
        let home = NodeCapabilities::management_node(home_key()).unwrap();
        let audience = RealmDescriptor {
            realm_id: home_user().realm_id,
            name: "Home".to_string(),
            description: String::new(),
            api_url: Url::parse("https://a.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://a.example.org").unwrap(),
            issued_at: 1,
        };
        let request = || LoginHandoffRequest {
            descriptor: Signed::sign(audience.clone(), &home).unwrap(),
            nonce: secret_nonce(&SECRET),
        };
        let session = |kind| {
            Some(SessionRef {
                sid: Ulid::generate().to_string(),
                kind,
                name: None,
                via: None,
            })
        };
        let federated = AuthContext {
            user_id: home_user(),
            realm_id: state.get_realm_id(),
            path_restrictions: None,
            session: session(SessionKind::Federated),
        };
        let error = create_login_handoff(
            State(state.clone()),
            Extension(Some(federated)),
            Json(request()),
        )
        .await
        .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
        let local = AuthContext {
            user_id: UserId::local(Ulid::generate(), state.get_realm_id()),
            realm_id: state.get_realm_id(),
            path_restrictions: None,
            session: session(SessionKind::Portal),
        };
        let Json(signed) = create_login_handoff(
            State(state.clone()),
            Extension(Some(local)),
            Json(request()),
        )
        .await
        .unwrap();
        assert_eq!(signed.verify(&state.get_realm_id()), Ok(()));
        assert_eq!(signed.payload.audience, home_user().realm_id);
    }

    #[test]
    fn login_attempts_limited() {
        let limiter = login_limiter();
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        for _ in 0..LOGINS_PER_MINUTE {
            admit_login(&limiter, ip).unwrap();
        }
        assert!(admit_login(&limiter, ip).is_err());
        admit_login(&limiter, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2))).unwrap();
    }

    #[tokio::test]
    async fn linked_login_maps() {
        // A login linked to one active account opens that account's session; the hint names
        // nothing and only matches a public name.
        let (_dir, state, settings) = serving().await;
        let (status, Json(plain)) = login(&state, handoff(&settings), SECRET).await.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert!(!plain.linked && !plain.account_hint);
        let realm_id = state.get_realm_id();
        let local = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: local,
            realm_id,
        };
        let user = aruna_core::structs::identity::user::User {
            user_id: local,
            name: "ada".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: [home_user()].into(),
            attributes: Default::default(),
        };
        let context = state.get_ctx();
        let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| {
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
        };
        let user_key = local.to_bytes();
        write(USER_KEYSPACE, user_key, user.to_bytes(&actor).unwrap()).await;
        let (_, Json(hinted)) = login(&state, handoff(&settings), SECRET).await.unwrap();
        assert!(!hinted.linked && hinted.account_hint);
        let claims = std::collections::BTreeSet::from([local]);
        let key = aruna_core::link::alias_claims_key(&home_user());
        let claims = postcard::to_allocvec(&claims).unwrap();
        write(FEDERATION_KEYSPACE, key, claims).await;
        let (_, Json(linked)) = login(&state, handoff(&settings), SECRET).await.unwrap();
        assert!(linked.linked && !linked.account_hint);
        let token = handle_token(&state, &linked.session.token).await.unwrap();
        let auth = AuthContext::try_from(token).unwrap();
        assert_eq!(auth.user_id, local);
        assert_eq!(auth.session.unwrap().via, Some(home_user()));
    }
}
