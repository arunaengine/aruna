//! Native federated login: the home realm signs a login handoff for its own user, and the
//! serving realm turns an admitted handoff into a federated session of its own.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::sessions::{CreateSessionResponse, map_create_error, unix_rfc3339};
use crate::routes::access::users::ensure_active;
use crate::server::state::ServerState;
use aruna_core::federation::{RealmDescriptor, Signed};
use aruna_core::handoff::{FEDERATED_SESSION_SECS, HandoffError, LoginHandoff, check_handoff};
use aruna_core::structs::identity::auth::{AuthContext, SessionKind};
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::realm::get_config::{GetConfigError, GetConfigOperation};
use aruna_operations::session::{CreateSessionConfig, CreateSessionOperation};
use aruna_operations::users::read_document::ReadUserOperation;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use serde::Deserialize;
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
const SECRET_LEN: usize = 32;

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
                "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
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
            "issuer": "<home realm id>",
            "user": "01JCNCTR0123456789ABCDEFGH@<home realm id>",
            "audience": "<serving realm id>",
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
    let local = state.get_realm_id();
    let portal = auth.session.as_ref().map(|session| session.kind) == Some(SessionKind::Portal);
    if auth.user_id.realm_id != local || !portal {
        return Err(ServerError::Forbidden);
    }
    let nonce_valid = hex::decode(&request.nonce).is_ok_and(|bytes| bytes.len() == SECRET_LEN);
    if !nonce_valid {
        return Err(ServerError::BadRequestReason(
            "nonce must be a hex SHA-256 digest".to_string(),
        ));
    }
    ensure_active(&state, auth.user_id).await?;
    let name = drive(ReadUserOperation::new(auth.user_id), &state.get_ctx())
        .await
        .ok()
        .map(|user| user.name)
        .filter(|name| !name.is_empty());
    let handoff = LoginHandoff::new(
        local,
        auth.user_id,
        &request.descriptor,
        request.nonce.to_ascii_lowercase(),
        name,
        unix_timestamp_secs(),
    )
    .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let signed = Signed::sign(handoff, state.node_capabilities())
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
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
- No user record is created for the foreign user.

**Limits**
- A handoff lives at most 60 seconds and may be issued at most 30 seconds in the future.
- At most 10 attempts per client address and minute; more answer 429 with code `rate_limited`."#,
    request_body(
        content = FederatedSessionRequest,
        description = "Signed handoff and browser secret",
        example = json!({
            "handoff": {
            "payload": {
                "issuer": "<home realm id>",
                "user": "01JCNCTR0123456789ABCDEFGH@<home realm id>",
                "audience": "<serving realm id>",
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
        (status = 201, description = "Federated session created; the token is shown only here", body = CreateSessionResponse,
            example = json!({
                "session_id": "01JCNCTR0123456789ABCDEFGH",
                "kind": "federated",
                "label": "",
                "token": "EXAMPLE-SESSION-TOKEN-PLACEHOLDER",
                "expires_at": "2026-04-09T20:00:00Z"
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
) -> ServerResult<(StatusCode, Json<CreateSessionResponse>)> {
    let peer = connect.map_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED), |Extension(info)| {
        info.0.ip()
    });
    let ip = crate::forwarded::client_ip(state.trusted_proxies(), peer, &headers);
    admit_login(&LOGIN_LIMITER, ip)?;
    let secret = hex::decode(&request.secret)
        .ok()
        .filter(|secret| secret.len() == SECRET_LEN)
        .ok_or_else(|| ServerError::BadRequestReason("secret must be 32 hex bytes".to_string()))?;
    let local = state.get_realm_id();
    let config = drive(GetConfigOperation::new(local), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GetConfigError::DocumentNotFound => ServerError::NotFound,
            error => ServerError::InternalError(error.to_string()),
        })?;
    let settings = config.federation.ok_or_else(|| {
        ServerError::Refused(
            StatusCode::FORBIDDEN,
            "federation_disabled",
            "this realm has no federation settings".to_string(),
        )
    })?;
    let now = unix_timestamp_secs();
    check_handoff(&request.handoff, &local, &settings, &secret, now).map_err(rejected)?;
    let handoff = request.handoff.payload;
    info!(
        handoff_id = %handoff.handoff_id,
        user_id = %handoff.user,
        "Federated login accepted"
    );
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now,
            expiry: now.saturating_add(FEDERATED_SESSION_SECS),
            user_id: handoff.user,
            realm_id: local,
            node_capabilities: state.node_capabilities().clone(),
            kind: SessionKind::Federated,
            label: None,
            name: handoff.name,
            restrictions: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;
    Ok((
        StatusCode::CREATED,
        Json(CreateSessionResponse {
            session_id: created.session.sid,
            kind: created.session.kind.to_string(),
            label: created.session.label.unwrap_or_default(),
            token: created.token.expose().to_string(),
            expires_at: unix_rfc3339(created.session.expires_at),
        }),
    ))
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
    use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, SessionRef};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
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
    ) -> ServerResult<(StatusCode, Json<CreateSessionResponse>)> {
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
        assert_eq!(created.kind, "federated");
        let claims = handle_token(&state, &created.token).await.unwrap();
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
                created.token,
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
}
