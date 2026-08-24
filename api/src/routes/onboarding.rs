use crate::error::{ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, CreateOnboardingSecretRequest,
    CreateOnboardingSecretResponse, OnboardingMode, OnboardingPurpose, OnboardingSecret,
    OnboardingSecretRecord, OnboardingSecretState, RequestedOnboardingMode,
    bootstrap_issuer_proof_message, bootstrap_node_proof_message,
};
use aruna_core::structs::{
    AuthContext, NodeCapabilities, Permission, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
    StaticRealmEndpoint,
};
use aruna_operations::bootstrap_onboarding_finalize::{
    BootstrapOnboardingFinalizeError, BootstrapOnboardingFinalizeInput,
    bootstrap_onboarding_finalize,
};
use aruna_operations::consume_onboarding_secret::ConsumeOnboardingSecretError;
use aruna_operations::create_onboarding_secret::{
    CreateOnboardingSecretError, CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
};
use aruna_operations::delete_onboarding_secret::{
    DeleteOnboardingSecretError, DeleteOnboardingSecretInput, DeleteOnboardingSecretOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::ensure_realm_config::EnsureRealmConfigError;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::list_onboarding_secrets::ListOnboardingSecretsOperation;
use aruna_operations::reserve_onboarding_secret::ReserveOnboardingSecretError;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey,
    aead::{Aead, AeadCore, OsRng as CryptoOsRng},
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand::Rng;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use url::Url;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const DEFAULT_ONBOARDING_SECRET_TTL_SECS: u64 = 3600;

#[derive(OpenApi)]
#[openapi(
    tags((name = "onboarding", description = "Node onboarding and bootstrap operations"))
)]
pub struct OnboardingApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(OnboardingApiDoc::openapi())
        .routes(routes!(bootstrap_onboarding))
        .routes(routes!(create_onboarding_secret, list_onboarding_secrets))
        .routes(routes!(revoke_onboarding_secret))
        .routes(routes!(get_onboarding_secret_status))
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BootstrapEndpointDoc {
    pub id: String,
    pub addrs: Vec<TransportAddressDoc>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum TransportAddressDoc {
    Ip(String),
    Relay(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CreateOnboardingSecretRequestDoc {
    pub seed_url: String,
    pub mode: String,
    pub expires_in_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CreateOnboardingSecretResponseDoc {
    pub onboarding_secret: String,
    pub mode: String,
    pub expires_at: u64,
    pub enroll_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BootstrapOnboardingRequestDoc {
    pub onboarding_secret: String,
    pub node_id: String,
    pub node_proof: String,
    pub transport_public_key: Option<String>,
    pub issuer_public_key: Option<String>,
    pub issuer_proof: Option<String>,
    pub node_location: Option<String>,
    pub node_weight: Option<u32>,
    pub node_labels: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BootstrapOnboardingResponseDoc {
    pub realm_id: String,
    pub mode: String,
    pub temporary_bootstrap_endpoint: BootstrapEndpointDoc,
    pub wrapped_realm_private_key: Option<String>,
    pub wrapped_realm_private_key_nonce: Option<String>,
    pub wrapping_public_key: Option<String>,
    pub delegation_signature: Option<String>,
    pub onboarding_sync_ticket: String,
    pub realm_endpoints: Vec<RealmEndpointDoc>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct RealmEndpointDoc {
    pub node_id: String,
    pub endpoint_addr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ListOnboardingSecretsResponse {
    pub secrets: Vec<OnboardingSecretSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct OnboardingSecretStatusResponse {
    pub enrollment_id: String,
    pub mode: String,
    /// Owner a `User` secret is bound to; null for infrastructure modes.
    pub owner: Option<String>,
    /// `pending`, `claimed` or `expired`.
    pub status: String,
    /// Node that claimed the secret, once one has.
    pub claimed_node_id: Option<String>,
    pub expires_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct OnboardingSecretSummary {
    pub enrollment_id: String,
    pub mode: String,
    /// Owner a `User` secret is bound to; null for infrastructure modes.
    pub owner: Option<String>,
    pub expires_at: u64,
    pub claimed_node_id: Option<String>,
}

impl From<OnboardingSecretRecord> for OnboardingSecretSummary {
    fn from(record: OnboardingSecretRecord) -> Self {
        Self {
            enrollment_id: record.enrollment_id.to_string(),
            mode: format!("{:?}", RequestedOnboardingMode::from(record.mode)),
            owner: record.mode.owner().map(|owner| owner.to_string()),
            expires_at: record.expires_at,
            claimed_node_id: record.claimed_node_id,
        }
    }
}

async fn authorize_onboarding_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id || !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    // Route through the single authorization boundary so realm and group
    // request policies also constrain onboarding administration.
    crate::auth::ensure_permission(
        state,
        &auth,
        format!("/{realm_id}/admin/onboarding"),
        Permission::WRITE,
    )
    .await?;

    Ok(auth)
}

/// Device enrollment is self-service: any authenticated member of this realm
/// may mint a User-mode secret, and it is always bound to the caller.
fn authorize_device_enrollment(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    // Only a management node redeems enrollment, so only it may mint.
    if auth.realm_id != state.get_realm_id() || auth.user_id.is_nil() || !state.is_management_node()
    {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

async fn prune_stale_onboarding_secrets(state: &Arc<ServerState>) -> ServerResult<()> {
    let now = now_timestamp();
    let secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    for secret in secrets {
        if secret.record.expires_at < now
            && !matches!(&secret.state, OnboardingSecretState::Finalizing { .. })
        {
            drive(
                DeleteOnboardingSecretOperation::new(DeleteOnboardingSecretInput {
                    enrollment_id: secret.record.enrollment_id,
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(map_delete_error)?;
        }
    }

    Ok(())
}

#[utoipa::path(
    post,
    path = "/admin/onboarding/secrets",
    tag = "onboarding",
    summary = "Mint a node enrollment secret",
    description = r#"Mints a single-use enrollment secret that a joining node or device redeems to enter the realm.

**Authentication**: `Management` and `Server` secrets need a bearer token of this realm with WRITE
on the realm's onboarding admin path. A `User` secret is self-service: any authenticated member of
this realm may mint one for itself. Only a management node serves the route and any other node
answers 403.

**Behavior**
- The response carries the enrollment secret exactly once: the node stores only its hash, so a
  secret that is lost cannot be recovered and must be revoked and minted again.
- Treat the value like a credential, hand it to exactly one joiner, and expect it to be single-use.
- `mode` fixes what the joiner may become and is one of `Management`, `Server` or `User`. A
  `Management` secret later lets the joiner receive the realm private key wrapped to its transport
  key, so it is the most sensitive of the three.
- A `User` secret enrolls an owner-bound device. Its owner is always the calling credential: the
  request body cannot name one, so a device secret can never enroll a node for somebody else.
- Every expired secret that is not already mid-enrollment is discarded before the new one is
  created.

- `seed_url` is the base URL the joiner calls back. Leave it empty to have this node fill in its
  own published REST base URL, which is what a device mint from the portal does.
- A `User` mint also returns `enroll_url`, the deep link a device app opens to claim the secret
  without a copy and paste. Its shape is a contract both the desktop app and the portal wizard
  parse: scheme `aruna`, host `enroll`, and the query keys `secret` (this very secret), `seed`
  (the seed URL above) and `realm` (this realm's id). It is null for the other modes.

**Limits**
- `expires_in_seconds` defaults to 3600 and is clamped to 60..=86400; `expires_at` is the resulting
  absolute expiry in Unix seconds.
- A `User` mint is refused once the owner holds the realm's `max_devices_per_user` devices.
  Enrolled devices and unclaimed device secrets both occupy a slot, so two mints cannot race past
  the cap. A realm without that quota set caps nothing.

**Errors**: an owner at the device cap answers 409. An empty `seed_url` on a node that publishes no
REST interface answers 400."#,
    request_body(
        content = CreateOnboardingSecretRequestDoc,
        description = "Seed URL the joiner calls back, the mode it is enrolled as, and an optional lifetime",
        example = json!({
            "seed_url": "https://node.example.test/api/v1",
            "mode": "Server",
            "expires_in_seconds": 3600
        })
    ),
    responses(
        (
            status = 201,
            description = "Secret created; `onboarding_secret` is shown here and never again",
            body = CreateOnboardingSecretResponseDoc,
            example = json!({
                "onboarding_secret": "<enrollment-secret-shown-once>",
                "mode": "Server",
                "expires_at": 1775748191,
                "enroll_url": null
            })
        ),
        (status = 400, description = "No seed URL was given and this node publishes no REST interface", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Token belongs to another realm, this is not a management node, or the caller lacks WRITE on the realm's onboarding admin path", body = crate::error::ErrorResponse),
        (status = 409, description = "The owner already holds the realm's maximum number of devices", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_onboarding_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateOnboardingSecretRequest>,
) -> ServerResult<(StatusCode, Json<CreateOnboardingSecretResponse>)> {
    let auth = match request.mode {
        RequestedOnboardingMode::User => authorize_device_enrollment(&state, auth)?,
        RequestedOnboardingMode::Management | RequestedOnboardingMode::Server => {
            authorize_onboarding_admin(&state, auth).await?
        }
    };
    // The owner binding comes from the caller's credential, never the body.
    let mode = match request.mode {
        RequestedOnboardingMode::Management => OnboardingMode::Management,
        RequestedOnboardingMode::Server => OnboardingMode::Server,
        RequestedOnboardingMode::User => OnboardingMode::User {
            owner: auth.user_id,
        },
    };
    prune_stale_onboarding_secrets(&state).await?;

    let ttl = request
        .expires_in_seconds
        .unwrap_or(DEFAULT_ONBOARDING_SECRET_TTL_SECS)
        .clamp(60, 86_400);
    let expires_at = now_timestamp().saturating_add(ttl);

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);

    let seed_url = seed_url(&state, request.seed_url).await?;
    let onboarding_secret = OnboardingSecret {
        seed_url,
        enrollment_id: ulid::Ulid::generate(),
        secret: secret_bytes,
        mode,
        realm_id: state.get_realm_id(),
        purpose: OnboardingPurpose::NodeEnrollment,
    };
    let encoded_secret = onboarding_secret
        .encode()
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let record = OnboardingSecretRecord {
        enrollment_id: onboarding_secret.enrollment_id,
        secret_hash: onboarding_secret.secret_hash(),
        mode: onboarding_secret.mode,
        purpose: OnboardingPurpose::NodeEnrollment,
        expires_at,
        claimed_node_id: None,
    };

    drive(
        CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput { record }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;

    let enroll_url = match request.mode {
        RequestedOnboardingMode::User => Some(enroll_url(
            &encoded_secret,
            &onboarding_secret.seed_url,
            state.get_realm_id(),
        )?),
        RequestedOnboardingMode::Management | RequestedOnboardingMode::Server => None,
    };

    Ok((
        StatusCode::CREATED,
        Json(CreateOnboardingSecretResponse {
            onboarding_secret: encoded_secret,
            mode: request.mode,
            expires_at,
            enroll_url,
        }),
    ))
}

/// The URL a joiner calls back. An empty request field means "this node", which
/// is what a self-service device mint sends, so the wizard needs no knowledge
/// of how the node is published.
async fn seed_url(state: &Arc<ServerState>, requested: String) -> ServerResult<String> {
    if !requested.trim().is_empty() {
        return Ok(requested);
    }
    state
        .interface_state()
        .await
        .rest
        .map(|rest| rest.base_url)
        .ok_or_else(|| {
            ServerError::BadRequestReason(
                "seed_url is required: this node serves no published REST interface".to_string(),
            )
        })
}

/// The `aruna://enroll` deep link a device app opens. The contract the desktop
/// app and the portal wizard both parse: scheme `aruna`, host `enroll`, and the
/// query keys `secret`, `seed` and `realm`.
fn enroll_url(secret: &str, seed_url: &str, realm_id: RealmId) -> ServerResult<String> {
    let mut url =
        Url::parse("aruna://enroll").map_err(|err| ServerError::InternalError(err.to_string()))?;
    url.query_pairs_mut()
        .append_pair("secret", secret)
        .append_pair("seed", seed_url)
        .append_pair("realm", &realm_id.to_string());
    Ok(url.to_string())
}

#[utoipa::path(
    get,
    path = "/admin/onboarding/secrets",
    tag = "onboarding",
    summary = "List outstanding node enrollment secrets",
    description = r#"Lists this management node's live and in-flight enrollment secrets as bookkeeping only.

**Authentication**: bearer token of this realm with WRITE on the realm's onboarding admin path;
only a management node serves it and any other node answers 403.

**Behavior**
- Each entry carries the enrollment id, the mode, the owner a `User` secret is bound to, the
  absolute expiry in Unix seconds and, once a joiner has claimed the secret, the node id it was
  claimed by.
- The secret value itself is never returned here, because only its hash was kept when it was
  minted.
- Expired secrets that are not mid-enrollment are discarded before the list is built, so the list
  holds live and in-flight enrollments; entries are ordered by expiry, soonest first.
- The list is this management node's local state, not a realm-wide fan-out."#,
    responses(
        (
            status = 200,
            description = "Live and in-flight enrollment secrets, soonest expiry first",
            body = ListOnboardingSecretsResponse,
            example = json!({
                "secrets": [
                    {
                        "enrollment_id": "01JABCDEF0123456789ABCDEFG",
                        "mode": "Server",
                        "owner": null,
                        "expires_at": 1775748191,
                        "claimed_node_id": null
                    },
                    {
                        "enrollment_id": "01JMETADATA0123456789ABCDE",
                        "mode": "User",
                        "owner": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "expires_at": 1775751791,
                        "claimed_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978"
                    }
                ]
            })
        ),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Token belongs to another realm, this is not a management node, or the caller lacks WRITE on the realm's onboarding admin path", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_onboarding_secrets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListOnboardingSecretsResponse>)> {
    let _auth = authorize_onboarding_admin(&state, auth).await?;
    prune_stale_onboarding_secrets(&state).await?;
    let mut secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    secrets.sort_by_key(|entry| entry.record.expires_at);

    Ok((
        StatusCode::OK,
        Json(ListOnboardingSecretsResponse {
            secrets: secrets
                .into_iter()
                .map(|entry| OnboardingSecretSummary::from(entry.record))
                .collect(),
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/admin/onboarding/secrets/{id}",
    tag = "onboarding",
    summary = "Revoke a pending node enrollment secret",
    description = r#"Deletes the enrollment record on this node, making the secret unredeemable from here on.

**Authentication**: bearer token of this realm with WRITE on the realm's onboarding admin path;
only a management node serves it and any other node answers 403.

**Behavior**
- This is the remedy for a secret that leaked or was never used, and it is the only remedy, since
  the secret value itself was never stored.
- Revocation does not undo an enrollment that already completed: a node that finished bootstrapping
  stays a member of the realm and has to be removed through the realm configuration instead.

**Errors**: a secret that is already gone, expired and pruned or revoked by an earlier call,
answers 404."#,
    params(("id" = String, Path, description = "Enrollment id of the secret, the ULID reported when it was minted and by the list endpoint")),
    responses(
        (status = 204, description = "Secret deleted and no longer redeemable; no response body"),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Token belongs to another realm, this is not a management node, or the caller lacks WRITE on the realm's onboarding admin path", body = crate::error::ErrorResponse),
        (status = 404, description = "No enrollment secret with this id on this node", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_onboarding_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(enrollment_id): Path<String>,
) -> ServerResult<StatusCode> {
    let _auth = authorize_onboarding_admin(&state, auth).await?;
    let enrollment_id = Ulid::from_string(&enrollment_id).map_err(|_| ServerError::BadRequest)?;

    drive(
        DeleteOnboardingSecretOperation::new(DeleteOnboardingSecretInput { enrollment_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_delete_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/onboarding/secrets/{id}/status",
    tag = "onboarding",
    summary = "Poll an enrollment secret's claim state",
    description = r#"Reports whether an outstanding enrollment secret is still pending, already claimed or expired.

**Authentication**: bearer token of this realm. The owner a `User` secret is bound to may poll its
own secret; every other caller needs WRITE on the realm's onboarding admin path. Only a management
node serves the route.

**Behavior**
- This is the wizard's progress poll: mint a secret, hand it to the joiner, then watch this route
  until `status` turns `claimed` and `claimed_node_id` names the node that redeemed it.
- `pending` means the secret is live and unclaimed, `claimed` means a joiner has reserved,
  finalized or consumed it, and `expired` means its lifetime ran out before any joiner claimed it.
- A claim outlives the secret's expiry: a secret claimed before it expired keeps reading `claimed`.
- The secret value itself is never returned, because only its hash was kept when it was minted.
- Nothing here is realm-wide: it is the claim state this management node recorded.

**Errors**: an unknown, revoked or already pruned enrollment id answers 404, which is also what a
caller sees after the enrollment finished and the record was cleaned up."#,
    params(("id" = String, Path, description = "Enrollment id of the secret, the ULID reported when it was minted")),
    responses(
        (
            status = 200,
            description = "The claim state this node recorded for the secret",
            body = OnboardingSecretStatusResponse,
            example = json!({
                "enrollment_id": "01JABCDEF0123456789ABCDEFG",
                "mode": "User",
                "owner": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "status": "claimed",
                "claimed_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                "expires_at": 1775748191
            })
        ),
        (status = 400, description = "The enrollment id is not a ULID", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or unusable bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is neither the secret's owner nor an onboarding administrator, or this is not a management node", body = crate::error::ErrorResponse),
        (status = 404, description = "No enrollment secret with this id on this node", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_onboarding_secret_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(enrollment_id): Path<String>,
) -> ServerResult<(StatusCode, Json<OnboardingSecretStatusResponse>)> {
    let enrollment_id = Ulid::from_string(&enrollment_id).map_err(|_| ServerError::BadRequest)?;
    let caller = auth.clone().ok_or(ServerError::Unauthorized)?;
    let entry = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?
        .into_iter()
        .find(|entry| entry.record.enrollment_id == enrollment_id)
        .ok_or(ServerError::NotFound)?;

    // The owner polls its own device; anybody else needs the admin path.
    let owns = caller.realm_id == state.get_realm_id()
        && entry.record.mode.owner() == Some(caller.user_id);
    if !owns {
        authorize_onboarding_admin(&state, auth).await?;
    }

    let claimed_node_id = entry.state.claimed_node_id().map(str::to_string);
    let status = match (&claimed_node_id, entry.record.expires_at < now_timestamp()) {
        (Some(_), _) => "claimed",
        (None, true) => "expired",
        (None, false) => "pending",
    };

    Ok((
        StatusCode::OK,
        Json(OnboardingSecretStatusResponse {
            enrollment_id: entry.record.enrollment_id.to_string(),
            mode: format!("{:?}", RequestedOnboardingMode::from(entry.record.mode)),
            owner: entry.record.mode.owner().map(|owner| owner.to_string()),
            status: status.to_string(),
            claimed_node_id,
            expires_at: entry.record.expires_at,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/onboarding/bootstrap",
    tag = "onboarding",
    summary = "Redeem an enrollment secret and join the realm",
    description = r#"Redeems an enrollment secret and returns the one-time material a joiner needs to enter the realm.

**Authentication**: none; deliberately unauthenticated, because a joining node has no realm token
yet. The enrollment secret plus a signature by the joiner's own node key are the credentials, and
only a management node serves this route.

**Behavior**
- The secret is single-use and is consumed when enrollment finalizes, which also adds the joiner to
  the realm configuration; a rejected attempt leaves the secret usable so an operator does not have
  to mint a new one after a typo.
- What must be sent depends on the mode the secret was minted for: a `Server` secret additionally
  requires `issuer_public_key` and a matching `issuer_proof` and returns a delegation signature; a
  `Management` secret requires `transport_public_key` and returns the realm private key encrypted
  to it, along with the nonce and the ephemeral public key needed to open it.
- A `User` secret needs neither: a device holds no realm key and no issuer delegation, and joins as
  an owner-bound member that never becomes a sync, holder or placement target. Its `mode` echoes
  the owner the secret was bound to at mint time.
- The response always carries the realm id, the temporary endpoint to dial, a one-time sync ticket
  the joiner uses to fetch the realm's core documents, and `realm_endpoints`.
- `realm_endpoints` carries the realm's declared static discovery endpoints, kept only for nodes
  that are configured, sync-eligible members. A joiner that cannot read the DHT, a device in
  particular, dials them to reach the realm without discovery. It is a starting point, not the
  realm membership, and a realm on dynamic discovery declares none, leaving the list empty.
- `node_location`, `node_weight` and `node_labels` seed the joiner's placement entry and are
  optional.
- Everything returned here is one-time joining material and must never be logged or reused.

**Errors**: an unknown, expired, already claimed or unmatched secret and a signature that does not
verify are both refused with 401, without saying which of the two failed. A node that does not
serve enrollment answers 403."#,
    request_body(
        content = BootstrapOnboardingRequestDoc,
        description = "The enrollment secret, the joiner's node id, its proof of possession of the node key, and any mode-specific key material",
        example = json!({
            "onboarding_secret": "<onboarding-secret-shown-once>",
            "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
            "node_proof": "<node-key-proof-signature>",
            "issuer_public_key": "<issuer-public-key>",
            "issuer_proof": "<issuer-proof-signature>",
            "node_location": "dc-a",
            "node_weight": 100,
            "node_labels": {
                "zone": "dc-a"
            }
        })
    ),
    responses(
        (
            status = 200,
            description = "Enrollment finalized; joining material for the mode the secret was minted for. `addrs` entries are tagged transport addresses, either `Ip` or `Relay`",
            body = BootstrapOnboardingResponseDoc,
            example = json!({
                "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "mode": "Server",
                "temporary_bootstrap_endpoint": {
                    "id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                    "addrs": [
                        {
                            "Ip": "192.0.2.10:4433"
                        },
                        {
                            "Relay": "https://relay.example.test/"
                        }
                    ]
                },
                "wrapped_realm_private_key": null,
                "wrapped_realm_private_key_nonce": null,
                "wrapping_public_key": null,
                "delegation_signature": "<realm-delegation-signature>",
                "onboarding_sync_ticket": "<one-time-onboarding-sync-ticket>",
                "realm_endpoints": [
                    {
                        "node_id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                        "endpoint_addr": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a;ip:192.0.2.10:4433"
                    }
                ]
            })
        ),
        (status = 400, description = "Malformed node id, key material or proof, or key material missing for the mode the secret was minted for", body = crate::error::ErrorResponse),
        (status = 401, description = "Unknown, expired, already claimed or non-matching enrollment secret, or a proof that does not verify", body = crate::error::ErrorResponse),
        (status = 403, description = "This node does not serve enrollment, or the secret was not minted for node enrollment", body = crate::error::ErrorResponse)
    )
)]
pub async fn bootstrap_onboarding(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<BootstrapOnboardingRequest>,
) -> ServerResult<(StatusCode, Json<BootstrapOnboardingResponse>)> {
    if !state.is_management_node() {
        return Err(ServerError::Forbidden);
    }

    let onboarding_secret = OnboardingSecret::decode(&request.onboarding_secret)
        .map_err(|_| ServerError::Unauthorized)?;
    let node_id = NodeId::from_str(&request.node_id).map_err(|_| ServerError::BadRequest)?;
    verify_node_proof(&request, node_id)?;

    let record = drive(
        InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: onboarding_secret.secret_hash(),
            node_id: request.node_id.clone(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_inspect_error)?;

    // Only node-enrollment secrets may bootstrap a node; an initial-admin
    // secret is rejected here before any reserve or consume.
    if record.purpose != OnboardingPurpose::NodeEnrollment {
        return Err(ServerError::Forbidden);
    }

    match record.mode {
        OnboardingMode::Server => {
            let issuer_public_key = request
                .issuer_public_key
                .as_deref()
                .ok_or(ServerError::BadRequest)?;
            verify_issuer_proof(&request, issuer_public_key)?;
        }
        // A device enrolls with no issuer key and no realm key.
        OnboardingMode::Management | OnboardingMode::User { .. } => {}
    }

    let bootstrap_endpoint = state
        .bootstrap_endpoint()
        .ok_or_else(|| ServerError::InternalError("net handle unavailable".to_string()))?;
    let wrapped_management_key = if matches!(record.mode, OnboardingMode::Management) {
        Some(wrap_realm_private_key(
            &state,
            request
                .transport_public_key
                .as_deref()
                .ok_or(ServerError::BadRequest)?,
        )?)
    } else {
        None
    };
    let delegation_signature = if matches!(record.mode, OnboardingMode::Server) {
        let issuer_public_key = request
            .issuer_public_key
            .as_deref()
            .ok_or(ServerError::BadRequest)?;
        Some(
            state
                .sign_server_delegation(issuer_public_key)
                .ok_or(ServerError::Forbidden)?,
        )
    } else {
        None
    };
    let realm_signing_key = match state.node_capabilities() {
        NodeCapabilities::Management {
            realm_signing_key, ..
        } => realm_signing_key.clone(),
        _ => return Err(ServerError::Forbidden),
    };

    let finalized = bootstrap_onboarding_finalize(
        BootstrapOnboardingFinalizeInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: onboarding_secret.secret_hash(),
            node_id,
            realm_id: state.get_realm_id(),
            local_node_id: state.get_node_id(),
            realm_signing_key,
            now: now_timestamp(),
            node_location: request.node_location.clone(),
            node_weight: request.node_weight,
            node_labels: request.node_labels.clone(),
        },
        state.get_ctx(),
    )
    .await
    .map_err(map_finalize_error)?;

    let realm_endpoints = realm_endpoints(&state, node_id).await?;

    let response = match finalized.mode {
        OnboardingMode::Management => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Management,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: wrapped_management_key.as_ref().map(|value| value.0.clone()),
            wrapped_realm_private_key_nonce: wrapped_management_key
                .as_ref()
                .map(|value| value.1.clone()),
            wrapping_public_key: wrapped_management_key.as_ref().map(|value| value.2.clone()),
            delegation_signature: None,
            onboarding_sync_ticket: finalized.onboarding_sync_ticket,
            realm_endpoints,
        },
        OnboardingMode::Server => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode: OnboardingMode::Server,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: None,
            wrapped_realm_private_key_nonce: None,
            wrapping_public_key: None,
            delegation_signature,
            onboarding_sync_ticket: finalized.onboarding_sync_ticket,
            realm_endpoints,
        },
        // A device receives no realm key and no issuer delegation.
        mode @ OnboardingMode::User { .. } => BootstrapOnboardingResponse {
            realm_id: state.get_realm_id().to_string(),
            mode,
            temporary_bootstrap_endpoint: bootstrap_endpoint,
            wrapped_realm_private_key: None,
            wrapped_realm_private_key_nonce: None,
            wrapping_public_key: None,
            delegation_signature: None,
            onboarding_sync_ticket: finalized.onboarding_sync_ticket,
            realm_endpoints,
        },
    };

    Ok((StatusCode::OK, Json(response)))
}

/// Realm endpoints a joiner may dial straight away: the discovery
/// configuration's declared ones, kept only for nodes that are configured,
/// sync-eligible members and not the joiner itself. The node serving this call
/// is handed over separately as the temporary bootstrap endpoint.
async fn realm_endpoints(
    state: &Arc<ServerState>,
    joiner: NodeId,
) -> ServerResult<Vec<StaticRealmEndpoint>> {
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(declared_endpoints(&config, &joiner.to_string()))
}

fn declared_endpoints(config: &RealmConfigDocument, joiner: &str) -> Vec<StaticRealmEndpoint> {
    let RealmDiscoveryConfig::Static { endpoints } = &config.discovery else {
        return Vec::new();
    };
    let sync_eligible = config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .map(|node| node.node_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();

    endpoints
        .iter()
        .filter(|endpoint| {
            endpoint.node_id != joiner && sync_eligible.contains(endpoint.node_id.as_str())
        })
        .cloned()
        .collect()
}

fn now_timestamp() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn map_consume_error(error: ConsumeOnboardingSecretError) -> ServerError {
    match error {
        ConsumeOnboardingSecretError::NotFound
        | ConsumeOnboardingSecretError::Expired
        | ConsumeOnboardingSecretError::AlreadyClaimed
        | ConsumeOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_finalize_error(error: BootstrapOnboardingFinalizeError) -> ServerError {
    match error {
        BootstrapOnboardingFinalizeError::Reserve(
            ReserveOnboardingSecretError::NotFound
            | ReserveOnboardingSecretError::Expired
            | ReserveOnboardingSecretError::AlreadyClaimed
            | ReserveOnboardingSecretError::InvalidSecret,
        ) => ServerError::Unauthorized,
        BootstrapOnboardingFinalizeError::Consume(error) => map_consume_error(error),
        BootstrapOnboardingFinalizeError::EnsureRealmConfig(
            EnsureRealmConfigError::NodeKindMismatch { .. },
        ) => ServerError::BadRequest,
        BootstrapOnboardingFinalizeError::EnsureRealmConfig(
            EnsureRealmConfigError::HandleSpaceExhausted,
        ) => ServerError::Conflict("realm handle space is exhausted".to_string()),
        BootstrapOnboardingFinalizeError::ReservedNodeLabel(label) => {
            ServerError::ReservedLabel(label)
        }
        BootstrapOnboardingFinalizeError::NodeLocationTooLong => ServerError::BadRequest,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_inspect_error(error: InspectOnboardingSecretError) -> ServerError {
    match error {
        InspectOnboardingSecretError::NotFound
        | InspectOnboardingSecretError::Expired
        | InspectOnboardingSecretError::AlreadyClaimed
        | InspectOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_create_error(error: CreateOnboardingSecretError) -> ServerError {
    match error {
        CreateOnboardingSecretError::DeviceCapExceeded { .. } => {
            ServerError::Conflict(error.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_delete_error(error: DeleteOnboardingSecretError) -> ServerError {
    match error {
        DeleteOnboardingSecretError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn verify_node_proof(request: &BootstrapOnboardingRequest, node_id: NodeId) -> ServerResult<()> {
    let signature =
        Signature::from_str(&request.node_proof).map_err(|_| ServerError::Unauthorized)?;
    let verifying_key =
        VerifyingKey::from_bytes(node_id.as_bytes()).map_err(|_| ServerError::BadRequest)?;
    verifying_key
        .verify(
            &bootstrap_node_proof_message(
                &request.onboarding_secret,
                &request.node_id,
                request.transport_public_key.as_deref(),
            ),
            &signature,
        )
        .map_err(|_| ServerError::Unauthorized)
}

fn verify_issuer_proof(
    request: &BootstrapOnboardingRequest,
    issuer_public_key: &str,
) -> ServerResult<()> {
    let issuer_proof = request
        .issuer_proof
        .as_ref()
        .ok_or(ServerError::BadRequest)?;
    let signature = Signature::from_str(issuer_proof).map_err(|_| ServerError::Unauthorized)?;
    let issuer_public_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(issuer_public_key)
        .map_err(|_| ServerError::BadRequest)?;
    let verifying_key = VerifyingKey::from_bytes(
        issuer_public_key_bytes
            .as_slice()
            .try_into()
            .map_err(|_| ServerError::BadRequest)?,
    )
    .map_err(|_| ServerError::BadRequest)?;
    verifying_key
        .verify(
            &bootstrap_issuer_proof_message(
                &request.onboarding_secret,
                &request.node_id,
                request
                    .issuer_public_key
                    .as_deref()
                    .ok_or(ServerError::BadRequest)?,
            ),
            &signature,
        )
        .map_err(|_| ServerError::Unauthorized)
}

fn wrap_realm_private_key(
    state: &Arc<ServerState>,
    transport_public_key: &str,
) -> ServerResult<(String, String, String)> {
    let realm_private_key_pem = state
        .realm_private_key_pem()
        .ok_or(ServerError::Forbidden)?;
    let transport_public_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(transport_public_key)
        .map_err(|_| ServerError::BadRequest)?;
    let transport_public_key = TransportPublicKey::from(
        <[u8; 32]>::try_from(transport_public_key_bytes.as_slice())
            .map_err(|_| ServerError::BadRequest)?,
    );
    let wrapping_secret_key = TransportSecretKey::generate(&mut CryptoOsRng);
    let wrapping_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(wrapping_secret_key.public_key().as_bytes());
    let cipher = SalsaBox::new(&transport_public_key, &wrapping_secret_key);
    let nonce = SalsaBox::generate_nonce(&mut CryptoOsRng);
    let ciphertext = cipher
        .encrypt(&nonce, realm_private_key_pem.as_bytes())
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ciphertext),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(nonce),
        wrapping_public_key,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        ServerError, bootstrap_onboarding, create_onboarding_secret, get_onboarding_secret_status,
        list_onboarding_secrets, map_finalize_error, revoke_onboarding_secret,
    };
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::AdminDocumentTarget;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::onboarding::{
        BootstrapOnboardingRequest, CreateOnboardingSecretRequest, OnboardingMode,
        OnboardingPurpose, OnboardingSecret, OnboardingSecretRecord, OnboardingSecretState,
        RequestedOnboardingMode, bootstrap_issuer_proof_message, bootstrap_node_proof_message,
    };
    use aruna_core::storage_entries::admin_document_reducer_state_key;
    use aruna_core::structs::{
        Actor, AuthContext, NodeCapabilities, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
        RealmNodeKind, StaticRealmEndpoint,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::bootstrap_onboarding_finalize::BootstrapOnboardingFinalizeError;
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::list_onboarding_secrets::ListOnboardingSecretsOperation;
    use aruna_operations::reserve_onboarding_secret::{
        ReserveOnboardingSecretInput, ReserveOnboardingSecretOperation,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::Extension;
    use axum::Json;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use base64::Engine;
    use crypto_box::{
        PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey, aead::Aead,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_management_state() -> (
        Arc<ServerState>,
        RealmId,
        iroh::PublicKey,
        UserId,
        NetHandle,
        TempDir,
    ) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });

        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = net_handle.node_id();

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: vec![],
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id,
                    user_id,
                    realm_id,
                },
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::management_node(realm_signing_key).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        (state, realm_id, node_id, user_id, net_handle, tempdir)
    }

    #[test]
    fn declares_dialable_members() {
        // Only configured, sync-eligible members other than the joiner.
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let server = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let device = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let stranger = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(server, RealmNodeKind::Server);
        config.ensure_node(
            device,
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        );
        let endpoint = |node: iroh::PublicKey| StaticRealmEndpoint {
            node_id: node.to_string(),
            endpoint_addr: format!("{node};ip:192.0.2.10:4433"),
        };
        config.discovery = RealmDiscoveryConfig::Static {
            endpoints: vec![endpoint(server), endpoint(device), endpoint(stranger)],
        };

        let declared = super::declared_endpoints(&config, &stranger.to_string());
        assert_eq!(declared, vec![endpoint(server)]);
        assert!(super::declared_endpoints(&config, &server.to_string()).is_empty());

        config.discovery = aruna_core::structs::default_realm_discovery_config();
        assert!(super::declared_endpoints(&config, &stranger.to_string()).is_empty());
    }

    #[test]
    fn placement_validation_errors_map_to_bad_request() {
        assert!(matches!(
            map_finalize_error(BootstrapOnboardingFinalizeError::ReservedNodeLabel(
                String::new()
            )),
            ServerError::ReservedLabel(_)
        ));
        assert!(matches!(
            map_finalize_error(BootstrapOnboardingFinalizeError::NodeLocationTooLong),
            ServerError::BadRequest
        ));
    }

    #[tokio::test]
    async fn create_and_consume_server_onboarding_secret() {
        let (state, realm_id, seed_node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::Server,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let issuer_key = generate_signing_key();
        let issuer_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_key.verifying_key().to_bytes());
        let onboarding_secret = created.onboarding_secret;
        let node_proof = SigningKey::from_bytes(&[9u8; 32]);
        let bootstrap_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
        let node_id = bootstrap_node_id.to_string();
        let node_signature = node_proof
            .sign(&bootstrap_node_proof_message(
                &onboarding_secret,
                &node_id,
                None,
            ))
            .to_string();
        let issuer_signature = issuer_key
            .sign(&bootstrap_issuer_proof_message(
                &onboarding_secret,
                &node_id,
                &issuer_public_key,
            ))
            .to_string();

        let (_, Json(bootstrap)) = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret,
                node_id,
                node_proof: node_signature,
                transport_public_key: None,
                issuer_public_key: Some(issuer_public_key.clone()),
                issuer_proof: Some(issuer_signature),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(bootstrap.mode, OnboardingMode::Server);
        assert_eq!(bootstrap.realm_id, realm_id.to_string());
        assert_eq!(bootstrap.temporary_bootstrap_endpoint.id, seed_node_id);
        assert!(bootstrap.wrapped_realm_private_key.is_none());
        assert!(bootstrap.delegation_signature.is_some());
        assert!(!bootstrap.onboarding_sync_ticket.is_empty());

        let config = match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: byteview::ByteView::from(*realm_id.as_bytes()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected realm config read result: {other:?}"),
        };
        assert!(config.has_node(bootstrap_node_id));

        let reducer_state = match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                key: admin_document_reducer_state_key(&AdminDocumentTarget::RealmConfig {
                    realm_id,
                }),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => postcard::from_bytes::<AdminDocumentReducerState>(&bytes).unwrap(),
            other => panic!("unexpected realm config reducer state read result: {other:?}"),
        };
        assert_eq!(
            reducer_state.materialized_realm_config_nodes()[&bootstrap_node_id],
            RealmNodeKind::Server
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn mint_binds_owner() {
        // A device secret takes its owner from the credential, not the body.
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::User,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        assert_eq!(created.mode, RequestedOnboardingMode::User);
        let secret = OnboardingSecret::decode(&created.onboarding_secret).unwrap();
        assert_eq!(secret.mode, OnboardingMode::User { owner: user_id });

        let (_, Json(listed)) = list_onboarding_secrets(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        let owner = user_id.to_string();
        assert_eq!(listed.secrets[0].mode, "User");
        assert_eq!(listed.secrets[0].owner.as_deref(), Some(owner.as_str()));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn builds_enroll_url() {
        // The deep link is a contract: aruna://enroll?secret=&seed=&realm=.
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        state
            .register_rest_interface_with_public_url(
                "0.0.0.0:3000".parse().unwrap(),
                Some("https://node.example.test"),
            )
            .await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: String::new(),
                mode: RequestedOnboardingMode::User,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let enroll =
            url::Url::parse(&created.enroll_url.expect("device mint carries a deep link")).unwrap();
        assert_eq!(enroll.scheme(), "aruna");
        assert_eq!(enroll.host_str(), Some("enroll"));
        let query = enroll
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(query["secret"], created.onboarding_secret);
        assert_eq!(query["seed"], "https://node.example.test");
        assert_eq!(query["realm"], realm_id.to_string());

        let secret = OnboardingSecret::decode(&created.onboarding_secret).unwrap();
        assert_eq!(
            secret.seed_url, query["seed"],
            "the link names the callback"
        );

        let (_, Json(server)) = create_onboarding_secret(
            State(state),
            Extension(Some(auth)),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::Server,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();
        assert!(server.enroll_url.is_none());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn polls_secret_status() {
        // The owner may watch its own secret; a stranger may not.
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::User,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();
        let secret = OnboardingSecret::decode(&created.onboarding_secret).unwrap();
        let enrollment_id = secret.enrollment_id.to_string();

        let (_, Json(status)) = get_onboarding_secret_status(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path(enrollment_id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(status.status, "pending");
        assert_eq!(status.mode, "User");
        assert_eq!(status.owner.as_deref(), Some(user_id.to_string().as_str()));
        assert!(status.claimed_node_id.is_none());

        let stranger = get_onboarding_secret_status(
            State(state.clone()),
            Extension(Some(AuthContext {
                user_id: UserId::local(Ulid::generate(), realm_id),
                realm_id,
                path_restrictions: None,
            })),
            Path(enrollment_id.clone()),
        )
        .await;
        assert!(matches!(stranger, Err(ServerError::Forbidden)));

        drive(
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
                enrollment_id: secret.enrollment_id,
                secret_hash: secret.secret_hash(),
                node_id: "device-a".to_string(),
                now: 1,
                reservation_expires_at: u64::MAX,
                finalizing: true,
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let (_, Json(status)) = get_onboarding_secret_status(
            State(state.clone()),
            Extension(Some(auth)),
            Path(enrollment_id),
        )
        .await
        .unwrap();
        assert_eq!(status.status, "claimed");
        assert_eq!(status.claimed_node_id.as_deref(), Some("device-a"));

        let missing = get_onboarding_secret_status(
            State(state),
            Extension(Some(AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            })),
            Path(Ulid::generate().to_string()),
        )
        .await;
        assert!(matches!(missing, Err(ServerError::NotFound)));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn enrolls_user_device() {
        // A device joins with no issuer key and lands as an owner-bound member.
        let (state, realm_id, _seed, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::User,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let device_key = SigningKey::from_bytes(&[23u8; 32]);
        let device_node_id = iroh::SecretKey::from_bytes(&device_key.to_bytes()).public();
        let node_id = device_node_id.to_string();
        let node_proof = device_key
            .sign(&bootstrap_node_proof_message(
                &created.onboarding_secret,
                &node_id,
                None,
            ))
            .to_string();

        let (_, Json(bootstrap)) = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: created.onboarding_secret,
                node_id,
                node_proof,
                transport_public_key: None,
                issuer_public_key: None,
                issuer_proof: None,
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(bootstrap.mode, OnboardingMode::User { owner: user_id });
        assert!(bootstrap.wrapped_realm_private_key.is_none());
        assert!(bootstrap.delegation_signature.is_none());

        let config = match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: byteview::ByteView::from(*realm_id.as_bytes()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected realm config read result: {other:?}"),
        };
        let device = config
            .nodes
            .iter()
            .find(|node| node.node_id == device_node_id.to_string())
            .expect("device joined the realm configuration");
        assert_eq!(device.kind, RealmNodeKind::User { owner: user_id });
        assert!(!device.kind.is_sync_eligible());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn mint_rejects_stranger() {
        // Self-service enrollment is still realm-scoped and never anonymous.
        let (state, _realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let request = || CreateOnboardingSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::User,
            expires_in_seconds: Some(600),
        };

        let anonymous =
            create_onboarding_secret(State(state.clone()), Extension(None), Json(request())).await;
        assert!(matches!(anonymous, Err(ServerError::Unauthorized)));

        let foreign = create_onboarding_secret(
            State(state),
            Extension(Some(AuthContext {
                user_id,
                realm_id: RealmId::from_bytes([31u8; 32]),
                path_restrictions: None,
            })),
            Json(request()),
        )
        .await;
        assert!(matches!(foreign, Err(ServerError::Forbidden)));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_rejects_secret() {
        // An initial-administrator secret must not onboard a node or be consumed.
        let (state, realm_id, _seed, _user_id, net_handle, _tempdir) =
            setup_management_state().await;

        let enrollment_id = Ulid::generate();
        let secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id,
            secret: [11u8; 32],
            mode: OnboardingMode::Server,
            realm_id,
            purpose: OnboardingPurpose::InitialAdministrator,
        };
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id,
                    secret_hash: secret.secret_hash(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::InitialAdministrator,
                    expires_at: u64::MAX,
                    claimed_node_id: None,
                },
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let encoded = secret.encode().unwrap();
        let node_proof = SigningKey::from_bytes(&[9u8; 32]);
        let bootstrap_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
        let node_id = bootstrap_node_id.to_string();
        let node_signature = node_proof
            .sign(&bootstrap_node_proof_message(&encoded, &node_id, None))
            .to_string();

        let result = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: encoded,
                node_id,
                node_proof: node_signature,
                transport_public_key: None,
                issuer_public_key: None,
                issuer_proof: None,
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));

        let entries = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
            .await
            .unwrap();
        let entry = entries
            .iter()
            .find(|entry| entry.record.enrollment_id == enrollment_id)
            .expect("secret still present");
        assert!(matches!(entry.state, OnboardingSecretState::Available));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn list_and_revoke_onboarding_secrets() {
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::Server,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let (_, Json(listed)) =
            list_onboarding_secrets(State(state.clone()), Extension(Some(auth.clone())))
                .await
                .unwrap();
        assert_eq!(listed.secrets.len(), 1);

        let secret =
            aruna_core::onboarding::OnboardingSecret::decode(&created.onboarding_secret).unwrap();
        let status = revoke_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Path(secret.enrollment_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (_, Json(listed)) = list_onboarding_secrets(
            State(state),
            Extension(Some(AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            })),
        )
        .await
        .unwrap();
        assert!(listed.secrets.is_empty());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn list_prunes_expired_available_but_keeps_expired_finalizing_secret() {
        let (state, realm_id, _node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let finalizing_id = Ulid::generate();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: finalizing_id,
                    secret_hash: "finalizing".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 1,
                    claimed_node_id: None,
                },
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        drive(
            ReserveOnboardingSecretOperation::new(ReserveOnboardingSecretInput {
                enrollment_id: finalizing_id,
                secret_hash: "finalizing".to_string(),
                node_id: "node-a".to_string(),
                now: 1,
                reservation_expires_at: 2,
                finalizing: true,
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let stale_id = Ulid::generate();
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: stale_id,
                    secret_hash: "stale".to_string(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: 1,
                    claimed_node_id: None,
                },
            }),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        let (_, Json(listed)) =
            list_onboarding_secrets(State(state.clone()), Extension(Some(auth)))
                .await
                .unwrap();
        assert_eq!(listed.secrets.len(), 1);
        assert_eq!(listed.secrets[0].enrollment_id, finalizing_id.to_string());
        assert_eq!(listed.secrets[0].claimed_node_id.as_deref(), Some("node-a"));

        let entries = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].record.enrollment_id, finalizing_id);

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_issuer_proof_does_not_consume_secret() {
        let (state, realm_id, _seed_node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::Server,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let node_proof = SigningKey::from_bytes(&[5u8; 32]);
        let joiner_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
        let joiner_node_id_string = joiner_node_id.to_string();
        let node_signature = node_proof
            .sign(&bootstrap_node_proof_message(
                &created.onboarding_secret,
                &joiner_node_id_string,
                None,
            ))
            .to_string();

        let issuer_key = generate_signing_key();
        let issuer_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_key.verifying_key().to_bytes());
        let onboarding_secret = created.onboarding_secret;

        let result = bootstrap_onboarding(
            State(state.clone()),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: onboarding_secret.clone(),
                node_id: joiner_node_id_string.clone(),
                node_proof: node_signature.clone(),
                transport_public_key: None,
                issuer_public_key: Some(issuer_public_key.clone()),
                issuer_proof: Some("invalid-signature".to_string()),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Unauthorized)));

        let (_, Json(listed)) =
            list_onboarding_secrets(State(state.clone()), Extension(Some(auth)))
                .await
                .unwrap();
        assert_eq!(listed.secrets.len(), 1);

        let issuer_signature = issuer_key
            .sign(&bootstrap_issuer_proof_message(
                &onboarding_secret,
                &joiner_node_id_string,
                &issuer_public_key,
            ))
            .to_string();
        let result = bootstrap_onboarding(
            State(state),
            Json(BootstrapOnboardingRequest {
                onboarding_secret,
                node_id: joiner_node_id_string,
                node_proof: node_signature,
                transport_public_key: None,
                issuer_public_key: Some(issuer_public_key),
                issuer_proof: Some(issuer_signature),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await;
        assert!(result.is_ok());

        net_handle.shutdown().await;
    }

    #[tokio::test]
    async fn management_bootstrap_wraps_realm_key() {
        let (state, realm_id, _seed_node_id, user_id, net_handle, _tempdir) =
            setup_management_state().await;
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        let (_, Json(created)) = create_onboarding_secret(
            State(state.clone()),
            Extension(Some(auth)),
            Json(CreateOnboardingSecretRequest {
                seed_url: "http://127.0.0.1:3000".to_string(),
                mode: RequestedOnboardingMode::Management,
                expires_in_seconds: Some(600),
            }),
        )
        .await
        .unwrap();

        let joiner_node_key = SigningKey::from_bytes(&[11u8; 32]);
        let joiner_node_id = iroh::SecretKey::from_bytes(&joiner_node_key.to_bytes()).public();
        let joiner_node_id_string = joiner_node_id.to_string();
        let transport_secret_key = TransportSecretKey::generate(&mut crypto_box::aead::OsRng);
        let transport_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(transport_secret_key.public_key().as_bytes());
        let node_signature = joiner_node_key
            .sign(&bootstrap_node_proof_message(
                &created.onboarding_secret,
                &joiner_node_id_string,
                Some(&transport_public_key),
            ))
            .to_string();

        let (_, Json(bootstrap)) = bootstrap_onboarding(
            State(state),
            Json(BootstrapOnboardingRequest {
                onboarding_secret: created.onboarding_secret,
                node_id: joiner_node_id_string,
                node_proof: node_signature,
                transport_public_key: Some(transport_public_key),
                issuer_public_key: None,
                issuer_proof: None,
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
        )
        .await
        .unwrap();

        let sender_public_key = TransportPublicKey::from(
            <[u8; 32]>::try_from(
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(bootstrap.wrapping_public_key.unwrap())
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        );
        let cipher = SalsaBox::new(&sender_public_key, &transport_secret_key);
        let nonce_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(bootstrap.wrapped_realm_private_key_nonce.unwrap())
            .unwrap();
        let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(bootstrap.wrapped_realm_private_key.unwrap())
            .unwrap();
        let nonce = crypto_box::Nonce::from(<[u8; 24]>::try_from(nonce_bytes.as_slice()).unwrap());
        let plaintext = cipher.decrypt(&nonce, ciphertext.as_ref()).unwrap();
        let pem = String::from_utf8(plaintext).unwrap();
        assert!(pem.contains("BEGIN PRIVATE KEY"));

        net_handle.shutdown().await;
    }
}
