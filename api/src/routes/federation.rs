//! Serves the realm federation settings and the signed realm descriptor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ServerError, ServerResult};
use crate::routes::git::expected;
use crate::server::state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::federation::{AcceptedRealms, FederationSettings, RegistrationMode};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::realm::get_config::{GetConfigError, GetConfigOperation};
use aruna_operations::realm::set_federation::{
    SetFederationConfig, SetFederationError, SetFederationOperation,
};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use url::Url;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(tags((name = "system/realm", description = "Realm configuration, placement and quota")))]
pub struct FederationApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(FederationApiDoc::openapi())
        .routes(routes!(get_realm_federation, set_realm_federation))
        .routes(routes!(get_realm_descriptor))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationSetting {
    #[default]
    Enabled,
    Disabled,
}

/// Who may log in here as a foreign user.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AcceptedRealmsSetting {
    #[default]
    None,
    Only {
        realms: Vec<String>,
    },
    Any,
}

/// Realm federation settings. The request replaces the stored settings; the
/// response adds the descriptor signed from them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RealmFederation {
    pub name: String,
    pub api_url: String,
    pub portal_url: String,
    /// No URL means no registry traffic.
    #[serde(default)]
    pub registry_url: Option<String>,
    /// Acts only when a registry URL is set.
    #[serde(default)]
    pub registration: RegistrationSetting,
    #[serde(default)]
    pub accepted_realms: AcceptedRealmsSetting,
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object, read_only)]
    pub descriptor: Option<Value>,
}

impl RealmFederation {
    fn from_settings(settings: &FederationSettings) -> ServerResult<Self> {
        Ok(Self {
            name: settings.name.clone(),
            api_url: settings.api_url.to_string(),
            portal_url: settings.portal_url.to_string(),
            registry_url: settings.registry_url.as_ref().map(Url::to_string),
            registration: match settings.registration {
                RegistrationMode::Enabled => RegistrationSetting::Enabled,
                RegistrationMode::Disabled => RegistrationSetting::Disabled,
            },
            accepted_realms: match &settings.accepted_realms {
                AcceptedRealms::None => AcceptedRealmsSetting::None,
                AcceptedRealms::Any => AcceptedRealmsSetting::Any,
                AcceptedRealms::Only(realms) => AcceptedRealmsSetting::Only {
                    realms: realms.iter().map(RealmId::to_string).collect(),
                },
            },
            descriptor: Some(
                serde_json::to_value(&settings.descriptor)
                    .map_err(|error| ServerError::InternalError(error.to_string()))?,
            ),
        })
    }
}

fn parse_url(value: &str) -> ServerResult<Url> {
    Url::parse(value).map_err(|error| ServerError::BadRequestReason(format!("{value}: {error}")))
}

fn config_missing() -> ServerError {
    ServerError::Refused(
        StatusCode::SERVICE_UNAVAILABLE,
        "realm_config_missing",
        "this node holds no configuration document for its realm".to_string(),
    )
}

/// Policy checks need the realm config, so its absence is read before they can turn it into 403.
async fn read_config(state: &ServerState) -> ServerResult<RealmConfigDocument> {
    drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        GetConfigError::DocumentNotFound => config_missing(),
        error => ServerError::InternalError(error.to_string()),
    })
}

/// The settings with their digest as a quoted `ETag`, the version a later change names.
fn settings_response(
    settings: &FederationSettings,
) -> ServerResult<(HeaderMap, Json<RealmFederation>)> {
    let digest = settings
        .digest()
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let etag = HeaderValue::from_str(&format!("\"{digest}\""))
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok((
        HeaderMap::from_iter([(header::ETAG, etag)]),
        Json(RealmFederation::from_settings(settings)?),
    ))
}

/// The settings digest a change expects: `If-Match` names one, `If-None-Match: *` none.
fn expected_settings(headers: &HeaderMap) -> ServerResult<Option<String>> {
    if let Some(digest) = expected(headers) {
        return Ok(Some(digest));
    }
    match headers.get(header::IF_NONE_MATCH) {
        Some(value) if value.as_bytes().trim_ascii() == b"*" => Ok(None),
        _ => Err(ServerError::Refused(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            "send If-Match with the settings ETag, or If-None-Match: * for a first setup"
                .to_string(),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/system/realm/federation",
    tag = "system/realm",
    summary = "Get the realm federation settings",
    description = r#"Returns the stored realm-wide federation settings with the descriptor signed from them.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path.

**Behavior**
- The body has the form the PUT route takes and returns. Load it before a change, so a save
  keeps the settings the change does not touch.
- The `ETag` header holds the quoted digest of the stored settings. A change sends it back as
  `If-Match`.
- A realm without federation settings answers 404 with code `federation_unset`; the first setup
  then sends `If-None-Match: *`."#,
    responses(
        (status = 200, description = "The stored settings with the signed descriptor", body = RealmFederation,
            headers(("ETag" = String, description = "Quoted digest of the stored settings")),
            example = json!({
                "name": "Example realm",
                "api_url": "https://api.example.org/api/v1",
                "portal_url": "https://portal.example.org/",
                "registry_url": "https://registry.example.org/",
                "registration": "enabled",
                "accepted_realms": {"mode": "none"},
                "descriptor": {
                    "payload": {
                        "realm_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
                        "name": "Example realm",
                        "description": "",
                        "api_url": "https://api.example.org/api/v1",
                        "portal_url": "https://portal.example.org/",
                        "issued_at": 1791000000
                    },
                    "signer": "Realm",
                    "signature": "<hex ed25519 signature>"
                }
            })),
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "The realm has no federation settings; code `federation_unset`", body = crate::error::ErrorResponse),
        (status = 503, description = "This node holds no configuration document for its realm; code `realm_config_missing`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_federation(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(HeaderMap, Json<RealmFederation>)> {
    let auth = require_realm_auth(&state, auth)?;
    let config = read_config(&state).await?;
    ensure_permission(
        &state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let settings = config.federation.ok_or_else(|| {
        ServerError::Refused(
            StatusCode::NOT_FOUND,
            "federation_unset",
            "the realm has no federation settings".to_string(),
        )
    })?;
    settings_response(&settings)
}

#[utoipa::path(
    put,
    path = "/system/realm/federation",
    tag = "system/realm",
    summary = "Replace the realm federation settings",
    description = r#"Replaces the realm-wide federation settings and signs a new realm descriptor from them.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and signs the descriptor; every other node relays it to one.

**Behavior**
- `name`, `api_url` and `portal_url` are the realm's public entry points and form the signed
  descriptor, together with the realm description. Each change issues a newer descriptor.
- `registry_url` is optional; without it the realm sends nothing to a registry.
- `registration` acts only when a registry URL is set.
- `accepted_realms` defaults to `none`. `any` must be chosen explicitly.
- The call needs one precondition. `If-Match` names the `ETag` of the settings the change is
  based on. `If-None-Match: *` is for the first setup, when the realm has no settings yet.
- The precondition is checked in the same transaction as the write. Settings changed since they
  were read, or already set up, answer 412 with code `settings_changed`; reload and apply the
  change again. A call without a precondition answers 428.
- The response carries the `ETag` of the new settings.

**Limits**
- URLs must use HTTPS; plain HTTP is accepted only for loopback hosts.
- `name` must not be empty and holds at most 128 characters; `only` lists at most 1024 realms."#,
    request_body(
        content = RealmFederation,
        description = "The complete federation settings to store",
        example = json!({
            "name": "Example realm",
            "api_url": "https://api.example.org/api/v1",
            "portal_url": "https://portal.example.org/",
            "registry_url": "https://registry.example.org/",
            "registration": "enabled",
            "accepted_realms": {"mode": "only", "realms": ["AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"]}
        })
    ),
    params(
        ("If-Match" = Option<String>, Header, description = "Quoted `ETag` of the settings the change is based on"),
        ("If-None-Match" = Option<String>, Header, description = "`*` for the first setup, when the realm has no settings yet")
    ),
    responses(
        (status = 200, description = "The stored settings with the signed descriptor", body = RealmFederation,
            headers(("ETag" = String, description = "Quoted digest of the new settings")),
            example = json!({
                "name": "Example realm",
                "api_url": "https://api.example.org/api/v1",
                "portal_url": "https://portal.example.org/",
                "registry_url": "https://registry.example.org/",
                "registration": "enabled",
                "accepted_realms": {"mode": "any"},
                "descriptor": {
                    "payload": {
                        "realm_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
                        "name": "Example realm",
                        "description": "",
                        "api_url": "https://api.example.org/api/v1",
                        "portal_url": "https://portal.example.org/",
                        "issued_at": 1791000000
                    },
                    "signer": "Realm",
                    "signature": "<hex ed25519 signature>"
                }
            })),
        (status = 400, description = "A malformed or non-HTTPS URL, an empty or too long name, a malformed realm id or too many accepted realms", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = crate::error::ErrorResponse),
        (status = 412, description = "The settings changed since they were read, or are already set up; code `settings_changed`", body = crate::error::ErrorResponse),
        (status = 428, description = "Neither `If-Match` nor `If-None-Match: *` was sent", body = crate::error::ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = crate::error::ErrorResponse),
        (status = 503, description = "This node holds no configuration document for its realm (code `realm_config_missing`), storage cleanup capacity is exhausted, or no management node was reachable (code `no_management_node`)", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_federation(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Json(request): Json<RealmFederation>,
) -> ServerResult<(HeaderMap, Json<RealmFederation>)> {
    let auth = require_realm_auth(&state, auth)?;
    read_config(&state).await?;
    ensure_permission(
        &state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let expected = expected_settings(&headers)?;
    let accepted_realms = match request.accepted_realms {
        AcceptedRealmsSetting::None => AcceptedRealms::None,
        AcceptedRealmsSetting::Any => AcceptedRealms::Any,
        AcceptedRealmsSetting::Only { realms } => AcceptedRealms::Only(
            realms
                .iter()
                .map(|realm| {
                    RealmId::from_base64(realm)
                        .map_err(|_| ServerError::BadRequestReason(format!("{realm}: realm id")))
                })
                .collect::<ServerResult<_>>()?,
        ),
    };
    let config = SetFederationConfig {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        auth_context: auth,
        node_capabilities: state.node_capabilities().clone(),
        name: request.name,
        api_url: parse_url(&request.api_url)?,
        portal_url: parse_url(&request.portal_url)?,
        registry_url: request.registry_url.as_deref().map(parse_url).transpose()?,
        registration: match request.registration {
            RegistrationSetting::Enabled => RegistrationMode::Enabled,
            RegistrationSetting::Disabled => RegistrationMode::Disabled,
        },
        accepted_realms,
        expected,
        now: unix_timestamp_secs(),
    };
    let stored = drive(SetFederationOperation::new(config), &state.get_ctx())
        .await
        .map_err(map_federation_error)?;
    let settings = stored
        .federation
        .as_ref()
        .ok_or_else(|| ServerError::InternalError("federation settings missing".to_string()))?;
    settings_response(settings)
}

fn map_federation_error(error: SetFederationError) -> ServerError {
    match error {
        SetFederationError::ConfigMissing => config_missing(),
        SetFederationError::SettingsChanged => ServerError::Refused(
            StatusCode::PRECONDITION_FAILED,
            "settings_changed",
            "the federation settings changed since they were read; reload them".to_string(),
        ),
        SetFederationError::Unauthorized | SetFederationError::NotManagementNode => {
            ServerError::Forbidden
        }
        SetFederationError::InvalidSettings { reason } => ServerError::BadRequestReason(reason),
        SetFederationError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent realm federation update conflict; retry".to_string())
        }
        SetFederationError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/system/realm/descriptor",
    tag = "system/realm",
    summary = "Get the signed realm descriptor",
    description = r#"Returns the realm descriptor signed by the realm key or a delegated issuer key.

**Authentication**: none. Any origin may read it (`Access-Control-Allow-Origin: *`).

**Behavior**
- The same descriptor is served by the portal at `/.well-known/aruna-realm`.
- The signature covers the domain-tagged postcard encoding of `payload` and verifies against
  the realm id, which is the realm's ed25519 public key.
- A newer `issued_at` supersedes older descriptors."#,
    responses(
        (status = 200, description = "The signed realm descriptor", body = Value,
            example = json!({
                "payload": {
                    "realm_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
                    "name": "Example realm",
                    "description": "",
                    "api_url": "https://api.example.org/api/v1",
                    "portal_url": "https://portal.example.org/",
                    "issued_at": 1791000000
                },
                "signer": "Realm",
                "signature": "<hex ed25519 signature>"
            })),
        (status = 404, description = "The realm has no federation settings")
    )
)]
pub async fn get_realm_descriptor(State(state): State<Arc<ServerState>>) -> Response {
    descriptor_response(&state).await
}

/// The stored signed descriptor, readable from any origin.
pub(crate) async fn descriptor_response(state: &ServerState) -> Response {
    let config = match drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(config) => config,
        Err(GetConfigError::DocumentNotFound) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return ServerError::InternalError(error.to_string()).into_response(),
    };
    let Some(settings) = config.federation else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = Json(settings.descriptor).into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}
