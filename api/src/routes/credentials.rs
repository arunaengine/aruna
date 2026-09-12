use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{
    AuthContext, PathRestriction, Permission, UserAccess, group_permission_path,
};
use aruna_operations::driver::drive;
use aruna_operations::s3::create_access::{
    CreateUserAccessConfig, CreateUserAccessError, CreateUserAccessOperation,
    DEFAULT_CREDENTIAL_TTL,
};
use aruna_operations::s3::get_access::{GetUserAccessError, GetUserAccessOperation};
use aruna_operations::s3::list_access::{ListUserAccessInput, ListUserAccessOperation};
use aruna_operations::s3::revoke_access::{RevokeUserAccessError, RevokeUserAccessOperation};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use std::{str::FromStr, sync::Arc};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

mod sessions;

#[derive(OpenApi)]
#[openapi(
    tags((name = "access/credentials", description = "User credential management"))
)]
pub struct CredentialsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(CredentialsApiDoc::openapi())
        .routes(routes!(list_s3_credentials, create_s3_credentials))
        .routes(routes!(revoke_s3_credentials))
        .merge(sessions::router())
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3PathRestriction {
    pub pattern: String,
    pub permission: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsRequest {
    pub group_id: String,
    #[schema(default = 31536000)]
    pub expires_in_seconds: Option<u64>,
    pub path_restrictions: Option<Vec<CreateS3PathRestriction>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsResponse {
    pub access_key_id: String,
    pub access_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct S3PathRestrictionResponse {
    pub pattern: String,
    pub permission: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatusResponse {
    Active,
    Expired,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct S3CredentialSummaryResponse {
    pub access_key_id: String,
    pub group_id: String,
    pub expires_at: String,
    pub revoked_at: Option<String>,
    pub issued_by: String,
    pub path_restrictions: Vec<S3PathRestrictionResponse>,
    pub status: CredentialStatusResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListS3CredentialsResponse {
    pub credentials: Vec<S3CredentialSummaryResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DelegationScope {
    root: String,
    recursive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedRestriction {
    scope: DelegationScope,
    permission: Permission,
}

impl DelegationScope {
    fn exact(root: String) -> Self {
        Self {
            root,
            recursive: false,
        }
    }

    fn descendants(root: String) -> Self {
        Self {
            root,
            recursive: true,
        }
    }

    fn parse_supported(pattern: &str) -> Option<Self> {
        if !pattern.starts_with('/')
            || pattern
                .chars()
                .any(|ch| matches!(ch, '?' | '[' | ']' | '{' | '}'))
        {
            return None;
        }

        if let Some(root) = pattern.strip_suffix("/**") {
            if root.is_empty() || root.contains('*') {
                return None;
            }
            return Some(Self::descendants(root.to_string()));
        }

        if pattern.contains('*') {
            return None;
        }

        Some(Self::exact(pattern.to_string()))
    }

    fn is_within(&self, root: &str) -> bool {
        path_within(&self.root, root)
    }

    fn intersect_group_root(&self, group_root: &str) -> Option<Self> {
        if path_within(&self.root, group_root) {
            return Some(self.clone());
        }

        if self.recursive && path_within(group_root, &self.root) {
            Some(Self::descendants(group_root.to_string()))
        } else {
            None
        }
    }

    fn authorization_probe_path(&self) -> String {
        if !self.recursive {
            return self.root.clone();
        }

        if self.root == "/" {
            "/.aruna-delegation-probe".to_string()
        } else {
            format!("{}/.aruna-delegation-probe", self.root)
        }
    }

    fn to_pattern(&self) -> String {
        if self.recursive {
            format!("{}/**", self.root)
        } else {
            self.root.clone()
        }
    }
}

impl NormalizedRestriction {
    fn to_path_restriction(&self) -> PathRestriction {
        PathRestriction {
            pattern: self.scope.to_pattern(),
            permission: self.permission.clone(),
        }
    }
}

fn parse_normalized_restriction(
    pattern: &str,
    permission: Permission,
) -> Option<NormalizedRestriction> {
    DelegationScope::parse_supported(pattern)
        .map(|scope| NormalizedRestriction { scope, permission })
}

fn serialize_restrictions(restrictions: &[NormalizedRestriction]) -> Vec<PathRestriction> {
    restrictions
        .iter()
        .map(NormalizedRestriction::to_path_restriction)
        .collect()
}

#[utoipa::path(
    get,
    path = "/access/credentials",
    tag = "access/credentials",
    summary = "List the caller's S3 credentials",
    description = r#"Lists the caller's own S3 credentials held by the node serving the request.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- The response only ever contains credentials issued to the calling user, and never a secret access
  key.
- Only credentials held by the serving node are listed, so a credential issued on another node of
  the realm does not appear here.
- `status` is derived from the timestamps as `active`, `expired` or `revoked`.

**Limits**
- The listing is not paginated and covers at most 16 active credentials per user, ordered by access
  key id."#,
    responses(
        (
            status = 200,
            description = "The caller's credentials held by this node, with every secret access key omitted",
            body = ListS3CredentialsResponse,
            example = json!({
                "credentials": [
                    {
                        "access_key_id": "01JAKEY0123456789ABCDEFGHJ",
                        "group_id": "01JGRP00123456789ABCDEFGHJ",
                        "expires_at": "2027-04-09T14:23:11Z",
                        "revoked_at": null,
                        "issued_by": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "path_restrictions": [
                            {
                                "pattern": "/YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA/g/01JGRP00123456789ABCDEFGHJ/data/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/shared/**",
                                "permission": "Read"
                            }
                        ],
                        "status": "active"
                    }
                ]
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or is path-restricted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListS3CredentialsResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;

    let credentials = drive(
        ListUserAccessOperation::new(ListUserAccessInput {
            user_identity: auth.user_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;

    Ok((
        StatusCode::OK,
        Json(ListS3CredentialsResponse {
            credentials: credentials.into_iter().map(map_redacted_access).collect(),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/access/credentials",
    tag = "access/credentials",
    summary = "Create an S3 credential for a group",
    description = r#"Issues an S3 access key and a one-time secret bound to a group, for the calling user.

**Authentication**: realm bearer token with READ or WRITE on the group's data path, or on a part of
it. A path-restricted token may be used: the credential inherits the caller's restrictions narrowed
to the group data root and can never widen them.

**Behavior**
- The credential is always issued to the calling user, so no caller can mint one for somebody else.
- The secret access key is returned in this response only: it is stored encrypted, later listings show
  only the access key id, and a lost secret means creating a new credential.
- The credential is stored on the node that served the request and is accepted by that node's S3
  endpoint.

**Limits**
- The optional lifetime is given in seconds between 60 and 31536000 and defaults to 31536000.
- A restriction pattern is relative to the group data root or an absolute path inside it, may name
  an exact path or a subtree with a trailing `/**`, and takes `READ`, `WRITE` or `DENY` case
  insensitively; at most 50 restrictions are accepted.
- A user holds at most 16 active credentials."#,
    request_body(
        content = CreateS3CredentialsRequest,
        description = "Group the credential is bound to, an optional lifetime in seconds, and optional path restrictions",
        example = json!({
            "group_id": "01JGRP00123456789ABCDEFGHJ",
            "expires_in_seconds": 86400,
            "path_restrictions": [
                {
                    "pattern": "shared/**",
                    "permission": "READ"
                }
            ]
        })
    ),
    responses(
        (
            status = 201,
            description = "Credential created; `access_secret` is the plaintext secret access key and is shown only here",
            body = CreateS3CredentialsResponse,
            example = json!({
                "access_key_id": "01JAKEY0123456789ABCDEFGHJ",
                "access_secret": "<one-time-secret-shown-only-in-this-response>"
            })
        ),
        (status = 400, description = "The group id is not a ULID, the lifetime is out of range, or a restriction is malformed or exceeds the count limit", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, the caller may read no part of the group data path, or a restriction reaches outside the group root or the caller's own grant", body = ErrorResponse),
        (status = 409, description = "The caller already holds 16 active credentials; revoke or let one expire first", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateS3CredentialsRequest>,
) -> ServerResult<(StatusCode, Json<CreateS3CredentialsResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();

    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let user_identity = auth.user_id;
    let group_id = Ulid::from_str(&request.group_id).map_err(|_| ServerError::BadRequest)?;
    if request
        .path_restrictions
        .as_ref()
        .is_some_and(|restrictions| {
            restrictions.len() > aruna_core::permission_path::MAX_TOKEN_RESTRICTIONS
        })
    {
        return Err(ServerError::BadRequest);
    }
    let path_restrictions =
        build_credential_restrictions(&auth, &state, group_id, request.path_restrictions.clone())
            .await?;
    authorize_credential_issuance(&auth, &state, group_id, path_restrictions.as_deref()).await?;
    let path_restrictions = path_restrictions.as_deref().map(serialize_restrictions);
    if let Some(restrictions) = path_restrictions.as_deref()
        && aruna_core::permission_path::validate_restriction_limits(restrictions).is_err()
    {
        return Err(ServerError::BadRequest);
    }
    let expiry = credential_expiry(SystemTime::now(), request.expires_in_seconds)?;
    let result = drive(
        CreateUserAccessOperation::new(
            CreateUserAccessConfig {
                user_identity,
                group_id,
                expiry,
                path_restrictions,
                issued_by: *node_id.as_bytes(),
            },
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    match result {
        Ok((access_key_id, access_secret, _)) => Ok((
            StatusCode::CREATED,
            Json(CreateS3CredentialsResponse {
                access_key_id,
                access_secret: access_secret.expose().to_string(),
            }),
        )),
        Err(CreateUserAccessError::LimitReached) => Err(ServerError::Conflict(
            "active credential limit reached".to_string(),
        )),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

#[utoipa::path(
    delete,
    path = "/access/credentials/{access_key_id}",
    tag = "access/credentials",
    summary = "Revoke an S3 credential",
    description = r#"Revokes an S3 credential held by the node serving the request.

**Authentication**: realm bearer token without path restrictions. Revoking one's own credential is
self-service; revoking another user's needs WRITE on that user's realm administration path, so
WRITE on the group the credential is bound to is deliberately not enough.

**Behavior**
- Only credentials held by the node that serves the request can be revoked here.
- The record is not deleted: it keeps appearing in the owner's listing with a revocation timestamp
  and the `revoked` status, and the node stops accepting the key for new S3 requests."#,
    params(("access_key_id" = String, Path, description = "Access key id of the credential to revoke, as returned when it was created or listed")),
    responses(
        (status = 204, description = "Credential revoked"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, is path-restricted, or lacks WRITE on the owning user's administration path", body = ErrorResponse),
        (status = 404, description = "This node holds no credential with that access key id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;

    let credential = match drive(
        GetUserAccessOperation::new(access_key_id.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(credential))) => credential,
        Ok(None)
        | Ok(Some(Err(GetUserAccessError::NotFound)))
        | Err(GetUserAccessError::NotFound) => return Err(ServerError::NotFound),
        Ok(Some(Err(err))) | Err(err) => return Err(ServerError::InternalError(err.to_string())),
    };

    // Group write cannot revoke another member's credential without user administration.
    if credential.user_identity != auth.user_id {
        crate::auth::ensure_permission(
            &state,
            &auth,
            format!(
                "/{}/admin/u/{}",
                state.get_realm_id(),
                credential.user_identity
            ),
            Permission::WRITE,
        )
        .await?;
    }

    match drive(
        RevokeUserAccessOperation::new(access_key_id),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(_))) => Ok(StatusCode::NO_CONTENT),
        Ok(None)
        | Ok(Some(Err(RevokeUserAccessError::NotFound)))
        | Err(RevokeUserAccessError::NotFound) => Err(ServerError::NotFound),
        Ok(Some(Err(err))) | Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

fn map_redacted_access(access: UserAccess) -> S3CredentialSummaryResponse {
    let now = SystemTime::now();
    let status = credential_status(&access, now);
    let expires_at = format_system_time(access.expiry);
    let revoked_at = access.revoked_at.map(format_system_time);
    S3CredentialSummaryResponse {
        access_key_id: access.access_key,
        group_id: access.group_id.to_string(),
        expires_at,
        revoked_at,
        issued_by: format_node_id(access.issued_by),
        path_restrictions: access
            .path_restrictions
            .unwrap_or_default()
            .into_iter()
            .map(|restriction| S3PathRestrictionResponse {
                pattern: restriction.pattern,
                permission: restriction.permission.to_string(),
            })
            .collect(),
        status,
    }
}

fn credential_status(access: &UserAccess, now: SystemTime) -> CredentialStatusResponse {
    if access.is_revoked() {
        CredentialStatusResponse::Revoked
    } else if access.is_expired(now) {
        CredentialStatusResponse::Expired
    } else {
        CredentialStatusResponse::Active
    }
}

fn format_system_time(value: SystemTime) -> String {
    DateTime::<Utc>::from(value).to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn format_node_id(bytes: [u8; 32]) -> String {
    iroh::PublicKey::from_bytes(&bytes)
        .map(|node_id| node_id.to_string())
        .unwrap_or_else(|_| bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn credential_expiry(now: SystemTime, expires_in_seconds: Option<u64>) -> ServerResult<SystemTime> {
    const MIN_TTL: u64 = 60;
    const MAX_TTL: u64 = DEFAULT_CREDENTIAL_TTL.as_secs();

    let ttl = expires_in_seconds.unwrap_or(MAX_TTL);
    if !(MIN_TTL..=MAX_TTL).contains(&ttl) {
        return Err(ServerError::BadRequest);
    }

    now.checked_add(Duration::from_secs(ttl))
        .ok_or(ServerError::BadRequest)
}

async fn build_credential_restrictions(
    auth: &AuthContext,
    state: &ServerState,
    group_id: Ulid,
    requested_restrictions: Option<Vec<CreateS3PathRestriction>>,
) -> ServerResult<Option<Vec<NormalizedRestriction>>> {
    let group_root = group_permission_path(state.get_realm_id(), group_id, state.get_node_id());
    let auth_restrictions = normalize_auth_restrictions(auth, &group_root)?;
    let requested_restrictions =
        normalize_requested_restrictions(requested_restrictions, &group_root)?;

    validate_requested_restrictions(auth, state, requested_restrictions.as_deref()).await?;

    Ok(merge_effective_restrictions(
        auth_restrictions.as_deref(),
        requested_restrictions.as_deref(),
    ))
}

fn normalize_auth_restrictions(
    auth: &AuthContext,
    group_root: &str,
) -> ServerResult<Option<Vec<NormalizedRestriction>>> {
    let Some(restrictions) = auth.path_restrictions.as_ref() else {
        return Ok(None);
    };

    let mut normalized = Vec::new();
    for restriction in restrictions {
        if let Some(restriction) =
            parse_normalized_restriction(&restriction.pattern, restriction.permission.clone())
        {
            if let Some(scope) = restriction.scope.intersect_group_root(group_root) {
                normalized.push(NormalizedRestriction {
                    scope,
                    permission: restriction.permission,
                });
            }
            continue;
        }

        if pattern_reaches_group(&restriction.pattern, group_root) {
            return Err(ServerError::Forbidden);
        }
    }

    Ok(Some(normalized))
}

fn normalize_requested_restrictions(
    requested_restrictions: Option<Vec<CreateS3PathRestriction>>,
    group_root: &str,
) -> ServerResult<Option<Vec<NormalizedRestriction>>> {
    let Some(requested_restrictions) = requested_restrictions else {
        return Ok(None);
    };

    let mut normalized = Vec::with_capacity(requested_restrictions.len());
    for restriction in requested_restrictions {
        let permission = parse_permission(&restriction.permission)?;
        let pattern = if restriction.pattern.starts_with('/') {
            restriction.pattern
        } else if restriction.pattern.is_empty() {
            group_root.to_string()
        } else {
            format!(
                "{group_root}/{}",
                restriction.pattern.trim_start_matches('/')
            )
        };
        let Some(restriction) = parse_normalized_restriction(&pattern, permission) else {
            return Err(ServerError::BadRequest);
        };

        if !restriction.scope.is_within(group_root) {
            return Err(ServerError::Forbidden);
        }

        normalized.push(restriction);
    }

    Ok(Some(normalized))
}

async fn validate_requested_restrictions(
    auth: &AuthContext,
    state: &ServerState,
    requested_restrictions: Option<&[NormalizedRestriction]>,
) -> ServerResult<()> {
    let Some(requested_restrictions) = requested_restrictions else {
        return Ok(());
    };

    for restriction in requested_restrictions {
        if restriction.permission == Permission::DENY {
            continue;
        }

        crate::auth::ensure_permission(
            state,
            auth,
            restriction.scope.authorization_probe_path(),
            restriction.permission.clone(),
        )
        .await?;
    }

    Ok(())
}

fn merge_effective_restrictions(
    auth_restrictions: Option<&[NormalizedRestriction]>,
    requested_restrictions: Option<&[NormalizedRestriction]>,
) -> Option<Vec<NormalizedRestriction>> {
    // Requested allow rules replace inherited allows, while deny rules from both sides are kept.
    match (auth_restrictions, requested_restrictions) {
        (None, None) => None,
        (Some(auth_restrictions), None) => Some(auth_restrictions.to_vec()),
        (None, Some(requested_restrictions)) => Some(requested_restrictions.to_vec()),
        (Some(auth_restrictions), Some(requested_restrictions)) => {
            let auth_allows = auth_restrictions
                .iter()
                .filter(|restriction| restriction.permission != Permission::DENY)
                .cloned()
                .collect::<Vec<_>>();
            let requested_allows = requested_restrictions
                .iter()
                .filter(|restriction| restriction.permission != Permission::DENY)
                .cloned()
                .collect::<Vec<_>>();

            let mut effective = if requested_allows.is_empty() {
                auth_allows
            } else {
                requested_allows
            };

            for restriction in auth_restrictions
                .iter()
                .chain(requested_restrictions.iter())
            {
                if restriction.permission != Permission::DENY || effective.contains(restriction) {
                    continue;
                }

                effective.push(restriction.clone());
            }
            Some(effective)
        }
    }
}

/// A credential never exceeds the caller's own grant, so any member who may
/// read part of the group data may take one; only a caller left without an
/// allowed scope is refused.
async fn authorize_credential_issuance(
    auth: &AuthContext,
    state: &ServerState,
    group_id: Ulid,
    effective_restrictions: Option<&[NormalizedRestriction]>,
) -> ServerResult<()> {
    let group_root = group_permission_path(state.get_realm_id(), group_id, state.get_node_id());
    let effective_auth = AuthContext {
        path_restrictions: effective_restrictions.map(serialize_restrictions),
        ..auth.clone()
    };

    let Some(effective_restrictions) = effective_restrictions else {
        for permission in [Permission::WRITE, Permission::READ] {
            match crate::auth::ensure_permission(
                state,
                &effective_auth,
                group_root.to_string(),
                permission,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(ServerError::Forbidden) => {}
                Err(error) => return Err(error),
            }
        }
        let roots = aruna_operations::auth::permission_rules::reachable_roots(
            &state.get_ctx(),
            &effective_auth,
            &group_root,
        )
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
        for root in roots {
            match crate::auth::ensure_permission(state, &effective_auth, root, Permission::READ)
                .await
            {
                Ok(()) => return Ok(()),
                Err(ServerError::Forbidden) => {}
                Err(error) => return Err(error),
            }
        }
        return Err(ServerError::Forbidden);
    };

    for restriction in effective_restrictions {
        if restriction.permission == Permission::DENY {
            continue;
        }

        match crate::auth::ensure_permission(
            state,
            &effective_auth,
            restriction.scope.authorization_probe_path(),
            restriction.permission.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(ServerError::Forbidden) => continue,
            Err(err) => return Err(err),
        }
    }

    Err(ServerError::Forbidden)
}

fn pattern_reaches_group(pattern: &str, group_root: &str) -> bool {
    if pattern.starts_with(group_root) {
        return true;
    }

    let literal_prefix = pattern
        .split(['*', '?', '[', ']', '{', '}'])
        .next()
        .unwrap_or_default()
        .trim_end_matches('/');

    if literal_prefix.is_empty() {
        return true;
    }

    path_within(group_root, literal_prefix) || path_within(literal_prefix, group_root)
}

fn path_within(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn parse_permission(permission: &str) -> ServerResult<Permission> {
    match permission.to_ascii_uppercase().as_str() {
        "READ" => Ok(Permission::READ),
        "WRITE" => Ok(Permission::WRITE),
        "DENY" => Ok(Permission::DENY),
        _ => Err(ServerError::BadRequest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ServerError;
    use crate::tests::fixtures::routes::{
        seed_group_docs, seed_realm_auth, seed_realm_config, test_context,
        test_state as build_state, test_storage,
    };
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::NodeCapabilities;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, PathRestriction, Permission,
        RealmAuthorizationDocument, RealmConfigDocument, group_permission_path,
    };
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    async fn test_state() -> (TempDir, Arc<ServerState>, AuthContext) {
        let (storage_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let state = Arc::new(
            build_state(
                Arc::new(test_context(storage)),
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
            )
            .await,
        );
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([3u8; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        (storage_dir, state, auth)
    }

    fn test_auth_context(path_restrictions: Option<Vec<PathRestriction>>) -> AuthContext {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        AuthContext {
            user_id: UserId::new(Ulid::from_bytes([9u8; 16]), realm_id),
            realm_id,
            path_restrictions,
            session: None,
        }
    }

    /// State whose caller holds group write, plus a credential of another member.
    async fn revoke_state() -> (TempDir, Arc<ServerState>, AuthContext, String) {
        let (dir, state, auth) = test_state().await;
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let group_id = Ulid::from_bytes([4u8; 16]);
        let owner = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: auth.user_id,
            realm_id,
        };
        seed_realm_config(&state.get_ctx(), realm_id, &actor).await;
        seed_realm_auth(&state.get_ctx(), realm_id, &actor).await;
        seed_group_docs(
            &state.get_ctx(),
            realm_id,
            &actor,
            group_id,
            "credential-group",
            auth.user_id,
        )
        .await;

        let (access_key_id, _, _) = drive(
            CreateUserAccessOperation::new(
                CreateUserAccessConfig {
                    user_identity: owner,
                    group_id,
                    expiry: SystemTime::now() + Duration::from_secs(3600),
                    path_restrictions: None,
                    issued_by: *node_id.as_bytes(),
                },
                state.credential_encryption_key().clone(),
            ),
            &state.get_ctx(),
        )
        .await
        .unwrap()
        .unwrap();

        (dir, state, auth, access_key_id)
    }

    /// Group whose only role for the caller carries the given permissions.
    async fn scoped_state(
        permissions: Vec<(String, Permission)>,
    ) -> (TempDir, Arc<ServerState>, AuthContext, Ulid) {
        use std::collections::{HashMap, HashSet};
        let (dir, state, auth) = test_state().await;
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let group_id = Ulid::from_bytes([7u8; 16]);
        let actor = Actor {
            node_id,
            user_id: auth.user_id,
            realm_id,
        };
        let role_id = Ulid::from_bytes([8u8; 16]);
        let group_auth = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(
                role_id,
                aruna_core::structs::Role {
                    role_id,
                    name: "scoped".to_string(),
                    permissions: permissions.into_iter().collect(),
                    assigned_users: HashSet::from([auth.user_id]),
                },
            )]),
            policies: Vec::new(),
        };
        let group = Group {
            display_name: "scoped-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: auth.user_id,
        };
        for (key_space, key, value) in [
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                    .to_bytes(&actor)
                    .unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                RealmAuthorizationDocument::default_realm_doc(realm_id)
                    .to_bytes(&actor)
                    .unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            state
                .get_ctx()
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }

        (dir, state, auth, group_id)
    }

    async fn create_credential(
        state: &Arc<ServerState>,
        auth: &AuthContext,
        group_id: Ulid,
        path_restrictions: Option<Vec<CreateS3PathRestriction>>,
    ) -> ServerResult<(StatusCode, Json<CreateS3CredentialsResponse>)> {
        create_s3_credentials(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(CreateS3CredentialsRequest {
                group_id: group_id.to_string(),
                expires_in_seconds: None,
                path_restrictions,
            }),
        )
        .await
    }

    #[tokio::test]
    async fn viewer_takes_credential() {
        // Read on the group data root is enough: the credential cannot widen it.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::from_bytes([7u8; 16]);
        let (_dir, state, auth, group_id) = scoped_state(vec![(
            format!("/{realm_id}/g/{group_id}/data/**"),
            Permission::READ,
        )])
        .await;

        assert!(
            create_credential(&state, &auth, group_id, None)
                .await
                .is_ok()
        );
        let (_, Json(response)) = create_credential(
            &state,
            &auth,
            group_id,
            Some(vec![CreateS3PathRestriction {
                pattern: "shared/**".to_string(),
                permission: "READ".to_string(),
            }]),
        )
        .await
        .unwrap();
        assert!(!response.access_secret.is_empty());
    }

    #[tokio::test]
    async fn viewer_cannot_write() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::from_bytes([7u8; 16]);
        let (_dir, state, auth, group_id) = scoped_state(vec![(
            format!("/{realm_id}/g/{group_id}/data/**"),
            Permission::READ,
        )])
        .await;

        let error = create_credential(
            &state,
            &auth,
            group_id,
            Some(vec![CreateS3PathRestriction {
                pattern: "shared/**".to_string(),
                permission: "WRITE".to_string(),
            }]),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Forbidden));
    }

    #[tokio::test]
    async fn subpath_takes_credential() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::from_bytes([7u8; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let group_root = group_permission_path(realm_id, group_id, node_id);
        let (_dir, state, auth, group_id) = scoped_state(vec![(
            format!("{group_root}/study/imaging/**"),
            Permission::READ,
        )])
        .await;

        assert!(
            create_credential(&state, &auth, group_id, None)
                .await
                .is_ok()
        );
        assert!(
            create_credential(
                &state,
                &auth,
                group_id,
                Some(vec![CreateS3PathRestriction {
                    pattern: "study/imaging/**".to_string(),
                    permission: "READ".to_string(),
                }]),
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn outsider_refused() {
        let (_dir, state, auth, group_id) = scoped_state(vec![(
            "/other/g/group/data/**".to_string(),
            Permission::WRITE,
        )])
        .await;

        assert!(matches!(
            create_credential(&state, &auth, group_id, None).await,
            Err(ServerError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn writer_cannot_revoke() {
        // Group write must not reach the S3 credential of another member.
        let (_dir, state, auth, access_key_id) = revoke_state().await;

        let error = revoke_s3_credentials(
            State(state.clone()),
            Extension(Some(auth)),
            Path(access_key_id.clone()),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ServerError::Forbidden));
        let credential = drive(GetUserAccessOperation::new(access_key_id), &state.get_ctx())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!credential.is_revoked());
    }

    #[test]
    fn group_root_canonical() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();

        let group_root = group_permission_path(realm_id, group_id, node_id);
        assert_eq!(
            group_root,
            format!("/{realm_id}/g/{group_id}/data/{node_id}")
        );
    }

    #[test]
    fn permission_case_insensitive() {
        assert_eq!(
            parse_permission("read").unwrap(),
            aruna_core::structs::Permission::READ
        );
        assert_eq!(
            parse_permission("WRITE").unwrap(),
            aruna_core::structs::Permission::WRITE
        );
        assert_eq!(
            parse_permission("Deny").unwrap(),
            aruna_core::structs::Permission::DENY
        );
    }

    #[test]
    fn delegation_accepts_descendants() {
        assert_eq!(
            DelegationScope::parse_supported("/root/path"),
            Some(DelegationScope::exact("/root/path".to_string()))
        );
        assert_eq!(
            DelegationScope::parse_supported("/root/path/**"),
            Some(DelegationScope::descendants("/root/path".to_string()))
        );
        assert_eq!(DelegationScope::parse_supported("/root/*/path"), None);
        assert_eq!(DelegationScope::parse_supported("/root/**/path"), None);
        assert_eq!(DelegationScope::parse_supported("relative/path"), None);
    }

    #[test]
    fn exact_scope_preserved() {
        let scope = DelegationScope::exact("/realm/g/group/data/node/object".to_string());
        assert_eq!(
            scope.intersect_group_root("/realm/g/group/data/node"),
            Some(DelegationScope::exact(
                "/realm/g/group/data/node/object".to_string()
            ))
        );
    }

    #[test]
    fn descendant_scope_narrowed() {
        let scope = DelegationScope::descendants("/realm/g/group/data".to_string());
        assert_eq!(
            scope.intersect_group_root("/realm/g/group/data/node"),
            Some(DelegationScope::descendants(
                "/realm/g/group/data/node".to_string()
            ))
        );
    }

    #[test]
    fn relative_paths_normalized() {
        let group_root = "/realm/g/group/data/node";

        assert_eq!(
            normalize_requested_restrictions(
                Some(vec![CreateS3PathRestriction {
                    pattern: "nested/path".to_string(),
                    permission: "WRITE".to_string(),
                }]),
                group_root,
            )
            .unwrap(),
            Some(vec![NormalizedRestriction {
                scope: DelegationScope::exact("/realm/g/group/data/node/nested/path".to_string()),
                permission: Permission::WRITE,
            }])
        );
    }

    #[test]
    fn empty_path_normalized() {
        let group_root = "/realm/g/group/data/node";

        assert_eq!(
            normalize_requested_restrictions(
                Some(vec![CreateS3PathRestriction {
                    pattern: String::new(),
                    permission: "READ".to_string(),
                }]),
                group_root,
            )
            .unwrap(),
            Some(vec![NormalizedRestriction {
                scope: DelegationScope::exact(group_root.to_string()),
                permission: Permission::READ,
            }])
        );
    }

    #[test]
    fn external_path_rejected() {
        let err = normalize_requested_restrictions(
            Some(vec![CreateS3PathRestriction {
                pattern: "/realm/g/other/data/node/object".to_string(),
                permission: "WRITE".to_string(),
            }]),
            "/realm/g/group/data/node",
        )
        .unwrap_err();

        assert!(matches!(err, ServerError::Forbidden));
    }

    #[test]
    fn wildcards_are_rejected() {
        let err = normalize_requested_restrictions(
            Some(vec![CreateS3PathRestriction {
                pattern: "nested/*/path".to_string(),
                permission: "WRITE".to_string(),
            }]),
            "/realm/g/group/data/node",
        )
        .unwrap_err();

        assert!(matches!(err, ServerError::BadRequest));
    }

    #[test]
    fn unrelated_groups_filtered() {
        let auth = test_auth_context(Some(vec![PathRestriction {
            pattern: "/realm/g/other/data/node/**".to_string(),
            permission: Permission::WRITE,
        }]));

        assert_eq!(
            normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap(),
            Some(Vec::new())
        );
    }

    #[tokio::test]
    async fn list_rejects_scope() {
        let (_storage_dir, state, auth) = test_state().await;
        for restrictions in [
            Some(vec![PathRestriction {
                pattern: "/restricted/**".to_string(),
                permission: Permission::READ,
            }]),
            Some(Vec::new()),
        ] {
            let mut restricted = auth.clone();
            restricted.path_restrictions = restrictions;
            let error = list_s3_credentials(State(state.clone()), Extension(Some(restricted)))
                .await
                .unwrap_err();
            assert!(matches!(error, ServerError::Forbidden));
        }

        let (status, Json(response)) = list_s3_credentials(State(state), Extension(Some(auth)))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(response.credentials.is_empty());
    }

    #[test]
    fn broad_scope_narrowed() {
        let auth = test_auth_context(Some(vec![PathRestriction {
            pattern: "/realm/g/group/data/**".to_string(),
            permission: Permission::WRITE,
        }]));

        assert_eq!(
            normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap(),
            Some(vec![NormalizedRestriction {
                scope: DelegationScope::descendants("/realm/g/group/data/node".to_string()),
                permission: Permission::WRITE,
            }])
        );
    }

    #[test]
    fn auth_wildcards_rejected() {
        let auth = test_auth_context(Some(vec![PathRestriction {
            pattern: "/realm/g/group/**/node".to_string(),
            permission: Permission::WRITE,
        }]));

        let err = normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap_err();
        assert!(matches!(err, ServerError::Forbidden));
    }

    #[test]
    fn auth_restrictions_inherited() {
        let auth = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::WRITE,
        }];

        assert_eq!(
            merge_effective_restrictions(Some(&auth), None),
            Some(auth.clone())
        );
    }

    #[test]
    fn request_restrictions_used() {
        let requested = vec![NormalizedRestriction {
            scope: DelegationScope::exact("/group/object".to_string()),
            permission: Permission::READ,
        }];

        assert_eq!(
            merge_effective_restrictions(None, Some(&requested)),
            Some(requested.clone())
        );
    }

    #[test]
    fn requested_allow_used() {
        let auth = vec![NormalizedRestriction {
            scope: DelegationScope::exact("/group/object".to_string()),
            permission: Permission::WRITE,
        }];
        let requested = auth.clone();

        assert_eq!(
            merge_effective_restrictions(Some(&auth), Some(&requested)),
            Some(requested)
        );
    }

    #[test]
    fn requested_read_preserved() {
        let auth = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::WRITE,
        }];
        let requested = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::READ,
        }];

        assert_eq!(
            merge_effective_restrictions(Some(&auth), Some(&requested)),
            Some(requested)
        );
    }

    #[test]
    fn requested_write_preserved() {
        let auth = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::READ,
        }];
        let requested = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::WRITE,
        }];

        assert_eq!(
            merge_effective_restrictions(Some(&auth), Some(&requested)),
            Some(requested)
        );
    }

    #[test]
    fn auth_denies_preserved() {
        let auth = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/blocked".to_string()),
            permission: Permission::DENY,
        }];
        let requested = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::WRITE,
        }];

        assert_eq!(
            merge_effective_restrictions(Some(&auth), Some(&requested)),
            Some(vec![requested[0].clone(), auth[0].clone(),])
        );
    }

    #[test]
    fn requested_denies_preserved() {
        let auth = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/allowed".to_string()),
            permission: Permission::WRITE,
        }];
        let requested = vec![NormalizedRestriction {
            scope: DelegationScope::descendants("/group/blocked".to_string()),
            permission: Permission::DENY,
        }];

        assert_eq!(
            merge_effective_restrictions(Some(&auth), Some(&requested)),
            Some(vec![auth[0].clone(), requested[0].clone(),])
        );
    }
}
