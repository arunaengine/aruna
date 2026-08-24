use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{
    AuthContext, PathRestriction, Permission, UserAccess, blob_group_permission_path,
};
use aruna_operations::driver::drive;
use aruna_operations::s3::create_user_access::{
    CreateUserAccessConfig, CreateUserAccessError, CreateUserAccessOperation,
    DEFAULT_CREDENTIAL_TTL,
};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use aruna_operations::s3::list_user_access::{ListUserAccessInput, ListUserAccessOperation};
use aruna_operations::s3::revoke_user_access::{RevokeUserAccessError, RevokeUserAccessOperation};
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
    tags((name = "credentials", description = "User credential management"))
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
        is_same_path_or_descendant(&self.root, root)
    }

    fn intersect_group_root(&self, group_root: &str) -> Option<Self> {
        if is_same_path_or_descendant(&self.root, group_root) {
            return Some(self.clone());
        }

        if self.recursive && is_same_path_or_descendant(group_root, &self.root) {
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
    path = "/users/credentials",
    tag = "credentials",
    summary = "List the caller's S3 credentials",
    description = r#"Lists the S3 credentials of the calling identity held by the node serving the request.

**Authentication**: realm bearer token that carries no path restrictions; a delegated token is
refused with 403.

**Behavior**
- Self-scoped: the response only ever contains credentials issued to the calling identity.
- Only credentials held by the serving node are listed, so a credential issued on another node of
  the realm does not appear here.
- Secret access keys are never returned by this operation.
- Every entry carries the access key id, the group the credential is bound to, expiry and
  revocation timestamps as RFC 3339 UTC with second precision, the id of the issuing node, the
  effective path restrictions and the derived status `active`, `expired` or `revoked`.

**Limits**
- The listing is not paginated and covers at most 16 active credentials per user, ordered by access
  key id."#,
    responses(
        (
            status = 200,
            description = "Credentials of the calling user held by this node, with every secret access key omitted",
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
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm or carries path restrictions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListS3CredentialsResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;

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
            credentials: credentials
                .into_iter()
                .map(map_user_access_redacted)
                .collect(),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users/credentials",
    tag = "credentials",
    summary = "Create an S3 credential for a group",
    description = r#"Issues an S3 access key and a one-time secret bound to a group, for the calling identity.

**Authentication**: realm bearer token with write access to the group's data path. A
path-restricted (delegated) token may be used; the issued credential inherits the caller's
restrictions narrowed to the group data root and can never widen them, and every requested allow
scope is authorized against the caller's own grant before it is written.

**Behavior**
- Self-scoped: the credential is always issued to the calling identity, so no caller can mint a
  credential for another user.
- The secret access key is returned once, in this response only: it is stored sealed and is not
  retrievable afterwards, so a caller that loses it has to create a new credential, and later
  listings show only the access key id.
- The credential is stored on the node that served the request and is accepted by that node's S3
  endpoint.

**Limits**
- The optional lifetime is given in seconds between 60 and 31536000 and defaults to 31536000.
- A restriction pattern is relative to the group data root or an absolute path inside it, may name
  an exact path or a subtree with a trailing `/**`, and takes the permission `READ`, `WRITE` or
  `DENY` case insensitively; at most 50 restrictions are accepted.
- A user holds at most 16 active credentials; a further request is refused with 409 until one is
  revoked or expires."#,
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
            description = "Credential created; access_secret is the plaintext secret access key and is shown only in this response, while access_key_id identifies the credential from now on",
            body = CreateS3CredentialsResponse,
            example = json!({
                "access_key_id": "01JAKEY0123456789ABCDEFGHJ",
                "access_secret": "<one-time-secret-shown-only-in-this-response>"
            })
        ),
        (status = 400, description = "The group id is not a ULID, the requested lifetime is outside 60 to 31536000 seconds, a restriction uses an unsupported wildcard, or more restrictions were requested than are accepted", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm, the caller has no write access to the group data path, or a requested restriction reaches outside the group root or outside the caller's own grant", body = ErrorResponse),
        (status = 409, description = "The caller already holds the maximum of 16 active credentials; revoke or let one expire before creating another", body = ErrorResponse)
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
    let group_root = blob_group_permission_path(realm_id, group_id, state.get_node_id());
    let path_restrictions =
        build_credential_restrictions(&auth, &state, group_id, request.path_restrictions.clone())
            .await?;
    authorize_credential_issuance(&auth, &state, &group_root, path_restrictions.as_deref()).await?;
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
            state.credential_seal_key().clone(),
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
    path = "/users/credentials/{access_key_id}",
    tag = "credentials",
    summary = "Revoke an S3 credential",
    description = r#"Revokes an S3 credential held by the node serving the request.

**Authentication**: realm bearer token that carries no path restrictions; a delegated token is
refused with 403. A caller may always revoke a credential issued to their own identity; revoking
another user's credential additionally requires write access on that user's realm administration
path, so write access to the group the credential is bound to is deliberately not enough.

**Behavior**
- Only credentials held by the node that serves the request can be revoked here, and an access key
  unknown to this node is answered with 404.
- The record is not deleted: it keeps appearing in the caller's listing with a revocation timestamp
  and the `revoked` status.
- The node stops accepting the key for new S3 requests."#,
    params(("access_key_id" = String, Path, description = "Access key id of the credential to revoke, a ULID as returned when the credential was created or listed")),
    responses(
        (status = 204, description = "Credential revoked; the response carries no body"),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm, carries path restrictions, or the caller lacks write access on the owning user's administration path", body = ErrorResponse),
        (status = 404, description = "This node holds no credential with that access key id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;

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

    // Credentials are user-owned like the list and create surfaces: write access
    // to the group must not reach another member's credential, so revoking one
    // needs either ownership or administrative authority over that user.
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

fn map_user_access_redacted(access: UserAccess) -> S3CredentialSummaryResponse {
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
    let group_root =
        blob_group_permission_path(state.get_realm_id(), group_id, state.get_node_id());
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

        if auth_pattern_may_apply_to_group_root(&restriction.pattern, group_root) {
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

        check_permission(
            auth,
            state,
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

async fn authorize_credential_issuance(
    auth: &AuthContext,
    state: &ServerState,
    group_root: &str,
    effective_restrictions: Option<&[NormalizedRestriction]>,
) -> ServerResult<()> {
    let effective_auth = AuthContext {
        path_restrictions: effective_restrictions.map(serialize_restrictions),
        ..auth.clone()
    };

    let Some(effective_restrictions) = effective_restrictions else {
        return check_permission(
            &effective_auth,
            state,
            group_root.to_string(),
            Permission::WRITE,
        )
        .await;
    };

    for restriction in effective_restrictions {
        if restriction.permission != Permission::WRITE {
            continue;
        }

        match check_permission(
            &effective_auth,
            state,
            restriction.scope.authorization_probe_path(),
            Permission::WRITE,
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

async fn check_permission(
    auth: &AuthContext,
    state: &ServerState,
    path: String,
    required_permission: Permission,
) -> ServerResult<()> {
    crate::auth::ensure_permission(state, auth, path, required_permission).await
}

fn auth_pattern_may_apply_to_group_root(pattern: &str, group_root: &str) -> bool {
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

    is_same_path_or_descendant(group_root, literal_prefix)
        || is_same_path_or_descendant(literal_prefix, group_root)
}

fn is_same_path_or_descendant(path: &str, root: &str) -> bool {
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
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::NodeCapabilities;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, PathRestriction, Permission,
        RealmAuthorizationDocument, RealmConfigDocument, blob_group_permission_path,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    async fn test_state() -> (TempDir, Arc<ServerState>, AuthContext) {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let state = Arc::new(
            ServerState::new(
                Arc::new(DriverContext {
                    storage_handle: storage,
                    net_handle: None,
                    blob_handle: None,
                    metadata_handle: None,
                    task_handle: None,
                    compute_handle: None,
                }),
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([3u8; 16]), realm_id),
            realm_id,
            path_restrictions: None,
        };
        (storage_dir, state, auth)
    }

    fn test_auth_context(path_restrictions: Option<Vec<PathRestriction>>) -> AuthContext {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        AuthContext {
            user_id: UserId::new(Ulid::from_bytes([9u8; 16]), realm_id),
            realm_id,
            path_restrictions,
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
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(auth.user_id, realm_id, group_id);
        let group = Group {
            display_name: "credential-group".to_string(),
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
                realm_auth.to_bytes(&actor).unwrap(),
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

        let (access_key_id, _, _) = drive(
            CreateUserAccessOperation::new(
                CreateUserAccessConfig {
                    user_identity: owner,
                    group_id,
                    expiry: SystemTime::now() + Duration::from_secs(3600),
                    path_restrictions: None,
                    issued_by: *node_id.as_bytes(),
                },
                state.credential_seal_key().clone(),
            ),
            &state.get_ctx(),
        )
        .await
        .unwrap()
        .unwrap();

        (dir, state, auth, access_key_id)
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
    fn credential_group_root_matches_canonical_blob_group_path() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();

        let group_root = blob_group_permission_path(realm_id, group_id, node_id);
        assert_eq!(
            group_root,
            format!("/{realm_id}/g/{group_id}/data/{node_id}")
        );
    }

    #[test]
    fn parse_permission_accepts_known_values_case_insensitively() {
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
    fn delegation_scope_accepts_exact_and_final_descendants_only() {
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
    fn delegation_scope_exact_within_group_root_is_preserved() {
        let scope = DelegationScope::exact("/realm/g/group/data/node/object".to_string());
        assert_eq!(
            scope.intersect_group_root("/realm/g/group/data/node"),
            Some(DelegationScope::exact(
                "/realm/g/group/data/node/object".to_string()
            ))
        );
    }

    #[test]
    fn delegation_scope_descendant_scope_is_narrowed_to_group_root() {
        let scope = DelegationScope::descendants("/realm/g/group/data".to_string());
        assert_eq!(
            scope.intersect_group_root("/realm/g/group/data/node"),
            Some(DelegationScope::descendants(
                "/realm/g/group/data/node".to_string()
            ))
        );
    }

    #[test]
    fn normalize_requested_restrictions_makes_relative_paths_absolute() {
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
    fn normalize_requested_restrictions_empty_path_becomes_group_root() {
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
    fn normalize_requested_restrictions_rejects_absolute_path_outside_group_root() {
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
    fn normalize_requested_restrictions_rejects_unsupported_wildcards() {
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
    fn normalize_auth_restrictions_filters_unrelated_groups() {
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
    fn normalize_auth_restrictions_narrows_broader_scope_to_group_root() {
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
    fn normalize_auth_restrictions_rejects_applicable_unsupported_wildcards() {
        let auth = test_auth_context(Some(vec![PathRestriction {
            pattern: "/realm/g/group/**/node".to_string(),
            permission: Permission::WRITE,
        }]));

        let err = normalize_auth_restrictions(&auth, "/realm/g/group/data/node").unwrap_err();
        assert!(matches!(err, ServerError::Forbidden));
    }

    #[test]
    fn effective_restrictions_inherit_auth_when_request_is_absent() {
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
    fn effective_restrictions_pass_through_request_when_auth_is_absent() {
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
    fn effective_restrictions_use_requested_allow_when_auth_and_request_match() {
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
    fn effective_restrictions_preserve_requested_read_under_auth_write() {
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
    fn effective_restrictions_follow_current_request_write_over_auth_read_semantics() {
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
    fn effective_restrictions_preserve_auth_denies() {
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
    fn requested_denies_keep_inherited_auth_allows() {
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
