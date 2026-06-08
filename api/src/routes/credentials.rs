use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{AuthContext, PathRestriction, Permission, blob_group_permission_path};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use aruna_operations::s3::create_user_access::{
    CreateUserAccessConfig, CreateUserAccessOperation, DEFAULT_CREDENTIAL_TTL,
};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use aruna_operations::s3::revoke_user_access::{RevokeUserAccessError, RevokeUserAccessOperation};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use std::{str::FromStr, sync::Arc};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "credentials", description = "User credential management")),
    paths(create_s3_credentials, revoke_s3_credentials)
)]
pub struct CredentialsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/users/credentials", post(create_s3_credentials))
        .route(
            "/users/credentials/{access_key_id}",
            delete(revoke_s3_credentials),
        )
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
    post,
    path = "/users/credentials",
    tag = "credentials",
    request_body = CreateS3CredentialsRequest,
    responses(
        (status = 201, description = "Credentials created", body = CreateS3CredentialsResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
    let group_root = blob_group_permission_path(realm_id, group_id, state.get_node_id());
    let path_restrictions =
        build_credential_restrictions(&auth, &state, group_id, request.path_restrictions.clone())
            .await?;
    authorize_credential_issuance(&auth, &state, &group_root, path_restrictions.as_deref()).await?;
    let path_restrictions = path_restrictions.as_deref().map(serialize_restrictions);
    let expiry = credential_expiry(SystemTime::now(), request.expires_in_seconds)?;
    let result = drive(
        CreateUserAccessOperation::new(CreateUserAccessConfig {
            user_identity,
            group_id,
            expiry,
            path_restrictions,
            issued_by: *node_id.as_bytes(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    match result {
        Ok((access_key_id, access_secret)) => Ok((
            StatusCode::CREATED,
            Json(CreateS3CredentialsResponse {
                access_key_id,
                access_secret: access_secret.secret,
            }),
        )),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

#[utoipa::path(
    delete,
    path = "/users/credentials/{access_key_id}",
    tag = "credentials",
    params(("access_key_id" = String, Path, description = "S3 access key to revoke")),
    responses(
        (status = 204, description = "Credential revoked"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Credential not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;

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

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth,
            path: blob_group_permission_path(
                state.get_realm_id(),
                credential.group_id,
                state.get_node_id(),
            ),
            required_permission: Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    if !allowed {
        return Err(ServerError::Forbidden);
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
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path,
            required_permission,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => ServerError::Forbidden,
        _ => ServerError::InternalError(err.to_string()),
    })?;

    if allowed {
        Ok(())
    } else {
        Err(ServerError::Forbidden)
    }
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
    use super::{
        CreateS3PathRestriction, DelegationScope, NormalizedRestriction,
        merge_effective_restrictions, normalize_auth_restrictions,
        normalize_requested_restrictions, parse_permission,
    };
    use crate::error::ServerError;
    use aruna_core::UserId;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::{
        AuthContext, PathRestriction, Permission, blob_group_permission_path,
    };
    use ulid::Ulid;

    fn test_auth_context(path_restrictions: Option<Vec<PathRestriction>>) -> AuthContext {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        AuthContext {
            user_id: UserId::new(Ulid::from_bytes([9u8; 16]), realm_id),
            realm_id,
            path_restrictions,
        }
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
