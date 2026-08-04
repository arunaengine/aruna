use crate::auth::{ensure_permission, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyDecision, PolicyFunctions, PolicyKind, PolicyRequest, PolicyTraceEntry,
    RequestPolicy, analyze_policy_source, policy_set_hash, validate_policy_set,
};
use aruna_core::structs::{Actor, AuthContext, Permission};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::set_group_policies::{
    SetGroupPoliciesConfig, SetGroupPoliciesError, SetGroupPoliciesOperation,
};
use aruna_operations::set_realm_policies::{
    SetRealmPoliciesConfig, SetRealmPoliciesError, SetRealmPoliciesOperation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "policies", description = "Deny-only CEL request policies")),
    paths(
        get_realm_policies,
        set_realm_policies,
        get_group_policies,
        set_group_policies,
        effective_policies,
        validate_policy,
        dry_run_policy
    )
)]
pub struct PoliciesApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/policies/realm",
            get(get_realm_policies).put(set_realm_policies),
        )
        .route(
            "/policies/group/{group_id}",
            get(get_group_policies).put(set_group_policies),
        )
        .route("/policies/effective", get(effective_policies))
        .route("/policies/validate", post(validate_policy))
        .route("/policies/dry-run", post(dry_run_policy))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PolicyBody {
    /// Stable id; minted when absent.
    #[serde(default)]
    pub policy_id: Option<String>,
    pub name: String,
    /// `deny` denies when the expression is true; `require` denies unless true.
    #[serde(default = "default_kind")]
    pub kind: String,
    /// Optional CEL applicability guard.
    #[serde(default)]
    pub when: Option<String>,
    /// CEL expression over `path`, `permission`, `user`, `anonymous`,
    /// `operation`, `params`, `headers`, `body`.
    pub expression: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

fn default_kind() -> String {
    "deny".to_string()
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SetPoliciesRequest {
    pub policies: Vec<PolicyBody>,
    /// When set, the write only applies if it matches the stored `set_hash`.
    #[serde(default)]
    pub expected_hash: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PoliciesResponse {
    pub policies: Vec<PolicyBody>,
    /// Content address of the stored set, for optimistic concurrency.
    pub set_hash: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ScopedPolicy {
    pub scope: String,
    #[serde(flatten)]
    pub policy: PolicyBody,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EffectivePoliciesResponse {
    pub policies: Vec<ScopedPolicy>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct EffectiveQuery {
    #[serde(default)]
    pub group_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ValidatePolicyRequest {
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub when: Option<String>,
    pub expression: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatePolicyResponse {
    pub valid: bool,
    pub errors: Vec<String>,
    pub referenced_variables: Vec<String>,
    pub unknown_variables: Vec<String>,
    pub unknown_functions: Vec<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DryRunRequest {
    /// Canonical permission path the request would target.
    pub path: String,
    /// `read` or `write`.
    pub permission: String,
    /// Caller attribution; empty means anonymous.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
    #[serde(default)]
    pub params: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub body: Option<serde_json::Value>,
    /// Ad hoc policies to try; when absent the requested scope is evaluated.
    #[serde(default)]
    pub candidate_policies: Option<Vec<PolicyBody>>,
    /// `realm` (default), `group`, or `effective` when no candidates are given.
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DryRunResponse {
    pub denied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[schema(value_type = Vec<Object>)]
    pub trace: Vec<ScopedTraceEntry>,
}

#[derive(Debug, Serialize)]
pub struct ScopedTraceEntry {
    pub scope: String,
    #[serde(flatten)]
    pub entry: PolicyTraceEntry,
}

fn kind_label(kind: PolicyKind) -> String {
    match kind {
        PolicyKind::Deny => "deny".to_string(),
        PolicyKind::Require => "require".to_string(),
    }
}

fn parse_kind(kind: &str) -> ServerResult<PolicyKind> {
    match kind {
        "deny" => Ok(PolicyKind::Deny),
        "require" => Ok(PolicyKind::Require),
        _ => Err(ServerError::BadRequestMessage(format!(
            "unknown policy kind `{kind}`"
        ))),
    }
}

fn to_request_policy(body: &PolicyBody) -> ServerResult<RequestPolicy> {
    let policy_id = body
        .policy_id
        .as_deref()
        .map(Ulid::from_str)
        .transpose()
        .map_err(|_| ServerError::BadRequest)?
        .unwrap_or_else(Ulid::generate);
    Ok(RequestPolicy {
        policy_id,
        name: body.name.clone(),
        kind: parse_kind(&body.kind)?,
        when: body.when.clone().filter(|guard| !guard.is_empty()),
        expression: body.expression.clone(),
        enabled: body.enabled,
    })
}

fn map_policy(policy: &RequestPolicy) -> PolicyBody {
    PolicyBody {
        policy_id: Some(policy.policy_id.to_string()),
        name: policy.name.clone(),
        kind: kind_label(policy.kind),
        when: policy.when.clone(),
        expression: policy.expression.clone(),
        enabled: policy.enabled,
    }
}

fn set_hash_hex(policies: &[RequestPolicy]) -> String {
    hex_encode(&policy_set_hash(policies))
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(64), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn policies_response(policies: &[RequestPolicy]) -> PoliciesResponse {
    PoliciesResponse {
        policies: policies.iter().map(map_policy).collect(),
        set_hash: set_hash_hex(policies),
    }
}

async fn realm_policies(state: &ServerState) -> ServerResult<Vec<RequestPolicy>> {
    match drive(GetRealmConfigOperation::new(state.get_realm_id()), &state.get_ctx()).await {
        Ok(config) => Ok(config.request_policies),
        Err(aruna_operations::get_realm_config::GetRealmConfigError::DocumentNotFound) => {
            Ok(Vec::new())
        }
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn group_policies(state: &ServerState, group_id: Ulid) -> ServerResult<Vec<RequestPolicy>> {
    match drive(GetGroupOperation::new(GetGroupConfig { group_id }), &state.get_ctx()).await {
        Ok((_, auth_doc)) => Ok(auth_doc.policies),
        Err(
            aruna_operations::get_group::GetGroupError::GroupNotFound
            | aruna_operations::get_group::GetGroupError::AuthDocNotFound,
        ) => Ok(Vec::new()),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn require_config_admin(state: &ServerState, auth: &AuthContext) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        format!("/{}/admin/config", state.get_realm_id()),
        Permission::WRITE,
    )
    .await
}

async fn require_group_config_admin(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        format!("/{}/g/{}/admin/config", state.get_realm_id(), group_id),
        Permission::WRITE,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/policies/realm",
    tag = "policies",
    responses(
        (status = 200, description = "Stored realm policy set", body = PoliciesResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let _auth = require_realm_auth(&state, auth)?;
    let policies = realm_policies(&state).await?;
    Ok((StatusCode::OK, Json(policies_response(&policies))))
}

#[utoipa::path(
    put,
    path = "/policies/realm",
    tag = "policies",
    request_body = SetPoliciesRequest,
    responses(
        (status = 200, description = "Stored policy set", body = PoliciesResponse),
        (status = 400, description = "Invalid policy set", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 409, description = "Stale expected_hash", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SetPoliciesRequest>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    require_config_admin(&state, &auth).await?;

    let policies = request
        .policies
        .iter()
        .map(to_request_policy)
        .collect::<ServerResult<Vec<_>>>()?;

    if let Some(expected) = &request.expected_hash {
        let current = set_hash_hex(&realm_policies(&state).await?);
        if &current != expected {
            return Err(ServerError::Conflict(
                "stored realm policy set changed".to_string(),
            ));
        }
    }

    let document = drive(
        SetRealmPoliciesOperation::new(SetRealmPoliciesConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            policies,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        SetRealmPoliciesError::InvalidPolicies { reason } => ServerError::BadRequestMessage(reason),
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((
        StatusCode::OK,
        Json(policies_response(&document.request_policies)),
    ))
}

#[utoipa::path(
    get,
    path = "/policies/group/{group_id}",
    tag = "policies",
    params(("group_id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Stored group policy set", body = PoliciesResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let _auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let policies = group_policies(&state, group_id).await?;
    Ok((StatusCode::OK, Json(policies_response(&policies))))
}

#[utoipa::path(
    put,
    path = "/policies/group/{group_id}",
    tag = "policies",
    params(("group_id" = String, Path, description = "Group id")),
    request_body = SetPoliciesRequest,
    responses(
        (status = 200, description = "Stored policy set", body = PoliciesResponse),
        (status = 400, description = "Invalid policy set", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 409, description = "Stale expected_hash", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_group_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<SetPoliciesRequest>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    require_group_config_admin(&state, &auth, group_id).await?;

    let policies = request
        .policies
        .iter()
        .map(to_request_policy)
        .collect::<ServerResult<Vec<_>>>()?;

    if let Some(expected) = &request.expected_hash {
        let current = set_hash_hex(&group_policies(&state, group_id).await?);
        if &current != expected {
            return Err(ServerError::Conflict(
                "stored group policy set changed".to_string(),
            ));
        }
    }

    let document = drive(
        SetGroupPoliciesOperation::new(SetGroupPoliciesConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            group_id,
            policies,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        SetGroupPoliciesError::InvalidPolicies { reason } => ServerError::BadRequestMessage(reason),
        SetGroupPoliciesError::GroupAuthDocNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((StatusCode::OK, Json(policies_response(&document.policies))))
}

#[utoipa::path(
    get,
    path = "/policies/effective",
    tag = "policies",
    params(("group_id" = Option<String>, Query, description = "Optional group scope")),
    responses(
        (status = 200, description = "Merged realm and group policy set", body = EffectivePoliciesResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn effective_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<EffectiveQuery>,
) -> ServerResult<(StatusCode, Json<EffectivePoliciesResponse>)> {
    let _auth = require_realm_auth(&state, auth)?;
    let mut policies: Vec<ScopedPolicy> = realm_policies(&state)
        .await?
        .iter()
        .map(|policy| ScopedPolicy {
            scope: "realm".to_string(),
            policy: map_policy(policy),
        })
        .collect();
    if let Some(group_id) = &query.group_id {
        let group_id = parse_group_id(group_id)?;
        let label = format!("group({group_id})");
        policies.extend(group_policies(&state, group_id).await?.iter().map(|policy| {
            ScopedPolicy {
                scope: label.clone(),
                policy: map_policy(policy),
            }
        }));
    }
    Ok((StatusCode::OK, Json(EffectivePoliciesResponse { policies })))
}

#[utoipa::path(
    post,
    path = "/policies/validate",
    tag = "policies",
    request_body = ValidatePolicyRequest,
    responses(
        (status = 200, description = "Analysis result", body = ValidatePolicyResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn validate_policy(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ValidatePolicyRequest>,
) -> ServerResult<(StatusCode, Json<ValidatePolicyResponse>)> {
    let _auth = require_realm_auth(&state, auth)?;
    let _ = parse_kind(&request.kind)?;
    let analysis = analyze_policy_source(
        request.when.as_deref(),
        &request.expression,
        &PolicyFunctions::default(),
    );
    Ok((
        StatusCode::OK,
        Json(ValidatePolicyResponse {
            valid: analysis.valid,
            errors: analysis.errors,
            referenced_variables: analysis.referenced_variables,
            unknown_variables: analysis.unknown_variables,
            unknown_functions: analysis.unknown_functions,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/policies/dry-run",
    tag = "policies",
    request_body = DryRunRequest,
    responses(
        (status = 200, description = "Evaluation result", body = DryRunResponse),
        (status = 400, description = "Invalid candidate policies", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn dry_run_policy(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<DryRunRequest>,
) -> ServerResult<(StatusCode, Json<DryRunResponse>)> {
    let _auth = require_realm_auth(&state, auth)?;

    let policy_request = PolicyRequest {
        path: request.path.clone(),
        permission: request.permission.clone(),
        user: request.user.clone().unwrap_or_default(),
        operation: request.operation.clone().unwrap_or_else(|| "rest".to_string()),
        params: request.params.clone().unwrap_or_default(),
        headers: request.headers.clone().unwrap_or_default(),
        body: request.body.clone(),
    };

    let scopes = collect_dry_run_scopes(&state, &request).await?;

    let functions = PolicyFunctions::default();
    let mut trace = Vec::new();
    let mut response = DryRunResponse {
        denied: false,
        matched_scope: None,
        policy_name: None,
        reason: None,
        trace: Vec::new(),
    };
    for (label, policies) in scopes {
        let set = CompiledPolicySet::compile(&policies)
            .map_err(|error| ServerError::BadRequestMessage(error.reason))?;
        let traced = set.evaluate_traced(&policy_request, &functions);
        for entry in traced.trace {
            trace.push(ScopedTraceEntry {
                scope: label.clone(),
                entry,
            });
        }
        if let PolicyDecision::Denied { name, reason, .. } = traced.decision {
            response.denied = true;
            response.matched_scope = Some(label);
            response.policy_name = Some(name);
            response.reason = Some(reason);
            break;
        }
    }
    response.trace = trace;
    Ok((StatusCode::OK, Json(response)))
}

/// Resolves the ordered (scope, policies) list a dry run evaluates: ad hoc
/// candidates when given, otherwise the requested stored scope. Candidate
/// expressions are size- and compile-checked before use (S10).
async fn collect_dry_run_scopes(
    state: &ServerState,
    request: &DryRunRequest,
) -> ServerResult<Vec<(String, Vec<RequestPolicy>)>> {
    if let Some(candidates) = &request.candidate_policies {
        let policies = candidates
            .iter()
            .map(to_request_policy)
            .collect::<ServerResult<Vec<_>>>()?;
        validate_policy_set(&policies).map_err(ServerError::BadRequestMessage)?;
        return Ok(vec![("candidate".to_string(), policies)]);
    }
    let scope = request.scope.as_deref().unwrap_or("realm");
    match scope {
        "realm" => Ok(vec![("realm".to_string(), realm_policies(state).await?)]),
        "group" => {
            let group_id = request
                .group_id
                .as_deref()
                .ok_or(ServerError::BadRequest)
                .and_then(parse_group_id)?;
            Ok(vec![(
                format!("group({group_id})"),
                group_policies(state, group_id).await?,
            )])
        }
        "effective" => {
            let mut scopes = vec![("realm".to_string(), realm_policies(state).await?)];
            if let Some(group_id) = request.group_id.as_deref() {
                let group_id = parse_group_id(group_id)?;
                scopes.push((
                    format!("group({group_id})"),
                    group_policies(state, group_id).await?,
                ));
            }
            Ok(scopes)
        }
        other => Err(ServerError::BadRequestMessage(format!(
            "unknown scope `{other}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use ulid::Ulid;

    struct Fixture {
        _dir: tempfile::TempDir,
        state: Arc<ServerState>,
        admin: AuthContext,
        actor: Actor,
        realm_id: RealmId,
    }

    async fn setup() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[31u8; 32])
                .verifying_key()
                .to_bytes(),
        );
        let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let admin_id = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: admin_id,
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "policies".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: actor.clone(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();
        let state = Arc::new(
            ServerState::new(
                context,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );
        Fixture {
            _dir: dir,
            state,
            admin: AuthContext {
                user_id: admin_id,
                realm_id,
                path_restrictions: None,
            },
            actor,
            realm_id,
        }
    }

    fn body(expression: &str) -> SetPoliciesRequest {
        SetPoliciesRequest {
            policies: vec![PolicyBody {
                policy_id: None,
                name: "no-group-writes".to_string(),
                kind: "deny".to_string(),
                when: None,
                expression: expression.to_string(),
                enabled: true,
            }],
            expected_hash: None,
        }
    }

    #[tokio::test]
    async fn stores_and_enforces() {
        // The stored set is served back and denies a previously allowed write.
        let fx = setup().await;
        let denied_path = format!("/{}/admin/roles/example", fx.realm_id);

        let (_, Json(stored)) = set_realm_policies(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(body(
                "permission == 'write' && path.contains('/admin/roles')",
            )),
        )
        .await
        .unwrap();
        assert_eq!(stored.policies.len(), 1);
        assert!(!stored.set_hash.is_empty());

        let (_, Json(read)) =
            get_realm_policies(State(fx.state.clone()), Extension(Some(fx.admin.clone())))
                .await
                .unwrap();
        assert_eq!(read.policies.len(), 1);

        let denied = ensure_permission(&fx.state, &fx.admin, denied_path, Permission::WRITE).await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn stale_expected_hash_conflicts() {
        let fx = setup().await;
        let mut request = body("permission == 'write'");
        request.expected_hash = Some("deadbeef".to_string());
        let result = set_realm_policies(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(request),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Conflict(_))));
    }

    #[tokio::test]
    async fn group_crud_authz_boundary() {
        // The owning group's config admin may set the group set; a stranger cannot.
        let fx = setup().await;
        let (group, _) = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: fx.actor.clone(),
                display_name: "policy group".to_string(),
                owner_cap: None,
            }),
            &fx.state.get_ctx(),
        )
        .await
        .unwrap();

        let (_, Json(stored)) = set_group_policies(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Path(group.group_id.to_string()),
            Json(body("permission == 'write'")),
        )
        .await
        .unwrap();
        assert_eq!(stored.policies.len(), 1);

        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([77; 16]), fx.realm_id),
            realm_id: fx.realm_id,
            path_restrictions: None,
        };
        let result = set_group_policies(
            State(fx.state.clone()),
            Extension(Some(stranger)),
            Path(group.group_id.to_string()),
            Json(body("true")),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn validate_reports_unknowns() {
        let fx = setup().await;
        let (_, Json(result)) = validate_policy(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(ValidatePolicyRequest {
                kind: "deny".to_string(),
                when: None,
                expression: "mystery(body.kind) && unknown_var".to_string(),
            }),
        )
        .await
        .unwrap();
        assert!(result.valid);
        assert!(result.unknown_variables.contains(&"unknown_var".to_string()));
        assert!(result.unknown_functions.contains(&"mystery".to_string()));
    }

    #[tokio::test]
    async fn dry_run_traces_candidates() {
        let fx = setup().await;
        let (_, Json(run)) = dry_run_policy(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(DryRunRequest {
                path: "/r/g/x/data/y".to_string(),
                permission: "write".to_string(),
                user: None,
                operation: None,
                params: None,
                headers: None,
                body: None,
                candidate_policies: Some(vec![PolicyBody {
                    policy_id: None,
                    name: "dry-run".to_string(),
                    kind: "deny".to_string(),
                    when: None,
                    expression: "permission == 'write'".to_string(),
                    enabled: true,
                }]),
                scope: None,
                group_id: None,
            }),
        )
        .await
        .unwrap();
        assert!(run.denied);
        assert_eq!(run.policy_name.as_deref(), Some("dry-run"));
        assert_eq!(run.trace.len(), 1);
    }

    #[tokio::test]
    async fn dry_run_rejects_oversized_expression() {
        // S10: a candidate expression above the policy limit is refused before compile.
        let fx = setup().await;
        let result = dry_run_policy(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(DryRunRequest {
                path: "/r/g/x/data/y".to_string(),
                permission: "write".to_string(),
                user: None,
                operation: None,
                params: None,
                headers: None,
                body: None,
                candidate_policies: Some(vec![PolicyBody {
                    policy_id: None,
                    name: "huge".to_string(),
                    kind: "deny".to_string(),
                    when: None,
                    expression: "x".repeat(5000),
                    enabled: true,
                }]),
                scope: None,
                group_id: None,
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
    }

    #[tokio::test]
    async fn realm_write_deny_covers_every_route() {
        // A realm `permission == "write"` deny must block write, delete, and
        // admin routes the admin could otherwise reach, while reads still pass.
        let fx = setup().await;
        let (_, Json(_stored)) = set_realm_policies(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(body("permission == 'write'")),
        )
        .await
        .unwrap();

        let realm = fx.realm_id;
        let write_routes = [
            format!("/{realm}/admin/roles/example"),
            format!("/{realm}/admin/config"),
            format!("/{realm}/admin/onboarding"),
        ];
        for path in write_routes {
            let denied =
                ensure_permission(&fx.state, &fx.admin, path.clone(), Permission::WRITE).await;
            assert!(
                matches!(denied, Err(ServerError::Forbidden)),
                "write not blocked on {path}"
            );
        }
        // A read on the same admin path is unaffected by the write deny.
        ensure_permission(
            &fx.state,
            &fx.admin,
            format!("/{realm}/admin/config"),
            Permission::READ,
        )
        .await
        .unwrap();

        // Anonymous callers are denied outright on a write route.
        let anonymous = AuthContext::anonymous(realm);
        let denied = ensure_permission(
            &fx.state,
            &anonymous,
            format!("/{realm}/admin/config"),
            Permission::WRITE,
        )
        .await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn requires_admin() {
        // A non-admin realm member cannot replace the policy set.
        let fx = setup().await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([77; 16]), fx.realm_id),
            realm_id: fx.realm_id,
            path_restrictions: None,
        };
        let result = set_realm_policies(
            State(fx.state.clone()),
            Extension(Some(stranger)),
            Json(body("true")),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }
}
