use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::request_policy::{
    PolicyDecision, PolicyRequest, RequestPolicy, evaluate_policies, validate_expression,
};
use aruna_core::structs::{Actor, AuthContext, Permission};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::set_realm_policies::{
    SetRealmPoliciesConfig, SetRealmPoliciesError, SetRealmPoliciesOperation,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "policies", description = "Deny-only CEL request policies")),
    paths(set_realm_policies, effective_policies, validate_policy, dry_run_policy)
)]
pub struct PoliciesApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/policies/realm", put(set_realm_policies))
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
    /// CEL expression over `path`, `permission`, `user`, `anonymous`;
    /// `true` denies the request.
    pub expression: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SetPoliciesRequest {
    pub policies: Vec<PolicyBody>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PoliciesResponse {
    pub policies: Vec<PolicyBody>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ValidatePolicyRequest {
    pub expression: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatePolicyResponse {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
    /// Expression to try; when absent the stored realm set is evaluated.
    #[serde(default)]
    pub expression: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DryRunResponse {
    pub denied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

fn map_policy(policy: &RequestPolicy) -> PolicyBody {
    PolicyBody {
        policy_id: Some(policy.policy_id.to_string()),
        name: policy.name.clone(),
        expression: policy.expression.clone(),
        enabled: policy.enabled,
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

#[utoipa::path(
    put,
    path = "/policies/realm",
    tag = "policies",
    request_body = SetPoliciesRequest,
    responses(
        (status = 200, description = "Stored policy set", body = PoliciesResponse),
        (status = 400, description = "Invalid policy set", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
        .map(|policy| {
            let policy_id = policy
                .policy_id
                .as_deref()
                .map(Ulid::from_str)
                .transpose()
                .map_err(|_| ServerError::BadRequest)?
                .unwrap_or_else(Ulid::generate);
            Ok(RequestPolicy {
                policy_id,
                name: policy.name.clone(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: policy.expression.clone(),
                enabled: policy.enabled,
            })
        })
        .collect::<ServerResult<Vec<_>>>()?;

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
        Json(PoliciesResponse {
            policies: document.request_policies.iter().map(map_policy).collect(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/policies/effective",
    tag = "policies",
    responses(
        (status = 200, description = "Effective policy set", body = PoliciesResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn effective_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let _auth = require_realm_auth(&state, auth)?;
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok((
        StatusCode::OK,
        Json(PoliciesResponse {
            policies: config.request_policies.iter().map(map_policy).collect(),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/policies/validate",
    tag = "policies",
    request_body = ValidatePolicyRequest,
    responses(
        (status = 200, description = "Compile result", body = ValidatePolicyResponse),
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
    let result = validate_expression(&request.expression);
    Ok((
        StatusCode::OK,
        Json(ValidatePolicyResponse {
            valid: result.is_ok(),
            error: result.err(),
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
    let policies = match &request.expression {
        Some(expression) => vec![RequestPolicy {
            policy_id: Ulid::generate(),
            name: "dry-run".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.clone(),
            enabled: true,
        }],
        None => {
            drive(
                GetRealmConfigOperation::new(state.get_realm_id()),
                &state.get_ctx(),
            )
            .await
            .map_err(|error| ServerError::InternalError(error.to_string()))?
            .request_policies
        }
    };
    let decision = evaluate_policies(
        &policies,
        &PolicyRequest::basic(
            request.path,
            request.permission,
            request.user.unwrap_or_default(),
        ),
    );
    let response = match decision {
        PolicyDecision::Allowed => DryRunResponse {
            denied: false,
            policy_name: None,
            reason: None,
        },
        PolicyDecision::Denied { name, reason, .. } => DryRunResponse {
            denied: true,
            policy_name: Some(name),
            reason: Some(reason),
        },
    };
    Ok((StatusCode::OK, Json(response)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use ulid::Ulid;

    struct Fixture {
        _dir: tempfile::TempDir,
        state: Arc<ServerState>,
        admin: AuthContext,
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
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput { actor }),
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
            realm_id,
        }
    }

    fn body(expression: &str) -> SetPoliciesRequest {
        SetPoliciesRequest {
            policies: vec![PolicyBody {
                policy_id: None,
                name: "no-group-writes".to_string(),
                expression: expression.to_string(),
                enabled: true,
            }],
        }
    }

    #[tokio::test]
    async fn stores_and_enforces() {
        // The stored set is served back and denies a previously allowed write.
        let fx = setup().await;
        let denied_path = format!("/{}/admin/roles/example", fx.realm_id);

        // Before any policy the admin write on the admin path passes.
        ensure_permission(
            &fx.state,
            &fx.admin,
            format!("/{}/admin/config", fx.realm_id),
            Permission::WRITE,
        )
        .await
        .unwrap();

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

        let (_, Json(effective)) =
            effective_policies(State(fx.state.clone()), Extension(Some(fx.admin.clone())))
                .await
                .unwrap();
        assert_eq!(effective.policies.len(), 1);

        // Matching writes are now denied for everyone, admin included…
        let denied = ensure_permission(&fx.state, &fx.admin, denied_path, Permission::WRITE).await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
        // …while non-matching requests stay allowed.
        ensure_permission(
            &fx.state,
            &fx.admin,
            format!("/{}/admin/config", fx.realm_id),
            Permission::WRITE,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validates_and_dry_runs() {
        let fx = setup().await;
        let (_, Json(invalid)) = validate_policy(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(ValidatePolicyRequest {
                expression: "path.startsWith(".to_string(),
            }),
        )
        .await
        .unwrap();
        assert!(!invalid.valid);
        assert!(invalid.error.is_some());

        let (_, Json(run)) = dry_run_policy(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(DryRunRequest {
                path: "/r/g/x/data/y".to_string(),
                permission: "write".to_string(),
                user: None,
                expression: Some("permission == 'write'".to_string()),
            }),
        )
        .await
        .unwrap();
        assert!(run.denied);
        assert_eq!(run.policy_name.as_deref(), Some("dry-run"));
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
