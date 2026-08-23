use crate::auth::{ensure_permission, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyDecision, PolicyFunctions, PolicyKind, PolicyRequest,
    PolicyTraceEntry, RequestPolicy, analyze_policy_source, policy_set_hash, validate_policy_set,
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
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "policies", description = "Deny-only CEL request policies")),
    components(schemas(
        PolicyTraceDoc,
        PolicyKindDoc,
        PolicyResultDoc,
        ScopedTraceEntry
    ))
)]
pub struct PoliciesApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PoliciesApiDoc::openapi())
        .routes(routes!(get_realm_policies, set_realm_policies))
        .routes(routes!(get_group_policies, set_group_policies))
        .routes(routes!(effective_policies))
        .routes(routes!(validate_policy))
        .routes(routes!(dry_run_policy))
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
    pub trace: Vec<ScopedTraceEntry>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ScopedTraceEntry {
    pub scope: String,
    #[serde(flatten)]
    #[schema(value_type = PolicyTraceDoc)]
    pub entry: PolicyTraceEntry,
}

#[derive(Debug, ToSchema)]
pub struct PolicyTraceDoc {
    pub policy_id: String,
    pub name: String,
    /// Response values are `Deny` or `Require`; request policy input is lowercase.
    pub kind: PolicyKindDoc,
    pub applicable: bool,
    pub result: PolicyResultDoc,
    pub detail: Option<String>,
}

#[derive(Debug, ToSchema)]
pub enum PolicyKindDoc {
    Deny,
    Require,
}

#[derive(Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyResultDoc {
    Passed,
    Denied,
    SkippedDisabled,
    Error,
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

/// Decodes a client-supplied `expected_hash` into the 32-byte digest the set
/// operations compare inside their write transaction.
fn parse_expected_hash(hash: &str) -> ServerResult<[u8; 32]> {
    hex::decode(hash)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ServerError::BadRequest)
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
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

async fn realm_policies(
    state: &ServerState,
    auth: &AuthContext,
) -> ServerResult<Vec<RequestPolicy>> {
    require_config_read(state, auth).await?;
    match drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(config) => Ok(config.request_policies),
        Err(aruna_operations::get_realm_config::GetRealmConfigError::DocumentNotFound) => {
            Ok(Vec::new())
        }
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn group_policies(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<Vec<RequestPolicy>> {
    require_group_read(state, auth, group_id).await?;
    match drive(
        GetGroupOperation::new(GetGroupConfig { group_id }),
        &state.get_ctx(),
    )
    .await
    {
        Ok((_, auth_doc)) => Ok(auth_doc.policies),
        Err(
            aruna_operations::get_group::GetGroupError::GroupNotFound
            | aruna_operations::get_group::GetGroupError::AuthDocNotFound,
        ) => Ok(Vec::new()),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

async fn require_group_admin(
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

async fn require_config_read(state: &ServerState, auth: &AuthContext) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        format!("/{}/admin/config", state.get_realm_id()),
        Permission::READ,
    )
    .await
}

async fn require_group_read(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        format!("/{}/g/{}/admin/config", state.get_realm_id(), group_id),
        Permission::READ,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/policies/realm",
    tag = "policies",
    summary = "Read the realm's request policy set",
    description = r#"Returns the stored realm-scoped request policies in the order enforcement evaluates them.

**Authentication**: realm bearer token with READ on the realm configuration path.

**Behavior**
- `set_hash` is the 64-character hexadecimal content address of the set, which a later write can
  pass as `expected_hash`.
- Policies only ever narrow access: a deny policy rejects a request its expression matches and a
  require policy rejects one it does not match, and neither can grant anything the caller's roles do
  not already allow.
- A realm whose configuration has not been written or replicated to this node reads as an empty set
  rather than a missing resource."#,
    responses(
        (
            status = 200,
            description = "The stored realm policy set and its content address",
            body = PoliciesResponse,
            example = json!({
                "policies": [
                    {
                        "policy_id": "01JPOLICY0123456789ABCDEFG",
                        "name": "no-admin-writes",
                        "kind": "deny",
                        "when": null,
                        "expression": "permission == 'write' && path.contains('/admin/')",
                        "enabled": true
                    }
                ],
                "set_hash": "0ce7c556ff2991526058a20c2372cae9b6ea5276638be50197e42f74aa988e18"
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not read the realm configuration", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let policies = realm_policies(&state, &auth).await?;
    Ok((StatusCode::OK, Json(policies_response(&policies))))
}

#[utoipa::path(
    put,
    path = "/policies/realm",
    tag = "policies",
    summary = "Replace the realm's request policy set",
    description = r#"Replaces the realm's request policy set wholesale with the submitted list.

**Authentication**: realm bearer token with WRITE on the realm configuration path.

**Behavior**
- Policies missing from the request are removed, an entry without a `policy_id` is given a fresh
  one, and the stored order is the evaluation order.
- When `expected_hash` is sent it is compared inside the write transaction, and a set changed in the
  meantime is rejected without writing, so re-read the set and reapply.
- The new set takes effect on this node as soon as it commits and reaches the rest of the realm
  afterwards as a single last-writer-wins value, so other nodes keep enforcing their previous set
  until it arrives, and concurrent writes on two nodes resolve to one of them rather than merging.

**Limits** (checked before storing; a failure names the offending policy and stores nothing)
- At most 64 policies per scope.
- At most 4096 bytes per expression or guard.
- Every expression and guard must compile."#,
    request_body(
        content = SetPoliciesRequest,
        description = "The complete realm policy set, optionally guarded by the hash of the set it is expected to replace.",
        example = json!({
            "policies": [
                {
                    "name": "no-admin-writes",
                    "kind": "deny",
                    "when": "operation == 'rest'",
                    "expression": "permission == 'write' && path.contains('/admin/')",
                    "enabled": true
                }
            ],
            "expected_hash": "0ce7c556ff2991526058a20c2372cae9b6ea5276638be50197e42f74aa988e18"
        })
    ),
    responses(
        (
            status = 200,
            description = "The set as stored, with generated ids filled in and the new content address to use as the next expected_hash",
            body = PoliciesResponse,
            example = json!({
                "policies": [
                    {
                        "policy_id": "01JPOLICY0123456789ABCDEFG",
                        "name": "no-admin-writes",
                        "kind": "deny",
                        "when": "operation == 'rest'",
                        "expression": "permission == 'write' && path.contains('/admin/')",
                        "enabled": true
                    }
                ],
                "set_hash": "beef76d4eb9ae51b89f1c27e62159a872a97c26c104ce407023bafa148821158"
            })
        ),
        (status = 400, description = "Unknown policy kind, malformed policy or hash, or a set that breaks the size or compile limits", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not write the realm configuration", body = ErrorResponse),
        (status = 409, description = "The stored set no longer matches expected_hash and nothing was written; re-read the set and retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SetPoliciesRequest>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;

    let policies = request
        .policies
        .iter()
        .map(to_request_policy)
        .collect::<ServerResult<Vec<_>>>()?;

    let expected_hash = request
        .expected_hash
        .as_deref()
        .map(parse_expected_hash)
        .transpose()?;

    let document = drive(
        SetRealmPoliciesOperation::new(SetRealmPoliciesConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            auth_context: auth,
            policies,
            expected_hash,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        SetRealmPoliciesError::InvalidPolicies { reason } => ServerError::BadRequestMessage(reason),
        SetRealmPoliciesError::Unauthorized => ServerError::Forbidden,
        SetRealmPoliciesError::StaleHash => {
            ServerError::Conflict("stored realm policy set changed".to_string())
        }
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
    summary = "Read a group's request policy set",
    description = r#"Returns the group-scoped request policies in the order enforcement evaluates them.

**Authentication**: realm bearer token with READ on the group's administrative configuration path,
which the group's built-in member and admin roles both grant.

**Behavior**
- `set_hash` is the 64-character hexadecimal content address to pass as `expected_hash` on a write.
- Group policies are evaluated after the realm ones and, like them, can only narrow access.
- A group that carries no policies, and one whose authorization document has not replicated to this
  node, both read as an empty set."#,
    params(("group_id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "The stored group policy set and its content address",
            body = PoliciesResponse,
            example = json!({
                "policies": [
                    {
                        "policy_id": "01JPOLICY0123456789ABCDEFG",
                        "name": "read-only-group",
                        "kind": "deny",
                        "when": null,
                        "expression": "permission == 'write'",
                        "enabled": true
                    }
                ],
                "set_hash": "0ce7c556ff2991526058a20c2372cae9b6ea5276638be50197e42f74aa988e18"
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not read this group's configuration", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let policies = group_policies(&state, &auth, group_id).await?;
    Ok((StatusCode::OK, Json(policies_response(&policies))))
}

#[utoipa::path(
    put,
    path = "/policies/group/{group_id}",
    tag = "policies",
    summary = "Replace a group's request policy set",
    description = r#"Replaces a group's request policy set wholesale with the submitted list.

**Authentication**: realm bearer token with WRITE on the group's configuration path.

**Behavior**
- Policies missing from the request are removed, an entry without a `policy_id` is given a fresh
  one, and the stored order is the evaluation order.
- When `expected_hash` is sent it is compared inside the write transaction and a set changed in the
  meantime is rejected without writing.
- The set takes effect on this node as soon as it commits and rides the group's authorization
  document to the rest of the realm afterwards as a single last-writer-wins value.
- Because policies only deny, a group set can restrict the group's own members further but can never
  widen what their roles grant.

**Limits** (the same as for the realm set, all checked before anything is stored)
- At most 64 policies.
- At most 4096 bytes per expression or guard.
- Every expression and guard must compile."#,
    params(("group_id" = String, Path, description = "Group id as a 26-character ULID")),
    request_body(
        content = SetPoliciesRequest,
        description = "The complete group policy set, optionally guarded by the hash of the set it is expected to replace.",
        example = json!({
            "policies": [
                {
                    "name": "read-only-group",
                    "kind": "deny",
                    "expression": "permission == 'write'",
                    "enabled": true
                }
            ],
            "expected_hash": "0ce7c556ff2991526058a20c2372cae9b6ea5276638be50197e42f74aa988e18"
        })
    ),
    responses(
        (
            status = 200,
            description = "The set as stored, with generated ids filled in and the new content address to use as the next expected_hash",
            body = PoliciesResponse,
            example = json!({
                "policies": [
                    {
                        "policy_id": "01JPOLICY0123456789ABCDEFG",
                        "name": "read-only-group",
                        "kind": "deny",
                        "when": null,
                        "expression": "permission == 'write'",
                        "enabled": true
                    }
                ],
                "set_hash": "beef76d4eb9ae51b89f1c27e62159a872a97c26c104ce407023bafa148821158"
            })
        ),
        (status = 400, description = "Malformed group id, unknown policy kind, malformed hash, or a set that breaks the size or compile limits", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not write this group's configuration", body = ErrorResponse),
        (status = 409, description = "The stored set no longer matches expected_hash and nothing was written; re-read the set and retry", body = ErrorResponse)
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

    let policies = request
        .policies
        .iter()
        .map(to_request_policy)
        .collect::<ServerResult<Vec<_>>>()?;

    let expected_hash = request
        .expected_hash
        .as_deref()
        .map(parse_expected_hash)
        .transpose()?;

    let document = drive(
        SetGroupPoliciesOperation::new(SetGroupPoliciesConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            auth_context: auth,
            group_id,
            policies,
            expected_hash,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        SetGroupPoliciesError::InvalidPolicies { reason } => ServerError::BadRequestMessage(reason),
        SetGroupPoliciesError::Unauthorized => ServerError::Forbidden,
        SetGroupPoliciesError::GroupAuthDocNotFound => ServerError::NotFound,
        SetGroupPoliciesError::StaleHash => {
            ServerError::Conflict("stored group policy set changed".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((StatusCode::OK, Json(policies_response(&document.policies))))
}

#[utoipa::path(
    get,
    path = "/policies/effective",
    tag = "policies",
    summary = "List the effective policies for a scope",
    description = r#"Returns the realm policies followed by the group policies, in the order enforcement walks them.

**Authentication**: realm bearer token with READ on the realm configuration path; naming a group
additionally requires READ on that group's configuration path.

**Behavior**
- Each entry is labelled with the scope it came from, so an inherited rule is distinguishable from a
  group-local one.
- Evaluation stops at the first policy that denies, so a later entry in this list only applies while
  every earlier one passes.
- The merged view is derived, not stored: it carries no content address, and changes are made
  through the per-scope endpoints."#,
    params(("group_id" = Option<String>, Query, description = "Group id as a 26-character ULID whose policies are appended after the realm ones; omit it to list realm policies only")),
    responses(
        (
            status = 200,
            description = "Realm policies followed by the requested group's policies, each tagged with its scope",
            body = EffectivePoliciesResponse,
            example = json!({
                "policies": [
                    {
                        "scope": "realm",
                        "policy_id": "01JPOLICY0123456789ABCDEFG",
                        "name": "no-admin-writes",
                        "kind": "deny",
                        "when": null,
                        "expression": "permission == 'write' && path.contains('/admin/')",
                        "enabled": true
                    },
                    {
                        "scope": "group(01JABCDEF0123456789ABCDEFG)",
                        "policy_id": "01JPOLICY1123456789ABCDEFG",
                        "name": "read-only-group",
                        "kind": "deny",
                        "when": null,
                        "expression": "permission == 'write'",
                        "enabled": true
                    }
                ]
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not read the realm or group configuration", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn effective_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<EffectiveQuery>,
) -> ServerResult<(StatusCode, Json<EffectivePoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let mut policies: Vec<ScopedPolicy> = realm_policies(&state, &auth)
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
        policies.extend(
            group_policies(&state, &auth, group_id)
                .await?
                .iter()
                .map(|policy| ScopedPolicy {
                    scope: label.clone(),
                    policy: map_policy(policy),
                }),
        );
    }
    Ok((StatusCode::OK, Json(EffectivePoliciesResponse { policies })))
}

#[utoipa::path(
    post,
    path = "/policies/validate",
    tag = "policies",
    summary = "Compile-check a candidate policy expression",
    description = r#"Parses a candidate policy guard and expression and reports whether they compile.

**Authentication**: realm bearer token with READ on the realm configuration path, because the
endpoint compiles caller-supplied expressions; it is restricted to policy authors for that reason.

**Behavior**
- Nothing is stored and no request is evaluated: the guard and expression are only parsed.
- The report names the request variables they reference plus any variable or function outside the
  set enforcement provides.
- Unknown names are informational and do not by themselves make the candidate invalid, since a
  helper may be registered elsewhere.
- `valid` turns false only when a source fails to compile or exceeds 4096 bytes, and each reason is
  listed in `errors`."#,
    request_body(
        content = ValidatePolicyRequest,
        description = "The candidate expression, its kind, and an optional applicability guard.",
        example = json!({
            "kind": "deny",
            "when": "operation == 'rest'",
            "expression": "permission == 'write' && path.contains('/admin/')"
        })
    ),
    responses(
        (
            status = 200,
            description = "Compilation result for the candidate, with the names it references and the ones that are not known",
            body = ValidatePolicyResponse,
            example = json!({
                "valid": true,
                "errors": [],
                "referenced_variables": [
                    "operation",
                    "path",
                    "permission"
                ],
                "unknown_variables": [],
                "unknown_functions": []
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not read the realm configuration", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn validate_policy(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ValidatePolicyRequest>,
) -> ServerResult<(StatusCode, Json<ValidatePolicyResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    // Validation compiles caller-supplied CEL; restrict it to policy authors.
    require_config_read(&state, &auth).await?;
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
    summary = "Evaluate policies against a hypothetical request",
    description = r#"Evaluates policies against a hypothetical request and reports the decision they would reach.

**Authentication**: realm bearer token; sending a `group_id` requires WRITE on that group's
configuration path, and otherwise READ on the realm configuration path is required, with the stored
scopes each additionally checked as if they were read directly.

**Behavior**
- The call never changes anything: no policy is stored, and no real request is authorized or denied
  by it.
- Either ad hoc `candidate_policies` are evaluated, size- and compile-checked first, or, when none
  are given, the stored set named by `scope`.
- Within a scope the policies run in stored order, disabled ones are skipped, a guard that is false
  skips its policy, a deny matches when its expression is true and a require matches when it is not,
  and the first match ends evaluation.
- An expression that errors or does not return a boolean also denies, so a broken policy fails
  closed.
- The response reports whether the request would be denied, which scope and policy decided it, and a
  trace of every policy considered up to that point."#,
    request_body(
        content = DryRunRequest,
        description = "The request attributes to evaluate, plus either candidate policies to try or the stored scope to evaluate.",
        example = json!({
            "path": "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/reports/q1.csv",
            "permission": "write",
            "user": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
            "operation": "rest",
            "scope": "effective",
            "group_id": "01JABCDEF0123456789ABCDEFG"
        })
    ),
    responses(
        (
            status = 200,
            description = "The decision the policy engine would reach, with the per-policy trace that produced it",
            body = DryRunResponse,
            example = json!({
                "denied": true,
                "matched_scope": "group(01JABCDEF0123456789ABCDEFG)",
                "policy_name": "read-only-group",
                "reason": "policy matched",
                "trace": [
                    {
                        "scope": "realm",
                        "policy_id": "01JPOLICY0123456789ABCDEFG",
                        "name": "no-admin-writes",
                        "kind": "Deny",
                        "applicable": true,
                        "result": "passed"
                    },
                    {
                        "scope": "group(01JABCDEF0123456789ABCDEFG)",
                        "policy_id": "01JPOLICY1123456789ABCDEFG",
                        "name": "read-only-group",
                        "kind": "Deny",
                        "applicable": true,
                        "result": "denied",
                        "detail": "policy matched"
                    }
                ]
            })
        ),
        (status = 400, description = "Malformed group id, unknown scope or policy kind, or candidate policies that break the size or compile limits", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not read the realm configuration or administer the named group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn dry_run_policy(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<DryRunRequest>,
) -> ServerResult<(StatusCode, Json<DryRunResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    // Dry runs compile caller-supplied CEL; restrict them to policy authors.
    match request.group_id.as_deref() {
        Some(group_id) => {
            let group_id = parse_group_id(group_id)?;
            require_group_admin(&state, &auth, group_id).await?;
        }
        None => require_config_read(&state, &auth).await?,
    }

    let policy_request = PolicyRequest {
        path: request.path.clone(),
        permission: request.permission.clone(),
        user: request.user.clone().unwrap_or_default(),
        operation: request
            .operation
            .clone()
            .unwrap_or_else(|| "rest".to_string()),
        params: request.params.clone().unwrap_or_default(),
        headers: request.headers.clone().unwrap_or_default(),
        body: request.body.clone(),
    };

    let scopes = dry_run_scopes(&state, &auth, &request).await?;

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
async fn dry_run_scopes(
    state: &ServerState,
    auth: &AuthContext,
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
        "realm" => Ok(vec![(
            "realm".to_string(),
            realm_policies(state, auth).await?,
        )]),
        "group" => {
            let group_id = request
                .group_id
                .as_deref()
                .ok_or(ServerError::BadRequest)
                .and_then(parse_group_id)?;
            Ok(vec![(
                format!("group({group_id})"),
                group_policies(state, auth, group_id).await?,
            )])
        }
        "effective" => {
            let mut scopes = vec![("realm".to_string(), realm_policies(state, auth).await?)];
            if let Some(group_id) = request.group_id.as_deref() {
                let group_id = parse_group_id(group_id)?;
                scopes.push((
                    format!("group({group_id})"),
                    group_policies(state, auth, group_id).await?,
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
    async fn stale_hash_conflicts() {
        let fx = setup().await;
        let mut request = body("permission == 'write'");
        // A well-formed but stale digest must abort the write transaction as 409.
        request.expected_hash = Some("ff".repeat(32));
        let result = set_realm_policies(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Json(request),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Conflict(_))));
    }

    #[tokio::test]
    async fn group_authz_boundary() {
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
        assert!(
            result
                .unknown_variables
                .contains(&"unknown_var".to_string())
        );
        assert!(result.unknown_functions.contains(&"mystery".to_string()));
    }

    #[tokio::test]
    async fn validate_needs_admin() {
        // Compiling caller-supplied CEL is gated like the dry run: a realm user
        // without config read must not reach the compiler.
        let fx = setup().await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([78; 16]), fx.realm_id),
            realm_id: fx.realm_id,
            path_restrictions: None,
        };
        let result = validate_policy(
            State(fx.state.clone()),
            Extension(Some(stranger)),
            Json(ValidatePolicyRequest {
                kind: "deny".to_string(),
                when: None,
                expression: "true".to_string(),
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn traces_candidates() {
        // A dry run reports the decision of every candidate path it evaluated.
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
        let body = serde_json::to_value(&run).unwrap();
        assert_eq!(body["trace"][0]["kind"], serde_json::json!("Deny"));
    }

    #[tokio::test]
    async fn rejects_oversized_expression() {
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
    async fn deny_covers_routes() {
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
