use crate::auth::{ensure_permission, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::groups::refuse_group_edit;
use crate::server_state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyDecision, PolicyKind, PolicyRequest, PolicySession, PolicyTraceEntry,
    RequestPolicy, analyze_policy_source, policy_set_hash, validate_policy_set,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_operations::driver::drive;
use aruna_operations::groups::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::groups::set_policies::{SetGroupConfig, SetGroupError, SetGroupOperation};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::realm::set_policies::{
    SetPoliciesConfig, SetPoliciesError, SetPoliciesOperation,
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
    tags((name = "access/policies", description = "Deny-only CEL request policies")),
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
    /// `operation`, `params`, `headers`, `body`, `request.session`.
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
    /// Hypothetical session exposed as `request.session`.
    #[serde(default)]
    pub session: Option<PolicySessionInput>,
    /// Ad hoc policies to try; when absent the requested scope is evaluated.
    #[serde(default)]
    pub candidate_policies: Option<Vec<PolicyBody>>,
    /// `realm` (default), `group`, or `effective` when no candidates are given.
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PolicySessionInput {
    pub sid: String,
    pub kind: String,
    #[serde(default)]
    pub label: String,
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
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(config) => Ok(config.request_policies),
        Err(aruna_operations::realm::get_config::GetConfigError::DocumentNotFound) => {
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
            aruna_operations::groups::get_group::GetGroupError::GroupNotFound
            | aruna_operations::groups::get_group::GetGroupError::DocNotFound,
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
    path = "/access/policies/realm",
    tag = "access/policies",
    summary = "Read the realm's request policy set",
    description = r#"Returns the stored realm request policies in the order enforcement evaluates them.

**Authentication**: realm bearer token with READ on the realm configuration path.

**Behavior**
- `set_hash` is the content address of the set, to pass as `expected_hash` on a later write.
- Request policies only ever narrow access: a deny policy rejects a request its expression matches
  and a require policy rejects one it does not match, and neither grants anything the caller's roles
  do not already allow.
- This is a node-local read of a replicated document, so a realm configuration that has not arrived
  here yet reads as an empty set rather than a missing resource."#,
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
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
    path = "/access/policies/realm",
    tag = "access/policies",
    summary = "Replace the realm's request policy set",
    description = r#"Replaces the realm's request policy set wholesale with the submitted list.

**Authentication**: realm bearer token with WRITE on the realm configuration path. A management
node serves it, and every other node relays the call to one.

**Behavior**
- Policies missing from the request are removed, an entry without a `policy_id` is given a fresh
  one, and the stored order is the evaluation order.
- When `expected_hash` is sent it is compared inside the write transaction, and a set changed in the
  meantime is rejected without writing.
- The new set takes effect here on commit and reaches the rest of the realm afterwards as a single
  last-writer-wins value, so other nodes keep enforcing their previous set until it arrives, and
  concurrent writes on two nodes resolve to one of them rather than merging.

**Limits** (all checked before anything is stored; a failure names the offending policy)
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
            description = "The set as stored, with generated ids filled in and the new content address for the next `expected_hash`",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not write the realm configuration", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "The stored set no longer matches `expected_hash` and nothing was written; re-read the set and retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable for the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_realm_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SetPoliciesRequest>,
) -> ServerResult<(StatusCode, Json<PoliciesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        &state,
        &auth,
        format!("/{}/admin/config", state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;

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
        SetPoliciesOperation::new(SetPoliciesConfig {
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
        SetPoliciesError::InvalidPolicies { reason } => ServerError::BadRequestMessage(reason),
        SetPoliciesError::Unauthorized | SetPoliciesError::NotManagementNode => {
            ServerError::Forbidden
        }
        SetPoliciesError::ConfigMissing => ServerError::NotFound,
        SetPoliciesError::StaleHash => {
            ServerError::Conflict("stored realm policy set changed".to_string())
        }
        SetPoliciesError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
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
    path = "/access/policies/group/{group_id}",
    tag = "access/policies",
    summary = "Read a group's request policy set",
    description = r#"Returns a group's request policies in the order enforcement evaluates them.

**Authentication**: realm bearer token with READ on the group's administrative configuration path,
which the group's built-in `user` and `admin` roles both grant.

**Behavior**
- `set_hash` is the content address of the set, to pass as `expected_hash` on a later write.
- A group's policies are evaluated after the realm ones and, like them, can only narrow access.
- This is a node-local read of the group's authorization document: a group carrying no policies and
  one whose document has not arrived here yet both read as an empty set."#,
    params(("group_id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "The group's stored policy set and its content address",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
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
    path = "/access/policies/group/{group_id}",
    tag = "access/policies",
    summary = "Replace a group's request policy set",
    description = r#"Replaces a group's request policy set wholesale with the submitted list.

**Authentication**: realm bearer token with WRITE on the group's configuration path.

**Behavior**
- Policies missing from the request are removed, an entry without a `policy_id` is given a fresh
  one, and the stored order is the evaluation order.
- When `expected_hash` is sent it is compared inside the write transaction, and a set changed in the
  meantime is rejected without writing.
- The set takes effect here on commit and rides the group's authorization document to the rest of
  the realm afterwards as a single last-writer-wins value.
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
            description = "The set as stored, with generated ids filled in and the new content address for the next `expected_hash`",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller may not write this group's configuration", body = ErrorResponse),
        (status = 404, description = "This node holds no authorization document for the group", body = ErrorResponse),
        (status = 409, description = "The stored set no longer matches `expected_hash` and nothing was written, or this node is a device where group changes are made through the realm", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted; retryable", body = ErrorResponse)
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
    refuse_group_edit(&state).await?;
    // Request policies live at this boundary; the operation only checks roles.
    require_group_admin(&state, &auth, group_id).await?;

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
        SetGroupOperation::new(SetGroupConfig {
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
        SetGroupError::InvalidPolicies { reason } => ServerError::BadRequestMessage(reason),
        SetGroupError::Unauthorized => ServerError::Forbidden,
        SetGroupError::GroupMissing => ServerError::NotFound,
        SetGroupError::StaleHash => {
            ServerError::Conflict("stored group policy set changed".to_string())
        }
        SetGroupError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((StatusCode::OK, Json(policies_response(&document.policies))))
}

#[utoipa::path(
    get,
    path = "/access/policies/effective",
    tag = "access/policies",
    summary = "List the effective policies for a scope",
    description = r#"Returns the realm policies followed by a group's, in the order enforcement walks them.

**Authentication**: realm bearer token with READ on the realm configuration path; naming a group
additionally requires READ on that group's configuration path.

**Behavior**
- Each entry is labelled with the scope it came from, so an inherited rule is distinguishable from a
  group-local one.
- Evaluation stops at the first policy that denies, so a later entry only applies while every
  earlier one passes.
- The merged view is derived, not stored: it carries no content address, and changes are made
  through the per-scope routes."#,
    params(("group_id" = Option<String>, Query, description = "Group id as a 26-character ULID whose policies are appended after the realm ones; omit it to list realm policies only")),
    responses(
        (
            status = 200,
            description = "Realm policies followed by the requested group's, each tagged with its scope",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
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
    path = "/access/policies/validate",
    tag = "access/policies",
    summary = "Compile-check a candidate policy expression",
    description = r#"Parses a candidate policy guard and expression and reports whether they compile.

**Authentication**: realm bearer token with READ on the realm configuration path, which confines
compiling caller-supplied expressions to policy authors.

**Behavior**
- Nothing is stored and no request is evaluated: the guard and expression are only parsed.
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
            description = "Compilation result for the candidate, with the names it references and the ones enforcement does not provide",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
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
    let analysis = analyze_policy_source(request.when.as_deref(), &request.expression);
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
    path = "/access/policies/dryrun",
    tag = "access/policies",
    summary = "Evaluate policies against a hypothetical request",
    description = r#"Evaluates policies against a hypothetical request and reports the decision they would reach.

**Authentication**: realm bearer token with READ on the realm configuration path, or WRITE on the
named group's configuration path when `group_id` is sent; each stored scope is additionally checked
as if it were read directly.

**Behavior**
- Nothing is stored and no real request is authorized or denied by the call.
- Either the ad hoc `candidate_policies` are evaluated, size- and compile-checked first, or, when
  none are given, the stored set named by `scope`.
- Within a scope the policies run in stored order, disabled ones are skipped, a false guard skips
  its policy, a deny matches when its expression is true and a require matches when it is not, and
  the first match ends evaluation.
- An expression that errors or does not return a boolean also denies, so a broken policy fails
  closed.
- Session-aware policies use `request.session.sid`, `request.session.kind` and
  `request.session.label`; an absent session exposes empty values."#,
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
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
        session: request.session.as_ref().map(|session| PolicySession {
            sid: session.sid.clone(),
            kind: session.kind.clone(),
            label: session.label.clone(),
        }),
    };

    let scopes = dry_run_scopes(&state, &auth, &request).await?;

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
        let traced = set.evaluate_traced(&policy_request);
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
#[path = "policies_tests.rs"]
mod tests;
