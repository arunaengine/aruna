//! Enforcement seam for the CEL request policies. Called at the request choke
//! points after authorization allowed the action and before it executes; policy
//! compile and evaluation failures deny (fail-closed), and a policy-state read
//! that fails for any reason other than an absent config also denies.

use crate::driver::{DriverContext, drive};
use crate::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyCompileError, PolicyDecision, PolicyFunctions, PolicyRequest,
    RequestPolicy, policy_set_hash,
};
use aruna_core::structs::RealmId;
use aruna_core::types::GroupId;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// Compiled policy sets are content-addressed, so a bounded LRU without a TTL or
/// invalidation protocol is enough: any change mints a new key and stale sets
/// age out.
const POLICY_CACHE_CAPACITY: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum PolicyEnforcementError {
    #[error("request denied by policy `{name}`: {reason}")]
    Denied { name: String, reason: String },
    /// Policy state could not be read or compiled; fail closed rather than allow.
    #[error("policy state unavailable: {0}")]
    Unavailable(String),
}

type PolicyProgramCache = Mutex<LruCache<[u8; 32], Arc<CompiledPolicySet>>>;

fn program_cache() -> &'static PolicyProgramCache {
    static CACHE: OnceLock<PolicyProgramCache> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(LruCache::new(
            NonZeroUsize::new(POLICY_CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN),
        ))
    })
}

/// Compiles a policy set once and caches it by content address. A compile
/// failure is returned so enforcement stays fail-closed.
fn compiled_set(policies: &[RequestPolicy]) -> Result<Arc<CompiledPolicySet>, PolicyCompileError> {
    let key = policy_set_hash(policies);
    let cache = program_cache();
    if let Some(set) = lock(cache).get(&key).cloned() {
        return Ok(set);
    }
    let set = Arc::new(CompiledPolicySet::compile(policies)?);
    lock(cache).put(key, set.clone());
    Ok(set)
}

fn lock(
    cache: &PolicyProgramCache,
) -> std::sync::MutexGuard<'_, LruCache<[u8; 32], Arc<CompiledPolicySet>>> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A realm and optional group policy set loaded and compiled once so a bulk
/// request can evaluate many candidate paths in memory without re-reading the
/// control-plane state per candidate.
pub struct PolicyEvaluator {
    realm: Option<Arc<CompiledPolicySet>>,
    group: Option<Arc<CompiledPolicySet>>,
}

impl PolicyEvaluator {
    /// Reads and compiles the realm's, and optionally one group's, policy set.
    /// An absent realm config or group document carries no policies; every other
    /// read failure or a compile failure fails closed.
    pub async fn load(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: Option<GroupId>,
    ) -> Result<Self, PolicyEnforcementError> {
        let realm = match drive(GetRealmConfigOperation::new(realm_id), context).await {
            Ok(config) => compile_scope(&config.request_policies, "realm")?,
            Err(GetRealmConfigError::DocumentNotFound) => None,
            Err(error) => return Err(PolicyEnforcementError::Unavailable(error.to_string())),
        };
        let group = match group_id {
            Some(group_id) => {
                match drive(GetGroupOperation::new(GetGroupConfig { group_id }), context).await {
                    Ok((_, auth_doc)) => compile_scope(&auth_doc.policies, "group")?,
                    Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => None,
                    Err(error) => {
                        return Err(PolicyEnforcementError::Unavailable(error.to_string()));
                    }
                }
            }
            None => None,
        };
        Ok(Self { realm, group })
    }

    /// Evaluates the realm then the group scope; either may deny, neither grants.
    pub fn evaluate(&self, request: &PolicyRequest) -> Result<(), PolicyEnforcementError> {
        for (scope, set) in [("realm", &self.realm), ("group", &self.group)] {
            if let Some(set) = set {
                decide(set, request, scope)?;
            }
        }
        Ok(())
    }
}

/// Evaluates the realm's then the group's request policies against one request.
/// An absent realm config or group document carries no policies and allows;
/// every other read failure, a policy that cannot be compiled, and any
/// evaluation error deny. Either scope may deny; neither may grant.
pub async fn enforce_policies(
    context: &DriverContext,
    realm_id: RealmId,
    request: &PolicyRequest,
) -> Result<(), PolicyEnforcementError> {
    let group_id = group_id_from_path(&request.path);
    PolicyEvaluator::load(context, realm_id, group_id)
        .await?
        .evaluate(request)
}

/// Extracts the group id from a canonical `/{realm}/g/{group}/...` path, mirroring
/// the permission-rule parser. Non-group paths carry no group scope.
fn group_id_from_path(path: &str) -> Option<GroupId> {
    let mut segments = path.split('/');
    segments.next();
    segments.next();
    if segments.next() != Some("g") {
        return None;
    }
    segments
        .next()
        .and_then(|value| Ulid::from_string(value).ok())
}

/// Compiles one scope's policy set, mapping a compile failure to a fail-closed
/// error. An empty set needs no compiled program.
fn compile_scope(
    policies: &[RequestPolicy],
    scope: &str,
) -> Result<Option<Arc<CompiledPolicySet>>, PolicyEnforcementError> {
    if policies.is_empty() {
        return Ok(None);
    }
    match compiled_set(policies) {
        Ok(set) => Ok(Some(set)),
        Err(error) => {
            warn!(
                policy_id = %error.policy_id,
                policy = %error.name,
                scope,
                "Policy set failed to compile; denying request"
            );
            Err(PolicyEnforcementError::Unavailable(format!(
                "policy `{}` failed to compile: {}",
                error.name, error.reason
            )))
        }
    }
}

/// Evaluates one compiled scope, mapping a match or an evaluation error to a
/// denial.
fn decide(
    set: &CompiledPolicySet,
    request: &PolicyRequest,
    scope: &str,
) -> Result<(), PolicyEnforcementError> {
    match set.evaluate(request, &PolicyFunctions::default()) {
        PolicyDecision::Allowed => Ok(()),
        PolicyDecision::Denied {
            policy_id,
            name,
            reason,
        } => {
            warn!(
                policy_id = %policy_id,
                policy = %name,
                scope,
                path = %request.path,
                permission = %request.permission,
                "Request denied by policy"
            );
            Err(PolicyEnforcementError::Denied { name, reason })
        }
    }
}

/// Extra request context threaded to a policy at a choke point. Body-content
/// policies only ever run for operations whose handler already holds the full
/// parsed body; the engine never reads or buffers a streaming body.
#[derive(Debug, Clone, Default)]
pub struct PolicyRequestExtras {
    pub operation: String,
    pub params: std::collections::BTreeMap<String, String>,
    pub headers: std::collections::BTreeMap<String, String>,
    pub body: Option<serde_json::Value>,
}

impl PolicyRequestExtras {
    /// Generic REST context with no parameters, headers, or body.
    pub fn rest() -> Self {
        Self {
            operation: "rest".to_string(),
            ..Default::default()
        }
    }

    /// Context carrying only an operation identifier.
    pub fn operation(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            ..Default::default()
        }
    }
}

/// Builds the policy request for one authorized action.
pub fn policy_request(
    path: &str,
    permission: &aruna_core::structs::Permission,
    user: Option<&aruna_core::UserId>,
) -> PolicyRequest {
    PolicyRequest::basic(
        path.to_string(),
        permission_label(permission),
        user.filter(|user| !user.is_nil())
            .map(|user| user.to_string())
            .unwrap_or_default(),
    )
}

/// Builds a policy request that additionally carries operation, parameters,
/// headers, and an already-parsed body.
pub fn policy_request_with(
    path: &str,
    permission: &aruna_core::structs::Permission,
    user: Option<&aruna_core::UserId>,
    extras: PolicyRequestExtras,
) -> PolicyRequest {
    let mut request = policy_request(path, permission, user);
    if !extras.operation.is_empty() {
        request.operation = extras.operation;
    }
    request.params = extras.params;
    request.headers = extras.headers;
    request.body = extras.body;
    request
}

fn permission_label(permission: &aruna_core::structs::Permission) -> String {
    match permission {
        aruna_core::structs::Permission::READ => "read".to_string(),
        aruna_core::structs::Permission::WRITE => "write".to_string(),
        aruna_core::structs::Permission::DENY => "deny".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use ulid::Ulid;

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([5u8; 16]),
            name: "test".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    #[test]
    fn caches_by_content_address() {
        // Identical bytes reuse the same Arc; an edit mints a new entry.
        let policies = [policy("permission == 'write'")];
        let first = compiled_set(&policies).unwrap();
        let second = compiled_set(&policies).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let edited = [policy("permission == 'read'")];
        let third = compiled_set(&edited).unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
    }

    #[test]
    fn compile_failure_is_unavailable() {
        let result = compile_scope(&[policy("path.startsWith(")], "realm");
        assert!(matches!(
            result,
            Err(PolicyEnforcementError::Unavailable(_))
        ));
    }

    #[test]
    fn match_denies() {
        let request = PolicyRequest::basic("/p".to_string(), "write".to_string(), "u".to_string());
        let set = compile_scope(&[policy("permission == 'write'")], "realm")
            .unwrap()
            .unwrap();
        let error = decide(&set, &request, "realm").unwrap_err();
        assert!(matches!(error, PolicyEnforcementError::Denied { .. }));
    }

    #[test]
    fn evaluates_each_member_path() {
        // The DeleteObjects model: one loaded set, each member path decided
        // separately so a path rule can deny one member and allow another.
        let set = compiled_set(&[policy("path.endsWith('/secret')")]).unwrap();
        let denied = PolicyRequest::basic(
            "/r/g/x/data/secret".to_string(),
            "write".to_string(),
            "u".to_string(),
        );
        let allowed = PolicyRequest::basic(
            "/r/g/x/data/public".to_string(),
            "write".to_string(),
            "u".to_string(),
        );
        assert!(decide(&set, &denied, "realm").is_err());
        assert!(decide(&set, &allowed, "realm").is_ok());
    }

    #[test]
    fn extras_thread_operation_and_body() {
        let extras = PolicyRequestExtras::operation("s3.PutObject");
        let request = policy_request_with(
            "/r/g/x/data/o",
            &aruna_core::structs::Permission::WRITE,
            None,
            extras,
        );
        assert_eq!(request.operation, "s3.PutObject");
        assert_eq!(request.permission, "write");
    }
}
