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

fn lock(cache: &PolicyProgramCache) -> std::sync::MutexGuard<'_, LruCache<[u8; 32], Arc<CompiledPolicySet>>> {
    cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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
    match drive(GetRealmConfigOperation::new(realm_id), context).await {
        Ok(config) => evaluate_scope(&config.request_policies, request, "realm")?,
        Err(GetRealmConfigError::DocumentNotFound) => {}
        Err(error) => return Err(PolicyEnforcementError::Unavailable(error.to_string())),
    }
    if let Some(group_id) = group_id_from_path(&request.path) {
        match drive(GetGroupOperation::new(GetGroupConfig { group_id }), context).await {
            Ok((_, auth_doc)) => evaluate_scope(&auth_doc.policies, request, "group")?,
            Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => {}
            Err(error) => return Err(PolicyEnforcementError::Unavailable(error.to_string())),
        }
    }
    Ok(())
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
    segments.next().and_then(|value| Ulid::from_string(value).ok())
}

/// Evaluates one scope's policy set, mapping a compile error, an evaluation
/// error, or a match to a denial.
fn evaluate_scope(
    policies: &[RequestPolicy],
    request: &PolicyRequest,
    scope: &str,
) -> Result<(), PolicyEnforcementError> {
    if policies.is_empty() {
        return Ok(());
    }
    let set = match compiled_set(policies) {
        Ok(set) => set,
        Err(error) => {
            warn!(
                policy_id = %error.policy_id,
                policy = %error.name,
                scope,
                "Policy set failed to compile; denying request"
            );
            return Err(PolicyEnforcementError::Unavailable(format!(
                "policy `{}` failed to compile: {}",
                error.name, error.reason
            )));
        }
    };
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
        let request = PolicyRequest::basic("/p".to_string(), "read".to_string(), "u".to_string());
        let error = evaluate_scope(&[policy("path.startsWith(")], &request, "realm").unwrap_err();
        assert!(matches!(error, PolicyEnforcementError::Unavailable(_)));
    }

    #[test]
    fn match_denies() {
        let request = PolicyRequest::basic("/p".to_string(), "write".to_string(), "u".to_string());
        let error = evaluate_scope(&[policy("permission == 'write'")], &request, "realm").unwrap_err();
        assert!(matches!(error, PolicyEnforcementError::Denied { .. }));
    }
}
