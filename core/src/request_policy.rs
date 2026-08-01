//! Deny-only CEL request policies. A policy can only ever narrow what the
//! authorization layer already allowed: any enabled expression that evaluates
//! to `true` denies the request, and a policy that fails to compile or to
//! evaluate denies as well (fail-closed).

use cel_interpreter::{Context, Program, Value};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Maximum number of policies one scope may carry.
pub const MAX_POLICIES_PER_SCOPE: usize = 64;
/// Maximum byte length of a single policy expression.
pub const MAX_POLICY_EXPRESSION_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestPolicy {
    pub policy_id: Ulid,
    pub name: String,
    /// CEL expression over the request variables; `true` denies the request.
    pub expression: String,
    pub enabled: bool,
}

/// The request attributes a policy expression may reference.
#[derive(Clone, Debug)]
pub struct PolicyRequest {
    /// Canonical permission path of the target resource.
    pub path: String,
    /// Required permission, `read` or `write`.
    pub permission: String,
    /// Caller's user id, empty for anonymous callers.
    pub user: String,
    /// Request plane, `rest` or `s3`.
    pub plane: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    Allowed,
    Denied {
        policy_id: Ulid,
        name: String,
        reason: String,
    },
}

impl PolicyDecision {
    pub fn is_denied(&self) -> bool {
        matches!(self, PolicyDecision::Denied { .. })
    }
}

/// Compile-checks one expression without evaluating it.
pub fn validate_expression(expression: &str) -> Result<(), String> {
    if expression.len() > MAX_POLICY_EXPRESSION_BYTES {
        return Err(format!(
            "expression exceeds {MAX_POLICY_EXPRESSION_BYTES} bytes"
        ));
    }
    Program::compile(expression)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Set-level limits enforced at policy administration time.
pub fn validate_policy_set(policies: &[RequestPolicy]) -> Result<(), String> {
    if policies.len() > MAX_POLICIES_PER_SCOPE {
        return Err(format!(
            "more than {MAX_POLICIES_PER_SCOPE} policies in one scope"
        ));
    }
    for policy in policies {
        validate_expression(&policy.expression)
            .map_err(|error| format!("policy `{}`: {error}", policy.name))?;
    }
    Ok(())
}

fn policy_context(request: &PolicyRequest) -> Context<'_> {
    let mut context = Context::default();
    context.add_variable_from_value("path", request.path.clone());
    context.add_variable_from_value("permission", request.permission.clone());
    context.add_variable_from_value("user", request.user.clone());
    context.add_variable_from_value("anonymous", request.user.is_empty());
    context.add_variable_from_value("plane", request.plane.clone());
    context
}

fn denied(policy: &RequestPolicy, reason: impl Into<String>) -> PolicyDecision {
    PolicyDecision::Denied {
        policy_id: policy.policy_id,
        name: policy.name.clone(),
        reason: reason.into(),
    }
}

/// Evaluates the enabled policies of one scope against a request. The first
/// deny wins; disabled policies are skipped; a policy that cannot be compiled,
/// cannot be evaluated, or yields a non-boolean denies (fail-closed).
pub fn evaluate_policies(policies: &[RequestPolicy], request: &PolicyRequest) -> PolicyDecision {
    let context = policy_context(request);
    for policy in policies {
        if !policy.enabled {
            continue;
        }
        let program = match Program::compile(&policy.expression) {
            Ok(program) => program,
            Err(error) => return denied(policy, format!("compile error: {error}")),
        };
        match program.execute(&context) {
            Ok(Value::Bool(false)) => {}
            Ok(Value::Bool(true)) => return denied(policy, "policy matched"),
            Ok(other) => {
                return denied(
                    policy,
                    format!(
                        "expression returned {:?} instead of a boolean",
                        other.type_of()
                    ),
                );
            }
            Err(error) => return denied(policy, format!("evaluation error: {error}")),
        }
    }
    PolicyDecision::Allowed
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_POLICIES_PER_SCOPE, PolicyDecision, PolicyRequest, RequestPolicy, evaluate_policies,
        validate_expression, validate_policy_set,
    };
    use ulid::Ulid;

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([1u8; 16]),
            name: "test".to_string(),
            expression: expression.to_string(),
            enabled: true,
        }
    }

    fn request(path: &str, permission: &str, user: &str) -> PolicyRequest {
        PolicyRequest {
            path: path.to_string(),
            permission: permission.to_string(),
            user: user.to_string(),
            plane: "rest".to_string(),
        }
    }

    #[test]
    fn denies_on_match() {
        let policies = [policy(
            "path.startsWith('/realm/g/abc/') && permission == 'write'",
        )];
        assert!(
            evaluate_policies(&policies, &request("/realm/g/abc/data/x", "write", "u")).is_denied()
        );
        assert_eq!(
            evaluate_policies(&policies, &request("/realm/g/abc/data/x", "read", "u")),
            PolicyDecision::Allowed
        );
        assert_eq!(
            evaluate_policies(&policies, &request("/realm/g/other/data/x", "write", "u")),
            PolicyDecision::Allowed
        );
    }

    #[test]
    fn fails_closed() {
        // Compile errors, runtime errors and non-boolean results all deny.
        for expression in ["path.startsWith(", "missing_var == 1", "'a string'"] {
            let policies = [policy(expression)];
            assert!(
                evaluate_policies(&policies, &request("/p", "read", "u")).is_denied(),
                "{expression}"
            );
        }
    }

    #[test]
    fn skips_disabled() {
        let mut disabled = policy("true");
        disabled.enabled = false;
        assert_eq!(
            evaluate_policies(&[disabled], &request("/p", "read", "u")),
            PolicyDecision::Allowed
        );
    }

    #[test]
    fn exposes_anonymous() {
        let policies = [policy("anonymous && plane == 's3'")];
        let mut anonymous = request("/p", "read", "");
        anonymous.plane = "s3".to_string();
        assert!(evaluate_policies(&policies, &anonymous).is_denied());
        assert_eq!(
            evaluate_policies(&policies, &request("/p", "read", "u")),
            PolicyDecision::Allowed
        );
    }

    #[test]
    fn validates_limits() {
        assert!(validate_expression("true").is_ok());
        assert!(validate_expression("path.startsWith(").is_err());
        assert!(validate_expression(&"x".repeat(5000)).is_err());

        let set = vec![policy("true"); MAX_POLICIES_PER_SCOPE + 1];
        assert!(validate_policy_set(&set).is_err());
        assert!(validate_policy_set(&[policy("true")]).is_ok());
    }
}
