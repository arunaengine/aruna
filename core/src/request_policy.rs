//! CEL request policies. A policy narrows what the authorization layer already
//! allowed: a `Deny` policy denies when its expression is `true`, a `Require`
//! policy denies unless its expression is `true`. An optional `when` guard
//! decides applicability. Anything that fails to compile, fails to evaluate, or
//! yields a non-boolean denies (fail-closed).
//!
//! Policy evaluation is pure. The caller assembles a [`PolicyRequest`]; the
//! engine never performs I/O and never reads, buffers, or delays a streaming
//! body. Body-content policies only ever run for operations whose handler
//! already holds the full parsed body.

use cel_interpreter::{Context, Program, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use ulid::Ulid;

/// Maximum number of policies one scope may carry.
pub const MAX_POLICIES_PER_SCOPE: usize = 64;
/// Maximum byte length of a single policy expression or guard.
pub const MAX_POLICY_EXPRESSION_BYTES: usize = 4 * 1024;

/// Request variables a policy expression may reference.
pub const KNOWN_POLICY_VARIABLES: &[&str] = &[
    "path",
    "permission",
    "user",
    "anonymous",
    "operation",
    "params",
    "headers",
    "body",
];

/// Built-in CEL functions and macros always available to a policy expression.
const KNOWN_POLICY_FUNCTIONS: &[&str] = &[
    "contains",
    "size",
    "max",
    "min",
    "startsWith",
    "endsWith",
    "string",
    "bytes",
    "double",
    "int",
    "uint",
    "matches",
    "has",
    "all",
    "exists",
    "exists_one",
    "map",
    "filter",
    "type",
    "dyn",
    "duration",
    "timestamp",
    "getFullYear",
    "getMonth",
    "getDayOfYear",
    "getDayOfMonth",
    "getDate",
    "getDayOfWeek",
    "getHours",
    "getMinutes",
    "getSeconds",
    "getMilliseconds",
];

/// Whether a policy denies on a match or requires a match to allow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyKind {
    /// Deny the request when the expression evaluates to `true`.
    #[default]
    Deny,
    /// Deny the request unless the expression evaluates to `true`.
    Require,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestPolicy {
    pub policy_id: Ulid,
    pub name: String,
    #[serde(default)]
    pub kind: PolicyKind,
    /// Optional applicability guard; the rule applies when it is absent or
    /// evaluates to `true`, and a guard that errors leaves the rule applicable.
    #[serde(default)]
    pub when: Option<String>,
    /// CEL expression over the request variables.
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
    /// Operation identifier, e.g. `metadata.create`, `s3.PutObject`, `rest`.
    pub operation: String,
    /// Allowlisted request parameters; the last value wins for a repeated key.
    pub params: BTreeMap<String, String>,
    /// Allowlisted, lowercased request headers.
    pub headers: BTreeMap<String, String>,
    /// Already-parsed request body; `None` becomes the CEL `null`.
    pub body: Option<serde_json::Value>,
}

impl PolicyRequest {
    /// Builds a request that carries only the authorization context, leaving the
    /// operation generic and the parameters, headers, and body empty.
    pub fn basic(path: String, permission: String, user: String) -> Self {
        Self {
            path,
            permission,
            user,
            operation: "rest".to_string(),
            params: BTreeMap::new(),
            headers: BTreeMap::new(),
            body: None,
        }
    }
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

/// A registered set of pure CEL helper functions. Providers capture pre-loaded
/// immutable data only, so registration stays free of I/O. This is the seam a
/// future SHACL provider slots into without touching the engine.
pub trait PolicyFunctionProvider: Send + Sync {
    fn names(&self) -> Vec<&'static str>;
    fn register(&self, context: &mut Context);
}

#[derive(Clone, Default)]
pub struct PolicyFunctions {
    providers: Vec<Arc<dyn PolicyFunctionProvider>>,
}

impl PolicyFunctions {
    pub fn new(providers: Vec<Arc<dyn PolicyFunctionProvider>>) -> Self {
        Self { providers }
    }

    /// Names contributed by the registered providers, for reference-checking.
    pub fn function_names(&self) -> BTreeSet<&'static str> {
        self.providers.iter().flat_map(|p| p.names()).collect()
    }

    fn register(&self, context: &mut Context) {
        for provider in &self.providers {
            provider.register(context);
        }
    }
}

/// A single compiled policy: the guard and expression are parsed once.
pub struct CompiledPolicy {
    pub policy_id: Ulid,
    pub name: String,
    pub kind: PolicyKind,
    pub enabled: bool,
    when: Option<Program>,
    program: Program,
}

/// A compiled, ready-to-evaluate policy set. `needs_body`/`needs_params` let the
/// caller skip the per-request cost of exposing variables no enabled policy
/// references.
pub struct CompiledPolicySet {
    policies: Vec<CompiledPolicy>,
    pub needs_body: bool,
    pub needs_params: bool,
}

/// The offending policy when a set fails to compile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyCompileError {
    pub policy_id: Ulid,
    pub name: String,
    pub reason: String,
}

/// One policy's evaluation outcome against a prepared context.
enum Outcome {
    Pass,
    Deny(String),
    Error(String),
}

/// A dry-run result value for one policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyResult {
    Passed,
    Denied,
    SkippedDisabled,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolicyTraceEntry {
    pub policy_id: Ulid,
    pub name: String,
    pub kind: PolicyKind,
    pub applicable: bool,
    pub result: PolicyResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// A decision paired with the per-policy trace produced by [`CompiledPolicySet::evaluate_traced`].
pub struct TracedDecision {
    pub decision: PolicyDecision,
    pub trace: Vec<PolicyTraceEntry>,
}

impl CompiledPolicy {
    fn is_applicable(&self, context: &Context) -> bool {
        match &self.when {
            None => true,
            Some(program) => !matches!(program.execute(context), Ok(Value::Bool(false))),
        }
    }

    fn outcome(&self, context: &Context) -> Outcome {
        match self.program.execute(context) {
            Ok(Value::Bool(value)) => {
                let deny = match self.kind {
                    PolicyKind::Deny => value,
                    PolicyKind::Require => !value,
                };
                if deny {
                    Outcome::Deny(self.match_reason())
                } else {
                    Outcome::Pass
                }
            }
            Ok(other) => Outcome::Error(format!(
                "expression returned {:?} instead of a boolean",
                other.type_of()
            )),
            Err(error) => Outcome::Error(format!("evaluation error: {error}")),
        }
    }

    fn match_reason(&self) -> String {
        match self.kind {
            PolicyKind::Deny => "policy matched".to_string(),
            PolicyKind::Require => "required condition not met".to_string(),
        }
    }

    fn denied(&self, reason: String) -> PolicyDecision {
        PolicyDecision::Denied {
            policy_id: self.policy_id,
            name: self.name.clone(),
            reason,
        }
    }
}

impl CompiledPolicySet {
    /// Compiles a policy set. A compile failure fails the whole build so
    /// enforcement stays fail-closed.
    pub fn compile(policies: &[RequestPolicy]) -> Result<Self, PolicyCompileError> {
        let mut compiled = Vec::with_capacity(policies.len());
        let mut needs_body = false;
        let mut needs_params = false;
        for policy in policies {
            let program = compile_program(policy, &policy.expression)?;
            let when = match &policy.when {
                Some(source) => Some(compile_program(policy, source)?),
                None => None,
            };
            if policy.enabled {
                for referenced in std::iter::once(&program).chain(when.iter()) {
                    let references = referenced.references();
                    needs_body |= references.has_variable("body");
                    needs_params |=
                        references.has_variable("params") || references.has_variable("headers");
                }
            }
            compiled.push(CompiledPolicy {
                policy_id: policy.policy_id,
                name: policy.name.clone(),
                kind: policy.kind,
                enabled: policy.enabled,
                when,
                program,
            });
        }
        Ok(Self {
            policies: compiled,
            needs_body,
            needs_params,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    fn build_context(
        &self,
        request: &PolicyRequest,
        functions: &PolicyFunctions,
    ) -> Context<'static> {
        let mut context = Context::default();
        functions.register(&mut context);
        context.add_variable_from_value("path", request.path.clone());
        context.add_variable_from_value("permission", request.permission.clone());
        context.add_variable_from_value("user", request.user.clone());
        context.add_variable_from_value("anonymous", request.user.is_empty());
        context.add_variable_from_value("operation", request.operation.clone());
        if self.needs_params {
            let _ = context.add_variable("params", &request.params);
            let _ = context.add_variable("headers", &request.headers);
        }
        if self.needs_body {
            let body = request
                .body
                .as_ref()
                .and_then(|value| cel_interpreter::to_value(value).ok())
                .unwrap_or(Value::Null);
            context.add_variable_from_value("body", body);
        }
        context
    }

    /// Evaluates the set against a request; the first deny wins.
    pub fn evaluate(&self, request: &PolicyRequest, functions: &PolicyFunctions) -> PolicyDecision {
        let context = self.build_context(request, functions);
        for policy in &self.policies {
            if !policy.enabled || !policy.is_applicable(&context) {
                continue;
            }
            match policy.outcome(&context) {
                Outcome::Pass => {}
                Outcome::Deny(reason) | Outcome::Error(reason) => return policy.denied(reason),
            }
        }
        PolicyDecision::Allowed
    }

    /// Evaluates the set and records why each policy passed, denied, or was
    /// skipped, stopping at the first deny.
    pub fn evaluate_traced(
        &self,
        request: &PolicyRequest,
        functions: &PolicyFunctions,
    ) -> TracedDecision {
        let context = self.build_context(request, functions);
        let mut trace = Vec::with_capacity(self.policies.len());
        let mut decision = PolicyDecision::Allowed;
        for policy in &self.policies {
            if !policy.enabled {
                trace.push(trace_entry(
                    policy,
                    false,
                    PolicyResult::SkippedDisabled,
                    None,
                ));
                continue;
            }
            if !policy.is_applicable(&context) {
                trace.push(trace_entry(policy, false, PolicyResult::Passed, None));
                continue;
            }
            match policy.outcome(&context) {
                Outcome::Pass => {
                    trace.push(trace_entry(policy, true, PolicyResult::Passed, None));
                }
                Outcome::Deny(reason) => {
                    trace.push(trace_entry(
                        policy,
                        true,
                        PolicyResult::Denied,
                        Some(reason.clone()),
                    ));
                    decision = policy.denied(reason);
                    break;
                }
                Outcome::Error(reason) => {
                    trace.push(trace_entry(
                        policy,
                        true,
                        PolicyResult::Error,
                        Some(reason.clone()),
                    ));
                    decision = policy.denied(reason);
                    break;
                }
            }
        }
        TracedDecision { decision, trace }
    }
}

fn compile_program(policy: &RequestPolicy, source: &str) -> Result<Program, PolicyCompileError> {
    Program::compile(source).map_err(|error| PolicyCompileError {
        policy_id: policy.policy_id,
        name: policy.name.clone(),
        reason: error.to_string(),
    })
}

fn trace_entry(
    policy: &CompiledPolicy,
    applicable: bool,
    result: PolicyResult,
    detail: Option<String>,
) -> PolicyTraceEntry {
    PolicyTraceEntry {
        policy_id: policy.policy_id,
        name: policy.name.clone(),
        kind: policy.kind,
        applicable,
        result,
        detail,
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
        if let Some(guard) = &policy.when {
            validate_expression(guard)
                .map_err(|error| format!("policy `{}` guard: {error}", policy.name))?;
        }
    }
    Ok(())
}

/// A pure compile-and-reference report for the validate endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolicyAnalysis {
    pub valid: bool,
    pub errors: Vec<String>,
    pub referenced_variables: Vec<String>,
    pub unknown_variables: Vec<String>,
    pub unknown_functions: Vec<String>,
}

/// Compiles a candidate guard and expression and reports referenced variables
/// and functions, flagging any that are unknown. Pure; performs no I/O.
pub fn analyze_policy_source(
    when: Option<&str>,
    expression: &str,
    functions: &PolicyFunctions,
) -> PolicyAnalysis {
    let mut errors = Vec::new();
    let mut variables = BTreeSet::new();
    let mut unknown_functions = BTreeSet::new();
    let registered = functions.function_names();

    for (label, source) in [("guard", when), ("expression", Some(expression))] {
        let Some(source) = source else { continue };
        if source.len() > MAX_POLICY_EXPRESSION_BYTES {
            errors.push(format!(
                "{label} exceeds {MAX_POLICY_EXPRESSION_BYTES} bytes"
            ));
            continue;
        }
        match Program::compile(source) {
            Ok(program) => {
                let references = program.references();
                for variable in references.variables() {
                    variables.insert(variable.to_string());
                }
                for function in references.functions() {
                    if !KNOWN_POLICY_FUNCTIONS.contains(&function) && !registered.contains(function)
                    {
                        unknown_functions.insert(function.to_string());
                    }
                }
            }
            Err(error) => errors.push(format!("{label}: {error}")),
        }
    }

    let unknown_variables = variables
        .iter()
        .filter(|variable| !KNOWN_POLICY_VARIABLES.contains(&variable.as_str()))
        .cloned()
        .collect();
    PolicyAnalysis {
        valid: errors.is_empty(),
        errors,
        referenced_variables: variables.into_iter().collect(),
        unknown_variables,
        unknown_functions: unknown_functions.into_iter().collect(),
    }
}

/// Evaluates a policy set by compiling it first; a compile failure denies with
/// the offending policy. Retained for tests and simple call sites; enforcement
/// uses a cached [`CompiledPolicySet`].
pub fn evaluate_policies(policies: &[RequestPolicy], request: &PolicyRequest) -> PolicyDecision {
    match CompiledPolicySet::compile(policies) {
        Ok(set) => set.evaluate(request, &PolicyFunctions::default()),
        Err(error) => PolicyDecision::Denied {
            policy_id: error.policy_id,
            name: error.name,
            reason: format!("compile error: {}", error.reason),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([1u8; 16]),
            name: "test".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    fn request(path: &str, permission: &str, user: &str) -> PolicyRequest {
        PolicyRequest::basic(path.to_string(), permission.to_string(), user.to_string())
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
    }

    #[test]
    fn require_denies_unless_true() {
        // A require policy inverts: false denies, true allows.
        let mut require = policy("permission == 'read'");
        require.kind = PolicyKind::Require;
        assert!(evaluate_policies(&[require.clone()], &request("/p", "write", "u")).is_denied());
        assert_eq!(
            evaluate_policies(&[require], &request("/p", "read", "u")),
            PolicyDecision::Allowed
        );
    }

    #[test]
    fn guard_scopes_applicability() {
        // The guard decides applicability; a false guard skips the rule.
        let mut guarded = policy("true");
        guarded.when = Some("permission == 'write'".to_string());
        assert!(evaluate_policies(&[guarded.clone()], &request("/p", "write", "u")).is_denied());
        assert_eq!(
            evaluate_policies(&[guarded], &request("/p", "read", "u")),
            PolicyDecision::Allowed
        );
    }

    #[test]
    fn guard_error_stays_applicable() {
        // A guard that errors leaves the rule applicable (fail-closed).
        let mut guarded = policy("true");
        guarded.when = Some("missing_var".to_string());
        assert!(evaluate_policies(&[guarded], &request("/p", "read", "u")).is_denied());
    }

    #[test]
    fn fails_closed() {
        // Runtime errors and non-boolean results deny for both kinds.
        for expression in ["missing_var == 1", "'a string'"] {
            let policies = [policy(expression)];
            assert!(
                evaluate_policies(&policies, &request("/p", "read", "u")).is_denied(),
                "{expression}"
            );
        }
        let mut require = policy("missing_var == 1");
        require.kind = PolicyKind::Require;
        assert!(evaluate_policies(&[require], &request("/p", "read", "u")).is_denied());
    }

    #[test]
    fn compile_error_denies() {
        assert!(
            evaluate_policies(&[policy("path.startsWith(")], &request("/p", "read", "u"))
                .is_denied()
        );
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
        let policies = [policy("anonymous")];
        assert!(evaluate_policies(&policies, &request("/p", "read", "")).is_denied());
        assert_eq!(
            evaluate_policies(&policies, &request("/p", "read", "u")),
            PolicyDecision::Allowed
        );
    }

    #[test]
    fn exposes_operation_params_headers() {
        let mut req = request("/p", "write", "u");
        req.operation = "s3.PutObject".to_string();
        req.params.insert("group_id".to_string(), "g1".to_string());
        req.headers
            .insert("content-type".to_string(), "text/plain".to_string());
        assert!(evaluate_policies(&[policy("operation == 's3.PutObject'")], &req).is_denied());
        assert!(evaluate_policies(&[policy("params.group_id == 'g1'")], &req).is_denied());
        assert!(
            evaluate_policies(&[policy("headers['content-type'] == 'text/plain'")], &req)
                .is_denied()
        );
    }

    #[test]
    fn exposes_body_and_null() {
        let mut req = request("/p", "write", "u");
        req.body = Some(serde_json::json!({"kind": "Dataset"}));
        assert!(evaluate_policies(&[policy("body.kind == 'Dataset'")], &req).is_denied());
        // Absent body is exposed as CEL null.
        let empty = request("/p", "write", "u");
        assert!(evaluate_policies(&[policy("body == null")], &empty).is_denied());
    }

    #[test]
    fn skips_body_conversion_when_unreferenced() {
        // No enabled policy references body, so the flag stays false.
        let set = CompiledPolicySet::compile(&[policy("permission == 'read'")]).unwrap();
        assert!(!set.needs_body);
        let with_body = CompiledPolicySet::compile(&[policy("body == null")]).unwrap();
        assert!(with_body.needs_body);
    }

    #[test]
    fn traces_each_policy() {
        let mut disabled = policy("true");
        disabled.name = "disabled".to_string();
        disabled.enabled = false;
        let deny = policy("permission == 'write'");
        let set = CompiledPolicySet::compile(&[disabled, deny]).unwrap();
        let traced = set.evaluate_traced(&request("/p", "write", "u"), &PolicyFunctions::default());
        assert!(traced.decision.is_denied());
        assert_eq!(traced.trace.len(), 2);
        assert_eq!(traced.trace[0].result, PolicyResult::SkippedDisabled);
        assert_eq!(traced.trace[1].result, PolicyResult::Denied);
    }

    #[test]
    fn analysis_flags_unknowns() {
        let analysis = analyze_policy_source(
            Some("permission == 'write'"),
            "mystery(body.kind) && unknown_var",
            &PolicyFunctions::default(),
        );
        assert!(analysis.valid);
        assert!(
            analysis
                .unknown_variables
                .contains(&"unknown_var".to_string())
        );
        assert!(analysis.unknown_functions.contains(&"mystery".to_string()));
        assert!(analysis.referenced_variables.contains(&"body".to_string()));
    }

    #[test]
    fn analysis_reports_compile_error() {
        let analysis = analyze_policy_source(None, "path.startsWith(", &PolicyFunctions::default());
        assert!(!analysis.valid);
        assert!(!analysis.errors.is_empty());
    }

    #[test]
    fn validates_limits() {
        assert!(validate_expression("true").is_ok());
        assert!(validate_expression("path.startsWith(").is_err());
        assert!(validate_expression(&"x".repeat(5000)).is_err());

        let set = vec![policy("true"); MAX_POLICIES_PER_SCOPE + 1];
        assert!(validate_policy_set(&set).is_err());
        assert!(validate_policy_set(&[policy("true")]).is_ok());

        let mut bad_guard = policy("true");
        bad_guard.when = Some("path.startsWith(".to_string());
        assert!(validate_policy_set(&[bad_guard]).is_err());
    }
}
