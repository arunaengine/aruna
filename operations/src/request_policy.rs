//! CEL policy enforcement runs after authorization allows an action and before
//! execution; compile/evaluation failures, policy-state read failures, and
//! absent policy state all deny.

use crate::driver::{DriverContext, drive};
use crate::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyCompileError, PolicyDecision, PolicyRequest, PolicySession,
    RequestPolicy, policy_set_hash,
};
use aruna_core::structs::{AuthContext, RealmId};
use aruna_core::types::{GroupId, TxnId};
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// Compiled policy sets are content-addressed, so a bounded LRU without a TTL or
/// invalidation protocol is enough: any change mints a new key and stale sets
/// age out.
const POLICY_CACHE_CAPACITY: usize = 256;
const POLICY_BULK_LIMIT: usize = 1024;

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
    realm: Arc<CompiledPolicySet>,
    /// Absent only when the request carries no group scope; a named group whose
    /// state cannot be read fails closed instead.
    group: Option<Arc<CompiledPolicySet>>,
}

impl PolicyEvaluator {
    /// Reads and compiles the realm's, and optionally one group's, policy set.
    /// Missing policy state, a read failure, and a compile failure all fail
    /// closed; the group scope is skipped only when the request names no group.
    pub async fn load(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: Option<GroupId>,
    ) -> Result<Self, PolicyEnforcementError> {
        let realm = realm_scope(context, realm_id).await?;
        let group = match group_id {
            Some(group_id) => Some(group_scope(context, realm_id, group_id).await?),
            None => None,
        };
        Ok(Self { realm, group })
    }

    pub async fn load_with_txn(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> Result<Self, PolicyEnforcementError> {
        let realm = realm_txn_scope(context, realm_id, txn_id).await?;
        let group = Some(group_txn_scope(context, realm_id, group_id, txn_id).await?);
        Ok(Self { realm, group })
    }

    /// Loads one evaluator per distinct realm/group scope. Realm policy state is
    /// read once per realm, then each group is read once and evaluated in memory.
    pub async fn load_bulk(
        context: &DriverContext,
        groups: impl IntoIterator<Item = (RealmId, GroupId)>,
    ) -> Result<HashMap<(RealmId, GroupId), PolicyEvaluator>, PolicyEnforcementError> {
        let plan = bulk_plan(groups)?;
        let mut realms = HashMap::with_capacity(plan.realms.len());
        for realm_id in plan.realms {
            realms.insert(realm_id, realm_scope(context, realm_id).await?);
        }

        let mut evaluators = HashMap::with_capacity(plan.scopes.len());
        for (realm_id, group_id) in plan.scopes {
            let group = group_scope(context, realm_id, group_id).await?;
            let Some(realm) = realms.get(&realm_id).cloned() else {
                return Err(PolicyEnforcementError::Unavailable(
                    "realm policy snapshot is unavailable".to_string(),
                ));
            };
            evaluators.insert(
                (realm_id, group_id),
                Self {
                    realm,
                    group: Some(group),
                },
            );
        }
        Ok(evaluators)
    }

    /// Evaluates the realm then the group scope; either may deny, neither grants.
    pub fn evaluate(&self, request: &PolicyRequest) -> Result<(), PolicyEnforcementError> {
        decide(&self.realm, request, "realm")?;
        if let Some(group) = &self.group {
            decide(group, request, "group")?;
        }
        Ok(())
    }
}

struct BulkPlan {
    scopes: Vec<(RealmId, GroupId)>,
    realms: Vec<RealmId>,
}

fn bulk_plan(
    groups: impl IntoIterator<Item = (RealmId, GroupId)>,
) -> Result<BulkPlan, PolicyEnforcementError> {
    let mut scopes = Vec::new();
    let mut realms = Vec::new();
    let mut seen_scopes = HashSet::new();
    let mut seen_realms = HashSet::new();

    for (index, scope) in groups.into_iter().enumerate() {
        if index >= POLICY_BULK_LIMIT {
            return Err(PolicyEnforcementError::Unavailable(
                "bulk policy candidate limit exceeded".to_string(),
            ));
        }
        if seen_scopes.insert(scope) {
            if seen_realms.insert(scope.0) {
                realms.push(scope.0);
            }
            scopes.push(scope);
        }
    }
    Ok(BulkPlan { scopes, realms })
}

/// Missing realm policy state denies: the realm's policies cannot be shown to
/// be satisfied, so the request must not proceed.
fn realm_unavailable() -> PolicyEnforcementError {
    PolicyEnforcementError::Unavailable("realm policy state is unavailable".to_string())
}

/// A named group whose document or auth document is missing denies; a request
/// without a group scope never reaches this loader.
fn group_unavailable() -> PolicyEnforcementError {
    PolicyEnforcementError::Unavailable("group policy state is unavailable".to_string())
}

async fn realm_scope(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    match drive(GetRealmConfigOperation::new(realm_id), context).await {
        Ok(config) => compile_scope(&config.request_policies, "realm"),
        Err(GetRealmConfigError::DocumentNotFound) => Err(realm_unavailable()),
        Err(error) => Err(PolicyEnforcementError::Unavailable(error.to_string())),
    }
}

async fn group_scope(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    match drive(GetGroupOperation::new(GetGroupConfig { group_id }), context).await {
        Ok((group, auth_doc)) => {
            if group.realm_id != realm_id {
                return Err(PolicyEnforcementError::Unavailable(
                    "group belongs to another realm".to_string(),
                ));
            }
            compile_scope(&auth_doc.policies, "group")
        }
        Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => {
            Err(group_unavailable())
        }
        Err(error) => Err(PolicyEnforcementError::Unavailable(error.to_string())),
    }
}

async fn realm_txn_scope(
    context: &DriverContext,
    realm_id: RealmId,
    txn_id: TxnId,
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    match drive(
        GetRealmConfigOperation::new_with_txn(realm_id, txn_id),
        context,
    )
    .await
    {
        Ok(config) => compile_scope(&config.request_policies, "realm"),
        Err(GetRealmConfigError::DocumentNotFound) => Err(realm_unavailable()),
        Err(error) => Err(PolicyEnforcementError::Unavailable(error.to_string())),
    }
}

async fn group_txn_scope(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
    txn_id: TxnId,
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    match drive(
        GetGroupOperation::new_with_txn(GetGroupConfig { group_id }, txn_id),
        context,
    )
    .await
    {
        Ok((group, auth_doc)) => {
            if group.realm_id != realm_id {
                return Err(PolicyEnforcementError::Unavailable(
                    "group belongs to another realm".to_string(),
                ));
            }
            compile_scope(&auth_doc.policies, "group")
        }
        Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => {
            Err(group_unavailable())
        }
        Err(error) => Err(PolicyEnforcementError::Unavailable(error.to_string())),
    }
}

/// Evaluates realm then group policies for one request. Missing state and read,
/// compile, or evaluation failures deny. Either scope may deny; neither may
/// grant.
pub async fn enforce_policies(
    context: &DriverContext,
    realm_id: RealmId,
    request: &PolicyRequest,
) -> Result<(), PolicyEnforcementError> {
    let group_id = group_from_path(&request.path);
    PolicyEvaluator::load(context, realm_id, group_id)
        .await?
        .evaluate(request)
}

/// Extracts the group id from a canonical `/{realm}/g/{group}/...` path, mirroring
/// the permission-rule parser. Non-group paths carry no group scope.
fn group_from_path(path: &str) -> Option<GroupId> {
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
/// error. An empty set is represented by an empty compiled program.
fn compile_scope(
    policies: &[RequestPolicy],
    scope: &str,
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    match compiled_set(policies) {
        Ok(set) => Ok(set),
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
    match set.evaluate(request) {
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
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PolicyRequestExtras {
    pub operation: String,
    pub params: std::collections::BTreeMap<String, String>,
    pub headers: std::collections::BTreeMap<String, String>,
    // Encoded as an optional JSON string so non-self-describing wire formats
    // such as postcard can carry the body across the mirror messages.
    #[serde(with = "json_body")]
    pub body: Option<serde_json::Value>,
}

mod json_body {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<serde_json::Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .as_ref()
            .map(serde_json::Value::to_string)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|text| serde_json::from_str(&text).map_err(serde::de::Error::custom))
            .transpose()
    }
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
    auth: Option<&AuthContext>,
    extras: PolicyRequestExtras,
) -> PolicyRequest {
    let mut request = policy_request(path, permission, auth.map(|auth| &auth.user_id));
    if !extras.operation.is_empty() {
        request.operation = extras.operation;
    }
    request.params = extras.params;
    request.headers = extras.headers;
    request.body = extras.body;
    request.session = auth.and_then(|auth| {
        auth.session.as_ref().map(|session| PolicySession {
            sid: session.sid.clone(),
            kind: session.kind.to_string(),
            label: String::new(),
        })
    });
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

    /// The compiled-set cache is process-wide and keyed by content, so a test
    /// that asserts on entry identity must own its policy id: a concurrent test
    /// compiling the same bytes would otherwise replace the entry.
    fn owned_policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([9u8; 16]),
            ..policy(expression)
        }
    }

    #[test]
    fn caches_by_content() {
        // Identical bytes reuse the same Arc; an edit mints a new entry.
        let policies = [owned_policy("permission == 'write'")];
        let first = compiled_set(&policies).unwrap();
        let second = compiled_set(&policies).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let edited = [owned_policy("permission == 'read'")];
        let third = compiled_set(&edited).unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
    }

    #[test]
    fn compile_failure_unavailable() {
        // A policy set that will not compile denies as unavailable, not allows.
        let result = compile_scope(&[policy("path.startsWith(")], "realm");
        assert!(matches!(
            result,
            Err(PolicyEnforcementError::Unavailable(_))
        ));
    }

    #[test]
    fn empty_scopes_present() {
        assert!(compile_scope(&[], "realm").unwrap().is_empty());
        assert!(compile_scope(&[], "group").unwrap().is_empty());
    }

    #[test]
    fn match_denies() {
        let request = PolicyRequest::basic("/p".to_string(), "write".to_string(), "u".to_string());
        let set = compile_scope(&[policy("permission == 'write'")], "realm").unwrap();
        let error = decide(&set, &request, "realm").unwrap_err();
        assert!(matches!(error, PolicyEnforcementError::Denied { .. }));
    }

    #[test]
    fn evaluates_member_paths() {
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
    fn extras_set_operation() {
        // Extras carry the operation name and body into the policy request.
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

    #[test]
    fn extras_survive_postcard() {
        // The mirror messages carry extras over postcard, which cannot decode a
        // bare serde_json::Value; the body must ride as an encoded string.
        let mut params = std::collections::BTreeMap::new();
        params.insert("versionId".to_string(), "1".to_string());
        let extras = PolicyRequestExtras {
            operation: "s3.PutBucketReplication".to_string(),
            params,
            headers: std::collections::BTreeMap::new(),
            body: Some(serde_json::json!({"rule": "deny"})),
        };
        let bytes = postcard::to_allocvec(&extras).unwrap();
        let decoded: PolicyRequestExtras = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.operation, extras.operation);
        assert_eq!(decoded.params, extras.params);
        assert_eq!(decoded.body, extras.body);
    }

    #[test]
    fn dedups_scope_realm() {
        let group_id = Ulid::from_bytes([7u8; 16]);
        let first = RealmId([1u8; 32]);
        let second = RealmId([2u8; 32]);
        let plan = bulk_plan([(first, group_id), (first, group_id), (second, group_id)]).unwrap();

        assert_eq!(plan.scopes, vec![(first, group_id), (second, group_id)]);
        assert_eq!(plan.realms, vec![first, second]);
    }

    #[test]
    fn plans_100_groups() {
        let realm_id = RealmId([3u8; 32]);
        let groups =
            (0..100).map(|index| (realm_id, Ulid::from_bytes((index as u128).to_be_bytes())));
        let plan = bulk_plan(groups).unwrap();

        assert_eq!(plan.scopes.len(), 100);
        assert_eq!(plan.realms.len(), 1);
    }

    #[test]
    fn rejects_bulk_bound() {
        let realm_id = RealmId([4u8; 32]);
        let groups = (0..=POLICY_BULK_LIMIT)
            .map(|index| (realm_id, Ulid::from_bytes((index as u128).to_be_bytes())));
        let result = bulk_plan(groups);

        assert!(matches!(
            result,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "bulk policy candidate limit exceeded"
        ));
    }

    fn context_at(directory: &tempfile::TempDir) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(
                directory.path().to_str().expect("storage path"),
            )
            .expect("storage opens"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn seed_realm(context: &DriverContext, realm_id: RealmId) {
        let actor = aruna_core::structs::Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        let config =
            aruna_core::structs::RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let event = context
            .storage_handle
            .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: aruna_core::types::Key::from(*realm_id.as_bytes()),
                value: config.to_bytes(&actor).expect("config encodes").into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            aruna_core::events::Event::Storage(
                aruna_core::events::StorageEvent::WriteResult { .. }
            )
        ));
    }

    #[tokio::test]
    async fn missing_is_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let context = context_at(&directory);
        let result = PolicyEvaluator::load_bulk(
            &context,
            [(RealmId([5u8; 32]), Ulid::from_bytes([8u8; 16]))],
        )
        .await;

        assert!(matches!(
            result,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "realm policy state is unavailable"
        ));
    }

    #[tokio::test]
    async fn missing_state_denies() {
        // Unreadable realm or group state must deny; only a request that names no
        // group may skip the group scope.
        let directory = tempfile::tempdir().unwrap();
        let context = context_at(&directory);
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::from_bytes([8u8; 16]);

        assert!(matches!(
            PolicyEvaluator::load(&context, realm_id, None).await,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "realm policy state is unavailable"
        ));

        seed_realm(&context, realm_id).await;

        assert!(matches!(
            PolicyEvaluator::load(&context, realm_id, Some(group_id)).await,
            Err(PolicyEnforcementError::Unavailable(message))
                if message == "group policy state is unavailable"
        ));
        let request = PolicyRequest::basic("/r".to_string(), "read".to_string(), String::new());
        assert!(
            PolicyEvaluator::load(&context, realm_id, None)
                .await
                .and_then(|evaluator| evaluator.evaluate(&request))
                .is_ok()
        );
    }
}
