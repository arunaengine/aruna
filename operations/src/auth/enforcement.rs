//! Enforces CEL request policies after authorization allows an action and before it runs.
//! Any compile, evaluation or policy read failure denies the request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::PolicyEnforcementError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::document_repository::{GetGroupConfig, GetGroupError, GetGroupOperation};
use crate::driver::{DriverContext, drive};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyCompileError, PolicyDecision, PolicyRequest, RequestPolicy,
    policy_set_hash,
};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key, KeySpace, TxnId, Value};
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::warn;
use ulid::Ulid;

/// Compiled policy sets are content-addressed, so a bounded LRU without a TTL or
/// invalidation protocol is enough: any change mints a new key and stale sets
/// age out.
const POLICY_CACHE_CAPACITY: usize = 256;
const POLICY_BULK_LIMIT: usize = 1024;

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

impl std::fmt::Debug for PolicyEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyEvaluator")
            .field("group", &self.group.is_some())
            .finish_non_exhaustive()
    }
}

/// Compiled sets are shared through the content-addressed cache, so the same sets are one `Arc`.
impl PartialEq for PolicyEvaluator {
    fn eq(&self, other: &Self) -> bool {
        let same_group = match (&self.group, &other.group) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (left, right) => left.is_none() && right.is_none(),
        };
        Arc::ptr_eq(&self.realm, &other.realm) && same_group
    }
}

impl From<IoError> for PolicyEnforcementError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// Reads the realm configuration and, when a group is named, the group and its authorization
/// document in one batch read, then compiles both policy sets.
#[derive(Debug, PartialEq)]
pub struct PolicyLoadOperation {
    realm_id: RealmId,
    group_id: Option<GroupId>,
}

impl PolicyLoadOperation {
    pub fn new(realm_id: RealmId, group_id: Option<GroupId>) -> Self {
        Self { realm_id, group_id }
    }

    /// The policy sets that govern `request`: the realm's, and the group's its path names.
    pub fn for_request(realm_id: RealmId, request: &PolicyRequest) -> Self {
        Self::new(realm_id, group_from_path(&request.path))
    }
}

/// The rows a policy load reads: the realm config, then the group and its authorization
/// document when a group is named. A parent can fold them into its own transaction.
pub(crate) fn policy_reads(realm_id: RealmId, group_id: Option<GroupId>) -> Vec<(KeySpace, Key)> {
    let config = DocumentTarget::RealmConfig { realm_id };
    let mut reads = vec![crate::document_repository::read_row(&config)];
    if let Some(group_id) = group_id {
        let key = Key::from(group_id.to_bytes().to_vec());
        reads.push((GROUP_KEYSPACE.to_string(), key.clone()));
        reads.push((AUTH_KEYSPACE.to_string(), key));
    }
    reads
}

/// The evaluator from the rows [`policy_reads`] named, answered in that order. Missing state
/// fails closed.
pub(crate) fn read_policies(
    realm_id: RealmId,
    group_id: Option<GroupId>,
    values: Vec<Option<Value>>,
) -> Result<PolicyEvaluator, PolicyEnforcementError> {
    let mut values = values.into_iter();
    let config = values.next().flatten().ok_or_else(realm_unavailable)?;
    let config = RealmConfigDocument::from_bytes(&config)
        .map_err(|error| PolicyEnforcementError::Unavailable(error.to_string()))?;
    let realm = compile_scope(&config.request_policies, "realm")?;
    let group = match group_id {
        Some(_) => {
            let (Some(group), Some(auth)) = (values.next().flatten(), values.next().flatten())
            else {
                return Err(group_unavailable());
            };
            Some(group_policies(realm_id, &group, &auth)?)
        }
        None => None,
    };
    Ok(PolicyEvaluator { realm, group })
}

/// Compiles a group's policy set from its stored group and authorization rows.
fn group_policies(
    realm_id: RealmId,
    group: &[u8],
    auth: &[u8],
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    let unavailable = |error: aruna_core::errors::ConversionError| {
        PolicyEnforcementError::Unavailable(error.to_string())
    };
    let group = Group::from_bytes(group).map_err(unavailable)?;
    if group.realm_id != realm_id {
        return Err(PolicyEnforcementError::Unavailable(
            "group belongs to another realm".to_string(),
        ));
    }
    let auth = GroupAuthorizationDocument::from_bytes(auth).map_err(unavailable)?;
    compile_scope(&auth.policies, "group")
}

impl Operation for PolicyLoadOperation {
    type Output = PolicyEvaluator;
    type Error = PolicyEnforcementError;

    fn first(&mut self) -> Result<Next<PolicyEvaluator>, PolicyEnforcementError> {
        let reads = policy_reads(self.realm_id, self.group_id);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PolicyEvaluator>, PolicyEnforcementError> {
        let IoResult::Values(values) = result else {
            return Err(PolicyEnforcementError::Unavailable(format!(
                "expected the policy rows, got {}",
                result.label()
            )));
        };
        read_policies(self.realm_id, self.group_id, values).map(Next::Done)
    }
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
        drive(PolicyLoadOperation::new(realm_id, group_id), context).await
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
    /// Whether an enabled policy decides by the request path, so a decision at one path says
    /// nothing about the paths below it.
    pub fn reads_path(&self) -> bool {
        self.realm.needs_path || self.group.as_ref().is_some_and(|group| group.needs_path)
    }

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
    match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => compile_scope(&config.request_policies, "realm"),
        Err(GetConfigError::DocumentNotFound) => Err(realm_unavailable()),
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
        Err(GetGroupError::GroupNotFound | GetGroupError::DocNotFound) => Err(group_unavailable()),
        Err(error) => Err(PolicyEnforcementError::Unavailable(error.to_string())),
    }
}

async fn realm_txn_scope(
    context: &DriverContext,
    realm_id: RealmId,
    txn_id: TxnId,
) -> Result<Arc<CompiledPolicySet>, PolicyEnforcementError> {
    match drive(GetConfigOperation::new_with_txn(realm_id, txn_id), context).await {
        Ok(config) => compile_scope(&config.request_policies, "realm"),
        Err(GetConfigError::DocumentNotFound) => Err(realm_unavailable()),
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
        Err(GetGroupError::GroupNotFound | GetGroupError::DocNotFound) => Err(group_unavailable()),
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
    drive(PolicyLoadOperation::for_request(realm_id, request), context)
        .await?
        .evaluate(request)
}

/// Extracts the group id from a canonical `/{realm}/g/{group}/...` path, mirroring
/// the permission-rule parser. Non-group paths carry no group scope.
pub(crate) fn group_from_path(path: &str) -> Option<GroupId> {
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
            graph_handle: None,
            outbound_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn seed_realm(context: &DriverContext, realm_id: RealmId) {
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        let config = aruna_core::structs::identity::realm::RealmConfigDocument::default_for_realm(
            realm_id,
            Vec::new(),
        );
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

    #[test]
    fn loads_one_read() {
        // The realm row alone, or with the group and its authorization row; no realm denies.
        let realm_id = RealmId([3u8; 32]);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let reads = |group_id| match PolicyLoadOperation::new(realm_id, group_id).first() {
            Ok(Next::Call(IoCall::Read(reads))) => reads.len(),
            other => panic!("expected one read, got {other:?}"),
        };
        assert_eq!((reads(None), reads(Some(group_id))), (1, 3));
        let mut operation = PolicyLoadOperation::new(realm_id, None);
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(realm_unavailable())
        );
    }
}
