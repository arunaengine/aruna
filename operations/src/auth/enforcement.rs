//! Enforces CEL request policies after authorization allows an action and before it runs.
//! Any compile, evaluation or policy read failure denies the request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::PolicyEnforcementError;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::request_policy::{
    CompiledPolicySet, PolicyCompileError, PolicyDecision, PolicyRequest, RequestPolicy,
    policy_set_hash,
};
use aruna_core::rows::all_values;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key, KeySpace, Value};
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
    reads.extend(group_id.into_iter().flat_map(group_reads));
    reads
}

/// The group row and its authorization row.
fn group_reads(group_id: GroupId) -> [(KeySpace, Key); 2] {
    let key = Key::from(group_id.to_bytes().to_vec());
    [
        (GROUP_KEYSPACE.to_string(), key.clone()),
        (AUTH_KEYSPACE.to_string(), key),
    ]
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

/// One evaluator per distinct realm and group scope.
pub type PolicyEvaluators = HashMap<(RealmId, GroupId), PolicyEvaluator>;

/// Reads the policy state of many scopes from one snapshot: each realm once, then each group.
/// Any missing or unreadable scope fails the whole load closed.
#[derive(Debug, PartialEq)]
pub struct PolicyBulkOperation {
    plan: BulkPlan,
}

impl PolicyBulkOperation {
    pub fn new(
        groups: impl IntoIterator<Item = (RealmId, GroupId)>,
    ) -> Result<Self, PolicyEnforcementError> {
        Ok(Self {
            plan: bulk_plan(groups)?,
        })
    }

    fn evaluators(
        &self,
        values: Vec<Option<Value>>,
    ) -> Result<PolicyEvaluators, PolicyEnforcementError> {
        let mut values = values.into_iter();
        let mut realms = HashMap::with_capacity(self.plan.realms.len());
        for realm_id in &self.plan.realms {
            let config = values.next().flatten().ok_or_else(realm_unavailable)?;
            let config = RealmConfigDocument::from_bytes(&config)
                .map_err(|error| PolicyEnforcementError::Unavailable(error.to_string()))?;
            realms.insert(*realm_id, compile_scope(&config.request_policies, "realm")?);
        }
        let mut evaluators = HashMap::with_capacity(self.plan.scopes.len());
        for (realm_id, group_id) in &self.plan.scopes {
            let (Some(group), Some(auth)) = (values.next().flatten(), values.next().flatten())
            else {
                return Err(group_unavailable());
            };
            let group = Some(group_policies(*realm_id, &group, &auth)?);
            let realm = realms
                .get(realm_id)
                .cloned()
                .ok_or_else(realm_unavailable)?;
            evaluators.insert((*realm_id, *group_id), PolicyEvaluator { realm, group });
        }
        Ok(evaluators)
    }
}

impl Operation for PolicyBulkOperation {
    type Output = PolicyEvaluators;
    type Error = PolicyEnforcementError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<PolicyEvaluators>, PolicyEnforcementError> {
        let realms = (self.plan.realms.iter()).flat_map(|realm_id| policy_reads(*realm_id, None));
        let groups = (self.plan.scopes.iter()).flat_map(|(_, group_id)| group_reads(*group_id));
        Ok(Next::Call(IoCall::Read(realms.chain(groups).collect())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PolicyEvaluators>, PolicyEnforcementError> {
        self.evaluators(all_values(result)?).map(Next::Done)
    }
}

impl PolicyEvaluator {
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

#[derive(Debug, PartialEq)]
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

    fn actor(realm_id: RealmId) -> aruna_core::structs::identity::auth::Actor {
        aruna_core::structs::identity::auth::Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_bytes([9u8; 16]), realm_id),
            realm_id,
        }
    }

    fn config_row(realm_id: RealmId) -> Option<Value> {
        let config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        Some(config.to_bytes(&actor(realm_id)).unwrap().into())
    }

    /// The group and authorization rows of `group_id`, stored under `realm_id`.
    fn group_rows(realm_id: RealmId, group_id: GroupId) -> [Option<Value>; 2] {
        let actor = actor(realm_id);
        let group = Group {
            display_name: "group".to_string(),
            group_id,
            realm_id,
            roles: Default::default(),
            owner: actor.user_id,
        };
        let auth = GroupAuthorizationDocument::default_group_doc(actor.user_id, realm_id, group_id);
        [
            Some(group.to_bytes(&actor).unwrap().into()),
            Some(auth.to_bytes(&actor).unwrap().into()),
        ]
    }

    #[test]
    fn bulk_reads_once() {
        // A realm shared by two groups is read once, all from one snapshot.
        let realm_id = RealmId([3u8; 32]);
        let (first, second) = (Ulid::from_bytes([4u8; 16]), Ulid::from_bytes([5u8; 16]));
        let mut operation =
            PolicyBulkOperation::new([(realm_id, first), (realm_id, second)]).unwrap();
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        let Ok(Next::Call(IoCall::Read(reads))) = operation.first() else {
            panic!("expected one read");
        };
        assert_eq!(reads.len(), 5);
        let mut values = vec![config_row(realm_id)];
        values.extend(group_rows(realm_id, first));
        values.extend(group_rows(realm_id, second));
        let Ok(Next::Done(evaluators)) = operation.next(IoResult::Values(values)) else {
            panic!("expected the evaluators");
        };
        assert_eq!(evaluators.len(), 2);
        assert!(evaluators[&(realm_id, first)].group.is_some());
    }

    #[test]
    fn bulk_fails_closed() {
        // A missing realm or group row, or a group of another realm, denies the whole load.
        let realm_id = RealmId([3u8; 32]);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let [group, auth] = group_rows(realm_id, group_id);
        let [foreign, foreign_auth] = group_rows(RealmId([8u8; 32]), group_id);
        let cases = [
            (vec![None, group.clone(), auth.clone()], realm_unavailable()),
            (vec![config_row(realm_id), None, auth], group_unavailable()),
            (vec![config_row(realm_id), group, None], group_unavailable()),
            (
                vec![config_row(realm_id), foreign, foreign_auth],
                PolicyEnforcementError::Unavailable("group belongs to another realm".to_string()),
            ),
        ];
        for (values, expected) in cases {
            let mut operation = PolicyBulkOperation::new([(realm_id, group_id)]).unwrap();
            assert_eq!(operation.next(IoResult::Values(values)), Err(expected));
        }
    }

    #[test]
    fn bulk_empty_ends() {
        let mut operation = PolicyBulkOperation::new([]).unwrap();
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(Vec::new()))));
        assert_eq!(
            operation.next(IoResult::Values(Vec::new())),
            Ok(Next::Done(HashMap::new()))
        );
    }
}
