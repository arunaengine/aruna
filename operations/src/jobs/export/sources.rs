//! Finds the readable copies of each File entity of an export: its exact version, the objects
//! that share its content hash and the nodes that hold that content.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::jobs::{JobEffect, JobReport, JobStops};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{ExportReportSource, ExportRoCrateSpec, ReasonCode};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::resolve::{
    CandidateSource, ExportCandidate, LocalSource, ResolveError, ResolveLocalOperation,
    ResolveResult,
};
use super::{ExportEntity, ExportFailure, MAX_LOCAL_CANDIDATES, REMOTE_ATTEMPTS, SourceScope};
use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluator, PolicyEvaluators};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::permission_rules::{
    PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::blob::holders::{GetHoldersError, GetHoldersOperation};
use crate::blob::permission_paths::{MAX_HASH_ALIASES, ResolvePathsError, ResolvePathsOperation};
use crate::jobs::import::{job_call, stops_of};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};

/// What the source lookups of one export run have learned, so each is asked only once.
#[derive(Debug, Default, PartialEq)]
pub(super) struct AliasState {
    policies: BTreeMap<GroupId, PolicyEvaluator>,
    rules: BTreeMap<GroupId, PermissionRules>,
    paths: BTreeSet<(GroupId, String)>,
    keys: BTreeSet<([u8; 32], String, Ulid)>,
    cache: BTreeMap<[u8; 32], Vec<HashIndex>>,
    resolved: BTreeMap<[u8; 32], (Vec<ExportCandidate>, bool)>,
    authorized: BTreeMap<(GroupId, String), bool>,
}

impl AliasState {
    /// The state a resumed export starts from: the aliases its candidates already use.
    pub(super) fn of(entities: &[ExportEntity]) -> Self {
        let mut state = Self::default();
        let candidates = entities.iter().flat_map(|entity| &entity.candidates);
        for candidate in candidates.filter(|it| it.report_source == ExportReportSource::Hash) {
            let CandidateSource::Local {
                group_id,
                permission_path,
                ..
            } = &candidate.source
            else {
                continue;
            };
            state.paths.insert((*group_id, permission_path.clone()));
            if let (Some(hash), Some(version)) =
                (candidate.expected_blake3, candidate.resolved_version)
            {
                state.keys.insert((hash, permission_path.clone(), version));
            }
        }
        state
    }

    pub(super) fn over_limit(&self) -> bool {
        self.paths.len() > MAX_HASH_ALIASES || self.keys.len() > MAX_HASH_ALIASES
    }

    fn missing_policies(&self, groups: impl IntoIterator<Item = GroupId>) -> BTreeSet<GroupId> {
        (groups.into_iter())
            .filter(|group_id| !self.policies.contains_key(group_id))
            .collect()
    }

    /// The groups of local candidates whose object policy is not loaded yet.
    pub(super) fn candidate_groups(&self, entities: &[ExportEntity]) -> BTreeSet<GroupId> {
        let candidates = entities.iter().flat_map(|entity| &entity.candidates);
        self.missing_policies(candidates.filter_map(|candidate| match &candidate.source {
            CandidateSource::Local { group_id, .. } => Some(*group_id),
            _ => None,
        }))
    }

    /// Keeps the evaluators of `pending` from a [`policy_call`] answer.
    pub(super) fn keep_policies(
        &mut self,
        realm_id: RealmId,
        pending: &BTreeSet<GroupId>,
        result: IoResult,
    ) -> Result<(), ExportFailure> {
        let mut loaded = (result.child::<PolicyEvaluators, PolicyEnforcementError>()?)
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
        for group_id in pending {
            let evaluator = loaded
                .remove(&(realm_id, *group_id))
                .ok_or_else(|| ExportFailure::Retryable("object policy unavailable".to_string()))?;
            self.policies.insert(*group_id, evaluator);
        }
        Ok(())
    }
}

/// Loads the object policies of `pending`.
pub(super) fn policy_call(realm_id: RealmId, pending: &BTreeSet<GroupId>) -> IoCall {
    let groups = pending.iter().map(|group_id| (realm_id, *group_id));
    IoCall::child(PolicyBulkOperation::new(groups))
}

/// A stop of the job as the failure that ends the running step.
pub(super) fn stop_failure(stops: JobStops) -> Option<ExportFailure> {
    match (stops.cancelled, stops.shutdown) {
        (true, _) => Some(ExportFailure::Cancelled),
        (false, true) => Some(ExportFailure::Interrupted),
        (false, false) => None,
    }
}

pub(super) fn export_failure(error: ResolveError) -> ExportFailure {
    match error {
        ResolveError::Retryable(message) => ExportFailure::Retryable(message),
        ResolveError::Permanent(message) => ExportFailure::Permanent(message),
    }
}

pub(super) fn cache_aliases(
    alias_cache: &mut BTreeMap<[u8; 32], Vec<HashIndex>>,
    hash: [u8; 32],
    aliases: Vec<HashIndex>,
) -> Result<(), ExportFailure> {
    if aliases.len() > MAX_HASH_ALIASES {
        return Err(ExportFailure::Retryable(
            "hash alias limit exceeded".to_string(),
        ));
    }
    let cached = alias_cache
        .values()
        .try_fold(0usize, |total, aliases| {
            total.checked_add(aliases.len().max(1))
        })
        .ok_or_else(|| ExportFailure::Retryable("export alias limit exceeded".to_string()))?;
    let cost = aliases.len().max(1);
    if cached
        .checked_add(cost)
        .is_none_or(|total| total > MAX_HASH_ALIASES)
    {
        return Err(ExportFailure::Retryable(
            "export alias limit exceeded".to_string(),
        ));
    }
    alias_cache.insert(hash, aliases);
    Ok(())
}

pub(super) fn collect_aliases(
    aliases: &[HashIndex],
    realm_id: RealmId,
    seen: &mut BTreeSet<(GroupId, String)>,
    alias_keys: &mut BTreeSet<([u8; 32], String, Ulid)>,
) -> Result<BTreeMap<(GroupId, String), Vec<HashIndex>>, ExportFailure> {
    if aliases.len() > MAX_HASH_ALIASES {
        return Err(ExportFailure::Retryable(
            "hash alias limit exceeded".to_string(),
        ));
    }
    let mut distinct = BTreeMap::new();
    for alias in aliases.iter().filter(|alias| alias.realm_id == realm_id) {
        let key = (alias.group_id, alias.permission_path());
        if !alias_keys.insert((alias.blake3_hash, key.1.clone(), alias.version_id)) {
            continue;
        }
        distinct
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(alias.clone());
        seen.insert(key);
    }
    if seen.len() > MAX_HASH_ALIASES || alias_keys.len() > MAX_HASH_ALIASES {
        return Err(ExportFailure::Retryable(
            "export alias limit exceeded".to_string(),
        ));
    }
    Ok(distinct)
}

pub(super) fn merge_candidates(
    target: &mut Vec<ExportCandidate>,
    additions: &[ExportCandidate],
    limit: usize,
) {
    if target.len() >= limit {
        return;
    }
    let mut local = BTreeSet::<(String, Ulid)>::new();
    let mut remote = BTreeSet::<(NodeId, [u8; 32])>::new();
    for candidate in target.iter() {
        match &candidate.source {
            CandidateSource::Local {
                permission_path, ..
            } => {
                if let Some(version) = candidate.resolved_version {
                    local.insert((permission_path.clone(), version));
                }
            }
            CandidateSource::RemoteHash { node_id, hash } => {
                remote.insert((*node_id, *hash));
            }
            CandidateSource::RemoteExact { .. } => {}
        }
    }
    for candidate in additions {
        if target.len() >= limit {
            break;
        }
        let insert = match &candidate.source {
            CandidateSource::Local {
                permission_path, ..
            } => candidate
                .resolved_version
                .is_none_or(|version| local.insert((permission_path.clone(), version))),
            CandidateSource::RemoteHash { node_id, hash } => remote.insert((*node_id, *hash)),
            CandidateSource::RemoteExact { .. } => true,
        };
        if insert {
            target.push(candidate.clone());
        }
    }
}

pub(super) fn alias_allowed(
    spec: &ExportRoCrateSpec,
    group_id: GroupId,
    path: &str,
    rules: &BTreeMap<GroupId, PermissionRules>,
    policies: &BTreeMap<GroupId, PolicyEvaluator>,
) -> Result<bool, ExportFailure> {
    let rules = rules
        .get(&group_id)
        .ok_or_else(|| ExportFailure::Retryable("authorization rules unavailable".to_string()))?;
    if !rules.allows(path, &Permission::READ) {
        return Ok(false);
    }
    let evaluator = policies
        .get(&group_id)
        .ok_or_else(|| ExportFailure::Retryable("object policy unavailable".to_string()))?;
    let request = policy_request_with(
        path,
        &Permission::READ,
        Some(&spec.auth_context),
        PolicyRequestExtras::operation("s3.GetObject"),
    );
    match evaluator.evaluate(&request) {
        Ok(()) => Ok(true),
        Err(PolicyEnforcementError::Denied { .. }) => Ok(false),
        Err(PolicyEnforcementError::Unavailable(error)) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
    }
}

pub(super) fn learn_probe_hash(
    entity: &mut ExportEntity,
    candidate_index: usize,
    hash: [u8; 32],
) -> bool {
    let Some(candidate) = entity.candidates.get_mut(candidate_index) else {
        return false;
    };
    let (node_id, realm_id, resolved_version) = match &candidate.source {
        CandidateSource::RemoteExact { node_id, target } if candidate.expected_blake3.is_none() => {
            (*node_id, target.realm_id, candidate.resolved_version)
        }
        _ => return false,
    };
    candidate.expected_blake3 = Some(hash);
    entity.hash = Some(hash);
    entity.hash_realm = Some(realm_id);
    let fallback = ExportCandidate {
        source: CandidateSource::RemoteHash { node_id, hash },
        report_source: ExportReportSource::Hash,
        resolved_version,
        expected_blake3: Some(hash),
    };
    merge_candidates(
        &mut entity.candidates,
        std::slice::from_ref(&fallback),
        REMOTE_ATTEMPTS,
    );
    true
}

/// The bucket of a candidate read on this node.
pub(super) fn local_bucket(candidate: Option<&ExportCandidate>) -> Option<String> {
    match candidate.map(|candidate| &candidate.source) {
        Some(CandidateSource::Local { bucket, .. }) => Some(bucket.clone()),
        _ => None,
    }
}

/// What a hash lookup added to an entity's candidates.
#[derive(Debug, PartialEq)]
pub(super) struct HashFound {
    pub(super) candidates: Vec<ExportCandidate>,
    pub(super) denied: bool,
    /// Holder discovery could not be asked.
    pub(super) unavailable: bool,
    pub(super) aliases: AliasState,
}

#[derive(Debug, PartialEq)]
enum HashStep {
    Paths,
    Rules(GroupId),
    Policies(BTreeSet<GroupId>),
    Alias,
    Holders,
}

/// Adds the local objects with content `hash` the caller may read and, unless the export goes
/// into another realm, the nodes that hold that content.
#[derive(Debug, PartialEq)]
pub(super) struct HashCandidatesOperation {
    scope: SourceScope,
    hash: [u8; 32],
    resolved_version: Option<Ulid>,
    candidates: Vec<ExportCandidate>,
    denied: bool,
    aliases: AliasState,
    distinct: BTreeMap<(GroupId, String), Vec<HashIndex>>,
    rule_groups: VecDeque<GroupId>,
    queue: VecDeque<HashIndex>,
    resolved: Vec<ExportCandidate>,
    alias_denied: bool,
    step: HashStep,
}

type HashTurn = Result<Next<HashFound>, ExportFailure>;

fn lost_cache() -> ExportFailure {
    ExportFailure::Retryable("alias cache unavailable".to_string())
}

impl HashCandidatesOperation {
    pub(super) fn new(
        scope: SourceScope,
        hash: [u8; 32],
        resolved_version: Option<Ulid>,
        candidates: Vec<ExportCandidate>,
        aliases: AliasState,
    ) -> Self {
        Self {
            scope,
            hash,
            resolved_version,
            candidates,
            denied: false,
            aliases,
            distinct: BTreeMap::new(),
            rule_groups: VecDeque::new(),
            queue: VecDeque::new(),
            resolved: Vec::new(),
            alias_denied: false,
            step: HashStep::Paths,
        }
    }

    fn realm_id(&self) -> RealmId {
        self.scope.spec.auth_context.realm_id
    }

    fn done(&mut self, unavailable: bool) -> Next<HashFound> {
        Next::Done(HashFound {
            candidates: std::mem::take(&mut self.candidates),
            denied: self.denied,
            unavailable,
            aliases: std::mem::take(&mut self.aliases),
        })
    }

    fn plan_aliases(&mut self) -> HashTurn {
        let realm_id = self.realm_id();
        let state = &mut self.aliases;
        let aliases = state.cache.get(&self.hash).ok_or_else(lost_cache)?;
        self.distinct = collect_aliases(aliases, realm_id, &mut state.paths, &mut state.keys)?;
        let groups: BTreeSet<GroupId> = self.distinct.keys().map(|key| key.0).collect();
        self.rule_groups = (groups.into_iter())
            .filter(|group_id| !self.aliases.rules.contains_key(group_id))
            .collect();
        self.next_rules()
    }

    fn next_rules(&mut self) -> HashTurn {
        let Some(group_id) = self.rule_groups.pop_front() else {
            return self.load_policies();
        };
        let auth = &self.scope.spec.auth_context;
        let rules = PermissionRulesOperation::new(PermissionRulesConfig {
            auth_context: auth.clone(),
            path: format!("/{}/g/{group_id}", auth.realm_id),
        });
        self.step = HashStep::Rules(group_id);
        Ok(Next::Call(IoCall::child(rules)))
    }

    /// Only a group whose rules already allow an alias needs its object policy; a foreign group
    /// is denied before its policy is consulted.
    fn load_policies(&mut self) -> HashTurn {
        let rules = &self.aliases.rules;
        let allowed = (self.distinct.keys()).filter(|(group_id, path)| {
            (rules.get(group_id)).is_some_and(|rules| rules.allows(path, &Permission::READ))
        });
        let pending = self
            .aliases
            .missing_policies(allowed.map(|(group_id, _)| *group_id));
        if pending.is_empty() {
            return self.authorize();
        }
        let call = policy_call(self.realm_id(), &pending);
        self.step = HashStep::Policies(pending);
        Ok(Next::Call(call))
    }

    fn authorize(&mut self) -> HashTurn {
        let state = &mut self.aliases;
        for key in self.distinct.keys() {
            if !state.authorized.contains_key(key) {
                let allowed = alias_allowed(
                    &self.scope.spec,
                    key.0,
                    &key.1,
                    &state.rules,
                    &state.policies,
                )?;
                state.authorized.insert(key.clone(), allowed);
            }
        }
        self.queue = std::mem::take(&mut self.distinct)
            .into_values()
            .flatten()
            .collect();
        self.next_alias()
    }

    fn next_alias(&mut self) -> HashTurn {
        while let Some(alias) = self.queue.pop_front() {
            let key = (alias.group_id, alias.permission_path());
            let Some(allowed) = self.aliases.authorized.get(&key) else {
                return Err(ExportFailure::Retryable(
                    "alias authorization unavailable".to_string(),
                ));
            };
            if !allowed {
                self.alias_denied = true;
                continue;
            }
            let auth = self.scope.spec.auth_context.clone();
            let resolve = ResolveLocalOperation::new(auth, LocalSource::Alias(alias));
            self.step = HashStep::Alias;
            return Ok(Next::Call(IoCall::child(resolve)));
        }
        let found = (std::mem::take(&mut self.resolved), self.alias_denied);
        self.aliases.resolved.insert(self.hash, found);
        self.use_resolved()
    }

    fn use_resolved(&mut self) -> HashTurn {
        let state = &self.aliases;
        let (cached, denied) = state.resolved.get(&self.hash).ok_or_else(lost_cache)?;
        merge_candidates(&mut self.candidates, cached, MAX_LOCAL_CANDIDATES);
        self.denied |= *denied;
        // An export into another realm reads only local copies, so other holders are never needed.
        if self.scope.spec.selection.is_some() {
            return Ok(self.done(false));
        }
        let realm_id = self.realm_id();
        let holders = GetHoldersOperation::new(self.hash, realm_id, self.scope.owner_node);
        self.step = HashStep::Holders;
        Ok(Next::Call(IoCall::child(holders)))
    }

    fn holders_found(&mut self, holders: Vec<NodeId>) -> Next<HashFound> {
        let remote = (self.candidates.iter())
            .filter(|candidate| matches!(candidate.source, CandidateSource::RemoteHash { .. }))
            .count();
        let hash = self.hash;
        let found: Vec<_> = (holders.into_iter())
            .take(REMOTE_ATTEMPTS.saturating_sub(remote))
            .map(|node_id| ExportCandidate {
                source: CandidateSource::RemoteHash { node_id, hash },
                report_source: ExportReportSource::Hash,
                resolved_version: self.resolved_version,
                expected_blake3: Some(hash),
            })
            .collect();
        merge_candidates(&mut self.candidates, &found, REMOTE_ATTEMPTS);
        self.done(false)
    }
}

impl Operation for HashCandidatesOperation {
    type Output = HashFound;
    type Error = ExportFailure;

    fn first(&mut self) -> HashTurn {
        if self.aliases.resolved.contains_key(&self.hash) {
            return self.use_resolved();
        }
        if self.aliases.cache.contains_key(&self.hash) {
            return self.plan_aliases();
        }
        Ok(Next::Call(IoCall::child(ResolvePathsOperation::new(
            self.hash,
        ))))
    }

    fn next(&mut self, result: IoResult) -> HashTurn {
        match std::mem::replace(&mut self.step, HashStep::Alias) {
            HashStep::Paths => {
                let aliases = (result.child::<Vec<HashIndex>, ResolvePathsError>()?)
                    .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
                cache_aliases(&mut self.aliases.cache, self.hash, aliases)?;
                self.plan_aliases()
            }
            HashStep::Rules(group_id) => {
                let rules = match result.child::<PermissionRules, AuthorizationError>()? {
                    Ok(rules) => rules,
                    // A group this node cannot read grants nothing: that is a denial, not an
                    // outage, so the export omits its aliases instead of retrying forever.
                    Err(AuthorizationError::DocNotFound | AuthorizationError::GroupNotFound) => {
                        PermissionRules::default()
                    }
                    Err(error) => return Err(ExportFailure::Retryable(error.to_string())),
                };
                self.aliases.rules.insert(group_id, rules);
                self.next_rules()
            }
            HashStep::Policies(pending) => {
                let realm_id = self.realm_id();
                self.aliases.keep_policies(realm_id, &pending, result)?;
                self.authorize()
            }
            HashStep::Alias => {
                let resolved = result.child::<ResolveResult, ResolveError>()?;
                match resolved.map_err(export_failure)? {
                    ResolveResult::Candidate(candidate) => {
                        if self.resolved.len() < MAX_LOCAL_CANDIDATES {
                            self.resolved.push(candidate);
                        }
                    }
                    ResolveResult::Denied => self.alias_denied = true,
                    ResolveResult::Missing { .. } => {}
                }
                self.next_alias()
            }
            HashStep::Holders => Ok(match result.child::<Vec<NodeId>, GetHoldersError>()? {
                Ok(holders) => self.holders_found(holders),
                Err(_) => self.done(true),
            }),
        }
    }
}

#[derive(Debug, PartialEq)]
enum EntityStep {
    OmittedVersion,
    Current,
    Exact,
    Hash,
}

/// Finds the candidates of one File entity, or records why it is left out.
#[derive(Debug, PartialEq)]
struct ResolveEntityOperation {
    scope: SourceScope,
    entity: ExportEntity,
    hash: Option<[u8; 32]>,
    current: Option<VersionedObjectArn>,
    candidates: Vec<ExportCandidate>,
    denied: bool,
    mismatched: bool,
    aliases: AliasState,
    step: EntityStep,
}

type EntityEnd = (ExportEntity, AliasState);
type EntityTurn = Result<Next<EntityEnd>, ExportFailure>;

impl ResolveEntityOperation {
    fn new(scope: SourceScope, entity: ExportEntity, aliases: AliasState) -> Self {
        Self {
            scope,
            entity,
            hash: None,
            current: None,
            candidates: Vec::new(),
            denied: false,
            mismatched: false,
            aliases,
            step: EntityStep::Current,
        }
    }

    fn spec(&self) -> &ExportRoCrateSpec {
        &self.scope.spec
    }

    fn realm_id(&self) -> RealmId {
        self.spec().auth_context.realm_id
    }

    fn head_call(&self) -> Option<IoCall> {
        let location = self.entity.storage_key.as_ref()?;
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: location.bucket.clone(),
            key: location.key.clone(),
            version_id: None,
        });
        Some(IoCall::child(head))
    }

    /// The current version of the entity's key on this node, as an exact identity.
    fn current_version(
        &self,
        result: IoResult,
    ) -> Result<Option<VersionedObjectArn>, ExportFailure> {
        let head = result.child::<HeadObjectResult, HeadObjectError>()?;
        let Some(location) = self.entity.storage_key.as_ref() else {
            return Ok(None);
        };
        let Some(version) = head.ok().and_then(|head| head.version_id) else {
            return Ok(None);
        };
        let (bucket, key) = (&location.bucket, &location.key);
        let node_id = self.scope.owner_node;
        let arn = VersionedObjectArn::new(self.realm_id(), node_id, bucket, key, version);
        Ok(arn.ok())
    }

    fn done(&mut self) -> Next<EntityEnd> {
        let entity = self.entity.clone();
        Next::Done((entity, std::mem::take(&mut self.aliases)))
    }

    fn left_out(&mut self, code: ReasonCode, message: &str) -> Next<EntityEnd> {
        self.entity.omission = Some(code);
        self.entity.message = Some(message.to_string());
        self.done()
    }

    fn exact(&self) -> Option<VersionedObjectArn> {
        let realm_id = self.realm_id();
        (self.entity.exact.as_ref())
            .filter(|exact| exact.realm_id == realm_id)
            .or(self.current.as_ref())
            .cloned()
    }

    /// Without an exact version or content hash of this realm, an `s3://` location names this
    /// node's current version of that key.
    fn start(&mut self) -> EntityTurn {
        let realm_id = self.realm_id();
        let entity = &self.entity;
        self.hash =
            (entity.hash).filter(|_| entity.hash_realm.is_none_or(|realm| realm == realm_id));
        let local_exact = (entity.exact.as_ref()).is_some_and(|it| it.realm_id == realm_id);
        if !local_exact
            && self.hash.is_none()
            && let Some(head) = self.head_call()
        {
            self.step = EntityStep::Current;
            return Ok(Next::Call(head));
        }
        self.use_exact()
    }

    fn use_exact(&mut self) -> EntityTurn {
        let Some(exact) = self.exact() else {
            return self.use_hash();
        };
        if exact.node_id == self.scope.owner_node {
            let auth = self.spec().auth_context.clone();
            let resolve = ResolveLocalOperation::new(auth, LocalSource::Exact(exact));
            self.step = EntityStep::Exact;
            return Ok(Next::Call(IoCall::child(resolve)));
        }
        self.candidates.push(ExportCandidate {
            source: CandidateSource::RemoteExact {
                node_id: exact.node_id,
                target: exact.clone(),
            },
            report_source: ExportReportSource::Remote,
            resolved_version: Some(exact.version),
            expected_blake3: self.hash,
        });
        self.use_hash()
    }

    fn exact_found(&mut self, found: ResolveResult) {
        match found {
            ResolveResult::Candidate(candidate) => {
                if self
                    .hash
                    .is_some_and(|it| Some(it) != candidate.expected_blake3)
                {
                    self.mismatched = true;
                } else {
                    self.candidates.push(candidate);
                }
            }
            ResolveResult::Denied => self.denied = true,
            ResolveResult::Missing { hash: discovered } => match (self.hash, discovered) {
                (Some(expected), Some(discovered)) if expected != discovered => {
                    self.mismatched = true;
                }
                (None, discovered) => self.hash = discovered,
                _ => {}
            },
        }
    }

    fn use_hash(&mut self) -> EntityTurn {
        if self.mismatched {
            let message = "versioned ARN and content hash disagree";
            return Ok(self.left_out(ReasonCode::Unsupported, message));
        }
        let Some(hash) = self.hash else {
            return Ok(self.finish());
        };
        let version = self.exact().map(|exact| exact.version);
        let lookup = HashCandidatesOperation::new(
            self.scope.clone(),
            hash,
            version,
            std::mem::take(&mut self.candidates),
            std::mem::take(&mut self.aliases),
        );
        self.step = EntityStep::Hash;
        Ok(Next::Call(IoCall::child(lookup)))
    }

    fn finish(&mut self) -> Next<EntityEnd> {
        self.entity.candidates = std::mem::take(&mut self.candidates);
        if self.entity.candidates.is_empty() {
            let (code, message) = match self.denied {
                true => (ReasonCode::Denied, "payload READ permission denied"),
                false => (ReasonCode::Missing, "no readable payload version was found"),
            };
            return self.left_out(code, message);
        }
        self.done()
    }
}

impl Operation for ResolveEntityOperation {
    type Output = EntityEnd;
    type Error = ExportFailure;

    fn first(&mut self) -> EntityTurn {
        if self.entity.omission.is_none() {
            return self.start();
        }
        // An export into a realm names a location it leaves out by its current version here.
        let entity = &self.entity;
        let unnamed = entity.exact.is_none() && entity.hash.is_none();
        if self.spec().selection.is_some()
            && unnamed
            && let Some(head) = self.head_call()
        {
            self.step = EntityStep::OmittedVersion;
            return Ok(Next::Call(head));
        }
        Ok(self.done())
    }

    fn next(&mut self, result: IoResult) -> EntityTurn {
        match self.step {
            EntityStep::OmittedVersion => {
                self.entity.exact = self.current_version(result)?;
                Ok(self.done())
            }
            EntityStep::Current => {
                self.current = self.current_version(result)?;
                self.use_exact()
            }
            EntityStep::Exact => {
                let found = result.child::<ResolveResult, ResolveError>()?;
                self.exact_found(found.map_err(export_failure)?);
                self.use_hash()
            }
            EntityStep::Hash => {
                let found = result.child::<HashFound, ExportFailure>()??;
                self.candidates = found.candidates;
                self.denied |= found.denied;
                self.aliases = found.aliases;
                if found.unavailable && self.candidates.is_empty() {
                    let message = "blob holder discovery is unavailable";
                    return Ok(self.left_out(ReasonCode::Offline, message));
                }
                Ok(self.finish())
            }
        }
    }
}

/// The resolved entities and what the lookups learned.
#[derive(Debug, PartialEq)]
pub(super) struct Resolved {
    pub(super) entities: Vec<ExportEntity>,
    pub(super) aliases: AliasState,
}

#[derive(Debug, PartialEq)]
enum ResolveStep {
    Stops,
    Entity,
    Reported,
}

/// Resolves every entity in order, stopping with the job and counting each in its progress.
#[derive(Debug, PartialEq)]
pub(super) struct ResolveEntriesOperation {
    scope: SourceScope,
    entities: Vec<ExportEntity>,
    aliases: AliasState,
    index: usize,
    step: ResolveStep,
}

type ResolveTurn = Result<Next<Resolved>, ExportFailure>;

impl ResolveEntriesOperation {
    pub(super) fn new(
        scope: SourceScope,
        entities: Vec<ExportEntity>,
        aliases: AliasState,
    ) -> Self {
        Self {
            scope,
            entities,
            aliases,
            index: 0,
            step: ResolveStep::Stops,
        }
    }

    fn check_stops(&mut self) -> ResolveTurn {
        if self.index >= self.entities.len() {
            return Ok(Next::Done(Resolved {
                entities: std::mem::take(&mut self.entities),
                aliases: std::mem::take(&mut self.aliases),
            }));
        }
        self.step = ResolveStep::Stops;
        Ok(Next::Call(job_call(JobEffect::Stops)))
    }
}

impl Operation for ResolveEntriesOperation {
    type Output = Resolved;
    type Error = ExportFailure;

    fn first(&mut self) -> ResolveTurn {
        self.check_stops()
    }

    fn next(&mut self, result: IoResult) -> ResolveTurn {
        match self.step {
            ResolveStep::Stops => {
                if let Some(stopped) = stop_failure(stops_of(result)?) {
                    return Err(stopped);
                }
                let entity = self.entities[self.index].clone();
                let aliases = std::mem::take(&mut self.aliases);
                let resolve = ResolveEntityOperation::new(self.scope.clone(), entity, aliases);
                self.step = ResolveStep::Entity;
                Ok(Next::Call(IoCall::child(resolve)))
            }
            ResolveStep::Entity => {
                let (entity, aliases) = result.child::<EntityEnd, ExportFailure>()??;
                self.entities[self.index] = entity;
                self.aliases = aliases;
                self.index += 1;
                self.step = ResolveStep::Reported;
                Ok(Next::Call(job_call(JobEffect::Report(JobReport {
                    current: None,
                    total: None,
                    advance: 1,
                    checkpoint: false,
                }))))
            }
            ResolveStep::Reported => {
                crate::jobs::import::reported(result)?;
                self.check_stops()
            }
        }
    }
}

#[cfg(test)]
#[path = "sources_tests.rs"]
mod tests;
