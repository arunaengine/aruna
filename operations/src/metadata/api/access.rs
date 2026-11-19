//! Decides which listed registry records a caller may read, from group read rules and policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::operation::{Operation, Runnable, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;

use super::MetadataApiError;
use super::read_types::metadata_read_request;
use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluators};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::permission_rules::{
    GroupPermissionRules, PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};

/// Children of one group run at most this many at a time, so a page cannot saturate the craqle
/// read permits.
pub(super) const FANOUT_LIMIT: usize = 8;

pub(super) fn internal(error: impl ToString) -> MetadataApiError {
    MetadataApiError::Internal(error.to_string())
}

fn member<O, K>(operation: O) -> Effect
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: std::fmt::Debug + Send + PartialEq + 'static,
    O::Error: std::fmt::Debug + Send + PartialEq + 'static,
{
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

fn group(members: Vec<Effect>) -> IoCall {
    IoCall::Group(EffectGroup {
        members,
        limit: FANOUT_LIMIT,
        form: GroupForm::All { deadline: None },
    })
}

/// Runs `operations` as one group of children, at most [`FANOUT_LIMIT`] at a time.
pub(crate) fn children<O, K>(operations: Vec<O>) -> IoCall
where
    O: Runnable<K> + 'static,
    K: 'static,
    O::Output: std::fmt::Debug + Send + PartialEq + 'static,
    O::Error: std::fmt::Debug + Send + PartialEq + 'static,
{
    group(operations.into_iter().map(member).collect())
}

/// The child results of a group from [`children`], in order; `None` for a member that did not
/// finish or answered with another type.
pub(crate) fn child_results<T: 'static, E: 'static>(
    result: IoResult,
) -> Result<Vec<Option<Result<T, E>>>, MetadataApiError> {
    let IoResult::Group(GroupEvent::Finished { results }) = result else {
        return Err(internal(format!(
            "expected a child group, got {}",
            result.label()
        )));
    };
    Ok(results.into_iter().map(child_result).collect())
}

fn child_result<T: 'static, E: 'static>(member: GroupResult) -> Option<Result<T, E>> {
    let GroupResult::Done(event) = member else {
        return None;
    };
    match *event {
        Event::Child(outcome) => outcome.take(),
        _ => None,
    }
}

/// The read rules of a caller per group and the request policies per scope.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ReadAccess {
    permissions: GroupPermissionRules,
    evaluators: PolicyEvaluators,
    auth: Option<AuthContext>,
}

impl ReadAccess {
    /// The number of groups the caller's read rules cover.
    pub(crate) fn group_count(&self) -> usize {
        self.permissions.group_count()
    }

    /// Whether the caller may read `record`: RBAC or public visibility, then the metadata.read
    /// request policies of its scope. A scope without policies hides its records.
    pub(crate) fn shows(&self, record: &MetadataRegistryRecord) -> bool {
        let scope = (record.realm_id, record.group_id);
        self.permissions.record_visible(record)
            && self.evaluators.get(&scope).is_some_and(|evaluator| {
                let request = metadata_read_request(&record.permission_path, self.auth.as_ref());
                evaluator.evaluate(&request).is_ok()
            })
    }
}

/// Collects the caller's read rules of every group of `records` in `realm_id` and the request
/// policies of their scopes in one group of children. A group whose rules fail keeps none, so
/// its records stay hidden; a policy load failure fails closed.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadAccessOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    scopes: BTreeSet<(RealmId, GroupId)>,
    rule_groups: Vec<GroupId>,
}

impl ReadAccessOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        auth: Option<AuthContext>,
        records: &[MetadataRegistryRecord],
    ) -> Self {
        let scopes = (records.iter())
            .filter(|record| record.realm_id == realm_id)
            .map(|record| (record.realm_id, record.group_id))
            .collect();
        Self {
            realm_id,
            auth,
            scopes,
            rule_groups: Vec::new(),
        }
    }

    /// The caller when it belongs to this realm; rules are collected only for it.
    fn rules_auth(&self) -> Option<&AuthContext> {
        (self.auth.as_ref()).filter(|auth| auth.realm_id == self.realm_id)
    }
}

impl Operation for ReadAccessOperation {
    type Output = ReadAccess;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<ReadAccess>, MetadataApiError> {
        if self.scopes.is_empty() {
            return Ok(Next::Done(ReadAccess::default()));
        }
        let policies = PolicyBulkOperation::new(self.scopes.iter().copied());
        let mut members = vec![member(policies)];
        if let Some(auth) = self.rules_auth().cloned() {
            let groups = self.scopes.iter().map(|(_, group_id)| *group_id);
            self.rule_groups = groups.collect::<BTreeSet<_>>().into_iter().collect();
            for group_id in &self.rule_groups {
                let rules = PermissionRulesOperation::new(PermissionRulesConfig {
                    path: format!("/{}/g/{group_id}", auth.realm_id),
                    auth_context: auth.clone(),
                });
                members.push(member(rules));
            }
        }
        Ok(Next::Call(group(members)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReadAccess>, MetadataApiError> {
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(internal(format!(
                "expected the access group, got {}",
                result.label()
            )));
        };
        let mut results = results.into_iter();
        let policies = results.next().and_then(child_result);
        let Some(Ok::<_, PolicyEnforcementError>(evaluators)) = policies else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let mut groups = HashMap::new();
        for (group_id, member) in self.rule_groups.iter().zip(results) {
            if let Some(Ok::<PermissionRules, AuthorizationError>(rules)) = child_result(member) {
                groups.insert(*group_id, rules);
            }
        }
        let permissions = match self.rules_auth() {
            Some(auth) => GroupPermissionRules::from_groups(Some(auth.realm_id), groups),
            None => GroupPermissionRules::default(),
        };
        Ok(Next::Done(ReadAccess {
            permissions,
            evaluators,
            auth: self.auth.clone(),
        }))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::auth::enforcement::{policy_reads, read_policies};
    use crate::auth::permission_rules::CollectedRole;
    use aruna_core::UserId;
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use aruna_core::types::Value;
    use std::collections::HashSet;
    use ulid::Ulid;

    pub(crate) fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    /// The policy rows of one group scope as `policy_reads` names them.
    fn policy_rows(group_id: GroupId) -> Vec<Option<Value>> {
        let owner = UserId::nil(realm());
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
            user_id: owner,
            realm_id: realm(),
        };
        let config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
        let auth = GroupAuthorizationDocument::default_group_doc(owner, realm(), group_id);
        let group = Group {
            display_name: "policy-fixture".to_string(),
            group_id,
            realm_id: realm(),
            roles: auth.roles.keys().copied().collect(),
            owner,
        };
        let rows = [
            config.to_bytes(&actor).unwrap(),
            group.to_bytes(&actor).unwrap(),
            auth.to_bytes(&actor).unwrap(),
        ];
        assert_eq!(policy_reads(realm(), Some(group_id)).len(), rows.len());
        rows.into_iter().map(|row| Some(Value::from(row))).collect()
    }

    fn evaluators(records: &[MetadataRegistryRecord]) -> PolicyEvaluators {
        (records.iter())
            .map(|record| {
                let rows = policy_rows(record.group_id);
                let evaluator = read_policies(realm(), Some(record.group_id), rows).unwrap();
                ((record.realm_id, record.group_id), evaluator)
            })
            .collect()
    }

    fn finished(results: Vec<GroupResult>) -> IoResult {
        IoResult::Group(GroupEvent::Finished { results })
    }

    fn done<T, E>(result: Result<T, E>) -> GroupResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(result))))
    }

    /// The access of a caller without read rules over `records`, with permissive policies.
    pub(crate) fn access_for(
        records: &[MetadataRegistryRecord],
        auth: Option<AuthContext>,
    ) -> ReadAccess {
        let mut access = ReadAccessOperation::new(realm(), auth, records);
        let Ok(Next::Call(IoCall::Group(group))) = access.first() else {
            panic!("expected the access group");
        };
        let mut results = vec![done(Ok::<_, PolicyEnforcementError>(evaluators(records)))];
        let failed = || done(Err::<PermissionRules, _>(AuthorizationError::GroupNotFound));
        results.extend((1..group.members.len()).map(|_| failed()));
        let Ok(Next::Done(access)) = access.next(finished(results)) else {
            panic!("expected the access");
        };
        access
    }

    fn record(public: bool) -> MetadataRegistryRecord {
        let document_id = Ulid::from_parts(3, 3);
        MetadataRegistryRecord {
            realm_id: realm(),
            group_id: Ulid::from_parts(2, 2),
            document_id,
            document_path: "datasets/access".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: format!("/{}/g/{}/datasets/access", realm(), Ulid::from_parts(2, 2)),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(4, 4),
            last_event_id: Ulid::from_parts(4, 4),
        }
    }

    fn caller() -> AuthContext {
        AuthContext {
            user_id: UserId::local(Ulid::from_parts(8, 8), realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        }
    }

    #[test]
    fn empty_reads_nothing() {
        let mut access = ReadAccessOperation::new(realm(), None, &[]);
        assert_eq!(access.first(), Ok(Next::Done(ReadAccess::default())));
    }

    #[test]
    fn rules_follow_caller() {
        let records = [record(true)];
        let mut anonymous = ReadAccessOperation::new(realm(), None, &records);
        let Ok(Next::Call(IoCall::Group(group))) = anonymous.first() else {
            panic!("expected the access group");
        };
        assert_eq!(group.members.len(), 1);
        let mut signed = ReadAccessOperation::new(realm(), Some(caller()), &records);
        let Ok(Next::Call(IoCall::Group(group))) = signed.first() else {
            panic!("expected the access group");
        };
        assert_eq!(group.members.len(), 2);
    }

    #[test]
    fn policies_fail_closed() {
        let records = [record(true)];
        let mut access = ReadAccessOperation::new(realm(), None, &records);
        access.first().unwrap();
        let failed =
            Err::<PolicyEvaluators, _>(PolicyEnforcementError::Unavailable("down".to_string()));
        let turn = access.next(finished(vec![done(failed)]));
        assert_eq!(turn, Err(MetadataApiError::ServiceUnavailable));
    }

    fn grouped(seed: u64, path: &str) -> MetadataRegistryRecord {
        let mut grouped = record(false);
        grouped.document_id = Ulid::from_parts(seed, 3);
        grouped.graph_iri = MetadataRegistryRecord::graph_iri_for(grouped.document_id);
        grouped.document_path = path.to_string();
        grouped.permission_path = MetadataRegistryRecord::permission_path_for(
            &realm(),
            grouped.group_id,
            path,
            grouped.document_id,
        );
        grouped
    }

    // The access of a caller holding one role of `patterns` in the records' group of `realm_id`.
    fn ruled(
        records: &[MetadataRegistryRecord],
        realm_id: RealmId,
        patterns: &[(&str, Permission)],
    ) -> ReadAccess {
        let role = Role {
            role_id: Ulid::from_parts(5, 5),
            name: "reader".to_string(),
            permissions: (patterns.iter())
                .map(|(pattern, permission)| ((*pattern).to_string(), permission.clone()))
                .collect(),
            assigned_users: HashSet::new(),
        };
        let collected = CollectedRole {
            role,
            direct: true,
            public: false,
        };
        let rules = PermissionRules::from_roles(vec![collected], None).unwrap();
        let groups = HashMap::from([(records[0].group_id, rules)]);
        ReadAccess {
            permissions: GroupPermissionRules::from_groups(Some(realm_id), groups),
            evaluators: evaluators(records),
            auth: Some(caller()),
        }
    }

    #[test]
    fn deny_hides_document() {
        // A per-document DENY under a group-wide READ hides only that document.
        let records = [grouped(1, "datasets/secret"), grouped(2, "datasets/open")];
        let group = format!("/{}/g/{}/meta/**", realm(), records[0].group_id);
        let patterns = [
            (group.as_str(), Permission::READ),
            (records[0].permission_path.as_str(), Permission::DENY),
        ];
        let access = ruled(&records, realm(), &patterns);
        assert!(!access.shows(&records[0]));
        assert!(access.shows(&records[1]));
    }

    #[test]
    fn narrow_grant_shows() {
        // A grant on one document shows it without opening the whole group.
        let records = [grouped(1, "datasets/shared"), grouped(2, "datasets/hidden")];
        let patterns = [(records[0].permission_path.as_str(), Permission::READ)];
        let access = ruled(&records, realm(), &patterns);
        assert!(access.shows(&records[0]));
        assert!(!access.shows(&records[1]));
    }

    #[test]
    fn foreign_rules_hide() {
        // Rules collected for another realm open no private document here.
        let records = [grouped(1, "datasets/shared"), record(true)];
        let patterns = [(records[0].permission_path.as_str(), Permission::READ)];
        let access = ruled(&records, RealmId::from_bytes([8; 32]), &patterns);
        assert!(!access.shows(&records[0]));
        assert!(access.shows(&records[1]));
    }

    #[test]
    fn anonymous_sees_public() {
        let records = [record(true), grouped(1, "datasets/private")];
        let access = access_for(&records, None);
        assert!(access.shows(&records[0]));
        assert!(!access.shows(&records[1]));
    }

    #[test]
    fn failed_rules_hide() {
        let records = [record(true), record(false)];
        let access = access_for(&records, Some(caller()));
        assert!(access.shows(&records[0]));
        assert!(!access.shows(&records[1]));
    }
}
