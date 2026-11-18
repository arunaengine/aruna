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
pub(super) fn children<O, K>(operations: Vec<O>) -> IoCall
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
pub(super) fn child_results<T: 'static, E: 'static>(
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
        let policies = PolicyBulkOperation::new(self.scopes.iter().copied())
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
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
