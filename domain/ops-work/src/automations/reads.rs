//! Reads automation records and group-indexed pages from node-local storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::keys::{group_auto_key, parse_group_key, record_key};
use aruna_core::automation::record::{Automation, Lifecycle};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::AUTOMATIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{GroupId, Key};
use ulid::Ulid;

use super::{AutoError, allowed, auto_answer, group_autos_page, group_read, read_auto};

/// Loads one automation record without caller auth checks (for internal runner and matcher).
#[derive(Debug, PartialEq)]
pub struct LoadAutoOperation {
    id: Ulid,
}

impl LoadAutoOperation {
    pub fn new(id: Ulid) -> Self {
        Self { id }
    }
}

impl Operation for LoadAutoOperation {
    type Output = Option<Automation>;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Self::Output>, AutoError> {
        Ok(Next::Call(read_auto(self.id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AutoError> {
        Ok(Next::Done(auto_answer(result)?))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AutoPage {
    pub items: Vec<Automation>,
    pub next: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum ListStep {
    Access,
    Index,
    ReadBatch(Option<Key>),
}

/// Pages automations belonging to `group_id` via the `GROUP_ROW` index for a caller with READ.
#[derive(Debug, PartialEq)]
pub struct ListAutosOperation {
    auth: AuthContext,
    group_id: GroupId,
    local: NodeId,
    lifecycle: Option<Lifecycle>,
    workflow_id: Option<Ulid>,
    after: Option<Ulid>,
    limit: usize,
    step: ListStep,
    found: Vec<Automation>,
}

impl ListAutosOperation {
    pub fn new(
        auth: &AuthContext,
        (group_id, local): (GroupId, NodeId),
        lifecycle: Option<Lifecycle>,
        after: Option<Ulid>,
        limit: usize,
    ) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            local,
            lifecycle,
            workflow_id: None,
            after,
            limit: limit.clamp(1, 500),
            step: ListStep::Access,
            found: Vec::new(),
        }
    }

    pub fn with_workflow(mut self, workflow_id: Option<Ulid>) -> Self {
        self.workflow_id = workflow_id;
        self
    }

    fn finish(&mut self) -> Next<AutoPage> {
        let mut items = std::mem::take(&mut self.found);
        let has_more = items.len() > self.limit;
        items.truncate(self.limit);
        let next = has_more
            .then(|| items.last().map(|item| item.automation_id))
            .flatten();
        Next::Done(AutoPage { items, next })
    }
}

impl Operation for ListAutosOperation {
    type Output = AutoPage;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<AutoPage>, AutoError> {
        self.step = ListStep::Access;
        Ok(Next::Call(group_read(
            &self.auth,
            self.group_id,
            self.local,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AutoPage>, AutoError> {
        match std::mem::replace(&mut self.step, ListStep::Index) {
            ListStep::Access => {
                if !allowed(result)? {
                    return Err(AutoError::NotFound);
                }
                let start_after = self.after.map(|id| group_auto_key(self.group_id, id));
                self.step = ListStep::Index;
                Ok(Next::Call(group_autos_page(self.group_id, start_after)))
            }
            ListStep::Index => {
                let (rows, more) = result.page()?;
                let reads: Vec<(String, Key)> = rows
                    .into_iter()
                    .filter_map(|(key, _)| parse_group_key(&key))
                    .map(|id| (AUTOMATIONS_KEYSPACE.to_string(), record_key(id)))
                    .collect();
                if reads.is_empty() {
                    if let Some(after) = more {
                        self.step = ListStep::Index;
                        return Ok(Next::Call(group_autos_page(self.group_id, Some(after))));
                    }
                    return Ok(self.finish());
                }
                self.step = ListStep::ReadBatch(more);
                Ok(Next::Call(IoCall::Read(reads)))
            }
            ListStep::ReadBatch(more) => {
                for bytes in result.values()?.into_iter().flatten() {
                    let item = Automation::from_bytes(&bytes)?;
                    if item.group_id == self.group_id
                        && item.lifecycle != Lifecycle::Removed
                        && self.lifecycle.is_none_or(|want| item.lifecycle == want)
                        && self.workflow_id.is_none_or(|workflow_id| {
                            item.workflow_id == Some(workflow_id)
                                || item.spec.runs_workflow(workflow_id)
                        })
                    {
                        self.found.push(item);
                        if self.found.len() > self.limit {
                            return Ok(self.finish());
                        }
                    }
                }
                if let Some(after) = more {
                    self.step = ListStep::Index;
                    return Ok(Next::Call(group_autos_page(self.group_id, Some(after))));
                }
                Ok(self.finish())
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::automation::keys::group_prefix;
    use aruna_core::automation::record::{Automation, Lifecycle, Spec};
    use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::user::id::UserId;
    use aruna_ops_identity::auth::error::AuthorizeError;
    use ulid::Ulid;

    use super::*;

    #[test]
    fn pages_group_index() {
        let realm = RealmId([4; 32]);
        let node = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let group_id = Ulid::from_bytes([6; 16]);
        let auth = AuthContext::new(UserId::local(Ulid::from_bytes([1; 16]), realm), realm);
        let first_id = Ulid::from_bytes([10; 16]);
        let second_id = Ulid::from_bytes([20; 16]);

        let make_auto = |automation_id: Ulid| Automation {
            automation_id,
            group_id,
            name: "a".into(),
            description: String::new(),
            host_node: node,
            lifecycle: Lifecycle::Draft,
            pause_reason: None,
            spec: Spec::default(),
            pending: None,
            account: None,
            role_id: None,
            approved_by: None,
            approved_at_ms: None,
            created_by: auth.user_id,
            created_at_ms: 1,
            updated_at_ms: 1,
            revision: 1,
            identity_synced_revision: 0,
            workflow_id: None,
        };

        let mut list = ListAutosOperation::new(&auth, (group_id, node), None, None, 1);
        assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));

        let page_call = list
            .next(IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizeError>(
                (),
            ))))
            .unwrap();
        assert!(matches!(
            page_call,
            Next::Call(IoCall::Page { ref prefix, .. }) if *prefix == group_prefix(group_id)
        ));

        let read_call = list
            .next(IoResult::Page {
                rows: vec![
                    (group_auto_key(group_id, first_id), Vec::new().into()),
                    (group_auto_key(group_id, second_id), Vec::new().into()),
                ],
                next: None,
            })
            .unwrap();
        assert!(matches!(read_call, Next::Call(IoCall::Read(ref keys)) if keys.len() == 2));

        let Next::Done(page) = list
            .next(IoResult::Values(vec![
                Some(make_auto(first_id).to_bytes().unwrap().into()),
                Some(make_auto(second_id).to_bytes().unwrap().into()),
            ]))
            .unwrap()
        else {
            panic!("expected Done");
        };
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].automation_id, first_id);
        assert_eq!(page.next, Some(first_id));
    }
}
