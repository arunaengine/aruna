//! Lists the applets of a group by page and finds the applet a host names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::hosts::{HostRow, host_key};
use aruna_core::applet::record::{Applet, applet_key, group_key, group_prefix};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{APPLET_HOSTS_KEYSPACE, APPLETS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::{GroupId, Key};
use ulid::Ulid;

use super::{AppletError, allowed, applet_answer, authorize_path, host_answer, read_applet};

/// One page of a group's applets and the applet id to continue after.
#[derive(Clone, Debug, PartialEq)]
pub struct AppletPage {
    pub applets: Vec<Applet>,
    pub next: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum ListStep {
    Authorize,
    Index,
    Records(Option<Ulid>),
}

/// Pages the applets of `group_id` for a caller with READ on the group's data here.
#[derive(Debug, PartialEq)]
pub struct ListAppletsOperation {
    auth: AuthContext,
    group_id: GroupId,
    local: NodeId,
    after: Option<Ulid>,
    limit: usize,
    step: ListStep,
}

impl ListAppletsOperation {
    pub fn new(
        auth: &AuthContext,
        (group_id, local): (GroupId, NodeId),
        after: Option<Ulid>,
        limit: usize,
    ) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            local,
            after,
            limit: limit.max(1),
            step: ListStep::Authorize,
        }
    }
}

impl Operation for ListAppletsOperation {
    type Output = AppletPage;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<AppletPage>, AppletError> {
        let path = group_permission_path(self.auth.realm_id, self.group_id, self.local);
        Ok(Next::Call(authorize_path(
            &self.auth,
            &path,
            Permission::READ,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AppletPage>, AppletError> {
        match std::mem::replace(&mut self.step, ListStep::Index) {
            ListStep::Authorize => {
                if !allowed(result)? {
                    return Err(AppletError::NotFound);
                }
                Ok(Next::Call(IoCall::Page {
                    space: APPLETS_KEYSPACE.to_string(),
                    prefix: group_prefix(self.group_id),
                    after: (self.after).map(|after| group_key(self.group_id, after)),
                    limit: self.limit,
                }))
            }
            ListStep::Index => {
                let (rows, more) = result.page()?;
                let ids: Vec<Ulid> = (rows.iter())
                    .filter_map(|(key, _)| key.get(17..33)?.try_into().ok())
                    .map(|bytes: [u8; 16]| Ulid::from_bytes(bytes))
                    .collect();
                let next = more.and(ids.last().copied());
                if ids.is_empty() {
                    return Ok(Next::Done(AppletPage {
                        applets: Vec::new(),
                        next: None,
                    }));
                }
                self.step = ListStep::Records(next);
                let reads = (ids.into_iter())
                    .map(|id| (APPLETS_KEYSPACE.to_string(), applet_key(id)))
                    .collect();
                Ok(Next::Call(IoCall::Read(reads)))
            }
            ListStep::Records(next) => {
                let mut applets = Vec::new();
                for value in result.values()?.into_iter().flatten() {
                    applets.push(Applet::from_bytes(&value)?);
                }
                Ok(Next::Done(AppletPage { applets, next }))
            }
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        matches!(error, AppletError::NotFound)
    }
}

/// The applet and host row a host names, for the edge; no caller check, as the edge's own
/// key authorized the call.
#[derive(Debug, PartialEq)]
pub struct HostLookupOperation {
    host: String,
    row: Option<HostRow>,
}

impl HostLookupOperation {
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
            row: None,
        }
    }
}

impl Operation for HostLookupOperation {
    type Output = Option<(HostRow, Applet)>;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Self::Output>, AppletError> {
        let row = host_key(&self.host);
        Ok(Next::Call(IoCall::Read(vec![(
            APPLET_HOSTS_KEYSPACE.to_string(),
            row,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AppletError> {
        let Some(row) = self.row else {
            let Some(row) = host_answer(result.value()?)? else {
                return Ok(Next::Done(None));
            };
            self.row = Some(row);
            return Ok(Next::Call(read_applet(row.applet_id)));
        };
        Ok(Next::Done(
            applet_answer(result)?.map(|applet| (row, applet)),
        ))
    }
}

/// One applet record without a caller check, for the edge, whose own key authorized the call.
#[derive(Debug, PartialEq)]
pub struct LoadAppletOperation {
    applet_id: Ulid,
}

impl LoadAppletOperation {
    pub fn new(applet_id: Ulid) -> Self {
        Self { applet_id }
    }
}

impl Operation for LoadAppletOperation {
    type Output = Option<Applet>;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Self::Output>, AppletError> {
        Ok(Next::Call(read_applet(self.applet_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AppletError> {
        Ok(Next::Done(applet_answer(result)?))
    }
}

#[derive(Debug, PartialEq)]
enum GroupReadStep {
    Page,
    Read,
}

/// Loads every applet of `group_id` without caller auth checks (for internal automation checks).
#[derive(Debug, PartialEq)]
pub struct GroupAppletsOperation {
    group_id: GroupId,
    ids: Vec<Ulid>,
    applets: Vec<Applet>,
    step: GroupReadStep,
}

impl GroupAppletsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            ids: Vec::new(),
            applets: Vec::new(),
            step: GroupReadStep::Page,
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<Applet>> {
        Next::Call(IoCall::Page {
            space: APPLETS_KEYSPACE.to_string(),
            prefix: group_prefix(self.group_id),
            after,
            limit: 256,
        })
    }

    fn next_batch(&mut self) -> Next<Vec<Applet>> {
        if self.ids.is_empty() {
            return Next::Done(std::mem::take(&mut self.applets));
        }
        let take = self.ids.len().min(128);
        let reads = self
            .ids
            .drain(..take)
            .map(|id| (APPLETS_KEYSPACE.to_string(), applet_key(id)))
            .collect();
        self.step = GroupReadStep::Read;
        Next::Call(IoCall::Read(reads))
    }
}

impl Operation for GroupAppletsOperation {
    type Output = Vec<Applet>;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Vec<Applet>>, AppletError> {
        self.step = GroupReadStep::Page;
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Applet>>, AppletError> {
        match self.step {
            GroupReadStep::Page => {
                let (rows, more) = result.page()?;
                for (key, _) in rows {
                    if let Some(bytes) = key
                        .get(17..33)
                        .and_then(|slice| <[u8; 16]>::try_from(slice).ok())
                    {
                        self.ids.push(Ulid::from_bytes(bytes));
                    }
                }
                if let Some(after) = more {
                    return Ok(self.page(Some(after)));
                }
                Ok(self.next_batch())
            }
            GroupReadStep::Read => {
                for value in result.values()?.into_iter().flatten() {
                    self.applets.push(Applet::from_bytes(&value)?);
                }
                Ok(self.next_batch())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::applet::record::{Access, Embedding, Grants, Lifecycle};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::user::id::UserId;

    fn sample_applet(group_id: GroupId, applet_id: Ulid) -> Applet {
        let realm = RealmId([2; 32]);
        Applet {
            applet_id,
            group_id,
            name: "Demo".into(),
            slug: "demo".into(),
            host: "demo.apps.local".into(),
            repository: None,
            host_node: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            lifecycle: Lifecycle::Live,
            access: Access::Private,
            live: Some(1),
            versions: Vec::new(),
            drafts: Vec::new(),
            grants: Grants::default(),
            embedding: Embedding::default(),
            guests: None,
            domains: Vec::new(),
            pending: None,
            account: None,
            role_id: None,
            visitor_salt: [0; 32],
            created_by: UserId::local(Ulid::from_bytes([1; 16]), realm),
            created_at_ms: 1,
            revision: 1,
            automations: Vec::new(),
        }
    }

    #[test]
    fn pages_group_applets() {
        let group_id = Ulid::from_bytes([11; 16]);
        let first_id = Ulid::from_bytes([12; 16]);
        let second_id = Ulid::from_bytes([13; 16]);
        let mut operation = GroupAppletsOperation::new(group_id);

        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { after: None, .. }))
        ));

        let cursor = group_key(group_id, first_id);
        let second_page = operation
            .next(IoResult::Page {
                rows: vec![(cursor.clone(), Vec::new().into())],
                next: Some(cursor),
            })
            .unwrap();
        assert!(matches!(
            second_page,
            Next::Call(IoCall::Page { after: Some(_), .. })
        ));

        let read_batch = operation
            .next(IoResult::Page {
                rows: vec![(group_key(group_id, second_id), Vec::new().into())],
                next: None,
            })
            .unwrap();
        assert!(matches!(read_batch, Next::Call(IoCall::Read(ref keys)) if keys.len() == 2));

        let first = sample_applet(group_id, first_id);
        let done = operation
            .next(IoResult::Values(vec![
                Some(first.to_bytes().unwrap().into()),
                None,
            ]))
            .unwrap();
        assert_eq!(done, Next::Done(vec![first]));

        // Empty group completes immediately after the index page without a Read call.
        let mut empty = GroupAppletsOperation::new(group_id);
        assert!(matches!(empty.first(), Ok(Next::Call(IoCall::Page { .. }))));
        assert_eq!(
            empty
                .next(IoResult::Page {
                    rows: Vec::new(),
                    next: None,
                })
                .unwrap(),
            Next::Done(Vec::new())
        );
    }
}
