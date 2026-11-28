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
use aruna_core::types::GroupId;
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

/// Loads every applet of `group_id` without caller auth checks (for internal automation checks).
#[derive(Debug, PartialEq)]
pub struct GroupAppletsOperation {
    group_id: GroupId,
    ids: Vec<Ulid>,
    applets: Vec<Applet>,
    reading: bool,
}

impl GroupAppletsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            ids: Vec::new(),
            applets: Vec::new(),
            reading: false,
        }
    }

    fn page(&self, after: Option<aruna_core::types::Key>) -> Next<Vec<Applet>> {
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
        self.reading = true;
        Next::Call(IoCall::Read(reads))
    }
}

impl Operation for GroupAppletsOperation {
    type Output = Vec<Applet>;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Vec<Applet>>, AppletError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Applet>>, AppletError> {
        if !self.reading {
            let (rows, more) = result.page()?;
            for (key, _) in rows {
                if let Some(bytes) = key.get(17..33).and_then(|s| <[u8; 16]>::try_from(s).ok()) {
                    self.ids.push(Ulid::from_bytes(bytes));
                }
            }
            if let Some(after) = more {
                return Ok(self.page(Some(after)));
            }
            return Ok(self.next_batch());
        }
        for value in result.values()?.into_iter().flatten() {
            self.applets.push(Applet::from_bytes(&value)?);
        }
        Ok(self.next_batch())
    }
}
