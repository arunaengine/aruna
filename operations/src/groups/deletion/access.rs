//! Reads a group's deletion state and decides what the caller may delete and see.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeleteRecord, GroupDeletionError,
};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::types::{GroupId, Key};

use super::resources::render;
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::forward::authorize::{ForwardAuthError, write_check};

/// The group row and its deletion record.
pub(super) type LoadedGroup = (Option<Group>, Option<GroupDeleteRecord>);

#[derive(Debug, PartialEq)]
pub(super) struct LoadGroupOperation {
    group_id: GroupId,
}

impl LoadGroupOperation {
    pub(super) fn new(group_id: GroupId) -> Self {
        Self { group_id }
    }
}

impl Operation for LoadGroupOperation {
    type Output = LoadedGroup;
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<LoadedGroup>, GroupDeletionError> {
        let key = Key::from(self.group_id.to_bytes().to_vec());
        Ok(Next::Call(IoCall::Read(vec![
            (GROUP_KEYSPACE.to_string(), key.clone()),
            (GROUP_DELETE_KEYSPACE.to_string(), key),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LoadedGroup>, GroupDeletionError> {
        let [group, record] = values::<2>(result)?;
        Ok(Next::Done((
            group.as_deref().map(Group::from_bytes).transpose()?,
            (record.as_deref())
                .map(GroupDeleteRecord::from_bytes)
                .transpose()?,
        )))
    }
}

/// Lets a realm or group admin delete, and whoever is named on a finished deletion see it again.
#[derive(Debug, PartialEq)]
pub(super) struct AuthorizeDeleteOperation {
    auth: AuthContext,
    /// Reversed, so the next path pops off the end.
    paths: Vec<String>,
    decided: Option<Result<(), GroupDeletionError>>,
}

impl AuthorizeDeleteOperation {
    pub(super) fn new(
        auth: &AuthContext,
        group_id: GroupId,
        record: Option<&GroupDeleteRecord>,
        exists: bool,
    ) -> Self {
        let named = record.is_some_and(|record| {
            record.phase == GroupDeletePhase::Deleted
                && record.plan.realm_id == auth.realm_id
                && (record.plan.owner == auth.user_id
                    || record.plan.requested_by == auth.user_id
                    || record.deleted_by == Some(auth.user_id))
        });
        let decided = match () {
            _ if auth.user_id.is_nil() || auth.path_restrictions.is_some() => {
                Some(Err(GroupDeletionError::Unauthorized))
            }
            _ if named => Some(Ok(())),
            _ => None,
        };
        let mut paths = Vec::new();
        if exists {
            paths.push(format!("/{}/g/{group_id}/admin", auth.realm_id));
        }
        paths.push(format!("/{}/admin/groups", auth.realm_id));
        Self {
            auth: auth.clone(),
            paths,
            decided,
        }
    }

    fn check_next(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let path = self.paths.pop().ok_or(GroupDeletionError::Unauthorized)?;
        Ok(Next::Call(IoCall::child(write_check(&self.auth, &path))))
    }
}

impl Operation for AuthorizeDeleteOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        match self.decided.take() {
            Some(decided) => decided.map(Next::Done),
            None => self.check_next(),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        let checked = result.child::<(), AuthorizeError>()?;
        match checked.map_err(ForwardAuthError::from) {
            Ok(()) => Ok(Next::Done(())),
            Err(ForwardAuthError::Unavailable(reason)) => {
                Err(GroupDeletionError::Unavailable(reason))
            }
            Err(ForwardAuthError::Forbidden | ForwardAuthError::Unauthorized) => self.check_next(),
        }
    }
}

/// Keeps the bucket names the caller may read, as the S3 bucket listing does.
/// Any authorization error drops the name.
#[derive(Debug, PartialEq)]
pub(super) struct VisibleOperation {
    auth: AuthContext,
    group_id: GroupId,
    node: NodeId,
    /// Reversed, so the next name pops off the end.
    names: Vec<String>,
    asked: Option<String>,
    kept: Vec<String>,
}

impl VisibleOperation {
    pub(super) fn new(
        auth: &AuthContext,
        group_id: GroupId,
        node: NodeId,
        mut names: Vec<String>,
    ) -> Self {
        names.reverse();
        Self {
            auth: auth.clone(),
            group_id,
            node,
            names,
            asked: None,
            kept: Vec::new(),
        }
    }

    fn check_next(&mut self) -> Next<Vec<String>> {
        let Some(name) = self.names.pop() else {
            return Next::Done(std::mem::take(&mut self.kept));
        };
        let realm_id = self.auth.realm_id;
        let path = bucket_permission_path(realm_id, self.group_id, self.node, &name);
        self.asked = Some(name);
        Next::Call(IoCall::child(AuthorizeOperation::new(
            realm_id,
            &self.auth,
            &path,
            &Permission::READ,
            PolicyRequestExtras::rest(),
        )))
    }
}

impl Operation for VisibleOperation {
    type Output = Vec<String>;
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<Vec<String>>, GroupDeletionError> {
        Ok(self.check_next())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<String>>, GroupDeletionError> {
        let readable = result.child::<(), AuthorizeError>()?.is_ok();
        if let Some(name) = self.asked.take().filter(|_| readable) {
            self.kept.push(name);
        }
        Ok(self.check_next())
    }
}

/// Words a node's refusal for the caller, naming only the buckets the caller may read.
#[derive(Debug, PartialEq)]
pub(super) struct BlockedOperation {
    auth: AuthContext,
    group_id: GroupId,
    node: NodeId,
    error: Option<GroupDeletionError>,
}

impl BlockedOperation {
    pub(super) fn new(
        auth: &AuthContext,
        group_id: GroupId,
        node: NodeId,
        error: GroupDeletionError,
    ) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            node,
            error: Some(error),
        }
    }
}

impl Operation for BlockedOperation {
    type Output = GroupDeletionError;
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<GroupDeletionError>, GroupDeletionError> {
        match &self.error {
            Some(GroupDeletionError::NotEmpty { buckets, .. }) => {
                let names = buckets.clone();
                let visible = VisibleOperation::new(&self.auth, self.group_id, self.node, names);
                Ok(Next::Call(IoCall::child(visible)))
            }
            _ => Ok(Next::Done(self.error.take().unwrap_or(
                GroupDeletionError::Unavailable("deletion refusal is missing".into()),
            ))),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GroupDeletionError>, GroupDeletionError> {
        let names = result.child::<Vec<String>, GroupDeletionError>()??;
        let Some(GroupDeletionError::NotEmpty {
            kinds,
            buckets,
            more,
            hidden,
        }) = self.error.take()
        else {
            return Err(GroupDeletionError::Unavailable(
                "deletion refusal is missing".into(),
            ));
        };
        let hidden = hidden + (buckets.len() - names.len()) as u64;
        Ok(Next::Done(GroupDeletionError::NotEmpty {
            kinds: vec![format!(
                "node {}: {}",
                self.node,
                render(&kinds, &names, hidden, more)
            )],
            buckets: Vec::new(),
            more: false,
            hidden: 0,
        }))
    }
}

#[cfg(test)]
#[path = "access_tests.rs"]
mod tests;
